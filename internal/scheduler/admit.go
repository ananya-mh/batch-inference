// This file admits pending requests into the free slots of the window by
// giving each one a River job.
package scheduler

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"

	"github.com/ananya-mh/batch-inference/internal/batches"
	"github.com/ananya-mh/batch-inference/internal/requests"
)

// requestKey names one request.
type requestKey struct {
	BatchID  string
	CustomID string
}

// admit fills the window's free slots with pending requests and returns how
// many it admitted.
func (s *Scheduler) admit(ctx context.Context, tx pgx.Tx) (int, error) {
	var inFlight int
	err := tx.QueryRow(ctx,
		"SELECT count(*) FROM requests WHERE status IN ($1, $2)",
		requests.StatusQueued, requests.StatusRunning,
	).Scan(&inFlight)
	if err != nil {
		return 0, fmt.Errorf("count requests in flight: %w", err)
	}
	free := s.config.Window - inFlight
	if free <= 0 {
		return 0, nil
	}

	pick := pickOldestFirst
	if s.config.FairScheduling {
		pick = pickFairly
	}
	picked, err := pick(ctx, tx, free)
	if err != nil {
		return 0, err
	}
	if len(picked) == 0 {
		return 0, nil
	}
	if err := s.queue(ctx, tx, picked); err != nil {
		return 0, err
	}
	return len(picked), nil
}

// pickOldestFirst returns up to limit pending requests of in-progress
// batches that are not past their deadline, oldest first, whichever tenant
// they belong to: first come, first served. It is used when fair scheduling
// is off, as the baseline for the fairness experiment.
func pickOldestFirst(ctx context.Context, tx pgx.Tx, limit int) ([]requestKey, error) {
	rows, err := tx.Query(ctx, `
		SELECT r.batch_id, r.custom_id
		FROM requests r
		JOIN batches b ON b.id = r.batch_id
		WHERE r.status = $1 AND b.status = $2
		  AND b.expires_at > clock_timestamp()
		ORDER BY r.seq
		LIMIT $3`,
		requests.StatusPending, batches.StatusInProgress, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("pick pending requests: %w", err)
	}
	picked, err := pgx.CollectRows(rows, pgx.RowToStructByPos[requestKey])
	if err != nil {
		return nil, fmt.Errorf("read pending requests: %w", err)
	}
	return picked, nil
}

// queue inserts a River job for each picked request and marks the requests
// queued, all inside tx. Because both happen in one transaction, there is
// never a queued request without a job, or a job whose request is not queued.
func (s *Scheduler) queue(ctx context.Context, tx pgx.Tx, picked []requestKey) error {
	params := make([]river.InsertManyParams, len(picked))
	for i, key := range picked {
		params[i] = river.InsertManyParams{Args: requests.RunArgs{BatchID: key.BatchID, CustomID: key.CustomID}}
	}
	inserted, err := s.river.InsertManyTx(ctx, tx, params)
	if err != nil {
		return fmt.Errorf("insert river jobs: %w", err)
	}

	// River returns the jobs in the order they were given.
	batchIDs := make([]string, len(picked))
	customIDs := make([]string, len(picked))
	jobIDs := make([]int64, len(picked))
	for i, key := range picked {
		batchIDs[i] = key.BatchID
		customIDs[i] = key.CustomID
		jobIDs[i] = inserted[i].Job.ID
	}

	// One UPDATE for all of them, matching rows by the three arrays.
	tag, err := tx.Exec(ctx, `
		UPDATE requests r
		SET status = $4, river_job_id = picked.job_id, queued_at = now()
		FROM unnest($1::text[], $2::text[], $3::bigint[]) AS picked(batch_id, custom_id, job_id)
		WHERE r.batch_id = picked.batch_id AND r.custom_id = picked.custom_id AND r.status = $5`,
		batchIDs, customIDs, jobIDs, requests.StatusQueued, requests.StatusPending,
	)
	if err != nil {
		return fmt.Errorf("mark requests queued: %w", err)
	}
	// Under the scheduler lock nothing else changes pending requests, so a
	// mismatch means a bug. Failing rolls back the jobs too.
	if int(tag.RowsAffected()) != len(picked) {
		return fmt.Errorf("marked %d requests queued, expected %d", tag.RowsAffected(), len(picked))
	}
	return nil
}
