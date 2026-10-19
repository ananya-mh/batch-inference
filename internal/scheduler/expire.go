// This file expires batches whose completion window has ended.
package scheduler

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ananya-mh/batch-inference/internal/batches"
	"github.com/ananya-mh/batch-inference/internal/database"
	"github.com/ananya-mh/batch-inference/internal/metrics"
	"github.com/ananya-mh/batch-inference/internal/requests"
	"github.com/ananya-mh/batch-inference/internal/results"
)

// expireOverdueBatches expires every batch whose deadline has passed while
// it still has unfinished work, and returns how many it expired.
//
// A batch whose requests have all finished is left alone even if its
// deadline has passed: its work is done, so it goes on to finalizing and
// completed. That is also what keeps expiry and finalizing from both
// happening to one batch.
func (s *Scheduler) expireOverdueBatches(ctx context.Context, tx pgx.Tx, tally metrics.Tally) (int, error) {
	// The deadline is compared with the database's clock, so every server
	// agrees on when a batch is overdue, whatever their own clocks say.
	rows, err := tx.Query(ctx, `
		SELECT b.id, b.status, b.tenant_id
		FROM batches b
		WHERE b.expires_at <= clock_timestamp()
		  AND (b.status = $1
		       OR (b.status = $2 AND EXISTS (
				SELECT 1 FROM requests r
				WHERE r.batch_id = b.id AND r.status IN ($3, $4, $5))))
		ORDER BY b.expires_at`,
		batches.StatusValidating, batches.StatusInProgress,
		requests.StatusPending, requests.StatusQueued, requests.StatusRunning,
	)
	if err != nil {
		return 0, fmt.Errorf("find overdue batches: %w", err)
	}
	type overdueBatch struct {
		ID       string
		Status   batches.Status
		TenantID string
	}
	overdue, err := pgx.CollectRows(rows, pgx.RowToStructByPos[overdueBatch])
	if err != nil {
		return 0, fmt.Errorf("read overdue batches: %w", err)
	}

	expired := 0
	for _, batch := range overdue {
		expiredRequests, err := s.expireBatch(ctx, tx, batch.ID, batch.Status)
		if err != nil {
			return expired, err
		}
		if expiredRequests > 0 {
			expired++
			tally.Add(batch.TenantID, metrics.OutcomeExpired, expiredRequests)
		}
	}
	return expired, nil
}

// expireBatch expires one batch inside a savepoint: it moves the batch to
// expired, marks every unfinished request expired, cancels their River jobs,
// and writes the results and errors files. It returns how many requests it
// expired: 0, changing nothing, if the batch's status changed first (a
// cancel won) or if its last requests turned out to have finished in time.
func (s *Scheduler) expireBatch(ctx context.Context, tx pgx.Tx, batchID string, from batches.Status) (int, error) {
	savepoint, err := tx.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin expiring %s: %w", batchID, err)
	}
	defer database.RollbackUnlessCommitted(ctx, savepoint)

	// Compare-and-set: if a cancel got there first, the cancel wins.
	err = batches.Transition(ctx, savepoint, batchID, from, batches.StatusExpired)
	if errors.Is(err, batches.ErrStatusChanged) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	var expiredCount int
	if from == batches.StatusValidating {
		// Never fanned out: create its requests already expired, so every
		// line still appears in the errors file.
		if expiredCount, err = requests.CreateForBatch(ctx, savepoint, batchID, requests.StatusExpired); err != nil {
			return 0, err
		}
	} else {
		expiredCount, err = s.expireUnfinishedRequests(ctx, savepoint, batchID)
		if err != nil {
			return 0, err
		}
		// The batch looked unfinished when we picked it, but a worker's save
		// that ran before the deadline may have committed since, finishing
		// the last request in time. Then the batch's work is done: undo, and
		// let it go on to finalizing and completed (see "Bugs found").
		if expiredCount == 0 {
			return 0, nil
		}
	}

	outputFileID, errorFileID, err := results.WriteFiles(ctx, savepoint, batchID)
	if err != nil {
		return 0, err
	}
	if _, err := savepoint.Exec(ctx,
		"UPDATE batches SET output_file_id = $2, error_file_id = $3 WHERE id = $1",
		batchID, outputFileID, errorFileID,
	); err != nil {
		return 0, fmt.Errorf("record files of batch %s: %w", batchID, err)
	}

	if err := savepoint.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit expiring %s: %w", batchID, err)
	}
	return expiredCount, nil
}

// expireUnfinishedRequests marks every pending, queued, or running request of
// the batch expired, including ones mid-call, and cancels the River jobs of
// the ones that had a job. A request mid-call that returns afterwards finds
// itself no longer running, so its late result is not saved. It returns how
// many requests it expired.
func (s *Scheduler) expireUnfinishedRequests(ctx context.Context, tx pgx.Tx, batchID string) (int, error) {
	rows, err := tx.Query(ctx, `
		UPDATE requests SET status = $2, finished_at = clock_timestamp()
		WHERE batch_id = $1 AND status IN ($3, $4, $5)
		RETURNING river_job_id`,
		batchID, requests.StatusExpired, requests.StatusPending, requests.StatusQueued, requests.StatusRunning,
	)
	if err != nil {
		return 0, fmt.Errorf("expire requests of %s: %w", batchID, err)
	}
	jobIDs, err := pgx.CollectRows(rows, pgx.RowTo[*int64])
	if err != nil {
		return 0, fmt.Errorf("read expired requests of %s: %w", batchID, err)
	}

	// Cancel their jobs: waiting ones never run, and running ones get their
	// context cancelled, which stops a model call whose result would be
	// dropped anyway. Pending requests had no job (nil).
	for _, jobID := range jobIDs {
		if jobID == nil {
			continue
		}
		if _, err := s.river.JobCancelTx(ctx, tx, *jobID); err != nil {
			return 0, fmt.Errorf("cancel job %d: %w", *jobID, err)
		}
	}
	return len(jobIDs), nil
}
