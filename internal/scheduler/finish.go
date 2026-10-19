// This file finishes things: requests whose jobs died without a result, and
// batches whose requests are all done (finalizing, then completed with their
// results and errors files).
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ananya-mh/batch-inference/internal/batches"
	"github.com/ananya-mh/batch-inference/internal/database"
	"github.com/ananya-mh/batch-inference/internal/metrics"
	"github.com/ananya-mh/batch-inference/internal/requests"
	"github.com/ananya-mh/batch-inference/internal/results"
)

// giveUpOnAbandonedRequests fails in-flight requests whose River job has
// ended without saving a result, and returns how many it failed.
//
// Workers always save a result before their last attempt ends, so this only
// happens when the worker crashed (or panicked) on every attempt and River
// gave up on the job. Without this, such a request would hold a slot forever
// and its batch would never finish.
func giveUpOnAbandonedRequests(ctx context.Context, tx pgx.Tx, tally metrics.Tally) (int, error) {
	rows, err := tx.Query(ctx, `
		SELECT r.batch_id, r.custom_id, r.tenant_id, coalesce(j.state::text, 'missing')
		FROM requests r
		LEFT JOIN river_job j ON j.id = r.river_job_id
		WHERE r.status IN ($1, $2)
		  AND (j.id IS NULL OR j.state IN ('discarded', 'cancelled'))`,
		requests.StatusQueued, requests.StatusRunning,
	)
	if err != nil {
		return 0, fmt.Errorf("find abandoned requests: %w", err)
	}
	type abandoned struct {
		BatchID  string
		CustomID string
		TenantID string
		JobState string
	}
	found, err := pgx.CollectRows(rows, pgx.RowToStructByPos[abandoned])
	if err != nil {
		return 0, fmt.Errorf("read abandoned requests: %w", err)
	}

	gaveUp := 0
	for _, request := range found {
		saved, err := requests.GiveUp(ctx, tx, request.BatchID, request.CustomID, requests.Outcome{
			ErrorCode: "worker_failed",
			ErrorMessage: fmt.Sprintf(
				"gave up: the request's job ended (%s) without a result, usually because the worker crashed on every attempt",
				request.JobState),
		})
		if err != nil {
			return gaveUp, err
		}
		if saved {
			gaveUp++
			tally.Add(request.TenantID, metrics.OutcomeFailed, 1)
		}
	}
	return gaveUp, nil
}

// startFinalizingDoneBatches moves in-progress batches that have no pending
// or in-flight requests left to finalizing, and returns how many it moved.
func startFinalizingDoneBatches(ctx context.Context, tx pgx.Tx) (int, error) {
	rows, err := tx.Query(ctx, `
		SELECT b.id
		FROM batches b
		WHERE b.status = $1
		  AND NOT EXISTS (
			SELECT 1 FROM requests r
			WHERE r.batch_id = b.id AND r.status IN ($2, $3, $4)
		  )`,
		batches.StatusInProgress, requests.StatusPending, requests.StatusQueued, requests.StatusRunning,
	)
	if err != nil {
		return 0, fmt.Errorf("find finished batches: %w", err)
	}
	batchIDs, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, fmt.Errorf("read finished batches: %w", err)
	}

	moved := 0
	for _, batchID := range batchIDs {
		err := batches.Transition(ctx, tx, batchID, batches.StatusInProgress, batches.StatusFinalizing)
		if errors.Is(err, batches.ErrStatusChanged) {
			// Cancelled at the same moment, and the cancel won. The batch
			// will become cancelled instead; nothing to do here.
			continue
		}
		if err != nil {
			return moved, err
		}
		moved++
	}
	return moved, nil
}

// endStatus is the final status each ending status leads to, once the
// batch's files are written.
var endStatus = map[batches.Status]batches.Status{
	batches.StatusFinalizing: batches.StatusCompleted,
	batches.StatusCancelling: batches.StatusCancelled,
}

// completeEndingBatches writes the files of every batch that is ready to end
// (finalizing batches, and cancelling batches whose running requests have
// all finished) and moves each to its final status, one batch per
// transaction. It returns how many batches it ended.
func completeEndingBatches(ctx context.Context, pool *pgxpool.Pool, m *metrics.Metrics) (int, error) {
	// A cancelling batch waits until its in-flight requests finish, so
	// their results make it into the files.
	rows, err := pool.Query(ctx, `
		SELECT b.id, b.status
		FROM batches b
		WHERE b.status = $1
		   OR (b.status = $2 AND NOT EXISTS (
				SELECT 1 FROM requests r
				WHERE r.batch_id = b.id AND r.status IN ($3, $4, $5)))
		ORDER BY b.created_at`,
		batches.StatusFinalizing, batches.StatusCancelling,
		requests.StatusPending, requests.StatusQueued, requests.StatusRunning,
	)
	if err != nil {
		return 0, fmt.Errorf("find batches ready to end: %w", err)
	}
	type endingBatch struct {
		ID     string
		Status batches.Status
	}
	ready, err := pgx.CollectRows(rows, pgx.RowToStructByPos[endingBatch])
	if err != nil {
		return 0, fmt.Errorf("read batches ready to end: %w", err)
	}

	completed := 0
	for _, batch := range ready {
		done, err := completeBatch(ctx, pool, m, batch.ID, batch.Status, endStatus[batch.Status])
		if err != nil {
			return completed, err
		}
		if done {
			completed++
		}
	}
	return completed, nil
}

// completeBatch writes one batch's results and errors files, sets
// output_file_id and error_file_id, and moves it from status `from`
// (finalizing or cancelling) to `to` (completed or cancelled), all in one
// transaction. A crash at any point before the commit leaves no files behind
// and the batch unchanged, so the next pass simply does it again; the files
// can never be written twice or half-written. It returns false if another
// server is already completing this batch.
func completeBatch(ctx context.Context, pool *pgxpool.Pool, m *metrics.Metrics, batchID string, from, to batches.Status) (bool, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin completing %s: %w", batchID, err)
	}
	defer database.RollbackUnlessCommitted(ctx, tx)

	// Lock the batch row. SKIP LOCKED: if another server is completing this
	// batch right now, leave it to them rather than waiting.
	var lockedID string
	err = tx.QueryRow(ctx,
		"SELECT id FROM batches WHERE id = $1 AND status = $2 FOR UPDATE SKIP LOCKED",
		batchID, from,
	).Scan(&lockedID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock batch %s: %w", batchID, err)
	}

	writeStart := time.Now()
	outputFileID, errorFileID, err := results.WriteFiles(ctx, tx, batchID)
	m.ResultsFilesWriteSeconds.Observe(time.Since(writeStart).Seconds())
	if err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx,
		"UPDATE batches SET output_file_id = $2, error_file_id = $3 WHERE id = $1",
		batchID, outputFileID, errorFileID,
	); err != nil {
		return false, fmt.Errorf("record files of batch %s: %w", batchID, err)
	}
	if err := batches.Transition(ctx, tx, batchID, from, to); err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit completing %s: %w", batchID, err)
	}
	return true, nil
}
