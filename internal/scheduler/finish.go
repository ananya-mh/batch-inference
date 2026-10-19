// This file finishes things: requests whose jobs died without a result, and
// batches whose requests are all done (finalizing, then completed with their
// results and errors files).
package scheduler

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ananya-mh/batch-inference/internal/batches"
	"github.com/ananya-mh/batch-inference/internal/database"
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
func giveUpOnAbandonedRequests(ctx context.Context, tx pgx.Tx) (int, error) {
	rows, err := tx.Query(ctx, `
		SELECT r.batch_id, r.custom_id, coalesce(j.state::text, 'missing')
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

	for _, batchID := range batchIDs {
		if err := batches.Transition(ctx, tx, batchID, batches.StatusInProgress, batches.StatusFinalizing); err != nil {
			return 0, err
		}
	}
	return len(batchIDs), nil
}

// completeFinalizingBatches writes the files of every finalizing batch and
// moves it to completed, one batch per transaction, and returns how many it
// completed.
func completeFinalizingBatches(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	rows, err := pool.Query(ctx, "SELECT id FROM batches WHERE status = $1 ORDER BY finalizing_at", batches.StatusFinalizing)
	if err != nil {
		return 0, fmt.Errorf("find finalizing batches: %w", err)
	}
	batchIDs, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, fmt.Errorf("read finalizing batches: %w", err)
	}

	completed := 0
	for _, batchID := range batchIDs {
		done, err := completeBatch(ctx, pool, batchID)
		if err != nil {
			return completed, err
		}
		if done {
			completed++
		}
	}
	return completed, nil
}

// completeBatch writes one finalizing batch's results and errors files, sets
// output_file_id and error_file_id, and moves it to completed, all in one
// transaction. A crash at any point before the commit leaves no files behind
// and the batch still finalizing, so the next pass simply does it again; the
// files can never be written twice or half-written. It returns false if
// another server is already completing this batch.
func completeBatch(ctx context.Context, pool *pgxpool.Pool, batchID string) (bool, error) {
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
		batchID, batches.StatusFinalizing,
	).Scan(&lockedID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock batch %s: %w", batchID, err)
	}

	outputFileID, errorFileID, err := results.WriteFiles(ctx, tx, batchID)
	if err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx,
		"UPDATE batches SET output_file_id = $2, error_file_id = $3 WHERE id = $1",
		batchID, outputFileID, errorFileID,
	); err != nil {
		return false, fmt.Errorf("record files of batch %s: %w", batchID, err)
	}
	if err := batches.Transition(ctx, tx, batchID, batches.StatusFinalizing, batches.StatusCompleted); err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit completing %s: %w", batchID, err)
	}
	return true, nil
}
