// This file finishes things: requests whose jobs died without a result, and
// batches whose requests are all done.
package scheduler

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ananya-mh/batch-inference/internal/batches"
	"github.com/ananya-mh/batch-inference/internal/requests"
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

// completeFinalizingBatches moves finalizing batches to completed, and
// returns how many it moved. Writing the results and errors files will be
// added here with Progress item 6.
func completeFinalizingBatches(ctx context.Context, tx pgx.Tx) (int, error) {
	rows, err := tx.Query(ctx, "SELECT id FROM batches WHERE status = $1", batches.StatusFinalizing)
	if err != nil {
		return 0, fmt.Errorf("find finalizing batches: %w", err)
	}
	batchIDs, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, fmt.Errorf("read finalizing batches: %w", err)
	}

	for _, batchID := range batchIDs {
		if err := batches.Transition(ctx, tx, batchID, batches.StatusFinalizing, batches.StatusCompleted); err != nil {
			return 0, err
		}
	}
	return len(batchIDs), nil
}
