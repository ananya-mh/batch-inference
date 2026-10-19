// Package cancel cancels batches. See "Guarantee 3: cancel" in DESIGN.md.
package cancel

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/ananya-mh/batch-inference/internal/batches"
	"github.com/ananya-mh/batch-inference/internal/database"
	"github.com/ananya-mh/batch-inference/internal/requests"
)

// maxTries is how many times Batch re-reads the status when it changes
// under it (for example, the scheduler moving the batch from validating to
// in_progress at the same moment). Two is always enough in practice.
const maxTries = 5

// NotCancellableError means the batch has already reached a status cancel
// cannot undo: finalizing, completed, failed, or expired.
type NotCancellableError struct {
	Status batches.Status
}

// Error uses OpenAI's wording.
func (e *NotCancellableError) Error() string {
	return fmt.Sprintf("Cannot cancel a batch with status '%s'.", e.Status)
}

// Batch cancels a tenant's batch and returns it as it is afterwards.
//
//   - validating or in_progress: moves it to cancelling. Requests that have
//     not started are cancelled at once and give back their slots; requests
//     already running finish, and then the scheduler moves the batch to
//     cancelled.
//   - cancelling or cancelled: nothing to do; returns the batch as it is, so
//     cancelling twice is harmless.
//   - anything else: returns *NotCancellableError.
//
// It returns batches.ErrNotFound for a batch that doesn't exist or belongs
// to another tenant.
func Batch(ctx context.Context, pool *pgxpool.Pool, riverClient *river.Client[pgx.Tx], tenantID, batchID string) (batches.Batch, error) {
	for range maxTries {
		batch, err := batches.Get(ctx, pool, tenantID, batchID)
		if err != nil {
			return batches.Batch{}, err
		}

		switch batch.Status {
		case batches.StatusCancelling, batches.StatusCancelled:
			return batch, nil
		case batches.StatusValidating, batches.StatusInProgress:
			err := cancelInTx(ctx, pool, riverClient, batchID, batch.Status)
			if errors.Is(err, batches.ErrStatusChanged) {
				// Something changed the status since we read it. Look
				// again and act on the new status.
				continue
			}
			if err != nil {
				return batches.Batch{}, err
			}
			return batches.Get(ctx, pool, tenantID, batchID)
		default:
			return batch, &NotCancellableError{Status: batch.Status}
		}
	}
	return batches.Batch{}, fmt.Errorf("batch %s kept changing status while cancelling", batchID)
}

// cancelInTx does the cancel in one transaction: the status change and the
// cancelling of every request that has not started.
func cancelInTx(ctx context.Context, pool *pgxpool.Pool, riverClient *river.Client[pgx.Tx], batchID string, from batches.Status) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin cancel: %w", err)
	}
	defer database.RollbackUnlessCommitted(ctx, tx)

	// Compare-and-set to cancelling. This needs the batch row exclusively,
	// so it waits for any worker start check happening right now (they hold
	// it with FOR SHARE); every start check after the commit sees
	// cancelling and does not start (guarantee 3).
	if err := batches.Transition(ctx, tx, batchID, from, batches.StatusCancelling); err != nil {
		return err
	}

	if from == batches.StatusValidating {
		// Not fanned out yet: create its requests already cancelled, so
		// every line still shows up in the errors file.
		if err := requests.CreateForBatch(ctx, tx, batchID, requests.StatusCancelled); err != nil {
			return err
		}
	} else if err := cancelUnstartedRequests(ctx, tx, riverClient, batchID); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit cancel: %w", err)
	}
	return nil
}

// cancelUnstartedRequests cancels every request of the batch that is not
// running right now: waiting ones (no River job yet), queued ones, and ones
// waiting for a retry. Queued and retrying ones also get their River job
// cancelled, so it never runs again. Only requests whose job is running at
// this moment are left to finish.
func cancelUnstartedRequests(ctx context.Context, tx pgx.Tx, riverClient *river.Client[pgx.Tx], batchID string) error {
	_, err := tx.Exec(ctx, `
		UPDATE requests SET status = $2, finished_at = clock_timestamp()
		WHERE batch_id = $1 AND status = $3`,
		batchID, requests.StatusCancelled, requests.StatusPending,
	)
	if err != nil {
		return fmt.Errorf("cancel waiting requests: %w", err)
	}

	// A request waiting for a retry is still "running" in our table, but its
	// River job is retryable, not running, so it is cancelled here too.
	rows, err := tx.Query(ctx, `
		SELECT r.custom_id, r.river_job_id
		FROM requests r
		JOIN river_job j ON j.id = r.river_job_id
		WHERE r.batch_id = $1
		  AND r.status IN ($2, $3)
		  AND j.state IN ('available', 'scheduled', 'retryable')`,
		batchID, requests.StatusQueued, requests.StatusRunning,
	)
	if err != nil {
		return fmt.Errorf("find unstarted requests: %w", err)
	}
	type unstarted struct {
		CustomID string
		JobID    int64
	}
	found, err := pgx.CollectRows(rows, pgx.RowToStructByPos[unstarted])
	if err != nil {
		return fmt.Errorf("read unstarted requests: %w", err)
	}

	for _, request := range found {
		// The request first, then its job: the same order workers lock
		// them in when saving a result, so the two can never deadlock.
		_, err := tx.Exec(ctx, `
			UPDATE requests SET status = $3, finished_at = clock_timestamp()
			WHERE batch_id = $1 AND custom_id = $2 AND status IN ($4, $5)`,
			batchID, request.CustomID, requests.StatusCancelled, requests.StatusQueued, requests.StatusRunning,
		)
		if err != nil {
			return fmt.Errorf("cancel request %s: %w", request.CustomID, err)
		}
		// If River fetched the job a moment ago, this only signals the
		// worker; its start check then waits for this transaction and sees
		// the batch is cancelling, so the model is still never called.
		if _, err := riverClient.JobCancelTx(ctx, tx, request.JobID); err != nil {
			return fmt.Errorf("cancel job of %s: %w", request.CustomID, err)
		}
	}
	return nil
}
