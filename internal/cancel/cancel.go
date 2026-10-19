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
	"github.com/ananya-mh/batch-inference/internal/metrics"
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
// to another tenant. Cancelled requests are counted in m.
func Batch(ctx context.Context, pool *pgxpool.Pool, riverClient *river.Client[pgx.Tx], m *metrics.Metrics, tenantID, batchID string) (batches.Batch, error) {
	for range maxTries {
		batch, err := batches.Get(ctx, pool, tenantID, batchID)
		if err != nil {
			return batches.Batch{}, err
		}

		switch batch.Status {
		case batches.StatusCancelling, batches.StatusCancelled:
			return batch, nil
		case batches.StatusValidating, batches.StatusInProgress:
			cancelled, err := cancelInTx(ctx, pool, riverClient, batchID, batch.Status)
			if errors.Is(err, batches.ErrStatusChanged) {
				// Something changed the status since we read it. Look
				// again and act on the new status.
				continue
			}
			if err != nil {
				return batches.Batch{}, err
			}
			// Counted after the commit, so a failed cancel counts nothing.
			m.RequestOutcomes.WithLabelValues(tenantID, metrics.OutcomeCancelled).Add(float64(cancelled))
			return batches.Get(ctx, pool, tenantID, batchID)
		default:
			return batch, &NotCancellableError{Status: batch.Status}
		}
	}
	return batches.Batch{}, fmt.Errorf("batch %s kept changing status while cancelling", batchID)
}

// cancelInTx does the cancel in one transaction: the status change and the
// cancelling of every request that has not started. It returns how many
// requests it cancelled.
func cancelInTx(ctx context.Context, pool *pgxpool.Pool, riverClient *river.Client[pgx.Tx], batchID string, from batches.Status) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin cancel: %w", err)
	}
	defer database.RollbackUnlessCommitted(ctx, tx)

	// Compare-and-set to cancelling. This needs the batch row exclusively,
	// so it waits for any worker start check happening right now (they hold
	// it with FOR SHARE); every start check after the commit sees
	// cancelling and does not start (guarantee 3).
	if err := batches.Transition(ctx, tx, batchID, from, batches.StatusCancelling); err != nil {
		return 0, err
	}

	var cancelled int
	if from == batches.StatusValidating {
		// Not fanned out yet: create its requests already cancelled, so
		// every line still shows up in the errors file.
		cancelled, err = requests.CreateForBatch(ctx, tx, batchID, requests.StatusCancelled)
	} else {
		cancelled, err = cancelUnstartedRequests(ctx, tx, riverClient, batchID)
	}
	if err != nil {
		return 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit cancel: %w", err)
	}
	return cancelled, nil
}

// cancelUnstartedRequests cancels every request of the batch that is not
// running right now: waiting ones (no River job yet), queued ones (including
// ones waiting for a retry), and running ones whose job is no longer running
// (an earlier attempt's worker crashed). Those with a River job also get it
// cancelled, so it never runs again. Only requests mid-call are left to
// finish. It returns how many requests it cancelled.
func cancelUnstartedRequests(ctx context.Context, tx pgx.Tx, riverClient *river.Client[pgx.Tx], batchID string) (int, error) {
	waiting, err := tx.Exec(ctx, `
		UPDATE requests SET status = $2, finished_at = clock_timestamp()
		WHERE batch_id = $1 AND status = $3`,
		batchID, requests.StatusCancelled, requests.StatusPending,
	)
	if err != nil {
		return 0, fmt.Errorf("cancel waiting requests: %w", err)
	}
	cancelled := int(waiting.RowsAffected())

	// Queued requests are cancelled whatever River shows for their job: a
	// worker sets a failed request back to queued before River records the
	// failure, so River may still show the job as running for a moment.
	rows, err := tx.Query(ctx, `
		SELECT r.custom_id, r.river_job_id
		FROM requests r
		JOIN river_job j ON j.id = r.river_job_id
		WHERE r.batch_id = $1
		  AND (r.status = $2
		       OR (r.status = $3 AND j.state IN ('available', 'scheduled', 'retryable')))`,
		batchID, requests.StatusQueued, requests.StatusRunning,
	)
	if err != nil {
		return 0, fmt.Errorf("find unstarted requests: %w", err)
	}
	type unstarted struct {
		CustomID string
		JobID    int64
	}
	found, err := pgx.CollectRows(rows, pgx.RowToStructByPos[unstarted])
	if err != nil {
		return 0, fmt.Errorf("read unstarted requests: %w", err)
	}

	for _, request := range found {
		// The request first, then its job: the same order workers lock
		// them in when saving a result, so the two can never deadlock.
		tag, err := tx.Exec(ctx, `
			UPDATE requests SET status = $3, finished_at = clock_timestamp()
			WHERE batch_id = $1 AND custom_id = $2 AND status IN ($4, $5)`,
			batchID, request.CustomID, requests.StatusCancelled, requests.StatusQueued, requests.StatusRunning,
		)
		if err != nil {
			return 0, fmt.Errorf("cancel request %s: %w", request.CustomID, err)
		}
		cancelled += int(tag.RowsAffected())
		// If River shows the job as running (it was fetched a moment ago, or
		// its failure is not recorded yet), this marks it for cancellation:
		// a start check then sees the batch is cancelling, and a failed
		// attempt ends cancelled instead of being retried.
		if _, err := riverClient.JobCancelTx(ctx, tx, request.JobID); err != nil {
			return 0, fmt.Errorf("cancel job of %s: %w", request.CustomID, err)
		}
	}
	return cancelled, nil
}
