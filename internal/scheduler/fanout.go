// This file turns new (validating) batches into one request row per line.
package scheduler

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ananya-mh/batch-inference/internal/batches"
	"github.com/ananya-mh/batch-inference/internal/database"
	"github.com/ananya-mh/batch-inference/internal/requests"
)

// fanOut creates the request rows for every validating batch, oldest first,
// and moves each batch to in_progress. It returns how many batches it did.
func fanOut(ctx context.Context, tx pgx.Tx) (int, error) {
	rows, err := tx.Query(ctx, "SELECT id FROM batches WHERE status = $1 ORDER BY created_at, id", batches.StatusValidating)
	if err != nil {
		return 0, fmt.Errorf("find validating batches: %w", err)
	}
	batchIDs, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, fmt.Errorf("read validating batches: %w", err)
	}

	fannedOut := 0
	for _, batchID := range batchIDs {
		done, err := fanOutBatch(ctx, tx, batchID)
		if err != nil {
			return fannedOut, err
		}
		if done {
			fannedOut++
		}
	}
	return fannedOut, nil
}

// fanOutBatch moves one batch to in_progress and creates its request rows.
// It returns false if the batch's status changed meanwhile (it was
// cancelled), in which case nothing is created.
func fanOutBatch(ctx context.Context, tx pgx.Tx, batchID string) (bool, error) {
	// A savepoint, so one batch's failure can be undone without undoing the
	// rest of the pass.
	savepoint, err := tx.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin fan-out of %s: %w", batchID, err)
	}
	defer database.RollbackUnlessCommitted(ctx, savepoint)

	// Change the status first. If the batch was cancelled meanwhile, this
	// fails and nothing is created. Going first also locks the batch row, so
	// a cancel arriving now waits until this pass commits, then sees
	// in_progress and cancels the requests created here.
	err = batches.Transition(ctx, savepoint, batchID, batches.StatusValidating, batches.StatusInProgress)
	if errors.Is(err, batches.ErrStatusChanged) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	if _, err := requests.CreateForBatch(ctx, savepoint, batchID, requests.StatusPending); err != nil {
		return false, err
	}

	if err := savepoint.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit fan-out of %s: %w", batchID, err)
	}
	return true, nil
}
