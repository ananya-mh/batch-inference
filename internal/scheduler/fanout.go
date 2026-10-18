// This file turns new (validating) batches into one request row per line.
package scheduler

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ananya-mh/batch-inference/internal/batches"
	"github.com/ananya-mh/batch-inference/internal/database"
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

// fanOutBatch creates one batch's request rows and moves it to in_progress.
// It returns false if the batch's status changed meanwhile (it was
// cancelled), in which case nothing is created.
func fanOutBatch(ctx context.Context, tx pgx.Tx, batchID string) (bool, error) {
	// A savepoint, so a batch that turns out to be cancelled can be undone
	// without undoing the rest of the pass.
	savepoint, err := tx.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin fan-out of %s: %w", batchID, err)
	}
	defer database.RollbackUnlessCommitted(ctx, savepoint)

	// One statement for the whole batch. ORDER BY makes seq follow line
	// order, so a batch's requests run in file order.
	_, err = savepoint.Exec(ctx, `
		INSERT INTO requests (batch_id, custom_id, tenant_id, line_number)
		SELECT b.id, l.custom_id, b.tenant_id, l.line_number
		FROM batches b
		JOIN file_lines l ON l.file_id = b.input_file_id
		WHERE b.id = $1
		ORDER BY l.line_number`,
		batchID,
	)
	if err != nil {
		return false, fmt.Errorf("create requests for %s: %w", batchID, err)
	}

	err = batches.Transition(ctx, savepoint, batchID, batches.StatusValidating, batches.StatusInProgress)
	if errors.Is(err, batches.ErrStatusChanged) {
		// Cancelled while we worked: the deferred rollback removes the
		// requests just created.
		return false, nil
	}
	if err != nil {
		return false, err
	}

	if err := savepoint.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit fan-out of %s: %w", batchID, err)
	}
	return true, nil
}
