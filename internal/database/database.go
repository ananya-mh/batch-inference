// Package database connects to PostgreSQL and keeps its schema up to date.
package database

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect opens a connection pool to the database at databaseURL and checks
// that the database is reachable.
func Connect(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open database pool: %w", err)
	}

	// pgxpool connects lazily, so ping now to fail fast on a bad URL or a
	// database that is down, instead of on the first real request.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

// RollbackUnlessCommitted undoes a transaction that was not committed. Call it
// with defer right after Begin. After a successful commit, Rollback returns
// pgx.ErrTxClosed, which is expected and ignored.
func RollbackUnlessCommitted(ctx context.Context, tx pgx.Tx) {
	// WithoutCancel: if the request was cancelled (say, the client hung up
	// mid-upload), the rollback must still reach the database.
	if err := tx.Rollback(context.WithoutCancel(ctx)); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		// The caller is already returning the error that caused the
		// rollback, so just log this one.
		slog.ErrorContext(ctx, "roll back transaction", "error", err)
	}
}
