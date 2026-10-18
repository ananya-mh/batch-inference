// This file creates and updates the database tables: River's job tables and
// our own tables from the SQL files in the migrations folder.
package database

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// migrationLockID is an arbitrary number used as a Postgres advisory lock key,
// so only one process runs migrations at a time.
const migrationLockID = 727_001

// Migrate brings the database schema up to date. It is safe to call on every
// startup and from several processes at once.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	// Hold one connection for the whole run: an advisory lock belongs to the
	// connection that took it.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection for migrations: %w", err)
	}
	defer conn.Release()

	// If two servers start together, the second waits here until the first
	// has finished, instead of both trying to create the same tables.
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockID); err != nil {
		return fmt.Errorf("take migration lock: %w", err)
	}
	defer unlockMigrations(conn)

	if err := migrateRiver(ctx, pool); err != nil {
		return err
	}
	return migrateApp(ctx, conn.Conn())
}

// unlockMigrations releases the migration lock. It uses a fresh context so the
// lock is still released when the caller's context has been cancelled.
func unlockMigrations(conn *pgxpool.Conn) {
	if _, err := conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", migrationLockID); err != nil {
		// The connection is probably broken. Closing it makes Postgres drop
		// the lock, rather than leaving it held on a pooled connection.
		slog.Error("release migration lock", "error", err)
		if err := conn.Hijack().Close(context.Background()); err != nil {
			slog.Error("close connection holding migration lock", "error", err)
		}
	}
}

// migrateRiver creates or updates the tables River needs for its job queue.
func migrateRiver(ctx context.Context, pool *pgxpool.Pool) error {
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), nil)
	if err != nil {
		return fmt.Errorf("create river migrator: %w", err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		return fmt.Errorf("run river migrations: %w", err)
	}
	return nil
}

// migrateApp runs each of our SQL migration files that has not run yet, in
// file name order, and records it in the schema_migrations table.
func migrateApp(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`)
	if err != nil {
		return fmt.Errorf("create schema_migrations table: %w", err)
	}

	names, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		return fmt.Errorf("list migration files: %w", err)
	}
	// Files are numbered (0001_, 0002_, ...), so sorting by name gives the
	// order they must run in.
	sort.Strings(names)

	for _, name := range names {
		if err := applyMigration(ctx, conn, name); err != nil {
			return err
		}
	}
	return nil
}

// applyMigration runs one migration file, unless it has already been applied.
func applyMigration(ctx context.Context, conn *pgx.Conn, name string) error {
	var alreadyApplied bool
	err := conn.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)", name,
	).Scan(&alreadyApplied)
	if err != nil {
		return fmt.Errorf("check migration %s: %w", name, err)
	}
	if alreadyApplied {
		return nil
	}

	sql, err := migrationFiles.ReadFile(name)
	if err != nil {
		return fmt.Errorf("read migration %s: %w", name, err)
	}

	// Run the file and record it in one transaction, so a failed migration
	// leaves no half-created tables behind and is retried on the next start.
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration %s: %w", name, err)
	}
	defer rollbackUnlessCommitted(ctx, tx)

	// Exec with no arguments uses Postgres's simple protocol, which allows
	// a file to contain several statements.
	if _, err := tx.Exec(ctx, string(sql)); err != nil {
		return fmt.Errorf("run migration %s: %w", name, err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", name); err != nil {
		return fmt.Errorf("record migration %s: %w", name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration %s: %w", name, err)
	}
	return nil
}

// rollbackUnlessCommitted undoes a transaction that was not committed. After a
// successful commit, Rollback returns pgx.ErrTxClosed, which is expected.
func rollbackUnlessCommitted(ctx context.Context, tx pgx.Tx) {
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		// The caller is already returning the error that caused the
		// rollback, so just log this one.
		slog.Error("roll back migration", "error", err)
	}
}
