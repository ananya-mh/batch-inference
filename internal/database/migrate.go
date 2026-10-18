// This file creates and updates the database tables: River's job tables and
// our own tables from the SQL files in the migrations folder.
package database

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
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
//
// Everything runs in one transaction on one connection. An earlier version
// held the lock on one connection while asking the pool for a second one to
// run River's migrations; with a small pool and several callers, the waiting
// callers held every connection and it deadlocked (see "Bugs found" in
// DESIGN.md).
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migrations: %w", err)
	}
	defer RollbackUnlessCommitted(ctx, tx)

	// If two servers start together, the second waits here until the first
	// has committed, instead of both trying to create the same tables. A
	// transaction-level lock is released by commit or rollback on its own,
	// so it can never be left held by mistake.
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", migrationLockID); err != nil {
		return fmt.Errorf("take migration lock: %w", err)
	}

	if err := migrateRiver(ctx, pool, tx); err != nil {
		return err
	}
	if err := migrateApp(ctx, tx); err != nil {
		return err
	}

	// Committing applies every migration at once: a failure anywhere leaves
	// the database as it was, and the next start simply tries again.
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migrations: %w", err)
	}
	return nil
}

// migrateRiver creates or updates the tables River needs for its job queue,
// inside tx.
func migrateRiver(ctx context.Context, pool *pgxpool.Pool, tx pgx.Tx) error {
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), nil)
	if err != nil {
		return fmt.Errorf("create river migrator: %w", err)
	}
	if _, err := migrator.MigrateTx(ctx, tx, rivermigrate.DirectionUp, nil); err != nil {
		return fmt.Errorf("run river migrations: %w", err)
	}
	return nil
}

// migrateApp runs each of our SQL migration files that has not run yet, in
// file name order, and records it in the schema_migrations table.
func migrateApp(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, `
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
		if err := applyMigration(ctx, tx, name); err != nil {
			return err
		}
	}
	return nil
}

// applyMigration runs one migration file, unless it has already been applied.
func applyMigration(ctx context.Context, tx pgx.Tx, name string) error {
	var alreadyApplied bool
	err := tx.QueryRow(ctx,
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
	// Exec with no arguments uses Postgres's simple protocol, which allows
	// a file to contain several statements.
	if _, err := tx.Exec(ctx, string(sql)); err != nil {
		return fmt.Errorf("run migration %s: %w", name, err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", name); err != nil {
		return fmt.Errorf("record migration %s: %w", name, err)
	}
	return nil
}
