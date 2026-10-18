// Tests for the migration runner in migrate.go.
package database_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ananya-mh/batch-inference/internal/database"
	"github.com/ananya-mh/batch-inference/internal/testdb"
)

// TestMigrateCreatesRiverAndAppTables checks that both River's tables and
// ours exist after migrating. testdb.New has already run Migrate once.
func TestMigrateCreatesRiverAndAppTables(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)

	for _, table := range []string{"river_job", "tenants", "api_keys", "schema_migrations"} {
		if !tableExists(t, pool, table) {
			t.Errorf("table %q does not exist after migrating", table)
		}
	}
}

// TestMigrateTwiceIsSafe checks that running migrations again, as happens on
// every restart, does nothing and does not fail.
func TestMigrateTwiceIsSafe(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	ctx := context.Background()

	countBefore := countAppliedMigrations(t, pool)
	if err := database.Migrate(ctx, pool); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	if countAfter := countAppliedMigrations(t, pool); countAfter != countBefore {
		t.Errorf("applied migrations went from %d to %d, want no change", countBefore, countAfter)
	}
}

// TestMigrateConcurrentlyIsSafe simulates several servers starting at once;
// the advisory lock should let them all succeed without clashing.
func TestMigrateConcurrentlyIsSafe(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	ctx := context.Background()

	const servers = 4
	errs := make(chan error, servers)
	var wg sync.WaitGroup
	for range servers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- database.Migrate(ctx, pool)
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("concurrent Migrate: %v", err)
		}
	}
}

// TestMigrateWorksWithOneConnection checks Migrate never needs more than one
// connection at a time. An earlier version needed two, so on a small pool it
// waited forever for a connection it was itself holding.
func TestMigrateWorksWithOneConnection(t *testing.T) {
	t.Parallel()
	config := testdb.New(t).Config().Copy()
	config.MaxConns = 1
	onePool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("open one-connection pool: %v", err)
	}
	defer onePool.Close()

	// A deadlock would hang forever; the timeout turns it into a failure.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	errs := make(chan error, 3)
	for range 3 {
		go func() { errs <- database.Migrate(ctx, onePool) }()
	}
	for range 3 {
		if err := <-errs; err != nil {
			t.Errorf("Migrate on a one-connection pool: %v", err)
		}
	}
}

// tableExists reports whether a table with this name exists in the database.
func tableExists(t *testing.T, pool *pgxpool.Pool, table string) bool {
	t.Helper()
	var exists bool
	err := pool.QueryRow(context.Background(),
		"SELECT to_regclass($1) IS NOT NULL", table,
	).Scan(&exists)
	if err != nil {
		t.Fatalf("check table %q: %v", table, err)
	}
	return exists
}

// countAppliedMigrations returns how many of our migration files are recorded
// as applied.
func countAppliedMigrations(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM schema_migrations").Scan(&count); err != nil {
		t.Fatalf("count applied migrations: %v", err)
	}
	return count
}
