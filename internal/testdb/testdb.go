// Package testdb gives each test its own empty, fully migrated database, so
// tests never see each other's data and can run in parallel.
package testdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ananya-mh/batch-inference/internal/database"
)

// defaultAdminURL matches the Postgres started by `docker compose up`.
const defaultAdminURL = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"

// maxConnsPerTest caps each test's pool. pgx defaults to one connection per
// CPU, and with many tests in many packages running at once that ran
// Postgres out of connections ("too many clients"). Four is still enough for
// tests to make truly concurrent database calls.
const maxConnsPerTest = 4

// New creates a fresh database for this test, runs all migrations on it, and
// returns a pool connected to it. The database is dropped when the test ends.
//
// Set TEST_DATABASE_URL to point at a different Postgres server.
func New(t *testing.T) *pgxpool.Pool {
	t.Helper()

	// t.Context() is cancelled before cleanup functions run, and cleanup
	// still needs to talk to the database, so use a background context.
	ctx := context.Background()

	adminURL := os.Getenv("TEST_DATABASE_URL")
	if adminURL == "" {
		adminURL = defaultAdminURL
	}

	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("connect to test Postgres at %s (is `docker compose up postgres` running?): %v", adminURL, err)
	}

	databaseName := "test_" + randomSuffix(t)
	// The name is generated above, never user input, so building the SQL
	// string is safe. CREATE DATABASE does not accept $1 parameters.
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+databaseName); err != nil {
		t.Fatalf("create test database: %v", err)
	}

	config, err := pgxpool.ParseConfig(adminURL)
	if err != nil {
		t.Fatalf("parse test database URL: %v", err)
	}
	config.ConnConfig.Database = databaseName
	config.MaxConns = maxConnsPerTest

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("connect to test database: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		// FORCE disconnects anything a test left open, so the drop
		// cannot hang.
		if _, err := admin.Exec(ctx, "DROP DATABASE "+databaseName+" WITH (FORCE)"); err != nil {
			t.Errorf("drop test database: %v", err)
		}
		if err := admin.Close(ctx); err != nil {
			t.Errorf("close admin connection: %v", err)
		}
	})

	if err := database.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate test database: %v", err)
	}
	return pool
}

// randomSuffix returns 16 random hex characters for a unique database name.
func randomSuffix(t *testing.T) string {
	t.Helper()
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		t.Fatalf("generate random database name: %v", err)
	}
	return hex.EncodeToString(bytes)
}
