// Tests for the HTTP handlers in api.go.
package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/ananya-mh/batch-inference/internal/metrics"
	"github.com/ananya-mh/batch-inference/internal/model"
	"github.com/ananya-mh/batch-inference/internal/testdb"
	"github.com/ananya-mh/batch-inference/internal/worker"
)

// testConfig returns API settings for tests: the default upload limit, and a
// 24h window plus a 2s one for tests that need a batch to expire quickly.
func testConfig() Config {
	return Config{
		MaxUploadBytes:    DefaultMaxUploadBytes,
		MaxBatchRequests:  DefaultMaxBatchRequests,
		CompletionWindows: map[string]time.Duration{"24h": 24 * time.Hour, "2s": 2 * time.Second},
	}
}

// newRiverClient returns a River client that is never started; the API only
// uses it to cancel jobs.
func newRiverClient(t *testing.T, pool *pgxpool.Pool) *river.Client[pgx.Tx] {
	t.Helper()
	client, err := worker.NewRiverClient(pool, model.NewClient("http://unused.invalid/v1", ""), worker.Config{
		Workers: 1, MaxAttempts: 3, JobTimeout: time.Second, RescueStuckJobsAfter: 2 * time.Second, RetryBaseDelay: time.Millisecond,
	}, metrics.New(nil), func() {})
	if err != nil {
		t.Fatalf("NewRiverClient: %v", err)
	}
	return client
}

// TestHealthReportsOKWhenDatabaseIsUp checks the happy path of /healthz.
func TestHealthReportsOKWhenDatabaseIsUp(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	server := NewServer(pool, newRiverClient(t, pool), metrics.New(pool), testConfig())

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", response.Code, http.StatusOK, response.Body)
	}
}

// TestHealthReportsUnavailableWhenDatabaseIsDown checks that /healthz fails
// once the database connection is gone, so Docker stops sending traffic.
func TestHealthReportsUnavailableWhenDatabaseIsDown(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	server := NewServer(pool, newRiverClient(t, pool), metrics.New(pool), testConfig())
	// A closed pool behaves like a database that cannot be reached.
	pool.Close()

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
}
