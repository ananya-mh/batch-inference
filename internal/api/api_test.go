// Tests for the HTTP handlers in api.go.
package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ananya-mh/batch-inference/internal/testdb"
)

// TestHealthReportsOKWhenDatabaseIsUp checks the happy path of /healthz.
func TestHealthReportsOKWhenDatabaseIsUp(t *testing.T) {
	t.Parallel()
	server := NewServer(testdb.New(t), Config{MaxUploadBytes: DefaultMaxUploadBytes})

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
	server := NewServer(pool, Config{MaxUploadBytes: DefaultMaxUploadBytes})
	// A closed pool behaves like a database that cannot be reached.
	pool.Close()

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
}
