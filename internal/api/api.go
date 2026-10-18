// Package api holds the HTTP handlers for the service's public endpoints.
package api

import (
	"log/slog"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultMaxUploadBytes is the default upload size limit: 100 MB.
const DefaultMaxUploadBytes = 100 << 20

// Config holds the API's settings.
type Config struct {
	// MaxUploadBytes is the largest upload accepted, in bytes.
	MaxUploadBytes int64
}

// Server holds what the HTTP handlers need to do their work.
type Server struct {
	pool   *pgxpool.Pool
	config Config
}

// NewServer returns a Server that uses pool for database access.
func NewServer(pool *pgxpool.Pool, config Config) *Server {
	return &Server{pool: pool, config: config}
}

// Handler returns an http.Handler with every route registered. Every /v1
// route is wrapped in requireTenant, so it knows which tenant is calling.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)

	mux.HandleFunc("POST /v1/files", s.requireTenant(s.handleUploadFile))
	mux.HandleFunc("GET /v1/files/{id}", s.requireTenant(s.handleGetFile))
	mux.HandleFunc("GET /v1/files/{id}/content", s.requireTenant(s.handleGetFileContent))
	return mux
}

// handleHealth reports whether the service can reach its database. Docker
// Compose and load balancers use it to know when the server is ready.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := s.pool.Ping(r.Context()); err != nil {
		slog.ErrorContext(r.Context(), "health check: database unreachable", "error", err)
		http.Error(w, "database unreachable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if _, err := w.Write([]byte("ok\n")); err != nil {
		// The client has gone away; there is no one left to tell.
		slog.WarnContext(r.Context(), "health check: write response", "error", err)
	}
}
