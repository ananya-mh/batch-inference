// Command server runs the batch inference HTTP API.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ananya-mh/batch-inference/internal/api"
	"github.com/ananya-mh/batch-inference/internal/database"
	"github.com/ananya-mh/batch-inference/internal/tenants"
)

// main starts the server and exits with a non-zero code if it fails.
func main() {
	if err := run(); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

// run connects to the database, migrates it, and serves HTTP until the
// process receives Ctrl+C or a stop signal from Docker.
func run() error {
	// Cancelled on SIGINT/SIGTERM, which starts a graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return errors.New("DATABASE_URL is not set")
	}
	httpAddr := os.Getenv("HTTP_ADDR")
	if httpAddr == "" {
		httpAddr = ":8080"
	}
	maxUploadBytes, err := readMaxUploadBytes()
	if err != nil {
		return err
	}

	pool, err := database.Connect(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := database.Migrate(ctx, pool); err != nil {
		return fmt.Errorf("migrate database: %w", err)
	}
	if err := createSeedTenants(ctx, pool); err != nil {
		return err
	}

	httpServer := &http.Server{
		Addr:    httpAddr,
		Handler: api.NewServer(pool, api.Config{MaxUploadBytes: maxUploadBytes}).Handler(),
		// Without timeouts, slow or stuck clients could hold connections
		// open forever.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serveErrors := make(chan error, 1)
	go func() {
		slog.Info("listening", "addr", httpAddr)
		serveErrors <- httpServer.ListenAndServe()
	}()

	select {
	case err := <-serveErrors:
		return fmt.Errorf("serve HTTP: %w", err)
	case <-ctx.Done():
	}

	// Give in-flight requests a few seconds to finish before exiting.
	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shut down HTTP server: %w", err)
	}
	return nil
}

// readMaxUploadBytes reads MAX_UPLOAD_BYTES, or returns the 100 MB default.
func readMaxUploadBytes() (int64, error) {
	value := os.Getenv("MAX_UPLOAD_BYTES")
	if value == "" {
		return api.DefaultMaxUploadBytes, nil
	}
	maxUploadBytes, err := strconv.ParseInt(value, 10, 64)
	if err != nil || maxUploadBytes <= 0 {
		return 0, fmt.Errorf("MAX_UPLOAD_BYTES must be a positive number of bytes, got %q", value)
	}
	return maxUploadBytes, nil
}

// createSeedTenants creates the tenants listed in SEED_TENANTS
// ("tenant_a=sk-a,tenant_b=sk-b"). There is no sign-up, so this is how
// development and experiment tenants are made.
func createSeedTenants(ctx context.Context, pool *pgxpool.Pool) error {
	seeds, err := tenants.ParseSeedList(os.Getenv("SEED_TENANTS"))
	if err != nil {
		return fmt.Errorf("SEED_TENANTS: %w", err)
	}
	for _, seed := range seeds {
		if err := tenants.Create(ctx, pool, seed.TenantID, seed.APIKey); err != nil {
			return fmt.Errorf("create seed tenant: %w", err)
		}
	}
	slog.Info("seed tenants ready", "count", len(seeds))
	return nil
}
