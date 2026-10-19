// Command server runs the batch inference service: the HTTP API, the
// scheduler, and the workers that call the model.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ananya-mh/batch-inference/internal/api"
	"github.com/ananya-mh/batch-inference/internal/database"
	"github.com/ananya-mh/batch-inference/internal/model"
	"github.com/ananya-mh/batch-inference/internal/scheduler"
	"github.com/ananya-mh/batch-inference/internal/tenants"
	"github.com/ananya-mh/batch-inference/internal/worker"
)

// shutdownTimeout is how long running requests get to finish on shutdown.
const shutdownTimeout = 30 * time.Second

// main starts the server and exits with a non-zero code if it fails.
func main() {
	if err := run(); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

// run sets everything up and serves until the process receives Ctrl+C or a
// stop signal from Docker, then shuts down gracefully.
func run() error {
	// Cancelled on SIGINT/SIGTERM, which starts a graceful shutdown.
	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	cfg, err := readConfig()
	if err != nil {
		return err
	}

	pool, err := database.Connect(signalCtx, cfg.databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := database.Migrate(signalCtx, pool); err != nil {
		return fmt.Errorf("migrate database: %w", err)
	}
	if err := createSeedTenants(signalCtx, pool, cfg.seedTenants); err != nil {
		return err
	}

	// Workers nudge the scheduler after each finished request, and the
	// scheduler inserts jobs through the River client, so each needs the
	// other. The nudge function is only called once workers are running,
	// by which time sched is set.
	var sched *scheduler.Scheduler
	riverClient, err := worker.NewRiverClient(pool, model.NewClient(cfg.modelBaseURL, cfg.modelAPIKey), cfg.worker, func() { sched.Nudge() })
	if err != nil {
		return err
	}
	sched = scheduler.New(pool, riverClient, cfg.scheduler)

	// River gets a context that is never cancelled: cancelling it would stop
	// workers abruptly. It is stopped gracefully with Stop below instead.
	if err := riverClient.Start(context.Background()); err != nil {
		return fmt.Errorf("start workers: %w", err)
	}
	schedulerCtx, stopScheduler := context.WithCancel(context.Background())
	schedulerDone := make(chan struct{})
	go func() {
		defer close(schedulerDone)
		sched.Run(schedulerCtx)
	}()
	slog.Info("workers and scheduler started",
		"workers", cfg.worker.Workers, "window", cfg.scheduler.Window,
		"fair", cfg.scheduler.FairScheduling, "model", cfg.modelBaseURL)

	httpServer := &http.Server{
		Addr:    cfg.httpAddr,
		Handler: api.NewServer(pool, riverClient, cfg.api).Handler(),
		// Without timeouts, slow or stuck clients could hold connections
		// open forever.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	serveErrors := make(chan error, 1)
	go func() {
		slog.Info("listening", "addr", cfg.httpAddr)
		serveErrors <- httpServer.ListenAndServe()
	}()

	var serveErr error
	select {
	case err := <-serveErrors:
		serveErr = fmt.Errorf("serve HTTP: %w", err)
	case <-signalCtx.Done():
	}

	// Shut down in order: stop taking API calls, stop admitting requests,
	// then let running requests finish. Anything unfinished is safe: its
	// job is still in the database and will run after a restart.
	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("shut down HTTP server", "error", err)
	}
	stopScheduler()
	<-schedulerDone
	if err := riverClient.Stop(shutdownCtx); err != nil {
		slog.Error("workers did not finish in time; cancelling them", "error", err)
		if err := riverClient.StopAndCancel(context.Background()); err != nil {
			slog.Error("cancel workers", "error", err)
		}
	}
	return serveErr
}

// createSeedTenants creates the tenants listed in SEED_TENANTS. There is no
// sign-up, so this is how development and experiment tenants are made.
func createSeedTenants(ctx context.Context, pool *pgxpool.Pool, seeds []tenants.Seed) error {
	for _, seed := range seeds {
		if err := tenants.Create(ctx, pool, seed.TenantID, seed.APIKey); err != nil {
			return fmt.Errorf("create seed tenant: %w", err)
		}
	}
	slog.Info("seed tenants ready", "count", len(seeds))
	return nil
}
