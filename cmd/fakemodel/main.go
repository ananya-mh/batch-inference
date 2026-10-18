// Command fakemodel runs a pretend AI model server for development and tests.
// See internal/fakemodel for what it does and the settings it reads.
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

	"github.com/ananya-mh/batch-inference/internal/fakemodel"
)

// main starts the fake model and exits with a non-zero code if it fails.
func main() {
	if err := run(); err != nil {
		slog.Error("fake model stopped", "error", err)
		os.Exit(1)
	}
}

// run reads the settings and serves HTTP until Ctrl+C or a stop signal.
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	config, err := fakemodel.ConfigFromEnv(os.Getenv)
	if err != nil {
		return fmt.Errorf("read settings: %w", err)
	}
	server, err := fakemodel.NewServer(config)
	if err != nil {
		return err
	}

	addr := os.Getenv("FAKE_MODEL_ADDR")
	if addr == "" {
		// vLLM's default port, so switching to a real model is just a
		// host name change.
		addr = ":8000"
	}

	httpServer := &http.Server{
		Addr:              addr,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serveErrors := make(chan error, 1)
	go func() {
		slog.Info("fake model listening", "addr", addr, "config", fmt.Sprintf("%+v", config))
		serveErrors <- httpServer.ListenAndServe()
	}()

	select {
	case err := <-serveErrors:
		return fmt.Errorf("serve HTTP: %w", err)
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shut down HTTP server: %w", err)
	}
	return nil
}
