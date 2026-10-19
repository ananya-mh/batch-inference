// This file reads the server's settings from environment variables.
package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/ananya-mh/batch-inference/internal/api"
	"github.com/ananya-mh/batch-inference/internal/batches"
	"github.com/ananya-mh/batch-inference/internal/scheduler"
	"github.com/ananya-mh/batch-inference/internal/tenants"
	"github.com/ananya-mh/batch-inference/internal/worker"
)

// config holds every setting the server reads from the environment.
type config struct {
	databaseURL string
	httpAddr    string
	api         api.Config
	seedTenants []tenants.Seed

	modelBaseURL string
	modelAPIKey  string
	worker       worker.Config
	scheduler    scheduler.Config
}

// readConfig reads and checks all settings. Only DATABASE_URL is required;
// everything else has a default suited to development with the fake model.
func readConfig() (config, error) {
	var cfg config
	var err error

	cfg.databaseURL = os.Getenv("DATABASE_URL")
	if cfg.databaseURL == "" {
		return config{}, errors.New("DATABASE_URL is not set")
	}
	cfg.httpAddr = envString("HTTP_ADDR", ":8080")

	if cfg.api.MaxUploadBytes, err = envInt64("MAX_UPLOAD_BYTES", api.DefaultMaxUploadBytes); err != nil {
		return config{}, err
	}
	if cfg.api.CompletionWindows, err = batches.ParseCompletionWindows(envString("COMPLETION_WINDOWS", batches.DefaultCompletionWindows)); err != nil {
		return config{}, fmt.Errorf("COMPLETION_WINDOWS: %w", err)
	}
	if cfg.seedTenants, err = tenants.ParseSeedList(os.Getenv("SEED_TENANTS")); err != nil {
		return config{}, fmt.Errorf("SEED_TENANTS: %w", err)
	}

	// The fake model's address in Compose; vLLM serves the same API.
	cfg.modelBaseURL = envString("MODEL_BASE_URL", "http://localhost:8000/v1")
	cfg.modelAPIKey = os.Getenv("MODEL_API_KEY")

	if cfg.worker.Workers, err = envInt("WORKERS", 16); err != nil {
		return config{}, err
	}
	if cfg.worker.MaxAttempts, err = envInt("MAX_ATTEMPTS", 5); err != nil {
		return config{}, err
	}
	if cfg.worker.JobTimeout, err = envDuration("JOB_TIMEOUT", 2*time.Minute); err != nil {
		return config{}, err
	}
	if cfg.worker.RescueStuckJobsAfter, err = envDuration("RESCUE_STUCK_JOBS_AFTER", 5*time.Minute); err != nil {
		return config{}, err
	}
	cfg.worker.RetryBaseDelay = time.Second

	// Twice the workers by default, so each worker always has its next job
	// ready (see "Settings" in DESIGN.md).
	if cfg.scheduler.Window, err = envInt("SCHEDULER_WINDOW", 2*cfg.worker.Workers); err != nil {
		return config{}, err
	}
	if cfg.scheduler.PassInterval, err = envDuration("SCHEDULER_INTERVAL", 200*time.Millisecond); err != nil {
		return config{}, err
	}
	if cfg.scheduler.FairScheduling, err = envBool("FAIR_SCHEDULING", true); err != nil {
		return config{}, err
	}

	if err := cfg.check(); err != nil {
		return config{}, err
	}
	return cfg, nil
}

// check returns an error for settings that make no sense together.
func (cfg config) check() error {
	if cfg.api.MaxUploadBytes <= 0 {
		return errors.New("MAX_UPLOAD_BYTES must be positive")
	}
	if cfg.worker.Workers < 1 || cfg.worker.MaxAttempts < 1 || cfg.scheduler.Window < 1 {
		return errors.New("WORKERS, MAX_ATTEMPTS, and SCHEDULER_WINDOW must each be at least 1")
	}
	// Otherwise River would "rescue" jobs that are still running normally.
	if cfg.worker.RescueStuckJobsAfter <= cfg.worker.JobTimeout {
		return errors.New("RESCUE_STUCK_JOBS_AFTER must be longer than JOB_TIMEOUT")
	}
	return nil
}

// envString returns the variable's value, or fallback if it is not set.
func envString(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

// envInt returns the variable as a whole number, or fallback if it is not set.
func envInt(name string, fallback int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a whole number, got %q", name, value)
	}
	return parsed, nil
}

// envInt64 is envInt for large numbers, such as byte sizes.
func envInt64(name string, fallback int64) (int64, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a whole number, got %q", name, value)
	}
	return parsed, nil
}

// envBool returns the variable as true or false, or fallback if it is not set.
func envBool(name string, fallback bool) (bool, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false, got %q", name, value)
	}
	return parsed, nil
}

// envDuration returns the variable as a duration like "2m" or "200ms", or
// fallback if it is not set.
func envDuration(name string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration like 2m or 200ms, got %q", name, value)
	}
	return parsed, nil
}
