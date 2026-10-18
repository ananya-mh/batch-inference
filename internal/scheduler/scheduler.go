// Package scheduler feeds batch requests into River a few at a time. Each pass
// fans out new batches into requests, gives up on requests whose jobs died,
// admits pending requests into free slots of the window, and finishes
// batches that are done. See "Fan-out and fair scheduling" in DESIGN.md.
package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/ananya-mh/batch-inference/internal/database"
)

// schedulerLockID is the advisory lock key that lets only one scheduler, in
// any server, run a pass at a time.
const schedulerLockID = 727_002

// Config holds the scheduler's settings.
type Config struct {
	// Window is the most requests allowed in flight (queued or running) at
	// once, across all tenants.
	Window int

	// PassInterval is how often a pass runs when nothing nudges it sooner.
	PassInterval time.Duration
}

// Scheduler runs scheduling passes.
type Scheduler struct {
	pool   *pgxpool.Pool
	river  *river.Client[pgx.Tx]
	config Config

	// nudges holds at most one pending "run a pass soon" signal. Many
	// nudges while a pass is running collapse into one extra pass.
	nudges chan struct{}
}

// New returns a scheduler that admits requests by inserting jobs with
// riverClient.
func New(pool *pgxpool.Pool, riverClient *river.Client[pgx.Tx], config Config) *Scheduler {
	return &Scheduler{pool: pool, river: riverClient, config: config, nudges: make(chan struct{}, 1)}
}

// Run runs passes until ctx is cancelled: every PassInterval, and straight
// away after a Nudge.
func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.config.PassInterval)
	defer ticker.Stop()

	for {
		if _, err := s.RunPass(ctx); err != nil && ctx.Err() == nil {
			// A failed pass changed nothing (it is one transaction), so
			// log it and let the next pass try again.
			slog.ErrorContext(ctx, "scheduler pass failed", "error", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-s.nudges:
		}
	}
}

// Nudge asks for a pass soon, without waiting for it. Workers call it after
// finishing a request, so the freed slot is refilled quickly.
func (s *Scheduler) Nudge() {
	select {
	case s.nudges <- struct{}{}:
	default:
		// A nudge is already waiting; one is enough.
	}
}

// PassResult says what one pass did. Tests and logs use it.
type PassResult struct {
	Skipped    bool // another scheduler holds the lock
	FannedOut  int  // batches turned into requests
	GaveUp     int  // requests failed because their job ended without a result
	Admitted   int  // requests given a River job
	Finalizing int  // batches moved to finalizing
	Completed  int  // batches moved to completed
}

// RunPass runs one scheduling pass in a single transaction, so a pass either
// happens completely or not at all.
func (s *Scheduler) RunPass(ctx context.Context) (PassResult, error) {
	var result PassResult

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return result, fmt.Errorf("begin scheduler pass: %w", err)
	}
	defer database.RollbackUnlessCommitted(ctx, tx)

	// Only one scheduler may admit requests at a time; otherwise two could
	// both see the same free slots and overfill the window. The lock is
	// held until this transaction ends. If another server holds it, skip.
	var locked bool
	if err := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock($1)", schedulerLockID).Scan(&locked); err != nil {
		return result, fmt.Errorf("take scheduler lock: %w", err)
	}
	if !locked {
		result.Skipped = true
		return result, nil
	}

	if result.FannedOut, err = fanOut(ctx, tx); err != nil {
		return result, err
	}
	// Giving up on dead jobs before admitting frees their slots for this pass.
	if result.GaveUp, err = giveUpOnAbandonedRequests(ctx, tx); err != nil {
		return result, err
	}
	if result.Admitted, err = s.admit(ctx, tx); err != nil {
		return result, err
	}
	// Completing before starting to finalize means a batch spends at least
	// one pass in finalizing, so the status is visible to clients.
	if result.Completed, err = completeFinalizingBatches(ctx, tx); err != nil {
		return result, err
	}
	if result.Finalizing, err = startFinalizingDoneBatches(ctx, tx); err != nil {
		return result, err
	}

	if err := tx.Commit(ctx); err != nil {
		return PassResult{}, fmt.Errorf("commit scheduler pass: %w", err)
	}
	return result, nil
}
