// Package worker runs batch requests: it is the River worker that passes the
// start check, calls the model, and saves the result. See "Workers" in
// DESIGN.md.
package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"

	"github.com/ananya-mh/batch-inference/internal/database"
	"github.com/ananya-mh/batch-inference/internal/model"
	"github.com/ananya-mh/batch-inference/internal/requests"
)

// maxRetryDelay caps the wait between attempts, so a request waiting for a
// retry does not hold its tenant's slot for long.
const maxRetryDelay = 8 * time.Second

// saveTimeout limits saving a result. Saving ignores the job's own deadline,
// so a reply that arrives just as the job times out is still kept.
const saveTimeout = 10 * time.Second

// Config holds the worker settings.
type Config struct {
	// Workers is how many requests are sent to the model at once.
	Workers int

	// MaxAttempts is how many times a request is tried before it counts as
	// failed.
	MaxAttempts int

	// JobTimeout is the longest one attempt (one model call) may take.
	JobTimeout time.Duration

	// RescueStuckJobsAfter is how long a job may stay running before River
	// assumes its worker died and runs it again. Must be more than
	// JobTimeout.
	RescueStuckJobsAfter time.Duration

	// RetryBaseDelay is the wait after the first failed attempt; each later
	// wait doubles, up to 8 seconds. Tests use a few milliseconds.
	RetryBaseDelay time.Duration
}

// Worker is the River worker for requests.RunArgs jobs.
type Worker struct {
	river.WorkerDefaults[requests.RunArgs]

	pool           *pgxpool.Pool
	model          *model.Client
	retryBaseDelay time.Duration

	// afterFinish is called after a request's result is saved; the server
	// uses it to nudge the scheduler to refill the freed slot.
	afterFinish func()
}

// NewRiverClient returns a River client whose workers run requests against
// modelClient. afterFinish is called each time a worker saves a result. The
// client is not started; call Start on it to begin working.
func NewRiverClient(pool *pgxpool.Pool, modelClient *model.Client, config Config, afterFinish func()) (*river.Client[pgx.Tx], error) {
	workers := river.NewWorkers()
	river.AddWorker(workers, &Worker{
		pool:           pool,
		model:          modelClient,
		retryBaseDelay: config.RetryBaseDelay,
		afterFinish:    afterFinish,
	})

	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Queues:               map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: config.Workers}},
		Workers:              workers,
		MaxAttempts:          config.MaxAttempts,
		JobTimeout:           config.JobTimeout,
		RescueStuckJobsAfter: config.RescueStuckJobsAfter,
	})
	if err != nil {
		return nil, fmt.Errorf("create river client: %w", err)
	}
	return client, nil
}

// Work runs one attempt of one request.
func (w *Worker) Work(ctx context.Context, job *river.Job[requests.RunArgs]) error {
	body, started, err := w.start(ctx, job.Args)
	if err != nil {
		return err
	}
	if !started {
		// The batch is no longer in progress, or the request already
		// finished. Not calling the model is the point; the job is done.
		return nil
	}

	reply, callErr := w.model.Complete(ctx, body)
	outcome, retry := decideOutcome(reply, callErr, job.Attempt, job.MaxAttempts)
	if retry {
		// Returning the error makes River try again after NextRetry.
		return callErr
	}
	return w.finish(ctx, job, outcome)
}

// start runs the start check in its own short transaction, so the batch row
// is locked only briefly and never during the model call.
func (w *Worker) start(ctx context.Context, args requests.RunArgs) ([]byte, bool, error) {
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("begin start check: %w", err)
	}
	defer database.RollbackUnlessCommitted(ctx, tx)

	body, started, err := requests.Start(ctx, tx, args.BatchID, args.CustomID)
	if err != nil {
		return nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("commit start check: %w", err)
	}
	return body, started, nil
}

// decideOutcome turns a model call's result into either a final outcome to
// save, or retry = true to have River try again later.
func decideOutcome(reply []byte, callErr error, attempt, maxAttempts int) (outcome requests.Outcome, retry bool) {
	if callErr == nil {
		return requests.Outcome{Succeeded: true, Response: reply}, false
	}

	// The model said the request itself is bad: retrying cannot help.
	var permanent *model.PermanentError
	if errors.As(callErr, &permanent) {
		return requests.Outcome{ErrorCode: "model_rejected", ErrorMessage: permanent.Error()}, false
	}

	// Cancelled means this server is shutting down, not that the request
	// failed: never use up the last attempt on it. (A job timeout shows up
	// as DeadlineExceeded instead, and does count.)
	if errors.Is(callErr, context.Canceled) {
		return requests.Outcome{}, true
	}

	if attempt >= maxAttempts {
		return requests.Outcome{
			ErrorCode:    "retries_exhausted",
			ErrorMessage: fmt.Sprintf("failed after %d attempts; last error: %v", attempt, callErr),
		}, false
	}
	return requests.Outcome{}, true
}

// finish saves the outcome and marks the River job complete in one
// transaction, so either both happen or neither does.
func (w *Worker) finish(workCtx context.Context, job *river.Job[requests.RunArgs], outcome requests.Outcome) error {
	// WithoutCancel keeps the job's context values but drops its deadline.
	// The values matter: JobCompleteTx finds the River client through them.
	// (An earlier version used context.Background() here, and every save
	// failed; see "Bugs found" in DESIGN.md.)
	ctx, cancel := context.WithTimeout(context.WithoutCancel(workCtx), saveTimeout)
	defer cancel()

	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin saving result: %w", err)
	}
	defer database.RollbackUnlessCommitted(ctx, tx)

	// saved is false if another attempt already finished this request, or
	// it expired. Then there is nothing to save, but the job is still done.
	if _, err := requests.Finish(ctx, tx, job.Args.BatchID, job.Args.CustomID, outcome); err != nil {
		return err
	}
	if _, err := river.JobCompleteTx[*riverpgxv5.Driver](ctx, tx, job); err != nil {
		return fmt.Errorf("complete job: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit result: %w", err)
	}

	w.afterFinish()
	return nil
}

// NextRetry says when to try a failed attempt again: RetryBaseDelay after the
// first failure, doubling each time, up to 8 seconds. River's default waits
// grow to minutes, which would hold the tenant's slot far too long.
func (w *Worker) NextRetry(job *river.Job[requests.RunArgs]) time.Time {
	delay := w.retryBaseDelay
	// job.Attempt is the attempt that just failed, starting at 1.
	for range job.Attempt - 1 {
		delay *= 2
		if delay >= maxRetryDelay {
			delay = maxRetryDelay
			break
		}
	}
	return time.Now().Add(delay)
}
