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
	"github.com/ananya-mh/batch-inference/internal/metrics"
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

	// FetchCooldown is the shortest time between two fetches of new jobs by
	// River. Each fetch takes at most as many jobs as there are free
	// workers, so this caps throughput at about Workers / FetchCooldown.
	// Zero means River's default (100 ms); the server's default is 10 ms.
	FetchCooldown time.Duration

	// FetchPollInterval is how often River looks for jobs when it hears of
	// none (new jobs normally arrive through LISTEN/NOTIFY). Zero means
	// River's default (1 s).
	FetchPollInterval time.Duration
}

// Worker is the River worker for requests.RunArgs jobs.
type Worker struct {
	river.WorkerDefaults[requests.RunArgs]

	pool           *pgxpool.Pool
	model          *model.Client
	metrics        *metrics.Metrics
	retryBaseDelay time.Duration

	// afterFinish is called after a request's result is saved; the server
	// uses it to nudge the scheduler to refill the freed slot.
	afterFinish func()
}

// NewRiverClient returns a River client whose workers run requests against
// modelClient and report to m. afterFinish is called each time a worker saves
// a result. The client is not started; call Start on it to begin working.
func NewRiverClient(pool *pgxpool.Pool, modelClient *model.Client, config Config, m *metrics.Metrics, afterFinish func()) (*river.Client[pgx.Tx], error) {
	workers := river.NewWorkers()
	river.AddWorker(workers, &Worker{
		pool:           pool,
		model:          modelClient,
		metrics:        m,
		retryBaseDelay: config.RetryBaseDelay,
		afterFinish:    afterFinish,
	})
	m.WorkersTotal.Set(float64(config.Workers))

	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Queues:               map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: config.Workers}},
		Workers:              workers,
		MaxAttempts:          config.MaxAttempts,
		JobTimeout:           config.JobTimeout,
		RescueStuckJobsAfter: config.RescueStuckJobsAfter,
		FetchCooldown:        config.FetchCooldown,
		FetchPollInterval:    config.FetchPollInterval,
	})
	if err != nil {
		return nil, fmt.Errorf("create river client: %w", err)
	}
	return client, nil
}

// Work runs one attempt of one request.
func (w *Worker) Work(ctx context.Context, job *river.Job[requests.RunArgs]) error {
	w.metrics.WorkersBusy.Inc()
	defer w.metrics.WorkersBusy.Dec()

	started, err := w.start(ctx, job.Args)
	if err != nil {
		return err
	}
	if started.Cancelled {
		w.metrics.RequestOutcomes.WithLabelValues(started.TenantID, metrics.OutcomeCancelled).Inc()
	}
	if !started.Started {
		// The batch is no longer in progress, or the request already
		// finished. Not calling the model is the point; the job is done.
		return nil
	}
	// The wait is how long the request waited before its first try; later
	// attempts would count the retry delays as waiting.
	if job.Attempt == 1 {
		w.metrics.RequestWaitSeconds.WithLabelValues(started.TenantID).Observe(started.Waited.Seconds())
	}

	callStart := time.Now()
	reply, callErr := w.model.Complete(ctx, started.Body)
	w.observeModelCall(time.Since(callStart), callErr)
	// If the job's own context was cancelled (not timed out), this server is
	// shutting down, or River was asked to cancel the job. Either way the
	// request did not fail. Snoozing for zero seconds hands the job back to
	// run again as soon as a worker is free, without counting this attempt;
	// returning the error instead would use up an attempt, and on the last
	// one a routine restart would fail the request (see "Bugs found"). The
	// context is checked rather than the error, because the error carries
	// River's cancel reason ("stop initiated"), not context.Canceled.
	if callErr != nil && errors.Is(ctx.Err(), context.Canceled) {
		return river.JobSnooze(0)
	}
	outcome, retry := decideOutcome(reply, callErr, job.Attempt, job.MaxAttempts)
	if !retry {
		return w.finish(ctx, job, started.TenantID, outcome)
	}

	// Before asking River for a retry, mark the request as waiting for its
	// next attempt, or cancel it if the batch was cancelled meanwhile:
	// waiting out a retry delay would only hold up the cancel (see "Bugs
	// found" in DESIGN.md).
	cancelled, err := w.prepareRetry(ctx, job)
	if err != nil {
		return err
	}
	if cancelled {
		w.metrics.RequestOutcomes.WithLabelValues(started.TenantID, metrics.OutcomeCancelled).Inc()
		return nil
	}
	// Returning the error makes River try again after NextRetry.
	w.metrics.RequestOutcomes.WithLabelValues(started.TenantID, metrics.OutcomeRetried).Inc()
	return callErr
}

// observeModelCall records how long a model call took and whether it worked.
func (w *Worker) observeModelCall(took time.Duration, callErr error) {
	outcome := metrics.CallSucceeded
	if callErr != nil {
		outcome = metrics.CallFailed
	}
	w.metrics.ModelCallSeconds.WithLabelValues(outcome).Observe(took.Seconds())
}

// prepareRetry runs requests.PrepareRetry in its own transaction. If it
// cancelled the request, it also completes the job in that transaction, so
// River does not retry it. It reports whether the request was cancelled.
func (w *Worker) prepareRetry(workCtx context.Context, job *river.Job[requests.RunArgs]) (bool, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(workCtx), saveTimeout)
	defer cancel()

	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin cancel check: %w", err)
	}
	defer database.RollbackUnlessCommitted(ctx, tx)

	cancelled, err := requests.PrepareRetry(ctx, tx, job.Args.BatchID, job.Args.CustomID)
	if err != nil {
		return false, err
	}
	if cancelled {
		if _, err := river.JobCompleteTx[*riverpgxv5.Driver](ctx, tx, job); err != nil {
			return false, fmt.Errorf("complete job: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit retry preparation: %w", err)
	}
	if cancelled {
		w.afterFinish()
	}
	return cancelled, nil
}

// start runs the start check in its own short transaction, so the batch row
// is locked only briefly and never during the model call.
func (w *Worker) start(ctx context.Context, args requests.RunArgs) (requests.StartResult, error) {
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return requests.StartResult{}, fmt.Errorf("begin start check: %w", err)
	}
	defer database.RollbackUnlessCommitted(ctx, tx)

	result, err := requests.Start(ctx, tx, args.BatchID, args.CustomID)
	if err != nil {
		return requests.StartResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return requests.StartResult{}, fmt.Errorf("commit start check: %w", err)
	}
	return result, nil
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

	if attempt >= maxAttempts {
		return requests.Outcome{
			ErrorCode:    "retries_exhausted",
			ErrorMessage: fmt.Sprintf("failed on attempt %d of %d; last error: %v", attempt, maxAttempts, callErr),
		}, false
	}
	return requests.Outcome{}, true
}

// finish saves the outcome and marks the River job complete in one
// transaction, so either both happen or neither does.
func (w *Worker) finish(workCtx context.Context, job *river.Job[requests.RunArgs], tenantID string, outcome requests.Outcome) error {
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
	saved, err := requests.Finish(ctx, tx, job.Args.BatchID, job.Args.CustomID, outcome)
	if err != nil {
		return err
	}
	if _, err := river.JobCompleteTx[*riverpgxv5.Driver](ctx, tx, job); err != nil {
		return fmt.Errorf("complete job: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit result: %w", err)
	}

	// Counted only after the commit, and only if this attempt's result was
	// the one saved.
	if saved {
		result := metrics.OutcomeCompleted
		if !outcome.Succeeded {
			result = metrics.OutcomeFailed
		}
		w.metrics.RequestOutcomes.WithLabelValues(tenantID, result).Inc()
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
