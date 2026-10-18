// Tests for scheduling passes: fan-out, the window, oldest-first picking,
// giving up on dead jobs, finishing batches, and two schedulers at once.
// Workers are not started here; tests play the worker's part directly.
package scheduler_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/ananya-mh/batch-inference/internal/batches"
	"github.com/ananya-mh/batch-inference/internal/model"
	"github.com/ananya-mh/batch-inference/internal/scheduler"
	"github.com/ananya-mh/batch-inference/internal/testdb"
	"github.com/ananya-mh/batch-inference/internal/testfixtures"
	"github.com/ananya-mh/batch-inference/internal/worker"
)

// newRiverClient returns a River client that is never started: the scheduler
// only uses it to insert jobs.
func newRiverClient(t *testing.T, pool *pgxpool.Pool) *river.Client[pgx.Tx] {
	t.Helper()
	client, err := worker.NewRiverClient(pool, model.NewClient("http://unused.invalid/v1", ""), worker.Config{
		Workers:              1,
		MaxAttempts:          3,
		JobTimeout:           time.Second,
		RescueStuckJobsAfter: 2 * time.Second,
		RetryBaseDelay:       time.Millisecond,
	}, func() {})
	if err != nil {
		t.Fatalf("NewRiverClient: %v", err)
	}
	return client
}

// newScheduler returns a scheduler with the given window.
func newScheduler(t *testing.T, pool *pgxpool.Pool, window int) *scheduler.Scheduler {
	t.Helper()
	return scheduler.New(pool, newRiverClient(t, pool), scheduler.Config{Window: window, PassInterval: time.Hour})
}

// runPass runs one pass and fails the test on error.
func runPass(t *testing.T, s *scheduler.Scheduler) scheduler.PassResult {
	t.Helper()
	result, err := s.RunPass(context.Background())
	if err != nil {
		t.Fatalf("RunPass: %v", err)
	}
	return result
}

// queuedCustomIDs returns the custom_ids of a batch's queued requests.
func queuedCustomIDs(t *testing.T, pool *pgxpool.Pool) map[string]bool {
	t.Helper()
	rows, err := pool.Query(context.Background(), "SELECT custom_id FROM requests WHERE status = 'queued'")
	if err != nil {
		t.Fatalf("query queued: %v", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("read queued: %v", err)
	}
	set := map[string]bool{}
	for _, id := range ids {
		set[id] = true
	}
	return set
}

// finishQueued plays the worker: it marks up to n queued requests succeeded
// and completes their River jobs, freeing their slots.
func finishQueued(t *testing.T, pool *pgxpool.Pool, n int) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		WITH done AS (
			UPDATE requests SET status = 'succeeded', finished_at = now()
			WHERE (batch_id, custom_id) IN (
				SELECT batch_id, custom_id FROM requests WHERE status = 'queued' ORDER BY seq LIMIT $1
			)
			RETURNING river_job_id
		)
		UPDATE river_job SET state = 'completed', finalized_at = now()
		WHERE id IN (SELECT river_job_id FROM done)`, n)
	if err != nil {
		t.Fatalf("finish queued requests: %v", err)
	}
}

// countRiverJobs returns how many River jobs exist.
func countRiverJobs(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM river_job").Scan(&count); err != nil {
		t.Fatalf("count river jobs: %v", err)
	}
	return count
}

// TestPassFansOutAndFillsWindow checks the first pass turns a batch into
// requests and admits only as many as the window allows.
func TestPassFansOutAndFillsWindow(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 10)
	s := newScheduler(t, pool, 4)

	result := runPass(t, s)

	if result.FannedOut != 1 || result.Admitted != 4 {
		t.Errorf("pass result = %+v, want 1 fanned out and 4 admitted", result)
	}
	if status := testfixtures.BatchStatus(t, pool, batch.ID); status != batches.StatusInProgress {
		t.Errorf("batch status = %s, want in_progress", status)
	}
	counts := testfixtures.CountRequests(t, pool, batch.ID)
	if counts["queued"] != 4 || counts["pending"] != 6 {
		t.Errorf("request counts = %v, want 4 queued and 6 pending", counts)
	}
	if jobs := countRiverJobs(t, pool); jobs != 4 {
		t.Errorf("%d River jobs, want 4 (one per queued request)", jobs)
	}

	// A second pass with no free slots admits nothing.
	if result := runPass(t, s); result.Admitted != 0 {
		t.Errorf("second pass admitted %d, want 0 while the window is full", result.Admitted)
	}
}

// TestPassAdmitsOldestFirst checks first-come-first-served order: an older
// batch's requests go before a newer batch's, and lines go in file order.
func TestPassAdmitsOldestFirst(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	testfixtures.CreateBatch(t, pool, "tenant_a", "old", 5)
	testfixtures.CreateBatch(t, pool, "tenant_b", "new", 5)
	s := newScheduler(t, pool, 3)

	runPass(t, s)

	queued := queuedCustomIDs(t, pool)
	for _, want := range []string{"old-0", "old-1", "old-2"} {
		if !queued[want] {
			t.Errorf("queued = %v, want %s among them", queued, want)
		}
	}
}

// TestPassRefillsFreedSlots checks finished requests' slots are filled again.
func TestPassRefillsFreedSlots(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 10)
	s := newScheduler(t, pool, 4)
	runPass(t, s)

	finishQueued(t, pool, 3)
	if result := runPass(t, s); result.Admitted != 3 {
		t.Errorf("admitted %d, want 3 (the freed slots)", result.Admitted)
	}
	counts := testfixtures.CountRequests(t, pool, batch.ID)
	if counts["queued"] != 4 || counts["succeeded"] != 3 || counts["pending"] != 3 {
		t.Errorf("request counts = %v, want 4 queued, 3 succeeded, 3 pending", counts)
	}
}

// TestCancelledBatchIsNotFannedOut checks a batch cancelled before its first
// pass gets no requests.
func TestCancelledBatchIsNotFannedOut(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 5)
	if err := batches.Transition(context.Background(), pool, batch.ID, batches.StatusValidating, batches.StatusCancelling); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	if result := runPass(t, newScheduler(t, pool, 4)); result.FannedOut != 0 || result.Admitted != 0 {
		t.Errorf("pass result = %+v, want nothing done for a cancelled batch", result)
	}
	if counts := testfixtures.CountRequests(t, pool, batch.ID); len(counts) != 0 {
		t.Errorf("request counts = %v, want none", counts)
	}
}

// TestRequestOfDeadJobIsGivenUp checks a request whose River job was
// discarded (the worker crashed on every attempt) is failed, with a reason,
// and its slot is reused.
func TestRequestOfDeadJobIsGivenUp(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	ctx := context.Background()
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 3)
	s := newScheduler(t, pool, 1)
	runPass(t, s)

	// What River does after the last attempt panics or is rescued too often.
	if _, err := pool.Exec(ctx, "UPDATE river_job SET state = 'discarded', finalized_at = now()"); err != nil {
		t.Fatalf("discard job: %v", err)
	}

	result := runPass(t, s)
	if result.GaveUp != 1 || result.Admitted != 1 {
		t.Errorf("pass result = %+v, want 1 given up and 1 admitted into the freed slot", result)
	}
	var errorCode string
	err := pool.QueryRow(ctx, "SELECT error_code FROM results WHERE batch_id = $1 AND custom_id = 'a-0'", batch.ID).Scan(&errorCode)
	if err != nil || errorCode != "worker_failed" {
		t.Errorf("result error_code = %q (err %v), want worker_failed", errorCode, err)
	}
}

// TestFinishedBatchIsFinalizedThenCompleted checks a batch with all requests
// done goes to finalizing, then to completed on the next pass.
func TestFinishedBatchIsFinalizedThenCompleted(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 2)
	s := newScheduler(t, pool, 4)
	runPass(t, s)
	finishQueued(t, pool, 2)

	if result := runPass(t, s); result.Finalizing != 1 {
		t.Errorf("pass result = %+v, want 1 batch finalizing", result)
	}
	if status := testfixtures.BatchStatus(t, pool, batch.ID); status != batches.StatusFinalizing {
		t.Errorf("status = %s, want finalizing", status)
	}
	if result := runPass(t, s); result.Completed != 1 {
		t.Errorf("pass result = %+v, want 1 batch completed", result)
	}
	if status := testfixtures.BatchStatus(t, pool, batch.ID); status != batches.StatusCompleted {
		t.Errorf("status = %s, want completed", status)
	}
}

// TestTwoSchedulersNeverOverfillWindow runs two schedulers (as two servers
// would) passing in tight loops while requests finish, and checks the window
// is never exceeded and no request ever gets two jobs.
func TestTwoSchedulersNeverOverfillWindow(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	ctx := context.Background()
	const window = 5
	testfixtures.CreateBatch(t, pool, "tenant_a", "a", 200)
	testfixtures.CreateBatch(t, pool, "tenant_b", "b", 200)
	schedulers := []*scheduler.Scheduler{newScheduler(t, pool, window), newScheduler(t, pool, window)}
	// Fan out first, so the race below is purely about admitting into the
	// window.
	runPass(t, schedulers[0])

	stop := make(chan struct{})
	var passes, skipped, failedPasses, maxInFlight atomic.Int64
	var wg sync.WaitGroup

	for _, s := range schedulers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				result, err := s.RunPass(ctx)
				passes.Add(1)
				if err != nil {
					// Keep going: stopping one scheduler would end the race.
					failedPasses.Add(1)
					continue
				}
				if result.Skipped {
					skipped.Add(1)
				}
			}
		}()
	}

	// The worker's part: keep finishing queued requests so slots free up.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			finishQueued(t, pool, 2)
		}
	}()

	// Watch the in-flight count the whole time.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			var inFlight int64
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM requests WHERE status IN ('queued', 'running')").Scan(&inFlight); err != nil {
				t.Errorf("count in flight: %v", err)
				return
			}
			if inFlight > maxInFlight.Load() {
				maxInFlight.Store(inFlight)
			}
		}
	}()

	time.Sleep(2 * time.Second)
	close(stop)
	wg.Wait()

	if failedPasses.Load() > 0 {
		t.Errorf("%d of %d passes failed", failedPasses.Load(), passes.Load())
	}
	if maxInFlight.Load() > window {
		t.Errorf("in flight reached %d, window is %d", maxInFlight.Load(), window)
	}
	// If one scheduler was never turned away by the lock, they never really
	// ran at the same time and the test proved nothing.
	if skipped.Load() == 0 {
		t.Errorf("no pass was skipped in %d passes; the schedulers never overlapped", passes.Load())
	}

	var duplicateJobs int
	err := pool.QueryRow(ctx, `
		SELECT count(*) FROM (
			SELECT args->>'batch_id', args->>'custom_id' FROM river_job
			GROUP BY 1, 2 HAVING count(*) > 1
		) duplicates`).Scan(&duplicateJobs)
	if err != nil {
		t.Fatalf("count duplicate jobs: %v", err)
	}
	if duplicateJobs != 0 {
		t.Errorf("%d requests got more than one River job", duplicateJobs)
	}
	var admitted int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM requests WHERE status <> 'pending'").Scan(&admitted); err != nil {
		t.Fatalf("count admitted: %v", err)
	}
	t.Logf("%d passes (%d skipped by the lock), %d requests admitted, max in flight %d", passes.Load(), skipped.Load(), admitted, maxInFlight.Load())
	if admitted < window*2 {
		t.Errorf("only %d requests were admitted; the test did not exercise refilling", admitted)
	}
}
