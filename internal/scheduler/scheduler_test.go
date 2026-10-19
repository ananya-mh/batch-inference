// Tests for scheduling passes: fan-out, the window, oldest-first picking,
// giving up on dead jobs, finishing batches, and two schedulers at once.
// Workers are not started here; tests play the worker's part directly.
package scheduler_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/ananya-mh/batch-inference/internal/batches"
	"github.com/ananya-mh/batch-inference/internal/model"
	"github.com/ananya-mh/batch-inference/internal/results"
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

// newScheduler returns a first-come-first-served scheduler with the given
// window.
func newScheduler(t *testing.T, pool *pgxpool.Pool, window int) *scheduler.Scheduler {
	t.Helper()
	return scheduler.New(pool, newRiverClient(t, pool), scheduler.Config{Window: window, PassInterval: time.Hour})
}

// newFairScheduler returns a fair scheduler with the given window.
func newFairScheduler(t *testing.T, pool *pgxpool.Pool, window int) *scheduler.Scheduler {
	t.Helper()
	return scheduler.New(pool, newRiverClient(t, pool), scheduler.Config{Window: window, PassInterval: time.Hour, FairScheduling: true})
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

// TestFinishedBatchGetsItsFiles checks completing a batch writes its files
// and sets output_file_id, with error_file_id left null when nothing failed.
func TestFinishedBatchGetsItsFiles(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 3)
	s := newScheduler(t, pool, 4)
	runPass(t, s)
	finishQueued(t, pool, 3)
	runPass(t, s) // to finalizing
	runPass(t, s) // to completed

	got, err := batches.Get(context.Background(), pool, "tenant_a", batch.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != batches.StatusCompleted || got.OutputFileID == nil || got.ErrorFileID != nil {
		t.Errorf("batch = status %s, output %v, error %v; want completed with only an output file",
			got.Status, got.OutputFileID, got.ErrorFileID)
	}
}

// TestFinalizingSurvivesCrash kills the database connection in the middle of
// completing a batch, after its files were written but before the commit, as
// a server crash would. Nothing may be left behind, and the next pass must
// produce exactly the same files, with no duplicate or missing lines.
func TestFinalizingSurvivesCrash(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	ctx := context.Background()
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 50)
	s := newScheduler(t, pool, 50)
	runPass(t, s)
	finishQueued(t, pool, 40)
	// The other 10 fail, so both files are involved.
	if _, err := pool.Exec(ctx, `
		WITH failed AS (
			UPDATE requests SET status = 'failed' WHERE status = 'queued' RETURNING batch_id, custom_id
		)
		INSERT INTO results (batch_id, custom_id, succeeded, error_code, error_message)
		SELECT batch_id, custom_id, false, 'retries_exhausted', 'it went wrong' FROM failed`); err != nil {
		t.Fatalf("fail requests: %v", err)
	}
	runPass(t, s) // to finalizing
	if status := testfixtures.BatchStatus(t, pool, batch.ID); status != batches.StatusFinalizing {
		t.Fatalf("status = %s, want finalizing", status)
	}

	// Write the files in a transaction, and note what they contain.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	var backendPID int
	if err := tx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&backendPID); err != nil {
		t.Fatalf("get backend pid: %v", err)
	}
	if _, _, err := results.WriteFiles(ctx, tx, batch.ID); err != nil {
		t.Fatalf("WriteFiles: %v", err)
	}
	beforeCrash := fileLines(t, tx, batch.ID)

	// Crash: the connection dies before the commit.
	if _, err := pool.Exec(ctx, "SELECT pg_terminate_backend($1)", backendPID); err != nil {
		t.Fatalf("terminate backend: %v", err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("commit succeeded on a terminated connection")
	}

	var leftOver int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM files WHERE purpose IN ('batch_output', 'batch_error')").Scan(&leftOver); err != nil {
		t.Fatalf("count files: %v", err)
	}
	if leftOver != 0 {
		t.Errorf("%d result files survived the crash, want 0", leftOver)
	}
	if status := testfixtures.BatchStatus(t, pool, batch.ID); status != batches.StatusFinalizing {
		t.Errorf("status after crash = %s, want still finalizing", status)
	}

	// The restart: the next pass completes the batch.
	if result := runPass(t, s); result.Completed != 1 {
		t.Fatalf("pass after crash = %+v, want 1 completed", result)
	}
	afterRestart := fileLines(t, pool, batch.ID)
	if strings.Join(afterRestart, "\n") != strings.Join(beforeCrash, "\n") {
		t.Errorf("files after restart differ from the files written before the crash")
	}
	if len(afterRestart) != 50 {
		t.Errorf("%d lines across both files, want 50 (one per request)", len(afterRestart))
	}
	unique := map[string]bool{}
	for _, line := range afterRestart {
		unique[line] = true
	}
	if len(unique) != 50 {
		t.Errorf("%d distinct lines, want 50 (no duplicates)", len(unique))
	}
}

// querier is anything that can run a query: a pool or a transaction.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// fileLines returns every line of the batch's results and errors files,
// results file first, each in line order.
func fileLines(t *testing.T, db querier, batchID string) []string {
	t.Helper()
	rows, err := db.Query(context.Background(), `
		SELECT l.line
		FROM file_lines l
		JOIN files f ON f.id = l.file_id
		WHERE f.filename LIKE $1 || '%'
		ORDER BY f.purpose DESC, l.line_number`, batchID)
	if err != nil {
		t.Fatalf("query file lines: %v", err)
	}
	lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("read file lines: %v", err)
	}
	return lines
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
