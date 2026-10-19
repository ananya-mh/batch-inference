// End-to-end tests: real River workers, the real scheduler, and a fake model
// server. They cover a batch running to completion, retries of cut-off
// replies, permanent failures, crash recovery (guarantee 1), and the same
// request running twice (guarantee 2).
package worker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/ananya-mh/batch-inference/internal/batches"
	"github.com/ananya-mh/batch-inference/internal/cancel"
	"github.com/ananya-mh/batch-inference/internal/fakemodel"
	"github.com/ananya-mh/batch-inference/internal/files"
	"github.com/ananya-mh/batch-inference/internal/model"
	"github.com/ananya-mh/batch-inference/internal/requests"
	"github.com/ananya-mh/batch-inference/internal/scheduler"
	"github.com/ananya-mh/batch-inference/internal/testdb"
	"github.com/ananya-mh/batch-inference/internal/testfixtures"
	"github.com/ananya-mh/batch-inference/internal/worker"
)

// testWorkerConfig returns quick settings for tests.
func testWorkerConfig() worker.Config {
	return worker.Config{
		Workers:              4,
		MaxAttempts:          3,
		JobTimeout:           5 * time.Second,
		RescueStuckJobsAfter: 10 * time.Second,
		RetryBaseDelay:       10 * time.Millisecond,
	}
}

// system is a running scheduler plus River workers for one test.
type system struct {
	pool      *pgxpool.Pool
	river     *river.Client[pgx.Tx]
	scheduler *scheduler.Scheduler
}

// newSystem builds a fair scheduler and River client against the model at
// modelURL. Neither is started; call start.
func newSystem(t *testing.T, pool *pgxpool.Pool, modelURL string, config worker.Config) *system {
	t.Helper()
	return newSystemWithFairness(t, pool, modelURL, config, true)
}

// newSystemWithFairness is newSystem with fair scheduling on or off.
func newSystemWithFairness(t *testing.T, pool *pgxpool.Pool, modelURL string, config worker.Config, fair bool) *system {
	t.Helper()
	sys := &system{pool: pool}
	// Registered before start's cleanup, so it runs after the workers have
	// stopped: whatever the test did, the invariants must hold at the end.
	t.Cleanup(func() { testfixtures.CheckIntegrity(t, pool) })
	riverClient, err := worker.NewRiverClient(pool, model.NewClient(modelURL, ""), config, func() { sys.scheduler.Nudge() })
	if err != nil {
		t.Fatalf("NewRiverClient: %v", err)
	}
	sys.river = riverClient
	sys.scheduler = scheduler.New(pool, riverClient, scheduler.Config{Window: 8, PassInterval: 20 * time.Millisecond, FairScheduling: fair})
	return sys
}

// start starts the workers and the scheduler loop, and stops both when the
// test ends.
func (sys *system) start(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	if err := sys.river.Start(ctx); err != nil {
		t.Fatalf("start river: %v", err)
	}
	schedulerDone := make(chan struct{})
	go func() {
		defer close(schedulerDone)
		sys.scheduler.Run(ctx)
	}()

	t.Cleanup(func() {
		cancel()
		<-schedulerDone
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		if err := sys.river.Stop(stopCtx); err != nil {
			t.Errorf("stop river: %v", err)
		}
	})
}

// startFakeModel runs the fake model with config and returns its /v1 URL.
func startFakeModel(t *testing.T, config fakemodel.Config) string {
	t.Helper()
	server, err := fakemodel.NewServer(config)
	if err != nil {
		t.Fatalf("fakemodel.NewServer: %v", err)
	}
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	return httpServer.URL + "/v1"
}

// waitForStatus polls until the batch reaches want, failing after timeout.
func waitForStatus(t *testing.T, pool *pgxpool.Pool, batchID string, want batches.Status, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if testfixtures.BatchStatus(t, pool, batchID) == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("batch %s is %s after %s, want %s; requests: %v",
		batchID, testfixtures.BatchStatus(t, pool, batchID), timeout, want, testfixtures.CountRequests(t, pool, batchID))
}

// result is one row of the results table.
type result struct {
	CustomID     string
	Succeeded    bool
	Response     []byte
	ErrorCode    *string
	ErrorMessage *string
}

// describeError returns a result's error code and message for test output.
func describeError(r result) string {
	if r.ErrorCode == nil || r.ErrorMessage == nil {
		return "(none)"
	}
	return *r.ErrorCode + ": " + *r.ErrorMessage
}

// readResults returns the batch's results.
func readResults(t *testing.T, pool *pgxpool.Pool, batchID string) []result {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		"SELECT custom_id, succeeded, response, error_code, error_message FROM results WHERE batch_id = $1 ORDER BY custom_id", batchID)
	if err != nil {
		t.Fatalf("query results: %v", err)
	}
	results, err := pgx.CollectRows(rows, pgx.RowToStructByPos[result])
	if err != nil {
		t.Fatalf("read results: %v", err)
	}
	return results
}

// jobAttempts returns the attempt count of each of the batch's River jobs.
func jobAttempts(t *testing.T, pool *pgxpool.Pool, batchID string) []int {
	t.Helper()
	rows, err := pool.Query(context.Background(), "SELECT attempt FROM river_job WHERE args->>'batch_id' = $1", batchID)
	if err != nil {
		t.Fatalf("query attempts: %v", err)
	}
	attempts, err := pgx.CollectRows(rows, pgx.RowTo[int])
	if err != nil {
		t.Fatalf("read attempts: %v", err)
	}
	return attempts
}

// TestBatchRunsEndToEnd checks a batch goes from validating to completed with
// one successful result per line, and the right progress counts.
func TestBatchRunsEndToEnd(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	modelURL := startFakeModel(t, fakemodel.Config{MaxDelay: 20 * time.Millisecond, OutputWords: 5})
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 30)
	newSystem(t, pool, modelURL, testWorkerConfig()).start(t)

	waitForStatus(t, pool, batch.ID, batches.StatusCompleted, 30*time.Second)

	results := readResults(t, pool, batch.ID)
	if len(results) != 30 {
		t.Fatalf("%d results, want 30", len(results))
	}
	for _, r := range results {
		if !r.Succeeded || !json.Valid(r.Response) {
			t.Errorf("result %s: succeeded %v, response %s, error %s", r.CustomID, r.Succeeded, r.Response, describeError(r))
		}
	}
	got, err := batches.Get(context.Background(), pool, "tenant_a", batch.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.RequestCounts != (batches.RequestCounts{Total: 30, Completed: 30}) {
		t.Errorf("request counts = %+v, want 30 total, 30 completed", got.RequestCounts)
	}
	if got.InProgressAt == nil || got.FinalizingAt == nil || got.CompletedAt == nil {
		t.Errorf("missing status times: %+v", got)
	}
	// Nothing failed, so there is a results file and no errors file.
	if got.OutputFileID == nil || got.ErrorFileID != nil {
		t.Fatalf("output file %v, error file %v; want only an output file", got.OutputFileID, got.ErrorFileID)
	}
	if lines := downloadLines(t, pool, *got.OutputFileID); len(lines) != 30 {
		t.Errorf("results file has %d lines, want 30", len(lines))
	}
}

// fileLine is the part of a results or errors file line these tests check.
type fileLine struct {
	CustomID string          `json:"custom_id"`
	Response json.RawMessage `json:"response"`
	Error    json.RawMessage `json:"error"`
}

// downloadLines downloads tenant_a's file and decodes its lines.
func downloadLines(t *testing.T, pool *pgxpool.Pool, fileID string) []fileLine {
	t.Helper()
	var content bytes.Buffer
	if err := files.WriteContent(context.Background(), pool, "tenant_a", fileID, &content); err != nil {
		t.Fatalf("download %s: %v", fileID, err)
	}
	var lines []fileLine
	for _, text := range strings.Split(strings.TrimSuffix(content.String(), "\n"), "\n") {
		var line fileLine
		if err := json.Unmarshal([]byte(text), &line); err != nil {
			t.Fatalf("bad line %q: %v", text, err)
		}
		lines = append(lines, line)
	}
	return lines
}

// TestEveryCustomIDInExactlyOneFile runs a batch where the model rejects some
// requests, and checks every custom_id of the input appears exactly once
// across the results and errors files: successes in one, failures in the
// other.
func TestEveryCustomIDInExactlyOneFile(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	healthy, err := fakemodel.NewServer(fakemodel.Config{OutputWords: 3})
	if err != nil {
		t.Fatalf("fakemodel.NewServer: %v", err)
	}
	// Reject every request whose ticket number ends in 3 or 7.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request: %v", err)
			return
		}
		if bytes.Contains(body, []byte("3\"")) || bytes.Contains(body, []byte("7\"")) {
			http.Error(w, `{"error": {"message": "rejected"}}`, http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		healthy.Handler().ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)

	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 40)
	newSystem(t, pool, server.URL+"/v1", testWorkerConfig()).start(t)
	waitForStatus(t, pool, batch.ID, batches.StatusCompleted, 30*time.Second)

	got, err := batches.Get(context.Background(), pool, "tenant_a", batch.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.OutputFileID == nil || got.ErrorFileID == nil {
		t.Fatalf("output file %v, error file %v; want both", got.OutputFileID, got.ErrorFileID)
	}

	seen := map[string]int{}
	for _, line := range downloadLines(t, pool, *got.OutputFileID) {
		seen[line.CustomID]++
		if string(line.Error) != "null" || string(line.Response) == "null" {
			t.Errorf("results line for %s should have a response and no error", line.CustomID)
		}
	}
	failures := 0
	for _, line := range downloadLines(t, pool, *got.ErrorFileID) {
		seen[line.CustomID]++
		failures++
		if string(line.Response) != "null" || !strings.Contains(string(line.Error), "model_rejected") {
			t.Errorf("errors line for %s should have no response and a model_rejected error", line.CustomID)
		}
	}
	if failures != 8 {
		t.Errorf("%d lines in the errors file, want 8 (ticket numbers ending in 3 or 7)", failures)
	}
	for i := range 40 {
		customID := fmt.Sprintf("a-%d", i)
		if seen[customID] != 1 {
			t.Errorf("%s appears %d times across both files, want exactly once", customID, seen[customID])
		}
	}
	if len(seen) != 40 {
		t.Errorf("%d distinct custom_ids across both files, want 40", len(seen))
	}
}

// crashFirstAttemptModel is a model server that, for each distinct request
// body, cuts off the first reply halfway (200 status, half the body, then a
// dropped connection), and answers later attempts normally.
func crashFirstAttemptModel(t *testing.T) (url string, calls *atomic.Int64) {
	t.Helper()
	crashing, err := fakemodel.NewServer(fakemodel.Config{OutputWords: 5, CrashRate: 1})
	if err != nil {
		t.Fatalf("fakemodel.NewServer: %v", err)
	}
	healthy, err := fakemodel.NewServer(fakemodel.Config{OutputWords: 5})
	if err != nil {
		t.Fatalf("fakemodel.NewServer: %v", err)
	}

	var mu sync.Mutex
	seen := map[string]bool{}
	calls = &atomic.Int64{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request: %v", err)
			return
		}
		mu.Lock()
		first := !seen[string(body)]
		seen[string(body)] = true
		mu.Unlock()

		// The body was read above, so hand the fake model a fresh copy.
		r.Body = io.NopCloser(bytes.NewReader(body))
		if first {
			crashing.Handler().ServeHTTP(w, r)
			return
		}
		healthy.Handler().ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	return server.URL + "/v1", calls
}

// TestCutOffReplyIsRetriedNeverSaved checks a 200 reply with a cut-off body
// counts as a failed attempt: the request is retried, and only the later,
// complete reply is saved.
func TestCutOffReplyIsRetriedNeverSaved(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	modelURL, calls := crashFirstAttemptModel(t)
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 5)
	newSystem(t, pool, modelURL, testWorkerConfig()).start(t)

	waitForStatus(t, pool, batch.ID, batches.StatusCompleted, 30*time.Second)

	results := readResults(t, pool, batch.ID)
	if len(results) != 5 {
		t.Fatalf("%d results, want 5", len(results))
	}
	for _, r := range results {
		var reply struct {
			Choices []json.RawMessage `json:"choices"`
		}
		if !r.Succeeded || json.Unmarshal(r.Response, &reply) != nil || len(reply.Choices) != 1 {
			t.Errorf("result %s is not a complete reply: succeeded %v, response %s", r.CustomID, r.Succeeded, r.Response)
		}
	}
	for _, attempts := range jobAttempts(t, pool, batch.ID) {
		if attempts != 2 {
			t.Errorf("a job took %d attempts, want 2 (cut off, then complete)", attempts)
		}
	}
	if calls.Load() != 10 {
		t.Errorf("model was called %d times, want 10 (two per request)", calls.Load())
	}
}

// TestAlwaysCutOffReplyFailsAfterMaxAttempts checks a request whose replies
// are always cut off fails after the last attempt, with a reason, and nothing
// partial is saved as a success.
func TestAlwaysCutOffReplyFailsAfterMaxAttempts(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	modelURL := startFakeModel(t, fakemodel.Config{OutputWords: 5, CrashRate: 1})
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 3)
	newSystem(t, pool, modelURL, testWorkerConfig()).start(t)

	waitForStatus(t, pool, batch.ID, batches.StatusCompleted, 30*time.Second)

	for _, r := range readResults(t, pool, batch.ID) {
		if r.Succeeded || r.Response != nil || r.ErrorCode == nil || *r.ErrorCode != "retries_exhausted" {
			t.Errorf("result %s: succeeded %v, response %s, error code %v; want retries_exhausted and no response",
				r.CustomID, r.Succeeded, r.Response, r.ErrorCode)
		}
	}
	for _, attempts := range jobAttempts(t, pool, batch.ID) {
		if attempts != 3 {
			t.Errorf("a job took %d attempts, want 3 (MaxAttempts)", attempts)
		}
	}
	got, err := batches.Get(context.Background(), pool, "tenant_a", batch.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.RequestCounts != (batches.RequestCounts{Total: 3, Failed: 3}) {
		t.Errorf("request counts = %+v, want 3 failed", got.RequestCounts)
	}
}

// TestRejectedRequestFailsWithoutRetry checks a 400 from the model fails the
// request on the first attempt, since retrying cannot help.
func TestRejectedRequestFailsWithoutRetry(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error": {"message": "unknown model"}}`, http.StatusBadRequest)
	}))
	t.Cleanup(server.Close)
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 2)
	newSystem(t, pool, server.URL+"/v1", testWorkerConfig()).start(t)

	waitForStatus(t, pool, batch.ID, batches.StatusCompleted, 30*time.Second)

	for _, r := range readResults(t, pool, batch.ID) {
		if r.Succeeded || r.ErrorCode == nil || *r.ErrorCode != "model_rejected" {
			t.Errorf("result %s: succeeded %v, error code %v; want model_rejected", r.CustomID, r.Succeeded, r.ErrorCode)
		}
	}
	for _, attempts := range jobAttempts(t, pool, batch.ID) {
		if attempts != 1 {
			t.Errorf("a job took %d attempts, want 1", attempts)
		}
	}
}

// queueOneRequest creates a one-line batch and runs one scheduler pass, so
// its request is queued with a River job, while no worker is running yet.
func queueOneRequest(t *testing.T, sys *system) batches.Batch {
	t.Helper()
	batch := testfixtures.CreateBatch(t, sys.pool, "tenant_a", "a", 1)
	if result, err := sys.scheduler.RunPass(context.Background()); err != nil || result.Admitted != 1 {
		t.Fatalf("RunPass = %+v, %v; want 1 admitted", result, err)
	}
	return batch
}

// TestRetryAfterCrashedAttemptFinishes checks our side of crash recovery
// (guarantee 1): a crashed attempt leaves the request running and its job
// up for retry (what River's rescuer does), and the retry still finishes it.
func TestRetryAfterCrashedAttemptFinishes(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	sys := newSystem(t, pool, startFakeModel(t, fakemodel.Config{OutputWords: 5}), testWorkerConfig())
	batch := queueOneRequest(t, sys)

	// The state a worker that died mid-request leaves behind, after River's
	// rescuer has made its job available again.
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "UPDATE requests SET status = 'running', started_at = now()"); err != nil {
		t.Fatalf("simulate crashed attempt: %v", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE river_job SET attempt = 1, attempted_at = now(), state = 'available'"); err != nil {
		t.Fatalf("simulate rescued job: %v", err)
	}
	sys.start(t)

	waitForStatus(t, pool, batch.ID, batches.StatusCompleted, 30*time.Second)
	if results := readResults(t, pool, batch.ID); len(results) != 1 || !results[0].Succeeded {
		t.Errorf("results = %+v, want one success", results)
	}
}

// TestStuckJobIsRescuedByRiver checks crash recovery end to end with River's
// real rescuer (guarantee 1): a job left "running" by a worker that died is
// found, run again, and the request finishes. River's rescuer runs when a
// client becomes leader and then every 30 seconds, so here it acts at start.
func TestStuckJobIsRescuedByRiver(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	config := testWorkerConfig()
	config.JobTimeout = time.Second
	config.RescueStuckJobsAfter = 2 * time.Second
	sys := newSystem(t, pool, startFakeModel(t, fakemodel.Config{OutputWords: 5}), config)
	batch := queueOneRequest(t, sys)

	// Exactly what a worker process killed mid-request leaves: the job
	// running, attempted by a client that no longer exists, a while ago.
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "UPDATE requests SET status = 'running', started_at = now()"); err != nil {
		t.Fatalf("simulate crashed attempt: %v", err)
	}
	_, err := pool.Exec(ctx, `
		UPDATE river_job
		SET state = 'running', attempt = 1, attempted_at = now() - interval '1 minute',
		    attempted_by = ARRAY['worker-that-died']`)
	if err != nil {
		t.Fatalf("simulate stuck job: %v", err)
	}
	sys.start(t)

	waitForStatus(t, pool, batch.ID, batches.StatusCompleted, 90*time.Second)
	if results := readResults(t, pool, batch.ID); len(results) != 1 || !results[0].Succeeded {
		t.Errorf("results = %+v, want one success", results)
	}

	// Make sure it really was River's rescuer that freed the job: the job
	// ran a second attempt, and River recorded the rescue as its error.
	var attempt int
	var jobErrors string
	err = pool.QueryRow(ctx, "SELECT attempt, coalesce(errors::text, '') FROM river_job").Scan(&attempt, &jobErrors)
	if err != nil {
		t.Fatalf("read job: %v", err)
	}
	if attempt != 2 || !strings.Contains(strings.ToLower(jobErrors), "rescue") {
		t.Errorf("job attempt = %d, errors = %q; want attempt 2 with a rescue recorded", attempt, jobErrors)
	}
}

// TestSameRequestRunTwiceSavesOneResult runs two jobs for the same request at
// the same time, as happens when a slow worker is rescued while still alive.
// Both call the model, but only one result is saved (guarantee 2).
func TestSameRequestRunTwiceSavesOneResult(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	var calls atomic.Int64
	slowModel, err := fakemodel.NewServer(fakemodel.Config{MinDelay: 300 * time.Millisecond, MaxDelay: 300 * time.Millisecond, OutputWords: 5})
	if err != nil {
		t.Fatalf("fakemodel.NewServer: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		slowModel.Handler().ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)

	sys := newSystem(t, pool, server.URL+"/v1", testWorkerConfig())
	batch := queueOneRequest(t, sys)
	// A second job for the same request.
	if _, err := sys.river.Insert(context.Background(), requests.RunArgs{BatchID: batch.ID, CustomID: "a-0"}, nil); err != nil {
		t.Fatalf("insert duplicate job: %v", err)
	}
	sys.start(t)

	waitForStatus(t, pool, batch.ID, batches.StatusCompleted, 30*time.Second)
	// The batch completes as soon as the first job saves; the second job
	// may still be finishing, so wait for both jobs.
	deadline := time.Now().Add(10 * time.Second)
	var unfinishedJobs int
	for {
		if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM river_job WHERE state <> 'completed'").Scan(&unfinishedJobs); err != nil {
			t.Fatalf("count jobs: %v", err)
		}
		if unfinishedJobs == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if unfinishedJobs != 0 {
		t.Errorf("%d jobs did not complete, want both completed", unfinishedJobs)
	}

	if calls.Load() != 2 {
		t.Fatalf("model was called %d times, want 2 (both jobs ran)", calls.Load())
	}
	if results := readResults(t, pool, batch.ID); len(results) != 1 {
		t.Errorf("%d results saved, want exactly 1", len(results))
	}
}

// TestCancelMidBatch cancels a batch while workers are busy with a slow model.
// No request may start after the cancel, running requests finish and keep
// their results, and every custom_id ends up in exactly one file.
func TestCancelMidBatch(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	modelURL := startFakeModel(t, fakemodel.Config{MinDelay: 200 * time.Millisecond, MaxDelay: 300 * time.Millisecond, OutputWords: 3})
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 60)
	sys := newSystem(t, pool, modelURL, testWorkerConfig())
	sys.start(t)

	// Wait until some requests are done and the window is busy.
	ctx := context.Background()
	deadline := time.Now().Add(20 * time.Second)
	for testfixtures.CountRequests(t, pool, batch.ID)["succeeded"] < 5 {
		if time.Now().After(deadline) {
			t.Fatal("no progress before cancelling")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := cancel.Batch(ctx, pool, sys.river, "tenant_a", batch.ID); err != nil {
		t.Fatalf("cancel.Batch: %v", err)
	}
	waitForStatus(t, pool, batch.ID, batches.StatusCancelled, 30*time.Second)

	var startedAfterCancel, succeeded, cancelled int
	err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE r.started_at > b.cancelling_at),
		       count(*) FILTER (WHERE r.status = 'succeeded'),
		       count(*) FILTER (WHERE r.status = 'cancelled')
		FROM requests r JOIN batches b ON b.id = r.batch_id
		WHERE b.id = $1`, batch.ID).Scan(&startedAfterCancel, &succeeded, &cancelled)
	if err != nil {
		t.Fatalf("read requests: %v", err)
	}
	if startedAfterCancel != 0 {
		t.Errorf("%d requests started after the cancel, want 0", startedAfterCancel)
	}
	if succeeded+cancelled != 60 || cancelled == 0 {
		t.Errorf("%d succeeded and %d cancelled, want 60 in total with some cancelled", succeeded, cancelled)
	}

	final, err := batches.Get(ctx, pool, "tenant_a", batch.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	seen := map[string]int{}
	for _, line := range downloadLines(t, pool, *final.OutputFileID) {
		seen[line.CustomID]++
	}
	for _, line := range downloadLines(t, pool, *final.ErrorFileID) {
		seen[line.CustomID]++
		if !strings.Contains(string(line.Error), "batch_cancelled") {
			t.Errorf("errors line for %s should say batch_cancelled: %s", line.CustomID, line.Error)
		}
	}
	for i := range 60 {
		if customID := fmt.Sprintf("a-%d", i); seen[customID] != 1 {
			t.Errorf("%s appears %d times across both files, want exactly once", customID, seen[customID])
		}
	}
	t.Logf("%d finished before or during the cancel, %d cancelled", succeeded, cancelled)
}

// TestCancelDoesNotWaitForRetries cancels while every request is mid-call to a
// model that always fails. Those attempts must not wait for a retry; the
// batch must become cancelled long before the (deliberately long) retry
// delay would pass, with no request tried twice.
func TestCancelDoesNotWaitForRetries(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	modelURL := startFakeModel(t, fakemodel.Config{MinDelay: 300 * time.Millisecond, MaxDelay: 300 * time.Millisecond, OutputWords: 3, ErrorRate: 1})
	config := testWorkerConfig()
	config.RetryBaseDelay = 10 * time.Second
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 20)
	sys := newSystem(t, pool, modelURL, config)
	sys.start(t)

	ctx := context.Background()
	deadline := time.Now().Add(10 * time.Second)
	for testfixtures.CountRequests(t, pool, batch.ID)["running"] == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no request started")
		}
		time.Sleep(10 * time.Millisecond)
	}
	start := time.Now()
	if _, err := cancel.Batch(ctx, pool, sys.river, "tenant_a", batch.ID); err != nil {
		t.Fatalf("cancel.Batch: %v", err)
	}
	waitForStatus(t, pool, batch.ID, batches.StatusCancelled, 30*time.Second)

	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("cancelled after %s; the 10 s retry delay was waited out", elapsed)
	}
	for _, attempts := range jobAttempts(t, pool, batch.ID) {
		if attempts > 1 {
			t.Errorf("a job ran %d attempts, want at most 1 (no retries after the cancel)", attempts)
		}
	}
}

// TestExpiryMidBatch runs a batch with a 2-second window against a model too
// slow to finish it. At the deadline the batch expires: results saved before
// the deadline are kept, nothing is saved after it, and every custom_id is in
// exactly one file.
func TestExpiryMidBatch(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	modelURL := startFakeModel(t, fakemodel.Config{MinDelay: 200 * time.Millisecond, MaxDelay: 400 * time.Millisecond, OutputWords: 3})
	batch := testfixtures.CreateBatchWithWindow(t, pool, "tenant_a", "a", 200, 2*time.Second)
	newSystem(t, pool, modelURL, testWorkerConfig()).start(t)

	waitForStatus(t, pool, batch.ID, batches.StatusExpired, 30*time.Second)

	ctx := context.Background()
	var succeeded, expired, savedLate int
	var expiredAfter time.Duration
	err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE r.status = 'succeeded'),
		       count(*) FILTER (WHERE r.status = 'expired'),
		       count(*) FILTER (WHERE r.status = 'succeeded' AND r.finished_at >= b.expires_at),
		       max(b.expired_at - b.expires_at)
		FROM requests r JOIN batches b ON b.id = r.batch_id WHERE b.id = $1`, batch.ID,
	).Scan(&succeeded, &expired, &savedLate, &expiredAfter)
	if err != nil {
		t.Fatalf("read requests: %v", err)
	}
	if succeeded == 0 || expired == 0 || succeeded+expired != 200 {
		t.Errorf("%d succeeded, %d expired; want some of each, 200 in total", succeeded, expired)
	}
	if savedLate != 0 {
		t.Errorf("%d results saved after the deadline were kept", savedLate)
	}

	final, err := batches.Get(ctx, pool, "tenant_a", batch.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	seen := map[string]int{}
	for _, line := range downloadLines(t, pool, *final.OutputFileID) {
		seen[line.CustomID]++
	}
	for _, line := range downloadLines(t, pool, *final.ErrorFileID) {
		seen[line.CustomID]++
		if !strings.Contains(string(line.Error), "batch_expired") {
			t.Errorf("errors line for %s should say batch_expired: %s", line.CustomID, line.Error)
		}
	}
	for i := range 200 {
		if customID := fmt.Sprintf("a-%d", i); seen[customID] != 1 {
			t.Errorf("%s appears %d times across both files, want exactly once", customID, seen[customID])
		}
	}
	t.Logf("%d finished before the deadline, %d expired; expired %s after the deadline", succeeded, expired, expiredAfter)
}

// TestSmallTenantNotBlockedByBigBatch is the fairness guarantee end to end: a
// big tenant's batch is running when a small tenant's batch arrives. With fair
// scheduling the small batch finishes while most of the big batch is still
// waiting. Without it (first come, first served), the small batch is stuck
// behind the big one, which shows the test can tell the two apart.
func TestSmallTenantNotBlockedByBigBatch(t *testing.T) {
	t.Parallel()
	for _, fair := range []bool{true, false} {
		t.Run(map[bool]string{true: "fair", false: "first come first served"}[fair], func(t *testing.T) {
			t.Parallel()
			pool := testdb.New(t)
			modelURL := startFakeModel(t, fakemodel.Config{MinDelay: 20 * time.Millisecond, MaxDelay: 40 * time.Millisecond, OutputWords: 3})
			big := testfixtures.CreateBatch(t, pool, "tenant_big", "big", 300)
			newSystemWithFairness(t, pool, modelURL, testWorkerConfig(), fair).start(t)

			// Let the big batch fill the window before the small one arrives.
			deadline := time.Now().Add(10 * time.Second)
			for testfixtures.CountRequests(t, pool, big.ID)["succeeded"] < 20 {
				if time.Now().After(deadline) {
					t.Fatal("big batch made no progress")
				}
				time.Sleep(10 * time.Millisecond)
			}
			small := testfixtures.CreateBatch(t, pool, "tenant_small", "small", 20)

			// Wait until the small batch is done, or until the big one is.
			deadline = time.Now().Add(60 * time.Second)
			var bigPending int
			for {
				smallDone := testfixtures.BatchStatus(t, pool, small.ID) == batches.StatusCompleted
				bigCounts := testfixtures.CountRequests(t, pool, big.ID)
				bigPending = bigCounts["pending"]
				if smallDone || bigPending == 0 || time.Now().After(deadline) {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}

			var firstSlotWait *time.Duration // nil if the small batch never got a slot
			err := pool.QueryRow(context.Background(), `
				SELECT min(r.queued_at) - b.created_at
				FROM requests r JOIN batches b ON b.id = r.batch_id
				WHERE b.id = $1 GROUP BY b.created_at`, small.ID).Scan(&firstSlotWait)
			if err != nil {
				t.Fatalf("read first slot wait: %v", err)
			}
			firstSlot := "never"
			if firstSlotWait != nil {
				firstSlot = firstSlotWait.String()
			}
			t.Logf("small batch status %s with %d of 300 big requests still pending; first slot: %s",
				testfixtures.BatchStatus(t, pool, small.ID), bigPending, firstSlot)

			if fair && bigPending < 150 {
				t.Errorf("fair: only %d of the big batch's requests were still pending when the small batch finished, want most (> 150)", bigPending)
			}
			if !fair && bigPending > 50 {
				t.Errorf("first come first served: the small batch finished with %d big requests still pending; expected it to wait behind the big batch", bigPending)
			}
		})
	}
}

// TestShutdownMidRequestKeepsItsAttempt stops the workers while a request is
// mid-call (as a graceful server shutdown does, cancelling running jobs),
// then starts new workers. The interrupted attempt must not count: even with
// a single attempt allowed, the request runs again and succeeds instead of
// being given up as failed (guarantee 1).
func TestShutdownMidRequestKeepsItsAttempt(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	ctx := context.Background()
	modelURL := startFakeModel(t, fakemodel.Config{MinDelay: time.Second, MaxDelay: time.Second, OutputWords: 3})
	config := testWorkerConfig()
	config.MaxAttempts = 1
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 1)

	// First "server": its workers pick up the request, and it shuts down
	// while the request is mid-call.
	first := newSystem(t, pool, modelURL, config)
	if _, err := first.scheduler.RunPass(ctx); err != nil {
		t.Fatalf("RunPass: %v", err)
	}
	if err := first.river.Start(ctx); err != nil {
		t.Fatalf("start first workers: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for testfixtures.CountRequests(t, pool, batch.ID)["running"] == 0 {
		if time.Now().After(deadline) {
			t.Fatal("request never started")
		}
		time.Sleep(10 * time.Millisecond)
	}
	stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := first.river.StopAndCancel(stopCtx); err != nil {
		t.Fatalf("stop first workers: %v", err)
	}

	// Second "server" starts up.
	newSystem(t, pool, modelURL, config).start(t)
	waitForStatus(t, pool, batch.ID, batches.StatusCompleted, 30*time.Second)

	results := readResults(t, pool, batch.ID)
	if len(results) != 1 || !results[0].Succeeded {
		t.Errorf("result = %s, want success; the shutdown used up the only attempt", describeResults(results))
	}
}

// describeResults summarizes results for test output.
func describeResults(results []result) string {
	parts := make([]string, len(results))
	for i, r := range results {
		parts[i] = fmt.Sprintf("%s succeeded=%v error=%s", r.CustomID, r.Succeeded, describeError(r))
	}
	return strings.Join(parts, "; ")
}
