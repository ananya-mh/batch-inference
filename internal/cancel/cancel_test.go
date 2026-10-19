// Tests for cancelling batches, using the real scheduler and River jobs set
// into each state by hand (no workers run here).
package cancel_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/ananya-mh/batch-inference/internal/batches"
	"github.com/ananya-mh/batch-inference/internal/cancel"
	"github.com/ananya-mh/batch-inference/internal/files"
	"github.com/ananya-mh/batch-inference/internal/model"
	"github.com/ananya-mh/batch-inference/internal/scheduler"
	"github.com/ananya-mh/batch-inference/internal/testdb"
	"github.com/ananya-mh/batch-inference/internal/testfixtures"
	"github.com/ananya-mh/batch-inference/internal/worker"
)

// setup returns a database, a River client that is never started, and a
// scheduler with the given window.
func setup(t *testing.T, window int) (*pgxpool.Pool, *river.Client[pgx.Tx], *scheduler.Scheduler) {
	t.Helper()
	pool := testdb.New(t)
	riverClient, err := worker.NewRiverClient(pool, model.NewClient("http://unused.invalid/v1", ""), worker.Config{
		Workers: 1, MaxAttempts: 3, JobTimeout: time.Second, RescueStuckJobsAfter: 2 * time.Second, RetryBaseDelay: time.Millisecond,
	}, func() {})
	if err != nil {
		t.Fatalf("NewRiverClient: %v", err)
	}
	return pool, riverClient, scheduler.New(pool, riverClient, scheduler.Config{Window: window, PassInterval: time.Hour})
}

// runPass runs one scheduler pass and fails the test on error.
func runPass(t *testing.T, s *scheduler.Scheduler) scheduler.PassResult {
	t.Helper()
	result, err := s.RunPass(context.Background())
	if err != nil {
		t.Fatalf("RunPass: %v", err)
	}
	return result
}

// exec runs SQL and fails the test on error.
func exec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

// succeed plays a worker finishing a request successfully.
func succeed(t *testing.T, pool *pgxpool.Pool, batchID, customID string) {
	t.Helper()
	exec(t, pool, "UPDATE requests SET status = 'succeeded' WHERE batch_id = $1 AND custom_id = $2", batchID, customID)
	exec(t, pool, `INSERT INTO results (batch_id, custom_id, succeeded, response) VALUES ($1, $2, true, '{"choices": []}')`, batchID, customID)
	exec(t, pool, "UPDATE river_job SET state = 'completed', finalized_at = now() WHERE args->>'custom_id' = $1", customID)
}

// requestStatuses returns each request's status by custom_id.
func requestStatuses(t *testing.T, pool *pgxpool.Pool, batchID string) map[string]string {
	t.Helper()
	rows, err := pool.Query(context.Background(), "SELECT custom_id, status FROM requests WHERE batch_id = $1", batchID)
	if err != nil {
		t.Fatalf("query requests: %v", err)
	}
	defer rows.Close()
	statuses := map[string]string{}
	for rows.Next() {
		var customID, status string
		if err := rows.Scan(&customID, &status); err != nil {
			t.Fatalf("scan: %v", err)
		}
		statuses[customID] = status
	}
	return statuses
}

// jobState returns the River job state of a request.
func jobState(t *testing.T, pool *pgxpool.Pool, customID string) string {
	t.Helper()
	var state string
	if err := pool.QueryRow(context.Background(), "SELECT state FROM river_job WHERE args->>'custom_id' = $1", customID).Scan(&state); err != nil {
		t.Fatalf("read job state of %s: %v", customID, err)
	}
	return state
}

// fileLine is the part of a results or errors line these tests check.
type fileLine struct {
	CustomID string `json:"custom_id"`
	Error    *struct {
		Code string `json:"code"`
	} `json:"error"`
}

// readFile returns the lines of tenant_a's file, or nil for a nil ID.
func readFile(t *testing.T, pool *pgxpool.Pool, fileID *string) []fileLine {
	t.Helper()
	if fileID == nil {
		return nil
	}
	var content bytes.Buffer
	if err := files.WriteContent(context.Background(), pool, "tenant_a", *fileID, &content); err != nil {
		t.Fatalf("download: %v", err)
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

// checkFiles checks every custom_id "a-0" ... "a-<total-1>" appears exactly
// once across the batch's files: wantSucceeded in the results file, and the
// rest in the errors file with code batch_cancelled.
func checkFiles(t *testing.T, pool *pgxpool.Pool, batch batches.Batch, total int, wantSucceeded map[string]bool) {
	t.Helper()
	seen := map[string]int{}
	for _, line := range readFile(t, pool, batch.OutputFileID) {
		seen[line.CustomID]++
		if !wantSucceeded[line.CustomID] {
			t.Errorf("%s is in the results file, but it was cancelled", line.CustomID)
		}
	}
	for _, line := range readFile(t, pool, batch.ErrorFileID) {
		seen[line.CustomID]++
		if wantSucceeded[line.CustomID] || line.Error == nil || line.Error.Code != "batch_cancelled" {
			t.Errorf("errors line for %s: want batch_cancelled for a request that did not finish", line.CustomID)
		}
	}
	for i := range total {
		customID := fmt.Sprintf("a-%d", i)
		if seen[customID] != 1 {
			t.Errorf("%s appears %d times across both files, want exactly once", customID, seen[customID])
		}
	}
}

// TestCancelInProgressBatch cancels a batch with a request in every state and
// checks each is handled as the design says.
func TestCancelInProgressBatch(t *testing.T) {
	t.Parallel()
	pool, riverClient, s := setup(t, 5)
	ctx := context.Background()
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 10)
	runPass(t, s) // a-0 ... a-4 queued, a-5 ... a-9 pending

	succeed(t, pool, batch.ID, "a-0") // finished before the cancel
	// a-1 is mid-call: running, and its River job is running.
	exec(t, pool, "UPDATE requests SET status = 'running' WHERE custom_id = 'a-1'")
	exec(t, pool, "UPDATE river_job SET state = 'running', attempt = 1, attempted_at = now() WHERE args->>'custom_id' = 'a-1'")
	// a-2 failed an attempt and waits for its retry.
	exec(t, pool, "UPDATE requests SET status = 'running' WHERE custom_id = 'a-2'")
	exec(t, pool, "UPDATE river_job SET state = 'retryable', attempt = 1, scheduled_at = now() + interval '1 minute' WHERE args->>'custom_id' = 'a-2'")
	// a-3 and a-4 are queued with available jobs; a-5 ... a-9 wait.

	got, err := cancel.Batch(ctx, pool, riverClient, "tenant_a", batch.ID)
	if err != nil {
		t.Fatalf("cancel.Batch: %v", err)
	}
	if got.Status != batches.StatusCancelling || got.CancellingAt == nil {
		t.Errorf("status = %s, cancelling_at = %v; want cancelling with a time", got.Status, got.CancellingAt)
	}

	statuses := requestStatuses(t, pool, batch.ID)
	want := map[string]string{"a-0": "succeeded", "a-1": "running"}
	for i := 2; i < 10; i++ {
		want[fmt.Sprintf("a-%d", i)] = "cancelled"
	}
	for customID, wantStatus := range want {
		if statuses[customID] != wantStatus {
			t.Errorf("%s is %s, want %s", customID, statuses[customID], wantStatus)
		}
	}
	// Queued and retrying jobs never run; the running one is left alone.
	for _, customID := range []string{"a-2", "a-3", "a-4"} {
		if state := jobState(t, pool, customID); state != "cancelled" {
			t.Errorf("job of %s is %s, want cancelled", customID, state)
		}
	}
	if state := jobState(t, pool, "a-1"); state != "running" {
		t.Errorf("job of a-1 is %s, want still running", state)
	}

	// Slots came back: only a-1 is in flight, and nothing new is admitted.
	result := runPass(t, s)
	if result.Admitted != 0 {
		t.Errorf("a pass admitted %d requests of a cancelling batch", result.Admitted)
	}
	if status := testfixtures.BatchStatus(t, pool, batch.ID); status != batches.StatusCancelling {
		t.Errorf("status = %s while a-1 is still running, want cancelling", status)
	}

	// a-1 finishes; then the batch becomes cancelled.
	succeed(t, pool, batch.ID, "a-1")
	if result := runPass(t, s); result.Completed != 1 {
		t.Fatalf("pass = %+v, want the batch ended", result)
	}
	final, err := batches.Get(ctx, pool, "tenant_a", batch.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if final.Status != batches.StatusCancelled || final.CancelledAt == nil {
		t.Errorf("status = %s, want cancelled with a time", final.Status)
	}
	checkFiles(t, pool, final, 10, map[string]bool{"a-0": true, "a-1": true})
}

// TestCancelBeforeFanOut cancels a batch the scheduler has not reached yet;
// its lines must still all appear in the errors file.
func TestCancelBeforeFanOut(t *testing.T) {
	t.Parallel()
	pool, riverClient, s := setup(t, 5)
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 4)

	if _, err := cancel.Batch(context.Background(), pool, riverClient, "tenant_a", batch.ID); err != nil {
		t.Fatalf("cancel.Batch: %v", err)
	}
	runPass(t, s)

	final, err := batches.Get(context.Background(), pool, "tenant_a", batch.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if final.Status != batches.StatusCancelled || final.OutputFileID != nil {
		t.Errorf("status = %s, output file %v; want cancelled with no results file", final.Status, final.OutputFileID)
	}
	checkFiles(t, pool, final, 4, nil)
}

// TestCancelTwiceReturnsCurrentState checks a second cancel changes nothing
// and returns the batch as it is, both while cancelling and once cancelled.
func TestCancelTwiceReturnsCurrentState(t *testing.T) {
	t.Parallel()
	pool, riverClient, s := setup(t, 5)
	ctx := context.Background()
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 3)
	runPass(t, s)
	exec(t, pool, "UPDATE requests SET status = 'running' WHERE custom_id = 'a-0'")
	exec(t, pool, "UPDATE river_job SET state = 'running', attempt = 1, attempted_at = now() WHERE args->>'custom_id' = 'a-0'")

	first, err := cancel.Batch(ctx, pool, riverClient, "tenant_a", batch.ID)
	if err != nil {
		t.Fatalf("first cancel: %v", err)
	}
	second, err := cancel.Batch(ctx, pool, riverClient, "tenant_a", batch.ID)
	if err != nil || second.Status != batches.StatusCancelling || !second.CancellingAt.Equal(*first.CancellingAt) {
		t.Errorf("second cancel = %s at %v (err %v); want cancelling, unchanged", second.Status, second.CancellingAt, err)
	}

	succeed(t, pool, batch.ID, "a-0")
	runPass(t, s)
	third, err := cancel.Batch(ctx, pool, riverClient, "tenant_a", batch.ID)
	if err != nil || third.Status != batches.StatusCancelled {
		t.Errorf("cancel after cancelled = %s (err %v); want cancelled, no error", third.Status, err)
	}
}

// TestCancelFinishedBatchIsRefused checks a batch that is finalizing or
// completed cannot be cancelled, with OpenAI's message.
func TestCancelFinishedBatchIsRefused(t *testing.T) {
	t.Parallel()
	pool, riverClient, s := setup(t, 5)
	ctx := context.Background()
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 2)
	runPass(t, s)
	succeed(t, pool, batch.ID, "a-0")
	succeed(t, pool, batch.ID, "a-1")

	for _, wantStatus := range []batches.Status{batches.StatusFinalizing, batches.StatusCompleted} {
		runPass(t, s)
		got, err := cancel.Batch(ctx, pool, riverClient, "tenant_a", batch.ID)
		var notCancellable *cancel.NotCancellableError
		if !errors.As(err, &notCancellable) || notCancellable.Status != wantStatus {
			t.Fatalf("cancel of %s batch: error = %v, want NotCancellableError", wantStatus, err)
		}
		if want := fmt.Sprintf("Cannot cancel a batch with status '%s'.", wantStatus); err.Error() != want {
			t.Errorf("message = %q, want %q", err, want)
		}
		if got.Status != wantStatus {
			t.Errorf("returned status %s, want %s unchanged", got.Status, wantStatus)
		}
	}
}

// TestCancelOtherTenantsBatch checks tenant isolation.
func TestCancelOtherTenantsBatch(t *testing.T) {
	t.Parallel()
	pool, riverClient, _ := setup(t, 5)
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 1)
	testfixtures.CreateTenant(t, pool, "tenant_b")

	if _, err := cancel.Batch(context.Background(), pool, riverClient, "tenant_b", batch.ID); !errors.Is(err, batches.ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
	if status := testfixtures.BatchStatus(t, pool, batch.ID); status != batches.StatusValidating {
		t.Errorf("status = %s, want unchanged", status)
	}
}

// TestCancelAndFinalizeRace runs a cancel and the scheduler's "all done, start
// finalizing" step at the same moment on a batch whose requests have all
// finished. Exactly one may win, and the loser must notice cleanly.
func TestCancelAndFinalizeRace(t *testing.T) {
	t.Parallel()
	pool, riverClient, s := setup(t, 5)
	ctx := context.Background()
	outcomes := map[string]int{}

	for round := range 20 {
		batch := testfixtures.CreateBatch(t, pool, "tenant_a", fmt.Sprintf("r%d", round), 2)
		runPass(t, s)
		exec(t, pool, `
			WITH done AS (UPDATE requests SET status = 'succeeded' WHERE batch_id = $1 RETURNING batch_id, custom_id)
			INSERT INTO results (batch_id, custom_id, succeeded, response) SELECT batch_id, custom_id, true, '{}' FROM done`, batch.ID)

		var cancelErr, passErr error
		var start, done sync.WaitGroup
		start.Add(1)
		done.Add(2)
		go func() {
			defer done.Done()
			start.Wait()
			_, cancelErr = cancel.Batch(ctx, pool, riverClient, "tenant_a", batch.ID)
		}()
		go func() {
			defer done.Done()
			start.Wait()
			_, passErr = s.RunPass(ctx)
		}()
		start.Done()
		done.Wait()

		if passErr != nil {
			t.Fatalf("round %d: scheduler pass failed: %v", round, passErr)
		}
		status := testfixtures.BatchStatus(t, pool, batch.ID)
		var notCancellable *cancel.NotCancellableError
		switch {
		// If the cancel lands just before the pass, the same pass may already
		// end the batch as cancelled, since nothing is in flight.
		case cancelErr == nil && (status == batches.StatusCancelling || status == batches.StatusCancelled):
			outcomes["cancel won"]++
		case errors.As(cancelErr, &notCancellable) && status == batches.StatusFinalizing:
			outcomes["finalize won"]++
		default:
			t.Fatalf("round %d: cancel error %v with status %s: not exactly one winner", round, cancelErr, status)
		}

		// Either way the batch ends, with both results in the results file.
		runPass(t, s)
		final, err := batches.Get(ctx, pool, "tenant_a", batch.ID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !batches.IsFinal(final.Status) || len(readFile(t, pool, final.OutputFileID)) != 2 || final.ErrorFileID != nil {
			t.Errorf("round %d: ended %s with output %v, errors %v; want both results in the results file", round, final.Status, final.OutputFileID, final.ErrorFileID)
		}
	}
	t.Logf("outcomes over 20 rounds: %v", outcomes)
}
