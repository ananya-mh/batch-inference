// Tests for storing batches, compare-and-set status changes, and paging,
// against a real database.
package batches

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ananya-mh/batch-inference/internal/files"
	"github.com/ananya-mh/batch-inference/internal/tenants"
	"github.com/ananya-mh/batch-inference/internal/testdb"
)

// testInputLine is a valid batch input line.
const testInputLine = `{"custom_id": "a", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "fake", "messages": [{"role": "user", "content": "hi"}]}}`

// setup returns a fresh database with tenant_a and tenant_b, and the ID of
// a one-line input file owned by tenant_a.
func setup(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	ctx := context.Background()
	pool := testdb.New(t)
	for _, tenantID := range []string{"tenant_a", "tenant_b"} {
		if err := tenants.Create(ctx, pool, tenantID, "sk-"+tenantID); err != nil {
			t.Fatalf("create %s: %v", tenantID, err)
		}
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	file, err := files.CreateBatchInput(ctx, tx, "tenant_a", "in.jsonl", strings.NewReader(testInputLine+"\n"))
	if err != nil {
		t.Fatalf("create input file: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return pool, file.ID
}

// createBatch creates a 24h batch for tenant_a from inputFileID.
func createBatch(t *testing.T, pool *pgxpool.Pool, inputFileID string) Batch {
	t.Helper()
	batch, err := Create(context.Background(), pool, NewBatch{
		TenantID:         "tenant_a",
		InputFileID:      inputFileID,
		Endpoint:         files.ChatCompletionsURL,
		CompletionWindow: "24h",
		WindowLength:     24 * time.Hour,
		RequestTotal:     1,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return batch
}

// TestCreateStartsValidatingWithExpiry checks a new batch's starting state.
func TestCreateStartsValidatingWithExpiry(t *testing.T) {
	t.Parallel()
	pool, inputFileID := setup(t)

	batch := createBatch(t, pool, inputFileID)

	if batch.Status != StatusValidating {
		t.Errorf("status = %s, want validating", batch.Status)
	}
	if batch.RequestCounts != (RequestCounts{Total: 1}) {
		t.Errorf("request counts = %+v, want total 1", batch.RequestCounts)
	}
	if window := batch.ExpiresAt.Sub(batch.CreatedAt); window != 24*time.Hour {
		t.Errorf("expires %s after creation, want 24h", window)
	}
	if batch.InProgressAt != nil || batch.OutputFileID != nil {
		t.Errorf("new batch already has in_progress_at or output file: %+v", batch)
	}
}

// TestGetIsLimitedToOwner checks another tenant cannot see a batch.
func TestGetIsLimitedToOwner(t *testing.T) {
	t.Parallel()
	pool, inputFileID := setup(t)
	batch := createBatch(t, pool, inputFileID)

	if _, err := Get(context.Background(), pool, "tenant_a", batch.ID); err != nil {
		t.Errorf("owner Get: %v", err)
	}
	if _, err := Get(context.Background(), pool, "tenant_b", batch.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("other tenant Get: error = %v, want ErrNotFound", err)
	}
}

// TestTransitionChangesStatusAndRecordsTime checks an allowed change works.
func TestTransitionChangesStatusAndRecordsTime(t *testing.T) {
	t.Parallel()
	pool, inputFileID := setup(t)
	ctx := context.Background()
	batch := createBatch(t, pool, inputFileID)

	if err := Transition(ctx, pool, batch.ID, StatusValidating, StatusInProgress); err != nil {
		t.Fatalf("Transition: %v", err)
	}

	updated, err := Get(ctx, pool, "tenant_a", batch.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if updated.Status != StatusInProgress || updated.InProgressAt == nil {
		t.Errorf("status = %s, in_progress_at = %v; want in_progress with a time", updated.Status, updated.InProgressAt)
	}
}

// TestInvalidTransitionIsRejected checks a change the state machine forbids
// is refused and leaves the batch untouched.
func TestInvalidTransitionIsRejected(t *testing.T) {
	t.Parallel()
	pool, inputFileID := setup(t)
	ctx := context.Background()
	batch := createBatch(t, pool, inputFileID)

	invalid := [][2]Status{
		{StatusValidating, StatusCompleted},  // skips in_progress and finalizing
		{StatusValidating, StatusCancelled},  // cancel must go through cancelling
		{StatusValidating, StatusValidating}, // not a change
	}
	for _, change := range invalid {
		err := Transition(ctx, pool, batch.ID, change[0], change[1])
		if !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("%s -> %s: error = %v, want ErrInvalidTransition", change[0], change[1], err)
		}
	}

	unchanged, err := Get(ctx, pool, "tenant_a", batch.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if unchanged.Status != StatusValidating {
		t.Errorf("status = %s after rejected changes, want validating", unchanged.Status)
	}
}

// TestFinalBatchCannotBeReopened walks a batch to completed, then checks no
// change out of completed is possible.
func TestFinalBatchCannotBeReopened(t *testing.T) {
	t.Parallel()
	pool, inputFileID := setup(t)
	ctx := context.Background()
	batch := createBatch(t, pool, inputFileID)

	path := []Status{StatusValidating, StatusInProgress, StatusFinalizing, StatusCompleted}
	for i := 1; i < len(path); i++ {
		if err := Transition(ctx, pool, batch.ID, path[i-1], path[i]); err != nil {
			t.Fatalf("%s -> %s: %v", path[i-1], path[i], err)
		}
	}

	for _, to := range allStatuses {
		if err := Transition(ctx, pool, batch.ID, StatusCompleted, to); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("completed -> %s: error = %v, want ErrInvalidTransition", to, err)
		}
	}
}

// TestStaleTransitionLoses checks compare-and-set: a change that expects an
// old status fails once the batch has moved on.
func TestStaleTransitionLoses(t *testing.T) {
	t.Parallel()
	pool, inputFileID := setup(t)
	ctx := context.Background()
	batch := createBatch(t, pool, inputFileID)

	if err := Transition(ctx, pool, batch.ID, StatusValidating, StatusCancelling); err != nil {
		t.Fatalf("first Transition: %v", err)
	}
	// This caller still thinks the batch is validating.
	err := Transition(ctx, pool, batch.ID, StatusValidating, StatusInProgress)
	if !errors.Is(err, ErrStatusChanged) {
		t.Fatalf("stale Transition: error = %v, want ErrStatusChanged", err)
	}
	if !strings.Contains(err.Error(), "cancelling") {
		t.Errorf("error %q should name the current status", err)
	}
}

// TestConcurrentTransitionsOnlyOneWins races many status changes from the
// same starting status; exactly one may succeed and the final status must be
// the winner's.
func TestConcurrentTransitionsOnlyOneWins(t *testing.T) {
	t.Parallel()
	pool, inputFileID := setup(t)
	ctx := context.Background()

	// Repeat the race a few times, since a race that happens to run in
	// order would pass by luck.
	for range 5 {
		batch := createBatch(t, pool, inputFileID)

		// Half try to start the batch, half try to cancel it, like a
		// worker and a cancel request arriving together.
		const racers = 20
		targets := make([]Status, racers)
		results := make([]error, racers)
		var start, done sync.WaitGroup
		start.Add(1)
		for i := range racers {
			targets[i] = StatusInProgress
			if i%2 == 1 {
				targets[i] = StatusCancelling
			}
			done.Add(1)
			go func() {
				defer done.Done()
				start.Wait() // release all racers at once
				results[i] = Transition(ctx, pool, batch.ID, StatusValidating, targets[i])
			}()
		}
		start.Done()
		done.Wait()

		var winner Status
		wins := 0
		for i, err := range results {
			switch {
			case err == nil:
				wins++
				winner = targets[i]
			case !errors.Is(err, ErrStatusChanged):
				t.Errorf("racer %d: unexpected error %v", i, err)
			}
		}
		if wins != 1 {
			t.Fatalf("%d racers succeeded, want exactly 1", wins)
		}

		final, err := Get(ctx, pool, "tenant_a", batch.ID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if final.Status != winner {
			t.Errorf("final status = %s, but the winner set %s", final.Status, winner)
		}
	}
}

// TestTransitionOfMissingBatch checks a missing batch gives ErrNotFound.
func TestTransitionOfMissingBatch(t *testing.T) {
	t.Parallel()
	pool, _ := setup(t)
	err := Transition(context.Background(), pool, "batch_missing", StatusValidating, StatusInProgress)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
}

// TestListPagesNewestFirst checks paging walks all of a tenant's batches,
// newest first, with no repeats or gaps, and shows nothing to other tenants.
func TestListPagesNewestFirst(t *testing.T) {
	t.Parallel()
	pool, inputFileID := setup(t)
	ctx := context.Background()

	var createdNewestFirst []string
	for range 5 {
		batch := createBatch(t, pool, inputFileID)
		createdNewestFirst = append([]string{batch.ID}, createdNewestFirst...)
	}

	var listed []string
	after := ""
	for pageNumber := 1; ; pageNumber++ {
		page, err := List(ctx, pool, "tenant_a", 2, after)
		if err != nil {
			t.Fatalf("List page %d: %v", pageNumber, err)
		}
		for _, batch := range page.Batches {
			listed = append(listed, batch.ID)
		}
		if !page.HasMore {
			break
		}
		if pageNumber > 5 {
			t.Fatal("paging did not stop")
		}
		after = page.Batches[len(page.Batches)-1].ID
	}
	if strings.Join(listed, ",") != strings.Join(createdNewestFirst, ",") {
		t.Errorf("listed %v, want %v", listed, createdNewestFirst)
	}

	otherTenant, err := List(ctx, pool, "tenant_b", 10, "")
	if err != nil {
		t.Fatalf("List as tenant_b: %v", err)
	}
	if len(otherTenant.Batches) != 0 {
		t.Errorf("tenant_b sees %d of tenant_a's batches, want 0", len(otherTenant.Batches))
	}
	if _, err := List(ctx, pool, "tenant_b", 10, createdNewestFirst[0]); !errors.Is(err, ErrNotFound) {
		t.Errorf("tenant_b paging after tenant_a's batch: error = %v, want ErrNotFound", err)
	}
}

// TestParseCompletionWindows checks the COMPLETION_WINDOWS format.
func TestParseCompletionWindows(t *testing.T) {
	t.Parallel()
	windows, err := ParseCompletionWindows("24h, 5s")
	if err != nil {
		t.Fatalf("ParseCompletionWindows: %v", err)
	}
	if len(windows) != 2 || windows["24h"] != 24*time.Hour || windows["5s"] != 5*time.Second {
		t.Errorf("windows = %v", windows)
	}

	for _, bad := range []string{"", "soon", "-5s", "0s"} {
		if _, err := ParseCompletionWindows(bad); err == nil {
			t.Errorf("ParseCompletionWindows(%q) succeeded, want an error", bad)
		}
	}
}
