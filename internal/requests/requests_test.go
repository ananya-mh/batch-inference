// Tests for the start check and saving results, against a real database.
package requests_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ananya-mh/batch-inference/internal/batches"
	"github.com/ananya-mh/batch-inference/internal/database"
	"github.com/ananya-mh/batch-inference/internal/requests"
	"github.com/ananya-mh/batch-inference/internal/testdb"
	"github.com/ananya-mh/batch-inference/internal/testfixtures"
)

// setupQueuedRequest creates a one-line batch that is in progress, with its
// request already queued (as if the scheduler had admitted it), and returns
// the pool and the batch ID. The custom_id is "r-0".
func setupQueuedRequest(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	ctx := context.Background()
	pool := testdb.New(t)
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "r", 1)

	_, err := pool.Exec(ctx, `
		INSERT INTO requests (batch_id, custom_id, tenant_id, line_number, status)
		VALUES ($1, 'r-0', 'tenant_a', 1, 'queued')`, batch.ID)
	if err != nil {
		t.Fatalf("insert request: %v", err)
	}
	if err := batches.Transition(ctx, pool, batch.ID, batches.StatusValidating, batches.StatusInProgress); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	return pool, batch.ID
}

// inTx runs fn in a transaction and commits it.
func inTx(t *testing.T, pool *pgxpool.Pool, fn func(tx pgx.Tx)) {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer database.RollbackUnlessCommitted(ctx, tx)
	fn(tx)
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// requestStatus returns the status of request r-0 of the batch.
func requestStatus(t *testing.T, pool *pgxpool.Pool, batchID string) requests.Status {
	t.Helper()
	var status requests.Status
	err := pool.QueryRow(context.Background(),
		"SELECT status FROM requests WHERE batch_id = $1 AND custom_id = 'r-0'", batchID,
	).Scan(&status)
	if err != nil {
		t.Fatalf("read request status: %v", err)
	}
	return status
}

// countResults returns how many results the batch has.
func countResults(t *testing.T, pool *pgxpool.Pool, batchID string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM results WHERE batch_id = $1", batchID).Scan(&count); err != nil {
		t.Fatalf("count results: %v", err)
	}
	return count
}

// successOutcome is a successful outcome with a small JSON reply.
var successOutcome = requests.Outcome{Succeeded: true, Response: []byte(`{"choices": []}`)}

// TestStartMarksRunningAndReturnsBody checks the start check on a request that
// may run.
func TestStartMarksRunningAndReturnsBody(t *testing.T) {
	t.Parallel()
	pool, batchID := setupQueuedRequest(t)

	inTx(t, pool, func(tx pgx.Tx) {
		body, started, err := requests.Start(context.Background(), tx, batchID, "r-0")
		if err != nil || !started {
			t.Fatalf("Start = started %v, err %v; want started", started, err)
		}
		if !strings.Contains(string(body), "classify ticket r-0") {
			t.Errorf("body = %s, want the line's request body", body)
		}
	})
	if status := requestStatus(t, pool, batchID); status != requests.StatusRunning {
		t.Errorf("status = %s, want running", status)
	}
}

// TestStartCancelsRequestOfCancellingBatch checks a queued request of a
// cancelling batch does not start, and is marked cancelled so it frees its
// slot (guarantee 3).
func TestStartCancelsRequestOfCancellingBatch(t *testing.T) {
	t.Parallel()
	pool, batchID := setupQueuedRequest(t)
	if err := batches.Transition(context.Background(), pool, batchID, batches.StatusInProgress, batches.StatusCancelling); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	inTx(t, pool, func(tx pgx.Tx) {
		if _, started, err := requests.Start(context.Background(), tx, batchID, "r-0"); err != nil || started {
			t.Errorf("Start = started %v, err %v; want not started", started, err)
		}
	})
	if status := requestStatus(t, pool, batchID); status != requests.StatusCancelled {
		t.Errorf("status = %s, want cancelled", status)
	}
}

// TestStartAcceptsRequestLeftRunningByCrashedAttempt checks that a request a
// crashed worker left running can be started again by the retry.
func TestStartAcceptsRequestLeftRunningByCrashedAttempt(t *testing.T) {
	t.Parallel()
	pool, batchID := setupQueuedRequest(t)

	for attempt := 1; attempt <= 2; attempt++ {
		inTx(t, pool, func(tx pgx.Tx) {
			if _, started, err := requests.Start(context.Background(), tx, batchID, "r-0"); err != nil || !started {
				t.Errorf("attempt %d: Start = started %v, err %v; want started", attempt, started, err)
			}
		})
	}
}

// TestFinishSavesOnlyOnce checks a second save for the same request saves
// nothing (guarantee 2).
func TestFinishSavesOnlyOnce(t *testing.T) {
	t.Parallel()
	pool, batchID := setupQueuedRequest(t)
	ctx := context.Background()
	inTx(t, pool, func(tx pgx.Tx) {
		if _, _, err := requests.Start(ctx, tx, batchID, "r-0"); err != nil {
			t.Fatalf("Start: %v", err)
		}
	})

	for i, wantSaved := range []bool{true, false} {
		inTx(t, pool, func(tx pgx.Tx) {
			saved, err := requests.Finish(ctx, tx, batchID, "r-0", successOutcome)
			if err != nil || saved != wantSaved {
				t.Errorf("save %d: saved = %v, err = %v; want saved = %v", i+1, saved, err, wantSaved)
			}
		})
	}
	if count := countResults(t, pool, batchID); count != 1 {
		t.Errorf("%d results saved, want 1", count)
	}
	if status := requestStatus(t, pool, batchID); status != requests.StatusSucceeded {
		t.Errorf("status = %s, want succeeded", status)
	}
}

// TestConcurrentFinishSavesOnlyOnce races two saves of the same request, as
// when a rescued job and the original worker both finish; exactly one wins.
func TestConcurrentFinishSavesOnlyOnce(t *testing.T) {
	t.Parallel()
	pool, batchID := setupQueuedRequest(t)
	ctx := context.Background()
	inTx(t, pool, func(tx pgx.Tx) {
		if _, _, err := requests.Start(ctx, tx, batchID, "r-0"); err != nil {
			t.Fatalf("Start: %v", err)
		}
	})

	const racers = 4
	savedCount := make(chan bool, racers)
	var start, done sync.WaitGroup
	start.Add(1)
	for range racers {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Errorf("begin: %v", err)
				return
			}
			defer database.RollbackUnlessCommitted(ctx, tx)
			saved, err := requests.Finish(ctx, tx, batchID, "r-0", successOutcome)
			if err != nil {
				t.Errorf("Finish: %v", err)
				return
			}
			if err := tx.Commit(ctx); err != nil {
				t.Errorf("commit: %v", err)
				return
			}
			savedCount <- saved
		}()
	}
	start.Done()
	done.Wait()
	close(savedCount)

	wins := 0
	for saved := range savedCount {
		if saved {
			wins++
		}
	}
	if wins != 1 {
		t.Errorf("%d saves succeeded, want exactly 1", wins)
	}
	if count := countResults(t, pool, batchID); count != 1 {
		t.Errorf("%d results saved, want 1", count)
	}
}

// TestFinishAfterExpirySavesNothing checks a result arriving after its
// request expired is dropped, so the request is expired and never also
// finished.
func TestFinishAfterExpirySavesNothing(t *testing.T) {
	t.Parallel()
	pool, batchID := setupQueuedRequest(t)
	ctx := context.Background()
	inTx(t, pool, func(tx pgx.Tx) {
		if _, _, err := requests.Start(ctx, tx, batchID, "r-0"); err != nil {
			t.Fatalf("Start: %v", err)
		}
	})
	// What the expiry step (Progress item 8) will do to unfinished requests.
	if _, err := pool.Exec(ctx, "UPDATE requests SET status = 'expired' WHERE batch_id = $1", batchID); err != nil {
		t.Fatalf("expire request: %v", err)
	}

	inTx(t, pool, func(tx pgx.Tx) {
		if saved, err := requests.Finish(ctx, tx, batchID, "r-0", successOutcome); err != nil || saved {
			t.Errorf("Finish after expiry: saved = %v, err = %v; want not saved", saved, err)
		}
	})
	if count := countResults(t, pool, batchID); count != 0 {
		t.Errorf("%d results saved for an expired request, want 0", count)
	}
	if status := requestStatus(t, pool, batchID); status != requests.StatusExpired {
		t.Errorf("status = %s, want expired", status)
	}
}

// TestGiveUpFailsQueuedRequest checks the scheduler's give-up path saves a
// failed result for a request that never started.
func TestGiveUpFailsQueuedRequest(t *testing.T) {
	t.Parallel()
	pool, batchID := setupQueuedRequest(t)

	inTx(t, pool, func(tx pgx.Tx) {
		saved, err := requests.GiveUp(context.Background(), tx, batchID, "r-0",
			requests.Outcome{ErrorCode: "worker_failed", ErrorMessage: "gave up"})
		if err != nil || !saved {
			t.Errorf("GiveUp: saved = %v, err = %v; want saved", saved, err)
		}
	})
	if status := requestStatus(t, pool, batchID); status != requests.StatusFailed {
		t.Errorf("status = %s, want failed", status)
	}
}
