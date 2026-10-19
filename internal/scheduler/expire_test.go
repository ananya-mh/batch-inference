// Tests for expiring batches whose completion window has ended, including
// races with cancel and with a request finishing at the deadline.
package scheduler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ananya-mh/batch-inference/internal/batches"
	"github.com/ananya-mh/batch-inference/internal/cancel"
	"github.com/ananya-mh/batch-inference/internal/database"
	"github.com/ananya-mh/batch-inference/internal/files"
	"github.com/ananya-mh/batch-inference/internal/requests"
	"github.com/ananya-mh/batch-inference/internal/testdb"
	"github.com/ananya-mh/batch-inference/internal/testfixtures"
)

// exec runs SQL and fails the test on error.
func exec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

// passDeadline moves a batch's deadline into the past, by the database clock.
func passDeadline(t *testing.T, pool *pgxpool.Pool, batchID string) {
	t.Helper()
	exec(t, pool, "UPDATE batches SET expires_at = clock_timestamp() - interval '1 second' WHERE id = $1", batchID)
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

// errorCodesByCustomID downloads the batch's files and returns, for every
// custom_id, how many times it appears and its error code ("" in the
// results file).
func errorCodesByCustomID(t *testing.T, pool *pgxpool.Pool, batch batches.Batch) (counts map[string]int, codes map[string]string) {
	t.Helper()
	counts, codes = map[string]int{}, map[string]string{}
	for _, fileID := range []*string{batch.OutputFileID, batch.ErrorFileID} {
		if fileID == nil {
			continue
		}
		var content bytes.Buffer
		if err := files.WriteContent(context.Background(), pool, "tenant_a", *fileID, &content); err != nil {
			t.Fatalf("download: %v", err)
		}
		for _, text := range strings.Split(strings.TrimSuffix(content.String(), "\n"), "\n") {
			var line struct {
				CustomID string `json:"custom_id"`
				Error    *struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal([]byte(text), &line); err != nil {
				t.Fatalf("bad line %q: %v", text, err)
			}
			counts[line.CustomID]++
			if line.Error != nil {
				codes[line.CustomID] = line.Error.Code
			}
		}
	}
	return counts, codes
}

// TestOverdueBatchExpires checks a batch past its deadline is expired in one
// pass: unfinished requests (waiting, queued, retrying, and mid-call) become
// expired, their jobs are cancelled, the finished result is kept, and every
// custom_id is in exactly one file.
func TestOverdueBatchExpires(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	ctx := context.Background()
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 8)
	s := newScheduler(t, pool, 4)
	runPass(t, s) // a-0 ... a-3 queued

	finishQueued(t, pool, 1) // a-0 succeeded before the deadline
	exec(t, pool, `INSERT INTO results (batch_id, custom_id, succeeded, response) VALUES ($1, 'a-0', true, '{}')`, batch.ID)
	exec(t, pool, "UPDATE requests SET status = 'running' WHERE custom_id IN ('a-1', 'a-2')")
	exec(t, pool, "UPDATE river_job SET state = 'running', attempt = 1, attempted_at = now() WHERE args->>'custom_id' = 'a-1'")
	exec(t, pool, "UPDATE river_job SET state = 'retryable', attempt = 1, scheduled_at = now() + interval '1 minute' WHERE args->>'custom_id' = 'a-2'")
	passDeadline(t, pool, batch.ID)

	result := runPass(t, s)
	if result.Expired != 1 || result.Admitted != 0 {
		t.Fatalf("pass = %+v, want 1 expired and nothing admitted", result)
	}

	statuses := requestStatuses(t, pool, batch.ID)
	for i := range 8 {
		customID := fmt.Sprintf("a-%d", i)
		want := "expired"
		if i == 0 {
			want = "succeeded"
		}
		if statuses[customID] != want {
			t.Errorf("%s is %s, want %s", customID, statuses[customID], want)
		}
	}
	// Waiting jobs are cancelled outright; the running one is asked to stop.
	var cancelledJobs int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM river_job WHERE state = 'cancelled'").Scan(&cancelledJobs); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if cancelledJobs != 2 {
		t.Errorf("%d jobs cancelled, want 2 (the retrying and the queued one)", cancelledJobs)
	}

	final, err := batches.Get(ctx, pool, "tenant_a", batch.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if final.Status != batches.StatusExpired || final.ExpiredAt == nil || final.OutputFileID == nil || final.ErrorFileID == nil {
		t.Fatalf("batch = %s, expired_at %v, files %v/%v; want expired with both files", final.Status, final.ExpiredAt, final.OutputFileID, final.ErrorFileID)
	}
	counts, codes := errorCodesByCustomID(t, pool, final)
	for i := range 8 {
		customID := fmt.Sprintf("a-%d", i)
		wantCode := "batch_expired"
		if i == 0 {
			wantCode = ""
		}
		if counts[customID] != 1 || codes[customID] != wantCode {
			t.Errorf("%s: %d times, code %q; want once with code %q", customID, counts[customID], codes[customID], wantCode)
		}
	}
}

// TestOverdueValidatingBatchExpires checks a batch that was never fanned out
// still lists every line in its errors file.
func TestOverdueValidatingBatchExpires(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 3)
	passDeadline(t, pool, batch.ID)

	if result := runPass(t, newScheduler(t, pool, 4)); result.Expired != 1 || result.FannedOut != 0 {
		t.Fatalf("pass = %+v, want 1 expired and none fanned out", result)
	}
	final, err := batches.Get(context.Background(), pool, "tenant_a", batch.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	counts, codes := errorCodesByCustomID(t, pool, final)
	if final.Status != batches.StatusExpired || len(counts) != 3 || codes["a-2"] != "batch_expired" {
		t.Errorf("status %s, %d custom_ids, codes %v; want expired with all 3 as batch_expired", final.Status, len(counts), codes)
	}
}

// TestFinishedWorkIsNotExpired checks a batch whose requests all finished is
// completed, not expired, even if its deadline passes before the scheduler
// gets to it.
func TestFinishedWorkIsNotExpired(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 2)
	s := newScheduler(t, pool, 4)
	runPass(t, s)
	finishQueued(t, pool, 2)
	passDeadline(t, pool, batch.ID)

	runPass(t, s)
	runPass(t, s)
	if status := testfixtures.BatchStatus(t, pool, batch.ID); status != batches.StatusCompleted {
		t.Errorf("status = %s, want completed", status)
	}
}

// TestExpiryAndCancelRace runs a cancel and an expiring pass at the same moment
// on an overdue batch, 20 times. Exactly one may win each time.
func TestExpiryAndCancelRace(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	ctx := context.Background()
	s := newScheduler(t, pool, 100)
	riverClient := newRiverClient(t, pool)
	outcomes := map[string]int{}

	for round := range 20 {
		batch := testfixtures.CreateBatch(t, pool, "tenant_a", fmt.Sprintf("r%d", round), 3)
		runPass(t, s)
		passDeadline(t, pool, batch.ID)

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
			t.Fatalf("round %d: pass failed: %v", round, passErr)
		}
		status := testfixtures.BatchStatus(t, pool, batch.ID)
		var notCancellable *cancel.NotCancellableError
		switch {
		case cancelErr == nil && (status == batches.StatusCancelling || status == batches.StatusCancelled):
			outcomes["cancel won"]++
		case errors.As(cancelErr, &notCancellable) && notCancellable.Status == batches.StatusExpired && status == batches.StatusExpired:
			outcomes["expiry won"]++
		default:
			t.Fatalf("round %d: cancel error %v, status %s: not exactly one winner", round, cancelErr, status)
		}
	}
	t.Logf("outcomes over 20 rounds: %v", outcomes)
}

// TestExpiryAndLastResultRace has a worker save a batch's last result right
// around the deadline while the scheduler runs passes. Exactly one may win:
// either the result is saved in time and the batch completes, or the result
// is dropped and the batch expires. Never both, and never a saved result for
// an expired request.
func TestExpiryAndLastResultRace(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	ctx := context.Background()
	s := newScheduler(t, pool, 100)
	outcomes := map[string]int{}

	for round := range 30 {
		batch := testfixtures.CreateBatch(t, pool, "tenant_a", fmt.Sprintf("r%d", round), 1)
		runPass(t, s)
		customID := fmt.Sprintf("r%d-0", round)
		exec(t, pool, "UPDATE requests SET status = 'running' WHERE batch_id = $1", batch.ID)
		// The deadline is 20 ms from now; the save and the passes start at
		// random moments around it.
		exec(t, pool, "UPDATE batches SET expires_at = clock_timestamp() + interval '20 milliseconds' WHERE id = $1", batch.ID)

		var saved bool
		var saveErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(rand.IntN(40)) * time.Millisecond)
			tx, err := pool.Begin(ctx)
			if err != nil {
				saveErr = err
				return
			}
			defer database.RollbackUnlessCommitted(ctx, tx)
			saved, saveErr = requests.Finish(ctx, tx, batch.ID, customID, requests.Outcome{Succeeded: true, Response: []byte(`{}`)})
			if saveErr == nil {
				saveErr = tx.Commit(ctx)
			}
		}()
		go func() {
			defer wg.Done()
			for range 10 {
				if _, err := s.RunPass(ctx); err != nil {
					t.Errorf("round %d: pass failed: %v", round, err)
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()
		wg.Wait()
		if saveErr != nil {
			t.Fatalf("round %d: save failed: %v", round, saveErr)
		}
		runPass(t, s) // let a finished batch reach completed

		status := testfixtures.BatchStatus(t, pool, batch.ID)
		requestStatus := requestStatuses(t, pool, batch.ID)[customID]
		// A kept result must have been saved before the deadline.
		var savedLate bool
		if err := pool.QueryRow(ctx, `
			SELECT coalesce(r.status = 'succeeded' AND r.finished_at >= b.expires_at, false)
			FROM requests r JOIN batches b ON b.id = r.batch_id WHERE b.id = $1`, batch.ID).Scan(&savedLate); err != nil {
			t.Fatalf("check save time: %v", err)
		}
		if savedLate {
			t.Fatalf("round %d: a result saved after the deadline was kept", round)
		}
		var resultRows int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM results WHERE batch_id = $1", batch.ID).Scan(&resultRows); err != nil {
			t.Fatalf("count results: %v", err)
		}
		// A batch that ended completed must never have been expired on the
		// way: expired_at stays unset.
		var everExpired bool
		if err := pool.QueryRow(ctx, "SELECT expired_at IS NOT NULL FROM batches WHERE id = $1", batch.ID).Scan(&everExpired); err != nil {
			t.Fatalf("read expired_at: %v", err)
		}
		if status == batches.StatusCompleted && everExpired {
			t.Fatalf("round %d: batch completed but has an expired_at", round)
		}
		switch {
		case saved && requestStatus == "succeeded" && resultRows == 1 && status == batches.StatusCompleted:
			outcomes["result in time, completed"]++
		case !saved && requestStatus == "expired" && resultRows == 0 && status == batches.StatusExpired:
			outcomes["result late, expired"]++
		default:
			t.Fatalf("round %d: saved %v, request %s, %d results, batch %s: not exactly one winner",
				round, saved, requestStatus, resultRows, status)
		}
	}
	t.Logf("outcomes over 30 rounds: %v", outcomes)
}

// TestUndoneExpiryIsNeverVisible reproduces, step by step, the case where a
// worker's save passes the deadline check just before the deadline but
// commits just after the expiry pass has started. Expiry must undo itself
// inside its own transaction: a watcher reading the batch's status the
// whole time must never see "expired", and the batch must end completed.
func TestUndoneExpiryIsNeverVisible(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	ctx := context.Background()
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "a", 1)
	s := newScheduler(t, pool, 4)
	runPass(t, s)
	exec(t, pool, "UPDATE requests SET status = 'running' WHERE batch_id = $1", batch.ID)
	exec(t, pool, "UPDATE batches SET expires_at = clock_timestamp() + interval '300 milliseconds' WHERE id = $1", batch.ID)

	// 1. The worker saves before the deadline, but does not commit yet. It
	// now holds the request's row lock.
	saveTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin save: %v", err)
	}
	defer database.RollbackUnlessCommitted(ctx, saveTx)
	saved, err := requests.Finish(ctx, saveTx, batch.ID, "a-0", requests.Outcome{Succeeded: true, Response: []byte(`{}`)})
	if err != nil || !saved {
		t.Fatalf("save before the deadline: saved %v, err %v", saved, err)
	}

	// Watch the batch's status from another connection the whole time. The
	// watcher reports back instead of failing the test itself, and is always
	// stopped before the test returns.
	stopWatching := make(chan struct{})
	type watchResult struct {
		sawExpired bool
		err        error
	}
	watched := make(chan watchResult, 1)
	go func() {
		var result watchResult
		defer func() { watched <- result }()
		for {
			select {
			case <-stopWatching:
				return
			default:
			}
			var status batches.Status
			if err := pool.QueryRow(ctx, "SELECT status FROM batches WHERE id = $1", batch.ID).Scan(&status); err != nil {
				result.err = err
				return
			}
			if status == batches.StatusExpired {
				result.sawExpired = true
			}
			time.Sleep(time.Millisecond)
		}
	}()
	watcherStopped := false
	stopWatcher := func() watchResult {
		if !watcherStopped {
			close(stopWatching)
			watcherStopped = true
		}
		return <-watched
	}
	defer func() {
		if !watcherStopped {
			stopWatcher()
		}
	}()

	// 2. After the deadline, an expiry pass starts. It sees the request as
	// running and blocks on the row lock the save holds.
	time.Sleep(400 * time.Millisecond)
	passDone := make(chan error, 1)
	go func() {
		result, err := s.RunPass(ctx)
		if err == nil && result.Expired != 0 {
			err = fmt.Errorf("pass expired %d batches, want 0 (it must undo)", result.Expired)
		}
		passDone <- err
	}()
	time.Sleep(200 * time.Millisecond)

	// 3. The save commits; the pass finds nothing left to expire.
	if err := saveTx.Commit(ctx); err != nil {
		t.Fatalf("commit save: %v", err)
	}
	if err := <-passDone; err != nil {
		t.Fatalf("expiry pass: %v", err)
	}
	runPass(t, s) // to finalizing
	runPass(t, s) // to completed

	watch := stopWatcher()
	if watch.err != nil {
		t.Fatalf("watcher: %v", watch.err)
	}
	if watch.sawExpired {
		t.Error("the watcher saw the batch as expired at some point")
	}
	final, err := batches.Get(ctx, pool, "tenant_a", batch.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if final.Status != batches.StatusCompleted || final.ExpiredAt != nil {
		t.Errorf("batch = %s, expired_at %v; want completed, never expired", final.Status, final.ExpiredAt)
	}
}
