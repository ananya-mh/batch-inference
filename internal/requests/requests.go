// Package requests holds the per-line requests of a batch: their statuses,
// the worker's start check, and saving their results. See "Fan-out and fair
// scheduling" in DESIGN.md.
package requests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ananya-mh/batch-inference/internal/batches"
)

// Status is where a request is in its life.
type Status string

// The request statuses. "In flight" means queued or running.
const (
	StatusPending   Status = "pending"   // waiting in our table, no River job yet
	StatusQueued    Status = "queued"    // has a River job, not started
	StatusRunning   Status = "running"   // a worker passed the start check
	StatusSucceeded Status = "succeeded" // final: has a successful result
	StatusFailed    Status = "failed"    // final: has a failed result
	StatusCancelled Status = "cancelled" // final: its batch was cancelled first
	StatusExpired   Status = "expired"   // final: its batch expired first
)

// RunArgs are the arguments of the River job that runs one request. They
// only name the request; everything else is read from the database, so a
// job never carries stale data.
type RunArgs struct {
	BatchID  string `json:"batch_id"`
	CustomID string `json:"custom_id"`
}

// Kind is the River job type name.
func (RunArgs) Kind() string { return "run_request" }

// Outcome is how a request ended.
type Outcome struct {
	Succeeded bool

	// Response is the model's full reply, when Succeeded.
	Response json.RawMessage

	// ErrorCode and ErrorMessage say why it failed, when not Succeeded.
	ErrorCode    string
	ErrorMessage string
}

// CreateForBatch creates one request row per line of the batch's input file,
// all with the given status: pending for a normal fan-out, or cancelled when
// a batch is cancelled before it was fanned out (so its lines still appear in
// the errors file). One statement for the whole batch; ORDER BY makes seq
// follow line order, so a batch's requests run in file order.
func CreateForBatch(ctx context.Context, tx pgx.Tx, batchID string, status Status) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO requests (batch_id, custom_id, tenant_id, line_number, status, finished_at)
		SELECT b.id, l.custom_id, b.tenant_id, l.line_number, $2::text,
		       CASE WHEN $2::text = $3::text THEN NULL ELSE clock_timestamp() END
		FROM batches b
		JOIN file_lines l ON l.file_id = b.input_file_id
		WHERE b.id = $1
		ORDER BY l.line_number`,
		batchID, status, StatusPending,
	)
	if err != nil {
		return fmt.Errorf("create requests for %s: %w", batchID, err)
	}
	return nil
}

// Start is the worker's start check, run inside tx. If the request may run,
// it moves it to running and returns its request body with started = true.
// It returns started = false when the request must not run: its batch is no
// longer in progress (cancelled or expired), its deadline has passed, or it
// has already finished.
func Start(ctx context.Context, tx pgx.Tx, batchID, customID string) (body json.RawMessage, started bool, err error) {
	// FOR SHARE locks the batch row against status changes until tx ends.
	// Cancel has to wait for start checks happening right now, and any
	// start check after cancel commits sees the new status, so once cancel
	// returns, no request can start (guarantee 3).
	//
	// The deadline is compared with the database's clock, not this server's,
	// so every server agrees on it.
	var batchStatus batches.Status
	var overdue bool
	err = tx.QueryRow(ctx,
		"SELECT status, expires_at <= clock_timestamp() FROM batches WHERE id = $1 FOR SHARE", batchID,
	).Scan(&batchStatus, &overdue)
	if err != nil {
		return nil, false, fmt.Errorf("read batch %s status: %w", batchID, err)
	}
	if batchStatus == batches.StatusCancelling {
		// The batch was cancelled after this job was queued (or while it
		// waited for a retry). Mark the request cancelled so it frees its
		// slot and lands in the errors file, and don't run it.
		if err := cancelRequest(ctx, tx, batchID, customID); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	// Past the deadline nothing new starts, even before the scheduler's next
	// pass marks the batch expired; that pass will expire this request.
	if batchStatus != batches.StatusInProgress || overdue {
		return nil, false, nil
	}

	// running is accepted as well as queued: if an earlier attempt's worker
	// crashed, River retries the job but the request still says running.
	var lineNumber int
	err = tx.QueryRow(ctx, `
		UPDATE requests SET status = $3, started_at = clock_timestamp()
		WHERE batch_id = $1 AND custom_id = $2 AND status IN ($4, $3)
		RETURNING line_number`,
		batchID, customID, StatusRunning, StatusQueued,
	).Scan(&lineNumber)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("mark request %s/%s running: %w", batchID, customID, err)
	}

	body, err = readRequestBody(ctx, tx, batchID, lineNumber)
	if err != nil {
		return nil, false, err
	}
	return body, true, nil
}

// CancelIfBatchCancelling cancels the request if its batch is cancelling,
// and reports whether it did. Workers call it when an attempt fails: a
// request whose batch is being cancelled should not wait for a retry, since
// that would only hold up the cancel.
func CancelIfBatchCancelling(ctx context.Context, tx pgx.Tx, batchID, customID string) (bool, error) {
	var batchStatus batches.Status
	if err := tx.QueryRow(ctx, "SELECT status FROM batches WHERE id = $1", batchID).Scan(&batchStatus); err != nil {
		return false, fmt.Errorf("read batch %s status: %w", batchID, err)
	}
	if batchStatus != batches.StatusCancelling {
		return false, nil
	}
	if err := cancelRequest(ctx, tx, batchID, customID); err != nil {
		return false, err
	}
	return true, nil
}

// cancelRequest marks a request cancelled if it has not finished yet. A
// request that already finished keeps its result.
func cancelRequest(ctx context.Context, tx pgx.Tx, batchID, customID string) error {
	_, err := tx.Exec(ctx, `
		UPDATE requests SET status = $3, finished_at = clock_timestamp()
		WHERE batch_id = $1 AND custom_id = $2 AND status IN ($4, $5, $6)`,
		batchID, customID, StatusCancelled, StatusPending, StatusQueued, StatusRunning,
	)
	if err != nil {
		return fmt.Errorf("cancel request %s/%s: %w", batchID, customID, err)
	}
	return nil
}

// readRequestBody returns the "body" part of a batch's input line, which is
// what gets sent to the model.
func readRequestBody(ctx context.Context, tx pgx.Tx, batchID string, lineNumber int) (json.RawMessage, error) {
	var line string
	err := tx.QueryRow(ctx, `
		SELECT l.line
		FROM file_lines l
		JOIN batches b ON b.input_file_id = l.file_id
		WHERE b.id = $1 AND l.line_number = $2`,
		batchID, lineNumber,
	).Scan(&line)
	if err != nil {
		return nil, fmt.Errorf("read input line %d of batch %s: %w", lineNumber, batchID, err)
	}

	var parsed struct {
		Body json.RawMessage `json:"body"`
	}
	if err := json.Unmarshal([]byte(line), &parsed); err != nil {
		// Lines were checked on upload, so this would mean stored data is
		// damaged.
		return nil, fmt.Errorf("parse input line %d of batch %s: %w", lineNumber, batchID, err)
	}
	return parsed.Body, nil
}

// Finish saves a running request's outcome inside tx. saved is false when the
// request was no longer running (another attempt already finished it, or its
// batch expired), or when the batch's deadline has passed: a late result is
// dropped, and the scheduler's next pass will expire the request. Then
// nothing is saved.
func Finish(ctx context.Context, tx pgx.Tx, batchID, customID string, outcome Outcome) (saved bool, err error) {
	return finishFrom(ctx, tx, batchID, customID, outcome, StatusRunning)
}

// GiveUp saves a failed outcome for a request that is still in flight
// (queued or running) but whose River job has ended without saving anything,
// for example because the worker crashed on every attempt. Like Finish, it
// saves nothing once the deadline has passed; the request will expire.
func GiveUp(ctx context.Context, tx pgx.Tx, batchID, customID string, outcome Outcome) (saved bool, err error) {
	return finishFrom(ctx, tx, batchID, customID, outcome, StatusQueued, StatusRunning)
}

// finishFrom moves a request from one of the given statuses to succeeded or
// failed, and saves its result, inside tx.
func finishFrom(ctx context.Context, tx pgx.Tx, batchID, customID string, outcome Outcome, from ...Status) (bool, error) {
	finalStatus := StatusFailed
	if outcome.Succeeded {
		finalStatus = StatusSucceeded
	}
	fromStatuses := make([]string, len(from))
	for i, status := range from {
		fromStatuses[i] = string(status)
	}

	// Change the status first, with compare-and-set. Only one attempt can
	// move the request out of running, and expiry moves it to expired the
	// same way, so a request ends up finished or expired, never both. The
	// row lock taken here makes a racing attempt wait, then find the
	// status changed and save nothing (guarantee 2).
	//
	// The deadline check uses the database's clock: a result that arrives
	// after expires_at is late and dropped, even if the expiry step has not
	// run yet (guarantee 5).
	tag, err := tx.Exec(ctx, `
		UPDATE requests SET status = $3, finished_at = clock_timestamp()
		WHERE batch_id = $1 AND custom_id = $2 AND status = ANY($4)
		  AND EXISTS (SELECT 1 FROM batches WHERE id = $1 AND expires_at > clock_timestamp())`,
		batchID, customID, finalStatus, fromStatuses,
	)
	if err != nil {
		return false, fmt.Errorf("mark request %s/%s %s: %w", batchID, customID, finalStatus, err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}

	// A plain INSERT, not ON CONFLICT DO NOTHING: after the check above, an
	// existing result would mean a bug, and failing loudly is better than
	// hiding it. The primary key still guarantees no second result.
	_, err = tx.Exec(ctx, `
		INSERT INTO results (batch_id, custom_id, succeeded, response, error_code, error_message)
		VALUES ($1, $2, $3, $4, NULLIF($5, ''), NULLIF($6, ''))`,
		batchID, customID, outcome.Succeeded, outcome.Response, outcome.ErrorCode, outcome.ErrorMessage,
	)
	if err != nil {
		return false, fmt.Errorf("save result for %s/%s: %w", batchID, customID, err)
	}
	return true, nil
}
