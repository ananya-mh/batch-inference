// Package batches creates batches, looks them up, lists them, and changes
// their status through compare-and-set updates.
package batches

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound means the batch does not exist or belongs to another tenant.
// Both look the same, so a tenant cannot learn other tenants' batch IDs.
var ErrNotFound = errors.New("batch not found")

// ErrInvalidTransition means the state machine does not allow the change.
var ErrInvalidTransition = errors.New("status change not allowed")

// ErrStatusChanged means the batch was no longer in the expected status when
// we tried to change it: something else changed it first.
var ErrStatusChanged = errors.New("batch status changed by someone else")

// DB is what this package needs from the database. Both a pool and a
// transaction fit, so callers can change status inside a larger transaction.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// RequestCounts says how many of a batch's requests are done.
type RequestCounts struct {
	Total     int
	Completed int
	Failed    int
}

// Batch is a stored batch.
type Batch struct {
	ID               string
	TenantID         string
	InputFileID      string
	Endpoint         string
	CompletionWindow string
	Status           Status
	OutputFileID     *string // nil until the results file exists
	ErrorFileID      *string // nil until the errors file exists
	Metadata         map[string]string
	RequestCounts    RequestCounts

	CreatedAt    time.Time
	ExpiresAt    time.Time
	InProgressAt *time.Time // each is nil until the batch reaches that status
	FinalizingAt *time.Time
	CompletedAt  *time.Time
	FailedAt     *time.Time
	ExpiredAt    *time.Time
	CancellingAt *time.Time
	CancelledAt  *time.Time
}

// NewBatch is what is needed to create a batch. The caller has already
// checked the input file, endpoint, and window.
type NewBatch struct {
	TenantID         string
	InputFileID      string
	Endpoint         string
	CompletionWindow string
	WindowLength     time.Duration
	Metadata         map[string]string
	RequestTotal     int
}

// selectBatches is the start of every query that reads batches, giving the
// columns in the order scanBatch expects. Queries add their own WHERE.
//
// Completed and failed counts are counted from the requests table rather
// than kept as counters on the batch row, so workers never all update one
// row, and the counts can never drift from the truth. The requests index on
// (batch_id, status) makes this a quick index-only count.
const selectBatches = `
	SELECT b.id, b.tenant_id, b.input_file_id, b.endpoint, b.completion_window, b.status,
		b.output_file_id, b.error_file_id, b.metadata,
		b.request_total, counts.completed, counts.failed,
		b.created_at, b.expires_at, b.in_progress_at, b.finalizing_at, b.completed_at,
		b.failed_at, b.expired_at, b.cancelling_at, b.cancelled_at
	FROM batches b
	CROSS JOIN LATERAL (
		SELECT count(*) FILTER (WHERE r.status = 'succeeded') AS completed,
		       count(*) FILTER (WHERE r.status = 'failed') AS failed
		FROM requests r
		WHERE r.batch_id = b.id
	) counts`

// scanBatch reads one row of selectBatches into a Batch.
func scanBatch(row pgx.Row) (Batch, error) {
	var batch Batch
	err := row.Scan(
		&batch.ID, &batch.TenantID, &batch.InputFileID, &batch.Endpoint, &batch.CompletionWindow, &batch.Status,
		&batch.OutputFileID, &batch.ErrorFileID, &batch.Metadata,
		&batch.RequestCounts.Total, &batch.RequestCounts.Completed, &batch.RequestCounts.Failed,
		&batch.CreatedAt, &batch.ExpiresAt, &batch.InProgressAt, &batch.FinalizingAt, &batch.CompletedAt,
		&batch.FailedAt, &batch.ExpiredAt, &batch.CancellingAt, &batch.CancelledAt,
	)
	return batch, err
}

// Create stores a new batch in the validating status and returns it.
func Create(ctx context.Context, db DB, newBatch NewBatch) (Batch, error) {
	// The expiry time is worked out by the database from its own clock,
	// the same clock the expiry check will later compare against.
	batchID := newBatchID()
	_, err := db.Exec(ctx, `
		INSERT INTO batches (
			id, tenant_id, input_file_id, endpoint, completion_window, status,
			metadata, request_total, expires_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now() + make_interval(secs => $9))`,
		batchID, newBatch.TenantID, newBatch.InputFileID, newBatch.Endpoint, newBatch.CompletionWindow,
		StatusValidating, newBatch.Metadata, newBatch.RequestTotal, newBatch.WindowLength.Seconds(),
	)
	if err != nil {
		return Batch{}, fmt.Errorf("insert batch: %w", err)
	}
	return Get(ctx, db, newBatch.TenantID, batchID)
}

// Get returns a tenant's batch, or ErrNotFound.
func Get(ctx context.Context, db DB, tenantID, batchID string) (Batch, error) {
	row := db.QueryRow(ctx,
		selectBatches+" WHERE b.id = $1 AND b.tenant_id = $2",
		batchID, tenantID,
	)
	batch, err := scanBatch(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Batch{}, ErrNotFound
	}
	if err != nil {
		return Batch{}, fmt.Errorf("get batch %s: %w", batchID, err)
	}
	return batch, nil
}

// Page is one page of a batch list.
type Page struct {
	Batches []Batch
	HasMore bool
}

// List returns up to limit of a tenant's batches, newest first. If afterID
// is set, the page starts just after that batch, which is how OpenAI-style
// cursor paging works: the client passes the last ID it saw. An afterID that
// is not one of the tenant's batches gives ErrNotFound.
func List(ctx context.Context, db DB, tenantID string, limit int, afterID string) (Page, error) {
	// Paging uses (created_at, id) rather than an offset, so batches created
	// while a client is paging do not shift pages and cause repeats or gaps.
	// The id breaks ties between batches created at the same instant.
	var rows pgx.Rows
	var err error
	if afterID == "" {
		rows, err = db.Query(ctx, selectBatches+`
			WHERE b.tenant_id = $1
			ORDER BY b.created_at DESC, b.id DESC
			LIMIT $2`,
			tenantID, limit+1,
		)
	} else {
		var cursorCreatedAt time.Time
		err = db.QueryRow(ctx,
			"SELECT created_at FROM batches WHERE id = $1 AND tenant_id = $2", afterID, tenantID,
		).Scan(&cursorCreatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return Page{}, ErrNotFound
		}
		if err != nil {
			return Page{}, fmt.Errorf("look up list cursor: %w", err)
		}
		rows, err = db.Query(ctx, selectBatches+`
			WHERE b.tenant_id = $1 AND (b.created_at, b.id) < ($2, $3)
			ORDER BY b.created_at DESC, b.id DESC
			LIMIT $4`,
			tenantID, cursorCreatedAt, afterID, limit+1,
		)
	}
	if err != nil {
		return Page{}, fmt.Errorf("list batches: %w", err)
	}

	batches, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Batch, error) {
		return scanBatch(row)
	})
	if err != nil {
		return Page{}, fmt.Errorf("read batch list: %w", err)
	}

	// One extra row was asked for, only to learn whether another page exists.
	page := Page{Batches: batches}
	if len(batches) > limit {
		page.Batches = batches[:limit]
		page.HasMore = true
	}
	return page, nil
}

// Transition moves a batch from one status to another and records when.
//
// It is a compare-and-set: the update only happens if the batch is still in
// status from. If something else changed the batch first, nothing is changed
// and ErrStatusChanged is returned, so of two racing changes only one can
// win. A change the state machine does not allow returns
// ErrInvalidTransition without touching the database.
func Transition(ctx context.Context, db DB, batchID string, from, to Status) error {
	if !CanTransition(from, to) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
	}

	// The column name comes from our own fixed map, never from input, so
	// putting it into the SQL text is safe. clock_timestamp() is the time
	// of this change; now() would be when the transaction began, which for
	// completing a batch is before its files were written.
	column := timestampColumn[to]
	tag, err := db.Exec(ctx,
		"UPDATE batches SET status = $1, "+column+" = clock_timestamp() WHERE id = $2 AND status = $3",
		to, batchID, from,
	)
	if err != nil {
		return fmt.Errorf("update batch %s status: %w", batchID, err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	// Nothing changed: either the batch is gone or its status moved on.
	// Look up which, to give a useful error.
	var current Status
	err = db.QueryRow(ctx, "SELECT status FROM batches WHERE id = $1", batchID).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("check batch %s status: %w", batchID, err)
	}
	return fmt.Errorf("%w: wanted %s, but it is %s", ErrStatusChanged, from, current)
}

// newBatchID returns a new random ID like "batch_k3j...".
func newBatchID() string {
	return "batch_" + strings.ToLower(cryptorand.Text())
}

// compile-time checks that both a pool and a transaction can be used as DB.
var (
	_ DB = (*pgxpool.Pool)(nil)
	_ DB = (pgx.Tx)(nil)
)
