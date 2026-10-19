// Package results writes a finished batch's results file and errors file, in
// OpenAI's batch output format.
package results

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ananya-mh/batch-inference/internal/files"
	"github.com/ananya-mh/batch-inference/internal/requests"
)

// Messages for requests that never ran, as OpenAI words them. They are put
// into the SQL as literals, so they must not contain a single quote.
const (
	expiredMessage   = "This request could not be executed before the completion window expired."
	cancelledMessage = "This request was not executed because the batch was cancelled."
)

// lineID is the SQL for a request's line ID in both files. It is worked out
// from the batch and custom_id rather than random, so writing the files
// again (after a crash) gives exactly the same lines.
const lineID = `'batch_req_' || md5(r.batch_id || '/' || r.custom_id)`

// outputLineSQL builds one results-file line for a succeeded request:
// {"id": ..., "custom_id": ..., "response": {"status_code": 200,
// "request_id": ..., "body": <model reply>}, "error": null}
const outputLineSQL = `jsonb_build_object(
	'id', ` + lineID + `,
	'custom_id', r.custom_id,
	'response', jsonb_build_object(
		'status_code', 200,
		'request_id', 'req_' || md5(r.batch_id || '/' || r.custom_id),
		'body', res.response),
	'error', NULL)::text`

// errorLineSQL builds one errors-file line for a request that did not
// succeed: {"id": ..., "custom_id": ..., "response": null,
// "error": {"code": ..., "message": ...}}. Expired and cancelled requests
// have no result row; their reason comes from their status.
const errorLineSQL = `jsonb_build_object(
	'id', ` + lineID + `,
	'custom_id', r.custom_id,
	'response', NULL,
	'error', jsonb_build_object(
		'code', CASE r.status
			WHEN 'expired' THEN 'batch_expired'
			WHEN 'cancelled' THEN 'batch_cancelled'
			ELSE coalesce(res.error_code, 'request_failed') END,
		'message', CASE r.status
			WHEN 'expired' THEN '` + expiredMessage + `'
			WHEN 'cancelled' THEN '` + cancelledMessage + `'
			ELSE coalesce(res.error_message, '') END))::text`

// WriteFiles writes a batch's results file (succeeded requests) and errors
// file (failed, expired, and cancelled requests) inside tx, and returns their
// IDs. An ID is nil when that file would be empty, so error_file_id is null
// when nothing failed, as with OpenAI.
//
// Every request goes in exactly one of the two files, in input-line order.
// The lines are built and copied inside Postgres with INSERT ... SELECT, so
// no file passes through this server's memory. The caller commits tx.
func WriteFiles(ctx context.Context, tx pgx.Tx, batchID string) (outputFileID, errorFileID *string, err error) {
	var tenantID string
	var unfinished int
	err = tx.QueryRow(ctx, `
		SELECT b.tenant_id,
		       (SELECT count(*) FROM requests r WHERE r.batch_id = b.id AND r.status IN ($2, $3, $4))
		FROM batches b WHERE b.id = $1`,
		batchID, requests.StatusPending, requests.StatusQueued, requests.StatusRunning,
	).Scan(&tenantID, &unfinished)
	if err != nil {
		return nil, nil, fmt.Errorf("read batch %s: %w", batchID, err)
	}
	// Writing now would leave those requests out of both files.
	if unfinished > 0 {
		return nil, nil, fmt.Errorf("batch %s still has %d unfinished requests", batchID, unfinished)
	}

	outputFileID, err = writeFile(ctx, tx, batchID, tenantID, files.PurposeBatchOutput, outputLineSQL,
		"r.status = 'succeeded'")
	if err != nil {
		return nil, nil, err
	}
	errorFileID, err = writeFile(ctx, tx, batchID, tenantID, files.PurposeBatchError, errorLineSQL,
		"r.status IN ('failed', 'expired', 'cancelled')")
	if err != nil {
		return nil, nil, err
	}
	return outputFileID, errorFileID, nil
}

// writeFile creates one file from the batch's requests that match where,
// building each line with lineSQL. It returns nil if no request matched.
func writeFile(ctx context.Context, tx pgx.Tx, batchID, tenantID, purpose, lineSQL, where string) (*string, error) {
	fileID := files.NewFileID()
	_, err := tx.Exec(ctx,
		"INSERT INTO files (id, tenant_id, purpose, filename) VALUES ($1, $2, $3, $4)",
		fileID, tenantID, purpose, batchID+"_"+purpose+".jsonl",
	)
	if err != nil {
		return nil, fmt.Errorf("create %s file: %w", purpose, err)
	}

	// lineSQL and where are constants from this file, never input, so
	// building the SQL text from them is safe. Line numbers follow the
	// input file's order.
	tag, err := tx.Exec(ctx, `
		INSERT INTO file_lines (file_id, line_number, custom_id, line)
		SELECT $1, row_number() OVER (ORDER BY r.line_number), r.custom_id, `+lineSQL+`
		FROM requests r
		LEFT JOIN results res ON res.batch_id = r.batch_id AND res.custom_id = r.custom_id
		WHERE r.batch_id = $2 AND `+where,
		fileID, batchID,
	)
	if err != nil {
		return nil, fmt.Errorf("write %s lines: %w", purpose, err)
	}

	if tag.RowsAffected() == 0 {
		if _, err := tx.Exec(ctx, "DELETE FROM files WHERE id = $1", fileID); err != nil {
			return nil, fmt.Errorf("remove empty %s file: %w", purpose, err)
		}
		return nil, nil
	}

	// Each line is downloaded with a newline after it, hence the + 1.
	_, err = tx.Exec(ctx, `
		UPDATE files SET
			line_count = $2,
			bytes = (SELECT sum(octet_length(line) + 1) FROM file_lines WHERE file_id = $1)
		WHERE id = $1`,
		fileID, tag.RowsAffected(),
	)
	if err != nil {
		return nil, fmt.Errorf("record %s file size: %w", purpose, err)
	}
	return &fileID, nil
}
