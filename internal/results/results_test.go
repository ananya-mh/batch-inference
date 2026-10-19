// Tests for writing results and errors files, against a real database.
package results_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ananya-mh/batch-inference/internal/database"
	"github.com/ananya-mh/batch-inference/internal/files"
	"github.com/ananya-mh/batch-inference/internal/results"
	"github.com/ananya-mh/batch-inference/internal/testdb"
	"github.com/ananya-mh/batch-inference/internal/testfixtures"
)

// finishedRequest describes one request to set up as already finished.
type finishedRequest struct {
	status    string // succeeded, failed, expired, or cancelled
	errorCode string // for failed
}

// setupFinishedBatch creates a batch whose requests (custom_ids "r-0",
// "r-1", ...) have the given final statuses, with results rows for the
// succeeded and failed ones, as the workers would leave them.
func setupFinishedBatch(t *testing.T, pool *pgxpool.Pool, finished []finishedRequest) string {
	t.Helper()
	ctx := context.Background()
	batch := testfixtures.CreateBatch(t, pool, "tenant_a", "r", len(finished))

	for i, request := range finished {
		customID := fmt.Sprintf("r-%d", i)
		_, err := pool.Exec(ctx, `
			INSERT INTO requests (batch_id, custom_id, tenant_id, line_number, status)
			VALUES ($1, $2, 'tenant_a', $3, $4)`, batch.ID, customID, i+1, request.status)
		if err != nil {
			t.Fatalf("insert request: %v", err)
		}
		switch request.status {
		case "succeeded":
			_, err = pool.Exec(ctx, `
				INSERT INTO results (batch_id, custom_id, succeeded, response)
				VALUES ($1, $2, true, $3)`, batch.ID, customID, fmt.Sprintf(`{"id": "chatcmpl-%d", "choices": []}`, i))
		case "failed":
			_, err = pool.Exec(ctx, `
				INSERT INTO results (batch_id, custom_id, succeeded, error_code, error_message)
				VALUES ($1, $2, false, $3, 'it went wrong')`, batch.ID, customID, request.errorCode)
		}
		if err != nil {
			t.Fatalf("insert result: %v", err)
		}
	}
	return batch.ID
}

// writeFiles runs WriteFiles in a committed transaction.
func writeFiles(t *testing.T, pool *pgxpool.Pool, batchID string) (outputFileID, errorFileID *string) {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer database.RollbackUnlessCommitted(ctx, tx)
	outputFileID, errorFileID, err = results.WriteFiles(ctx, tx, batchID)
	if err != nil {
		t.Fatalf("WriteFiles: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return outputFileID, errorFileID
}

// outputLine is one line of a results or errors file, decoded strictly:
// unknown fields are an error, so the format is checked exactly.
type outputLine struct {
	ID       string `json:"id"`
	CustomID string `json:"custom_id"`
	Response *struct {
		StatusCode int             `json:"status_code"`
		RequestID  string          `json:"request_id"`
		Body       json.RawMessage `json:"body"`
	} `json:"response"`
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// readLines downloads a file and decodes each line, checking that every line
// has exactly the four fields id, custom_id, response, and error.
func readLines(t *testing.T, pool *pgxpool.Pool, fileID *string) []outputLine {
	t.Helper()
	if fileID == nil {
		return nil
	}
	var content bytes.Buffer
	if err := files.WriteContent(context.Background(), pool, "tenant_a", *fileID, &content); err != nil {
		t.Fatalf("download file: %v", err)
	}

	var lines []outputLine
	for _, text := range strings.Split(strings.TrimSuffix(content.String(), "\n"), "\n") {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal([]byte(text), &fields); err != nil {
			t.Fatalf("line is not JSON: %q", text)
		}
		if len(fields) != 4 {
			t.Errorf("line has fields %v, want exactly id, custom_id, response, error", keys(fields))
		}
		decoder := json.NewDecoder(strings.NewReader(text))
		decoder.DisallowUnknownFields()
		var line outputLine
		if err := decoder.Decode(&line); err != nil {
			t.Fatalf("line %q does not match the output format: %v", text, err)
		}
		lines = append(lines, line)
	}
	return lines
}

// keys returns a map's keys, for error messages.
func keys(fields map[string]json.RawMessage) []string {
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	return names
}

// TestEveryRequestInExactlyOneFile checks each custom_id appears exactly once,
// in the results file if it succeeded and in the errors file otherwise, with
// the right content in OpenAI's format.
func TestEveryRequestInExactlyOneFile(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	batchID := setupFinishedBatch(t, pool, []finishedRequest{
		{status: "succeeded"},
		{status: "failed", errorCode: "retries_exhausted"},
		{status: "succeeded"},
		{status: "expired"},
		{status: "cancelled"},
		{status: "failed", errorCode: "model_rejected"},
	})

	outputFileID, errorFileID := writeFiles(t, pool, batchID)
	outputLines := readLines(t, pool, outputFileID)
	errorLines := readLines(t, pool, errorFileID)

	seen := map[string]int{}
	for _, line := range outputLines {
		seen[line.CustomID]++
		if line.Error != nil || line.Response == nil || line.Response.StatusCode != 200 ||
			!strings.HasPrefix(line.ID, "batch_req_") || !strings.HasPrefix(line.Response.RequestID, "req_") ||
			!strings.Contains(string(line.Response.Body), "chatcmpl-") {
			t.Errorf("bad results line: %+v", line)
		}
	}
	wantErrors := map[string]string{
		"r-1": "retries_exhausted",
		"r-3": "batch_expired",
		"r-4": "batch_cancelled",
		"r-5": "model_rejected",
	}
	for _, line := range errorLines {
		seen[line.CustomID]++
		if line.Response != nil || line.Error == nil || line.Error.Code != wantErrors[line.CustomID] || line.Error.Message == "" {
			t.Errorf("bad errors line for %s: %+v", line.CustomID, line)
		}
	}
	if errorLines[1].Error.Message != "This request could not be executed before the completion window expired." {
		t.Errorf("expired message = %q, want OpenAI's wording", errorLines[1].Error.Message)
	}

	if len(outputLines) != 2 || len(errorLines) != 4 {
		t.Errorf("%d results lines and %d errors lines, want 2 and 4", len(outputLines), len(errorLines))
	}
	for i := range 6 {
		customID := fmt.Sprintf("r-%d", i)
		if seen[customID] != 1 {
			t.Errorf("%s appears %d times across both files, want exactly once", customID, seen[customID])
		}
	}
	// Lines keep the input file's order.
	if outputLines[0].CustomID != "r-0" || outputLines[1].CustomID != "r-2" || errorLines[0].CustomID != "r-1" {
		t.Errorf("lines are not in input order")
	}
}

// TestNoErrorsMeansNoErrorFile checks error_file_id is null (nil) when every
// request succeeded, and output_file_id is null when none did.
func TestNoErrorsMeansNoErrorFile(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)

	allGood := setupFinishedBatch(t, pool, []finishedRequest{{status: "succeeded"}, {status: "succeeded"}})
	outputFileID, errorFileID := writeFiles(t, pool, allGood)
	if outputFileID == nil || errorFileID != nil {
		t.Errorf("all succeeded: output %v, error %v; want an output file and no error file", outputFileID, errorFileID)
	}

	allBad := setupFinishedBatch(t, pool, []finishedRequest{{status: "expired"}})
	outputFileID, errorFileID = writeFiles(t, pool, allBad)
	if outputFileID != nil || errorFileID == nil {
		t.Errorf("all expired: output %v, error %v; want no output file and an error file", outputFileID, errorFileID)
	}

	var emptyFiles int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM files WHERE purpose <> 'batch' AND line_count = 0").Scan(&emptyFiles); err != nil {
		t.Fatalf("count empty files: %v", err)
	}
	if emptyFiles != 0 {
		t.Errorf("%d empty files were left behind", emptyFiles)
	}
}

// TestFileSizeIsRecorded checks the file's byte count matches its download.
func TestFileSizeIsRecorded(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	batchID := setupFinishedBatch(t, pool, []finishedRequest{{status: "succeeded"}, {status: "succeeded"}})
	outputFileID, _ := writeFiles(t, pool, batchID)

	file, err := files.Get(context.Background(), pool, "tenant_a", *outputFileID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var content bytes.Buffer
	if err := files.WriteContent(context.Background(), pool, "tenant_a", *outputFileID, &content); err != nil {
		t.Fatalf("WriteContent: %v", err)
	}
	if file.Bytes != int64(content.Len()) || file.LineCount != 2 || file.Purpose != "batch_output" {
		t.Errorf("file = %+v, downloaded %d bytes; want matching size, 2 lines, purpose batch_output", file, content.Len())
	}
}

// TestUnfinishedBatchIsRefused checks files are never written while requests
// are still running, since those would be missing from both files.
func TestUnfinishedBatchIsRefused(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	batchID := setupFinishedBatch(t, pool, []finishedRequest{{status: "succeeded"}, {status: "running"}})

	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer database.RollbackUnlessCommitted(ctx, tx)
	if _, _, err := results.WriteFiles(ctx, tx, batchID); err == nil {
		t.Error("WriteFiles succeeded with a request still running, want an error")
	}
}

// TestWritingAgainGivesSameLines checks two writes of the same batch give
// byte-identical lines, which is what makes redoing it after a crash safe.
func TestWritingAgainGivesSameLines(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	batchID := setupFinishedBatch(t, pool, []finishedRequest{{status: "succeeded"}, {status: "failed", errorCode: "x"}, {status: "succeeded"}})

	contents := make([]string, 2)
	for i := range contents {
		outputFileID, errorFileID := writeFiles(t, pool, batchID)
		var content bytes.Buffer
		for _, fileID := range []*string{outputFileID, errorFileID} {
			if err := files.WriteContent(context.Background(), pool, "tenant_a", *fileID, &content); err != nil {
				t.Fatalf("download: %v", err)
			}
		}
		contents[i] = content.String()
	}
	if contents[0] != contents[1] {
		t.Errorf("second write differs:\n%s\nvs\n%s", contents[0], contents[1])
	}
}
