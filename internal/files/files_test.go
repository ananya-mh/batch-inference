// Tests for storing files and streaming them back, against a real database.
package files

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ananya-mh/batch-inference/internal/tenants"
	"github.com/ananya-mh/batch-inference/internal/testdb"
)

// newPoolWithTenants returns a fresh database with tenant_a and tenant_b.
func newPoolWithTenants(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := testdb.New(t)
	for _, tenantID := range []string{"tenant_a", "tenant_b"} {
		if err := tenants.Create(context.Background(), pool, tenantID, "sk-"+tenantID); err != nil {
			t.Fatalf("create %s: %v", tenantID, err)
		}
	}
	return pool
}

// createInTx runs CreateBatchInput in its own transaction and commits it if
// it succeeds, like the upload handler does.
func createInTx(t *testing.T, pool *pgxpool.Pool, tenantID string, content io.Reader) (File, error) {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			t.Errorf("rollback: %v", err)
		}
	}()

	file, err := CreateBatchInput(ctx, tx, tenantID, "input.jsonl", content)
	if err != nil {
		return File{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return file, nil
}

// countFileLines returns how many line rows are stored in the database.
func countFileLines(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM file_lines").Scan(&count); err != nil {
		t.Fatalf("count file lines: %v", err)
	}
	return count
}

// TestUploadThenDownloadGivesSameContent checks a stored file comes back
// byte for byte, with the right size and line count.
func TestUploadThenDownloadGivesSameContent(t *testing.T) {
	t.Parallel()
	pool := newPoolWithTenants(t)
	content := validLine("a") + "\n" + validLine("b") + "\n"

	file, err := createInTx(t, pool, "tenant_a", strings.NewReader(content))
	if err != nil {
		t.Fatalf("CreateBatchInput: %v", err)
	}
	if file.LineCount != 2 || file.Bytes != int64(len(content)) {
		t.Errorf("file = %d lines, %d bytes; want 2 lines, %d bytes", file.LineCount, file.Bytes, len(content))
	}

	var downloaded bytes.Buffer
	if err := WriteContent(context.Background(), pool, "tenant_a", file.ID, &downloaded); err != nil {
		t.Fatalf("WriteContent: %v", err)
	}
	if downloaded.String() != content {
		t.Errorf("downloaded content differs:\n got: %q\nwant: %q", downloaded.String(), content)
	}
}

// TestUploadAcceptsWindowsLineEndingsAndNoFinalNewline checks common file
// shapes from different editors are accepted.
func TestUploadAcceptsWindowsLineEndingsAndNoFinalNewline(t *testing.T) {
	t.Parallel()
	pool := newPoolWithTenants(t)

	file, err := createInTx(t, pool, "tenant_a", strings.NewReader(validLine("a")+"\r\n"+validLine("b")))
	if err != nil {
		t.Fatalf("CreateBatchInput: %v", err)
	}
	if file.LineCount != 2 {
		t.Errorf("line count = %d, want 2", file.LineCount)
	}
}

// TestInvalidLineStopsUploadAndSavesNothing checks a bad line reports its
// line number and, after rollback, leaves no part of the file behind.
func TestInvalidLineStopsUploadAndSavesNothing(t *testing.T) {
	t.Parallel()
	pool := newPoolWithTenants(t)
	content := validLine("a") + "\n" + validLine("b") + "\n" + validLine("a") + "\n"

	_, err := createInTx(t, pool, "tenant_a", strings.NewReader(content))
	var lineErr *LineError
	if !errors.As(err, &lineErr) || lineErr.Line != 3 {
		t.Fatalf("error = %v, want a LineError for line 3", err)
	}
	if count := countFileLines(t, pool); count != 0 {
		t.Errorf("%d lines were saved, want 0", count)
	}
}

// TestTooLongLineIsRejected checks the per-line size limit.
func TestTooLongLineIsRejected(t *testing.T) {
	t.Parallel()
	pool := newPoolWithTenants(t)
	content := validLine("a") + "\n" + strings.Repeat("x", MaxLineBytes+1) + "\n"

	_, err := createInTx(t, pool, "tenant_a", strings.NewReader(content))
	var lineErr *LineError
	if !errors.As(err, &lineErr) || lineErr.Line != 2 {
		t.Fatalf("error = %v, want a LineError for line 2", err)
	}
}

// TestReadErrorMidLineIsReportedAsReadError checks that when reading stops
// with an error in the middle of a line, the read error is reported, not a
// misleading "invalid JSON" for the cut-off line.
func TestReadErrorMidLineIsReportedAsReadError(t *testing.T) {
	t.Parallel()
	pool := newPoolWithTenants(t)
	readFailure := errors.New("connection reset")
	content := validLine("a") + "\n" + validLine("b")[:30]
	reader := io.MultiReader(strings.NewReader(content), &failingReader{err: readFailure})

	_, err := createInTx(t, pool, "tenant_a", reader)
	if !errors.Is(err, readFailure) {
		t.Errorf("error = %v, want the read error", err)
	}
}

// failingReader always fails with err.
type failingReader struct {
	err error
}

// Read returns the reader's error.
func (f *failingReader) Read([]byte) (int, error) {
	return 0, f.err
}

// TestEmptyFileIsRejected checks a file with no lines is refused.
func TestEmptyFileIsRejected(t *testing.T) {
	t.Parallel()
	pool := newPoolWithTenants(t)

	if _, err := createInTx(t, pool, "tenant_a", strings.NewReader("")); !errors.Is(err, ErrEmptyFile) {
		t.Errorf("error = %v, want ErrEmptyFile", err)
	}
}

// TestLargeFileStreamsThrough checks a file the size of the experiment's big
// batch (50,000 lines) is stored. The content comes from a generator through
// a pipe, so it never exists in memory as a whole.
func TestLargeFileStreamsThrough(t *testing.T) {
	t.Parallel()
	pool := newPoolWithTenants(t)
	const lineCount = 50_000

	reader, writer := io.Pipe()
	// PipeWriter's Close and CloseWithError always return nil, so there is
	// no error to check from them below.
	go func() {
		for i := range lineCount {
			if _, err := fmt.Fprintln(writer, validLine(fmt.Sprintf("ticket-%d", i))); err != nil {
				writer.CloseWithError(err)
				return
			}
		}
		writer.Close()
	}()

	file, err := createInTx(t, pool, "tenant_a", reader)
	if err != nil {
		t.Fatalf("CreateBatchInput: %v", err)
	}
	if file.LineCount != lineCount {
		t.Errorf("line count = %d, want %d", file.LineCount, lineCount)
	}
}

// TestOtherTenantCannotSeeFile checks tenant isolation at the storage level:
// tenant_b gets ErrNotFound for tenant_a's file and downloads nothing.
func TestOtherTenantCannotSeeFile(t *testing.T) {
	t.Parallel()
	pool := newPoolWithTenants(t)
	ctx := context.Background()

	file, err := createInTx(t, pool, "tenant_a", strings.NewReader(validLine("a")+"\n"))
	if err != nil {
		t.Fatalf("CreateBatchInput: %v", err)
	}

	if _, err := Get(ctx, pool, "tenant_b", file.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get as tenant_b: error = %v, want ErrNotFound", err)
	}
	var downloaded bytes.Buffer
	if err := WriteContent(ctx, pool, "tenant_b", file.ID, &downloaded); err != nil {
		t.Fatalf("WriteContent as tenant_b: %v", err)
	}
	if downloaded.Len() != 0 {
		t.Errorf("tenant_b downloaded %d bytes of tenant_a's file, want 0", downloaded.Len())
	}
}
