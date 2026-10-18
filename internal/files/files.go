// Package files stores uploaded batch files and streams them back out.
package files

import (
	"bufio"
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PurposeBatch is the purpose of a file uploaded as batch input.
const PurposeBatch = "batch"

// MaxLineBytes is the longest line we accept. A line holds one request, so
// this is far more than any real prompt needs.
const MaxLineBytes = 10 << 20

// ErrNotFound means the file does not exist or belongs to another tenant.
// Both cases look the same to callers, so a tenant cannot learn which file
// IDs other tenants have.
var ErrNotFound = errors.New("file not found")

// ErrEmptyFile means an upload had no lines.
var ErrEmptyFile = errors.New("file has no lines")

// File describes a stored file.
type File struct {
	ID        string
	TenantID  string
	Purpose   string
	Filename  string
	Bytes     int64
	LineCount int
	CreatedAt time.Time
}

// CreateBatchInput reads a batch input file from content, checks every line,
// and stores it inside tx. It streams: only the current line and the set of
// custom_ids seen so far are held in memory. The caller must commit tx; if
// any line is invalid, a *LineError is returned and the caller should roll
// back so nothing is saved.
func CreateBatchInput(ctx context.Context, tx pgx.Tx, tenantID, filename string, content io.Reader) (File, error) {
	file := File{
		ID:       newFileID(),
		TenantID: tenantID,
		Purpose:  PurposeBatch,
		Filename: filename,
	}
	// Insert the file row first, because each line row points to it. Size
	// and line count are filled in once the whole file has been read.
	err := tx.QueryRow(ctx,
		"INSERT INTO files (id, tenant_id, purpose, filename) VALUES ($1, $2, $3, $4) RETURNING created_at",
		file.ID, file.TenantID, file.Purpose, file.Filename,
	).Scan(&file.CreatedAt)
	if err != nil {
		return File{}, fmt.Errorf("insert file: %w", err)
	}

	source := newLineSource(file.ID, content)
	// COPY sends rows to Postgres as they are read, instead of one INSERT
	// per line, which matters for files with tens of thousands of lines.
	_, err = tx.CopyFrom(ctx,
		pgx.Identifier{"file_lines"},
		[]string{"file_id", "line_number", "custom_id", "line"},
		source,
	)
	// pgx reports a failed source as a generic COPY error, so check the
	// source's own error first to keep the line number and the cause.
	if source.err != nil {
		return File{}, source.err
	}
	if err != nil {
		return File{}, fmt.Errorf("store file lines: %w", err)
	}
	if source.lineNumber == 0 {
		return File{}, ErrEmptyFile
	}

	file.Bytes = source.reader.bytesRead
	file.LineCount = source.lineNumber
	if _, err := tx.Exec(ctx,
		"UPDATE files SET bytes = $1, line_count = $2 WHERE id = $3",
		file.Bytes, file.LineCount, file.ID,
	); err != nil {
		return File{}, fmt.Errorf("update file size: %w", err)
	}
	return file, nil
}

// Get returns a file's details, or ErrNotFound if it does not exist or
// belongs to another tenant.
func Get(ctx context.Context, pool *pgxpool.Pool, tenantID, fileID string) (File, error) {
	var file File
	err := pool.QueryRow(ctx, `
		SELECT id, tenant_id, purpose, filename, bytes, line_count, created_at
		FROM files
		WHERE id = $1 AND tenant_id = $2`,
		fileID, tenantID,
	).Scan(&file.ID, &file.TenantID, &file.Purpose, &file.Filename, &file.Bytes, &file.LineCount, &file.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return File{}, ErrNotFound
	}
	if err != nil {
		return File{}, fmt.Errorf("get file %s: %w", fileID, err)
	}
	return file, nil
}

// WriteContent writes a file's lines to w in order, one per line, streaming
// rows from the database rather than loading the whole file.
func WriteContent(ctx context.Context, pool *pgxpool.Pool, tenantID, fileID string, w io.Writer) error {
	// Joining on files.tenant_id repeats the ownership check, so this
	// function is safe even if a caller forgets to call Get first.
	rows, err := pool.Query(ctx, `
		SELECT l.line
		FROM file_lines l
		JOIN files f ON f.id = l.file_id
		WHERE l.file_id = $1 AND f.tenant_id = $2
		ORDER BY l.line_number`,
		fileID, tenantID,
	)
	if err != nil {
		return fmt.Errorf("query file lines: %w", err)
	}
	defer rows.Close()

	buffered := bufio.NewWriter(w)
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return fmt.Errorf("read file line: %w", err)
		}
		if _, err := buffered.WriteString(line + "\n"); err != nil {
			return fmt.Errorf("write file line: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read file lines: %w", err)
	}
	if err := buffered.Flush(); err != nil {
		return fmt.Errorf("write file content: %w", err)
	}
	return nil
}

// lineSource feeds pgx's CopyFrom one checked line at a time. It stops at
// the first invalid line and keeps the reason in err.
type lineSource struct {
	fileID     string
	reader     *countingReader
	scanner    *bufio.Scanner
	validator  *Validator
	lineNumber int
	row        []any
	err        error
}

// newLineSource returns a lineSource that reads lines from content.
func newLineSource(fileID string, content io.Reader) *lineSource {
	reader := &countingReader{reader: content}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), MaxLineBytes)
	return &lineSource{fileID: fileID, reader: reader, scanner: scanner, validator: NewValidator()}
}

// Next reads and checks the next line. It returns false at the end of the
// file or at the first problem.
func (s *lineSource) Next() bool {
	if !s.scanner.Scan() {
		err := s.scanner.Err()
		if errors.Is(err, bufio.ErrTooLong) {
			s.err = &LineError{Line: s.lineNumber + 1, Reason: fmt.Sprintf("line is longer than %d MB", MaxLineBytes>>20)}
		} else if err != nil {
			s.err = fmt.Errorf("read upload: %w", err)
		}
		return false
	}
	s.lineNumber++

	// Accept Windows line endings, which editors on Windows produce.
	line := bytes.TrimSuffix(s.scanner.Bytes(), []byte("\r"))
	customID, err := s.validator.CheckLine(s.lineNumber, line)
	if err != nil {
		// When reading fails mid-line (say, the upload passed the size
		// limit), the scanner still hands us the cut-off piece of line
		// before it reports the read error. The piece is bound to be
		// invalid, so report the read error, which is the real cause.
		if s.reader.err != nil {
			s.err = fmt.Errorf("read upload: %w", s.reader.err)
			return false
		}
		s.err = err
		return false
	}
	// string() copies the bytes, which is needed because the scanner
	// reuses its buffer for the next line.
	s.row = []any{s.fileID, s.lineNumber, customID, string(line)}
	return true
}

// Values returns the current line as a database row.
func (s *lineSource) Values() ([]any, error) {
	return s.row, nil
}

// Err returns the reason reading stopped early, if any.
func (s *lineSource) Err() error {
	return s.err
}

// countingReader counts the bytes read through it, to record the file size
// without buffering the file. It also remembers the first read error other
// than the normal end of file.
type countingReader struct {
	reader    io.Reader
	bytesRead int64
	err       error
}

// Read reads from the wrapped reader, adds to the count, and notes errors.
func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.reader.Read(p)
	c.bytesRead += int64(n)
	if err != nil && !errors.Is(err, io.EOF) && c.err == nil {
		c.err = err
	}
	return n, err
}

// newFileID returns a new random ID like "file-k3j...".
func newFileID() string {
	return "file-" + strings.ToLower(cryptorand.Text())
}
