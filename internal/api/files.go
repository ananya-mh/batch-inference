// This file has the Files API handlers: upload a batch input file, get its
// details, and download its content.
package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"

	"github.com/jackc/pgx/v5"

	"github.com/ananya-mh/batch-inference/internal/database"
	"github.com/ananya-mh/batch-inference/internal/files"
)

// maxPurposeBytes caps how much of the "purpose" form field we read. Valid
// values are a few characters long.
const maxPurposeBytes = 64

// fileObject is a file as the OpenAI API returns it.
type fileObject struct {
	ID        string `json:"id"`
	Object    string `json:"object"`
	Bytes     int64  `json:"bytes"`
	CreatedAt int64  `json:"created_at"`
	Filename  string `json:"filename"`
	Purpose   string `json:"purpose"`
	// Status is deprecated in OpenAI's API but older SDKs still read it.
	Status string `json:"status"`
}

// toFileObject converts a stored file to its API form.
func toFileObject(file files.File) fileObject {
	return fileObject{
		ID:        file.ID,
		Object:    "file",
		Bytes:     file.Bytes,
		CreatedAt: file.CreatedAt.Unix(),
		Filename:  file.Filename,
		Purpose:   file.Purpose,
		Status:    "processed",
	}
}

// requestError is a problem with the client's request, with the HTTP status
// to send back.
type requestError struct {
	status  int
	message string
}

// Error returns the message.
func (e *requestError) Error() string {
	return e.message
}

// handleUploadFile stores a batch input file sent as multipart/form-data
// with a "purpose" field (must be "batch") and a "file" field.
func (s *Server) handleUploadFile(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// When the client says the size up front, reject an oversized upload
	// before reading any of it.
	if r.ContentLength > s.config.MaxUploadBytes {
		s.writeUploadTooLarge(w)
		return
	}
	// The size may be unknown (chunked upload) or wrong, so also stop
	// reading once the limit is passed.
	r.Body = http.MaxBytesReader(w, r.Body, s.config.MaxUploadBytes)

	multipartReader, err := r.MultipartReader()
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "upload must be multipart/form-data: "+err.Error())
		return
	}

	// The file is stored inside a transaction, so if anything goes wrong
	// later (a bad line, a wrong purpose), nothing is kept.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	defer database.RollbackUnlessCommitted(ctx, tx)

	file, err := readUploadForm(ctx, tx, tenantIDFrom(ctx), multipartReader)
	if err != nil {
		s.writeUploadError(w, r, err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		writeInternalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toFileObject(file))
}

// readUploadForm reads the upload's form fields and stores the file in tx.
//
// "purpose" and "file" may come in either order, because SDKs differ: the
// Python SDK sends purpose first, the Node SDK sends file first. The file is
// streamed into tx as it arrives, and purpose is checked at the end; if it
// is wrong, the caller rolls tx back.
func readUploadForm(ctx context.Context, tx pgx.Tx, tenantID string, reader *multipart.Reader) (files.File, error) {
	var file files.File
	var purpose string
	sawFile := false

	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return files.File{}, fmt.Errorf("read upload: %w", err)
		}

		switch part.FormName() {
		case "purpose":
			value, err := io.ReadAll(io.LimitReader(part, maxPurposeBytes))
			if err != nil {
				return files.File{}, fmt.Errorf("read purpose field: %w", err)
			}
			purpose = string(value)
		case "file":
			if sawFile {
				return files.File{}, &requestError{http.StatusBadRequest, `send only one "file" field per upload`}
			}
			sawFile = true
			file, err = files.CreateBatchInput(ctx, tx, tenantID, part.FileName(), part)
			if err != nil {
				return files.File{}, err
			}
		}
		// Other fields are ignored; NextPart skips any unread data.
	}

	if !sawFile {
		return files.File{}, &requestError{http.StatusBadRequest, `missing "file" field`}
	}
	if purpose != files.PurposeBatch {
		return files.File{}, &requestError{http.StatusBadRequest, fmt.Sprintf(`"purpose" must be %q, got %q`, files.PurposeBatch, purpose)}
	}
	return file, nil
}

// writeUploadError sends the right status for an upload that failed.
func (s *Server) writeUploadError(w http.ResponseWriter, r *http.Request, err error) {
	var tooLarge *http.MaxBytesError
	var lineErr *files.LineError
	var reqErr *requestError

	switch {
	case errors.As(err, &tooLarge):
		s.writeUploadTooLarge(w)
	case errors.As(err, &lineErr):
		writeError(w, http.StatusBadRequest, "invalid_request_error", "invalid batch file: "+lineErr.Error())
	case errors.Is(err, files.ErrEmptyFile):
		writeError(w, http.StatusBadRequest, "invalid_request_error", "invalid batch file: "+err.Error())
	case errors.As(err, &reqErr):
		writeError(w, reqErr.status, "invalid_request_error", reqErr.message)
	default:
		writeInternalError(w, r, err)
	}
}

// writeUploadTooLarge sends 413 with the configured limit in the message.
func (s *Server) writeUploadTooLarge(w http.ResponseWriter) {
	writeError(w, http.StatusRequestEntityTooLarge, "invalid_request_error",
		fmt.Sprintf("upload is larger than the %d byte limit", s.config.MaxUploadBytes))
}

// handleGetFile returns a file's details.
func (s *Server) handleGetFile(w http.ResponseWriter, r *http.Request) {
	file, err := files.Get(r.Context(), s.pool, tenantIDFrom(r.Context()), r.PathValue("id"))
	if errors.Is(err, files.ErrNotFound) {
		writeFileNotFound(w)
		return
	}
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toFileObject(file))
}

// handleGetFileContent streams a file's content back as JSONL.
func (s *Server) handleGetFileContent(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tenantID := tenantIDFrom(ctx)
	fileID := r.PathValue("id")

	// Check ownership before sending any bytes, while we can still answer
	// with a 404 status.
	if _, err := files.Get(ctx, s.pool, tenantID, fileID); errors.Is(err, files.ErrNotFound) {
		writeFileNotFound(w)
		return
	} else if err != nil {
		writeInternalError(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	if err := files.WriteContent(ctx, s.pool, tenantID, fileID, w); err != nil {
		// The 200 status has already gone out, so we cannot send an error
		// now. Aborting the connection makes the download fail visibly,
		// instead of looking like a complete but shorter file.
		slog.ErrorContext(ctx, "stream file content", "file_id", fileID, "error", err)
		panic(http.ErrAbortHandler)
	}
}

// writeFileNotFound sends 404. It is used both for missing files and other
// tenants' files, so the two cannot be told apart.
func writeFileNotFound(w http.ResponseWriter) {
	writeError(w, http.StatusNotFound, "invalid_request_error", "no such file")
}
