// This file has the Batches API handlers: create a batch, get one, list
// them, and cancel one.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ananya-mh/batch-inference/internal/batches"
	"github.com/ananya-mh/batch-inference/internal/cancel"
	"github.com/ananya-mh/batch-inference/internal/files"
)

// Paging limits, the same as OpenAI's for batches.
const (
	defaultListLimit = 20
	maxListLimit     = 100
)

// Metadata limits, the same as OpenAI's.
const (
	maxMetadataPairs       = 16
	maxMetadataKeyLength   = 64
	maxMetadataValueLength = 512
)

// maxCreateBodyBytes caps the create request body; it only holds a few IDs
// and the metadata.
const maxCreateBodyBytes = 64 * 1024

// createBatchRequest is the body of POST /v1/batches.
type createBatchRequest struct {
	InputFileID      string            `json:"input_file_id"`
	Endpoint         string            `json:"endpoint"`
	CompletionWindow string            `json:"completion_window"`
	Metadata         map[string]string `json:"metadata"`
}

// batchObject is a batch as the OpenAI API returns it.
type batchObject struct {
	ID               string            `json:"id"`
	Object           string            `json:"object"`
	Endpoint         string            `json:"endpoint"`
	Errors           any               `json:"errors"` // always null for now
	InputFileID      string            `json:"input_file_id"`
	CompletionWindow string            `json:"completion_window"`
	Status           batches.Status    `json:"status"`
	OutputFileID     *string           `json:"output_file_id"`
	ErrorFileID      *string           `json:"error_file_id"`
	CreatedAt        int64             `json:"created_at"`
	InProgressAt     *int64            `json:"in_progress_at"`
	ExpiresAt        int64             `json:"expires_at"`
	FinalizingAt     *int64            `json:"finalizing_at"`
	CompletedAt      *int64            `json:"completed_at"`
	FailedAt         *int64            `json:"failed_at"`
	ExpiredAt        *int64            `json:"expired_at"`
	CancellingAt     *int64            `json:"cancelling_at"`
	CancelledAt      *int64            `json:"cancelled_at"`
	RequestCounts    requestCounts     `json:"request_counts"`
	Metadata         map[string]string `json:"metadata"`
}

// requestCounts is the request_counts field of a batch object.
type requestCounts struct {
	Total     int `json:"total"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
}

// batchList is a page of batches in OpenAI's list format. The SDKs page by
// passing the last ID back as "after" while has_more is true.
type batchList struct {
	Object  string        `json:"object"`
	Data    []batchObject `json:"data"`
	FirstID *string       `json:"first_id"`
	LastID  *string       `json:"last_id"`
	HasMore bool          `json:"has_more"`
}

// toBatchObject converts a stored batch to its API form.
func toBatchObject(batch batches.Batch) batchObject {
	return batchObject{
		ID:               batch.ID,
		Object:           "batch",
		Endpoint:         batch.Endpoint,
		InputFileID:      batch.InputFileID,
		CompletionWindow: batch.CompletionWindow,
		Status:           batch.Status,
		OutputFileID:     batch.OutputFileID,
		ErrorFileID:      batch.ErrorFileID,
		CreatedAt:        batch.CreatedAt.Unix(),
		InProgressAt:     unixOrNil(batch.InProgressAt),
		ExpiresAt:        batch.ExpiresAt.Unix(),
		FinalizingAt:     unixOrNil(batch.FinalizingAt),
		CompletedAt:      unixOrNil(batch.CompletedAt),
		FailedAt:         unixOrNil(batch.FailedAt),
		ExpiredAt:        unixOrNil(batch.ExpiredAt),
		CancellingAt:     unixOrNil(batch.CancellingAt),
		CancelledAt:      unixOrNil(batch.CancelledAt),
		RequestCounts: requestCounts{
			Total:     batch.RequestCounts.Total,
			Completed: batch.RequestCounts.Completed,
			Failed:    batch.RequestCounts.Failed,
		},
		Metadata: batch.Metadata,
	}
}

// unixOrNil turns an optional time into optional Unix seconds, so unset
// times appear as null in JSON, as in OpenAI's API.
func unixOrNil(t *time.Time) *int64 {
	if t == nil {
		return nil
	}
	seconds := t.Unix()
	return &seconds
}

// handleCreateBatch starts a batch from an uploaded input file.
func (s *Server) handleCreateBatch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tenantID := tenantIDFrom(ctx)

	var request createBatchRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxCreateBodyBytes)).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "request body is not valid JSON: "+err.Error())
		return
	}

	if request.Endpoint != files.ChatCompletionsURL {
		writeError(w, http.StatusBadRequest, "invalid_request_error",
			fmt.Sprintf(`"endpoint" must be %q, got %q`, files.ChatCompletionsURL, request.Endpoint))
		return
	}
	windowLength, windowAllowed := s.config.CompletionWindows[request.CompletionWindow]
	if !windowAllowed {
		writeError(w, http.StatusBadRequest, "invalid_request_error",
			fmt.Sprintf(`"completion_window" must be one of %s, got %q`, s.allowedWindowList(), request.CompletionWindow))
		return
	}
	if problem := checkMetadata(request.Metadata); problem != "" {
		writeError(w, http.StatusBadRequest, "invalid_request_error", problem)
		return
	}

	// files.Get only finds the tenant's own files, so another tenant's file
	// gets the same answer as a file that does not exist.
	inputFile, err := files.Get(ctx, s.pool, tenantID, request.InputFileID)
	if errors.Is(err, files.ErrNotFound) {
		writeError(w, http.StatusBadRequest, "invalid_request_error", fmt.Sprintf("input file %q not found", request.InputFileID))
		return
	}
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	if inputFile.Purpose != files.PurposeBatch {
		writeError(w, http.StatusBadRequest, "invalid_request_error",
			fmt.Sprintf("input file %q has purpose %q, want %q", inputFile.ID, inputFile.Purpose, files.PurposeBatch))
		return
	}

	batch, err := batches.Create(ctx, s.pool, batches.NewBatch{
		TenantID:         tenantID,
		InputFileID:      inputFile.ID,
		Endpoint:         request.Endpoint,
		CompletionWindow: request.CompletionWindow,
		WindowLength:     windowLength,
		Metadata:         request.Metadata,
		RequestTotal:     inputFile.LineCount,
	})
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toBatchObject(batch))
}

// checkMetadata returns a description of what is wrong with metadata, or ""
// if it is fine.
func checkMetadata(metadata map[string]string) string {
	if len(metadata) > maxMetadataPairs {
		return fmt.Sprintf(`"metadata" may have at most %d keys, got %d`, maxMetadataPairs, len(metadata))
	}
	for key, value := range metadata {
		if len(key) > maxMetadataKeyLength {
			return fmt.Sprintf(`"metadata" key %q is longer than %d characters`, key, maxMetadataKeyLength)
		}
		if len(value) > maxMetadataValueLength {
			return fmt.Sprintf(`"metadata" value for %q is longer than %d characters`, key, maxMetadataValueLength)
		}
	}
	return ""
}

// allowedWindowList returns the allowed completion windows, sorted, for
// error messages.
func (s *Server) allowedWindowList() string {
	windows := make([]string, 0, len(s.config.CompletionWindows))
	for window := range s.config.CompletionWindows {
		windows = append(windows, strconv.Quote(window))
	}
	sort.Strings(windows)
	return strings.Join(windows, ", ")
}

// handleGetBatch returns one batch, including its status and request counts.
func (s *Server) handleGetBatch(w http.ResponseWriter, r *http.Request) {
	batch, err := batches.Get(r.Context(), s.pool, tenantIDFrom(r.Context()), r.PathValue("id"))
	if errors.Is(err, batches.ErrNotFound) {
		writeError(w, http.StatusNotFound, "invalid_request_error", "no such batch")
		return
	}
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toBatchObject(batch))
}

// handleCancelBatch cancels a batch and returns it, usually as cancelling.
// Cancelling a batch that is already cancelling or cancelled returns it as it
// is (200). A batch that has already finished or is finalizing gets 409
// Conflict with OpenAI's wording, which the OpenAI SDKs raise as a
// ConflictError.
func (s *Server) handleCancelBatch(w http.ResponseWriter, r *http.Request) {
	batch, err := cancel.Batch(r.Context(), s.pool, s.river, tenantIDFrom(r.Context()), r.PathValue("id"))
	var notCancellable *cancel.NotCancellableError
	switch {
	case errors.Is(err, batches.ErrNotFound):
		writeError(w, http.StatusNotFound, "invalid_request_error", "no such batch")
	case errors.As(err, &notCancellable):
		writeError(w, http.StatusConflict, "invalid_request_error", notCancellable.Error())
	case err != nil:
		writeInternalError(w, r, err)
	default:
		writeJSON(w, http.StatusOK, toBatchObject(batch))
	}
}

// handleListBatches returns a page of the tenant's batches, newest first.
// Query parameters: limit (1 to 100, default 20) and after (a batch ID).
func (s *Server) handleListBatches(w http.ResponseWriter, r *http.Request) {
	limit := defaultListLimit
	if value := r.URL.Query().Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > maxListLimit {
			writeError(w, http.StatusBadRequest, "invalid_request_error",
				fmt.Sprintf(`"limit" must be a number from 1 to %d, got %q`, maxListLimit, value))
			return
		}
		limit = parsed
	}
	after := r.URL.Query().Get("after")

	page, err := batches.List(r.Context(), s.pool, tenantIDFrom(r.Context()), limit, after)
	if errors.Is(err, batches.ErrNotFound) {
		writeError(w, http.StatusBadRequest, "invalid_request_error", fmt.Sprintf(`"after" batch %q not found`, after))
		return
	}
	if err != nil {
		writeInternalError(w, r, err)
		return
	}

	// Always send a list, never null, even when there are no batches.
	list := batchList{Object: "list", Data: make([]batchObject, 0, len(page.Batches)), HasMore: page.HasMore}
	for _, batch := range page.Batches {
		list.Data = append(list.Data, toBatchObject(batch))
	}
	if len(list.Data) > 0 {
		list.FirstID = &list.Data[0].ID
		list.LastID = &list.Data[len(list.Data)-1].ID
	}
	writeJSON(w, http.StatusOK, list)
}
