// This file has helpers for writing JSON responses and OpenAI-format errors.
package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// apiError is OpenAI's error body: {"error": {"message": ..., "type": ...}}.
// Using the same shape lets the OpenAI SDKs show our error messages.
type apiError struct {
	Error apiErrorDetail `json:"error"`
}

// apiErrorDetail is the inside of an apiError.
type apiErrorDetail struct {
	Message string `json:"message"`
	Type    string `json:"type"`
}

// writeError sends an error in OpenAI's format.
func writeError(w http.ResponseWriter, status int, errorType, message string) {
	writeJSON(w, status, apiError{Error: apiErrorDetail{Message: message, Type: errorType}})
}

// writeInternalError logs the real cause and sends the client a generic
// message, so database details are not leaked to callers.
func writeInternalError(w http.ResponseWriter, r *http.Request, err error) {
	slog.ErrorContext(r.Context(), "request failed", "method", r.Method, "path", r.URL.Path, "error", err)
	writeError(w, http.StatusInternalServerError, "server_error", "internal server error")
}

// writeJSON sends value as a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, value any) {
	body, err := json.Marshal(value)
	if err != nil {
		// Our own response types always encode; reaching here is a bug.
		slog.Error("encode response", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(body); err != nil {
		slog.Warn("write response", "error", err)
	}
}
