// Package model sends chat completion requests to any OpenAI-compatible model
// server (the fake model, vLLM, or a hosted provider) and sorts failures into
// "try again later" and "will never work".
package model

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
)

// maxReplyBytes caps how much of a reply we read. Real replies are a few KB.
const maxReplyBytes = 10 << 20

// Client talks to one model server.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// NewClient returns a client for the server at baseURL, which includes the
// /v1 part, like "http://fakemodel:8000/v1". apiKey may be empty; it is only
// needed for hosted providers.
func NewClient(baseURL, apiKey string) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Go keeps only 2 idle connections per server by default. With 16
	// workers calling one server, the rest would open a new connection for
	// every request.
	transport.MaxIdleConnsPerHost = 64
	return &Client{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		apiKey:  apiKey,
		// No overall timeout here: each call is limited by its context, which
		// the worker sets from the job timeout.
		httpClient: &http.Client{Transport: transport},
	}
}

// PermanentError means the model server rejected the request itself (for
// example HTTP 400: bad parameters). Sending it again would fail the same
// way, so it should not be retried.
type PermanentError struct {
	StatusCode int
	Body       []byte
}

// Error describes the rejection.
func (e *PermanentError) Error() string {
	return fmt.Sprintf("model server rejected the request: HTTP %d: %s", e.StatusCode, e.Body)
}

// Complete sends one chat completion request body and returns the model's
// reply. It only returns a reply once the whole body has arrived and is valid
// JSON, so a reply cut off midway is never mistaken for a result.
//
// Errors are either a *PermanentError, or a temporary problem worth retrying:
// a network error, a timeout, a cut-off or invalid reply, HTTP 408, 429, or 5xx.
func (c *Client) Complete(ctx context.Context, body []byte) (json.RawMessage, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build model request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		request.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("call model: %w", err)
	}
	defer closeBody(response.Body)

	// Read everything before looking at the status. If the server dies
	// midway, this is where it shows up: Go reports "unexpected EOF" when
	// the body is shorter than the server promised.
	reply, err := io.ReadAll(io.LimitReader(response.Body, maxReplyBytes))
	if err != nil {
		return nil, fmt.Errorf("read model reply (HTTP %d): %w", response.StatusCode, err)
	}

	switch {
	case response.StatusCode == http.StatusOK:
		// A reply cut off without a promised length (chunked) reads
		// "successfully" but is incomplete JSON, so check it parses.
		if !json.Valid(reply) {
			return nil, errors.New("model reply is not valid JSON (it may have been cut off)")
		}
		return reply, nil
	case isTemporaryStatus(response.StatusCode):
		return nil, fmt.Errorf("model server error: HTTP %d: %s", response.StatusCode, reply)
	default:
		return nil, &PermanentError{StatusCode: response.StatusCode, Body: reply}
	}
}

// isTemporaryStatus reports whether an HTTP status means "try again later":
// a timeout (408), rate limiting (429), or a server-side error (5xx).
func isTemporaryStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500
}

// closeBody closes a response body. By then the reply has been read or
// abandoned, so a close error changes nothing; it is only logged.
func closeBody(body io.Closer) {
	if err := body.Close(); err != nil {
		slog.Debug("close model reply body", "error", err)
	}
}
