// Tests for the model client: good replies, and which failures are retried.
package model

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ananya-mh/batch-inference/internal/fakemodel"
)

// requestBody is a valid chat completion request.
const requestBody = `{"model": "fake", "messages": [{"role": "user", "content": "hi"}]}`

// startFakeModel runs the fake model with config for this test and returns
// a client pointed at it.
func startFakeModel(t *testing.T, config fakemodel.Config) *Client {
	t.Helper()
	server, err := fakemodel.NewServer(config)
	if err != nil {
		t.Fatalf("fakemodel.NewServer: %v", err)
	}
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	return NewClient(httpServer.URL+"/v1", "")
}

// startStatusServer runs a server that always answers with status and body.
func startStatusServer(t *testing.T, status int, body string) *Client {
	t.Helper()
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		if _, err := w.Write([]byte(body)); err != nil {
			t.Errorf("write reply: %v", err)
		}
	}))
	t.Cleanup(httpServer.Close)
	return NewClient(httpServer.URL+"/v1", "")
}

// TestCompleteReturnsReply checks a normal reply comes back whole.
func TestCompleteReturnsReply(t *testing.T) {
	t.Parallel()
	client := startFakeModel(t, fakemodel.Config{OutputWords: 3})

	reply, err := client.Complete(context.Background(), []byte(requestBody))
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if len(reply) == 0 {
		t.Error("reply is empty")
	}
}

// TestCutOffReplyIsTemporaryError checks a 200 whose body is cut off midway
// (the fake model's crash mode) is an error to retry, never a reply.
func TestCutOffReplyIsTemporaryError(t *testing.T) {
	t.Parallel()
	client := startFakeModel(t, fakemodel.Config{OutputWords: 3, CrashRate: 1})

	reply, err := client.Complete(context.Background(), []byte(requestBody))
	if err == nil {
		t.Fatalf("got reply %q, want an error", reply)
	}
	var permanent *PermanentError
	if errors.As(err, &permanent) {
		t.Errorf("error %v is permanent, want temporary", err)
	}
}

// TestInvalidJSONReplyIsTemporaryError checks a 200 with broken JSON (as from
// a cut-off chunked reply, which has no promised length) is not accepted.
func TestInvalidJSONReplyIsTemporaryError(t *testing.T) {
	t.Parallel()
	client := startStatusServer(t, http.StatusOK, `{"choices": [{"message": `)

	if _, err := client.Complete(context.Background(), []byte(requestBody)); err == nil {
		t.Fatal("cut-off JSON was accepted")
	}
}

// TestStatusCodesAreSortedCorrectly checks which HTTP errors are retried.
func TestStatusCodesAreSortedCorrectly(t *testing.T) {
	t.Parallel()
	cases := map[int]bool{ // status -> want permanent
		http.StatusInternalServerError: false,
		http.StatusBadGateway:          false,
		http.StatusServiceUnavailable:  false,
		http.StatusTooManyRequests:     false,
		http.StatusRequestTimeout:      false,
		http.StatusBadRequest:          true,
		http.StatusNotFound:            true,
		http.StatusUnprocessableEntity: true,
	}
	for status, wantPermanent := range cases {
		client := startStatusServer(t, status, `{"error": {"message": "nope"}}`)
		_, err := client.Complete(context.Background(), []byte(requestBody))
		var permanent *PermanentError
		if err == nil || errors.As(err, &permanent) != wantPermanent {
			t.Errorf("HTTP %d: error = %v, want permanent = %v", status, err, wantPermanent)
		}
	}
}
