// Tests for checking batch input lines.
package files

import (
	"errors"
	"strings"
	"testing"
)

// validLine returns a correct batch line with the given custom_id.
func validLine(customID string) string {
	return `{"custom_id": "` + customID + `", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "fake", "messages": [{"role": "user", "content": "hi"}]}}`
}

// TestCheckLineAcceptsValidLine checks a correct line passes and its
// custom_id is returned.
func TestCheckLineAcceptsValidLine(t *testing.T) {
	t.Parallel()
	customID, err := NewValidator().CheckLine(1, []byte(validLine("ticket-1")))
	if err != nil {
		t.Fatalf("CheckLine: %v", err)
	}
	if customID != "ticket-1" {
		t.Errorf("custom_id = %q, want ticket-1", customID)
	}
}

// TestCheckLineRejectsBadLines checks each kind of bad line is rejected with
// the line number and a reason that names the problem.
func TestCheckLineRejectsBadLines(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		line       string
		wantReason string
	}{
		{"empty", ``, "empty"},
		{"not JSON", `{oops`, "not a valid JSON object"},
		{"JSON array", `[1, 2]`, "not a valid JSON object"},
		{"invalid UTF-8", "{\"custom_id\": \"\xff\"}", "UTF-8"},
		{"missing custom_id", `{"method": "POST", "url": "/v1/chat/completions", "body": {"model": "m", "messages": [{}]}}`, `missing "custom_id"`},
		{"custom_id not a string", `{"custom_id": 5, "method": "POST", "url": "/v1/chat/completions", "body": {"model": "m", "messages": [{}]}}`, "not a valid JSON object"},
		{"wrong method", `{"custom_id": "a", "method": "GET", "url": "/v1/chat/completions", "body": {"model": "m", "messages": [{}]}}`, `"method" must be "POST"`},
		{"missing url", `{"custom_id": "a", "method": "POST", "body": {"model": "m", "messages": [{}]}}`, `"url" must be`},
		{"unsupported url", `{"custom_id": "a", "method": "POST", "url": "/v1/embeddings", "body": {"model": "m", "messages": [{}]}}`, `"url" must be`},
		{"missing body", `{"custom_id": "a", "method": "POST", "url": "/v1/chat/completions"}`, `missing "body"`},
		{"body not an object", `{"custom_id": "a", "method": "POST", "url": "/v1/chat/completions", "body": "hi"}`, `"body" must be a JSON object`},
		{"missing model", `{"custom_id": "a", "method": "POST", "url": "/v1/chat/completions", "body": {"messages": [{}]}}`, `missing "body.model"`},
		{"empty messages", `{"custom_id": "a", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "m", "messages": []}}`, `missing "body.messages"`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewValidator().CheckLine(4, []byte(tc.line))
			var lineErr *LineError
			if !errors.As(err, &lineErr) {
				t.Fatalf("error = %v, want a *LineError", err)
			}
			if lineErr.Line != 4 {
				t.Errorf("line = %d, want 4", lineErr.Line)
			}
			if !strings.Contains(lineErr.Reason, tc.wantReason) {
				t.Errorf("reason = %q, want it to mention %q", lineErr.Reason, tc.wantReason)
			}
		})
	}
}

// TestCheckLineRejectsDuplicateCustomID checks a repeated custom_id is
// rejected, naming both the duplicate line and the first one.
func TestCheckLineRejectsDuplicateCustomID(t *testing.T) {
	t.Parallel()
	validator := NewValidator()
	for lineNumber, customID := range []string{"a", "b"} {
		if _, err := validator.CheckLine(lineNumber+1, []byte(validLine(customID))); err != nil {
			t.Fatalf("CheckLine(%s): %v", customID, err)
		}
	}

	_, err := validator.CheckLine(3, []byte(validLine("a")))
	if err == nil {
		t.Fatal("duplicate custom_id was accepted")
	}
	if want := `line 3: duplicate custom_id "a" (first used on line 1)`; err.Error() != want {
		t.Errorf("error = %q, want %q", err, want)
	}
}
