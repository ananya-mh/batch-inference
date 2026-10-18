// This file checks the lines of a batch input file, one line at a time.
package files

import (
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

// ChatCompletionsURL is the only request URL a batch line may use, because
// it is the only endpoint our model backends serve.
const ChatCompletionsURL = "/v1/chat/completions"

// LineError says which line of an uploaded file is invalid, and why.
type LineError struct {
	Line   int
	Reason string
}

// Error returns a message like `line 7: missing "custom_id"`.
func (e *LineError) Error() string {
	return fmt.Sprintf("line %d: %s", e.Line, e.Reason)
}

// Validator checks the lines of one batch input file. It remembers every
// custom_id it has seen, to catch duplicates; that set is the only thing
// that grows with the file.
type Validator struct {
	firstLineOfCustomID map[string]int
}

// NewValidator returns a Validator for a new file.
func NewValidator() *Validator {
	return &Validator{firstLineOfCustomID: make(map[string]int)}
}

// inputLine is one line of a batch input file, in OpenAI's format:
// {"custom_id": "...", "method": "POST", "url": "/v1/chat/completions", "body": {...}}
type inputLine struct {
	CustomID string          `json:"custom_id"`
	Method   string          `json:"method"`
	URL      string          `json:"url"`
	Body     json.RawMessage `json:"body"`
}

// requestBody is the part of a line's body we check up front. Missing
// these would make every attempt of the request fail later, so it is
// kinder to reject the file now.
type requestBody struct {
	Model    string            `json:"model"`
	Messages []json.RawMessage `json:"messages"`
}

// CheckLine checks one line (without its newline) and returns its custom_id.
// lineNumber counts from 1 and is used in the error message.
func (v *Validator) CheckLine(lineNumber int, line []byte) (string, error) {
	fail := func(format string, args ...any) (string, error) {
		return "", &LineError{Line: lineNumber, Reason: fmt.Sprintf(format, args...)}
	}

	if len(line) == 0 {
		return fail("line is empty")
	}
	// Postgres text columns reject invalid UTF-8, and Go's JSON parser
	// quietly replaces it, so check explicitly to give a clear message.
	if !utf8.Valid(line) {
		return fail("line is not valid UTF-8")
	}

	var parsed inputLine
	if err := json.Unmarshal(line, &parsed); err != nil {
		return fail("not a valid JSON object: %v", err)
	}
	if parsed.CustomID == "" {
		return fail(`missing "custom_id"`)
	}
	if parsed.Method != "POST" {
		return fail(`"method" must be "POST", got %q`, parsed.Method)
	}
	if parsed.URL != ChatCompletionsURL {
		return fail(`"url" must be %q, got %q`, ChatCompletionsURL, parsed.URL)
	}

	var body requestBody
	if len(parsed.Body) == 0 || string(parsed.Body) == "null" {
		return fail(`missing "body"`)
	}
	if err := json.Unmarshal(parsed.Body, &body); err != nil {
		return fail(`"body" must be a JSON object: %v`, err)
	}
	if body.Model == "" {
		return fail(`missing "body.model"`)
	}
	if len(body.Messages) == 0 {
		return fail(`missing "body.messages"`)
	}

	// custom_id is how results are matched back to requests, so two lines
	// sharing one would make their results impossible to tell apart.
	if firstLine, seen := v.firstLineOfCustomID[parsed.CustomID]; seen {
		return fail("duplicate custom_id %q (first used on line %d)", parsed.CustomID, firstLine)
	}
	v.firstLineOfCustomID[parsed.CustomID] = lineNumber

	return parsed.CustomID, nil
}
