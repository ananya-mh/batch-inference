// Package fakemodel is a pretend AI model server. It speaks the same
// /v1/chat/completions API as OpenAI and vLLM, but just waits a while and
// returns filler text, and can be told to fail or crash on purpose. This lets
// us develop and test without a GPU.
package fakemodel

import (
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// maxRequestBytes caps the request body size, so a bad client cannot make
// the server read an endless body.
const maxRequestBytes = 1 << 20

// Server is the fake model's HTTP server.
type Server struct {
	config Config

	// With a seed, timesSeen counts how often each request body (by hash)
	// has arrived, so a retry of a failed request gets a fresh roll instead
	// of failing the same way forever. mu guards it, since requests are
	// handled concurrently.
	mu        sync.Mutex
	timesSeen map[[sha256.Size]byte]uint64
}

// NewServer returns a fake model server with the given settings.
func NewServer(config Config) (*Server, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &Server{config: config, timesSeen: make(map[[sha256.Size]byte]uint64)}, nil
}

// outcome is what the fake model does with one request.
type outcome int

const (
	outcomeSuccess outcome = iota
	outcomeError
	outcomeCrash
)

// plan is the delay and outcome chosen for one request.
type plan struct {
	delay   time.Duration
	outcome outcome
}

// Handler returns an http.Handler with the fake model's routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", s.handleChatCompletion)
	return mux
}

// chatRequest is the part of an OpenAI chat request the fake model reads.
type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
}

// chatMessage is one message in a chat. Content is kept raw because OpenAI
// allows either a plain string or a list of parts.
type chatMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

// chatResponse is an OpenAI-format chat completion reply.
type chatResponse struct {
	ID      string       `json:"id"`
	Object  string       `json:"object"`
	Created int64        `json:"created"`
	Model   string       `json:"model"`
	Choices []chatChoice `json:"choices"`
	Usage   usage        `json:"usage"`
}

// chatChoice is one generated reply inside a chatResponse.
type chatChoice struct {
	Index        int          `json:"index"`
	Message      replyMessage `json:"message"`
	FinishReason string       `json:"finish_reason"`
}

// replyMessage is the assistant's message inside a chatChoice.
type replyMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// usage reports token counts. The fake model counts words as tokens.
type usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// handleChatCompletion waits a random delay, then replies with filler text,
// a simulated error, or a simulated crash, depending on the settings.
func (s *Server) handleChatCompletion(w http.ResponseWriter, r *http.Request) {
	// Read the raw body first: with a seed, the exact bytes decide what
	// happens to this request.
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "read request body: "+err.Error())
		return
	}
	var request chatRequest
	if err := json.Unmarshal(body, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "request body is not valid JSON: "+err.Error())
		return
	}
	if request.Model == "" || len(request.Messages) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "model and messages are required")
		return
	}

	plan := s.planFor(body)

	// Stop early if the caller gives up (for example, a worker that timed
	// out or was shut down), just as a real server would stop generating.
	select {
	case <-time.After(plan.delay):
	case <-r.Context().Done():
		return
	}

	switch plan.outcome {
	case outcomeCrash:
		s.crashMidResponse(w, request)
	case outcomeError:
		writeError(w, http.StatusInternalServerError, "server_error", "fake model: simulated server error")
	case outcomeSuccess:
		writeJSON(w, http.StatusOK, s.buildReply(request))
	}
}

// planFor picks the delay and outcome for a request with this body.
func (s *Server) planFor(body []byte) plan {
	random := s.randomSourceFor(body)

	delay := s.config.MinDelay
	if spread := s.config.MaxDelay - s.config.MinDelay; spread > 0 {
		delay += time.Duration(random.Int64N(int64(spread) + 1))
	}

	// One roll decides the outcome: [0, crash) crashes, [crash,
	// crash+error) fails, and the rest succeed.
	roll := random.Float64()
	switch {
	case roll < s.config.CrashRate:
		return plan{delay: delay, outcome: outcomeCrash}
	case roll < s.config.CrashRate+s.config.ErrorRate:
		return plan{delay: delay, outcome: outcomeError}
	default:
		return plan{delay: delay, outcome: outcomeSuccess}
	}
}

// randomSourceFor returns the random numbers to use for one request.
//
// Without a seed they are truly random. With a seed they come from hashing
// the seed, the body, and how many times this body was seen before. A single
// shared random sequence would not be enough: in the fair and unfair
// experiment runs requests arrive in a different order, so the same sequence
// would hand the errors to different requests. Hashing per request means the
// same request is treated the same way in both runs.
func (s *Server) randomSourceFor(body []byte) *rand.Rand {
	if s.config.Seed == 0 {
		return rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	}

	bodyHash := sha256.Sum256(body)
	s.mu.Lock()
	attempt := s.timesSeen[bodyHash]
	s.timesSeen[bodyHash] = attempt + 1
	s.mu.Unlock()

	hasher := sha256.New()
	// Writes to a hash never fail, so there is no error to check.
	hasher.Write(binary.LittleEndian.AppendUint64(nil, s.config.Seed))
	hasher.Write(bodyHash[:])
	hasher.Write(binary.LittleEndian.AppendUint64(nil, attempt))
	sum := hasher.Sum(nil)

	return rand.New(rand.NewPCG(binary.LittleEndian.Uint64(sum[0:8]), binary.LittleEndian.Uint64(sum[8:16])))
}

// buildReply makes a successful chat completion with OutputWords words.
func (s *Server) buildReply(request chatRequest) chatResponse {
	promptWords := 0
	for _, message := range request.Messages {
		promptWords += len(strings.Fields(messageText(message.Content)))
	}

	return chatResponse{
		ID:      "chatcmpl-" + cryptorand.Text(),
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   request.Model,
		Choices: []chatChoice{{
			Index:        0,
			Message:      replyMessage{Role: "assistant", Content: fillerText(s.config.OutputWords)},
			FinishReason: "stop",
		}},
		Usage: usage{
			PromptTokens:     promptWords,
			CompletionTokens: s.config.OutputWords,
			TotalTokens:      promptWords + s.config.OutputWords,
		},
	}
}

// crashMidResponse starts a successful reply, sends only half of it, then
// drops the connection. The client sees a 200 status followed by a cut-off
// body, which is what happens when a real model server dies mid-reply.
func (s *Server) crashMidResponse(w http.ResponseWriter, request chatRequest) {
	body, err := json.Marshal(s.buildReply(request))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "server_error", "encode reply: "+err.Error())
		return
	}

	// Promise the full length, so the client knows the body was cut short.
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(body[:len(body)/2]); err != nil {
		slog.Warn("fake model: write partial reply", "error", err)
	}
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}

	// This panic is the standard library's documented way to abort a
	// reply: net/http closes the connection and does not log it.
	panic(http.ErrAbortHandler)
}

// messageText returns the text of a message's content, whether it was sent
// as a plain string or as a list of {"type": "text", "text": ...} parts.
func messageText(content json.RawMessage) string {
	var text string
	if err := json.Unmarshal(content, &text); err == nil {
		return text
	}

	var parts []struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(content, &parts); err != nil {
		// Unknown content shape: count it as no words rather than
		// rejecting the request, since only the usage numbers depend on it.
		return ""
	}
	texts := make([]string, 0, len(parts))
	for _, part := range parts {
		texts = append(texts, part.Text)
	}
	return strings.Join(texts, " ")
}

// fillerWords are cycled through to build replies of any length.
var fillerWords = []string{"this", "is", "a", "fake", "model", "reply", "for", "testing"}

// fillerText returns a string of exactly wordCount words.
func fillerText(wordCount int) string {
	words := make([]string, wordCount)
	for i := range words {
		words[i] = fillerWords[i%len(fillerWords)]
	}
	return strings.Join(words, " ")
}

// apiError is OpenAI's error body: {"error": {"message": ..., "type": ...}}.
type apiError struct {
	Error apiErrorDetail `json:"error"`
}

// apiErrorDetail is the inside of an apiError.
type apiErrorDetail struct {
	Message string `json:"message"`
	Type    string `json:"type"`
}

// writeError sends an error in OpenAI's format, so client libraries that
// expect that format can read it.
func writeError(w http.ResponseWriter, status int, errorType, message string) {
	writeJSON(w, status, apiError{Error: apiErrorDetail{Message: message, Type: errorType}})
}

// writeJSON sends value as a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, value any) {
	body, err := json.Marshal(value)
	if err != nil {
		// Our own response types always encode; reaching here is a bug.
		slog.Error("fake model: encode response", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(body); err != nil {
		slog.Warn("fake model: write response", "error", err)
	}
}
