// Tests for the fake model server: normal replies, delays, simulated errors,
// simulated crashes, and bad requests.
package fakemodel

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// validRequestBody is a minimal chat request, like one line of a batch file.
const validRequestBody = `{"model": "fake", "messages": [{"role": "user", "content": "classify this ticket please"}]}`

// reliableConfig returns settings with no delay, errors, or crashes, so each
// test can turn on just the behavior it checks.
func reliableConfig() Config {
	return Config{OutputWords: 5}
}

// startServer runs a fake model with config on a local port for this test.
func startServer(t *testing.T, config Config) *httptest.Server {
	t.Helper()
	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	return httpServer
}

// postChat sends body to the fake model's chat endpoint.
func postChat(t *testing.T, baseURL, body string) *http.Response {
	t.Helper()
	response, err := http.Post(baseURL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/chat/completions: %v", err)
	}
	t.Cleanup(func() {
		if err := response.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	})
	return response
}

// TestReplyHasOpenAIShape checks a successful reply looks like OpenAI's, so
// the real worker code can talk to vLLM or the fake model the same way.
func TestReplyHasOpenAIShape(t *testing.T) {
	t.Parallel()
	server := startServer(t, reliableConfig())

	response := postChat(t, server.URL, validRequestBody)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.StatusCode)
	}

	var reply chatResponse
	if err := json.NewDecoder(response.Body).Decode(&reply); err != nil {
		t.Fatalf("decode reply: %v", err)
	}
	if reply.Object != "chat.completion" || reply.Model != "fake" || !strings.HasPrefix(reply.ID, "chatcmpl-") {
		t.Errorf("unexpected reply header fields: %+v", reply)
	}
	if len(reply.Choices) != 1 {
		t.Fatalf("got %d choices, want 1", len(reply.Choices))
	}
	if words := len(strings.Fields(reply.Choices[0].Message.Content)); words != 5 {
		t.Errorf("reply has %d words, want 5", words)
	}
	wantUsage := usage{PromptTokens: 4, CompletionTokens: 5, TotalTokens: 9}
	if reply.Usage != wantUsage {
		t.Errorf("usage = %+v, want %+v", reply.Usage, wantUsage)
	}
}

// TestContentAsListOfPartsIsAccepted checks the other content format OpenAI
// allows, a list of text parts, is understood.
func TestContentAsListOfPartsIsAccepted(t *testing.T) {
	t.Parallel()
	server := startServer(t, reliableConfig())

	body := `{"model": "fake", "messages": [{"role": "user", "content": [{"type": "text", "text": "two words"}]}]}`
	response := postChat(t, server.URL, body)

	var reply chatResponse
	if err := json.NewDecoder(response.Body).Decode(&reply); err != nil {
		t.Fatalf("decode reply: %v", err)
	}
	if reply.Usage.PromptTokens != 2 {
		t.Errorf("prompt tokens = %d, want 2", reply.Usage.PromptTokens)
	}
}

// TestReplyWaitsForDelay checks the configured delay is applied.
func TestReplyWaitsForDelay(t *testing.T) {
	t.Parallel()
	config := reliableConfig()
	config.MinDelay = 100 * time.Millisecond
	config.MaxDelay = 100 * time.Millisecond
	server := startServer(t, config)

	start := time.Now()
	postChat(t, server.URL, validRequestBody)
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Errorf("reply took %s, want at least 100ms", elapsed)
	}
}

// TestCallerCancelStopsTheWait checks the server stops waiting when the
// caller gives up, instead of holding the request for the whole delay.
func TestCallerCancelStopsTheWait(t *testing.T) {
	t.Parallel()
	config := reliableConfig()
	config.MinDelay = time.Minute
	config.MaxDelay = time.Minute
	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	request := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions", strings.NewReader(validRequestBody))

	start := time.Now()
	server.Handler().ServeHTTP(httptest.NewRecorder(), request)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("handler returned after %s, want soon after the 50ms cancel", elapsed)
	}
}

// TestErrorRateOneAlwaysFails checks simulated errors come back as HTTP 500
// with an OpenAI-format error body.
func TestErrorRateOneAlwaysFails(t *testing.T) {
	t.Parallel()
	config := reliableConfig()
	config.ErrorRate = 1
	server := startServer(t, config)

	response := postChat(t, server.URL, validRequestBody)
	if response.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", response.StatusCode)
	}
	var body apiError
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if body.Error.Type != "server_error" {
		t.Errorf("error type = %q, want server_error", body.Error.Type)
	}
}

// TestCrashRateOneCutsTheReplyShort checks a simulated crash sends a 200 status
// and then a cut-off body, so the client gets an error while reading it.
func TestCrashRateOneCutsTheReplyShort(t *testing.T) {
	t.Parallel()
	config := reliableConfig()
	config.CrashRate = 1
	server := startServer(t, config)

	response := postChat(t, server.URL, validRequestBody)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (the crash happens after the status line)", response.StatusCode)
	}
	if _, err := io.ReadAll(response.Body); err == nil {
		t.Fatal("reading the body succeeded, want an error from the cut connection")
	}
}

// TestBadRequestsAreRejected checks malformed requests get HTTP 400.
func TestBadRequestsAreRejected(t *testing.T) {
	t.Parallel()
	server := startServer(t, reliableConfig())

	badBodies := map[string]string{
		"not JSON":       `{not json`,
		"no messages":    `{"model": "fake"}`,
		"empty messages": `{"model": "fake", "messages": []}`,
		"no model":       `{"messages": [{"role": "user", "content": "hi"}]}`,
	}
	for name, body := range badBodies {
		t.Run(name, func(t *testing.T) {
			response := postChat(t, server.URL, body)
			if response.StatusCode != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", response.StatusCode)
			}
		})
	}
}

// TestInvalidConfigIsRejected checks NewServer refuses settings that make no
// sense.
func TestInvalidConfigIsRejected(t *testing.T) {
	t.Parallel()
	invalid := map[string]Config{
		"min delay above max": {MinDelay: time.Second, MaxDelay: time.Millisecond, OutputWords: 1},
		"zero output words":   {OutputWords: 0},
		"error rate above 1":  {OutputWords: 1, ErrorRate: 1.5},
		"negative crash rate": {OutputWords: 1, CrashRate: -0.1},
		"rates sum above 1":   {OutputWords: 1, ErrorRate: 0.6, CrashRate: 0.6},
	}
	for name, config := range invalid {
		if _, err := NewServer(config); err == nil {
			t.Errorf("%s: NewServer succeeded, want an error", name)
		}
	}
}

// TestConfigFromEnv checks defaults are kept for unset variables and set
// variables override them.
func TestConfigFromEnv(t *testing.T) {
	t.Parallel()
	env := map[string]string{
		"FAKE_MODEL_MIN_DELAY":    "10ms",
		"FAKE_MODEL_OUTPUT_WORDS": "7",
		"FAKE_MODEL_CRASH_RATE":   "0",
		"FAKE_MODEL_SEED":         "123",
	}
	config, err := ConfigFromEnv(func(name string) string { return env[name] })
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}

	want := DefaultConfig()
	want.MinDelay = 10 * time.Millisecond
	want.OutputWords = 7
	want.CrashRate = 0
	want.Seed = 123
	if config != want {
		t.Errorf("config = %+v, want %+v", config, want)
	}
}

// TestConfigFromEnvRejectsBadValues checks unparseable values are reported.
func TestConfigFromEnvRejectsBadValues(t *testing.T) {
	t.Parallel()
	bad := map[string]string{
		"FAKE_MODEL_MIN_DELAY":    "soon",
		"FAKE_MODEL_OUTPUT_WORDS": "many",
		"FAKE_MODEL_ERROR_RATE":   "2", // parses, but fails validation
		"FAKE_MODEL_SEED":         "0", // would silently mean "no seed"
	}
	for name, value := range bad {
		env := map[string]string{name: value}
		if _, err := ConfigFromEnv(func(n string) string { return env[n] }); err == nil {
			t.Errorf("%s=%q: ConfigFromEnv succeeded, want an error", name, value)
		}
	}
}

// seededConfig returns flaky settings with a seed, for the seed tests.
func seededConfig(seed uint64) Config {
	return Config{
		MinDelay:    0,
		MaxDelay:    time.Second,
		OutputWords: 1,
		ErrorRate:   0.3,
		CrashRate:   0.2,
		Seed:        seed,
	}
}

// requestBodies returns count different chat request bodies.
func requestBodies(count int) [][]byte {
	bodies := make([][]byte, count)
	for i := range bodies {
		bodies[i] = []byte(fmt.Sprintf(`{"model": "fake", "messages": [{"role": "user", "content": "ticket %d"}]}`, i))
	}
	return bodies
}

// newTestServer builds a Server or fails the test.
func newTestServer(t *testing.T, config Config) *Server {
	t.Helper()
	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return server
}

// TestSameSeedGivesSamePlansInAnyOrder checks the experiment's key need: two
// runs with the same seed treat every request the same way, even when the
// requests arrive in a different order.
func TestSameSeedGivesSamePlansInAnyOrder(t *testing.T) {
	t.Parallel()
	bodies := requestBodies(200)
	firstRun := newTestServer(t, seededConfig(42))
	secondRun := newTestServer(t, seededConfig(42))

	plansInOrder := make([]plan, len(bodies))
	for i, body := range bodies {
		plansInOrder[i] = firstRun.planFor(body)
	}
	// The second run sees the same requests in reverse order.
	for i := len(bodies) - 1; i >= 0; i-- {
		if got := secondRun.planFor(bodies[i]); got != plansInOrder[i] {
			t.Fatalf("request %d: second run planned %+v, first run planned %+v", i, got, plansInOrder[i])
		}
	}
}

// TestDifferentSeedsGiveDifferentPlans checks the seed actually matters.
func TestDifferentSeedsGiveDifferentPlans(t *testing.T) {
	t.Parallel()
	bodies := requestBodies(50)
	seedOne := newTestServer(t, seededConfig(1))
	seedTwo := newTestServer(t, seededConfig(2))

	differences := 0
	for _, body := range bodies {
		if seedOne.planFor(body) != seedTwo.planFor(body) {
			differences++
		}
	}
	if differences == 0 {
		t.Error("seeds 1 and 2 planned all 50 requests identically")
	}
}

// TestSeededRetryGetsAFreshRoll checks that retrying the same request does
// not repeat the same outcome forever, so a failed request can still
// succeed on a later attempt.
func TestSeededRetryGetsAFreshRoll(t *testing.T) {
	t.Parallel()
	server := newTestServer(t, seededConfig(7))
	body := requestBodies(1)[0]

	outcomes := map[outcome]bool{}
	for range 30 {
		outcomes[server.planFor(body).outcome] = true
	}
	if !outcomes[outcomeSuccess] || len(outcomes) < 2 {
		t.Errorf("30 attempts of one request gave outcomes %v, want a mix including success", outcomes)
	}
}

// TestSeededRunMatchesConfiguredRates checks seeding keeps the error and
// crash rates roughly as configured (30% and 20% here).
func TestSeededRunMatchesConfiguredRates(t *testing.T) {
	t.Parallel()
	server := newTestServer(t, seededConfig(99))

	counts := map[outcome]int{}
	for _, body := range requestBodies(5000) {
		counts[server.planFor(body).outcome]++
	}
	if errorCount := counts[outcomeError]; errorCount < 1350 || errorCount > 1650 {
		t.Errorf("errors = %d of 5000, want about 1500", errorCount)
	}
	if crashCount := counts[outcomeCrash]; crashCount < 850 || crashCount > 1150 {
		t.Errorf("crashes = %d of 5000, want about 1000", crashCount)
	}
}
