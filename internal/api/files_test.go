// Tests for the Files API over HTTP: auth, uploads, size limits, tenant
// isolation, and downloads.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ananya-mh/batch-inference/internal/tenants"
	"github.com/ananya-mh/batch-inference/internal/testdb"
)

// API keys of the two tenants every test in this file starts with.
const (
	keyTenantA = "sk-test-a"
	keyTenantB = "sk-test-b"
)

// batchLine returns a valid batch input line with the given custom_id.
func batchLine(customID string) string {
	return `{"custom_id": "` + customID + `", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "fake", "messages": [{"role": "user", "content": "hi"}]}}`
}

// newTestAPI returns the API handler on a fresh database with tenant_a and
// tenant_b, and the given upload limit.
func newTestAPI(t *testing.T, maxUploadBytes int64) http.Handler {
	t.Helper()
	handler, _ := newTestAPIWithPool(t, maxUploadBytes)
	return handler
}

// newTestAPIWithPool is newTestAPI, also returning the database pool for
// tests that need to set up or inspect rows directly.
func newTestAPIWithPool(t *testing.T, maxUploadBytes int64) (http.Handler, *pgxpool.Pool) {
	t.Helper()
	pool := testdb.New(t)
	for tenantID, key := range map[string]string{"tenant_a": keyTenantA, "tenant_b": keyTenantB} {
		if err := tenants.Create(context.Background(), pool, tenantID, key); err != nil {
			t.Fatalf("create %s: %v", tenantID, err)
		}
	}
	config := testConfig()
	config.MaxUploadBytes = maxUploadBytes
	return NewServer(pool, newRiverClient(t, pool), config).Handler(), pool
}

// formField is one field of a multipart upload, in the order it is sent.
type formField struct {
	name     string
	filename string // empty for plain fields
	value    string
}

// multipartBody encodes fields as multipart/form-data and returns the body
// and its Content-Type header.
func multipartBody(t *testing.T, fields ...formField) (*bytes.Buffer, string) {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for _, field := range fields {
		var part io.Writer
		var err error
		if field.filename != "" {
			part, err = writer.CreateFormFile(field.name, field.filename)
		} else {
			part, err = writer.CreateFormField(field.name)
		}
		if err != nil {
			t.Fatalf("create form field %s: %v", field.name, err)
		}
		if _, err := io.WriteString(part, field.value); err != nil {
			t.Fatalf("write form field %s: %v", field.name, err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}
	return &body, writer.FormDataContentType()
}

// upload posts fields to /v1/files as the tenant with apiKey.
func upload(t *testing.T, handler http.Handler, apiKey string, fields ...formField) *httptest.ResponseRecorder {
	t.Helper()
	body, contentType := multipartBody(t, fields...)
	request := httptest.NewRequest(http.MethodPost, "/v1/files", body)
	request.Header.Set("Content-Type", contentType)
	request.Header.Set("Authorization", "Bearer "+apiKey)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

// get sends a GET to path as the tenant with apiKey.
func get(t *testing.T, handler http.Handler, apiKey, path string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	request.Header.Set("Authorization", "Bearer "+apiKey)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

// uploadBatchFile uploads content as a valid batch file and returns its ID.
func uploadBatchFile(t *testing.T, handler http.Handler, apiKey, content string) string {
	t.Helper()
	response := upload(t, handler, apiKey,
		formField{name: "purpose", value: "batch"},
		formField{name: "file", filename: "tickets.jsonl", value: content},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("upload: status %d, body %s", response.Code, response.Body)
	}
	var file fileObject
	if err := json.Unmarshal(response.Body.Bytes(), &file); err != nil {
		t.Fatalf("decode upload response: %v", err)
	}
	return file.ID
}

// errorMessage returns the message from an OpenAI-format error response.
func errorMessage(t *testing.T, response *httptest.ResponseRecorder) string {
	t.Helper()
	var body apiError
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body %q: %v", response.Body, err)
	}
	return body.Error.Message
}

// TestUploadReturnsFileObject checks a good upload returns the file in
// OpenAI's format.
func TestUploadReturnsFileObject(t *testing.T) {
	t.Parallel()
	handler := newTestAPI(t, DefaultMaxUploadBytes)
	content := batchLine("a") + "\n" + batchLine("b") + "\n"

	response := upload(t, handler, keyTenantA,
		formField{name: "purpose", value: "batch"},
		formField{name: "file", filename: "tickets.jsonl", value: content},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", response.Code, response.Body)
	}

	var file fileObject
	if err := json.Unmarshal(response.Body.Bytes(), &file); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !strings.HasPrefix(file.ID, "file-") || file.Object != "file" || file.Purpose != "batch" ||
		file.Filename != "tickets.jsonl" || file.Bytes != int64(len(content)) {
		t.Errorf("unexpected file object: %+v", file)
	}
}

// TestUploadAcceptsFileBeforePurpose checks the Node SDK's field order
// (file first, then purpose) works too.
func TestUploadAcceptsFileBeforePurpose(t *testing.T) {
	t.Parallel()
	handler := newTestAPI(t, DefaultMaxUploadBytes)

	response := upload(t, handler, keyTenantA,
		formField{name: "file", filename: "tickets.jsonl", value: batchLine("a")},
		formField{name: "purpose", value: "batch"},
	)
	if response.Code != http.StatusOK {
		t.Errorf("status = %d, body %s", response.Code, response.Body)
	}
}

// TestUploadRejectsBadForms checks missing or wrong form fields get 400, and
// that a file sent before a wrong purpose is not kept.
func TestUploadRejectsBadForms(t *testing.T) {
	t.Parallel()
	handler := newTestAPI(t, DefaultMaxUploadBytes)

	cases := map[string][]formField{
		"missing purpose": {{name: "file", filename: "f.jsonl", value: batchLine("a")}},
		"wrong purpose":   {{name: "file", filename: "f.jsonl", value: batchLine("a")}, {name: "purpose", value: "fine-tune"}},
		"missing file":    {{name: "purpose", value: "batch"}},
		"two files": {
			{name: "purpose", value: "batch"},
			{name: "file", filename: "f.jsonl", value: batchLine("a")},
			{name: "file", filename: "g.jsonl", value: batchLine("b")},
		},
	}
	for name, fields := range cases {
		t.Run(name, func(t *testing.T) {
			if response := upload(t, handler, keyTenantA, fields...); response.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400; body %s", response.Code, response.Body)
			}
		})
	}
}

// TestUploadReportsWhichLineFailed checks an invalid line is rejected with
// its line number in the message.
func TestUploadReportsWhichLineFailed(t *testing.T) {
	t.Parallel()
	handler := newTestAPI(t, DefaultMaxUploadBytes)
	content := batchLine("a") + "\n" + batchLine("b") + "\n" + batchLine("a") + "\n"

	response := upload(t, handler, keyTenantA,
		formField{name: "purpose", value: "batch"},
		formField{name: "file", filename: "f.jsonl", value: content},
	)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.Code)
	}
	want := `invalid batch file: line 3: duplicate custom_id "a" (first used on line 1)`
	if message := errorMessage(t, response); message != want {
		t.Errorf("message = %q, want %q", message, want)
	}
}

// TestUploadOverLimitIsRejected checks the upload size limit, both when the
// client declares the size up front and when it streams without one.
func TestUploadOverLimitIsRejected(t *testing.T) {
	t.Parallel()
	// The limit must be well above the multipart reader's 4 KB buffer, so the
	// streaming case is cut off in the middle of the file's lines rather than
	// while reading the form's headers.
	const limit = 64 * 1024
	handler := newTestAPI(t, limit)
	// About 450 KB of valid, unique lines: far over the limit.
	var content strings.Builder
	for i := range 3000 {
		content.WriteString(batchLine(fmt.Sprintf("ticket-%d", i)) + "\n")
	}

	for _, sizeKnown := range []bool{true, false} {
		body, contentType := multipartBody(t,
			formField{name: "purpose", value: "batch"},
			formField{name: "file", filename: "big.jsonl", value: content.String()},
		)
		request := httptest.NewRequest(http.MethodPost, "/v1/files", body)
		request.Header.Set("Content-Type", contentType)
		request.Header.Set("Authorization", "Bearer "+keyTenantA)
		if !sizeKnown {
			// -1 means "unknown length", as with a chunked upload, so
			// only the streaming limit can catch it.
			request.ContentLength = -1
		}

		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusRequestEntityTooLarge {
			t.Errorf("size known = %v: status = %d, want 413; body %s", sizeKnown, response.Code, response.Body)
		}
	}
}

// TestFilesAPIRequiresValidKey checks requests without a key, or with an
// unknown key, are refused.
func TestFilesAPIRequiresValidKey(t *testing.T) {
	t.Parallel()
	handler := newTestAPI(t, DefaultMaxUploadBytes)

	noKey := httptest.NewRecorder()
	handler.ServeHTTP(noKey, httptest.NewRequest(http.MethodGet, "/v1/files/file-x", nil))
	if noKey.Code != http.StatusUnauthorized {
		t.Errorf("no key: status = %d, want 401", noKey.Code)
	}

	if response := get(t, handler, "sk-wrong", "/v1/files/file-x"); response.Code != http.StatusUnauthorized {
		t.Errorf("wrong key: status = %d, want 401", response.Code)
	}
}

// TestOwnerCanGetAndDownloadFile checks a tenant can read back its own file,
// byte for byte.
func TestOwnerCanGetAndDownloadFile(t *testing.T) {
	t.Parallel()
	handler := newTestAPI(t, DefaultMaxUploadBytes)
	content := batchLine("a") + "\n" + batchLine("b") + "\n"
	fileID := uploadBatchFile(t, handler, keyTenantA, content)

	if response := get(t, handler, keyTenantA, "/v1/files/"+fileID); response.Code != http.StatusOK {
		t.Errorf("get file: status = %d, want 200", response.Code)
	}

	download := get(t, handler, keyTenantA, "/v1/files/"+fileID+"/content")
	if download.Code != http.StatusOK {
		t.Fatalf("download: status = %d, want 200", download.Code)
	}
	if download.Body.String() != content {
		t.Errorf("downloaded content differs:\n got: %q\nwant: %q", download.Body.String(), content)
	}
}

// TestTenantCannotSeeAnotherTenantsFile checks tenant isolation: tenant B
// gets 404 for tenant A's file, exactly as for a file that does not exist,
// so B cannot even learn the file exists.
func TestTenantCannotSeeAnotherTenantsFile(t *testing.T) {
	t.Parallel()
	handler := newTestAPI(t, DefaultMaxUploadBytes)
	fileID := uploadBatchFile(t, handler, keyTenantA, batchLine("secret")+"\n")

	for _, path := range []string{"/v1/files/" + fileID, "/v1/files/" + fileID + "/content"} {
		response := get(t, handler, keyTenantB, path)
		if response.Code != http.StatusNotFound {
			t.Errorf("tenant B GET %s: status = %d, want 404", path, response.Code)
		}
		if strings.Contains(response.Body.String(), "secret") {
			t.Errorf("tenant B GET %s: response leaked tenant A's content", path)
		}

		missing := get(t, handler, keyTenantB, strings.Replace(path, fileID, "file-doesnotexist", 1))
		if response.Body.String() != missing.Body.String() {
			t.Errorf("tenant B GET %s: response %q differs from missing-file response %q", path, response.Body, missing.Body)
		}
	}
}
