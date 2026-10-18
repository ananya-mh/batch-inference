// Tests for the Batches API over HTTP: create checks, tenant isolation,
// the batch object's shape, and list paging.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// postBatch sends body to POST /v1/batches as the tenant with apiKey.
func postBatch(t *testing.T, handler http.Handler, apiKey, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/v1/batches", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+apiKey)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

// createBatchBody returns a create request for fileID with the given
// endpoint and window.
func createBatchBody(fileID, endpoint, window string) string {
	return fmt.Sprintf(`{"input_file_id": %q, "endpoint": %q, "completion_window": %q}`, fileID, endpoint, window)
}

// createTestBatch creates a 24h batch as tenant A and returns it.
func createTestBatch(t *testing.T, handler http.Handler, fileID string) batchObject {
	t.Helper()
	response := postBatch(t, handler, keyTenantA, createBatchBody(fileID, "/v1/chat/completions", "24h"))
	if response.Code != http.StatusOK {
		t.Fatalf("create batch: status %d, body %s", response.Code, response.Body)
	}
	var batch batchObject
	if err := json.Unmarshal(response.Body.Bytes(), &batch); err != nil {
		t.Fatalf("decode batch: %v", err)
	}
	return batch
}

// TestCreateBatchReturnsBatchObject checks a good create returns an
// OpenAI-shaped batch with the request total from the file.
func TestCreateBatchReturnsBatchObject(t *testing.T) {
	t.Parallel()
	handler := newTestAPI(t, DefaultMaxUploadBytes)
	fileID := uploadBatchFile(t, handler, keyTenantA, batchLine("a")+"\n"+batchLine("b")+"\n")

	body := `{"input_file_id": "` + fileID + `", "endpoint": "/v1/chat/completions", "completion_window": "24h", "metadata": {"job": "nightly"}}`
	response := postBatch(t, handler, keyTenantA, body)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", response.Code, response.Body)
	}

	// Decode loosely too, to check fields that must be present as null.
	var raw map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, field := range []string{"output_file_id", "error_file_id", "in_progress_at", "completed_at", "cancelled_at", "errors"} {
		if value, present := raw[field]; !present || value != nil {
			t.Errorf("field %q = %v (present %v), want null", field, value, present)
		}
	}

	var batch batchObject
	if err := json.Unmarshal(response.Body.Bytes(), &batch); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !strings.HasPrefix(batch.ID, "batch_") || batch.Object != "batch" || batch.Status != "validating" ||
		batch.InputFileID != fileID || batch.CompletionWindow != "24h" || batch.Metadata["job"] != "nightly" {
		t.Errorf("unexpected batch: %+v", batch)
	}
	if batch.RequestCounts != (requestCounts{Total: 2}) {
		t.Errorf("request_counts = %+v, want total 2", batch.RequestCounts)
	}
	if batch.ExpiresAt-batch.CreatedAt != 24*60*60 {
		t.Errorf("expires_at - created_at = %d seconds, want 86400", batch.ExpiresAt-batch.CreatedAt)
	}
}

// TestCreateBatchUsesConfiguredShortWindow checks a short test window is
// accepted and sets expires_at accordingly.
func TestCreateBatchUsesConfiguredShortWindow(t *testing.T) {
	t.Parallel()
	handler := newTestAPI(t, DefaultMaxUploadBytes)
	fileID := uploadBatchFile(t, handler, keyTenantA, batchLine("a")+"\n")

	response := postBatch(t, handler, keyTenantA, createBatchBody(fileID, "/v1/chat/completions", "2s"))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", response.Code, response.Body)
	}
	var batch batchObject
	if err := json.Unmarshal(response.Body.Bytes(), &batch); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Unix seconds round down, so allow 1 or 2 seconds.
	if gap := batch.ExpiresAt - batch.CreatedAt; gap < 1 || gap > 2 {
		t.Errorf("expires_at - created_at = %d seconds, want about 2", gap)
	}
}

// TestCreateBatchRejectsBadRequests checks each create rule.
func TestCreateBatchRejectsBadRequests(t *testing.T) {
	t.Parallel()
	handler, pool := newTestAPIWithPool(t, DefaultMaxUploadBytes)
	fileOfA := uploadBatchFile(t, handler, keyTenantA, batchLine("a")+"\n")
	fileOfB := uploadBatchFile(t, handler, keyTenantB, batchLine("b")+"\n")

	// Make a file with another purpose directly, since results files (the
	// only other purpose) do not exist yet.
	if _, err := pool.Exec(context.Background(),
		"INSERT INTO files (id, tenant_id, purpose, filename) VALUES ('file-output', 'tenant_a', 'batch_output', 'out.jsonl')",
	); err != nil {
		t.Fatalf("insert output file: %v", err)
	}

	tooMuchMetadata := map[string]string{}
	for i := range 17 {
		tooMuchMetadata[fmt.Sprint("key", i)] = "v"
	}
	tooMuchMetadataJSON, err := json.Marshal(tooMuchMetadata)
	if err != nil {
		t.Fatalf("encode metadata: %v", err)
	}

	cases := map[string]struct {
		body        string
		wantMessage string
	}{
		"not JSON":                {`{oops`, "not valid JSON"},
		"wrong endpoint":          {createBatchBody(fileOfA, "/v1/embeddings", "24h"), `"endpoint" must be`},
		"window not allowed":      {createBatchBody(fileOfA, "/v1/chat/completions", "48h"), `"completion_window" must be one of "24h", "2s"`},
		"missing file":            {createBatchBody("file-nope", "/v1/chat/completions", "24h"), "not found"},
		"another tenant's file":   {createBatchBody(fileOfB, "/v1/chat/completions", "24h"), "not found"},
		"file with wrong purpose": {createBatchBody("file-output", "/v1/chat/completions", "24h"), `purpose "batch_output"`},
		"too much metadata": {
			`{"input_file_id": "` + fileOfA + `", "endpoint": "/v1/chat/completions", "completion_window": "24h", "metadata": ` + string(tooMuchMetadataJSON) + `}`,
			"at most 16 keys",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			response := postBatch(t, handler, keyTenantA, tc.body)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body %s", response.Code, response.Body)
			}
			if message := errorMessage(t, response); !strings.Contains(message, tc.wantMessage) {
				t.Errorf("message = %q, want it to contain %q", message, tc.wantMessage)
			}
		})
	}
}

// TestAnotherTenantsFileLooksMissing checks using another tenant's file gives
// exactly the same error as a file that does not exist.
func TestAnotherTenantsFileLooksMissing(t *testing.T) {
	t.Parallel()
	handler := newTestAPI(t, DefaultMaxUploadBytes)
	fileOfB := uploadBatchFile(t, handler, keyTenantB, batchLine("b")+"\n")

	usingB := postBatch(t, handler, keyTenantA, createBatchBody(fileOfB, "/v1/chat/completions", "24h"))
	missing := postBatch(t, handler, keyTenantA, createBatchBody("file-nope", "/v1/chat/completions", "24h"))
	normalize := func(message string) string {
		return strings.NewReplacer(fileOfB, "ID", "file-nope", "ID").Replace(message)
	}
	if normalize(errorMessage(t, usingB)) != normalize(errorMessage(t, missing)) {
		t.Errorf("another tenant's file: %q; missing file: %q; want the same message", errorMessage(t, usingB), errorMessage(t, missing))
	}
}

// TestGetBatchIsLimitedToOwner checks tenant B gets 404 for tenant A's batch.
func TestGetBatchIsLimitedToOwner(t *testing.T) {
	t.Parallel()
	handler := newTestAPI(t, DefaultMaxUploadBytes)
	batch := createTestBatch(t, handler, uploadBatchFile(t, handler, keyTenantA, batchLine("a")+"\n"))

	if response := get(t, handler, keyTenantA, "/v1/batches/"+batch.ID); response.Code != http.StatusOK {
		t.Errorf("owner: status = %d, want 200", response.Code)
	}
	if response := get(t, handler, keyTenantB, "/v1/batches/"+batch.ID); response.Code != http.StatusNotFound {
		t.Errorf("tenant B: status = %d, want 404", response.Code)
	}
}

// TestListBatchesPagesLikeOpenAI walks the list the way the OpenAI SDK does:
// pass the last ID as "after" while has_more is true.
func TestListBatchesPagesLikeOpenAI(t *testing.T) {
	t.Parallel()
	handler := newTestAPI(t, DefaultMaxUploadBytes)
	fileID := uploadBatchFile(t, handler, keyTenantA, batchLine("a")+"\n")
	var createdNewestFirst []string
	for range 3 {
		createdNewestFirst = append([]string{createTestBatch(t, handler, fileID).ID}, createdNewestFirst...)
	}

	var listed []string
	path := "/v1/batches?limit=2"
	for pageNumber := 1; pageNumber <= 3; pageNumber++ {
		response := get(t, handler, keyTenantA, path)
		if response.Code != http.StatusOK {
			t.Fatalf("page %d: status %d, body %s", pageNumber, response.Code, response.Body)
		}
		var page batchList
		if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
			t.Fatalf("decode page %d: %v", pageNumber, err)
		}
		if page.Object != "list" || page.FirstID == nil || *page.FirstID != page.Data[0].ID ||
			page.LastID == nil || *page.LastID != page.Data[len(page.Data)-1].ID {
			t.Errorf("page %d has wrong list fields: %+v", pageNumber, page)
		}
		for _, batch := range page.Data {
			listed = append(listed, batch.ID)
		}
		if !page.HasMore {
			break
		}
		path = "/v1/batches?limit=2&after=" + *page.LastID
	}
	if strings.Join(listed, ",") != strings.Join(createdNewestFirst, ",") {
		t.Errorf("listed %v, want %v", listed, createdNewestFirst)
	}
}

// TestListBatchesEmptyAndBadParams checks an empty list is [] (not null),
// and bad limit or after values get 400.
func TestListBatchesEmptyAndBadParams(t *testing.T) {
	t.Parallel()
	handler := newTestAPI(t, DefaultMaxUploadBytes)

	response := get(t, handler, keyTenantB, "/v1/batches")
	if want := `{"object":"list","data":[],"first_id":null,"last_id":null,"has_more":false}`; response.Body.String() != want {
		t.Errorf("empty list = %s, want %s", response.Body, want)
	}

	for _, query := range []string{"limit=0", "limit=101", "limit=ten", "after=batch_nope"} {
		if response := get(t, handler, keyTenantA, "/v1/batches?"+query); response.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", query, response.Code)
		}
	}
}
