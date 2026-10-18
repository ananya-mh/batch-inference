// Package testfixtures creates tenants, input files, and batches for tests,
// so each test can set up a realistic starting point in a line or two.
package testfixtures

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ananya-mh/batch-inference/internal/batches"
	"github.com/ananya-mh/batch-inference/internal/files"
	"github.com/ananya-mh/batch-inference/internal/tenants"
)

// Line returns a valid batch input line for customID. The prompt includes
// customID, so every line's body is different (the fake model's seed relies
// on that).
func Line(customID string) string {
	return fmt.Sprintf(`{"custom_id": %q, "method": "POST", "url": "/v1/chat/completions", "body": {"model": "fake", "messages": [{"role": "user", "content": "classify ticket %s"}]}}`, customID, customID)
}

// CreateTenant creates a tenant with the API key "sk-" + tenantID. Creating
// the same tenant twice is fine.
func CreateTenant(t *testing.T, pool *pgxpool.Pool, tenantID string) {
	t.Helper()
	if err := tenants.Create(context.Background(), pool, tenantID, "sk-"+tenantID); err != nil {
		t.Fatalf("create tenant %s: %v", tenantID, err)
	}
}

// CreateBatch creates the tenant (if needed), an input file with lineCount
// lines with custom_ids "<prefix>-0", "<prefix>-1", ..., and a 24h batch from
// it. The batch starts in validating, as after POST /v1/batches.
func CreateBatch(t *testing.T, pool *pgxpool.Pool, tenantID, prefix string, lineCount int) batches.Batch {
	t.Helper()
	ctx := context.Background()
	CreateTenant(t, pool, tenantID)

	var content strings.Builder
	for i := range lineCount {
		content.WriteString(Line(fmt.Sprintf("%s-%d", prefix, i)) + "\n")
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	file, err := files.CreateBatchInput(ctx, tx, tenantID, prefix+".jsonl", strings.NewReader(content.String()))
	if err != nil {
		t.Fatalf("create input file: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit input file: %v", err)
	}

	batch, err := batches.Create(ctx, pool, batches.NewBatch{
		TenantID:         tenantID,
		InputFileID:      file.ID,
		Endpoint:         files.ChatCompletionsURL,
		CompletionWindow: "24h",
		WindowLength:     24 * time.Hour,
		RequestTotal:     file.LineCount,
	})
	if err != nil {
		t.Fatalf("create batch: %v", err)
	}
	return batch
}

// CountRequests returns how many of a batch's requests have each status.
func CountRequests(t *testing.T, pool *pgxpool.Pool, batchID string) map[string]int {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		"SELECT status, count(*) FROM requests WHERE batch_id = $1 GROUP BY status", batchID)
	if err != nil {
		t.Fatalf("count requests: %v", err)
	}
	defer rows.Close()

	counts := map[string]int{}
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			t.Fatalf("scan request count: %v", err)
		}
		counts[status] = count
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read request counts: %v", err)
	}
	return counts
}

// BatchStatus returns a batch's current status.
func BatchStatus(t *testing.T, pool *pgxpool.Pool, batchID string) batches.Status {
	t.Helper()
	var status batches.Status
	if err := pool.QueryRow(context.Background(), "SELECT status FROM batches WHERE id = $1", batchID).Scan(&status); err != nil {
		t.Fatalf("read batch status: %v", err)
	}
	return status
}
