// Package testfixtures creates tenants, input files, and batches for tests,
// so each test can set up a realistic starting point in a line or two.
package testfixtures

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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
	return CreateBatchWithWindow(t, pool, tenantID, prefix, lineCount, 24*time.Hour)
}

// CreateBatchWithWindow is CreateBatch with a chosen completion window, for
// tests that need a batch to expire soon.
func CreateBatchWithWindow(t *testing.T, pool *pgxpool.Pool, tenantID, prefix string, lineCount int, window time.Duration) batches.Batch {
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
		CompletionWindow: window.String(),
		WindowLength:     window,
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

// integrityChecks are SQL queries that each return one row per broken
// invariant, described in plain words. Every query must return no rows.
var integrityChecks = []struct {
	name string
	sql  string
}{
	{"every queued or running request has an existing River job for it", `
		SELECT r.batch_id || '/' || r.custom_id || ' is ' || r.status || ' without a matching job'
		FROM requests r LEFT JOIN river_job j ON j.id = r.river_job_id
		WHERE r.status IN ('queued', 'running')
		  AND (j.id IS NULL OR j.args->>'batch_id' <> r.batch_id OR j.args->>'custom_id' <> r.custom_id)`},
	{"no two requests share a River job", `
		SELECT 'job ' || river_job_id || ' is used by ' || count(*) || ' requests'
		FROM requests WHERE river_job_id IS NOT NULL
		GROUP BY river_job_id HAVING count(*) > 1`},
	{"results exist exactly for succeeded and failed requests, and agree with them", `
		SELECT r.batch_id || '/' || r.custom_id || ' is ' || r.status ||
		       CASE WHEN res.batch_id IS NULL THEN ' with no result' ELSE ' with a result (succeeded=' || res.succeeded || ')' END
		FROM requests r
		LEFT JOIN results res ON res.batch_id = r.batch_id AND res.custom_id = r.custom_id
		WHERE (r.status IN ('succeeded', 'failed')) <> (res.batch_id IS NOT NULL)
		   OR (res.batch_id IS NOT NULL AND res.succeeded <> (r.status = 'succeeded'))`},
	{"a finished batch has no unfinished requests", `
		SELECT b.id || ' is ' || b.status || ' with ' || count(*) || ' unfinished requests'
		FROM batches b JOIN requests r ON r.batch_id = b.id
		WHERE b.status IN ('completed', 'cancelled', 'expired', 'failed')
		  AND r.status IN ('pending', 'queued', 'running')
		GROUP BY b.id, b.status`},
	{"a finished batch lists every custom_id exactly once across its two files", `
		SELECT b.id || ' (' || b.status || '): ' || r.custom_id || ' appears ' || count(l.custom_id) || ' times'
		FROM batches b
		JOIN requests r ON r.batch_id = b.id
		LEFT JOIN file_lines l ON l.file_id IN (b.output_file_id, b.error_file_id) AND l.custom_id = r.custom_id
		WHERE b.status IN ('completed', 'cancelled', 'expired')
		GROUP BY b.id, b.status, r.custom_id HAVING count(l.custom_id) <> 1`},
	{"file line counts match their lines", `
		SELECT f.id || ' says ' || f.line_count || ' lines but has ' || count(l.line_number)
		FROM files f LEFT JOIN file_lines l ON l.file_id = f.id
		GROUP BY f.id, f.line_count HAVING f.line_count <> count(l.line_number)`},
	{"a batch reached at most one ending", `
		SELECT b.id || ' has more than one ending time set'
		FROM batches b
		WHERE (b.completed_at IS NOT NULL)::int + (b.expired_at IS NOT NULL)::int
		    + (b.cancelled_at IS NOT NULL)::int + (b.failed_at IS NOT NULL)::int > 1`},
}

// IntegrityProblems checks the invariants that must hold at every moment no
// matter what happened (crashes, races, cancels, expiry), and returns one
// line per broken invariant.
func IntegrityProblems(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	var problems []string
	for _, check := range integrityChecks {
		rows, err := pool.Query(ctx, check.sql)
		if err != nil {
			return nil, fmt.Errorf("integrity check %q: %w", check.name, err)
		}
		found, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return nil, fmt.Errorf("integrity check %q: %w", check.name, err)
		}
		for _, problem := range found {
			problems = append(problems, check.name+": "+problem)
		}
	}
	return problems, nil
}

// CheckIntegrity fails the test for each broken invariant (see
// IntegrityProblems). End-to-end tests run it after their workers stop.
func CheckIntegrity(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	problems, err := IntegrityProblems(context.Background(), pool)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	for i, problem := range problems {
		if i == 10 {
			t.Errorf("integrity: ... and %d more", len(problems)-10)
			break
		}
		t.Errorf("integrity: %s", problem)
	}
}
