// Tests that the integrity checks really notice broken data, so a clean
// result from them means something.
package testfixtures

import (
	"context"
	"strings"
	"testing"

	"github.com/ananya-mh/batch-inference/internal/testdb"
)

// TestIntegrityChecksNoticeBrokenData breaks one invariant at a time on a fresh
// database and checks the matching integrity check reports it.
func TestIntegrityChecksNoticeBrokenData(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		breakIt   string // SQL that breaks the invariant; $1 is the batch ID
		wantCheck string
	}{
		{"queued request without a job",
			`INSERT INTO requests (batch_id, custom_id, tenant_id, line_number, status) VALUES ($1, 'x-0', 'tenant_a', 1, 'queued')`,
			"every queued or running request has an existing River job"},
		{"succeeded request without a result",
			`INSERT INTO requests (batch_id, custom_id, tenant_id, line_number, status) VALUES ($1, 'x-0', 'tenant_a', 1, 'succeeded')`,
			"results exist exactly for succeeded and failed requests"},
		{"completed batch with a pending request",
			`WITH r AS (INSERT INTO requests (batch_id, custom_id, tenant_id, line_number) VALUES ($1, 'x-0', 'tenant_a', 1))
			 UPDATE batches SET status = 'completed' WHERE id = $1`,
			"a finished batch has no unfinished requests"},
		{"batch with two endings",
			`UPDATE batches SET completed_at = now(), expired_at = now() WHERE id = $1`,
			"a batch reached at most one ending"},
		{"file with a wrong line count",
			`UPDATE files SET line_count = line_count + 1 WHERE id = (SELECT input_file_id FROM batches WHERE id = $1)`,
			"file line counts match their lines"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool := testdb.New(t)
			ctx := context.Background()
			batch := CreateBatch(t, pool, "tenant_a", "x", 1)

			if problems, err := IntegrityProblems(ctx, pool); err != nil || len(problems) != 0 {
				t.Fatalf("clean database: problems %v, err %v; want none", problems, err)
			}
			if _, err := pool.Exec(ctx, tc.breakIt, batch.ID); err != nil {
				t.Fatalf("break invariant: %v", err)
			}
			problems, err := IntegrityProblems(ctx, pool)
			if err != nil {
				t.Fatalf("IntegrityProblems: %v", err)
			}
			found := false
			for _, problem := range problems {
				if strings.HasPrefix(problem, tc.wantCheck) {
					found = true
				}
			}
			if !found {
				t.Errorf("problems %v, want one from %q", problems, tc.wantCheck)
			}
		})
	}
}
