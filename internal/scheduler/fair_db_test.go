// Tests for fair picking against a real database: a lone tenant gets the
// whole window, and a newly arriving tenant gets freed slots ahead of a
// tenant with a big backlog (unlike first come, first served).
package scheduler_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ananya-mh/batch-inference/internal/testdb"
	"github.com/ananya-mh/batch-inference/internal/testfixtures"
)

// inFlightByTenant returns how many requests each tenant has in flight.
func inFlightByTenant(t *testing.T, pool *pgxpool.Pool) map[string]int {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		"SELECT tenant_id, count(*) FROM requests WHERE status IN ('queued', 'running') GROUP BY tenant_id")
	if err != nil {
		t.Fatalf("count in flight: %v", err)
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var tenant string
		var count int
		if err := rows.Scan(&tenant, &count); err != nil {
			t.Fatalf("scan: %v", err)
		}
		counts[tenant] = count
	}
	return counts
}

// TestLoneTenantGetsWholeWindow checks that when only one tenant has work,
// fair scheduling still fills the whole window with it, pass after pass.
func TestLoneTenantGetsWholeWindow(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	testfixtures.CreateBatch(t, pool, "tenant_a", "a", 100)
	s := newFairScheduler(t, pool, 8)

	runPass(t, s)
	for round := range 3 {
		if counts := inFlightByTenant(t, pool); counts["tenant_a"] != 8 {
			t.Fatalf("round %d: tenant_a has %d in flight, want the whole window of 8", round, counts["tenant_a"])
		}
		finishQueued(t, pool, 5)
		runPass(t, s)
	}
}

// TestArrivingTenantGetsFreedSlots fills the window with a big tenant, then lets a
// small tenant arrive. With fair scheduling, freed slots go to the small
// tenant until both have the same share.
func TestArrivingTenantGetsFreedSlots(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	testfixtures.CreateBatch(t, pool, "tenant_big", "big", 200)
	s := newFairScheduler(t, pool, 8)
	runPass(t, s) // tenant_big takes all 8 slots

	testfixtures.CreateBatch(t, pool, "tenant_small", "small", 20)
	finishQueued(t, pool, 3) // three of big's requests finish
	runPass(t, s)
	if counts := inFlightByTenant(t, pool); counts["tenant_small"] != 3 || counts["tenant_big"] != 5 {
		t.Fatalf("in flight = %v, want the 3 freed slots to go to tenant_small", counts)
	}

	// More finish (the oldest first, so big's); shares level out at 4 and 4.
	finishQueued(t, pool, 2)
	runPass(t, s)
	if counts := inFlightByTenant(t, pool); counts["tenant_small"] != 4 || counts["tenant_big"] != 4 {
		t.Errorf("in flight = %v, want 4 each", counts)
	}
}

// TestFirstComeFirstServedStarvesNewTenant is the same scenario without fair
// scheduling: freed slots keep going to the big tenant's older requests.
// This is the behavior fair scheduling exists to fix.
func TestFirstComeFirstServedStarvesNewTenant(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	testfixtures.CreateBatch(t, pool, "tenant_big", "big", 200)
	s := newScheduler(t, pool, 8)
	runPass(t, s)

	testfixtures.CreateBatch(t, pool, "tenant_small", "small", 20)
	for range 5 {
		finishQueued(t, pool, 3)
		runPass(t, s)
	}
	if counts := inFlightByTenant(t, pool); counts["tenant_small"] != 0 {
		t.Errorf("in flight = %v, want tenant_small still waiting behind tenant_big", counts)
	}
}
