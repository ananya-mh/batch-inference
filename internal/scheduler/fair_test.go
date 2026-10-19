// Tests for shareSlots, the fair slot-sharing rule, without a database.
package scheduler

import (
	"fmt"
	"testing"
)

// pendingRequests returns count candidates for tenant, with the given number
// already in flight and seqs starting at firstSeq.
func pendingRequests(tenant string, inFlight, count int, firstSeq int64) []candidate {
	candidates := make([]candidate, count)
	for i := range candidates {
		candidates[i] = candidate{
			TenantID: tenant,
			InFlight: inFlight,
			BatchID:  "batch_" + tenant,
			CustomID: fmt.Sprintf("%s-%d", tenant, i),
			Seq:      firstSeq + int64(i),
		}
	}
	return candidates
}

// sortedBySeq merges candidate lists into one list sorted by seq, as
// loadCandidates returns them.
func sortedBySeq(lists ...[]candidate) []candidate {
	var all []candidate
	for _, list := range lists {
		all = append(all, list...)
	}
	for i := 1; i < len(all); i++ {
		for j := i; j > 0 && all[j].Seq < all[j-1].Seq; j-- {
			all[j], all[j-1] = all[j-1], all[j]
		}
	}
	return all
}

// customIDs returns the custom_ids of picked requests, in order.
func customIDs(picked []requestKey) []string {
	ids := make([]string, len(picked))
	for i, key := range picked {
		ids[i] = key.CustomID
	}
	return ids
}

// TestOnlyTenantGetsAllSlots checks a tenant alone gets every free slot.
func TestOnlyTenantGetsAllSlots(t *testing.T) {
	t.Parallel()
	picked := shareSlots(pendingRequests("a", 0, 100, 1), 32)
	if len(picked) != 32 {
		t.Fatalf("picked %d, want all 32 slots", len(picked))
	}
	if picked[0].CustomID != "a-0" || picked[31].CustomID != "a-31" {
		t.Errorf("picked %v..%v, want a-0..a-31 (oldest first)", picked[0].CustomID, picked[31].CustomID)
	}
}

// TestTiedTenantsTakeTurnsOldestFirst checks tenants with equal in-flight
// counts alternate, starting with the one whose request waited longest.
func TestTiedTenantsTakeTurnsOldestFirst(t *testing.T) {
	t.Parallel()
	// b's requests arrived before a's.
	candidates := sortedBySeq(pendingRequests("a", 0, 10, 100), pendingRequests("b", 0, 10, 1))

	got := fmt.Sprint(customIDs(shareSlots(candidates, 4)))
	if want := "[b-0 a-0 b-1 a-1]"; got != want {
		t.Errorf("picked %s, want %s", got, want)
	}
}

// TestNewTenantGetsFreedSlots checks a newly arrived tenant with nothing in
// flight gets slots before a tenant that already has many.
func TestNewTenantGetsFreedSlots(t *testing.T) {
	t.Parallel()
	// a has 28 of 32 slots and a big backlog that arrived first; b just arrived.
	candidates := sortedBySeq(pendingRequests("a", 28, 50, 1), pendingRequests("b", 0, 10, 1000))

	got := fmt.Sprint(customIDs(shareSlots(candidates, 4)))
	if want := "[b-0 b-1 b-2 b-3]"; got != want {
		t.Errorf("picked %s, want %s (all freed slots to the new tenant)", got, want)
	}
}

// TestSlotsEvenOutAcrossTenants checks that once counts are level, three
// tenants share slots evenly.
func TestSlotsEvenOutAcrossTenants(t *testing.T) {
	t.Parallel()
	candidates := sortedBySeq(
		pendingRequests("a", 6, 50, 1),
		pendingRequests("b", 2, 50, 100),
		pendingRequests("c", 0, 50, 200),
	)

	counts := map[string]int{}
	for _, key := range shareSlots(candidates, 10) {
		counts[key.BatchID]++
	}
	// After 10 more slots, in flight is a 6+0, b 2+4, c 0+6: level at 6.
	if counts["batch_a"] != 0 || counts["batch_b"] != 4 || counts["batch_c"] != 6 {
		t.Errorf("slots per tenant = %v, want a 0, b 4, c 6", counts)
	}
}

// TestTenantWithLittleWorkDoesNotHoldSlots checks a tenant runs out of
// requests and the rest of the slots go to others.
func TestTenantWithLittleWorkDoesNotHoldSlots(t *testing.T) {
	t.Parallel()
	candidates := sortedBySeq(pendingRequests("a", 0, 50, 1), pendingRequests("b", 0, 2, 100))

	counts := map[string]int{}
	for _, key := range shareSlots(candidates, 10) {
		counts[key.BatchID]++
	}
	if counts["batch_b"] != 2 || counts["batch_a"] != 8 {
		t.Errorf("slots per tenant = %v, want b 2 (all it has), a 8", counts)
	}
}

// TestNoCandidatesPicksNothing checks the empty case.
func TestNoCandidatesPicksNothing(t *testing.T) {
	t.Parallel()
	if picked := shareSlots(nil, 8); len(picked) != 0 {
		t.Errorf("picked %v from nothing", picked)
	}
}
