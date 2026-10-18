// Tests for the batch state machine rules.
package batches

import "testing"

// allStatuses lists every status, for checking every possible pair.
var allStatuses = []Status{
	StatusValidating, StatusInProgress, StatusFinalizing, StatusCompleted,
	StatusFailed, StatusExpired, StatusCancelling, StatusCancelled,
}

// TestOnlyListedTransitionsAreAllowed checks all 64 pairs of statuses against
// the exact list of allowed changes, so any accidental change to the rules
// fails this test.
func TestOnlyListedTransitionsAreAllowed(t *testing.T) {
	t.Parallel()
	want := map[[2]Status]bool{
		{StatusValidating, StatusInProgress}: true,
		{StatusValidating, StatusFailed}:     true,
		{StatusValidating, StatusCancelling}: true,
		{StatusValidating, StatusExpired}:    true,
		{StatusInProgress, StatusFinalizing}: true,
		{StatusInProgress, StatusCancelling}: true,
		{StatusInProgress, StatusExpired}:    true,
		{StatusFinalizing, StatusCompleted}:  true,
		{StatusCancelling, StatusCancelled}:  true,
	}

	for _, from := range allStatuses {
		for _, to := range allStatuses {
			if got := CanTransition(from, to); got != want[[2]Status{from, to}] {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", from, to, got, !got)
			}
		}
	}
}

// TestFinalStatuses checks exactly the four end states are final.
func TestFinalStatuses(t *testing.T) {
	t.Parallel()
	final := map[Status]bool{StatusCompleted: true, StatusFailed: true, StatusExpired: true, StatusCancelled: true}
	for _, status := range allStatuses {
		if IsFinal(status) != final[status] {
			t.Errorf("IsFinal(%s) = %v, want %v", status, IsFinal(status), final[status])
		}
	}
}

// TestEveryReachableStatusHasATimestampColumn checks Transition always has a
// column to record the time in.
func TestEveryReachableStatusHasATimestampColumn(t *testing.T) {
	t.Parallel()
	for _, targets := range allowedTransitions {
		for _, to := range targets {
			if timestampColumn[to] == "" {
				t.Errorf("status %s has no timestamp column", to)
			}
		}
	}
}
