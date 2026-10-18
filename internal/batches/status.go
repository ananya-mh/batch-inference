// This file defines the batch statuses and which changes between them are
// allowed. See "Batch status: a strict state machine" in DESIGN.md.
package batches

// Status is where a batch is in its life.
type Status string

// The statuses, using OpenAI's names so the SDKs understand them.
const (
	StatusValidating Status = "validating"
	StatusInProgress Status = "in_progress"
	StatusFinalizing Status = "finalizing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
	StatusExpired    Status = "expired"
	StatusCancelling Status = "cancelling"
	StatusCancelled  Status = "cancelled"
)

// allowedTransitions lists, for each status, the statuses it may move to.
// A status that is missing here is final: nothing may follow it.
var allowedTransitions = map[Status][]Status{
	StatusValidating: {StatusInProgress, StatusFailed, StatusCancelling, StatusExpired},
	StatusInProgress: {StatusFinalizing, StatusCancelling, StatusExpired},
	StatusFinalizing: {StatusCompleted},
	// Cancel always passes through cancelling, so requests that are already
	// running can finish before the batch is called cancelled.
	StatusCancelling: {StatusCancelled},
}

// timestampColumn is the column that records when a batch entered each
// status, matching OpenAI's batch fields (in_progress_at, completed_at, ...).
// validating has none: it is the starting status, recorded by created_at.
var timestampColumn = map[Status]string{
	StatusInProgress: "in_progress_at",
	StatusFinalizing: "finalizing_at",
	StatusCompleted:  "completed_at",
	StatusFailed:     "failed_at",
	StatusExpired:    "expired_at",
	StatusCancelling: "cancelling_at",
	StatusCancelled:  "cancelled_at",
}

// CanTransition reports whether a batch may move from one status to another.
func CanTransition(from, to Status) bool {
	for _, allowed := range allowedTransitions[from] {
		if allowed == to {
			return true
		}
	}
	return false
}

// IsFinal reports whether a status is final: no other status can follow it.
func IsFinal(status Status) bool {
	return len(allowedTransitions[status]) == 0
}
