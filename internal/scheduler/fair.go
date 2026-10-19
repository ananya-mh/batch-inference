// This file picks pending requests fairly across tenants. See "Picking
// fairly" in DESIGN.md.
package scheduler

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ananya-mh/batch-inference/internal/batches"
	"github.com/ananya-mh/batch-inference/internal/requests"
)

// candidate is one pending request that could be admitted.
type candidate struct {
	TenantID string
	InFlight int // how many of this tenant's requests are in flight now
	BatchID  string
	CustomID string
	Seq      int64 // arrival order; lower waited longer
}

// pickFairly returns up to limit pending requests, giving each free slot to
// the tenant with the fewest requests in flight.
func pickFairly(ctx context.Context, tx pgx.Tx, limit int) ([]requestKey, error) {
	candidates, err := loadCandidates(ctx, tx, limit)
	if err != nil {
		return nil, err
	}
	return shareSlots(candidates, limit), nil
}

// loadCandidates returns, for every tenant with pending work, its next
// pending requests (oldest first, at most limit of them) and how many of its
// requests are in flight. Each tenant can get at most limit slots, so there
// is no need to look further; a pass reads about (tenants x limit) rows, not
// every pending request.
func loadCandidates(ctx context.Context, tx pgx.Tx, limit int) ([]candidate, error) {
	rows, err := tx.Query(ctx, `
		SELECT t.tenant_id,
		       (SELECT count(*) FROM requests f
		        WHERE f.tenant_id = t.tenant_id AND f.status IN ($1, $2)) AS in_flight,
		       next.batch_id, next.custom_id, next.seq
		FROM (
			SELECT DISTINCT tenant_id FROM batches
			WHERE status = $3 AND expires_at > clock_timestamp()
		) t
		CROSS JOIN LATERAL (
			SELECT r.batch_id, r.custom_id, r.seq
			FROM requests r
			JOIN batches b ON b.id = r.batch_id
			WHERE r.tenant_id = t.tenant_id AND r.status = $4
			  AND b.status = $3 AND b.expires_at > clock_timestamp()
			ORDER BY r.seq
			LIMIT $5
		) next
		ORDER BY next.seq`,
		requests.StatusQueued, requests.StatusRunning, batches.StatusInProgress, requests.StatusPending, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("load pending requests by tenant: %w", err)
	}
	candidates, err := pgx.CollectRows(rows, pgx.RowToStructByPos[candidate])
	if err != nil {
		return nil, fmt.Errorf("read pending requests by tenant: %w", err)
	}
	return candidates, nil
}

// shareSlots hands out up to limit slots, one at a time. Each slot goes to the
// tenant with the fewest requests in flight (counting slots handed out so
// far). If tenants are tied, the slot goes to the one whose next request has
// waited longest (lowest seq). Within a tenant, requests go oldest first.
//
// candidates must be sorted by seq. This is plain logic with no database, so
// it is easy to test on its own.
func shareSlots(candidates []candidate, limit int) []requestKey {
	// Each tenant's waiting requests, oldest first, and its in-flight count.
	queues := map[string][]candidate{}
	inFlight := map[string]int{}
	var tenants []string
	for _, c := range candidates {
		if _, seen := queues[c.TenantID]; !seen {
			tenants = append(tenants, c.TenantID)
			inFlight[c.TenantID] = c.InFlight
		}
		queues[c.TenantID] = append(queues[c.TenantID], c)
	}

	var picked []requestKey
	for len(picked) < limit {
		chosen := ""
		for _, tenant := range tenants {
			if len(queues[tenant]) == 0 {
				continue
			}
			if chosen == "" || isBetterChoice(tenant, chosen, inFlight, queues) {
				chosen = tenant
			}
		}
		if chosen == "" {
			break // no tenant has anything left to admit
		}

		next := queues[chosen][0]
		queues[chosen] = queues[chosen][1:]
		inFlight[chosen]++
		picked = append(picked, requestKey{BatchID: next.BatchID, CustomID: next.CustomID})
	}
	return picked
}

// isBetterChoice reports whether tenant should get the next slot rather than
// current: it has fewer requests in flight, or the same number and its next
// request has waited longer.
func isBetterChoice(tenant, current string, inFlight map[string]int, queues map[string][]candidate) bool {
	if inFlight[tenant] != inFlight[current] {
		return inFlight[tenant] < inFlight[current]
	}
	return queues[tenant][0].Seq < queues[current][0].Seq
}
