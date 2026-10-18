// Package tenants stores tenants and their API keys, and finds which tenant
// an API key belongs to.
package tenants

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ananya-mh/batch-inference/internal/database"
)

// Seed is a tenant and API key to create when the server starts. There is
// no sign-up (it is out of scope), so this is how development and
// experiment tenants get created.
type Seed struct {
	TenantID string
	APIKey   string
}

// ParseSeedList reads a list like "tenant_a=sk-a,tenant_b=sk-b".
func ParseSeedList(list string) ([]Seed, error) {
	var seeds []Seed
	for _, entry := range strings.Split(list, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		tenantID, apiKey, found := strings.Cut(entry, "=")
		if !found || tenantID == "" || apiKey == "" {
			return nil, fmt.Errorf("tenant entry %q must look like tenant_id=api_key", entry)
		}
		seeds = append(seeds, Seed{TenantID: tenantID, APIKey: apiKey})
	}
	return seeds, nil
}

// Create adds the tenant and its API key. Running it again with the same
// values does nothing, so it is safe on every server start.
func Create(ctx context.Context, pool *pgxpool.Pool, tenantID, apiKey string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin creating tenant: %w", err)
	}
	defer database.RollbackUnlessCommitted(ctx, tx)

	if _, err := tx.Exec(ctx,
		"INSERT INTO tenants (id, name) VALUES ($1, $1) ON CONFLICT (id) DO NOTHING", tenantID,
	); err != nil {
		return fmt.Errorf("insert tenant %s: %w", tenantID, err)
	}

	keyHash := hashAPIKey(apiKey)
	if _, err := tx.Exec(ctx,
		"INSERT INTO api_keys (key_hash, tenant_id) VALUES ($1, $2) ON CONFLICT (key_hash) DO NOTHING",
		keyHash, tenantID,
	); err != nil {
		return fmt.Errorf("insert API key for %s: %w", tenantID, err)
	}

	// If the key already existed, make sure it is this tenant's. Quietly
	// leaving it with another tenant would let one tenant see another's data.
	var owner string
	if err := tx.QueryRow(ctx, "SELECT tenant_id FROM api_keys WHERE key_hash = $1", keyHash).Scan(&owner); err != nil {
		return fmt.Errorf("check API key owner: %w", err)
	}
	if owner != tenantID {
		return fmt.Errorf("API key for %s already belongs to tenant %s", tenantID, owner)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tenant %s: %w", tenantID, err)
	}
	return nil
}

// FindByAPIKey returns the ID of the tenant that owns apiKey. found is false
// when no tenant has that key.
func FindByAPIKey(ctx context.Context, pool *pgxpool.Pool, apiKey string) (tenantID string, found bool, err error) {
	err = pool.QueryRow(ctx,
		"SELECT tenant_id FROM api_keys WHERE key_hash = $1", hashAPIKey(apiKey),
	).Scan(&tenantID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("look up API key: %w", err)
	}
	return tenantID, true, nil
}

// hashAPIKey returns the SHA-256 of a key as hex. Only hashes are stored, so
// a leaked database does not leak working keys.
func hashAPIKey(apiKey string) string {
	sum := sha256.Sum256([]byte(apiKey))
	return hex.EncodeToString(sum[:])
}
