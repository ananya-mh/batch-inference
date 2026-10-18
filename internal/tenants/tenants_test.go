// Tests for creating tenants and looking up API keys.
package tenants

import (
	"context"
	"testing"

	"github.com/ananya-mh/batch-inference/internal/testdb"
)

// TestCreateThenFindByAPIKey checks a created key leads back to its tenant,
// and an unknown key finds nothing.
func TestCreateThenFindByAPIKey(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	ctx := context.Background()

	if err := Create(ctx, pool, "tenant_a", "sk-a"); err != nil {
		t.Fatalf("Create: %v", err)
	}

	tenantID, found, err := FindByAPIKey(ctx, pool, "sk-a")
	if err != nil || !found || tenantID != "tenant_a" {
		t.Errorf("FindByAPIKey(sk-a) = %q, %v, %v; want tenant_a, true, nil", tenantID, found, err)
	}

	_, found, err = FindByAPIKey(ctx, pool, "sk-unknown")
	if err != nil || found {
		t.Errorf("FindByAPIKey(sk-unknown) found = %v, err = %v; want false, nil", found, err)
	}
}

// TestCreateTwiceIsSafe checks server restarts can re-create seed tenants.
func TestCreateTwiceIsSafe(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	ctx := context.Background()

	for range 2 {
		if err := Create(ctx, pool, "tenant_a", "sk-a"); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}
}

// TestCreateRefusesAnotherTenantsKey checks one key can never be shared by two
// tenants, which would let them see each other's files.
func TestCreateRefusesAnotherTenantsKey(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	ctx := context.Background()

	if err := Create(ctx, pool, "tenant_a", "sk-shared"); err != nil {
		t.Fatalf("Create tenant_a: %v", err)
	}
	if err := Create(ctx, pool, "tenant_b", "sk-shared"); err == nil {
		t.Fatal("Create tenant_b with tenant_a's key succeeded, want an error")
	}

	tenantID, _, err := FindByAPIKey(ctx, pool, "sk-shared")
	if err != nil || tenantID != "tenant_a" {
		t.Errorf("key now belongs to %q (err %v), want tenant_a", tenantID, err)
	}
}

// TestParseSeedList checks the SEED_TENANTS format.
func TestParseSeedList(t *testing.T) {
	t.Parallel()
	seeds, err := ParseSeedList(" tenant_a=sk-a, tenant_b=sk-b ,")
	if err != nil {
		t.Fatalf("ParseSeedList: %v", err)
	}
	want := []Seed{{TenantID: "tenant_a", APIKey: "sk-a"}, {TenantID: "tenant_b", APIKey: "sk-b"}}
	if len(seeds) != len(want) || seeds[0] != want[0] || seeds[1] != want[1] {
		t.Errorf("seeds = %+v, want %+v", seeds, want)
	}

	for _, bad := range []string{"tenant_a", "=sk-a", "tenant_a="} {
		if _, err := ParseSeedList(bad); err == nil {
			t.Errorf("ParseSeedList(%q) succeeded, want an error", bad)
		}
	}
}
