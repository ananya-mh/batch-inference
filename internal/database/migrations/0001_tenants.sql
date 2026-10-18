-- Tenants are the customers sharing the service. Each one signs requests with
-- an API key; the key is how we know which tenant a request belongs to.

CREATE TABLE tenants (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Only a SHA-256 hash of each key is stored, so a leaked database does not
-- leak working keys.
CREATE TABLE api_keys (
    key_hash   TEXT PRIMARY KEY,
    tenant_id  TEXT NOT NULL REFERENCES tenants (id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX api_keys_tenant_id_idx ON api_keys (tenant_id);
