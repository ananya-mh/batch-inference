-- Batches: one row per batch, with its status and request counts.
--
-- Status only changes through compare-and-set updates in
-- internal/batches (see DESIGN.md). The CHECK below makes sure no code can
-- store a status that does not exist.

CREATE TABLE batches (
    id                TEXT PRIMARY KEY,
    tenant_id         TEXT NOT NULL REFERENCES tenants (id),
    input_file_id     TEXT NOT NULL REFERENCES files (id),
    endpoint          TEXT NOT NULL,
    completion_window TEXT NOT NULL,
    status            TEXT NOT NULL CHECK (status IN (
                          'validating', 'in_progress', 'finalizing', 'completed',
                          'failed', 'expired', 'cancelling', 'cancelled')),
    output_file_id    TEXT REFERENCES files (id),
    error_file_id     TEXT REFERENCES files (id),
    -- Free-form string key/value pairs the client attached, as in OpenAI.
    metadata          JSONB,

    request_total     INTEGER NOT NULL,
    request_completed INTEGER NOT NULL DEFAULT 0,
    request_failed    INTEGER NOT NULL DEFAULT 0,

    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at        TIMESTAMPTZ NOT NULL,
    in_progress_at    TIMESTAMPTZ,
    finalizing_at     TIMESTAMPTZ,
    completed_at      TIMESTAMPTZ,
    failed_at         TIMESTAMPTZ,
    expired_at        TIMESTAMPTZ,
    cancelling_at     TIMESTAMPTZ,
    cancelled_at      TIMESTAMPTZ
);

-- Listing shows a tenant's batches newest first, paging by (created_at, id).
CREATE INDEX batches_tenant_list_idx ON batches (tenant_id, created_at DESC, id DESC);
