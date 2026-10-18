-- Requests (one row per batch line) and their results. See "Fan-out and
-- fair scheduling" in DESIGN.md.

-- Completed and failed counts are now counted from the requests table when a
-- batch is read. Counters on the batch row would make every worker update
-- the same row, and workers already lock that row briefly in their start
-- check, so they would queue up behind each other.
ALTER TABLE batches
    DROP COLUMN request_completed,
    DROP COLUMN request_failed;

CREATE TABLE requests (
    batch_id     TEXT NOT NULL REFERENCES batches (id),
    custom_id    TEXT NOT NULL,
    tenant_id    TEXT NOT NULL REFERENCES tenants (id),
    line_number  INTEGER NOT NULL,
    -- Goes up as rows are created, so it records arrival order across all
    -- batches. "Oldest first" means lowest seq first.
    seq          BIGINT GENERATED ALWAYS AS IDENTITY,
    status       TEXT NOT NULL DEFAULT 'pending' CHECK (status IN (
                     'pending', 'queued', 'running',
                     'succeeded', 'failed', 'cancelled', 'expired')),
    -- The River job running this request, once it has one.
    river_job_id BIGINT,
    queued_at    TIMESTAMPTZ,
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    PRIMARY KEY (batch_id, custom_id)
);

-- The scheduler picks the oldest pending requests, overall and per tenant.
-- Partial indexes keep these small: only pending rows are in them.
CREATE INDEX requests_pending_idx ON requests (seq) WHERE status = 'pending';
CREATE INDEX requests_pending_by_tenant_idx ON requests (tenant_id, seq) WHERE status = 'pending';
-- Counting what is in flight, overall and per tenant.
CREATE INDEX requests_in_flight_idx ON requests (tenant_id) WHERE status IN ('queued', 'running');
-- Counting a batch's requests by status (progress, and "is it finished?").
CREATE INDEX requests_batch_status_idx ON requests (batch_id, status);

CREATE TABLE results (
    batch_id      TEXT NOT NULL,
    custom_id     TEXT NOT NULL,
    succeeded     BOOLEAN NOT NULL,
    -- The model's full reply, for succeeded requests.
    response      JSONB,
    -- Why it failed, for failed requests.
    error_code    TEXT,
    error_message TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Guarantee 2: at most one result per request, enforced by the database
    -- itself even if the code had a bug.
    PRIMARY KEY (batch_id, custom_id),
    FOREIGN KEY (batch_id, custom_id) REFERENCES requests (batch_id, custom_id)
);
