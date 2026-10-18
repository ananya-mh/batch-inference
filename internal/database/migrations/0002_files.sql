-- Uploaded batch input files, and later the results and errors files.
--
-- Each file is stored as one row per line (file_lines) instead of one big
-- blob. That way uploads and downloads can stream without holding a whole
-- 100 MB file in memory, and later steps can read single lines.

CREATE TABLE files (
    id         TEXT PRIMARY KEY,
    tenant_id  TEXT NOT NULL REFERENCES tenants (id),
    purpose    TEXT NOT NULL,
    filename   TEXT NOT NULL,
    bytes      BIGINT NOT NULL DEFAULT 0,
    line_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX files_tenant_id_idx ON files (tenant_id);

CREATE TABLE file_lines (
    file_id     TEXT NOT NULL REFERENCES files (id) ON DELETE CASCADE,
    line_number INTEGER NOT NULL,
    custom_id   TEXT NOT NULL,
    line        TEXT NOT NULL,
    PRIMARY KEY (file_id, line_number),
    -- custom_id is how a request is matched to its result, so it must be
    -- unique within a file.
    UNIQUE (file_id, custom_id)
);
