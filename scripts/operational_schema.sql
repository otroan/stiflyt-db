-- Operational schema for mutable data (endpoint names, number spaces)
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.endpoint_names (
    id BIGSERIAL PRIMARY KEY,
    anchor_node_id INTEGER NOT NULL,
    rutenummer TEXT NULL,
    rutenummer_key TEXT GENERATED ALWAYS AS (COALESCE(rutenummer, '')) STORED,
    name TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_id TEXT NULL,
    distance_meters DOUBLE PRECISION NULL,
    validated_by TEXT NULL,
    validated_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (anchor_node_id, rutenummer_key)
);

CREATE INDEX IF NOT EXISTS endpoint_names_anchor_idx
ON ops.endpoint_names (anchor_node_id);

CREATE TABLE IF NOT EXISTS ops.number_spaces (
    id BIGSERIAL PRIMARY KEY,
    scope TEXT NOT NULL,
    prefix TEXT NOT NULL,
    number TEXT NULL,
    status TEXT NOT NULL DEFAULT 'available',
    metadata JSONB NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (scope, prefix, number)
);

-- Grant access for backend + current user (best-effort)
DO $$
DECLARE
    current_user_name TEXT;
BEGIN
    current_user_name := current_user;
    -- Grant to current user
    BEGIN
        EXECUTE format('GRANT USAGE ON SCHEMA ops TO %I', current_user_name);
        EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ops TO %I', current_user_name);
        EXECUTE format('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA ops TO %I', current_user_name);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA ops GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %I', current_user_name);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA ops GRANT USAGE, SELECT ON SEQUENCES TO %I', current_user_name);
    EXCEPTION WHEN insufficient_privilege THEN
        RAISE NOTICE 'Could not grant privileges to current user (insufficient privileges). Please run GRANT commands manually.';
    END;

    -- Grant to backend role if it exists
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stiflyt_reader') THEN
        BEGIN
            EXECUTE 'GRANT USAGE ON SCHEMA ops TO stiflyt_reader';
            EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ops TO stiflyt_reader';
            EXECUTE 'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA ops TO stiflyt_reader';
            EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA ops GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO stiflyt_reader';
            EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA ops GRANT USAGE, SELECT ON SEQUENCES TO stiflyt_reader';
        EXCEPTION WHEN insufficient_privilege THEN
            RAISE NOTICE 'Could not grant privileges to stiflyt_reader (insufficient privileges). Please run GRANT commands manually.';
        END;
    END IF;
END $$;
