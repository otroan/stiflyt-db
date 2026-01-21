-- Changeset Editor MVP - Initial Schema
-- Run this against your PostGIS-enabled database

-- Create schema for changeset editor
CREATE SCHEMA IF NOT EXISTS changeset;

-- Changeset table
CREATE TABLE IF NOT EXISTS changeset.changeset (
    id TEXT PRIMARY KEY,  -- UUID as text, or custom ID format
    title TEXT NOT NULL,
    description TEXT,
    area TEXT,
    status TEXT NOT NULL CHECK (status IN ('draft', 'review', 'approved', 'exported')),
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    base_snapshot TEXT NOT NULL,  -- Import ID or timestamp identifying base state
    linked_issue_url TEXT,
    pr_url TEXT
);

CREATE INDEX IF NOT EXISTS idx_changeset_status ON changeset.changeset(status);
CREATE INDEX IF NOT EXISTS idx_changeset_created_at ON changeset.changeset(created_at);
CREATE INDEX IF NOT EXISTS idx_changeset_created_by ON changeset.changeset(created_by);

-- Change event table (append-only event log)
CREATE TABLE IF NOT EXISTS changeset.change_event (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    changeset_id TEXT NOT NULL REFERENCES changeset.changeset(id) ON DELETE CASCADE,
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    user_id TEXT NOT NULL,
    event JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_change_event_changeset ON changeset.change_event(changeset_id);
CREATE INDEX IF NOT EXISTS idx_change_event_ts ON changeset.change_event(ts);
CREATE INDEX IF NOT EXISTS idx_change_event_type ON changeset.change_event((event->>'type'));

-- Materialized cache (optional, for performance)
CREATE TABLE IF NOT EXISTS changeset.materialized_cache (
    changeset_id TEXT NOT NULL REFERENCES changeset.changeset(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('diff', 'effective')),
    geojson JSONB NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (changeset_id, kind)
);

-- Base schema (readonly, assumed to exist)
-- Example structure (adjust to match your actual base schema):
-- CREATE SCHEMA IF NOT EXISTS base;
-- 
-- CREATE TABLE base.segment_base (
--     id TEXT PRIMARY KEY,
--     geom GEOMETRY(LINESTRING, 4326) NOT NULL,  -- or 25833
--     attrs JSONB,  -- or explicit columns: name, route_ref, etc.
--     created_at TIMESTAMPTZ DEFAULT NOW()
-- );
-- 
-- CREATE TABLE base.route_base (
--     id TEXT PRIMARY KEY,
--     name TEXT,
--     number TEXT,
--     attrs JSONB
-- );
-- 
-- CREATE INDEX idx_segment_base_geom ON base.segment_base USING GIST(geom);

-- Helper function to update updated_at timestamp
CREATE OR REPLACE FUNCTION changeset.update_changeset_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE changeset.changeset
    SET updated_at = NOW()
    WHERE id = NEW.changeset_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgname = 'trigger_update_changeset_updated_at'
          AND tgrelid = 'changeset.change_event'::regclass
    ) THEN
        CREATE TRIGGER trigger_update_changeset_updated_at
            AFTER INSERT ON changeset.change_event
            FOR EACH ROW
            EXECUTE FUNCTION changeset.update_changeset_updated_at();
    END IF;
END $$;

-- Grant permissions
-- Note: These commands may fail if run by a user without GRANT privileges
-- In that case, a database admin should run these manually:
--   GRANT USAGE ON SCHEMA changeset TO stiflyt_reader;
--   GRANT ALL ON ALL TABLES IN SCHEMA changeset TO stiflyt_reader;
--   GRANT ALL ON ALL SEQUENCES IN SCHEMA changeset TO stiflyt_reader;
--   ALTER DEFAULT PRIVILEGES IN SCHEMA changeset GRANT ALL ON TABLES TO stiflyt_reader;
--   ALTER DEFAULT PRIVILEGES IN SCHEMA changeset GRANT ALL ON SEQUENCES TO stiflyt_reader;

-- Try to grant permissions (will fail silently if user doesn't have privileges)
DO $$
DECLARE
    current_user_name TEXT;
BEGIN
    current_user_name := current_user;
    -- Grant to current user
    BEGIN
        EXECUTE format('GRANT USAGE ON SCHEMA changeset TO %I', current_user_name);
        EXECUTE format('GRANT ALL ON ALL TABLES IN SCHEMA changeset TO %I', current_user_name);
        EXECUTE format('GRANT ALL ON ALL SEQUENCES IN SCHEMA changeset TO %I', current_user_name);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA changeset GRANT ALL ON TABLES TO %I', current_user_name);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA changeset GRANT ALL ON SEQUENCES TO %I', current_user_name);
    EXCEPTION WHEN insufficient_privilege THEN
        RAISE NOTICE 'Could not grant permissions to current user (insufficient privileges). Please run GRANT commands manually.';
    END;
    
    -- Also grant to stiflyt_reader if it exists and we have privileges
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stiflyt_reader') THEN
        BEGIN
            EXECUTE 'GRANT USAGE ON SCHEMA changeset TO stiflyt_reader';
            EXECUTE 'GRANT ALL ON ALL TABLES IN SCHEMA changeset TO stiflyt_reader';
            EXECUTE 'GRANT ALL ON ALL SEQUENCES IN SCHEMA changeset TO stiflyt_reader';
            EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA changeset GRANT ALL ON TABLES TO stiflyt_reader';
            EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA changeset GRANT ALL ON SEQUENCES TO stiflyt_reader';
        EXCEPTION WHEN insufficient_privilege THEN
            RAISE NOTICE 'Could not grant permissions to stiflyt_reader (insufficient privileges). Please run GRANT commands manually.';
        END;
    END IF;
END $$;
