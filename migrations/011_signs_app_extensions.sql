-- Migration 011: signs_app schema extensions
--
-- Adds the bits the new signs_app frontend needs on top of the existing
-- ops.sign_sites / ops.sign_site_destinations / ops.endpoint_names tables.
-- All DDL lives here (not in the Python app's ensure_operational_schema()) so
-- the runtime user (stiflyt_reader) doesn't need ALTER / CREATE privileges.
--
-- Idempotent. Safe to re-run.
--
-- Run as the operational DB owner (typically stiflyt_owner or postgres):
--   psql -d matrikkel -f migrations/011_signs_app_extensions.sql

DO $$
BEGIN
    -- Schema must already exist (created by scripts/operational_schema.sql)
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'ops') THEN
        RAISE EXCEPTION 'ops schema does not exist. Run scripts/operational_schema.sql first.';
    END IF;

    -- Required parent tables must exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema='ops' AND table_name='sign_sites'
    ) THEN
        RAISE EXCEPTION 'ops.sign_sites does not exist. Run scripts/operational_schema.sql first.';
    END IF;

    RAISE NOTICE 'Migration 011: extending ops for signs_app...';
END $$;

-- New columns on ops.sign_sites
ALTER TABLE ops.sign_sites
    ADD COLUMN IF NOT EXISTS area_code TEXT NULL;
ALTER TABLE ops.sign_sites
    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'accepted';
ALTER TABLE ops.sign_sites
    ADD COLUMN IF NOT EXISTS site_code TEXT NULL;

-- Status must be one of the known values.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'sign_sites_status_check'
    ) THEN
        ALTER TABLE ops.sign_sites
            ADD CONSTRAINT sign_sites_status_check
            CHECK (status IN ('proposed','accepted','rejected','installed'));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS sign_sites_area_status_idx
    ON ops.sign_sites (area_code, status);

-- One row per anchor per area; the row carries the latest status.
CREATE UNIQUE INDEX IF NOT EXISTS sign_sites_area_anchor_uidx
    ON ops.sign_sites (area_code, anchor_node_id)
    WHERE area_code IS NOT NULL AND anchor_node_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS sign_sites_site_code_uidx
    ON ops.sign_sites (site_code)
    WHERE site_code IS NOT NULL;

-- ops.sign_site_skilt: per-destination panel overrides on a sign site
-- (direction arrow, color override, distance override). The Python app used
-- to create this lazily; making it permanent here so stiflyt_reader can DML it.
CREATE TABLE IF NOT EXISTS ops.sign_site_skilt (
    id                BIGSERIAL PRIMARY KEY,
    sign_site_id      BIGINT NOT NULL REFERENCES ops.sign_sites(id) ON DELETE CASCADE,
    anchor_node_id    INTEGER NOT NULL,
    direction         TEXT NULL,
    status            TEXT NULL,
    skiltfarge        TEXT NULL,
    distance_meters   DOUBLE PRECISION NULL,
    destination_name  TEXT NULL,
    updated_by        TEXT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (sign_site_id, anchor_node_id)
);
-- New column for the signs_app (destination name override per panel)
ALTER TABLE ops.sign_site_skilt
    ADD COLUMN IF NOT EXISTS destination_name TEXT NULL;
CREATE INDEX IF NOT EXISTS sign_site_skilt_site_idx
    ON ops.sign_site_skilt (sign_site_id);

-- Grants: same pattern as scripts/operational_schema.sql — backend (stiflyt_reader)
-- gets full DML; stiflyt_owner already owns via inheritance.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stiflyt_reader') THEN
        GRANT USAGE ON SCHEMA ops TO stiflyt_reader;
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ops TO stiflyt_reader;
        GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA ops TO stiflyt_reader;
        ALTER DEFAULT PRIVILEGES IN SCHEMA ops
            GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO stiflyt_reader;
        ALTER DEFAULT PRIVILEGES IN SCHEMA ops
            GRANT USAGE, SELECT ON SEQUENCES TO stiflyt_reader;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stiflyt_updater') THEN
        GRANT USAGE ON SCHEMA ops TO stiflyt_updater;
        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ops TO stiflyt_updater;
        GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA ops TO stiflyt_updater;
    END IF;
END $$;

DO $$ BEGIN RAISE NOTICE 'Migration 011 done.'; END $$;
