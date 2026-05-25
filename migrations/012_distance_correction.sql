-- Migration 012: per-area distance correction factor.
--
-- The signs_app multiplies raw along-route metres by a correction factor to
-- account for elevation, polyline-chord shortcuts and other underestimates
-- in the 2D turrutebasen geometry. The factor is a heuristic — see the
-- README — so we make it configurable per area instead of hardcoding 1.125.
--
-- Idempotent. Safe to re-run.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'ops') THEN
        RAISE EXCEPTION 'ops schema does not exist. Run earlier ops migrations first.';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS ops.distance_correction (
    area_code  TEXT PRIMARY KEY,
    factor     DOUBLE PRECISION NOT NULL DEFAULT 1.125,
    comment    TEXT,
    updated_by TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (factor > 0 AND factor < 5)
);

-- Seed bre with the existing 1.125 so behaviour is unchanged after this migration.
INSERT INTO ops.distance_correction (area_code, factor, comment, updated_by)
VALUES ('bre', 1.125, 'Initial: hiking-community heuristic mixing elevation + chord-shortcut', 'migration_012')
ON CONFLICT (area_code) DO NOTHING;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stiflyt_reader') THEN
        GRANT SELECT, INSERT, UPDATE, DELETE ON ops.distance_correction TO stiflyt_reader;
    END IF;
END $$;

DO $$ BEGIN RAISE NOTICE 'Migration 012 done.'; END $$;
