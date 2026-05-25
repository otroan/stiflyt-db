-- Migration 013: discriminate sign_site_skilt rows by first_link_id.
--
-- After we started splitting panels by the physical out-link from a junction
-- (commit X — parallel paths to a shared destination), two panels can share
-- (sign_site_id, anchor_node_id) but go via different physical links. The old
-- UNIQUE (sign_site_id, anchor_node_id) prevented independent overrides; the
-- second panel's edits silently overwrote the first.
--
-- Add first_link_id to the unique key. Existing rows (first_link_id NULL)
-- coexist with the new ones because COALESCE(first_link_id, -1) treats NULL
-- as a distinct "no-link" slot — that's the legacy entry from before the
-- split-panel logic, and remains the fallback for callers (e.g. the legacy
-- /signs endpoint) that don't know first_link_id.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'ops') THEN
        RAISE EXCEPTION 'ops schema does not exist.';
    END IF;
END $$;

ALTER TABLE ops.sign_site_skilt
    ADD COLUMN IF NOT EXISTS first_link_id INTEGER NULL;

-- Drop the old (sign_site_id, anchor_node_id) unique constraint.
DO $$
DECLARE
    ck text;
BEGIN
    SELECT conname INTO ck
    FROM pg_constraint
    WHERE conrelid = 'ops.sign_site_skilt'::regclass
      AND contype = 'u'
      AND conkey @> ARRAY[
        (SELECT attnum FROM pg_attribute WHERE attrelid = 'ops.sign_site_skilt'::regclass AND attname = 'sign_site_id'),
        (SELECT attnum FROM pg_attribute WHERE attrelid = 'ops.sign_site_skilt'::regclass AND attname = 'anchor_node_id')
      ]::smallint[]
      AND cardinality(conkey) = 2;
    IF ck IS NOT NULL THEN
        EXECUTE format('ALTER TABLE ops.sign_site_skilt DROP CONSTRAINT %I', ck);
        RAISE NOTICE 'Dropped old unique constraint %', ck;
    END IF;
END $$;

-- New uniqueness on (sign_site_id, anchor_node_id, COALESCE(first_link_id, -1)).
-- Functional unique index — NULLs are treated as the sentinel -1 so we still
-- get one canonical "no-link" row per (site, anchor) alongside the per-link rows.
CREATE UNIQUE INDEX IF NOT EXISTS sign_site_skilt_site_anchor_link_uidx
    ON ops.sign_site_skilt (sign_site_id, anchor_node_id, COALESCE(first_link_id, -1));

DO $$ BEGIN RAISE NOTICE 'Migration 013 done.'; END $$;
