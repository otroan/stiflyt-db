-- Migration 014: manual signs can belong to multiple routes.
--
-- A manual sign placed on a shared trail (bre21 + bre62 etc.) was forced to
-- pick a single rutenummer; the sign post physically serves every route that
-- crosses there. Adds rutenummer_list TEXT[] alongside the existing single
-- rutenummer column. The legacy column stays populated with the first element
-- so old code (services/signs.py legacy paths) still reads it; signs_app reads
-- the array.
--
-- Idempotent.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'ops') THEN
        RAISE EXCEPTION 'ops schema does not exist.';
    END IF;
END $$;

ALTER TABLE ops.sign_sites
    ADD COLUMN IF NOT EXISTS rutenummer_list TEXT[] NOT NULL DEFAULT '{}';

-- Backfill: existing rows with a single rutenummer get a 1-element array.
UPDATE ops.sign_sites
   SET rutenummer_list = ARRAY[rutenummer]
 WHERE rutenummer IS NOT NULL
   AND (rutenummer_list IS NULL OR cardinality(rutenummer_list) = 0);

CREATE INDEX IF NOT EXISTS sign_sites_rutenummer_list_gin
    ON ops.sign_sites USING GIN (rutenummer_list);

DO $$ BEGIN RAISE NOTICE 'Migration 014 done.'; END $$;
