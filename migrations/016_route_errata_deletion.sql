-- Migration 016: extend ops.rutenummer_remap to also support deletions.
--
-- Some routes shouldn't be remapped to another rutenummer — they should
-- disappear entirely from the signs_app's view (e.g. bre7 was a stub that
-- DNT removed). We add a `deleted` boolean alongside the existing
-- to_rutenummer target. The patched fotruteinfo view filters out segments
-- whose rutenummer is flagged deleted, so they no longer contribute to
-- any route's geometry, panels, endpoints, or distance.
--
-- to_rutenummer becomes optional; a row is valid iff it either retargets
-- the rutenummer (to_rutenummer NOT NULL) or marks it deleted (deleted=true).
-- Idempotent.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'ops') THEN
        RAISE EXCEPTION 'ops schema does not exist.';
    END IF;
END $$;

ALTER TABLE ops.rutenummer_remap
    ADD COLUMN IF NOT EXISTS deleted BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE ops.rutenummer_remap
    ALTER COLUMN to_rutenummer DROP NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'remap_target_or_deletion'
    ) THEN
        ALTER TABLE ops.rutenummer_remap
            ADD CONSTRAINT remap_target_or_deletion
            CHECK (deleted = TRUE OR to_rutenummer IS NOT NULL);
    END IF;
END $$;

-- Recreate the view so it filters out deleted-rutenummer segments. Anything
-- downstream (link aggregation, route geometry, route summary, panels) will
-- behave as if those segments don't exist for the signs_app.
CREATE OR REPLACE VIEW ops.fotruteinfo_patched AS
SELECT
    fi.objid,
    fi.objtype,
    fi.rutenavn,
    COALESCE(rm.to_rutenummer, fi.rutenummer) AS rutenummer,
    fi.vedlikeholdsansvarlig,
    fi.ruteinformasjon,
    fi.spesialfotrutetype,
    fi.gradering,
    fi.rutetype,
    fi.rutebetydning,
    fi.tilpasning,
    fi.fotrute_fk,
    fi.rutenummer AS original_rutenummer,
    (rm.from_rutenummer IS NOT NULL) AS is_patched
FROM stiflyt.fotruteinfo fi
LEFT JOIN ops.rutenummer_remap rm ON rm.from_rutenummer = fi.rutenummer
WHERE COALESCE(rm.deleted, FALSE) = FALSE;

GRANT SELECT ON ops.fotruteinfo_patched TO stiflyt_reader;

DO $$ BEGIN RAISE NOTICE 'Migration 016 done.'; END $$;
