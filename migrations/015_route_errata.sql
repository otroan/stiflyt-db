-- Migration 015: per-rutenummer errata table and patched fotruteinfo view.
--
-- Lets us locally retag segments while waiting for Kartverket fixes. The
-- canonical source of patches is `data/route_errata.yaml` in the app repo;
-- `scripts/apply_route_errata.py` syncs the YAML into this table. Queries
-- in the signs_app go through `ops.fotruteinfo_patched` so the remap is
-- transparent to everything downstream (filter, dedup, endpoint detection,
-- panels, manual signs, route summary).
--
-- Lives in `ops` so a turrutebasen refresh doesn't wipe it. Idempotent.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'ops') THEN
        RAISE EXCEPTION 'ops schema does not exist.';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS ops.rutenummer_remap (
    from_rutenummer TEXT PRIMARY KEY,
    to_rutenummer   TEXT NOT NULL,
    comment         TEXT,
    reported_at     DATE,
    updated_by      TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

GRANT SELECT ON ops.rutenummer_remap TO stiflyt_reader;
GRANT INSERT, UPDATE, DELETE ON ops.rutenummer_remap TO stiflyt_reader;

-- Patched fotruteinfo view: same columns as stiflyt.fotruteinfo, but with
-- `rutenummer` swapped via the remap table when an entry matches. The
-- COALESCE means rows without a matching remap pass through unchanged.
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
    -- Carry the original rutenummer so callers can audit the patch
    fi.rutenummer AS original_rutenummer,
    (rm.from_rutenummer IS NOT NULL) AS is_patched
FROM stiflyt.fotruteinfo fi
LEFT JOIN ops.rutenummer_remap rm ON rm.from_rutenummer = fi.rutenummer;

GRANT SELECT ON ops.fotruteinfo_patched TO stiflyt_reader;

DO $$ BEGIN RAISE NOTICE 'Migration 015 done.'; END $$;
