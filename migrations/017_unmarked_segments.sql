-- Migration 017: per-fotrute "unmarked segment" flag (boats, glaciers).
--
-- Kartverket encodes boat / DNT-ferry crossings (e.g. M/S Fæmund across Lake
-- Femunden) and glacier traverses (e.g. bre8 across Fortundalsbreen) as
-- ordinary `fotrute` rows. They have very sparse vertices — geometrically
-- impossible for an on-the-ground walking trail. DNT does not mark or take
-- responsibility for these segments, so signs should flag them ("via bre",
-- "via båt") rather than presenting them as regular trail.
--
-- We flag the offending fotrute_fk values in ops.unmarked_segment and extend
-- ops.fotruteinfo_patched with three additive columns:
--   is_unmarked       BOOLEAN
--   unmarked_kind     TEXT  ('boat' | 'glacier' | 'other')
--   unmarked_label    TEXT  optional ("Fortundalsbreen", "M/S Fæmund", …)
--
-- Rows are NOT dropped — keeping the segments in the graph means topology stays
-- connected (so a future routeplanner can still find paths), but downstream
-- callers can filter `is_unmarked = true` when computing trail-length stats
-- and suffix sign panels with the kind/label.
--
-- Canonical source of entries is `data/route_errata.yaml` (unmarked_segments:
-- section); `scripts/apply_route_errata.py` syncs it in. Idempotent.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'ops') THEN
        RAISE EXCEPTION 'ops schema does not exist.';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS ops.unmarked_segment (
    fotrute_fk      INTEGER PRIMARY KEY,
    kind            TEXT NOT NULL CHECK (kind IN ('boat', 'glacier', 'other')),
    label           TEXT,
    lokalid         TEXT,
    comment         TEXT,
    reported_at     DATE,
    updated_by      TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

GRANT SELECT ON ops.unmarked_segment TO stiflyt_reader;
GRANT INSERT, UPDATE, DELETE ON ops.unmarked_segment TO stiflyt_reader;

-- Extend the patched view: add is_unmarked / unmarked_kind / unmarked_label
-- columns, keep all rows (boat/glacier segments still need to appear so the
-- graph stays connected for routing). Existing remap + deletion semantics
-- are unchanged.
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
    (rm.from_rutenummer IS NOT NULL) AS is_patched,
    (us.fotrute_fk IS NOT NULL)      AS is_unmarked,
    us.kind                          AS unmarked_kind,
    us.label                         AS unmarked_label
FROM stiflyt.fotruteinfo fi
LEFT JOIN ops.rutenummer_remap rm  ON rm.from_rutenummer = fi.rutenummer
LEFT JOIN ops.unmarked_segment us  ON us.fotrute_fk      = fi.fotrute_fk
WHERE COALESCE(rm.deleted, FALSE) = FALSE;

GRANT SELECT ON ops.fotruteinfo_patched TO stiflyt_reader;

DO $$ BEGIN RAISE NOTICE 'Migration 017 done.'; END $$;
