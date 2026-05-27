-- Migration 019: ops.route_annotations — polymorphic per-route entries.
--
-- One table for the rutebok diary, inspection reports, dugnad reports, and
-- georeferenced maintenance markers (klipping/bro/klopp/other). The `kind`
-- column discriminates; the UI groups them into sub-tabs in the Rute
-- sidebar panel.
--
-- - geom is null for plain text entries (diary/inspection/dugnad).
-- - work_* kinds typically have a non-null geom; the marker renders on the
--   map and resolved_at flips from null → timestamp when the OK has dealt
--   with it.
-- - position_along_m is an optional km-marker along the route, used by the
--   UI to sort an entry into its rough location on the route profile.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'ops') THEN
        RAISE EXCEPTION 'ops schema does not exist.';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS ops.route_annotations (
    id               BIGSERIAL PRIMARY KEY,
    area_code        TEXT NOT NULL,
    rutenummer       TEXT NOT NULL,
    kind             TEXT NOT NULL,
    position_along_m DOUBLE PRECISION NULL,
    geom             GEOMETRY(Point, 25833) NULL,
    title            TEXT NULL,
    body             TEXT NULL,
    occurred_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    recorded_by      TEXT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at      TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS route_annotations_area_route_idx
    ON ops.route_annotations (area_code, rutenummer);
CREATE INDEX IF NOT EXISTS route_annotations_area_kind_idx
    ON ops.route_annotations (area_code, kind);
CREATE INDEX IF NOT EXISTS route_annotations_geom_idx
    ON ops.route_annotations USING GIST (geom)
    WHERE geom IS NOT NULL;

GRANT SELECT, INSERT, UPDATE, DELETE ON ops.route_annotations TO stiflyt_reader;
GRANT USAGE, SELECT ON SEQUENCE ops.route_annotations_id_seq TO stiflyt_reader;
