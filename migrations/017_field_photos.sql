-- Migration 017: ops.field_photos — uncoupled trail / sign documentation photos.
--
-- Photos live independently of sign_sites; they're displayed as a separate
-- map layer pinned to their own lat/lon. Two main use cases:
--   1. Document a signpost / panel in the field (status, condition, arrow).
--   2. Document the route itself (bridge state, slide damage, cairn quality).
--
-- lon/lat are nullable so the user can upload photos without EXIF GPS and
-- geotag them manually later — those sit in a "pending placement" tray in
-- the signs_app until the user clicks a position on the map.
--
-- File bytes live under data/photos/<area_code>/<uuid>.{heic,jpg}; this
-- table only stores paths and metadata. Idempotent.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'ops') THEN
        RAISE EXCEPTION 'ops schema does not exist.';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS ops.field_photos (
    id                BIGSERIAL PRIMARY KEY,
    area_code         TEXT NOT NULL,
    -- WGS84 lon/lat; NULL until the user manually geotags an EXIF-less upload.
    lon               DOUBLE PRECISION,
    lat               DOUBLE PRECISION,
    -- Storage paths relative to the data/ root (e.g. 'photos/bre/<uuid>.heic').
    file_path         TEXT NOT NULL,         -- original (HEIC, JPG, ...)
    display_path      TEXT NOT NULL,         -- browser-renderable 1600 px JPEG
    thumb_path        TEXT NOT NULL,         -- 200 px square JPEG for map markers
    mime_type         TEXT NOT NULL,
    bytes             BIGINT,
    -- EXIF DateTimeOriginal when present; falls back to uploaded_at.
    taken_at          TIMESTAMPTZ,
    -- EXIF GPSImgDirection — degrees clockwise from true north [0,360).
    exif_heading_deg  SMALLINT,
    -- Constrained-but-extensible tag set; see services/field_photos.py.
    tags              TEXT[] NOT NULL DEFAULT '{}',
    caption           TEXT,
    uploaded_by       TEXT,
    uploaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS field_photos_area_idx ON ops.field_photos (area_code, taken_at DESC);
-- Partial index speeds up the map-layer query (placed photos only).
CREATE INDEX IF NOT EXISTS field_photos_geo_idx
    ON ops.field_photos (area_code, lon, lat)
    WHERE lon IS NOT NULL AND lat IS NOT NULL;
-- Pending-placement queue lookup.
CREATE INDEX IF NOT EXISTS field_photos_pending_idx
    ON ops.field_photos (area_code, uploaded_at)
    WHERE lon IS NULL;

GRANT SELECT, INSERT, UPDATE, DELETE ON ops.field_photos TO stiflyt_reader;
GRANT USAGE, SELECT ON SEQUENCE ops.field_photos_id_seq TO stiflyt_reader;

-- Local-dev fallback: the backend runs as PG role `otroan` on this machine
-- (see `DB_USER=otroan` in make backend). Same pattern as migration 011.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'otroan') THEN
        EXECUTE 'GRANT USAGE ON SCHEMA ops TO otroan';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON ops.field_photos TO otroan';
        EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE ops.field_photos_id_seq TO otroan';
    END IF;
END $$;

DO $$ BEGIN RAISE NOTICE 'Migration 017 done.'; END $$;
