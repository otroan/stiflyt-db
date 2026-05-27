-- Migration 021: per-route link bridge + fold bridges into route_link_graph.
--
-- The mirror image of ops.route_link_exclusion (migration 020): where an
-- exclusion REMOVES a link from one route's graph (to resolve a loop/variant),
-- a bridge ADDS a synthetic connector edge between two near nodes to reconnect
-- a route that turrutebasen split into separate components (e.g. fem22, whose
-- parts sit 10–14 m apart — digitizing gaps, not real separations).
--
-- Bridges are folded into ops.route_link_graph as extra rows, so connectivity,
-- the loop/disconnected validators, endpoint detection, blade distances, and
-- the rendered map line all see the route as connected with no further code
-- changes — the same leverage the canonical view gave exclusions.
--
-- Canonical source is the signs_app UI (writes straight to the DB);
-- data/route_errata.yaml `bridges:` is the version-controlled snapshot via
-- `make dump-route-errata`. Idempotent.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'ops') THEN
        RAISE EXCEPTION 'ops schema does not exist.';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS ops.route_link_bridge (
    bridge_id    BIGSERIAL UNIQUE,
    rutenummer   TEXT    NOT NULL,
    a_node       BIGINT  NOT NULL,
    b_node       BIGINT  NOT NULL,
    reason       TEXT,
    comment      TEXT,
    reported_at  DATE,
    updated_by   TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (rutenummer, a_node, b_node),
    -- store the node pair sorted so the same bridge can't be inserted twice
    CHECK (a_node < b_node)
);

GRANT SELECT, INSERT, UPDATE, DELETE ON ops.route_link_bridge TO stiflyt_reader;
GRANT USAGE, SELECT ON SEQUENCE ops.route_link_bridge_bridge_id_seq TO stiflyt_reader;

-- Recreate the canonical view with the bridge UNION. DROP+CREATE (not CREATE OR
-- REPLACE) because the geom column's typmod changes once a generic synthetic
-- geometry joins the real MultiLineString links. Nothing else depends on it.
DROP VIEW IF EXISTS ops.route_link_graph;
CREATE VIEW ops.route_link_graph AS
SELECT
    fi.rutenummer,
    l.link_id,
    l.a_node,
    l.b_node,
    l.length_m,
    l.geom,
    bool_or(fi.is_unmarked) AS is_unmarked,
    max(fi.unmarked_kind)   AS unmarked_kind,
    max(fi.unmarked_label)  AS unmarked_label
FROM ops.fotruteinfo_patched fi
JOIN stiflyt.link_segments ls ON ls.segment_id = fi.fotrute_fk
JOIN stiflyt.links l          ON l.link_id     = ls.link_id
WHERE fi.rutenummer IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
        FROM ops.route_link_exclusion x
       WHERE x.rutenummer = fi.rutenummer
         AND x.link_id    = l.link_id
  )
GROUP BY fi.rutenummer, l.link_id, l.a_node, l.b_node, l.length_m, l.geom
UNION ALL
-- Synthetic bridge edges: negative link_id (never collides with real ids),
-- geometry/length derived from the two node points.
SELECT
    b.rutenummer,
    (-b.bridge_id)::bigint                     AS link_id,
    b.a_node,
    b.b_node,
    ST_Length(ST_MakeLine(na.geom, nb.geom))   AS length_m,
    ST_Multi(ST_MakeLine(na.geom, nb.geom))    AS geom,
    FALSE        AS is_unmarked,
    NULL::text   AS unmarked_kind,
    NULL::text   AS unmarked_label
FROM ops.route_link_bridge b
JOIN stiflyt.nodes na ON na.node_id = b.a_node
JOIN stiflyt.nodes nb ON nb.node_id = b.b_node;

GRANT SELECT ON ops.route_link_graph TO stiflyt_reader;

DO $$ BEGIN RAISE NOTICE 'Migration 021 done.'; END $$;
