-- Migration 020: per-route link exclusion + canonical corrected route-graph view.
--
-- Some fotruter in turrutebasen carry links that aren't actually part of the
-- marked route. The motivating case is loop/variant routes (e.g. fem30): one
-- rutenummer whose link_segments form two parallel arms between the same fork
-- nodes, where only one arm is the real marked route. Such a route reports a
-- topology cycle, and the sign blade-walk picks an arm non-deterministically.
--
-- This is a *link-level, route-scoped* correction: the same physical link can
-- belong to several fotrute_fk (and several routes), so the fk-keyed
-- ops.unmarked_segment pattern can't express "drop this link from THIS route".
-- The key is (rutenummer, link_id).
--
-- ops.route_link_graph is the canonical corrected route graph: one row per
-- (route, link), with remaps/deletions (via fotruteinfo_patched) and exclusions
-- already applied. Route-subgraph queries should read from it instead of
-- hand-rolling the link_segments -> links join, so a correction takes effect
-- tool-wide (map render, sign candidates, validation, exports).
--
-- Canonical source of entries is `data/route_errata.yaml` (link_exclusions:
-- section); the signs_app UI writes directly and `make dump-route-errata`
-- serialises the table back to YAML for version control. Idempotent.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'ops') THEN
        RAISE EXCEPTION 'ops schema does not exist.';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS ops.route_link_exclusion (
    rutenummer   TEXT    NOT NULL,
    link_id      BIGINT  NOT NULL,
    reason       TEXT,
    comment      TEXT,
    reported_at  DATE,
    updated_by   TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (rutenummer, link_id)
);

GRANT SELECT, INSERT, UPDATE, DELETE ON ops.route_link_exclusion TO stiflyt_reader;

-- Canonical corrected route graph: one row per (rutenummer, link_id).
--
-- Goes through ops.fotruteinfo_patched so rutenummer remaps and deletions are
-- already applied; drops any (rutenummer, link_id) present in
-- ops.route_link_exclusion. A link can be covered by several fotrute_fk for the
-- same route, so we GROUP BY to collapse to one row per link and fold the
-- per-segment unmarked flags with bool_or.
CREATE OR REPLACE VIEW ops.route_link_graph AS
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
GROUP BY fi.rutenummer, l.link_id, l.a_node, l.b_node, l.length_m, l.geom;

GRANT SELECT ON ops.route_link_graph TO stiflyt_reader;

DO $$ BEGIN RAISE NOTICE 'Migration 020 done.'; END $$;
