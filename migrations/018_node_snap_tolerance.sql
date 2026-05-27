-- Migration 018: consolidate near-duplicate nodes (snap tolerance).
--
-- Kartverket sometimes digitizes adjacent fotrute segments with their shared
-- endpoint a fraction of a metre apart. Migration 002 hashes endpoints
-- byte-exact, so a 0.085 m offset produces TWO distinct nodes — the trail
-- visually continues but topologically dead-ends, generating spurious
-- 0-panel sign candidates. See scripts/detect_node_snap_errors.py + the
-- artifacts/fem_node_snap_errors.csv report for the population.
--
-- This pass post-processes the topology built by migration 002: for every
-- cluster of nodes within TOLERANCE_M of each other, picks the smallest
-- id as canonical, repoints fotrute / links references to it, deletes the
-- duplicates, and refreshes the dependent materialized views. Idempotent —
-- a second run finds zero pairs and is a no-op.
--
-- The tolerance is conservative (1 m). Pairs further apart are reported but
-- left untouched; review them visually in detect_node_snap_errors.py output
-- before deciding whether to widen the tolerance or report to Kartverket.

DO $$
DECLARE
    schema_name TEXT;
    tolerance_m REAL := 1.0;
    pair_count INTEGER;
    map_count INTEGER;
    fotrute_updated INTEGER := 0;
    links_updated INTEGER := 0;
    tmp_count INTEGER;
    deleted_count INTEGER;
    fotrute_exists BOOLEAN;
    links_exists BOOLEAN;
    node_degree_exists BOOLEAN;
    anchor_nodes_exists BOOLEAN;
BEGIN
    -- Find the most recent turogfriluftsruter schema (same heuristic as 002).
    SELECT nspname INTO schema_name
    FROM pg_namespace
    WHERE nspname LIKE 'turogfriluftsruter_%'
      AND nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
    ORDER BY nspname DESC LIMIT 1;

    IF schema_name IS NULL THEN
        RAISE NOTICE 'No turogfriluftsruter_* schema found, skipping node-snap consolidation.';
        RETURN;
    END IF;

    -- Verify nodes table exists in that schema (migration 002 has run).
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = schema_name AND table_name = 'nodes'
    ) THEN
        RAISE NOTICE 'No nodes table in %.nodes, skipping.', schema_name;
        RETURN;
    END IF;

    -- Count near-duplicate pairs.
    EXECUTE format($f$
        SELECT count(*)
        FROM %I.nodes n1
        JOIN %I.nodes n2 ON n2.id > n1.id
        WHERE ST_DWithin(n1.geom, n2.geom, %s)
    $f$, schema_name, schema_name, tolerance_m) INTO pair_count;

    RAISE NOTICE 'node-snap: % near-duplicate pair(s) within % m', pair_count, tolerance_m;
    IF pair_count = 0 THEN
        RAISE NOTICE 'Nothing to do.';
        RETURN;
    END IF;

    -- Build the canonical-id map in ops (where we have CREATE rights) rather
    -- than a TEMP table — the migration runner role can't create temp tables
    -- in this database. We TRUNCATE on entry so the migration is idempotent.
    CREATE TABLE IF NOT EXISTS ops.node_canonical_map_tmp (
        id INTEGER PRIMARY KEY,
        canonical_id INTEGER NOT NULL
    );
    TRUNCATE ops.node_canonical_map_tmp;

    EXECUTE format($f$
        INSERT INTO ops.node_canonical_map_tmp (id, canonical_id)
        WITH RECURSIVE
        pairs AS (
            SELECT n1.id AS a, n2.id AS b
            FROM %I.nodes n1
            JOIN %I.nodes n2 ON n2.id <> n1.id
            WHERE ST_DWithin(n1.geom, n2.geom, %s)
        ),
        edges AS (
            SELECT a, b FROM pairs
            UNION
            SELECT b, a FROM pairs
        ),
        reachable AS (
            SELECT n.id AS start_id, n.id AS reached_id, 0 AS hops
            FROM %I.nodes n
            WHERE EXISTS (SELECT 1 FROM edges e WHERE e.a = n.id)
            UNION
            SELECT r.start_id, e.b, r.hops + 1
            FROM reachable r
            JOIN edges e ON e.a = r.reached_id
            WHERE r.hops < 20    -- safety cap; real clusters are ≤3 nodes
        )
        SELECT start_id AS id, MIN(reached_id) AS canonical_id
        FROM reachable
        GROUP BY start_id;
    $f$, schema_name, schema_name, tolerance_m, schema_name);

    SELECT count(*) INTO map_count FROM ops.node_canonical_map_tmp WHERE id <> canonical_id;
    RAISE NOTICE 'node-snap: % node(s) will be re-pointed to a canonical id', map_count;

    -- Repoint fotrute references.
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = schema_name AND table_name = 'fotrute'
    ) INTO fotrute_exists;
    IF fotrute_exists THEN
        EXECUTE format($f$
            UPDATE %I.fotrute f
            SET source_node = m.canonical_id
            FROM ops.node_canonical_map_tmp m
            WHERE f.source_node = m.id AND m.canonical_id <> m.id
        $f$, schema_name);
        GET DIAGNOSTICS tmp_count = ROW_COUNT;
        fotrute_updated := fotrute_updated + tmp_count;
        EXECUTE format($f$
            UPDATE %I.fotrute f
            SET target_node = m.canonical_id
            FROM ops.node_canonical_map_tmp m
            WHERE f.target_node = m.id AND m.canonical_id <> m.id
        $f$, schema_name);
        GET DIAGNOSTICS tmp_count = ROW_COUNT;
        fotrute_updated := fotrute_updated + tmp_count;
        RAISE NOTICE 'node-snap: % fotrute.source/target_node ref(s) repointed', fotrute_updated;
    END IF;

    -- Repoint links references.
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = schema_name AND table_name = 'links'
    ) INTO links_exists;
    IF links_exists THEN
        EXECUTE format($f$
            UPDATE %I.links l
            SET a_node = m.canonical_id
            FROM ops.node_canonical_map_tmp m
            WHERE l.a_node = m.id AND m.canonical_id <> m.id
        $f$, schema_name);
        GET DIAGNOSTICS tmp_count = ROW_COUNT;
        links_updated := links_updated + tmp_count;
        EXECUTE format($f$
            UPDATE %I.links l
            SET b_node = m.canonical_id
            FROM ops.node_canonical_map_tmp m
            WHERE l.b_node = m.id AND m.canonical_id <> m.id
        $f$, schema_name);
        GET DIAGNOSTICS tmp_count = ROW_COUNT;
        links_updated := links_updated + tmp_count;
        RAISE NOTICE 'node-snap: % links.a/b_node ref(s) repointed', links_updated;
    END IF;

    -- Delete the now-orphaned nodes.
    EXECUTE format($f$
        DELETE FROM %I.nodes
        WHERE id IN (
            SELECT id FROM ops.node_canonical_map_tmp WHERE canonical_id <> id
        )
    $f$, schema_name);
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RAISE NOTICE 'node-snap: deleted % duplicate node(s)', deleted_count;

    -- Refresh dependent materialized views so degree / anchor-nodes reflect
    -- the merged topology.
    SELECT EXISTS (
        SELECT 1 FROM pg_matviews
        WHERE schemaname = schema_name AND matviewname = 'node_degree'
    ) INTO node_degree_exists;
    IF node_degree_exists THEN
        EXECUTE format('REFRESH MATERIALIZED VIEW %I.node_degree', schema_name);
        RAISE NOTICE 'node-snap: refreshed %.node_degree', schema_name;
    END IF;

    SELECT EXISTS (
        SELECT 1 FROM pg_matviews
        WHERE schemaname = schema_name AND matviewname = 'anchor_nodes'
    ) INTO anchor_nodes_exists;
    IF anchor_nodes_exists THEN
        EXECUTE format('REFRESH MATERIALIZED VIEW %I.anchor_nodes', schema_name);
        RAISE NOTICE 'node-snap: refreshed %.anchor_nodes', schema_name;
    END IF;
END $$;

DO $$ BEGIN RAISE NOTICE 'Migration 018 done.'; END $$;
