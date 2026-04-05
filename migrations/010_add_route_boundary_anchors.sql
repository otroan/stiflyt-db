-- Migration 010: Add route-boundary anchors to anchor_nodes
--
-- Problem: When routes end/start at a node with degree=2, that node should be
-- identified as an anchor node, but currently only topology anchors (degree != 2)
-- and ruteinfopunkt matches are included.
--
-- Solution: Add route-boundary anchors - nodes where incident segments have
-- different route sets (rutenummer sets). This matches the logic in
-- compute_metadata_anchor_nodes() in build_links.py.
--
-- Example: When route bre6 ends and routes bre5 and bre57 start at the same node,
-- that node should be an anchor node even if it has degree=2.

DO $$
DECLARE
    dynamic_schema TEXT;
    route_boundary_count INTEGER;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE 'Migration 010: Adding route-boundary anchors to anchor_nodes...';
    RAISE NOTICE '  This identifies nodes where route sets change (e.g., bre6 ends, bre5/bre57 start)';

    -- Find the current dynamic schema with prefix 'turogfriluftsruter_'
    SELECT nspname INTO dynamic_schema
    FROM pg_namespace
    WHERE nspname LIKE 'turogfriluftsruter_%'
      AND nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
    ORDER BY nspname DESC  -- Get the most recent one
    LIMIT 1;

    IF dynamic_schema IS NULL THEN
        RAISE EXCEPTION 'No dynamic schema found (turogfriluftsruter_*). Run migration 002 first.';
    END IF;

    RAISE NOTICE '  Using dynamic schema: %', dynamic_schema;

    -- Check if anchor_nodes MATERIALIZED VIEW exists in dynamic schema
    IF NOT EXISTS (
        SELECT 1 FROM pg_matviews
        WHERE schemaname = dynamic_schema AND matviewname = 'anchor_nodes'
    ) THEN
        RAISE EXCEPTION 'anchor_nodes materialized view does not exist in schema %. Run migration 002 first.', dynamic_schema;
    END IF;

    -- Drop the MATERIALIZED VIEW in the dynamic schema
    -- Note: The VIEW in stiflyt schema (created by migration 005) will automatically reflect changes
    EXECUTE format('DROP MATERIALIZED VIEW IF EXISTS %I.anchor_nodes CASCADE', dynamic_schema);

    -- Recreate anchor_nodes MATERIALIZED VIEW in dynamic schema with route-boundary anchors added
    EXECUTE format('
        CREATE MATERIALIZED VIEW %I.anchor_nodes AS
        WITH topology_anchors AS (
            -- Nodes with degree != 2 (endpoints, junctions, isolated nodes)
            SELECT
                node_id,
                geom,
                degree,
                ''topology'' as anchor_type,
                NULL::bigint as ruteinfopunkt_objid,
                NULL::double precision as ruteinfopunkt_distance_m
            FROM %I.node_degree
            WHERE degree != 2
        ),
        ruteinfopunkt_matches AS (
            -- Nodes within 100m of ruteinfopunkt (even if degree = 2)
            SELECT DISTINCT ON (n.node_id)
                n.node_id,
                n.geom,
                n.degree,
                rp.objid as ruteinfopunkt_objid,
                ST_Distance(n.geom, rp.posisjon) as distance_m
            FROM %I.node_degree n
            JOIN %I.ruteinfopunkt rp ON ST_DWithin(n.geom, rp.posisjon, 100)
            WHERE n.degree = 2  -- Only check degree=2 nodes (topology anchors already handled)
              AND rp.posisjon IS NOT NULL
            ORDER BY n.node_id, ST_Distance(n.geom, rp.posisjon)
        ),
        route_boundary_anchors_base AS (
            -- Nodes where incident segments have different route sets
            -- This identifies route boundaries (e.g., where bre6 ends and bre5/bre57 start)
            WITH node_segment_routes AS (
                -- Get all routes for each segment connected to each node
                SELECT DISTINCT
                    n.id as node_id,
                    n.geom,
                    nd.degree,
                    f.objid as segment_id,
                    fi.rutenummer
                FROM %I.nodes n
                JOIN %I.node_degree nd ON nd.node_id = n.id
                JOIN %I.fotrute f ON (f.source_node = n.id OR f.target_node = n.id)
                JOIN %I.fotruteinfo fi ON fi.fotrute_fk = f.objid
                WHERE fi.rutenummer IS NOT NULL
                  AND nd.degree = 2  -- Only check degree=2 nodes (topology anchors already handled)
            ),
            node_route_sets AS (
                -- Aggregate routes per node-segment pair into arrays
                SELECT
                    node_id,
                    geom,
                    degree,
                    segment_id,
                    array_agg(DISTINCT rutenummer ORDER BY rutenummer) as route_set
                FROM node_segment_routes
                GROUP BY node_id, geom, degree, segment_id
            ),
            nodes_with_multiple_route_sets AS (
                -- Find nodes where different segments have different route sets
                SELECT DISTINCT
                    node_id,
                    geom,
                    degree
                FROM node_route_sets
                GROUP BY node_id, geom, degree
                HAVING COUNT(DISTINCT route_set) > 1
            )
            SELECT
                node_id,
                geom,
                degree
            FROM nodes_with_multiple_route_sets
        ),
        route_boundary_anchors AS (
            -- Filter out nodes already identified as topology or ruteinfopunkt anchors
            SELECT
                rba.node_id,
                rba.geom,
                rba.degree,
                NULL::bigint as ruteinfopunkt_objid,
                NULL::double precision as ruteinfopunkt_distance_m
            FROM route_boundary_anchors_base rba
            WHERE NOT EXISTS (
                SELECT 1 FROM topology_anchors ta WHERE ta.node_id = rba.node_id
            )
            AND NOT EXISTS (
                SELECT 1 FROM ruteinfopunkt_matches rm WHERE rm.node_id = rba.node_id
            )
        )
        -- Combine all anchor types
        SELECT
            node_id,
            geom,
            degree,
            anchor_type,
            ruteinfopunkt_objid,
            ruteinfopunkt_distance_m
        FROM topology_anchors
        UNION ALL
        SELECT
            node_id,
            geom,
            degree,
            ''ruteinfopunkt'' as anchor_type,
            ruteinfopunkt_objid,
            distance_m as ruteinfopunkt_distance_m
        FROM ruteinfopunkt_matches
        UNION ALL
        SELECT
            node_id,
            geom,
            degree,
            ''route_boundary'' as anchor_type,
            ruteinfopunkt_objid,
            ruteinfopunkt_distance_m
        FROM route_boundary_anchors;
    ', dynamic_schema, dynamic_schema, dynamic_schema, dynamic_schema, dynamic_schema, dynamic_schema, dynamic_schema, dynamic_schema, dynamic_schema);

    -- Count route-boundary anchors
    EXECUTE format('
        SELECT COUNT(*) FROM %I.anchor_nodes WHERE anchor_type = ''route_boundary''
    ', dynamic_schema) INTO route_boundary_count;

    RAISE NOTICE '  ✓ Recreated anchor_nodes materialized view';
    RAISE NOTICE '  Found % route-boundary anchors', route_boundary_count;

    -- Recreate indexes
    RAISE NOTICE '  Recreating indexes...';
    EXECUTE format('
        CREATE INDEX IF NOT EXISTS idx_anchor_nodes_node_id
        ON %I.anchor_nodes USING BTREE (node_id);

        CREATE INDEX IF NOT EXISTS idx_anchor_nodes_geom_gist
        ON %I.anchor_nodes USING GIST (geom);

        CREATE INDEX IF NOT EXISTS idx_anchor_nodes_anchor_type
        ON %I.anchor_nodes USING BTREE (anchor_type);
    ', dynamic_schema, dynamic_schema, dynamic_schema);

    RAISE NOTICE '  ✓ Indexes recreated';

    -- Update statistics
    EXECUTE format('ANALYZE %I.anchor_nodes', dynamic_schema);
    RAISE NOTICE '  ✓ Analyzed anchor_nodes';

    -- Recreate VIEW in stiflyt schema (it was dropped due to CASCADE)
    -- This matches what migration 005 does
    IF EXISTS (
        SELECT 1 FROM pg_matviews
        WHERE schemaname = dynamic_schema AND matviewname = 'anchor_nodes'
    ) THEN
        RAISE NOTICE '  Recreating VIEW in stiflyt schema...';
        -- Ensure stiflyt schema exists
        BEGIN
            EXECUTE 'CREATE SCHEMA IF NOT EXISTS stiflyt';
            EXECUTE 'ALTER SCHEMA stiflyt OWNER TO stiflyt_owner';
        EXCEPTION WHEN insufficient_privilege THEN
            RAISE WARNING 'Insufficient privileges to create stiflyt schema. VIEW may not be created.';
        END;

        EXECUTE format('DROP VIEW IF EXISTS stiflyt.anchor_nodes CASCADE');
        EXECUTE format('CREATE VIEW stiflyt.anchor_nodes AS SELECT * FROM %I.anchor_nodes', dynamic_schema);

        -- Grant permissions (matching migration 005)
        BEGIN
            EXECUTE 'GRANT USAGE ON SCHEMA stiflyt TO stiflyt_owner';
            EXECUTE 'GRANT USAGE ON SCHEMA stiflyt TO stiflyt_updater';
            EXECUTE 'GRANT USAGE ON SCHEMA stiflyt TO stiflyt_reader';
            EXECUTE 'GRANT SELECT ON stiflyt.anchor_nodes TO stiflyt_reader';
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Could not grant permissions on stiflyt.anchor_nodes: %', SQLERRM;
        END;

        RAISE NOTICE '  ✓ Recreated stiflyt.anchor_nodes VIEW';
    END IF;

    RAISE NOTICE '';
    RAISE NOTICE 'Migration 010 completed successfully!';
    RAISE NOTICE '  Route-boundary anchors are now included in anchor_nodes.';
    RAISE NOTICE '  Example: Nodes where bre6 ends and bre5/bre57 start will now be anchor nodes.';

END $$;
