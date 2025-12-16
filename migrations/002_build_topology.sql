-- Migration: Build topology foundation from fotrute segments
-- Creates nodes table, links fotrute segments to nodes, and computes node degrees
-- Created: 2024
--
-- This migration builds a deterministic topology from fotrute segments by:
-- 1. Extracting unique nodes from segment endpoints (exact geometry matching)
-- 2. Linking fotrute segments to nodes via source_node/target_node
-- 3. Computing node degrees for graph analysis
-- 4. Identifying anchor nodes (degree != 2)

DO $$
DECLARE
    schema_name TEXT;
    segments_table TEXT := 'fotrute';
    geom_column TEXT := 'senterlinje';
    nodes_table TEXT := 'nodes';
    geom_column_quoted TEXT;
    segment_count BIGINT;
    node_count BIGINT;
    anchor_count BIGINT;
    start_time TIMESTAMP;
    step_time TIMESTAMP;
BEGIN
    start_time := clock_timestamp();
    RAISE NOTICE '=== Starting topology build migration ===';
    RAISE NOTICE 'Start time: %', start_time;
    -- Quote geometry column name for safe use in dynamic SQL
    geom_column_quoted := quote_ident(geom_column);
    -- Find the schema with prefix 'turogfriluftsruter_'
    -- The hash suffix changes with each dataset update, so we need to find it dynamically
    SELECT nspname INTO schema_name
    FROM pg_namespace
    WHERE nspname LIKE 'turogfriluftsruter_%'
      AND nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
    ORDER BY nspname DESC  -- Get the most recent one (typically highest hash or latest)
    LIMIT 1;

    IF schema_name IS NULL THEN
        RAISE WARNING 'Schema with prefix turogfriluftsruter_* not found. Skipping topology build.';
        RETURN;
    END IF;

    -- Verify fotrute table exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = schema_name AND table_name = segments_table
    ) THEN
        RAISE WARNING 'Table %.% does not exist. Skipping topology build.', schema_name, segments_table;
        RETURN;
    END IF;

    RAISE NOTICE 'Building topology in schema: %', schema_name;

    -- Get initial segment count for progress tracking
    EXECUTE format('SELECT COUNT(*) FROM %I.%I', schema_name, segments_table) INTO segment_count;
    RAISE NOTICE 'Found % segments in %.%', segment_count, schema_name, segments_table;

    -- Step 1: Create nodes table with unique endpoints
    step_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE 'Step 1: Creating nodes table...';
    -- Preserve SRID from segments table
    EXECUTE format('
        DROP TABLE IF EXISTS %I.%I CASCADE;

        CREATE TABLE %I.%I (
            id SERIAL PRIMARY KEY,
            geom GEOMETRY(POINT) NOT NULL,
            -- Deterministic hash for exact geometry matching
            geom_hash BYTEA NOT NULL UNIQUE
        );
    ', schema_name, nodes_table, schema_name, nodes_table);

    RAISE NOTICE '  ✓ Created table: %.%', schema_name, nodes_table;
    RAISE NOTICE '  Time: %', clock_timestamp() - step_time;

    -- Step 2: Populate nodes from all segment endpoints
    step_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE 'Step 2: Extracting unique nodes from segment endpoints...';
    RAISE NOTICE '  This may take 10-30 seconds for large datasets...';
    -- Use ST_AsEWKB for deterministic byte representation of geometry
    -- DISTINCT ON ensures we only get unique geometries
    EXECUTE format('
        INSERT INTO %I.%I (geom, geom_hash)
        SELECT DISTINCT ON (geom_hash)
            endpoint AS geom,
            geom_hash
        FROM (
            SELECT
                endpoint,
                ST_AsEWKB(endpoint) AS geom_hash
            FROM (
                -- Start points
                SELECT ST_StartPoint(%I) AS endpoint
                FROM %I.%I
                WHERE %I IS NOT NULL
                UNION ALL
                -- End points
                SELECT ST_EndPoint(%I) AS endpoint
                FROM %I.%I
                WHERE %I IS NOT NULL
            ) AS all_endpoints
        ) AS endpoints_with_hash
        ORDER BY geom_hash;
    ', schema_name, nodes_table,
       geom_column_quoted, schema_name, segments_table, geom_column_quoted,
       geom_column_quoted, schema_name, segments_table, geom_column_quoted);

    EXECUTE format('SELECT COUNT(*) FROM %I.%I', schema_name, nodes_table) INTO node_count;
    RAISE NOTICE '  ✓ Populated % unique nodes from segment endpoints', node_count;
    RAISE NOTICE '  Time: %', clock_timestamp() - step_time;

    -- Step 3: Create indexes on nodes BEFORE updating segments (critical for performance)
    -- The geom_hash index is especially important for the UPDATE in Step 4
    step_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE 'Step 3: Creating indexes on nodes table...';
    RAISE NOTICE '  Creating BTREE index on geom_hash...';
    EXECUTE format('
        CREATE INDEX IF NOT EXISTS idx_nodes_geom_hash
        ON %I.%I USING BTREE (geom_hash);
    ', schema_name, nodes_table);
    RAISE NOTICE '  ✓ BTREE index on geom_hash created';
    RAISE NOTICE '  Creating GIST index on geom (this may take 5-15 seconds)...';
    EXECUTE format('
        CREATE INDEX IF NOT EXISTS idx_nodes_geom_gist
        ON %I.%I USING GIST (geom);
    ', schema_name, nodes_table);
    RAISE NOTICE '  ✓ GIST index on geom created';
    RAISE NOTICE '  Time: %', clock_timestamp() - step_time;

    -- Step 4: Add node reference columns to segments
    step_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE 'Step 4: Adding node reference columns to segments...';
    EXECUTE format('
        ALTER TABLE %I.%I
        ADD COLUMN IF NOT EXISTS source_node INTEGER,
        ADD COLUMN IF NOT EXISTS target_node INTEGER;
    ', schema_name, segments_table, schema_name, segments_table);
    RAISE NOTICE '  ✓ Added source_node and target_node columns';
    RAISE NOTICE '  Time: %', clock_timestamp() - step_time;

    -- Step 5: Update fotrute segments with node references
    -- Match nodes by exact geometry hash (no tolerance)
    -- Using separate UPDATEs for source and target nodes for optimal performance
    -- The geom_hash index makes these lookups very fast
    step_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE 'Step 5: Updating segments with node references...';
    RAISE NOTICE '  Updating source_node for % segments (this may take 30 seconds - 2 minutes)...', segment_count;
    EXECUTE format('
        UPDATE %I.%I s
        SET source_node = n.id
        FROM %I.%I n
        WHERE s.%I IS NOT NULL
          AND n.geom_hash = ST_AsEWKB(ST_StartPoint(s.%I));
    ', schema_name, segments_table, schema_name, nodes_table,
       geom_column_quoted, geom_column_quoted);
    RAISE NOTICE '  ✓ Updated source_node';
    RAISE NOTICE '  Updating target_node for % segments...', segment_count;
    EXECUTE format('
        UPDATE %I.%I s
        SET target_node = n.id
        FROM %I.%I n
        WHERE s.%I IS NOT NULL
          AND n.geom_hash = ST_AsEWKB(ST_EndPoint(s.%I));
    ', schema_name, segments_table, schema_name, nodes_table,
       geom_column_quoted, geom_column_quoted);
    RAISE NOTICE '  ✓ Updated target_node';
    RAISE NOTICE '  Time: %', clock_timestamp() - step_time;

    -- Step 6: Create indexes on fotrute for node lookups
    step_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE 'Step 6: Creating indexes on segments node columns...';
    EXECUTE format('
        CREATE INDEX IF NOT EXISTS idx_fotrute_source_node
        ON %I.%I USING BTREE (source_node);

        CREATE INDEX IF NOT EXISTS idx_fotrute_target_node
        ON %I.%I USING BTREE (target_node);
    ', schema_name, segments_table, schema_name, segments_table);
    RAISE NOTICE '  ✓ Created indexes on source_node and target_node';
    RAISE NOTICE '  Time: %', clock_timestamp() - step_time;

    -- Step 7: Create materialized view for node degree
    -- Degree = count of segments connected to this node (as source or target)
    step_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE 'Step 7: Creating materialized view for node degree...';
    RAISE NOTICE '  Computing degrees for % nodes (this may take 10-30 seconds)...', node_count;
    EXECUTE format('
        DROP MATERIALIZED VIEW IF EXISTS %I.node_degree CASCADE;

        CREATE MATERIALIZED VIEW %I.node_degree AS
        SELECT
            n.id AS node_id,
            n.geom,
            COALESCE(degree_counts.degree, 0) AS degree
        FROM %I.%I n
        LEFT JOIN (
            SELECT
                node_id,
                COUNT(*) AS degree
            FROM (
                SELECT source_node AS node_id FROM %I.%I WHERE source_node IS NOT NULL
                UNION ALL
                SELECT target_node AS node_id FROM %I.%I WHERE target_node IS NOT NULL
            ) AS all_connections
            GROUP BY node_id
        ) AS degree_counts ON n.id = degree_counts.node_id;
    ', schema_name, schema_name, schema_name, nodes_table, schema_name, segments_table, schema_name, segments_table);
    RAISE NOTICE '  ✓ Created materialized view: node_degree';
    RAISE NOTICE '  Time: %', clock_timestamp() - step_time;

    -- Step 8: Create index on node_degree
    step_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE 'Step 8: Creating indexes on node_degree...';
    EXECUTE format('
        CREATE INDEX IF NOT EXISTS idx_node_degree_node_id
        ON %I.node_degree USING BTREE (node_id);

        CREATE INDEX IF NOT EXISTS idx_node_degree_degree
        ON %I.node_degree USING BTREE (degree);
    ', schema_name, schema_name);
    RAISE NOTICE '  ✓ Created indexes on node_degree';
    RAISE NOTICE '  Time: %', clock_timestamp() - step_time;

    -- Step 8.5: Create index on ruteinfopunkt.posisjon BEFORE spatial join (critical for performance)
    -- This index is essential for the ST_DWithin join in Step 9
    step_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE 'Step 8.5: Creating index on ruteinfopunkt.posisjon...';
    RAISE NOTICE '  This index is critical for the spatial join in Step 9';
    EXECUTE format('
        CREATE INDEX IF NOT EXISTS idx_ruteinfopunkt_posisjon_gist
        ON %I.ruteinfopunkt USING GIST (posisjon)
        WHERE posisjon IS NOT NULL;
    ', schema_name);
    RAISE NOTICE '  ✓ Created GIST index on ruteinfopunkt.posisjon';
    RAISE NOTICE '  Time: %', clock_timestamp() - step_time;

    -- Step 9: Create materialized view for anchor nodes
    -- Anchor nodes are:
    -- 1. Nodes with degree != 2 (endpoints, junctions, isolated nodes)
    -- 2. Segment endpoint nodes within 100m of ruteinfopunkt positions (DNT huts, route info points, etc.)
    --    These are important landmarks even if they have degree=2
    -- Optimized: First get topology anchors, then spatial join for ruteinfopunkt matches
    step_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE 'Step 9: Creating materialized view for anchor nodes...';
    RAISE NOTICE '  This step includes a spatial join and may take 1-5 minutes...';
    RAISE NOTICE '  Getting topology anchors (degree != 2)...';
    EXECUTE format('
        DROP MATERIALIZED VIEW IF EXISTS %I.anchor_nodes CASCADE;

        CREATE MATERIALIZED VIEW %I.anchor_nodes AS
        WITH topology_anchors AS (
            -- Fast: Get all nodes with degree != 2 (topology-based anchors)
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
            -- Optimized spatial join: Only check nodes with degree=2 that are not already anchors
            -- Use spatial index on both sides for fast lookup
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
        )
        -- Combine topology anchors and ruteinfopunkt matches
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
        FROM ruteinfopunkt_matches;
    ', schema_name, schema_name, schema_name, schema_name, schema_name);
    EXECUTE format('SELECT COUNT(*) FROM %I.anchor_nodes', schema_name) INTO anchor_count;
    RAISE NOTICE '  ✓ Created materialized view: anchor_nodes';
    RAISE NOTICE '  Found % anchor nodes', anchor_count;
    RAISE NOTICE '  Time: %', clock_timestamp() - step_time;

    -- Step 10: Create index on anchor_nodes
    step_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE 'Step 10: Creating indexes on anchor_nodes...';
    RAISE NOTICE '  Creating BTREE index on node_id...';
    EXECUTE format('
        CREATE INDEX IF NOT EXISTS idx_anchor_nodes_node_id
        ON %I.anchor_nodes USING BTREE (node_id);
    ', schema_name);
    RAISE NOTICE '  ✓ BTREE index on node_id created';
    RAISE NOTICE '  Creating GIST index on geom (this may take 5-15 seconds)...';
    EXECUTE format('
        CREATE INDEX IF NOT EXISTS idx_anchor_nodes_geom_gist
        ON %I.anchor_nodes USING GIST (geom);
    ', schema_name);
    RAISE NOTICE '  ✓ GIST index on geom created';
    EXECUTE format('
        CREATE INDEX IF NOT EXISTS idx_anchor_nodes_anchor_type
        ON %I.anchor_nodes USING BTREE (anchor_type);
    ', schema_name);
    RAISE NOTICE '  ✓ BTREE index on anchor_type created';
    RAISE NOTICE '  Time: %', clock_timestamp() - step_time;

    -- Step 11: Update statistics
    step_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE 'Step 11: Updating statistics (ANALYZE)...';
    EXECUTE format('ANALYZE %I.%I', schema_name, nodes_table);
    RAISE NOTICE '  ✓ Analyzed nodes';
    EXECUTE format('ANALYZE %I.%I', schema_name, segments_table);
    RAISE NOTICE '  ✓ Analyzed segments';
    EXECUTE format('ANALYZE %I.node_degree', schema_name);
    RAISE NOTICE '  ✓ Analyzed node_degree';
    EXECUTE format('ANALYZE %I.anchor_nodes', schema_name);
    RAISE NOTICE '  ✓ Analyzed anchor_nodes';
    EXECUTE format('ANALYZE %I.ruteinfopunkt', schema_name);
    RAISE NOTICE '  ✓ Analyzed ruteinfopunkt';
    RAISE NOTICE '  Time: %', clock_timestamp() - step_time;

    -- Summary
    RAISE NOTICE '';
    RAISE NOTICE '=== Topology build complete ===';
    RAISE NOTICE 'Summary:';
    RAISE NOTICE '  Schema: %', schema_name;
    RAISE NOTICE '  Segments: %', segment_count;
    RAISE NOTICE '  Nodes: %', node_count;
    RAISE NOTICE '  Anchor nodes: %', anchor_count;
    RAISE NOTICE '  Total time: %', clock_timestamp() - start_time;
    RAISE NOTICE 'Anchor nodes include: topology nodes (degree != 2) and ruteinfopunkt matches';

END $$;

