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
BEGIN
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

    -- Step 1: Create nodes table with unique endpoints
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

    RAISE NOTICE 'Created table: %.%', schema_name, nodes_table;

    -- Step 2: Populate nodes from all segment endpoints
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

    RAISE NOTICE 'Populated nodes from segment endpoints';

    -- Step 3: Add node reference columns to segments
    EXECUTE format('
        ALTER TABLE %I.%I
        ADD COLUMN IF NOT EXISTS source_node INTEGER,
        ADD COLUMN IF NOT EXISTS target_node INTEGER;
    ', schema_name, segments_table, schema_name, segments_table);

    RAISE NOTICE 'Added source_node and target_node columns to segments';

    -- Step 4: Update fotrute segments with node references
    -- Match nodes by exact geometry hash (no tolerance)
    -- Use separate subqueries to avoid cartesian product
    EXECUTE format('
        UPDATE %I.%I s
        SET
            source_node = (
                SELECT id FROM %I.%I
                WHERE geom_hash = ST_AsEWKB(ST_StartPoint(s.%I))
                LIMIT 1
            ),
            target_node = (
                SELECT id FROM %I.%I
                WHERE geom_hash = ST_AsEWKB(ST_EndPoint(s.%I))
                LIMIT 1
            )
        WHERE s.%I IS NOT NULL;
    ', schema_name, segments_table, schema_name, nodes_table, geom_column_quoted,
       schema_name, nodes_table, geom_column_quoted, geom_column_quoted);

    RAISE NOTICE 'Updated segments with node references';

    -- Step 5: Create indexes on nodes
    EXECUTE format('
        CREATE INDEX IF NOT EXISTS idx_nodes_geom_gist
        ON %I.%I USING GIST (geom);

        CREATE INDEX IF NOT EXISTS idx_nodes_geom_hash
        ON %I.%I USING BTREE (geom_hash);
    ', schema_name, nodes_table, schema_name, nodes_table);

    RAISE NOTICE 'Created indexes on nodes table';

    -- Step 6: Create indexes on fotrute for node lookups
    EXECUTE format('
        CREATE INDEX IF NOT EXISTS idx_fotrute_source_node
        ON %I.%I USING BTREE (source_node);

        CREATE INDEX IF NOT EXISTS idx_fotrute_target_node
        ON %I.%I USING BTREE (target_node);
    ', schema_name, segments_table, schema_name, segments_table);

    RAISE NOTICE 'Created indexes on segments node columns';

    -- Step 7: Create materialized view for node degree
    -- Degree = count of segments connected to this node (as source or target)
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

    RAISE NOTICE 'Created materialized view: node_degree';

    -- Step 8: Create index on node_degree
    EXECUTE format('
        CREATE INDEX IF NOT EXISTS idx_node_degree_node_id
        ON %I.node_degree USING BTREE (node_id);

        CREATE INDEX IF NOT EXISTS idx_node_degree_degree
        ON %I.node_degree USING BTREE (degree);
    ', schema_name, schema_name);

    RAISE NOTICE 'Created indexes on node_degree';

    -- Step 9: Create materialized view for anchor nodes
    -- Anchor nodes are:
    -- 1. Nodes with degree != 2 (endpoints, junctions, isolated nodes)
    -- 2. Segment endpoint nodes within 100m of ruteinfopunkt positions (DNT huts, route info points, etc.)
    --    These are important landmarks even if they have degree=2
    EXECUTE format('
        DROP MATERIALIZED VIEW IF EXISTS %I.anchor_nodes CASCADE;

        CREATE MATERIALIZED VIEW %I.anchor_nodes AS
        WITH ruteinfopunkt_matches AS (
            -- Find segment endpoint nodes within 100m of each ruteinfopunkt
            SELECT DISTINCT
                n.node_id,
                n.degree,
                rp.objid as ruteinfopunkt_objid,
                ST_Distance(n.geom, rp.posisjon) as distance_m
            FROM %I.ruteinfopunkt rp
            JOIN %I.node_degree n ON ST_DWithin(n.geom, rp.posisjon, 100)  -- Node within 100m
            WHERE rp.posisjon IS NOT NULL
        )
        SELECT DISTINCT
            n.node_id,
            n.geom,
            n.degree,
            CASE
                WHEN n.degree != 2 THEN ''topology''
                WHEN rm.ruteinfopunkt_objid IS NOT NULL THEN ''ruteinfopunkt''
                ELSE ''unknown''
            END as anchor_type,
            rm.ruteinfopunkt_objid,
            rm.distance_m as ruteinfopunkt_distance_m
        FROM %I.node_degree n
        LEFT JOIN ruteinfopunkt_matches rm ON n.node_id = rm.node_id
        WHERE n.degree != 2
           OR rm.ruteinfopunkt_objid IS NOT NULL;
    ', schema_name, schema_name, schema_name, schema_name, schema_name);

    RAISE NOTICE 'Created materialized view: anchor_nodes';

    -- Step 10: Create index on anchor_nodes
    EXECUTE format('
        CREATE INDEX IF NOT EXISTS idx_anchor_nodes_node_id
        ON %I.anchor_nodes USING BTREE (node_id);

        CREATE INDEX IF NOT EXISTS idx_anchor_nodes_geom_gist
        ON %I.anchor_nodes USING GIST (geom);

        CREATE INDEX IF NOT EXISTS idx_anchor_nodes_anchor_type
        ON %I.anchor_nodes USING BTREE (anchor_type);
    ', schema_name, schema_name, schema_name);

    -- Step 11: Create index on ruteinfopunkt for faster matching
    EXECUTE format('
        CREATE INDEX IF NOT EXISTS idx_ruteinfopunkt_posisjon_gist
        ON %I.ruteinfopunkt USING GIST (posisjon)
        WHERE posisjon IS NOT NULL;
    ', schema_name);

    RAISE NOTICE 'Created indexes on anchor_nodes';

    -- Step 12: Update statistics
    EXECUTE format('ANALYZE %I.%I', schema_name, nodes_table);
    EXECUTE format('ANALYZE %I.%I', schema_name, segments_table);
    EXECUTE format('ANALYZE %I.node_degree', schema_name);
    EXECUTE format('ANALYZE %I.anchor_nodes', schema_name);
    EXECUTE format('ANALYZE %I.ruteinfopunkt', schema_name);

    RAISE NOTICE 'Topology build complete';
    RAISE NOTICE 'Anchor nodes include: topology nodes (degree != 2) and ruteinfopunkt matches';

END $$;

