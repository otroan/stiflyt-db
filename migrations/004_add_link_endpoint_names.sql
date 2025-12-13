-- Migration: Add names to anchor nodes from ruteinfopunkt and stedsnavn
-- Enriches anchor_nodes with names from nearby ruteinfopunkt and stedsnavn
-- Created: 2024
--
-- This migration:
-- 1. Creates node_names materialized view that matches nodes to:
--    - ruteinfopunkt (route info points) - within 100m
--      - Uses opphav field as primary name source, falls back to informasjon if opphav is empty
--    - stedsnavn (place names) - within 200m (fallback if no ruteinfopunkt match)
-- 2. Updates anchor_nodes to include names by joining with node_names
-- 3. Prioritizes ruteinfopunkt names over stedsnavn names

DO $$
DECLARE
    schema_name TEXT;
    stedsnavn_schema TEXT := 'public';
    stedsnavn_table TEXT := 'stedsnavn';
    stedsnavn_exists BOOLEAN;
BEGIN
    -- Find the schema with prefix 'turogfriluftsruter_'
    SELECT nspname INTO schema_name
    FROM pg_namespace
    WHERE nspname LIKE 'turogfriluftsruter_%'
      AND nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
    ORDER BY nspname DESC
    LIMIT 1;

    IF schema_name IS NULL THEN
        RAISE WARNING 'Schema with prefix turogfriluftsruter_* not found. Skipping endpoint names view creation.';
        RETURN;
    END IF;

    RAISE NOTICE 'Adding names to anchor nodes in schema: %', schema_name;

    -- Check if stedsnavn table exists
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = stedsnavn_schema AND table_name = stedsnavn_table
    ) INTO stedsnavn_exists;

    IF NOT stedsnavn_exists THEN
        RAISE WARNING 'Table %.% does not exist. Will only use ruteinfopunkt for endpoint names.', stedsnavn_schema, stedsnavn_table;
    END IF;

    -- Step 1: Create materialized view for node names
    -- This matches nodes to ruteinfopunkt and stedsnavn using spatial proximity
    IF stedsnavn_exists THEN
            -- Version with both ruteinfopunkt and stedsnavn
            EXECUTE format('
                DROP MATERIALIZED VIEW IF EXISTS %I.node_names CASCADE;

                CREATE MATERIALIZED VIEW %I.node_names AS
                WITH node_ruteinfopunkt AS (
                    -- Match nodes to ruteinfopunkt within 100m
                    SELECT DISTINCT ON (n.id)
                        n.id as node_id,
                        rp.objid as ruteinfopunkt_objid,
                        -- Use opphav as primary name field, fallback to informasjon if opphav is empty
                        COALESCE(
                            NULLIF(TRIM(rp.opphav), ''''),
                            NULLIF(TRIM(rp.informasjon), '''')
                        ) as navn,
                        ST_Distance(n.geom, rp.posisjon) as distance_m,
                        ''ruteinfopunkt'' as navn_kilde
                    FROM %I.nodes n
                    JOIN %I.ruteinfopunkt rp ON ST_DWithin(n.geom, rp.posisjon, 100)
                    WHERE rp.posisjon IS NOT NULL
                    ORDER BY n.id, ST_Distance(n.geom, rp.posisjon)
                ),
                node_stedsnavn AS (
                    -- Match nodes to stedsnavn within 200m (only if no ruteinfopunkt match)
                    SELECT DISTINCT ON (n.id)
                        n.id as node_id,
                        sn.objid as stedsnavn_objid,
                        -- Try common name columns in stedsnavn
                        COALESCE(
                            NULLIF(sn.navn, ''''),
                            NULLIF(sn.stedsnavn, ''''),
                            NULLIF(sn.name, '''')
                        ) as navn,
                        ST_Distance(n.geom, sn.geom) as distance_m,
                        ''stedsnavn'' as navn_kilde
                    FROM %I.nodes n
                    LEFT JOIN node_ruteinfopunkt nrp ON n.id = nrp.node_id
                    JOIN %I.%I sn ON ST_DWithin(n.geom, sn.geom, 200)
                    WHERE nrp.node_id IS NULL  -- Only if no ruteinfopunkt match
                      AND sn.geom IS NOT NULL
                    ORDER BY n.id, ST_Distance(n.geom, sn.geom)
                )
                -- Combine ruteinfopunkt and stedsnavn matches, prioritizing ruteinfopunkt
                SELECT
                    node_id,
                    navn,
                    navn_kilde,
                    distance_m
                FROM node_ruteinfopunkt
                UNION ALL
                SELECT
                    node_id,
                    navn,
                    navn_kilde,
                    distance_m
                FROM node_stedsnavn;
            ', schema_name, schema_name, schema_name, schema_name, schema_name, stedsnavn_schema, stedsnavn_table);
        ELSE
            -- Version with only ruteinfopunkt
            EXECUTE format('
                DROP MATERIALIZED VIEW IF EXISTS %I.node_names CASCADE;

                CREATE MATERIALIZED VIEW %I.node_names AS
                SELECT DISTINCT ON (n.id)
                    n.id as node_id,
                    rp.objid as ruteinfopunkt_objid,
                    -- Use opphav as primary name field, fallback to informasjon if opphav is empty
                    COALESCE(
                        NULLIF(TRIM(rp.opphav), ''''),
                        NULLIF(TRIM(rp.informasjon), '''')
                    ) as navn,
                    ST_Distance(n.geom, rp.posisjon) as distance_m,
                    ''ruteinfopunkt'' as navn_kilde
                FROM %I.nodes n
                JOIN %I.ruteinfopunkt rp ON ST_DWithin(n.geom, rp.posisjon, 100)
                WHERE rp.posisjon IS NOT NULL
                ORDER BY n.id, ST_Distance(n.geom, rp.posisjon);
        ', schema_name, schema_name, schema_name, schema_name, schema_name);
    END IF;

    RAISE NOTICE 'Created materialized view: %.node_names', schema_name;

    -- Step 2: Create indexes on node_names
    EXECUTE format('
        CREATE INDEX IF NOT EXISTS idx_node_names_node_id
        ON %I.node_names USING BTREE (node_id);

        CREATE INDEX IF NOT EXISTS idx_node_names_navn_kilde
        ON %I.node_names USING BTREE (navn_kilde);
    ', schema_name, schema_name);

    RAISE NOTICE 'Created indexes on node_names';

    -- Step 3: Update anchor_nodes to include names
    -- We recreate anchor_nodes with names by joining with node_names
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
            rm.distance_m as ruteinfopunkt_distance_m,
            -- Add name information from node_names
            nn.navn,
            nn.navn_kilde,
            nn.distance_m as navn_distance_m
        FROM %I.node_degree n
        LEFT JOIN ruteinfopunkt_matches rm ON n.node_id = rm.node_id
        LEFT JOIN %I.node_names nn ON n.node_id = nn.node_id
        WHERE n.degree != 2
           OR rm.ruteinfopunkt_objid IS NOT NULL;
    ', schema_name, schema_name, schema_name, schema_name, schema_name, schema_name, schema_name);

    RAISE NOTICE 'Updated materialized view: %.anchor_nodes (now includes names)', schema_name;

    -- Step 4: Recreate indexes on anchor_nodes
    EXECUTE format('
        CREATE INDEX IF NOT EXISTS idx_anchor_nodes_node_id
        ON %I.anchor_nodes USING BTREE (node_id);

        CREATE INDEX IF NOT EXISTS idx_anchor_nodes_geom_gist
        ON %I.anchor_nodes USING GIST (geom);

        CREATE INDEX IF NOT EXISTS idx_anchor_nodes_anchor_type
        ON %I.anchor_nodes USING BTREE (anchor_type);

        CREATE INDEX IF NOT EXISTS idx_anchor_nodes_navn
        ON %I.anchor_nodes USING BTREE (navn) WHERE navn IS NOT NULL;
    ', schema_name, schema_name, schema_name, schema_name);

    RAISE NOTICE 'Created indexes on anchor_nodes';

    -- Step 5: Update statistics
    EXECUTE format('ANALYZE %I.node_names', schema_name);
    EXECUTE format('ANALYZE %I.anchor_nodes', schema_name);

    RAISE NOTICE 'Anchor node names migration complete';

END $$;

