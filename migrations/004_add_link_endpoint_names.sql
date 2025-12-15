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
    stedsnavn_exists BOOLEAN;
    skrivemate_exists BOOLEAN;
    sted_posisjon_exists BOOLEAN;
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
    -- Stedsnavn has a normalized structure:
    -- - stedsnavn table: metadata (objid, sted_fk, navnestatus, etc.)
    -- - skrivemate table: actual name (komplettskrivemate) linked via stedsnavn.objid = skrivemate.stedsnavn_fk
    -- - sted_posisjon/sted_omrade/sted_senterlinje/sted_multipunkt: geometry linked via stedsnavn.sted_fk = sted_*.stedsnummer
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = stedsnavn_schema AND table_name = 'stedsnavn'
    ) INTO stedsnavn_exists;

    IF NOT stedsnavn_exists THEN
        RAISE WARNING 'Stedsnavn table not found in %. Will only use ruteinfopunkt for endpoint names.', stedsnavn_schema;
    ELSE
        -- Check if required related tables exist
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = stedsnavn_schema AND table_name = 'skrivemate'
        ) INTO skrivemate_exists;

        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = stedsnavn_schema AND table_name = 'sted_posisjon'
        ) INTO sted_posisjon_exists;

        IF NOT skrivemate_exists THEN
            RAISE WARNING 'skrivemate table not found in %. Will only use ruteinfopunkt for endpoint names.', stedsnavn_schema;
            stedsnavn_exists := FALSE;
        ELSIF NOT sted_posisjon_exists THEN
            RAISE WARNING 'sted_posisjon table not found in %. Will only use ruteinfopunkt for endpoint names.', stedsnavn_schema;
            stedsnavn_exists := FALSE;
        ELSE
            RAISE NOTICE 'Found stedsnavn structure: stedsnavn + skrivemate + sted_posisjon';
        END IF;
    END IF;

    -- Step 1: Create materialized view for node names
    -- This matches nodes to ruteinfopunkt and stedsnavn using spatial proximity
    IF stedsnavn_exists THEN
            -- Version with both ruteinfopunkt and stedsnavn
            -- Stedsnavn structure: stedsnavn (metadata) -> skrivemate (name) + sted_posisjon (geometry)
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
                      AND rp.tilrettelegging IN (
                          ''12'', -- Hytte
                          ''42'', -- Hytte betjent
                          ''43'', -- Hytte selvbetjent
                          ''44'', -- Hytte ubetjent
                          ''22''  -- Parkeringsplass
                      )
                    ORDER BY n.id, ST_Distance(n.geom, rp.posisjon)
                ),
                node_stedsnavn AS (
                    -- Match nodes to stedsnavn within 200m (only if no ruteinfopunkt match)
                    -- Join: stedsnavn -> skrivemate (for name) -> sted_posisjon (for geometry)
                    SELECT DISTINCT ON (n.id)
                        n.id as node_id,
                        sn.objid as stedsnavn_objid,
                        -- Get name from skrivemate table
                        NULLIF(TRIM(sm.komplettskrivemate), '''') as navn,
                        ST_Distance(n.geom, sp.geom) as distance_m,
                        ''stedsnavn'' as navn_kilde
                    FROM %I.nodes n
                    LEFT JOIN node_ruteinfopunkt nrp ON n.id = nrp.node_id
                    JOIN %I.stedsnavn sn ON sn.sted_fk IS NOT NULL
                    JOIN %I.skrivemate sm ON sn.objid = sm.stedsnavn_fk
                    JOIN %I.sted_posisjon sp ON sn.sted_fk = sp.stedsnummer
                    WHERE nrp.node_id IS NULL  -- Only if no ruteinfopunkt match
                      AND ST_DWithin(n.geom, sp.geom, 500)
                      AND sm.komplettskrivemate IS NOT NULL
                      AND sp.geom IS NOT NULL
                    ORDER BY n.id, ST_Distance(n.geom, sp.geom)
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
            ', schema_name, schema_name, schema_name, schema_name, schema_name,
               stedsnavn_schema, stedsnavn_schema, stedsnavn_schema);
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
            -- Add name information from node_names, with coordinate-based fallback
            COALESCE(
                nn.navn,
                ''UTM25833 '' ||
                ROUND(ST_X(n.geom))::text || '' '' ||
                ROUND(ST_Y(n.geom))::text
            ) as navn,
            CASE
                WHEN nn.navn IS NOT NULL THEN nn.navn_kilde
                ELSE ''koordinat''
            END as navn_kilde,
            CASE
                WHEN nn.navn IS NOT NULL THEN nn.distance_m
                ELSE NULL::double precision
            END as navn_distance_m
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

