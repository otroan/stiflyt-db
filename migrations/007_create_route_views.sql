-- Migration: Create route views for easy route lookup and visualization
-- Creates route_segments view and routes materialized view
-- Created: 2025
--
-- This migration creates:
-- 1. route_segments view - Individual segments with route info
-- 2. routes materialized view - Complete routes with aggregated geometry
-- These views are then made available in stiflyt schema via migration 005

DO $$
DECLARE
    schema_name TEXT;
    fotrute_exists BOOLEAN;
    fotruteinfo_exists BOOLEAN;
    start_time TIMESTAMP;
    step_time TIMESTAMP;
    duplicate_count INTEGER;
BEGIN
    start_time := clock_timestamp();
    RAISE NOTICE '=== Starting route views migration ===';
    RAISE NOTICE 'Start time: %', start_time;

    -- Find the schema with prefix 'turogfriluftsruter_'
    SELECT nspname INTO schema_name
    FROM pg_namespace
    WHERE nspname LIKE 'turogfriluftsruter_%'
      AND nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
    ORDER BY nspname DESC
    LIMIT 1;

    IF schema_name IS NULL THEN
        RAISE WARNING 'Schema with prefix turogfriluftsruter_* not found. Skipping route views creation.';
        RETURN;
    END IF;

    RAISE NOTICE 'Creating route views in schema: %', schema_name;

    -- Check if required tables exist
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = schema_name AND table_name = 'fotrute'
    ) INTO fotrute_exists;

    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = schema_name AND table_name = 'fotruteinfo'
    ) INTO fotruteinfo_exists;

    IF NOT fotrute_exists OR NOT fotruteinfo_exists THEN
        RAISE WARNING 'Required tables missing in %.%. Skipping route views creation.', schema_name,
            CASE WHEN NOT fotrute_exists THEN 'fotrute' ELSE 'fotruteinfo' END;
        RETURN;
    END IF;

    -- Step 1: Create route_segments view
    step_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE 'Step 1: Creating route_segments view...';
    BEGIN
        EXECUTE format('
            DROP VIEW IF EXISTS %I.route_segments CASCADE;

            CREATE VIEW %I.route_segments AS
            SELECT
                fi.rutenummer,
                f.objid as segment_objid,
                f.senterlinje,
                f.source_node,
                f.target_node,
                fi.rutenavn,
                fi.vedlikeholdsansvarlig,
                fi.rutetype,
                fi.gradering,
                fi.ruteinformasjon,
                fi.spesialfotrutetype,
                fi.rutebetydning,
                fi.tilpasning
            FROM %I.fotrute f
            JOIN %I.fotruteinfo fi ON fi.fotrute_fk = f.objid
            WHERE fi.rutenummer IS NOT NULL;
        ', schema_name, schema_name, schema_name, schema_name);

        RAISE NOTICE '  ✓ Created view: %.route_segments', schema_name;
        RAISE NOTICE '  Time: %', clock_timestamp() - step_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Failed to create route_segments view: %', SQLERRM;
        RAISE NOTICE 'This is non-fatal - view will be skipped';
    END;

    -- Step 2: Create routes materialized view
    step_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE 'Step 2: Creating routes materialized view...';
    RAISE NOTICE '  This may take 30 seconds - 2 minutes for large datasets...';
    BEGIN
        EXECUTE format('
            DROP MATERIALIZED VIEW IF EXISTS %I.routes CASCADE;

            CREATE MATERIALIZED VIEW %I.routes AS
            SELECT
                fi.rutenummer,
                -- Use MAX() to pick one value when same rutenummer has different metadata
                -- (typically all segments of a route should have same metadata, but handle edge cases)
                MAX(fi.rutenavn) as rutenavn,
                MAX(fi.vedlikeholdsansvarlig) as vedlikeholdsansvarlig,
                MAX(fi.rutetype) as rutetype,
                COUNT(DISTINCT f.objid) as segment_count,
                SUM(ST_Length(f.senterlinje)) as total_length_m,
                ST_Union(f.senterlinje) as route_geometry,
                array_agg(DISTINCT f.objid ORDER BY f.objid) as segment_objids
            FROM %I.fotrute f
            JOIN %I.fotruteinfo fi ON fi.fotrute_fk = f.objid
            WHERE fi.rutenummer IS NOT NULL
            GROUP BY fi.rutenummer;
        ', schema_name, schema_name, schema_name, schema_name);

        RAISE NOTICE '  ✓ Created materialized view: %.routes', schema_name;
        RAISE NOTICE '  Time: %', clock_timestamp() - step_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Failed to create routes materialized view: %', SQLERRM;
        RAISE NOTICE 'This is non-fatal - view will be skipped';
    END;

    -- Step 3: Create indexes on routes materialized view
    step_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE 'Step 3: Creating indexes on routes materialized view...';
    BEGIN
        -- Check for duplicate rutenummer values (should not happen after fix, but check anyway)
        EXECUTE format('SELECT COUNT(*) - COUNT(DISTINCT rutenummer) FROM %I.routes', schema_name) INTO duplicate_count;
        IF duplicate_count > 0 THEN
            RAISE WARNING 'Found % duplicate rutenummer values in routes view. This will prevent unique index creation.', duplicate_count;
            RAISE WARNING 'This may indicate data quality issues - same rutenummer with different metadata.';
        ELSE
            RAISE NOTICE '  ✓ Verified: No duplicate rutenummer values';
        END IF;

        -- Unique index on rutenummer for fast route lookup
        EXECUTE format('
            CREATE UNIQUE INDEX IF NOT EXISTS idx_routes_rutenummer
            ON %I.routes USING BTREE (rutenummer);
        ', schema_name);
        RAISE NOTICE '  ✓ Created unique index: idx_routes_rutenummer';

        -- GIST index on route_geometry for spatial queries
        EXECUTE format('
            CREATE INDEX IF NOT EXISTS idx_routes_geometry_gist
            ON %I.routes USING GIST (route_geometry);
        ', schema_name);
        RAISE NOTICE '  ✓ Created GIST index: idx_routes_geometry_gist';

        -- Index on vedlikeholdsansvarlig for organization filtering
        EXECUTE format('
            CREATE INDEX IF NOT EXISTS idx_routes_vedlikeholdsansvarlig
            ON %I.routes USING BTREE (vedlikeholdsansvarlig);
        ', schema_name);
        RAISE NOTICE '  ✓ Created index: idx_routes_vedlikeholdsansvarlig';

        RAISE NOTICE '  Time: %', clock_timestamp() - step_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Failed to create indexes on routes: %', SQLERRM;
        RAISE NOTICE 'This is non-fatal - indexes will be skipped';
    END;

    -- Step 4: Create index on route_segments view (on underlying table)
    step_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE 'Step 4: Creating index on route_segments (via fotruteinfo)...';
    BEGIN
        -- Index on rutenummer in fotruteinfo (if not already exists from migration 001)
        EXECUTE format('
            CREATE INDEX IF NOT EXISTS idx_fotruteinfo_rutenummer_route_segments
            ON %I.fotruteinfo USING BTREE (rutenummer)
            WHERE rutenummer IS NOT NULL;
        ', schema_name);
        RAISE NOTICE '  ✓ Created/verified index on fotruteinfo.rutenummer';
        RAISE NOTICE '  Time: %', clock_timestamp() - step_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Failed to create index on fotruteinfo: %', SQLERRM;
        RAISE NOTICE 'This is non-fatal - index may already exist';
    END;

    -- Step 5: Update statistics
    step_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE 'Step 5: Updating statistics (ANALYZE)...';
    BEGIN
        EXECUTE format('ANALYZE %I.routes', schema_name);
        RAISE NOTICE '  ✓ Analyzed routes';
        EXECUTE format('ANALYZE %I.fotruteinfo', schema_name);
        RAISE NOTICE '  ✓ Analyzed fotruteinfo';
        RAISE NOTICE '  Time: %', clock_timestamp() - step_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Failed to analyze tables: %', SQLERRM;
        RAISE NOTICE 'This is non-fatal - statistics update skipped';
    END;

    -- Step 6: Update stiflyt stable views (since migration 005 runs before this)
    step_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE 'Step 6: Updating stiflyt stable views...';
    BEGIN
        -- Ensure stiflyt schema exists
        EXECUTE format('CREATE SCHEMA IF NOT EXISTS stiflyt');
        EXECUTE format('ALTER SCHEMA stiflyt OWNER TO stiflyt_owner');
        EXECUTE format('GRANT USAGE ON SCHEMA stiflyt TO stiflyt_owner');
        EXECUTE format('GRANT USAGE ON SCHEMA stiflyt TO stiflyt_updater');
        EXECUTE format('GRANT USAGE ON SCHEMA stiflyt TO stiflyt_reader');
        EXECUTE format('GRANT CREATE ON SCHEMA stiflyt TO stiflyt_updater');
        EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA stiflyt TO stiflyt_reader');
        EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE stiflyt_owner IN SCHEMA stiflyt GRANT SELECT ON TABLES TO stiflyt_reader');

        -- Create stable view for route_segments
        EXECUTE format('DROP VIEW IF EXISTS stiflyt.route_segments CASCADE');
        EXECUTE format('CREATE VIEW stiflyt.route_segments AS SELECT * FROM %I.route_segments', schema_name);
        RAISE NOTICE '  ✓ Created stable view: stiflyt.route_segments';

        -- Create stable view for routes (materialized view)
        EXECUTE format('DROP VIEW IF EXISTS stiflyt.routes CASCADE');
        EXECUTE format('CREATE VIEW stiflyt.routes AS SELECT * FROM %I.routes', schema_name);
        RAISE NOTICE '  ✓ Created stable view: stiflyt.routes';

        RAISE NOTICE '  Time: %', clock_timestamp() - step_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Failed to update stiflyt stable views: %', SQLERRM;
        RAISE NOTICE 'This is non-fatal - views can be created by re-running migration 005';
    END;

    -- Summary
    RAISE NOTICE '';
    RAISE NOTICE '=== Route views migration complete ===';
    RAISE NOTICE 'Summary:';
    RAISE NOTICE '  Schema: %', schema_name;
    RAISE NOTICE '  Views created:';
    RAISE NOTICE '    - %.route_segments (view)', schema_name;
    RAISE NOTICE '    - %.routes (materialized view)', schema_name;
    RAISE NOTICE '  Stable views updated:';
    RAISE NOTICE '    - stiflyt.route_segments';
    RAISE NOTICE '    - stiflyt.routes';
    RAISE NOTICE '  Total time: %', clock_timestamp() - start_time;

END $$;

