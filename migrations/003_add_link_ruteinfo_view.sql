-- Migration: Add view for link ruteinfo
-- Creates a view that joins links with fotruteinfo for easy access to rutenavn, rutenummer, etc.
-- Created: 2024

DO $$
DECLARE
    schema_name TEXT;
    links_exists BOOLEAN;
BEGIN
    -- Find the schema with prefix 'turogfriluftsruter_'
    SELECT nspname INTO schema_name
    FROM pg_namespace
    WHERE nspname LIKE 'turogfriluftsruter_%'
      AND nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
    ORDER BY nspname DESC
    LIMIT 1;

    IF schema_name IS NULL THEN
        RAISE WARNING 'Schema with prefix turogfriluftsruter_* not found. Skipping view creation.';
        RETURN;
    END IF;

    -- Check if links table exists (it's created by build_links.py, which runs automatically before this migration)
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = schema_name AND table_name = 'links'
    ) INTO links_exists;

    IF NOT links_exists THEN
        RAISE WARNING 'Table %.links does not exist. Skipping view creation.', schema_name;
        RAISE NOTICE 'Note: links table should be created automatically by the migrations system before this migration runs.';
        RAISE NOTICE 'If this warning appears, build-links may have failed. Check migration logs for details.';
        RAISE NOTICE 'You can also run "make build-links" manually to create the links table.';
        RETURN;
    END IF;

    RAISE NOTICE 'Creating link_ruteinfo view in schema: %', schema_name;

    -- Check if fotruteinfo exists (required for the view)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = schema_name AND table_name = 'fotruteinfo'
    ) THEN
        RAISE WARNING 'Table %.fotruteinfo does not exist. Skipping view creation.', schema_name;
        RETURN;
    END IF;

    -- Create view that joins links with fotruteinfo
    -- This view expands the segment_objids array and joins with fotruteinfo
    -- Note: A link can have multiple ruteinfo rows if segments belong to different routes
    BEGIN
        EXECUTE format('
            DROP VIEW IF EXISTS %I.link_ruteinfo CASCADE;

            CREATE VIEW %I.link_ruteinfo AS
            SELECT DISTINCT
                l.link_id,
                l.a_node,
                l.b_node,
                l.length_m,
                l.geom,
                l.segment_objids,
                fi.rutenavn,
                fi.rutenummer,
                fi.vedlikeholdsansvarlig,
                fi.ruteinformasjon,
                fi.spesialfotrutetype,
                fi.gradering,
                fi.rutetype,
                fi.rutebetydning,
                fi.tilpasning
            FROM %I.links l
            CROSS JOIN LATERAL unnest(l.segment_objids) AS segment_objid
            JOIN %I.fotruteinfo fi ON fi.fotrute_fk = segment_objid
            WHERE l.segment_objids IS NOT NULL;
        ', schema_name, schema_name, schema_name, schema_name);

        RAISE NOTICE 'Created view: %.link_ruteinfo', schema_name;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Failed to create link_ruteinfo view: %', SQLERRM;
        RAISE NOTICE 'This is non-fatal - view will be skipped';
    END;

    -- Create a simpler view that aggregates ruteinfo per link
    -- This gives one row per link with aggregated route information
    BEGIN
        EXECUTE format('
            DROP VIEW IF EXISTS %I.links_with_routes CASCADE;

            CREATE VIEW %I.links_with_routes AS
            SELECT
                l.link_id,
                l.a_node,
                l.b_node,
                l.length_m,
                l.geom,
                l.segment_objids,
                -- Aggregate route information (distinct values)
                array_agg(DISTINCT fi.rutenavn) FILTER (WHERE fi.rutenavn IS NOT NULL) as rutenavn_list,
                array_agg(DISTINCT fi.rutenummer) FILTER (WHERE fi.rutenummer IS NOT NULL) as rutenummer_list,
                array_agg(DISTINCT fi.vedlikeholdsansvarlig) FILTER (WHERE fi.vedlikeholdsansvarlig IS NOT NULL) as vedlikeholdsansvarlig_list,
                array_agg(DISTINCT fi.rutetype) FILTER (WHERE fi.rutetype IS NOT NULL) as rutetype_list
            FROM %I.links l
            LEFT JOIN LATERAL unnest(COALESCE(l.segment_objids, ARRAY[]::bigint[])) AS segment_objid ON true
            LEFT JOIN %I.fotruteinfo fi ON fi.fotrute_fk = segment_objid
            GROUP BY l.link_id, l.a_node, l.b_node, l.length_m, l.geom, l.segment_objids;
        ', schema_name, schema_name, schema_name, schema_name);

        RAISE NOTICE 'Created view: %.links_with_routes', schema_name;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Failed to create links_with_routes view: %', SQLERRM;
        RAISE NOTICE 'This is non-fatal - view will be skipped';
    END;

END $$;

