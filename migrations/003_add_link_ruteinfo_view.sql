-- Migration: Add view for link ruteinfo
-- Creates a view that joins links with fotruteinfo for easy access to rutenavn, rutenummer, etc.
-- Created: 2024

DO $$
DECLARE
    schema_name TEXT;
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

    RAISE NOTICE 'Creating link_ruteinfo view in schema: %', schema_name;

    -- Create view that joins links with fotruteinfo
    -- This view expands the segment_objids array and joins with fotruteinfo
    -- Note: A link can have multiple ruteinfo rows if segments belong to different routes
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

    -- Create a simpler view that aggregates ruteinfo per link
    -- This gives one row per link with aggregated route information
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

END $$;

