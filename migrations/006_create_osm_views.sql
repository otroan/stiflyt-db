-- Migration: Create OSM path views and comparison structures
-- Creates views for OSM hiking paths and structures for comparing with Turrutebasen
-- Created: 2026-01-05
--
-- This migration:
-- 1. Creates views for OSM hiking paths (filtered from lines table)
-- 2. Adds OSM views to stiflyt schema for backend access
-- 3. Creates a table for tracking OSM path matches with Turrutebasen routes
-- 4. Creates a table for changeset tracking (OSM -> Turrutebasen)

DO $$
DECLARE
    view_schema TEXT := 'stiflyt';
    osm_lines_exists BOOLEAN;
    osm_points_exists BOOLEAN;
BEGIN
    RAISE NOTICE '=== Creating OSM path views and comparison structures ===';

    -- Check if OSM tables exist in public schema
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'lines'
    ) INTO osm_lines_exists;

    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'points'
    ) INTO osm_points_exists;

    IF NOT osm_lines_exists THEN
        RAISE WARNING 'OSM lines table not found in public schema. OSM data may not be loaded yet.';
        RAISE NOTICE 'Skipping OSM view creation. Run this migration again after loading OSM data.';
        RETURN;
    END IF;

    -- Ensure stiflyt schema exists
    BEGIN
        EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', view_schema);
        EXECUTE format('ALTER SCHEMA %I OWNER TO stiflyt_owner', view_schema);
    EXCEPTION WHEN insufficient_privilege THEN
        RAISE WARNING 'Insufficient privileges to create schema %.', view_schema;
        RAISE;
    END;

    -- Grant privileges on stiflyt schema
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO stiflyt_owner', view_schema);
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO stiflyt_updater', view_schema);
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO stiflyt_reader', view_schema);
    EXECUTE format('GRANT CREATE ON SCHEMA %I TO stiflyt_updater', view_schema);

    -- Create view for OSM hiking paths
    -- Filters OSM lines for hiking-related highway types
    RAISE NOTICE 'Creating view: osm_hiking_paths';
    EXECUTE format('
        DROP VIEW IF EXISTS %I.osm_hiking_paths CASCADE;

        CREATE VIEW %I.osm_hiking_paths AS
        SELECT
            ogc_fid,
            osm_id,
            name,
            highway,
            sac_scale,
            trail_visibility,
            surface,
            tracktype,
            route,
            operator,
            ref,
            network,
            geom,
            -- Additional OSM tags (if present in the lines table)
            CASE
                WHEN highway IN (''path'', ''footway'', ''track'', ''bridleway'') THEN true
                WHEN route = ''hiking'' THEN true
                ELSE false
            END AS is_hiking_path
        FROM public.lines
        WHERE (
            -- Primary hiking path types
            highway IN (''path'', ''footway'', ''track'', ''bridleway'')
            OR route = ''hiking''
            OR route = ''foot''
            -- Include paths with hiking-related tags even if highway type is different
            OR (highway IS NOT NULL AND (
                sac_scale IS NOT NULL
                OR trail_visibility IS NOT NULL
                OR (name IS NOT NULL AND (
                    name ILIKE ''%%sti%%''
                    OR name ILIKE ''%%path%%''
                    OR name ILIKE ''%%rute%%''
                    OR name ILIKE ''%%DNT%%''
                ))
            ))
        )
        AND geom IS NOT NULL;
    ', view_schema, view_schema);

    RAISE NOTICE '  ✓ Created view: osm_hiking_paths';

    -- Create view for OSM path points (huts, shelters, etc.)
    IF osm_points_exists THEN
        RAISE NOTICE 'Creating view: osm_hiking_points';
        EXECUTE format('
            DROP VIEW IF EXISTS %I.osm_hiking_points CASCADE;

            CREATE VIEW %I.osm_hiking_points AS
            SELECT
                ogc_fid,
                osm_id,
                name,
                tourism,
                amenity,
                shelter_type,
                operator,
                ref,
                geom
            FROM public.points
            WHERE (
                tourism IN (''alpine_hut'', ''wilderness_hut'', ''hut'', ''camp_site'')
                OR amenity IN (''shelter'', ''hunting_stand'')
                OR shelter_type IS NOT NULL
                OR (name IS NOT NULL AND (
                    name ILIKE ''%%hytte%%''
                    OR name ILIKE ''%%shelter%%''
                    OR name ILIKE ''%%DNT%%''
                ))
            )
            AND geom IS NOT NULL;
        ', view_schema, view_schema);

        RAISE NOTICE '  ✓ Created view: osm_hiking_points';
    ELSE
        RAISE NOTICE '  ⊙ OSM points table not found, skipping osm_hiking_points view';
    END IF;

    -- Create table for tracking OSM path matches with Turrutebasen routes
    -- This allows the backend to identify which OSM paths correspond to which Turrutebasen routes
    RAISE NOTICE 'Creating table: osm_path_matches';
    EXECUTE format('
        DROP TABLE IF EXISTS %I.osm_path_matches CASCADE;

        CREATE TABLE %I.osm_path_matches (
            id SERIAL PRIMARY KEY,
            osm_path_id BIGINT NOT NULL,  -- OSM way ID (osm_id from lines table)
            fotrute_objid BIGINT,  -- Turrutebasen fotrute.objid (nullable - may not match)
            match_confidence NUMERIC(5,2) DEFAULT 0.0,  -- 0.0-1.0 confidence score
            match_type TEXT,  -- ''exact'', ''nearby'', ''manual'', ''rejected''
            distance_m NUMERIC(10,2),  -- Distance between OSM path and Turrutebasen route
            similarity_score NUMERIC(5,2),  -- Geometric similarity (0.0-1.0)
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_by TEXT,  -- User/system that created the match
            notes TEXT,  -- Manual notes about the match
            UNIQUE(osm_path_id, fotrute_objid)
        );

        CREATE INDEX idx_osm_path_matches_osm_path_id ON %I.osm_path_matches(osm_path_id);
        CREATE INDEX idx_osm_path_matches_fotrute_objid ON %I.osm_path_matches(fotrute_objid);
        CREATE INDEX idx_osm_path_matches_match_type ON %I.osm_path_matches(match_type);
        CREATE INDEX idx_osm_path_matches_match_confidence ON %I.osm_path_matches(match_confidence DESC);

        ALTER TABLE %I.osm_path_matches OWNER TO stiflyt_owner;
        GRANT SELECT ON %I.osm_path_matches TO stiflyt_reader;
        GRANT SELECT, INSERT, UPDATE, DELETE ON %I.osm_path_matches TO stiflyt_updater;
    ', view_schema, view_schema, view_schema, view_schema, view_schema, view_schema);

    RAISE NOTICE '  ✓ Created table: osm_path_matches';

    -- Create table for changeset tracking
    -- Tracks differences between OSM and Turrutebasen that should be reported to Kartverket
    RAISE NOTICE 'Creating table: osm_changesets';
    EXECUTE format('
        DROP TABLE IF EXISTS %I.osm_changesets CASCADE;

        CREATE TABLE %I.osm_changesets (
            id SERIAL PRIMARY KEY,
            changeset_type TEXT NOT NULL,  -- ''add'', ''modify'', ''delete'', ''geometry'', ''metadata''
            osm_path_id BIGINT,  -- OSM way ID
            fotrute_objid BIGINT,  -- Turrutebasen route ID
            field_name TEXT,  -- Field that differs (e.g., ''name'', ''geometry'', ''operator'')
            osm_value TEXT,  -- Value in OSM
            turrutebasen_value TEXT,  -- Value in Turrutebasen
            geometry_diff GEOMETRY(LINESTRING, 25833),  -- Geometry difference (if applicable)
            priority INTEGER DEFAULT 5,  -- 1-10 priority (1=highest, 10=lowest)
            status TEXT DEFAULT ''pending'',  -- ''pending'', ''reviewed'', ''submitted'', ''rejected''
            reviewed_by TEXT,
            reviewed_at TIMESTAMP,
            submitted_to_kartverket_at TIMESTAMP,
            kartverket_reference TEXT,  -- Reference number from Kartverket
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_by TEXT,
            notes TEXT
        );

        CREATE INDEX idx_osm_changesets_status ON %I.osm_changesets(status);
        CREATE INDEX idx_osm_changesets_priority ON %I.osm_changesets(priority);
        CREATE INDEX idx_osm_changesets_osm_path_id ON %I.osm_changesets(osm_path_id);
        CREATE INDEX idx_osm_changesets_fotrute_objid ON %I.osm_changesets(fotrute_objid);
        CREATE INDEX idx_osm_changesets_changeset_type ON %I.osm_changesets(changeset_type);
        CREATE INDEX idx_osm_changesets_created_at ON %I.osm_changesets(created_at DESC);

        ALTER TABLE %I.osm_changesets OWNER TO stiflyt_owner;
        GRANT SELECT ON %I.osm_changesets TO stiflyt_reader;
        GRANT SELECT, INSERT, UPDATE, DELETE ON %I.osm_changesets TO stiflyt_updater;
    ', view_schema, view_schema, view_schema, view_schema, view_schema, view_schema);

    RAISE NOTICE '  ✓ Created table: osm_changesets';

    -- Create view that joins OSM paths with matches and Turrutebasen routes
    -- This makes it easy for the backend to query OSM paths with their corresponding Turrutebasen routes
    RAISE NOTICE 'Creating view: osm_paths_with_matches';
    EXECUTE format('
        DROP VIEW IF EXISTS %I.osm_paths_with_matches CASCADE;

        CREATE VIEW %I.osm_paths_with_matches AS
        SELECT
            o.ogc_fid,
            o.osm_id,
            o.name AS osm_name,
            o.highway,
            o.sac_scale,
            o.trail_visibility,
            o.surface,
            o.route,
            o.operator AS osm_operator,
            o.ref AS osm_ref,
            o.geom AS osm_geom,
            m.id AS match_id,
            m.fotrute_objid,
            m.match_confidence,
            m.match_type,
            m.distance_m,
            m.similarity_score,
            m.notes AS match_notes,
            -- Join with Turrutebasen route info if match exists
            fi.rutenavn,
            fi.rutenummer,
            fi.vedlikeholdsansvarlig
        FROM %I.osm_hiking_paths o
        LEFT JOIN %I.osm_path_matches m ON o.osm_id = m.osm_path_id
        LEFT JOIN stiflyt.fotruteinfo fi ON m.fotrute_objid = fi.fotrute_fk
        WHERE o.is_hiking_path = true;
    ', view_schema, view_schema, view_schema, view_schema);

    RAISE NOTICE '  ✓ Created view: osm_paths_with_matches';

    -- Create function to update updated_at timestamp
    RAISE NOTICE 'Creating trigger function for updated_at';
    EXECUTE format('
        CREATE OR REPLACE FUNCTION %I.update_updated_at_column()
        RETURNS TRIGGER AS $func$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $func$ LANGUAGE plpgsql;
    ', view_schema);

    -- Add triggers for updated_at
    EXECUTE format('
        DROP TRIGGER IF EXISTS osm_path_matches_updated_at ON %I.osm_path_matches;
        CREATE TRIGGER osm_path_matches_updated_at
            BEFORE UPDATE ON %I.osm_path_matches
            FOR EACH ROW
            EXECUTE FUNCTION %I.update_updated_at_column();

        DROP TRIGGER IF EXISTS osm_changesets_updated_at ON %I.osm_changesets;
        CREATE TRIGGER osm_changesets_updated_at
            BEFORE UPDATE ON %I.osm_changesets
            FOR EACH ROW
            EXECUTE FUNCTION %I.update_updated_at_column();
    ', view_schema, view_schema, view_schema, view_schema, view_schema);

    RAISE NOTICE '  ✓ Created triggers for updated_at';

    -- Grant SELECT on views
    EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO stiflyt_reader', view_schema);
    EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE stiflyt_owner IN SCHEMA %I GRANT SELECT ON TABLES TO stiflyt_reader', view_schema);

    RAISE NOTICE '';
    RAISE NOTICE '=== OSM views and structures created successfully ===';
    RAISE NOTICE 'Backend can now access:';
    RAISE NOTICE '  - stiflyt.osm_hiking_paths - All OSM hiking paths';
    RAISE NOTICE '  - stiflyt.osm_hiking_points - OSM huts/shelters (if points table exists)';
    RAISE NOTICE '  - stiflyt.osm_paths_with_matches - OSM paths with Turrutebasen matches';
    RAISE NOTICE '  - stiflyt.osm_path_matches - Match tracking table';
    RAISE NOTICE '  - stiflyt.osm_changesets - Changeset tracking for Kartverket reporting';

EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Error creating OSM views: %', SQLERRM;
    RAISE NOTICE 'This is non-fatal - views will be created on next migration run';
END $$;

