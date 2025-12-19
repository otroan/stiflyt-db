-- Migration: Create stable views in fixed schema for backend access
-- Creates views in 'stiflyt' schema that point to current dynamic schema
-- This allows backend to use fixed names like stiflyt.fotrute instead of turogfriluftsruter_abc123.fotrute
-- Created: 2024
--
-- This migration:
-- 1. Creates 'stiflyt' schema if it doesn't exist
-- 2. Creates views in 'stiflyt' schema that point to current dynamic schema
-- 3. Updates views after each dataset update (idempotent - safe to run multiple times)

DO $$
DECLARE
    dynamic_schema TEXT;
    view_schema TEXT := 'stiflyt';
    tbl_name TEXT;  -- Changed from table_name to avoid ambiguity with information_schema.tables.table_name
    view_exists BOOLEAN;
BEGIN
    -- Find the current dynamic schema with prefix 'turogfriluftsruter_'
    SELECT nspname INTO dynamic_schema
    FROM pg_namespace
    WHERE nspname LIKE 'turogfriluftsruter_%'
      AND nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
    ORDER BY nspname DESC  -- Get the most recent one
    LIMIT 1;

    IF dynamic_schema IS NULL THEN
        RAISE WARNING 'Schema with prefix turogfriluftsruter_* not found. Skipping stable view creation.';
        RETURN;
    END IF;

    RAISE NOTICE 'Creating stable views in schema "%" pointing to dynamic schema "%"', view_schema, dynamic_schema;

    -- Create stiflyt schema if it doesn't exist
    -- Use IF NOT EXISTS to make it idempotent
    BEGIN
        EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', view_schema);
        RAISE NOTICE 'Schema % exists or was created', view_schema;
    EXCEPTION WHEN insufficient_privilege THEN
        RAISE WARNING 'Insufficient privileges to create schema %. Grant CREATE ON DATABASE to stiflyt_updater.', view_schema;
        RAISE;
    END;

    -- Grant privileges on stiflyt schema
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO stiflyt_updater', view_schema);
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO stiflyt_reader', view_schema);
    EXECUTE format('GRANT CREATE ON SCHEMA %I TO stiflyt_updater', view_schema);
    EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO stiflyt_reader', view_schema);
    EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO stiflyt_reader', view_schema);

    -- List of tables to create stable views for
    -- These are the main tables the backend needs to access
    FOR tbl_name IN
        SELECT unnest(ARRAY[
            'fotrute',
            'fotruteinfo',
            'links',
            'link_segments',
            'nodes',
            'anchor_nodes'
        ])
    LOOP
        -- Check if table exists in dynamic schema
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables t
            WHERE t.table_schema = dynamic_schema
              AND t.table_name = tbl_name
              AND t.table_type = 'BASE TABLE'  -- Only base tables, not views
        ) INTO view_exists;

        IF view_exists THEN
            -- Drop existing view if it exists (for idempotency)
            EXECUTE format('DROP VIEW IF EXISTS %I.%I CASCADE', view_schema, tbl_name);

            -- Create view pointing to current dynamic schema
            EXECUTE format('CREATE VIEW %I.%I AS SELECT * FROM %I.%I',
                view_schema, tbl_name, dynamic_schema, tbl_name);

            RAISE NOTICE 'Created stable view: %.% -> %.%', view_schema, tbl_name, dynamic_schema, tbl_name;
        ELSE
            RAISE NOTICE 'Table %.% does not exist, skipping', dynamic_schema, tbl_name;
        END IF;
    END LOOP;

    -- Create stable views for views in dynamic schema
    -- These views are created by migrations and may not exist initially
    FOR tbl_name IN
        SELECT unnest(ARRAY[
            'link_ruteinfo',
            'links_with_routes'
        ])
    LOOP
        -- Check if view exists in dynamic schema
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.views v
            WHERE v.table_schema = dynamic_schema
              AND v.table_name = tbl_name
        ) INTO view_exists;

        IF view_exists THEN
            -- Drop existing view if it exists (for idempotency)
            EXECUTE format('DROP VIEW IF EXISTS %I.%I CASCADE', view_schema, tbl_name);

            -- Create view pointing to current dynamic schema view
            EXECUTE format('CREATE VIEW %I.%I AS SELECT * FROM %I.%I',
                view_schema, tbl_name, dynamic_schema, tbl_name);

            RAISE NOTICE 'Created stable view: %.% -> %.%', view_schema, tbl_name, dynamic_schema, tbl_name;
        ELSE
            RAISE NOTICE 'View %.% does not exist yet (may be created by later migrations), skipping', dynamic_schema, tbl_name;
        END IF;
    END LOOP;

    RAISE NOTICE '=== Stable views created successfully ===';
    RAISE NOTICE 'Backend can now use fixed schema name: stiflyt';
    RAISE NOTICE 'Example: SELECT * FROM stiflyt.fotrute';
    RAISE NOTICE 'Example: SELECT * FROM stiflyt.links';

EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Error creating stable views: %', SQLERRM;
    RAISE NOTICE 'This is non-fatal - views will be created on next migration run';
END $$;

