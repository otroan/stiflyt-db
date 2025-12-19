-- Migration: Setup database roles and permissions
-- Creates roles for cron updates and backend read-only access
-- Created: 2024
--
-- This migration sets up:
-- 1. stiflyt_updater role - Full write access for cron updates (update-datasets, migrations)
-- 2. stiflyt_reader role - Read-only access for backend application
--
-- Usage:
--   Run this migration once after creating the database:
--   psql -d matrikkel -f migrations/000_setup_roles.sql
--
--   Or as superuser:
--   sudo -u postgres psql -d matrikkel -f migrations/000_setup_roles.sql

-- Create roles if they don't exist
DO $$
BEGIN
    -- Role for cron updates (needs write access)
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stiflyt_updater') THEN
        CREATE ROLE stiflyt_updater WITH LOGIN;
        RAISE NOTICE 'Created role: stiflyt_updater';
    ELSE
        RAISE NOTICE 'Role stiflyt_updater already exists';
    END IF;

    -- Role for backend application (read-only)
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stiflyt_reader') THEN
        CREATE ROLE stiflyt_reader WITH LOGIN;
        RAISE NOTICE 'Created role: stiflyt_reader';
    ELSE
        RAISE NOTICE 'Role stiflyt_reader already exists';
    END IF;
END $$;

-- Grant database connection privilege and CREATE privilege for schema creation
DO $$
DECLARE
    db_name TEXT;
BEGIN
    SELECT current_database() INTO db_name;
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO stiflyt_updater', db_name);
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO stiflyt_reader', db_name);
    -- Grant CREATE privilege so stiflyt_updater can create schemas (needed for migration 005)
    EXECUTE format('GRANT CREATE ON DATABASE %I TO stiflyt_updater', db_name);
END $$;

-- Grant usage on public schema (for datasets that use public schema)
GRANT USAGE ON SCHEMA public TO stiflyt_updater;
GRANT USAGE ON SCHEMA public TO stiflyt_reader;

-- Grant create privilege on public schema for updater (needed to create schemas)
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO stiflyt_updater;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO stiflyt_reader;

-- Grant all privileges on public schema to updater (for CREATE/DROP operations)
GRANT ALL PRIVILEGES ON SCHEMA public TO stiflyt_updater;
GRANT CREATE ON SCHEMA public TO stiflyt_updater;

-- Grant read-only on public schema to reader
GRANT SELECT ON ALL TABLES IN SCHEMA public TO stiflyt_reader;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO stiflyt_reader;

-- For future tables/sequences in public schema
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO stiflyt_updater;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO stiflyt_updater;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO stiflyt_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON SEQUENCES TO stiflyt_reader;

-- Grant privileges on all existing schemas (including dynamic ones like turogfriluftsruter_*)
-- This handles schemas that already exist
DO $$
DECLARE
    schema_rec RECORD;
    obj_rec RECORD;
BEGIN
    FOR schema_rec IN
        SELECT nspname
        FROM pg_namespace
        WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
          AND nspname NOT LIKE 'pg_temp_%'
          AND nspname NOT LIKE 'pg_toast_temp_%'
    LOOP
        -- Grant schema usage
        EXECUTE format('GRANT USAGE ON SCHEMA %I TO stiflyt_updater', schema_rec.nspname);
        EXECUTE format('GRANT USAGE ON SCHEMA %I TO stiflyt_reader', schema_rec.nspname);

        -- Set OWNER on existing tables to stiflyt_updater (so it can DROP/CREATE them)
        -- Only if we have permission (skip if not owner/superuser)
        FOR obj_rec IN
            SELECT tablename FROM pg_tables WHERE schemaname = schema_rec.nspname
        LOOP
            BEGIN
                EXECUTE format('ALTER TABLE %I.%I OWNER TO stiflyt_updater', schema_rec.nspname, obj_rec.tablename);
            EXCEPTION WHEN insufficient_privilege THEN
                -- Not owner - skip (will be handled by grant_schema_privileges function)
                RAISE NOTICE 'Skipping OWNER change for table %.% (not owner)', schema_rec.nspname, obj_rec.tablename;
            END;
        END LOOP;

        -- Set OWNER on existing sequences to stiflyt_updater
        FOR obj_rec IN
            SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = schema_rec.nspname
        LOOP
            BEGIN
                EXECUTE format('ALTER SEQUENCE %I.%I OWNER TO stiflyt_updater', schema_rec.nspname, obj_rec.sequence_name);
            EXCEPTION WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping OWNER change for sequence %.% (not owner)', schema_rec.nspname, obj_rec.sequence_name;
            END;
        END LOOP;

        -- Set OWNER on existing views to stiflyt_updater
        FOR obj_rec IN
            SELECT viewname FROM pg_views WHERE schemaname = schema_rec.nspname
        LOOP
            BEGIN
                EXECUTE format('ALTER VIEW %I.%I OWNER TO stiflyt_updater', schema_rec.nspname, obj_rec.viewname);
            EXCEPTION WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping OWNER change for view %.% (not owner)', schema_rec.nspname, obj_rec.viewname;
            END;
        END LOOP;

        -- Grant all privileges to updater
        EXECUTE format('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA %I TO stiflyt_updater', schema_rec.nspname);
        EXECUTE format('GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %I TO stiflyt_updater', schema_rec.nspname);
        EXECUTE format('GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA %I TO stiflyt_updater', schema_rec.nspname);

        -- Grant create privilege for updater (needed to create tables/views/indexes)
        EXECUTE format('GRANT CREATE ON SCHEMA %I TO stiflyt_updater', schema_rec.nspname);

        -- Grant read-only to reader
        EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO stiflyt_reader', schema_rec.nspname);
        EXECUTE format('GRANT SELECT ON ALL SEQUENCES IN SCHEMA %I TO stiflyt_reader', schema_rec.nspname);

        -- Set default privileges for future objects
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT ALL ON TABLES TO stiflyt_updater', schema_rec.nspname);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT ALL ON SEQUENCES TO stiflyt_updater', schema_rec.nspname);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT ALL ON FUNCTIONS TO stiflyt_updater', schema_rec.nspname);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO stiflyt_reader', schema_rec.nspname);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON SEQUENCES TO stiflyt_reader', schema_rec.nspname);

        RAISE NOTICE 'Granted privileges on schema: %', schema_rec.nspname;
    END LOOP;
END $$;

-- Create a function to grant privileges on new schemas (for future use)
-- This can be called manually when new schemas are created
-- Also sets OWNER on existing objects so stiflyt_updater can DROP/CREATE them
-- Only create if it doesn't exist (idempotent - skip if already exists with different owner)
DO $$
BEGIN
    -- Check if function already exists
    IF NOT EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'public' AND p.proname = 'grant_schema_privileges'
    ) THEN
        -- Function doesn't exist, try to create it
        BEGIN
            CREATE FUNCTION grant_schema_privileges(schema_name TEXT)
            RETURNS void
            LANGUAGE plpgsql
            SECURITY DEFINER
            AS $func$
            DECLARE
                obj_rec RECORD;
            BEGIN
                -- Grant schema usage
                EXECUTE format('GRANT USAGE ON SCHEMA %I TO stiflyt_updater', schema_name);
                EXECUTE format('GRANT USAGE ON SCHEMA %I TO stiflyt_reader', schema_name);

                -- Set OWNER on all existing tables to stiflyt_updater (so it can DROP/CREATE them)
                FOR obj_rec IN
                    SELECT tablename
                    FROM pg_tables
                    WHERE schemaname = schema_name
                LOOP
                    BEGIN
                        EXECUTE format('ALTER TABLE %I.%I OWNER TO stiflyt_updater', schema_name, obj_rec.tablename);
                    EXCEPTION WHEN insufficient_privilege THEN
                        RAISE NOTICE 'Skipping OWNER change for table %.% (not owner)', schema_name, obj_rec.tablename;
                    END;
                END LOOP;

                -- Set OWNER on all existing sequences to stiflyt_updater
                FOR obj_rec IN
                    SELECT sequence_name
                    FROM information_schema.sequences
                    WHERE sequence_schema = schema_name
                LOOP
                    BEGIN
                        EXECUTE format('ALTER SEQUENCE %I.%I OWNER TO stiflyt_updater', schema_name, obj_rec.sequence_name);
                    EXCEPTION WHEN insufficient_privilege THEN
                        RAISE NOTICE 'Skipping OWNER change for sequence %.% (not owner)', schema_name, obj_rec.sequence_name;
                    END;
                END LOOP;

                -- Set OWNER on all existing views to stiflyt_updater
                FOR obj_rec IN
                    SELECT viewname
                    FROM pg_views
                    WHERE schemaname = schema_name
                LOOP
                    BEGIN
                        EXECUTE format('ALTER VIEW %I.%I OWNER TO stiflyt_updater', schema_name, obj_rec.viewname);
                    EXCEPTION WHEN insufficient_privilege THEN
                        RAISE NOTICE 'Skipping OWNER change for view %.% (not owner)', schema_name, obj_rec.viewname;
                    END;
                END LOOP;

                -- Grant all privileges to updater
                EXECUTE format('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA %I TO stiflyt_updater', schema_name);
                EXECUTE format('GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %I TO stiflyt_updater', schema_name);
                EXECUTE format('GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA %I TO stiflyt_updater', schema_name);

                -- Grant create privilege for updater
                EXECUTE format('GRANT CREATE ON SCHEMA %I TO stiflyt_updater', schema_name);

                -- Grant read-only to reader
                EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO stiflyt_reader', schema_name);
                EXECUTE format('GRANT SELECT ON ALL SEQUENCES IN SCHEMA %I TO stiflyt_reader', schema_name);

                -- Set default privileges for future objects
                EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT ALL ON TABLES TO stiflyt_updater', schema_name);
                EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT ALL ON SEQUENCES TO stiflyt_updater', schema_name);
                EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT ALL ON FUNCTIONS TO stiflyt_updater', schema_name);
                EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO stiflyt_reader', schema_name);
                EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON SEQUENCES TO stiflyt_reader', schema_name);
            END;
            $func$;
        EXCEPTION WHEN insufficient_privilege OR OTHERS THEN
            -- If creation fails (e.g., function exists with different owner), that's OK
            RAISE NOTICE 'Could not create grant_schema_privileges function (may already exist): %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'Function grant_schema_privileges already exists, skipping creation';
    END IF;
END $$;

-- Grant execute on the helper function to updater (so it can grant privileges on schemas it creates)
-- Function is SECURITY DEFINER so it runs with creator's privileges (postgres)
DO $$
BEGIN
    GRANT EXECUTE ON FUNCTION grant_schema_privileges(TEXT) TO stiflyt_updater;
EXCEPTION WHEN insufficient_privilege THEN
    -- If we're not the owner, skip (function will be created by superuser anyway)
    RAISE NOTICE 'Skipping GRANT on grant_schema_privileges function (not owner)';
END $$;

-- Final summary
DO $$
BEGIN
    RAISE NOTICE '=== Role setup complete ===';
    RAISE NOTICE 'Roles created:';
    RAISE NOTICE '  - stiflyt_updater: Full write access (for cron updates)';
    RAISE NOTICE '  - stiflyt_reader: Read-only access (for backend application)';
    RAISE NOTICE '';
    RAISE NOTICE 'Next steps:';
    RAISE NOTICE '  1. Set passwords for the roles:';
    RAISE NOTICE '     ALTER ROLE stiflyt_updater WITH PASSWORD ''your_password'';';
    RAISE NOTICE '     ALTER ROLE stiflyt_reader WITH PASSWORD ''your_password'';';
    RAISE NOTICE '  2. For cron, use stiflyt_updater user:';
    RAISE NOTICE '     PGUSER=stiflyt_updater PGPASSWORD=... make update-datasets';
    RAISE NOTICE '  3. For backend, use stiflyt_reader user:';
    RAISE NOTICE '     PGUSER=stiflyt_reader PGPASSWORD=... (in your backend config)';
    RAISE NOTICE '';
    RAISE NOTICE 'Note: If you create new schemas manually, run:';
    RAISE NOTICE '     SELECT grant_schema_privileges(''schema_name'');';
END $$;

