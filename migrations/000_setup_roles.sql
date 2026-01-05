-- Migration: Setup database roles and permissions
-- Creates roles for cron updates and backend read-only access
-- Created: 2024
--
-- This migration sets up:
-- 1. stiflyt_owner  role - Owner of all objects (NOLOGIN)
-- 2. stiflyt_updater role - Full write access for cron updates (update-datasets, migrations)
-- 3. stiflyt_reader role - Read-only access for backend application
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
    -- Owner role for all objects (no login)
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stiflyt_owner') THEN
        CREATE ROLE stiflyt_owner NOLOGIN;
        RAISE NOTICE 'Created role: stiflyt_owner';
    ELSE
        RAISE NOTICE 'Role stiflyt_owner already exists';
    END IF;

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

-- Grant ownership role to updater and current user (dev)
DO $$
BEGIN
    BEGIN
        EXECUTE format('GRANT stiflyt_owner TO %I', 'stiflyt_updater');
    EXCEPTION WHEN insufficient_privilege OR OTHERS THEN
        RAISE NOTICE 'Could not grant stiflyt_owner to stiflyt_updater: %', SQLERRM;
    END;
    BEGIN
        EXECUTE format('GRANT stiflyt_owner TO %I', current_user);
    EXCEPTION WHEN insufficient_privilege OR OTHERS THEN
        RAISE NOTICE 'Could not grant stiflyt_owner to %: %', current_user, SQLERRM;
    END;
END $$;

-- Ensure updater defaults to owner role for consistent object ownership
DO $$
BEGIN
    BEGIN
        EXECUTE 'ALTER ROLE stiflyt_updater SET ROLE stiflyt_owner';
    EXCEPTION WHEN insufficient_privilege OR OTHERS THEN
        RAISE NOTICE 'Could not set default role for stiflyt_updater: %', SQLERRM;
    END;
END $$;

-- Revoke default public access
DO $$
DECLARE
    db_name TEXT;
BEGIN
    SELECT current_database() INTO db_name;
    EXECUTE format('REVOKE ALL ON DATABASE %I FROM PUBLIC', db_name);
END $$;
REVOKE ALL ON SCHEMA public FROM PUBLIC;

-- Grant database connection privilege and CREATE privilege for schema creation
DO $$
DECLARE
    db_name TEXT;
BEGIN
    SELECT current_database() INTO db_name;
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO stiflyt_owner', db_name);
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO stiflyt_updater', db_name);
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO stiflyt_reader', db_name);
    -- Grant CREATE privilege so stiflyt_owner can create schemas (used by migrations)
    EXECUTE format('GRANT CREATE ON DATABASE %I TO stiflyt_owner', db_name);
    -- Keep CREATE for updater for backward compatibility
    EXECUTE format('GRANT CREATE ON DATABASE %I TO stiflyt_updater', db_name);
END $$;

-- Grant usage on public schema (for datasets that use public schema)
GRANT USAGE ON SCHEMA public TO stiflyt_owner;
GRANT USAGE ON SCHEMA public TO stiflyt_updater;
GRANT USAGE ON SCHEMA public TO stiflyt_reader;

-- Ensure default privileges are set for the owner role
DO $$
DECLARE
BEGIN
    EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE stiflyt_owner IN SCHEMA public GRANT ALL ON TABLES TO stiflyt_updater';
    EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE stiflyt_owner IN SCHEMA public GRANT ALL ON SEQUENCES TO stiflyt_updater';
    EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE stiflyt_owner IN SCHEMA public GRANT ALL ON FUNCTIONS TO stiflyt_updater';
    EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE stiflyt_owner IN SCHEMA public GRANT SELECT ON TABLES TO stiflyt_reader';
    EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE stiflyt_owner IN SCHEMA public GRANT SELECT ON SEQUENCES TO stiflyt_reader';
    EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE stiflyt_updater IN SCHEMA public GRANT SELECT ON TABLES TO stiflyt_reader';
    EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE stiflyt_updater IN SCHEMA public GRANT SELECT ON SEQUENCES TO stiflyt_reader';
END $$;

-- Grant all privileges on public schema to owner (for CREATE/DROP operations)
GRANT ALL PRIVILEGES ON SCHEMA public TO stiflyt_owner;
GRANT CREATE ON SCHEMA public TO stiflyt_owner;
GRANT USAGE, CREATE ON SCHEMA public TO stiflyt_updater;

-- Grant read-only on public schema to reader
-- First set ownership to stiflyt_owner, then grant privileges
DO $$
DECLARE
    table_rec RECORD;
    seq_rec RECORD;
BEGIN
    -- First, set ownership of all tables to stiflyt_owner (requires superuser or current owner)
    FOR table_rec IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
    LOOP
        BEGIN
            EXECUTE format('ALTER TABLE %I.%I OWNER TO stiflyt_owner', 'public', table_rec.tablename);
        EXCEPTION WHEN insufficient_privilege OR OTHERS THEN
            RAISE NOTICE 'Could not set owner for public.% (may require superuser): %', table_rec.tablename, SQLERRM;
        END;
    END LOOP;

    -- Set ownership of all sequences to stiflyt_owner
    FOR seq_rec IN
        SELECT sequence_name
        FROM information_schema.sequences
        WHERE sequence_schema = 'public'
    LOOP
        BEGIN
            EXECUTE format('ALTER SEQUENCE %I.%I OWNER TO stiflyt_owner', 'public', seq_rec.sequence_name);
        EXCEPTION WHEN insufficient_privilege OR OTHERS THEN
            RAISE NOTICE 'Could not set owner for public.% (may require superuser): %', seq_rec.sequence_name, SQLERRM;
        END;
    END LOOP;

    -- Now grant SELECT on tables (should work since we own them or have privileges)
    FOR table_rec IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
    LOOP
        BEGIN
            EXECUTE format('GRANT SELECT ON TABLE %I.%I TO stiflyt_reader', 'public', table_rec.tablename);
        EXCEPTION WHEN insufficient_privilege OR OTHERS THEN
            RAISE NOTICE 'Could not grant SELECT on public.%: %', table_rec.tablename, SQLERRM;
        END;
    END LOOP;

    -- Grant SELECT on sequences
    FOR seq_rec IN
        SELECT sequence_name
        FROM information_schema.sequences
        WHERE sequence_schema = 'public'
    LOOP
        BEGIN
            EXECUTE format('GRANT SELECT ON SEQUENCE %I.%I TO stiflyt_reader', 'public', seq_rec.sequence_name);
        EXCEPTION WHEN insufficient_privilege OR OTHERS THEN
            RAISE NOTICE 'Could not grant SELECT on public.%: %', seq_rec.sequence_name, SQLERRM;
        END;
    END LOOP;
END $$;

-- Default privileges for public schema are handled above for both roles.

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
        -- Grant schema usage (skip if we don't have permission)
        BEGIN
            EXECUTE format('GRANT USAGE ON SCHEMA %I TO stiflyt_updater', schema_rec.nspname);
        EXCEPTION WHEN insufficient_privilege OR OTHERS THEN
            RAISE NOTICE 'Could not grant USAGE on schema % to stiflyt_updater: %', schema_rec.nspname, SQLERRM;
        END;
        BEGIN
            EXECUTE format('GRANT USAGE ON SCHEMA %I TO stiflyt_reader', schema_rec.nspname);
        EXCEPTION WHEN insufficient_privilege OR OTHERS THEN
            RAISE NOTICE 'Could not grant USAGE on schema % to stiflyt_reader: %', schema_rec.nspname, SQLERRM;
        END;
        BEGIN
            EXECUTE format('ALTER SCHEMA %I OWNER TO stiflyt_owner', schema_rec.nspname);
        EXCEPTION WHEN insufficient_privilege OR OTHERS THEN
            RAISE NOTICE 'Could not set owner for schema %: %', schema_rec.nspname, SQLERRM;
        END;

        -- Set OWNER on existing tables to stiflyt_owner (so it can DROP/CREATE them)
        -- Only if we have permission (skip if not owner/superuser)
        FOR obj_rec IN
            SELECT tablename FROM pg_tables WHERE schemaname = schema_rec.nspname
        LOOP
            BEGIN
                EXECUTE format('ALTER TABLE %I.%I OWNER TO stiflyt_owner', schema_rec.nspname, obj_rec.tablename);
            EXCEPTION WHEN insufficient_privilege THEN
                -- Not owner - skip (will be handled by grant_schema_privileges function)
                RAISE NOTICE 'Skipping OWNER change for table %.% (not owner)', schema_rec.nspname, obj_rec.tablename;
            END;
        END LOOP;

        -- Set OWNER on existing sequences to stiflyt_owner
        FOR obj_rec IN
            SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = schema_rec.nspname
        LOOP
            BEGIN
                EXECUTE format('ALTER SEQUENCE %I.%I OWNER TO stiflyt_owner', schema_rec.nspname, obj_rec.sequence_name);
            EXCEPTION WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping OWNER change for sequence %.% (not owner)', schema_rec.nspname, obj_rec.sequence_name;
            END;
        END LOOP;

        -- Set OWNER on existing views/materialized views to stiflyt_owner
        FOR obj_rec IN
            SELECT viewname FROM pg_views WHERE schemaname = schema_rec.nspname
        LOOP
            BEGIN
                EXECUTE format('ALTER VIEW %I.%I OWNER TO stiflyt_owner', schema_rec.nspname, obj_rec.viewname);
            EXCEPTION WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping OWNER change for view %.% (not owner)', schema_rec.nspname, obj_rec.viewname;
            END;
        END LOOP;
        FOR obj_rec IN
            SELECT matviewname FROM pg_matviews WHERE schemaname = schema_rec.nspname
        LOOP
            BEGIN
                EXECUTE format('ALTER MATERIALIZED VIEW %I.%I OWNER TO stiflyt_owner', schema_rec.nspname, obj_rec.matviewname);
            EXCEPTION WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping OWNER change for materialized view %.% (not owner)', schema_rec.nspname, obj_rec.matviewname;
            END;
        END LOOP;

        -- Grant all privileges to updater (skip if we don't have permission)
        BEGIN
            EXECUTE format('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA %I TO stiflyt_updater', schema_rec.nspname);
        EXCEPTION WHEN insufficient_privilege OR OTHERS THEN
            RAISE NOTICE 'Could not grant privileges on tables in schema %: %', schema_rec.nspname, SQLERRM;
        END;
        BEGIN
            EXECUTE format('GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %I TO stiflyt_updater', schema_rec.nspname);
        EXCEPTION WHEN insufficient_privilege OR OTHERS THEN
            RAISE NOTICE 'Could not grant privileges on sequences in schema %: %', schema_rec.nspname, SQLERRM;
        END;
        BEGIN
            EXECUTE format('GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA %I TO stiflyt_updater', schema_rec.nspname);
        EXCEPTION WHEN insufficient_privilege OR OTHERS THEN
            RAISE NOTICE 'Could not grant privileges on functions in schema %: %', schema_rec.nspname, SQLERRM;
        END;

        -- Grant create privilege for updater (needed to create tables/views/indexes)
        BEGIN
            EXECUTE format('GRANT CREATE ON SCHEMA %I TO stiflyt_updater', schema_rec.nspname);
        EXCEPTION WHEN insufficient_privilege OR OTHERS THEN
            RAISE NOTICE 'Could not grant CREATE on schema %: %', schema_rec.nspname, SQLERRM;
        END;

        -- Grant read-only to reader
        BEGIN
            EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO stiflyt_reader', schema_rec.nspname);
        EXCEPTION WHEN insufficient_privilege OR OTHERS THEN
            RAISE NOTICE 'Could not grant SELECT on tables in schema %: %', schema_rec.nspname, SQLERRM;
        END;
        BEGIN
            EXECUTE format('GRANT SELECT ON ALL SEQUENCES IN SCHEMA %I TO stiflyt_reader', schema_rec.nspname);
        EXCEPTION WHEN insufficient_privilege OR OTHERS THEN
            RAISE NOTICE 'Could not grant SELECT on sequences in schema %: %', schema_rec.nspname, SQLERRM;
        END;

        -- Set default privileges for future objects for owner role
        BEGIN
            EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE stiflyt_owner IN SCHEMA %I GRANT ALL ON TABLES TO stiflyt_updater', schema_rec.nspname);
            EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE stiflyt_owner IN SCHEMA %I GRANT ALL ON SEQUENCES TO stiflyt_updater', schema_rec.nspname);
            EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE stiflyt_owner IN SCHEMA %I GRANT ALL ON FUNCTIONS TO stiflyt_updater', schema_rec.nspname);
            EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE stiflyt_owner IN SCHEMA %I GRANT SELECT ON TABLES TO stiflyt_reader', schema_rec.nspname);
            EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE stiflyt_owner IN SCHEMA %I GRANT SELECT ON SEQUENCES TO stiflyt_reader', schema_rec.nspname);
            EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE stiflyt_updater IN SCHEMA %I GRANT SELECT ON TABLES TO stiflyt_reader', schema_rec.nspname);
            EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE stiflyt_updater IN SCHEMA %I GRANT SELECT ON SEQUENCES TO stiflyt_reader', schema_rec.nspname);
        EXCEPTION WHEN insufficient_privilege OR OTHERS THEN
            RAISE NOTICE 'Could not set default privileges in schema % for stiflyt_owner: %', schema_rec.nspname, SQLERRM;
        END;

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
            SET search_path = pg_catalog
            AS $func$
            DECLARE
                obj_rec RECORD;
            BEGIN
                -- Grant schema usage
                EXECUTE format('GRANT USAGE ON SCHEMA %I TO stiflyt_updater', schema_name);
                EXECUTE format('GRANT USAGE ON SCHEMA %I TO stiflyt_reader', schema_name);
                EXECUTE format('ALTER SCHEMA %I OWNER TO stiflyt_owner', schema_name);

                -- Set OWNER on all existing tables to stiflyt_owner (so it can DROP/CREATE them)
                FOR obj_rec IN
                    SELECT tablename
                    FROM pg_tables
                    WHERE schemaname = schema_name
                LOOP
                    BEGIN
                        EXECUTE format('ALTER TABLE %I.%I OWNER TO stiflyt_owner', schema_name, obj_rec.tablename);
                    EXCEPTION WHEN insufficient_privilege THEN
                        RAISE NOTICE 'Skipping OWNER change for table %.% (not owner)', schema_name, obj_rec.tablename;
                    END;
                END LOOP;

                -- Set OWNER on all existing sequences to stiflyt_owner
                FOR obj_rec IN
                    SELECT sequence_name
                    FROM information_schema.sequences
                    WHERE sequence_schema = schema_name
                LOOP
                    BEGIN
                        EXECUTE format('ALTER SEQUENCE %I.%I OWNER TO stiflyt_owner', schema_name, obj_rec.sequence_name);
                    EXCEPTION WHEN insufficient_privilege THEN
                        RAISE NOTICE 'Skipping OWNER change for sequence %.% (not owner)', schema_name, obj_rec.sequence_name;
                    END;
                END LOOP;

                -- Set OWNER on all existing views/materialized views to stiflyt_owner
                FOR obj_rec IN
                    SELECT viewname
                    FROM pg_views
                    WHERE schemaname = schema_name
                LOOP
                    BEGIN
                        EXECUTE format('ALTER VIEW %I.%I OWNER TO stiflyt_owner', schema_name, obj_rec.viewname);
                    EXCEPTION WHEN insufficient_privilege THEN
                        RAISE NOTICE 'Skipping OWNER change for view %.% (not owner)', schema_name, obj_rec.viewname;
                    END;
                END LOOP;
                FOR obj_rec IN
                    SELECT matviewname
                    FROM pg_matviews
                    WHERE schemaname = schema_name
                LOOP
                    BEGIN
                        EXECUTE format('ALTER MATERIALIZED VIEW %I.%I OWNER TO stiflyt_owner', schema_name, obj_rec.matviewname);
                    EXCEPTION WHEN insufficient_privilege THEN
                        RAISE NOTICE 'Skipping OWNER change for materialized view %.% (not owner)', schema_name, obj_rec.matviewname;
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

                -- Set default privileges for future objects for owner role
                EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE stiflyt_owner IN SCHEMA %I GRANT ALL ON TABLES TO stiflyt_updater', schema_name);
                EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE stiflyt_owner IN SCHEMA %I GRANT ALL ON SEQUENCES TO stiflyt_updater', schema_name);
                EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE stiflyt_owner IN SCHEMA %I GRANT ALL ON FUNCTIONS TO stiflyt_updater', schema_name);
                EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE stiflyt_owner IN SCHEMA %I GRANT SELECT ON TABLES TO stiflyt_reader', schema_name);
                EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE stiflyt_owner IN SCHEMA %I GRANT SELECT ON SEQUENCES TO stiflyt_reader', schema_name);
                EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE stiflyt_updater IN SCHEMA %I GRANT SELECT ON TABLES TO stiflyt_reader', schema_name);
                EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE stiflyt_updater IN SCHEMA %I GRANT SELECT ON SEQUENCES TO stiflyt_reader', schema_name);
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
    GRANT EXECUTE ON FUNCTION grant_schema_privileges(TEXT) TO PUBLIC;
EXCEPTION WHEN insufficient_privilege THEN
    -- If we're not the owner, skip (function will be created by superuser anyway)
    RAISE NOTICE 'Skipping GRANT on grant_schema_privileges function (not owner)';
END $$;

-- Final summary
DO $$
BEGIN
    RAISE NOTICE '=== Role setup complete ===';
    RAISE NOTICE 'Roles created:';
    RAISE NOTICE '  - stiflyt_owner: Object owner (NOLOGIN)';
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
