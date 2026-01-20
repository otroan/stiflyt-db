-- Migration: Fix grant_schema_privileges_for_prefix search path
-- Skips safely if current user is not function owner.

DO $$
DECLARE
    owner_name TEXT;
BEGIN
    SELECT pg_get_userbyid(p.proowner)
    INTO owner_name
    FROM pg_proc p
    JOIN pg_namespace n ON p.pronamespace = n.oid
    WHERE n.nspname = 'public'
      AND p.proname = 'grant_schema_privileges_for_prefix';

    IF owner_name IS NULL THEN
        BEGIN
            EXECUTE $sql$
                CREATE FUNCTION public.grant_schema_privileges_for_prefix(schema_prefix TEXT)
                RETURNS void
                LANGUAGE plpgsql
                SECURITY DEFINER
                SET search_path = pg_catalog, public
                AS $func$
                DECLARE
                    schema_rec RECORD;
                BEGIN
                    FOR schema_rec IN
                        SELECT nspname
                        FROM pg_namespace
                        WHERE nspname LIKE schema_prefix || '_%'
                          AND nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
                          AND nspname NOT LIKE 'pg_temp_%'
                          AND nspname NOT LIKE 'pg_toast_temp_%'
                    LOOP
                        PERFORM public.grant_schema_privileges(schema_rec.nspname);
                    END LOOP;
                END;
                $func$;
            $sql$;
        EXCEPTION WHEN insufficient_privilege THEN
            RAISE NOTICE 'Skipping create of grant_schema_privileges_for_prefix (insufficient privilege)';
            RETURN;
        END;
    ELSIF owner_name <> current_user THEN
        RAISE NOTICE 'Skipping fix for grant_schema_privileges_for_prefix (owner: %)', owner_name;
        RETURN;
    ELSE
        BEGIN
            EXECUTE $sql$
                CREATE OR REPLACE FUNCTION public.grant_schema_privileges_for_prefix(schema_prefix TEXT)
                RETURNS void
                LANGUAGE plpgsql
                SECURITY DEFINER
                SET search_path = pg_catalog, public
                AS $func$
                DECLARE
                    schema_rec RECORD;
                BEGIN
                    FOR schema_rec IN
                        SELECT nspname
                        FROM pg_namespace
                        WHERE nspname LIKE schema_prefix || '_%'
                          AND nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
                          AND nspname NOT LIKE 'pg_temp_%'
                          AND nspname NOT LIKE 'pg_toast_temp_%'
                    LOOP
                        PERFORM public.grant_schema_privileges(schema_rec.nspname);
                    END LOOP;
                END;
                $func$;
            $sql$;
        EXCEPTION WHEN insufficient_privilege THEN
            RAISE NOTICE 'Skipping replace of grant_schema_privileges_for_prefix (insufficient privilege)';
            RETURN;
        END;
    END IF;
END $$;

DO $$
BEGIN
    GRANT EXECUTE ON FUNCTION public.grant_schema_privileges_for_prefix(TEXT) TO stiflyt_updater;
    GRANT EXECUTE ON FUNCTION public.grant_schema_privileges_for_prefix(TEXT) TO PUBLIC;
EXCEPTION WHEN insufficient_privilege THEN
    RAISE NOTICE 'Skipping GRANT on grant_schema_privileges_for_prefix (not owner)';
END $$;
