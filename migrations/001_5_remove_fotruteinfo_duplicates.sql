-- Migration: Remove duplicate rutenummer entries in fotruteinfo
-- Created: 2025-01-23
--
-- This migration removes duplicate entries in fotruteinfo where the same
-- segment (fotrute_fk) has the same rutenummer multiple times.
--
-- Example: If segment 12345 has two rows with rutenummer='bre16', we keep only one.
--
-- This should run AFTER 001_add_fotrute_indexes.sql (needs indexes for performance)
-- and BEFORE 002_build_topology.sql (clean data before topology calculation)

DO $$
DECLARE
    schema_name TEXT;
    duplicates_count BIGINT;
    deleted_count BIGINT;
    start_time TIMESTAMP;
    step_time TIMESTAMP;
BEGIN
    start_time := clock_timestamp();
    RAISE NOTICE '=== Starting fotruteinfo deduplication ===';
    RAISE NOTICE 'Start time: %', start_time;

    -- Find the schema with prefix 'turogfriluftsruter_'
    SELECT nspname INTO schema_name
    FROM pg_namespace
    WHERE nspname LIKE 'turogfriluftsruter_%'
      AND nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
    ORDER BY nspname DESC
    LIMIT 1;

    IF schema_name IS NULL THEN
        RAISE WARNING 'Schema with prefix turogfriluftsruter_* not found. Skipping deduplication.';
        RETURN;
    END IF;

    -- Verify fotruteinfo table exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = schema_name AND table_name = 'fotruteinfo'
    ) THEN
        RAISE WARNING 'Table %.fotruteinfo does not exist. Skipping deduplication.', schema_name;
        RETURN;
    END IF;

    RAISE NOTICE 'Deduplicating fotruteinfo in schema: %', schema_name;

    -- Count duplicates first
    step_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE 'Step 1: Counting duplicates...';
    EXECUTE format('
        SELECT COALESCE(SUM(cnt - 1), 0)
        FROM (
            SELECT COUNT(*) as cnt
            FROM %I.fotruteinfo
            WHERE rutenummer IS NOT NULL
            GROUP BY fotrute_fk, rutenummer
            HAVING COUNT(*) > 1
        ) AS dup_counts
    ', schema_name) INTO duplicates_count;

    IF duplicates_count IS NULL OR duplicates_count = 0 THEN
        RAISE NOTICE '  ✓ No duplicates found in fotruteinfo';
        RAISE NOTICE '  Time: %', clock_timestamp() - step_time;
        RAISE NOTICE '';
        RAISE NOTICE '=== Deduplication complete (no action needed) ===';
        RAISE NOTICE 'Total time: %', clock_timestamp() - start_time;
        RETURN;
    END IF;

    RAISE NOTICE '  Found % duplicate entries to remove', duplicates_count;
    RAISE NOTICE '  Time: %', clock_timestamp() - step_time;

    -- Remove duplicates
    -- Strategy: Keep the row with the lowest objid (or first row if no objid)
    -- This ensures deterministic results
    step_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE 'Step 2: Removing duplicates...';
    RAISE NOTICE '  Keeping first occurrence (lowest objid) for each (fotrute_fk, rutenummer) pair...';
    
    -- Use a simpler approach: delete rows where there exists another row with same fotrute_fk and rutenummer
    -- but with lower objid (or objid exists when this one doesn't)
    EXECUTE format('
        DELETE FROM %I.fotruteinfo f1
        WHERE rutenummer IS NOT NULL
        AND EXISTS (
            SELECT 1 FROM %I.fotruteinfo f2
            WHERE f2.fotrute_fk = f1.fotrute_fk
            AND f2.rutenummer = f1.rutenummer
            AND (
                CASE 
                    WHEN f2.objid IS NOT NULL AND f1.objid IS NOT NULL THEN f2.objid < f1.objid
                    WHEN f2.objid IS NOT NULL AND f1.objid IS NULL THEN true
                    WHEN f2.objid IS NULL AND f1.objid IS NULL THEN f2.ctid < f1.ctid
                    ELSE false
                END
            )
        )
    ', schema_name, schema_name);

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RAISE NOTICE '  ✓ Removed % duplicate entries', deleted_count;
    RAISE NOTICE '  Time: %', clock_timestamp() - step_time;

    -- Verify no duplicates remain
    step_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE 'Step 3: Verifying no duplicates remain...';
    EXECUTE format('
        SELECT COALESCE(SUM(cnt - 1), 0)
        FROM (
            SELECT COUNT(*) as cnt
            FROM %I.fotruteinfo
            WHERE rutenummer IS NOT NULL
            GROUP BY fotrute_fk, rutenummer
            HAVING COUNT(*) > 1
        ) AS dup_counts
    ', schema_name) INTO duplicates_count;

    IF duplicates_count > 0 THEN
        RAISE WARNING '  ⚠ Warning: % duplicates still remain after cleanup', duplicates_count;
    ELSE
        RAISE NOTICE '  ✓ Verification passed: No duplicates remaining';
    END IF;
    RAISE NOTICE '  Time: %', clock_timestamp() - step_time;

    -- Update statistics
    step_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE 'Step 4: Updating statistics (ANALYZE)...';
    EXECUTE format('ANALYZE %I.fotruteinfo', schema_name);
    RAISE NOTICE '  ✓ Analyzed fotruteinfo';
    RAISE NOTICE '  Time: %', clock_timestamp() - step_time;

    -- Summary
    RAISE NOTICE '';
    RAISE NOTICE '=== Deduplication complete ===';
    RAISE NOTICE 'Summary:';
    RAISE NOTICE '  Schema: %', schema_name;
    RAISE NOTICE '  Duplicates removed: %', deleted_count;
    RAISE NOTICE '  Total time: %', clock_timestamp() - start_time;

END $$;
