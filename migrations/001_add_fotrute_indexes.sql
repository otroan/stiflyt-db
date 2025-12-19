-- Migration: Add spatial GIST index on fotrute.senterlinje and BTREE indexes on fotruteinfo
-- This migration is critical for bounding box query performance in the stiflyt API
-- Created: 2024
--
-- This migration dynamically finds the schema with prefix 'turogfriluftsruter_*'
-- since the schema hash changes with each dataset update.

DO $$
DECLARE
    schema_name TEXT;
    schema_found BOOLEAN := FALSE;
BEGIN
    -- Find the schema with prefix 'turogfriluftsruter_'
    -- The hash suffix changes with each dataset update, so we need to find it dynamically
    SELECT nspname INTO schema_name
    FROM pg_namespace
    WHERE nspname LIKE 'turogfriluftsruter_%'
      AND nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
    ORDER BY nspname DESC  -- Get the most recent one (typically highest hash or latest)
    LIMIT 1;

    IF schema_name IS NULL THEN
        RAISE WARNING 'Schema with prefix turogfriluftsruter_* not found. Skipping index creation.';
        RETURN;
    END IF;

    RAISE NOTICE 'Found schema: %', schema_name;
    schema_found := TRUE;

    -- Create GIST index on fotrute.senterlinje for spatial queries
    -- This is critical for bounding box query performance
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = schema_name
          AND table_name = 'fotrute'
    ) THEN
        -- Drop index if it already exists (for idempotency)
        -- Handle case where we're not owner of the index
        BEGIN
            EXECUTE format('DROP INDEX IF EXISTS %I.%I', schema_name, 'idx_fotrute_senterlinje_gist');
        EXCEPTION WHEN insufficient_privilege THEN
            -- Not owner - try to alter owner first, then drop
            BEGIN
                EXECUTE format('ALTER INDEX %I.%I OWNER TO stiflyt_updater', schema_name, 'idx_fotrute_senterlinje_gist');
                EXECUTE format('DROP INDEX IF EXISTS %I.%I', schema_name, 'idx_fotrute_senterlinje_gist');
            EXCEPTION WHEN OTHERS THEN
                -- If that fails too, just continue - CREATE INDEX will handle it
                RAISE NOTICE 'Could not drop existing index (not owner), will create new one';
            END;
        END;

        -- Create GIST index on senterlinje geometry column (idempotent - will fail if exists, but that's OK)
        BEGIN
            EXECUTE format('CREATE INDEX idx_fotrute_senterlinje_gist ON %I.fotrute USING GIST (senterlinje)', schema_name);
            RAISE NOTICE 'Created GIST index: %.idx_fotrute_senterlinje_gist', schema_name;
        EXCEPTION WHEN duplicate_table THEN
            RAISE NOTICE 'Index idx_fotrute_senterlinje_gist already exists, skipping creation';
        END;
    ELSE
        RAISE WARNING 'Table %.fotrute does not exist. Skipping GIST index creation.', schema_name;
    END IF;

    -- Create BTREE indexes on fotruteinfo for faster JOINs and filtering
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = schema_name
          AND table_name = 'fotruteinfo'
    ) THEN
        -- Index on fotrute_fk for faster JOINs
        BEGIN
            EXECUTE format('DROP INDEX IF EXISTS %I.%I', schema_name, 'idx_fotruteinfo_fotrute_fk');
        EXCEPTION WHEN insufficient_privilege THEN
            BEGIN
                EXECUTE format('ALTER INDEX %I.%I OWNER TO stiflyt_updater', schema_name, 'idx_fotruteinfo_fotrute_fk');
                EXECUTE format('DROP INDEX IF EXISTS %I.%I', schema_name, 'idx_fotruteinfo_fotrute_fk');
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Could not drop index idx_fotruteinfo_fotrute_fk (not owner)';
            END;
        END;
        BEGIN
            EXECUTE format('CREATE INDEX idx_fotruteinfo_fotrute_fk ON %I.fotruteinfo USING BTREE (fotrute_fk)', schema_name);
            RAISE NOTICE 'Created BTREE index: %.idx_fotruteinfo_fotrute_fk', schema_name;
        EXCEPTION WHEN duplicate_table THEN
            RAISE NOTICE 'Index idx_fotruteinfo_fotrute_fk already exists, skipping';
        END;

        -- Index on rutenummer for prefix filtering
        BEGIN
            EXECUTE format('DROP INDEX IF EXISTS %I.%I', schema_name, 'idx_fotruteinfo_rutenummer');
        EXCEPTION WHEN insufficient_privilege THEN
            BEGIN
                EXECUTE format('ALTER INDEX %I.%I OWNER TO stiflyt_updater', schema_name, 'idx_fotruteinfo_rutenummer');
                EXECUTE format('DROP INDEX IF EXISTS %I.%I', schema_name, 'idx_fotruteinfo_rutenummer');
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Could not drop index idx_fotruteinfo_rutenummer (not owner)';
            END;
        END;
        BEGIN
            EXECUTE format('CREATE INDEX idx_fotruteinfo_rutenummer ON %I.fotruteinfo USING BTREE (rutenummer)', schema_name);
            RAISE NOTICE 'Created BTREE index: %.idx_fotruteinfo_rutenummer', schema_name;
        EXCEPTION WHEN duplicate_table THEN
            RAISE NOTICE 'Index idx_fotruteinfo_rutenummer already exists, skipping';
        END;

        -- Index on vedlikeholdsansvarlig for organization filtering
        BEGIN
            EXECUTE format('DROP INDEX IF EXISTS %I.%I', schema_name, 'idx_fotruteinfo_vedlikeholdsansvarlig');
        EXCEPTION WHEN insufficient_privilege THEN
            BEGIN
                EXECUTE format('ALTER INDEX %I.%I OWNER TO stiflyt_updater', schema_name, 'idx_fotruteinfo_vedlikeholdsansvarlig');
                EXECUTE format('DROP INDEX IF EXISTS %I.%I', schema_name, 'idx_fotruteinfo_vedlikeholdsansvarlig');
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Could not drop index idx_fotruteinfo_vedlikeholdsansvarlig (not owner)';
            END;
        END;
        BEGIN
            EXECUTE format('CREATE INDEX idx_fotruteinfo_vedlikeholdsansvarlig ON %I.fotruteinfo USING BTREE (vedlikeholdsansvarlig)', schema_name);
            RAISE NOTICE 'Created BTREE index: %.idx_fotruteinfo_vedlikeholdsansvarlig', schema_name;
        EXCEPTION WHEN duplicate_table THEN
            RAISE NOTICE 'Index idx_fotruteinfo_vedlikeholdsansvarlig already exists, skipping';
        END;
        RAISE NOTICE 'Created BTREE index: %.idx_fotruteinfo_vedlikeholdsansvarlig', schema_name;
    ELSE
        RAISE WARNING 'Table %.fotruteinfo does not exist. Skipping BTREE index creation.', schema_name;
    END IF;

    -- Update statistics with ANALYZE for query planner optimization
    IF schema_found THEN
        EXECUTE format('ANALYZE %I.fotrute', schema_name);
        RAISE NOTICE 'Analyzed table: %.fotrute', schema_name;

        EXECUTE format('ANALYZE %I.fotruteinfo', schema_name);
        RAISE NOTICE 'Analyzed table: %.fotruteinfo', schema_name;
    END IF;

END $$;
