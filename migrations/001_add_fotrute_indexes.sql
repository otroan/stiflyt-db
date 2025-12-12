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
        EXECUTE format('DROP INDEX IF EXISTS %I.%I', schema_name, 'idx_fotrute_senterlinje_gist');

        -- Create GIST index on senterlinje geometry column
        EXECUTE format('CREATE INDEX idx_fotrute_senterlinje_gist ON %I.fotrute USING GIST (senterlinje)', schema_name);

        RAISE NOTICE 'Created GIST index: %.idx_fotrute_senterlinje_gist', schema_name;
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
        EXECUTE format('DROP INDEX IF EXISTS %I.%I', schema_name, 'idx_fotruteinfo_fotrute_fk');
        EXECUTE format('CREATE INDEX idx_fotruteinfo_fotrute_fk ON %I.fotruteinfo USING BTREE (fotrute_fk)', schema_name);
        RAISE NOTICE 'Created BTREE index: %.idx_fotruteinfo_fotrute_fk', schema_name;

        -- Index on rutenummer for prefix filtering
        EXECUTE format('DROP INDEX IF EXISTS %I.%I', schema_name, 'idx_fotruteinfo_rutenummer');
        EXECUTE format('CREATE INDEX idx_fotruteinfo_rutenummer ON %I.fotruteinfo USING BTREE (rutenummer)', schema_name);
        RAISE NOTICE 'Created BTREE index: %.idx_fotruteinfo_rutenummer', schema_name;

        -- Index on vedlikeholdsansvarlig for organization filtering
        EXECUTE format('DROP INDEX IF EXISTS %I.%I', schema_name, 'idx_fotruteinfo_vedlikeholdsansvarlig');
        EXECUTE format('CREATE INDEX idx_fotruteinfo_vedlikeholdsansvarlig ON %I.fotruteinfo USING BTREE (vedlikeholdsansvarlig)', schema_name);
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
