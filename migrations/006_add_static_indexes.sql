-- Migration: Add spatial indexes for static datasets (teig, stedsnavn)
-- Creates GIST indexes on geometry columns if present.

DO $$
DECLARE
    matrikkel_schema TEXT;
    geom_col TEXT;
BEGIN
    -- Find the current matrikkel schema with prefix 'matrikkeleneiendomskartteig_'
    SELECT nspname INTO matrikkel_schema
    FROM pg_namespace
    WHERE nspname LIKE 'matrikkeleneiendomskartteig_%'
      AND nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
    ORDER BY nspname DESC
    LIMIT 1;

    IF matrikkel_schema IS NULL THEN
        RAISE NOTICE 'No matrikkel schema found (matrikkeleneiendomskartteig_*). Skipping teig index.';
    ELSE
        SELECT gc.f_geometry_column INTO geom_col
        FROM public.geometry_columns gc
        WHERE gc.f_table_schema = matrikkel_schema
          AND gc.f_table_name = 'teig'
        ORDER BY gc.f_geometry_column
        LIMIT 1;

        IF geom_col IS NULL THEN
            SELECT a.attname INTO geom_col
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_type t ON t.oid = a.atttypid
            WHERE n.nspname = matrikkel_schema
              AND c.relname = 'teig'
              AND t.typname = 'geometry'
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
            LIMIT 1;
        END IF;

        IF geom_col IS NULL THEN
            RAISE WARNING 'No geometry column found for %.teig. Skipping GIST index.', matrikkel_schema;
        ELSE
            EXECUTE format(
                'CREATE INDEX IF NOT EXISTS idx_teig_%I_gist ON %I.teig USING GIST (%I)',
                geom_col, matrikkel_schema, geom_col
            );
            RAISE NOTICE 'Created/verified GIST index on %.teig(%).', matrikkel_schema, geom_col;
        END IF;
    END IF;

    -- Stedsnavn positions live in public.sted_posisjon
    SELECT gc.f_geometry_column INTO geom_col
    FROM public.geometry_columns gc
    WHERE gc.f_table_schema = 'public'
      AND gc.f_table_name = 'sted_posisjon'
    ORDER BY gc.f_geometry_column
    LIMIT 1;

    IF geom_col IS NULL THEN
        SELECT a.attname INTO geom_col
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_type t ON t.oid = a.atttypid
        WHERE n.nspname = 'public'
          AND c.relname = 'sted_posisjon'
          AND t.typname = 'geometry'
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum
        LIMIT 1;
    END IF;

    IF geom_col IS NULL THEN
        RAISE NOTICE 'No geometry column found for public.sted_posisjon. Skipping GIST index.';
    ELSE
        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS idx_sted_posisjon_%I_gist ON public.sted_posisjon USING GIST (%I)',
            geom_col, geom_col
        );
        RAISE NOTICE 'Created/verified GIST index on public.sted_posisjon(%).', geom_col;
    END IF;
END $$;
