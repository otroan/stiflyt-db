-- Migration: Add geometry column to endpoint_names for stable matching
-- Created: 2026-01-28
--
-- Problem: anchor_node_id in ops.endpoint_names becomes stale after refresh
-- because node IDs are regenerated (SERIAL) even though geometries are stable.
--
-- Solution: Store geometry in endpoint_names and match by geometry instead of ID.

DO $$
BEGIN
    -- Add geometry column if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'ops'
          AND table_name = 'endpoint_names'
          AND column_name = 'geom'
    ) THEN
        ALTER TABLE ops.endpoint_names
        ADD COLUMN geom GEOMETRY(POINT) NULL;

        RAISE NOTICE 'Added geom column to ops.endpoint_names';
    ELSE
        RAISE NOTICE 'Column ops.endpoint_names.geom already exists';
    END IF;

    -- Create GIST index on geometry if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'ops'
          AND tablename = 'endpoint_names'
          AND indexname = 'endpoint_names_geom_gist'
    ) THEN
        CREATE INDEX endpoint_names_geom_gist
        ON ops.endpoint_names USING GIST (geom);

        RAISE NOTICE 'Created GIST index on ops.endpoint_names.geom';
    ELSE
        RAISE NOTICE 'Index endpoint_names_geom_gist already exists';
    END IF;

    -- Populate geometry for existing endpoint_names that don't have it
    -- Match by current anchor_node_id
    UPDATE ops.endpoint_names en
    SET geom = an.geom
    FROM stiflyt.anchor_nodes an
    WHERE en.anchor_node_id = an.node_id
      AND en.geom IS NULL
      AND an.geom IS NOT NULL;

    RAISE NOTICE 'Populated geometry for existing endpoint_names';
END $$;
