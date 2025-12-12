# Database Migrations

This directory contains SQL migration files that are executed after data import to optimize the database for API queries.

## How It Works

1. **Automatic Execution**: Migrations run automatically after `update-datasets` completes successfully
2. **Manual Execution**: Run migrations manually with `make run-migrations` or `python3 scripts/run_migrations.py [database]`
3. **Execution Order**: Migrations are executed in alphabetical order (by filename)
4. **Idempotency**: Migrations are designed to be safe to run multiple times

## Migration Files

### `001_add_fotrute_indexes.sql`

Creates critical indexes for the turrutebasen dataset:

- **GIST index** on `fotrute.senterlinje` - Critical for bounding box query performance in the stiflyt API
- **BTREE indexes** on `fotruteinfo`:
  - `fotrute_fk` - For faster JOINs
  - `rutenummer` - For prefix filtering
  - `vedlikeholdsansvarlig` - For organization filtering
- **ANALYZE** commands for `fotrute` and `fotruteinfo` tables to update query planner statistics

**Note**: This migration dynamically finds the schema with prefix `turogfriluftsruter_*` since the schema hash changes with each dataset update.

## Creating New Migrations

1. Create a new SQL file with a numbered prefix (e.g., `002_add_another_index.sql`)
2. Use PostgreSQL `DO $$ ... END $$;` blocks for dynamic schema discovery if needed
3. Make migrations idempotent (use `DROP INDEX IF EXISTS`, `CREATE INDEX IF NOT EXISTS`, etc.)
4. Test the migration manually before committing

## Manual Migration Execution

```bash
# Run all migrations
make run-migrations

# Or specify database
make run-migrations PGDATABASE=your_database

# Or use the script directly
python3 scripts/run_migrations.py your_database
```

## Verifying Migrations

After running migrations, verify that indexes were created:

```bash
# Verify migration indexes
make verify-migration

# Or specify database
make verify-migration PGDATABASE=your_database

# Or use the script directly
python3 scripts/verify_migration.py your_database
```

## Migration Timing

**How long should migrations take?**

Migration speed depends on several factors:

1. **Table size**:
   - Small/empty tables: **< 1 second** per index
   - Medium tables (thousands of rows): **1-10 seconds** per index
   - Large tables (millions of rows): **minutes to hours** per index

2. **Index type**:
   - **BTREE indexes**: Fast, typically seconds even for large tables
   - **GIST indexes**: Slower, can take minutes for large geometry tables

3. **ANALYZE operations**: Usually very fast (< 1 second per table)

4. **If indexes already exist**: Very fast (< 1 second) - just DROP + CREATE

**If migration runs very fast (< 5 seconds total):**
- ✅ Could be normal if tables are small/empty
- ✅ Could be normal if indexes already existed
- ⚠️ **Verify with `make verify-migration`** to confirm indexes were created
- ⚠️ Check migration logs for warnings (schema not found, tables don't exist)

**Expected timing for typical turrutebasen dataset:**
- If tables have data: **10-60 seconds** (GIST index on geometry is the slowest)
- If tables are empty: **< 5 seconds**
- If indexes already exist: **< 2 seconds**

## Migration Best Practices

- **Idempotency**: Always use `IF EXISTS` / `IF NOT EXISTS` clauses
- **Schema Discovery**: Use dynamic schema finding for datasets with changing schema hashes
- **Error Handling**: Use `RAISE NOTICE` for informational messages, `RAISE WARNING` for non-fatal issues
- **Testing**: Test migrations on a copy of production data before deploying
