# Stiflyt Database - Kartverket/Geonorge Dataset Manager

Automated tool for downloading and maintaining PostGIS databases with Kartverket/Geonorge datasets.

## Purpose

This package is designed to **keep PostGIS databases up to date** by automatically downloading and loading datasets from Kartverket/Geonorge. It's optimized for **cron-based updates** where datasets are kept read-only and accessed by backend applications in separate databases.

## Quick Start

```bash
# 1. Install dependencies (Ubuntu/Debian)
#    This creates a virtual environment and installs Python packages
make dependencies

# 2. Start PostgreSQL
sudo systemctl start postgresql

# 3. Create database
make create-db

# 4. Download and load datasets
make update-datasets

# 5. Verify everything worked
make db-status
```

**Note**: All Makefile targets automatically use the virtual environment. For manual script execution, activate the venv first:
```bash
source venv/bin/activate
python3 scripts/update_datasets.py datasets.yaml matrikkel
```

## Installation

### System Dependencies

**Ubuntu/Debian:**
```bash
make dependencies
```

This automatically:
- Installs PostgreSQL, PostGIS, GDAL, and Python packages
- Creates a virtual environment (`venv/`)
- Installs Python dependencies

**Manual installation:**
```bash
sudo apt-get update
sudo apt-get install -y \
    postgresql \
    postgresql-contrib \
    postgis \
    postgresql-postgis \
    gdal-bin \
    python3-gdal \
    python3 \
    python3-pip \
    python3-venv
```

### Virtual Environment

The package uses a Python virtual environment for dependency management. The `make dependencies` target automatically creates and configures it.

**Manual setup:**
```bash
python3 -m venv venv
source venv/bin/activate  # On Linux/macOS
pip install -e .
```

**Using the virtual environment:**
- **Option 1 (Recommended)**: Use Makefile targets - they automatically use the venv
- **Option 2**: Activate manually: `source venv/bin/activate`
- **Option 3**: Use direct path: `venv/bin/python3 scripts/update_datasets.py ...`

### Requirements

- PostgreSQL with PostGIS extension
- GDAL (for GML files)
- Python 3.8+ with venv support

**Python Dependencies** (managed via `pyproject.toml`):
- `psycopg2-binary>=2.9.0` - PostgreSQL adapter
- `pyyaml>=6.0` - YAML configuration parsing

## Core Components

### Essential Scripts

1. **`scripts/download_kartverket.py`** - Downloads datasets from ATOM feeds
   - Discovers datasets from Tjenestefeed.xml catalog
   - Only downloads if newer versions available
   - Supports batch downloads from config file

2. **`scripts/load_dataset.py`** - Unified loader (auto-detects format)
   - Auto-detects PostGIS SQL or GML format
   - Automatically replaces old data
   - Single script for all formats

3. **`scripts/update_datasets.py`** - Cron-friendly update orchestrator
   - Downloads updates (only if newer)
   - Loads all datasets from config
   - Comprehensive logging
   - Post-update health verification

### Database Management Scripts

4. **`scripts/db_status.py`** - Database health check
   - Verify connectivity and PostGIS status
   - Check table counts and database size
   - Monitor database health

5. **`scripts/inspect_db.py`** - Schema inspection
   - List tables, columns, indexes
   - Show spatial reference systems
   - Detailed table schemas

## Configuration

**`datasets.yaml`** - Dataset configuration file:
```yaml
- name: teig
  dataset: "Matrikkelen - Eiendomskart Teig PostGIS-format"
  format: PostGIS
  utm_zone: 25833
  area_filter: Norge
  output_dir: ./data/matrikkel
```

## Usage

### Complete Workflow

```bash
# 1. Install dependencies (if not done)
make dependencies

# 2. Start PostgreSQL
sudo systemctl start postgresql

# 3. Create empty database with PostGIS
make create-db

# 4. Download and load all datasets (creates tables automatically)
make update-datasets

# 5. Verify everything worked
make db-status
```

### What Creates Tables?

**Tables are created automatically when you LOAD data:**

1. **PostGIS SQL files**: When `load_dataset.py` runs SQL files, they contain `CREATE TABLE` statements
2. **GML files**: When `ogr2ogr` loads GML files, it creates tables automatically

**The `create-db` step only:**
- Creates the database container
- Enables PostGIS extension
- Does NOT create any data tables

### Alternative: Step-by-Step

If you want to download and load separately:

```bash
# 1. Create database
make create-db

# 2. Download only (no tables created yet)
make download-matrikkel

# 3. Load data (this creates tables)
make load-matrikkel
```

But `make update-datasets` does both steps automatically, so that's easier!

### Verifying After Loading

```bash
# Check database status
make db-status

# Inspect tables
make inspect-db

# See specific table schema
venv/bin/python3 scripts/inspect_db.py matrikkel --schema teig
```

## Database Migrations

Migrations are SQL files that optimize the database for API queries after data import.

### How It Works

1. **Automatic Execution**: Migrations run automatically after `update-datasets` completes successfully
2. **Manual Execution**: Run migrations manually with `make run-migrations` or `python3 scripts/run_migrations.py [database]`
3. **Execution Order**: Migrations are executed in alphabetical order (by filename)
4. **Idempotency**: Migrations are designed to be safe to run multiple times

### Migration Files

Located in `migrations/` directory:

- **`001_add_fotrute_indexes.sql`** - Creates critical indexes for the turrutebasen dataset
- **`002_build_topology.sql`** - Builds topology for route networks
- **`003_add_link_ruteinfo_view.sql`** - Creates views for route information
- **`004_add_link_endpoint_names.sql`** - Adds endpoint name mappings

### Manual Migration Execution

```bash
# Run all migrations
make run-migrations

# Or specify database
make run-migrations PGDATABASE=your_database

# Or use the script directly
python3 scripts/run_migrations.py your_database
```

### Verifying Migrations

```bash
# Verify migration indexes
make verify-migration

# Or specify database
make verify-migration PGDATABASE=your_database

# Or use the script directly
python3 scripts/verify_migration.py your_database
```

### Migration Timing

Migration speed depends on:
- **Table size**: Small tables (< 1 second), large tables (minutes to hours)
- **Index type**: BTREE indexes are fast, GIST indexes on geometry are slower
- **If indexes already exist**: Very fast (< 1 second)

**Expected timing for typical turrutebasen dataset:**
- If tables have data: **10-60 seconds** (GIST index on geometry is the slowest)
- If tables are empty: **< 5 seconds**
- If indexes already exist: **< 2 seconds**

### Creating New Migrations

1. Create a new SQL file with a numbered prefix (e.g., `005_add_another_index.sql`)
2. Use PostgreSQL `DO $$ ... END $$;` blocks for dynamic schema discovery if needed
3. Make migrations idempotent (use `DROP INDEX IF EXISTS`, `CREATE INDEX IF NOT EXISTS`, etc.)
4. Test the migration manually before committing

**Best Practices:**
- Always use `IF EXISTS` / `IF NOT EXISTS` clauses
- Use dynamic schema finding for datasets with changing schema hashes
- Use `RAISE NOTICE` for informational messages, `RAISE WARNING` for non-fatal issues
- Test migrations on a copy of production data before deploying

## Cron Setup

Add to crontab for daily updates:

```bash
# Update datasets daily at 2 AM
0 2 * * * cd /path/to/stiflyt-db && make update-datasets >> logs/cron.log 2>&1
```

For cron jobs, the Makefile automatically handles the virtual environment. Alternatively, use the full path to the Python interpreter:

```bash
0 2 * * * cd /path/to/stiflyt-db && /path/to/stiflyt-db/venv/bin/python3 scripts/update_datasets.py datasets.yaml matrikkel >> logs/cron.log 2>&1
```

## Makefile Targets

```bash
make dependencies      # Install system dependencies
make create-db         # Create PostGIS database
make update-datasets   # Update all datasets from config (cron-friendly)
make load-dataset      # Load a specific dataset
make db-status         # Check database health and status
make inspect-db        # Inspect database schema (tables, indexes, SRIDs)
make run-migrations    # Run database migrations
make verify-migration  # Verify migration indexes were created
```

## Environment Variables

- `PGDATABASE` - Database name (default: matrikkel)
- `PGHOST` - PostgreSQL host (default: localhost)
- `PGPORT` - PostgreSQL port (default: 5432)
- `PGUSER` - PostgreSQL user (default: current user)
- `PGPASSWORD` - PostgreSQL password (required if using password auth)
- `LOG_DIR` - Log directory (default: ./logs)

**Note**: If you get authentication errors, try:
- `PGUSER=postgres make create-db` (use postgres superuser)
- Or set `PGPASSWORD` environment variable

## Key Features

✅ **Automatic cleanup**: Old data is automatically removed before loading new data
✅ **Format auto-detection**: Handles PostGIS SQL and GML formats automatically
✅ **Update detection**: Only downloads if newer versions available
✅ **Cron-ready**: Designed for automated updates
✅ **Read-only safe**: Tables are replaced, not appended
✅ **Health monitoring**: Post-update verification and database status checks
✅ **Schema inspection**: Tools for debugging and verification
✅ **Automatic migrations**: Database optimizations run after updates

## Troubleshooting

### Virtual Environment Not Found

If you see errors about missing packages:
```bash
# Recreate venv
rm -rf venv
make dependencies
```

### Permission Errors

If you get permission errors:
```bash
# Make scripts executable
chmod +x scripts/*.py

# Or use Python explicitly
python3 scripts/update_datasets.py ...
```

### Import Errors

If imports fail:
```bash
# Verify venv is activated
which python3  # Should show venv/bin/python3

# Reinstall package
pip install -e .
```

### Migration Runs Very Fast

If migration runs very fast (< 5 seconds total):
- ✅ Could be normal if tables are small/empty
- ✅ Could be normal if indexes already existed
- ⚠️ **Verify with `make verify-migration`** to confirm indexes were created
- ⚠️ Check migration logs for warnings (schema not found, tables don't exist)

## Package Structure

```
stiflyt-db/
├── pyproject.toml      # Package configuration and dependencies
├── scripts/            # Python scripts
│   ├── download_kartverket.py
│   ├── load_dataset.py
│   ├── update_datasets.py
│   ├── db_status.py
│   ├── inspect_db.py
│   └── run_migrations.py
├── migrations/         # SQL migration files
│   ├── 001_add_fotrute_indexes.sql
│   └── ...
├── venv/               # Virtual environment (created by make dependencies)
├── datasets.yaml       # Configuration file
└── Makefile            # Build automation
```

## Updating Dependencies

To update dependencies:
```bash
source venv/bin/activate
pip install --upgrade -e .
```

Or edit `pyproject.toml` and reinstall:
```bash
make dependencies  # Recreates venv with new dependencies
```
