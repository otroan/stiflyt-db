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
```

**Note**: All Makefile targets automatically use the virtual environment. For manual script execution, activate the venv first:
```bash
source venv/bin/activate
python3 scripts/update_datasets.py datasets.yaml matrikkel
```

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

### Configuration

**`datasets.yaml`** - Dataset configuration file:
```yaml
- name: teig
  dataset: "Matrikkelen - Eiendomskart Teig PostGIS-format"
  format: PostGIS
  utm_zone: 25833
  area_filter: Norge
  output_dir: ./data/matrikkel
```

## Cron Setup

Add to crontab for daily updates:
```bash
# Update datasets daily at 2 AM
0 2 * * * cd /path/to/stiflyt-db && make update-datasets >> logs/cron.log 2>&1
```

## Key Features

✅ **Automatic cleanup**: Old data is automatically removed before loading new data
✅ **Format auto-detection**: Handles PostGIS SQL and GML formats automatically
✅ **Update detection**: Only downloads if newer versions available
✅ **Cron-ready**: Designed for automated updates
✅ **Read-only safe**: Tables are replaced, not appended
✅ **Health monitoring**: Post-update verification and database status checks
✅ **Schema inspection**: Tools for debugging and verification

## Documentation

- **`CRON_UPDATES.md`** - Detailed cron update guide
- **`SCRIPTS_OVERVIEW.md`** - Scripts reference
- **`STEDSNAVN_USAGE.md`** - Stedsnavn dataset usage guide

## Makefile Targets

```bash
make dependencies      # Install system dependencies
make create-db         # Create PostGIS database
make update-datasets   # Update all datasets from config (cron-friendly)
make load-dataset      # Load a specific dataset
make db-status         # Check database health and status
make inspect-db        # Inspect database schema (tables, indexes, SRIDs)
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
- See `POSTGRES_AUTH.md` for authentication setup options

## Requirements

- PostgreSQL with PostGIS extension
- GDAL (for GML files)
- Python 3.8+ with venv support

### Python Dependencies

Managed via `pyproject.toml`:
- `psycopg2-binary>=2.9.0` - PostgreSQL adapter
- `pyyaml>=6.0` - YAML configuration parsing

All dependencies are automatically installed in a virtual environment by `make dependencies`.

See `INSTALLATION.md` for detailed installation instructions.

