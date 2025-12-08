# Cron-Based Dataset Updates

## Overview

This package is designed to run from cron to automatically download and update datasets from Kartverket/Geonorge. The update process handles **automatic cleanup of old data** - you don't need to manually remove old tables.

## How It Works

### 1. Download Script (`download_kartverket.py`)
- **Checks for updates**: Compares file timestamps from the ATOM feed
- **Only downloads if newer**: Skips download if files are already up-to-date
- **Verifies integrity**: Checks file size and ZIP integrity before using

### 2. Load Scripts

#### PostGIS SQL Files (via `load_dataset.py`)
- **`--drop-tables` flag**: Automatically drops existing tables before loading
- **Extracts table names**: From SQL CREATE TABLE statements
- **Clean replacement**: Old data is completely removed before new data is loaded

#### GML Files (via `load_dataset.py`)
- **`-overwrite` flag**: ogr2ogr automatically replaces the entire table
- **No duplicates**: Old data is completely replaced

### 3. Update Script (`update_datasets.py`)
- **Orchestrates the process**: Downloads and loads all datasets from config
- **Automatic cleanup**: Uses `--drop-tables` for PostGIS, `-overwrite` for GML
- **Logging**: All operations logged to `./logs/update_YYYYMMDD_HHMMSS.log`
- **Error handling**: Continues with other datasets if one fails

## Answer: Do You Need to Remove Old Data?

**No, you don't need any magic!** The scripts handle it automatically:

1. **PostGIS datasets**: Use `--drop-tables` flag which:
   - Extracts table names from SQL files
   - Runs `DROP TABLE IF EXISTS ... CASCADE` before loading
   - Ensures clean replacement

2. **GML datasets**: Use `-overwrite` flag in ogr2ogr which:
   - Completely replaces the table
   - No old data remains

## Cron Setup

### Basic Cron Entry

```bash
# Update datasets daily at 2 AM
0 2 * * * /usr/bin/python3 /path/to/stiflyt-db/scripts/update_datasets.py /path/to/datasets.yaml >> /path/to/logs/cron.log 2>&1
```

### Using Makefile

```bash
# Add to crontab
0 2 * * * cd /path/to/stiflyt-db && make update-datasets >> logs/cron.log 2>&1
```

### Environment Variables

Set in crontab or systemd service:
```bash
PGDATABASE=matrikkel
PGHOST=localhost
PGPORT=5432
PGUSER=postgres
PGPASSWORD=your_password
LOG_DIR=/path/to/logs
```

## Manual Update

```bash
# Update all datasets from config
make update-datasets

# Or directly
python3 scripts/update_datasets.py datasets.yaml matrikkel
```

## What Happens During Update

1. **Download phase**:
   - Checks ATOM feed for updates
   - Downloads only if newer versions available
   - Verifies file integrity

2. **Load phase** (for each dataset):
   - **PostGIS**: Drops existing tables → Loads new SQL files
   - **GML**: Replaces entire table with ogr2ogr `-overwrite`
   - Old data is completely removed before new data is loaded

3. **Logging**:
   - All operations logged to timestamped log files
   - Old logs cleaned up after 30 days

## Read-Only Database Considerations

Since your database should be read-only:

1. **Tables are replaced, not appended**: Old data is completely removed
2. **No manual cleanup needed**: Scripts handle everything
3. **Atomic replacement**: For GML, ogr2ogr replaces tables atomically
4. **Transaction safety**: PostGIS SQL files typically use transactions

## Monitoring

Check logs for update status:
```bash
# View latest log
tail -f logs/update_*.log

# Check for errors
grep -i error logs/update_*.log

# View update history
ls -lth logs/update_*.log
```

## Troubleshooting

### If updates fail:
1. Check log files in `./logs/`
2. Verify database connection (PGHOST, PGPORT, etc.)
3. Ensure PostgreSQL is running
4. Check disk space
5. Verify GDAL/ogr2ogr is installed (for GML files)

### If old data persists:
- PostGIS: Ensure `--drop-tables` flag is used (handled automatically by `update_datasets.py`)
- GML: Verify ogr2ogr uses `-overwrite` flag (handled automatically by `load_dataset.py`)
- Check logs for DROP TABLE statements

## Summary

✅ **Automatic cleanup**: Scripts handle old data removal
✅ **No manual intervention**: Just run the update script
✅ **Read-only safe**: Tables are replaced, not appended
✅ **Cron-ready**: Designed for automated updates

You don't need to do anything special - the scripts handle all cleanup automatically!

