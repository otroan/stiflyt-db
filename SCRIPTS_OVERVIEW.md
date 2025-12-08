# Scripts Overview

## Core Scripts (Essential)

These scripts are essential for the main purpose: keeping PostGIS databases up to date.

### `download_kartverket.py`
**Purpose**: Download datasets from Kartverket/Geonorge ATOM feeds
**Usage**:
- `python3 scripts/download_kartverket.py --config datasets.yaml`
- `python3 scripts/download_kartverket.py --list-datasets`

**Features**:
- Discovers datasets from Tjenestefeed.xml catalog
- Downloads only if newer versions available
- Supports batch downloads from config file

### `load_dataset.py` ⭐ **Unified Loader**
**Purpose**: Load datasets into PostGIS (auto-detects format)
**Usage**:
- `python3 scripts/load_dataset.py <zip_file> <database> [table_name] [srid] --drop-tables`

**Features**:
- Auto-detects format (PostGIS SQL or GML)
- Automatically replaces old data (`--drop-tables` for SQL, `-overwrite` for GML)
- Single unified script for all formats

**Replaces**: `load_matrikkel_teig.py` and `load_gml.py` (kept for backward compatibility)

### `update_datasets.py`
**Purpose**: Cron-friendly script to update all datasets
**Usage**:
- `python3 scripts/update_datasets.py datasets.yaml [database]`
- `make update-datasets`

**Features**:
- Downloads updates (only if newer)
- Loads all datasets from config
- Automatic cleanup of old data
- Comprehensive logging

## Optional Scripts (For Debugging/Inspection)

These scripts are useful for debugging and inspection but not essential for the main purpose.

### `inspect_matrikkel.py`
**Purpose**: Inspect database tables and sample data
**When to use**: Debugging, checking what's in the database
**Not needed if**: Backends query the database directly

### `query_matrikkel.py`
**Purpose**: Query matrikkel data with geometries
**When to use**: Testing queries, debugging
**Not needed if**: Backends are in different databases and query directly

### `query_stedsnavn.py`
**Purpose**: Query place names with geospatial searches
**When to use**: Testing queries, debugging
**Not needed if**: Backends are in different databases and query directly

### `inspect_matrikkel_wsdl*.py`
**Purpose**: Inspect Matrikkel API WSDL
**When to use**: Exploring API capabilities
**Not needed if**: Only using downloaded datasets

## Deprecated Scripts (Kept for Backward Compatibility)

These scripts are kept for backward compatibility but should use `load_dataset.py` instead:

- `load_matrikkel_teig.py` → Use `load_dataset.py` instead
- `load_gml.py` → Use `load_dataset.py` instead

## Recommended Workflow

### For Cron Updates (Main Purpose)
```bash
# Install dependencies once
make dependencies

# Setup database once
make create-db

# Cron entry (daily updates)
0 2 * * * cd /path/to/stiflyt-db && make update-datasets >> logs/cron.log 2>&1
```

### For Manual Operations
```bash
# Download datasets
python3 scripts/download_kartverket.py --config datasets.yaml

# Load a dataset (auto-detects format)
python3 scripts/load_dataset.py data/matrikkel/file.zip mydb --drop-tables

# Or use Makefile
make load-dataset ZIP_FILE=data/stedsnavn/file.zip TABLE=stedsnavn SRID=25833
```

## Script Dependencies

### Required
- `download_kartverket.py` - Core download functionality
- `load_dataset.py` - Core loading functionality
- `update_datasets.py` - Cron orchestration

### Optional
- Query scripts - Only if you need to test queries locally
- Inspect scripts - Only if you need to debug database contents

## Summary

**Essential for cron updates**: `download_kartverket.py`, `load_dataset.py`, `update_datasets.py`

**Optional**: All query and inspect scripts (backends query databases directly)

The unified `load_dataset.py` replaces the need for separate load scripts - it auto-detects format and handles everything automatically.

