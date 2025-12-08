# Quick Start Guide

## Step-by-Step Workflow

### 1. ✅ PostgreSQL Started
You've already done this!

### 2. Create Database (Next Step)
```bash
make create-db
```

**What this does:**
- Creates an empty database (default: `matrikkel`)
- Enables PostGIS extension
- **Does NOT create any tables** (tables are created when you load data)

### 3. Download and Load Data
```bash
make update-datasets
```

**What this does:**
- Downloads datasets from Kartverket/Geonorge (if newer versions available)
- Loads them into the database
- **Creates all tables automatically** during the load process

## Complete Workflow

```bash
# 1. Install dependencies (if not done)
make dependencies

# 2. Start PostgreSQL (you've done this)
sudo systemctl start postgresql

# 3. Create empty database with PostGIS
make create-db

# 4. Download and load all datasets (creates tables)
make update-datasets

# 5. Verify everything worked
make db-status
```

## What Creates Tables?

**Tables are created automatically when you LOAD data:**

1. **PostGIS SQL files**: When `load_dataset.py` runs SQL files, they contain `CREATE TABLE` statements
2. **GML files**: When `ogr2ogr` loads GML files, it creates tables automatically

**The `create-db` step only:**
- Creates the database container
- Enables PostGIS extension
- Does NOT create any data tables

## Alternative: Step-by-Step

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

## Verify After Loading

```bash
# Check database status
make db-status

# Inspect tables
make inspect-db

# See specific table schema
venv/bin/python3 scripts/inspect_db.py matrikkel --schema teig
```

## Summary

| Step | Command | What It Does |
|------|---------|--------------|
| 1 | `make dependencies` | Install system packages + create venv |
| 2 | `sudo systemctl start postgresql` | Start PostgreSQL (✅ you did this) |
| 3 | `make create-db` | **Create empty database + PostGIS** ← You are here |
| 4 | `make update-datasets` | **Download + Load data (creates tables)** ← Next step |

**Answer: Next step is `make create-db`, then `make update-datasets` will create all tables.**

