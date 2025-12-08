# Installation Guide

## Quick Start

```bash
# 1. Install system dependencies (Ubuntu/Debian)
make dependencies

# 2. Start PostgreSQL
sudo systemctl start postgresql

# 3. Create database
make create-db

# 4. Download and load datasets
make update-datasets
```

## Virtual Environment Setup

This package uses a Python virtual environment for dependency management.

### Automatic Setup (Recommended)

The `make dependencies` target automatically creates a virtual environment and installs dependencies:

```bash
make dependencies
```

This creates `venv/` and installs all required packages.

### Manual Setup

If you prefer manual setup:

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # On Linux/macOS
# or
venv\Scripts\activate  # On Windows

# Install package in editable mode
pip install -e .

# Or install dependencies only
pip install psycopg2-binary pyyaml
```

### Using the Virtual Environment

**Option 1: Makefile (Recommended)**
All Makefile targets automatically use the virtual environment:
```bash
make update-datasets
make db-status
make inspect-db
```

**Option 2: Activate manually**
```bash
source venv/bin/activate
python3 scripts/update_datasets.py datasets.yaml matrikkel
python3 scripts/db_status.py matrikkel
deactivate
```

**Option 3: Direct path**
```bash
venv/bin/python3 scripts/update_datasets.py datasets.yaml matrikkel
```

## Cron Setup with Virtual Environment

For cron jobs, use the full path to the Python interpreter:

```bash
# In crontab
0 2 * * * cd /path/to/stiflyt-db && /path/to/stiflyt-db/venv/bin/python3 scripts/update_datasets.py datasets.yaml matrikkel >> logs/cron.log 2>&1
```

Or use the Makefile (which handles venv automatically):
```bash
0 2 * * * cd /path/to/stiflyt-db && make update-datasets >> logs/cron.log 2>&1
```

## Dependencies

### System Dependencies
- PostgreSQL with PostGIS extension
- GDAL (for GML files)
- Python 3.8+

### Python Dependencies
Managed via `pyproject.toml`:
- `psycopg2-binary>=2.9.0` - PostgreSQL adapter
- `pyyaml>=6.0` - YAML configuration parsing

### Installing System Dependencies

**Ubuntu/Debian:**
```bash
make dependencies
```

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

## Development Setup

For development with additional tools:

```bash
# Create venv
python3 -m venv venv
source venv/bin/activate

# Install with dev dependencies
pip install -e ".[dev]"

# This installs:
# - pytest (testing)
# - black (code formatting)
# - flake8 (linting)
```

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

## Package Structure

```
stiflyt-db/
├── pyproject.toml      # Package configuration and dependencies
├── scripts/            # Python scripts
│   ├── __init__.py
│   ├── download_kartverket.py
│   ├── load_dataset.py
│   ├── update_datasets.py
│   ├── db_status.py
│   └── inspect_db.py
├── venv/               # Virtual environment (created by make dependencies)
├── datasets.yaml       # Configuration file
└── Makefile            # Build automation
```

## Why Virtual Environment?

Using a virtual environment:
- ✅ Isolates dependencies from system Python
- ✅ Ensures consistent versions across environments
- ✅ Prevents conflicts with other projects
- ✅ Makes deployment easier
- ✅ Follows Python best practices

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

