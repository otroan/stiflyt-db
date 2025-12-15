SHELL := /bin/bash

DATA_DIR := data/matrikkel
PGDATABASE ?= matrikkel
PGHOST ?= localhost
PGPORT ?= 5432
PGUSER ?= $(shell whoami)

# Virtual environment
VENV := venv
PYTHON := $(VENV)/bin/python3
PIP := $(VENV)/bin/pip

.PHONY: dependencies download-matrikkel create-db ensure-db drop-db load-matrikkel setup-matrikkel reload-matrikkel inspect-matrikkel inspect-wsdl run-api test update-datasets db-status inspect-db run-migrations verify-migration build-links cron-update

# Install all required system dependencies (Ubuntu/Debian)
dependencies:
	@echo "==> Installing system dependencies..."
	@if ! command -v apt-get > /dev/null 2>&1; then \
		echo "Feil: Dette scriptet er kun for Ubuntu/Debian systemer"; \
		echo "For andre distribusjoner, installer manuelt:"; \
		echo "  - PostgreSQL og PostGIS"; \
		echo "  - GDAL (gdal-bin)"; \
		echo "  - Python3 og pip"; \
		exit 1; \
	fi
	@echo "==> Oppdaterer pakkeliste..."
	@sudo apt-get update -qq
	@echo "==> Installerer PostgreSQL og PostGIS..."
	@sudo apt-get install -y \
		postgresql \
		postgresql-contrib \
		postgis \
		postgresql-postgis \
		postgresql-postgis-scripts \
		> /dev/null 2>&1 || true
	@echo "  ✓ PostgreSQL og PostGIS installert"
	@echo "==> Installerer GDAL (for ogr2ogr)..."
	@sudo apt-get install -y \
		gdal-bin \
		python3-gdal \
		> /dev/null 2>&1 || true
	@echo "  ✓ GDAL installert"
	@echo "==> Installerer Python verktøy..."
	@sudo apt-get install -y \
		python3 \
		python3-pip \
		python3-venv \
		> /dev/null 2>&1 || true
	@echo "  ✓ Python installert"
	@echo "==> Oppretter virtual environment..."
	@python3 -m venv $(VENV) || true
	@echo "  ✓ Virtual environment opprettet"
	@echo "==> Installerer Python pakker..."
	@$(PIP) install --upgrade pip setuptools wheel > /dev/null 2>&1 || true
	@$(PIP) install -e . > /dev/null 2>&1 || true
	@echo "  ✓ Python pakker installert"
	@echo ""
	@echo "==> Verifiserer installasjoner..."
	@command -v psql > /dev/null && echo "  ✓ psql funnet" || echo "  ✗ psql ikke funnet"
	@command -v ogr2ogr > /dev/null && echo "  ✓ ogr2ogr funnet" || echo "  ✗ ogr2ogr ikke funnet"
	@command -v python3 > /dev/null && echo "  ✓ python3 funnet" || echo "  ✗ python3 ikke funnet"
	@$(PYTHON) -c "import psycopg2" 2>/dev/null && echo "  ✓ psycopg2 installert" || echo "  ✗ psycopg2 ikke installert"
	@$(PYTHON) -c "import yaml" 2>/dev/null && echo "  ✓ pyyaml installert" || echo "  ✗ pyyaml ikke installert"
	@echo ""
	@echo "==> Ferdig! System dependencies installert."
	@echo ""
	@echo "Neste steg:"
	@echo "  1. Start PostgreSQL: sudo systemctl start postgresql"
	@echo "  2. Opprett database: make create-db"
	@echo "  3. Last ned data: make download-matrikkel"
	@echo ""
	@echo "Note: Scripts bruker virtual environment i $(VENV)/"
	@echo "      For manuell bruk: source $(VENV)/bin/activate"

create-db:
	@echo "==> Oppretter database '$(PGDATABASE)' ..."
	@if [ "$(PGHOST)" = "localhost" ]; then \
		PSQL_HOST="" PSQL_PORT=""; \
	else \
		PSQL_HOST="-h $(PGHOST)"; PSQL_PORT="-p $(PGPORT)"; \
	fi; \
	if [ -z "$(PGPASSWORD)" ]; then \
		echo "  ℹ Bruker peer authentication (local socket)"; \
	fi; \
	PGPASSWORD=$(PGPASSWORD) psql $$PSQL_HOST $$PSQL_PORT -U $(PGUSER) -d postgres \
		-c "SELECT 1 FROM pg_database WHERE datname = '$(PGDATABASE)'" 2>&1 | grep -q 1 \
		&& echo "  ⊙ Database '$(PGDATABASE)' eksisterer allerede" \
		|| (PGPASSWORD=$(PGPASSWORD) psql $$PSQL_HOST $$PSQL_PORT -U $(PGUSER) -d postgres \
			-c "CREATE DATABASE $(PGDATABASE);" 2>&1 && echo "  ✓ Database opprettet" || \
			(echo "  ✗ Kunne ikke opprette database."; \
			 echo "     For remote connections, sett PGPASSWORD eller bruk:"; \
			 echo "     PGUSER=postgres PGPASSWORD=password make create-db"; \
			 exit 1)); \
	if [ "$(PGHOST)" = "localhost" ]; then \
		PGPASSWORD=$(PGPASSWORD) psql -U $(PGUSER) -d $(PGDATABASE) \
			-c "CREATE EXTENSION IF NOT EXISTS postgis;" > /dev/null 2>&1 && echo "  ✓ PostGIS extension aktivert" || \
			(echo "  ⚠ Kunne ikke aktivere PostGIS extension som $(PGUSER)"; \
			 echo "     Prøver som postgres superuser..."; \
			 sudo -u postgres psql -d $(PGDATABASE) -c "CREATE EXTENSION IF NOT EXISTS postgis;" > /dev/null 2>&1 && echo "  ✓ PostGIS extension aktivert (som postgres)" || \
			 (echo "  ✗ Kunne ikke aktivere PostGIS extension"; \
			  echo "     Kjør manuelt: sudo -u postgres psql -d $(PGDATABASE) -c 'CREATE EXTENSION postgis;'"; \
			  exit 1)); \
	else \
		PGPASSWORD=$(PGPASSWORD) psql $$PSQL_HOST $$PSQL_PORT -U $(PGUSER) -d $(PGDATABASE) \
			-c "CREATE EXTENSION IF NOT EXISTS postgis;" > /dev/null 2>&1 && echo "  ✓ PostGIS extension aktivert" || \
			(echo "  ✗ Kunne ikke aktivere PostGIS extension"; \
			 echo "     Bruk superuser: PGUSER=postgres PGPASSWORD=password make create-db"; \
			 exit 1); \
	fi

# Ensure database exists (safe to run multiple times)
ensure-db: create-db

drop-db:
	@echo "==> Sletter database '$(PGDATABASE)' ..."
	@if [ "$(PGHOST)" = "localhost" ]; then \
		PSQL_HOST="" PSQL_PORT=""; \
	else \
		PSQL_HOST="-h $(PGHOST)"; PSQL_PORT="-p $(PGPORT)"; \
	fi; \
	PGPASSWORD=$(PGPASSWORD) psql $$PSQL_HOST $$PSQL_PORT -U $(PGUSER) -d postgres \
		-c "DROP DATABASE IF EXISTS $(PGDATABASE);" 2>&1 && echo "  ✓ Database slettet" || \
		(echo "  ✗ Kunne ikke slette database"; exit 1)


# Load any dataset (auto-detects PostGIS SQL or GML format)
# Usage: make load-dataset ZIP_FILE=data/stedsnavn/file.zip TABLE=stedsnavn SRID=25833
load-dataset: ensure-db $(VENV)
	@if [ -z "$(ZIP_FILE)" ]; then \
		echo "Feil: ZIP_FILE må angis. Eksempel: make load-dataset ZIP_FILE=data/stedsnavn/file.zip TABLE=stedsnavn"; \
		exit 1; \
	fi
	@$(PYTHON) ./scripts/load_dataset.py "$(ZIP_FILE)" $(PGDATABASE) $(TABLE) $(SRID) --drop-tables

# Update all datasets from config file (cron-friendly)
# This downloads updates and reloads data, replacing old tables
update-datasets: $(VENV)
	@$(PYTHON) scripts/update_datasets.py $(or $(CONFIG_FILE),datasets.yaml) $(PGDATABASE)

# Check database status and health
db-status: $(VENV)
	@$(PYTHON) scripts/db_status.py $(PGDATABASE)

# Inspect database schema (tables, columns, indexes, SRIDs)
inspect-db: $(VENV)
	@$(PYTHON) scripts/inspect_db.py $(PGDATABASE) --tables

# Run database migrations (creates indexes, updates statistics, etc.)
# Migrations run automatically after update-datasets, but can be run manually
run-migrations: $(VENV)
	@$(PYTHON) scripts/run_migrations.py $(PGDATABASE)

# Verify that migration indexes were created successfully
verify-migration: $(VENV)
	@$(PYTHON) scripts/verify_migration.py $(PGDATABASE)

# Build links from segments and anchor nodes
# Creates links and link_segments tables with topology
# This is a heavy operation and should be run after data import
build-links: $(VENV)
	@echo "==> Building links (this may take a while)..."
	@$(PYTHON) scripts/build_links.py --log-dir ./logs || (echo "✗ build-links failed - check logs/build_links_*.log" && exit 1)

# Cron-friendly full refresh: download/update datasets, migrations, build links
# Each step runs independently - if one fails, the next still runs
# Check logs/ for detailed output
cron-update: $(VENV)
	@echo "=== Starting cron-update pipeline ==="
	@echo "Step 1: Update datasets..."
	@$(MAKE) update-datasets || (echo "⚠ update-datasets failed - continuing with migrations" && true)
	@echo ""
	@echo "Step 2: Run migrations..."
	@$(MAKE) run-migrations || (echo "⚠ run-migrations failed - continuing with build-links" && true)
	@echo ""
	@echo "Step 3: Build links..."
	@$(MAKE) build-links || (echo "✗ build-links failed" && exit 1)
	@echo ""
	@echo "=== cron-update pipeline completed ==="

# Ensure virtual environment exists
$(VENV):
	@echo "==> Oppretter virtual environment..."
	@python3 -m venv $(VENV)
	@$(PIP) install --upgrade pip setuptools wheel > /dev/null 2>&1
	@$(PIP) install -e . > /dev/null 2>&1
	@echo "  ✓ Virtual environment klar"

# Run tests
test:
	@echo "==> Running tests..."
	@source venv/bin/activate && pip install -e ".[dev]" > /dev/null 2>&1 && pytest tests/ -v
