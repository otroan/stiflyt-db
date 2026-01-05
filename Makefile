SHELL := /bin/bash

DATA_DIR := data/matrikkel
PGDATABASE ?= matrikkel
PGHOST ?= localhost
PGPORT ?= 5432
PGUSER ?= stiflyt_updater
PGADMIN_USER ?= postgres

# OSM import performance optimizations
# These can be overridden via environment variables or Makefile variables
OSM_CACHE_MB ?= 8000
OSM_NUM_PROCESSES ?= 4
OSM_FLAT_NODES_DIR ?= ./data/flat_nodes
OSM_FLAT_NODES_FILE ?= $(OSM_FLAT_NODES_DIR)/norway_flat_nodes.bin
OSM_USE_SLIM ?= true

# Virtual environment
VENV := venv
PYTHON := $(VENV)/bin/python3
PIP := $(VENV)/bin/pip

.PHONY: dependencies help create-db ensure-db drop-db load-dataset update-datasets db-status inspect-db run-migrations verify-migration build-links cron-update setup-roles fresh-start init-db refresh-db test show-pg-optimizations

help:
	@echo "stiflyt-db Makefile"
	@echo ""
	@echo "Common targets:"
	@echo "  make dependencies   Install system deps (Debian/Ubuntu)"
	@echo "  make create-db      Create database + PostGIS extension"
	@echo "  make setup-roles    Create roles/privileges (superuser)"
	@echo "  make update-datasets Download + load datasets from config"
	@echo "  make run-migrations Run SQL migrations"
	@echo "  make fresh-start    Drop + recreate + load + migrate"
	@echo "  make refresh-db     Reload data + migrations without drop"
	@echo "  make db-status      Health check"
	@echo "  make inspect-db     Schema overview"
	@echo "  make test           Run tests"
	@echo "  make show-pg-optimizations  Show PostgreSQL optimization recommendations"
	@echo ""
	@echo "Variables:"
	@echo "  PGDATABASE=$(PGDATABASE) PGHOST=$(PGHOST) PGPORT=$(PGPORT)"
	@echo "  PGUSER=$(PGUSER) PGADMIN_USER=$(PGADMIN_USER)"
	@echo ""
	@echo "OSM Import Optimizations:"
	@echo "  OSM_CACHE_MB=$(OSM_CACHE_MB) (osm2pgsql cache size in MB)"
	@echo "  OSM_NUM_PROCESSES=$(OSM_NUM_PROCESSES) (parallel processes)"
	@echo "  OSM_FLAT_NODES_FILE=$(OSM_FLAT_NODES_FILE) (flat nodes file path)"
	@echo "  OSM_USE_SLIM=$(OSM_USE_SLIM) (use --slim mode: true/false)"

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
	@echo "==> Installerer osm2pgsql (for OSM import)..."
	@sudo apt-get install -y \
		osm2pgsql \
		> /dev/null 2>&1 || true
	@echo "  ✓ osm2pgsql installert"
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
	@command -v osm2pgsql > /dev/null && echo "  ✓ osm2pgsql funnet" || echo "  ✗ osm2pgsql ikke funnet"
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
	PGPASSWORD=$(PGPASSWORD) psql $$PSQL_HOST $$PSQL_PORT -U $(PGADMIN_USER) -d postgres \
		-c "SELECT 1 FROM pg_database WHERE datname = '$(PGDATABASE)'" 2>&1 | grep -q 1 \
		&& echo "  ⊙ Database '$(PGDATABASE)' eksisterer allerede" \
		|| (PGPASSWORD=$(PGPASSWORD) psql $$PSQL_HOST $$PSQL_PORT -U $(PGADMIN_USER) -d postgres \
			-c "CREATE DATABASE $(PGDATABASE);" 2>&1 && echo "  ✓ Database opprettet" || \
			(echo "  ✗ Kunne ikke opprette database."; \
			 echo "     For remote connections, sett PGPASSWORD eller bruk:"; \
			 echo "     PGADMIN_USER=postgres PGPASSWORD=password make create-db"; \
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

# Fresh start: Drop and recreate database with proper setup
fresh-start:
	@echo "==> FRESH START: Sletter og gjenoppretter database..."
	@echo "  ⚠ Dette vil slette all eksisterende data!"
	@read -p "  Er du sikker? (skriv 'yes' for å bekrefte): " confirm && [ "$$confirm" = "yes" ] || exit 1
	@echo ""
	@echo "==> Steg 1: Sletter eksisterende database..."
	@sudo -u postgres psql -c "DROP DATABASE IF EXISTS $(PGDATABASE);" 2>&1 && echo "  ✓ Database slettet" || echo "  ⊙ Database eksisterte ikke"
	@echo ""
	@echo "==> Steg 2: Oppretter ny database..."
	@sudo -u postgres psql -c "CREATE DATABASE $(PGDATABASE);" 2>&1 && echo "  ✓ Database opprettet" || (echo "  ✗ Kunne ikke opprette database"; exit 1)
	@sudo -u postgres psql -d $(PGDATABASE) -c "CREATE EXTENSION IF NOT EXISTS postgis;" 2>&1 > /dev/null && echo "  ✓ PostGIS extension aktivert" || (echo "  ✗ Kunne ikke aktivere PostGIS"; exit 1)
	@echo ""
	@echo "==> Steg 3: Setter opp roller og privilegier..."
	@sudo -u postgres psql -d $(PGDATABASE) -f migrations/000_setup_roles.sql 2>&1 | grep -v "^WARNING:" | grep -v "^NOTICE:" || true
	@echo "  ✓ Roller konfigurert"
	@echo ""
	@echo "==> Steg 4: Laster inn data (dette kan ta lang tid)..."
	@PGUSER=stiflyt_updater $(MAKE) update-datasets
	@echo ""
	@echo "==> Steg 5: Bygger links-tabell (kreves for views)..."
	@PGUSER=stiflyt_updater $(MAKE) build-links || (echo "  ⚠ build-links feilet - migrasjoner vil prøve igjen automatisk" && true)
	@echo ""
	@echo "==> Steg 6: Kjør migrasjoner..."
	@PGUSER=stiflyt_updater $(MAKE) run-migrations
	@echo ""
	@echo "==> Fresh start fullført!"
	@echo "  ✅ Database er initialisert og oppdatert"

# Initialize everything from zero (drop + recreate + load + migrations)
init-db: fresh-start

# Refresh data and migrations without dropping database
refresh-db: $(VENV)
	@echo "==> Refresh: Oppdaterer data og kjører migrasjoner..."
	@PGUSER=stiflyt_updater $(MAKE) update-datasets
	@echo ""
	@echo "==> Bygger links-tabell (kreves for views)..."
	@PGUSER=stiflyt_updater $(MAKE) build-links || (echo "  ⚠ build-links feilet - migrasjoner vil prøve igjen automatisk" && true)
	@echo ""
	@PGUSER=stiflyt_updater $(MAKE) run-migrations

# Setup database roles and permissions (run once after create-db)
# Requires superuser privileges - automatically tries as postgres if current user fails
setup-roles:
	@echo "==> Setting up database roles and permissions..."
	@if [ "$(PGHOST)" = "localhost" ]; then \
		PSQL_HOST="" PSQL_PORT=""; \
		if command -v sudo > /dev/null 2>&1; then \
			echo "  ℹ Kjører som postgres superuser (kreves for å opprette roller)..."; \
			sudo -u postgres psql -d $(PGDATABASE) -f migrations/000_setup_roles.sql 2>&1 | grep -v "^ERROR:" || true; \
			if sudo -u postgres psql -d $(PGDATABASE) -t -c "SELECT 1 FROM pg_roles WHERE rolname = 'stiflyt_updater'" 2>/dev/null | grep -q 1; then \
				echo "  ✓ Roles configured"; \
			else \
				echo "  ✗ Kunne ikke opprette roller."; \
				echo "     Kjør manuelt:"; \
				echo "     sudo -u postgres psql -d $(PGDATABASE) -f migrations/000_setup_roles.sql"; \
				exit 1; \
			fi; \
		else \
			echo "  ✗ sudo ikke tilgjengelig. Kjør manuelt:"; \
			echo "     sudo -u postgres psql -d $(PGDATABASE) -f migrations/000_setup_roles.sql"; \
			exit 1; \
		fi; \
	else \
		PSQL_HOST="-h $(PGHOST)"; PSQL_PORT="-p $(PGPORT)"; \
		if [ -z "$(PGPASSWORD)" ]; then \
			echo "  ⚠ PGPASSWORD må settes for remote connections"; \
			exit 1; \
		fi; \
		echo "  ℹ Kjører som postgres superuser..."; \
		PGPASSWORD=$(PGPASSWORD) psql $$PSQL_HOST $$PSQL_PORT -U postgres -d $(PGDATABASE) \
			-f migrations/000_setup_roles.sql 2>&1 | grep -v "^ERROR:" || true; \
		if PGPASSWORD=$(PGPASSWORD) psql $$PSQL_HOST $$PSQL_PORT -U postgres -d $(PGDATABASE) \
			-t -c "SELECT 1 FROM pg_roles WHERE rolname = 'stiflyt_updater'" 2>/dev/null | grep -q 1; then \
			echo "  ✓ Roles configured"; \
		else \
			echo "  ✗ Kunne ikke opprette roller."; \
			exit 1; \
		fi; \
	fi

drop-db:
	@echo "==> Sletter database '$(PGDATABASE)' ..."
	@if [ "$(PGHOST)" = "localhost" ]; then \
		PSQL_HOST="" PSQL_PORT=""; \
	else \
		PSQL_HOST="-h $(PGHOST)"; PSQL_PORT="-p $(PGPORT)"; \
	fi; \
	PGPASSWORD=$(PGPASSWORD) psql $$PSQL_HOST $$PSQL_PORT -U $(PGADMIN_USER) -d postgres \
		-c "DROP DATABASE IF EXISTS $(PGDATABASE);" 2>&1 && echo "  ✓ Database slettet" || \
		(echo "  ✗ Kunne ikke slette database"; exit 1)


# Load any dataset (auto-detects PostGIS SQL or GML format)
# Usage: make load-dataset ZIP_FILE=data/stedsnavn/file.zip TABLE=stedsnavn SRID=25833
# Note: Uses stiflyt_updater by default (requires write permissions)
# OSM imports automatically use optimized settings
load-dataset: ensure-db $(VENV)
	@if [ -z "$(ZIP_FILE)" ]; then \
		echo "Feil: ZIP_FILE må angis. Eksempel: make load-dataset ZIP_FILE=data/stedsnavn/file.zip TABLE=stedsnavn"; \
		exit 1; \
	fi
	@mkdir -p $(OSM_FLAT_NODES_DIR) || true
	@OSM2PGSQL_ARGS="--cache $(OSM_CACHE_MB) --number-processes $(OSM_NUM_PROCESSES) --flat-nodes $(OSM_FLAT_NODES_FILE)" \
		PGUSER=$${PGUSER:-stiflyt_updater} $(PYTHON) ./scripts/load_dataset.py "$(ZIP_FILE)" $(PGDATABASE) $(TABLE) $(SRID) --drop-tables

# Update all datasets from config file (cron-friendly)
# This downloads updates and reloads data, replacing old tables
# OSM imports are optimized with cache, parallel processing, and flat nodes
update-datasets: $(VENV)
	@echo "==> OSM import optimizations:"
	@echo "  Cache: $(OSM_CACHE_MB)MB"
	@echo "  Parallel processes: $(OSM_NUM_PROCESSES)"
	@echo "  Flat nodes file: $(OSM_FLAT_NODES_FILE)"
	@echo "  Use slim mode: $(OSM_USE_SLIM)"
	@mkdir -p $(OSM_FLAT_NODES_DIR) || true
	@# Note: Flat nodes file can be very large (18GB+ for Norway). If import fails with "Could not resize file",
	@# try without flat nodes: OSM2PGSQL_ARGS="--cache $(OSM_CACHE_MB) --number-processes $(OSM_NUM_PROCESSES)" make update-datasets
	@# Or try without slim mode: OSM_USE_SLIM=false make update-datasets
	@OSM_USE_SLIM=$(OSM_USE_SLIM) OSM2PGSQL_ARGS="--cache $(OSM_CACHE_MB) --number-processes $(OSM_NUM_PROCESSES) --flat-nodes $(OSM_FLAT_NODES_FILE)" \
		PGUSER=$${PGUSER:-stiflyt_updater} $(PYTHON) scripts/update_datasets.py $(or $(CONFIG_FILE),datasets.yaml) $(PGDATABASE)

# Check database status and health
db-status: $(VENV)
	@$(PYTHON) scripts/db_status.py $(PGDATABASE)

# Inspect database schema (tables, columns, indexes, SRIDs)
inspect-db: $(VENV)
	@if [ "$(PGHOST)" = "localhost" ] || [ -z "$(PGHOST)" ]; then \
		PGUSER=$(PGUSER) PGPASSWORD=$(PGPASSWORD) $(PYTHON) scripts/inspect_db.py $(PGDATABASE) --tables; \
	else \
		PGUSER=$(PGUSER) PGHOST=$(PGHOST) PGPORT=$(PGPORT) PGPASSWORD=$(PGPASSWORD) $(PYTHON) scripts/inspect_db.py $(PGDATABASE) --tables; \
	fi

# Run database migrations (creates indexes, updates statistics, etc.)
# Migrations run automatically after update-datasets, but can be run manually
# Note: build-links runs automatically before migration 003 if needed
run-migrations: $(VENV)
	@PGUSER=$${PGUSER:-stiflyt_updater} $(PYTHON) scripts/run_migrations.py $(PGDATABASE)

# Verify that migration indexes were created successfully
verify-migration: $(VENV)
	@$(PYTHON) scripts/verify_migration.py $(PGDATABASE)

# Build links from segments and anchor nodes
# Creates links and link_segments tables with topology
# This is a heavy operation and should be run after data import
# Note: build-links runs automatically via run-migrations before migration 003
# This target is for manual execution or when you need to rebuild links separately
build-links: $(VENV)
	@echo "==> Building links (this may take a while)..."
	@$(PYTHON) scripts/build_links.py --log-dir ./logs || (echo "✗ build-links failed - check logs/build_links_*.log" && exit 1)

# Cron-friendly full refresh: download/update datasets and run migrations
# Migrations automatically run build-links before migration 003 if needed
# Each step runs independently - if one fails, the next still runs
# Check logs/ for detailed output
cron-update: $(VENV)
	@echo "=== Starting cron-update pipeline ==="
	@echo "Step 1: Update datasets..."
	@$(MAKE) update-datasets || (echo "⚠ update-datasets failed - continuing with migrations" && true)
	@echo ""
	@echo "Step 2: Build links (required for views)..."
	@$(MAKE) build-links || (echo "⚠ build-links failed - migrations will try again automatically" && true)
	@echo ""
	@echo "Step 3: Run migrations..."
	@$(MAKE) run-migrations || (echo "✗ run-migrations failed" && exit 1)
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
test: $(VENV)
	@echo "==> Running tests..."
	@$(PIP) install -e ".[dev]" > /dev/null 2>&1
	@$(VENV)/bin/python -m pytest tests/ -v

# Show PostgreSQL optimization recommendations for OSM imports
show-pg-optimizations:
	@echo "==> PostgreSQL Optimization Recommendations for OSM Import"
	@echo ""
	@echo "For optimal OSM import performance, configure PostgreSQL with these settings:"
	@echo ""
	@echo "Add to postgresql.conf (typically /etc/postgresql/*/main/postgresql.conf):"
	@echo ""
	@echo "  # Memory settings for bulk operations"
	@echo "  maintenance_work_mem = 1GB"
	@echo "  work_mem = 256MB"
	@echo ""
	@echo "  # Checkpoint settings - spread checkpoints over longer period"
	@echo "  checkpoint_completion_target = 0.9"
	@echo "  max_wal_size = 2GB"
	@echo "  checkpoint_timeout = 30min"
	@echo ""
	@echo "  # WAL settings for bulk writes"
	@echo "  wal_buffers = 16MB"
	@echo ""
	@echo "  # Disable synchronous commit for faster writes (imports only)"
	@echo "  synchronous_commit = off"
	@echo ""
	@echo "After making changes:"
	@echo "  sudo systemctl reload postgresql"
	@echo ""
	@echo "See POSTGRESQL_CONF_OPTIMIZATIONS.md for detailed instructions."
	@echo ""
	@echo "Current OSM import settings:"
	@echo "  Cache: $(OSM_CACHE_MB)MB"
	@echo "  Parallel processes: $(OSM_NUM_PROCESSES)"
	@echo "  Flat nodes file: $(OSM_FLAT_NODES_FILE)"
	@echo "  Use slim mode: $(OSM_USE_SLIM)"
	@echo ""
	@echo "To customize, set environment variables:"
	@echo "  OSM_CACHE_MB=8000 OSM_NUM_PROCESSES=4 make update-datasets"
	@echo "  OSM_USE_SLIM=false make update-datasets  # Disable --slim mode (uses more RAM, may avoid temp table issues)"
