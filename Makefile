SHELL := /bin/bash

DATA_DIR := data/matrikkel
PGDATABASE ?= matrikkel
PGHOST ?= localhost
PGPORT ?= 5432
PGUSER ?= stiflyt_updater
PGADMIN_USER ?= postgres

# Virtual environment
VENV := venv
PYTHON := $(VENV)/bin/python3
PIP := $(VENV)/bin/pip

.PHONY: dependencies help create-db refresh-turrutebasen refresh-static \
	db-migrate-operational db-migrate-changeset db-migrate-all run-migrations inspect-db

help:
	@echo "stiflyt-db Makefile"
	@echo ""
	@echo "Common targets:"
	@echo "  make dependencies   Install system deps (Debian/Ubuntu)"
	@echo "  make create-db      Create database + PostGIS extension"
	@echo "  make refresh-turrutebasen Daily turrutebasen refresh (data + migrations + ankernavn)"
	@echo "  make refresh-static Monthly static refresh"
	@echo "  make db-migrate-operational Run operational schema migration"
	@echo "  make db-migrate-changeset  Run changeset schema migration"
	@echo "  make db-migrate-all        Run operational then changeset"
	@echo "  make run-migrations       Apply numbered SQL migrations from migrations/"
	@echo "  make inspect-db           List schemas, tables, views + access"
	@echo ""
	@echo "Variables:"
	@echo "  PGDATABASE=$(PGDATABASE) PGHOST=$(PGHOST) PGPORT=$(PGPORT)"
	@echo "  PGUSER=$(PGUSER) PGADMIN_USER=$(PGADMIN_USER)"
	@echo ""

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
	@echo "  3. Månedlig: make refresh-static"
	@echo "  4. Daglig: make refresh-turrutebasen"
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
		PGPASSWORD=$(PGPASSWORD) psql -U $(PGADMIN_USER) -d $(PGDATABASE) -t -A \
			-c "SELECT extname FROM pg_extension WHERE extname = 'postgis';" 2>&1 | grep -q postgis \
			&& echo "  ✓ PostGIS extension allerede aktivert" \
			|| (PGPASSWORD=$(PGPASSWORD) psql -U $(PGUSER) -d $(PGDATABASE) \
				-c "CREATE EXTENSION IF NOT EXISTS postgis;" > /dev/null 2>&1 && echo "  ✓ PostGIS extension aktivert" || \
				(echo "  ⚠ Kunne ikke aktivere PostGIS extension som $(PGUSER)"; \
				 echo "     Prøver som postgres superuser..."; \
				 sudo -u postgres psql -d $(PGDATABASE) -c "CREATE EXTENSION IF NOT EXISTS postgis;" > /dev/null 2>&1 && echo "  ✓ PostGIS extension aktivert (som postgres)" || \
				 (echo "  ✗ Kunne ikke aktivere PostGIS extension"; \
				  echo "     Kjør manuelt: sudo -u postgres psql -d $(PGDATABASE) -c 'CREATE EXTENSION postgis;'"; \
				  exit 1))); \
	else \
		PGPASSWORD=$(PGPASSWORD) psql $$PSQL_HOST $$PSQL_PORT -U $(PGADMIN_USER) -d $(PGDATABASE) -t -A \
			-c "SELECT extname FROM pg_extension WHERE extname = 'postgis';" 2>&1 | grep -q postgis \
			&& echo "  ✓ PostGIS extension allerede aktivert" \
			|| (PGPASSWORD=$(PGPASSWORD) psql $$PSQL_HOST $$PSQL_PORT -U $(PGUSER) -d $(PGDATABASE) \
				-c "CREATE EXTENSION IF NOT EXISTS postgis;" > /dev/null 2>&1 && echo "  ✓ PostGIS extension aktivert" || \
				(echo "  ✗ Kunne ikke aktivere PostGIS extension"; \
				 echo "     Bruk superuser: PGUSER=postgres PGPASSWORD=password make create-db"; \
				 exit 1)); \
	fi

db-migrate-operational: $(VENV)
	@echo "==> Run operational schema migration ..."
	@$(PYTHON) scripts/run_operational_migration.py

db-migrate-changeset: $(VENV)
	@echo "==> Run changeset schema migration ..."
	@$(PYTHON) scripts/run_changeset_migration.py

db-migrate-all: db-migrate-operational db-migrate-changeset
	@echo "==> Operational + changeset migrations complete."

run-migrations: $(VENV)
	@echo "==> Apply numbered migrations from migrations/ to $(PGDATABASE) ..."
	@$(PYTHON) scripts/run_migrations.py $(PGDATABASE)

inspect-db: $(VENV)
	@echo "==> Inspect database schemas, tables, and access ..."
	@$(PYTHON) scripts/inspect_db.py --schemas --tables --access

refresh-turrutebasen: $(VENV)
	@echo "==> Refresh turrutebasen (daily) ..."
	@PGUSER=stiflyt_updater $(PYTHON) scripts/refresh_swap.py $(PGDATABASE) --config-file datasets_turrutebasen_only.yaml
	@echo "==> Synkroniserer ankernavn (endpoint_names) ..."
	@PGUSER=stiflyt_updater $(PYTHON) scripts/sync_endpoint_names_anchors.py --tolerance 1.0
	@echo "==> Populerer geometri for endpoint_names ..."
	@PGUSER=stiflyt_updater $(PYTHON) scripts/populate_endpoint_geometries.py
	@echo "✓ Turrutebasen-oppdatering ferdig"

refresh-static: $(VENV)
	@echo "==> Refresh static datasets (monthly) ..."
	@PGUSER=stiflyt_updater $(PYTHON) scripts/refresh_swap.py $(PGDATABASE) --config-file datasets.yaml

# Ensure virtual environment exists
$(VENV):
	@echo "==> Oppretter virtual environment..."
	@python3 -m venv $(VENV)
	@$(PIP) install --upgrade pip setuptools wheel > /dev/null 2>&1
	@$(PIP) install -e . > /dev/null 2>&1
	@echo "  ✓ Virtual environment klar"


