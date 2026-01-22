# Database Migrations

This repo uses SQL migrations to prepare imported datasets for API queries.
Migrations run after data import and are designed to be idempotent.

## How migrations run

- **Automatic:** `update-datasets` triggers migrations after a successful import.
- **Manual:** `make run-migrations` or `python3 scripts/run_migrations.py [database]`.
- **Order:** alphabetical by filename in `migrations/`.

## Prerequisites

- PostGIS installed (migrations use `GEOMETRY` and `ST_*` functions).
- Roles configured once via `migrations/000_setup_roles.sql`.

## One-time role setup

Run as a superuser (or via the Makefile target) once per database:

```bash
sudo -u postgres psql -d <db_name> -f migrations/000_setup_roles.sql
make setup-roles
```

Then set passwords for the login roles:

```sql
ALTER ROLE stiflyt_updater WITH PASSWORD 'your_secure_password';
ALTER ROLE stiflyt_reader WITH PASSWORD 'your_secure_password';
```

## Migration catalog

### `000_setup_roles.sql`

Creates roles and grants needed for updates and read-only access:

- `stiflyt_owner` (NOLOGIN) owns objects.
- `stiflyt_updater` runs imports and migrations.
- `stiflyt_reader` is read-only for the backend.
- Helper functions `grant_schema_privileges()` and
  `grant_schema_privileges_for_prefix()` grant ownership and privileges on new
  schemas.

### `001_add_fotrute_indexes.sql`

Adds critical indexes for turrutebasen queries in the dynamic
`turogfriluftsruter_*` schema:

- GIST on `fotrute.senterlinje`.
- BTREE on `fotruteinfo.fotrute_fk`, `rutenummer`, `vedlikeholdsansvarlig`.
- `ANALYZE` for planner stats.

### `002_build_topology.sql`

Builds a topology layer in the turrutebasen schema:

- `nodes` table from `fotrute` endpoints (with hash-based matching).
- `node_degree` and `anchor_nodes` materialized views.
- Indexes to speed up joins and spatial queries.

### `003_add_link_ruteinfo_view.sql`

Creates views that join `links` with `fotruteinfo`:

- `link_ruteinfo` (one row per link/segment).
- `links_with_routes` (one row per link with aggregated route info).

Depends on `links` being created by the build-links step.

### `005_create_stable_views.sql`

Creates a fixed `stiflyt` schema with views pointing to the latest dynamic
schemas, so the backend can rely on stable names like `stiflyt.fotrute`.

Includes views for:

- Turrutebasen tables and views.
- Matrikkel tables (if present).
- Stedsnavn tables from `public`.

### `006_add_static_indexes.sql`

Adds spatial GIST indexes for static datasets:

- `matrikkeleneiendomskartteig_*.teig(<geom>)`
- `public.sted_posisjon(<geom>)`

## Troubleshooting

- Run migrations manually with increased verbosity: `make run-migrations`.
- If a migration mentions missing schemas, verify the dataset import completed.
- If views or tables are missing, rerun migrations after fixing the upstream
  build/import step.
