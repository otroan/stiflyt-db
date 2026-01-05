#!/usr/bin/env python3
"""
Unified dataset loader for PostGIS databases.

This script automatically detects the format (PostGIS SQL or GML) and loads
the dataset accordingly. It replaces old data automatically.

Usage:
    python3 scripts/load_dataset.py <zip_file> <database_name> [table_name] [target_srid] [--drop-tables]

Environment variables:
    PGHOST       - PostgreSQL host (default: localhost)
    PGPORT       - PostgreSQL port (default: 5432)
    PGUSER       - PostgreSQL user (default: current user)
    PGPASSWORD   - PostgreSQL password (if needed)
    PGDATABASE   - Database name (can also be passed as argument)
    TARGET_SRID  - Target SRID for transformation (default: 25833)
    OSM2PGSQL_ARGS    - Extra osm2pgsql args (e.g. "--hstore --cache 8000")
    OSM2PGSQL_SCHEMA  - Target schema for osm2pgsql (default: public)
    OSM2PGSQL_PREFIX  - Table prefix for osm2pgsql (default: planet_osm)
    OSM2PGSQL_STYLE   - Path to osm2pgsql style file (optional)
"""

import os
import sys
import zipfile
import subprocess
import re
import argparse
import shlex
from pathlib import Path
from typing import Optional, List, Tuple


def get_db_connection_params() -> dict:
    """Get database connection parameters from environment or defaults.

    For localhost, uses None for host to enable Unix socket (peer auth).
    """
    host = os.environ.get('PGHOST', 'localhost')
    # Use None for localhost to enable Unix socket connection (peer auth)
    if host == 'localhost' or host == '127.0.0.1':
        host = None

    return {
        'host': host,
        'port': os.environ.get('PGPORT', '5432') if host else None,
        'user': os.environ.get('PGUSER', os.environ.get('USER', 'postgres')),
        'password': os.environ.get('PGPASSWORD', ''),
        'database': os.environ.get('PGDATABASE', ''),
    }


def detect_format(extract_dir: Path) -> Tuple[str, List[Path]]:
    """Detect dataset format by examining extracted files.

    Returns:
        Tuple of (format_type, list of files to process)
    """
    # Check for SQL files (PostGIS format)
    sql_files = list(extract_dir.rglob('*.sql'))
    if sql_files:
        return ('PostGIS', sql_files)

    # Check for GML files
    gml_files = list(extract_dir.rglob('*.gml'))
    if gml_files:
        return ('GML', gml_files)

    # Check for FileGDB datasets (.gdb directories or .gdbtable files)
    gdb_dirs = set(p for p in extract_dir.rglob('*.gdb') if p.is_dir())
    gdbtable_parents = set(p.parent for p in extract_dir.rglob('*.gdbtable'))
    gdb_dirs |= gdbtable_parents
    if gdb_dirs:
        return ('FGDB', sorted(gdb_dirs))

    # Check for OSM PBF files
    osm_files = list(extract_dir.rglob('*.osm.pbf'))
    if osm_files:
        return ('OSM', osm_files)

    # Could not detect format
    return (None, [])


def detect_format_from_zip(zip_path: Path) -> Tuple[Optional[str], List[str]]:
    """Detect dataset format by examining ZIP contents without extracting.

    Returns:
        Tuple of (format_type, list of file paths in ZIP)
    """
    if zip_path is None:
        return None, []

    sql_files = []
    gml_files = []
    gdb_entries = []
    osm_files = []

    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for name in zip_ref.namelist():
                if name.endswith('.sql'):
                    sql_files.append(name)
                elif name.endswith('.gml'):
                    gml_files.append(name)
                elif '.gdb/' in name or name.endswith('.gdbtable'):
                    gdb_entries.append(name)
                elif name.endswith('.osm.pbf'):
                    osm_files.append(name)
    except zipfile.BadZipFile:
        # Check if it's a standalone OSM PBF file (not a ZIP)
        if zip_path.suffixes == ['.osm', '.pbf'] or zip_path.name.endswith('.osm.pbf'):
            return ('OSM', [str(zip_path)])
        return None, []

    if sql_files:
        return ('PostGIS', sql_files)
    elif gml_files:
        return ('GML', gml_files)
    elif gdb_entries:
        return ('FGDB', gdb_entries)
    elif osm_files:
        return ('OSM', osm_files)
    else:
        # Check if the file itself is an OSM PBF (not a ZIP)
        if zip_path.suffixes == ['.osm', '.pbf'] or zip_path.name.endswith('.osm.pbf'):
            return ('OSM', [str(zip_path)])
        return None, []


def extract_zip(zip_path: Path, extract_dir: Path) -> bool:
    """Extract ZIP file."""
    print(f"==> Pakker ut {zip_path.name} ...")
    try:
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        print("  ✓ Utpakket")
        return True
    except zipfile.BadZipFile:
        print(f"  ✗ Feil: {zip_path} er ikke en gyldig ZIP-fil", file=sys.stderr)
        return False
    except Exception as e:
        print(f"  ✗ Feil ved utpakking: {e}", file=sys.stderr)
        return False


def ensure_postgis_extension(db_params: dict) -> bool:
    """Ensure PostGIS extension is enabled in the database."""
    env = os.environ.copy()
    if db_params.get('password'):
        env['PGPASSWORD'] = db_params['password']

    cmd = ['psql', '-v', 'ON_ERROR_STOP=1']
    if db_params.get('host'):
        cmd.extend(['-h', db_params['host']])
    if db_params.get('port'):
        cmd.extend(['-p', str(db_params['port'])])
    cmd.extend([
        '-U', db_params['user'],
        '-d', db_params['database'],
        '-c', 'CREATE EXTENSION IF NOT EXISTS postgis;',
        '-q'
    ])

    try:
        subprocess.run(cmd, env=env, capture_output=True, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Feil ved aktivering av PostGIS extension: {e}", file=sys.stderr)
        return False
    except FileNotFoundError:
        print("Feil: psql ikke funnet. Er PostgreSQL installert?", file=sys.stderr)
        return False


def extract_schema_prefix_from_sql(content: str) -> Optional[str]:
    """Extract schema prefix from SQL content.

    Kartverket SQL files use schemas like: turogfriluftsruter_b9b25c7668da494b9894d492fc35290d
    This function extracts the prefix part (before the hash): turogfriluftsruter

    The hash is typically 32 hexadecimal characters after an underscore.

    Returns:
        Schema prefix (e.g., 'turogfriluftsruter') or None if not found
    """
    # Match CREATE TABLE schema.table or CREATE SCHEMA schema
    # Schema names follow pattern: prefix_32hexchars
    # Examples: turogfriluftsruter_b9b25c7668da494b9894d492fc35290d
    #           matrikkeleneiendomskartteig_d56c3a44c39b43ae8081f08a97a28c7d
    pattern = r'(?:CREATE\s+(?:TABLE|SCHEMA)\s+(?:IF\s+NOT\s+EXISTS\s+)?|SET\s+search_path\s+TO\s+)(\w+)_[a-f0-9]{32}'
    matches = re.findall(pattern, content, re.IGNORECASE)
    if matches:
        # Return the first matching prefix
        return matches[0]

    # Alternative: try to find schema names in CREATE TABLE statements
    pattern2 = r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)_[a-f0-9]{32}\.'
    matches2 = re.findall(pattern2, content, re.IGNORECASE)
    if matches2:
        return matches2[0]

    return None


def extract_table_names_from_sql(sql_file: Path) -> Tuple[List[str], Optional[str]]:
    """Extract table names and schema prefix from SQL file by looking for CREATE TABLE statements.

    Returns:
        Tuple of (table_names, schema_prefix)
    """
    table_names = []
    schema_prefix = None
    try:
        with open(sql_file, 'r', encoding='utf-8') as f:
            content = f.read()

            # Extract schema prefix
            schema_prefix = extract_schema_prefix_from_sql(content)

            # Match CREATE TABLE schema.table_name or CREATE TABLE table_name
            pattern = r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:(\w+)\.)?(\w+)'
            matches = re.findall(pattern, content, re.IGNORECASE)
            for schema, table in matches:
                if schema:
                    table_names.append(f"{schema}.{table}")
                else:
                    table_names.append(table)
    except Exception:
        pass
    return table_names, schema_prefix


def drop_schemas_by_prefix(db_params: dict, schema_prefix: str) -> bool:
    """Drop all schemas matching the given prefix pattern.

    This ensures old schemas with different hashes are removed.
    Example: drops all schemas matching 'turogfriluftsruter_*'

    Args:
        db_params: Database connection parameters
        schema_prefix: Schema prefix to match (e.g., 'turogfriluftsruter')

    Returns:
        True if successful, False otherwise
    """
    env = os.environ.copy()
    if db_params.get('password'):
        env['PGPASSWORD'] = db_params['password']

    # Build psql command
    cmd = ['psql', '-v', 'ON_ERROR_STOP=1']
    if db_params.get('host'):
        cmd.extend(['-h', db_params['host']])
    if db_params.get('port'):
        cmd.extend(['-p', str(db_params['port'])])
    cmd.extend(['-U', db_params['user'], '-d', db_params['database'], '-q', '-t'])

    # Query for schemas matching the prefix pattern
    # Exclude system schemas
    find_schemas_sql = f"""
        {role_preamble_sql()}
        SELECT nspname
        FROM pg_namespace
        WHERE nspname LIKE '{schema_prefix}_%'
        AND nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
        ORDER BY nspname;
        {role_reset_sql()}
    """

    try:
        result = subprocess.run(
            cmd,
            input=find_schemas_sql,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )

        # Parse schema names from output (one per line, trimmed)
        schema_names = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]

        if not schema_names:
            return True  # No schemas to drop

        print(f"  Fant {len(schema_names)} schema(er) med prefix '{schema_prefix}': {', '.join(schema_names)}")

        # Drop each schema (CASCADE will drop all tables in the schema)
        drop_sql = (
            role_preamble_sql()
            + '; '.join([f'DROP SCHEMA IF EXISTS "{name}" CASCADE' for name in schema_names])
            + ';'
            + role_reset_sql()
        )

        # Remove -t flag for drop command (we want to see output)
        drop_cmd = cmd[:-1]  # Remove -t flag

        drop_result = subprocess.run(
            drop_cmd,
            input=drop_sql,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )

        print(f"  ✓ Slettet {len(schema_names)} schema(er)")
        return True

    except subprocess.CalledProcessError as e:
        print(f"  ⚠ Kunne ikke slette schemas: {e.stderr}", file=sys.stderr)
        return False


def drop_tables(db_params: dict, table_names: List[str]) -> bool:
    """Drop tables if they exist."""
    env = os.environ.copy()
    if db_params.get('password'):
        env['PGPASSWORD'] = db_params['password']

    cmd = ['psql', '-v', 'ON_ERROR_STOP=1']
    if db_params.get('host'):
        cmd.extend(['-h', db_params['host']])
    if db_params.get('port'):
        cmd.extend(['-p', str(db_params['port'])])
    cmd.extend(['-U', db_params['user'], '-d', db_params['database'], '-q'])

    # Build SQL to drop tables
    drop_sql = (
        role_preamble_sql()
        + '; '.join([f'DROP TABLE IF EXISTS {name} CASCADE' for name in table_names])
        + ';'
        + role_reset_sql()
    )

    try:
        result = subprocess.run(
            cmd,
            input=drop_sql,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"Advarsel: Kunne ikke slette tabeller: {e.stderr}", file=sys.stderr)
        return False


def sanitize_identifier(name: str) -> str:
    """Sanitize a string to be a valid PostgreSQL identifier (lowercase, alnum + underscore)."""
    return re.sub(r'[^a-zA-Z0-9_]', '_', name).lower()


def role_preamble_sql() -> str:
    """Return SQL that requires SET ROLE stiflyt_owner."""
    return "SET ROLE stiflyt_owner;\n"


def role_reset_sql() -> str:
    """Return SQL that resets the active role."""
    return "RESET ROLE;\n"


def check_owner_membership(db_params: dict) -> bool:
    """Verify current_user can SET ROLE stiflyt_owner and role exists."""
    env = os.environ.copy()
    if db_params.get('password'):
        env['PGPASSWORD'] = db_params['password']

    cmd = ['psql', '-v', 'ON_ERROR_STOP=1', '-t', '-A', '-F', '|']
    if db_params.get('host'):
        cmd.extend(['-h', db_params['host']])
    if db_params.get('port'):
        cmd.extend(['-p', str(db_params['port'])])
    cmd.extend(['-U', db_params['user'], '-d', db_params['database']])
    cmd.extend(['-c', """
        SELECT
            current_user,
            EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stiflyt_owner') AS owner_exists,
            pg_has_role(current_user, 'stiflyt_owner', 'member') AS is_member;
    """])

    try:
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False

    line = result.stdout.strip()
    if not line:
        return False
    parts = line.split('|')
    if len(parts) != 3:
        return False
    current_user, owner_exists, is_member = parts
    if owner_exists != 't':
        print("✗ Role stiflyt_owner does not exist", file=sys.stderr)
        print("  Fix (as superuser):", file=sys.stderr)
        print("  psql -d <db> -f migrations/000_setup_roles.sql", file=sys.stderr)
        return False
    if is_member != 't':
        print("✗ Current role is not a member of stiflyt_owner", file=sys.stderr)
        print(f"  current_user: {current_user}", file=sys.stderr)
        print("  Fix (as superuser):", file=sys.stderr)
        print(f"  GRANT stiflyt_owner TO {current_user};", file=sys.stderr)
        return False
    return True


def grant_privileges_for_schema(db_params: dict, schema_name: Optional[str]) -> bool:
    """Grant privileges for a specific schema using grant_schema_privileges()."""
    if not schema_name:
        return True

    env = os.environ.copy()
    if db_params.get('password'):
        env['PGPASSWORD'] = db_params['password']

    cmd = ['psql', '-v', 'ON_ERROR_STOP=1']
    if db_params.get('host'):
        cmd.extend(['-h', db_params['host']])
    if db_params.get('port'):
        cmd.extend(['-p', str(db_params['port'])])
    cmd.extend(['-U', db_params['user'], '-d', db_params['database'], '-q'])

    schema_literal = schema_name.replace("'", "''")
    sql = f"""
    {role_preamble_sql()}
    SELECT grant_schema_privileges('{schema_literal}');
    {role_reset_sql()}
    """

    try:
        subprocess.run(
            cmd,
            input=sql,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"  ⚠ Kunne ikke sette privileges for schema {schema_name}: {e.stderr}", file=sys.stderr)
        return False


def run_psql_stream(
    db_params: dict,
    stream,
    pre_sql: str = "",
    post_sql: str = ""
) -> subprocess.CompletedProcess:
    """Run psql with streamed input, optionally prepending/appending SQL."""
    env = os.environ.copy()
    if db_params.get('password'):
        env['PGPASSWORD'] = db_params['password']

    cmd = ['psql', '-v', 'ON_ERROR_STOP=1']
    if db_params.get('host'):
        cmd.extend(['-h', db_params['host']])
    if db_params.get('port'):
        cmd.extend(['-p', str(db_params['port'])])
    cmd.extend(['-U', db_params['user'], '-d', db_params['database']])

    process = subprocess.Popen(
        cmd,
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    try:
        if pre_sql:
            process.stdin.write(pre_sql.encode('utf-8'))
            process.stdin.flush()
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            process.stdin.write(chunk)
            process.stdin.flush()
        if post_sql:
            process.stdin.write(post_sql.encode('utf-8'))
            process.stdin.flush()
        process.stdin.close()
    except (BrokenPipeError, OSError) as e:
        # Process may have terminated early
        process.kill()
        stdout, stderr = process.communicate()
        raise subprocess.CalledProcessError(
            process.returncode if process.returncode else 1,
            cmd,
            output=stdout,
            stderr=stderr or (str(e).encode() if isinstance(e, Exception) else b'')
        )
    except Exception as e:
        process.kill()
        stdout, stderr = process.communicate()
        error_msg = f"Error streaming to psql: {e}"
        raise subprocess.CalledProcessError(
            process.returncode if process.returncode else 1,
            cmd,
            output=stdout,
            stderr=stderr or error_msg.encode() if stderr else error_msg.encode()
        )

    stdout, stderr = process.communicate()
    return subprocess.CompletedProcess(cmd, process.returncode, stdout, stderr)

def ensure_schema_exists(db_params: dict, schema: str) -> bool:
    """Create schema if not exists and grant privileges to roles.

    After creating a schema, this function also grants privileges to:
    - stiflyt_updater: Full write access
    - stiflyt_reader: Read-only access

    Uses the grant_schema_privileges() function (created by migration 000_setup_roles.sql).
    """
    env = os.environ.copy()
    if db_params.get('password'):
        env['PGPASSWORD'] = db_params['password']

    cmd = ['psql', '-v', 'ON_ERROR_STOP=1']
    if db_params.get('host'):
        cmd.extend(['-h', db_params['host']])
    if db_params.get('port'):
        cmd.extend(['-p', str(db_params['port'])])
    cmd.extend(['-U', db_params['user'], '-d', db_params['database'], '-q'])

    # Create schema and grant privileges
    # Use the helper function if it exists (from migration 000_setup_roles.sql)
    # Also ensure stiflyt_owner is the owner (required for ogr2ogr)
    sql = f"""
    {role_preamble_sql()}
    CREATE SCHEMA IF NOT EXISTS {schema};

    -- Ensure stiflyt_owner is the owner (required for ogr2ogr operations)
    ALTER SCHEMA {schema} OWNER TO stiflyt_owner;

    -- Grant privileges using helper function
    SELECT grant_schema_privileges('{schema}');
    {role_reset_sql()}
    """

    try:
        result = subprocess.run(
            cmd,
            input=sql,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"  ⚠ Kunne ikke opprette schema {schema}: {e.stderr}", file=sys.stderr)
        return False


def grant_privileges_for_schema_prefix(db_params: dict, schema_prefix: Optional[str]) -> bool:
    """Grant privileges for all schemas matching a prefix.

    Uses grant_schema_privileges_for_prefix() (created by migration 000_setup_roles.sql).
    """
    if not schema_prefix:
        return True

    env = os.environ.copy()
    if db_params.get('password'):
        env['PGPASSWORD'] = db_params['password']

    cmd = ['psql', '-v', 'ON_ERROR_STOP=1']
    if db_params.get('host'):
        cmd.extend(['-h', db_params['host']])
    if db_params.get('port'):
        cmd.extend(['-p', str(db_params['port'])])
    cmd.extend(['-U', db_params['user'], '-d', db_params['database'], '-q'])

    prefix_literal = schema_prefix.replace("'", "''")
    sql = f"""
    {role_preamble_sql()}
    SELECT grant_schema_privileges_for_prefix('{prefix_literal}');
    {role_reset_sql()}
    """

    try:
        subprocess.run(
            cmd,
            input=sql,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"  ⚠ Kunne ikke sette privileges for schema prefix {schema_prefix}: {e.stderr}", file=sys.stderr)
        return False


def move_schema_objects(db_params: dict, source_schema: str, target_schema: str = 'public') -> bool:
    """Move tables and sequences from source_schema to target_schema, dropping conflicts.
    Also sets ownership to stiflyt_owner after moving."""
    env = os.environ.copy()
    if db_params.get('password'):
        env['PGPASSWORD'] = db_params['password']

    cmd = ['psql']
    if db_params.get('host'):
        cmd.extend(['-h', db_params['host']])
    if db_params.get('port'):
        cmd.extend(['-p', str(db_params['port'])])
    cmd.extend(['-U', db_params['user'], '-d', db_params['database'], '-q'])

    sql = f"""
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stiflyt_owner') THEN
        BEGIN
            EXECUTE 'SET ROLE stiflyt_owner';
        EXCEPTION WHEN insufficient_privilege THEN
            RAISE NOTICE 'Could not SET ROLE stiflyt_owner (not member)';
        END;
    END IF;
END $$;

DO $$
DECLARE
    r record;
BEGIN
    -- Move tables
    FOR r IN SELECT tablename FROM pg_tables WHERE schemaname = '{source_schema}'
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE;', '{target_schema}', r.tablename);
        EXECUTE format('ALTER TABLE %I.%I SET SCHEMA %I;', '{source_schema}', r.tablename, '{target_schema}');
        -- Set ownership to stiflyt_owner
        BEGIN
            EXECUTE format('ALTER TABLE %I.%I OWNER TO stiflyt_owner', '{target_schema}', r.tablename);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Could not set owner for table %.%: %', '{target_schema}', r.tablename, SQLERRM;
        END;
    END LOOP;

    -- Move sequences
    FOR r IN SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = '{source_schema}'
    LOOP
        EXECUTE format('DROP SEQUENCE IF EXISTS %I.%I CASCADE;', '{target_schema}', r.sequence_name);
        EXECUTE format('ALTER SEQUENCE %I.%I SET SCHEMA %I;', '{source_schema}', r.sequence_name, '{target_schema}');
        -- Set ownership to stiflyt_owner
        BEGIN
            EXECUTE format('ALTER SEQUENCE %I.%I OWNER TO stiflyt_owner', '{target_schema}', r.sequence_name);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Could not set owner for sequence %.%: %', '{target_schema}', r.sequence_name, SQLERRM;
        END;
    END LOOP;
END$$;

DROP SCHEMA IF EXISTS {source_schema} CASCADE;

RESET ROLE;
"""

    try:
        subprocess.run(
            cmd,
            input=sql,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"  ⚠ Kunne ikke flytte objekter fra schema {source_schema}: {e.stderr}", file=sys.stderr)
        return False


def analyze_tables(db_params: dict, tables: Optional[List[str]] = None, schemas: Optional[List[str]] = None, use_vacuum: bool = False) -> bool:
    """Run ANALYZE or VACUUM ANALYZE on tables to update planner statistics.

    Args:
        db_params: Database connection parameters
        tables: List of table names (schema.table or just table for public schema)
        schemas: List of schema names to analyze all tables in (if tables not specified)
        use_vacuum: If True, use VACUUM ANALYZE (slower but also reclaims space)

    Returns:
        True if successful, False otherwise
    """
    env = os.environ.copy()
    if db_params.get('password'):
        env['PGPASSWORD'] = db_params['password']

    cmd = ['psql']
    if db_params.get('host'):
        cmd.extend(['-h', db_params['host']])
    if db_params.get('port'):
        cmd.extend(['-p', str(db_params['port'])])
    cmd.extend(['-U', db_params['user'], '-d', db_params['database'], '-q'])

    if tables:
        # Analyze specific tables
        analyze_cmd = 'VACUUM ANALYZE' if use_vacuum else 'ANALYZE'
        print(f"==> Kjører {analyze_cmd} på {len(tables)} tabell(er) ...")

        for table in tables:
            sql = f"{analyze_cmd} {table};"
            try:
                subprocess.run(
                    cmd,
                    input=sql,
                    env=env,
                    capture_output=True,
                    text=True,
                    check=True
                )
            except subprocess.CalledProcessError as e:
                print(f"  ⚠ Kunne ikke kjøre {analyze_cmd} på {table}: {e.stderr}", file=sys.stderr)
                # Continue with other tables

        print(f"  ✓ {analyze_cmd} fullført")
        return True

    elif schemas:
        # Analyze all tables in specified schemas
        analyze_cmd = 'VACUUM ANALYZE' if use_vacuum else 'ANALYZE'
        print(f"==> Kjører {analyze_cmd} på alle tabeller i {len(schemas)} schema(er) ...")

        schema_list = ", ".join([f"'{s}'" for s in schemas])
        sql = f"""
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname IN ({schema_list})
          AND schemaname NOT IN ('pg_catalog', 'information_schema')
    LOOP
        EXECUTE format('{analyze_cmd} %I.%I;', r.schemaname, r.tablename);
    END LOOP;
END$$;
"""
        try:
            subprocess.run(
                cmd,
                input=sql,
                env=env,
                capture_output=True,
                text=True,
                check=True
            )
            print(f"  ✓ {analyze_cmd} fullført")
            return True
        except subprocess.CalledProcessError as e:
            print(f"  ⚠ Kunne ikke kjøre {analyze_cmd}: {e.stderr}", file=sys.stderr)
            return False

    else:
        # Analyze public schema by default
        analyze_cmd = 'VACUUM ANALYZE' if use_vacuum else 'ANALYZE'
        print(f"==> Kjører {analyze_cmd} på alle tabeller i public schema ...")

        sql = f"""
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
    LOOP
        EXECUTE format('{analyze_cmd} public.%I;', r.tablename);
    END LOOP;
END$$;
"""
        try:
            subprocess.run(
                cmd,
                input=sql,
                env=env,
                capture_output=True,
                text=True,
                check=True
            )
            print(f"  ✓ {analyze_cmd} fullført")
            return True
        except subprocess.CalledProcessError as e:
            print(f"  ⚠ Kunne ikke kjøre {analyze_cmd}: {e.stderr}", file=sys.stderr)
            return False


def create_missing_spatial_indexes(db_params: dict, schemas: Optional[List[str]] = None) -> bool:
    """Create GIST indexes CONCURRENTLY for geometry columns that lack a spatial index.

    Uses CONCURRENTLY to avoid locking tables during index creation, allowing
    reads and writes to continue. This is slower but non-blocking.
    """
    env = os.environ.copy()
    if db_params.get('password'):
        env['PGPASSWORD'] = db_params['password']

    base_cmd = ['psql']
    if db_params.get('host'):
        base_cmd.extend(['-h', db_params['host']])
    if db_params.get('port'):
        base_cmd.extend(['-p', str(db_params['port'])])
    base_cmd.extend(['-U', db_params['user'], '-d', db_params['database'], '-q', '-t'])

    schema_filter = ""
    if schemas:
        schema_list = ", ".join([f"'{s}'" for s in schemas])
        schema_filter = f"AND f_table_schema IN ({schema_list})"

    # First, get list of missing indexes (query outside transaction)
    query_sql = f"""
SELECT format('%I.%I', f_table_schema, f_table_name) AS table_name,
       f_geometry_column AS geomcol,
       format('idx_%s_%s_gist', f_table_name, f_geometry_column) AS idx_name
FROM public.geometry_columns
WHERE 1=1 {schema_filter}
  AND NOT EXISTS (
      SELECT 1 FROM pg_indexes
      WHERE schemaname = f_table_schema
        AND tablename = f_table_name
        AND indexname = format('idx_%s_%s_gist', f_table_name, f_geometry_column)
  )
ORDER BY f_table_schema, f_table_name, f_geometry_column;
"""

    try:
        # Get list of indexes to create
        result = subprocess.run(
            base_cmd,
            input=query_sql,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )

        # Parse results (tab-separated: table_name, geomcol, idx_name)
        indexes_to_create = []
        for line in result.stdout.strip().split('\n'):
            if not line.strip():
                continue
            parts = line.split('\t')
            if len(parts) >= 3:
                table_name = parts[0].strip()
                geomcol = parts[1].strip()
                idx_name = parts[2].strip()
                indexes_to_create.append((table_name, geomcol, idx_name))

        if not indexes_to_create:
            print("  ⊙ Alle spatial-indekser eksisterer allerede")
            return True

        print(f"  → Bygger {len(indexes_to_create)} spatial-indeks(er) CONCURRENTLY...")

        # Create each index CONCURRENTLY (must be outside transaction)
        # Remove -t flag for index creation (we want to see progress)
        create_cmd = base_cmd[:-1]  # Remove -t flag

        success_count = 0
        failed_count = 0

        for table_name, geomcol, idx_name in indexes_to_create:
            # CREATE INDEX CONCURRENTLY cannot be run in a transaction
            create_sql = f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {idx_name} ON {table_name} USING GIST ({geomcol});"

            try:
                subprocess.run(
                    create_cmd,
                    input=create_sql,
                    env=env,
                    capture_output=True,
                    text=True,
                    check=True
                )
                success_count += 1
                if success_count % 10 == 0:
                    print(f"    → {success_count}/{len(indexes_to_create)} indekser bygget...")
            except subprocess.CalledProcessError as e:
                failed_count += 1
                print(f"    ⚠ Kunne ikke lage indeks {idx_name} på {table_name}: {e.stderr}", file=sys.stderr)

        if success_count > 0:
            print(f"  ✓ {success_count} spatial-indeks(er) bygget")
        if failed_count > 0:
            print(f"  ⚠ {failed_count} indeks(er) feilet", file=sys.stderr)

        return failed_count == 0

    except subprocess.CalledProcessError as e:
        print(f"  ⚠ Kunne ikke hente liste over manglende indekser: {e.stderr}", file=sys.stderr)
        return False


def extract_table_names_from_zip_sql(zip_path: Path, sql_file_in_zip: str) -> Tuple[List[str], Optional[str]]:
    """Extract table names and schema prefix from SQL file in ZIP.

    Returns:
        Tuple of (table_names, schema_prefix)
    """
    table_names = []
    schema_prefix = None
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            with zip_ref.open(sql_file_in_zip) as f:
                # Read first 1MB to find CREATE TABLE statements
                content = f.read(1024 * 1024).decode('utf-8', errors='ignore')

                # Extract schema prefix
                schema_prefix = extract_schema_prefix_from_sql(content)

                # Extract table names
                pattern = r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:(\w+)\.)?(\w+)'
                matches = re.findall(pattern, content, re.IGNORECASE)
                for schema, table in matches:
                    if schema:
                        table_names.append(f"{schema}.{table}")
                    else:
                        table_names.append(table)
    except Exception:
        pass
    return table_names, schema_prefix


def load_postgis_sql_from_zip_stream(
    db_params: dict,
    zip_path: Path,
    sql_files: List[str],
    drop_tables_flag: bool
) -> bool:
    """Load PostGIS SQL files directly from ZIP using Python zipfile (no unzip command needed)."""
    if zip_path is None:
        print("Feil: zip_path er None i load_postgis_sql_from_zip_stream", file=sys.stderr)
        return False

    if drop_tables_flag:
        print("==> Sletter eksisterende data ...")

        # Extract schema prefix from SQL files
        schema_prefix = None
        all_table_names = []

        for sql_file in sql_files:
            table_names, prefix = extract_table_names_from_zip_sql(zip_path, sql_file)
            all_table_names.extend(table_names)
            if prefix and not schema_prefix:
                schema_prefix = prefix

        # First, drop all schemas matching the prefix (handles hash changes)
        if schema_prefix:
            print(f"  Sletter alle schemas med prefix '{schema_prefix}_*' ...")
            drop_schemas_by_prefix(db_params, schema_prefix)
        else:
            # Fallback: drop individual tables if we can't find prefix
            print("  Kunne ikke finne schema prefix, sletter individuelle tabeller ...")
            if all_table_names:
                unique_tables = list(set(all_table_names))
                print(f"  Sletter {len(unique_tables)} tabell(er): {', '.join(unique_tables)}")
                if drop_tables(db_params, unique_tables):
                    print("  ✓ Tabeller slettet")
                else:
                    print("  ⚠ Kunne ikke slette alle tabeller (fortsetter likevel)")

    success_count = 0
    for sql_file in sql_files:
        print(f"  -> Laster {sql_file} (direkte fra ZIP) ...")

        try:
            # Read from ZIP and pipe to psql
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                with zip_ref.open(sql_file) as zip_file:
                    psql_proc = run_psql_stream(
                        db_params,
                        zip_file,
                        pre_sql=role_preamble_sql(),
                        post_sql=role_reset_sql()
                    )
                    if psql_proc.returncode != 0:
                        raise subprocess.CalledProcessError(
                            psql_proc.returncode,
                            psql_proc.args,
                            output=psql_proc.stdout,
                            stderr=psql_proc.stderr
                        )

            success_count += 1
            print(f"     ✓ Lastet")
        except subprocess.CalledProcessError as e:
            stderr_text = e.stderr.decode() if isinstance(e.stderr, (bytes, bytearray)) else e.stderr
            print(f"     ✗ Feil ved lasting: {stderr_text if stderr_text else str(e)}", file=sys.stderr)
            return False
        except Exception as e:
            print(f"     ✗ Feil: {e}", file=sys.stderr)
            return False

    return success_count > 0


def load_postgis_sql(db_params: dict, sql_files: List[Path], drop_tables_flag: bool) -> bool:
    """Load PostGIS SQL files."""
    if drop_tables_flag:
        print("==> Sletter eksisterende data ...")

        # Extract schema prefix from SQL files
        schema_prefix = None
        all_table_names = []

        for sql_file in sql_files:
            table_names, prefix = extract_table_names_from_sql(sql_file)
            all_table_names.extend(table_names)
            if prefix and not schema_prefix:
                schema_prefix = prefix

        # First, drop all schemas matching the prefix (handles hash changes)
        if schema_prefix:
            print(f"  Sletter alle schemas med prefix '{schema_prefix}_*' ...")
            drop_schemas_by_prefix(db_params, schema_prefix)
        else:
            # Fallback: drop individual tables if we can't find prefix
            print("  Kunne ikke finne schema prefix, sletter individuelle tabeller ...")
            if all_table_names:
                unique_tables = list(set(all_table_names))
                print(f"  Sletter {len(unique_tables)} tabell(er): {', '.join(unique_tables)}")
                if drop_tables(db_params, unique_tables):
                    print("  ✓ Tabeller slettet")
                else:
                    print("  ⚠ Kunne ikke slette alle tabeller (fortsetter likevel)")

    # Load SQL files
    env = os.environ.copy()
    if db_params.get('password'):
        env['PGPASSWORD'] = db_params['password']

    success_count = 0
    for sql_file in sql_files:
        print(f"  -> Laster {sql_file.name} ...")
        file_size_mb = sql_file.stat().st_size / (1024 * 1024)
        print(f"     (SQL-fil størrelse: {file_size_mb:.1f} MB)")

        cmd = ['psql', '-v', 'ON_ERROR_STOP=1']
        if db_params.get('host'):
            cmd.extend(['-h', db_params['host']])
        if db_params.get('port'):
            cmd.extend(['-p', str(db_params['port'])])
        cmd.extend(['-U', db_params['user'], '-d', db_params['database']])
        cmd.extend(['-c', role_preamble_sql(), '-f', str(sql_file), '-c', role_reset_sql()])

        try:
            result = subprocess.run(cmd, env=env, check=True, capture_output=True, text=True)
            success_count += 1
            print(f"     ✓ Lastet")
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr if e.stderr else str(e)
            if e.stdout:
                error_msg = f"{error_msg}\nSTDOUT: {e.stdout}"
            print(f"     ✗ Feil ved lasting: {error_msg}", file=sys.stderr)
            return False

    return success_count > 0


def check_ogr2ogr() -> bool:
    """Check if ogr2ogr is available."""
    try:
        subprocess.run(['ogr2ogr', '--version'], capture_output=True, check=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


def check_osm2pgsql() -> bool:
    """Check if osm2pgsql is available."""
    try:
        subprocess.run(['osm2pgsql', '--version'], capture_output=True, check=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


def get_fgdb_feature_count(gdb_dir: Path) -> Optional[int]:
    """Get total feature count from FGDB file using ogrinfo.

    Returns total number of features across all layers, or None if unable to determine.
    """
    try:
        # Use ogrinfo to get layer information
        # -al: list all layers
        # -so: summary only (fast, no feature reading)
        cmd = ['ogrinfo', '-al', '-so', str(gdb_dir)]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

        if result.returncode != 0:
            return None

        # Parse output to find feature counts
        # ogrinfo output format: "Feature Count: 12345"
        total_features = 0
        for line in result.stdout.split('\n'):
            if 'Feature Count:' in line:
                try:
                    # Extract number after "Feature Count: "
                    count_str = line.split('Feature Count:')[1].strip()
                    count = int(count_str)
                    total_features += count
                except (ValueError, IndexError):
                    continue

        return total_features if total_features > 0 else None
    except (FileNotFoundError, subprocess.TimeoutExpired, Exception):
        return None


def load_gml_from_zip_stream(
    db_params: dict,
    zip_path: Path,
    gml_files: List[str],
    table_name: str,
    target_srid: Optional[int],
    staging_schema: Optional[str],
    append: bool = False
) -> bool:
    """Load GML files directly from ZIP using GDAL virtual file system (/vsizip/).

    Args:
        append: If True, append to existing table instead of overwriting
    """
    if not check_ogr2ogr():
        print("Feil: ogr2ogr ikke funnet. Installer GDAL:", file=sys.stderr)
        return False

    env = os.environ.copy()
    if db_params.get('password'):
        env['PGPASSWORD'] = db_params['password']

    # When host is None, unset PGHOST to ensure Unix socket is used
    if db_params.get('host') is None:
        env.pop('PGHOST', None)
        env.pop('PGPORT', None)

    # Build connection string
    # When host is None, omit host/port to use Unix socket (peer auth)
    # When host is set, use TCP/IP connection
    conn_parts = []
    if db_params.get('host'):
        conn_parts.append(f"host={db_params['host']}")
        if db_params.get('port'):
            conn_parts.append(f"port={db_params['port']}")
    # If host is None, don't include host/port - ogr2ogr will use Unix socket

    conn_parts.append(f"user={db_params['user']}")
    conn_parts.append(f"dbname={db_params['database']}")

    if db_params.get('password'):
        conn_parts.append(f"password={db_params['password']}")

    conn_str = "PG:" + " ".join(conn_parts)

    success_count = 0
    is_first_file = not append  # First file if not appending
    for gml_file in gml_files:
        print(f"  -> Laster {gml_file} (direkte fra ZIP via /vsizip/) ...")

        # Use GDAL virtual file system to read from ZIP without extraction
        # Format: /vsizip/path/to/zip.zip/path/to/file.gml
        vsi_path = f"/vsizip/{zip_path}/{gml_file}"

        target_name = table_name
        if staging_schema:
            target_name = f"{staging_schema}.{table_name}"

        cmd = [
            'ogr2ogr',
            '-f', 'PostgreSQL',
            conn_str,
            vsi_path,
            '-nln', target_name,
            '-lco', 'GEOMETRY_NAME=geom',
            '-lco', 'SPATIAL_INDEX=GIST',
            '-lco', 'LAUNDER=YES',  # Better column name handling for complex GML
            '-lco', 'FID=ogc_fid',  # Ensure unique feature ID column
            '-lco', 'PROMOTE_TO_MULTI=YES',  # Convert nested structures to arrays to avoid duplicate columns
            '-lco', 'EXPLODE_COLLECTIONS=YES',  # Explode collections into separate features to avoid duplicate column names
            '-splitlistfields',  # Split list fields into separate columns
            '-maxsubfields', '10',  # Maximum number of subfields to create from list fields
            '-progress',
            '-skipfailures'  # Skip rows with errors (e.g., duplicate column names)
        ]

        # Use -overwrite for first file, -append for subsequent files
        if is_first_file:
            cmd.append('-overwrite')
            is_first_file = False
        else:
            cmd.append('-append')

        if target_srid:
            cmd.extend(['-t_srs', f'EPSG:{target_srid}'])

        try:
            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True
            )

            if result.returncode == 0:
                success_count += 1
                print(f"     ✓ Lastet")
            else:
                print(f"     ✗ Feil: {result.stderr}", file=sys.stderr)
                return False
        except FileNotFoundError:
            print("     ✗ ogr2ogr ikke funnet", file=sys.stderr)
            return False

    return success_count > 0


def load_gml_files(
    db_params: dict,
    gml_files: List[Path],
    table_name: str,
    target_srid: Optional[int],
    staging_schema: Optional[str],
    append: bool = False
) -> bool:
    """Load GML files using ogr2ogr.

    Args:
        append: If True, append to existing table instead of overwriting
    """
    if not check_ogr2ogr():
        print("Feil: ogr2ogr ikke funnet. Installer GDAL:", file=sys.stderr)
        print("  Ubuntu/Debian: sudo apt-get install gdal-bin", file=sys.stderr)
        print("  macOS: brew install gdal", file=sys.stderr)
        return False

    env = os.environ.copy()
    if db_params.get('password'):
        env['PGPASSWORD'] = db_params['password']

    # When host is None, unset PGHOST to ensure Unix socket is used
    if db_params.get('host') is None:
        env.pop('PGHOST', None)
        env.pop('PGPORT', None)

    # Build connection string
    # When host is None, omit host/port to use Unix socket (peer auth)
    # When host is set, use TCP/IP connection
    conn_parts = []
    if db_params.get('host'):
        conn_parts.append(f"host={db_params['host']}")
        if db_params.get('port'):
            conn_parts.append(f"port={db_params['port']}")
    # If host is None, don't include host/port - ogr2ogr will use Unix socket

    conn_parts.append(f"user={db_params['user']}")
    conn_parts.append(f"dbname={db_params['database']}")

    if db_params.get('password'):
        conn_parts.append(f"password={db_params['password']}")

    conn_str = "PG:" + " ".join(conn_parts)

    success_count = 0
    is_first_file = not append  # First file if not appending
    for gml_file in gml_files:
        print(f"  -> Laster {gml_file.name} ...")

        target_name = table_name
        if staging_schema:
            target_name = f"{staging_schema}.{table_name}"

        cmd = [
            'ogr2ogr',
            '-f', 'PostgreSQL',
            conn_str,
            str(gml_file),
            '-nln', target_name,
            '-lco', 'GEOMETRY_NAME=geom',
            '-lco', 'SPATIAL_INDEX=GIST',
            '-lco', 'LAUNDER=YES',  # Better column name handling for complex GML
            '-lco', 'FID=ogc_fid',  # Ensure unique feature ID column
            '-lco', 'PROMOTE_TO_MULTI=YES',  # Convert nested structures to arrays to avoid duplicate columns
            '-lco', 'EXPLODE_COLLECTIONS=YES',  # Explode collections into separate features to avoid duplicate column names
            '-splitlistfields',  # Split list fields into separate columns
            '-maxsubfields', '10',  # Maximum number of subfields to create from list fields
            '-progress',
            '-skipfailures'  # Skip rows with errors (e.g., duplicate column names)
        ]

        # Use -overwrite for first file, -append for subsequent files
        if is_first_file:
            cmd.append('-overwrite')
            is_first_file = False
        else:
            cmd.append('-append')

        # Add SRID transformation if specified
        if target_srid:
            cmd.extend(['-t_srs', f'EPSG:{target_srid}'])

        try:
            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True
            )

            if result.returncode == 0:
                success_count += 1
                print(f"     ✓ Lastet")
            else:
                print(f"     ✗ Feil: {result.stderr}", file=sys.stderr)
                return False
        except FileNotFoundError:
            print("     ✗ ogr2ogr ikke funnet", file=sys.stderr)
            return False

    return success_count > 0


def load_fgdb(db_params: dict, gdb_dirs: List[Path], target_srid: Optional[int], staging_schema: Optional[str]) -> bool:
    """Load FileGDB directories using ogr2ogr with real-time progress and performance optimizations."""
    if not check_ogr2ogr():
        print("Feil: ogr2ogr ikke funnet. Installer GDAL:", file=sys.stderr)
        print("  Ubuntu/Debian: sudo apt-get install gdal-bin", file=sys.stderr)
        print("  macOS: brew install gdal", file=sys.stderr)
        return False

    env = os.environ.copy()
    if db_params.get('password'):
        env['PGPASSWORD'] = db_params['password']

    # When host is None, unset PGHOST to ensure Unix socket is used
    if db_params.get('host') is None:
        env.pop('PGHOST', None)
        env.pop('PGPORT', None)

    # Build connection string
    conn_parts = []
    if db_params.get('host'):
        conn_parts.append(f"host={db_params['host']}")
        if db_params.get('port'):
            conn_parts.append(f"port={db_params['port']}")
    conn_parts.append(f"user={db_params['user']}")
    conn_parts.append(f"dbname={db_params['database']}")
    if db_params.get('password'):
        conn_parts.append(f"password={db_params['password']}")
    conn_str = "PG:" + " ".join(conn_parts)

    # Clean up staging schema if it exists (from previous failed/aborted imports)
    # Then recreate it for fresh import
    if staging_schema:
        print(f"  -> Rydder opp i staging schema {staging_schema} (hvis eksisterer)...")
        drop_schema_sql = f"DROP SCHEMA IF EXISTS {staging_schema} CASCADE;"
        try:
            drop_cmd = ['psql']
            if db_params.get('host'):
                drop_cmd.extend(['-h', db_params['host']])
            if db_params.get('port'):
                drop_cmd.extend(['-p', str(db_params['port'])])
            drop_cmd.extend(['-U', db_params['user'], '-d', db_params['database'], '-q', '-c', drop_schema_sql])
            subprocess.run(drop_cmd, env=env, capture_output=True, check=False)
            print("     ✓ Staging schema ryddet")
        except Exception as e:
            print(f"     ⚠ Kunne ikke rydde staging schema (fortsetter): {e}")

        # Create staging schema for fresh import
        print(f"  -> Oppretter staging schema {staging_schema}...")
        if not ensure_schema_exists(db_params, staging_schema):
            print("     ✗ Kunne ikke opprette staging schema", file=sys.stderr)
            return False
        print("     ✓ Staging schema opprettet")

    success_count = 0
    for gdb_dir in gdb_dirs:
        print(f"  -> Laster {gdb_dir} (FileGDB) ...")

        # Get total feature count from FGDB for progress calculation
        print("     Sjekker antall features i FGDB...")
        total_features = get_fgdb_feature_count(gdb_dir)
        if total_features:
            print(f"     Totalt antall features: {total_features:,}")
        else:
            print("     ⚠ Kunne ikke bestemme totalt antall features (fortsetter uten prosent)")

        cmd = [
            'ogr2ogr',
            '-f', 'PostgreSQL',
            conn_str,
            str(gdb_dir),
            '-nlt', 'PROMOTE_TO_MULTI',
            '-lco', 'GEOMETRY_NAME=geom',
            '-lco', 'FID=ogc_fid',
            '-lco', 'SPATIAL_INDEX=NO',  # Deaktiver indekser under import for raskere import
            '-lco', 'OVERWRITE=YES',  # Overskriv eksisterende tabeller
            '-lco', 'UNLOGGED=YES',  # Bruk unlogged tables for raskere import (kan konverteres til logged etterpå)
            '--config', 'PG_USE_COPY', 'YES',  # hurtigere innlasting
            '--config', 'OGR_SQLITE_CACHE', '512',  # Cache for SQLite/FGDB lesing
            '-gt', '500000',  # større batcher til COPY (økt fra 200000 for bedre ytelse)
            '-progress',
            '-skipfailures',
            '-overwrite'
        ]

        if staging_schema:
            cmd.extend(['-lco', f"SCHEMA={staging_schema}"])

        if target_srid:
            cmd.extend(['-t_srs', f'EPSG:{target_srid}'])

        process = None
        try:
            # Use Popen for real-time progress output
            process = subprocess.Popen(
                cmd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # Combine stderr with stdout
                text=True,
                bufsize=1,  # Line buffered
                universal_newlines=True
            )

            # Read and print output line by line for real-time progress
            # ogr2ogr sends progress to stderr, which we've combined with stdout
            import threading
            import time

            last_progress_line = ""
            error_lines = []
            line_count = 0
            has_output = False

            # Show a heartbeat indicator if no progress messages appear
            # Also check database for row counts as alternative progress
            heartbeat_counter = [0]  # Use list to allow modification from inner function
            last_row_count = [0]

            def show_heartbeat():
                """Show heartbeat while waiting for output."""
                time.sleep(2)  # Wait 2 seconds before showing heartbeat
                while process.poll() is None:
                    heartbeat_counter[0] += 1
                    dots = "." * (heartbeat_counter[0] % 4)  # Rotating dots

                    # Try to get row count from database as progress indicator
                    row_count_msg = ""
                    if staging_schema and heartbeat_counter[0] % 5 == 0:  # Check every 5 seconds
                        try:
                            check_cmd = ['psql']
                            if db_params.get('host'):
                                check_cmd.extend(['-h', db_params['host']])
                            if db_params.get('port'):
                                check_cmd.extend(['-p', str(db_params['port'])])
                            check_cmd.extend(['-U', db_params['user'], '-d', db_params['database'], '-t', '-q', '-c',
                                f"SELECT COALESCE(SUM(n_live_tup), 0) FROM pg_stat_user_tables WHERE schemaname = '{staging_schema}';"])
                            result = subprocess.run(check_cmd, env=env, capture_output=True, text=True, timeout=2)
                            if result.returncode == 0 and result.stdout.strip():
                                current_count = int(result.stdout.strip())
                                if current_count > last_row_count[0]:
                                    # Calculate percentage if we have total feature count
                                    if total_features and total_features > 0:
                                        percentage = min(100.0, (current_count / total_features) * 100.0)
                                        row_count_msg = f" ({current_count:,}/{total_features:,} rader, {percentage:.1f}%)"
                                    else:
                                        row_count_msg = f" (~{current_count:,} rader importert)"
                                    last_row_count[0] = current_count
                        except Exception:
                            pass  # Ignore errors in progress check

                    print(f"\r     ⏳ Importerer{dots}{row_count_msg}", end='', flush=True)
                    time.sleep(1)

            heartbeat_thread = threading.Thread(target=show_heartbeat, daemon=True)
            heartbeat_thread.start()

            # Read from both stdout and stderr in real-time
            for line in process.stdout:
                line = line.rstrip()
                if not line:
                    continue

                line_count += 1
                has_output = True  # We got output, disable heartbeat
                heartbeat_counter[0] = -1  # Stop heartbeat

                # Check for progress indicators (ogr2ogr format: "0...10...20...30...")
                # or "Progress: X%" or feature counts
                is_progress = (
                    'Progress' in line or
                    '%' in line or
                    'features' in line.lower() or
                    re.match(r'^\d+\.\.\.', line) or  # "0...10...20..."
                    re.search(r'\d+/\d+', line) or  # "1234/5678"
                    '...' in line or  # Progress dots
                    'Copying' in line or  # COPY operations
                    'Creating' in line  # Table creation
                )

                # Check for errors
                is_error = line.startswith('ERROR') or 'ERROR' in line.upper()

                if is_progress:
                    # Print progress on same line (overwrite)
                    # Truncate to 80 chars to avoid line wrapping
                    display_line = line[:80] if len(line) <= 80 else line[:77] + "..."
                    print(f"\r     {display_line}", end='', flush=True)
                    last_progress_line = line
                elif is_error:
                    # Show errors immediately but don't overwrite progress
                    if last_progress_line:
                        print()  # New line before error
                    print(f"     ⚠ {line[:80]}", flush=True)
                    error_lines.append(line)
                    last_progress_line = ""  # Reset so next progress shows on new line
                else:
                    # Show all output lines (ogr2ogr may not send progress with COPY)
                    # This helps user see that something is happening
                    if last_progress_line:
                        print()  # New line before message
                    # Show important messages, filter out noise
                    # Always show first few lines and important operations
                    should_show = (
                        line_count <= 5 or  # Always show first 5 lines
                        'Creating' in line or
                        'Copying' in line or
                        'Layer' in line or
                        'table' in line.lower() or
                        'feature' in line.lower()
                    )
                    if should_show and line.strip() and not line.startswith('Warning'):
                        print(f"     {line[:80]}", flush=True)
                    last_progress_line = ""

            # Print final newline after progress
            if last_progress_line:
                print()  # New line after progress

            # Show error summary if many errors
            if len(error_lines) > 10:
                print(f"\n     ⚠ {len(error_lines)} feilmelding(er) totalt (vist første 10 over)")

            # Wait for process to complete
            returncode = process.wait()

            if returncode == 0:
                success_count += 1
                print(f"     ✓ Lastet")
            else:
                print(f"\n     ✗ Feil: Import feilet (exit code {returncode})", file=sys.stderr)
                return False
        except FileNotFoundError:
            print("     ✗ ogr2ogr ikke funnet", file=sys.stderr)
            return False
        except KeyboardInterrupt:
            print("\n     ⚠ Import avbrutt av bruker", file=sys.stderr)
            if process:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
            return False

    # Set ownership and convert unlogged tables to logged after import
    if success_count > 0 and staging_schema:
        print("  -> Setter eierskap til stiflyt_owner...")
        try:
            owner_cmd = ['psql']
            if db_params.get('host'):
                owner_cmd.extend(['-h', db_params['host']])
            if db_params.get('port'):
                owner_cmd.extend(['-p', str(db_params['port'])])
            owner_cmd.extend(['-U', db_params['user'], '-d', db_params['database'], '-q', '-c',
                f"""
                DO $$
                BEGIN
                    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stiflyt_owner') THEN
                        BEGIN
                            EXECUTE 'SET ROLE stiflyt_owner';
                        EXCEPTION WHEN insufficient_privilege THEN
                            RAISE NOTICE 'Could not SET ROLE stiflyt_owner (not member)';
                        END;
                    END IF;
                END $$;

                DO $$
                DECLARE
                    r record;
                BEGIN
                    -- Set ownership of all tables
                    FOR r IN
                        SELECT tablename
                        FROM pg_tables
                        WHERE schemaname = '{staging_schema}'
                    LOOP
                        BEGIN
                            EXECUTE format('ALTER TABLE %I.%I OWNER TO stiflyt_owner', '{staging_schema}', r.tablename);
                        EXCEPTION WHEN OTHERS THEN
                            RAISE NOTICE 'Could not set owner for table %.%: %', '{staging_schema}', r.tablename, SQLERRM;
                        END;
                    END LOOP;

                    -- Set ownership of all sequences
                    FOR r IN
                        SELECT sequence_name
                        FROM information_schema.sequences
                        WHERE sequence_schema = '{staging_schema}'
                    LOOP
                        BEGIN
                            EXECUTE format('ALTER SEQUENCE %I.%I OWNER TO stiflyt_owner', '{staging_schema}', r.sequence_name);
                        EXCEPTION WHEN OTHERS THEN
                            RAISE NOTICE 'Could not set owner for sequence %.%: %', '{staging_schema}', r.sequence_name, SQLERRM;
                        END;
                    END LOOP;
                END $$;

                RESET ROLE;
                """])
            subprocess.run(owner_cmd, env=env, capture_output=True, check=False)
            print("     ✓ Eierskap satt")
        except Exception as e:
            print(f"     ⚠ Kunne ikke sette eierskap (fortsetter): {e}")

        print("  -> Konverterer unlogged tables til logged...")
        try:
            convert_cmd = ['psql']
            if db_params.get('host'):
                convert_cmd.extend(['-h', db_params['host']])
            if db_params.get('port'):
                convert_cmd.extend(['-p', str(db_params['port'])])
            convert_cmd.extend(['-U', db_params['user'], '-d', db_params['database'], '-q', '-c',
                f"""
                DO $$
                DECLARE
                    r record;
                BEGIN
                    FOR r IN
                        SELECT tablename
                        FROM pg_tables
                        WHERE schemaname = '{staging_schema}'
                    LOOP
                        EXECUTE format('ALTER TABLE %I.%I SET LOGGED', '{staging_schema}', r.tablename);
                    END LOOP;
                END $$;
                """])
            subprocess.run(convert_cmd, env=env, capture_output=True, check=False)
            print("     ✓ Tables konvertert til logged")
        except Exception as e:
            print(f"     ⚠ Kunne ikke konvertere tables (fortsetter): {e}")

        print("  -> Bygger spatial-indekser (dette kan ta noen minutter)...")
        if create_missing_spatial_indexes(db_params, schemas=[staging_schema]):
            print("     ✓ Indekser bygget")
        else:
            print("     ⚠ Kunne ikke bygge alle indekser (kan bygges manuelt senere)")

    return success_count > 0


def load_osm_pbf(
    db_params: dict,
    osm_file: Path,
    target_srid: Optional[int],
    staging_schema: Optional[str],
    drop_tables: bool = False
) -> bool:
    """Load OSM PBF file using osm2pgsql.

    Args:
        db_params: Database connection parameters
        osm_file: Path to OSM PBF file
        target_srid: Target SRID for transformation
        staging_schema: Schema name for staging (or None for public)
        drop_tables: If True, drop existing tables before loading
    """
    if not check_osm2pgsql():
        print("Feil: osm2pgsql ikke funnet. Installer osm2pgsql:", file=sys.stderr)
        print("  Ubuntu/Debian: sudo apt-get install osm2pgsql", file=sys.stderr)
        print("  macOS: brew install osm2pgsql", file=sys.stderr)
        return False

    env = os.environ.copy()
    if db_params.get('password'):
        env['PGPASSWORD'] = db_params['password']

    schema = staging_schema or os.environ.get('OSM2PGSQL_SCHEMA', 'public')
    prefix = os.environ.get('OSM2PGSQL_PREFIX', 'planet_osm')
    style = os.environ.get('OSM2PGSQL_STYLE')
    extra_args = shlex.split(os.environ.get('OSM2PGSQL_ARGS', ''))

    # Clean up staging schema if it exists (from previous failed/aborted imports)
    if staging_schema:
        print(f"  -> Rydder opp i staging schema {staging_schema} (hvis eksisterer)...")
        drop_schema_sql = f"DROP SCHEMA IF EXISTS {staging_schema} CASCADE;"
        try:
            drop_cmd = ['psql']
            if db_params.get('host'):
                drop_cmd.extend(['-h', db_params['host']])
            if db_params.get('port'):
                drop_cmd.extend(['-p', str(db_params['port'])])
            drop_cmd.extend(['-U', db_params['user'], '-d', db_params['database'], '-q', '-c', drop_schema_sql])
            subprocess.run(drop_cmd, env=env, capture_output=True, check=False)
            print("     ✓ Staging schema ryddet")
        except Exception as e:
            print(f"     ⚠ Kunne ikke rydde staging schema (fortsetter): {e}")

        # Create staging schema for fresh import
        print(f"  -> Oppretter staging schema {staging_schema}...")
        if not ensure_schema_exists(db_params, staging_schema):
            print("     ✗ Kunne ikke opprette staging schema", file=sys.stderr)
            return False
        print("     ✓ Staging schema opprettet")

    print(f"  -> Laster {osm_file.name} (OSM PBF) med osm2pgsql ...")

    cmd = [
        'osm2pgsql',
        '--slim',
        '--schema', schema,
        '--prefix', prefix,
        '-d', db_params['database'],
        '-U', db_params['user']
    ]

    if db_params.get('host'):
        cmd.extend(['-H', db_params['host']])
    if db_params.get('port'):
        cmd.extend(['-P', str(db_params['port'])])

    cmd.append('--create')
    if drop_tables:
        cmd.append('--drop')

    if style:
        cmd.extend(['--style', style])

    cmd.extend(extra_args)
    cmd.append(str(osm_file))

    try:
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)

        if result.returncode == 0:
            print(f"     ✓ Lastet")

            # Set ownership and convert unlogged tables to logged after import
            if staging_schema:
                print("  -> Setter eierskap til stiflyt_owner...")
                try:
                    owner_cmd = ['psql']
                    if db_params.get('host'):
                        owner_cmd.extend(['-h', db_params['host']])
                    if db_params.get('port'):
                        owner_cmd.extend(['-p', str(db_params['port'])])
                    owner_cmd.extend(['-U', db_params['user'], '-d', db_params['database'], '-q', '-c',
                        f"""
                        DO $$
                        BEGIN
                            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stiflyt_owner') THEN
                                BEGIN
                                    EXECUTE 'SET ROLE stiflyt_owner';
                                EXCEPTION WHEN insufficient_privilege THEN
                                    RAISE NOTICE 'Could not SET ROLE stiflyt_owner (not member)';
                                END;
                            END IF;
                        END $$;

                        DO $$
                        DECLARE
                            r record;
                        BEGIN
                            -- Set ownership of all tables
                            FOR r IN
                                SELECT tablename
                                FROM pg_tables
                                WHERE schemaname = '{staging_schema}'
                            LOOP
                                BEGIN
                                    EXECUTE format('ALTER TABLE %I.%I OWNER TO stiflyt_owner', '{staging_schema}', r.tablename);
                                EXCEPTION WHEN OTHERS THEN
                                    RAISE NOTICE 'Could not set owner for table %.%: %', '{staging_schema}', r.tablename, SQLERRM;
                                END;
                            END LOOP;

                            -- Set ownership of all sequences
                            FOR r IN
                                SELECT sequence_name
                                FROM information_schema.sequences
                                WHERE sequence_schema = '{staging_schema}'
                            LOOP
                                BEGIN
                                    EXECUTE format('ALTER SEQUENCE %I.%I OWNER TO stiflyt_owner', '{staging_schema}', r.sequence_name);
                                EXCEPTION WHEN OTHERS THEN
                                    RAISE NOTICE 'Could not set owner for sequence %.%: %', '{staging_schema}', r.sequence_name, SQLERRM;
                                END;
                            END LOOP;
                        END $$;

                        RESET ROLE;
                        """])
                    subprocess.run(owner_cmd, env=env, capture_output=True, check=False)
                    print("     ✓ Eierskap satt")
                except Exception as e:
                    print(f"     ⚠ Kunne ikke sette eierskap (fortsetter): {e}")

                print("  -> Konverterer unlogged tables til logged...")
                try:
                    convert_cmd = ['psql']
                    if db_params.get('host'):
                        convert_cmd.extend(['-h', db_params['host']])
                    if db_params.get('port'):
                        convert_cmd.extend(['-p', str(db_params['port'])])
                    convert_cmd.extend(['-U', db_params['user'], '-d', db_params['database'], '-q', '-c',
                        f"""
                        DO $$
                        DECLARE
                            r record;
                        BEGIN
                            FOR r IN
                                SELECT tablename
                                FROM pg_tables
                                WHERE schemaname = '{staging_schema}'
                            LOOP
                                EXECUTE format('ALTER TABLE %I.%I SET LOGGED', '{staging_schema}', r.tablename);
                            END LOOP;
                        END $$;
                        """])
                    subprocess.run(convert_cmd, env=env, capture_output=True, check=False)
                    print("     ✓ Tables konvertert til logged")
                except Exception as e:
                    print(f"     ⚠ Kunne ikke konvertere tables (fortsetter): {e}")

                print("  -> Bygger spatial-indekser (dette kan ta noen minutter)...")
                if create_missing_spatial_indexes(db_params, schemas=[staging_schema]):
                    print("     ✓ Indekser bygget")
                else:
                    print("     ⚠ Kunne ikke bygge alle indekser (kan bygges manuelt senere)")

            return True
        else:
            print(f"     ✗ Feil: {result.stderr}", file=sys.stderr)
            return False
    except FileNotFoundError:
        print("     ✗ osm2pgsql ikke funnet", file=sys.stderr)
        return False


def load_dataset(
    zip_path: Path,
    database: str,
    table_name: Optional[str] = None,
    target_srid: Optional[int] = None,
    drop_tables: bool = False,
    stream: bool = True,
    append: bool = False
) -> bool:
    """Load dataset from ZIP file or standalone file into PostGIS database.

    Args:
        zip_path: Path to ZIP file or standalone file (e.g., .osm.pbf)
        database: Database name
        table_name: Table name (required for GML, auto-detected for PostGIS SQL)
        target_srid: Target SRID for transformation (default: 25833)
        drop_tables: Drop existing tables before loading (PostGIS SQL only)
        stream: If True, load directly from ZIP without extracting (default: True)
        append: If True, append to existing table instead of overwriting (GML only)

    Returns:
        True if successful, False otherwise
    """
    # Validate zip_path
    if zip_path is None:
        print("Feil: zip_path er None", file=sys.stderr)
        return False

    # Ensure zip_path is a Path object
    if not isinstance(zip_path, Path):
        zip_path = Path(zip_path)

    if not zip_path.exists():
        print(f"Feil: Filen {zip_path} eksisterer ikke", file=sys.stderr)
        return False

    db_params = get_db_connection_params()
    db_params['database'] = database

    if not check_owner_membership(db_params):
        return False

    if target_srid is None:
        target_srid = int(os.environ.get('TARGET_SRID', '25833'))
    if table_name is None:
        table_name = zip_path.stem.lower().replace('-', '_')

    # Check if this is a standalone OSM PBF file (not a ZIP)
    if zip_path.suffixes == ['.osm', '.pbf'] or zip_path.name.endswith('.osm.pbf'):
        print(f"==> Detektert OSM PBF fil: {zip_path.name}")
        print(f"==> Sjekker PostGIS extension i database '{db_params['database']}' ...")
        if not ensure_postgis_extension(db_params):
            return False
        print("  ✓ PostGIS extension klar")

        staging_schema = f"staging_{sanitize_identifier(table_name)}"
        if not ensure_schema_exists(db_params, staging_schema):
            return False

        if load_osm_pbf(db_params, zip_path, target_srid, staging_schema, drop_tables):
            print(f"==> Ferdig. OSM PBF fil lastet inn")
            print(f"==> Flytter staging-schema {staging_schema} til public ...")
            move_schema_objects(db_params, staging_schema, 'public')
            grant_privileges_for_schema(db_params, 'public')
            print("==> Bygger manglende spatial-indekser ...")
            create_missing_spatial_indexes(db_params)
            analyze_tables(db_params, schemas=['public'])
            return True
        else:
            return False

    # Try streaming first (no extraction)
    if stream:
        print(f"==> Undersøker ZIP-innhold (uten ekstraksjon) ...")
        format_type, files_in_zip = detect_format_from_zip(zip_path)

        if format_type:
            print(f"  ✓ Detektert format: {format_type}")
            print(f"  ✓ Fant {len(files_in_zip)} fil(er) i ZIP")

            # Ensure PostGIS extension
            print(f"==> Sjekker PostGIS extension i database '{db_params['database']}' ...")
            if not ensure_postgis_extension(db_params):
                return False
            print("  ✓ PostGIS extension klar")

            staging_schema = None
            if format_type == 'GML':
                staging_schema = f"staging_{sanitize_identifier(table_name)}"
                if not ensure_schema_exists(db_params, staging_schema):
                    return False
            elif format_type == 'FGDB':
                staging_schema = f"staging_{sanitize_identifier(zip_path.stem)}"
                if not ensure_schema_exists(db_params, staging_schema):
                    return False

            # Load based on format (streaming)
            print(f"==> Laster data direkte fra ZIP inn i database '{db_params['database']}' ...")

            if format_type == 'PostGIS':
                if load_postgis_sql_from_zip_stream(db_params, zip_path, files_in_zip, drop_tables):
                    print(f"==> Ferdig. {len(files_in_zip)} SQL-fil(er) lastet inn (uten ekstraksjon)")
                    # Extract schema prefix to analyze imported tables
                    table_names, schema_prefix = extract_table_names_from_zip_sql(zip_path, files_in_zip[0])
                    if schema_prefix:
                        grant_privileges_for_schema_prefix(db_params, schema_prefix)
                    else:
                        grant_privileges_for_schema(db_params, 'public')
                    if table_names:
                        # Analyze imported tables
                        analyze_tables(db_params, tables=table_names)
                    else:
                        # Fallback: analyze public schema
                        analyze_tables(db_params, schemas=['public'])
                    return True
                else:
                    print("  ⚠ Streaming feilet, prøver med ekstraksjon...", file=sys.stderr)
                    # Fall through to extraction method
            elif format_type == 'GML':
                print(f"    Tabell: {table_name}")
                print(f"    Transformering til EPSG:{target_srid}")
                if append:
                    print(f"    Modus: Legger til eksisterende tabell")

                if load_gml_from_zip_stream(db_params, zip_path, files_in_zip, table_name, target_srid, staging_schema, append):
                    print(f"==> Ferdig. {len(files_in_zip)} GML-fil(er) lastet inn (uten ekstraksjon)")
                    print(f"    Tabell: {table_name}")
                    if staging_schema:
                        print(f"==> Flytter staging-schema {staging_schema} til public ...")
                        move_schema_objects(db_params, staging_schema, 'public')
                        grant_privileges_for_schema(db_params, 'public')
                    # Analyze imported table
                    analyze_tables(db_params, tables=[f'public.{table_name}'])
                    return True
                else:
                    print("  ⚠ Streaming feilet, prøver med ekstraksjon...", file=sys.stderr)
                    # Fall through to extraction method
            elif format_type == 'FGDB':
                print("  ℹ FGDB oppdaget. Streaming støttes ikke, prøver med ekstraksjon ...")
                # fall through to extraction
            elif format_type == 'OSM':
                print("  ℹ OSM PBF oppdaget i ZIP. Prøver med ekstraksjon ...")
                # fall through to extraction

    # Fallback: Extract ZIP file (original method)
    extract_dir = zip_path.parent / f"{zip_path.stem}_extracted"

    try:
        if not extract_zip(zip_path, extract_dir):
            return False

        # Detect format
        format_type, files = detect_format(extract_dir)

        if not format_type:
            print(f"Feil: Kunne ikke detektere format i {zip_path}", file=sys.stderr)
            print("Forventet SQL (.sql), GML (.gml), FileGDB (.gdb) eller OSM PBF (.osm.pbf)", file=sys.stderr)
            return False

        print(f"  ✓ Detektert format: {format_type}")
        print(f"  ✓ Fant {len(files)} fil(er)")

        staging_schema = None
        if format_type == 'GML':
            staging_schema = f"staging_{sanitize_identifier(table_name)}"
            if not ensure_schema_exists(db_params, staging_schema):
                return False
        elif format_type == 'FGDB':
            staging_schema = f"staging_{sanitize_identifier(zip_path.stem)}"
            if not ensure_schema_exists(db_params, staging_schema):
                return False
        elif format_type == 'OSM':
            staging_schema = f"staging_{sanitize_identifier(table_name)}"
            if not ensure_schema_exists(db_params, staging_schema):
                return False

        # Ensure PostGIS extension
        print(f"==> Sjekker PostGIS extension i database '{db_params['database']}' ...")
        if not ensure_postgis_extension(db_params):
            return False
        print("  ✓ PostGIS extension klar")

        # Load based on format
        print(f"==> Laster data inn i database '{db_params['database']}' ...")

        if format_type == 'PostGIS':
            if load_postgis_sql(db_params, files, drop_tables):
                print(f"==> Ferdig. {len(files)} SQL-fil(er) lastet inn")
                # Extract table names from first SQL file
                table_names, schema_prefix = extract_table_names_from_sql(files[0])
                if schema_prefix:
                    grant_privileges_for_schema_prefix(db_params, schema_prefix)
                else:
                    grant_privileges_for_schema(db_params, 'public')
                if table_names:
                    # Analyze imported tables
                    analyze_tables(db_params, tables=table_names)
                else:
                    # Fallback: analyze public schema
                    analyze_tables(db_params, schemas=['public'])
                return True
            else:
                print("Feil: Kunne ikke laste inn SQL-filer", file=sys.stderr)
                return False

        elif format_type == 'GML':
            print(f"    Tabell: {table_name}")
            print(f"    Transformering til EPSG:{target_srid}")

            if load_gml_files(db_params, files, table_name, target_srid, staging_schema, append):
                print(f"==> Ferdig. {len(files)} GML-fil(er) lastet inn")
                print(f"    Tabell: {table_name}")
                if staging_schema:
                    print(f"==> Flytter staging-schema {staging_schema} til public ...")
                    move_schema_objects(db_params, staging_schema, 'public')
                    grant_privileges_for_schema(db_params, 'public')
                # Analyze imported table
                analyze_tables(db_params, tables=[f'public.{table_name}'])
                return True
            else:
                print("Feil: Kunne ikke laste inn GML-filer", file=sys.stderr)
                return False

        elif format_type == 'FGDB':
            if load_fgdb(db_params, files, target_srid, staging_schema):
                print(f"==> Ferdig. {len(files)} FGDB-katalog(er) lastet inn")
                print("==> Flytter staging-schema til public ...")
                if staging_schema:
                    move_schema_objects(db_params, staging_schema, 'public')
                    grant_privileges_for_schema(db_params, 'public')
                print("==> Bygger manglende spatial-indekser ...")
                create_missing_spatial_indexes(db_params)
                # Analyze imported tables (analyze public schema after move)
                analyze_tables(db_params, schemas=['public'])
                return True
            else:
                print("Feil: Kunne ikke laste inn FGDB", file=sys.stderr)
                return False

        elif format_type == 'OSM':
            if len(files) > 0:
                # Use first OSM file (typically only one)
                osm_file = files[0]
                if load_osm_pbf(db_params, osm_file, target_srid, staging_schema, drop_tables):
                    print(f"==> Ferdig. OSM PBF fil lastet inn")
                    if staging_schema:
                        print(f"==> Flytter staging-schema {staging_schema} til public ...")
                        move_schema_objects(db_params, staging_schema, 'public')
                        grant_privileges_for_schema(db_params, 'public')
                    print("==> Bygger manglende spatial-indekser ...")
                    create_missing_spatial_indexes(db_params)
                    analyze_tables(db_params, schemas=['public'])
                    return True
                else:
                    print("Feil: Kunne ikke laste inn OSM PBF", file=sys.stderr)
                    return False
            else:
                print("Feil: Ingen OSM PBF filer funnet", file=sys.stderr)
                return False

        return False

    finally:
        # Clean up extracted files
        if extract_dir.exists():
            import shutil
            shutil.rmtree(extract_dir)


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Load dataset into PostGIS database')
    parser.add_argument('zip_file', help='Path to ZIP file')
    parser.add_argument('database', help='Database name')
    parser.add_argument('table_name', nargs='?', default=None,
                       help='Table name (required for GML, auto-detected for PostGIS SQL)')
    parser.add_argument('target_srid', nargs='?', type=int, default=None,
                       help='Target SRID for transformation (default: 25833)')
    parser.add_argument('--drop-tables', action='store_true',
                       help='Drop existing tables before loading (PostGIS SQL only)')
    parser.add_argument('--no-stream', action='store_true',
                       help='Disable streaming mode (extract ZIP to disk first)')

    args = parser.parse_args()

    zip_path = Path(args.zip_file)
    success = load_dataset(
        zip_path,
        args.database,
        args.table_name,
        args.target_srid,
        args.drop_tables,
        stream=not args.no_stream  # Stream by default
    )

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
