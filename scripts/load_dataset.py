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
"""

import os
import sys
import zipfile
import subprocess
import re
import argparse
from pathlib import Path
from typing import Optional, List


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


def detect_format(extract_dir: Path) -> tuple[str, List[Path]]:
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

    # Could not detect format
    return (None, [])


def detect_format_from_zip(zip_path: Path) -> tuple[Optional[str], List[str]]:
    """Detect dataset format by examining ZIP contents without extracting.

    Returns:
        Tuple of (format_type, list of file paths in ZIP)
    """
    if zip_path is None:
        return None, []

    sql_files = []
    gml_files = []

    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for name in zip_ref.namelist():
                if name.endswith('.sql'):
                    sql_files.append(name)
                elif name.endswith('.gml'):
                    gml_files.append(name)
    except zipfile.BadZipFile:
        return None, []

    if sql_files:
        return ('PostGIS', sql_files)
    elif gml_files:
        return ('GML', gml_files)
    else:
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

    cmd = ['psql']
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


def extract_table_names_from_sql(sql_file: Path) -> tuple[List[str], Optional[str]]:
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
    cmd = ['psql']
    if db_params.get('host'):
        cmd.extend(['-h', db_params['host']])
    if db_params.get('port'):
        cmd.extend(['-p', str(db_params['port'])])
    cmd.extend(['-U', db_params['user'], '-d', db_params['database'], '-q', '-t'])

    # Query for schemas matching the prefix pattern
    # Exclude system schemas
    find_schemas_sql = f"""
        SELECT nspname
        FROM pg_namespace
        WHERE nspname LIKE '{schema_prefix}_%'
        AND nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
        ORDER BY nspname;
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
        drop_sql = '; '.join([f'DROP SCHEMA IF EXISTS "{name}" CASCADE' for name in schema_names]) + ';'

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

    cmd = ['psql']
    if db_params.get('host'):
        cmd.extend(['-h', db_params['host']])
    if db_params.get('port'):
        cmd.extend(['-p', str(db_params['port'])])
    cmd.extend(['-U', db_params['user'], '-d', db_params['database'], '-q'])

    # Build SQL to drop tables
    drop_sql = '; '.join([f'DROP TABLE IF EXISTS {name} CASCADE' for name in table_names]) + ';'

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


def extract_table_names_from_zip_sql(zip_path: Path, sql_file_in_zip: str) -> tuple[List[str], Optional[str]]:
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

    env = os.environ.copy()
    if db_params.get('password'):
        env['PGPASSWORD'] = db_params['password']

    # Build psql command
    psql_cmd = ['psql']
    if db_params.get('host'):
        psql_cmd.extend(['-h', db_params['host']])
    if db_params.get('port'):
        psql_cmd.extend(['-p', str(db_params['port'])])
    psql_cmd.extend(['-U', db_params['user'], '-d', db_params['database']])

    success_count = 0
    for sql_file in sql_files:
        print(f"  -> Laster {sql_file} (direkte fra ZIP) ...")

        try:
            # Read from ZIP and pipe to psql
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                with zip_ref.open(sql_file) as zip_file:
                    psql_proc = subprocess.run(
                        psql_cmd,
                        stdin=zip_file,
                        env=env,
                        capture_output=True,
                        text=False,  # Binary mode for stdin
                        check=True
                    )

            success_count += 1
            print(f"     ✓ Lastet")
        except subprocess.CalledProcessError as e:
            print(f"     ✗ Feil ved lasting: {e.stderr.decode() if e.stderr else str(e)}", file=sys.stderr)
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

        cmd = ['psql']
        if db_params.get('host'):
            cmd.extend(['-h', db_params['host']])
        if db_params.get('port'):
            cmd.extend(['-p', str(db_params['port'])])
        cmd.extend(['-U', db_params['user'], '-d', db_params['database'], '-f', str(sql_file)])

        try:
            subprocess.run(cmd, env=env, check=True)
            success_count += 1
            print(f"     ✓ Lastet")
        except subprocess.CalledProcessError as e:
            print(f"     ✗ Feil ved lasting: {e}", file=sys.stderr)
            return False

    return success_count > 0


def check_ogr2ogr() -> bool:
    """Check if ogr2ogr is available."""
    try:
        subprocess.run(['ogr2ogr', '--version'], capture_output=True, check=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


def load_gml_from_zip_stream(
    db_params: dict,
    zip_path: Path,
    gml_files: List[str],
    table_name: str,
    target_srid: Optional[int],
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

        cmd = [
            'ogr2ogr',
            '-f', 'PostgreSQL',
            conn_str,
            vsi_path,
            '-nln', table_name,
            '-lco', 'GEOMETRY_NAME=geom',
            '-lco', 'SPATIAL_INDEX=GIST',
            '-lco', 'LAUNDER=YES',  # Better column name handling for complex GML
            '-lco', 'FID=ogc_fid',  # Ensure unique feature ID column
            '-lco', 'PROMOTE_TO_MULTI=YES',  # Convert nested structures to arrays to avoid duplicate columns
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


def load_gml_files(db_params: dict, gml_files: List[Path], table_name: str, target_srid: Optional[int], append: bool = False) -> bool:
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

        cmd = [
            'ogr2ogr',
            '-f', 'PostgreSQL',
            conn_str,
            str(gml_file),
            '-nln', table_name,
            '-lco', 'GEOMETRY_NAME=geom',
            '-lco', 'SPATIAL_INDEX=GIST',
            '-lco', 'LAUNDER=YES',  # Better column name handling for complex GML
            '-lco', 'FID=ogc_fid',  # Ensure unique feature ID column
            '-lco', 'PROMOTE_TO_MULTI=YES',  # Convert nested structures to arrays to avoid duplicate columns
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


def load_dataset(
    zip_path: Path,
    database: str,
    table_name: Optional[str] = None,
    target_srid: Optional[int] = None,
    drop_tables: bool = False,
    stream: bool = True,
    append: bool = False
) -> bool:
    """Load dataset from ZIP file into PostGIS database.

    Args:
        zip_path: Path to ZIP file
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

    if target_srid is None:
        target_srid = int(os.environ.get('TARGET_SRID', '25833'))
    if table_name is None:
        table_name = zip_path.stem.lower().replace('-', '_')

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

            # Load based on format (streaming)
            print(f"==> Laster data direkte fra ZIP inn i database '{db_params['database']}' ...")

            if format_type == 'PostGIS':
                if load_postgis_sql_from_zip_stream(db_params, zip_path, files_in_zip, drop_tables):
                    print(f"==> Ferdig. {len(files_in_zip)} SQL-fil(er) lastet inn (uten ekstraksjon)")
                    return True
                else:
                    print("  ⚠ Streaming feilet, prøver med ekstraksjon...", file=sys.stderr)
                    # Fall through to extraction method
            elif format_type == 'GML':
                print(f"    Tabell: {table_name}")
                print(f"    Transformering til EPSG:{target_srid}")
                if append:
                    print(f"    Modus: Legger til eksisterende tabell")

                if load_gml_from_zip_stream(db_params, zip_path, files_in_zip, table_name, target_srid, append):
                    print(f"==> Ferdig. {len(files_in_zip)} GML-fil(er) lastet inn (uten ekstraksjon)")
                    print(f"    Tabell: {table_name}")
                    return True
                else:
                    print("  ⚠ Streaming feilet, prøver med ekstraksjon...", file=sys.stderr)
                    # Fall through to extraction method

    # Fallback: Extract ZIP file (original method)
    extract_dir = zip_path.parent / f"{zip_path.stem}_extracted"

    try:
        if not extract_zip(zip_path, extract_dir):
            return False

        # Detect format
        format_type, files = detect_format(extract_dir)

        if not format_type:
            print(f"Feil: Kunne ikke detektere format i {zip_path}", file=sys.stderr)
            print("Forventet enten SQL-filer (.sql) eller GML-filer (.gml)", file=sys.stderr)
            return False

        print(f"  ✓ Detektert format: {format_type}")
        print(f"  ✓ Fant {len(files)} fil(er)")

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
                return True
            else:
                print("Feil: Kunne ikke laste inn SQL-filer", file=sys.stderr)
                return False

        elif format_type == 'GML':
            print(f"    Tabell: {table_name}")
            print(f"    Transformering til EPSG:{target_srid}")

            if load_gml_files(db_params, files, table_name, target_srid, append):
                print(f"==> Ferdig. {len(files)} GML-fil(er) lastet inn")
                print(f"    Tabell: {table_name}")
                return True
            else:
                print("Feil: Kunne ikke laste inn GML-filer", file=sys.stderr)
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

