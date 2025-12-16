#!/usr/bin/env python3
"""
Cron-friendly script to update all datasets from configuration file.

This script:
1. Downloads updated datasets (only if newer versions are available)
2. Loads them into PostGIS database (replacing old data)
3. Logs everything for monitoring

Usage:
    python3 scripts/update_datasets.py [config_file] [database_name]

Environment variables:
    PGDATABASE - Database name (default: matrikkel)
    LOG_DIR    - Directory for logs (default: ./logs)

For cron, add to crontab:
    0 2 * * * /path/to/python3 /path/to/stiflyt-db/scripts/update_datasets.py /path/to/datasets.yaml >> /path/to/logs/cron.log 2>&1
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

try:
    import yaml
except ImportError:
    print("Feil: PyYAML ikke installert. Installer med: pip install pyyaml", file=sys.stderr)
    sys.exit(1)

# Import functions directly instead of using subprocess
try:
    # Add scripts directory to path for imports
    scripts_dir = Path(__file__).parent
    sys.path.insert(0, str(scripts_dir))

    from download_kartverket import download_from_config
    from load_dataset import load_dataset, extract_table_names_from_zip_sql, detect_format_from_zip
    from db_status import check_database_health, get_db_connection_params, connect_db
except ImportError as e:
    print(f"Feil: Kunne ikke importere nødvendige moduler: {e}", file=sys.stderr)
    sys.exit(1)


def setup_logging(log_dir: Path) -> Path:
    """Setup logging directory and return log file path."""
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f"update_{timestamp}.log"
    return log_file


def log(message: str, log_file: Path, also_print: bool = True):
    """Log message to file and optionally print."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_line = f"[{timestamp}] {message}\n"
    with open(log_file, 'a') as f:
        f.write(log_line)
    if also_print:
        print(message)


def load_config(config_path: Path) -> List[Dict[str, Any]]:
    """Load YAML configuration file."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"Feil: Kunne ikke laste konfigurasjonsfil: {e}", file=sys.stderr)
        sys.exit(1)


def check_table_exists_and_modified(conn, table_name: str) -> Tuple[bool, Optional[float]]:
    """Check if table exists and get its last modification time.

    Uses pg_stat_user_tables which tracks vacuum/analyze times as a proxy
    for modification time. If table was recently loaded, these will be recent.

    Args:
        conn: Database connection
        table_name: Table name (can be schema.table or just table)

    Returns:
        Tuple of (exists, modification_timestamp) where timestamp is Unix time or None
    """
    try:
        # Parse schema.table or just table
        if '.' in table_name:
            schema, table = table_name.split('.', 1)
        else:
            schema = 'public'
            table = table_name

        with conn.cursor() as cur:
            # Check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                )
            """, (schema, table))

            exists = cur.fetchone()[0]
            if not exists:
                return False, None

            # Get last modification time from pg_stat_user_tables
            # This tracks vacuum/analyze times which are good proxies for when data was loaded
            cur.execute("""
                SELECT GREATEST(
                    COALESCE(last_vacuum, '1970-01-01'::timestamp),
                    COALESCE(last_autovacuum, '1970-01-01'::timestamp),
                    COALESCE(last_analyze, '1970-01-01'::timestamp),
                    COALESCE(last_autoanalyze, '1970-01-01'::timestamp)
                )
                FROM pg_stat_user_tables
                WHERE schemaname = %s AND relname = %s
            """, (schema, table))

            result = cur.fetchone()
            if result and result[0]:
                # Convert to Unix timestamp
                return True, result[0].timestamp()

            # Fallback: table exists but no stats (very new table or stats disabled)
            # Use current time as conservative estimate
            return True, datetime.now().timestamp()

    except Exception as e:
        # If we can't check, assume table doesn't exist (safer to import)
        return False, None


def verify_imported_data(database: str, configs: List[Dict[str, Any]], log_file: Path) -> bool:
    """Verify that imported tables have data (row count > 0).

    Args:
        database: Database name
        configs: List of dataset configurations
        log_file: Log file path

    Returns:
        True if all checks pass, False otherwise
    """
    db_params = get_db_connection_params()
    db_params['database'] = database
    conn = connect_db(db_params)

    if not conn:
        log("  ✗ Cannot connect to database for sanity checks", log_file)
        return False

    all_checks_passed = True

    try:
        with conn.cursor() as cur:
            for cfg in configs:
                name = cfg.get('name', 'unknown')
                format_type = cfg.get('format', '')

                # Determine expected table names based on format and dataset name
                expected_tables = []

                if format_type == 'PostGIS':
                    # For PostGIS, we need to check what tables were actually created
                    # This is tricky without knowing the schema prefix
                    # Check for tables in public schema that might be from this dataset
                    cur.execute("""
                        SELECT tablename
                        FROM pg_tables
                        WHERE schemaname = 'public'
                        ORDER BY tablename
                    """)
                    all_tables = [row[0] for row in cur.fetchall()]

                    # For known datasets, check specific table patterns
                    if 'teig' in name.lower() or 'matrikkel' in name.lower():
                        # Matrikkel typically has tables like teig, eiendom, etc.
                        expected_tables = [t for t in all_tables if any(keyword in t.lower() for keyword in ['teig', 'eiendom', 'matrikkel'])]
                    elif 'turrute' in name.lower() or 'friluft' in name.lower():
                        # Turrutebasen has tables in a schema with prefix
                        # Check for common table names
                        expected_tables = [t for t in all_tables if any(keyword in t.lower() for keyword in ['rute', 'friluft', 'tur'])]
                    else:
                        # For other PostGIS datasets, check all tables in public
                        expected_tables = all_tables[:5]  # Limit to first 5 tables

                elif format_type == 'GML':
                    # GML uses a single table with dataset name
                    table_name = name.lower().replace('-', '_').replace(' ', '_')
                    expected_tables = [table_name]

                elif format_type == 'FGDB':
                    # FGDB can have multiple tables, check common patterns
                    table_name = name.lower().replace('-', '_').replace(' ', '_')
                    # Check for tables that might be from this dataset
                    cur.execute("""
                        SELECT tablename
                        FROM pg_tables
                        WHERE schemaname = 'public'
                          AND (tablename LIKE %s OR tablename LIKE %s)
                        ORDER BY tablename
                    """, (f'%{table_name}%', f'%{name.lower()}%'))
                    expected_tables = [row[0] for row in cur.fetchall()]

                    # If no specific tables found, check all tables in public
                    if not expected_tables:
                        cur.execute("""
                            SELECT tablename
                            FROM pg_tables
                            WHERE schemaname = 'public'
                            ORDER BY tablename
                        """)
                        expected_tables = [row[0] for row in cur.fetchall()][:10]  # Limit to first 10

                # Verify each expected table has data
                for table in expected_tables:
                    try:
                        # Use psycopg2.sql.Identifier for safe identifier quoting
                        from psycopg2 import sql
                        query = sql.SQL("SELECT COUNT(*) FROM public.{}").format(
                            sql.Identifier(table)
                        )
                        cur.execute(query)
                        row_count = cur.fetchone()[0]

                        if row_count == 0:
                            log(f"    ✗ Table {table} is empty (0 rows)", log_file)
                            all_checks_passed = False
                        else:
                            log(f"    ✓ Table {table}: {row_count:,} rows", log_file)
                    except Exception as e:
                        log(f"    ⚠ Could not check table {table}: {e}", log_file)
                        # Don't fail on this, might be a view or permission issue

                # If no expected tables found, log a warning
                if not expected_tables:
                    log(f"    ⚠ No tables found to verify for {name} ({format_type})", log_file)

    except Exception as e:
        log(f"  ✗ Error during sanity checks: {e}", log_file)
        all_checks_passed = False
    finally:
        conn.close()

    return all_checks_passed


def check_import_needed(
    zip_file: Path,
    database: str,
    format_type: str,
    table_name: Optional[str] = None
) -> Tuple[bool, str]:
    """Check if import is needed by comparing ZIP file time with database table modification time.

    Args:
        zip_file: Path to ZIP file
        database: Database name
        format_type: Format type ('PostGIS' or 'GML')
        table_name: Table name (for GML format)

    Returns:
        Tuple of (import_needed, reason)
    """
    if not zip_file.exists():
        return True, "ZIP file does not exist"

    zip_mtime = zip_file.stat().st_mtime

    # Get database connection
    db_params = get_db_connection_params()
    db_params['database'] = database
    conn = connect_db(db_params)

    if not conn:
        # Can't connect to database, assume import is needed
        return True, "Cannot connect to database to check"

    try:
        if format_type == 'PostGIS':
            # Extract table names from SQL files in ZIP
            format_detected, sql_files = detect_format_from_zip(zip_file)
            if format_detected != 'PostGIS' or not sql_files:
                return True, "Cannot detect PostGIS format or no SQL files found"

            # Get table names from first SQL file
            table_names, _ = extract_table_names_from_zip_sql(zip_file, sql_files[0])

            if not table_names:
                return True, "Cannot extract table names from SQL file"

            # Check if all tables exist and are up-to-date
            all_exist = True
            all_up_to_date = True
            oldest_table_time = None

            for table in table_names:
                exists, mod_time = check_table_exists_and_modified(conn, table)
                if not exists:
                    all_exist = False
                    break
                if mod_time:
                    if oldest_table_time is None or mod_time < oldest_table_time:
                        oldest_table_time = mod_time
                    if zip_mtime > mod_time:
                        all_up_to_date = False

            if not all_exist:
                return True, f"Tables {', '.join(table_names)} do not exist"

            if not all_up_to_date:
                return True, f"ZIP file ({datetime.fromtimestamp(zip_mtime)}) is newer than tables ({datetime.fromtimestamp(oldest_table_time) if oldest_table_time else 'unknown'})"

            return False, f"Tables {', '.join(table_names)} are up-to-date"

        elif format_type == 'GML':
            if not table_name:
                return True, "Table name required for GML format"

            exists, mod_time = check_table_exists_and_modified(conn, table_name)

            if not exists:
                return True, f"Table {table_name} does not exist"

            if mod_time and zip_mtime > mod_time:
                return True, f"ZIP file ({datetime.fromtimestamp(zip_mtime)}) is newer than table ({datetime.fromtimestamp(mod_time)})"

            return False, f"Table {table_name} is up-to-date"

        elif format_type == 'FGDB':
            # For FGDB, we don't know table names in advance without extracting
            # Check if any tables in public schema are newer than ZIP file
            # This is a heuristic - if all tables are newer, assume import not needed

            with conn.cursor() as cur:
                # Get all tables in public schema with their modification times
                cur.execute("""
                    SELECT relname,
                           GREATEST(
                               COALESCE(last_vacuum, '1970-01-01'::timestamp),
                               COALESCE(last_autovacuum, '1970-01-01'::timestamp),
                               COALESCE(last_analyze, '1970-01-01'::timestamp),
                               COALESCE(last_autoanalyze, '1970-01-01'::timestamp)
                           ) as mod_time
                    FROM pg_stat_user_tables
                    WHERE schemaname = 'public'
                    ORDER BY mod_time DESC
                """)

                results = cur.fetchall()

                if not results:
                    # No tables in public schema, need import
                    return True, "No tables found in public schema"

                # Check if any table is older than ZIP file
                # If all tables are newer than ZIP, assume import not needed
                all_newer = True
                oldest_table_time = None
                table_names = []

                for table_name, mod_time in results:
                    table_names.append(table_name)
                    if mod_time:
                        mod_timestamp = mod_time.timestamp()
                        if oldest_table_time is None or mod_timestamp < oldest_table_time:
                            oldest_table_time = mod_timestamp
                        if zip_mtime > mod_timestamp:
                            all_newer = False
                            break

                if all_newer and oldest_table_time:
                    return False, f"All tables in public schema ({len(table_names)} tables) are newer than ZIP file ({datetime.fromtimestamp(zip_mtime)})"
                elif oldest_table_time:
                    return True, f"ZIP file ({datetime.fromtimestamp(zip_mtime)}) is newer than oldest table ({datetime.fromtimestamp(oldest_table_time)})"
                else:
                    # No modification times available, assume import needed
                    return True, "Cannot determine table modification times"

        else:
            return True, f"Unknown format type: {format_type}"

    finally:
        conn.close()


def download_datasets(config_path: Path, log_file: Path) -> bool:
    """Download datasets using download function."""
    log("==> Downloading datasets...", log_file)

    try:
        # Capture stdout/stderr by redirecting temporarily
        import io
        from contextlib import redirect_stdout, redirect_stderr

        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()

        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            download_from_config(config_path)

        # Write captured output to log
        stdout_output = stdout_capture.getvalue()
        stderr_output = stderr_capture.getvalue()

        with open(log_file, 'a') as f:
            if stdout_output:
                f.write(stdout_output)
            if stderr_output:
                f.write(stderr_output)

        log("✓ Download completed", log_file)
        return True
    except Exception as e:
        log(f"✗ Download failed: {e}", log_file)
        return False


def load_postgis_dataset(zip_file: Path, database: str, log_file: Path) -> bool:
    """Load PostGIS SQL dataset."""
    try:
        # Validate inputs
        if zip_file is None:
            log(f"✗ Feil: zip_file er None", log_file)
            return False

        if not isinstance(zip_file, Path):
            zip_file = Path(zip_file)

        zip_file = zip_file.resolve()

        if not zip_file.exists():
            log(f"✗ Feil: ZIP-fil eksisterer ikke: {zip_file}", log_file)
            return False

        import io
        from contextlib import redirect_stdout, redirect_stderr

        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()

        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            success = load_dataset(zip_file, database, drop_tables=True)

        # Write captured output to log
        stdout_output = stdout_capture.getvalue()
        stderr_output = stderr_capture.getvalue()

        with open(log_file, 'a') as f:
            if stdout_output:
                f.write(stdout_output)
            if stderr_output:
                f.write(stderr_output)

        return success
    except Exception as e:
        log(f"✗ Failed to load PostGIS dataset: {e}", log_file)
        return False


def load_gml_dataset(zip_file: Path, database: str, table_name: str, srid: int, log_file: Path, append: bool = False) -> bool:
    """Load GML dataset.

    Args:
        zip_file: Path to ZIP file
        database: Database name
        table_name: Table name
        srid: Target SRID
        log_file: Log file path
        append: If True, append to existing table instead of overwriting
    """
    try:
        # Validate inputs
        if zip_file is None:
            log(f"✗ Feil: zip_file er None", log_file)
            return False

        if not isinstance(zip_file, Path):
            zip_file = Path(zip_file)

        zip_file = zip_file.resolve()

        if not zip_file.exists():
            log(f"✗ Feil: ZIP-fil eksisterer ikke: {zip_file}", log_file)
            return False

        import io
        from contextlib import redirect_stdout, redirect_stderr

        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()

        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            success = load_dataset(zip_file, database, table_name=table_name, target_srid=srid, append=append)

        # Write captured output to log
        stdout_output = stdout_capture.getvalue()
        stderr_output = stderr_capture.getvalue()

        with open(log_file, 'a') as f:
            if stdout_output:
                f.write(stdout_output)
            if stderr_output:
                f.write(stderr_output)

        return success
    except Exception as e:
        log(f"✗ Failed to load GML dataset: {e}", log_file)
        return False


def load_fgdb_dataset(zip_file: Path, database: str, srid: int, log_file: Path) -> bool:
    """Load FGDB dataset using unified loader."""
    try:
        if zip_file is None:
            log(f"✗ Feil: zip_file er None", log_file)
            return False

        if not isinstance(zip_file, Path):
            zip_file = Path(zip_file)

        zip_file = zip_file.resolve()

        if not zip_file.exists():
            log(f"✗ Feil: ZIP-fil eksisterer ikke: {zip_file}", log_file)
            return False

        import io
        from contextlib import redirect_stdout, redirect_stderr

        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()

        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            success = load_dataset(zip_file, database, target_srid=srid)

        stdout_output = stdout_capture.getvalue()
        stderr_output = stderr_capture.getvalue()

        with open(log_file, 'a') as f:
            if stdout_output:
                f.write(stdout_output)
            if stderr_output:
                f.write(stderr_output)

        return success
    except Exception as e:
        log(f"✗ Failed to load FGDB dataset: {e}", log_file)
        return False


def main():
    """Main function."""
    import argparse

    parser = argparse.ArgumentParser(description='Update datasets from configuration file')
    parser.add_argument('config_file', nargs='?', default='datasets.yaml',
                       help='Path to YAML configuration file (default: datasets.yaml)')
    parser.add_argument('database', nargs='?', default=None,
                       help='Database name (default: from PGDATABASE env or matrikkel)')
    parser.add_argument('--log-dir', default='./logs',
                       help='Directory for log files (default: ./logs)')

    args = parser.parse_args()

    # Setup
    config_path = Path(args.config_file)
    database = args.database or os.environ.get('PGDATABASE', 'matrikkel')
    log_dir = Path(args.log_dir)

    if not config_path.exists():
        print(f"Feil: Konfigurasjonsfil ikke funnet: {config_path}", file=sys.stderr)
        sys.exit(1)

    # Setup logging
    log_file = setup_logging(log_dir)
    log("=== Starting dataset update ===", log_file)
    log(f"Config file: {config_path}", log_file)
    log(f"Database: {database}", log_file)
    log(f"Log file: {log_file}", log_file)

    # Load configuration
    configs = load_config(config_path)
    log(f"Found {len(configs)} datasets in configuration", log_file)

    # Log feed URL information for each dataset (from config)
    log("==> Feed URL configuration:", log_file)
    for cfg in configs:
        name = cfg.get('name', 'unknown')
        dataset_name = cfg.get('dataset', '')
        format_pref = cfg.get('format', 'PostGIS')
        feed_url_override = cfg.get('feed_url', None)

        if feed_url_override:
            log(f"  [{name}] Feed URL override: {feed_url_override}", log_file)
        else:
            log(f"  [{name}] Feed URL: will be discovered from catalog (dataset: {dataset_name})", log_file)
        log(f"  [{name}] Format preference: {format_pref}", log_file)

    # Download datasets
    if not download_datasets(config_path, log_file):
        log("ERROR: Download failed - aborting", log_file)
        sys.exit(1)

    # Load each dataset
    log("==> Loading datasets into database...", log_file)
    success_count = 0
    failed_count = 0

    for cfg in configs:
        name = cfg.get('name', 'unknown')
        dataset = cfg.get('dataset', '')
        format_type = cfg.get('format', '')
        output_dir_str = cfg.get('output_dir', './data')
        # Resolve relative paths to absolute
        output_dir = Path(output_dir_str).resolve()
        utm_zone = cfg.get('utm_zone', '25833')

        log(f"  -> Processing {name} ({format_type} format)...", log_file)

        # Find ZIP files in output directory
        zip_files = list(output_dir.glob('*.zip'))

        if not zip_files:
            log(f"    ⚠ No ZIP files found in {output_dir} - skipping", log_file)
            failed_count += 1
            continue

        # Determine table name for GML format (needed for import check)
        gml_table_name = None
        if format_type == 'GML':
            gml_table_name = name.lower().replace('-', '_').replace(' ', '_')

        # For PostGIS format or single-file GML: use most recent ZIP
        # For multi-file GML datasets (like FKB-TraktorvegSti with municipalities): process all ZIPs
        is_multi_file_gml = (format_type == 'GML' and len(zip_files) > 10)
        # Threshold: if more than 10 ZIP files, assume it's a multi-file dataset

        if is_multi_file_gml:
            log(f"    ℹ Found {len(zip_files)} ZIP files (multi-file dataset), processing all...", log_file)
            # Sort by modification time for consistent processing
            zip_files = sorted(zip_files, key=lambda p: p.stat().st_mtime)

            # Check if import is needed (check first file as representative)
            first_zip = zip_files[0]
            if not isinstance(first_zip, Path):
                first_zip = Path(first_zip)
            first_zip = first_zip.resolve()

            import_needed, reason = check_import_needed(first_zip, database, format_type, gml_table_name)

            if not import_needed:
                log(f"    ⊙ Skipping import: {reason}", log_file)
                success_count += 1
                continue

            log(f"    → Import needed: {reason}", log_file)

            # Process all ZIP files for multi-file GML dataset
            table_name = gml_table_name
            if isinstance(utm_zone, int):
                srid = utm_zone
            elif isinstance(utm_zone, str) and utm_zone.isdigit():
                srid = int(utm_zone)
            else:
                srid = 25833  # Default

            # Load first file with overwrite, then append others
            success = True
            for idx, zip_file in enumerate(zip_files):
                if not isinstance(zip_file, Path):
                    zip_file = Path(zip_file)
                zip_file = zip_file.resolve()

                if idx == 0:
                    # First file: overwrite table
                    if not load_gml_dataset(zip_file, database, table_name, srid, log_file, append=False):
                        success = False
                        break
                else:
                    # Subsequent files: append to table
                    if not load_gml_dataset(zip_file, database, table_name, srid, log_file, append=True):
                        log(f"    ⚠ Failed to append {zip_file.name}, continuing with next file...", log_file)
                        # Continue with other files even if one fails

            if success:
                log(f"    ✓ {name} loaded successfully ({len(zip_files)} files)", log_file)
                success_count += 1
            else:
                log(f"    ✗ Failed to load {name}", log_file)
                failed_count += 1

        else:
            # Single file or PostGIS: use most recent ZIP
            if len(zip_files) > 1:
                zip_files = sorted(zip_files, key=lambda p: p.stat().st_mtime, reverse=True)
                log(f"    ℹ Found {len(zip_files)} ZIP files, using most recent: {zip_files[0].name}", log_file)
                zip_files = zip_files[:1]

            zip_file = zip_files[0]
            if not isinstance(zip_file, Path):
                zip_file = Path(zip_file)
            zip_file = zip_file.resolve()

            # Check if import is needed
            import_needed, reason = check_import_needed(zip_file, database, format_type, gml_table_name)

            if not import_needed:
                log(f"    ⊙ Skipping import: {reason}", log_file)
                success_count += 1
                continue

            log(f"    → Import needed: {reason}", log_file)

            if format_type == 'PostGIS':
                if load_postgis_dataset(zip_file, database, log_file):
                    log(f"    ✓ {name} loaded successfully", log_file)
                    success_count += 1
                else:
                    log(f"    ✗ Failed to load {name}", log_file)
                    failed_count += 1

            elif format_type == 'GML':
                table_name = gml_table_name
                if isinstance(utm_zone, int):
                    srid = utm_zone
                elif isinstance(utm_zone, str) and utm_zone.isdigit():
                    srid = int(utm_zone)
                else:
                    srid = 25833  # Default

                if load_gml_dataset(zip_file, database, table_name, srid, log_file):
                    log(f"    ✓ {name} loaded successfully", log_file)
                    success_count += 1
                else:
                    log(f"    ✗ Failed to load {name}", log_file)
                    failed_count += 1

            elif format_type == 'FGDB':
                if isinstance(utm_zone, int):
                    srid = utm_zone
                elif isinstance(utm_zone, str) and utm_zone.isdigit():
                    srid = int(utm_zone)
                else:
                    srid = 25833  # Default

                if load_fgdb_dataset(zip_file, database, srid, log_file):
                    log(f"    ✓ {name} loaded successfully", log_file)
                    success_count += 1
                else:
                    log(f"    ✗ Failed to load {name}", log_file)
                    failed_count += 1

            else:
                log(f"    ⚠ Unknown format '{format_type}' - skipping", log_file)
                failed_count += 1

    # Summary
    log("==> Update completed", log_file)
    log(f"  ✓ Successful: {success_count}", log_file)
    log(f"  ✗ Failed: {failed_count}", log_file)

    # Sanity checks: verify imported tables have data before running migrations
    if success_count > 0:
        log("==> Running sanity checks on imported data...", log_file)
        sanity_ok = verify_imported_data(database, configs, log_file)
        if not sanity_ok:
            log("  ✗ Sanity checks failed - aborting migrations", log_file)
            log("  ⚠ Database may be in inconsistent state", log_file)
            sys.exit(1)
        log("  ✓ Sanity checks passed", log_file)
    else:
        log("  ⊙ No successful imports - skipping sanity checks", log_file)

    # Run migrations after data import
    log("==> Running database migrations...", log_file)
    try:
        # Import migration functions
        from run_migrations import find_migration_files, run_migration, get_db_connection_params as get_migration_db_params

        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        migration_dir = project_root / 'migrations'

        migration_files = find_migration_files(migration_dir)
        if migration_files:
            log(f"  Found {len(migration_files)} migration(s)", log_file)
            migration_db_params = get_migration_db_params()
            migration_db_params['database'] = database

            migration_success = 0
            migration_failed = 0
            for migration_file in migration_files:
                log(f"  -> Running {migration_file.name}...", log_file)
                if run_migration(migration_db_params, migration_file):
                    log(f"     ✓ {migration_file.name} completed", log_file)
                    migration_success += 1
                else:
                    log(f"     ✗ {migration_file.name} failed", log_file)
                    migration_failed += 1

            if migration_success > 0:
                log(f"  ✓ Migrations completed: {migration_success} successful", log_file)
            if migration_failed > 0:
                log(f"  ⚠ Some migrations failed: {migration_failed} failed", log_file)
        else:
            log("  ℹ No migrations found", log_file)
    except ImportError as e:
        log(f"  ⚠ Could not import migration runner: {e}", log_file)
        log("  ⚠ Skipping migrations", log_file)
    except Exception as e:
        log(f"  ⚠ Migration execution failed: {e}", log_file)
        log("  ⚠ Continuing with health check", log_file)

    # Post-update health check
    log("==> Verifying database health...", log_file)
    try:
        from db_status import get_db_connection_params
        db_params = get_db_connection_params()
        db_params['database'] = database

        is_healthy, status = check_database_health(db_params, min_tables=len(configs))

        if is_healthy:
            log("  ✓ Database health check passed", log_file)
            log(f"  Tables: {status['table_count']}", log_file)
            if status['database_size']:
                log(f"  Database size: {status['database_size']}", log_file)
        else:
            log("  ✗ Database health check failed", log_file)
            for error in status.get('errors', []):
                log(f"    • {error}", log_file)
            log("  ⚠ Database may be in inconsistent state", log_file)
    except Exception as e:
        log(f"  ⚠ Health check failed: {e}", log_file)

    log("=== End of update ===", log_file)

    # Clean up old logs (keep last 30 days)
    try:
        from datetime import timedelta
        cutoff_date = datetime.now() - timedelta(days=30)
        for old_log in log_dir.glob('update_*.log'):
            if old_log.stat().st_mtime < cutoff_date.timestamp():
                old_log.unlink()
    except Exception:
        pass  # Ignore cleanup errors

    sys.exit(0 if failed_count == 0 else 1)


if __name__ == "__main__":
    main()

