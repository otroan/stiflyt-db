#!/usr/bin/env python3
"""
Run database migrations after data import.

This script executes SQL migration files from the migrations/ directory.
Migrations are run in alphabetical order and are idempotent (safe to run multiple times).

Usage:
    python3 scripts/run_migrations.py [database_name] [--migration-dir migrations]

Environment variables:
    PGHOST       - PostgreSQL host (default: localhost)
    PGPORT       - PostgreSQL port (default: 5432)
    PGDATABASE   - Database name (can also be passed as argument)
    PGUSER       - PostgreSQL user (default: current user)
    PGPASSWORD   - PostgreSQL password (if needed)
"""

import os
import sys
import subprocess
import argparse
import re
from pathlib import Path
from typing import List, Optional, Tuple, Dict
from update_datasets import load_config

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


def find_migration_files(migration_dir: Path) -> List[Path]:
    """Find all SQL migration files in the migrations directory.

    Returns:
        List of migration file paths, sorted alphabetically
    """
    if not migration_dir.exists():
        return []

    migration_files = sorted(migration_dir.glob('*.sql'))
    return migration_files


def check_links_table_exists(db_params: dict) -> bool:
    """Check if links table exists in the latest turrutebasen schema.

    Args:
        db_params: Database connection parameters

    Returns:
        True if links table exists, False otherwise
    """
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
        '-t',  # Tuples only (no headers)
        '-A',  # Unaligned output
        '-c', """
            WITH latest_schema AS (
                SELECT nspname
                FROM pg_namespace
                WHERE nspname LIKE 'turogfriluftsruter_%'
                  AND nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
                ORDER BY nspname DESC
                LIMIT 1
            )
            SELECT COALESCE((
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables t
                    JOIN latest_schema s ON t.table_schema = s.nspname
                    WHERE t.table_name = 'links'
                )
            ), false);
        """
    ])

    try:
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )
        # Result should be 't' (true) or 'f' (false)
        return result.stdout.strip() == 't'
    except (subprocess.CalledProcessError, FileNotFoundError):
        # If we can't check, assume it doesn't exist (safer to run build-links)
        return False


def check_teig_omrade_spatial_index(db_params: dict) -> bool:
    """Check if spatial GIST index exists on teig.omrade (KRITISK).

    This function checks if:
    1. The teig table exists in any of these schemas:
       - 'public' schema
       - Schema with prefix 'teig' or 'teig_*'
       - Schema with prefix 'matrikkel*' or 'matrikkeleneiendomskartteig_*'
    2. The omrade column exists as a geometry column
    3. A GIST spatial index exists on teig.omrade

    Args:
        db_params: Database connection parameters

    Returns:
        True if spatial index exists, False otherwise
    """
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
        '-t',  # Tuples only (no headers)
        '-A',  # Unaligned output
        '-c', """
            SELECT EXISTS (
                SELECT 1
                FROM public.geometry_columns gc
                JOIN pg_indexes pi ON pi.schemaname = gc.f_table_schema
                                  AND pi.tablename = gc.f_table_name
                JOIN pg_class pc ON pc.relname = pi.indexname
                JOIN pg_am am ON am.oid = pc.relam
                WHERE (
                    gc.f_table_schema = 'public'
                    OR gc.f_table_schema = 'teig'
                    OR gc.f_table_schema LIKE 'teig_%'
                    OR gc.f_table_schema LIKE 'matrikkel%'
                    OR gc.f_table_schema LIKE 'matrikkeleneiendomskartteig_%'
                )
                  AND gc.f_table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
                  AND gc.f_table_name = 'teig'
                  AND gc.f_geometry_column = 'omrade'
                  AND am.amname = 'gist'
                  AND EXISTS (
                      SELECT 1
                      FROM pg_index pidx
                      JOIN pg_attribute pattr ON pattr.attrelid = pidx.indrelid
                      JOIN pg_class ptab ON ptab.oid = pidx.indrelid
                      JOIN pg_namespace pns ON pns.oid = ptab.relnamespace
                      WHERE pidx.indexrelid = pc.oid
                        AND pattr.attnum = ANY(pidx.indkey)
                        AND pattr.attname = 'omrade'
                        AND pns.nspname = gc.f_table_schema
                        AND ptab.relname = 'teig'
                  )
            );
        """
    ])

    try:
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )
        # Result should be 't' (true) or 'f' (false)
        return result.stdout.strip() == 't'
    except (subprocess.CalledProcessError, FileNotFoundError):
        # If we can't check, assume it doesn't exist (safer to warn)
        return False


def should_run_build_links(migration_003: Optional[Path], db_params: dict) -> bool:
    """Determine if build-links should run before migration 003.

    Args:
        migration_003: Path to migration 003 file, or None if it doesn't exist
        db_params: Database connection parameters

    Returns:
        True if build-links should run, False otherwise
    """
    if not migration_003:
        return False  # No migration 003, no need for build-links

    # Check if links table already exists
    if check_links_table_exists(db_params):
        return False  # Links table exists, no need to rebuild

    return True  # Need to run build-links


def run_build_links(db_params: dict, project_root: Path, quiet: bool = True) -> bool:
    """Run build_links.py script as subprocess.

    Args:
        db_params: Database connection parameters
        project_root: Root directory of the project
        quiet: If True, suppress QA report output

    Returns:
        True if successful, False otherwise
    """
    script_path = project_root / 'scripts' / 'build_links.py'

    if not script_path.exists():
        print(f"  ✗ build_links.py not found at {script_path}", file=sys.stderr)
        return False

    env = os.environ.copy()
    if db_params.get('password'):
        env['PGPASSWORD'] = db_params['password']
    if db_params.get('host'):
        env['PGHOST'] = db_params['host']
    if db_params.get('port'):
        env['PGPORT'] = str(db_params['port'])
    env['PGUSER'] = db_params['user']
    env['PGDATABASE'] = db_params['database']

    cmd = [sys.executable, str(script_path), '--log-dir', str(project_root / 'logs')]
    if quiet:
        cmd.append('--quiet')

    try:
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )
        # Print important output (success/failure messages and progress)
        if result.stdout:
            for line in result.stdout.split('\n'):
                line = line.rstrip()
                if not line:
                    continue
                # Show important status messages
                # In quiet mode: only show major steps and results
                # In normal mode: show all important messages
                if not quiet:
                    # Show all lines with status indicators
                    if any(indicator in line for indicator in ['✓', '✗', '⚠', '==>']):
                        print(f"     {line}")
                else:
                    # Quiet mode: only show major steps (==>) and final results (✓/✗)
                    if '==>' in line or line.startswith('✓') or line.startswith('✗'):
                        print(f"     {line}")
        # Show stderr (errors)
        if result.stderr:
            for line in result.stderr.split('\n'):
                line = line.rstrip()
                if line and ('ERROR' in line or '✗' in line or 'Failed' in line):
                    print(f"     {line}", file=sys.stderr)
        return True
    except subprocess.CalledProcessError as e:
        print(f"  ✗ build-links failed", file=sys.stderr)
        if e.stdout:
            # Show important lines from stdout
            for line in e.stdout.split('\n'):
                if any(indicator in line for indicator in ['✗', 'ERROR', 'Failed']):
                    print(f"     {line}", file=sys.stderr)
        if e.stderr:
            for line in e.stderr.split('\n'):
                if line.strip():
                    print(f"     {line}", file=sys.stderr)
        return False
    except FileNotFoundError:
        print(f"  ✗ Python interpreter not found", file=sys.stderr)
        return False


def parse_psql_output(stdout: str, stderr: str, verbose: bool = False) -> Dict[str, List[str]]:
    """Parse psql output to extract meaningful messages.

    Args:
        stdout: Standard output from psql
        stderr: Standard error from psql
        verbose: If True, include all output including SQL commands

    Returns:
        Dictionary with keys: 'notices', 'warnings', 'errors', 'sql' (if verbose)
    """
    result = {
        'notices': [],
        'warnings': [],
        'errors': [],
        'sql': [] if verbose else None
    }

    # Combine stdout and stderr (psql sends NOTICE/WARNING to stderr, but sometimes stdout too)
    all_output = stdout + '\n' + stderr if stderr else stdout

    lines = all_output.split('\n')
    current_sql_block = []
    in_sql_block = False

    for line in lines:
        line_stripped = line.strip()

        # Skip empty lines
        if not line_stripped:
            if verbose and in_sql_block:
                current_sql_block.append('')
            continue

        # Extract NOTICE messages
        if 'NOTICE:' in line:
            # Extract the actual notice message (after "NOTICE:")
            notice_match = re.search(r'NOTICE:\s*(.+)', line, re.IGNORECASE)
            if notice_match:
                notice_msg = notice_match.group(1).strip()
                # Filter out connection/authentication notices
                if not any(skip in notice_msg.lower() for skip in [
                    'connection', 'authentication', 'password', 'ssl', 'tls'
                ]):
                    result['notices'].append(notice_msg)
            continue

        # Extract WARNING messages
        if 'WARNING:' in line:
            warning_match = re.search(r'WARNING:\s*(.+)', line, re.IGNORECASE)
            if warning_match:
                result['warnings'].append(warning_match.group(1).strip())
            continue

        # Extract ERROR messages
        if 'ERROR:' in line or line.startswith('ERROR'):
            error_match = re.search(r'ERROR:\s*(.+)', line, re.IGNORECASE)
            if error_match:
                result['errors'].append(error_match.group(1).strip())
            else:
                result['errors'].append(line_stripped)
            continue

        # In verbose mode, collect SQL commands (lines that look like SQL)
        if verbose:
            # SQL commands typically start with keywords or are part of DO blocks
            if (line_stripped.startswith(('CREATE', 'DROP', 'ALTER', 'SET', 'RESET', 'DO', 'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'ANALYZE', 'EXECUTE')) or
                line_stripped.startswith('--') or
                in_sql_block):
                if not in_sql_block and not line_stripped.startswith('--'):
                    in_sql_block = True
                current_sql_block.append(line)
                # End of SQL block (semicolon at end, or END $$)
                if line_stripped.endswith(';') or 'END $$' in line_stripped.upper():
                    if current_sql_block:
                        result['sql'].append('\n'.join(current_sql_block))
                    current_sql_block = []
                    in_sql_block = False

    return result


def extract_error_message(stdout: str, stderr: str) -> Tuple[str, Optional[str], Optional[str]]:
    """Extract the actual error message from psql output.

    Args:
        stdout: Standard output from psql
        stderr: Standard error from psql

    Returns:
        Tuple of (error_message, hint, failing_sql_statement)
    """
    all_output = stdout + '\n' + (stderr or '')

    # Look for ERROR: pattern
    error_match = re.search(r'ERROR:\s*([^\n]+)', all_output, re.IGNORECASE | re.MULTILINE)
    error_msg = error_match.group(1).strip() if error_match else None

    # Look for HINT: pattern
    hint_match = re.search(r'HINT:\s*([^\n]+)', all_output, re.IGNORECASE | re.MULTILINE)
    hint = hint_match.group(1).strip() if hint_match else None

    # Try to find the failing SQL statement (last CREATE/DROP/ALTER before error)
    sql_match = None
    lines = all_output.split('\n')
    last_sql = []
    for line in lines:
        line_stripped = line.strip()
        if line_stripped and not line_stripped.startswith('ERROR') and not line_stripped.startswith('HINT'):
            if any(line_stripped.upper().startswith(kw) for kw in ['CREATE', 'DROP', 'ALTER', 'SET', 'RESET', 'DO', 'EXECUTE']):
                last_sql = [line_stripped]
            elif last_sql and not line_stripped.startswith('--'):
                last_sql.append(line_stripped)
                if line_stripped.endswith(';'):
                    sql_match = ' '.join(last_sql)
                    last_sql = []
        elif line_stripped.startswith('ERROR'):
            if last_sql:
                sql_match = ' '.join(last_sql)
            break

    return (error_msg or "Unknown error", hint, sql_match)


def run_migration(db_params: dict, migration_file: Path, verbose: bool = False, quiet: bool = False) -> bool:
    """Run a single migration file.

    Args:
        db_params: Database connection parameters
        migration_file: Path to SQL migration file
        verbose: If True, show all SQL commands
        quiet: If True, suppress all output except errors

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
    cmd.extend([
        '-U', db_params['user'],
        '-d', db_params['database'],
        '-v', 'ON_ERROR_STOP=1',  # Stop on first error
        '-v', 'client_min_messages=notice',  # Show NOTICE and WARNING messages
    ])
    # Only add -a flag in verbose mode
    if verbose:
        cmd.append('-a')  # Echo all commands (show what's being executed)

    if migration_file.name != '000_setup_roles.sql':
        cmd.extend([
            '-c', "SET ROLE stiflyt_owner;"
        ])
    cmd.extend(['-f', str(migration_file)])
    if migration_file.name != '000_setup_roles.sql':
        cmd.extend(['-c', "RESET ROLE;"])

    try:
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )

        # Parse output intelligently
        parsed = parse_psql_output(result.stdout, result.stderr, verbose=verbose)

        # In verbose mode, show all SQL
        if verbose and parsed['sql']:
            for sql_block in parsed['sql']:
                print(sql_block)

        # Show warnings (always, unless quiet)
        if not quiet:
            for warning in parsed['warnings']:
                print(f"     ⚠ {warning}", file=sys.stderr)

        # Show notices only in verbose mode (suppress in normal mode for clean single-line output)
        if verbose:
            for notice in parsed['notices']:
                print(f"     ℹ {notice}")

        return True
    except subprocess.CalledProcessError as e:
        # For 000_setup_roles.sql, try running as postgres superuser if permission errors occur
        if migration_file.name == '000_setup_roles.sql' and db_params.get('host') is None:
            # Check if error is permission-related
            error_output = (e.stdout or '') + (e.stderr or '')
            if any(phrase in error_output for phrase in ['permission denied', 'must be owner', 'insufficient_privilege']):
                if not quiet:
                    print(f"  ⚠ {migration_file.name} feilet med permission errors", file=sys.stderr)
                    print(f"  ℹ Prøver som postgres superuser...", file=sys.stderr)

                # Try as postgres superuser
                postgres_cmd = ['sudo', '-u', 'postgres', 'psql']
                postgres_cmd.extend([
                    '-d', db_params['database'],
                    '-f', str(migration_file),
                    '-v', 'ON_ERROR_STOP=1',
                    '-v', 'client_min_messages=notice',
                ])
                if verbose:
                    postgres_cmd.append('-a')

                try:
                    postgres_result = subprocess.run(
                        postgres_cmd,
                        capture_output=True,
                        text=True,
                        check=True
                    )
                    # Parse output intelligently
                    parsed = parse_psql_output(postgres_result.stdout, postgres_result.stderr, verbose=verbose)

                    if verbose and parsed['sql']:
                        for sql_block in parsed['sql']:
                            print(sql_block)

                    if not quiet:
                        for warning in parsed['warnings']:
                            print(f"     ⚠ {warning}", file=sys.stderr)

                    # Show notices only in verbose mode
                    if verbose:
                        for notice in parsed['notices']:
                            print(f"     ℹ {notice}")

                    if not quiet:
                        print(f"  ✓ {migration_file.name} fullført som postgres superuser", file=sys.stderr)
                    return True
                except subprocess.CalledProcessError as postgres_e:
                    # Extract error message
                    error_msg, hint, failing_sql = extract_error_message(
                        postgres_e.stdout or '', postgres_e.stderr or ''
                    )
                    print(f"✗ Migration failed even as postgres: {migration_file.name}", file=sys.stderr)
                    print(f"  Feil: {error_msg}", file=sys.stderr)
                    if hint:
                        print(f"  Hint: {hint}", file=sys.stderr)
                    if failing_sql and verbose:
                        print(f"  Feilende kommando: {failing_sql[:200]}...", file=sys.stderr)
                    return False
                except FileNotFoundError:
                    if not quiet:
                        print(f"  ⚠ sudo ikke tilgjengelig, kan ikke prøve som postgres", file=sys.stderr)

        # Extract error message from main failure
        error_msg, hint, failing_sql = extract_error_message(e.stdout or '', e.stderr or '')
        print(f"✗ Migration failed: {migration_file.name}", file=sys.stderr)
        print(f"  Feil: {error_msg}", file=sys.stderr)
        if hint:
            print(f"  Hint: {hint}", file=sys.stderr)
        if failing_sql and verbose:
            print(f"  Feilende kommando: {failing_sql[:200]}...", file=sys.stderr)
        elif failing_sql and not quiet:
            # Show truncated SQL even in normal mode
            sql_preview = failing_sql[:100] + '...' if len(failing_sql) > 100 else failing_sql
            print(f"  Feilende kommando: {sql_preview}", file=sys.stderr)
        return False
    except FileNotFoundError:
        print("Feil: psql ikke funnet. Er PostgreSQL installert?", file=sys.stderr)
        return False


def verify_critical_views(db_params: dict) -> Tuple[bool, List[str]]:
    """Verify that critical views exist in stiflyt schema.

    Args:
        db_params: Database connection parameters

    Returns:
        Tuple of (all_exist: bool, missing_views: List[str])
    """
    env = os.environ.copy()
    if db_params.get('password'):
        env['PGPASSWORD'] = db_params['password']

    # Critical views that should exist after migrations
    critical_views = ['links', 'links_with_routes']

    cmd = ['psql']
    if db_params.get('host'):
        cmd.extend(['-h', db_params['host']])
    if db_params.get('port'):
        cmd.extend(['-p', str(db_params['port'])])
    cmd.extend([
        '-U', db_params['user'],
        '-d', db_params['database'],
        '-t', '-A', '-F', '|',
        '-c', f"""
            SELECT viewname
            FROM pg_views
            WHERE schemaname = 'stiflyt'
              AND viewname = ANY(ARRAY[{','.join([f"'{v}'" for v in critical_views])}]);
        """
    ])

    try:
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )
        existing_views = set(line.strip() for line in result.stdout.strip().split('\n') if line.strip())
        missing_views = [v for v in critical_views if v not in existing_views]
        return len(missing_views) == 0, missing_views
    except (subprocess.CalledProcessError, FileNotFoundError):
        # If we can't check, assume they're missing (safer)
        return False, critical_views


def check_owner_membership(db_params: dict) -> bool:
    """Verify current_user is a member of stiflyt_owner if the role exists."""
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
        '-t', '-A', '-F', '|',
        '-c', """
            SELECT
                current_user,
                EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stiflyt_owner') AS owner_exists,
                pg_has_role(current_user, 'stiflyt_owner', 'member') AS is_member;
        """
    ])

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
    if owner_exists == 't' and is_member != 't':
        print("✗ Current role is not a member of stiflyt_owner", file=sys.stderr)
        print(f"  current_user: {current_user}", file=sys.stderr)
        print("  Fix (as superuser):", file=sys.stderr)
        print(f"  GRANT stiflyt_owner TO {current_user};", file=sys.stderr)
        return False
    return True


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Run database migrations')
    parser.add_argument('database', nargs='?', default=None,
                       help='Database name (default: from PGDATABASE env)')
    parser.add_argument('--migration-dir', default='migrations',
                       help='Directory containing migration files (default: migrations)')
    parser.add_argument('--quiet', '-q', action='store_true',
                       help='Suppress all output except errors')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Show all SQL commands and detailed output')

    args = parser.parse_args()

    # Quiet and verbose are mutually exclusive
    if args.quiet and args.verbose:
        print("Feil: --quiet og --verbose kan ikke brukes samtidig", file=sys.stderr)
        sys.exit(1)

    # Setup
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    migration_dir = project_root / args.migration_dir

    db_params = get_db_connection_params()
    if args.database:
        db_params['database'] = args.database
    elif not db_params.get('database'):
        print("Feil: Database name må angis enten som argument eller via PGDATABASE env", file=sys.stderr)
        sys.exit(1)

    # Find migration files
    migration_files = find_migration_files(migration_dir)

    if not migration_files:
        if not args.quiet:
            print(f"ℹ Ingen migrasjoner funnet i {migration_dir}")
        sys.exit(0)

    # Preflight: ensure role membership for ownership
    if not check_owner_membership(db_params):
        sys.exit(1)

    if not args.quiet:
        print(f"==> Kjører migrasjoner for database '{db_params['database']}' ...")
        print(f"  Fant {len(migration_files)} migrasjon(er) i {migration_dir}")

    # Run migrations in order, but run build-links after migration 002 (topology) and before 003 (link views)
    # Migration order:
    #   000: setup_roles
    #   001: add_fotrute_indexes (requires fotrute data)
    #   002: build_topology (requires fotrute, creates nodes)
    #   -> build-links (requires fotrute + nodes, creates links)
    #   003: add_link_ruteinfo_view (requires links)
    #   004: add_link_endpoint_names (requires links)
    #   005: create_stable_views (requires links)
    #   007: create_route_views (requires links)

    migration_002 = next((f for f in migration_files if '002' in f.name), None)
    migration_003 = next((f for f in migration_files if '003' in f.name), None)

    # KRITISK: Check if spatial index exists on teig.omrade
    # Note: PostGIS typically creates this index automatically, but we verify it exists
    if not check_teig_omrade_spatial_index(db_params):
        print(f"  ⚠ KRITISK: Spatial index mangler på teig.omrade!", file=sys.stderr)
        print(f"     Dette kan forårsake alvorlige ytelsesproblemer.", file=sys.stderr)
        print(f"     PostGIS bør opprette indeksen automatisk, men hvis ikke:", file=sys.stderr)
        print(f"     CREATE INDEX teig_omrade_gix ON <schema>.teig USING GIST (omrade);", file=sys.stderr)
    elif not args.quiet:
        print(f"  ✓ Spatial index eksisterer på teig.omrade (PostGIS oppretter automatisk)")

    # Track which migrations have been run
    success_count = 0
    failed_count = 0
    failed_migrations = []
    migrations_run = []  # Not used in summary, but referenced in code

    for migration_file in migration_files:
        # Run migration 002 first (build_topology)
        if migration_file == migration_002:
            if run_migration(db_params, migration_file, verbose=args.verbose, quiet=args.quiet):
                if not args.quiet:
                    print(f"  ✓ {migration_file.name} fullført")
                success_count += 1
            else:
                if not args.quiet:
                    print(f"  ✗ {migration_file.name} feilet")
                failed_count += 1
                failed_migrations.append(migration_file.name)
                if not args.quiet:
                    print(f"", file=sys.stderr)
                    print(f"✗ Migrasjon feilet - stopper videre kjøring", file=sys.stderr)
                    print(f"  Feilet på: {migration_file.name}", file=sys.stderr)
                break

            # After migration 002, always run build-links if migration 003 exists
            if migration_003:
                if not args.quiet:
                    print(f"  -> Kjører build-links (kreves etter migrasjon 002, før migrasjon 003)...")
                if run_build_links(db_params, project_root, quiet=args.quiet or not args.verbose):
                    if not args.quiet:
                        print(f"     ✓ build-links fullført")
                else:
                    print(f"     ✗ build-links feilet", file=sys.stderr)
                    print(f"     ⚠ KRITISK: Migrasjon 003 vil feile uten links-tabellen", file=sys.stderr)
                    print(f"     ⚠ Kjør 'make build-links' manuelt og re-run migrasjoner", file=sys.stderr)
                    sys.exit(1)
            elif migration_003 and not args.quiet:
                print(f"  ⊙ build-links hoppet over (ingen migrasjon 003 funnet)")

            continue  # Skip normal migration handling for 002 (already handled)

        # Run other migrations normally
        if run_migration(db_params, migration_file, verbose=args.verbose, quiet=args.quiet):
            if not args.quiet:
                print(f"  ✓ {migration_file.name} fullført")
            migrations_run.append(migration_file.name)
            success_count += 1
        else:
            if not args.quiet:
                print(f"  ✗ {migration_file.name} feilet")
            failed_count += 1
            failed_migrations.append(migration_file.name)
            # Stop on first failure
            if not args.quiet:
                print(f"", file=sys.stderr)
                print(f"✗ Migrasjon feilet - stopper videre kjøring", file=sys.stderr)
                print(f"  Feilet på: {migration_file.name}", file=sys.stderr)
            break

    # Summary
    if failed_count > 0:
        if not args.quiet:
            print(f"==> Migrasjoner fullført")
            print(f"  ✗ Feilet: {failed_count}")
            print(f"  Feilede migrasjoner: {', '.join(failed_migrations)}")
        else:
            # In quiet mode, still show failures
            print(f"✗ {failed_count} migrasjon(er) feilet: {', '.join(failed_migrations)}", file=sys.stderr)
    elif not args.quiet:
        print(f"==> Migrasjoner fullført")
        if success_count > 0:
            print(f"  ✓ Vellykket: {success_count}")

    # Verify critical views exist (even if migrations "succeeded")
    # This catches cases where migrations skip silently (e.g., if build-links failed)
    if not args.quiet:
        print(f"==> Verifiserer kritiske views...")
    all_views_exist, missing_views = verify_critical_views(db_params)
    if all_views_exist:
        if not args.quiet:
            print(f"  ✓ Alle kritiske views eksisterer")
    else:
        print(f"  ✗ KRITISK: Manglende views i stiflyt schema:", file=sys.stderr)
        for view in missing_views:
            print(f"     - stiflyt.{view}", file=sys.stderr)
        print(f"", file=sys.stderr)
        print(f"  ⚠ Dette kan bety at build-links feilet eller ikke kjørte", file=sys.stderr)
        print(f"  ⚠ Løsning:", file=sys.stderr)
        print(f"     1. Kjør: make build-links", file=sys.stderr)
        print(f"     2. Re-run: make run-migrations", file=sys.stderr)
        # Don't exit with error if migrations themselves succeeded
        # But warn loudly so user knows something is wrong
        if failed_count == 0:
            print(f"", file=sys.stderr)
            print(f"  ⚠ Migrasjoner fullført, men views mangler - dette er IKKE normalt!", file=sys.stderr)

    if failed_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
