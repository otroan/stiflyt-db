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
from pathlib import Path
from typing import List, Optional


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
    """Check if links table exists in turrutebasen schema.

    Args:
        db_params: Database connection parameters

    Returns:
        True if links table exists, False otherwise
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
                FROM information_schema.tables
                WHERE table_schema LIKE 'turogfriluftsruter_%'
                  AND table_name = 'links'
                  AND table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
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
        # If we can't check, assume it doesn't exist (safer to run build-links)
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


def run_migration(db_params: dict, migration_file: Path) -> bool:
    """Run a single migration file.

    Args:
        db_params: Database connection parameters
        migration_file: Path to SQL migration file

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
        '-f', str(migration_file),
        '-v', 'ON_ERROR_STOP=1',  # Stop on first error
        '-v', 'client_min_messages=notice',  # Show NOTICE and WARNING messages
        '-a'  # Echo all commands (show what's being executed)
    ])

    try:
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )
        # Print notices and output (NOTICE messages go to stderr in psql)
        if result.stdout:
            print(result.stdout, end='')
        if result.stderr:
            # Filter out connection notices, but show important messages
            stderr_lines = result.stderr.split('\n')
            for line in stderr_lines:
                # Show NOTICE and WARNING messages (they're important)
                if 'NOTICE:' in line or 'WARNING:' in line:
                    print(line, file=sys.stderr)
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Migration failed: {migration_file.name}", file=sys.stderr)
        if e.stdout:
            print(e.stdout, file=sys.stderr)
        if e.stderr:
            print(e.stderr, file=sys.stderr)
        return False
    except FileNotFoundError:
        print("Feil: psql ikke funnet. Er PostgreSQL installert?", file=sys.stderr)
        return False


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Run database migrations')
    parser.add_argument('database', nargs='?', default=None,
                       help='Database name (default: from PGDATABASE env)')
    parser.add_argument('--migration-dir', default='migrations',
                       help='Directory containing migration files (default: migrations)')

    args = parser.parse_args()

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
        print(f"ℹ Ingen migrasjoner funnet i {migration_dir}")
        sys.exit(0)

    print(f"==> Kjører migrasjoner for database '{db_params['database']}' ...")
    print(f"  Fant {len(migration_files)} migrasjon(er) i {migration_dir}")

    # Check if build-links needs to run before migration 003
    migration_003 = next((f for f in migration_files if '003' in f.name), None)
    if migration_003 and should_run_build_links(migration_003, db_params):
        print(f"  -> Kjører build-links (kreves før migrasjon 003)...")
        if run_build_links(db_params, project_root, quiet=True):
            print(f"     ✓ build-links fullført")
        else:
            print(f"     ✗ build-links feilet", file=sys.stderr)
            print(f"     ⚠ Migrasjon 003 vil sannsynligvis feile uten links-tabellen", file=sys.stderr)
            # Continue anyway - migration 003 will handle the error gracefully
    elif migration_003:
        print(f"  ⊙ build-links ikke nødvendig (links-tabellen eksisterer allerede)")

    # Run migrations in order
    success_count = 0
    failed_count = 0
    failed_migrations = []

    for migration_file in migration_files:
        print(f"  -> Kjører {migration_file.name} ...")
        if run_migration(db_params, migration_file):
            print(f"     ✓ {migration_file.name} fullført")
            success_count += 1
        else:
            print(f"     ✗ {migration_file.name} feilet")
            failed_count += 1
            failed_migrations.append(migration_file.name)
            # Continue with other migrations even if one fails
            # (you can change this behavior if needed)

    # Summary
    print(f"==> Migrasjoner fullført")
    print(f"  ✓ Vellykket: {success_count}")
    print(f"  ✗ Feilet: {failed_count}")
    if failed_migrations:
        print(f"  Feilede migrasjoner: {', '.join(failed_migrations)}")

    if failed_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
