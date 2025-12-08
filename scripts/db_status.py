#!/usr/bin/env python3
"""
Database status and health check utility.

Checks database connectivity, PostGIS status, table counts, and provides
a quick health overview.

Usage:
    python3 scripts/db_status.py [database_name]

Environment variables:
    PGHOST       - PostgreSQL host (default: localhost)
    PGPORT       - PostgreSQL port (default: 5432)
    PGUSER       - PostgreSQL user (default: current user)
    PGPASSWORD   - PostgreSQL password (if needed)
"""

import os
import sys
import argparse
from datetime import datetime
from typing import Optional, Dict, List, Tuple

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    print("Feil: psycopg2 ikke installert. Installer med: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)


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


def connect_db(db_params: dict) -> Optional[psycopg2.extensions.connection]:
    """Connect to database and return connection."""
    try:
        # Build connection kwargs, omitting None values
        conn_kwargs = {
            'user': db_params['user'],
            'database': db_params['database'],
            'connect_timeout': 5
        }
        if db_params['host']:
            conn_kwargs['host'] = db_params['host']
        if db_params['port']:
            conn_kwargs['port'] = db_params['port']
        if db_params['password']:
            conn_kwargs['password'] = db_params['password']

        conn = psycopg2.connect(**conn_kwargs)
        return conn
    except psycopg2.OperationalError as e:
        print(f"✗ Kunne ikke koble til database: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"✗ Feil ved tilkobling: {e}", file=sys.stderr)
        return None


def check_postgis(conn: psycopg2.extensions.connection) -> Tuple[bool, Optional[str]]:
    """Check if PostGIS extension is enabled and return version."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM pg_extension WHERE extname = 'postgis'
                ) as enabled,
                (
                    SELECT extversion FROM pg_extension WHERE extname = 'postgis'
                ) as version
            """)
            result = cur.fetchone()
            if result and result[0]:
                return True, result[1]
            return False, None
    except Exception:
        return False, None


def get_table_info(conn: psycopg2.extensions.connection) -> List[Dict]:
    """Get list of tables with row counts."""
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    schemaname,
                    tablename,
                    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                    (SELECT reltuples::bigint
                     FROM pg_class
                     WHERE relname = tablename
                     AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = schemaname)
                    ) as estimated_rows
                FROM pg_tables
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                ORDER BY schemaname, tablename
            """)
            return cur.fetchall()
    except Exception as e:
        print(f"Advarsel: Kunne ikke hente tabellinfo: {e}", file=sys.stderr)
        return []


def get_database_size(conn: psycopg2.extensions.connection) -> Optional[str]:
    """Get database size in human-readable format."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT pg_size_pretty(pg_database_size(current_database())) as size
            """)
            result = cur.fetchone()
            return result[0] if result else None
    except Exception:
        return None


def get_spatial_tables(conn: psycopg2.extensions.connection) -> List[str]:
    """Get list of tables with geometry columns."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT f_table_schema || '.' || f_table_name as table_name
                FROM geometry_columns
                ORDER BY table_name
            """)
            return [row[0] for row in cur.fetchall()]
    except Exception:
        return []


def check_database_health(db_params: dict, min_tables: int = 1) -> Tuple[bool, Dict]:
    """Check database health and return status.

    Args:
        db_params: Database connection parameters
        min_tables: Minimum number of tables expected

    Returns:
        Tuple of (is_healthy, status_dict)
    """
    status = {
        'connected': False,
        'postgis_enabled': False,
        'postgis_version': None,
        'table_count': 0,
        'spatial_table_count': 0,
        'database_size': None,
        'tables': [],
        'errors': []
    }

    conn = connect_db(db_params)
    if not conn:
        status['errors'].append('Database connection failed')
        return False, status

    status['connected'] = True

    try:
        # Check PostGIS
        postgis_enabled, postgis_version = check_postgis(conn)
        status['postgis_enabled'] = postgis_enabled
        status['postgis_version'] = postgis_version

        # Get table info
        tables = get_table_info(conn)
        status['table_count'] = len(tables)
        status['tables'] = tables

        # Get spatial tables
        spatial_tables = get_spatial_tables(conn)
        status['spatial_table_count'] = len(spatial_tables)

        # Get database size
        status['database_size'] = get_database_size(conn)

        # Determine health
        is_healthy = (
            postgis_enabled and
            status['table_count'] >= min_tables
        )

        if not postgis_enabled:
            status['errors'].append('PostGIS extension not enabled')
        if status['table_count'] < min_tables:
            status['errors'].append(f'Too few tables: {status["table_count"]} < {min_tables}')

    finally:
        conn.close()

    return is_healthy, status


def format_status(status: Dict, database: str) -> None:
    """Format and print database status."""
    print(f"Database: {database}")
    print(f"Status: {'✓ Connected' if status['connected'] else '✗ Not connected'}")

    if not status['connected']:
        if status['errors']:
            for error in status['errors']:
                print(f"  Error: {error}")
        return

    # PostGIS status
    if status['postgis_enabled']:
        version = status['postgis_version'] or 'unknown'
        print(f"PostGIS: ✓ Enabled (version {version})")
    else:
        print("PostGIS: ✗ Not enabled")

    # Table counts
    print(f"Tables: {status['table_count']}")
    if status['spatial_table_count'] > 0:
        print(f"Spatial tables: {status['spatial_table_count']}")

    # Database size
    if status['database_size']:
        print(f"Database size: {status['database_size']}")

    # List tables if requested
    if status['tables']:
        print("\nTables:")
        for table in status['tables']:
            schema_table = f"{table['schemaname']}.{table['tablename']}"
            rows = table['estimated_rows'] or 0
            size = table['size'] or 'unknown'
            print(f"  • {schema_table}: ~{rows:,} rows, {size}")

    # Errors
    if status['errors']:
        print("\n⚠ Issues found:")
        for error in status['errors']:
            print(f"  • {error}")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Check database status and health'
    )
    parser.add_argument('database', nargs='?', default=None,
                       help='Database name (default: from PGDATABASE env)')
    parser.add_argument('--min-tables', type=int, default=1,
                       help='Minimum number of tables expected (default: 1)')
    parser.add_argument('--json', action='store_true',
                       help='Output as JSON')

    args = parser.parse_args()

    db_params = get_db_connection_params()
    if args.database:
        db_params['database'] = args.database
    elif not db_params['database']:
        print("Feil: Database name må angis eller settes via PGDATABASE", file=sys.stderr)
        sys.exit(1)

    is_healthy, status = check_database_health(db_params, args.min_tables)

    if args.json:
        import json
        status['healthy'] = is_healthy
        print(json.dumps(status, indent=2, default=str))
    else:
        format_status(status, db_params['database'])

    sys.exit(0 if is_healthy else 1)


if __name__ == "__main__":
    main()

