#!/usr/bin/env python3
"""
Test script for querying stedsnavn (place names) database.

This script demonstrates various ways to query the stedsnavn table:
- Search by name (exact match, partial match, case-insensitive)
- Search by location (bounding box, point proximity)
- Show table structure and sample data

Usage:
    python3 scripts/test_stedsnavn.py [database_name] [--name NAME] [--bbox MINX MINY MAXX MAXY] [--point X Y RADIUS]

Environment variables:
    PGHOST       - PostgreSQL host (default: localhost)
    PGPORT       - PostgreSQL port (default: 5432)
    PGUSER       - PostgreSQL user (default: current user)
    PGPASSWORD   - PostgreSQL password (if needed)
"""

import os
import sys
import argparse
from typing import Optional, List, Dict, Any

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    print("Feil: psycopg2 ikke installert. Installer med: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)


def get_db_connection_params() -> dict:
    """Get database connection parameters from environment or defaults."""
    host = os.environ.get('PGHOST', 'localhost')
    if host == 'localhost' or host == '127.0.0.1':
        host = None

    return {
        'host': host,
        'port': os.environ.get('PGPORT', '5432') if host else None,
        'user': os.environ.get('PGUSER', os.environ.get('USER', 'postgres')),
        'password': os.environ.get('PGPASSWORD', ''),
        'database': os.environ.get('PGDATABASE', ''),
    }


def connect_db(db_params: dict):
    """Connect to database and return connection."""
    try:
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

        return psycopg2.connect(**conn_kwargs)
    except psycopg2.OperationalError as e:
        print(f"Feil: Kunne ikke koble til database: {e}", file=sys.stderr)
        sys.exit(1)


def check_table_exists(conn, table_name: str = 'stedsnavn', schema: str = 'public') -> bool:
    """Check if stedsnavn table exists."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
        """, (schema, table_name))
        return cur.fetchone()[0]


def get_table_info(conn, table_name: str = 'stedsnavn', schema: str = 'public') -> Dict[str, Any]:
    """Get information about the stedsnavn table."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get column information
        cur.execute("""
            SELECT
                column_name,
                data_type,
                is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table_name))
        columns = cur.fetchall()

        # Get row count
        cur.execute(f'SELECT COUNT(*) as count FROM {schema}.{table_name}')
        row_count = cur.fetchone()['count']

        # Get geometry column info
        cur.execute("""
            SELECT
                f_geometry_column as geom_column,
                coord_dimension,
                srid,
                type
            FROM geometry_columns
            WHERE f_table_schema = %s AND f_table_name = %s
        """, (schema, table_name))
        geom_info = cur.fetchone()

        return {
            'columns': columns,
            'row_count': row_count,
            'geometry_info': geom_info
        }


def search_by_name(conn, name: str, exact: bool = False, limit: int = 10, schema: str = 'public', table: str = 'stedsnavn'):
    """Search stedsnavn by name."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if exact:
            # Try to find a column that contains the name
            # Common column names in stedsnavn: navn, stedsnavn, name, etc.
            query = f"""
                SELECT *
                FROM {schema}.{table}
                WHERE navn = %s OR stedsnavn = %s OR name = %s
                LIMIT %s
            """
            cur.execute(query, (name, name, name, limit))
        else:
            # Partial match - try common name columns
            query = f"""
                SELECT *
                FROM {schema}.{table}
                WHERE
                    navn ILIKE %s OR
                    stedsnavn ILIKE %s OR
                    name ILIKE %s OR
                    navneobjekttype ILIKE %s
                LIMIT %s
            """
            pattern = f'%{name}%'
            cur.execute(query, (pattern, pattern, pattern, pattern, limit))

        return cur.fetchall()


def search_by_bbox(conn, minx: float, miny: float, maxx: float, maxy: float,
                   srid: int = 25833, limit: int = 10, schema: str = 'public', table: str = 'stedsnavn'):
    """Search stedsnavn within a bounding box."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Create bounding box geometry
        # Note: Assuming geometry column is named 'geom'
        query = f"""
            SELECT *
            FROM {schema}.{table}
            WHERE ST_Intersects(
                geom,
                ST_MakeEnvelope(%s, %s, %s, %s, %s)
            )
            LIMIT %s
        """
        cur.execute(query, (minx, miny, maxx, maxy, srid, limit))
        return cur.fetchall()


def search_by_point(conn, x: float, y: float, radius: float,
                   srid: int = 25833, limit: int = 10, schema: str = 'public', table: str = 'stedsnavn'):
    """Search stedsnavn within radius of a point."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Create point and buffer
        query = f"""
            SELECT
                *,
                ST_Distance(geom, ST_SetSRID(ST_MakePoint(%s, %s), %s)) as distance
            FROM {schema}.{table}
            WHERE ST_DWithin(
                geom,
                ST_SetSRID(ST_MakePoint(%s, %s), %s),
                %s
            )
            ORDER BY distance
            LIMIT %s
        """
        cur.execute(query, (x, y, srid, x, y, srid, radius, limit))
        return cur.fetchall()


def show_sample_data(conn, limit: int = 5, schema: str = 'public', table: str = 'stedsnavn'):
    """Show sample data from stedsnavn table."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get column names first
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))
        columns = [row['column_name'] for row in cur.fetchall()]

        # Build query - convert geometry to text for display
        select_cols = []
        for col in columns:
            if col == 'geom' or col.startswith('geometry'):
                select_cols.append(f"ST_AsText({col}) as {col}")
            else:
                select_cols.append(col)

        query = f"""
            SELECT {', '.join(select_cols)}
            FROM {schema}.{table}
            LIMIT %s
        """
        cur.execute(query, (limit,))
        return cur.fetchall()


def print_results(results: List[Dict], title: str = "Results"):
    """Print query results in a readable format."""
    if not results:
        print(f"\n{title}: Ingen resultater funnet")
        return

    print(f"\n{title} ({len(results)} resultater):")
    print("=" * 80)

    # Print column headers
    if results:
        keys = list(results[0].keys())
        # Limit display to first 10 columns for readability
        display_keys = keys[:10]
        print(" | ".join(f"{k:20}" for k in display_keys))
        print("-" * 80)

        # Print rows
        for row in results:
            values = []
            for k in display_keys:
                val = row.get(k)
                if val is None:
                    val = "NULL"
                elif isinstance(val, (int, float)):
                    val = str(val)
                elif isinstance(val, str):
                    # Truncate long strings
                    val = val[:30] + "..." if len(val) > 30 else val
                else:
                    val = str(val)[:30]
                values.append(f"{val:20}")
            print(" | ".join(values))

        if len(keys) > 10:
            print(f"\n... og {len(keys) - 10} flere kolonner")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Test script for querying stedsnavn database')
    parser.add_argument('database', nargs='?', default=None,
                       help='Database name (default: from PGDATABASE env or matrikkel)')
    parser.add_argument('--name', type=str,
                       help='Search by name (partial match, case-insensitive)')
    parser.add_argument('--exact-name', type=str,
                       help='Search by exact name match')
    parser.add_argument('--bbox', nargs=4, type=float, metavar=('MINX', 'MINY', 'MAXX', 'MAXY'),
                       help='Search within bounding box (minx miny maxx maxy)')
    parser.add_argument('--point', nargs=3, type=float, metavar=('X', 'Y', 'RADIUS'),
                       help='Search within radius of point (x y radius in meters)')
    parser.add_argument('--srid', type=int, default=25833,
                       help='SRID for spatial queries (default: 25833)')
    parser.add_argument('--limit', type=int, default=10,
                       help='Maximum number of results (default: 10)')
    parser.add_argument('--schema', type=str, default='public',
                       help='Schema name (default: public)')
    parser.add_argument('--table', type=str, default='stedsnavn',
                       help='Table name (default: stedsnavn)')
    parser.add_argument('--info', action='store_true',
                       help='Show table information only')
    parser.add_argument('--sample', type=int, default=5, metavar='N',
                       help='Show N sample rows (default: 5)')

    args = parser.parse_args()

    # Get database connection
    db_params = get_db_connection_params()
    db_params['database'] = args.database or db_params['database'] or 'matrikkel'

    print(f"==> Kobler til database: {db_params['database']}")
    conn = connect_db(db_params)
    print("✓ Tilkoblet")

    try:
        # Check if table exists
        if not check_table_exists(conn, args.table, args.schema):
            print(f"\n✗ Tabellen {args.schema}.{args.table} eksisterer ikke")
            print("\nMulige årsaker:")
            print("  1. Stedsnavn-datasettet er ikke lastet ennå")
            print("  2. Tabellen har et annet navn")
            print("\nFor å laste stedsnavn:")
            print("  make update-datasets")
            print("\nFor å sjekke hvilke tabeller som finnes:")
            print("  make inspect-db")
            sys.exit(1)

        print(f"✓ Tabellen {args.schema}.{args.table} eksisterer")

        # Show table info
        info = get_table_info(conn, args.table, args.schema)
        print(f"\n==> Tabellinformasjon:")
        print(f"  Antall rader: {info['row_count']:,}")

        if info['geometry_info']:
            geom = info['geometry_info']
            print(f"  Geometrikolonne: {geom['geom_column']}")
            print(f"  SRID: {geom['srid']}")
            print(f"  Type: {geom['type']}")

        print(f"\n  Kolonner ({len(info['columns'])}):")
        for col in info['columns'][:15]:  # Show first 15 columns
            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
            print(f"    - {col['column_name']:30} {col['data_type']:20} {nullable}")
        if len(info['columns']) > 15:
            print(f"    ... og {len(info['columns']) - 15} flere kolonner")

        # If --info only, exit here
        if args.info:
            return

        # Show sample data if no specific query
        if not args.name and not args.exact_name and not args.bbox and not args.point:
            print(f"\n==> Eksempeldata (første {args.sample} rader):")
            sample = show_sample_data(conn, args.sample, args.schema, args.table)
            print_results(sample, "Eksempeldata")

        # Execute queries
        if args.exact_name:
            print(f"\n==> Søker etter eksakt navn: '{args.exact_name}'")
            results = search_by_name(conn, args.exact_name, exact=True, limit=args.limit,
                                   schema=args.schema, table=args.table)
            print_results(results, f"Resultater for '{args.exact_name}'")

        if args.name:
            print(f"\n==> Søker etter navn (delvis match): '{args.name}'")
            results = search_by_name(conn, args.name, exact=False, limit=args.limit,
                                   schema=args.schema, table=args.table)
            print_results(results, f"Resultater for '{args.name}'")

        if args.bbox:
            minx, miny, maxx, maxy = args.bbox
            print(f"\n==> Søker i bounding box: ({minx}, {miny}) til ({maxx}, {maxy})")
            results = search_by_bbox(conn, minx, miny, maxx, maxy, args.srid, args.limit,
                                   args.schema, args.table)
            print_results(results, f"Resultater i bounding box")

        if args.point:
            x, y, radius = args.point
            print(f"\n==> Søker innenfor {radius}m radius fra punkt ({x}, {y})")
            results = search_by_point(conn, x, y, radius, args.srid, args.limit,
                                    args.schema, args.table)
            print_results(results, f"Resultater innenfor {radius}m radius")

    finally:
        conn.close()
        print("\n✓ Tilkobling lukket")


if __name__ == "__main__":
    main()
