#!/usr/bin/env python3
"""
Database schema inspection utility.

Lists tables, columns, indexes, and spatial reference systems.

Usage:
    python3 scripts/inspect_db.py [database_name] [--tables] [--schema TABLE] [--indexes] [--srids]

Environment variables:
    PGHOST       - PostgreSQL host (default: localhost)
    PGPORT       - PostgreSQL port (default: 5432)
    PGUSER       - PostgreSQL user (default: current user)
    PGPASSWORD   - PostgreSQL password (if needed)
"""

import os
import sys
import argparse
from typing import Optional, List, Dict

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


def connect_db(db_params: dict):
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

        return psycopg2.connect(**conn_kwargs)
    except psycopg2.OperationalError as e:
        print(f"Feil: Kunne ikke koble til database: {e}", file=sys.stderr)
        sys.exit(1)


def list_tables(conn) -> List[Dict]:
    """List all tables with basic info."""
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


def show_table_schema(conn, table_name: str):
    """Show detailed schema for a specific table."""
    schema, table = table_name.split('.') if '.' in table_name else ('public', table_name)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get columns
        cur.execute("""
            SELECT
                column_name,
                data_type,
                character_maximum_length,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))
        columns = cur.fetchall()

        # Get geometry columns
        cur.execute("""
            SELECT
                f_geometry_column as column_name,
                coord_dimension,
                srid,
                type as geometry_type
            FROM geometry_columns
            WHERE f_table_schema = %s AND f_table_name = %s
        """, (schema, table))
        geometry_cols = cur.fetchall()

        # Get indexes
        cur.execute("""
            SELECT
                indexname,
                indexdef
            FROM pg_indexes
            WHERE schemaname = %s AND tablename = %s
            ORDER BY indexname
        """, (schema, table))
        indexes = cur.fetchall()

        return {
            'columns': columns,
            'geometry_columns': geometry_cols,
            'indexes': indexes
        }


def list_indexes(conn) -> List[Dict]:
    """List all indexes."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT
                schemaname,
                tablename,
                indexname,
                indexdef
            FROM pg_indexes
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY schemaname, tablename, indexname
        """)
        return cur.fetchall()


def list_srids(conn) -> List[Dict]:
    """List all spatial reference systems in use."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT DISTINCT
                f_table_schema || '.' || f_table_name as table_name,
                f_geometry_column as column_name,
                srid,
                type as geometry_type
            FROM geometry_columns
            ORDER BY srid, table_name
        """)
        return cur.fetchall()


def show_sample_data(conn, table_name: str, num_rows: int = 5) -> Optional[Dict]:
    """Show sample data from a table."""
    # Parse schema.table or just table
    if '.' in table_name:
        schema, table = table_name.split('.', 1)
    else:
        schema = 'public'
        table = table_name

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Check if table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
        """, (schema, table))

        if not cur.fetchone()['exists']:
            return None

        # Get column names
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))
        columns = cur.fetchall()

        if not columns:
            return None

        # Build SELECT query
        column_names = [col['column_name'] for col in columns]
        # For geometry columns, use ST_AsText to make readable
        select_cols = []
        for col in columns:
            col_name = col['column_name']
            if col['data_type'] == 'USER-DEFINED':  # Likely geometry
                select_cols.append(f"ST_AsText({col_name}) as {col_name}")
            else:
                select_cols.append(col_name)

        query = f"""
            SELECT {', '.join(select_cols)}
            FROM {schema}.{table}
            LIMIT %s
        """

        cur.execute(query, (num_rows,))
        rows = cur.fetchall()

        # Get total row count
        cur.execute(f"SELECT COUNT(*) as count FROM {schema}.{table}")
        total_count = cur.fetchone()['count']

        return {
            'schema': schema,
            'table': table,
            'columns': columns,
            'rows': rows,
            'total_count': total_count
        }


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Inspect database schema'
    )
    parser.add_argument('database', nargs='?', default=None,
                       help='Database name (default: from PGDATABASE env)')
    parser.add_argument('--tables', action='store_true',
                       help='List all tables')
    parser.add_argument('--schema', metavar='TABLE',
                       help='Show detailed schema for table (format: schema.table or table)')
    parser.add_argument('--indexes', action='store_true',
                       help='List all indexes')
    parser.add_argument('--srids', action='store_true',
                       help='List spatial reference systems in use')
    parser.add_argument('--sample', type=str, metavar='TABLE',
                       help='Show sample data from a specific table (default: 5 rows)')
    parser.add_argument('--rows', type=int, default=5,
                       help='Number of sample rows to show (default: 5, use with --sample)')
    parser.add_argument('--all', action='store_true',
                       help='Show all available information')

    args = parser.parse_args()

    # Default to showing tables if no specific option
    if not any([args.tables, args.schema, args.indexes, args.srids, args.sample, args.all]):
        args.tables = True

    if args.all:
        args.tables = True
        args.indexes = True
        args.srids = True

    db_params = get_db_connection_params()
    if args.database:
        db_params['database'] = args.database
    elif not db_params['database']:
        print("Feil: Database name må angis eller settes via PGDATABASE", file=sys.stderr)
        sys.exit(1)

    conn = connect_db(db_params)

    try:
        if args.tables:
            print("==> Tables:")
            tables = list_tables(conn)
            if tables:
                for table in tables:
                    schema_table = f"{table['schemaname']}.{table['tablename']}"
                    rows = table['estimated_rows'] or 0
                    size = table['size'] or 'unknown'
                    print(f"  {schema_table}: ~{rows:,} rows, {size}")
            else:
                print("  No tables found")
            print()

        if args.schema:
            print(f"==> Schema for {args.schema}:")
            schema_info = show_table_schema(conn, args.schema)

            print("\nColumns:")
            for col in schema_info['columns']:
                col_type = col['data_type']
                if col['character_maximum_length']:
                    col_type += f"({col['character_maximum_length']})"
                nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                default = f" DEFAULT {col['column_default']}" if col['column_default'] else ""
                print(f"  {col['column_name']}: {col_type} {nullable}{default}")

            if schema_info['geometry_columns']:
                print("\nGeometry Columns:")
                for geom_col in schema_info['geometry_columns']:
                    print(f"  {geom_col['column_name']}: {geom_col['geometry_type']} "
                          f"(SRID: {geom_col['srid']}, Dims: {geom_col['coord_dimension']})")

            if schema_info['indexes']:
                print("\nIndexes:")
                for idx in schema_info['indexes']:
                    print(f"  {idx['indexname']}")
                    print(f"    {idx['indexdef']}")
            print()

        if args.indexes:
            print("==> Indexes:")
            indexes = list_indexes(conn)
            if indexes:
                current_table = None
                for idx in indexes:
                    schema_table = f"{idx['schemaname']}.{idx['tablename']}"
                    if current_table != schema_table:
                        print(f"\n  {schema_table}:")
                        current_table = schema_table
                    print(f"    {idx['indexname']}")
            else:
                print("  No indexes found")
            print()

        if args.srids:
            print("==> Spatial Reference Systems in use:")
            srids = list_srids(conn)
            if srids:
                current_srid = None
                for srid_info in srids:
                    if current_srid != srid_info['srid']:
                        print(f"\n  SRID {srid_info['srid']}:")
                        current_srid = srid_info['srid']
                    print(f"    {srid_info['table_name']}.{srid_info['column_name']} "
                          f"({srid_info['geometry_type']})")
            else:
                print("  No spatial tables found")
            print()

        if args.sample:
            print(f"==> Sample data from {args.sample} (showing {args.rows} rows):")
            sample_data = show_sample_data(conn, args.sample, args.rows)
            if sample_data:
                print(f"\nTotal rows in table: {sample_data['total_count']:,}")
                print(f"\nColumns: {', '.join([col['column_name'] for col in sample_data['columns']])}")
                print("\nSample data:")

                if sample_data['rows']:
                    # Print header
                    col_names = [col['column_name'] for col in sample_data['columns']]
                    col_widths = {}
                    for col_name in col_names:
                        col_widths[col_name] = max(len(col_name),
                                                   max([len(str(row.get(col_name, ''))) for row in sample_data['rows']] + [0]))

                    # Print separator
                    separator = '+' + '+'.join(['-' * (col_widths[col] + 2) for col in col_names]) + '+'
                    print(separator)

                    # Print header row
                    header = '| ' + ' | '.join([col.ljust(col_widths[col]) for col in col_names]) + ' |'
                    print(header)
                    print(separator)

                    # Print data rows
                    for row in sample_data['rows']:
                        data_row = '| '
                        for col in col_names:
                            value = str(row.get(col, ''))
                            # Truncate long values
                            if len(value) > 50:
                                value = value[:47] + '...'
                            data_row += value.ljust(col_widths[col]) + ' | '
                        print(data_row.rstrip())
                    print(separator)
                else:
                    print("  (Table is empty)")
            else:
                print(f"  Table '{args.sample}' not found")
            print()

    finally:
        conn.close()


if __name__ == "__main__":
    main()

