#!/usr/bin/env python3
"""
Verify that migration indexes were created successfully.

This script checks if the expected indexes exist in the database.

Usage:
    python3 scripts/verify_migration.py [database_name]
"""

import os
import sys
import argparse
from pathlib import Path
from typing import List, Dict

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


def find_turrutebasen_schema(conn) -> str:
    """Find the turrutebasen schema (turogfriluftsruter_*)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT nspname
            FROM pg_namespace
            WHERE nspname LIKE 'turogfriluftsruter_%'
              AND nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
            ORDER BY nspname DESC
            LIMIT 1
        """)
        result = cur.fetchone()
        return result[0] if result else None


def list_all_indexes(conn, schema_name: str, table_name: str) -> List[dict]:
    """List all indexes on a table."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT
                indexname,
                indexdef
            FROM pg_indexes
            WHERE schemaname = %s AND tablename = %s
            ORDER BY indexname
        """, (schema_name, table_name))
        return cur.fetchall()


def check_indexes(conn, schema_name: str) -> dict:
    """Check if expected indexes exist and list all indexes."""
    expected_indexes = {
        'fotrute': [
            'idx_fotrute_senterlinje_gist'
        ],
        'fotruteinfo': [
            'idx_fotruteinfo_fotrute_fk',
            'idx_fotruteinfo_rutenummer',
            'idx_fotruteinfo_vedlikeholdsansvarlig'
        ]
    }

    results = {}

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        for table_name, index_names in expected_indexes.items():
            # Check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                ) as exists
            """, (schema_name, table_name))

            table_exists = cur.fetchone()['exists']

            if not table_exists:
                results[table_name] = {
                    'exists': False,
                    'indexes': {}
                }
                continue

            # Get row count
            cur.execute(f"""
                SELECT COUNT(*) as count
                FROM {schema_name}.{table_name}
            """)
            row_count = cur.fetchone()['count']

            # Check indexes
            index_status = {}
            for index_name in index_names:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_indexes
                        WHERE schemaname = %s
                          AND tablename = %s
                          AND indexname = %s
                    ) as exists
                """, (schema_name, table_name, index_name))

                exists = cur.fetchone()['exists']

                # Get index size if it exists
                index_size = None
                if exists:
                    cur.execute("""
                        SELECT pg_size_pretty(pg_relation_size(indexrelid)) as size
                        FROM pg_index i
                        JOIN pg_class c ON i.indexrelid = c.oid
                        JOIN pg_namespace n ON c.relnamespace = n.oid
                        WHERE n.nspname = %s
                          AND c.relname = %s
                    """, (schema_name, index_name))
                    size_result = cur.fetchone()
                    if size_result:
                        index_size = size_result['size']

                index_status[index_name] = {
                    'exists': exists,
                    'size': index_size
                }

            # Get all indexes on this table (to see what existed before migration)
            all_indexes = list_all_indexes(conn, schema_name, table_name)

            results[table_name] = {
                'exists': True,
                'row_count': row_count,
                'indexes': index_status,
                'all_indexes': all_indexes  # All indexes including pre-existing ones
            }

    return results


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Verify migration indexes')
    parser.add_argument('database', nargs='?', default=None,
                       help='Database name (default: from PGDATABASE env)')

    args = parser.parse_args()

    db_params = get_db_connection_params()
    if args.database:
        db_params['database'] = args.database
    elif not db_params.get('database'):
        print("Feil: Database name må angis enten som argument eller via PGDATABASE env", file=sys.stderr)
        sys.exit(1)

    print(f"==> Verifying migration indexes for database '{db_params['database']}' ...")
    print()

    conn = connect_db(db_params)

    try:
        # Find schema
        schema_name = find_turrutebasen_schema(conn)
        if not schema_name:
            print("✗ Schema with prefix 'turogfriluftsruter_*' not found")
            print("  This means the turrutebasen dataset hasn't been loaded yet.")
            print("  Run 'make update-datasets' to load the data first.")
            sys.exit(1)

        print(f"✓ Found schema: {schema_name}")
        print()

        # Check indexes
        results = check_indexes(conn, schema_name)

        all_good = True

        for table_name, table_info in results.items():
            if not table_info['exists']:
                print(f"⚠ Table {schema_name}.{table_name} does not exist")
                all_good = False
                continue

            row_count = table_info.get('row_count', 0)
            print(f"Table: {schema_name}.{table_name} ({row_count:,} rows)")

            # Show all indexes (including pre-existing ones)
            all_indexes = table_info.get('all_indexes', [])
            if all_indexes:
                print(f"  All indexes on this table ({len(all_indexes)} total):")
                for idx in all_indexes:
                    idx_name = idx['indexname']
                    idx_def = idx['indexdef']
                    # Check if this is one of our expected indexes
                    is_expected = idx_name in table_info['indexes']
                    marker = "✓" if is_expected else "ℹ"
                    # Extract index type from definition
                    idx_type = "GIST" if "USING gist" in idx_def.lower() else "BTREE" if "USING btree" in idx_def.lower() else "OTHER"
                    print(f"    {marker} {idx_name} ({idx_type})")
                    if not is_expected:
                        print(f"      (pre-existing index from import)")
            else:
                print(f"  ⚠ No indexes found on this table")

            # Check expected indexes
            print(f"  Expected migration indexes:")
            for index_name, index_info in table_info['indexes'].items():
                if index_info['exists']:
                    size = index_info.get('size', 'unknown size')
                    print(f"    ✓ {index_name} exists ({size})")
                else:
                    print(f"    ✗ {index_name} MISSING")
                    all_good = False
            print()

        # Summary
        if all_good:
            print("==> ✓ All indexes verified successfully!")
            print()
            print("Note: If the migration ran very fast, it could be because:")
            print("  - Tables are small/empty (index creation is fast on small tables)")
            print("  - Indexes already existed (DROP + CREATE is fast)")
            print("  - The migration completed successfully but quietly")
        else:
            print("==> ✗ Some indexes are missing!")
            print()
            print("To fix, run migrations manually:")
            print(f"  make run-migrations PGDATABASE={db_params['database']}")
            sys.exit(1)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
