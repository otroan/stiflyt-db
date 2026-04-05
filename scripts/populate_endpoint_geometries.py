#!/usr/bin/env python3
"""
Populate geometry column in endpoint_names from current anchor_nodes.

This should be run after refresh to ensure geometries are stored for future matching.
"""

import os
import sys
from pathlib import Path

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError:
        print("Error: psycopg2 or psycopg3 required", file=sys.stderr)
        sys.exit(1)


def get_db_connection():
    """Create database connection from environment variables."""
    host = os.getenv('PGHOST', 'localhost')
    if host == 'localhost' or host == '127.0.0.1':
        host = None

    db_params = {
        'user': os.getenv('PGUSER', os.getenv('USER', 'postgres')),
        'database': os.getenv('PGDATABASE', 'matrikkel'),
        'connect_timeout': 5
    }

    if host:
        db_params['host'] = host
    if host:
        port = os.getenv('PGPORT', '5432')
        if port:
            db_params['port'] = port
    password = os.getenv('PGPASSWORD', '')
    if password:
        db_params['password'] = password

    try:
        if 'psycopg' in sys.modules and 'psycopg2' not in sys.modules:
            return psycopg.connect(**db_params)
        else:
            return psycopg2.connect(**db_params)
    except Exception as e:
        print(f"Error connecting to database: {e}", file=sys.stderr)
        raise


def populate_geometries():
    """Populate geometry column in endpoint_names from anchor_nodes."""
    conn = get_db_connection()

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Check current state
            cur.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(geom) as with_geom,
                    COUNT(*) - COUNT(geom) as without_geom
                FROM ops.endpoint_names
            """)
            stats = cur.fetchone()
            print(f"Current state:")
            print(f"  Total endpoint_names: {stats['total']}")
            print(f"  With geometry: {stats['with_geom']}")
            print(f"  Without geometry: {stats['without_geom']}")

            if stats['without_geom'] == 0:
                print("\n✓ All endpoint_names already have geometry!")
                return True

            # Populate geometry from current anchor_nodes
            print(f"\nPopulating geometry for {stats['without_geom']} endpoint_names...")
            cur.execute("""
                UPDATE ops.endpoint_names en
                SET geom = an.geom,
                    updated_at = NOW()
                FROM stiflyt.anchor_nodes an
                WHERE en.anchor_node_id = an.node_id
                  AND en.geom IS NULL
                  AND an.geom IS NOT NULL
            """)

            updated = cur.rowcount
            conn.commit()

            print(f"✓ Updated geometry for {updated} endpoint_names")

            # Verify results
            cur.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(geom) as with_geom,
                    COUNT(*) - COUNT(geom) as without_geom
                FROM ops.endpoint_names
            """)
            final_stats = cur.fetchone()
            print(f"\nFinal state:")
            print(f"  Total endpoint_names: {final_stats['total']}")
            print(f"  With geometry: {final_stats['with_geom']}")
            print(f"  Without geometry: {final_stats['without_geom']}")

            return True

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        conn.rollback()
        return False
    finally:
        conn.close()


if __name__ == '__main__':
    success = populate_geometries()
    sys.exit(0 if success else 1)
