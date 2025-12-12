#!/usr/bin/env python3
"""
Analyze loop errors from link building.

Investigates whether loops are due to:
1. Incorrect node degrees (nodes that should be anchor but aren't)
2. Actual circular paths in the data
3. Processing errors in the walk algorithm

Usage:
    python3 scripts/analyze_loops.py [--schema SCHEMA_NAME] [--limit N]
"""

import os
import sys
import argparse
from typing import Dict, List, Set, Tuple, Optional

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    PSYCOPG_VERSION = 2
except ImportError:
    try:
        import psycopg
        from psycopg.rows import dict_row
        PSYCOPG_VERSION = 3
    except ImportError:
        print("Error: psycopg2 or psycopg3 required", file=sys.stderr)
        sys.exit(1)


def get_db_connection(schema: Optional[str] = None):
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
        if PSYCOPG_VERSION == 2:
            conn = psycopg2.connect(**db_params)
        else:
            conn = psycopg.connect(**db_params)

        if schema:
            with conn.cursor() as cur:
                cur.execute(f'SET search_path TO {schema}, public')
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}", file=sys.stderr)
        raise


def find_schema(conn) -> Optional[str]:
    """Find turrutebasen schema dynamically."""
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


def analyze_loop_node(conn, schema: str, node_id: int) -> Dict:
    """Analyze a specific node that caused a loop."""
    with conn.cursor() as cur:
        # Get node degree
        cur.execute(f"""
            SELECT degree
            FROM {schema}.node_degree
            WHERE node_id = %s
        """, (node_id,))
        degree_row = cur.fetchone()
        degree = degree_row[0] if degree_row else None

        # Check if it's an anchor node
        cur.execute(f"""
            SELECT 1 FROM {schema}.anchor_nodes
            WHERE node_id = %s
        """, (node_id,))
        is_anchor = cur.fetchone() is not None

        # Get all segments connected to this node
        cur.execute(f"""
            SELECT
                objid as segment_id,
                source_node,
                target_node,
                ST_Length(senterlinje) as length_m
            FROM {schema}.fotrute
            WHERE source_node = %s OR target_node = %s
            ORDER BY objid
        """, (node_id, node_id))
        segments = cur.fetchall()

        # Get node geometry
        cur.execute(f"""
            SELECT geom
            FROM {schema}.nodes
            WHERE id = %s
        """, (node_id,))
        geom_row = cur.fetchone()
        geom = geom_row[0] if geom_row else None

        return {
            'node_id': node_id,
            'degree': degree,
            'is_anchor': is_anchor,
            'should_be_anchor': degree is not None and degree != 2,
            'segment_count': len(segments),
            'segments': segments,
            'has_geometry': geom is not None
        }


def find_loops_in_data(conn, schema: str, limit: int = 10) -> List[Dict]:
    """Find actual circular paths in the data."""
    with conn.cursor() as cur:
        # Find nodes that appear multiple times in link_segments
        # This indicates we might have processed them incorrectly
        cur.execute(f"""
            SELECT
                ls.segment_id,
                f.source_node,
                f.target_node,
                COUNT(*) as usage_count
            FROM {schema}.link_segments ls
            JOIN {schema}.fotrute f ON ls.segment_id = f.objid
            GROUP BY ls.segment_id, f.source_node, f.target_node
            HAVING COUNT(*) > 1
            LIMIT %s
        """, (limit,))
        return cur.fetchall()


def check_node_degree_consistency(conn, schema: str, node_ids: List[int]) -> Dict:
    """Check if node degrees are consistent with anchor_nodes."""
    if not node_ids:
        return {}

    with conn.cursor() as cur:
        # Get degrees for these nodes
        placeholders = ','.join(['%s'] * len(node_ids))
        cur.execute(f"""
            SELECT
                node_id,
                degree
            FROM {schema}.node_degree
            WHERE node_id IN ({placeholders})
        """, node_ids)
        degrees = {row[0]: row[1] for row in cur.fetchall()}

        # Check which are anchors
        cur.execute(f"""
            SELECT node_id
            FROM {schema}.anchor_nodes
            WHERE node_id IN ({placeholders})
        """, node_ids)
        anchors = {row[0] for row in cur.fetchall()}

        # Find inconsistencies
        inconsistencies = []
        for node_id in node_ids:
            degree = degrees.get(node_id)
            is_anchor = node_id in anchors
            should_be_anchor = degree is not None and degree != 2

            if is_anchor != should_be_anchor:
                inconsistencies.append({
                    'node_id': node_id,
                    'degree': degree,
                    'is_anchor': is_anchor,
                    'should_be_anchor': should_be_anchor
                })

        return {
            'total_checked': len(node_ids),
            'inconsistencies': inconsistencies,
            'degrees': degrees,
            'anchors': anchors
        }


def main():
    parser = argparse.ArgumentParser(description='Analyze loop errors from link building')
    parser.add_argument('--schema', type=str, help='Schema name (default: auto-detect)')
    parser.add_argument('--limit', type=int, default=20, help='Number of loop nodes to analyze')
    parser.add_argument('--node', type=int, help='Analyze specific node ID')
    args = parser.parse_args()

    conn = get_db_connection()

    try:
        if args.schema:
            schema = args.schema
        else:
            schema = find_schema(conn)
            if not schema:
                print("Error: Could not find turrutebasen schema. Use --schema to specify.", file=sys.stderr)
                sys.exit(1)

        print(f"Analyzing loops in schema: {schema}\n")

        if args.node:
            # Analyze specific node
            print(f"Analyzing node {args.node}:")
            print("="*60)
            result = analyze_loop_node(conn, schema, args.node)
            print(f"Node ID: {result['node_id']}")
            print(f"Degree: {result['degree']}")
            print(f"Is anchor: {result['is_anchor']}")
            print(f"Should be anchor: {result['should_be_anchor']}")
            print(f"Connected segments: {result['segment_count']}")
            print(f"\nSegments:")
            for seg in result['segments']:
                print(f"  Segment {seg[0]}: {seg[1]} → {seg[2]} (length: {seg[3]:.2f}m)")

            if result['is_anchor'] != result['should_be_anchor']:
                print(f"\n⚠ INCONSISTENCY: Node is {'anchor' if result['is_anchor'] else 'not anchor'} but degree={result['degree']} (should be {'anchor' if result['should_be_anchor'] else 'not anchor'})")
        else:
            # Get loop nodes from recent run (if available)
            # For now, we'll check a sample of nodes that might cause loops
            print("Checking for duplicate segment usage (indicates processing issues):")
            print("="*60)
            duplicates = find_loops_in_data(conn, schema, limit=args.limit)
            if duplicates:
                print(f"Found {len(duplicates)} segments used multiple times:")
                for dup in duplicates:
                    print(f"  Segment {dup[0]}: used {dup[3]} times (connects {dup[1]} → {dup[2]})")
            else:
                print("No duplicate segment usage found.")

            print("\n" + "="*60)
            print("To analyze a specific loop node, use:")
            print(f"  python3 scripts/analyze_loops.py --node <node_id>")
            print("\nExample nodes from error messages:")
            print("  python3 scripts/analyze_loops.py --node 109646")
            print("  python3 scripts/analyze_loops.py --node 112298")

    finally:
        conn.close()


if __name__ == '__main__':
    main()

