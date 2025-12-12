#!/usr/bin/env python3
"""
Trace a path through the graph to understand loop formation.

Usage:
    python3 scripts/trace_path.py --start-node NODE_ID [--max-depth N]
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


def trace_path(conn, schema: str, start_node: int, max_depth: int = 20) -> List[Dict]:
    """Trace a path starting from a node."""
    with conn.cursor() as cur:
        # Get anchor nodes
        cur.execute(f"SELECT node_id FROM {schema}.anchor_nodes")
        anchor_nodes = {row[0] for row in cur.fetchall()}

        # Build adjacency
        cur.execute(f"""
            SELECT objid, source_node, target_node
            FROM {schema}.fotrute
            WHERE source_node IS NOT NULL AND target_node IS NOT NULL
        """)

        adjacency = {}
        segments = {}
        for row in cur.fetchall():
            seg_id, source, target = row
            segments[seg_id] = {'source': source, 'target': target}

            if source not in adjacency:
                adjacency[source] = []
            if target not in adjacency:
                adjacency[target] = []

            adjacency[source].append((seg_id, target))
            adjacency[target].append((seg_id, source))

        # Trace path
        path = []
        current_node = start_node
        visited_segments = set()
        visited_nodes = set()
        depth = 0

        while depth < max_depth and current_node in adjacency:
            if current_node in visited_nodes:
                path.append({
                    'node': current_node,
                    'type': 'LOOP_DETECTED',
                    'depth': depth
                })
                break

            visited_nodes.add(current_node)
            is_anchor = current_node in anchor_nodes

            # Get available segments
            available = [
                (seg_id, other_node)
                for seg_id, other_node in adjacency[current_node]
                if seg_id not in visited_segments
            ]

            path.append({
                'node': current_node,
                'is_anchor': is_anchor,
                'available_segments': len(available),
                'depth': depth
            })

            if is_anchor and depth > 0:
                path[-1]['type'] = 'ANCHOR_REACHED'
                break

            if len(available) == 0:
                path[-1]['type'] = 'DEAD_END'
                break
            elif len(available) > 1:
                path[-1]['type'] = 'BRANCHING'
                # Take first segment
                seg_id, next_node = available[0]
            else:
                seg_id, next_node = available[0]

            visited_segments.add(seg_id)
            current_node = next_node
            depth += 1

        return path


def main():
    parser = argparse.ArgumentParser(description='Trace path through graph')
    parser.add_argument('--start-node', type=int, required=True, help='Starting node ID')
    parser.add_argument('--max-depth', type=int, default=20, help='Maximum depth to trace')
    parser.add_argument('--schema', type=str, help='Schema name (default: auto-detect)')
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

        print(f"Tracing path from node {args.start_node} in schema: {schema}\n")
        path = trace_path(conn, schema, args.start_node, args.max_depth)

        for step in path:
            node = step['node']
            is_anchor = step.get('is_anchor', False)
            depth = step['depth']
            available = step.get('available_segments', 0)
            step_type = step.get('type', 'CONTINUE')

            anchor_str = " [ANCHOR]" if is_anchor else ""
            print(f"Depth {depth}: Node {node}{anchor_str} (available: {available}) - {step_type}")

    finally:
        conn.close()


if __name__ == '__main__':
    main()

