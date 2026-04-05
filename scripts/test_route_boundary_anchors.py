#!/usr/bin/env python3
"""Test script to verify route-boundary anchors are correctly identified.

This script checks if nodes where routes end/start (e.g., bre6 ending and bre5/bre57 starting)
are correctly identified as anchor nodes.
"""

import argparse
import sys
from pathlib import Path

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).parent))

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    print("Error: psycopg2 not installed. Install with: pip install psycopg2-binary")
    sys.exit(1)


def test_route_boundary_anchors(conn, schema: str = 'stiflyt', route: str = None):
    """Test if route-boundary anchors are correctly identified."""

    cur = conn.cursor(cursor_factory=RealDictCursor)

    print("=" * 80)
    print("Testing Route-Boundary Anchors")
    print("=" * 80)
    print()

    # 1. Count anchor nodes by type
    print("1. Anchor nodes by type:")
    cur.execute(f"""
        SELECT
            anchor_type,
            COUNT(*) as count
        FROM {schema}.anchor_nodes
        GROUP BY anchor_type
        ORDER BY anchor_type;
    """)
    for row in cur.fetchall():
        print(f"   {row['anchor_type']}: {row['count']}")
    print()

    # 2. Find route-boundary anchors
    print("2. Route-boundary anchors (sample):")
    cur.execute(f"""
        SELECT
            an.node_id,
            an.degree,
            nd.degree as node_degree_check,
            array_agg(DISTINCT fi.rutenummer ORDER BY fi.rutenummer) FILTER (WHERE fi.rutenummer IS NOT NULL) as routes
        FROM {schema}.anchor_nodes an
        JOIN {schema}.node_degree nd ON nd.node_id = an.node_id
        JOIN {schema}.fotrute f ON (f.source_node = an.node_id OR f.target_node = an.node_id)
        LEFT JOIN {schema}.fotruteinfo fi ON fi.fotrute_fk = f.objid
        WHERE an.anchor_type = 'route_boundary'
        GROUP BY an.node_id, an.degree, nd.degree
        ORDER BY an.node_id
        LIMIT 10;
    """)
    route_boundary_count = 0
    for row in cur.fetchall():
        route_boundary_count += 1
        routes_str = ', '.join(row['routes']) if row['routes'] else 'no routes'
        print(f"   Node {row['node_id']}: degree={row['degree']}, routes=[{routes_str}]")
    print()

    # 3. Test specific case: bre6 ending, bre5/bre57 starting
    if route:
        print(f"3. Testing specific route: {route}")
        print(f"   Finding nodes where {route} ends and other routes start...")
        cur.execute(f"""
            WITH route_segments AS (
                SELECT DISTINCT
                    f.objid as segment_id,
                    f.source_node,
                    f.target_node,
                    array_agg(DISTINCT fi.rutenummer ORDER BY fi.rutenummer) FILTER (WHERE fi.rutenummer IS NOT NULL) as routes
                FROM {schema}.fotrute f
                JOIN {schema}.fotruteinfo fi ON fi.fotrute_fk = f.objid
                WHERE fi.rutenummer IS NOT NULL
                GROUP BY f.objid, f.source_node, f.target_node
            ),
            route_nodes AS (
                SELECT
                    node_id,
                    array_agg(DISTINCT routes ORDER BY routes) as route_sets
                FROM (
                    SELECT source_node as node_id, routes FROM route_segments
                    UNION ALL
                    SELECT target_node as node_id, routes FROM route_segments
                ) t
                GROUP BY node_id
                HAVING COUNT(DISTINCT routes) > 1
            )
            SELECT
                rn.node_id,
                rn.route_sets,
                CASE WHEN an.node_id IS NOT NULL THEN true ELSE false END as is_anchor,
                an.anchor_type
            FROM route_nodes rn
            LEFT JOIN {schema}.anchor_nodes an ON an.node_id = rn.node_id
            WHERE EXISTS (
                SELECT 1 FROM unnest(rn.route_sets) as routes
                WHERE %s = ANY(routes)
            )
            ORDER BY rn.node_id
            LIMIT 20;
        """, (route,))

        found_nodes = []
        for row in cur.fetchall():
            found_nodes.append(row)
            route_sets_str = ' | '.join([str(rs) for rs in row['route_sets']])
            anchor_status = f"✓ Anchor ({row['anchor_type']})" if row['is_anchor'] else "✗ NOT an anchor"
            print(f"   Node {row['node_id']}: route_sets=[{route_sets_str}] - {anchor_status}")

        if not found_nodes:
            print(f"   No nodes found where {route} intersects with other routes")
        else:
            missing_anchors = [n for n in found_nodes if not n['is_anchor']]
            if missing_anchors:
                print()
                print(f"   ⚠ WARNING: {len(missing_anchors)} nodes are NOT identified as anchors!")
                print("   These should be route-boundary anchors.")
            else:
                print()
                print(f"   ✓ All {len(found_nodes)} nodes are correctly identified as anchors")
        print()

    # 4. Verify route-boundary logic matches Python implementation
    print("4. Verifying route-boundary detection logic:")
    print("   Checking nodes where incident segments have different route sets...")
    cur.execute(f"""
        WITH node_segment_routes AS (
            SELECT DISTINCT
                n.id as node_id,
                f.objid as segment_id,
                fi.rutenummer
            FROM {schema}.nodes n
            JOIN {schema}.node_degree nd ON nd.node_id = n.id
            JOIN {schema}.fotrute f ON (f.source_node = n.id OR f.target_node = n.id)
            JOIN {schema}.fotruteinfo fi ON fi.fotrute_fk = f.objid
            WHERE fi.rutenummer IS NOT NULL
              AND nd.degree = 2
        ),
        node_route_sets AS (
            SELECT
                node_id,
                segment_id,
                array_agg(DISTINCT rutenummer ORDER BY rutenummer) as route_set
            FROM node_segment_routes
            GROUP BY node_id, segment_id
        ),
        nodes_with_multiple_route_sets AS (
            SELECT
                node_id,
                COUNT(DISTINCT route_set) as distinct_route_sets
            FROM node_route_sets
            GROUP BY node_id
            HAVING COUNT(DISTINCT route_set) > 1
        )
        SELECT
            COUNT(*) as total_route_boundary_candidates,
            COUNT(CASE WHEN an.node_id IS NOT NULL THEN 1 END) as identified_as_anchors,
            COUNT(CASE WHEN an.node_id IS NULL THEN 1 END) as missing_anchors
        FROM nodes_with_multiple_route_sets nw
        LEFT JOIN {schema}.anchor_nodes an ON an.node_id = nw.node_id AND an.anchor_type = 'route_boundary';
    """)

    row = cur.fetchone()
    print(f"   Total route-boundary candidates: {row['total_route_boundary_candidates']}")
    print(f"   Identified as route_boundary anchors: {row['identified_as_anchors']}")
    print(f"   Missing anchors: {row['missing_anchors']}")

    if row['missing_anchors'] > 0:
        print()
        print("   ⚠ WARNING: Some route-boundary nodes are not identified as anchors!")
    else:
        print()
        print("   ✓ All route-boundary nodes are correctly identified")
    print()

    cur.close()


def main():
    parser = argparse.ArgumentParser(
        description='Test route-boundary anchor identification'
    )
    parser.add_argument(
        '--schema',
        default='stiflyt',
        help='Database schema (default: stiflyt)'
    )
    parser.add_argument(
        '--route',
        help='Test specific route (e.g., bre6)'
    )
    parser.add_argument(
        '--db-url',
        help='Database connection URL (overrides env vars)'
    )

    args = parser.parse_args()

    # Connect to database
    try:
        if args.db_url:
            conn = psycopg2.connect(args.db_url)
        else:
            # Try to get connection from environment
            import os
            db_url = os.getenv('DATABASE_URL')
            if not db_url:
                print("Error: DATABASE_URL environment variable not set")
                print("   Set it or use --db-url option")
                sys.exit(1)
            conn = psycopg2.connect(db_url)
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

    try:
        test_route_boundary_anchors(conn, schema=args.schema, route=args.route)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
