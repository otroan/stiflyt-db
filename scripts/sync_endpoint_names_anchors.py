#!/usr/bin/env python3
"""
Sync endpoint_names anchor_node_id with current anchor_nodes based on geometry.

Problem: anchor_node_id in ops.endpoint_names can become stale after refresh
because node IDs are regenerated (SERIAL) even though geometries are stable.

Solution: Match endpoint_names to anchor_nodes by geometry and update anchor_node_id.

Usage:
    python3 scripts/sync_endpoint_names_anchors.py [--dry-run] [--tolerance 0.1]

Geometry matching is exact (ST_Equals); --tolerance is not used for geom match.
"""

import os
import sys
import argparse
from pathlib import Path

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
        if PSYCOPG_VERSION == 2:
            return psycopg2.connect(**db_params)
        else:
            return psycopg.connect(**db_params)
    except Exception as e:
        print(f"Error connecting to database: {e}", file=sys.stderr)
        raise


def find_schema(conn) -> str:
    """Find turrutebasen schema dynamically."""
    if PSYCOPG_VERSION == 2:
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
    else:
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


def sync_endpoint_names(dry_run: bool = False, tolerance: float = 0.1):
    """Sync endpoint_names anchor_node_id with current anchor_nodes by geometry."""
    conn = get_db_connection()

    try:
        schema = find_schema(conn)
        if not schema:
            print("Error: Could not find turrutebasen schema", file=sys.stderr)
            return False

        print(f"Using schema: {schema}")
        print("Geometry match: exact (ST_Equals)")
        print(f"Mode: {'DRY RUN' if dry_run else 'UPDATE'}")

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # First, check current state
            cur.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(an.node_id) as matched,
                    COUNT(*) - COUNT(an.node_id) as unmatched
                FROM ops.endpoint_names en
                LEFT JOIN stiflyt.anchor_nodes an ON en.anchor_node_id = an.node_id
            """)
            stats = cur.fetchone()
            print(f"\nCurrent state:")
            print(f"  Total endpoint_names: {stats['total']}")
            print(f"  Matched: {stats['matched']}")
            print(f"  Unmatched: {stats['unmatched']}")

            if stats['unmatched'] == 0:
                print("\n✓ All endpoint_names are already matched!")
                return True

            # Find unmatched endpoint_names and try to match by geometry
            # We need to get the geometry from the old anchor_node_id
            # Since we don't store geometry in endpoint_names, we'll match based on
            # finding the nearest anchor node within tolerance
            print("\nFinding geometry matches (exact: ST_Equals)...")

            # Strategy: Match by exact geometry (ST_Equals); no tolerance/search radius.

            # First, get all unmatched endpoint_names with their old anchor_node_id
            cur.execute("""
                SELECT
                    en.id,
                    en.anchor_node_id as old_anchor_node_id,
                    en.name,
                    en.rutenummer,
                    en.rutenummer_key
                FROM ops.endpoint_names en
                LEFT JOIN stiflyt.anchor_nodes an ON en.anchor_node_id = an.node_id
                WHERE an.node_id IS NULL
            """)
            unmatched = cur.fetchall()

            print(f"Found {len(unmatched)} unmatched endpoint_names")

            if not unmatched:
                print("No unmatched endpoint_names found")
                return True

            # Strategy: Match by geometry stored in endpoint_names.geom if available,
            # otherwise try to find geometry from old node_id, or use name-based matching

            updates = []
            not_found = []

            for ep in unmatched:
                match = None
                method = None

                # First try: Use stored geometry if available
                cur.execute("""
                    SELECT geom FROM ops.endpoint_names WHERE id = %s
                """, (ep['id'],))
                stored_geom = cur.fetchone()

                if stored_geom and stored_geom['geom']:
                    # Match by stored geometry (exact: same point)
                    cur.execute("""
                        SELECT an.node_id, 0::double precision as dist
                        FROM stiflyt.anchor_nodes an
                        WHERE ST_Equals(an.geom, %s)
                        LIMIT 1
                    """, (stored_geom['geom'],))
                    match = cur.fetchone()
                    if match:
                        method = 'stored_geometry'

                # Second try: Find geometry from old node_id if it still exists in nodes table (exact match)
                if not match:
                    cur.execute("""
                        WITH old_node_geom AS (
                            SELECT geom
                            FROM stiflyt.nodes
                            WHERE node_id = %s
                            LIMIT 1
                        )
                        SELECT an.node_id, 0::double precision as dist
                        FROM old_node_geom old
                        JOIN stiflyt.anchor_nodes an ON ST_Equals(an.geom, old.geom)
                        LIMIT 1
                    """, (ep['old_anchor_node_id'],))
                    match = cur.fetchone()
                    if match:
                        method = 'old_node_geometry'

                # Third try: Name-based matching (find anchor nodes near other endpoints with same name)
                if not match:
                    cur.execute("""
                        WITH same_name_endpoints AS (
                            SELECT an.node_id, an.geom
                            FROM ops.endpoint_names en2
                            JOIN stiflyt.anchor_nodes an ON en2.anchor_node_id = an.node_id
                            WHERE en2.name = %s
                              AND en2.id != %s
                        ),
                        nearby_anchors AS (
                            SELECT an.node_id,
                                   MIN(ST_Distance(an.geom, sn.geom)) as min_dist
                            FROM stiflyt.anchor_nodes an
                            CROSS JOIN same_name_endpoints sn
                            GROUP BY an.node_id
                            HAVING MIN(ST_Distance(an.geom, sn.geom)) < 1000  -- Within 1km
                            ORDER BY min_dist
                            LIMIT 1
                        )
                        SELECT node_id, min_dist as dist
                        FROM nearby_anchors
                    """, (ep['name'], ep['id']))
                    alt_match = cur.fetchone()
                    if alt_match:
                        match = alt_match
                        method = 'name_context'

                if match:
                    updates.append({
                        'id': ep['id'],
                        'old_id': ep['old_anchor_node_id'],
                        'new_id': match['node_id'],
                        'name': ep['name'],
                        'distance': match['dist'],
                        'method': method
                    })
                else:
                    not_found.append(ep)

            print(f"\nFound {len(updates)} matches to update")
            print(f"Could not match {len(not_found)} endpoint_names")

            if updates:
                print("\nUpdates to apply:")
                for u in updates[:10]:  # Show first 10
                    method = u.get('method', 'geometry')
                    print(f"  ID {u['id']}: {u['old_id']} -> {u['new_id']} ({u['name']}, {method}, {u['distance']:.2f}m)")
                if len(updates) > 10:
                    print(f"  ... and {len(updates) - 10} more")

            if not_found:
                print("\nCould not match:")
                for nf in not_found[:10]:  # Show first 10
                    print(f"  ID {nf['id']}: anchor_node_id={nf['old_anchor_node_id']}, name='{nf['name']}'")
                if len(not_found) > 10:
                    print(f"  ... and {len(not_found) - 10} more")

            if dry_run:
                print("\n[DRY RUN] Would update anchor_node_id for matched endpoint_names")
                return True

            # Apply updates
            if updates:
                print("\nApplying updates...")
                updated_count = 0
                merged_count = 0
                skipped_count = 0

                for u in updates:
                    # Check if this update would create a duplicate (another row already has new_id + same rutenummer_key)
                    cur.execute("""
                        SELECT id, name, rutenummer_key
                        FROM ops.endpoint_names
                        WHERE anchor_node_id = %s
                          AND rutenummer_key = (SELECT rutenummer_key FROM ops.endpoint_names WHERE id = %s)
                          AND id != %s
                    """, (u['new_id'], u['id'], u['id']))

                    duplicate = cur.fetchone()
                    if duplicate:
                        # Merge: the correct row (with new_id) already exists. Copy our name into it, then delete our stale row.
                        try:
                            cur.execute("""
                                UPDATE ops.endpoint_names
                                SET name = COALESCE(NULLIF(TRIM(name), ''), %s),
                                    updated_at = NOW()
                                WHERE id = %s
                            """, (u['name'], duplicate['id']))
                            cur.execute("DELETE FROM ops.endpoint_names WHERE id = %s", (u['id'],))
                            conn.commit()
                            merged_count += 1
                        except Exception as e:
                            print(f"  ⚠ Error merging ID {u['id']} into {duplicate['id']}: {e}")
                            skipped_count += 1
                            conn.rollback()
                        continue

                    try:
                        # Also update geometry if we matched by old node geometry
                        if u['method'] == 'old_node_geometry':
                            cur.execute("""
                                UPDATE ops.endpoint_names
                                SET anchor_node_id = %s,
                                    geom = (SELECT geom FROM stiflyt.anchor_nodes WHERE node_id = %s),
                                    updated_at = NOW()
                                WHERE id = %s
                            """, (u['new_id'], u['new_id'], u['id']))
                        else:
                            cur.execute("""
                                UPDATE ops.endpoint_names
                                SET anchor_node_id = %s,
                                    geom = COALESCE(geom, (SELECT geom FROM stiflyt.anchor_nodes WHERE node_id = %s)),
                                    updated_at = NOW()
                                WHERE id = %s
                            """, (u['new_id'], u['new_id'], u['id']))

                        updated_count += 1
                        conn.commit()
                    except Exception as e:
                        print(f"  ⚠ Error updating ID {u['id']}: {e}")
                        skipped_count += 1
                        conn.rollback()

                print(f"✓ Updated {updated_count} endpoint_names")
                if merged_count > 0:
                    print(f"✓ Merged {merged_count} endpoint_names (stale row removed, name kept on correct anchor)")
                if skipped_count > 0:
                    print(f"⚠ Skipped {skipped_count} endpoint_names (errors)")

            # Remove endpoint_names without geometry (they cannot be synced and are stale)
            cur.execute("""
                SELECT COUNT(*) AS count FROM ops.endpoint_names WHERE geom IS NULL
            """)
            row = cur.fetchone()
            without_geom_count = row['count'] if row else 0

            if without_geom_count > 0:
                print(f"\nRemoving {without_geom_count} endpoint_names without geometry (stale data)...")
                cur.execute("DELETE FROM ops.endpoint_names WHERE geom IS NULL")
                conn.commit()
                print(f"✓ Removed {without_geom_count} stale endpoint_names")

            # Verify results
            cur.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(an.node_id) as matched,
                    COUNT(*) - COUNT(an.node_id) as unmatched
                FROM ops.endpoint_names en
                LEFT JOIN stiflyt.anchor_nodes an ON en.anchor_node_id = an.node_id
            """)
            final_stats = cur.fetchone()
            print(f"\nFinal state:")
            print(f"  Total endpoint_names: {final_stats['total']}")
            print(f"  Matched: {final_stats['matched']}")
            print(f"  Unmatched: {final_stats['unmatched']}")

            return True

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        conn.rollback()
        return False
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Sync endpoint_names anchor_node_id with current anchor_nodes"
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be updated without making changes'
    )
    parser.add_argument(
        '--tolerance',
        type=float,
        default=0.1,
        help='Tolerance in meters for geometry matching (default: 0.1)'
    )

    args = parser.parse_args()

    success = sync_endpoint_names(dry_run=args.dry_run, tolerance=args.tolerance)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
