#!/usr/bin/env python3
"""
Build links from segments and anchor nodes.

A link is a maximal chain of segments between two anchor nodes,
where all intermediate nodes are non-anchor (typically degree=2).

Usage:
    python3 scripts/build_links.py [--schema SCHEMA_NAME]

Environment variables:
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict

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
    """Create database connection from environment variables.

    For localhost, uses None for host to enable Unix socket (peer auth).
    """
    host = os.getenv('PGHOST', 'localhost')
    # Use None for localhost to enable Unix socket connection (peer auth)
    if host == 'localhost' or host == '127.0.0.1':
        host = None

    # Build connection parameters, omitting None/empty values
    db_params = {
        'user': os.getenv('PGUSER', os.getenv('USER', 'postgres')),
        'database': os.getenv('PGDATABASE', 'matrikkel'),
        'connect_timeout': 5
    }

    if host:
        db_params['host'] = host
    if host:  # Only set port if host is set (not using Unix socket)
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
        print(f"  Host: {host or 'localhost (Unix socket)'}", file=sys.stderr)
        print(f"  Database: {db_params['database']}", file=sys.stderr)
        print(f"  User: {db_params['user']}", file=sys.stderr)
        raise


def find_schema(conn) -> Optional[str]:
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


def create_tables(conn, schema: str):
    """Create links and link_segments tables if they don't exist."""
    cursor_class = RealDictCursor if PSYCOPG_VERSION == 2 else dict_row

    with conn.cursor() as cur:
        # Get SRID from segments table (fotrute)
        cur.execute(f"""
            SELECT srid
            FROM geometry_columns
            WHERE f_table_schema = %s AND f_table_name = 'fotrute'
            LIMIT 1
        """, (schema,))
        srid_result = cur.fetchone()
        srid = srid_result[0] if srid_result else 25833  # Default UTM 33N

        # Create links table
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.links (
                link_id BIGSERIAL PRIMARY KEY,
                a_node BIGINT NOT NULL,
                b_node BIGINT NOT NULL,
                length_m DOUBLE PRECISION NOT NULL,
                geom GEOMETRY(MULTILINESTRING, {srid}),
                segment_objids BIGINT[] -- Array of segment objids for joining with fotruteinfo
            )
        """)

        # Add segment_objids column if table exists but column is missing (migration)
        cur.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = 'links' AND column_name = 'segment_objids'
                ) THEN
                    ALTER TABLE {schema}.links ADD COLUMN segment_objids BIGINT[];
                END IF;
            END $$;
        """, (schema,))

        # Create link_segments table
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.link_segments (
                link_id BIGINT NOT NULL REFERENCES {schema}.links(link_id) ON DELETE CASCADE,
                seq INT NOT NULL,
                segment_id BIGINT NOT NULL,
                from_node BIGINT NOT NULL,
                PRIMARY KEY (link_id, seq)
            )
        """)

        # Add from_node column if table exists but column is missing (migration)
        cur.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = 'link_segments' AND column_name = 'from_node'
                ) THEN
                    ALTER TABLE {schema}.link_segments ADD COLUMN from_node BIGINT;
                END IF;
            END $$;
        """, (schema,))

        # Create indexes
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_links_a_node
            ON {schema}.links USING BTREE (a_node)
        """)
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_links_b_node
            ON {schema}.links USING BTREE (b_node)
        """)
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_links_geom_gist
            ON {schema}.links USING GIST (geom)
        """)
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_link_segments_segment_id
            ON {schema}.link_segments USING BTREE (segment_id)
        """)

        conn.commit()
        print(f"✓ Created/verified tables in schema: {schema}")


def load_anchor_nodes(conn, schema: str) -> Set[int]:
    """Load all anchor node IDs into a set."""
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT node_id FROM {schema}.anchor_nodes
        """)
        anchors = {row[0] for row in cur.fetchall()}
    print(f"✓ Loaded {len(anchors)} anchor nodes")
    return anchors


def load_segments(conn, schema: str) -> Tuple[Dict[int, Dict], Dict[int, List[Tuple[int, int]]]]:
    """
    Load all segments and build adjacency structure.

    Returns:
        segments_dict: {segment_id: {source_node, target_node, length_m}}
        adjacency: {node_id: [(segment_id, other_node), ...]}
    """
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                objid,
                source_node,
                target_node,
                COALESCE(ST_Length(senterlinje), 0) as length_m
            FROM {schema}.fotrute
            WHERE source_node IS NOT NULL
              AND target_node IS NOT NULL
            ORDER BY objid
        """)

        segments_dict: Dict[int, Dict] = {}
        adjacency: Dict[int, List[Tuple[int, int]]] = defaultdict(list)

        for row in cur.fetchall():
            seg_id = row[0]
            source = row[1]
            target = row[2]
            length = float(row[3]) if row[3] else 0.0

            segments_dict[seg_id] = {
                'source_node': source,
                'target_node': target,
                'length_m': length
            }

            # Add to adjacency for both nodes
            adjacency[source].append((seg_id, target))
            adjacency[target].append((seg_id, source))

        # Sort adjacency lists for determinism
        for node_id in adjacency:
            adjacency[node_id].sort()

    print(f"✓ Loaded {len(segments_dict)} segments")
    return segments_dict, dict(adjacency)


def build_links(
    segments_dict: Dict[int, Dict],
    adjacency: Dict[int, List[Tuple[int, int]]],
    anchor_nodes: Set[int]
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Build links by walking from anchor nodes.

    Returns:
        links: List of {a_node, b_node, segment_ids, length_m}
        link_segments: List of {link_id, seq, segment_id}
        errors: List of {type, node_id, segment_id, message}
    """
    used_segments: Set[int] = set()
    links: List[Dict] = []
    link_segments_list: List[Dict] = []
    errors: List[Dict] = []

    # Sort anchor nodes for determinism
    sorted_anchors = sorted(anchor_nodes)

    for anchor in sorted_anchors:
        if anchor not in adjacency:
            continue

        # Get all segments incident to this anchor
        incident_segments = [seg for seg in adjacency[anchor] if seg[0] not in used_segments]

        for seg_id, next_node in incident_segments:
            if seg_id in used_segments:
                continue

            # Start new link
            current_node = next_node
            link_segment_ids = [seg_id]
            used_segments.add(seg_id)
            visited_nodes = {anchor}  # Track visited nodes for loop detection (start with anchor only)

            # Walk until we hit an anchor or error
            while current_node not in anchor_nodes:
                if current_node not in adjacency:
                    # Dangling segment
                    errors.append({
                        'type': 'dangling',
                        'node_id': current_node,
                        'segment_id': seg_id,
                        'message': f'Dangling segment at node {current_node}'
                    })
                    break

                # Check for loop (visited same non-anchor node twice)
                # Note: We check BEFORE adding to visited_nodes to detect actual loops
                if current_node in visited_nodes:
                    errors.append({
                        'type': 'loop',
                        'node_id': current_node,
                        'segment_id': link_segment_ids[-1],
                        'message': f'Loop detected: revisited node {current_node}'
                    })
                    break

                # Find next segment (exclude the one we came from)
                available = [
                    (s_id, other_node)
                    for s_id, other_node in adjacency[current_node]
                    if s_id not in used_segments and s_id != link_segment_ids[-1]
                ]

                if len(available) == 0:
                    # Dead end
                    errors.append({
                        'type': 'dangling',
                        'node_id': current_node,
                        'segment_id': link_segment_ids[-1],
                        'message': f'Dead end at node {current_node}'
                    })
                    break
                elif len(available) > 1:
                    # Branching without anchor
                    errors.append({
                        'type': 'branching',
                        'node_id': current_node,
                        'segment_id': link_segment_ids[-1],
                        'message': f'Branching at node {current_node} (degree {len(available) + 1})'
                    })
                    break

                # Exactly one segment - continue walking
                next_seg_id, next_node = available[0]
                link_segment_ids.append(next_seg_id)
                used_segments.add(next_seg_id)
                visited_nodes.add(current_node)
                current_node = next_node

            # Determine b_node
            if current_node in anchor_nodes:
                # Valid link from anchor to anchor
                b_node = current_node
                # Check for self-loop (anchor to same anchor)
                if anchor == b_node and len(link_segment_ids) > 0:
                    errors.append({
                        'type': 'loop',
                        'node_id': anchor,
                        'segment_id': link_segment_ids[0],
                        'message': f'Loop detected: anchor {anchor} to itself'
                    })
            else:
                # Incomplete link (error case) - use current_node as b_node anyway
                b_node = current_node

            # Calculate total length
            total_length = sum(segments_dict[sid]['length_m'] for sid in link_segment_ids)

            # Store link
            link_id = len(links) + 1
            links.append({
                'link_id': link_id,
                'a_node': anchor,
                'b_node': b_node,
                'length_m': total_length,
                'segment_ids': link_segment_ids
            })

            # Store link_segments with from_node info for geometry orientation
            # Track which node we're coming from for each segment
            prev_node = anchor
            for seq, seg_id in enumerate(link_segment_ids):
                seg_info = segments_dict[seg_id]
                # Store the node we're coming from (for geometry orientation)
                from_node = prev_node

                # Determine next node for next iteration
                if seg_info['source_node'] == prev_node:
                    next_node = seg_info['target_node']
                else:
                    next_node = seg_info['source_node']

                link_segments_list.append({
                    'link_id': link_id,
                    'seq': seq,
                    'segment_id': seg_id,
                    'from_node': from_node  # Store for geometry orientation
                })

                prev_node = next_node

    return links, link_segments_list, errors


def insert_links(conn, schema: str, links: List[Dict], link_segments: List[Dict]):
    """Insert links and link_segments into database."""
    with conn.cursor() as cur:
        # Truncate existing data
        cur.execute(f"TRUNCATE {schema}.link_segments RESTART IDENTITY CASCADE")
        cur.execute(f"TRUNCATE {schema}.links RESTART IDENTITY CASCADE")

        # Insert links with segment_objids array
        if links:
            cur.executemany(f"""
                INSERT INTO {schema}.links (link_id, a_node, b_node, length_m, segment_objids)
                VALUES (%s, %s, %s, %s, %s)
            """, [
                (link['link_id'], link['a_node'], link['b_node'], link['length_m'], link['segment_ids'])
                for link in links
            ])

        # Insert link_segments
        if link_segments:
            cur.executemany(f"""
                INSERT INTO {schema}.link_segments (link_id, seq, segment_id, from_node)
                VALUES (%s, %s, %s, %s)
            """, [
                (ls['link_id'], ls['seq'], ls['segment_id'], ls['from_node'])
                for ls in link_segments
            ])

        conn.commit()
        print(f"✓ Inserted {len(links)} links and {len(link_segments)} link_segments")


def update_link_geometries(conn, schema: str):
    """Update link geometries from segment geometries.

    Segments are collected in sequential order (seq) and oriented correctly:
    - If segment's source_node matches the from_node, use geometry as-is
    - Otherwise, reverse the geometry with ST_Reverse()
    This ensures the link geometry flows continuously from a_node to b_node.
    """
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE {schema}.links l
            SET geom = seg_geoms.geom
            FROM (
                SELECT
                    ls.link_id,
                    ST_Collect(
                        CASE
                            WHEN f.source_node = ls.from_node THEN f.senterlinje
                            ELSE ST_Reverse(f.senterlinje)
                        END
                        ORDER BY ls.seq
                    ) as geom
                FROM {schema}.link_segments ls
                JOIN {schema}.fotrute f ON ls.segment_id = f.objid
                GROUP BY ls.link_id
            ) seg_geoms
            WHERE l.link_id = seg_geoms.link_id
        """)
        conn.commit()
        print(f"✓ Updated link geometries (oriented and ordered by seq)")


def print_qa_report(links: List[Dict], total_segments: int, used_segments: Set[int], errors: List[Dict]):
    """Print QA report to stdout."""
    used_count = len(used_segments)
    unused_count = total_segments - used_count

    print("\n" + "="*60)
    print("QA REPORT")
    print("="*60)
    print(f"Total links:              {len(links)}")
    print(f"Total segments:           {total_segments}")
    print(f"Segments used:            {used_count}")
    print(f"Segments unused:          {unused_count}")
    print(f"Errors (dangling/branch/loop): {len(errors)}")

    if errors:
        print("\nError examples (max 20):")
        for i, err in enumerate(errors[:20], 1):
            print(f"  {i}. [{err['type']}] Node {err['node_id']}, Segment {err['segment_id']}: {err['message']}")
        if len(errors) > 20:
            print(f"  ... and {len(errors) - 20} more errors")

    print("="*60)


def setup_logging(log_dir: Path) -> Path:
    """Setup logging directory and return log file path."""
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f"build_links_{timestamp}.log"
    return log_file


def log(message: str, log_file: Optional[Path] = None, also_print: bool = True):
    """Log message to file and optionally print."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_line = f"[{timestamp}] {message}\n"
    if log_file:
        with open(log_file, 'a') as f:
            f.write(log_line)
    if also_print:
        print(message)


def validate_prerequisites(conn, schema: str, log_file: Optional[Path] = None) -> bool:
    """Validate that required tables exist before building links."""
    try:
        if PSYCOPG_VERSION == 2:
            with conn.cursor() as cur:
                # Check for required tables
                cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                      AND table_name IN ('nodes', 'fotrute', 'anchor_nodes')
                """, (schema,))
                tables = [row[0] for row in cur.fetchall()]

                if len(tables) < 3:
                    missing = set(['nodes', 'fotrute', 'anchor_nodes']) - set(tables)
                    log(f"✗ Missing required tables: {', '.join(missing)}", log_file)
                    return False

                # Check that tables have data
                for table in ['nodes', 'fotrute', 'anchor_nodes']:
                    # Use parameterized query with identifier quoting for safety
                    from psycopg2 import sql
                    query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                        sql.Identifier(schema),
                        sql.Identifier(table)
                    )
                    cur.execute(query)
                    count = cur.fetchone()[0]
                    if count == 0:
                        log(f"✗ Table {schema}.{table} is empty", log_file)
                        return False
                    log(f"  ✓ Table {schema}.{table}: {count:,} rows", log_file)

                return True
        else:
            # Similar logic for psycopg3
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                      AND table_name IN ('nodes', 'fotrute', 'anchor_nodes')
                """, (schema,))
                tables = [row[0] for row in cur.fetchall()]

                if len(tables) < 3:
                    missing = set(['nodes', 'fotrute', 'anchor_nodes']) - set(tables)
                    log(f"✗ Missing required tables: {', '.join(missing)}", log_file)
                    return False

                for table in ['nodes', 'fotrute', 'anchor_nodes']:
                    # Use parameterized query with identifier quoting for safety
                    from psycopg import sql
                    query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                        sql.Identifier(schema),
                        sql.Identifier(table)
                    )
                    cur.execute(query)
                    count = cur.fetchone()[0]
                    if count == 0:
                        log(f"✗ Table {schema}.{table} is empty", log_file)
                        return False
                    log(f"  ✓ Table {schema}.{table}: {count:,} rows", log_file)

                return True
    except Exception as e:
        log(f"✗ Error validating prerequisites: {e}", log_file)
        return False


def main():
    parser = argparse.ArgumentParser(description='Build links from segments and anchor nodes')
    parser.add_argument('--schema', type=str, help='Schema name (default: auto-detect turrutebasen)')
    parser.add_argument('--log-dir', type=Path, default=Path('./logs'),
                       help='Directory for log files (default: ./logs)')
    parser.add_argument('--skip-validation', action='store_true',
                       help='Skip prerequisite validation (not recommended)')
    args = parser.parse_args()

    # Setup logging
    log_file = setup_logging(args.log_dir)
    log("=== Starting build-links ===", log_file)
    log(f"Log file: {log_file}", log_file)

    # Connect to database
    try:
        conn = get_db_connection()
    except Exception as e:
        log(f"✗ Failed to connect to database: {e}", log_file)
        sys.exit(1)

    try:
        # Find or use schema
        if args.schema:
            schema = args.schema
        else:
            schema = find_schema(conn)
            if not schema:
                log("✗ Could not find turrutebasen schema. Use --schema to specify.", log_file)
                sys.exit(1)

        log(f"Using schema: {schema}", log_file)

        # Validate prerequisites
        if not args.skip_validation:
            log("==> Validating prerequisites...", log_file)
            if not validate_prerequisites(conn, schema, log_file):
                log("✗ Prerequisites validation failed - aborting", log_file)
                sys.exit(1)
            log("  ✓ Prerequisites validated", log_file)

        # Create tables
        log("==> Creating link tables...", log_file)
        try:
            create_tables(conn, schema)
            log("  ✓ Tables created", log_file)
        except Exception as e:
            log(f"✗ Failed to create tables: {e}", log_file)
            sys.exit(1)

        # Load data
        log("==> Loading data...", log_file)
        try:
            anchor_nodes = load_anchor_nodes(conn, schema)
            log(f"  ✓ Loaded {len(anchor_nodes)} anchor nodes", log_file)

            segments_dict, adjacency = load_segments(conn, schema)
            log(f"  ✓ Loaded {len(segments_dict)} segments", log_file)

            if not segments_dict:
                log("⚠ Warning: No segments found", log_file)
                sys.exit(0)  # Not an error, just nothing to do
        except Exception as e:
            log(f"✗ Failed to load data: {e}", log_file)
            sys.exit(1)

        # Build links
        log("==> Building links...", log_file)
        try:
            links, link_segments, errors = build_links(segments_dict, adjacency, anchor_nodes)
            log(f"  ✓ Built {len(links)} links from {len(link_segments)} segments", log_file)
            if errors:
                log(f"  ⚠ Found {len(errors)} errors (dangling/branch/loop)", log_file)
        except Exception as e:
            log(f"✗ Failed to build links: {e}", log_file)
            sys.exit(1)

        # Insert into database
        log("==> Inserting links into database...", log_file)
        try:
            insert_links(conn, schema, links, link_segments)
            log("  ✓ Links inserted", log_file)
        except Exception as e:
            log(f"✗ Failed to insert links: {e}", log_file)
            sys.exit(1)

        # Update geometries
        log("==> Updating link geometries...", log_file)
        try:
            update_link_geometries(conn, schema)
            log("  ✓ Link geometries updated", log_file)
        except Exception as e:
            log(f"✗ Failed to update geometries: {e}", log_file)
            sys.exit(1)

        # Print QA report
        used_segment_ids = {ls['segment_id'] for ls in link_segments}
        log("==> QA Report", log_file)
        print_qa_report(links, len(segments_dict), used_segment_ids, errors)

        # Also log QA report to file
        with open(log_file, 'a') as f:
            f.write("\n" + "="*60 + "\n")
            f.write("QA REPORT\n")
            f.write("="*60 + "\n")
            f.write(f"Total links:              {len(links)}\n")
            f.write(f"Total segments:           {len(segments_dict)}\n")
            f.write(f"Segments used:            {len(used_segment_ids)}\n")
            f.write(f"Segments unused:          {len(segments_dict) - len(used_segment_ids)}\n")
            f.write(f"Errors (dangling/branch/loop): {len(errors)}\n")
            f.write("="*60 + "\n")

        log("=== Build-links completed successfully ===", log_file)

    except KeyboardInterrupt:
        log("⚠ Interrupted by user", log_file)
        sys.exit(130)
    except Exception as e:
        log(f"✗ Unexpected error: {e}", log_file)
        import traceback
        log(traceback.format_exc(), log_file, also_print=False)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == '__main__':
    main()

