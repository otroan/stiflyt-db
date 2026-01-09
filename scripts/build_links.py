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
import json
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
                segment_objids BIGINT[], -- Array of segment objids for joining with fotruteinfo
                segment_gaps JSONB -- Gap information: gap_count, max_gap_m, avg_gap_m, gap_segment_ids array
            )
        """)
        
        # Add segment_gaps column if table exists but column is missing (migration)
        cur.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = 'links' AND column_name = 'segment_gaps'
                ) THEN
                    ALTER TABLE {schema}.links ADD COLUMN segment_gaps JSONB;
                END IF;
            END $$;
        """, (schema,))

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


def load_route_info(conn, schema: str) -> Dict[int, Set[str]]:
    """Load route information for segments.
    
    Returns:
        segment_routes: {segment_id: {rutenummer, ...}}
    """
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT fotrute_fk, rutenummer
            FROM {schema}.fotruteinfo
            WHERE rutenummer IS NOT NULL
        """)
        
        segment_routes: Dict[int, Set[str]] = defaultdict(set)
        for row in cur.fetchall():
            segment_id = row[0]
            rutenummer = row[1]
            segment_routes[segment_id].add(rutenummer)
    
    return dict(segment_routes)


def build_route_continuous_geometries(
    links: List[Dict],
    link_segments: List[Dict],
    segment_routes: Dict[int, Set[str]],
    conn,
    schema: str,
    log_file: Optional[Path]
) -> None:
    """Build continuous geometries for routes using Python (much faster than SQL).
    
    Groups links by route, orders them, and creates continuous geometry.
    """
    
    # Map link_id to routes it belongs to
    link_routes: Dict[int, Set[str]] = defaultdict(set)
    for ls in link_segments:
        link_id = ls['link_id']
        segment_id = ls['segment_id']
        if segment_id in segment_routes:
            link_routes[link_id].update(segment_routes[segment_id])
    
    # Group links by route
    route_links: Dict[str, List[Dict]] = defaultdict(list)
    link_id_to_link = {link['link_id']: link for link in links}
    
    for link_id, routes in link_routes.items():
        link = link_id_to_link[link_id]
        for rutenummer in routes:
            route_links[rutenummer].append(link)
    
    log(f"  Building continuous geometries for {len(route_links)} routes...", log_file)
    
    # Build continuous geometry for each route
    route_geom_data = []
    
    for rutenummer, route_link_list in route_links.items():
        if len(route_link_list) == 0:
            continue
        
        if len(route_link_list) == 1:
            # Single link - use its geometry directly (will fetch from DB)
            link = route_link_list[0]
            route_geom_data.append({
                'rutenummer': rutenummer,
                'link_ids': [link['link_id']],
                'link_orientations': [True]  # True = use as-is, False = reverse
            })
            continue
        
        # Multiple links - need to order and connect them
        # Build adjacency for links in this route
        link_adjacency: Dict[int, List[Tuple[int, bool]]] = defaultdict(list)
        # (link_id, needs_reverse)
        
        for link in route_link_list:
            link_id = link['link_id']
            a_node = link['a_node']
            b_node = link['b_node']
            
            # Find connecting links
            for other_link in route_link_list:
                if other_link['link_id'] == link_id:
                    continue
                other_a = other_link['a_node']
                other_b = other_link['b_node']
                
                # This link's b_node connects to other link's a_node
                if b_node == other_a:
                    link_adjacency[link_id].append((other_link['link_id'], False))
                # This link's b_node connects to other link's b_node (need to reverse other)
                elif b_node == other_b:
                    link_adjacency[link_id].append((other_link['link_id'], True))
        
        # Find start link (link with a_node that has no incoming connection)
        start_link_id = None
        for link in route_link_list:
            link_id = link['link_id']
            a_node = link['a_node']
            # Check if any other link's b_node connects to this a_node
            has_incoming = any(
                other_link['b_node'] == a_node
                for other_link in route_link_list
                if other_link['link_id'] != link_id
            )
            if not has_incoming:
                start_link_id = link_id
                break
        
        if not start_link_id:
            # No clear start, use first link
            start_link_id = route_link_list[0]['link_id']
        
        # Traverse links in order
        ordered_link_ids = []
        ordered_orientations = []
        visited_links = set()
        current_link_id = start_link_id
        
        while current_link_id and current_link_id not in visited_links:
            visited_links.add(current_link_id)
            ordered_link_ids.append(current_link_id)
            ordered_orientations.append(False)  # Use as-is
            
            # Find next link
            next_link = None
            if current_link_id in link_adjacency:
                for next_id, needs_reverse in link_adjacency[current_link_id]:
                    if next_id not in visited_links:
                        next_link = (next_id, needs_reverse)
                        break
            
            if next_link:
                current_link_id, needs_reverse = next_link
                # Store orientation for next link
                if len(ordered_orientations) < len(ordered_link_ids):
                    ordered_orientations.append(needs_reverse)
            else:
                current_link_id = None
        
        route_geom_data.append({
            'rutenummer': rutenummer,
            'link_ids': ordered_link_ids,
            'link_orientations': ordered_orientations,
            'all_links_traversed': len(ordered_link_ids) == len(route_link_list),
            'has_duplicate_links': len(ordered_link_ids) != len(set(ordered_link_ids))
        })
    
    # Now fetch geometries from database and combine them
    log(f"  Combining geometries for {len(route_geom_data)} routes...", log_file)
    
    with conn.cursor() as cur:
        # Create table for route continuous geometries
        cur.execute(f"""
            DROP TABLE IF EXISTS {schema}.route_continuous_geometries CASCADE;
            CREATE TABLE {schema}.route_continuous_geometries (
                rutenummer TEXT PRIMARY KEY,
                continuous_geometry GEOMETRY,
                multilinestring_reason TEXT
            );
        """)
        
        # Fetch all link geometries once
        cur.execute(f"""
            SELECT link_id, geom
            FROM {schema}.links
            WHERE geom IS NOT NULL
        """)
        link_geometries = {row[0]: row[1] for row in cur.fetchall()}
        
        # For each route, combine link geometries
        for route_data in route_geom_data:
            rutenummer = route_data['rutenummer']
            link_ids = route_data['link_ids']
            orientations = route_data['link_orientations']
            
            if not link_ids:
                continue
            
            # Build list of geometries with proper orientation
            geom_list = []
            for i, link_id in enumerate(link_ids):
                if link_id not in link_geometries:
                    continue
                geom = link_geometries[link_id]
                needs_reverse = orientations[i] if i < len(orientations) else False
                if needs_reverse:
                    # Reverse geometry
                    cur.execute(f"SELECT ST_Reverse(%s::geometry) as geom", (geom,))
                    reversed_geom = cur.fetchone()[0]
                    geom_list.append(reversed_geom)
                else:
                    geom_list.append(geom)
            
            if not geom_list:
                continue
            
            # Combine geometries and analyze why it might be MultiLineString
            if len(geom_list) == 1:
                # Single geometry - check if it's already MultiLineString
                single_geom = geom_list[0]
                cur.execute(f"""
                    SELECT 
                        ST_GeometryType(%s::geometry) as geom_type,
                        ST_NumGeometries(%s::geometry) as num_geoms
                """, (single_geom, single_geom))
                geom_info = cur.fetchone()
                
                if geom_info[0] == 'ST_LineString' or (geom_info[0] == 'ST_MultiLineString' and geom_info[1] == 1):
                    reason = 'single_linestring'
                else:
                    reason = 'link_is_multilinestring'
                
                cur.execute(f"""
                    INSERT INTO {schema}.route_continuous_geometries (rutenummer, continuous_geometry, multilinestring_reason)
                    VALUES (%s, %s, %s)
                """, (rutenummer, single_geom, reason))
            else:
                # Multiple geometries - combine them and analyze
                # First, check if traversal found all links (linear) or missed some (branch/loop)
                all_links_traversed = route_data.get('all_links_traversed', True)
                has_duplicate_links = route_data.get('has_duplicate_links', False)
                
                # Use ST_Collect with array via unnest in FROM clause
                cur.execute(f"""
                    WITH merged AS (
                        SELECT ST_LineMerge(ST_Collect(geom)) as merged_geom
                        FROM unnest(%s::geometry[]) AS geom
                    )
                    SELECT 
                        merged_geom,
                        ST_GeometryType(merged_geom) as geom_type,
                        ST_NumGeometries(merged_geom) as num_geoms
                    FROM merged
                """, (geom_list,))
                
                result = cur.fetchone()
                merged_geom = result[0]
                geom_type = result[1]
                num_geoms = result[2]
                
                # Determine reason for MultiLineString
                if num_geoms == 1:
                    reason = 'single_linestring'
                elif not all_links_traversed or has_duplicate_links:
                    # Traversal didn't find all links or found duplicates = loop or branch
                    reason = 'loop_or_branch'
                else:
                    # All links traversed in order, but still MultiLineString
                    # Check for gaps between consecutive geometries
                    cur.execute(f"""
                        WITH ordered_geoms AS (
                            SELECT 
                                geom,
                                idx
                            FROM unnest(%s::geometry[]) WITH ORDINALITY AS t(geom, idx)
                        ),
                        gaps AS (
                            SELECT 
                                ST_Distance(
                                    ST_EndPoint(og1.geom),
                                    ST_StartPoint(og2.geom)
                                ) as gap_distance
                            FROM ordered_geoms og1
                            JOIN ordered_geoms og2 ON og2.idx = og1.idx + 1
                        )
                        SELECT 
                            COUNT(*) as gap_count,
                            AVG(gap_distance) as avg_gap,
                            MAX(gap_distance) as max_gap
                        FROM gaps
                    """, (geom_list,))
                    
                    gap_info = cur.fetchone()
                    if gap_info and gap_info[0] > 0:
                        avg_gap = gap_info[1] if gap_info[1] else 0
                        max_gap = gap_info[2] if gap_info[2] else 0
                        
                        if max_gap < 0.01:  # Less than 1cm - likely precision issue
                            reason = 'precision_gap'
                        else:
                            reason = 'disconnected_components'
                    else:
                        # No gaps detected but still MultiLineString - might be traversal issue
                        reason = 'traversal_issue'
                
                cur.execute(f"""
                    INSERT INTO {schema}.route_continuous_geometries (rutenummer, continuous_geometry, multilinestring_reason)
                    VALUES (%s, %s, %s)
                """, (rutenummer, merged_geom, reason))
        
        conn.commit()
        log(f"  ✓ Created continuous geometries for {len(route_geom_data)} routes", log_file)
        
        # Create index on table
        cur.execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_route_continuous_geometries_rutenummer
            ON {schema}.route_continuous_geometries (rutenummer);
        """)
        conn.commit()


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
    - Uses ST_LineMerge() to merge continuous segments into LineString
    - Validates that segment endpoints match exactly (no gaps allowed)
    - Flags and logs any gaps found between consecutive segments
    
    This ensures the link geometry flows continuously from a_node to b_node.
    If segments are continuous, link will be LineString; if gaps exist, MultiLineString.
    """
    with conn.cursor() as cur:
        # First, validate and detect gaps, then update geometries
        cur.execute(f"""
            WITH oriented_segments AS (
                SELECT
                    ls.link_id,
                    ls.seq,
                    ls.segment_id,
                    CASE
                        WHEN f.source_node = ls.from_node THEN f.senterlinje
                        ELSE ST_Reverse(f.senterlinje)
                    END as oriented_geom
                FROM {schema}.link_segments ls
                JOIN {schema}.fotrute f ON ls.segment_id = f.objid
            ),
            segment_gaps AS (
                SELECT
                    os1.link_id,
                    os1.seq as seq1,
                    os1.segment_id as seg1_id,
                    os2.seq as seq2,
                    os2.segment_id as seg2_id,
                    ST_Distance(
                        ST_EndPoint(os1.oriented_geom),
                        ST_StartPoint(os2.oriented_geom)
                    ) as gap_distance
                FROM oriented_segments os1
                JOIN oriented_segments os2 
                    ON os1.link_id = os2.link_id 
                    AND os2.seq = os1.seq + 1
            ),
            link_gaps AS (
                SELECT
                    link_id,
                    COUNT(*) as gap_count,
                    MAX(gap_distance) as max_gap,
                    AVG(gap_distance) as avg_gap,
                    array_agg(seg1_id ORDER BY seq1) as gap_segment_ids
                FROM segment_gaps
                WHERE gap_distance > 0.0  -- Any gap, even tiny ones
                GROUP BY link_id
            ),
            updated_geoms AS (
                SELECT
                    ls.link_id,
                    ST_LineMerge(
                        ST_Collect(
                            CASE
                                WHEN f.source_node = ls.from_node THEN f.senterlinje
                                ELSE ST_Reverse(f.senterlinje)
                            END
                            ORDER BY ls.seq
                        )
                    ) as geom
                FROM {schema}.link_segments ls
                JOIN {schema}.fotrute f ON ls.segment_id = f.objid
                GROUP BY ls.link_id
            )
            SELECT
                ug.link_id,
                ug.geom,
                COALESCE(lg.gap_count, 0) as gap_count,
                COALESCE(lg.max_gap, 0.0) as max_gap,
                COALESCE(lg.avg_gap, 0.0) as avg_gap,
                lg.gap_segment_ids
            FROM updated_geoms ug
            LEFT JOIN link_gaps lg ON ug.link_id = lg.link_id
        """)
        
        gap_warnings = []
        for row in cur.fetchall():
            link_id = row[0]
            geom = row[1]
            gap_count = row[2]
            max_gap = row[3]
            avg_gap = row[4]
            gap_segment_ids = row[5]
            
            if gap_count > 0:
                gap_warnings.append({
                    'link_id': link_id,
                    'gap_count': gap_count,
                    'max_gap': max_gap,
                    'avg_gap': avg_gap,
                    'segment_ids': gap_segment_ids
                })
                
                # Store gap information as JSONB
                gap_info = {
                    'gap_count': gap_count,
                    'max_gap_m': float(max_gap),
                    'avg_gap_m': float(avg_gap),
                    'gap_segment_ids': gap_segment_ids if gap_segment_ids else []
                }
            else:
                gap_info = None  # No gaps - store NULL
            
            # Update geometry and gap information
            cur.execute(f"""
                UPDATE {schema}.links
                SET geom = %s, segment_gaps = %s
                WHERE link_id = %s
            """, (geom, json.dumps(gap_info) if gap_info else None, link_id))
        
        conn.commit()
        
        # Log warnings for gaps
        if gap_warnings:
            print(f"⚠ WARNING: Found {len(gap_warnings)} link(s) with gaps between segments:")
            for warning in gap_warnings[:10]:  # Show first 10
                print(f"  Link {warning['link_id']}: {warning['gap_count']} gap(s), "
                      f"max gap: {warning['max_gap']:.6f}m, "
                      f"avg gap: {warning['avg_gap']:.6f}m, "
                      f"segments: {warning['segment_ids']}")
            if len(gap_warnings) > 10:
                print(f"  ... and {len(gap_warnings) - 10} more links with gaps")
        else:
            print(f"✓ Updated link geometries (oriented, ordered by seq, and merged where continuous)")
            print(f"✓ All segment endpoints match exactly - no gaps detected")


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
    """Validate that required tables and materialized views exist before building links.

    Note: anchor_nodes is a materialized view, not a table, so we need to check
    both information_schema.tables and pg_matviews.
    """
    try:
        if PSYCOPG_VERSION == 2:
            with conn.cursor() as cur:
                # Check for tables (nodes, fotrute)
                cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                      AND table_name IN ('nodes', 'fotrute')
                """, (schema,))
                tables = {row[0] for row in cur.fetchall()}

                # Check for materialized views (anchor_nodes)
                cur.execute("""
                    SELECT matviewname
                    FROM pg_matviews
                    WHERE schemaname = %s
                      AND matviewname = 'anchor_nodes'
                """, (schema,))
                matviews = {row[0] for row in cur.fetchall()}

                # Combine found objects
                found_objects = tables | matviews
                required = {'nodes', 'fotrute', 'anchor_nodes'}
                missing = required - found_objects

                if missing:
                    missing_list = []
                    for name in missing:
                        if name == 'anchor_nodes':
                            missing_list.append(f"{name} (materialized view)")
                        else:
                            missing_list.append(f"{name} (table)")
                    log(f"✗ Missing required objects: {', '.join(missing_list)}", log_file)
                    if 'anchor_nodes' in missing:
                        log(f"  Run 'make run-migrations' to create anchor_nodes materialized view", log_file)
                    return False

                # Check that objects have data
                for name in ['nodes', 'fotrute', 'anchor_nodes']:
                    # Use parameterized query with identifier quoting for safety
                    from psycopg2 import sql
                    query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                        sql.Identifier(schema),
                        sql.Identifier(name)
                    )
                    cur.execute(query)
                    count = cur.fetchone()[0]
                    if count == 0:
                        obj_type = 'materialized view' if name == 'anchor_nodes' else 'table'
                        log(f"✗ {obj_type.capitalize()} {schema}.{name} is empty", log_file)
                        return False
                    obj_type = 'materialized view' if name == 'anchor_nodes' else 'table'
                    log(f"  ✓ {obj_type.capitalize()} {schema}.{name}: {count:,} rows", log_file)

                return True
        else:
            # Similar logic for psycopg3
            with conn.cursor() as cur:
                # Check for tables (nodes, fotrute)
                cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                      AND table_name IN ('nodes', 'fotrute')
                """, (schema,))
                tables = {row[0] for row in cur.fetchall()}

                # Check for materialized views (anchor_nodes)
                cur.execute("""
                    SELECT matviewname
                    FROM pg_matviews
                    WHERE schemaname = %s
                      AND matviewname = 'anchor_nodes'
                """, (schema,))
                matviews = {row[0] for row in cur.fetchall()}

                # Combine found objects
                found_objects = tables | matviews
                required = {'nodes', 'fotrute', 'anchor_nodes'}
                missing = required - found_objects

                if missing:
                    missing_list = []
                    for name in missing:
                        if name == 'anchor_nodes':
                            missing_list.append(f"{name} (materialized view)")
                        else:
                            missing_list.append(f"{name} (table)")
                    log(f"✗ Missing required objects: {', '.join(missing_list)}", log_file)
                    if 'anchor_nodes' in missing:
                        log(f"  Run 'make run-migrations' to create anchor_nodes materialized view", log_file)
                    return False

                for name in ['nodes', 'fotrute', 'anchor_nodes']:
                    # Use parameterized query with identifier quoting for safety
                    from psycopg import sql
                    query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                        sql.Identifier(schema),
                        sql.Identifier(name)
                    )
                    cur.execute(query)
                    count = cur.fetchone()[0]
                    if count == 0:
                        obj_type = 'materialized view' if name == 'anchor_nodes' else 'table'
                        log(f"✗ {obj_type.capitalize()} {schema}.{name} is empty", log_file)
                        return False
                    obj_type = 'materialized view' if name == 'anchor_nodes' else 'table'
                    log(f"  ✓ {obj_type.capitalize()} {schema}.{name}: {count:,} rows", log_file)

                return True
    except Exception as e:
        log(f"✗ Error validating prerequisites: {e}", log_file)
        return False


def build_links_main(args=None, log_file=None) -> int:
    """Main entry point that can be called programmatically.

    Args:
        args: Optional argparse.Namespace with arguments. If None, will parse from command line.
        log_file: Optional log file path. If None, will be created from args.log_dir.

    Returns:
        Exit code: 0 for success, non-zero for failure
    """
    # Parse arguments if not provided
    if args is None:
        parser = argparse.ArgumentParser(description='Build links from segments and anchor nodes')
        parser.add_argument('--schema', type=str, help='Schema name (default: auto-detect turrutebasen)')
        parser.add_argument('--log-dir', type=Path, default=Path('./logs'),
                           help='Directory for log files (default: ./logs)')
        parser.add_argument('--skip-validation', action='store_true',
                           help='Skip prerequisite validation (not recommended)')
        parser.add_argument('--quiet', action='store_true',
                           help='Quiet mode: suppress QA report output (useful when called from migrations)')
        args = parser.parse_args()

    # Setup logging
    if log_file is None:
        log_file = setup_logging(args.log_dir if hasattr(args, 'log_dir') else Path('./logs'))

    quiet = getattr(args, 'quiet', False)

    log("=== Starting build-links ===", log_file)
    if not quiet:
        log(f"Log file: {log_file}", log_file)

    # Connect to database
    conn = None
    try:
        conn = get_db_connection()
    except Exception as e:
        log(f"✗ Failed to connect to database: {e}", log_file)
        return 1

    try:
        # Find or use schema
        if hasattr(args, 'schema') and args.schema:
            schema = args.schema
        else:
            schema = find_schema(conn)
            if not schema:
                log("✗ Could not find turrutebasen schema. Use --schema to specify.", log_file)
                return 1

        log(f"Using schema: {schema}", log_file)

        # Validate prerequisites
        skip_validation = getattr(args, 'skip_validation', False)
        if not skip_validation:
            log("==> Validating prerequisites...", log_file)
            if not validate_prerequisites(conn, schema, log_file):
                log("✗ Prerequisites validation failed - aborting", log_file)
                return 1
            log("  ✓ Prerequisites validated", log_file)

        # Create tables
        log("==> Creating link tables...", log_file)
        try:
            create_tables(conn, schema)
            log("  ✓ Tables created", log_file)
        except Exception as e:
            log(f"✗ Failed to create tables: {e}", log_file)
            return 1

        # Load data
        log("==> Loading data...", log_file)
        try:
            anchor_nodes = load_anchor_nodes(conn, schema)
            log(f"  ✓ Loaded {len(anchor_nodes)} anchor nodes", log_file)

            segments_dict, adjacency = load_segments(conn, schema)
            log(f"  ✓ Loaded {len(segments_dict)} segments", log_file)

            if not segments_dict:
                log("⚠ Warning: No segments found", log_file)
                return 0  # Not an error, just nothing to do
            
            # Load route info for continuous geometry building
            segment_routes = load_route_info(conn, schema)
            log(f"  ✓ Loaded route info for {len(segment_routes)} segments", log_file)
        except Exception as e:
            log(f"✗ Failed to load data: {e}", log_file)
            return 1

        # Build links
        log("==> Building links...", log_file)
        try:
            links, link_segments, errors = build_links(segments_dict, adjacency, anchor_nodes)
            log(f"  ✓ Built {len(links)} links from {len(link_segments)} segments", log_file)
            if errors:
                log(f"  ⚠ Found {len(errors)} errors (dangling/branch/loop)", log_file)
        except Exception as e:
            log(f"✗ Failed to build links: {e}", log_file)
            return 1

        # Insert into database
        log("==> Inserting links into database...", log_file)
        try:
            insert_links(conn, schema, links, link_segments)
            log("  ✓ Links inserted", log_file)
        except Exception as e:
            log(f"✗ Failed to insert links: {e}", log_file)
            return 1

        # Update geometries
        log("==> Updating link geometries...", log_file)
        try:
            update_link_geometries(conn, schema)
            log("  ✓ Link geometries updated", log_file)
        except Exception as e:
            log(f"✗ Failed to update geometries: {e}", log_file)
            return 1

        # Build route continuous geometries using Python (much faster than SQL)
        log("==> Building route continuous geometries...", log_file)
        try:
            build_route_continuous_geometries(
                links, link_segments, segment_routes, conn, schema, log_file
            )
            log("  ✓ Route continuous geometries built", log_file)
        except Exception as e:
            log(f"✗ Failed to build route continuous geometries: {e}", log_file)
            import traceback
            log(traceback.format_exc(), log_file)
            # Non-fatal - continue even if this fails
            log("  (Continuing despite error)", log_file)

        # Print QA report (unless quiet mode)
        used_segment_ids = {ls['segment_id'] for ls in link_segments}
        if not quiet:
            log("==> QA Report", log_file)
            print_qa_report(links, len(segments_dict), used_segment_ids, errors)
        else:
            # In quiet mode, just log summary to file
            log(f"QA Summary: {len(links)} links, {len(used_segment_ids)}/{len(segments_dict)} segments used, {len(errors)} errors", log_file)

        # Always log QA report to file
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
        return 0

    except KeyboardInterrupt:
        log("⚠ Interrupted by user", log_file)
        return 130
    except Exception as e:
        log(f"✗ Unexpected error: {e}", log_file)
        import traceback
        log(traceback.format_exc(), log_file, also_print=False)
        return 1
    finally:
        if conn:
            conn.close()


def main():
    """CLI entry point."""
    exit_code = build_links_main()
    sys.exit(exit_code)


if __name__ == '__main__':
    main()

