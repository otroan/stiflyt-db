"""
Integration tests for route views and queries.

Tests that verify:
- route_segments view exists and has correct structure
- routes materialized view exists and has correct structure
- Route queries work correctly (lookup, listing, filtering, bounding box)
- Indexes are created for performance
"""

import pytest
import psycopg2
from psycopg2.extras import RealDictCursor

from scripts import load_dataset


def _connection_kwargs(db_params):
    kwargs = {
        "dbname": db_params.get("database"),
        "user": db_params.get("user"),
        "password": db_params.get("password") or None,
        "host": db_params.get("host") or None,
        "port": db_params.get("port") or None,
    }
    return {k: v for k, v in kwargs.items() if v is not None}


@pytest.mark.integration
def test_route_segments_view_exists():
    """Test that route_segments view exists after migration 007."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.views
                    WHERE table_schema = 'stiflyt' AND table_name = 'route_segments'
                )
            """)
            exists = cur.fetchone()[0]
            assert exists, "route_segments view should exist in stiflyt schema"
    finally:
        conn.close()


@pytest.mark.integration
def test_routes_view_exists():
    """Test that routes view exists in stiflyt schema after migration 007.

    Note: stiflyt.routes is a VIEW (not materialized view) that points to
    the materialized view in the dynamic schema. This matches the pattern
    used by migration 005 for other tables/materialized views.
    """
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        with conn.cursor() as cur:
            # Check for view in stiflyt schema
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.views
                    WHERE table_schema = 'stiflyt' AND table_name = 'routes'
                )
            """)
            exists = cur.fetchone()[0]
            assert exists, "routes view should exist in stiflyt schema"

            # Also verify the underlying materialized view exists in dynamic schema
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_matviews mv
                    JOIN pg_namespace n ON mv.schemaname = n.nspname
                    WHERE n.nspname LIKE 'turogfriluftsruter_%'
                      AND mv.matviewname = 'routes'
                )
            """)
            underlying_exists = cur.fetchone()[0]
            assert underlying_exists, "Underlying routes materialized view should exist in dynamic schema"
    finally:
        conn.close()


@pytest.mark.integration
def test_route_segments_has_required_columns():
    """Test that route_segments has all required columns."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'stiflyt' AND table_name = 'route_segments'
                ORDER BY column_name
            """)
            columns = {row[0]: row[1] for row in cur.fetchall()}

            required_columns = {
                'rutenummer': 'text',
                'segment_objid': 'bigint',
                'senterlinje': 'USER-DEFINED',  # geometry type
                'source_node': 'integer',
                'target_node': 'integer',
                'rutenavn': 'text',
            }

            for col_name in required_columns.keys():
                assert col_name in columns, f"route_segments should have column {col_name}"
    finally:
        conn.close()


@pytest.mark.integration
def test_routes_has_required_columns():
    """Test that routes materialized view has all required columns."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        with conn.cursor() as cur:
            # Use pg_attribute for materialized views
            cur.execute("""
                SELECT a.attname as column_name,
                       pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type
                FROM pg_catalog.pg_attribute a
                JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = 'stiflyt'
                  AND c.relname = 'routes'
                  AND a.attnum > 0
                  AND NOT a.attisdropped
                ORDER BY a.attname
            """)
            columns = {row[0]: row[1] for row in cur.fetchall()}

            required_columns = {
                'rutenummer': 'text',
                'rutenavn': 'text',
                'route_geometry': 'USER-DEFINED',  # geometry type
                'total_length_m': 'double precision',
                'segment_count': 'bigint',
                'segment_objids': 'ARRAY',  # bigint array
            }

            for col_name in required_columns.keys():
                assert col_name in columns, f"routes should have column {col_name}"
    finally:
        conn.close()


@pytest.mark.integration
def test_route_lookup_by_rutenummer():
    """Test that we can look up a route by rutenummer."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # First, get a sample rutenummer
            cur.execute("""
                SELECT rutenummer
                FROM stiflyt.routes
                LIMIT 1
            """)
            result = cur.fetchone()
            if not result:
                pytest.skip("No routes found in database")

            sample_rutenummer = result['rutenummer']

            # Now test lookup
            cur.execute("""
                SELECT
                    rutenummer,
                    rutenavn,
                    total_length_m,
                    segment_count,
                    route_geometry
                FROM stiflyt.routes
                WHERE rutenummer = %s
            """, (sample_rutenummer,))
            route = cur.fetchone()

            assert route is not None, f"Should find route with rutenummer={sample_rutenummer}"
            assert route['rutenummer'] == sample_rutenummer
            assert route['segment_count'] > 0
            assert route['total_length_m'] > 0
            assert route['route_geometry'] is not None
    finally:
        conn.close()


@pytest.mark.integration
def test_fotrute_spatial_lookup():
    """Basic spatial lookup on stiflyt.fotrute."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT senterlinje
                FROM stiflyt.fotrute
                WHERE senterlinje IS NOT NULL
                LIMIT 1
            """)
            row = cur.fetchone()
            if not row:
                pytest.skip("No fotrute geometries found")

            cur.execute("""
                SELECT ST_DWithin(senterlinje, ST_PointOnSurface(senterlinje), 0)
                FROM stiflyt.fotrute
                WHERE senterlinje IS NOT NULL
                LIMIT 1
            """)
            assert cur.fetchone()[0] is True
    finally:
        conn.close()


@pytest.mark.integration
def test_fotruteinfo_lookup_by_rutenummer():
    """Lookup in stiflyt.fotruteinfo by rutenummer."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT rutenummer
                FROM stiflyt.fotruteinfo
                WHERE rutenummer IS NOT NULL
                LIMIT 1
            """)
            row = cur.fetchone()
            if not row:
                pytest.skip("No rutenummer found in fotruteinfo")

            rutenummer = row[0]
            cur.execute("""
                SELECT COUNT(*)
                FROM stiflyt.fotruteinfo
                WHERE rutenummer = %s
            """, (rutenummer,))
            assert cur.fetchone()[0] > 0
    finally:
        conn.close()


@pytest.mark.integration
def test_route_listing():
    """Test that we can list all routes."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    rutenummer,
                    rutenavn,
                    vedlikeholdsansvarlig,
                    rutetype,
                    segment_count,
                    total_length_m
                FROM stiflyt.routes
                ORDER BY rutenummer
                LIMIT 100
            """)
            routes = cur.fetchall()

            assert len(routes) > 0, "Should have at least one route"

            # Verify structure
            for route in routes:
                assert route['rutenummer'] is not None
                assert route['segment_count'] > 0
                assert route['total_length_m'] > 0
    finally:
        conn.close()


@pytest.mark.integration
def test_route_filtering_by_prefix():
    """Test that we can filter routes by prefix (e.g., 'bre', 'jot', 'ron')."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get all unique prefixes (first 3 characters)
            cur.execute("""
                SELECT DISTINCT LEFT(rutenummer, 3) as prefix
                FROM stiflyt.routes
                WHERE rutenummer IS NOT NULL
                ORDER BY prefix
                LIMIT 10
            """)
            prefixes = [row['prefix'] for row in cur.fetchall()]

            if not prefixes:
                pytest.skip("No routes with prefixes found")

            # Test filtering by first prefix
            test_prefix = prefixes[0]
            cur.execute("""
                SELECT rutenummer
                FROM stiflyt.routes
                WHERE rutenummer LIKE %s
                ORDER BY rutenummer
                LIMIT 10
            """, (f"{test_prefix}%",))
            filtered_routes = cur.fetchall()

            assert len(filtered_routes) > 0, f"Should find routes with prefix {test_prefix}"

            # Verify all returned routes have the prefix
            for route in filtered_routes:
                assert route['rutenummer'].startswith(test_prefix), \
                    f"Route {route['rutenummer']} should start with {test_prefix}"
    finally:
        conn.close()


@pytest.mark.integration
def test_route_filtering_by_organization():
    """Test that we can filter routes by vedlikeholdsansvarlig (organization)."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get a sample organization
            cur.execute("""
                SELECT DISTINCT vedlikeholdsansvarlig
                FROM stiflyt.routes
                WHERE vedlikeholdsansvarlig IS NOT NULL
                LIMIT 1
            """)
            result = cur.fetchone()
            if not result:
                pytest.skip("No routes with vedlikeholdsansvarlig found")

            test_org = result['vedlikeholdsansvarlig']

            # Test filtering
            cur.execute("""
                SELECT rutenummer, vedlikeholdsansvarlig
                FROM stiflyt.routes
                WHERE vedlikeholdsansvarlig = %s
                LIMIT 10
            """, (test_org,))
            filtered_routes = cur.fetchall()

            assert len(filtered_routes) > 0, f"Should find routes for organization {test_org}"

            # Verify all returned routes have the correct organization
            for route in filtered_routes:
                assert route['vedlikeholdsansvarlig'] == test_org, \
                    f"Route {route['rutenummer']} should have vedlikeholdsansvarlig={test_org}"
    finally:
        conn.close()


@pytest.mark.integration
def test_routes_bounding_box_query():
    """Test that we can query routes within a bounding box."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # First, get a sample route to determine bounding box
            cur.execute("""
                SELECT
                    rutenummer,
                    ST_Envelope(route_geometry) as bbox
                FROM stiflyt.routes
                WHERE route_geometry IS NOT NULL
                LIMIT 1
            """)
            result = cur.fetchone()
            if not result:
                pytest.skip("No routes with geometry found")

            # Extract bounding box coordinates
            bbox = result['bbox']
            cur.execute("""
                SELECT
                    ST_XMin(%s::geometry) as minx,
                    ST_YMin(%s::geometry) as miny,
                    ST_XMax(%s::geometry) as maxx,
                    ST_YMax(%s::geometry) as maxy
            """, (bbox, bbox, bbox, bbox))
            coords = cur.fetchone()

            # Expand bounding box slightly
            minx = coords['minx'] - 1000
            miny = coords['miny'] - 1000
            maxx = coords['maxx'] + 1000
            maxy = coords['maxy'] + 1000

            # Test bounding box query
            cur.execute("""
                SELECT
                    rutenummer,
                    rutenavn,
                    total_length_m
                FROM stiflyt.routes
                WHERE ST_Intersects(
                    route_geometry,
                    ST_MakeEnvelope(%s, %s, %s, %s, 25833)
                )
                LIMIT 10
            """, (minx, miny, maxx, maxy))
            routes_in_bbox = cur.fetchall()

            assert len(routes_in_bbox) > 0, "Should find at least one route in bounding box"

            # Verify the sample route is included
            route_numbers = [r['rutenummer'] for r in routes_in_bbox]
            assert result['rutenummer'] in route_numbers, \
                f"Sample route {result['rutenummer']} should be in bounding box results"
    finally:
        conn.close()


@pytest.mark.integration
def test_route_segments_query():
    """Test that we can query segments for a specific route."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get a sample route
            cur.execute("""
                SELECT rutenummer
                FROM stiflyt.routes
                LIMIT 1
            """)
            result = cur.fetchone()
            if not result:
                pytest.skip("No routes found")

            sample_rutenummer = result['rutenummer']

            # Query segments for this route
            cur.execute("""
                SELECT
                    segment_objid,
                    senterlinje,
                    source_node,
                    target_node,
                    rutenavn,
                    rutenummer
                FROM stiflyt.route_segments
                WHERE rutenummer = %s
                ORDER BY segment_objid
            """, (sample_rutenummer,))
            segments = cur.fetchall()

            assert len(segments) > 0, f"Should find segments for route {sample_rutenummer}"

            # Verify all segments belong to the route
            for segment in segments:
                assert segment['rutenummer'] == sample_rutenummer
                assert segment['segment_objid'] is not None
                assert segment['senterlinje'] is not None
    finally:
        conn.close()


@pytest.mark.integration
def test_routes_indexes_exist():
    """Test that required indexes exist on the underlying routes materialized view.

    Note: Indexes are on the materialized view in the dynamic schema, not on
    the stiflyt.routes view (views cannot have indexes).
    """
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        # First, find the dynamic schema
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
            if not result:
                pytest.skip("No turrutebasen schema found")
            dynamic_schema = result[0]

        # Check indexes on the underlying materialized view
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    a.attname as column_name,
                    am.amname as index_type,
                    idx.indisunique as is_unique
                FROM pg_index idx
                JOIN pg_class i ON idx.indexrelid = i.oid
                JOIN pg_class t ON idx.indrelid = t.oid
                JOIN pg_namespace n ON t.relnamespace = n.oid
                JOIN pg_am am ON i.relam = am.oid
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(idx.indkey)
                WHERE n.nspname = %s
                  AND t.relname = 'routes'
            """, (dynamic_schema,))
            all_indexes = cur.fetchall()

            # Check for unique index on rutenummer
            has_rutenummer_unique = any(
                col == 'rutenummer' and idx_type == 'btree' and is_unique
                for col, idx_type, is_unique in all_indexes
            )
            assert has_rutenummer_unique, "Should have unique BTREE index on rutenummer in underlying materialized view"

            # Check for GIST index on route_geometry
            has_geometry_gist = any(
                col == 'route_geometry' and idx_type == 'gist'
                for col, idx_type, is_unique in all_indexes
            )
            assert has_geometry_gist, "Should have GIST index on route_geometry in underlying materialized view"
    finally:
        conn.close()


@pytest.mark.integration
def test_route_segments_consistency():
    """Test that route_segments and routes are consistent."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get a sample route
            cur.execute("""
                SELECT rutenummer, segment_count
                FROM stiflyt.routes
                LIMIT 1
            """)
            route = cur.fetchone()
            if not route:
                pytest.skip("No routes found")

            # Count segments in route_segments
            cur.execute("""
                SELECT COUNT(*) as count
                FROM stiflyt.route_segments
                WHERE rutenummer = %s
            """, (route['rutenummer'],))
            segment_count = cur.fetchone()['count']

            # Segment count should match
            assert segment_count == route['segment_count'], \
                f"Segment count mismatch: route_segments has {segment_count}, routes says {route['segment_count']}"
    finally:
        conn.close()


@pytest.mark.integration
def test_route_geometry_aggregation():
    """Test that route geometry is properly aggregated from segments."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get a route with geometry
            cur.execute("""
                SELECT
                    r.rutenummer,
                    r.route_geometry,
                    r.total_length_m,
                    array_agg(rs.senterlinje) as segment_geometries
                FROM stiflyt.routes r
                JOIN stiflyt.route_segments rs ON rs.rutenummer = r.rutenummer
                WHERE r.route_geometry IS NOT NULL
                GROUP BY r.rutenummer, r.route_geometry, r.total_length_m
                LIMIT 1
            """)
            result = cur.fetchone()
            if not result:
                pytest.skip("No routes with geometry found")

            # Verify route geometry is not null
            assert result['route_geometry'] is not None, "Route geometry should not be null"

            # Verify total length is positive
            assert result['total_length_m'] > 0, "Total length should be positive"

            # Verify we have segments
            assert len(result['segment_geometries']) > 0, "Should have segment geometries"
    finally:
        conn.close()


@pytest.mark.integration
def test_links_with_routes_has_route_geometries():
    """Test that links_with_routes view has route_geometries column with continuous geometry."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Check that route_geometries column exists
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'stiflyt' AND table_name = 'links_with_routes'
                  AND column_name = 'route_geometries'
            """)
            column = cur.fetchone()
            assert column is not None, "links_with_routes should have route_geometries column"
            assert column['data_type'] == 'jsonb', "route_geometries should be JSONB type"

            # Check if stable view stiflyt.route_continuous_geometries exists
            # (links_with_routes depends on this view/table)
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.views
                    WHERE table_schema = 'stiflyt'
                      AND table_name = 'route_continuous_geometries'
                ) as view_exists
            """)
            result = cur.fetchone()
            view_exists = result['view_exists'] if result else False

            assert view_exists, "stiflyt.route_continuous_geometries view should exist (migration 005 should have created it, required for links_with_routes.route_geometries)"

            # Get a link that belongs to at least one route
            cur.execute("""
                SELECT
                    link_id,
                    rutenummer_list,
                    route_geometries
                FROM stiflyt.links_with_routes
                WHERE rutenummer_list IS NOT NULL
                  AND array_length(rutenummer_list, 1) > 0
                LIMIT 1
            """)
            link = cur.fetchone()
            if not link:
                pytest.skip("No links with routes found")

            assert link['rutenummer_list'] is not None, "rutenummer_list should not be null"
            assert len(link['rutenummer_list']) > 0, "Should have at least one route"

            # Test that route_geometries contains continuous geometry from route_continuous_geometries table
            test_rutenummer = link['rutenummer_list'][0]

            # Check if route_geometries contains the route
            # route_geometries is a JSONB object from route_continuous_geometries table
            # Use ->> to get text (GeoJSON string) instead of -> (JSONB object)
            cur.execute("""
                SELECT
                    route_geometries ? %s as has_route,
                    route_geometries->>%s as route_geom_json_text
                FROM stiflyt.links_with_routes
                WHERE link_id = %s
            """, (test_rutenummer, test_rutenummer, link['link_id']))
            result = cur.fetchone()

            # route_geometries should contain the route (from route_continuous_geometries table)
            if result and result['has_route'] and result['route_geom_json_text']:
                # route_geom_json_text is a GeoJSON string, convert it to geometry
                # Use ST_GeomFromGeoJSON to convert GeoJSON to PostGIS geometry
                cur.execute("""
                    SELECT
                        ST_IsValid(ST_GeomFromGeoJSON(%s)) as is_valid,
                        ST_GeometryType(ST_GeomFromGeoJSON(%s)) as geom_type,
                        ST_Length(ST_GeomFromGeoJSON(%s)) as length
                """, (result['route_geom_json_text'], result['route_geom_json_text'], result['route_geom_json_text']))
                geom_info = cur.fetchone()

                assert geom_info is not None, f"Should be able to parse geometry for {test_rutenummer}"
                assert geom_info['is_valid'], f"Route geometry for {test_rutenummer} should be valid"
                assert geom_info['length'] > 0, f"Route geometry for {test_rutenummer} should have positive length"
            else:
                # route_geometries might be null if route_continuous_geometries table hasn't been populated yet
                # This is OK, but we should at least verify the column exists
                assert link['route_geometries'] is not None or link['route_geometries'] == {}, \
                    "route_geometries should exist (may be null if build-links hasn't run yet)"
    finally:
        conn.close()


@pytest.mark.integration
def test_route_geometries_continuous():
    """Test that route_geometries provides continuous geometry for routes where possible."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Find a route that has multiple links (more likely to test continuity)
            cur.execute("""
                SELECT
                    lri.rutenummer,
                    COUNT(DISTINCT lri.link_id) as link_count
                FROM stiflyt.link_ruteinfo lri
                GROUP BY lri.rutenummer
                HAVING COUNT(DISTINCT lri.link_id) > 1
                ORDER BY link_count DESC
                LIMIT 1
            """)
            route_info = cur.fetchone()
            if not route_info:
                pytest.skip("No routes with multiple links found")

            test_rutenummer = route_info['rutenummer']

            # Check if stable view stiflyt.route_continuous_geometries exists
            # (links_with_routes depends on this view/table)
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.views
                    WHERE table_schema = 'stiflyt'
                      AND table_name = 'route_continuous_geometries'
                ) as view_exists
            """)
            result = cur.fetchone()
            view_exists = result['view_exists'] if result else False

            assert view_exists, "stiflyt.route_continuous_geometries view should exist (migration 005 should have created it, required for links_with_routes.route_geometries)"

            # Get the continuous geometry from links_with_routes (from route_continuous_geometries table)
            cur.execute("""
                SELECT
                    route_geometries->>%s as continuous_geom
                FROM stiflyt.links_with_routes
                WHERE %s = ANY(rutenummer_list)
                LIMIT 1
            """, (test_rutenummer, test_rutenummer))
            result = cur.fetchone()

            if not result or not result['continuous_geom']:
                pytest.skip(f"No continuous geometry computed for route {test_rutenummer} (build-links may need to run)")

            continuous_geom = result['continuous_geom']

            # Compare with simple union of link geometries (should be same or better)
            cur.execute("""
                WITH route_links AS (
                    SELECT DISTINCT l.geom
                    FROM stiflyt.links l
                    JOIN stiflyt.link_ruteinfo lri ON lri.link_id = l.link_id
                    WHERE lri.rutenummer = %s
                )
                SELECT
                    ST_Union(geom) as simple_union_geom,
                    ST_LineMerge(ST_Union(geom)) as merged_union_geom
                FROM route_links
            """, (test_rutenummer,))
            union_result = cur.fetchone()

            if union_result and union_result['simple_union_geom']:
                # Verify continuous geometry is valid
                cur.execute("""
                    SELECT
                        ST_IsValid(%s::geometry) as is_valid,
                        ST_GeometryType(%s::geometry) as geom_type,
                        ST_NumGeometries(%s::geometry) as num_parts,
                        ST_Length(%s::geometry) as length
                """, (continuous_geom, continuous_geom, continuous_geom, continuous_geom))
                continuous_info = cur.fetchone()

                assert continuous_info['is_valid'], f"Continuous geometry for {test_rutenummer} should be valid"
                assert continuous_info['length'] > 0, f"Continuous geometry for {test_rutenummer} should have positive length"

                # The continuous geometry should ideally be a LINESTRING (single part) or have fewer parts than simple union
                # (indicating that links were properly connected)
                cur.execute("""
                    SELECT
                        ST_NumGeometries(%s::geometry) as union_parts
                """, (union_result['simple_union_geom'],))
                union_parts = cur.fetchone()['union_parts']

                # Continuous geometry should have same or fewer parts (better connected)
                assert continuous_info['num_parts'] <= union_parts, \
                    f"Continuous geometry should have same or fewer parts than simple union " \
                    f"({continuous_info['num_parts']} vs {union_parts})"
    finally:
        conn.close()


@pytest.mark.integration
def test_multilinestring_reason():
    """Test that multilinestring_reason column exists and provides meaningful reasons."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Find the dynamic schema
            cur.execute("""
                SELECT nspname
                FROM pg_namespace
                WHERE nspname LIKE 'turogfriluftsruter_%'
                ORDER BY nspname DESC
                LIMIT 1
            """)
            schema_result = cur.fetchone()
            if not schema_result:
                pytest.skip("No turrutebasen schema found")

            schema_name = schema_result['nspname']

            # First, check if stable view stiflyt.route_continuous_geometries exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.views
                    WHERE table_schema = 'stiflyt'
                      AND table_name = 'route_continuous_geometries'
                ) as view_exists
            """)
            result = cur.fetchone()
            view_exists = result['view_exists'] if result else False

            assert view_exists, "stiflyt.route_continuous_geometries view should exist (migration 005 should have created it)"

            # Also check if route_continuous_geometries table exists in dynamic schema
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = %s
                      AND table_name = 'route_continuous_geometries'
                ) as table_exists
            """, (schema_name,))
            result = cur.fetchone()
            table_exists = result['table_exists'] if result else False

            assert table_exists, "route_continuous_geometries table should exist in dynamic schema (build-links should have run)"

            # Check if multilinestring_reason column exists
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = 'route_continuous_geometries'
                  AND column_name = 'multilinestring_reason'
            """, (schema_name,))
            column = cur.fetchone()

            assert column is not None, "multilinestring_reason column should exist in route_continuous_geometries (build-links should have run with latest version)"
            assert column['data_type'] == 'text', "multilinestring_reason should be TEXT type"

            # Get some routes with their reasons - prioritize routes with non-single_linestring reasons
            # Use the stable view stiflyt.route_continuous_geometries instead of direct table access
            cur.execute("""
                (
                    SELECT
                        rutenummer,
                        continuous_geometry,
                        multilinestring_reason,
                        ST_GeometryType(continuous_geometry) as geom_type,
                        ST_NumGeometries(continuous_geometry) as num_geoms
                    FROM stiflyt.route_continuous_geometries
                    WHERE continuous_geometry IS NOT NULL
                      AND multilinestring_reason != 'single_linestring'
                    LIMIT 5
                )
                UNION ALL
                (
                    SELECT
                        rutenummer,
                        continuous_geometry,
                        multilinestring_reason,
                        ST_GeometryType(continuous_geometry) as geom_type,
                        ST_NumGeometries(continuous_geometry) as num_geoms
                    FROM stiflyt.route_continuous_geometries
                    WHERE continuous_geometry IS NOT NULL
                    LIMIT 5
                )
            """)
            routes = cur.fetchall()

            if not routes:
                pytest.skip("No routes found in route_continuous_geometries (build-links may need to run)")

            # Valid reasons
            valid_reasons = {
                'single_linestring',
                'link_is_multilinestring',
                'loop_or_branch',
                'precision_gap',
                'disconnected_components',
                'traversal_issue'
            }

            # Check that all routes have valid reasons and print examples
            print(f"\n  Found {len(routes)} routes with multilinestring_reason:")
            reason_counts = {}

            for route in routes:
                reason = route['multilinestring_reason']
                num_geoms = route['num_geoms']
                geom_type = route['geom_type']

                assert reason is not None, f"Route {route['rutenummer']} should have a multilinestring_reason"
                assert reason in valid_reasons, \
                    f"Route {route['rutenummer']} has invalid reason: {reason}"

                # Count reasons
                reason_counts[reason] = reason_counts.get(reason, 0) + 1

                # If it's a single LineString, reason should be 'single_linestring'
                if geom_type == 'ST_LineString' or (geom_type == 'ST_MultiLineString' and num_geoms == 1):
                    assert reason == 'single_linestring', \
                        f"Route {route['rutenummer']} is LineString but reason is {reason}, expected 'single_linestring'"

                # If it's MultiLineString with multiple parts, reason should not be 'single_linestring'
                if geom_type == 'ST_MultiLineString' and num_geoms > 1:
                    assert reason != 'single_linestring', \
                        f"Route {route['rutenummer']} is MultiLineString with {num_geoms} parts but reason is 'single_linestring'"

            # Print summary of reasons found
            print(f"  Reason distribution:")
            for reason, count in sorted(reason_counts.items()):
                print(f"    {reason}: {count} route(s)")

            # Print first few examples
            print(f"  Example routes:")
            for route in routes[:5]:
                print(f"    {route['rutenummer']}: {route['multilinestring_reason']} "
                      f"({route['geom_type']}, {route['num_geoms']} part(s))")

            # Check that we have at least one route with a reason
            assert len(routes) > 0, "Should have at least one route to test"

    finally:
        conn.close()

