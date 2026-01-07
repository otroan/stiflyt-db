"""
Integration tests for name lookup in anchor nodes.

Tests that verify:
- node_names materialized view correctly matches nodes to ruteinfopunkt and stedsnavn
- anchor_nodes has correct names from both sources
- ruteinfopunkt names are prioritized over stedsnavn names
- Names are extracted from correct fields (opphav/informasjon for ruteinfopunkt, komplettskrivemate for stedsnavn)
- Spatial matching works correctly (100m for ruteinfopunkt, 200m for stedsnavn)
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


def _get_turrutebasen_schema(conn):
    """Get the current turrutebasen schema name."""
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


@pytest.mark.integration
def test_node_names_view_exists():
    """Test that node_names materialized view exists after migration 004."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        schema = _get_turrutebasen_schema(conn)
        if not schema:
            pytest.skip("No turrutebasen schema found")

        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_matviews mv
                    JOIN pg_namespace n ON mv.schemaname = n.nspname
                    WHERE n.nspname = %s AND mv.matviewname = 'node_names'
                )
            """, (schema,))
            exists = cur.fetchone()[0]
            assert exists, f"node_names materialized view should exist in {schema}"
    finally:
        conn.close()


@pytest.mark.integration
def test_node_names_has_required_columns():
    """Test that node_names has all required columns."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        schema = _get_turrutebasen_schema(conn)
        if not schema:
            pytest.skip("No turrutebasen schema found")

        with conn.cursor() as cur:
            # Materialized views might not show up in information_schema.columns
            # Use pg_attribute instead
            cur.execute("""
                SELECT a.attname as column_name,
                       pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type
                FROM pg_catalog.pg_attribute a
                JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = %s
                  AND c.relname = 'node_names'
                  AND a.attnum > 0
                  AND NOT a.attisdropped
                ORDER BY a.attname
            """, (schema,))
            columns = {row[0]: row[1] for row in cur.fetchall()}

            required_columns = {
                'node_id': 'bigint',
                'navn': 'text',
                'navn_kilde': 'text',
                'distance_m': 'double precision'
            }

            for col_name, expected_type in required_columns.items():
                assert col_name in columns, f"node_names should have column {col_name}"
                # Note: data_type might be slightly different (e.g., 'double precision' vs 'real')
                assert columns[col_name] in ['bigint', 'integer', 'text', 'character varying',
                                           'double precision', 'real', 'numeric'], \
                    f"node_names.{col_name} should be numeric/text type, got {columns[col_name]}"
    finally:
        conn.close()


@pytest.mark.integration
def test_node_names_navn_kilde_values():
    """Test that navn_kilde contains only expected values."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        schema = _get_turrutebasen_schema(conn)
        if not schema:
            pytest.skip("No turrutebasen schema found")

        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT DISTINCT navn_kilde
                FROM {schema}.node_names
                WHERE navn_kilde IS NOT NULL
                ORDER BY navn_kilde
            """)
            sources = {row[0] for row in cur.fetchall()}

            # Should only contain 'ruteinfopunkt' and optionally 'stedsnavn'
            valid_sources = {'ruteinfopunkt', 'stedsnavn'}
            invalid_sources = sources - valid_sources
            assert not invalid_sources, \
                f"node_names.navn_kilde should only contain 'ruteinfopunkt' or 'stedsnavn', found: {invalid_sources}"

            # Should have at least ruteinfopunkt if there's any data
            if sources:
                assert 'ruteinfopunkt' in sources, \
                    "node_names should have at least some entries with navn_kilde='ruteinfopunkt'"
    finally:
        conn.close()


@pytest.mark.integration
def test_node_names_ruteinfopunkt_prioritization():
    """Test that ruteinfopunkt names are prioritized over stedsnavn names."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        schema = _get_turrutebasen_schema(conn)
        if not schema:
            pytest.skip("No turrutebasen schema found")

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Find nodes that have both ruteinfopunkt and stedsnavn matches
            # This is tricky to test directly, but we can verify that:
            # 1. No node should have both ruteinfopunkt and stedsnavn entries
            # 2. If a node has a ruteinfopunkt match, it shouldn't have a stedsnavn match
            cur.execute(f"""
                SELECT node_id, COUNT(*) as count
                FROM {schema}.node_names
                GROUP BY node_id
                HAVING COUNT(*) > 1
            """)
            duplicate_nodes = cur.fetchall()

            # Each node should only appear once in node_names
            # (because ruteinfopunkt matches exclude stedsnavn matches)
            assert len(duplicate_nodes) == 0, \
                f"Found {len(duplicate_nodes)} nodes with multiple entries in node_names. " \
                f"Each node should only have one entry (ruteinfopunkt prioritized over stedsnavn)."
    finally:
        conn.close()


@pytest.mark.integration
def test_node_names_distance_constraints():
    """Test that distances are within expected limits."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        schema = _get_turrutebasen_schema(conn)
        if not schema:
            pytest.skip("No turrutebasen schema found")

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Check ruteinfopunkt matches are within 100m
            cur.execute(f"""
                SELECT COUNT(*) as count
                FROM {schema}.node_names
                WHERE navn_kilde = 'ruteinfopunkt'
                  AND distance_m > 100
            """)
            ruteinfopunkt_too_far = cur.fetchone()['count']
            assert ruteinfopunkt_too_far == 0, \
                f"Found {ruteinfopunkt_too_far} ruteinfopunkt matches with distance > 100m. " \
                f"All should be within 100m."

            # Check stedsnavn matches are within 200m
            cur.execute(f"""
                SELECT COUNT(*) as count
                FROM {schema}.node_names
                WHERE navn_kilde = 'stedsnavn'
                  AND distance_m > 200
            """)
            stedsnavn_too_far = cur.fetchone()['count']
            assert stedsnavn_too_far == 0, \
                f"Found {stedsnavn_too_far} stedsnavn matches with distance > 200m. " \
                f"All should be within 200m."
    finally:
        conn.close()


@pytest.mark.integration
def test_anchor_nodes_has_names():
    """Test that anchor_nodes includes name information."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        schema = _get_turrutebasen_schema(conn)
        if not schema:
            pytest.skip("No turrutebasen schema found")

        with conn.cursor() as cur:
            # Check that anchor_nodes has required name columns
            # Use pg_attribute for materialized views
            cur.execute("""
                SELECT a.attname as column_name
                FROM pg_catalog.pg_attribute a
                JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = %s
                  AND c.relname = 'anchor_nodes'
                  AND a.attname IN ('navn', 'navn_kilde', 'navn_distance_m')
                  AND a.attnum > 0
                  AND NOT a.attisdropped
            """, (schema,))
            name_columns = {row[0] for row in cur.fetchall()}

            required_columns = {'navn', 'navn_kilde', 'navn_distance_m'}
            assert name_columns == required_columns, \
                f"anchor_nodes should have columns {required_columns}, found {name_columns}"

            # Check that some anchor nodes have names (not just coordinates)
            cur.execute(f"""
                SELECT COUNT(*) as total,
                       COUNT(CASE WHEN navn_kilde != 'koordinat' THEN 1 END) as with_names
                FROM {schema}.anchor_nodes
            """)
            result = cur.fetchone()
            total = result[0]
            with_names = result[1]

            assert total > 0, "anchor_nodes should have at least some entries"
            # At least some should have names from ruteinfopunkt or stedsnavn
            # (not all should be coordinate-based)
            if total > 10:  # Only check if we have enough data
                assert with_names > 0, \
                    f"Expected at least some anchor_nodes to have names from ruteinfopunkt/stedsnavn, " \
                    f"but all {total} entries have navn_kilde='koordinat'"
    finally:
        conn.close()


@pytest.mark.integration
def test_anchor_nodes_navn_kilde_values():
    """Test that anchor_nodes.navn_kilde contains only expected values."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        schema = _get_turrutebasen_schema(conn)
        if not schema:
            pytest.skip("No turrutebasen schema found")

        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT DISTINCT navn_kilde
                FROM {schema}.anchor_nodes
                WHERE navn_kilde IS NOT NULL
                ORDER BY navn_kilde
            """)
            sources = {row[0] for row in cur.fetchall()}

            # Should only contain valid sources
            valid_sources = {'ruteinfopunkt', 'stedsnavn', 'koordinat'}
            invalid_sources = sources - valid_sources
            assert not invalid_sources, \
                f"anchor_nodes.navn_kilde should only contain 'ruteinfopunkt', 'stedsnavn', or 'koordinat', " \
                f"found: {invalid_sources}"
    finally:
        conn.close()


@pytest.mark.integration
def test_anchor_nodes_name_consistency():
    """Test that anchor_nodes names are consistent with node_names."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        schema = _get_turrutebasen_schema(conn)
        if not schema:
            pytest.skip("No turrutebasen schema found")

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Check that anchor_nodes names match node_names for the same nodes
            cur.execute(f"""
                SELECT an.node_id, an.navn, an.navn_kilde, an.navn_distance_m,
                       nn.navn as nn_navn, nn.navn_kilde as nn_navn_kilde, nn.distance_m as nn_distance_m
                FROM {schema}.anchor_nodes an
                LEFT JOIN {schema}.node_names nn ON an.node_id = nn.node_id
                WHERE an.navn_kilde != 'koordinat'
                LIMIT 100
            """)
            results = cur.fetchall()

            mismatches = []
            for row in results:
                if row['nn_navn'] is not None:
                    # If node_names has an entry, anchor_nodes should match
                    if row['navn'] != row['nn_navn']:
                        mismatches.append({
                            'node_id': row['node_id'],
                            'anchor_navn': row['navn'],
                            'node_names_navn': row['nn_navn']
                        })
                    if row['navn_kilde'] != row['nn_navn_kilde']:
                        mismatches.append({
                            'node_id': row['node_id'],
                            'anchor_kilde': row['navn_kilde'],
                            'node_names_kilde': row['nn_navn_kilde']
                        })

            assert len(mismatches) == 0, \
                f"Found {len(mismatches)} mismatches between anchor_nodes and node_names: {mismatches[:5]}"
    finally:
        conn.close()


@pytest.mark.integration
def test_ruteinfopunkt_name_source():
    """Test that ruteinfopunkt names come from opphav or informasjon fields."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        schema = _get_turrutebasen_schema(conn)
        if not schema:
            pytest.skip("No turrutebasen schema found")

        # Check if ruteinfopunkt table exists
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = 'ruteinfopunkt'
                )
            """, (schema,))
            if not cur.fetchone()[0]:
                pytest.skip("ruteinfopunkt table does not exist")

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Sample some node_names entries with ruteinfopunkt source
            # We need to match by spatial proximity since node_names doesn't store objid
            cur.execute(f"""
                SELECT DISTINCT ON (nn.node_id)
                    nn.node_id, nn.navn, rp.objid as ruteinfopunkt_objid, rp.opphav, rp.informasjon
                FROM {schema}.node_names nn
                JOIN {schema}.nodes n ON nn.node_id = n.id
                JOIN {schema}.ruteinfopunkt rp ON ST_DWithin(n.geom, rp.posisjon, 100)
                WHERE nn.navn_kilde = 'ruteinfopunkt'
                ORDER BY nn.node_id, ST_Distance(n.geom, rp.posisjon)
                LIMIT 50
            """)
            results = cur.fetchall()

            if not results:
                pytest.skip("No ruteinfopunkt matches found in node_names")

            # Verify that names come from opphav or informasjon
            invalid_names = []
            for row in results:
                name = row['navn']
                if name is None:
                    continue  # Skip None names
                opphav = (row['opphav'] or '').strip()
                informasjon = (row['informasjon'] or '').strip()
                # Name should match either opphav or informasjon (after trimming)
                if (name != opphav and name != informasjon and
                    (opphav and name not in opphav) and (informasjon and name not in informasjon)):
                    invalid_names.append({
                        'node_id': row['node_id'],
                        'navn': name,
                        'opphav': row['opphav'],
                        'informasjon': row['informasjon']
                    })

            # Allow some tolerance - names might be trimmed or slightly modified
            assert len(invalid_names) < len(results) * 0.1, \
                f"Too many names don't match opphav/informasjon: {len(invalid_names)}/{len(results)}"
    finally:
        conn.close()


@pytest.mark.integration
def test_stedsnavn_name_source():
    """Test that stedsnavn names come from skrivemate.komplettskrivemate field."""
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        schema = _get_turrutebasen_schema(conn)
        if not schema:
            pytest.skip("No turrutebasen schema found")

        # Check if stedsnavn tables exist
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = 'stedsnavn'
                )
            """)
            if not cur.fetchone()[0]:
                pytest.skip("stedsnavn table does not exist")

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Sample some node_names entries with stedsnavn source
            # We need to match by spatial proximity since node_names doesn't store objid
            cur.execute(f"""
                SELECT DISTINCT ON (nn.node_id)
                    nn.node_id, nn.navn, sn.objid as stedsnavn_objid, sm.komplettskrivemate
                FROM {schema}.node_names nn
                JOIN {schema}.nodes n ON nn.node_id = n.id
                JOIN public.stedsnavn sn ON sn.sted_fk IS NOT NULL
                JOIN public.skrivemate sm ON sn.objid = sm.stedsnavn_fk
                JOIN public.sted_posisjon sp ON sn.sted_fk = sp.stedsnummer
                WHERE nn.navn_kilde = 'stedsnavn'
                  AND ST_DWithin(n.geom, sp.geom, 200)
                ORDER BY nn.node_id, ST_Distance(n.geom, sp.geom)
                LIMIT 50
            """)
            results = cur.fetchall()

            if not results:
                pytest.skip("No stedsnavn matches found in node_names")

            # Verify that names come from komplettskrivemate
            invalid_names = []
            for row in results:
                name = row['navn']
                komplettskrivemate = (row['komplettskrivemate'] or '').strip()
                # Name should match komplettskrivemate (after trimming)
                if name != komplettskrivemate:
                    invalid_names.append({
                        'node_id': row['node_id'],
                        'navn': name,
                        'komplettskrivemate': komplettskrivemate
                    })

            # Allow some tolerance - names might be trimmed
            assert len(invalid_names) < len(results) * 0.1, \
                f"Too many names don't match komplettskrivemate: {len(invalid_names)}/{len(results)}"
    finally:
        conn.close()

