"""
Integration tests for static datasets (teig + stedsnavn).

Verifies:
- Latest matrikkel schema exists and teig has geometry column
- GIST index exists on teig geometry column
- public.sted_posisjon exists with geometry column
- GIST index exists on sted_posisjon geometry column
- Basic spatial lookups work for both datasets
"""

import re
import pytest
import psycopg2

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


def _safe_ident(value: str) -> str:
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", value):
        raise ValueError(f"Unsafe identifier: {value}")
    return value


def _get_latest_schema(conn, prefix: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT nspname
            FROM pg_namespace
            WHERE nspname LIKE %s
              AND nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
            ORDER BY nspname DESC
            LIMIT 1
        """, (f"{prefix}%",))
        result = cur.fetchone()
        return result[0] if result else None


def _get_geometry_column(conn, schema: str, table: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT f_geometry_column
            FROM public.geometry_columns
            WHERE f_table_schema = %s
              AND f_table_name = %s
            ORDER BY f_geometry_column
            LIMIT 1
        """, (schema, table))
        result = cur.fetchone()
        return result[0] if result else None


def _gist_index_exists(conn, schema: str, table: str, column: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM pg_index idx
                JOIN pg_class ic ON ic.oid = idx.indexrelid
                JOIN pg_class tc ON tc.oid = idx.indrelid
                JOIN pg_namespace ns ON ns.oid = tc.relnamespace
                JOIN pg_am am ON am.oid = ic.relam
                JOIN pg_attribute att ON att.attrelid = tc.oid
                WHERE ns.nspname = %s
                  AND tc.relname = %s
                  AND am.amname = 'gist'
                  AND att.attname = %s
                  AND att.attnum = ANY(idx.indkey)
            );
        """, (schema, table, column))
        return cur.fetchone()[0]


def _get_common_columns(conn, schema_a: str, table_a: str, schema_b: str, table_b: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """, (schema_a, table_a))
        cols_a = {row[0] for row in cur.fetchall()}

        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """, (schema_b, table_b))
        cols_b = {row[0] for row in cur.fetchall()}

    common = cols_a & cols_b
    common = {c for c in common if c not in {"geom", "geometry", "posisjon", "omrade"}}
    id_like = sorted([c for c in common if "id" in c.lower()])
    return id_like or sorted(common)


def _print_sample(cur, label: str, table: str) -> None:
    cur.execute(f"SELECT * FROM {table} LIMIT 1")
    row = cur.fetchone()
    if row is None:
        print(f"[debug] {label}: no rows")
        return
    columns = [desc[0] for desc in cur.description]
    print(f"[debug] {label}: {dict(zip(columns, row))}")


@pytest.mark.integration
def test_teig_geometry_and_index():
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        schema = _get_latest_schema(conn, "matrikkeleneiendomskartteig_")
        if not schema:
            pytest.skip("No matrikkel schema found")

        geom_col = _get_geometry_column(conn, schema, "teig")
        if not geom_col:
            pytest.skip("No geometry column found for teig")

        assert _gist_index_exists(conn, schema, "teig", geom_col), \
            f"GIST index missing on {schema}.teig({geom_col})"

        schema = _safe_ident(schema)
        geom_col = _safe_ident(geom_col)
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT COUNT(*)
                FROM {schema}.teig
                WHERE {geom_col} IS NOT NULL
            """)
            count = cur.fetchone()[0]
            if count == 0:
                pytest.skip("teig has no geometries to test")

            cur.execute(f"""
                SELECT ST_Intersects({geom_col}, ST_PointOnSurface({geom_col}))
                FROM {schema}.teig
                WHERE {geom_col} IS NOT NULL
                LIMIT 1
            """)
            assert cur.fetchone()[0] is True
    finally:
        conn.close()


@pytest.mark.integration
def test_sted_posisjon_geometry_and_index():
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        geom_col = _get_geometry_column(conn, "public", "sted_posisjon")
        if not geom_col:
            pytest.skip("No geometry column found for public.sted_posisjon")

        assert _gist_index_exists(conn, "public", "sted_posisjon", geom_col), \
            f"GIST index missing on public.sted_posisjon({geom_col})"

        geom_col = _safe_ident(geom_col)
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT COUNT(*)
                FROM public.sted_posisjon
                WHERE {geom_col} IS NOT NULL
            """)
            count = cur.fetchone()[0]
            if count == 0:
                pytest.skip("sted_posisjon has no geometries to test")

            cur.execute(f"""
                SELECT ST_DWithin({geom_col}, {geom_col}, 0)
                FROM public.sted_posisjon
                WHERE {geom_col} IS NOT NULL
                LIMIT 1
            """)
            assert cur.fetchone()[0] is True
    finally:
        conn.close()


@pytest.mark.integration
def test_stedsnavn_tables_exist():
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name IN ('stedsnavn', 'skrivemate', 'sted_posisjon')
            """)
            found = {row[0] for row in cur.fetchall()}
            missing = {'stedsnavn', 'skrivemate', 'sted_posisjon'} - found
            assert not missing, f"Missing tables in public schema: {missing}"
    finally:
        conn.close()


@pytest.mark.integration
def test_stedsnavn_lookup_via_sted_posisjon():
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM public.stedsnavn")
            if cur.fetchone()[0] == 0:
                pytest.skip("stedsnavn has no rows")

            cur.execute("SELECT COUNT(*) FROM public.sted_posisjon")
            if cur.fetchone()[0] == 0:
                pytest.skip("sted_posisjon has no rows")

            _print_sample(cur, "stedsnavn sample", "public.stedsnavn")
            _print_sample(cur, "sted_posisjon sample", "public.sted_posisjon")

        candidates = _get_common_columns(conn, "public", "stedsnavn", "public", "sted_posisjon")
        if not candidates:
            pytest.skip("No common columns found between stedsnavn and sted_posisjon")

        matched = False
        for column in candidates:
            column = _safe_ident(column)
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT COUNT(*)
                    FROM public.stedsnavn s
                    JOIN public.sted_posisjon p ON s.{column} = p.{column}
                    WHERE s.{column} IS NOT NULL
                """)
                if cur.fetchone()[0] > 0:
                    matched = True
                    break

        assert matched, f"No joinable rows found on columns: {candidates}"
    finally:
        conn.close()
