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


@pytest.mark.integration
def test_grant_schema_privileges_for_prefix():
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    if not load_dataset.check_owner_membership(db_params):
        pytest.skip("missing stiflyt_owner membership")

    schema_prefix = "syntheticprefix"
    schema_one = f"{schema_prefix}_11111111111111111111111111111111"
    schema_two = f"{schema_prefix}_22222222222222222222222222222222"

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SET ROLE stiflyt_owner;")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_one};")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_two};")
            cur.execute(f"CREATE TABLE {schema_one}.t1 (id int);")
            cur.execute(f"CREATE TABLE {schema_two}.t2 (id int);")
            cur.execute("RESET ROLE;")

        assert load_dataset.grant_privileges_for_schema_prefix(db_params, schema_prefix)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT has_table_privilege('stiflyt_reader', %s, 'SELECT');",
                (f"{schema_one}.t1",)
            )
            assert cur.fetchone()[0]
            cur.execute(
                "SELECT has_table_privilege('stiflyt_reader', %s, 'SELECT');",
                (f"{schema_two}.t2",)
            )
            assert cur.fetchone()[0]
    finally:
        with conn.cursor() as cur:
            cur.execute("SET ROLE stiflyt_owner;")
            cur.execute(f"DROP SCHEMA IF EXISTS {schema_one} CASCADE;")
            cur.execute(f"DROP SCHEMA IF EXISTS {schema_two} CASCADE;")
            cur.execute("RESET ROLE;")
        conn.close()
