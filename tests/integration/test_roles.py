import os
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
def test_owner_membership():
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")
    assert load_dataset.check_owner_membership(db_params)


@pytest.mark.integration
def test_privilege_functions_exist():
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    conn = psycopg2.connect(**_connection_kwargs(db_params))
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT proname
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                WHERE n.nspname = 'public'
                  AND p.proname IN ('grant_schema_privileges', 'grant_schema_privileges_for_prefix')
                """
            )
            found = {row[0] for row in cur.fetchall()}
            assert "grant_schema_privileges" in found
            assert "grant_schema_privileges_for_prefix" in found
    finally:
        conn.close()
