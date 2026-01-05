import zipfile
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


@pytest.mark.pipeline
def test_synthetic_postgis_pipeline(tmp_path):
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    schema = "turogfriluftsruter_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    sql_content = f"""
    CREATE SCHEMA {schema};
    CREATE TABLE {schema}.routes (id integer primary key, name text);
    INSERT INTO {schema}.routes (id, name) VALUES (1, 'synthetic');
    """

    zip_path = tmp_path / "synthetic.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("dataset.sql", sql_content)

    try:
        ok = load_dataset.load_dataset(
            zip_path,
            db_params["database"],
            drop_tables=True,
            stream=True
        )
        assert ok

        conn = psycopg2.connect(**_connection_kwargs(db_params))
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM information_schema.tables WHERE table_schema = %s AND table_name = 'routes'",
                    (schema,)
                )
                assert cur.fetchone()[0] == 1
                cur.execute(f"SELECT count(*) FROM {schema}.routes")
                assert cur.fetchone()[0] == 1
        finally:
            conn.close()
    finally:
        try:
            conn = psycopg2.connect(**_connection_kwargs(db_params))
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            conn.close()
        except Exception:
            pass


@pytest.mark.pipeline
def test_synthetic_postgis_drop_by_prefix(tmp_path):
    db_params = load_dataset.get_db_connection_params()
    if not db_params.get("database"):
        pytest.skip("PGDATABASE not set")

    schema_prefix = "syntheticdrop"
    schema_one = f"{schema_prefix}_11111111111111111111111111111111"
    schema_two = f"{schema_prefix}_22222222222222222222222222222222"

    sql_one = f"""
    CREATE SCHEMA {schema_one};
    CREATE TABLE {schema_one}.routes (id integer primary key);
    INSERT INTO {schema_one}.routes (id) VALUES (1);
    """
    sql_two = f"""
    CREATE SCHEMA {schema_two};
    CREATE TABLE {schema_two}.routes (id integer primary key);
    INSERT INTO {schema_two}.routes (id) VALUES (2);
    """

    zip_one = tmp_path / "first.zip"
    zip_two = tmp_path / "second.zip"
    with zipfile.ZipFile(zip_one, "w") as zf:
        zf.writestr("data.sql", sql_one)
    with zipfile.ZipFile(zip_two, "w") as zf:
        zf.writestr("data.sql", sql_two)

    conn = None
    try:
        assert load_dataset.load_dataset(zip_one, db_params["database"], drop_tables=True, stream=True)
        assert load_dataset.load_dataset(zip_two, db_params["database"], drop_tables=True, stream=True)

        conn = psycopg2.connect(**_connection_kwargs(db_params))
        with conn.cursor() as cur:
            cur.execute(
                "SELECT nspname FROM pg_namespace WHERE nspname IN (%s, %s) ORDER BY nspname",
                (schema_one, schema_two)
            )
            schemas = [row[0] for row in cur.fetchall()]
            assert schemas == [schema_two]
    finally:
        if conn is None:
            conn = psycopg2.connect(**_connection_kwargs(db_params))
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET ROLE stiflyt_owner;")
            cur.execute(f"DROP SCHEMA IF EXISTS {schema_one} CASCADE;")
            cur.execute(f"DROP SCHEMA IF EXISTS {schema_two} CASCADE;")
            cur.execute("RESET ROLE;")
        conn.close()
