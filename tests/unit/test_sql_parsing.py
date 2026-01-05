from pathlib import Path
import zipfile

from scripts import load_dataset


def test_sanitize_identifier():
    assert load_dataset.sanitize_identifier("A b-c") == "a_b_c"
    assert load_dataset.sanitize_identifier("ab__CD") == "ab__cd"


def test_extract_schema_prefix_from_sql():
    content = """
    CREATE SCHEMA turogfriluftsruter_abcdef0123456789abcdef0123456789;
    CREATE TABLE turogfriluftsruter_abcdef0123456789abcdef0123456789.routes (id int);
    """
    assert load_dataset.extract_schema_prefix_from_sql(content) == "turogfriluftsruter"


def test_extract_table_names_from_sql(tmp_path: Path):
    sql = """
    CREATE TABLE turogfriluftsruter_abcdef0123456789abcdef0123456789.routes (id int);
    CREATE TABLE public.simple (id int);
    """
    sql_file = tmp_path / "schema.sql"
    sql_file.write_text(sql, encoding="utf-8")

    tables, prefix = load_dataset.extract_table_names_from_sql(sql_file)
    assert prefix == "turogfriluftsruter"
    assert "turogfriluftsruter_abcdef0123456789abcdef0123456789.routes" in tables
    assert "public.simple" in tables


def test_detect_format_from_zip(tmp_path: Path):
    zip_path = tmp_path / "dataset.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("data.sql", "CREATE TABLE test (id int);")
        zf.writestr("readme.txt", "info")

    fmt, files = load_dataset.detect_format_from_zip(zip_path)
    assert fmt == "PostGIS"
    assert "data.sql" in files


def test_role_sql_helpers():
    assert load_dataset.role_preamble_sql().strip() == "SET ROLE stiflyt_owner;"
    assert load_dataset.role_reset_sql().strip() == "RESET ROLE;"
