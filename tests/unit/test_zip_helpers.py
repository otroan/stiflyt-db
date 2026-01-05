from pathlib import Path
import zipfile

from scripts import load_dataset


def test_extract_table_names_from_zip_sql(tmp_path: Path):
    sql = """
    CREATE SCHEMA turogfriluftsruter_abcdef0123456789abcdef0123456789;
    CREATE TABLE turogfriluftsruter_abcdef0123456789abcdef0123456789.routes (id int);
    """
    zip_path = tmp_path / "dataset.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("data.sql", sql)

    tables, prefix = load_dataset.extract_table_names_from_zip_sql(zip_path, "data.sql")
    assert prefix == "turogfriluftsruter"
    assert "turogfriluftsruter_abcdef0123456789abcdef0123456789.routes" in tables


def test_detect_format_from_zip_gml(tmp_path: Path):
    zip_path = tmp_path / "gml.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("data.gml", "<gml></gml>")
    fmt, files = load_dataset.detect_format_from_zip(zip_path)
    assert fmt == "GML"
    assert "data.gml" in files


def test_detect_format_from_zip_fgdb(tmp_path: Path):
    zip_path = tmp_path / "fgdb.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("data.gdb/a.gdbtable", "x")
    fmt, files = load_dataset.detect_format_from_zip(zip_path)
    assert fmt == "FGDB"
    assert "data.gdb/a.gdbtable" in files
