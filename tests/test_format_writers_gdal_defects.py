"""GDAL writer defects fixed once in ``write_gdal_format`` (#1161, #1320, #1333).

Each test drives the real CLI. Measured on the base (DuckDB 1.5.6):

* #1161: a WKB file whose geometry DuckDB reads as BLOB (no ``geo`` key) died in
  ``convert flatgeobuf`` with "ICreateFeature: NULL geometry not supported with
  spatial index", and left the partial ``.fgb`` behind.
* #1320: ``--layer-name mylayer`` produced a layer named after the file stem.
* #1333: STRUCT, LIST and MAP columns vanished from GeoPackage/FlatGeobuf output.
"""

import json
import sqlite3
from contextlib import closing
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.duckdb_utils import fetch_one

WKT = "MULTIPOLYGON(((0 0,1 0,1 1,0 0)))"


def _table(with_geo: bool):
    con = duckdb.connect()
    con.execute("LOAD spatial")
    table = (
        con.sql(
            f"""SELECT i AS id, ST_AsWKB(ST_GeomFromText('{WKT}')) AS geometry,
                   {{'a': i, 'b': 'x'}} AS s, [i, i + 1] AS l, MAP {{'k': 'v'}} AS m
            FROM range(3) t(i)"""
        )
        .arrow()
        .read_all()
    )
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["MultiPolygon"]}},
    }
    return table.replace_schema_metadata({b"geo": json.dumps(geo).encode()} if with_geo else {})


@pytest.fixture
def with_geo(tmp_path: Path) -> Path:
    path = tmp_path / "with_geo.parquet"
    pq.write_table(_table(True), path)
    return path


@pytest.fixture
def without_geo(tmp_path: Path) -> Path:
    path = tmp_path / "without_geo.parquet"
    pq.write_table(_table(False), path)
    return path


def _run(*args):
    return CliRunner().invoke(cli, ["convert", *map(str, args)])


def _gpkg(path: Path):
    with closing(sqlite3.connect(path)) as db:
        layers = [r[0] for r in db.execute("SELECT table_name FROM gpkg_contents")]
        rows = db.execute(f'SELECT * FROM "{layers[0]}" ORDER BY id').fetchall()
        cols = [r[1] for r in db.execute(f'PRAGMA table_info("{layers[0]}")')]
    return layers, cols, rows


def test_issue_1161_wkb_without_geo_key_converts_to_flatgeobuf(without_geo, tmp_path):
    out = tmp_path / "out.fgb"
    result = _run("flatgeobuf", without_geo, out)
    assert result.exit_code == 0, result.output
    with closing(duckdb.connect()) as con:
        con.execute("LOAD spatial")
        n = fetch_one(con.execute(f"SELECT count(*) FROM ST_Read('{out}') WHERE geom IS NOT NULL"))
    assert n == (3,)


def test_issue_1161_failed_conversion_leaves_no_partial_file(tmp_path):
    bad = tmp_path / "bad.parquet"
    con = duckdb.connect()
    con.execute("LOAD spatial")
    table = con.sql("SELECT i AS id, 'not wkb'::BLOB AS geometry FROM range(2) t(i)").arrow()
    pq.write_table(table.read_all(), bad)
    out = tmp_path / "out.fgb"
    result = _run("flatgeobuf", bad, out)
    assert result.exit_code != 0
    assert not out.exists()


def test_issue_1320_layer_name_is_honoured(with_geo, tmp_path):
    out = tmp_path / "stem.gpkg"
    result = _run("geopackage", with_geo, out, "--layer-name", "mylayer")
    assert result.exit_code == 0, result.output
    assert _gpkg(out)[0] == ["mylayer"]


def test_issue_1320_layer_name_with_quote(with_geo, tmp_path):
    out = tmp_path / "stem.gpkg"
    result = _run("geopackage", with_geo, out, "--layer-name", "o'brien")
    assert result.exit_code == 0, result.output
    assert _gpkg(out)[0] == ["o'brien"]


@pytest.mark.parametrize("fmt,suffix", [("geopackage", "gpkg"), ("flatgeobuf", "fgb")])
def test_issue_1333_nested_columns_are_json_encoded(with_geo, tmp_path, fmt, suffix):
    out = tmp_path / f"out.{suffix}"
    result = _run(fmt, with_geo, out)
    assert result.exit_code == 0, result.output
    with closing(duckdb.connect()) as con:
        con.execute("LOAD spatial")
        row = fetch_one(con.execute(f"SELECT s, l, m FROM ST_Read('{out}') ORDER BY id LIMIT 1"))
    assert json.loads(row[0]) == {"a": 0, "b": "x"}
    assert json.loads(row[1]) == [0, 1]
    assert json.loads(row[2]) == {"k": "v"}


def test_issue_1320_no_flag_keeps_file_stem(with_geo, tmp_path):
    out = tmp_path / "stem.gpkg"
    result = _run("geopackage", with_geo, out)
    assert result.exit_code == 0, result.output
    assert _gpkg(out)[0] == ["stem"]


@pytest.mark.parametrize("fmt,suffix", [("geopackage", "gpkg"), ("flatgeobuf", "fgb")])
def test_issue_1161_native_geoarrow_geometry_is_a_clear_error(tmp_path, fmt, suffix):
    source = Path(__file__).parent / "data" / "data-point-encoding_native.parquet"
    out = tmp_path / f"out.{suffix}"
    result = _run(fmt, source, out)
    assert result.exit_code != 0
    assert "GeoArrow-native" in result.output
    assert "Re-encode it as WKB" in result.output
    assert "NULL geometry" not in result.output
    assert not out.exists()


@pytest.mark.parametrize("overwrite", [False, True])
def test_failed_conversion_never_touches_a_pre_existing_output(tmp_path, overwrite):
    bad = tmp_path / "bad.parquet"
    con = duckdb.connect()
    con.execute("LOAD spatial")
    table = con.sql("SELECT i AS id, 'not wkb'::BLOB AS geometry FROM range(2) t(i)").arrow()
    pq.write_table(table.read_all(), bad)
    out = tmp_path / "out.fgb"
    out.write_bytes(b"precious")
    result = _run("flatgeobuf", bad, out, *(["--overwrite"] if overwrite else []))
    assert result.exit_code != 0
    assert out.read_bytes() == b"precious"
    assert [p.name for p in tmp_path.iterdir() if p.name.startswith(("tmp", ".gpio"))] == []


def test_shapefile_converts_through_staging_and_leaves_no_stage_dir(with_geo, tmp_path):
    out = tmp_path / "out.shp"
    result = _run("shapefile", with_geo, out)
    assert result.exit_code == 0, result.output
    assert out.exists() and (tmp_path / "out.dbf").exists()
    assert not [p for p in tmp_path.iterdir() if p.name.startswith(".gpio")]


def test_overwrite_removes_a_stale_shapefile_sidecar(with_geo, tmp_path):
    out = tmp_path / "out.shp"
    assert _run("shapefile", with_geo, out).exit_code == 0
    stale = tmp_path / "out.sbn"
    stale.write_bytes(b"old index")
    unrelated = tmp_path / "out.txt"
    unrelated.write_text("keep")
    assert _run("shapefile", with_geo, out, "--overwrite").exit_code == 0
    assert not stale.exists()
    assert unrelated.read_text() == "keep"
    assert out.exists()


def test_interrupted_write_leaves_no_staging_directory(tmp_path):
    from geoparquet_io.core.write_strategies.base import atomic_write

    out = tmp_path / "out.fgb"
    with pytest.raises(KeyboardInterrupt):
        with atomic_write(str(out), suffix=".fgb") as staged:
            Path(staged).write_bytes(b"partial")
            raise KeyboardInterrupt
    assert list(tmp_path.iterdir()) == []
