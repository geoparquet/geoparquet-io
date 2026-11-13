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


def test_exports_do_not_stage_filtered_rows(with_geo, tmp_path, monkeypatch):
    from geoparquet_io.core import format_writers

    materialized_rows = []
    factory = format_writers.get_duckdb_connection

    class MeasuredConnection:
        def __init__(self, con):
            self.con = con

        def __getattr__(self, name):
            return getattr(self.con, name)

        def close(self):
            try:
                materialized_rows.append(
                    self.con.sql(
                        "SELECT coalesce(sum(estimated_size), 0) FROM duckdb_tables() "
                        "WHERE temporary AND NOT internal"
                    ).fetchall()[0][0]
                )
            finally:
                self.con.close()

    monkeypatch.setattr(
        format_writers,
        "get_duckdb_connection",
        lambda *args, **kwargs: MeasuredConnection(factory(*args, **kwargs)),
    )
    for fmt, suffix in (("shapefile", "shp"), ("flatgeobuf", "fgb"), ("geopackage", "gpkg")):
        for filtered in (False, True):
            out = tmp_path / f"out.{suffix}"
            format_writers.write_gdal_format(
                str(with_geo),
                str(out),
                fmt,
                overwrite=True,
                where="id % 2 = 0" if filtered else None,
            )
            assert out.exists()
    assert (tmp_path / "out.dbf").exists()
    assert not [p for p in tmp_path.iterdir() if p.name.startswith(".gpio")]
    assert materialized_rows == [0, 0, 0, 0, 0, 0]


def test_overwrite_removes_a_stale_shapefile_sidecar(with_geo, tmp_path):
    out = tmp_path / "out.shp"
    assert _run("shapefile", with_geo, out).exit_code == 0
    stale = tmp_path / "out.sbn"
    stale.write_bytes(b"old index")
    unrelated = tmp_path / "out.txt"
    unrelated.write_text("keep", encoding="utf-8")
    assert _run("shapefile", with_geo, out, "--overwrite").exit_code == 0
    assert not stale.exists()
    assert unrelated.read_text(encoding="utf-8") == "keep"
    assert out.exists()


def test_interrupted_write_leaves_no_staging_directory(tmp_path):
    from geoparquet_io.core.file_utils import atomic_write

    out = tmp_path / "out.fgb"
    with pytest.raises(KeyboardInterrupt):
        with atomic_write(str(out), suffix=".fgb") as staged:
            Path(staged).write_bytes(b"partial")
            raise KeyboardInterrupt
    assert list(tmp_path.iterdir()) == []


def test_shapefile_zip_contains_every_sidecar(tmp_path):
    from zipfile import ZipFile

    from geoparquet_io.core.format_writers import SHAPEFILE_SIDECARS, create_shapefile_zip

    shp = tmp_path / "roads.shp"
    shp.write_bytes(b"x")
    for ext in SHAPEFILE_SIDECARS:
        (tmp_path / f"roads{ext}").write_bytes(b"x")
    (tmp_path / "roads.txt").write_bytes(b"not part of it")
    archive = create_shapefile_zip(shp)
    with ZipFile(archive) as z:
        assert sorted(z.namelist()) == sorted(
            ["roads.shp", *(f"roads{ext}" for ext in SHAPEFILE_SIDECARS)]
        )


def _reader_snapshot(path):
    """Compare reader-visible declarations and ordered features, not SQLite pages."""
    from pyproj import CRS

    from geoparquet_io.core.duckdb_utils import get_duckdb_connection, quote_identifier, sql_path

    with closing(get_duckdb_connection(load_spatial=True)) as con:
        relation = f"ST_Read({sql_path(path)})"
        schema = con.sql(f"DESCRIBE SELECT * FROM {relation}").fetchall()
        columns = [
            f"ST_AsWKB({quote_identifier(name)})"
            if kind == "GEOMETRY"
            else f"CAST({quote_identifier(name)} AS VARCHAR)"
            if kind == "TIMESTAMP WITH TIME ZONE"
            else quote_identifier(name)
            for name, kind, *_ in schema
        ]
        rows = con.sql(f"SELECT {', '.join(columns)} FROM {relation}").fetchall()
        layers = con.sql(f"SELECT layers FROM ST_Read_Meta({sql_path(path)})").fetchall()
    for (file_layers,) in layers:
        for layer in file_layers:
            for field in layer["geometry_fields"]:
                crs = field["crs"]
                if crs["projjson"]:
                    # GDAL's WKT1 expansion may use a datum or a datum ensemble.
                    crs["projjson"] = CRS.from_json(crs["projjson"])
    sqlite = {}
    if path.suffix == ".gpkg":
        with closing(sqlite3.connect(path)) as db:
            tables = db.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table' ORDER BY name"
            ).fetchall()
            sqlite["schema"] = tables
            for name, _ in tables:
                # Internal rtree nodes encode layout; compare the logical index below.
                if name.endswith(("_node", "_parent", "_rowid")):
                    continue
                fields = [
                    row[1] for row in db.execute(f"PRAGMA table_info({quote_identifier(name)})")
                ]
                selected = [quote_identifier(field) for field in fields if field != "last_change"]
                selected += [f"typeof({field})" for field in selected]
                sqlite[name] = db.execute(
                    f"SELECT {', '.join(selected)} FROM {quote_identifier(name)} ORDER BY 1"
                ).fetchall()
    return schema, rows, layers, sqlite


def test_staged_and_direct_exports_are_reader_equivalent(with_geo, tmp_path, monkeypatch):
    import re

    from geoparquet_io.core import format_writers

    factory = format_writers.get_duckdb_connection
    # Cross vector/row-group boundaries with distinct, ordered attributes.
    with closing(factory(load_spatial=True)) as con:
        table = (
            con.sql(
                "SELECT i AS id, ST_AsWKB(ST_GeomFromText('" + WKT + "')) AS geometry, "
                "CASE WHEN i % 17 = 0 THEN NULL ELSE 'café' || i END AS label, "
                "i % 2 = 0 AS flag, {'a': i} AS nested FROM range(12289) t(i)"
            )
            .arrow()
            .read_all()
        )
    pq.write_table(
        table.replace_schema_metadata(pq.read_schema(with_geo).metadata),
        with_geo,
        row_group_size=2048,
    )

    class StagedConnection:
        def __init__(self, con):
            self.con = con

        def __getattr__(self, name):
            return getattr(self.con, name)

        def execute(self, query, *args, **kwargs):
            match = re.search(r"COPY\s*\((.*)\)\s*TO\s", query, re.S | re.I)
            if match:
                self.con.execute(f"CREATE OR REPLACE TEMP TABLE baseline AS {match[1]}")
                query = query[: match.start(1)] + "SELECT * FROM baseline" + query[match.end(1) :]
            return self.con.execute(query, *args, **kwargs)

    for filtered in (False, True):
        for fmt, suffix in (
            ("geopackage", "gpkg"),
            ("flatgeobuf", "fgb"),
            ("shapefile", "shp"),
            ("csv", "csv"),
            ("geojson", "geojson"),
        ):
            outputs = []
            for staged in (True, False):
                directory = tmp_path / f"{fmt}-{filtered}-{staged}"
                directory.mkdir()
                out = directory / f"features.{suffix}"
                with monkeypatch.context() as patch:
                    if staged:
                        patch.setattr(
                            format_writers,
                            "get_duckdb_connection",
                            lambda *a, **kw: StagedConnection(factory(*a, **kw)),
                        )
                    kwargs = {"where": "id % 2 = 0"} if filtered else {}
                    if fmt == "geojson":
                        # This separate writer has no filter API; feed both routes the same selection.
                        source = with_geo
                        if filtered:
                            source = directory / "selected.parquet"
                            pq.write_table(
                                table.take(list(range(0, len(table), 2))).replace_schema_metadata(
                                    pq.read_schema(with_geo).metadata
                                ),
                                source,
                            )
                        format_writers.write_geojson(str(source), str(out))
                    elif fmt == "csv":
                        format_writers.write_csv(str(with_geo), str(out), **kwargs)
                    else:
                        format_writers.write_gdal_format(str(with_geo), str(out), fmt, **kwargs)
                outputs.append(out.read_bytes() if fmt == "csv" else _reader_snapshot(out))
            assert outputs[0] == outputs[1], (fmt, filtered)


def test_reader_snapshot_compares_projjson_as_a_crs(with_geo, tmp_path, monkeypatch):
    from copy import deepcopy
    from types import SimpleNamespace

    from pyproj import CRS

    from geoparquet_io.core import duckdb_utils

    out = tmp_path / "features.fgb"
    assert _run("flatgeobuf", with_geo, out).exit_code == 0
    expected = _reader_snapshot(out)
    layers = deepcopy(expected[2])
    crs = layers[0][0][0]["geometry_fields"][0]["crs"]
    # GDAL can expand the same WKT1 WGS84 into a datum or a datum ensemble.
    crs["projjson"] = CRS.from_wkt(crs["wkt"]).to_json()
    factory = duckdb_utils.get_duckdb_connection

    class MetadataConnection:
        def __init__(self, con):
            self.con = con

        def __getattr__(self, name):
            return getattr(self.con, name)

        def sql(self, query):
            if "SELECT layers FROM ST_Read_Meta" in query:
                return SimpleNamespace(fetchall=lambda: deepcopy(layers))
            return self.con.sql(query)

    monkeypatch.setattr(
        duckdb_utils,
        "get_duckdb_connection",
        lambda *a, **kw: MetadataConnection(factory(*a, **kw)),
    )
    assert _reader_snapshot(out) == expected
    for different in ("EPSG:3857", "OGC:CRS84"):
        crs["projjson"] = CRS(different).to_json()
        assert _reader_snapshot(out) != expected, different
