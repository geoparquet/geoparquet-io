"""A parquet-geo-only output types every secondary with its own CRS (#1277 review).

parquet-geo-only writes no ``geo`` block, so the Parquet logical type is the only
place a column's CRS lives. A type with no CRS reads as OGC:CRS84. A secondary
whose CRS is unknown must therefore be typed ``srid:0``, and a named one must
carry its CRS, on every write path, or ``check spec`` reads projected metres as
lon/lat and fails the file gpio just wrote.
"""

from __future__ import annotations

import json
import struct

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli
from geoparquet_io.core.crs_utils import PARQUET_UNKNOWN_CRS, reset_null_crs_warnings

WGS84 = {"id": {"authority": "EPSG", "code": 4326}}
ALBERS = {"id": {"authority": "EPSG", "code": 5070}}
LONLAT = [(10.0 + i, 20.0 + i) for i in range(5)]
# Albers (EPSG:5070) sized metres. Read as lon/lat they are far out of range.
PROJECTED = [(1500000.0 + i, 2000000.0 + i) for i in range(5)]

SECONDARY_CRS = {"unknown": None, "epsg5070": ALBERS}
VERSION = ["--geoparquet-version", "parquet-geo-only"]
COMMANDS = {
    "convert": lambda src, out: ["convert", "geoparquet", src, out, *VERSION],
    "sort-hilbert": lambda src, out: ["sort", "hilbert", src, out, *VERSION],
    "add-bbox": lambda src, out: ["add", "bbox", src, out, *VERSION],
    "extract": lambda src, out: ["extract", "geoparquet", src, out, *VERSION],
}
STRATEGIES = ["duckdb-kv", "in-memory", "streaming", "disk-rewrite"]


@pytest.fixture(autouse=True)
def _fresh_warnings():
    reset_null_crs_warnings()
    yield
    reset_null_crs_warnings()


def _wkb(points) -> pa.Array:
    return pa.array([struct.pack("<BIdd", 1, 1, x, y) for x, y in points], pa.binary())


def _write_two_geoms(path, secondary_crs) -> str:
    """A 1.1 file, primary EPSG:4326, ``geom2`` projected with ``secondary_crs``."""
    table = pa.table({"id": list(range(5)), "geometry": _wkb(LONLAT), "geom2": _wkb(PROJECTED)})
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {"encoding": "WKB", "geometry_types": ["Point"], "crs": WGS84},
            "geom2": {"encoding": "WKB", "geometry_types": ["Point"], "crs": secondary_crs},
        },
    }
    pq.write_table(table.replace_schema_metadata({"geo": json.dumps(geo)}), path)
    return str(path)


def _logical(path, name) -> dict:
    schema = pq.ParquetFile(path).schema
    logical = json.loads(schema.column(schema.names.index(name)).logical_type.to_json())
    assert logical.get("Type") == "Geometry", f"{name} has logical type {logical}"
    return logical


def _assert_typed(path, secondary_crs) -> None:
    assert b"geo" not in (pq.ParquetFile(path).metadata.metadata or {})
    # EPSG:4326 is the default lon/lat, which a type spells as no CRS at all.
    primary = _logical(path, "geometry").get("crs")
    assert primary in (None, "") or "4326" in str(primary), primary
    secondary = _logical(path, "geom2").get("crs")
    if secondary_crs is None:
        assert secondary == PARQUET_UNKNOWN_CRS, secondary
    else:
        assert secondary and json.loads(secondary) == secondary_crs, secondary
    result = CliRunner().invoke(cli, ["check", "spec", str(path), "--json"])
    failed = [c for c in json.loads(result.output)["checks"] if c["status"] == "failed"]
    assert failed == [], failed


@pytest.mark.parametrize("secondary", sorted(SECONDARY_CRS))
@pytest.mark.parametrize("command", sorted(COMMANDS))
def test_cli_parquet_geo_only_types_each_secondary(command, secondary, tmp_path):
    src = _write_two_geoms(tmp_path / "in.parquet", SECONDARY_CRS[secondary])
    out = str(tmp_path / "out.parquet")
    result = CliRunner().invoke(cli, COMMANDS[command](src, out))
    assert result.exit_code == 0, result.output
    _assert_typed(out, SECONDARY_CRS[secondary])


@pytest.mark.parametrize("secondary", sorted(SECONDARY_CRS))
@pytest.mark.parametrize("strategy", STRATEGIES)
def test_table_write_parquet_geo_only_types_each_secondary(strategy, secondary, tmp_path):
    src = _write_two_geoms(tmp_path / "in.parquet", SECONDARY_CRS[secondary])
    out = tmp_path / "out.parquet"
    gpio.read(src).write(str(out), geoparquet_version="parquet-geo-only", write_strategy=strategy)
    _assert_typed(out, SECONDARY_CRS[secondary])


@pytest.mark.parametrize("secondary", sorted(SECONDARY_CRS))
@pytest.mark.parametrize("strategy", STRATEGIES)
def test_parquet_geo_only_secondary_crs_survives_a_1x_write(strategy, secondary, tmp_path):
    """Reading the type back into a 1.1 block keeps the secondary's CRS.

    The type is the only carrier, so an unknown ``srid:0`` secondary must come
    back as ``crs: null`` and never crash PROJ or drop to the OGC:CRS84 default.
    """
    src = _write_two_geoms(tmp_path / "in.parquet", SECONDARY_CRS[secondary])
    native = str(tmp_path / "native.parquet")
    gpio.read(src).write(native, geoparquet_version="parquet-geo-only")
    out = tmp_path / "out.parquet"
    gpio.read(native).write(str(out), geoparquet_version="1.1", write_strategy=strategy)
    geo = json.loads(pq.read_schema(out).metadata[b"geo"])
    entry = geo["columns"]["geom2"]
    assert "crs" in entry, entry
    if SECONDARY_CRS[secondary] is None:
        assert entry["crs"] is None
    else:
        assert entry["crs"]["id"] == ALBERS["id"]
