"""Reading an unknown CRS is never reading lon/lat (#1277 review).

#1276 made gpio write an unknown CRS as ``srid:0`` on the Parquet type. A file
with no ``geo`` block and a ``srid:0`` type is therefore a shape gpio itself now
writes, and reading it back must say "unknown", the same as ``crs: null``:

* ``extract_crs_from_parquet`` warns, it does not stay quiet.
* ``convert reproject`` refuses it without ``--src-crs`` or ``--assume-crs84``,
  and honors ``--src-crs`` for every unknown input (null, empty, ``srid:0``).
* ``add h3``, ``add quadkey`` and ``convert geojson`` warn.

DuckDB's own V2 writer spells an unknown CRS as a ``geo`` entry with no ``crs``
key over a ``srid:0`` type. The explicit ``srid:0`` wins there, so that file is
unknown too, not OGC:CRS84.
"""

from __future__ import annotations

import json
import logging
import struct

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli
from geoparquet_io.core.crs_utils import (
    PARQUET_UNKNOWN_CRS,
    extract_crs_from_parquet,
    reset_null_crs_warnings,
    source_crs_is_unknown,
    table_crs_is_unknown,
)

# Albers (EPSG:5070) sized metres. Read as lon/lat they are far out of range.
PROJECTED = [(1500000.0 + i, 2000000.0 + i) for i in range(5)]


@pytest.fixture(autouse=True)
def _fresh_warnings():
    reset_null_crs_warnings()
    yield
    reset_null_crs_warnings()


def _wkb(points) -> pa.Array:
    return pa.array([struct.pack("<BIdd", 1, 1, x, y) for x, y in points], pa.binary())


def _write_v11(path, crs, points=PROJECTED) -> str:
    table = pa.table({"id": list(range(len(points))), "geometry": _wkb(points)})
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"], "crs": crs}},
    }
    pq.write_table(table.replace_schema_metadata({"geo": json.dumps(geo)}), path)
    return str(path)


def _write_duckdb(path, version: str, srid: str = "srid:0", points=PROJECTED) -> str:
    rows = " UNION ALL ".join(
        f"SELECT {i} AS id, ST_SetCRS(ST_Point({x}, {y}), '{srid}') AS geometry"
        for i, (x, y) in enumerate(points)
    )
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(f"COPY ({rows}) TO '{path}' (FORMAT PARQUET, GEOPARQUET_VERSION '{version}')")
    con.close()
    return str(path)


def _pgo_srid0(tmp_path) -> str:
    return _write_duckdb(tmp_path / "pgo_srid0.parquet", "NONE")


def _duckdb_v2_srid0(tmp_path) -> str:
    return _write_duckdb(tmp_path / "v2_srid0.parquet", "V2")


UNKNOWN_INPUTS = {
    "v11_null": lambda d: _write_v11(d / "v11_null.parquet", None),
    "v11_empty_object": lambda d: _write_v11(d / "v11_empty.parquet", {}),
    "pgo_srid0": _pgo_srid0,
    "duckdb_v2_srid0": _duckdb_v2_srid0,
}


def _unknown_warnings(records) -> list[str]:
    return [r.getMessage() for r in records if "unknown" in r.getMessage().lower()]


def _first_xy(path):
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    row = con.execute(
        f"SELECT ST_X(geometry), ST_Y(geometry) FROM read_parquet('{path}') ORDER BY id LIMIT 1"
    ).fetchone()
    con.close()
    return row


# --------------------------------------------------------------------------- #
# The shared resolver
# --------------------------------------------------------------------------- #


def test_duckdb_v2_srid0_shape_is_what_this_test_thinks():
    """Pin the input: DuckDB's V2 writer omits the geo crs key over a srid:0 type."""
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as d:
        path = _duckdb_v2_srid0(Path(d))
        geo = json.loads(pq.ParquetFile(path).metadata.metadata[b"geo"])
        assert "crs" not in geo["columns"]["geometry"]
        logical = pq.ParquetFile(path).schema.column(1).logical_type
        assert json.loads(logical.to_json()).get("crs") == PARQUET_UNKNOWN_CRS


@pytest.mark.parametrize("source", ["pgo_srid0", "duckdb_v2_srid0"])
def test_srid0_type_is_unknown(source, tmp_path):
    path = UNKNOWN_INPUTS[source](tmp_path)
    assert source_crs_is_unknown(path) is True


@pytest.mark.parametrize("source", ["pgo_srid0", "duckdb_v2_srid0"])
def test_extract_crs_warns_on_srid0_type(source, tmp_path, caplog):
    path = UNKNOWN_INPUTS[source](tmp_path)
    with caplog.at_level(logging.WARNING):
        assert extract_crs_from_parquet(path) is None
    messages = _unknown_warnings(caplog.records)
    assert messages, caplog.text
    assert PARQUET_UNKNOWN_CRS in messages[0]
    assert "--src-crs" in messages[0]


def test_a_named_srid_type_is_not_unknown(tmp_path, caplog):
    path = _write_duckdb(tmp_path / "pgo_5070.parquet", "NONE", srid="EPSG:5070")
    with caplog.at_level(logging.WARNING):
        assert extract_crs_from_parquet(path) is not None
    assert source_crs_is_unknown(path) is False
    assert not _unknown_warnings(caplog.records)


def test_table_with_srid0_field_and_no_crs_key_is_unknown(tmp_path):
    """The in-memory rule matches the file rule for DuckDB's V2 shape."""
    table = gpio.read(_duckdb_v2_srid0(tmp_path)).to_arrow()
    assert table_crs_is_unknown(table, "geometry") is True


# --------------------------------------------------------------------------- #
# convert reproject
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("source", list(UNKNOWN_INPUTS))
def test_reproject_refuses_unknown_without_a_flag(source, tmp_path):
    src = UNKNOWN_INPUTS[source](tmp_path)
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(cli, ["convert", "reproject", src, str(out)])
    assert result.exit_code != 0, result.output
    assert "unknown" in result.output.lower()
    assert "--src-crs" in result.output
    assert not out.exists()


@pytest.mark.parametrize("source", list(UNKNOWN_INPUTS))
def test_reproject_honors_src_crs_for_unknown(source, tmp_path):
    """The hint says `--src-crs`, so `--src-crs` has to work."""
    src = UNKNOWN_INPUTS[source](tmp_path)
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(
        cli,
        ["convert", "reproject", src, str(out), "--src-crs", "EPSG:5070", "--dst-crs", "EPSG:4326"],
    )
    assert result.exit_code == 0, result.output
    x, y = _first_xy(out)
    assert -180 <= x <= 180 and -90 <= y <= 90, (x, y)
    assert extract_crs_from_parquet(str(out)) is None
    assert source_crs_is_unknown(str(out)) is False


@pytest.mark.parametrize("source", ["pgo_srid0", "duckdb_v2_srid0"])
def test_reproject_assume_crs84_reads_srid0_as_lonlat(source, tmp_path):
    src = _write_duckdb(
        tmp_path / f"{source}_ll.parquet",
        "NONE" if source == "pgo_srid0" else "V2",
        points=[(10.0, 20.0), (11.0, 21.0)],
    )
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(cli, ["convert", "reproject", src, str(out), "--assume-crs84"])
    assert result.exit_code == 0, result.output
    assert source_crs_is_unknown(str(out)) is False
    assert _first_xy(out) == pytest.approx((10.0, 20.0))


def test_reproject_stdout_refuses_srid0(tmp_path):
    """The streaming path makes the same decision as the file path."""
    src = _pgo_srid0(tmp_path)
    result = CliRunner().invoke(cli, ["convert", "reproject", src, "-"])
    assert result.exit_code != 0
    assert "unknown" in result.output.lower()


def test_table_reproject_refuses_srid0_field(tmp_path):
    table = gpio.read(_pgo_srid0(tmp_path))
    with pytest.raises(ValueError, match="unknown"):
        table.reproject(target_crs="EPSG:4326")


def test_table_reproject_with_source_crs_on_srid0_field(tmp_path):
    table = gpio.read(_pgo_srid0(tmp_path))
    out = table.reproject(target_crs="EPSG:4326", source_crs="EPSG:5070")
    out.write(str(tmp_path / "out.parquet"))
    x, y = _first_xy(tmp_path / "out.parquet")
    assert -180 <= x <= 180 and -90 <= y <= 90


# --------------------------------------------------------------------------- #
# lon/lat commands warn
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("source", ["pgo_srid0", "duckdb_v2_srid0"])
@pytest.mark.parametrize(
    "command,suffix",
    [
        (["add", "h3"], ".parquet"),
        (["add", "quadkey"], ".parquet"),
        (["convert", "geojson"], ".geojson"),
    ],
    ids=["h3", "quadkey", "geojson"],
)
def test_lonlat_commands_warn_on_srid0(command, suffix, source, tmp_path):
    src = UNKNOWN_INPUTS[source](tmp_path)
    result = CliRunner().invoke(cli, [*command, src, str(tmp_path / f"out{suffix}")])
    loud = result.exit_code != 0 or (
        "unknown" in result.output.lower() and PARQUET_UNKNOWN_CRS in result.output
    )
    assert loud, result.output


@pytest.mark.parametrize("source", ["pgo_srid0", "duckdb_v2_srid0"])
def test_table_add_h3_warns_on_srid0_field(source, tmp_path, caplog):
    table = gpio.read(UNKNOWN_INPUTS[source](tmp_path))
    with caplog.at_level(logging.WARNING):
        table.add_h3()
    messages = _unknown_warnings(caplog.records)
    assert messages and PARQUET_UNKNOWN_CRS in messages[0], caplog.text


def test_table_add_h3_null_block_does_not_also_claim_srid0(tmp_path, caplog):
    """A geo crs key answered already, so the srid:0 warning stays out of it."""
    table = gpio.read(_write_v11(tmp_path / "null.parquet", None))
    with caplog.at_level(logging.WARNING):
        table.add_h3()
    assert not [m for m in _unknown_warnings(caplog.records) if PARQUET_UNKNOWN_CRS in m]


# --------------------------------------------------------------------------- #
# DuckDB V2 srid:0 stays unknown through a rewrite
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("version", ["1.1", "2.0"])
def test_duckdb_v2_srid0_rewrite_stays_unknown(version, tmp_path):
    src = _duckdb_v2_srid0(tmp_path)
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(
        cli, ["convert", "geoparquet", src, str(out), "--geoparquet-version", version]
    )
    assert result.exit_code == 0, result.output
    geo = json.loads(pq.ParquetFile(out).metadata.metadata[b"geo"])
    col = geo["columns"]["geometry"]
    assert "crs" in col and col["crs"] is None, col


# --------------------------------------------------------------------------- #
# A secondary column with crs: null is typed srid:0 at 2.0 on every write path
# --------------------------------------------------------------------------- #


def _write_two_geoms(path) -> str:
    """A 1.1 file whose primary is lon/lat and whose ``geom2`` CRS is unknown."""
    lonlat = [(10.0 + i, 20.0 + i) for i in range(5)]
    table = pa.table({"id": list(range(5)), "geometry": _wkb(lonlat), "geom2": _wkb(PROJECTED)})
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {"encoding": "WKB", "geometry_types": ["Point"]},
            "geom2": {"encoding": "WKB", "geometry_types": ["Point"], "crs": None},
        },
    }
    pq.write_table(table.replace_schema_metadata({"geo": json.dumps(geo)}), path)
    return str(path)


def _column_type_crs(path, name):
    schema = pq.ParquetFile(path).schema
    logical = schema.column(schema.names.index(name)).logical_type
    return json.loads(logical.to_json()).get("crs") if logical.type != "NONE" else None


def _assert_secondary_unknown(path):
    geo = json.loads(pq.ParquetFile(path).metadata.metadata[b"geo"])
    col = geo["columns"]["geom2"]
    assert "crs" in col and col["crs"] is None, col
    assert _column_type_crs(path, "geom2") == PARQUET_UNKNOWN_CRS
    result = CliRunner().invoke(cli, ["check", "spec", str(path), "--json"])
    failed = [c for c in json.loads(result.output)["checks"] if c["status"] == "failed"]
    assert failed == [], failed


@pytest.mark.parametrize("strategy", ["duckdb-kv", "in-memory", "streaming", "disk-rewrite"])
def test_table_write_types_a_null_secondary_srid0(strategy, tmp_path):
    out = tmp_path / "out.parquet"
    gpio.read(_write_two_geoms(tmp_path / "in.parquet")).write(
        str(out), geoparquet_version="2.0", write_strategy=strategy
    )
    _assert_secondary_unknown(out)


@pytest.mark.parametrize("command", [["convert", "geoparquet"], ["sort", "hilbert"]])
def test_cli_types_a_null_secondary_srid0(command, tmp_path):
    out = tmp_path / "out.parquet"
    src = _write_two_geoms(tmp_path / "in.parquet")
    result = CliRunner().invoke(cli, [*command, src, str(out), "--geoparquet-version", "2.0"])
    assert result.exit_code == 0, result.output
    _assert_secondary_unknown(out)


@pytest.mark.parametrize("source", sorted(UNKNOWN_INPUTS))
def test_check_spec_skips_the_lonlat_range_for_an_unknown_crs(source, tmp_path):
    """Projected metres under an unknown CRS are not out of the lon/lat range."""
    path = UNKNOWN_INPUTS[source](tmp_path)
    result = CliRunner().invoke(cli, ["check", "spec", path, "--json"])
    checks = {c["name"]: c for c in json.loads(result.output)["checks"]}
    range_checks = [c for n, c in checks.items() if n.startswith("coordinates_valid_for_crs")]
    assert range_checks, sorted(checks)
    assert all(c["status"] == "skipped" for c in range_checks), range_checks


def test_check_spec_still_fails_projected_metres_under_the_default_crs(tmp_path):
    path = _write_v11(tmp_path / "default.parquet", {"id": {"authority": "OGC", "code": "CRS84"}})
    result = CliRunner().invoke(cli, ["check", "spec", path, "--json"])
    checks = json.loads(result.output)["checks"]
    statuses = {c["status"] for c in checks if c["name"].startswith("coordinates_valid_for_crs")}
    assert statuses == {"failed"}


@pytest.mark.parametrize("command", [["convert", "geoparquet"], ["sort", "hilbert"]])
def test_cli_types_a_named_secondary_with_its_crs(command, tmp_path):
    """The same paths lost a named secondary CRS too, so the type carries it."""
    albers = {"id": {"authority": "EPSG", "code": 5070}}
    src = tmp_path / "in.parquet"
    _write_two_geoms(src)
    table = pq.read_table(src)
    geo = json.loads(table.schema.metadata[b"geo"])
    geo["columns"]["geom2"]["crs"] = albers
    pq.write_table(table.replace_schema_metadata({"geo": json.dumps(geo)}), src)
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(cli, [*command, str(src), str(out), "--geoparquet-version", "2.0"])
    assert result.exit_code == 0, result.output
    written = json.loads(pq.ParquetFile(out).metadata.metadata[b"geo"])["columns"]["geom2"]
    assert written.get("crs") == albers, written
    assert json.loads(_column_type_crs(out, "geom2")) == albers
