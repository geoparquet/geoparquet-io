"""``convert`` reads the primary geometry's CRS, not a secondary's that comes first.

A Parquet input with no ``geo`` block records each geometry column's CRS only
in its native logical type. ``convert`` asked ``extract_crs_from_parquet`` for
"the" CRS without naming the column it was converting, so the answer was the
first non-default CRS in schema order. With a secondary column ahead of the
primary, that is the secondary's CRS, and the primary was relabelled with it --
in both the ``geo`` block and the logical type, over coordinates that never
moved, and ``gpio check spec`` then passed the output clean.

Which column a 2.0 output names *primary* is a separate question (DuckDB's own
block names the first geometry column); these tests assert on the ``geometry``
column's own entry, not on ``primary_column``.
"""

import json

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path

EPSG_3857 = {"authority": "EPSG", "code": 3857}
EPSG_32631 = {"authority": "EPSG", "code": 32631}


def _write_native_only(path, primary_crs: str, primary_xy: tuple[float, float]) -> None:
    """Secondary ``other`` (EPSG:3857) FIRST, primary ``geometry`` second; no geo block."""
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(f"""
            COPY (
                SELECT 1 AS id,
                    ST_SetCRS(ST_Point(1113194.9, 1118889.97), 'EPSG:3857') AS other,
                    ST_SetCRS(ST_Point({primary_xy[0]}, {primary_xy[1]}), '{primary_crs}')
                        AS geometry
            ) TO {sql_path(str(path))} (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE')
        """)
    finally:
        con.close()
    with pq.ParquetFile(str(path)) as reader:
        assert b"geo" not in (reader.metadata.metadata or {}), "input must be native-geo-only"


def _geo(path) -> dict:
    with pq.ParquetFile(str(path)) as reader:
        return json.loads(reader.metadata.metadata[b"geo"].decode("utf-8"))


def _crs_id(column_meta: dict):
    crs = column_meta.get("crs")
    return crs.get("id") if isinstance(crs, dict) else crs


def _logical_crs(path, column: str) -> str:
    con = get_duckdb_connection(load_spatial=True)
    try:
        rows = con.execute(f"DESCRIBE SELECT * FROM {sql_path(str(path))}").fetchall()
    finally:
        con.close()
    return next(r[1] for r in rows if r[0] == column)


@pytest.fixture
def default_primary(tmp_path):
    path = tmp_path / "secondary_first_default_primary.parquet"
    _write_native_only(path, "OGC:CRS84", (10.0, 10.0))
    return path


@pytest.fixture
def projected_primary(tmp_path):
    path = tmp_path / "secondary_first_utm_primary.parquet"
    _write_native_only(path, "EPSG:32631", (500000.0, 1000000.0))
    return path


@pytest.mark.parametrize("version", ["1.1", "2.0"])
def test_cli_convert_keeps_default_primary_crs(default_primary, tmp_path, version):
    out = tmp_path / f"out_{version}.parquet"
    result = CliRunner().invoke(
        cli,
        ["convert", "geoparquet", str(default_primary), str(out), "--geoparquet-version", version],
    )
    assert result.exit_code == 0, result.output

    columns = _geo(out)["columns"]
    assert _crs_id(columns["geometry"]) is None, columns["geometry"]
    assert "3857" not in _logical_crs(out, "geometry")
    # The secondary keeps its own CRS.
    assert _crs_id(columns["other"]) == EPSG_3857


@pytest.mark.parametrize("version", ["1.1", "2.0"])
def test_cli_convert_keeps_projected_primary_crs(projected_primary, tmp_path, version):
    out = tmp_path / f"out_{version}.parquet"
    result = CliRunner().invoke(
        cli,
        [
            "convert",
            "geoparquet",
            str(projected_primary),
            str(out),
            "--geoparquet-version",
            version,
        ],
    )
    assert result.exit_code == 0, result.output

    columns = _geo(out)["columns"]
    assert _crs_id(columns["geometry"]) == EPSG_32631, columns["geometry"]
    assert "32631" in _logical_crs(out, "geometry")
    assert _crs_id(columns["other"]) == EPSG_3857


def test_api_convert_keeps_primary_crs(default_primary, projected_primary):
    import geoparquet_io as gpio

    assert gpio.convert(str(default_primary)).crs is None
    assert gpio.convert(str(projected_primary)).crs["id"] == EPSG_32631


def test_cli_convert_projected_primary_after_default_secondary(tmp_path):
    """The mirror case: a default-CRS secondary first must not erase the primary's CRS."""
    path = tmp_path / "default_secondary_first.parquet"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(f"""
            COPY (
                SELECT 1 AS id,
                    ST_SetCRS(ST_Point(10, 10), 'OGC:CRS84') AS other,
                    ST_SetCRS(ST_Point(500000, 1000000), 'EPSG:32631') AS geometry
            ) TO {sql_path(str(path))} (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE')
        """)
    finally:
        con.close()
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(
        cli, ["convert", "geoparquet", str(path), str(out), "--geoparquet-version", "1.1"]
    )
    assert result.exit_code == 0, result.output
    assert _crs_id(_geo(out)["columns"]["geometry"]) == EPSG_32631
