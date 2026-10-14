"""Every command reads the PRIMARY geometry's CRS, not a secondary's (#1248).

A Parquet input with no ``geo`` block records each geometry column's CRS only
in its native logical type. ``extract_crs_from_parquet`` asked with no column
answered with the first non-default CRS in schema order, so a secondary column
ahead of the primary lent the primary its CRS. #1235 patched ``convert`` by
naming the column at two call sites. Every other caller (``convert reproject``,
the ``add`` grid keys, ``process aggregate``, the GDAL writers) still asked
without one.

The resolver now finds the primary itself: the column a ``geo`` block names,
else the column the caller names, else gpio's own name-based primary detection
over the file's native geometry columns. These tests hold each caller to it.
"""

from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.crs_utils import extract_crs_from_parquet, source_crs_string
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path

# POINT(10 10) in CRS84 projected to UTM 31N, and the H3 res-5 cell it lies in.
UTM31_X, UTM31_Y = 1268980.13, 1113599.93
H3_RES5_POINT_10_10 = "8558e06bfffffff"


def _copy(path, select_sql: str, geoparquet_version: str = "NONE") -> None:
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            f"COPY ({select_sql}) TO {sql_path(str(path))} "
            f"(FORMAT PARQUET, GEOPARQUET_VERSION '{geoparquet_version}')"
        )
    finally:
        con.close()


def _secondary_first(path, primary_crs: str = "OGC:CRS84", primary_xy=(10.0, 10.0)):
    """``other`` EPSG:3857 FIRST, primary ``geometry`` second, no geo block."""
    _copy(
        path,
        "SELECT 1 AS id, "
        "ST_SetCRS(ST_Point(1113194.9, 1118889.97), 'EPSG:3857') AS other, "
        f"ST_SetCRS(ST_Point({primary_xy[0]}, {primary_xy[1]}), '{primary_crs}') AS geometry",
    )
    with pq.ParquetFile(str(path)) as reader:
        assert b"geo" not in (reader.metadata.metadata or {}), "input must be native-geo-only"
    return path


@pytest.fixture
def secondary_first(tmp_path):
    return _secondary_first(tmp_path / "secondary_first.parquet")


@pytest.fixture
def control(tmp_path):
    """The same primary point, alone: what every keyed output must match."""
    path = tmp_path / "control.parquet"
    _copy(path, "SELECT 1 AS id, ST_SetCRS(ST_Point(10, 10), 'OGC:CRS84') AS geometry")
    return path


def _with_geo_block(path, geo: dict) -> None:
    """Add a ``geo`` key, keeping the native logical types (geoarrow round-trips them)."""
    import geoarrow.pyarrow  # noqa: F401 - registers the extension types

    table = pq.read_table(str(path))
    meta = dict(table.schema.metadata or {})
    meta[b"geo"] = json.dumps(geo).encode("utf-8")
    pq.write_table(table.replace_schema_metadata(meta), str(path))


def _crs_code(crs):
    if isinstance(crs, dict):
        ident = crs.get("id") or {}
        return f"{ident.get('authority')}:{ident.get('code')}"
    return crs


# --------------------------------------------------------------------------- #
# The resolver
# --------------------------------------------------------------------------- #


class TestPrimaryCrsResolver:
    def test_default_primary_after_projected_secondary(self, secondary_first):
        assert extract_crs_from_parquet(str(secondary_first)) is None
        assert source_crs_string(str(secondary_first)) is None

    def test_projected_primary_after_projected_secondary(self, tmp_path):
        path = _secondary_first(
            tmp_path / "utm.parquet", primary_crs="EPSG:32631", primary_xy=(5e5, 1e6)
        )
        assert _crs_code(extract_crs_from_parquet(str(path))) == "EPSG:32631"
        assert source_crs_string(str(path)) == "EPSG:32631"

    def test_named_column_is_read(self, secondary_first):
        crs = extract_crs_from_parquet(str(secondary_first), geometry_column="other")
        assert _crs_code(crs) == "EPSG:3857"

    def test_unknown_column_name_falls_back_to_the_primary(self, secondary_first):
        assert extract_crs_from_parquet(str(secondary_first), geometry_column="nope") is None

    def test_no_standard_name_reads_the_first_geometry_column(self, tmp_path):
        """Without a ``geometry``-like name the first native column is primary."""
        path = tmp_path / "unnamed.parquet"
        _copy(
            path,
            "SELECT ST_SetCRS(ST_Point(10, 10), 'OGC:CRS84') AS a, "
            "ST_SetCRS(ST_Point(1113194.9, 1118889.97), 'EPSG:3857') AS b",
        )
        assert extract_crs_from_parquet(str(path)) is None

    def test_geo_block_default_primary_ignores_a_projected_secondary(self, secondary_first):
        """A block naming a default-CRS primary must not fall to schema order either."""
        _with_geo_block(
            secondary_first,
            {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {
                    "geometry": {"encoding": "WKB", "geometry_types": []},
                    "other": {
                        "encoding": "WKB",
                        "geometry_types": [],
                        "crs": {"id": {"authority": "EPSG", "code": 3857}},
                    },
                },
            },
        )
        assert extract_crs_from_parquet(str(secondary_first)) is None

    def test_named_column_the_geo_block_omits_reads_its_native_type(self, secondary_first):
        """A requested column the block does not describe is still that column.

        Falling back to the block's primary would answer for the wrong column.
        """
        _with_geo_block(
            secondary_first,
            {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {"geometry": {"encoding": "WKB", "geometry_types": []}},
            },
        )
        crs = extract_crs_from_parquet(str(secondary_first), geometry_column="other")
        assert _crs_code(crs) == "EPSG:3857"
        assert source_crs_string(str(secondary_first), geometry_column="other") == "EPSG:3857"


# --------------------------------------------------------------------------- #
# The callers
# --------------------------------------------------------------------------- #


def _first_row(path, expr: str):
    con = get_duckdb_connection(load_spatial=True)
    try:
        return con.execute(f"SELECT {expr} FROM {sql_path(str(path))}").fetchone()
    finally:
        con.close()


def test_convert_reproject_reads_primary_crs(secondary_first, tmp_path):
    out = tmp_path / "rp.parquet"
    result = CliRunner().invoke(
        cli,
        ["convert", "reproject", str(secondary_first), str(out), "-d", "EPSG:32631"],
    )
    assert result.exit_code == 0, result.output
    x, y = _first_row(out, "ST_X(geometry), ST_Y(geometry)")
    assert x == pytest.approx(UTM31_X, abs=1.0)
    assert y == pytest.approx(UTM31_Y, abs=1.0)


_GRID_CASES = [
    ("h3", "h3_cell", ["--resolution", "5"]),
    ("a5", "a5_cell", ["--resolution", "8"]),
    ("s2", "s2_cell", ["--level", "10"]),
    ("quadkey", "quadkey", ["--resolution", "10"]),
]


@pytest.mark.parametrize(("cmd", "col", "args"), _GRID_CASES, ids=[c[0] for c in _GRID_CASES])
def test_add_grid_key_reads_primary_crs(cmd, col, args, secondary_first, control, tmp_path):
    out = tmp_path / f"{cmd}.parquet"
    expected_out = tmp_path / f"{cmd}_control.parquet"
    for src, dst in ((secondary_first, out), (control, expected_out)):
        result = CliRunner().invoke(cli, ["add", cmd, str(src), str(dst), *args])
        assert result.exit_code == 0, result.output
    got = pq.read_table(str(out)).column(col).to_pylist()
    expected = pq.read_table(str(expected_out)).column(col).to_pylist()
    assert got == expected
    if cmd == "h3":
        assert got == [H3_RES5_POINT_10_10]


def test_aggregate_h3_reads_primary_crs(secondary_first, tmp_path):
    out = tmp_path / "agg.parquet"
    result = CliRunner().invoke(
        cli,
        ["process", "aggregate", "h3", str(secondary_first), str(out), "--resolution", "5"],
    )
    assert result.exit_code == 0, result.output
    assert pq.read_table(str(out)).column("h3_cell").to_pylist() == [H3_RES5_POINT_10_10]


def test_gdal_writer_srs_reads_primary_crs(secondary_first):
    from geoparquet_io.core.format_writers import _get_srs_parameter

    assert _get_srs_parameter(str(secondary_first)) == "EPSG:4326"


@pytest.mark.parametrize("secondary", [False, True], ids=["alone", "secondary-first"])
def test_geojson_export_reprojects_native_only_primary(secondary, tmp_path):
    """RFC 7946 output: a native-geo-only projected primary is reprojected, not copied.

    ``convert geojson`` read the CRS from the ``geo`` block alone, so a file with
    none wrote projected metres as GeoJSON coordinates.
    """
    src = tmp_path / "utm.parquet"
    other = (
        "ST_SetCRS(ST_Point(1113194.9, 1118889.97), 'EPSG:3857') AS other, " if secondary else ""
    )
    _copy(
        src,
        f"SELECT 1 AS id, {other}"
        f"ST_SetCRS(ST_Point({UTM31_X}, {UTM31_Y}), 'EPSG:32631') AS geometry",
    )
    out = tmp_path / "out.geojson"
    result = CliRunner().invoke(cli, ["convert", "geojson", str(src), str(out)])
    assert result.exit_code == 0, result.output
    feature = json.loads(out.read_text())["features"][0]
    x, y = feature["geometry"]["coordinates"]
    assert x == pytest.approx(10.0, abs=1e-5)
    assert y == pytest.approx(10.0, abs=1e-5)
