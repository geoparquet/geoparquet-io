"""Tests for --bucket-point on `gpio process aggregate` (issue #567).

Keying can derive its point from a bbox covering column (or an existing point
column) instead of the full geometry, so the (usually huge) geometry column is
never scanned for the common cell-polygon output.
"""

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5
from geoparquet_io.core.process.aggregate.by_h3 import aggregate_by_h3


def _write_points_with_bbox(path, rows, bbox_col="bbox"):
    """rows: list of (lon, lat, height). Writes points plus a bbox struct column."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    values = ", ".join(f"({lon}, {lat}, {height})" for lon, lat, height in rows)
    con.execute(
        f"""
        COPY (
            SELECT ST_Point(lon, lat) AS geometry,
                   {{'xmin': lon - 0.0004, 'ymin': lat - 0.0004,
                     'xmax': lon + 0.0004, 'ymax': lat + 0.0004}} AS "{bbox_col}",
                   height
            FROM (VALUES {values}) AS t(lon, lat, height)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


def _write_plain_points(path, rows):
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    values = ", ".join(f"({lon}, {lat}, {height})" for lon, lat, height in rows)
    con.execute(
        f"""
        COPY (
            SELECT ST_Point(lon, lat) AS geometry, height
            FROM (VALUES {values}) AS t(lon, lat, height)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


POINTS = [
    (10.00, 50.00, 4.0),
    (10.001, 50.001, 6.0),
    (-120.0, 40.0, 1.0),
]


# ---------------------------------------------------------------------------
# Source-SQL construction (fast, no grid extension)
# ---------------------------------------------------------------------------


def test_read_grid_source_sql_bbox_mode(tmp_path):
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate.grid_common import read_grid_source_sql

    src = tmp_path / "f.parquet"
    _write_points_with_bbox(src, POINTS)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        sql = read_grid_source_sql(
            con, str(src), "geometry", bucket_point="bbox", bbox_column="bbox"
        )
        # Keying point from the bbox center, geometry column excluded from the scan.
        assert "AS __pt" in sql
        assert "ST_Point" in sql
        assert 'EXCLUDE ("geometry")' in sql
        cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM ({sql})").fetchall()}
        assert "geometry" not in cols
        assert "__pt" in cols
        # bbox center == the original point (symmetric box)
        row = con.execute(f"SELECT ST_X(__pt), ST_Y(__pt) FROM ({sql}) LIMIT 1").fetchone()
        assert row[0] == pytest.approx(10.0)
        assert row[1] == pytest.approx(50.0)
    finally:
        con.close()


def test_read_grid_source_sql_geometry_mode_default(tmp_path):
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate.grid_common import read_grid_source_sql

    src = tmp_path / "f.parquet"
    _write_plain_points(src, POINTS)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        sql = read_grid_source_sql(con, str(src), "geometry")
        assert "ST_Centroid" in sql
        assert "AS __pt" in sql
        cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM ({sql})").fetchall()}
        assert "geometry" in cols  # geometry passthrough kept in default mode
    finally:
        con.close()


def test_read_grid_source_sql_point_column_mode(tmp_path):
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate.grid_common import read_grid_source_sql

    src = tmp_path / "f.parquet"
    con0 = duckdb.connect()
    con0.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    con0.execute(
        f"""
        COPY (
            SELECT ST_Point(0.0, 0.0) AS geometry, ST_Point(10.0, 50.0) AS anchor, 1.0 AS height
        ) TO '{src}' (FORMAT PARQUET)
        """
    )
    con0.close()
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        sql = read_grid_source_sql(con, str(src), "geometry", bucket_point="anchor")
        assert 'EXCLUDE ("geometry")' in sql
        row = con.execute(f"SELECT ST_X(__pt), ST_Y(__pt) FROM ({sql})").fetchone()
        assert row == (10.0, 50.0)
    finally:
        con.close()


def test_read_grid_source_sql_bbox_mode_transforms_non_crs84(tmp_path):
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate.grid_common import read_grid_source_sql

    src = tmp_path / "f.parquet"
    _write_points_with_bbox(src, POINTS)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        sql = read_grid_source_sql(
            con,
            str(src),
            "geometry",
            source_crs="EPSG:5070",
            bucket_point="bbox",
            bbox_column="bbox",
        )
        assert "ST_Transform" in sql  # bbox coords are in the file's CRS
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Validation / detection
# ---------------------------------------------------------------------------


def test_bbox_mode_errors_when_no_bbox_column(tmp_path):
    src = tmp_path / "f.parquet"
    _write_plain_points(src, POINTS)
    with pytest.raises(InvalidParameterError, match="bbox"):
        aggregate_by_a5(str(src), str(tmp_path / "o.parquet"), resolution=5, bucket_point="bbox")


def test_bbox_column_requires_bbox_mode(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_with_bbox(src, POINTS)
    with pytest.raises(InvalidParameterError, match="bucket-point"):
        aggregate_by_a5(str(src), str(tmp_path / "o.parquet"), resolution=5, bbox_column="bbox")


def test_h3_bbox_mode_errors_when_no_bbox_column(tmp_path):
    src = tmp_path / "f.parquet"
    _write_plain_points(src, POINTS)
    with pytest.raises(InvalidParameterError, match="bbox"):
        aggregate_by_h3(str(src), str(tmp_path / "o.parquet"), resolution=5, bucket_point="bbox")


def test_admin_bbox_mode_errors_when_no_bbox_column(tmp_path):
    """Admin path resolves the bbox column before any dataset setup or download."""
    from geoparquet_io.core.process.aggregate.by_admin import aggregate_by_admin

    src = tmp_path / "f.parquet"
    _write_plain_points(src, POINTS)
    with pytest.raises(InvalidParameterError, match="bbox"):
        aggregate_by_admin(
            str(src), str(tmp_path / "o.parquet"), level="country", bucket_point="bbox"
        )


def test_table_bbox_mode_errors_when_no_bbox_column():
    from geoparquet_io.core.process.aggregate.by_a5 import aggregate_a5_table

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    tbl = (
        con.execute("SELECT ST_AsWKB(ST_Point(10.0, 50.0)) AS geometry, 1.0 AS height")
        .arrow()
        .read_all()
    )
    con.close()
    with pytest.raises(InvalidParameterError, match="bbox"):
        aggregate_a5_table(tbl, resolution=5, bucket_point="bbox")


def test_table_bbox_mode_autodetects_from_schema():
    """Table-path detection finds a conventional bbox struct in the Arrow schema."""
    from geoparquet_io.core.process.aggregate.grid_common import (
        _resolve_bbox_column_for_table,
    )

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    tbl = (
        con.execute(
            "SELECT ST_AsWKB(ST_Point(10.0, 50.0)) AS geometry, "
            "{'xmin': 9.9, 'ymin': 49.9, 'xmax': 10.1, 'ymax': 50.1} AS bbox"
        )
        .arrow()
        .read_all()
    )
    con.close()
    assert _resolve_bbox_column_for_table(tbl, None) == "bbox"
    # An explicit column that does not exist fails up front, not at bind time.
    with pytest.raises(InvalidParameterError, match="custom"):
        _resolve_bbox_column_for_table(tbl, "custom")


def test_admin_joined_sql_uses_point_expr_and_exclude():
    from geoparquet_io.core.process.aggregate.by_admin import _build_joined_sql

    sql = _build_joined_sql(
        "in.parquet",
        "ST_Point((bbox.xmin + bbox.xmax) / 2.0, (bbox.ymin + bbox.ymax) / 2.0)",
        "read_parquet('admin.parquet')",
        "country",
        "country",
        "geometry",
        exclude_sql=' EXCLUDE ("geometry")',
    )
    inner = sql.split(") s")[0]
    assert "ST_Point((bbox.xmin" in inner
    assert 'EXCLUDE ("geometry")' in inner


# ---------------------------------------------------------------------------
# Antimeridian-crossing bboxes (xmin > xmax encodes a dateline crossing)
# ---------------------------------------------------------------------------


def _write_bbox_rows(path, rows, bbox_col="bbox", with_geometry=False):
    """rows: list of (id, xmin, ymin, xmax, ymax). Optional point geometry at bbox 'center'."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    values = ", ".join(
        f"({i}, {xmin}, {ymin}, {xmax}, {ymax})" for i, xmin, ymin, xmax, ymax in rows
    )
    geom_sql = "ST_Point(xmin, ymin) AS geometry, " if with_geometry else ""
    con.execute(
        f"""
        COPY (
            SELECT id, {geom_sql}
                   {{'xmin': xmin, 'ymin': ymin, 'xmax': xmax, 'ymax': ymax}} AS "{bbox_col}"
            FROM (VALUES {values}) AS t(id, xmin, ymin, xmax, ymax)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


def test_bbox_center_wraps_antimeridian(tmp_path):
    """A covering with xmin > xmax crosses the dateline; its center must not be lon 0."""
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate.grid_common import read_grid_source_sql

    src = tmp_path / "f.parquet"
    _write_bbox_rows(
        src,
        [
            (1, 179.9, -17.0, -179.9, -16.0),  # Fiji-style crossing -> lon 180
            (2, 170.0, 0.0, -160.0, 2.0),  # wide crossing -> (170-160+360)/2=185 -> -175
            (3, 9.9, 49.9, 10.1, 50.1),  # ordinary box -> lon 10
        ],
    )
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        sql = read_grid_source_sql(
            con, str(src), "geometry", bucket_point="bbox", bbox_column="bbox"
        )
        rows = con.execute(f"SELECT id, ST_X(__pt), ST_Y(__pt) FROM ({sql}) ORDER BY id").fetchall()
        lons = {r[0]: r[1] for r in rows}
        assert lons[1] == pytest.approx(180.0)
        assert lons[2] == pytest.approx(-175.0)
        assert lons[3] == pytest.approx(10.0)
        assert rows[0][2] == pytest.approx(-16.5)
    finally:
        con.close()


def test_bbox_center_no_wraparound_for_projected_crs(tmp_path):
    """xmin > xmax has no dateline meaning in a projected CRS -> plain midpoint."""
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate.grid_common import bucket_point_expr

    src = tmp_path / "f.parquet"
    _write_bbox_rows(src, [(1, 1500000.0, 2000000.0, 1600000.0, 2100000.0)])
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        projected = {"type": "ProjectedCRS", "id": {"authority": "EPSG", "code": 5070}}
        expr, _ = bucket_point_expr(
            con, f"read_parquet('{src}')", "geometry", projected, "bbox", "bbox"
        )
        assert "360.0" not in expr  # no antimeridian arithmetic for projected coords
        geographic_expr, _ = bucket_point_expr(
            con, f"read_parquet('{src}')", "geometry", None, "bbox", "bbox"
        )
        assert "360.0" in geographic_expr
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Up-front validation of keying columns and --bucket-point values
# ---------------------------------------------------------------------------


def test_bucket_point_expr_bbox_mode_requires_column(tmp_path):
    """bbox mode with no column raises InvalidParameterError, not AttributeError."""
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate.grid_common import bucket_point_expr

    src = tmp_path / "f.parquet"
    _write_points_with_bbox(src, POINTS)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        with pytest.raises(InvalidParameterError, match="bbox"):
            bucket_point_expr(con, f"read_parquet('{src}')", "geometry", None, "bbox", None)
    finally:
        con.close()


def test_bucket_point_empty_string_rejected(tmp_path):
    src = tmp_path / "f.parquet"
    _write_plain_points(src, POINTS)
    with pytest.raises(InvalidParameterError, match="empty"):
        aggregate_by_a5(str(src), str(tmp_path / "o.parquet"), resolution=5, bucket_point="")


def test_explicit_bbox_column_missing_fails_fast(tmp_path):
    """A typo'd --bbox-column errors up front (before the --auto probe or grid install)."""
    src = tmp_path / "f.parquet"
    _write_points_with_bbox(src, POINTS)
    with pytest.raises(InvalidParameterError, match="nope"):
        aggregate_by_a5(
            str(src),
            str(tmp_path / "o.parquet"),
            auto=True,  # would be expensive; validation must precede it
            bucket_point="bbox",
            bbox_column="nope",
        )


def test_explicit_bbox_column_not_a_struct_fails_fast(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_with_bbox(src, POINTS)
    with pytest.raises(InvalidParameterError, match="height"):
        aggregate_by_a5(
            str(src),
            str(tmp_path / "o.parquet"),
            resolution=5,
            bucket_point="bbox",
            bbox_column="height",
        )


def test_explicit_bbox_column_wrong_struct_fields_fails_fast(tmp_path):
    """A struct with minx/maxx-style fields is rejected with a clear error."""
    src = tmp_path / "f.parquet"
    con = duckdb.connect()
    con.execute(
        f"""
        COPY (
            SELECT 1 AS id,
                   {{'minx': 9.9, 'miny': 49.9, 'maxx': 10.1, 'maxy': 50.1}} AS my_box
        ) TO '{src}' (FORMAT PARQUET)
        """
    )
    con.close()
    with pytest.raises(InvalidParameterError, match="xmin"):
        aggregate_by_a5(
            str(src),
            str(tmp_path / "o.parquet"),
            resolution=5,
            bucket_point="bbox",
            bbox_column="my_box",
        )


def test_nonexistent_point_column_fails_fast(tmp_path):
    src = tmp_path / "f.parquet"
    _write_plain_points(src, POINTS)
    with pytest.raises(InvalidParameterError, match="anchor"):
        aggregate_by_a5(str(src), str(tmp_path / "o.parquet"), resolution=5, bucket_point="anchor")


def test_wrong_case_bbox_keyword_gets_hint(tmp_path):
    """'BBOX' falls into point-column mode by design; the error must say why."""
    src = tmp_path / "f.parquet"
    _write_points_with_bbox(src, POINTS)
    with pytest.raises(InvalidParameterError, match="did you mean 'bbox'"):
        aggregate_by_a5(str(src), str(tmp_path / "o.parquet"), resolution=5, bucket_point="BBOX")


def test_core_error_messages_are_flag_neutral(tmp_path):
    """Core errors surface through the Python API too — no CLI flag wording."""
    src = tmp_path / "f.parquet"
    _write_plain_points(src, POINTS)
    with pytest.raises(InvalidParameterError) as exc1:
        aggregate_by_a5(str(src), str(tmp_path / "o.parquet"), resolution=5, bucket_point="bbox")
    assert "--" not in str(exc1.value)
    src2 = tmp_path / "g.parquet"
    _write_points_with_bbox(src2, POINTS)
    with pytest.raises(InvalidParameterError) as exc2:
        aggregate_by_a5(str(src2), str(tmp_path / "o.parquet"), resolution=5, bbox_column="bbox")
    assert "--" not in str(exc2.value)


# ---------------------------------------------------------------------------
# bbox mode combined with --where
# ---------------------------------------------------------------------------


def test_read_grid_source_sql_bbox_mode_with_where(tmp_path):
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate.grid_common import read_grid_source_sql

    src = tmp_path / "f.parquet"
    _write_points_with_bbox(src, POINTS)  # heights 4.0, 6.0, 1.0
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        sql = read_grid_source_sql(
            con, str(src), "geometry", where="height > 2", bucket_point="bbox", bbox_column="bbox"
        )
        assert con.execute(f"SELECT count(*) FROM ({sql})").fetchone()[0] == 2
    finally:
        con.close()


# ---------------------------------------------------------------------------
# NULL bboxes and heterogeneous globs
# ---------------------------------------------------------------------------


def test_null_bbox_yields_null_keying_point(tmp_path):
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate.grid_common import read_grid_source_sql

    src = tmp_path / "f.parquet"
    con0 = duckdb.connect()
    con0.execute(
        f"""
        COPY (
            SELECT 1 AS id, {{'xmin': 9.9, 'ymin': 49.9, 'xmax': 10.1, 'ymax': 50.1}} AS bbox
            UNION ALL
            SELECT 2 AS id, NULL AS bbox
        ) TO '{src}' (FORMAT PARQUET)
        """
    )
    con0.close()
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        sql = read_grid_source_sql(
            con, str(src), "geometry", bucket_point="bbox", bbox_column="bbox"
        )
        rows = con.execute(f"SELECT id, __pt IS NULL FROM ({sql}) ORDER BY id").fetchall()
        assert rows == [(1, False), (2, True)]
    finally:
        con.close()


def test_unassigned_reason_is_mode_aware():
    from geoparquet_io.core.process.aggregate.grid_common import _unassigned_reason

    assert "geometry" in _unassigned_reason("geometry", None)
    bbox_reason = _unassigned_reason("bbox", "my_box")
    assert "my_box" in bbox_reason and "geometry" not in bbox_reason
    pt_reason = _unassigned_reason("anchor", None)
    assert "anchor" in pt_reason and "geometry" not in pt_reason


def test_heterogeneous_glob_warns_about_missing_column(tmp_path, caplog):
    """union_by_name NULL-fills the keying column for files that lack it — warn."""
    import logging

    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate.grid_common import _warn_files_missing_column

    _write_points_with_bbox(tmp_path / "a.parquet", POINTS)
    _write_plain_points(tmp_path / "b.parquet", POINTS)  # no bbox column
    con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
    try:
        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            _warn_files_missing_column(con, f"{tmp_path}/*.parquet", "bbox")
        assert any("bbox" in r.message and "unassigned" in r.message for r in caplog.records)
        caplog.clear()
        # Homogeneous glob: no warning.
        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            _warn_files_missing_column(con, f"{tmp_path}/a*.parquet", "bbox")
        assert not caplog.records
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Covering-metadata-first bbox auto-detection
# ---------------------------------------------------------------------------


def _write_parquet_with_covering(path, bbox_col, extra_bbox_col=None):
    """Write a GeoParquet file whose covering metadata references ``bbox_col``."""
    import json

    import pyarrow as pa

    box = {"xmin": 9.9, "ymin": 49.9, "xmax": 10.1, "ymax": 50.1}
    struct_type = pa.struct([(k, pa.float64()) for k in ("xmin", "ymin", "xmax", "ymax")])
    cols = {
        # WKB for POINT(10 50), little-endian.
        "geometry": pa.array(
            [bytes.fromhex("010100000000000000000024400000000000004940")],
            type=pa.binary(),
        ),
        bbox_col: pa.array([box], type=struct_type),
    }
    if extra_bbox_col:
        cols[extra_bbox_col] = pa.array([box], type=struct_type)
    geo_meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "covering": {
                    "bbox": {
                        "xmin": [bbox_col, "xmin"],
                        "ymin": [bbox_col, "ymin"],
                        "xmax": [bbox_col, "xmax"],
                        "ymax": [bbox_col, "ymax"],
                    }
                },
            }
        },
    }
    table = pa.table(cols)
    table = table.replace_schema_metadata({b"geo": json.dumps(geo_meta).encode()})
    pq.write_table(table, path)
    return table


def test_check_bbox_structure_uses_covering_metadata_first(tmp_path):
    """Spec-valid files with non-conventional covering names are detected."""
    from geoparquet_io.core.common import check_bbox_structure

    src = tmp_path / "f.parquet"
    _write_parquet_with_covering(src, "my_box")
    result = check_bbox_structure(str(src))
    assert result["bbox_column_name"] == "my_box"
    assert result["has_bbox_column"] is True


def test_check_bbox_structure_covering_beats_name_convention(tmp_path):
    """When covering points at one struct and a decoy conventional name exists,
    the authoritative covering wins."""
    from geoparquet_io.core.common import check_bbox_structure

    src = tmp_path / "f.parquet"
    _write_parquet_with_covering(src, "my_box", extra_bbox_col="bbox")
    assert check_bbox_structure(str(src))["bbox_column_name"] == "my_box"


def test_detect_bbox_column_from_table_uses_covering_first(tmp_path):
    from geoparquet_io.core.common import _detect_bbox_column_from_table

    table = _write_parquet_with_covering(tmp_path / "f.parquet", "my_box", extra_bbox_col="bbox")
    assert _detect_bbox_column_from_table(table, verbose=True) == "my_box"


def test_resolve_bbox_column_autodetects_covering_name(tmp_path):
    from geoparquet_io.core.process.aggregate.grid_common import _resolve_bbox_column_for_file

    src = tmp_path / "f.parquet"
    _write_parquet_with_covering(src, "my_box")
    assert _resolve_bbox_column_for_file(str(src), None, False) == "my_box"


# ---------------------------------------------------------------------------
# Geometry-less (attribute + bbox only) inputs — the input this feature enables
# ---------------------------------------------------------------------------


def _write_geometry_less_bbox_file(path, rows, bbox_col="bbox"):
    """Attribute + bbox covering columns only — no geometry column at all."""
    con = duckdb.connect()
    values = ", ".join(f"({lon}, {lat}, {height})" for lon, lat, height in rows)
    con.execute(
        f"""
        COPY (
            SELECT {{'xmin': lon - 0.0004, 'ymin': lat - 0.0004,
                     'xmax': lon + 0.0004, 'ymax': lat + 0.0004}} AS "{bbox_col}",
                   height
            FROM (VALUES {values}) AS t(lon, lat, height)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


def test_grid_source_sql_geometry_less_bbox_input(tmp_path):
    """The a5/h3 source relation binds cleanly with no geometry column present."""
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate.grid_common import read_grid_source_sql

    src = tmp_path / "f.parquet"
    _write_geometry_less_bbox_file(src, POINTS)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        sql = read_grid_source_sql(
            con, str(src), "geometry", bucket_point="bbox", bbox_column="bbox"
        )
        assert con.execute(f"SELECT count(*) FROM ({sql})").fetchone()[0] == len(POINTS)
    finally:
        con.close()


class _FakeAdminDataset:
    """Local stand-in for the Overture dataset so the admin path runs offline."""

    def __init__(self, admin_path):
        self._path = str(admin_path)

    def get_level_column_mapping(self):
        return {"country": "country"}

    def get_geometry_column(self):
        return "geometry"

    def get_bbox_column(self):
        return None

    def configure_s3(self, con):
        pass

    def supports_per_level_sources(self):
        return True

    def get_source_for_level(self, level):
        return self._path


def _write_admin_polygons(path):
    """One country polygon around (10, 50) with code 'XX'."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    con.execute(
        f"""
        COPY (
            SELECT 'XX' AS country,
                   ST_GeomFromText('POLYGON((9 49, 11 49, 11 51, 9 51, 9 49))') AS geometry
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


def _run_admin_offline(monkeypatch, admin_path):
    from geoparquet_io.core.process.aggregate import by_admin

    monkeypatch.setattr(
        by_admin,
        "_setup_admin_dataset",
        lambda dataset, verbose, levels: (_FakeAdminDataset(admin_path), None),
    )


def test_admin_geometry_less_bbox_input(tmp_path, monkeypatch):
    """Admin path must not EXCLUDE a geometry column the input does not have (P2)."""
    from geoparquet_io.core.process.aggregate.by_admin import aggregate_by_admin

    admin_path = tmp_path / "admin.parquet"
    _write_admin_polygons(admin_path)
    _run_admin_offline(monkeypatch, admin_path)

    src = tmp_path / "pts.parquet"
    out = tmp_path / "out.parquet"
    _write_geometry_less_bbox_file(src, POINTS)  # two near (10, 50), one at (-120, 40)
    aggregate_by_admin(str(src), str(out), level="country", bucket_point="bbox", where="height > 0")
    df = pq.read_table(out).to_pandas()
    assert int(df.loc[df["admin_code"] == "XX", "count"].iloc[0]) == 2
    assert int(df.loc[df["admin_code"] == "unassigned", "count"].iloc[0]) == 1


def test_admin_bbox_mode_still_excludes_real_geometry(tmp_path, monkeypatch):
    """With a geometry column present, bbox mode still works (column excluded from scan)."""
    from geoparquet_io.core.process.aggregate.by_admin import aggregate_by_admin

    admin_path = tmp_path / "admin.parquet"
    _write_admin_polygons(admin_path)
    _run_admin_offline(monkeypatch, admin_path)

    src = tmp_path / "pts.parquet"
    out = tmp_path / "out.parquet"
    _write_points_with_bbox(src, POINTS)
    aggregate_by_admin(str(src), str(out), level="country", bucket_point="bbox")
    df = pq.read_table(out).to_pandas()
    assert int(df.loc[df["admin_code"] == "XX", "count"].iloc[0]) == 2


def test_table_explicit_bbox_column_wrong_shape_errors():
    from geoparquet_io.core.process.aggregate.grid_common import _resolve_bbox_column_for_table

    con = duckdb.connect()
    tbl = con.execute("SELECT 1.5 AS height").arrow().read_all()
    con.close()
    with pytest.raises(InvalidParameterError, match="xmin"):
        _resolve_bbox_column_for_table(tbl, "height")


def test_cli_bad_bbox_column_is_clean_error(tmp_path):
    """Core InvalidParameterError surfaces as a clean CLI error, not a traceback."""
    src = tmp_path / "f.parquet"
    _write_points_with_bbox(src, POINTS)
    runner = CliRunner()
    for sub in ("a5", "h3", "admin"):
        args = ["process", "aggregate", sub, str(src), str(tmp_path / "o.parquet")]
        if sub != "admin":
            args += ["--resolution", "5"]
        args += ["--bucket-point", "bbox", "--bbox-column", "nope"]
        result = runner.invoke(cli, args)
        assert result.exit_code != 0
        assert "nope" in result.output


def test_validate_keying_columns_accepts_existing_point_column(tmp_path):
    from geoparquet_io.core.process.aggregate.grid_common import (
        _validate_keying_columns_for_file,
    )

    src = tmp_path / "f.parquet"
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    con.execute(
        f"COPY (SELECT ST_Point(10.0, 50.0) AS geometry, ST_Point(10.0, 50.0) AS anchor) "
        f"TO '{src}' (FORMAT PARQUET)"
    )
    con.close()
    _validate_keying_columns_for_file(str(src), "anchor", None, False)  # no raise


def test_bbox_column_from_covering_edge_cases():
    from geoparquet_io.core.common import _bbox_column_from_covering

    refs = {k: ["my_box", k] for k in ("xmin", "ymin", "xmax", "ymax")}
    good = {"columns": {"geometry": {"covering": {"bbox": refs}}}}
    assert _bbox_column_from_covering(good) == "my_box"
    assert _bbox_column_from_covering(None) is None
    assert _bbox_column_from_covering({"columns": ["not-a-dict"]}) is None
    assert _bbox_column_from_covering({"columns": {"geometry": "not-a-dict"}}) is None
    assert _bbox_column_from_covering({"columns": {"geometry": {"covering": {}}}}) is None
    # Malformed refs (not [column, field] pairs) are ignored.
    bad_refs = dict.fromkeys(("xmin", "ymin", "xmax", "ymax"), "my_box.xmin")
    assert _bbox_column_from_covering({"columns": {"g": {"covering": {"bbox": bad_refs}}}}) is None


def test_covering_reference_to_bad_column_falls_back(tmp_path):
    """Covering pointing at a non-struct or missing column falls back to conventions."""
    import json

    import pyarrow as pa

    from geoparquet_io.core.common import check_bbox_structure

    box = {"xmin": 9.9, "ymin": 49.9, "xmax": 10.1, "ymax": 50.1}
    struct_type = pa.struct([(k, pa.float64()) for k in ("xmin", "ymin", "xmax", "ymax")])
    for target, name in (("height", "nonstruct"), ("ghost", "missing")):
        table = pa.table(
            {
                "geometry": pa.array(
                    [bytes.fromhex("010100000000000000000024400000000000004940")],
                    type=pa.binary(),
                ),
                "height": pa.array([1.5]),
                "bbox": pa.array([box], type=struct_type),
            }
        )
        geo_meta = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": ["Point"],
                    "covering": {"bbox": {k: [target, k] for k in box}},
                }
            },
        }
        table = table.replace_schema_metadata({b"geo": json.dumps(geo_meta).encode()})
        path = tmp_path / f"{name}.parquet"
        pq.write_table(table, path)
        assert check_bbox_structure(str(path))["bbox_column_name"] == "bbox"


def test_detect_bbox_column_from_table_bad_covering_falls_back():
    """Table-path detection ignores a covering that references a missing column."""
    import json

    import pyarrow as pa

    from geoparquet_io.core.common import _detect_bbox_column_from_table

    box = {"xmin": 9.9, "ymin": 49.9, "xmax": 10.1, "ymax": 50.1}
    struct_type = pa.struct([(k, pa.float64()) for k in ("xmin", "ymin", "xmax", "ymax")])
    table = pa.table({"bbox": pa.array([box], type=struct_type)})
    geo_meta = {"columns": {"geometry": {"covering": {"bbox": {k: ["ghost", k] for k in box}}}}}
    table = table.replace_schema_metadata({b"geo": json.dumps(geo_meta).encode()})
    assert _detect_bbox_column_from_table(table, verbose=True) == "bbox"


def test_build_grid_query_with_metric_and_breakdown(tmp_path):
    """The shared grid query builder runs end-to-end with a plain-SQL scheme."""
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate.grid_common import (
        GridScheme,
        build_grid_query,
        read_grid_source_sql,
    )

    scheme = GridScheme(
        name="dummy",
        extension="",
        min_resolution=0,
        max_resolution=5,
        default_column="cell",
        key_template="CAST(floor(ST_X({pt})) AS INTEGER)",
        boundary_template="ST_MakeEnvelope(CAST({cell} AS DOUBLE), 0.0, CAST({cell} AS DOUBLE) + 1.0, 1.0)",
        latlng_template="{cell}",
        centroid_wkb_template="ST_AsWKB(ST_Point(CAST({ll} AS DOUBLE), 0.0))",
    )
    src = tmp_path / "f.parquet"
    _write_points_with_bbox(src, POINTS)

    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        source_sql = read_grid_source_sql(
            con, str(src), "geometry", bucket_point="bbox", bbox_column="bbox"
        )
        sql = build_grid_query(con, scheme, source_sql, 1, "cell", "avg:height", None, 20, "none")
        rows = con.execute(sql).fetchall()
        assert sum(r[1] for r in rows) == len(POINTS)
    finally:
        con.close()

    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        source_sql = read_grid_source_sql(con, str(src), "geometry")
        sql = build_grid_query(con, scheme, source_sql, 1, "cell", None, "height", 20, "polygon")
        result = con.execute(sql).arrow().read_all()
        assert "geometry" in result.column_names
        assert sum(result.column("count").to_pylist()) == len(POINTS)
    finally:
        con.close()


def test_build_exclude_clause_drops_only_existing_columns(tmp_path):
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate.grid_common import build_exclude_clause

    src = tmp_path / "f.parquet"
    _write_geometry_less_bbox_file(src, POINTS)
    con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
    try:
        rel = f"read_parquet('{src}')"
        assert build_exclude_clause(con, rel, ("geometry",)) == ""
        assert build_exclude_clause(con, rel, ("geometry", "height")) == ' EXCLUDE ("height")'
    finally:
        con.close()


# ---------------------------------------------------------------------------
# End-to-end (grid community extensions)
# ---------------------------------------------------------------------------


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_a5_bbox_matches_geometry_keying(tmp_path):
    """For symmetric boxes around points, bbox keying == centroid keying."""
    src = tmp_path / "f.parquet"
    _write_points_with_bbox(src, POINTS)
    out_geom = tmp_path / "geom.parquet"
    out_bbox = tmp_path / "bbox.parquet"
    aggregate_by_a5(str(src), str(out_geom), resolution=5, metric="avg:height")
    aggregate_by_a5(str(src), str(out_bbox), resolution=5, metric="avg:height", bucket_point="bbox")
    df_g = pq.read_table(out_geom).to_pandas().sort_values("a5_cell").reset_index(drop=True)
    df_b = pq.read_table(out_bbox).to_pandas().sort_values("a5_cell").reset_index(drop=True)
    assert list(df_g["a5_cell"]) == list(df_b["a5_cell"])
    assert list(df_g["count"]) == list(df_b["count"])
    assert list(df_g["avg_height"]) == list(df_b["avg_height"])


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_a5_bbox_autodetects_column(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_with_bbox(src, POINTS)  # conventional name "bbox" -> auto-detected
    out = tmp_path / "o.parquet"
    aggregate_by_a5(str(src), str(out), resolution=5, bucket_point="bbox")
    assert sum(pq.read_table(out).column("count").to_pylist()) == 3


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_a5_bbox_explicit_nonstandard_column(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_with_bbox(src, POINTS, bbox_col="my_box")  # convention can't find this
    out = tmp_path / "o.parquet"
    aggregate_by_a5(str(src), str(out), resolution=5, bucket_point="bbox", bbox_column="my_box")
    assert sum(pq.read_table(out).column("count").to_pylist()) == 3


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_h3_bbox_mode(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_with_bbox(src, POINTS)
    out = tmp_path / "o.parquet"
    aggregate_by_h3(str(src), str(out), resolution=5, bucket_point="bbox")
    assert sum(pq.read_table(out).column("count").to_pylist()) == 3


@pytest.mark.slow
@pytest.mark.network
def test_cli_aggregate_a5_bucket_point_bbox(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_with_bbox(src, POINTS)
    out = tmp_path / "o.parquet"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "process",
            "aggregate",
            "a5",
            str(src),
            str(out),
            "--resolution",
            "5",
            "--bucket-point",
            "bbox",
        ],
    )
    assert result.exit_code == 0, result.output
    assert out.exists()


@pytest.mark.slow
@pytest.mark.network
def test_table_aggregate_a5_bucket_point_bbox():
    from geoparquet_io.api.table import Table

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    tbl = (
        con.execute(
            """
            SELECT ST_AsWKB(ST_Point(lon, lat)) AS geometry,
                   {'xmin': lon - 0.001, 'ymin': lat - 0.001,
                    'xmax': lon + 0.001, 'ymax': lat + 0.001} AS bbox,
                   height
            FROM (VALUES (10.0, 50.0, 2.0), (-120.0, 40.0, 4.0)) AS t(lon, lat, height)
            """
        )
        .arrow()
        .read_all()
    )
    con.close()
    result = Table(tbl).aggregate_a5(resolution=5, bucket_point="bbox")
    assert sum(result.table.column("count").to_pylist()) == 2


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_admin_bucket_point_bbox(tmp_path):
    from geoparquet_io.core.process.aggregate.by_admin import aggregate_by_admin

    src = tmp_path / "pts.parquet"
    out = tmp_path / "by_country.parquet"
    # Paris + Lyon (France) and one ocean point.
    _write_points_with_bbox(src, [(2.35, 48.85, 1.0), (4.85, 45.75, 2.0), (-30.0, 0.0, 3.0)])
    aggregate_by_admin(str(src), str(out), level="country", bucket_point="bbox")
    df = pq.read_table(out).to_pandas()
    assert int(df.loc[df["admin_code"] == "FR", "count"].iloc[0]) == 2
    assert int(df.loc[df["admin_code"] == "unassigned", "count"].iloc[0]) == 1
