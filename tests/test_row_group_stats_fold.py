"""One fold of per-row-group geospatial statistics, one policy (#1367 concept 5).

``geo_metadata.merged_geometry_stats`` is the one fold and
``geo_metadata.row_group_geometry_stats`` its row-group reader;
``duckdb_metadata.aggregate_native_geo_stats`` is a thin dict view of it. The inspect
"Overall Bbox" (``metadata_utils._calculate_overall_bbox``) and the derived
2.0 block (``derive_geo_from_file._geo_col_meta_from_stats``) used to fold the
same row-group extents themselves with a plain min/max, which turns a chunk
that wraps the antimeridian (xmin > xmax, permitted by the Parquet geospatial
statistics) into its complement; and the fold's own type formatter spelled a
DuckDB ``polygon_z`` as ``Polygon_ Z`` and kept ``Unknown(9)`` in a type list.
"""

from __future__ import annotations

from geoparquet_io.core.duckdb_metadata import aggregate_native_geo_stats
from geoparquet_io.core.metadata_utils import _calculate_overall_bbox


def _chunk(xmin, ymin, xmax, ymax, types=(), zmin=None, zmax=None):
    return {
        "xmin": xmin,
        "ymin": ymin,
        "xmax": xmax,
        "ymax": ymax,
        "zmin": zmin,
        "zmax": zmax,
        "geometry_types": list(types),
    }


def test_the_fold_names_duckdb_dimension_strings_as_the_spec_does():
    stats = aggregate_native_geo_stats(
        [_chunk(0, 0, 1, 1, ["polygon_z"]), _chunk(0, 0, 1, 1, ["multipolygon_zm", "point"])]
    )
    assert stats["geometry_types"] == ["MultiPolygon ZM", "Point", "Polygon Z"]


def test_the_fold_drops_type_codes_that_are_not_spec_types():
    stats = aggregate_native_geo_stats([_chunk(0, 0, 1, 1, [0, 9, 3, 1003])])
    assert stats["geometry_types"] == ["Polygon", "Polygon Z"]


def test_the_inspect_overall_bbox_reads_a_wrapping_row_group_on_the_circle():
    rows = [
        {"row_group": 0, "xmin": 170.0, "ymin": -10.0, "xmax": -170.0, "ymax": 10.0},
        {"row_group": 1, "xmin": 175.0, "ymin": -5.0, "xmax": 178.0, "ymax": 5.0},
    ]
    assert _calculate_overall_bbox(rows) == {
        "xmin": 170.0,
        "ymin": -10.0,
        "xmax": -170.0,
        "ymax": 10.0,
    }


def test_the_derived_block_folds_through_the_one_reader(tmp_path):
    """A 2.0 file's derived ``bbox``/``geometry_types`` equal the fold's answer."""
    import pyarrow.parquet as pq

    from geoparquet_io.core.derive_geo_from_file import _geo_col_meta_from_stats
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection

    path = tmp_path / "v2.parquet"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            "COPY (SELECT * FROM (VALUES (1, ST_GeomFromText('POLYGON Z ((0 0 10, 4 0 12, "
            "4 4 15, 0 4 11, 0 0 10))')), (2, ST_GeomFromText('POINT (10 12)'))) t(id, geometry)) "
            f"TO '{path.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')"
        )
    finally:
        con.close()
    with pq.ParquetFile(str(path)) as pf:
        index = pf.schema_arrow.get_field_index("geometry")
        meta = _geo_col_meta_from_stats(pf, index, "Geometry", str(path))
    assert meta["geometry_types"] == ["Point", "Polygon Z"]
    assert meta["bbox"] == [0.0, 0.0, 10.0, 10.0, 12.0, 15.0]


def test_the_fold_returns_the_spec_interleaved_bbox_with_z():
    stats = aggregate_native_geo_stats(
        [_chunk(0, 0, 4, 4, zmin=10, zmax=15), _chunk(10, 10, 12, 12, zmin=5, zmax=5)]
    )
    assert stats["bbox"] == [0, 0, 5, 12, 12, 15]


def test_one_type_table_names_codes_and_duckdb_strings():
    from geoparquet_io.core import duckdb_metadata, geo_metadata
    from geoparquet_io.core.geo_metadata import geometry_type_name

    assert geometry_type_name(1003) == geometry_type_name("polygon_z") == "Polygon Z"
    assert geometry_type_name("POLYGON ZM") == "Polygon ZM"
    assert geometry_type_name(0) is None
    assert geometry_type_name("triangle") is None
    assert not hasattr(duckdb_metadata, "_format_geo_types")
    assert not hasattr(duckdb_metadata, "_GEOM_TYPE_NAMES")
    assert not hasattr(geo_metadata, "_DUCKDB_TO_SPEC_TYPE")


def test_the_one_fold_wraps_on_the_circle_and_plainly_for_projected_data():
    from geoparquet_io.core.geo_metadata import GeometryStats, merged_geometry_stats

    parts = [
        GeometryStats([170.0, -10.0, -170.0, 10.0], []),
        GeometryStats([175.0, 0, 178.0, 1], []),
    ]
    assert merged_geometry_stats(parts).bbox == [170.0, -10.0, -170.0, 10.0]
    assert merged_geometry_stats(parts, geographic=False).bbox == [170.0, -10.0, 178.0, 10.0]


def test_the_spatial_order_extent_and_the_geodesic_whole_use_the_one_fold():
    from geoparquet_io.core.check_spatial_order import _compute_data_extent

    rows = [
        {"xmin": 170.0, "ymin": -10.0, "xmax": -170.0, "ymax": 10.0},
        {"xmin": 175.0, "ymin": -5.0, "xmax": 178.0, "ymax": 5.0},
    ]
    assert _compute_data_extent(rows) == {
        "xmin": 170.0,
        "ymin": -10.0,
        "xmax": -170.0,
        "ymax": 10.0,
    }
