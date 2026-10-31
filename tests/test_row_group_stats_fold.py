"""One fold of per-row-group geospatial statistics, one policy (#1367 concept 5).

``duckdb_metadata.aggregate_native_geo_stats`` is the one fold. The inspect
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
