"""Rebuilding a column's ``geo`` entry from native geo statistics.

``core/derive_geo_from_file._geo_col_meta_from_stats`` folds every row group's
geospatial statistics into one column entry. The single-row-group case runs
whenever DuckDB writes an M/ZM file; the folding itself -- a row group with no
statistics, a bbox that has to widen, a Z range that has to widen, an unknown
type code -- never had a fixture. Stand-ins for the pyarrow metadata objects
exercise it directly.
"""

from __future__ import annotations

from types import SimpleNamespace

from geoparquet_io.core.derive_geo_from_file import (
    _geo_col_meta_from_stats,
    _geography_edges_from_logical,
)


def _stats(types, xmin, ymin, xmax, ymax, zmin=None, zmax=None):
    return SimpleNamespace(
        geospatial_types=types, xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax, zmin=zmin, zmax=zmax
    )


class _FakeParquetFile:
    """Just enough of ``pq.ParquetFile`` for ``_geo_col_meta_from_stats``: one column."""

    def __init__(self, per_row_group):
        self.metadata = SimpleNamespace(
            num_row_groups=len(per_row_group),
            row_group=lambda index: SimpleNamespace(
                column=lambda _col: SimpleNamespace(geo_statistics=per_row_group[index])
            ),
        )


def test_type_codes_map_to_names_and_unknown_codes_are_dropped():
    """The fold (``duckdb_metadata.aggregate_native_geo_stats``) names the codes."""
    pf = _FakeParquetFile([_stats([3002, 1, 9999, 4001, 0], 0.0, 0.0, 1.0, 1.0)])
    meta = _geo_col_meta_from_stats(pf, 0, "Geometry", "unused.parquet")
    assert meta["geometry_types"] == ["LineString ZM", "Point"]


def test_geography_edges_default_to_spherical_and_read_a_declared_algorithm():
    assert _geography_edges_from_logical("Geometry(crs=EPSG:4326)") is None
    assert _geography_edges_from_logical("Geography(crs=EPSG:4326)") == "spherical"
    assert _geography_edges_from_logical("Geography(algorithm=VINCENTY)") == "vincenty"


def test_stats_fold_across_row_groups_widening_bbox_and_z_range(tmp_path):
    """Three row groups: one with no statistics at all, two that each extend the other."""
    pf = _FakeParquetFile(
        [
            _stats([1001], 0.0, 0.0, 1.0, 1.0, zmin=5.0, zmax=6.0),
            None,
            _stats([1001, 9999], -2.0, 0.5, 0.5, 3.0, zmin=1.0, zmax=9.0),
        ]
    )

    meta = _geo_col_meta_from_stats(pf, 0, "Geography", str(tmp_path / "unused.parquet"))

    assert meta["geometry_types"] == ["Point Z"], "9999 is not a type; it must not appear"
    # RFC 7946 order with the Z range folded in: xmin, ymin, zmin, xmax, ymax, zmax.
    assert meta["bbox"] == [-2.0, 0.0, 1.0, 1.0, 3.0, 9.0]
    assert meta["edges"] == "spherical"
    assert "crs" not in meta, "a logical type naming no CRS writes no crs key"


def test_no_statistics_anywhere_gives_types_only(tmp_path):
    pf = _FakeParquetFile([None, None])

    meta = _geo_col_meta_from_stats(pf, 0, "Geometry", str(tmp_path / "unused.parquet"))

    assert meta == {"encoding": "WKB", "geometry_types": []}


def test_derive_v2_geo_metadata_describes_a_native_geography_column(tmp_path):
    """A file whose only geo column is a native GEOGRAPHY (no ``geo`` key) gets a block.

    Pins the logical-type test in ``derive_v2_geo_metadata`` itself: a check that
    knew only ``Geometry`` would find no geo column here and return None.
    """
    import geoarrow.pyarrow as ga
    import pyarrow as pa
    import pyarrow.parquet as pq

    from geoparquet_io.core.derive_geo_from_file import derive_v2_geo_metadata

    wkb = ga.as_wkb(["POINT (1 2)", "POINT (3 4)"]).storage
    geography = ga.wkb().with_edge_type(ga.EdgeType.SPHERICAL).wrap_array(wkb)
    path = tmp_path / "geography.parquet"
    pq.write_table(pa.table({"id": [1, 2], "geometry": geography}), str(path))
    logical = str(pq.ParquetFile(str(path)).metadata.schema.column(1).logical_type)
    assert logical == "Geography(crs=, algorithm=spherical)"

    block = derive_v2_geo_metadata(str(path))

    assert block is not None
    assert block["primary_column"] == "geometry"
    assert block["columns"]["geometry"]["edges"] == "spherical"
