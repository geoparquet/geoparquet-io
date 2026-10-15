"""gpio's GEOGRAPHY outputs carry a geodesic bounding box again (#1272).

#1254 dropped the planar bounding box when it retyped a column GEOGRAPHY --
safe, but it gave up row-group pruning on every GEOGRAPHY column gpio writes.
These tests hold the write paths to the replacement: every row group of a
retyped column carries a box that bounds the geodesic edges, the ``geo``
block's file-level ``bbox`` says the same thing, and a planar output's
statistics stay exactly what DuckDB wrote (control).
"""

from __future__ import annotations

import json
import logging
import math
from pathlib import Path

# Keeps pyarrow materialising GEOGRAPHY columns as GeoArrow extension types.
import geoarrow.pyarrow  # noqa: F401
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.convert import convert_to_geoparquet
from tests.native_geo_probes import geo_block, native_geo_edges, spec_problems

BULGED_YMAX = 63.4349  # LINESTRING(-30 60, 30 60)'s great-circle peak


def _row_group_boxes(path, column="geometry") -> list:
    metadata = pq.read_metadata(str(path))
    index = [metadata.schema.column(i).path for i in range(metadata.num_columns)].index(column)
    boxes = []
    for group in range(metadata.num_row_groups):
        stats = metadata.row_group(group).column(index).geo_statistics
        if stats is None or stats.xmin is None:
            boxes.append(None)
        else:
            boxes.append((stats.xmin, stats.ymin, stats.xmax, stats.ymax))
    return boxes


def _spherical_input(tmp_path, wkts: list[str], geometry_types: list[str]) -> Path:
    """A 1.1 WKB file whose ``geo`` block declares spherical edges."""
    import shapely

    table = pa.table(
        {
            "id": list(range(len(wkts))),
            "geometry": pa.array([shapely.to_wkb(shapely.from_wkt(w)) for w in wkts], pa.binary()),
        }
    )
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": geometry_types,
                "edges": "spherical",
            }
        },
    }
    path = tmp_path / "in.parquet"
    pq.write_table(table.replace_schema_metadata({"geo": json.dumps(geo)}), str(path))
    return path


def test_a_high_latitude_edge_gets_a_bulged_row_group_box(tmp_path):
    source = _spherical_input(tmp_path, ["LINESTRING (-30 60, 30 60)"], ["LineString"])
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(source), str(out), skip_hilbert=True, geoparquet_version="2.0")

    assert native_geo_edges(out) == ("Geography", "EdgeType.SPHERICAL")
    boxes = _row_group_boxes(out)
    assert boxes != [None], "the GEOGRAPHY row group lost its bounding box"
    xmin, ymin, xmax, ymax = boxes[0]
    assert ymax == pytest.approx(BULGED_YMAX, abs=0.01), "the box does not cover the edge bulge"
    assert ymin == pytest.approx(60.0, abs=1e-6)
    assert (xmin, xmax) == (pytest.approx(-30.0), pytest.approx(30.0))


def test_the_geo_block_bbox_covers_the_bulge_too(tmp_path):
    source = _spherical_input(tmp_path, ["LINESTRING (-30 60, 30 60)"], ["LineString"])
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(source), str(out), skip_hilbert=True, geoparquet_version="2.0")

    bbox = geo_block(out)["columns"]["geometry"]["bbox"]
    assert bbox[3] == pytest.approx(BULGED_YMAX, abs=0.01)
    assert bbox[0] == pytest.approx(-30.0)


def test_an_antimeridian_crossing_geometry_wraps_the_box(tmp_path):
    source = _spherical_input(tmp_path, ["LINESTRING (170 10, -170 15)"], ["LineString"])
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(source), str(out), skip_hilbert=True, geoparquet_version="2.0")

    xmin, _, xmax, _ = _row_group_boxes(out)[0]
    assert xmin > xmax, f"expected an antimeridian wrap, got [{xmin}, {xmax}]"
    assert (xmin, xmax) == (pytest.approx(170.0), pytest.approx(-170.0))
    bbox = geo_block(out)["columns"]["geometry"]["bbox"]
    assert bbox[0] > bbox[2], "the geo block bbox does not wrap"


def test_a_pole_enclosing_ring_reaches_the_pole(tmp_path):
    """The funnel's own patch bounds a pole ring to the pole.

    Driven at `_apply_nonplanar_edges`, the unit every write path ends in: a
    ring that winds every longitude is *degenerate on the plane*, so gpio's
    planar geometry repair rewrites it before any end-to-end write reaches
    this code (pre-existing, independent of #1272).
    """
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
    from geoparquet_io.core.write_funnels import _apply_nonplanar_edges

    path = tmp_path / "ring.parquet"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            "COPY (SELECT ST_GeomFromText('POLYGON ((0 80, 90 80, 180 80, -90 80, 0 80))') "
            f"AS geometry) TO {sql_path(str(path))} (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')"
        )
    finally:
        con.close()

    _apply_nonplanar_edges({"geometry": "spherical"}, str(path))

    assert native_geo_edges(path) == ("Geography", "EdgeType.SPHERICAL")
    xmin, _, xmax, ymax = _row_group_boxes(path)[0]
    assert ymax == 90.0
    assert (xmin, xmax) == (-180.0, 180.0)
    bbox = geo_block(path)["columns"]["geometry"]["bbox"]
    assert bbox[3] == 90.0


def test_the_output_still_passes_check_spec(tmp_path):
    source = _spherical_input(
        tmp_path,
        ["LINESTRING (-30 60, 30 60)", "LINESTRING (170 10, -170 15)"],
        ["LineString"],
    )
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(source), str(out), skip_hilbert=True, geoparquet_version="2.0")

    assert spec_problems(out) == []
    assert spec_problems(out, status="warning") == []


def test_a_planar_output_keeps_duckdbs_statistics(tmp_path):
    """Control: no GEOGRAPHY, no geodesic recompute -- the vertex box stands."""
    import shapely

    table = pa.table(
        {
            "id": [0],
            "geometry": pa.array(
                [shapely.to_wkb(shapely.from_wkt("LINESTRING (-30 60, 30 60)"))], pa.binary()
            ),
        }
    )
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["LineString"]}},
    }
    source = tmp_path / "planar.parquet"
    pq.write_table(table.replace_schema_metadata({"geo": json.dumps(geo)}), str(source))
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(source), str(out), skip_hilbert=True, geoparquet_version="2.0")

    boxes = _row_group_boxes(out)
    assert boxes == [(-30.0, 60.0, 30.0, 60.0)], "a planar box moved"


def test_the_mzm_merged_patch_carries_the_geodesic_box(tmp_path):
    """The #1270 single patch and the #1272 box land together."""
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
    from geoparquet_io.core.parquet_footer import patch_footer

    source = tmp_path / "xym.parquet"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            "COPY (SELECT ST_GeomFromText('LINESTRING M (-30 60 1, 30 60 2)') AS geometry) "
            f"TO {sql_path(str(source))} (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')"
        )
    finally:
        con.close()
    patch_footer(str(source), geography={"geometry": "spherical"})
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(source), str(out), skip_hilbert=True, geoparquet_version="2.0")

    assert native_geo_edges(out) == ("Geography", "EdgeType.SPHERICAL")
    assert geo_block(out)["columns"]["geometry"].get("edges") == "spherical"
    boxes = _row_group_boxes(out)
    assert boxes != [None]
    assert boxes[0][3] == pytest.approx(BULGED_YMAX, abs=0.01)


def _declare(source: Path, **column_meta) -> None:
    """Rewrite ``source``'s geo block with extra per-column keys (version 2.0.0)."""
    table = pq.read_table(str(source))
    meta = dict(table.schema.metadata)
    geo = json.loads(meta[b"geo"])
    geo["version"] = "2.0.0"
    geo["columns"]["geometry"].update(column_meta)
    meta[b"geo"] = json.dumps(geo).encode()
    pq.write_table(table.replace_schema_metadata(meta), str(source))


def test_an_ellipsoidal_algorithm_gets_its_geodesic_box(tmp_path):
    """The bound is the geodesic's own vertex, not a fixed pad (#1286 review)."""
    from pyproj import Geod

    source = _spherical_input(tmp_path, ["LINESTRING (-30 60, 30 60)"], ["LineString"])
    _declare(source, edges="karney")
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(source), str(out), skip_hilbert=True, geoparquet_version="2.0")

    assert native_geo_edges(out) == ("Geography", "EdgeType.KARNEY")
    _, ymin, _, ymax = _row_group_boxes(out)[0]
    true_max = max(lat for _, lat in Geod(ellps="WGS84").npts(-30, 60, 30, 60, 5000))
    assert true_max <= ymax <= true_max + 1e-4
    assert ymin == 60.0


def test_a_near_antipodal_karney_edge_is_covered(tmp_path):
    """(0 0)->(179.5 0) on WGS84 reaches 34 N; the old 0.2 degree pad wrote +-0.2."""
    source = _spherical_input(tmp_path, ["LINESTRING (0 0, 179.5 0)"], ["LineString"])
    _declare(source, edges="karney")
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(source), str(out), skip_hilbert=True, geoparquet_version="2.0")

    box = _row_group_boxes(out)[0]
    assert box is None or (box[1] <= 0.0 and box[3] >= 34.2)


def _duckdb_v2(path: Path, wkt: str) -> None:
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path

    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            f"COPY (SELECT ST_GeomFromText('{wkt}') AS geometry) "
            f"TO {sql_path(str(path))} (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')"
        )
    finally:
        con.close()


@pytest.mark.parametrize(
    "orientation, expected",
    [(None, (0.0, 0.0, 1.0)), ("counterclockwise", (-180.0, -90.0, 180.0))],
)
def test_the_declared_orientation_decides_the_interior(tmp_path, orientation, expected):
    """A clockwise square is the square unoriented, and the rest of the globe if ccw."""
    from geoparquet_io.core.parquet_footer import patch_footer_kv
    from geoparquet_io.core.write_funnels import _apply_nonplanar_edges

    path = tmp_path / "square.parquet"
    _duckdb_v2(path, "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))")
    if orientation:
        geo = geo_block(path)
        geo["columns"]["geometry"]["orientation"] = orientation
        patch_footer_kv(str(path), {"geo": json.dumps(geo)})

    _apply_nonplanar_edges({"geometry": "spherical"}, str(path))

    xmin, ymin, xmax, _ = _row_group_boxes(path)[0]
    assert (xmin, ymin, xmax) == pytest.approx(expected, abs=1e-6)


def test_a_non_finite_vertex_drops_the_box_with_a_warning(tmp_path, caplog):
    """NaN never reaches the statistics or the geo JSON; the drop is said out loud."""
    import struct

    def line(points):
        return struct.pack("<BII", 1, 2, len(points)) + b"".join(
            struct.pack("<dd", x, y) for x, y in points
        )

    table = pa.table(
        {
            "id": [0, 1],
            "geometry": pa.array(
                [line([(0, 0), (float("nan"), 10), (20, 20)]), line([(-30, 60), (30, 60)])],
                pa.binary(),
            ),
        }
    )
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {"encoding": "WKB", "geometry_types": ["LineString"], "edges": "spherical"}
        },
    }
    source = tmp_path / "nan.parquet"
    pq.write_table(table.replace_schema_metadata({"geo": json.dumps(geo)}), str(source))
    out = tmp_path / "out.parquet"
    with caplog.at_level(logging.WARNING):
        convert_to_geoparquet(str(source), str(out), skip_hilbert=True, geoparquet_version="2.0")

    raw = pq.read_metadata(str(out)).metadata[b"geo"].decode()
    assert "NaN" not in raw and "Infinity" not in raw
    json.loads(raw)  # strict JSON
    for box in _row_group_boxes(out):
        assert box is None or all(math.isfinite(v) for v in box)
    assert "bbox" not in geo_block(out)["columns"]["geometry"]
    assert any("bounding box" in r.getMessage() for r in caplog.records)


def test_a_spherical_1x_output_gets_a_geodesic_geo_bbox(tmp_path):
    """1.x has no GEOGRAPHY type, but its geo block bbox must still cover the edges."""
    source = _spherical_input(tmp_path, ["LINESTRING (-30 60, 30 60)"], ["LineString"])
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(source), str(out), skip_hilbert=True, geoparquet_version="1.1")

    column = geo_block(out)["columns"]["geometry"]
    assert column.get("edges") == "spherical"
    assert column["bbox"][3] == pytest.approx(BULGED_YMAX, abs=0.01)


def test_a_six_element_geo_bbox_keeps_its_z_bounds():
    from geoparquet_io.core.write_funnels import _geo_block_updates

    geo_meta = {
        "version": "2.0.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["LineString Z"],
                "bbox": [-30.0, 60.0, 5.0, 30.0, 60.0, 7.0],
            }
        },
    }
    updates = _geo_block_updates(geo_meta, {}, {"geometry": (-30.0, 60.0, 30.0, 63.44)})

    assert updates, "a changed bbox must mark the block for writing"
    assert geo_meta["columns"]["geometry"]["bbox"] == [-30.0, 60.0, 5.0, 30.0, 63.44, 7.0]


def test_an_unboundable_column_still_drops_the_box(tmp_path, monkeypatch, caplog):
    """When nothing can bound the edges, #1254's drop is the fallback -- with a warning."""
    from geoparquet_io.core import write_funnels

    def boom(*args, **kwargs):
        raise OSError("cannot read the column back")

    monkeypatch.setattr(write_funnels, "geography_statistics", boom)
    source = _spherical_input(tmp_path, ["LINESTRING (-30 60, 30 60)"], ["LineString"])
    out = tmp_path / "out.parquet"
    with caplog.at_level(logging.WARNING):
        convert_to_geoparquet(str(source), str(out), skip_hilbert=True, geoparquet_version="2.0")

    assert native_geo_edges(out) == ("Geography", "EdgeType.SPHERICAL")
    assert _row_group_boxes(out) == [None]
    assert "bbox" not in geo_block(out)["columns"]["geometry"], "a planar block bbox stayed"
    assert any("cannot read the column back" in r.getMessage() for r in caplog.records)


def test_running_out_of_memory_is_not_swallowed(tmp_path, monkeypatch):
    from geoparquet_io.core import write_funnels

    def oom(*args, **kwargs):
        raise MemoryError

    monkeypatch.setattr(write_funnels, "geography_statistics", oom)
    with pytest.raises(MemoryError):
        write_funnels._geodesic_bounds("unused.parquet", {"geometry": "spherical"}, None)


def test_an_unbounded_column_loses_its_planar_block_bbox():
    from geoparquet_io.core.write_funnels import _geo_block_updates

    geo_meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "bbox": [-30.0, 60.0, 30.0, 60.0]}},
    }
    updates = _geo_block_updates(geo_meta, {}, {}, unbounded={"geometry", "absent"})
    assert updates
    assert "bbox" not in geo_meta["columns"]["geometry"]


def test_block_bbox_targets_skip_projected_and_planar_columns():
    from geoparquet_io.core.write_funnels import _block_bbox_targets

    geo_meta = {
        "columns": {
            "a": {"encoding": "WKB"},
            "b": {"encoding": "WKB", "crs": {"type": "ProjectedCRS", "name": "UTM zone 33N"}},
            "c": {"encoding": "WKB"},
            "d": "not a dict",
        }
    }
    edges = {"a": "spherical", "b": "spherical", "c": "planar", "d": "spherical", "e": "karney"}
    assert _block_bbox_targets(geo_meta, edges, {}) == {"a": "spherical"}
    assert _block_bbox_targets(geo_meta, edges, {"a": "spherical"}) == {}
