"""The layout guarantee preserves row order, with an all-degenerate exception.

The layout is ``parquet_writer.located_row_group_layout``'s; every position of
the unlocated rows runs on the default strategy, and each other strategy shows
it takes the owner's layout on one position.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import geoparquet_io as gpio
from geoparquet_io.core.geo_metadata import geometry_stats_of
from tests._defrag_fixtures import geo_block, point_wkb, write_with_geo

_CELLS = [("duckdb-kv", p) for p in ("leading", "middle", "trailing", "all")] + [
    (s, "middle") for s in ("in-memory", "streaming", "disk-rewrite")
]


@pytest.mark.parametrize(
    ("strategy", "position"),
    [pytest.param(s, p, id=f"{p}-{s}") for s, p in _CELLS],
)
def test_table_write_merges_unlocated_groups_without_reordering(tmp_path, strategy, position):
    located = [point_wkb(1, 2)] * 2048
    # EMPTY is also unlocated; a non-null-only check would miss this group.
    from shapely import Point, to_wkb

    unlocated = [None, to_wkb(Point())] * 1024
    geoms = {
        "leading": unlocated + located + located,
        "middle": located + unlocated + located,
        "trailing": located + located + unlocated,
        "all": unlocated + unlocated,
    }[position]
    table = pa.table({"id": range(len(geoms)), "geometry": pa.array(geoms, pa.binary())})
    src = write_with_geo(tmp_path / "source.parquet", table, geo_block())
    out = tmp_path / "out.parquet"
    gpio.read(src).write(out, write_strategy=strategy, row_group_rows=2048)
    pf = pq.ParquetFile(out)
    assert pq.read_table(out)["id"].to_pylist() == list(range(len(geoms)))
    for index in range(pf.num_row_groups):
        stats = geometry_stats_of(pf.read_row_group(index), "geometry")
        assert (stats.bbox is None) == (position == "all")
    if position == "all":
        # No layout can place a located row into a group when none exists.
        assert pf.num_row_groups == 2


@pytest.mark.parametrize("strategy", ["duckdb-kv", "in-memory", "streaming", "disk-rewrite"])
def test_spherical_layout_retains_footer_statistics(tmp_path, strategy):
    from shapely import from_wkt, to_wkb

    line = to_wkb(from_wkt("LINESTRING (-30 60, 30 60)"))
    table = pa.table({"id": range(4096), "geometry": [line] * 2048 + [None] * 2048})
    block = geo_block()
    block["columns"]["geometry"]["edges"] = "spherical"
    block["columns"]["geometry"]["geometry_types"] = ["LineString"]
    src = write_with_geo(tmp_path / "source.parquet", table, block)
    out = tmp_path / "out.parquet"
    gpio.read(src).write(
        out, write_strategy=strategy, geoparquet_version="2.0", row_group_rows=2048
    )
    metadata = pq.read_metadata(out)
    assert metadata.num_row_groups == 1
    assert pq.read_table(out)["id"].to_pylist() == list(range(4096))
    stats = metadata.row_group(0).column(1).geo_statistics
    assert stats is not None
    assert stats.ymax == pytest.approx(63.43494982292201)
