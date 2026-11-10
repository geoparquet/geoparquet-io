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
from tests._defrag_fixtures import (
    footer_geo,
    footer_kv,
    geo_block,
    point_wkb,
    polygon_wkb,
    write_with_geo,
)

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


# ---------------------------------------------------------------------------
# #1146 -- the streamed simplify write cuts its own row groups, so it has to
# ask the same owner for the layout the two write funnels already ask for.
# ---------------------------------------------------------------------------

_GROUP = 2048


def _simplify_source(tmp_path, position):
    """A WKB source whose unlocated rows sit at ``position``; ``id`` is row order."""
    from shapely import Point, to_wkb

    located = [polygon_wkb([(x, 0.0), (x + 0.5, 0.0), (x + 0.25, 0.5)]) for x in range(_GROUP)]
    # NULL and EMPTY are both unlocated; a null-count-only check would miss EMPTY.
    unlocated = [None, to_wkb(Point())]
    geoms = {
        # The issue's shape: the located rows are an exact multiple of the group.
        "trailing": located + located + unlocated + [None],
        # More unlocated rows than one group holds.
        "trailing-run": located + unlocated * _GROUP,
        "middle": located + unlocated * (_GROUP // 2) + located,
        # A full group of unlocated rows ahead of the located ones, so the
        # writer really does flush a leading group with no located row and the
        # layout's leading-merge branch has something to fold.
        "leading": unlocated * (_GROUP // 2) + located + located,
        # Nothing to fold in: the requested layout must survive untouched.
        "none": located + located,
        # No layout can give a group a located row when the file has none.
        "all": unlocated * _GROUP,
    }[position]
    table = pa.table(
        {"id": pa.array(range(len(geoms)), pa.int64()), "geometry": pa.array(geoms, pa.binary())}
    )
    block = geo_block(geometry_types=["Polygon"])
    return write_with_geo(tmp_path / "source.parquet", table, block), geoms


@pytest.mark.parametrize(
    "position", ["trailing", "trailing-run", "middle", "leading", "none", "all"]
)
def test_streamed_simplify_leaves_no_row_group_without_a_located_row(tmp_path, position):
    """``gpio process simplify`` streams its own row groups, and must still fold.

    ``sort hilbert`` / ``convert geoparquet`` put the NULL and EMPTY rows last,
    so a simplify of their output cuts a trailing group with no bbox covering
    at all when the located count is an exact multiple of the group size.
    """
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    src, geoms = _simplify_source(tmp_path, position)
    out = tmp_path / "out.parquet"
    args = ["process", "simplify", str(src), str(out), "--tolerance", "0.01"]
    result = CliRunner().invoke(cli, [*args, "--row-group-size", str(_GROUP)])
    assert result.exit_code == 0, f"exit {result.exit_code}: {result.exception!r}\n{result.output}"
    pf = pq.ParquetFile(out)
    assert pq.read_table(out)["id"].to_pylist() == list(range(len(geoms)))
    located = [
        geometry_stats_of(pf.read_row_group(i, columns=["geometry"]), "geometry").bbox is not None
        for i in range(pf.num_row_groups)
    ]
    sizes = [pf.metadata.row_group(i).num_rows for i in range(pf.num_row_groups)]
    if position == "all":
        # Every row is unlocated: the groups the writer cut are left alone.
        assert located == [False] * pf.num_row_groups, sizes
    else:
        assert all(located), (sizes, located)
    if position == "none":
        assert sizes == [_GROUP, _GROUP]
    if position == "leading":
        # The leading unlocated group joins the first located one.
        assert sizes == [2 * _GROUP, _GROUP], located
    if position == "trailing":
        # The fold is a re-encode, not a footer patch, so it re-derives every
        # footer key. A fold that dropped the `geo` block would still satisfy
        # every assertion above, so pin the footer the rewrite hands back.
        keys = set(footer_kv(out))
        assert b"geo" in keys, keys
        block = footer_geo(out)
        assert block["primary_column"] == "geometry"
        assert block["columns"]["geometry"]["bbox"]


def test_located_repair_reads_at_the_callers_row_group_size(tmp_path, monkeypatch):
    """The repair must re-encode at the caller's group size, not the facade default.

    ``_rewrite_parquet`` sizes its *read* batches from the ``WriteOptions`` it is
    handed, so a repair that forgets the caller's ``--row-group-size`` re-reads a
    2,048-row file in 49,152-row batches -- undoing exactly the memory the small
    group was asked for (measured 87.5 MB against 9.4 MB).
    """
    from geoparquet_io.core import write_funnels

    geoms = [point_wkb(1.0, 2.0)] * (2 * _GROUP) + [None] * _GROUP
    table = pa.table(
        {"id": pa.array(range(len(geoms)), pa.int64()), "geometry": pa.array(geoms, pa.binary())}
    )
    out = write_with_geo(tmp_path / "out.parquet", table, geo_block(), row_group_size=_GROUP)

    seen: list[int | None] = []
    rewrite = write_funnels._rewrite_parquet

    def record(*args, **kwargs):
        seen.append(kwargs["write"].row_group_rows)
        return rewrite(*args, **kwargs)

    monkeypatch.setattr(write_funnels, "_rewrite_parquet", record)
    write_funnels.ensure_located_row_groups(str(out), "geometry", None, None, row_group_rows=_GROUP)

    assert seen == [_GROUP]
    pf = pq.ParquetFile(out)
    assert [pf.metadata.row_group(i).num_rows for i in range(pf.num_row_groups)] == [
        _GROUP,
        2 * _GROUP,
    ]
