"""A merged file is the one that gets bbox-read again and again (#1151).

``gpio extract "staged/*.parquet" merged.parquet`` concatenates many spatially
compact inputs -- per-country or per-UTM-zone files -- into the single file
every later windowed read opens. Whether that file carries *per-row-group*
spatial statistics decides whether those reads prune or rescan: a 90 GB merge
with none of them made twelve downstream shard jobs read 12,764 of 12,764 row
groups apiece.

Two things are pinned here.

The first is that ``--geoparquet-version 2.0`` on the merge itself is a real
substitute for the second whole-file pass the reporting pipeline was paying
for (``extract`` then ``convert geoparquet --geoparquet-version 2.0``, ~30
minutes and a second full write of 108 GB). The one-pass output has to reach
the same discriminating state as the two-pass one -- 2.0, native geometry
logical type, native per-row-group geo statistics that actually bound their own
rows -- or the substitution is not sound. Row-group *boundaries* legitimately
differ between the two (the merge writes what it reads, the convert rewrites),
so the comparison is on those properties, not on bytes.

The second is that a merge that will *not* be prunable says so. ``sort
hilbert`` already tells a user sorting to 1.1 that the sort buys no pushdown
and names ``--geoparquet-version 2.0``; #1151 asks the merge path to advertise
the same way, since a merge is the write that benefits most. The advice is
scoped to the case that is actually unprunable -- more than one input, no
native geo statistics, and no bbox covering column left in the output -- so a
single-file extract and an already-prunable merge stay quiet.

The default is deliberately unchanged: a merge of 1.x inputs still writes 1.1.
Silently promoting it to 2.0 would hand a caller a file older readers cannot
open, which is not a change ``extract`` gets to make on its own.
"""

from __future__ import annotations

import json
import logging

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely

from geoparquet_io.core.duckdb_metadata import get_per_row_group_native_geo_stats
from geoparquet_io.core.extract import extract
from geoparquet_io.core.write_funnels import write_geoparquet_table

# Three spatially disjoint clusters, one per input file, standing in for the
# per-country inputs of the report. Disjoint is the point: it is what makes the
# merged row groups prunable, and what lets a bbox over one cluster prove that
# the other two can be skipped.
CLUSTERS = [(0.0, 0.0), (20.0, 20.0), (-40.0, 10.0)]
ROWS_PER_CLUSTER = 200


def _cluster_table(x0, y0, *, with_bbox_column):
    xs = [x0 + (i % 10) * 0.1 for i in range(ROWS_PER_CLUSTER)]
    ys = [y0 + (i % 10) * 0.1 for i in range(ROWS_PER_CLUSTER)]
    columns = {
        "id": pa.array(range(ROWS_PER_CLUSTER)),
        "geometry": pa.array(
            [shapely.to_wkb(shapely.Point(x, y)) for x, y in zip(xs, ys, strict=True)],
            pa.binary(),
        ),
    }
    if with_bbox_column:
        columns["bbox"] = pa.StructArray.from_arrays(
            [pa.array(xs), pa.array(ys), pa.array(xs), pa.array(ys)],
            names=["xmin", "ymin", "xmax", "ymax"],
        )
    return pa.table(columns)


def _write_inputs(directory, version, *, with_bbox_column):
    """Write one GeoParquet file per cluster, at ``version``.

    ``with_bbox_column`` mirrors the two shapes a staging step produces: the
    reporting pipeline stages "all columns except bbox", while a conventional
    1.1 writer adds the covering struct.
    """
    directory.mkdir(parents=True, exist_ok=True)
    for index, (x0, y0) in enumerate(CLUSTERS):
        write_geoparquet_table(
            _cluster_table(x0, y0, with_bbox_column=with_bbox_column),
            str(directory / f"part{index}.parquet"),
            geometry_column="geometry",
            geoparquet_version=version,
        )
    return str(directory / "part*.parquet")


def _geo_version(path):
    kv = pq.ParquetFile(path).metadata.metadata or {}
    return json.loads(kv[b"geo"])["version"]


@pytest.fixture
def merge_inputs_no_bbox(tmp_path):
    """1.1 inputs with no bbox covering column -- the reported pipeline's shape."""
    return _write_inputs(tmp_path / "staged", "1.1", with_bbox_column=False)


@pytest.fixture
def merge_inputs_with_bbox(tmp_path):
    """1.1 inputs that do carry a bbox covering column."""
    return _write_inputs(tmp_path / "staged_bbox", "1.1", with_bbox_column=True)


class TestOnePassMergeReplacesTheSecondConvertPass:
    """#1151: the merge itself can produce the 2.0 file, in one write."""

    def test_merge_to_2_0_carries_native_per_row_group_geo_statistics(
        self, merge_inputs_no_bbox, tmp_path
    ):
        """The merged file's row groups each bound their own rows, natively."""
        merged = str(tmp_path / "merged.parquet")
        extract(merge_inputs_no_bbox, merged, geoparquet_version="2.0", row_group_rows=256)

        assert _geo_version(merged) == "2.0.0"
        per_rg = get_per_row_group_native_geo_stats(merged, "geometry")
        assert per_rg, "2.0 merge wrote no native per-row-group geo statistics"

    def test_those_statistics_let_a_bbox_read_prune_row_groups(
        self, merge_inputs_no_bbox, tmp_path
    ):
        """A window over one cluster must not intersect every row group."""
        merged = str(tmp_path / "merged.parquet")
        extract(merge_inputs_no_bbox, merged, geoparquet_version="2.0", row_group_rows=256)

        per_rg = get_per_row_group_native_geo_stats(merged, "geometry")
        assert len(per_rg) > 1, "need several row groups for pruning to mean anything"

        # The window around the first cluster only.
        wxmin, wymin, wxmax, wymax = -1.0, -1.0, 2.0, 2.0
        hit = [
            rg
            for rg in per_rg
            if rg["xmin"] <= wxmax
            and rg["xmax"] >= wxmin
            and rg["ymin"] <= wymax
            and rg["ymax"] >= wymin
        ]
        assert 0 < len(hit) < len(per_rg), (
            f"bbox read prunes nothing: {len(hit)} of {len(per_rg)} row groups intersect"
        )

    def test_one_pass_matches_extract_then_convert(self, merge_inputs_no_bbox, tmp_path):
        """The one-pass merge reaches the same state as the two-pass pipeline."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        one_pass = str(tmp_path / "one_pass.parquet")
        extract(merge_inputs_no_bbox, one_pass, geoparquet_version="2.0", row_group_rows=256)

        staged = str(tmp_path / "staged_merge.parquet")
        extract(merge_inputs_no_bbox, staged, row_group_rows=256)
        two_pass = str(tmp_path / "two_pass.parquet")
        convert_to_geoparquet(
            staged, two_pass, geoparquet_version="2.0", skip_hilbert=True, row_group_rows=256
        )

        assert _geo_version(one_pass) == _geo_version(two_pass) == "2.0.0"

        one_schema = pq.ParquetFile(one_pass).schema_arrow
        two_schema = pq.ParquetFile(two_pass).schema_arrow
        assert one_schema.names == two_schema.names

        # Both must carry native geo statistics, and both must describe the same
        # data overall. Row-group boundaries differ by construction, so the
        # comparison is over the union of each file's per-row-group bounds.
        def union(path):
            per_rg = get_per_row_group_native_geo_stats(path, "geometry")
            assert per_rg, f"{path} carries no native per-row-group geo statistics"
            return (
                round(min(r["xmin"] for r in per_rg), 6),
                round(min(r["ymin"] for r in per_rg), 6),
                round(max(r["xmax"] for r in per_rg), 6),
                round(max(r["ymax"] for r in per_rg), 6),
            )

        assert union(one_pass) == union(two_pass)


class TestAnUnprunableMergeSaysSo:
    """#1151: advertise 2.0 on the merge, the way ``sort hilbert`` does."""

    ADVICE = "--geoparquet-version 2.0"

    def test_merge_with_no_spatial_statistics_advises_2_0(self, merge_inputs_no_bbox, tmp_path):
        merged = str(tmp_path / "merged.parquet")
        with caplog_warnings() as caplog:
            extract(merge_inputs_no_bbox, merged)
        assert self.ADVICE in caplog.text
        assert "row group" in caplog.text.lower()

    def test_the_advice_does_not_change_the_version_written(self, merge_inputs_no_bbox, tmp_path):
        """Default behaviour is unchanged: a 1.x merge still writes 1.1."""
        merged = str(tmp_path / "merged.parquet")
        extract(merge_inputs_no_bbox, merged)
        assert _geo_version(merged) == "1.1.0"
        assert get_per_row_group_native_geo_stats(merged, "geometry") == []

    def test_a_2_0_merge_is_not_advised(self, merge_inputs_no_bbox, tmp_path):
        merged = str(tmp_path / "merged.parquet")
        with caplog_warnings() as caplog:
            extract(merge_inputs_no_bbox, merged, geoparquet_version="2.0")
        assert self.ADVICE not in caplog.text

    def test_a_merge_that_keeps_a_bbox_covering_is_not_advised(
        self, merge_inputs_with_bbox, tmp_path
    ):
        """A covering column carries ordinary per-row-group stats; that prunes too."""
        merged = str(tmp_path / "merged.parquet")
        with caplog_warnings() as caplog:
            extract(merge_inputs_with_bbox, merged)
        assert self.ADVICE not in caplog.text

    def test_a_merge_that_excludes_its_bbox_covering_is_advised(
        self, merge_inputs_with_bbox, tmp_path
    ):
        """Dropping the covering leaves the merge with nothing to prune on."""
        merged = str(tmp_path / "merged.parquet")
        with caplog_warnings() as caplog:
            extract(merge_inputs_with_bbox, merged, exclude_cols="bbox")
        assert self.ADVICE in caplog.text

    def test_a_merge_with_no_geometry_at_all_is_not_advised(self, tmp_path):
        """2.0 is a verified no-op for plain Parquet, so advising it misleads.

        Merging non-spatial files writes no ``geo`` key, and there is nothing
        for ``geo_bbox`` statistics to describe. The advice used to fire anyway,
        sending the reader after a flag that cannot help them.
        """
        src = tmp_path / "plain"
        src.mkdir()
        for name in ("part0", "part1"):
            pq.write_table(
                pa.table({"id": [1, 2, 3], "v": ["a", "b", "c"]}), str(src / f"{name}.parquet")
            )
        merged = str(tmp_path / "merged.parquet")
        with caplog_warnings() as caplog:
            extract(str(src), merged)
        assert self.ADVICE not in caplog.text

    def test_a_merge_that_excludes_its_geometry_is_not_advised(
        self, merge_inputs_with_bbox, tmp_path
    ):
        """Dropping the geometry leaves a plain Parquet output (#1163)."""
        merged = str(tmp_path / "merged.parquet")
        with caplog_warnings() as caplog:
            extract(merge_inputs_with_bbox, merged, exclude_cols="geometry,bbox")
        assert self.ADVICE not in caplog.text

    def test_a_single_file_extract_is_not_advised(self, tmp_path, merge_inputs_no_bbox):
        """The advice is about merges; a one-file extract is left alone."""
        single = merge_inputs_no_bbox.replace("part*.parquet", "part0.parquet")
        merged = str(tmp_path / "single.parquet")
        with caplog_warnings() as caplog:
            extract(single, merged)
        assert self.ADVICE not in caplog.text

    def test_a_glob_matching_one_file_is_not_advised(self, tmp_path, merge_inputs_no_bbox):
        """A glob that resolves to a single file is no more a merge than a path.

        This is the case a "does it look like a partition path?" test alone
        cannot tell from a real merge -- only counting the matches can.
        """
        one_match = merge_inputs_no_bbox.replace("part*.parquet", "part0*.parquet")
        merged = str(tmp_path / "one_match.parquet")
        with caplog_warnings() as caplog:
            extract(one_match, merged)
        assert self.ADVICE not in caplog.text


class caplog_warnings:  # noqa: N801 - a context manager, used as one
    """Capture ``geoparquet_io`` warnings for the duration of a block."""

    def __enter__(self):
        self._records = []
        self._handler = _ListHandler(self._records)
        self._logger = logging.getLogger("geoparquet_io")
        self._previous = self._logger.level
        self._logger.setLevel(logging.WARNING)
        self._logger.addHandler(self._handler)
        return self

    def __exit__(self, *exc):
        self._logger.removeHandler(self._handler)
        self._logger.setLevel(self._previous)
        return False

    @property
    def text(self):
        return "\n".join(self._records)


class _ListHandler(logging.Handler):
    def __init__(self, sink):
        super().__init__()
        self._sink = sink

    def emit(self, record):
        self._sink.append(record.getMessage())
