"""The bbox query-plan preview: which row groups would a bbox read touch (#1310).

The contract under test, in order of importance:

1. Fidelity: on a file with bbox-covering row-group statistics, every row an
   actual DuckDB filtered read returns comes from a row group the preview
   predicted, and a group the preview left out contributes no rows.
2. Honesty: a file with no usable per-row-group spatial statistics gets an
   explicit "no estimate", never numbers.
3. Embeddability: the module imports without pulling duckdb into the process
   (tracker #1311's embeddable set).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.preview import (
    BboxFilterPreview,
    RowGroupEstimate,
    _matching_row_groups,
    preview_bbox_filter,
)

DATA = Path(__file__).parent / "data"
PLACES = DATA / "places_test.parquet"
#: No bbox column and no native geo statistics: the "no estimate" file.
BUILDINGS = DATA / "buildings_test.parquet"
#: GeoParquet 2.0 with native geo statistics and no bbox column.
GPQ2 = DATA / "fields_gpq2_crs84_zstd.parquet"

WORLD = (-180.0, -90.0, 180.0, 90.0)
NOWHERE = (200.0, 200.0, 201.0, 201.0)


@pytest.fixture(scope="module")
def sorted_places(tmp_path_factory) -> str:
    """places_test rewritten x-sorted in 100-row groups, so groups have distinct extents."""
    table = pq.read_table(PLACES)
    order = pc.sort_indices(pc.struct_field(table["bbox"], "xmin"))
    out = tmp_path_factory.mktemp("preview") / "places_sorted.parquet"
    pq.write_table(table.take(order), out, row_group_size=100)
    return str(out)


def _group_of_row(path: str, row_number: int) -> int:
    """Which row group a 0-based file row number lives in."""
    metadata = pq.ParquetFile(path).metadata
    start = 0
    for group in range(metadata.num_row_groups):
        start += metadata.row_group(group).num_rows
        if row_number < start:
            return group
    raise AssertionError(f"row {row_number} beyond the file's {start} rows")


def _rows_duckdb_returns(path: str, bbox: tuple[float, float, float, float]) -> list[int]:
    """File row numbers an actual DuckDB bbox-filtered read of ``path`` returns."""
    import duckdb

    from geoparquet_io.core.duckdb_utils import sql_path

    xmin, ymin, xmax, ymax = bbox
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"""
            SELECT file_row_number
            FROM read_parquet({sql_path(path)}, file_row_number=true)
            WHERE bbox.xmin <= ? AND bbox.xmax >= ?
              AND bbox.ymin <= ? AND bbox.ymax >= ?
            """,
            [xmax, xmin, ymax, ymin],
        ).fetchall()
    finally:
        con.close()
    return [row[0] for row in rows]


def _query_bbox_of_rows(path: str, offset: int, length: int) -> tuple[float, float, float, float]:
    """The bbox of a run of rows in the (sorted) file, to use as the query bbox."""
    bbox = pq.read_table(path).slice(offset, length)["bbox"]
    return (
        pc.min(pc.struct_field(bbox, "xmin")).as_py(),
        pc.min(pc.struct_field(bbox, "ymin")).as_py(),
        pc.max(pc.struct_field(bbox, "xmax")).as_py(),
        pc.max(pc.struct_field(bbox, "ymax")).as_py(),
    )


class TestFidelityAgainstDuckDB:
    def test_every_returned_row_comes_from_a_predicted_group(self, sorted_places):
        query_bbox = _query_bbox_of_rows(sorted_places, 300, 50)
        preview = preview_bbox_filter(sorted_places, query_bbox)

        assert preview.has_estimate
        predicted = {group.row_group_id for group in preview.row_groups}
        returned_rows = _rows_duckdb_returns(sorted_places, query_bbox)
        contributing = {_group_of_row(sorted_places, row) for row in returned_rows}

        assert returned_rows, "the query bbox was built from real rows"
        # Every row the real read returns is in a predicted group, so no
        # predicted-empty group contributes rows.
        assert contributing <= predicted
        assert preview.est_rows >= len(returned_rows)

    def test_prediction_actually_prunes(self, sorted_places):
        """The x-sorted fixture makes a narrow bbox skip most row groups."""
        query_bbox = _query_bbox_of_rows(sorted_places, 300, 50)
        preview = preview_bbox_filter(sorted_places, query_bbox)

        assert 1 <= len(preview.row_groups) < preview.total_row_groups
        assert preview.stats_source == "bbox_struct"

    def test_prediction_equals_the_footer_stats_intersection(self, sorted_places):
        """Against an independent oracle: pyarrow's own chunk statistics.

        Every group whose bbox-column statistics intersect the query bbox is
        predicted, and no other -- so a break in any one of the four comparison
        edges fails this, not just a wholesale one.
        """
        query_bbox = _query_bbox_of_rows(sorted_places, 300, 50)
        preview = preview_bbox_filter(sorted_places, query_bbox)

        qxmin, qymin, qxmax, qymax = query_bbox
        metadata = pq.ParquetFile(sorted_places).metadata
        paths = {
            metadata.row_group(0).column(c).path_in_schema: c for c in range(metadata.num_columns)
        }

        def stat(group: int, path: str, side: str) -> float:
            statistics = metadata.row_group(group).column(paths[path]).statistics
            return getattr(statistics, side)

        expected = {
            group
            for group in range(metadata.num_row_groups)
            if stat(group, "bbox.xmin", "min") <= qxmax
            and stat(group, "bbox.xmax", "max") >= qxmin
            and stat(group, "bbox.ymin", "min") <= qymax
            and stat(group, "bbox.ymax", "max") >= qymin
        }
        assert {group.row_group_id for group in preview.row_groups} == expected

    def test_estimates_sum_the_predicted_groups_parquet_metadata(self, sorted_places):
        query_bbox = _query_bbox_of_rows(sorted_places, 300, 50)
        preview = preview_bbox_filter(sorted_places, query_bbox)

        metadata = pq.ParquetFile(sorted_places).metadata
        predicted = sorted(group.row_group_id for group in preview.row_groups)
        expected_rows = sum(metadata.row_group(g).num_rows for g in predicted)
        expected_bytes = sum(
            metadata.row_group(g).column(c).total_compressed_size
            for g in predicted
            for c in range(metadata.num_columns)
        )
        assert preview.est_rows == expected_rows
        assert preview.est_bytes == expected_bytes


class TestEdgeBboxes:
    def test_bbox_overlapping_nothing_is_a_zero_estimate(self, sorted_places):
        preview = preview_bbox_filter(sorted_places, NOWHERE)

        assert preview.has_estimate
        assert preview.row_groups == ()
        assert preview.est_rows == 0
        assert preview.est_bytes == 0
        assert _rows_duckdb_returns(sorted_places, NOWHERE) == []

    def test_bbox_covering_everything_estimates_the_whole_file(self, sorted_places):
        preview = preview_bbox_filter(sorted_places, WORLD)

        assert preview.has_estimate
        assert len(preview.row_groups) == preview.total_row_groups
        assert preview.est_rows == preview.total_rows == 766
        assert preview.est_bytes == preview.total_bytes > 0


class TestNoUsableStats:
    def test_file_without_stats_says_so_instead_of_guessing(self):
        preview = preview_bbox_filter(str(BUILDINGS), WORLD)

        assert isinstance(preview, BboxFilterPreview)
        assert not preview.has_estimate
        assert preview.reason
        assert preview.stats_source is None
        assert preview.row_groups == ()
        assert preview.est_rows is None
        assert preview.est_bytes is None
        # The totals are still facts of the file, not estimates.
        assert preview.total_rows == 42
        assert preview.total_bytes > 0

    def test_bbox_column_without_chunk_statistics_is_not_usable(self, tmp_path):
        """A bbox column whose chunks carry no statistics backs no estimate."""
        out = tmp_path / "no_stats.parquet"
        pq.write_table(pq.read_table(PLACES), out, write_statistics=False)

        preview = preview_bbox_filter(str(out), WORLD)

        assert not preview.has_estimate
        assert preview.est_rows is None

    def test_a_group_without_stats_is_never_pruned(self):
        """A group the stats cannot vouch for would be read, so it is predicted."""
        sizes = [
            {"row_group_id": 0, "num_rows": 10, "compressed_bytes": 100},
            {"row_group_id": 1, "num_rows": 20, "compressed_bytes": 200},
        ]
        stats = [{"row_group_id": 0, "xmin": 0.0, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0}]

        matched = _matching_row_groups(stats, sizes, (10.0, 10.0, 11.0, 11.0))

        assert matched == (RowGroupEstimate(row_group_id=1, num_rows=20, compressed_bytes=200),)


class TestNativeStats:
    def test_native_geo_stats_carry_the_estimate(self):
        preview = preview_bbox_filter(str(GPQ2), WORLD)

        assert preview.has_estimate
        assert preview.stats_source == "native"
        assert preview.est_rows == preview.total_rows == 100

    def test_native_file_nowhere_bbox_prunes_everything(self):
        preview = preview_bbox_filter(str(GPQ2), NOWHERE)

        assert preview.has_estimate
        assert preview.est_rows == 0


class TestBboxValidation:
    def test_wrong_arity_raises(self):
        with pytest.raises(InvalidParameterError, match="four numbers"):
            preview_bbox_filter(str(PLACES), (0.0, 1.0, 2.0))

    def test_inverted_bbox_raises(self):
        with pytest.raises(InvalidParameterError, match="xmin"):
            preview_bbox_filter(str(PLACES), (2.0, 0.0, 1.0, 1.0))

    def test_nan_bbox_raises_instead_of_matching_everything(self):
        """NaN compares False everywhere, so unrejected it would "match" every group."""
        nan = float("nan")
        with pytest.raises(InvalidParameterError, match="finite"):
            preview_bbox_filter(str(PLACES), (nan, nan, nan, nan))


class TestGeometryColumnArgument:
    """An explicit ``geometry_column`` must be honored, not silently remapped."""

    def test_a_missing_column_raises_instead_of_diagnosing_the_file(self):
        """A typo'd column is the caller's fault, not a file without statistics."""
        with pytest.raises(InvalidParameterError, match="no_such_col"):
            preview_bbox_filter(str(GPQ2), WORLD, geometry_column="no_such_col")

    def test_a_non_geometry_column_never_borrows_the_primary_covering(self, sorted_places):
        """The bbox covering belongs to the primary column; pruning another
        column's read on it would break the upper-bound guarantee."""
        preview = preview_bbox_filter(sorted_places, WORLD, geometry_column="name")
        assert not preview.has_estimate

    def test_the_primary_column_named_explicitly_matches_the_default(self, sorted_places):
        explicit = preview_bbox_filter(sorted_places, WORLD, geometry_column="geometry")
        assert explicit == preview_bbox_filter(sorted_places, WORLD)

    def test_a_plain_parquet_with_an_explicit_column_reports_no_estimate(self, tmp_path):
        """No geometry anywhere: the primary lookup fails and the explicit
        column has no covering, so the honest answer is no estimate."""
        out = tmp_path / "plain.parquet"
        pa_table = pa.table({"id": [1, 2, 3]})
        pq.write_table(pa_table, out)
        preview = preview_bbox_filter(str(out), WORLD, geometry_column="id")
        assert not preview.has_estimate


class TestAntimeridianWrappedStats:
    """Native geo statistics may wrap the antimeridian (xmin > xmax); such a
    group must never be pruned away from a query it really contains rows for."""

    SIZES = [{"row_group_id": 0, "num_rows": 10, "compressed_bytes": 100}]
    #: A Fiji-area row group spanning 170 east through the antimeridian to -170.
    WRAPPED = [{"row_group_id": 0, "xmin": 170.0, "ymin": -10.0, "xmax": -170.0, "ymax": 10.0}]

    def _matches(self, bbox: tuple[float, float, float, float]) -> bool:
        return bool(_matching_row_groups(self.WRAPPED, self.SIZES, bbox))

    def test_the_eastern_lobe_matches(self):
        assert self._matches((171.0, -5.0, 175.0, 5.0))

    def test_the_western_lobe_matches(self):
        assert self._matches((-175.0, -5.0, -172.0, 5.0))

    def test_a_query_between_the_lobes_is_pruned(self):
        assert not self._matches((0.0, -5.0, 10.0, 5.0))

    def test_a_y_disjoint_query_is_pruned_even_when_x_wraps(self):
        assert not self._matches((171.0, 20.0, 175.0, 30.0))


class TestApiFrontDoor:
    def test_ops_preview_bbox_filter_delegates(self, sorted_places):
        from geoparquet_io.api import ops

        preview = ops.preview_bbox_filter(sorted_places, WORLD)
        assert preview == preview_bbox_filter(sorted_places, WORLD)


def test_preview_module_imports_without_duckdb():
    """The embeddable contract (#1311): importing the module must not pull duckdb.

    Since the lazy root (#1307) a plain subprocess import proves it; the
    module is also in tests/test_embeddable_imports.py's EMBEDDABLE_MODULES,
    which the bare-venv CI job (no duckdb installed at all) enforces.
    """
    code = (
        "import sys\n"
        "import geoparquet_io.core.preview\n"
        "assert 'duckdb' not in sys.modules, 'core.preview must defer its duckdb imports'\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True, timeout=120)
