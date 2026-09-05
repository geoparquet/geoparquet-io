"""Spatial-order verdict is pruning-first, judged against what is achievable.

Regression tests for #755. The verdict used to be gated on the fraction of
*consecutive* row-group pairs whose bboxes overlap. On Hilbert-sorted data
consecutive groups are spatially adjacent by construction, so their boxes touch
and that fraction is ~1.0 for a perfectly ordered file -- it cannot distinguish
"every row group covers the whole country" from "row groups tile the country
perfectly but neighbours touch".

The verdict now comes from the estimated row-group skip rate, judged relative to
the skip rate an ideal grid tiling of the same extent and row-group count would
achieve. That is self-calibrating: a 2-row-group file can never skip more than
~50%, while a 589-group file should be skipping ~98%, and an absolute threshold
is wrong at both ends.
"""

import math

import pytest

from geoparquet_io.core.check_spatial_order import (
    _check_spatial_order_from_row_group_bboxes as verdict,
)
from geoparquet_io.core.check_spatial_order import (
    _compute_data_extent,
    _ideal_grid_bboxes,
    _spatial_locality_metrics,
)

UNIT = {"xmin": 0.0, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0}


def grid(n, extent=None):
    """n row groups tiling the extent in a grid -- the best a sort could do."""
    return _ideal_grid_bboxes(extent or UNIT, n)


def grid_with_slop(n, slop=0.02):
    """A grid whose boxes bleed into their neighbours: the real Hilbert shape."""
    boxes = [dict(b) for b in grid(n)]
    pad = (boxes[0]["xmax"] - boxes[0]["xmin"]) * slop
    for b in boxes:
        b["xmin"] -= pad
        b["xmax"] += pad
        b["ymin"] -= pad
        b["ymax"] += pad
    return boxes


def unsorted(n):
    """Every row group covers the whole extent -- rows in random order."""
    return [{"row_group_id": i, **UNIT} for i in range(n)]


def result(boxes):
    return verdict(boxes, "f.parquet", return_results=True, quiet=True)


class TestWellSortedFilesPass:
    """The bug: perfectly ordered files were reported Poor."""

    @pytest.mark.parametrize("n", [2, 3, 5, 13, 16, 49, 59, 240, 589])
    def test_perfect_tiling_passes_at_every_row_group_count(self, n):
        r = result(grid(n))
        assert r["passed"], f"perfect tiling of {n} row groups reported as poorly ordered"
        assert r["skip_rate_efficiency"] == pytest.approx(1.0, abs=0.05)

    @pytest.mark.parametrize("n", [2, 3, 13, 59, 589])
    def test_touching_neighbours_pass_despite_full_consecutive_overlap(self, n):
        """The reporter's case: overlap ratio 1.00, verdict must still be good."""
        boxes = grid_with_slop(n)
        r = result(boxes)

        assert r["ratio"] > 0.5, "fixture must actually exhibit high consecutive overlap"
        assert r["passed"], f"n={n}: high consecutive overlap still drove the verdict"

    def test_two_row_groups_are_not_flagged(self):
        """#755's residual case: a 2-group file was flagged and never measured."""
        r = result(grid_with_slop(2))

        assert r["passed"]
        assert r["skip_rate_efficiency"] is not None, "metrics must be computed at n=2"


class TestVerdictIsWithheldBelowTheFloor:
    """Too few row groups to judge: measure and report, but do not fail (#755).

    Measured across 60 runs per count of Hilbert-sorted clustered data, a
    PERFECTLY sorted file scores as low as 0.105 at two row groups and 0.295 at
    three. A grid of two or three cells is a poor model of what a sort can do to
    clustered data, so failing on that score would measure the row-group count.
    """

    @pytest.mark.parametrize("n", [2, 3, 4])
    def test_unsorted_data_below_the_floor_is_not_failed(self, n):
        r = result(unsorted(n))

        assert r["passed"], f"n={n} is below the floor and must not be failed"

    @pytest.mark.parametrize("n", [2, 3, 4])
    def test_metrics_are_still_reported_below_the_floor(self, n):
        """Withholding the verdict must not withhold the numbers."""
        r = result(unsorted(n))

        assert r["estimated_skip_rate"] is not None
        assert r["skip_rate_efficiency"] is not None

    def test_the_floor_is_exactly_five(self):
        """One row group either side of the boundary, same badly-ordered data."""
        assert result(unsorted(4))["passed"]
        assert not result(unsorted(5))["passed"]


class TestBadlyOrderedFilesStillFail:
    """The check must not have become permissive."""

    @pytest.mark.parametrize("n", [5, 13, 59, 240])
    def test_unsorted_data_fails(self, n):
        r = result(unsorted(n))

        assert not r["passed"]
        assert r["estimated_skip_rate"] == pytest.approx(0.0, abs=0.01)

    def test_a_single_outlier_group_is_caught(self):
        """One row group spanning the extent ruins pruning for queries that hit it."""
        boxes = grid(20)
        for b in boxes:
            b.update(UNIT)
        r = result(boxes)

        assert not r["passed"]


class TestMetricsAreAlwaysReported:
    """A passing file must still show how good it is (#755)."""

    def test_passing_file_reports_its_numbers(self):
        r = result(grid(59))

        for key in (
            "estimated_skip_rate",
            "ideal_skip_rate",
            "skip_rate_efficiency",
            "avg_bbox_area_ratio",
        ):
            assert r[key] is not None, f"{key} was not reported for a passing file"

    def test_consecutive_overlap_is_still_reported_as_a_statistic(self):
        """`ratio` stays in the payload for back-compat, demoted to informational."""
        r = result(grid_with_slop(59))

        assert 0.0 <= r["ratio"] <= 1.0
        assert r["overlap_count"] + 1 <= r["total_pairs"] + 1

    def test_failure_message_quotes_the_deciding_numbers(self):
        """Not 'overlap ratio: 1.00' -- the number the reporter proved meaningless."""
        r = result(unsorted(20))

        assert r["issues"], "a failing file must explain itself"
        message = " ".join(r["issues"])
        assert "skip" in message.lower()
        assert "overlap ratio" not in message.lower()


class TestSingleRowGroup:
    def test_one_row_group_is_trivially_ordered(self):
        r = result([{"row_group_id": 0, **UNIT}])

        assert r["passed"]

    def test_zero_row_groups_is_trivially_ordered(self):
        r = result([])

        assert r["passed"]


class TestIdealGridBboxes:
    def test_tiles_the_extent_without_gaps_or_overlap(self):
        boxes = _ideal_grid_bboxes(UNIT, 9)

        assert len(boxes) == 9
        assert _compute_data_extent(boxes) == pytest.approx(UNIT)
        total = sum((b["xmax"] - b["xmin"]) * (b["ymax"] - b["ymin"]) for b in boxes)
        assert total == pytest.approx(1.0)

    @pytest.mark.parametrize("n", [1, 2, 3, 7, 100])
    def test_returns_exactly_n_boxes(self, n):
        assert len(_ideal_grid_bboxes(UNIT, n)) == n

    def test_handles_a_degenerate_extent(self):
        """A file at one location has zero-area extent; must not divide by zero."""
        point = {"xmin": 5.0, "ymin": 5.0, "xmax": 5.0, "ymax": 5.0}

        assert len(_ideal_grid_bboxes(point, 4)) == 4


class TestLocalityMetrics:
    def test_efficiency_is_one_for_an_ideal_tiling(self):
        m = _spatial_locality_metrics(grid(49))

        assert m["skip_rate_efficiency"] == pytest.approx(1.0, abs=0.02)

    def test_efficiency_is_zero_for_unsorted_data(self):
        m = _spatial_locality_metrics(unsorted(49))

        assert m["skip_rate_efficiency"] == pytest.approx(0.0, abs=0.02)

    def test_is_deterministic_for_a_fixed_seed(self):
        assert _spatial_locality_metrics(grid(20)) == _spatial_locality_metrics(grid(20))

    def test_query_fraction_is_honoured(self):
        """A larger query window touches more row groups, so skips fewer."""
        small = _spatial_locality_metrics(grid(100), query_fraction=0.05)
        large = _spatial_locality_metrics(grid(100), query_fraction=0.5)

        assert small["estimated_skip_rate"] > large["estimated_skip_rate"]

    def test_degenerate_extent_does_not_raise(self):
        boxes = [
            {"row_group_id": i, "xmin": 5.0, "ymin": 5.0, "xmax": 5.0, "ymax": 5.0}
            for i in range(4)
        ]

        m = _spatial_locality_metrics(boxes)

        assert m["skip_rate_efficiency"] is not None


class TestRealWorldShapes:
    """Shapes most likely to break an ideal-grid comparison."""

    def _chunked(self, points, n_groups):
        size = math.ceil(len(points) / n_groups)
        out = []
        for i in range(n_groups):
            chunk = points[i * size : (i + 1) * size]
            if not chunk:
                continue
            out.append(
                {
                    "row_group_id": i,
                    "xmin": min(p[0] for p in chunk),
                    "xmax": max(p[0] for p in chunk),
                    "ymin": min(p[1] for p in chunk),
                    "ymax": max(p[1] for p in chunk),
                }
            )
        return out

    @staticmethod
    def _morton(x, y, order=16):
        xi, yi = int(x * (2**order - 1)), int(y * (2**order - 1))
        key = 0
        for bit in range(order):
            key |= ((xi >> bit) & 1) << (2 * bit) | ((yi >> bit) & 1) << (2 * bit + 1)
        return key

    def test_linear_coastline_shaped_data_passes(self):
        """Data on a 1-D manifold inside a 2-D extent (coastline, river, road net)."""
        pts = [(t / 20000, 0.5 + 0.4 * math.sin(t / 20000 * 6)) for t in range(20000)]
        pts.sort(key=lambda p: self._morton(p[0], min(max(p[1], 0), 1)))

        assert result(self._chunked(pts, 59))["passed"]

    def test_very_elongated_extent_passes(self):
        """A tall narrow country: extent aspect ratio ~50:1."""
        pts = [((i % 100) / 5000.0, (i // 100) / 200.0) for i in range(20000)]
        pts.sort(key=lambda p: self._morton(min(p[0] * 50, 1.0), min(p[1], 1.0)))

        assert result(self._chunked(pts, 59))["passed"]


class TestIdealGridEdgeCases:
    def test_zero_row_groups_returns_no_boxes(self):
        assert _ideal_grid_bboxes(UNIT, 0) == []

    def test_negative_count_returns_no_boxes(self):
        assert _ideal_grid_bboxes(UNIT, -1) == []


class TestVerboseReporting:
    def test_verbose_logs_the_deciding_numbers(self, caplog):
        import logging

        with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
            verdict(grid_with_slop(20), "f.parquet", verbose=True, return_results=True, quiet=True)

        assert "Locality:" in caplog.text
        assert "achievable" in caplog.text
        assert "efficiency" in caplog.text


class TestCliReportsBothVerdicts:
    """End-to-end: the ordering verdict and pushdown readiness, computed once."""

    @pytest.fixture
    def sorted_file(self, tmp_path):
        """A bbox-bearing file with many row groups whose boxes tile the extent."""
        import json
        import struct

        import pyarrow as pa
        import pyarrow.parquet as pq

        n, groups = 4000, 40
        per = n // groups
        rows, bboxes = [], []
        for i in range(n):
            g = i // per
            gx, gy = (g % 8) / 8.0, (g // 8) / 5.0
            x = gx + (i % per) / per * 0.125
            y = gy + 0.1
            rows.append(struct.pack("<BI2d", 1, 1, x, y))
            bboxes.append({"xmin": x, "ymin": y, "xmax": x, "ymax": y})
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": ["Point"],
                    "covering": {
                        "bbox": {
                            "xmin": ["bbox", "xmin"],
                            "ymin": ["bbox", "ymin"],
                            "xmax": ["bbox", "xmax"],
                            "ymax": ["bbox", "ymax"],
                        }
                    },
                }
            },
        }
        table = pa.table(
            {
                "geometry": rows,
                "bbox": pa.array(
                    bboxes,
                    type=pa.struct([(k, pa.float64()) for k in ("xmin", "ymin", "xmax", "ymax")]),
                ),
            }
        ).replace_schema_metadata({b"geo": json.dumps(geo).encode()})
        path = tmp_path / "sorted.parquet"
        pq.write_table(table, path, row_group_size=per)
        return str(path)

    @pytest.fixture
    def unsorted_file(self, tmp_path, sorted_file):
        """Same data, rows shuffled: every row group's bbox spans the extent."""
        import random

        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pq.read_table(sorted_file)
        order = list(range(table.num_rows))
        random.Random(0).shuffle(order)
        shuffled = table.take(pa.array(order)).replace_schema_metadata(table.schema.metadata)
        path = tmp_path / "unsorted.parquet"
        pq.write_table(shuffled, path, row_group_size=table.num_rows // 40)
        return str(path)

    def _run(self, args):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import check

        return CliRunner().invoke(check, args)

    def test_well_sorted_file_passes_and_shows_the_achievable_rate(self, sorted_file):
        r = self._run(["spatial", sorted_file])

        assert r.exit_code == 0, r.output
        assert "spatially ordered" in r.output
        assert "achievable at this row-group count" in r.output

    def test_min_efficiency_flag_changes_the_verdict(self, unsorted_file):
        """The same file, both verdicts, decided only by the threshold."""
        default = self._run(["spatial", unsorted_file])
        lenient = self._run(["spatial", unsorted_file, "--min-efficiency", "0.0"])

        assert "may not be optimally spatially ordered" in default.output
        assert "✓ Data appears to be spatially ordered" in lenient.output

    def test_unsorted_file_names_the_achievable_rate_in_the_warning(self, unsorted_file):
        r = self._run(["spatial", unsorted_file, "--verbose"])

        assert "achievable" in r.output

    def test_query_fraction_and_samples_are_accepted(self, sorted_file):
        r = self._run(
            ["spatial", sorted_file, "--query-fraction", "0.2", "--num-samples", "5", "--seed", "7"]
        )

        assert r.exit_code == 0, r.output

    def test_pushdown_falls_back_when_ordering_reports_no_metrics(self, sorted_file, monkeypatch):
        """Sampling-fallback results carry no skip rate; pushdown must still run."""
        from geoparquet_io.cli import main as cli_main

        def _no_metrics(*args, **kwargs):
            return {"ratio": 0.1, "passed": True, "total_pairs": 0}

        monkeypatch.setattr(cli_main, "check_spatial_impl", _no_metrics)
        r = self._run(["spatial", sorted_file])

        assert r.exit_code == 0, r.output
        assert "Spatial Filter Pushdown Readiness" in r.output
        assert "Estimated skip rate" in r.output


class TestSkipRateEdgeCases:
    def test_empty_box_list_skips_nothing(self):
        from geoparquet_io.core.check_spatial_order import _compute_skip_rate_for_query

        assert _compute_skip_rate_for_query(UNIT, []) == 0.0

    def test_bbox_stats_failure_falls_back_to_sampling(self, tmp_path, monkeypatch):
        """A malformed bbox column must fall through, not abort the check."""
        import geoparquet_io.core.check_spatial_order as mod

        def _boom(*args, **kwargs):
            raise ValueError("malformed bbox column")

        monkeypatch.setattr(mod, "check_spatial_order_bbox_stats", _boom)
        monkeypatch.setattr(
            "geoparquet_io.core.duckdb_metadata.has_bbox_column", lambda *a, **k: (True, "bbox")
        )
        monkeypatch.setattr(
            "geoparquet_io.core.duckdb_metadata.get_per_row_group_native_geo_stats",
            lambda *a, **k: grid(9),
        )
        # module-level import, so patch the binding the function actually uses
        monkeypatch.setattr(mod, "find_primary_geometry_column", lambda *a, **k: "geometry")
        monkeypatch.setattr(mod, "safe_file_url", lambda p, v=False: str(p))

        out = mod.check_spatial_order(
            str(tmp_path / "x.parquet"),
            random_sample_size=10,
            limit_rows=100,
            verbose=True,
            return_results=True,
            quiet=True,
        )

        assert out["passed"], "fell through to the native-stats path and judged it"
