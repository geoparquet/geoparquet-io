"""Spatial-order verdict is pruning-first, judged against what is achievable.

Regression tests for #755. The verdict used to be gated on the fraction of
*consecutive* row-group pairs whose bboxes overlap. On Hilbert-sorted data
consecutive groups are spatially adjacent by construction, so their boxes touch
and that fraction is ~1.0 for a perfectly ordered file -- it cannot distinguish
"every row group covers the whole country" from "row groups tile the country
perfectly but neighbours touch".

The verdict now comes from the EXPECTED share of the file's bytes a query
window skips. The window's centre falls in a row group with probability
proportional to its rows, uniform within its box, and spans 10% of each
dimension of that box; each group weighs its compressed bytes. That is judged
relative to the same expectation for a full tiling of the extent into the same
number of cells. The expectation has a closed form: deterministic, O(n^2),
footer only, no seed. The definition is the Portolan spec's (portolan-spec#188,
enforced by rashid#174); ``TestSharedDefinitionCrossChecks`` holds the
cross-checks every implementation must pass, and ``TestSharedVectors`` the
spec's normative vectors.
"""

import json
import logging
import math
import random
from pathlib import Path

import pytest

import geoparquet_io.core.check_spatial_order as mod
from geoparquet_io.core.check_spatial_order import (
    SPATIAL_ORDER_MIN_EFFICIENCY,
    SPATIAL_ORDER_MIN_ROW_GROUPS,
    SPATIAL_ORDER_OBVIOUS_FAIL_EFFICIENCY,
    _bboxes_overlap,
    _compute_data_extent,
    _full_tiling_bboxes,
    _spatial_locality_metrics,
    _spatial_order_verdict,
    _weighted_skip_rate,
    spatial_verdict_withheld,
)
from geoparquet_io.core.check_spatial_order import (
    _check_spatial_order_from_row_group_bboxes as verdict,
)

UNIT = {"xmin": 0.0, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0}
FLOOR = SPATIAL_ORDER_MIN_ROW_GROUPS


@pytest.fixture(autouse=True)
def _no_size_floor(monkeypatch):
    """The fixtures here are kilobytes; the spec's 5 MB floor would mute every
    verdict. ``TestSizeFloor`` sets it back explicitly."""
    monkeypatch.setattr(mod, "SPATIAL_ORDER_MIN_FILE_BYTES", 0)


def tiling(n, extent=None):
    """n row groups tiling the extent -- the best a sort could do."""
    return [{"row_group_id": i, **b} for i, b in enumerate(_full_tiling_bboxes(extent or UNIT, n))]


def tiling_with_slop(n, slop=0.02):
    """A tiling whose boxes bleed into their neighbours: the real Hilbert shape."""
    boxes = tiling(n)
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


def strips(n):
    """Vertical strips, overlapping their neighbours: middling order.

    Scores between the obvious-fail bar and the pass bar at small n, which is
    exactly the band where the verdict is withheld below the floor.
    """
    width = 1.0 / (0.5 + 0.5 * n)
    out = []
    for i in range(n):
        x0 = i * width / 2
        out.append({"row_group_id": i, "xmin": x0, "ymin": 0.0, "xmax": x0 + width, "ymax": 1.0})
    return out


def result(boxes):
    return verdict(boxes, "f.parquet", return_results=True, quiet=True)


def _morton(x, y, order=16):
    xi, yi = int(x * (2**order - 1)), int(y * (2**order - 1))
    key = 0
    for bit in range(order):
        key |= ((xi >> bit) & 1) << (2 * bit) | ((yi >> bit) & 1) << (2 * bit + 1)
    return key


def _chunked(points, n_groups):
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


def coastline(n_groups):
    """Data on a 1-D manifold inside a 2-D extent, curve-sorted, in n row groups."""
    pts = [(t / 20000, 0.5 + 0.4 * math.sin(t / 20000 * 6)) for t in range(20000)]
    pts.sort(key=lambda p: _morton(p[0], min(max(p[1], 0), 1)))
    return _chunked(pts, n_groups)


def _sampled_skip_rate(boxes, rows, sizes, query_fraction, num_samples, seed):
    """The quantity the spec defines, estimated the slow way.

    A window is drawn by choosing a source box by rows, a centre uniform in it,
    and a window at ``query_fraction`` of that box; the bytes of the boxes it
    overlaps are read. Kept here, not in the module, purely to prove the closed
    form is the same quantity (shared-definition cross-check 3).
    """
    rng = random.Random(seed)  # nosec B311 - test oracle, not security
    total_bytes = sum(sizes)
    read = 0.0
    for _ in range(num_samples):
        (i,) = rng.choices(range(len(boxes)), weights=rows)  # nosec B311
        b = boxes[i]
        cx = rng.uniform(b["xmin"], b["xmax"])  # nosec B311
        cy = rng.uniform(b["ymin"], b["ymax"])  # nosec B311
        hw = query_fraction * (b["xmax"] - b["xmin"]) / 2
        hh = query_fraction * (b["ymax"] - b["ymin"]) / 2
        window = {"xmin": cx - hw, "ymin": cy - hh, "xmax": cx + hw, "ymax": cy + hh}
        read += sum(
            sizes[j] / total_bytes
            for j, t in enumerate(boxes)
            if t["xmin"] <= window["xmax"]
            and window["xmin"] <= t["xmax"]
            and t["ymin"] <= window["ymax"]
            and window["ymin"] <= t["ymax"]
        )
    return 1.0 - read / num_samples


class TestSharedDefinitionCrossChecks:
    """The cross-checks every implementation of the shared definition must pass."""

    @pytest.mark.parametrize("n", [2, 3, 5, 8, 13, 100])
    def test_1_perfect_tiling_scores_exactly_one(self, n):
        m = _spatial_locality_metrics(tiling(n))

        assert m["skip_rate_efficiency"] == 1.0

    def test_2_every_box_equal_to_the_extent_scores_zero(self):
        m = _spatial_locality_metrics(unsorted(20))

        assert m["estimated_skip_rate"] == 0.0
        assert m["skip_rate_efficiency"] == 0.0

    def test_3_closed_form_equals_the_sample_mean(self):
        """The formula is the expectation of the sampled process the spec names."""
        boxes = coastline(13)
        rows = [3.0, 1.0, 2.0, 2.0, 1.0, 1.0, 2.0, 1.0, 3.0, 1.0, 1.0, 2.0, 1.0]
        sizes = [2.0, 1.0, 4.0, 1.0, 1.0, 2.0, 1.0, 1.0, 2.0, 3.0, 1.0, 1.0, 1.0]

        exact = _weighted_skip_rate(boxes, rows, sizes, 0.1)
        sampled = _sampled_skip_rate(boxes, rows, sizes, 0.1, 40_000, seed=0)

        assert 0.05 < exact < 0.95, "fixture must not sit at a trivial 0 or 1"
        assert exact == pytest.approx(sampled, abs=0.01)

    def test_4_full_tiling_reference_at_n3(self):
        """The reference is the pinned full tiling, not the grid with an empty cell."""
        old_empty_cell_grid = [
            {"xmin": 0.0, "ymin": 0.0, "xmax": 0.5, "ymax": 0.5},
            {"xmin": 0.5, "ymin": 0.0, "xmax": 1.0, "ymax": 0.5},
            {"xmin": 0.0, "ymin": 0.5, "xmax": 0.5, "ymax": 1.0},
        ]

        assert _weighted_skip_rate(old_empty_cell_grid) == pytest.approx(0.644, abs=0.001)
        assert _weighted_skip_rate(tiling(3)) == pytest.approx(0.638, abs=0.001)

    def test_5a_zero_height_extent_gives_a_defined_answer(self):
        """All boxes on a line: the y axis is degenerate, every window hits it."""
        boxes = [
            {"row_group_id": i, "xmin": i / 4, "ymin": 3.0, "xmax": (i + 1) / 4, "ymax": 3.0}
            for i in range(4)
        ]

        m = _spatial_locality_metrics(boxes)

        assert m["skip_rate_efficiency"] == 1.0
        assert 0.0 < m["estimated_skip_rate"] < 1.0

    @pytest.mark.parametrize("q", [0.01, 0.1, 0.5, 0.99, 1.0])
    def test_5b_one_row_group_reference_skips_nothing_at_any_query_fraction(self, q):
        """The reference for n=1 is the whole extent: hit by every window, undefined."""
        m = _spatial_locality_metrics([{"row_group_id": 0, **UNIT}], query_fraction=q)

        assert m["estimated_skip_rate"] == 0.0
        assert m["ideal_skip_rate"] == 0.0
        assert m["skip_rate_efficiency"] is None, "nothing to fall short of: undefined"

    def test_5c_zero_width_and_height_above_the_floor_is_not_judged_and_does_not_raise(self):
        """Every box at one point: both axes degenerate, reference skip 0, no verdict.

        Zero area alone is not enough: a line (``line-n8`` in the shared
        vectors) has a degenerate y axis, a normal x axis, and IS judged.
        """
        boxes = [
            {"row_group_id": i, "xmin": 5.0, "ymin": 5.0, "xmax": 5.0, "ymax": 5.0}
            for i in range(FLOOR + 2)
        ]

        r = result(boxes)

        assert r["judged"] is False and r["passed"] is True
        assert r["skip_rate_efficiency"] is None
        assert r["bbox_area_sum"] is None, "0/0 is undefined, not vanishingly tight"
        assert r["estimated_skip_rate"] == 0.0 and r["ideal_skip_rate"] == 0.0
        assert any("not judged" in w for w in r["warnings"])
        assert r["fix_available"] is False


VECTORS = json.loads(
    (Path(__file__).parent / "data" / "spatial_order_vectors.json").read_text(encoding="utf-8")
)


class TestSharedVectors:
    """The vectors the Portolan spec, rashid and gpio all pin (PORTO-FMT-054).

    Vendored byte-for-byte from ``specs/portolan/abstract-tests/spatial-metric-vectors.json``
    in portolan-spec. Only the ``layouts`` apply here: the ``rows`` section is
    the spec's row rule, which gpio does not implement.
    """

    @pytest.mark.parametrize("name", list(VECTORS["layouts"]))
    def test_layout_vector(self, name):
        vector = VECTORS["layouts"][name]
        boxes = [
            dict(zip(("xmin", "ymin", "xmax", "ymax"), b, strict=True)) for b in vector["boxes"]
        ]
        rows = [float(r) for r in vector["rows"]] if "rows" in vector else None
        sizes = [float(x) for x in vector["sizes"]] if "sizes" in vector else None
        expected = vector["expected"]

        m = _spatial_locality_metrics(boxes, rows=rows, sizes=sizes, query_fraction=0.1)

        assert len(boxes) == expected["count"]
        assert m["estimated_skip_rate"] == pytest.approx(expected["achieved"], abs=1e-6)
        assert m["ideal_skip_rate"] == pytest.approx(expected["achievable"], abs=1e-6)
        if expected["efficiency"] is None:
            assert m["skip_rate_efficiency"] is None
        else:
            assert m["skip_rate_efficiency"] == pytest.approx(expected["efficiency"], abs=1e-6)
        if expected["area_sum"] is None:
            assert m["bbox_area_sum"] is None
        else:
            assert m["bbox_area_sum"] == pytest.approx(expected["area_sum"], abs=1e-5)
        # below_bar is null exactly where the efficiency is: undefined is not judged
        assert (expected["below_bar"] is None) is (expected["efficiency"] is None)
        if expected["below_bar"] is not None:
            # the bar alone; the footer check's floors are applied on top
            assert (m["skip_rate_efficiency"] < SPATIAL_ORDER_MIN_EFFICIENCY) is expected[
                "below_bar"
            ]


class TestVerdictFunction:
    """``_spatial_order_verdict``: the one thing a reader wants to check."""

    def test_at_or_above_the_floor_the_bar_decides(self):
        assert _spatial_order_verdict(FLOOR, 0.70, None) == (True, True, [])
        assert _spatial_order_verdict(FLOOR, 0.69, None)[:2] == (False, True)

    def test_below_the_floor_a_clear_failure_is_judged(self):
        """The spec's PORTO-FMT-059: efficiency under 0.25 fails from two groups."""
        assert _spatial_order_verdict(2, 0.0, None)[:2] == (False, True)
        assert _spatial_order_verdict(FLOOR - 1, 0.24, None)[:2] == (False, True)

    def test_below_the_floor_a_middling_file_is_not_judged_and_names_the_lever(self):
        passed, judged, warnings = _spatial_order_verdict(FLOOR - 1, 0.5, None)

        assert (passed, judged) == (True, False)
        assert "not judged below 8 row groups (7 in this file)" in warnings[0]
        assert "--row-group-size" in warnings[0]

    def test_under_the_size_floor_nothing_is_judged(self, monkeypatch):
        """The spec's PORTO-FMT-058: under 5 MB even a shuffled file is only reported."""
        monkeypatch.setattr(mod, "SPATIAL_ORDER_MIN_FILE_BYTES", 5_000_000)
        passed, judged, warnings = _spatial_order_verdict(20, 0.0, 4_999_999)

        assert (passed, judged) == (True, False)
        assert "5 MB floor" in warnings[0]

    def test_at_the_size_floor_the_bar_decides_again(self, monkeypatch):
        monkeypatch.setattr(mod, "SPATIAL_ORDER_MIN_FILE_BYTES", 5_000_000)
        assert _spatial_order_verdict(20, 0.0, 5_000_000)[:2] == (False, True)

    def test_undefined_efficiency_and_no_statistics_are_not_judged(self):
        assert _spatial_order_verdict(FLOOR, None, None)[:2] == (True, False)
        assert "no row-group statistics" in _spatial_order_verdict(0, None, None)[2][0]

    def test_the_predicate_reads_judged_and_defaults_to_judged(self):
        assert spatial_verdict_withheld({"passed": True, "judged": False}) is True
        assert spatial_verdict_withheld({"passed": False}) is False

    def test_the_bars_and_floors_are_the_spec_constants(self):
        assert SPATIAL_ORDER_MIN_EFFICIENCY == 0.70
        assert SPATIAL_ORDER_OBVIOUS_FAIL_EFFICIENCY == 0.25
        assert FLOOR == 8
        assert mod.SPATIAL_ORDER_MIN_FILE_BYTES == 0, "patched for these tests"


class TestClosedForm:
    def test_hit_probability_matches_the_hand_computation(self):
        """Two half-extent boxes side by side, q=0.1.

        A window from the left box (width 0.05) overlaps the right box exactly
        when its centre lies within 0.025 of the boundary: Fx = 0.025/0.5.
        Reading the own box is certain, so the skip rate is 1 - (1 + 0.05)/2.
        """
        boxes = [
            {"xmin": 0.0, "ymin": 0.0, "xmax": 0.5, "ymax": 1.0},
            {"xmin": 0.5, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0},
        ]

        assert _weighted_skip_rate(boxes, query_fraction=0.1) == pytest.approx(
            1 - (1 + 0.025 / 0.5) / 2
        )

    def test_extent_is_the_union_of_the_boxes_not_a_declared_bbox(self):
        """Judged against where the row groups are, so a sub-extent tiling is perfect."""
        sub = {"xmin": 10.0, "ymin": 10.0, "xmax": 20.0, "ymax": 15.0}

        assert _spatial_locality_metrics(tiling(9, sub))["skip_rate_efficiency"] == 1.0

    def test_is_deterministic(self):
        assert _spatial_locality_metrics(coastline(13)) == _spatial_locality_metrics(coastline(13))

    def test_query_fraction_is_honoured(self):
        """A larger query window touches more row groups, so skips fewer."""
        small = _spatial_locality_metrics(tiling(100), query_fraction=0.05)
        large = _spatial_locality_metrics(tiling(100), query_fraction=0.5)

        assert small["estimated_skip_rate"] > large["estimated_skip_rate"]

    def test_byte_weights_decide_the_cost_of_a_hit(self):
        """One unprunable box holding most of the bytes ruins the skip rate."""
        boxes = tiling(7) + [dict(UNIT)]
        uniform = _spatial_locality_metrics(boxes)
        heavy = _spatial_locality_metrics(boxes, sizes=[1.0] * 7 + [93.0])

        assert heavy["estimated_skip_rate"] < uniform["estimated_skip_rate"] / 5

    def test_row_weights_decide_where_queries_land(self):
        """Queries follow the rows, so a crowded tile is read almost always."""
        boxes = tiling(4)
        crowded = _spatial_locality_metrics(boxes, rows=[97.0, 1.0, 1.0, 1.0])

        assert crowded["skip_rate_efficiency"] < 0.1
        assert _spatial_locality_metrics(boxes)["skip_rate_efficiency"] == 1.0

    def test_one_far_outlier_does_not_flip_the_verdict(self):
        """The extent-relative blind spot the review found, both ways (#1099)."""
        shuffled = unsorted(20)
        shuffled[0] = {**shuffled[0], "xmax": 100.0, "ymax": 100.0}
        sorted_boxes = tiling(20, {"xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0})
        sorted_boxes[19] = {**sorted_boxes[19], "xmax": 100.0, "ymax": 100.0}

        assert _spatial_locality_metrics(shuffled)["skip_rate_efficiency"] < 0.1
        assert _spatial_locality_metrics(sorted_boxes)["skip_rate_efficiency"] > 0.9

    def test_a_nan_statistic_is_not_a_silent_pass(self):
        """Non-finite stats drop the file to 'no statistics', never efficiency 1.00."""
        boxes = tiling(12)
        boxes[3] = {**boxes[3], "xmax": float("nan")}

        r = verdict(boxes, "f.parquet", return_results=True, quiet=True)

        assert r["judged"] is False
        assert any("no row-group statistics" in w for w in r["warnings"])

    def test_efficiency_is_capped_at_one(self):
        """Clustered data can beat the grid; the cap keeps the number a fraction."""
        boxes = tiling(16)
        for b in boxes:  # shrink every box to a point cluster in its cell
            cx, cy = (b["xmin"] + b["xmax"]) / 2, (b["ymin"] + b["ymax"]) / 2
            b.update(xmin=cx - 0.01, xmax=cx + 0.01, ymin=cy - 0.01, ymax=cy + 0.01)

        m = _spatial_locality_metrics(boxes)

        assert m["estimated_skip_rate"] > m["ideal_skip_rate"]
        assert m["skip_rate_efficiency"] == 1.0

    def test_area_sum_is_reported(self):
        """Sum of box areas over the extent area: 1.0 for a tiling, n for unsorted."""
        assert _spatial_locality_metrics(tiling(9))["bbox_area_sum"] == pytest.approx(1.0)
        assert _spatial_locality_metrics(unsorted(9))["bbox_area_sum"] == pytest.approx(9.0)

    def test_read_profile_and_largest_boxes_are_reported(self):
        """The spec's reporting extras (PORTO-FMT-061)."""
        boxes = tiling(7) + [dict(UNIT)]

        m = _spatial_locality_metrics(boxes)

        assert len(m["read_profile"]) == 3
        assert m["read_profile"][0] <= m["read_profile"][2]
        assert m["largest_row_groups"][0] == boxes[-1].get("row_group_id")

    def test_area_sum_exposes_the_high_n_saturation_the_efficiency_hides(self):
        """A 10x10 grid with every box dilated 2x still scores ~0.95 -- but 3.6x the area."""
        boxes = tiling(100)
        for b in boxes:
            w, h = b["xmax"] - b["xmin"], b["ymax"] - b["ymin"]
            b.update(xmin=b["xmin"] - w / 2, xmax=b["xmax"] + w / 2)
            b.update(ymin=b["ymin"] - h / 2, ymax=b["ymax"] + h / 2)

        m = _spatial_locality_metrics(boxes)

        assert m["skip_rate_efficiency"] > 0.9
        assert m["bbox_area_sum"] > 3.0


class TestFullTiling:
    @pytest.mark.parametrize("n", [1, 2, 3, 5, 7, 8, 10, 100])
    def test_n_cells_cover_the_extent_exactly(self, n):
        boxes = _full_tiling_bboxes(UNIT, n)

        assert len(boxes) == n
        assert _compute_data_extent(boxes) == pytest.approx(UNIT)
        total = sum((b["xmax"] - b["xmin"]) * (b["ymax"] - b["ymin"]) for b in boxes)
        assert total == pytest.approx(1.0), "no uncovered area"

    def test_last_row_stretches_to_the_full_width(self):
        """n=3: two cells on the first row, one full-width cell on the last."""
        boxes = _full_tiling_bboxes(UNIT, 3)

        assert boxes[2]["xmin"] == 0.0 and boxes[2]["xmax"] == 1.0
        assert boxes[0]["xmax"] == 0.5

    def test_cells_do_not_overlap(self):
        boxes = _full_tiling_bboxes(UNIT, 7)

        for i, a in enumerate(boxes):
            for b in boxes[i + 1 :]:
                assert not _bboxes_overlap(a, b)

    def test_handles_a_degenerate_extent(self):
        point = {"xmin": 5.0, "ymin": 5.0, "xmax": 5.0, "ymax": 5.0}

        assert len(_full_tiling_bboxes(point, 4)) == 4


class TestWellSortedFilesPass:
    """The bug: perfectly ordered files were reported Poor."""

    @pytest.mark.parametrize("n", [8, 13, 16, 49, 59, 240, 589])
    def test_perfect_tiling_passes_at_every_row_group_count(self, n):
        r = result(tiling(n))

        assert r["judged"] and r["passed"], f"perfect tiling of {n} row groups reported as poor"
        assert r["skip_rate_efficiency"] == 1.0

    @pytest.mark.parametrize("n", [8, 13, 59, 589])
    def test_touching_neighbours_pass_despite_full_consecutive_overlap(self, n):
        """The reporter's case: overlap ratio 1.00, verdict must still be good."""
        r = result(tiling_with_slop(n))

        assert r["ratio"] > 0.5, "fixture must actually exhibit high consecutive overlap"
        assert r["judged"] and r["passed"], f"n={n}: consecutive overlap still drove the verdict"

    def test_linear_coastline_shaped_data_passes(self):
        assert result(coastline(59))["passed"] is True

    def test_very_elongated_extent_passes(self):
        """A tall narrow country: extent aspect ratio ~50:1."""
        pts = [((i % 100) / 5000.0, (i // 100) / 200.0) for i in range(20000)]
        pts.sort(key=lambda p: _morton(min(p[0] * 50, 1.0), min(p[1], 1.0)))

        assert result(_chunked(pts, 59))["passed"] is True


class TestBadlyOrderedFilesStillFail:
    """The check must not have become permissive."""

    @pytest.mark.parametrize("n", [8, 13, 59, 240])
    def test_unsorted_data_fails(self, n):
        r = result(unsorted(n))

        assert r["judged"] is True and r["passed"] is False
        assert r["estimated_skip_rate"] == 0.0
        assert r["fix_available"] is True

    def test_a_single_outlier_group_is_caught(self):
        """Every row group spanning the extent ruins pruning."""
        boxes = tiling(20)
        for b in boxes:
            b.update(UNIT)

        assert result(boxes)["passed"] is False

    @pytest.mark.parametrize("n", [2, 3, 5, FLOOR - 1])
    def test_unsorted_data_below_the_floor_is_a_clear_failure(self, n):
        """PORTO-FMT-059: efficiency 0.00 is plain at any count of two or more."""
        r = result(unsorted(n))

        assert r["judged"] is True and r["passed"] is False
        assert r["fix_available"] is True


class TestVerdictIsWithheldBelowTheFloor:
    """Middling order on too few row groups: measure and report, neither pass nor fail.

    ``passed`` keeps meaning "no failure found", so an untouched consumer that
    never reads ``judged`` treats a withheld verdict as not-a-failure -- never
    as the failure that would tell every small file to re-sort (#755 again).
    """

    @pytest.mark.parametrize("n", [3, 5, FLOOR - 1])
    def test_middling_order_below_the_floor_is_not_judged(self, n):
        r = result(strips(n))

        eff = r["skip_rate_efficiency"]
        assert SPATIAL_ORDER_OBVIOUS_FAIL_EFFICIENCY < eff, "fixture must sit between the bars"
        assert r["judged"] is False, f"n={n} is below the floor and must not be judged"
        assert r["passed"] is True, "no failure found"
        assert r["fix_available"] is False
        assert r["issues"] == []

    @pytest.mark.parametrize("n", [3, FLOOR - 1])
    def test_metrics_are_still_reported_below_the_floor(self, n):
        """Withholding the verdict must not withhold the numbers."""
        r = result(strips(n))

        assert r["estimated_skip_rate"] > 0.0
        assert r["ideal_skip_rate"] > 0.0
        assert r["skip_rate_efficiency"] is not None
        assert r["bbox_area_sum"] is not None

    def test_the_withheld_verdict_says_so_in_the_payload(self):
        r = result(strips(3))

        assert any("not judged below 8 row groups" in w for w in r["warnings"])

    def test_one_row_group_either_side_of_the_floor(self):
        """Same middling data: withheld at FLOOR-1, judged at FLOOR."""
        assert result(strips(FLOOR - 1))["judged"] is False
        assert result(strips(FLOOR))["judged"] is True

    def test_a_perfect_tiling_below_the_floor_is_not_judged_either(self):
        r = result(tiling(2))

        assert r["skip_rate_efficiency"] == 1.0
        assert r["judged"] is False

    def test_zero_row_groups_reports_nothing_and_no_verdict(self):
        r = result([])

        assert r["judged"] is False and r["passed"] is True
        assert r["skip_rate_efficiency"] is None


class TestMetricsAreAlwaysReported:
    """A passing file must still show how good it is (#755)."""

    def test_passing_file_reports_its_numbers(self):
        r = result(tiling(59))

        for key in (
            "estimated_skip_rate",
            "ideal_skip_rate",
            "skip_rate_efficiency",
            "avg_bbox_area_ratio",
            "bbox_area_sum",
            "read_profile",
        ):
            assert r[key] is not None, f"{key} was not reported for a passing file"

    def test_consecutive_overlap_is_still_reported_as_a_statistic(self):
        """`ratio` stays in the payload for back-compat, demoted to informational."""
        r = result(tiling_with_slop(59))

        assert 0.0 <= r["ratio"] <= 1.0
        assert r["overlap_count"] <= r["total_pairs"]

    def test_failure_message_quotes_the_deciding_numbers(self):
        """Not 'overlap ratio: 1.00' -- the number the reporter proved meaningless."""
        r = result(unsorted(20))

        assert r["issues"], "a failing file must explain itself"
        message = " ".join(r["issues"])
        assert "reads" in message.lower()
        assert "overlap ratio" not in message.lower()


class TestStandalonePrintPath:
    """``return_results=False, quiet=False``: the core's own summary lines."""

    @pytest.mark.parametrize(
        ("boxes", "expected"),
        [
            (tiling(20), "well spatially ordered"),
            (unsorted(20), "may benefit from spatial ordering"),
            (strips(2), "not judged"),
        ],
        ids=["pass", "fail", "withheld"],
    )
    def test_verdict_line_matches_the_structured_verdict(self, caplog, boxes, expected):
        with caplog.at_level(logging.INFO):
            verdict(boxes, "f.parquet", return_results=False, quiet=False)

        messages = " ".join(r.message for r in caplog.records)
        assert expected in messages
        assert "area sum" in messages
        assert messages.count("spatially ordered") <= 1


def _write_bbox_file(path, n_rows, n_groups, shuffle_seed=None, bbox_name="bbox", decoy=None):
    """A 1.1 file with a bbox covering column, ``n_groups`` row groups, points on a grid walk.

    ``decoy`` adds a struct column of that name BEFORE the real one, carrying
    the boxes of a perfectly sorted file: what a stale or secondary bbox
    column looks like to a name-suffix heuristic.
    """
    import json
    import struct

    import pyarrow as pa
    import pyarrow.parquet as pq

    per = n_rows // n_groups
    cols = math.ceil(math.sqrt(n_groups))
    rows_of_cells = math.ceil(n_groups / cols)
    order = list(range(n_rows))
    if shuffle_seed is not None:
        random.Random(shuffle_seed).shuffle(order)
    geoms, bboxes, sorted_boxes = [], [], []
    for k, i in enumerate(order):
        for target, idx in ((bboxes, i), (sorted_boxes, k)):
            g = min(idx // per, n_groups - 1)
            gx, gy = (g % cols) / cols, (g // cols) / rows_of_cells
            x = gx + (idx % per) / per / cols
            y = gy + 0.5 / rows_of_cells
            target.append({"xmin": x, "ymin": y, "xmax": x, "ymax": y})
        geoms.append(struct.pack("<BI2d", 1, 1, bboxes[-1]["xmin"], bboxes[-1]["ymin"]))
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "covering": {"bbox": {k: [bbox_name, k] for k in ("xmin", "ymin", "xmax", "ymax")}},
            }
        },
    }
    struct_type = pa.struct([(k, pa.float64()) for k in ("xmin", "ymin", "xmax", "ymax")])
    columns = {"geometry": geoms}
    if decoy:
        columns[decoy] = pa.array(sorted_boxes, type=struct_type)
    columns[bbox_name] = pa.array(bboxes, type=struct_type)
    table = pa.table(columns).replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    pq.write_table(table, path, row_group_size=per, compression="zstd")
    assert pq.ParquetFile(path).metadata.num_row_groups == n_groups
    return str(path)


def _run(args):
    from click.testing import CliRunner

    from geoparquet_io.cli.commands.check import check

    return CliRunner().invoke(check, args)


class TestSizeFloor:
    """The spec's 5 MB floor (PORTO-FMT-058), on real files."""

    def test_a_small_shuffled_file_is_not_judged(self, tmp_path, monkeypatch):
        monkeypatch.setattr(mod, "SPATIAL_ORDER_MIN_FILE_BYTES", 5_000_000)
        path = _write_bbox_file(tmp_path / "small.parquet", 4000, 40, shuffle_seed=0)

        r = mod.check_spatial_order(path, 10, 100, verbose=False, return_results=True, quiet=True)

        assert r["judged"] is False and r["passed"] is True
        assert any("5 MB floor" in w for w in r["warnings"])
        assert r["fix_available"] is False

    def test_the_floor_reads_the_footer_compressed_bytes(self, tmp_path, monkeypatch):
        path = _write_bbox_file(tmp_path / "boundary.parquet", 4000, 40, shuffle_seed=0)
        r = mod.check_spatial_order(path, 10, 100, verbose=False, return_results=True, quiet=True)
        footer_bytes = r["file_bytes"]
        assert footer_bytes and footer_bytes > 0

        monkeypatch.setattr(mod, "SPATIAL_ORDER_MIN_FILE_BYTES", footer_bytes + 1)
        below = mod.check_spatial_order(
            path, 10, 100, verbose=False, return_results=True, quiet=True
        )
        monkeypatch.setattr(mod, "SPATIAL_ORDER_MIN_FILE_BYTES", footer_bytes)
        at_floor = mod.check_spatial_order(
            path, 10, 100, verbose=False, return_results=True, quiet=True
        )

        assert below["judged"] is False
        assert at_floor["judged"] is True and at_floor["passed"] is False

    def test_cli_says_the_file_is_too_small(self, tmp_path, monkeypatch):
        monkeypatch.setattr(mod, "SPATIAL_ORDER_MIN_FILE_BYTES", 5_000_000)
        path = _write_bbox_file(tmp_path / "small.parquet", 4000, 40, shuffle_seed=0)

        r = _run(["spatial", path])

        assert r.exit_code == 0, r.output
        assert "5 MB floor" in r.output
        assert "spatially ordered" not in r.output


class TestCliVerdicts:
    """End-to-end on a bbox-bearing file: one line per verdict, the numbers on it."""

    @pytest.fixture(autouse=True)
    def _cli_no_size_floor(self, monkeypatch):
        monkeypatch.setattr(mod, "SPATIAL_ORDER_MIN_FILE_BYTES", 0)

    @pytest.fixture
    def sorted_file(self, tmp_path):
        return _write_bbox_file(tmp_path / "sorted.parquet", 4000, 40)

    @pytest.fixture
    def unsorted_file(self, tmp_path):
        return _write_bbox_file(tmp_path / "unsorted.parquet", 4000, 40, shuffle_seed=0)

    def test_well_sorted_file_passes_with_its_numbers(self, sorted_file):
        r = _run(["spatial", sorted_file])

        assert r.exit_code == 0, r.output
        assert "Data appears to be spatially ordered" in r.output
        assert "efficiency 1.00" in r.output
        assert "a typical query reads" in r.output
        assert r.output.count("efficiency") == 1, "the numbers are printed once"

    def test_unsorted_file_fails_with_its_numbers(self, unsorted_file):
        r = _run(["spatial", unsorted_file])

        assert r.exit_code == 0, r.output
        assert "Data may not be optimally spatially ordered" in r.output
        assert "efficiency 0.00" in r.output
        assert "gpio sort hilbert" in r.output

    def test_verbose_adds_the_achievable_rate_and_the_area_sum(self, unsorted_file):
        r = _run(["spatial", unsorted_file, "--verbose"])

        assert "ideal tiling" in r.output
        assert "area sum" in r.output


class TestCliWithheldVerdict:
    """Below the floors the CLI prints the reason and no check mark (review of #774)."""

    @pytest.fixture(autouse=True)
    def _cli_no_size_floor(self, monkeypatch):
        monkeypatch.setattr(mod, "SPATIAL_ORDER_MIN_FILE_BYTES", 0)

    @pytest.fixture
    def small_file(self, tmp_path):
        # One row group: no verdict at any order.
        return _write_bbox_file(tmp_path / "small.parquet", 400, 1, shuffle_seed=1)

    def test_check_spatial_prints_the_reason_and_no_check_mark(self, small_file):
        r = _run(["spatial", small_file])

        assert r.exit_code == 0, r.output
        assert "not judged below 8 row groups" in r.output
        assert "✓" not in r.output.split("Pushdown")[0]
        assert "spatially ordered" not in r.output.split("Pushdown")[0]
        assert "Row groups:" in r.output

    def test_two_shuffled_row_groups_are_a_clear_failure(self, tmp_path):
        """PORTO-FMT-059 end to end: the case that used to print a green pass."""
        path = _write_bbox_file(tmp_path / "two.parquet", 400, 2, shuffle_seed=1)

        r = _run(["spatial", path])

        assert "Data may not be optimally spatially ordered" in r.output
        assert "efficiency 0.00" in r.output

    def test_check_all_prints_no_check_mark_either(self, small_file):
        r = _run(["all", small_file])

        assert r.exit_code == 0, r.output
        assert "not judged below 8 row groups" in r.output

    def test_a_withheld_verdict_is_not_a_failure_in_a_multi_file_run(self, tmp_path):
        d = tmp_path / "part"
        d.mkdir()
        for i in range(2):
            _write_bbox_file(d / f"p{i}.parquet", 400, 1, shuffle_seed=i)

        r = _run(["spatial", str(d), "--all-files"])

        assert r.exit_code == 0, r.output
        assert "2 passed" not in r.output, "a withheld verdict must not be counted as a pass"
        assert "failed" not in r.output
        assert "2 not judged" in r.output

    def test_fix_does_not_claim_the_file_is_ordered(self, small_file, tmp_path):
        out = tmp_path / "fixed.parquet"
        r = _run(["spatial", small_file, "--fix", "--fix-output", str(out)])

        assert r.exit_code == 0, r.output
        assert "already spatially ordered" not in r.output
        assert "not judged" in r.output
        assert not out.exists(), "nothing was judged, so nothing was rewritten"

    def test_fix_on_a_judged_sorted_file_says_no_fix_needed(self, tmp_path):
        path = _write_bbox_file(tmp_path / "sorted.parquet", 4000, 40)
        out = tmp_path / "fixed.parquet"

        r = _run(["spatial", path, "--fix", "--fix-output", str(out)])

        assert r.exit_code == 0, r.output
        assert "No fix needed - already spatially ordered" in r.output
        assert "Nothing to fix" in r.output, "an explicit --fix-output still gets a copy"


class TestWithheldVerdictReachesEverySurface:
    """The places a withheld verdict leaked as a pass, all through one predicate."""

    @pytest.fixture(autouse=True)
    def _cli_no_size_floor(self, monkeypatch):
        monkeypatch.setattr(mod, "SPATIAL_ORDER_MIN_FILE_BYTES", 0)

    def test_check_all_multi_file_counts_a_withheld_verdict_as_not_judged(self, tmp_path):
        d = tmp_path / "part"
        d.mkdir()
        for i in range(2):  # one ZSTD row group: every other check passes
            _write_bbox_file(d / f"p{i}.parquet", 400, 1, shuffle_seed=i)

        r = _run(["all", str(d), "--all-files"])

        assert r.exit_code == 0, r.output
        assert "2 passed" not in r.output, "a withheld verdict must not be counted as a pass"
        assert "2 not judged" in r.output, r.output

    def test_python_api_exposes_judged(self, tmp_path):
        import geoparquet_io as gpio

        path = _write_bbox_file(tmp_path / "one.parquet", 400, 1, shuffle_seed=1)

        assert gpio.read(path).check_spatial().judged() is False
        assert (
            gpio.read(_write_bbox_file(tmp_path / "s.parquet", 4000, 40)).check_spatial().judged()
        )

    def test_check_result_all_reports_judged_across_categories(self):
        from geoparquet_io.api.check import CheckResult

        withheld = CheckResult({"spatial": {"passed": True, "judged": False}}, check_type="all")
        judged = CheckResult(
            {"spatial": {"passed": True}, "bbox": {"passed": True}}, check_type="all"
        )

        assert withheld.judged() is False and withheld.passed() is True
        assert judged.judged() is True

    def test_python_api_reports_no_failure_and_says_why(self, tmp_path):
        import geoparquet_io as gpio

        path = _write_bbox_file(tmp_path / "one.parquet", 400, 1, shuffle_seed=1)
        r = gpio.read(path).check_spatial()

        assert r.passed() is True and r.failures() == []
        assert r.to_dict()["judged"] is False
        assert any("not judged" in w for w in r.warnings())

    def test_check_spatial_has_no_metric_knobs(self):
        import inspect

        from geoparquet_io.api.table import Table

        params = inspect.signature(Table.check_spatial).parameters
        assert set(params) == {"self", "sample_size", "limit_rows"}


def struct_point(x, y):
    import struct

    return struct.pack("<BI2d", 1, 1, x, y)


class TestFixKeepsTheFileJudgeable:
    """``--fix`` must not turn a judgeable file into an unjudgeable one (#1098)."""

    @pytest.fixture(autouse=True)
    def _cli_no_size_floor(self, monkeypatch):
        monkeypatch.setattr(mod, "SPATIAL_ORDER_MIN_FILE_BYTES", 0)

    def test_fix_keeps_at_least_the_input_row_group_count(self, tmp_path):
        import pyarrow.parquet as pq

        path = _write_bbox_file(tmp_path / "ten.parquet", 20_480, 10, shuffle_seed=0)
        out = tmp_path / "fixed.parquet"

        r = _run(["spatial", path, "--fix", "--fix-output", str(out)])

        assert r.exit_code == 0, r.output
        assert "Spatial ordering applied successfully" in r.output
        assert out.exists()
        assert pq.ParquetFile(out).metadata.num_row_groups >= 10

    def test_the_target_is_computed_from_the_footer(self, tmp_path):
        from geoparquet_io.core.check_fixes import _judgeable_row_group_rows

        path = _write_bbox_file(tmp_path / "ten.parquet", 20_480, 10, shuffle_seed=0)

        assert _judgeable_row_group_rows(path) == 2048

    def test_a_large_input_still_gets_the_facade_default(self, monkeypatch):
        from geoparquet_io.core import check_fixes

        monkeypatch.setattr(
            check_fixes,
            "_judgeable_row_group_rows",
            check_fixes._judgeable_row_group_rows,
        )
        monkeypatch.setattr(
            "geoparquet_io.core.duckdb_metadata.get_file_metadata",
            lambda *a, **k: {"num_rows": 10_000_000, "num_row_groups": 9},
        )

        from geoparquet_io.core.check_fixes import DEFAULT_ROW_GROUP_ROWS

        assert check_fixes._judgeable_row_group_rows("whatever.parquet") == DEFAULT_ROW_GROUP_ROWS


class TestSamplingFallbackSaysWhatItMeasures:
    """No bbox column -> consecutive-feature distances, a different test (review of #774)."""

    def test_cli_names_the_method_and_recommends_a_bbox_column(self, buildings_test_file):
        r = _run(["spatial", buildings_test_file])

        assert r.exit_code == 0, r.output
        assert "Consecutive features are spatially close (sampling method)" in r.output
        assert r.output.count("does not measure row-group pruning") == 1
        assert "gpio add bbox" in r.output
        assert "Data appears to be spatially ordered" not in r.output

    def test_failing_sampling_result_names_the_method_too(self, capsys):
        from geoparquet_io.cli.commands.check import _echo_spatial_order

        _echo_spatial_order({"ratio": 0.9, "passed": False, "method": "sampling"})

        out = capsys.readouterr().out
        assert "Consecutive features are not spatially close (sampling method)" in out
        assert "gpio sort hilbert" in out


class TestFooterCheckOwnsTheVerdict:
    """A file the footer check judged never reaches the sampling fallback."""

    @pytest.mark.parametrize(
        ("n_groups", "shuffle", "expect"),
        [(FLOOR + 2, 3, "judged"), (2, None, "withheld")],
        ids=["n=10-shuffled", "n=2-sorted"],
    )
    def test_a_bbox_file_never_runs_the_sampling_fallback(
        self, tmp_path, monkeypatch, n_groups, shuffle, expect
    ):
        path = _write_bbox_file(tmp_path / "f.parquet", 400, n_groups, shuffle_seed=shuffle)

        def _must_not_run(*args, **kwargs):
            raise AssertionError("the sampling fallback ran on a file with footer statistics")

        monkeypatch.setattr(mod, "SPATIAL_ORDER_MIN_FILE_BYTES", 0)
        monkeypatch.setattr(mod, "_calculate_consecutive_avg", _must_not_run)
        monkeypatch.setattr(mod, "_calculate_random_avg", _must_not_run)

        r = mod.check_spatial_order(path, 10, 100, verbose=False, return_results=True, quiet=True)

        assert r["method"] == "bbox_stats"
        assert r["judged"] is (expect == "judged")
        assert r["passed"] is (expect != "judged")

    def test_bbox_stats_failure_falls_back_to_native_stats(self, tmp_path, monkeypatch):
        """A malformed bbox column must fall through, not abort the check."""

        def _boom(*args, **kwargs):
            raise ValueError("malformed bbox column")

        monkeypatch.setattr(mod, "SPATIAL_ORDER_MIN_FILE_BYTES", 0)
        monkeypatch.setattr(mod, "check_spatial_order_bbox_stats", _boom)
        monkeypatch.setattr(
            mod, "check_bbox_structure", lambda *a, **k: {"bbox_column_name": "bbox"}
        )
        monkeypatch.setattr(mod, "get_per_row_group_native_geo_stats", lambda *a, **k: tiling(9))
        # module-level import, so patch the binding the function actually uses
        monkeypatch.setattr(mod, "find_primary_geometry_column", lambda *a, **k: "geometry")
        monkeypatch.setattr(mod, "resolve_file_url", lambda p, v=False: str(p))

        out = mod.check_spatial_order(
            str(tmp_path / "x.parquet"),
            random_sample_size=10,
            limit_rows=100,
            verbose=True,
            return_results=True,
            quiet=True,
        )

        assert out["judged"] and out["passed"], "fell through to the native-stats path"


class TestPushdownGoesThroughCore:
    """``check spatial`` and ``Table.check_spatial_pushdown()`` reach the verdict the same way."""

    @pytest.fixture(autouse=True)
    def _cli_no_size_floor(self, monkeypatch):
        monkeypatch.setattr(mod, "SPATIAL_ORDER_MIN_FILE_BYTES", 0)

    def test_cli_pushdown_matches_the_api_on_a_bbox_file(self, tmp_path):
        import geoparquet_io as gpio

        path = _write_bbox_file(tmp_path / "f.parquet", 4000, 40, shuffle_seed=0)

        cli = _run(["spatial", path]).output
        api = gpio.read(path).check_spatial_pushdown().to_dict()

        assert api["num_row_groups"] == 40 and api["passed"] is False
        assert "Row groups: 40" in cli and "Low pushdown efficiency" in cli
        assert api["issues"], "the core verdict carries its issues"

    def test_api_pushdown_reads_native_geo_stats_like_the_cli(self, tmp_path, monkeypatch):
        """A GeoParquet 2.0 file has no bbox column; its native statistics count."""
        monkeypatch.setattr(mod, "check_bbox_structure", lambda *a, **k: {"bbox_column_name": None})
        monkeypatch.setattr(mod, "find_primary_geometry_column", lambda *a, **k: "geometry")
        monkeypatch.setattr(mod, "get_per_row_group_native_geo_stats", lambda *a, **k: tiling(9))
        monkeypatch.setattr(mod, "get_per_row_group_weights", lambda *a, **k: {})

        r = mod.check_spatial_pushdown_readiness(str(tmp_path / "native.parquet"))

        assert r["has_geo_bbox"] is True and r["num_row_groups"] == 9
        assert r["passed"] is True


class TestBboxColumnIsTheCoveringsColumn:
    """The verdict reads the column ``geo.covering.bbox`` names, not the first ``*bbox`` struct."""

    @pytest.fixture(autouse=True)
    def _cli_no_size_floor(self, monkeypatch):
        monkeypatch.setattr(mod, "SPATIAL_ORDER_MIN_FILE_BYTES", 0)

    def test_a_stale_bbox_column_ahead_of_the_real_one_is_ignored(self, tmp_path):
        """A sorted-looking decoy first in the schema must not pass a shuffled file."""
        path = _write_bbox_file(tmp_path / "f.parquet", 4000, 40, shuffle_seed=0, decoy="old_bbox")

        r = _run(["spatial", path])

        assert "may not be optimally spatially ordered" in r.output, r.output

    def test_a_stale_bbox_column_does_not_make_fix_rewrite_a_sorted_file(self, tmp_path):
        import pyarrow.parquet as pq

        path = _write_bbox_file(tmp_path / "f.parquet", 4000, 40, decoy="old_bbox")
        # the decoy carries the boxes of a shuffled file: the heuristic's pick
        t = pq.read_table(path)
        pq.write_table(
            t.drop(["old_bbox"])
            .append_column("old_bbox", t.column("bbox"))
            .select(["geometry", "old_bbox", "bbox"]),
            path,
            row_group_size=100,
        )
        out = tmp_path / "fixed.parquet"

        r = _run(["spatial", path, "--fix", "--fix-output", str(out)])

        assert "No fix needed - already spatially ordered" in r.output, r.output

    def test_a_covering_column_with_an_unconventional_name_is_found(self, tmp_path):
        """``--bbox-name bounding_box`` is gpio's own output; it must not fall to sampling."""
        path = _write_bbox_file(tmp_path / "f.parquet", 4000, 40, bbox_name="bounding_box")

        r = _run(["spatial", path])

        assert "sampling method" not in r.output
        assert "Data appears to be spatially ordered" in r.output


class TestUntouchedConsumersSeeNoFailure:
    """A withheld verdict must read as not-a-failure to code that only knows ``passed``."""

    @pytest.fixture(autouse=True)
    def _cli_no_size_floor(self, monkeypatch):
        monkeypatch.setattr(mod, "SPATIAL_ORDER_MIN_FILE_BYTES", 0)

    def test_check_optimization_does_not_tell_a_withheld_file_to_resort(self, tmp_path):
        """A one-group file has no verdict, so no re-sort advice (#755 downstream)."""
        from geoparquet_io.core.check_optimization import check_optimization

        path = _write_bbox_file(tmp_path / "one.parquet", 400, 1, shuffle_seed=1)

        checks = check_optimization(path, return_results=True, quiet=True)["checks"]

        assert checks["spatial_sorting"]["passed"] is True

    def test_check_optimization_fails_a_clearly_shuffled_small_file(self, tmp_path):
        """Two shuffled groups are a clear failure now (PORTO-FMT-059)."""
        from geoparquet_io.core.check_optimization import check_optimization

        path = _write_bbox_file(tmp_path / "two.parquet", 400, 2, shuffle_seed=1)

        checks = check_optimization(path, return_results=True, quiet=True)["checks"]

        assert checks["spatial_sorting"]["passed"] is False
