"""Tests for spatial filter pushdown readiness metric."""

import pytest

from geoparquet_io.core.check_spatial_order import (
    _axis_hit_probability,
    _compute_data_extent,
    _expected_skip_rate,
    check_spatial_pushdown_readiness,
)


class TestComputeDataExtent:
    """Tests for _compute_data_extent helper."""

    def test_single_row_group(self):
        """Extent of a single RG is just its bbox."""
        bboxes = [{"row_group_id": 0, "xmin": 1.0, "ymin": 2.0, "xmax": 3.0, "ymax": 4.0}]
        extent = _compute_data_extent(bboxes)
        assert extent == {"xmin": 1.0, "ymin": 2.0, "xmax": 3.0, "ymax": 4.0}

    def test_multiple_row_groups(self):
        """Extent is the union of all RG bboxes."""
        bboxes = [
            {"row_group_id": 0, "xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0},
            {"row_group_id": 1, "xmin": 5.0, "ymin": 5.0, "xmax": 20.0, "ymax": 20.0},
            {"row_group_id": 2, "xmin": -5.0, "ymin": -5.0, "xmax": 3.0, "ymax": 3.0},
        ]
        extent = _compute_data_extent(bboxes)
        assert extent == {"xmin": -5.0, "ymin": -5.0, "xmax": 20.0, "ymax": 20.0}

    def test_empty_list_raises(self):
        """Empty bbox list should raise ValueError."""
        with pytest.raises(ValueError, match="No row group bboxes"):
            _compute_data_extent([])


class TestAxisHitProbability:
    """Tests for the one-axis closed form the expected skip rate is built from."""

    def test_box_spanning_the_extent_is_always_hit(self):
        assert _axis_hit_probability(0.0, 100.0, 0.0, 100.0, 10.0) == 1.0

    def test_box_at_the_low_edge(self):
        """Window low edge uniform on [0, 90]; hits [0, 10] when it lies in [-10, 10] -> 10/90."""
        assert _axis_hit_probability(0.0, 10.0, 0.0, 100.0, 10.0) == pytest.approx(10 / 90)

    def test_interior_box_counts_the_window_width(self):
        """[40, 50] is hit by any low edge in [30, 50] -> 20/90."""
        assert _axis_hit_probability(40.0, 50.0, 0.0, 100.0, 10.0) == pytest.approx(20 / 90)

    def test_larger_window_hits_more(self):
        small = _axis_hit_probability(40.0, 50.0, 0.0, 100.0, 5.0)
        large = _axis_hit_probability(40.0, 50.0, 0.0, 100.0, 50.0)
        assert large > small

    def test_degenerate_axis_is_always_hit(self):
        """Zero-width extent, or a window as wide as the extent: every placement hits."""
        assert _axis_hit_probability(5.0, 5.0, 5.0, 5.0, 0.0) == 1.0
        assert _axis_hit_probability(0.0, 10.0, 0.0, 100.0, 100.0) == 1.0


class TestExpectedSkipRate:
    """Tests for _expected_skip_rate, the closed form of the sampled skip rate."""

    def test_boxes_covering_the_extent_skip_nothing(self):
        extent = {"xmin": 0.0, "ymin": 0.0, "xmax": 100.0, "ymax": 100.0}
        rg_bboxes = [{"row_group_id": i, **extent} for i in range(2)]
        assert _expected_skip_rate(rg_bboxes, extent, 0.1) == 0.0

    def test_disjoint_strips_skip_most(self):
        """Ten disjoint strips across a 10% window: each is hit with P = 20/90."""
        rg_bboxes = [
            {"row_group_id": i, "xmin": i * 10.0, "ymin": 0.0, "xmax": (i + 1) * 10.0, "ymax": 10.0}
            for i in range(10)
        ]
        extent = _compute_data_extent(rg_bboxes)
        # x: interior strips 20/90, the two edge strips 10/90; y is degenerate (P=1)
        expected_hit = (8 * 20 / 90 + 2 * 10 / 90) / 10
        assert _expected_skip_rate(rg_bboxes, extent, 0.1) == pytest.approx(1 - expected_hit)

    def test_single_box_equal_to_the_extent(self):
        extent = {"xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0}
        assert _expected_skip_rate([{"row_group_id": 0, **extent}], extent, 0.5) == 0.0


class TestCheckSpatialPushdownReadiness:
    """Tests for the main check_spatial_pushdown_readiness function."""

    def test_returns_dict_structure(self, places_test_file):
        """Test that function returns proper dict structure."""
        result = check_spatial_pushdown_readiness(places_test_file)
        assert isinstance(result, dict)
        assert "has_geo_bbox" in result
        assert "num_row_groups" in result
        assert "estimated_skip_rate" in result
        assert "issues" in result
        assert "recommendations" in result
        assert "passed" in result

    def test_file_with_bbox(self, places_test_file):
        """Test with a file that has bbox columns."""
        result = check_spatial_pushdown_readiness(places_test_file)
        assert result["has_geo_bbox"] is True
        assert result["num_row_groups"] >= 1
        assert isinstance(result["estimated_skip_rate"], float)
        assert 0.0 <= result["estimated_skip_rate"] <= 1.0

    def test_file_without_bbox(self, buildings_test_file):
        """Test with a file that lacks bbox columns."""
        result = check_spatial_pushdown_readiness(buildings_test_file)
        assert result["has_geo_bbox"] is False
        assert result["estimated_skip_rate"] == 0.0
        assert result["passed"] is False
        assert any("bbox" in i.lower() or "geo_bbox" in i.lower() for i in result["issues"])

    def test_verbose_mode(self, places_test_file):
        """Test with verbose flag."""
        result = check_spatial_pushdown_readiness(places_test_file, verbose=True)
        assert isinstance(result, dict)
        assert "has_geo_bbox" in result

    def test_avg_bbox_area_ratio(self, places_test_file):
        """Test that avg_bbox_area_ratio is present and in range."""
        result = check_spatial_pushdown_readiness(places_test_file)
        if result["has_geo_bbox"] and result["num_row_groups"] > 0:
            assert "avg_bbox_area_ratio" in result
            assert isinstance(result["avg_bbox_area_ratio"], float)
            assert result["avg_bbox_area_ratio"] >= 0.0


class TestCheckSpatialPushdownReadinessUnit:
    """Unit tests using mock data for pushdown readiness."""

    def test_well_sorted_data_high_skip_rate(self):
        """Well-sorted data (non-overlapping RGs) should have high skip rate."""
        mock_bboxes = [
            {"row_group_id": i, "xmin": i * 10.0, "ymin": 0.0, "xmax": (i + 1) * 10.0, "ymax": 10.0}
            for i in range(10)
        ]
        extent = _compute_data_extent(mock_bboxes)
        # With 10 disjoint RGs and a 10% query, expect a high skip rate
        assert _expected_skip_rate(mock_bboxes, extent, 0.1) >= 0.5

    def test_poorly_sorted_data_low_skip_rate(self):
        """Poorly sorted data (all overlapping RGs) should have low skip rate."""
        mock_bboxes = [
            {"row_group_id": i, "xmin": 0.0, "ymin": 0.0, "xmax": 100.0, "ymax": 100.0}
            for i in range(10)
        ]
        extent = _compute_data_extent(mock_bboxes)
        # Any query overlapping anything will hit all 10 RGs
        assert _expected_skip_rate(mock_bboxes, extent, 0.1) == 0.0
