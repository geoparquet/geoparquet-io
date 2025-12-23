"""Tests for check_spatial_order helper functions."""

from geoparquet_io.core.check_spatial_order import _build_results_dict


class TestBuildResultsDict:
    """Tests for _build_results_dict function."""

    def test_good_spatial_ordering(self):
        """Test with good spatial ordering (ratio < 0.5)."""
        result = _build_results_dict(ratio=0.3, consecutive_avg=10.0, random_avg=33.33)

        assert result["passed"] is True
        assert result["ratio"] == 0.3
        assert result["consecutive_avg"] == 10.0
        assert result["random_avg"] == 33.33
        assert result["issues"] == []
        assert result["recommendations"] == []
        assert result["fix_available"] is False

    def test_poor_spatial_ordering(self):
        """Test with poor spatial ordering (ratio >= 0.5)."""
        result = _build_results_dict(ratio=0.8, consecutive_avg=40.0, random_avg=50.0)

        assert result["passed"] is False
        assert result["ratio"] == 0.8
        assert len(result["issues"]) == 1
        assert "Poor spatial ordering" in result["issues"][0]
        assert len(result["recommendations"]) == 1
        assert "Hilbert" in result["recommendations"][0]
        assert result["fix_available"] is True

    def test_none_ratio(self):
        """Test with None ratio (couldn't calculate)."""
        result = _build_results_dict(ratio=None, consecutive_avg=None, random_avg=None)

        assert result["passed"] is False
        assert result["ratio"] is None
        assert result["issues"] == []
        assert result["fix_available"] is True

    def test_edge_case_ratio_exactly_0_5(self):
        """Test with ratio exactly at threshold."""
        result = _build_results_dict(ratio=0.5, consecutive_avg=25.0, random_avg=50.0)

        assert result["passed"] is False  # >= 0.5 is not passed
        assert len(result["issues"]) == 1

    def test_ratio_just_below_threshold(self):
        """Test with ratio just below threshold."""
        result = _build_results_dict(ratio=0.499, consecutive_avg=24.95, random_avg=50.0)

        assert result["passed"] is True
        assert result["issues"] == []
