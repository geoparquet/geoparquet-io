"""Tests for spatial filter pushdown readiness metric."""

import pytest

from geoparquet_io.core.check_spatial_order import (
    _compute_data_extent,
    _compute_skip_rate_for_query,
    _generate_sample_query_bboxes,
    check_spatial_pushdown_readiness,
)


class TestComputeDataExtent:
    """Tests for _compute_data_extent helper."""

    def test_single_row_group(self):
        """Extent of a single RG is just its bbox."""
        bboxes = [{"row_group_id": 0, "xmin": 1.0, "ymin": 2.0, "xmax": 3.0, "ymax": 4.0}]
        extent = _compute_data_extent(bboxes, geographic=True)
        assert extent == {"xmin": 1.0, "ymin": 2.0, "xmax": 3.0, "ymax": 4.0}

    def test_multiple_row_groups(self):
        """Extent is the union of all RG bboxes."""
        bboxes = [
            {"row_group_id": 0, "xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0},
            {"row_group_id": 1, "xmin": 5.0, "ymin": 5.0, "xmax": 20.0, "ymax": 20.0},
            {"row_group_id": 2, "xmin": -5.0, "ymin": -5.0, "xmax": 3.0, "ymax": 3.0},
        ]
        extent = _compute_data_extent(bboxes, geographic=True)
        assert extent == {"xmin": -5.0, "ymin": -5.0, "xmax": 20.0, "ymax": 20.0}

    def test_empty_list_raises(self):
        """Empty bbox list should raise ValueError."""
        with pytest.raises(ValueError, match="No row group bboxes"):
            _compute_data_extent([], geographic=True)


class TestGenerateSampleQueryBboxes:
    """Tests for _generate_sample_query_bboxes helper."""

    def test_generates_requested_count(self):
        """Should generate the requested number of sample bboxes."""
        extent = {"xmin": 0.0, "ymin": 0.0, "xmax": 100.0, "ymax": 100.0}
        samples = _generate_sample_query_bboxes(extent, num_samples=5, query_fraction=0.1)
        assert len(samples) == 5

    def test_sample_within_extent(self):
        """Generated bboxes should be within the data extent."""
        extent = {"xmin": -180.0, "ymin": -90.0, "xmax": 180.0, "ymax": 90.0}
        samples = _generate_sample_query_bboxes(extent, num_samples=10, query_fraction=0.1)
        for s in samples:
            assert s["xmin"] >= extent["xmin"]
            assert s["ymin"] >= extent["ymin"]
            assert s["xmax"] <= extent["xmax"]
            assert s["ymax"] <= extent["ymax"]
            assert s["xmin"] < s["xmax"]
            assert s["ymin"] < s["ymax"]

    def test_query_fraction_affects_size(self):
        """Larger query_fraction means larger sample bboxes."""
        extent = {"xmin": 0.0, "ymin": 0.0, "xmax": 100.0, "ymax": 100.0}
        small = _generate_sample_query_bboxes(extent, num_samples=1, query_fraction=0.05, seed=42)
        large = _generate_sample_query_bboxes(extent, num_samples=1, query_fraction=0.5, seed=42)
        small_area = (small[0]["xmax"] - small[0]["xmin"]) * (small[0]["ymax"] - small[0]["ymin"])
        large_area = (large[0]["xmax"] - large[0]["xmin"]) * (large[0]["ymax"] - large[0]["ymin"])
        assert large_area > small_area

    def test_deterministic_with_seed(self):
        """Same seed produces same bboxes."""
        extent = {"xmin": 0.0, "ymin": 0.0, "xmax": 100.0, "ymax": 100.0}
        a = _generate_sample_query_bboxes(extent, num_samples=3, query_fraction=0.1, seed=123)
        b = _generate_sample_query_bboxes(extent, num_samples=3, query_fraction=0.1, seed=123)
        assert a == b


class TestComputeSkipRateForQuery:
    """Tests for _compute_skip_rate_for_query helper."""

    def test_all_overlap(self):
        """Query covering everything skips nothing."""
        query_bbox = {"xmin": 0.0, "ymin": 0.0, "xmax": 100.0, "ymax": 100.0}
        rg_bboxes = [
            {"row_group_id": 0, "xmin": 10.0, "ymin": 10.0, "xmax": 20.0, "ymax": 20.0},
            {"row_group_id": 1, "xmin": 50.0, "ymin": 50.0, "xmax": 60.0, "ymax": 60.0},
        ]
        skip_rate = _compute_skip_rate_for_query(query_bbox, rg_bboxes)
        assert skip_rate == 0.0

    def test_none_overlap(self):
        """Query outside all RGs skips all."""
        query_bbox = {"xmin": 200.0, "ymin": 200.0, "xmax": 210.0, "ymax": 210.0}
        rg_bboxes = [
            {"row_group_id": 0, "xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0},
            {"row_group_id": 1, "xmin": 20.0, "ymin": 20.0, "xmax": 30.0, "ymax": 30.0},
        ]
        skip_rate = _compute_skip_rate_for_query(query_bbox, rg_bboxes)
        assert skip_rate == 1.0

    def test_partial_overlap(self):
        """Query overlapping 1 of 4 RGs gives 75% skip rate."""
        query_bbox = {"xmin": 0.0, "ymin": 0.0, "xmax": 5.0, "ymax": 5.0}
        rg_bboxes = [
            {"row_group_id": 0, "xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0},
            {"row_group_id": 1, "xmin": 20.0, "ymin": 20.0, "xmax": 30.0, "ymax": 30.0},
            {"row_group_id": 2, "xmin": 40.0, "ymin": 40.0, "xmax": 50.0, "ymax": 50.0},
            {"row_group_id": 3, "xmin": 60.0, "ymin": 60.0, "xmax": 70.0, "ymax": 70.0},
        ]
        skip_rate = _compute_skip_rate_for_query(query_bbox, rg_bboxes)
        assert skip_rate == 0.75

    def test_single_rg_overlap(self):
        """With 1 RG, either 0% or 100% skip."""
        query_bbox = {"xmin": 0.0, "ymin": 0.0, "xmax": 5.0, "ymax": 5.0}
        rg_bboxes = [
            {"row_group_id": 0, "xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0},
        ]
        skip_rate = _compute_skip_rate_for_query(query_bbox, rg_bboxes)
        assert skip_rate == 0.0


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
        # Create mock row group bboxes that are spatially disjoint
        mock_bboxes = [
            {"row_group_id": i, "xmin": i * 10.0, "ymin": 0.0, "xmax": (i + 1) * 10.0, "ymax": 10.0}
            for i in range(10)
        ]
        # A query covering 10% of extent should skip ~90% of RGs
        extent = _compute_data_extent(mock_bboxes, geographic=True)
        samples = _generate_sample_query_bboxes(extent, num_samples=20, query_fraction=0.1, seed=42)
        skip_rates = [_compute_skip_rate_for_query(s, mock_bboxes) for s in samples]
        avg_skip = sum(skip_rates) / len(skip_rates)
        # With 10 disjoint RGs and 10% query, expect high skip rate
        assert avg_skip >= 0.5

    def test_poorly_sorted_data_low_skip_rate(self):
        """Poorly sorted data (all overlapping RGs) should have low skip rate."""
        # Create mock row group bboxes that all cover the same area
        mock_bboxes = [
            {"row_group_id": i, "xmin": 0.0, "ymin": 0.0, "xmax": 100.0, "ymax": 100.0}
            for i in range(10)
        ]
        # Any query overlapping anything will hit all 10 RGs
        extent = _compute_data_extent(mock_bboxes, geographic=True)
        samples = _generate_sample_query_bboxes(extent, num_samples=20, query_fraction=0.1, seed=42)
        skip_rates = [_compute_skip_rate_for_query(s, mock_bboxes) for s in samples]
        avg_skip = sum(skip_rates) / len(skip_rates)
        # All RGs overlap, so skip rate should be 0
        assert avg_skip == 0.0


@pytest.mark.parametrize("with_stats", [1, 2])
def test_cli_reports_all_three_row_groups_including_one_without_bbox(tmp_path, with_stats):
    import json

    import pyarrow as pa
    import pyarrow.parquet as pq
    from click.testing import CliRunner
    from shapely import Point, to_wkb

    from geoparquet_io.cli.main import cli

    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "covering": {"bbox": {k: ["bbox", k] for k in ("xmin", "ymin", "xmax", "ymax")}},
            }
        },
    }
    bbox = pa.array(
        [
            {"xmin": 0.0, "ymin": 0.0, "xmax": 0.0, "ymax": 0.0},
            None,
            ({"xmin": 2.0, "ymin": 2.0, "xmax": 2.0, "ymax": 2.0} if with_stats == 2 else None),
        ],
        type=pa.struct([(k, pa.float64()) for k in ("xmin", "ymin", "xmax", "ymax")]),
    )
    table = pa.table({"geometry": [to_wkb(Point(0, 0)), None, to_wkb(Point(2, 2))], "bbox": bbox})
    table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    path = tmp_path / "three.parquet"
    pq.write_table(table, path, row_group_size=1)
    assert pq.ParquetFile(path).metadata.num_row_groups == 3
    result = CliRunner().invoke(cli, ["check", "spatial", str(path)])
    assert "Row groups: 3" in result.output, result.output

    from geoparquet_io.core.check_spatial_order import check_spatial_pushdown_readiness

    readiness = check_spatial_pushdown_readiness(str(path))
    assert readiness["num_row_groups"] == 3
    assert "Single row group provides no pushdown benefit" not in readiness["issues"]
    assert readiness["estimated_skip_rate"] <= with_stats / 3
