"""Tests for the combined spatial query optimization check."""


class TestCheckOptimization:
    """Tests for check_optimization core function."""

    def test_returns_dict_with_expected_keys(self, places_test_file):
        """check_optimization should return a dict with standard check keys."""
        from geoparquet_io.core.check_optimization import check_optimization

        result = check_optimization(places_test_file, return_results=True, quiet=True)

        assert isinstance(result, dict)
        assert "passed" in result
        assert "score" in result
        assert "total_checks" in result
        assert "checks" in result
        assert "level" in result
        assert "issues" in result
        assert "recommendations" in result

    def test_checks_dict_has_expected_subchecks(self, places_test_file):
        """Result should include all five sub-check categories."""
        from geoparquet_io.core.check_optimization import check_optimization

        result = check_optimization(places_test_file, return_results=True, quiet=True)

        checks = result["checks"]
        assert "native_geo_types" in checks
        assert "geo_bbox_stats" in checks
        assert "spatial_sorting" in checks
        assert "row_group_size" in checks
        assert "compression" in checks

    def test_spatial_sorting_is_not_a_failure_on_a_one_row_group_file(self):
        """The spatial-order check withholds its verdict below eight row groups.

        A withheld verdict is not a failure: this sub-check reads only
        ``passed`` from it and must not tell every small file to re-sort. It
        did once, when the verdict was carried as ``None`` (review of #774).
        """
        from geoparquet_io.core.check_optimization import check_optimization

        result = check_optimization(
            "tests/data/fields_pgo_5070_snappy.parquet", return_results=True, quiet=True
        )

        assert result["checks"]["spatial_sorting"]["passed"] is True
        assert not any("spatial sorting" in r.lower() for r in result["recommendations"])

    def test_each_subcheck_has_passed_and_detail(self, places_test_file):
        """Each sub-check should have passed (bool) and detail (str)."""
        from geoparquet_io.core.check_optimization import check_optimization

        result = check_optimization(places_test_file, return_results=True, quiet=True)

        for name, check in result["checks"].items():
            assert "passed" in check, f"Sub-check '{name}' missing 'passed'"
            assert "detail" in check, f"Sub-check '{name}' missing 'detail'"
            assert isinstance(check["passed"], bool), f"Sub-check '{name}' passed not bool"
            assert isinstance(check["detail"], str), f"Sub-check '{name}' detail not str"

    def test_score_matches_passed_count(self, places_test_file):
        """score should equal the number of passed sub-checks."""
        from geoparquet_io.core.check_optimization import check_optimization

        result = check_optimization(places_test_file, return_results=True, quiet=True)

        expected_score = sum(1 for c in result["checks"].values() if c["passed"])
        assert result["score"] == expected_score

    def test_total_checks_is_five(self, places_test_file):
        """There should always be exactly 5 sub-checks."""
        from geoparquet_io.core.check_optimization import check_optimization

        result = check_optimization(places_test_file, return_results=True, quiet=True)

        assert result["total_checks"] == 5

    def test_level_fully_optimized_when_all_pass(self, places_test_file):
        """Level should be 'fully_optimized' when score is 5."""
        from geoparquet_io.core.check_optimization import check_optimization

        result = check_optimization(places_test_file, return_results=True, quiet=True)

        if result["score"] == 5:
            assert result["level"] == "fully_optimized"

    def test_level_not_optimized_when_low_score(self):
        """Level should be 'not_optimized' when score is 0-2."""
        from geoparquet_io.core.check_optimization import _score_to_level

        assert _score_to_level(0) == "not_optimized"
        assert _score_to_level(1) == "not_optimized"
        assert _score_to_level(2) == "not_optimized"

    def test_level_partially_optimized_when_mid_score(self):
        """Level should be 'partially_optimized' when score is 3-4."""
        from geoparquet_io.core.check_optimization import _score_to_level

        assert _score_to_level(3) == "partially_optimized"
        assert _score_to_level(4) == "partially_optimized"

    def test_level_fully_optimized_when_max_score(self):
        """Level should be 'fully_optimized' when score is 5."""
        from geoparquet_io.core.check_optimization import _score_to_level

        assert _score_to_level(5) == "fully_optimized"

    def test_issues_populated_for_failing_checks(self, places_test_file):
        """Issues list should have entries for each failing sub-check."""
        from geoparquet_io.core.check_optimization import check_optimization

        result = check_optimization(places_test_file, return_results=True, quiet=True)

        failing_count = result["total_checks"] - result["score"]
        # issues should have at least one entry per failing check
        assert len(result["issues"]) >= failing_count

    def test_recommendations_populated_for_failing_checks(self, places_test_file):
        """Recommendations should have entries for each failing sub-check."""
        from geoparquet_io.core.check_optimization import check_optimization

        result = check_optimization(places_test_file, return_results=True, quiet=True)

        failing_count = result["total_checks"] - result["score"]
        assert len(result["recommendations"]) >= failing_count

    def test_passed_true_only_when_fully_optimized(self, places_test_file):
        """passed should be True only when all 5 checks pass."""
        from geoparquet_io.core.check_optimization import check_optimization

        result = check_optimization(places_test_file, return_results=True, quiet=True)

        if result["score"] == 5:
            assert result["passed"] is True
        else:
            assert result["passed"] is False


class TestCheckOptimizationV2File:
    """Tests with GeoParquet 2.0 file (should pass native_geo_types and geo_bbox_stats)."""

    def test_native_geo_types_passes_for_v2(self, fields_v2_file):
        """GeoParquet 2.0 file should pass native_geo_types check."""
        from geoparquet_io.core.check_optimization import check_optimization

        result = check_optimization(fields_v2_file, return_results=True, quiet=True)

        assert result["checks"]["native_geo_types"]["passed"] is True

    def test_compression_passes_for_zstd_file(self, fields_v2_file):
        """File with ZSTD compression should pass compression check."""
        from geoparquet_io.core.check_optimization import check_optimization

        result = check_optimization(fields_v2_file, return_results=True, quiet=True)

        assert result["checks"]["compression"]["passed"] is True


class TestCheckOptimizationV1File:
    """Tests with GeoParquet 1.x file (places_test is v1)."""

    def test_native_geo_types_may_fail_for_v1(self, places_test_file):
        """GeoParquet 1.x file without native types should fail native_geo_types check."""
        from geoparquet_io.core.check_optimization import check_optimization

        result = check_optimization(places_test_file, return_results=True, quiet=True)

        # places_test.parquet is v1, so native_geo_types should fail
        assert result["checks"]["native_geo_types"]["passed"] is False

    def test_recommendations_include_upgrade_for_v1(self, places_test_file):
        """V1 file should get recommendation to upgrade."""
        from geoparquet_io.core.check_optimization import check_optimization

        result = check_optimization(places_test_file, return_results=True, quiet=True)

        # Should recommend upgrading to v2 for native geo types
        recs = result["recommendations"]
        assert any("2.0" in r or "native" in r.lower() for r in recs)


class TestCheckOptimizationQuietMode:
    """Test quiet mode suppresses output."""

    def test_quiet_mode_returns_results(self, places_test_file):
        """Quiet mode should still return results when return_results=True."""
        from geoparquet_io.core.check_optimization import check_optimization

        result = check_optimization(places_test_file, return_results=True, quiet=True)

        assert result is not None
        assert isinstance(result, dict)

    def test_non_quiet_mode_returns_none_without_return_results(self, places_test_file):
        """Without return_results, function returns None (just prints)."""
        from geoparquet_io.core.check_optimization import check_optimization

        result = check_optimization(places_test_file, return_results=False, quiet=True)

        assert result is None


class TestCheckOptimizationCLI:
    """Tests for the CLI command."""

    def test_cli_runs_on_file(self, places_test_file):
        """CLI should run successfully on a valid parquet file."""
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["check", "optimization", places_test_file])
        assert result.exit_code == 0


class TestCheckOptimizationAPI:
    """Tests for the Python API."""

    def test_table_check_optimization_method(self, places_test_file):
        """Table.check_optimization() should return a CheckResult."""
        import geoparquet_io as gpio
        from geoparquet_io.api.check import CheckResult

        table = gpio.read(places_test_file)
        result = table.check_optimization()

        assert isinstance(result, CheckResult)

    def test_table_check_optimization_has_score(self, places_test_file):
        """CheckResult from check_optimization should expose score in to_dict()."""
        import geoparquet_io as gpio

        table = gpio.read(places_test_file)
        result = table.check_optimization()
        d = result.to_dict()

        assert "score" in d
        assert "level" in d
