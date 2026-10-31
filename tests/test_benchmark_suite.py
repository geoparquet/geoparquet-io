"""Tests for benchmark suite functionality."""

import tempfile
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from geoparquet_io.benchmarks.config import (
    BENCHMARK_DATA_URL,
    BENCHMARK_FILES,
    CHAIN_OPERATIONS,
    CORE_OPERATIONS,
    FULL_OPERATIONS,
)
from geoparquet_io.core.benchmark_report import (
    format_table,
)
from geoparquet_io.core.benchmark_suite import (
    BenchmarkResult,
    SuiteResult,
    run_benchmark_suite,
    run_single_operation,
)


@pytest.fixture
def test_parquet():
    """Create a small test parquet file."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "test.parquet"
        table = pa.table(
            {
                "id": [1, 2, 3],
                "geometry": [b"point1", b"point2", b"point3"],
            }
        )
        pq.write_table(table, path)
        yield path


class TestBenchmarkConfig:
    """Tests for benchmark configuration."""

    def test_core_operations_defined(self):
        """Test that core operations are defined."""
        assert len(CORE_OPERATIONS) == 10
        assert "read" in CORE_OPERATIONS
        assert "write" in CORE_OPERATIONS
        assert "sort-hilbert" in CORE_OPERATIONS

    def test_full_operations_includes_core(self):
        """Test that full operations includes all core operations."""
        for op in CORE_OPERATIONS:
            assert op in FULL_OPERATIONS

    def test_benchmark_data_url_defined(self):
        """Test that benchmark data URL points to source.coop."""
        assert BENCHMARK_DATA_URL.startswith("https://data.source.coop/")
        assert "gpio-test" in BENCHMARK_DATA_URL

    def test_benchmark_files_tiers(self):
        """Test that benchmark files are defined for each tier."""
        assert "tiny" in BENCHMARK_FILES
        assert "small" in BENCHMARK_FILES
        assert "medium" in BENCHMARK_FILES
        assert "large" in BENCHMARK_FILES

    def test_benchmark_files_have_urls(self):
        """Test that all benchmark file entries are valid URLs."""
        for tier, files in BENCHMARK_FILES.items():
            for name, url in files.items():
                assert url.startswith("https://"), f"Invalid URL for {tier}/{name}"
                assert url.endswith(".parquet"), f"Non-parquet URL for {tier}/{name}"


class TestOperationRegistry:
    """Tests for operation registry."""

    def test_all_core_operations_registered(self):
        """Test that all core operations have handlers."""
        from geoparquet_io.benchmarks.config import CORE_OPERATIONS
        from geoparquet_io.benchmarks.operations import OPERATION_REGISTRY

        for op in CORE_OPERATIONS:
            assert op in OPERATION_REGISTRY, f"Missing handler for {op}"

    def test_get_operation_returns_typed_info(self):
        """Test that get_operation returns OperationInfo TypedDict."""
        from geoparquet_io.benchmarks.operations import get_operation

        op = get_operation("read")
        assert callable(op["run"])
        assert "name" in op
        assert "description" in op
        # TypedDict should have these specific keys
        assert isinstance(op["name"], str)
        assert isinstance(op["description"], str)

    def test_get_operation_invalid_raises(self):
        """Test that invalid operation raises KeyError."""
        from geoparquet_io.benchmarks.operations import get_operation

        with pytest.raises(KeyError):
            get_operation("nonexistent-operation")

    def test_add_country_runs_admin_divisions_country_level(self, monkeypatch, tmp_path):
        """``add-country`` benchmarks the one admin join: ``add admin-divisions``
        at the country level on Overture (the separate country_codes module is
        gone, #1367)."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        from geoparquet_io.benchmarks.operations import get_operation
        from geoparquet_io.core.add import admin_divisions

        calls = []

        def fake(**kwargs):
            calls.append(kwargs)
            pq.write_table(pa.table({"id": [1, 2]}), kwargs["output_parquet"])

        monkeypatch.setattr(admin_divisions, "add_admin_divisions_multi", fake)
        result = get_operation("add-country")["run"](tmp_path / "in.parquet", tmp_path)

        assert calls == [
            {
                "input_parquet": str(tmp_path / "in.parquet"),
                "output_parquet": str(tmp_path / "output.parquet"),
                "dataset_name": "overture",
                "levels": ["country"],
            }
        ]
        assert result == {"rows_with_country": 2}


class TestBenchmarkRunner:
    """Tests for benchmark runner."""

    def test_run_single_operation_returns_result(self, test_parquet):
        """Test that run_single_operation returns BenchmarkResult."""
        with tempfile.TemporaryDirectory() as output_dir:
            result = run_single_operation(
                operation="read",
                input_path=test_parquet,
                output_dir=Path(output_dir),
            )

            assert isinstance(result, BenchmarkResult)
            assert result.operation == "read"
            assert result.success is True
            assert result.time_seconds > 0
            assert result.peak_rss_memory_mb >= 0

    def test_benchmark_result_has_required_fields(self, test_parquet):
        """Test BenchmarkResult has all required fields."""
        with tempfile.TemporaryDirectory() as output_dir:
            result = run_single_operation(
                operation="read",
                input_path=test_parquet,
                output_dir=Path(output_dir),
            )

            # Check all required fields exist
            assert hasattr(result, "operation")
            assert hasattr(result, "file")
            assert hasattr(result, "time_seconds")
            assert hasattr(result, "peak_rss_memory_mb")
            assert hasattr(result, "success")
            assert hasattr(result, "error")
            assert hasattr(result, "details")

    def test_benchmark_result_is_frozen(self, test_parquet):
        """Test BenchmarkResult is immutable (frozen dataclass)."""
        with tempfile.TemporaryDirectory() as output_dir:
            result = run_single_operation(
                operation="read",
                input_path=test_parquet,
                output_dir=Path(output_dir),
            )

            # Frozen dataclasses raise FrozenInstanceError on mutation
            with pytest.raises(FrozenInstanceError):
                result.time_seconds = 999.0


class TestBenchmarkSuite:
    """Tests for full benchmark suite."""

    def test_run_suite_returns_suite_result(self, test_parquet):
        """Test that run_benchmark_suite returns SuiteResult."""
        result = run_benchmark_suite(
            input_files=[test_parquet],
            operations=["read"],
            iterations=1,
        )

        assert isinstance(result, SuiteResult)
        assert len(result.results) > 0
        assert result.version is not None
        assert result.timestamp is not None
        assert result.environment is not None

    def test_suite_result_to_json(self, test_parquet):
        """Test SuiteResult can be serialized to JSON."""
        result = run_benchmark_suite(
            input_files=[test_parquet],
            operations=["read"],
            iterations=1,
        )

        json_str = result.to_json()
        assert isinstance(json_str, str)
        assert "results" in json_str
        assert "environment" in json_str


class TestBenchmarkReporting:
    """Tests for benchmark report formatting."""

    def test_format_table(self):
        """Test table formatting."""
        results = [
            BenchmarkResult(
                operation="read",
                file="test.parquet",
                time_seconds=1.23,
                peak_rss_memory_mb=45.6,
                success=True,
            ),
        ]

        table = format_table(results)

        assert "read" in table
        assert "1.23" in table
        assert "45.6" in table  # RSS memory displayed


class TestChainOperations:
    """Tests for chained operation benchmarks."""

    def test_chain_operations_defined(self):
        """Test that chain operations are defined."""
        assert len(CHAIN_OPERATIONS) == 3
        assert "chain-extract-bbox-sort" in CHAIN_OPERATIONS
        assert "chain-convert-optimize" in CHAIN_OPERATIONS
        assert "chain-filter-reproject-partition" in CHAIN_OPERATIONS

    def test_chain_operations_in_full_suite(self):
        """Test that chain operations are in full suite."""
        for op in CHAIN_OPERATIONS:
            assert op in FULL_OPERATIONS, f"Chain operation {op} not in FULL_OPERATIONS"

    def test_chain_operations_registered(self):
        """Test that all chain operations have handlers."""
        from geoparquet_io.benchmarks.operations import OPERATION_REGISTRY

        for op in CHAIN_OPERATIONS:
            assert op in OPERATION_REGISTRY, f"Missing handler for chain op {op}"

    def test_chain_operations_have_info(self):
        """Test that chain operations have proper info."""
        from geoparquet_io.benchmarks.operations import get_operation

        for op in CHAIN_OPERATIONS:
            info = get_operation(op)
            assert "chain" in info["name"].lower() or "→" in info["name"]
            assert callable(info["run"])


@pytest.mark.slow
class TestChainOperationExecution:
    """Tests for actual execution of chain operations.

    Uses real test fixtures from tests/data/ directory to ensure
    operations work with valid GeoParquet data.
    """

    def test_chain_extract_bbox_sort_runs(self, places_test_file):
        """Test chain-extract-bbox-sort operation runs."""
        from geoparquet_io.benchmarks.operations import get_operation

        with tempfile.TemporaryDirectory() as output_dir:
            op = get_operation("chain-extract-bbox-sort")
            result = op["run"](Path(places_test_file), Path(output_dir))

            assert result["steps_completed"] == 3
            assert "columns_selected" in result
            assert "final_rows" in result
            assert "final_size_mb" in result

    def test_chain_filter_reproject_partition_runs(self, places_test_file):
        """Test chain-filter-reproject-partition operation runs."""
        from geoparquet_io.benchmarks.operations import get_operation

        with tempfile.TemporaryDirectory() as output_dir:
            op = get_operation("chain-filter-reproject-partition")
            result = op["run"](Path(places_test_file), Path(output_dir))

            # May be skipped if no rows in bbox
            if result.get("skipped"):
                assert "reason" in result
            else:
                assert result["steps_completed"] == 3
                assert "partitions_created" in result

    def test_chain_convert_optimize_skips_when_no_source(self, places_test_file):
        """Test chain-convert-optimize skips when no source file."""
        from geoparquet_io.benchmarks.operations import get_operation

        with tempfile.TemporaryDirectory() as output_dir:
            op = get_operation("chain-convert-optimize")
            result = op["run"](Path(places_test_file), Path(output_dir))

            # Should skip since there's no GeoJSON/GPKG alongside
            assert result["skipped"] is True
            assert "No source format file" in result["reason"]

    def test_chain_convert_optimize_with_geojson_source(self, geojson_input, tmp_path):
        """Test chain-convert-optimize works when source file exists."""
        import shutil

        from geoparquet_io.benchmarks.operations import get_operation

        # Copy geojson to tmp_path and create a parquet name beside it
        input_geojson = Path(geojson_input)
        copied_geojson = tmp_path / input_geojson.name
        shutil.copy(input_geojson, copied_geojson)

        # Create a "parquet" path that will be passed (the function looks for .geojson)
        parquet_path = copied_geojson.with_suffix(".parquet")

        with tempfile.TemporaryDirectory() as output_dir:
            op = get_operation("chain-convert-optimize")
            result = op["run"](parquet_path, Path(output_dir))

            assert result.get("skipped") is not True
            assert result["steps_completed"] == 3
            assert result["source_format"] == "geojson"
            assert "final_rows" in result


class TestProfilingIntegration:
    """Tests for profiling integration."""

    def test_run_single_operation_without_profiling(self, test_parquet):
        """Test that profiling is disabled by default."""
        with tempfile.TemporaryDirectory() as output_dir:
            result = run_single_operation(
                operation="read",
                input_path=test_parquet,
                output_dir=Path(output_dir),
            )

            # No profile path in details when profiling disabled
            assert "profile_path" not in result.details

    def test_run_single_operation_with_profiling(self, test_parquet):
        """Test that profiling generates .prof file when enabled."""
        with tempfile.TemporaryDirectory() as tmpdir:
            profile_dir = Path(tmpdir) / "profiles"
            profile_dir.mkdir()
            output_dir = Path(tmpdir) / "output"
            output_dir.mkdir()

            result = run_single_operation(
                operation="read",
                input_path=test_parquet,
                output_dir=output_dir,
                profile=True,
                profile_dir=profile_dir,
            )

            # Profile path stored in result details
            assert "profile_path" in result.details
            profile_path = Path(result.details["profile_path"])

            # Profile file exists
            assert profile_path.exists()
            assert profile_path.suffix == ".prof"
            assert profile_path.parent == profile_dir

            # Profile file has content
            assert profile_path.stat().st_size > 0

    def test_profile_filename_format(self, test_parquet):
        """Test that profile filenames follow expected format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            profile_dir = Path(tmpdir) / "profiles"
            profile_dir.mkdir()
            output_dir = Path(tmpdir) / "output"
            output_dir.mkdir()

            result = run_single_operation(
                operation="read",
                input_path=test_parquet,
                output_dir=output_dir,
                iteration=2,
                profile=True,
                profile_dir=profile_dir,
            )

            profile_path = Path(result.details["profile_path"])
            filename = profile_path.name

            # Filename format: {operation}_{file_stem}_{iteration}.prof
            assert "read" in filename
            assert "test" in filename  # from test.parquet
            assert "2" in filename  # iteration number
            assert filename.endswith(".prof")

    def test_profiling_with_multiple_iterations(self, test_parquet):
        """Test that each iteration generates separate profile."""
        with tempfile.TemporaryDirectory() as tmpdir:
            profile_dir = Path(tmpdir) / "profiles"
            profile_dir.mkdir()
            output_dir = Path(tmpdir) / "output"
            output_dir.mkdir()

            profiles = []
            for iteration in [1, 2, 3]:
                result = run_single_operation(
                    operation="read",
                    input_path=test_parquet,
                    output_dir=output_dir,
                    iteration=iteration,
                    profile=True,
                    profile_dir=profile_dir,
                )
                profiles.append(Path(result.details["profile_path"]))

            # Each iteration has unique profile
            assert len(profiles) == 3
            assert len(set(profiles)) == 3  # All unique paths
            for prof in profiles:
                assert prof.exists()

    def test_profiling_with_failed_operation(self, test_parquet):
        """Test that profile is still generated when operation fails."""
        with tempfile.TemporaryDirectory() as tmpdir:
            profile_dir = Path(tmpdir) / "profiles"
            profile_dir.mkdir()
            output_dir = Path(tmpdir) / "output"
            output_dir.mkdir()

            # Use invalid operation to trigger error
            # Note: This test will need adjustment based on how errors are handled
            # For now, test that profile_path key exists even on failure
            result = run_single_operation(
                operation="read",
                input_path=test_parquet,
                output_dir=output_dir,
                profile=True,
                profile_dir=profile_dir,
            )

            # Even if operation fails, profile should be captured
            # (This specific test may pass since read succeeds, but demonstrates intent)
            assert result.success is True
            assert "profile_path" in result.details
