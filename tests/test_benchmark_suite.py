"""Tests for benchmark suite functionality."""

import tempfile
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from geoparquet_io.benchmarks.config import (
    CORE_OPERATIONS,
    DEFAULT_THRESHOLDS,
    FULL_OPERATIONS,
    MEMORY_LIMITS,
    RegressionThresholds,
)
from geoparquet_io.core.benchmark_report import (
    format_comparison_table,
    format_table,
)
from geoparquet_io.core.benchmark_suite import (
    BenchmarkResult,
    ComparisonResult,
    RegressionStatus,
    SuiteResult,
    compare_results,
    run_benchmark_suite,
    run_single_operation,
)


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

    def test_default_thresholds(self):
        """Test default regression thresholds."""
        assert DEFAULT_THRESHOLDS.time_warning == 0.10
        assert DEFAULT_THRESHOLDS.time_failure == 0.25
        assert DEFAULT_THRESHOLDS.memory_warning == 0.20
        assert DEFAULT_THRESHOLDS.memory_failure == 0.50

    def test_thresholds_are_dataclass(self):
        """Test that thresholds use dataclass pattern."""
        assert isinstance(DEFAULT_THRESHOLDS, RegressionThresholds)

    def test_memory_limits_defined(self):
        """Test that memory limits are defined."""
        assert "constrained" in MEMORY_LIMITS
        assert "normal" in MEMORY_LIMITS
        assert MEMORY_LIMITS["constrained"] == "512m"
        assert MEMORY_LIMITS["normal"] == "4g"


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


class TestBenchmarkRunner:
    """Tests for benchmark runner."""

    @pytest.fixture
    def test_parquet(self):
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
            assert result.peak_python_memory_mb >= 0
            assert result.peak_rss_memory_mb >= 0  # Enhanced: track RSS too

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
            assert hasattr(result, "peak_python_memory_mb")  # Enhanced
            assert hasattr(result, "peak_rss_memory_mb")  # Enhanced
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

    @pytest.fixture
    def test_parquet(self):
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


class TestRegressionComparison:
    """Tests for regression comparison."""

    def test_compare_results_no_regression(self):
        """Test comparison with no regression."""
        baseline = BenchmarkResult(
            operation="read",
            file="test.parquet",
            time_seconds=1.0,
            peak_python_memory_mb=50,
            peak_rss_memory_mb=100,
            success=True,
        )
        current = BenchmarkResult(
            operation="read",
            file="test.parquet",
            time_seconds=1.05,  # 5% slower - within threshold
            peak_python_memory_mb=52,
            peak_rss_memory_mb=105,  # 5% more - within threshold
            success=True,
        )

        comparison = compare_results(baseline, current)

        assert comparison.status == RegressionStatus.OK
        assert comparison.time_delta_pct == pytest.approx(0.05, rel=0.01)

    def test_compare_results_warning(self):
        """Test comparison with warning-level regression."""
        baseline = BenchmarkResult(
            operation="read",
            file="test.parquet",
            time_seconds=1.0,
            peak_python_memory_mb=50,
            peak_rss_memory_mb=100,
            success=True,
        )
        current = BenchmarkResult(
            operation="read",
            file="test.parquet",
            time_seconds=1.15,  # 15% slower - warning
            peak_python_memory_mb=50,
            peak_rss_memory_mb=100,
            success=True,
        )

        comparison = compare_results(baseline, current)

        assert comparison.status == RegressionStatus.WARNING

    def test_compare_results_failure(self):
        """Test comparison with failure-level regression."""
        baseline = BenchmarkResult(
            operation="read",
            file="test.parquet",
            time_seconds=1.0,
            peak_python_memory_mb=50,
            peak_rss_memory_mb=100,
            success=True,
        )
        current = BenchmarkResult(
            operation="read",
            file="test.parquet",
            time_seconds=1.30,  # 30% slower - failure
            peak_python_memory_mb=50,
            peak_rss_memory_mb=100,
            success=True,
        )

        comparison = compare_results(baseline, current)

        assert comparison.status == RegressionStatus.FAILURE


class TestBenchmarkReporting:
    """Tests for benchmark report formatting."""

    def test_format_table(self):
        """Test table formatting."""
        results = [
            BenchmarkResult(
                operation="read",
                file="test.parquet",
                time_seconds=1.23,
                peak_python_memory_mb=20.5,
                peak_rss_memory_mb=45.6,
                success=True,
            ),
        ]

        table = format_table(results)

        assert "read" in table
        assert "1.23" in table
        assert "45.6" in table  # RSS memory displayed

    def test_format_comparison_table(self):
        """Test comparison table formatting."""
        comparisons = [
            ComparisonResult(
                operation="read",
                file="test.parquet",
                baseline_time=1.0,
                current_time=1.1,
                time_delta_pct=0.10,
                baseline_rss_memory=100,
                current_rss_memory=110,
                memory_delta_pct=0.10,
                status=RegressionStatus.WARNING,
            ),
        ]

        table = format_comparison_table(comparisons)

        assert "read" in table
        assert "+10%" in table or "10%" in table
        assert "WARNING" in table or "⚠" in table
