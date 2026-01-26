# Benchmark Suite Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a comprehensive benchmark system that measures speed and memory across file sizes and operations, with regression detection.

**Architecture:** Core benchmark runner in `benchmark_suite.py` orchestrates operation execution with timing/memory tracking. CLI exposes `gpio benchmark suite|compare|report` commands. Results stored as JSON for historical comparison.

**Tech Stack:** Click (CLI), tracemalloc/psutil (memory), pytest (testing), Docker (memory limits in CI)

---

## Task 1: Create Benchmark Suite Configuration Module

**Files:**
- Create: `benchmarks/config.py`
- Test: `tests/test_benchmark_suite.py`

**Step 1: Write the failing test**

```python
# tests/test_benchmark_suite.py
"""Tests for benchmark suite functionality."""

import pytest

from benchmarks.config import (
    CORE_OPERATIONS,
    FULL_OPERATIONS,
    DEFAULT_THRESHOLDS,
    FILE_SIZES,
    MEMORY_LIMITS,
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
        assert DEFAULT_THRESHOLDS["time_warning"] == 0.10
        assert DEFAULT_THRESHOLDS["time_failure"] == 0.25
        assert DEFAULT_THRESHOLDS["memory_warning"] == 0.20
        assert DEFAULT_THRESHOLDS["memory_failure"] == 0.50

    def test_file_sizes_defined(self):
        """Test that file sizes are defined."""
        assert "tiny" in FILE_SIZES
        assert "small" in FILE_SIZES
        assert "medium" in FILE_SIZES
        assert "large" in FILE_SIZES
        assert "xlarge" in FILE_SIZES

    def test_memory_limits_defined(self):
        """Test that memory limits are defined."""
        assert "constrained" in MEMORY_LIMITS
        assert "normal" in MEMORY_LIMITS
        assert MEMORY_LIMITS["constrained"] == "512m"
        assert MEMORY_LIMITS["normal"] == "4g"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_benchmark_suite.py::TestBenchmarkConfig -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'benchmarks'"

**Step 3: Create benchmarks directory and config**

```python
# benchmarks/__init__.py
"""Benchmark suite for geoparquet-io."""

# benchmarks/config.py
"""Configuration for benchmark suite."""

from __future__ import annotations

# Core operations (~10) - run by default
CORE_OPERATIONS = [
    "read",
    "write",
    "convert-geojson",
    "convert-gpkg",
    "extract-bbox",
    "extract-columns",
    "reproject",
    "sort-hilbert",
    "add-bbox",
    "partition-quadkey",
]

# Full suite operations - includes core + extras
FULL_OPERATIONS = CORE_OPERATIONS + [
    "convert-shapefile",
    "convert-fgb",
    "sort-quadkey",
    "add-h3",
    "add-quadkey",
    "add-country",
    "partition-h3",
    "partition-country",
]

# Regression thresholds (as decimals, e.g., 0.10 = 10%)
DEFAULT_THRESHOLDS = {
    "time_warning": 0.10,      # >10% slower = warning
    "time_failure": 0.25,      # >25% slower = failure
    "memory_warning": 0.20,    # >20% more memory = warning
    "memory_failure": 0.50,    # >50% more memory = failure
}

# File sizes with S3 URLs on source.coop
FILE_SIZES = {
    "tiny": {
        "name": "tiny.parquet",
        "url": "s3://source.coop/geoparquet-io/benchmarks/tiny.parquet",
        "approx_size_mb": 0.08,
    },
    "small": {
        "name": "small.parquet",
        "url": "s3://source.coop/geoparquet-io/benchmarks/small.parquet",
        "approx_size_mb": 1,
    },
    "medium": {
        "name": "medium.parquet",
        "url": "s3://source.coop/geoparquet-io/benchmarks/medium.parquet",
        "approx_size_mb": 50,
    },
    "large": {
        "name": "large.parquet",
        "url": "s3://source.coop/geoparquet-io/benchmarks/large.parquet",
        "approx_size_mb": 500,
    },
    "xlarge": {
        "name": "xlarge.parquet",
        "url": "s3://source.coop/geoparquet-io/benchmarks/xlarge.parquet",
        "approx_size_mb": 2000,
    },
}

# Memory limit configurations
MEMORY_LIMITS = {
    "constrained": "512m",
    "normal": "4g",
}

# S3 bucket base URL
BENCHMARK_FILES_BASE = "s3://source.coop/geoparquet-io/benchmarks"
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_benchmark_suite.py::TestBenchmarkConfig -v`
Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/ tests/test_benchmark_suite.py
git commit -m "Add benchmark suite configuration module"
```

---

## Task 2: Create Operation Registry

**Files:**
- Modify: `benchmarks/config.py`
- Create: `benchmarks/operations.py`
- Modify: `tests/test_benchmark_suite.py`

**Step 1: Write the failing test**

```python
# Add to tests/test_benchmark_suite.py

from benchmarks.operations import OPERATION_REGISTRY, get_operation


class TestOperationRegistry:
    """Tests for operation registry."""

    def test_all_core_operations_registered(self):
        """Test that all core operations have handlers."""
        from benchmarks.config import CORE_OPERATIONS
        for op in CORE_OPERATIONS:
            assert op in OPERATION_REGISTRY, f"Missing handler for {op}"

    def test_get_operation_returns_callable(self):
        """Test that get_operation returns a callable."""
        op = get_operation("read")
        assert callable(op["run"])
        assert "name" in op
        assert "description" in op

    def test_get_operation_invalid_raises(self):
        """Test that invalid operation raises KeyError."""
        with pytest.raises(KeyError):
            get_operation("nonexistent-operation")
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_benchmark_suite.py::TestOperationRegistry -v`
Expected: FAIL with "ModuleNotFoundError"

**Step 3: Create operations registry**

```python
# benchmarks/operations.py
"""Operation definitions for benchmark suite."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any, Callable

import pyarrow.parquet as pq


def _run_read(input_path: Path, _output_dir: Path) -> dict[str, Any]:
    """Benchmark read operation."""
    import pyarrow.parquet as pq
    table = pq.read_table(input_path)
    return {"rows": table.num_rows, "columns": table.num_columns}


def _run_write(input_path: Path, output_dir: Path) -> dict[str, Any]:
    """Benchmark write operation."""
    import pyarrow.parquet as pq
    table = pq.read_table(input_path)
    output_path = output_dir / "output.parquet"
    pq.write_table(table, output_path, compression="zstd")
    return {"output_size_mb": output_path.stat().st_size / (1024 * 1024)}


def _run_convert_geojson(input_path: Path, output_dir: Path) -> dict[str, Any]:
    """Benchmark GeoJSON conversion."""
    from geoparquet_io.core.convert import convert_to_geoparquet
    # Need a GeoJSON input - for now just validate the operation exists
    output_path = output_dir / "output.parquet"
    # This will use a pre-created GeoJSON version of the test file
    geojson_path = input_path.with_suffix(".geojson")
    if geojson_path.exists():
        convert_to_geoparquet(str(geojson_path), str(output_path))
        return {"output_size_mb": output_path.stat().st_size / (1024 * 1024)}
    return {"skipped": True, "reason": "No GeoJSON version available"}


def _run_convert_gpkg(input_path: Path, output_dir: Path) -> dict[str, Any]:
    """Benchmark GeoPackage conversion."""
    from geoparquet_io.core.convert import convert_to_geoparquet
    output_path = output_dir / "output.parquet"
    gpkg_path = input_path.with_suffix(".gpkg")
    if gpkg_path.exists():
        convert_to_geoparquet(str(gpkg_path), str(output_path))
        return {"output_size_mb": output_path.stat().st_size / (1024 * 1024)}
    return {"skipped": True, "reason": "No GeoPackage version available"}


def _run_extract_bbox(input_path: Path, output_dir: Path) -> dict[str, Any]:
    """Benchmark bbox extraction."""
    from geoparquet_io.core.extract import extract
    output_path = output_dir / "output.parquet"
    # Use a bbox that covers ~50% of typical data
    extract(
        str(input_path),
        str(output_path),
        bbox=(-180, -45, 0, 45),
    )
    result_table = pq.read_table(output_path)
    return {"output_rows": result_table.num_rows}


def _run_extract_columns(input_path: Path, output_dir: Path) -> dict[str, Any]:
    """Benchmark column extraction."""
    from geoparquet_io.core.extract import extract
    output_path = output_dir / "output.parquet"
    # Read schema to get first 3 columns
    schema = pq.read_schema(input_path)
    columns = [schema.field(i).name for i in range(min(3, len(schema)))]
    extract(
        str(input_path),
        str(output_path),
        columns=columns,
    )
    return {"columns_selected": len(columns)}


def _run_reproject(input_path: Path, output_dir: Path) -> dict[str, Any]:
    """Benchmark reprojection."""
    from geoparquet_io.core.reproject import reproject
    output_path = output_dir / "output.parquet"
    reproject(str(input_path), str(output_path), target_crs="EPSG:3857")
    return {"target_crs": "EPSG:3857"}


def _run_sort_hilbert(input_path: Path, output_dir: Path) -> dict[str, Any]:
    """Benchmark Hilbert sorting."""
    from geoparquet_io.core.hilbert_order import hilbert_order
    output_path = output_dir / "output.parquet"
    hilbert_order(str(input_path), str(output_path))
    return {"sorted": True}


def _run_add_bbox(input_path: Path, output_dir: Path) -> dict[str, Any]:
    """Benchmark adding bbox column."""
    from geoparquet_io.core.add_bbox_column import add_bbox_column
    output_path = output_dir / "output.parquet"
    add_bbox_column(str(input_path), str(output_path))
    return {"bbox_added": True}


def _run_partition_quadkey(input_path: Path, output_dir: Path) -> dict[str, Any]:
    """Benchmark quadkey partitioning."""
    from geoparquet_io.core.partition_by_quadkey import partition_by_quadkey
    output_path = output_dir / "partitioned"
    partition_by_quadkey(str(input_path), str(output_path), level=4)
    # Count output files
    output_files = list(Path(output_path).glob("**/*.parquet"))
    return {"partitions": len(output_files)}


# Registry mapping operation names to handlers
OPERATION_REGISTRY: dict[str, dict[str, Any]] = {
    "read": {
        "name": "Read",
        "description": "Load parquet into memory",
        "run": _run_read,
    },
    "write": {
        "name": "Write",
        "description": "Write table back to parquet",
        "run": _run_write,
    },
    "convert-geojson": {
        "name": "Convert GeoJSON",
        "description": "GeoJSON to GeoParquet",
        "run": _run_convert_geojson,
    },
    "convert-gpkg": {
        "name": "Convert GeoPackage",
        "description": "GeoPackage to GeoParquet",
        "run": _run_convert_gpkg,
    },
    "extract-bbox": {
        "name": "Extract BBox",
        "description": "Spatial filtering",
        "run": _run_extract_bbox,
    },
    "extract-columns": {
        "name": "Extract Columns",
        "description": "Column selection",
        "run": _run_extract_columns,
    },
    "reproject": {
        "name": "Reproject",
        "description": "CRS transformation (4326→3857)",
        "run": _run_reproject,
    },
    "sort-hilbert": {
        "name": "Sort Hilbert",
        "description": "Hilbert curve ordering",
        "run": _run_sort_hilbert,
    },
    "add-bbox": {
        "name": "Add BBox",
        "description": "Compute bbox column",
        "run": _run_add_bbox,
    },
    "partition-quadkey": {
        "name": "Partition Quadkey",
        "description": "Partition by quadkey",
        "run": _run_partition_quadkey,
    },
}


def get_operation(name: str) -> dict[str, Any]:
    """Get operation by name."""
    if name not in OPERATION_REGISTRY:
        raise KeyError(f"Unknown operation: {name}")
    return OPERATION_REGISTRY[name]
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_benchmark_suite.py::TestOperationRegistry -v`
Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/operations.py tests/test_benchmark_suite.py
git commit -m "Add operation registry for benchmark suite"
```

---

## Task 3: Create Benchmark Runner Core

**Files:**
- Create: `geoparquet_io/core/benchmark_suite.py`
- Modify: `tests/test_benchmark_suite.py`

**Step 1: Write the failing test**

```python
# Add to tests/test_benchmark_suite.py
import tempfile
from pathlib import Path

from geoparquet_io.core.benchmark_suite import (
    run_single_operation,
    BenchmarkResult,
)


class TestBenchmarkRunner:
    """Tests for benchmark runner."""

    @pytest.fixture
    def test_parquet(self):
        """Create a small test parquet file."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.parquet"
            table = pa.table({
                "id": [1, 2, 3],
                "geometry": [b"point1", b"point2", b"point3"],
            })
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
            assert result.peak_memory_mb >= 0

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
            assert hasattr(result, "peak_memory_mb")
            assert hasattr(result, "success")
            assert hasattr(result, "error")
            assert hasattr(result, "details")
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_benchmark_suite.py::TestBenchmarkRunner -v`
Expected: FAIL with "ModuleNotFoundError"

**Step 3: Create benchmark runner**

```python
# geoparquet_io/core/benchmark_suite.py
"""Benchmark suite runner for comprehensive performance testing."""

from __future__ import annotations

import sys
import time
import tracemalloc
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Import from benchmarks package
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from benchmarks.operations import get_operation


@dataclass
class BenchmarkResult:
    """Result from a single benchmark run."""

    operation: str
    file: str
    time_seconds: float
    peak_memory_mb: float
    success: bool
    error: str | None = None
    details: dict[str, Any] = field(default_factory=dict)
    memory_limit_mb: int | None = None
    iteration: int = 1


def run_single_operation(
    operation: str,
    input_path: Path,
    output_dir: Path,
    iteration: int = 1,
    memory_limit_mb: int | None = None,
) -> BenchmarkResult:
    """
    Run a single benchmark operation with timing and memory tracking.

    Args:
        operation: Name of the operation to run
        input_path: Path to input file
        output_dir: Directory for output files
        iteration: Iteration number (for multiple runs)
        memory_limit_mb: Optional memory limit context

    Returns:
        BenchmarkResult with timing and memory data
    """
    op_info = get_operation(operation)
    run_func = op_info["run"]

    # Start memory tracking
    tracemalloc.start()
    start_time = time.perf_counter()

    try:
        details = run_func(input_path, output_dir)
        elapsed = time.perf_counter() - start_time
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        return BenchmarkResult(
            operation=operation,
            file=input_path.name,
            time_seconds=round(elapsed, 3),
            peak_memory_mb=round(peak / (1024 * 1024), 2),
            success=True,
            details=details or {},
            memory_limit_mb=memory_limit_mb,
            iteration=iteration,
        )

    except Exception as e:
        elapsed = time.perf_counter() - start_time
        try:
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            peak_mb = round(peak / (1024 * 1024), 2)
        except Exception:
            peak_mb = 0

        return BenchmarkResult(
            operation=operation,
            file=input_path.name,
            time_seconds=round(elapsed, 3),
            peak_memory_mb=peak_mb,
            success=False,
            error=str(e),
            memory_limit_mb=memory_limit_mb,
            iteration=iteration,
        )
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_benchmark_suite.py::TestBenchmarkRunner -v`
Expected: PASS

**Step 5: Commit**

```bash
git add geoparquet_io/core/benchmark_suite.py tests/test_benchmark_suite.py
git commit -m "Add benchmark suite runner core"
```

---

## Task 4: Add Full Suite Runner

**Files:**
- Modify: `geoparquet_io/core/benchmark_suite.py`
- Modify: `tests/test_benchmark_suite.py`

**Step 1: Write the failing test**

```python
# Add to tests/test_benchmark_suite.py

from geoparquet_io.core.benchmark_suite import (
    run_benchmark_suite,
    SuiteResult,
)


class TestBenchmarkSuite:
    """Tests for full benchmark suite."""

    @pytest.fixture
    def test_parquet(self):
        """Create a small test parquet file."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.parquet"
            table = pa.table({
                "id": [1, 2, 3],
                "geometry": [b"point1", b"point2", b"point3"],
            })
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
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_benchmark_suite.py::TestBenchmarkSuite -v`
Expected: FAIL with "cannot import name 'run_benchmark_suite'"

**Step 3: Add suite runner**

```python
# Add to geoparquet_io/core/benchmark_suite.py

import json
import platform
from dataclasses import asdict
from datetime import datetime, timezone

import duckdb
import psutil


@dataclass
class SuiteResult:
    """Result from a full benchmark suite run."""

    version: str
    timestamp: str
    environment: dict[str, Any]
    results: list[BenchmarkResult]
    config: dict[str, Any] = field(default_factory=dict)

    def to_json(self, indent: int = 2) -> str:
        """Serialize to JSON string."""
        data = {
            "version": self.version,
            "timestamp": self.timestamp,
            "environment": self.environment,
            "config": self.config,
            "results": [asdict(r) for r in self.results],
        }
        return json.dumps(data, indent=indent)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "version": self.version,
            "timestamp": self.timestamp,
            "environment": self.environment,
            "config": self.config,
            "results": [asdict(r) for r in self.results],
        }


def get_environment_info() -> dict[str, Any]:
    """Collect environment information for benchmark results."""
    env = {
        "os": platform.system(),
        "os_version": platform.version(),
        "python_version": platform.python_version(),
        "duckdb_version": duckdb.__version__,
        "cpu": _get_cpu_info(),
    }

    try:
        ram_gb = psutil.virtual_memory().total / (1024**3)
        env["ram_gb"] = round(ram_gb, 1)
    except Exception:
        env["ram_gb"] = None

    return env


def _get_cpu_info() -> str:
    """Get CPU information string."""
    try:
        cpu_count = psutil.cpu_count(logical=True)
        processor = platform.processor()
        if processor:
            return f"{processor} / {cpu_count} cores"
        return f"{cpu_count} cores"
    except Exception:
        return "Unknown"


def _get_version() -> str:
    """Get current geoparquet-io version."""
    try:
        from geoparquet_io.cli.main import __version__
        return __version__
    except ImportError:
        return "unknown"


def run_benchmark_suite(
    input_files: list[Path],
    operations: list[str] | None = None,
    iterations: int = 3,
    memory_limit_mb: int | None = None,
    verbose: bool = False,
) -> SuiteResult:
    """
    Run the full benchmark suite.

    Args:
        input_files: List of input files to benchmark
        operations: Operations to run (default: core operations)
        iterations: Number of iterations per operation
        memory_limit_mb: Memory limit context (for reporting)
        verbose: Show progress output

    Returns:
        SuiteResult with all benchmark data
    """
    import tempfile

    from benchmarks.config import CORE_OPERATIONS

    if operations is None:
        operations = CORE_OPERATIONS

    results: list[BenchmarkResult] = []

    for input_file in input_files:
        input_path = Path(input_file)

        for operation in operations:
            for iteration in range(1, iterations + 1):
                with tempfile.TemporaryDirectory() as output_dir:
                    result = run_single_operation(
                        operation=operation,
                        input_path=input_path,
                        output_dir=Path(output_dir),
                        iteration=iteration,
                        memory_limit_mb=memory_limit_mb,
                    )
                    results.append(result)

                    if verbose:
                        status = "✓" if result.success else "✗"
                        print(f"  {status} {operation} ({input_path.name}) - {result.time_seconds}s")

    return SuiteResult(
        version=_get_version(),
        timestamp=datetime.now(timezone.utc).isoformat(),
        environment=get_environment_info(),
        results=results,
        config={
            "operations": operations,
            "iterations": iterations,
            "memory_limit_mb": memory_limit_mb,
            "files": [str(f) for f in input_files],
        },
    )
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_benchmark_suite.py::TestBenchmarkSuite -v`
Expected: PASS

**Step 5: Commit**

```bash
git add geoparquet_io/core/benchmark_suite.py tests/test_benchmark_suite.py
git commit -m "Add full benchmark suite runner"
```

---

## Task 5: Add Regression Comparison

**Files:**
- Modify: `geoparquet_io/core/benchmark_suite.py`
- Modify: `tests/test_benchmark_suite.py`

**Step 1: Write the failing test**

```python
# Add to tests/test_benchmark_suite.py

from geoparquet_io.core.benchmark_suite import (
    compare_results,
    ComparisonResult,
    RegressionStatus,
)


class TestRegressionComparison:
    """Tests for regression comparison."""

    def test_compare_results_no_regression(self):
        """Test comparison with no regression."""
        baseline = BenchmarkResult(
            operation="read",
            file="test.parquet",
            time_seconds=1.0,
            peak_memory_mb=100,
            success=True,
        )
        current = BenchmarkResult(
            operation="read",
            file="test.parquet",
            time_seconds=1.05,  # 5% slower - within threshold
            peak_memory_mb=105,  # 5% more - within threshold
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
            peak_memory_mb=100,
            success=True,
        )
        current = BenchmarkResult(
            operation="read",
            file="test.parquet",
            time_seconds=1.15,  # 15% slower - warning
            peak_memory_mb=100,
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
            peak_memory_mb=100,
            success=True,
        )
        current = BenchmarkResult(
            operation="read",
            file="test.parquet",
            time_seconds=1.30,  # 30% slower - failure
            peak_memory_mb=100,
            success=True,
        )

        comparison = compare_results(baseline, current)

        assert comparison.status == RegressionStatus.FAILURE
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_benchmark_suite.py::TestRegressionComparison -v`
Expected: FAIL with "cannot import name 'compare_results'"

**Step 3: Add comparison functionality**

```python
# Add to geoparquet_io/core/benchmark_suite.py

from enum import Enum

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from benchmarks.config import DEFAULT_THRESHOLDS


class RegressionStatus(Enum):
    """Status of a regression comparison."""
    OK = "ok"
    WARNING = "warning"
    FAILURE = "failure"
    IMPROVED = "improved"


@dataclass
class ComparisonResult:
    """Result of comparing two benchmark results."""

    operation: str
    file: str
    baseline_time: float
    current_time: float
    time_delta_pct: float
    baseline_memory: float
    current_memory: float
    memory_delta_pct: float
    status: RegressionStatus


def compare_results(
    baseline: BenchmarkResult,
    current: BenchmarkResult,
    thresholds: dict[str, float] | None = None,
) -> ComparisonResult:
    """
    Compare current result against baseline for regression.

    Args:
        baseline: Baseline benchmark result
        current: Current benchmark result
        thresholds: Optional custom thresholds

    Returns:
        ComparisonResult with delta and status
    """
    if thresholds is None:
        thresholds = DEFAULT_THRESHOLDS

    # Calculate deltas
    time_delta = (current.time_seconds - baseline.time_seconds) / baseline.time_seconds
    memory_delta = (current.peak_memory_mb - baseline.peak_memory_mb) / max(baseline.peak_memory_mb, 0.01)

    # Determine status
    status = RegressionStatus.OK

    if time_delta < -0.05 and memory_delta < -0.05:
        status = RegressionStatus.IMPROVED
    elif time_delta >= thresholds["time_failure"] or memory_delta >= thresholds["memory_failure"]:
        status = RegressionStatus.FAILURE
    elif time_delta >= thresholds["time_warning"] or memory_delta >= thresholds["memory_warning"]:
        status = RegressionStatus.WARNING

    return ComparisonResult(
        operation=current.operation,
        file=current.file,
        baseline_time=baseline.time_seconds,
        current_time=current.time_seconds,
        time_delta_pct=round(time_delta, 4),
        baseline_memory=baseline.peak_memory_mb,
        current_memory=current.peak_memory_mb,
        memory_delta_pct=round(memory_delta, 4),
        status=status,
    )
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_benchmark_suite.py::TestRegressionComparison -v`
Expected: PASS

**Step 5: Commit**

```bash
git add geoparquet_io/core/benchmark_suite.py tests/test_benchmark_suite.py
git commit -m "Add regression comparison for benchmarks"
```

---

## Task 6: Convert Benchmark CLI to Command Group

**Files:**
- Modify: `geoparquet_io/cli/main.py`
- Modify: `tests/test_benchmark.py`

**Step 1: Write the failing test**

```python
# Add to tests/test_benchmark.py

class TestBenchmarkCommandGroup:
    """Tests for benchmark command group."""

    def test_benchmark_is_group(self):
        """Test that benchmark is a command group."""
        runner = CliRunner()
        result = runner.invoke(cli, ["benchmark", "--help"])

        assert result.exit_code == 0
        assert "suite" in result.output
        assert "compare" in result.output

    def test_benchmark_suite_help(self):
        """Test benchmark suite --help."""
        runner = CliRunner()
        result = runner.invoke(cli, ["benchmark", "suite", "--help"])

        assert result.exit_code == 0
        assert "--operations" in result.output
        assert "--iterations" in result.output

    def test_benchmark_compare_help(self):
        """Test benchmark compare --help (existing functionality)."""
        runner = CliRunner()
        result = runner.invoke(cli, ["benchmark", "compare", "--help"])

        assert result.exit_code == 0
        assert "INPUT_FILE" in result.output
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_benchmark.py::TestBenchmarkCommandGroup -v`
Expected: FAIL (benchmark is currently a command, not a group)

**Step 3: Convert benchmark to command group**

In `geoparquet_io/cli/main.py`, find the `@cli.command()` for benchmark (around line 4294) and refactor:

```python
# Replace the existing benchmark command with a group

# Benchmark commands group
@cli.group()
@click.pass_context
def benchmark(ctx):
    """Benchmark GeoParquet performance.

    Commands for measuring and comparing performance of GeoParquet operations.

    \b
    Subcommands:
      suite    Run comprehensive benchmark suite
      compare  Compare converter performance on a single file
      report   View and compare benchmark results
    """
    ctx.ensure_object(dict)
    timestamps = ctx.obj.get("timestamps", False)
    if timestamps:
        setup_cli_logging(timestamps=timestamps)


@benchmark.command("compare")
@click.argument("input_file", type=click.Path(exists=True))
@click.option(
    "--iterations", "-n", default=3, help="Number of iterations per converter"
)
@click.option("--converters", "-c", multiple=True, help="Specific converters to test")
@click.option("--output-json", "-o", help="Save results to JSON file")
@click.option("--keep-output", help="Keep output files in specified directory")
@click.option("--no-warmup", is_flag=True, help="Skip warmup run")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format",
)
@click.option("--quiet", "-q", is_flag=True, help="Suppress progress output")
@verbose_option
def benchmark_compare(
    input_file,
    iterations,
    converters,
    output_json,
    keep_output,
    no_warmup,
    output_format,
    quiet,
    verbose,
):
    """
    Compare converter performance on a single file.

    Tests different conversion methods (DuckDB, GeoPandas, GDAL) on an input
    geospatial file and reports time and memory usage.

    \b
    Example:
        gpio benchmark compare input.geojson --iterations 5
    """
    configure_verbose(verbose)
    from geoparquet_io.core.benchmark import run_benchmark

    run_benchmark(
        input_file=input_file,
        iterations=iterations,
        converters=list(converters) if converters else None,
        output_json=output_json,
        keep_output=keep_output,
        warmup=not no_warmup,
        output_format=output_format,
        quiet=quiet,
    )


@benchmark.command("suite")
@click.option(
    "--operations",
    type=click.Choice(["core", "full"]),
    default="core",
    help="Operation set to run",
)
@click.option(
    "--files",
    multiple=True,
    help="File sizes to test (tiny, small, medium, large, xlarge, or paths)",
)
@click.option("--iterations", "-n", default=3, help="Runs per operation")
@click.option("--compare", "baseline_path", type=click.Path(exists=True), help="Compare against baseline JSON")
@click.option("--output", "-o", type=click.Path(), help="Write results to JSON file")
@click.option(
    "--profile",
    type=click.Choice(["standard", "comprehensive"]),
    default="standard",
    help="Output detail level",
)
@click.option("--threshold-time", default=0.10, help="Regression threshold for time")
@click.option("--threshold-memory", default=0.20, help="Regression threshold for memory")
@verbose_option
def benchmark_suite(
    operations,
    files,
    iterations,
    baseline_path,
    output,
    profile,
    threshold_time,
    threshold_memory,
    verbose,
):
    """
    Run comprehensive benchmark suite.

    Tests gpio operations across multiple file sizes with timing and memory tracking.
    Results can be compared against a baseline to detect regressions.

    \b
    Example:
        gpio benchmark suite --operations core --output results.json
        gpio benchmark suite --compare baseline.json
    """
    configure_verbose(verbose)
    from geoparquet_io.core.benchmark_suite import run_benchmark_suite
    from geoparquet_io.core.logging_config import progress, success, warn

    progress("Benchmark suite not yet fully implemented")
    progress(f"Would run: operations={operations}, files={files}, iterations={iterations}")


@benchmark.command("report")
@click.argument("result_files", nargs=-1, type=click.Path(exists=True))
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json", "markdown"]),
    default="table",
    help="Output format",
)
@click.option("--compare", type=click.Path(exists=True), help="Compare two result files")
@click.option("--trend", is_flag=True, help="Show trend across multiple versions")
@verbose_option
def benchmark_report(result_files, output_format, compare, trend, verbose):
    """
    View and compare benchmark results.

    \b
    Example:
        gpio benchmark report results.json
        gpio benchmark report v0.5.0.json --compare v0.4.0.json
        gpio benchmark report results/*.json --trend
    """
    configure_verbose(verbose)
    from geoparquet_io.core.logging_config import progress

    progress("Benchmark report not yet fully implemented")
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_benchmark.py::TestBenchmarkCommandGroup -v`
Expected: PASS

**Step 5: Commit**

```bash
git add geoparquet_io/cli/main.py tests/test_benchmark.py
git commit -m "Convert benchmark to command group with suite/compare/report"
```

---

## Task 7: Create GitHub Actions Workflow

**Files:**
- Create: `.github/workflows/benchmark.yml`

**Step 1: Create workflow file**

```yaml
# .github/workflows/benchmark.yml
name: Benchmark Suite

on:
  workflow_dispatch:
    inputs:
      operations:
        description: 'Operation set (core or full)'
        required: false
        default: 'core'
        type: choice
        options:
          - core
          - full
      files:
        description: 'File sizes to test (comma-separated: tiny,small,medium,large,xlarge)'
        required: false
        default: 'tiny,small,medium'
      profile:
        description: 'Profile level'
        required: false
        default: 'standard'
        type: choice
        options:
          - standard
          - comprehensive

  pull_request:
    types: [labeled]

jobs:
  check-trigger:
    runs-on: ubuntu-latest
    outputs:
      should_run: ${{ steps.check.outputs.should_run }}
    steps:
      - id: check
        run: |
          if [[ "${{ github.event_name }}" == "workflow_dispatch" ]]; then
            echo "should_run=true" >> $GITHUB_OUTPUT
          elif [[ "${{ github.event.label.name }}" == "benchmark" ]]; then
            echo "should_run=true" >> $GITHUB_OUTPUT
          else
            echo "should_run=false" >> $GITHUB_OUTPUT
          fi

  benchmark:
    needs: check-trigger
    if: needs.check-trigger.outputs.should_run == 'true'
    strategy:
      matrix:
        include:
          - name: constrained-512mb
            memory: 512m
          - name: normal-4gb
            memory: 4g
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install -e ".[dev]"

      - name: Download benchmark files
        run: |
          mkdir -p benchmarks/data
          # TODO: Download files from source.coop
          echo "Would download benchmark files here"

      - name: Run benchmarks
        run: |
          # Run in Docker with memory limit
          docker run --rm \
            --memory=${{ matrix.memory }} \
            -v $(pwd):/app \
            -w /app \
            python:3.11-slim \
            bash -c "pip install -e . && gpio benchmark suite --output benchmarks/results/${{ matrix.name }}.json"

      - name: Upload results
        uses: actions/upload-artifact@v4
        with:
          name: benchmark-${{ matrix.name }}
          path: benchmarks/results/${{ matrix.name }}.json

  aggregate:
    needs: benchmark
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Download all results
        uses: actions/download-artifact@v4
        with:
          path: benchmarks/results

      - name: Merge results
        run: |
          echo "Would merge results and generate report here"

      - name: Comment on PR
        if: github.event_name == 'pull_request'
        uses: actions/github-script@v7
        with:
          script: |
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: '## Benchmark Results\n\nResults will be posted here after implementation.'
            })
```

**Step 2: Commit**

```bash
git add .github/workflows/benchmark.yml
git commit -m "Add GitHub Actions workflow for benchmarks"
```

---

## Task 8: Create Dockerfile for Benchmarks

**Files:**
- Create: `benchmarks/Dockerfile`

**Step 1: Create Dockerfile**

```dockerfile
# benchmarks/Dockerfile
# Docker image for running benchmarks with memory constraints

FROM python:3.11-slim

# Install system dependencies for GDAL/spatial
RUN apt-get update && apt-get install -y \
    libgdal-dev \
    gdal-bin \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy and install package
COPY . .
RUN pip install --no-cache-dir -e ".[dev,benchmark]"

# Pre-download DuckDB extensions
RUN python -c "import duckdb; c = duckdb.connect(); c.execute('INSTALL spatial'); c.execute('INSTALL httpfs')"

ENTRYPOINT ["gpio", "benchmark"]
CMD ["suite", "--help"]
```

**Step 2: Commit**

```bash
git add benchmarks/Dockerfile
git commit -m "Add Dockerfile for benchmark suite"
```

---

## Task 9: Add Report Formatting

**Files:**
- Create: `geoparquet_io/core/benchmark_report.py`
- Modify: `tests/test_benchmark_suite.py`

**Step 1: Write the failing test**

```python
# Add to tests/test_benchmark_suite.py

from geoparquet_io.core.benchmark_report import (
    format_table,
    format_markdown,
    format_comparison_table,
)


class TestBenchmarkReporting:
    """Tests for benchmark report formatting."""

    def test_format_table(self):
        """Test table formatting."""
        results = [
            BenchmarkResult(
                operation="read",
                file="test.parquet",
                time_seconds=1.23,
                peak_memory_mb=45.6,
                success=True,
            ),
        ]

        table = format_table(results)

        assert "read" in table
        assert "1.23" in table
        assert "45.6" in table

    def test_format_markdown(self):
        """Test markdown formatting."""
        results = [
            BenchmarkResult(
                operation="read",
                file="test.parquet",
                time_seconds=1.23,
                peak_memory_mb=45.6,
                success=True,
            ),
        ]

        md = format_markdown(results)

        assert "|" in md  # Table syntax
        assert "read" in md

    def test_format_comparison_table(self):
        """Test comparison table formatting."""
        comparisons = [
            ComparisonResult(
                operation="read",
                file="test.parquet",
                baseline_time=1.0,
                current_time=1.1,
                time_delta_pct=0.10,
                baseline_memory=100,
                current_memory=110,
                memory_delta_pct=0.10,
                status=RegressionStatus.WARNING,
            ),
        ]

        table = format_comparison_table(comparisons)

        assert "read" in table
        assert "+10%" in table or "10%" in table
        assert "WARNING" in table or "⚠" in table
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_benchmark_suite.py::TestBenchmarkReporting -v`
Expected: FAIL

**Step 3: Create report formatting module**

```python
# geoparquet_io/core/benchmark_report.py
"""Report formatting for benchmark results."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from geoparquet_io.core.benchmark_suite import (
        BenchmarkResult,
        ComparisonResult,
        RegressionStatus,
    )


def format_table(results: list[BenchmarkResult]) -> str:
    """Format results as ASCII table."""
    lines = []
    lines.append(f"{'Operation':<20} {'File':<20} {'Time (s)':<12} {'Memory (MB)':<12} {'Status':<8}")
    lines.append("-" * 72)

    for r in results:
        status = "✓" if r.success else "✗"
        lines.append(
            f"{r.operation:<20} {r.file:<20} {r.time_seconds:<12.3f} {r.peak_memory_mb:<12.1f} {status:<8}"
        )

    return "\n".join(lines)


def format_markdown(results: list[BenchmarkResult]) -> str:
    """Format results as Markdown table."""
    lines = []
    lines.append("| Operation | File | Time (s) | Memory (MB) | Status |")
    lines.append("|-----------|------|----------|-------------|--------|")

    for r in results:
        status = "✓" if r.success else "✗"
        lines.append(
            f"| {r.operation} | {r.file} | {r.time_seconds:.3f} | {r.peak_memory_mb:.1f} | {status} |"
        )

    return "\n".join(lines)


def format_comparison_table(comparisons: list[ComparisonResult]) -> str:
    """Format comparison results as table."""
    from geoparquet_io.core.benchmark_suite import RegressionStatus

    lines = []
    lines.append(
        f"{'Operation':<20} {'File':<15} {'Time':<10} {'Δ':<8} {'Memory':<10} {'Δ':<8} {'Status':<10}"
    )
    lines.append("-" * 81)

    status_icons = {
        RegressionStatus.OK: "✓ OK",
        RegressionStatus.WARNING: "⚠ WARNING",
        RegressionStatus.FAILURE: "✗ FAILURE",
        RegressionStatus.IMPROVED: "↑ IMPROVED",
    }

    for c in comparisons:
        time_delta_str = f"{c.time_delta_pct:+.0%}"
        memory_delta_str = f"{c.memory_delta_pct:+.0%}"
        status_str = status_icons.get(c.status, str(c.status))

        lines.append(
            f"{c.operation:<20} {c.file:<15} {c.current_time:<10.3f} {time_delta_str:<8} "
            f"{c.current_memory:<10.1f} {memory_delta_str:<8} {status_str:<10}"
        )

    return "\n".join(lines)


def format_comparison_markdown(comparisons: list[ComparisonResult]) -> str:
    """Format comparison results as Markdown."""
    from geoparquet_io.core.benchmark_suite import RegressionStatus

    lines = []
    lines.append("| Operation | File | Time | Δ | Memory | Δ | Status |")
    lines.append("|-----------|------|------|---|--------|---|--------|")

    status_icons = {
        RegressionStatus.OK: "✓",
        RegressionStatus.WARNING: "⚠️",
        RegressionStatus.FAILURE: "❌",
        RegressionStatus.IMPROVED: "⬆️",
    }

    for c in comparisons:
        time_delta_str = f"{c.time_delta_pct:+.0%}"
        memory_delta_str = f"{c.memory_delta_pct:+.0%}"
        status_str = status_icons.get(c.status, "?")

        lines.append(
            f"| {c.operation} | {c.file} | {c.current_time:.2f}s | {time_delta_str} | "
            f"{c.current_memory:.0f}MB | {memory_delta_str} | {status_str} |"
        )

    return "\n".join(lines)
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_benchmark_suite.py::TestBenchmarkReporting -v`
Expected: PASS

**Step 5: Commit**

```bash
git add geoparquet_io/core/benchmark_report.py tests/test_benchmark_suite.py
git commit -m "Add benchmark report formatting"
```

---

## Task 10: Wire Up CLI Commands

**Files:**
- Modify: `geoparquet_io/cli/main.py`
- Modify: `tests/test_benchmark.py`

**Step 1: Write the failing test**

```python
# Add to tests/test_benchmark.py

class TestBenchmarkSuiteCLI:
    """Tests for benchmark suite CLI."""

    @pytest.fixture
    def test_parquet(self):
        """Create a test parquet file."""
        import pyarrow as pa
        import pyarrow.parquet as pq
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.parquet"
            table = pa.table({
                "id": [1, 2, 3],
                "geometry": [b"point1", b"point2", b"point3"],
            })
            pq.write_table(table, path)
            yield str(path)

    def test_benchmark_suite_runs(self, test_parquet):
        """Test that benchmark suite runs with a local file."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["benchmark", "suite", "--files", test_parquet, "--operations", "core", "--iterations", "1"],
        )

        # Should complete (may skip some operations)
        assert result.exit_code == 0 or "not yet" in result.output.lower()
```

**Step 2: Update CLI to wire up the suite runner**

Update the `benchmark_suite` command in `main.py` to actually call the runner:

```python
@benchmark.command("suite")
@click.option(
    "--operations",
    type=click.Choice(["core", "full"]),
    default="core",
    help="Operation set to run",
)
@click.option(
    "--files",
    multiple=True,
    help="File sizes to test (tiny, small, medium, large, xlarge, or paths)",
)
@click.option("--iterations", "-n", default=3, help="Runs per operation")
@click.option("--compare", "baseline_path", type=click.Path(exists=True), help="Compare against baseline JSON")
@click.option("--output", "-o", type=click.Path(), help="Write results to JSON file")
@click.option(
    "--profile",
    type=click.Choice(["standard", "comprehensive"]),
    default="standard",
    help="Output detail level",
)
@click.option("--threshold-time", default=0.10, help="Regression threshold for time")
@click.option("--threshold-memory", default=0.20, help="Regression threshold for memory")
@verbose_option
def benchmark_suite(
    operations,
    files,
    iterations,
    baseline_path,
    output,
    profile,
    threshold_time,
    threshold_memory,
    verbose,
):
    """
    Run comprehensive benchmark suite.

    Tests gpio operations across multiple file sizes with timing and memory tracking.
    """
    from pathlib import Path

    configure_verbose(verbose)
    from benchmarks.config import CORE_OPERATIONS, FULL_OPERATIONS
    from geoparquet_io.core.benchmark_report import format_table
    from geoparquet_io.core.benchmark_suite import run_benchmark_suite
    from geoparquet_io.core.logging_config import progress, success

    # Determine operations
    ops = CORE_OPERATIONS if operations == "core" else FULL_OPERATIONS

    # Resolve files
    if not files:
        raise click.ClickException("No files specified. Use --files with paths or size names.")

    input_files = []
    for f in files:
        path = Path(f)
        if path.exists():
            input_files.append(path)
        else:
            raise click.ClickException(f"File not found: {f}")

    progress(f"Running benchmark suite: {len(ops)} operations, {len(input_files)} files, {iterations} iterations")

    result = run_benchmark_suite(
        input_files=input_files,
        operations=ops,
        iterations=iterations,
        verbose=verbose,
    )

    # Display results
    progress("\n" + format_table(result.results))

    # Save if requested
    if output:
        Path(output).write_text(result.to_json())
        success(f"Results saved to {output}")
```

**Step 3: Run tests**

Run: `pytest tests/test_benchmark.py -v`
Expected: PASS

**Step 4: Commit**

```bash
git add geoparquet_io/cli/main.py tests/test_benchmark.py
git commit -m "Wire up benchmark suite CLI command"
```

---

## Summary

This plan covers the core implementation in 10 tasks:

1. **Config module** - Define operations, thresholds, file sizes
2. **Operation registry** - Map operation names to handler functions
3. **Benchmark runner core** - Single operation with timing/memory
4. **Suite runner** - Run multiple operations/files
5. **Regression comparison** - Compare against baseline
6. **CLI command group** - `gpio benchmark suite|compare|report`
7. **GitHub Actions** - CI workflow with Docker memory limits
8. **Dockerfile** - Container for memory-constrained runs
9. **Report formatting** - Table and Markdown output
10. **CLI wiring** - Connect CLI to runner

**Not covered (future tasks):**
- Downloading benchmark files from source.coop
- Comprehensive profiling mode (flame graphs, memory timeline)
- PR comment integration
- Trend reporting across versions
- Documentation updates
