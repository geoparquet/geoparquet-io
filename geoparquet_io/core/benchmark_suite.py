"""Benchmark suite runner for comprehensive performance testing."""

from __future__ import annotations

import gc
import json
import platform
import tempfile
import time
import tracemalloc
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

import duckdb
import psutil

from geoparquet_io.benchmarks.config import DEFAULT_THRESHOLDS, RegressionThresholds
from geoparquet_io.benchmarks.operations import get_operation
from geoparquet_io.core.logging_config import debug, progress


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    """Result from a single benchmark run.

    Immutable (frozen) to prevent accidental mutation after creation.
    Uses slots for ~10-20% memory savings.
    """

    operation: str
    file: str
    time_seconds: float
    peak_python_memory_mb: float  # tracemalloc - Python heap only
    peak_rss_memory_mb: float  # psutil RSS - includes PyArrow/DuckDB C memory
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

    Tracks both Python heap memory (tracemalloc) and total process RSS (psutil).
    This is critical because PyArrow and DuckDB allocate memory in C/Rust,
    which tracemalloc cannot see.

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

    # Force garbage collection for consistent baseline
    gc.collect()

    # Get baseline RSS before operation
    process = psutil.Process()
    baseline_rss = process.memory_info().rss

    # Start Python memory tracking
    tracemalloc.start()
    start_time = time.perf_counter()

    peak_rss = baseline_rss  # Track peak RSS during execution

    try:
        details = run_func(input_path, output_dir)
        elapsed = time.perf_counter() - start_time

        # Get peak RSS after operation
        current_rss = process.memory_info().rss
        peak_rss = max(peak_rss, current_rss)

        _, peak_python = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        # Calculate RSS delta from baseline
        rss_delta_mb = (peak_rss - baseline_rss) / (1024 * 1024)

        return BenchmarkResult(
            operation=operation,
            file=input_path.name,
            time_seconds=round(elapsed, 3),
            peak_python_memory_mb=round(peak_python / (1024 * 1024), 2),
            peak_rss_memory_mb=round(max(0, rss_delta_mb), 2),
            success=True,
            details=details or {},
            memory_limit_mb=memory_limit_mb,
            iteration=iteration,
        )

    except Exception as e:
        elapsed = time.perf_counter() - start_time
        try:
            _, peak_python = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            peak_python_mb = round(peak_python / (1024 * 1024), 2)
        except Exception:
            peak_python_mb = 0.0

        current_rss = process.memory_info().rss
        rss_delta_mb = (current_rss - baseline_rss) / (1024 * 1024)

        return BenchmarkResult(
            operation=operation,
            file=input_path.name,
            time_seconds=round(elapsed, 3),
            peak_python_memory_mb=peak_python_mb,
            peak_rss_memory_mb=round(max(0, rss_delta_mb), 2),
            success=False,
            error=str(e),
            memory_limit_mb=memory_limit_mb,
            iteration=iteration,
        )


@dataclass(slots=True)
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
        from geoparquet_io import __version__

        return __version__
    except ImportError:
        return "unknown"


def run_benchmark_suite(
    input_files: list[Path],
    operations: list[str] | None = None,
    iterations: int = 3,
    memory_limit_mb: int | None = None,
    warmup: bool = True,
    verbose: bool = False,
) -> SuiteResult:
    """
    Run the full benchmark suite.

    Args:
        input_files: List of input files to benchmark
        operations: Operations to run (default: core operations)
        iterations: Number of iterations per operation
        memory_limit_mb: Memory limit context (for reporting)
        warmup: Run a warmup iteration first (discarded from results)
        verbose: Show progress output

    Returns:
        SuiteResult with all benchmark data
    """
    from geoparquet_io.benchmarks.config import CORE_OPERATIONS

    if operations is None:
        operations = CORE_OPERATIONS

    results: list[BenchmarkResult] = []

    for input_file in input_files:
        input_path = Path(input_file)

        for operation in operations:
            # Warmup run (discarded) - avoids JIT/caching overhead on first run
            if warmup:
                debug(f"Warmup: {operation} ({input_path.name})")
                with tempfile.TemporaryDirectory() as output_dir:
                    run_single_operation(
                        operation=operation,
                        input_path=input_path,
                        output_dir=Path(output_dir),
                        iteration=0,  # Warmup iteration
                    )

            # Actual benchmark runs
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
                        status = "+" if result.success else "x"
                        progress(
                            f"  {status} {operation} ({input_path.name}) - {result.time_seconds}s"
                        )

    return SuiteResult(
        version=_get_version(),
        timestamp=datetime.now(timezone.utc).isoformat(),
        environment=get_environment_info(),
        results=results,
        config={
            "operations": operations,
            "iterations": iterations,
            "warmup": warmup,
            "memory_limit_mb": memory_limit_mb,
            "files": [str(f) for f in input_files],
        },
    )


class RegressionStatus(Enum):
    """Status of a regression comparison."""

    OK = "ok"
    WARNING = "warning"
    FAILURE = "failure"
    IMPROVED = "improved"


@dataclass(frozen=True, slots=True)
class ComparisonResult:
    """Result of comparing two benchmark results.

    Immutable (frozen) to prevent accidental mutation.
    """

    operation: str
    file: str
    baseline_time: float
    current_time: float
    time_delta_pct: float
    baseline_rss_memory: float
    current_rss_memory: float
    memory_delta_pct: float
    status: RegressionStatus


def compare_results(
    baseline: BenchmarkResult,
    current: BenchmarkResult,
    thresholds: RegressionThresholds | None = None,
) -> ComparisonResult:
    """
    Compare current result against baseline for regression.

    Uses RSS memory for comparison since it captures PyArrow/DuckDB allocations.

    Args:
        baseline: Baseline benchmark result
        current: Current benchmark result
        thresholds: Optional custom thresholds (uses DEFAULT_THRESHOLDS if None)

    Returns:
        ComparisonResult with delta and status
    """
    if thresholds is None:
        thresholds = DEFAULT_THRESHOLDS

    # Calculate deltas - use RSS memory (captures PyArrow/DuckDB allocations)
    # Guard against zero baseline time (instant operations or timing errors)
    if baseline.time_seconds > 0:
        time_delta = (current.time_seconds - baseline.time_seconds) / baseline.time_seconds
    else:
        time_delta = 0.0 if current.time_seconds == 0 else float("inf")
    baseline_mem = baseline.peak_rss_memory_mb
    current_mem = current.peak_rss_memory_mb
    memory_delta = (current_mem - baseline_mem) / max(baseline_mem, 0.01)

    # Determine status using RegressionThresholds dataclass attributes
    status = RegressionStatus.OK

    if time_delta < -0.05 and memory_delta < -0.05:
        status = RegressionStatus.IMPROVED
    elif time_delta >= thresholds.time_failure or memory_delta >= thresholds.memory_failure:
        status = RegressionStatus.FAILURE
    elif time_delta >= thresholds.time_warning or memory_delta >= thresholds.memory_warning:
        status = RegressionStatus.WARNING

    return ComparisonResult(
        operation=current.operation,
        file=current.file,
        baseline_time=baseline.time_seconds,
        current_time=current.time_seconds,
        time_delta_pct=round(time_delta, 4),
        baseline_rss_memory=baseline_mem,
        current_rss_memory=current_mem,
        memory_delta_pct=round(memory_delta, 4),
        status=status,
    )
