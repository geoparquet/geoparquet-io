"""
Performance benchmarks comparing file-based vs piped workflows.

These tests measure and compare:
1. File-based workflow (multiple intermediate files)
2. Piped workflow (Arrow IPC streaming, no intermediate files)
3. Python API workflow (in-memory Arrow tables)

Run with: pytest tests/test_benchmark_piping.py -v -s
"""

from __future__ import annotations

import os
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from tests.conftest import safe_rmtree

# Benchmark file - 78MB GeoParquet
# Use environment variable with fallback to allow contributors to override
BENCHMARK_FILE = Path(
    os.environ.get("BENCHMARK_FILE", "/Users/cholmes/Downloads/ca-fiboa-nobbox.parquet")
)


def run_command(cmd: str, timeout: int = 300) -> tuple[float, subprocess.CompletedProcess]:
    """Run a shell command and return elapsed time and result."""
    start = time.perf_counter()
    result = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    elapsed = time.perf_counter() - start
    return elapsed, result


@pytest.fixture
def temp_dir():
    """Create a temporary directory for benchmark outputs."""
    tmp_path = Path(tempfile.gettempdir()) / f"benchmark_{uuid.uuid4()}"
    tmp_path.mkdir(exist_ok=True)
    yield tmp_path
    safe_rmtree(tmp_path)


def _run_file_based_workflow(temp_dir: Path) -> float:
    """
    Run file-based workflow and return total elapsed time.

    gpio add bbox input tmp1.parquet
    gpio add quadkey tmp1.parquet tmp2.parquet
    gpio sort hilbert tmp2.parquet output.parquet
    """
    tmp1 = temp_dir / "tmp1.parquet"
    tmp2 = temp_dir / "tmp2.parquet"
    output = temp_dir / "output_file.parquet"

    # Step 1: add bbox
    elapsed1, result1 = run_command(f"gpio add bbox {BENCHMARK_FILE} {tmp1}")
    assert result1.returncode == 0, f"Step 1 failed: {result1.stderr}"

    # Step 2: add quadkey
    elapsed2, result2 = run_command(f"gpio add quadkey {tmp1} {tmp2}")
    assert result2.returncode == 0, f"Step 2 failed: {result2.stderr}"

    # Step 3: sort hilbert
    elapsed3, result3 = run_command(f"gpio sort hilbert {tmp2} {output}")
    assert result3.returncode == 0, f"Step 3 failed: {result3.stderr}"

    total_elapsed = elapsed1 + elapsed2 + elapsed3

    # Verify output
    assert output.exists()
    table = pq.read_table(output)
    assert "bbox" in table.column_names
    assert "quadkey" in table.column_names

    print("\n=== FILE-BASED WORKFLOW ===")
    print(f"  Step 1 (add bbox):     {elapsed1:.2f}s")
    print(f"  Step 2 (add quadkey):  {elapsed2:.2f}s")
    print(f"  Step 3 (sort hilbert): {elapsed3:.2f}s")
    print(f"  TOTAL:                 {total_elapsed:.2f}s")
    print(f"  Output rows:           {table.num_rows}")

    return total_elapsed


def _run_piped_workflow(temp_dir: Path) -> float:
    """
    Run piped workflow and return elapsed time.

    gpio add bbox input - | gpio add quadkey - - | gpio sort hilbert - output.parquet
    """
    output = temp_dir / "output_piped.parquet"

    pipeline = (
        f"gpio add bbox {BENCHMARK_FILE} - | gpio add quadkey - - | gpio sort hilbert - {output}"
    )

    elapsed, result = run_command(pipeline)
    assert result.returncode == 0, f"Pipeline failed: {result.stderr}"

    # Verify output
    assert output.exists()
    table = pq.read_table(output)
    assert "bbox" in table.column_names
    assert "quadkey" in table.column_names

    print("\n=== PIPED WORKFLOW ===")
    print(f"  TOTAL:       {elapsed:.2f}s")
    print(f"  Output rows: {table.num_rows}")

    return elapsed


def _run_python_api_workflow(temp_dir: Path) -> float:
    """
    Run Python API workflow and return elapsed time.

    gpio.read(input).add_bbox().add_quadkey().sort_hilbert().write(output)
    """
    from geoparquet_io.api import read

    output = temp_dir / "output_api.parquet"

    start = time.perf_counter()

    read(BENCHMARK_FILE).add_bbox().add_quadkey().sort_hilbert().write(str(output))

    elapsed = time.perf_counter() - start

    # Verify output
    assert output.exists()
    result = pq.read_table(output)
    assert "bbox" in result.column_names
    assert "quadkey" in result.column_names

    print("\n=== PYTHON API WORKFLOW ===")
    print(f"  TOTAL:       {elapsed:.2f}s")
    print(f"  Output rows: {result.num_rows}")

    return elapsed


@pytest.mark.skipif(not BENCHMARK_FILE.exists(), reason="Benchmark file not available")
@pytest.mark.slow
class TestPipingPerformance:
    """Performance comparison between file-based and piped workflows."""

    def test_file_based_workflow(self, temp_dir):
        """File-based workflow: Multiple intermediate files."""
        _run_file_based_workflow(temp_dir)

    def test_piped_workflow(self, temp_dir):
        """Piped workflow: Single pipeline, no intermediate files."""
        _run_piped_workflow(temp_dir)

    def test_python_api_workflow(self, temp_dir):
        """Python API workflow: In-memory Arrow tables."""
        _run_python_api_workflow(temp_dir)

    def test_compare_all_workflows(self, temp_dir):
        """Run all workflows and compare performance."""
        print("\n" + "=" * 60)
        print("PERFORMANCE COMPARISON: file-based vs piped vs Python API")
        print("=" * 60)
        print(f"Benchmark file: {BENCHMARK_FILE}")
        print(f"File size: {BENCHMARK_FILE.stat().st_size / 1024 / 1024:.1f} MB")

        # Run all workflows using helper functions
        file_time = _run_file_based_workflow(temp_dir)
        pipe_time = _run_piped_workflow(temp_dir)
        api_time = _run_python_api_workflow(temp_dir)

        # Summary
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"  File-based:  {file_time:.2f}s")
        print(f"  Piped:       {pipe_time:.2f}s ({(1 - pipe_time / file_time) * 100:+.1f}%)")
        print(f"  Python API:  {api_time:.2f}s ({(1 - api_time / file_time) * 100:+.1f}%)")
        print("=" * 60)


@pytest.mark.skipif(not BENCHMARK_FILE.exists(), reason="Benchmark file not available")
@pytest.mark.slow
class TestExtractPerformance:
    """Performance tests for extract with different row counts."""

    @pytest.mark.parametrize("limit", [1000, 10000, 100000])
    def test_extract_piped_vs_file(self, temp_dir, limit):
        """Compare extract + add_bbox performance for different row counts."""
        output_file = temp_dir / f"file_{limit}.parquet"
        output_pipe = temp_dir / f"pipe_{limit}.parquet"
        tmp = temp_dir / f"tmp_{limit}.parquet"

        # File-based
        cmd1 = f"gpio extract --limit {limit} {BENCHMARK_FILE} {tmp}"
        cmd2 = f"gpio add bbox {tmp} {output_file}"

        elapsed1, r1 = run_command(cmd1)
        assert r1.returncode == 0
        elapsed2, r2 = run_command(cmd2)
        assert r2.returncode == 0
        file_time = elapsed1 + elapsed2

        # Piped
        pipeline = (
            f"gpio extract --limit {limit} {BENCHMARK_FILE} - | gpio add bbox - {output_pipe}"
        )
        pipe_time, r3 = run_command(pipeline)
        assert r3.returncode == 0

        speedup = (1 - pipe_time / file_time) * 100

        print(f"\n[{limit} rows] File: {file_time:.2f}s, Pipe: {pipe_time:.2f}s ({speedup:+.1f}%)")

        # Verify both outputs have same row count
        t1 = pq.read_table(output_file)
        t2 = pq.read_table(output_pipe)
        assert t1.num_rows == t2.num_rows == limit
