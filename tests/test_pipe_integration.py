"""
Integration tests for Arrow IPC piping between CLI commands.

Tests multi-stage pipelines like:
    gpio add bbox input.parquet | gpio sort hilbert - output.parquet
    gpio extract input.parquet | gpio add bbox - | gpio add quadkey - output.parquet
"""

from __future__ import annotations

import subprocess
import tempfile
import uuid
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from tests.conftest import safe_rmtree, safe_unlink

TEST_DATA_DIR = Path(__file__).parent / "data"
PLACES_PARQUET = TEST_DATA_DIR / "places_test.parquet"


def run_pipeline(commands: list[str], timeout: int = 60) -> subprocess.CompletedProcess:
    """Run a shell pipeline and return the result."""
    pipeline = " | ".join(commands)
    return subprocess.run(
        pipeline,
        shell=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


@pytest.fixture
def output_file():
    """Create a temporary output file path."""
    tmp_path = Path(tempfile.gettempdir()) / f"test_pipe_{uuid.uuid4()}.parquet"
    yield str(tmp_path)
    safe_unlink(tmp_path)


@pytest.fixture
def output_dir():
    """Create a temporary output directory."""
    tmp_path = Path(tempfile.gettempdir()) / f"test_pipe_dir_{uuid.uuid4()}"
    tmp_path.mkdir(exist_ok=True)
    yield str(tmp_path)
    safe_rmtree(tmp_path)


class TestTwoStagePipelines:
    """Tests for two-stage command pipelines."""

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_add_bbox_to_sort_hilbert(self, output_file):
        """Test: gpio add bbox input | gpio sort hilbert - output."""
        result = run_pipeline(
            [
                f"gpio add bbox {PLACES_PARQUET} -",
                f"gpio sort hilbert - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        assert Path(output_file).exists()

        # Verify output has bbox and is sorted
        table = pq.read_table(output_file)
        assert "bbox" in table.column_names
        assert table.num_rows == 766

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_extract_to_add_bbox(self, output_file):
        """Test: gpio extract --limit 100 input | gpio add bbox - output."""
        result = run_pipeline(
            [
                f"gpio extract --limit 100 {PLACES_PARQUET} -",
                f"gpio add bbox - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        assert Path(output_file).exists()

        table = pq.read_table(output_file)
        assert "bbox" in table.column_names
        assert table.num_rows == 100

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_add_bbox_to_add_quadkey(self, output_file):
        """Test: gpio add bbox input | gpio add quadkey - output."""
        result = run_pipeline(
            [
                f"gpio add bbox {PLACES_PARQUET} -",
                f"gpio add quadkey - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        assert Path(output_file).exists()

        table = pq.read_table(output_file)
        assert "bbox" in table.column_names
        assert "quadkey" in table.column_names
        assert table.num_rows == 766


class TestThreeStagePipelines:
    """Tests for three-stage command pipelines."""

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_extract_add_bbox_add_quadkey(self, output_file):
        """Test: extract | add bbox | add quadkey."""
        result = run_pipeline(
            [
                f"gpio extract --limit 50 {PLACES_PARQUET} -",
                "gpio add bbox - -",
                f"gpio add quadkey - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        assert Path(output_file).exists()

        table = pq.read_table(output_file)
        assert "bbox" in table.column_names
        assert "quadkey" in table.column_names
        assert table.num_rows == 50

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_add_bbox_add_quadkey_sort_hilbert(self, output_file):
        """Test: add bbox | add quadkey | sort hilbert."""
        result = run_pipeline(
            [
                f"gpio add bbox {PLACES_PARQUET} -",
                "gpio add quadkey - -",
                f"gpio sort hilbert - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        assert Path(output_file).exists()

        table = pq.read_table(output_file)
        assert "bbox" in table.column_names
        assert "quadkey" in table.column_names
        assert table.num_rows == 766


class TestPartitionWithPipes:
    """Tests for partition command with stdin input."""

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_add_quadkey_to_partition(self, output_dir):
        """Test: add quadkey | partition string (stdin to directory)."""
        result = run_pipeline(
            [
                f"gpio add quadkey {PLACES_PARQUET} -",
                f"gpio partition string --column quadkey --chars 2 - {output_dir}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"

        # Check that partitioned files were created
        output_path = Path(output_dir)
        parquet_files = list(output_path.glob("**/*.parquet"))
        assert len(parquet_files) > 0, "No partitioned files created"


class TestFullPipeline:
    """Tests for full multi-stage pipelines."""

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_full_transform_pipeline(self, output_file):
        """Test: extract | add bbox | add quadkey | sort hilbert."""
        result = run_pipeline(
            [
                f"gpio extract --limit 100 {PLACES_PARQUET} -",
                "gpio add bbox - -",
                "gpio add quadkey - -",
                f"gpio sort hilbert - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        assert Path(output_file).exists()

        table = pq.read_table(output_file)
        assert "bbox" in table.column_names
        assert "quadkey" in table.column_names
        assert table.num_rows == 100


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_single_row_pipeline(self, output_file):
        """Test pipeline with single row extract."""
        result = run_pipeline(
            [
                f"gpio extract --limit 1 {PLACES_PARQUET} -",
                f"gpio add bbox - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"

        table = pq.read_table(output_file)
        assert table.num_rows == 1
        assert "bbox" in table.column_names

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_column_selection_through_pipe(self, output_file):
        """Test that column selection works through pipe."""
        result = run_pipeline(
            [
                f"gpio extract --include-cols name,address {PLACES_PARQUET} -",
                f"gpio add bbox - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"

        table = pq.read_table(output_file)
        # Should have: name, address, geometry (auto-included), bbox (added)
        assert "name" in table.column_names
        assert "address" in table.column_names
        assert "geometry" in table.column_names
        assert "bbox" in table.column_names
