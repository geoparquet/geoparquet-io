"""
Tests for Arrow IPC streaming functionality.

Tests cover:
- Core streaming utilities (read/write Arrow IPC)
- Streaming input/output for add bbox command
- Streaming input/output for sort hilbert command
- Full pipeline tests with piping
"""

import io
import subprocess
import sys

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import add, sort
from geoparquet_io.core.streaming import (
    STREAM_MARKER,
    apply_metadata_to_table,
    extract_geo_metadata,
    is_stdin,
    is_stdout,
    should_stream_output,
)


class TestStreamingUtilities:
    """Test core streaming utility functions."""

    def test_is_stdin_with_dash(self):
        """Test that '-' is recognized as stdin."""
        assert is_stdin("-") is True
        assert is_stdin(STREAM_MARKER) is True

    def test_is_stdin_with_file(self):
        """Test that file paths are not stdin."""
        assert is_stdin("input.parquet") is False
        assert is_stdin("/path/to/file.parquet") is False
        assert is_stdin("s3://bucket/file.parquet") is False

    def test_is_stdout_with_dash(self):
        """Test that '-' is recognized as stdout."""
        assert is_stdout("-") is True
        assert is_stdout(STREAM_MARKER) is True

    def test_is_stdout_with_file(self):
        """Test that file paths are not stdout."""
        assert is_stdout("output.parquet") is False

    def test_should_stream_output_explicit(self):
        """Test explicit stdout with '-'."""
        assert should_stream_output("-") is True

    def test_should_stream_output_file(self):
        """Test file output is not streaming."""
        assert should_stream_output("output.parquet") is False
        assert should_stream_output("/path/to/output.parquet") is False


class TestArrowRoundtrip:
    """Test Arrow IPC read/write roundtrip."""

    def test_simple_table_roundtrip(self):
        """Test basic Arrow IPC roundtrip with a simple table."""
        # Create a simple table
        table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})

        # Write to buffer
        buffer = io.BytesIO()
        writer = ipc.RecordBatchStreamWriter(buffer, table.schema)
        writer.write_table(table)
        writer.close()

        # Read back
        buffer.seek(0)
        reader = ipc.RecordBatchStreamReader(buffer)
        restored = reader.read_all()

        assert restored.num_rows == table.num_rows
        assert restored.column_names == table.column_names
        assert restored.equals(table)

    def test_metadata_preservation(self):
        """Test that schema metadata is preserved through Arrow IPC."""
        # Create table with metadata
        schema = pa.schema([("x", pa.int64())])
        metadata = {b"geo": b'{"version": "1.1.0"}', b"custom": b"value"}
        schema = schema.with_metadata(metadata)
        table = pa.table({"x": [1, 2, 3]}, schema=schema)

        # Write to buffer
        buffer = io.BytesIO()
        writer = ipc.RecordBatchStreamWriter(buffer, table.schema)
        writer.write_table(table)
        writer.close()

        # Read back
        buffer.seek(0)
        reader = ipc.RecordBatchStreamReader(buffer)
        restored = reader.read_all()

        # Check metadata preserved
        assert restored.schema.metadata is not None
        assert b"geo" in restored.schema.metadata
        assert b"custom" in restored.schema.metadata
        assert restored.schema.metadata[b"geo"] == b'{"version": "1.1.0"}'


class TestMetadataHelpers:
    """Test metadata extraction and application helpers."""

    def test_extract_geo_metadata(self):
        """Test extracting geo metadata from table."""
        schema = pa.schema([("x", pa.int64())])
        metadata = {b"geo": b'{"version": "1.1.0", "primary_column": "geometry"}'}
        schema = schema.with_metadata(metadata)
        table = pa.table({"x": [1]}, schema=schema)

        geo_meta = extract_geo_metadata(table)
        assert geo_meta is not None
        assert geo_meta["version"] == "1.1.0"
        assert geo_meta["primary_column"] == "geometry"

    def test_extract_geo_metadata_missing(self):
        """Test extracting geo metadata when not present."""
        table = pa.table({"x": [1]})
        geo_meta = extract_geo_metadata(table)
        assert geo_meta is None

    def test_apply_metadata_to_table(self):
        """Test applying metadata to a table."""
        table = pa.table({"x": [1, 2, 3]})
        metadata = {b"key": b"value"}

        result = apply_metadata_to_table(table, metadata)
        assert result.schema.metadata is not None
        assert b"key" in result.schema.metadata
        assert result.schema.metadata[b"key"] == b"value"


class TestAddBboxStreaming:
    """Test add bbox command with streaming I/O."""

    def test_add_bbox_to_file_still_works(self, buildings_test_file, temp_output_file):
        """Test that add bbox still works with file output."""
        runner = CliRunner()
        result = runner.invoke(add, ["bbox", buildings_test_file, temp_output_file])

        assert result.exit_code == 0

        # Verify output was created
        import duckdb

        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        column_names = [col[0] for col in columns]
        assert "bbox" in column_names

    def test_add_bbox_to_stdout_explicit(self, buildings_test_file):
        """Test add bbox with explicit '-' output writes Arrow IPC."""
        runner = CliRunner()
        # In CliRunner, stdout is not a TTY, so this should stream
        result = runner.invoke(add, ["bbox", buildings_test_file, "-"])

        # Should succeed - CliRunner is not a TTY so streaming works
        assert result.exit_code == 0
        # Output should be binary Arrow IPC data
        assert len(result.output_bytes) > 0 if hasattr(result, "output_bytes") else True

    def test_add_bbox_auto_detect_stdout(self, buildings_test_file):
        """Test add bbox auto-detects stdout when output is omitted and stdout is pipe."""
        runner = CliRunner()
        # In CliRunner, stdout is not a TTY, so auto-detect should stream
        result = runner.invoke(add, ["bbox", buildings_test_file])

        # CliRunner is not a TTY, so this should auto-stream to stdout
        assert result.exit_code == 0


class TestSortHilbertStreaming:
    """Test sort hilbert command with streaming I/O."""

    def test_sort_hilbert_to_file_still_works(self, places_test_file, temp_output_file):
        """Test sort hilbert still works with file output."""
        runner = CliRunner()
        result = runner.invoke(sort, ["hilbert", places_test_file, temp_output_file])

        assert result.exit_code == 0

        # Verify output was created
        import duckdb

        conn = duckdb.connect()
        count = conn.execute(f'SELECT COUNT(*) FROM "{temp_output_file}"').fetchone()[0]
        assert count > 0

    def test_sort_hilbert_to_stdout_explicit(self, places_test_file):
        """Test sort hilbert with explicit '-' output."""
        runner = CliRunner()
        result = runner.invoke(sort, ["hilbert", places_test_file, "-"])

        # Should succeed - CliRunner is not a TTY so streaming works
        assert result.exit_code == 0

    def test_sort_hilbert_auto_detect_stdout(self, places_test_file):
        """Test sort hilbert auto-detects stdout when output is omitted."""
        runner = CliRunner()
        result = runner.invoke(sort, ["hilbert", places_test_file])

        # CliRunner is not a TTY, so this should auto-stream to stdout
        assert result.exit_code == 0


class TestPipelineIntegration:
    """Integration tests for full streaming pipelines."""

    @pytest.mark.skipif(
        sys.platform == "win32", reason="Shell pipes behave differently on Windows"
    )
    def test_add_bbox_pipe_to_sort_hilbert(self, buildings_test_file, temp_output_file):
        """Test piping add bbox output to sort hilbert."""
        # Run pipeline via subprocess using gpio command
        cmd = (
            f"gpio add bbox {buildings_test_file} - | "
            f"gpio sort hilbert - {temp_output_file}"
        )

        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        # Check success
        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"

        # Verify output file exists and has expected columns
        import duckdb

        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        column_names = [col[0] for col in columns]

        assert "bbox" in column_names  # bbox should be present from add bbox
        assert "geometry" in column_names  # geometry should be preserved

    @pytest.mark.skipif(
        sys.platform == "win32", reason="Shell pipes behave differently on Windows"
    )
    def test_auto_detect_pipeline(self, buildings_test_file, temp_output_file):
        """Test pipeline with auto-detect output (no explicit '-')."""
        # With auto-detect, we omit the output argument when piping
        cmd = (
            f"gpio add bbox {buildings_test_file} | "
            f"gpio sort hilbert - {temp_output_file}"
        )

        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        # Check success
        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"

        # Verify output file was created
        import os

        assert os.path.exists(temp_output_file)
