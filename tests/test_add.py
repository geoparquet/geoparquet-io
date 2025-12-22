"""
Tests for add commands.
"""

import os

import duckdb
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import add


class TestAddCommands:
    """Test suite for add commands."""

    def test_add_bbox_to_buildings(self, buildings_test_file, temp_output_file):
        """Test adding bbox column to buildings file (which doesn't have bbox)."""
        runner = CliRunner()
        result = runner.invoke(add, ["bbox", buildings_test_file, temp_output_file])
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        # Verify bbox column was added
        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        column_names = [col[0] for col in columns]
        assert "bbox" in column_names

        # Verify row count matches
        input_count = conn.execute(f'SELECT COUNT(*) FROM "{buildings_test_file}"').fetchone()[0]
        output_count = conn.execute(f'SELECT COUNT(*) FROM "{temp_output_file}"').fetchone()[0]
        assert input_count == output_count

        # Verify bbox structure
        bbox_col = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        bbox_info = [col for col in bbox_col if col[0] == "bbox"][0]
        assert "STRUCT" in bbox_info[1]

    def test_add_bbox_to_places_skips_existing(self, places_test_file, temp_output_file):
        """Test adding bbox to file with existing bbox skips and informs user."""
        runner = CliRunner()
        result = runner.invoke(add, ["bbox", places_test_file, temp_output_file])
        # Should succeed but not create output file (bbox already exists)
        assert result.exit_code == 0
        assert "already has bbox column" in result.output or "covering metadata" in result.output
        # Output file should NOT be created since we're skipping
        assert not os.path.exists(temp_output_file)

    def test_add_bbox_force_replaces_existing(self, places_test_file, temp_output_file):
        """Test --force flag replaces existing bbox column."""
        runner = CliRunner()
        result = runner.invoke(add, ["bbox", places_test_file, temp_output_file, "--force"])
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)
        assert "Replacing existing bbox column" in result.output

        # Verify only 1 bbox column exists in output
        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        bbox_columns = [col for col in columns if col[0] == "bbox"]
        assert len(bbox_columns) == 1

        # Verify row count preserved
        input_count = conn.execute(f'SELECT COUNT(*) FROM "{places_test_file}"').fetchone()[0]
        output_count = conn.execute(f'SELECT COUNT(*) FROM "{temp_output_file}"').fetchone()[0]
        assert input_count == output_count

    def test_add_bbox_force_with_custom_name(self, places_test_file, temp_output_file):
        """Test --force with custom name keeps both columns and warns."""
        runner = CliRunner()
        result = runner.invoke(
            add, ["bbox", places_test_file, temp_output_file, "--force", "--bbox-name", "bounds"]
        )
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)
        # Should warn about 2 bbox columns
        assert "2 bbox columns" in result.output

        # Verify both bbox and bounds columns exist
        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        column_names = [col[0] for col in columns]
        assert "bbox" in column_names  # Original kept
        assert "bounds" in column_names  # New one added

    def test_add_bbox_with_custom_name(self, buildings_test_file, temp_output_file):
        """Test adding bbox column with custom name."""
        runner = CliRunner()
        result = runner.invoke(
            add, ["bbox", buildings_test_file, temp_output_file, "--bbox-name", "bounds"]
        )
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        # Verify custom bbox column name was used
        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        column_names = [col[0] for col in columns]
        assert "bounds" in column_names

    def test_add_bbox_with_verbose(self, buildings_test_file, temp_output_file):
        """Test adding bbox column with verbose flag."""
        runner = CliRunner()
        result = runner.invoke(add, ["bbox", buildings_test_file, temp_output_file, "--verbose"])
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

    def test_add_bbox_preserves_columns(self, buildings_test_file, temp_output_file):
        """Test that add bbox preserves all original columns."""
        runner = CliRunner()
        result = runner.invoke(add, ["bbox", buildings_test_file, temp_output_file])
        assert result.exit_code == 0

        # Verify columns are preserved
        conn = duckdb.connect()
        input_columns = conn.execute(f'DESCRIBE SELECT * FROM "{buildings_test_file}"').fetchall()
        output_columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()

        input_col_names = {col[0] for col in input_columns}
        output_col_names = {col[0] for col in output_columns}

        # All input columns should be in output
        assert input_col_names.issubset(output_col_names)
        # Output should have bbox column added
        assert "bbox" in output_col_names

    def test_add_bbox_nonexistent_file(self, temp_output_file):
        """Test add bbox on nonexistent file."""
        runner = CliRunner()
        result = runner.invoke(add, ["bbox", "nonexistent.parquet", temp_output_file])
        # Should fail with non-zero exit code
        assert result.exit_code != 0

    def test_add_bbox_with_metadata_always_added(self, buildings_test_file, temp_output_file):
        """Test that bbox metadata is automatically added."""
        import json

        import pyarrow.parquet as pq

        runner = CliRunner()
        result = runner.invoke(add, ["bbox", buildings_test_file, temp_output_file])
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        # Verify bbox metadata was added automatically
        pf = pq.ParquetFile(temp_output_file)
        metadata = pf.schema_arrow.metadata
        assert b"geo" in metadata

        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))

        # Verify bbox covering metadata exists
        assert "columns" in geo_meta
        assert "geometry" in geo_meta["columns"]
        assert "covering" in geo_meta["columns"]["geometry"]
        assert "bbox" in geo_meta["columns"]["geometry"]["covering"]

    def test_add_bbox_metadata_to_existing_bbox(self, temp_output_dir):
        """Test add bbox-metadata command for files with bbox column."""
        import shutil

        # Use places file which has a bbox column
        from pathlib import Path

        places_path = Path(__file__).parent / "data" / "places_test.parquet"
        temp_file = os.path.join(temp_output_dir, "places_copy.parquet")
        shutil.copy2(places_path, temp_file)

        runner = CliRunner()
        result = runner.invoke(add, ["bbox-metadata", temp_file])
        assert result.exit_code == 0
        # Should either add metadata or report it already exists
        assert "Added bbox covering metadata" in result.output or "already exists" in result.output

    def test_add_bbox_metadata_no_bbox_column(self, buildings_test_file):
        """Test add bbox-metadata when there's no bbox column."""
        runner = CliRunner()
        result = runner.invoke(add, ["bbox-metadata", buildings_test_file])
        # Should warn that no bbox column exists
        assert "No valid bbox column found" in result.output

    # H3 tests
    def test_add_h3_to_buildings(self, buildings_test_file, temp_output_file):
        """Test adding H3 column to buildings file."""
        runner = CliRunner()
        result = runner.invoke(add, ["h3", buildings_test_file, temp_output_file])
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        # Verify h3_cell column was added
        conn = duckdb.connect()
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("INSTALL h3 FROM community; LOAD h3;")

        columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        column_names = [col[0] for col in columns]
        assert "h3_cell" in column_names

        # Verify row count is preserved
        input_count = conn.execute(f'SELECT COUNT(*) FROM "{buildings_test_file}"').fetchone()[0]
        output_count = conn.execute(f'SELECT COUNT(*) FROM "{temp_output_file}"').fetchone()[0]
        assert input_count == output_count

        # Verify H3 column is VARCHAR
        h3_col = [col for col in columns if col[0] == "h3_cell"][0]
        assert "VARCHAR" in h3_col[1]

        # Verify H3 cells are valid
        valid_count = conn.execute(
            f'SELECT COUNT(*) FROM "{temp_output_file}" '
            f"WHERE h3_is_valid_cell(h3_string_to_h3(h3_cell))"
        ).fetchone()[0]
        assert valid_count == output_count

    def test_add_h3_default_resolution(self, buildings_test_file, temp_output_file):
        """Test that H3 uses resolution 9 by default."""
        runner = CliRunner()
        result = runner.invoke(add, ["h3", buildings_test_file, temp_output_file])
        assert result.exit_code == 0

        # Verify all cells are resolution 9
        conn = duckdb.connect()
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("INSTALL h3 FROM community; LOAD h3;")

        resolutions = conn.execute(
            f'SELECT DISTINCT h3_get_resolution(h3_string_to_h3(h3_cell)) FROM "{temp_output_file}"'
        ).fetchall()
        assert len(resolutions) == 1
        assert resolutions[0][0] == 9

    def test_add_h3_custom_resolution(self, buildings_test_file, temp_output_file):
        """Test adding H3 column with custom resolution."""
        runner = CliRunner()
        result = runner.invoke(
            add, ["h3", buildings_test_file, temp_output_file, "--resolution", "13"]
        )
        assert result.exit_code == 0

        # Verify all cells are resolution 13
        conn = duckdb.connect()
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("INSTALL h3 FROM community; LOAD h3;")

        resolutions = conn.execute(
            f'SELECT DISTINCT h3_get_resolution(h3_string_to_h3(h3_cell)) FROM "{temp_output_file}"'
        ).fetchall()
        assert len(resolutions) == 1
        assert resolutions[0][0] == 13

    def test_add_h3_with_custom_name(self, buildings_test_file, temp_output_file):
        """Test adding H3 column with custom name."""
        runner = CliRunner()
        result = runner.invoke(
            add, ["h3", buildings_test_file, temp_output_file, "--h3-name", "h3_building"]
        )
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        # Verify custom H3 column name was used
        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        column_names = [col[0] for col in columns]
        assert "h3_building" in column_names

    def test_add_h3_with_verbose(self, buildings_test_file, temp_output_file):
        """Test adding H3 column with verbose flag."""
        runner = CliRunner()
        result = runner.invoke(add, ["h3", buildings_test_file, temp_output_file, "--verbose"])
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)
        assert "Loading DuckDB extension: h3" in result.output

    def test_add_h3_preserves_columns(self, buildings_test_file, temp_output_file):
        """Test that add H3 preserves all original columns."""
        runner = CliRunner()
        result = runner.invoke(add, ["h3", buildings_test_file, temp_output_file])
        assert result.exit_code == 0

        # Verify columns are preserved
        conn = duckdb.connect()
        input_columns = conn.execute(f'DESCRIBE SELECT * FROM "{buildings_test_file}"').fetchall()
        output_columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()

        input_col_names = {col[0] for col in input_columns}
        output_col_names = {col[0] for col in output_columns}

        # All input columns should be in output
        assert input_col_names.issubset(output_col_names)
        # Output should have h3_cell column added
        assert "h3_cell" in output_col_names

    def test_add_h3_nonexistent_file(self, temp_output_file):
        """Test add H3 on nonexistent file."""
        runner = CliRunner()
        result = runner.invoke(add, ["h3", "nonexistent.parquet", temp_output_file])
        # Should fail with non-zero exit code
        assert result.exit_code != 0

    def test_add_h3_metadata(self, buildings_test_file, temp_output_file):
        """Test that H3 metadata is added to GeoParquet file."""
        import json

        import pyarrow.parquet as pq

        runner = CliRunner()
        result = runner.invoke(
            add, ["h3", buildings_test_file, temp_output_file, "--resolution", "13"]
        )
        assert result.exit_code == 0

        # Read metadata
        pf = pq.ParquetFile(temp_output_file)
        metadata = pf.schema_arrow.metadata
        assert b"geo" in metadata

        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))

        # Verify H3 covering metadata exists
        assert "columns" in geo_meta
        assert "geometry" in geo_meta["columns"]
        assert "covering" in geo_meta["columns"]["geometry"]
        assert "h3" in geo_meta["columns"]["geometry"]["covering"]

        # Verify H3 metadata content
        h3_meta = geo_meta["columns"]["geometry"]["covering"]["h3"]
        assert h3_meta["column"] == "h3_cell"
        assert h3_meta["resolution"] == 13

    def test_add_h3_invalid_resolution_too_low(self, buildings_test_file, temp_output_file):
        """Test adding H3 with invalid resolution (too low)."""
        runner = CliRunner()
        result = runner.invoke(
            add, ["h3", buildings_test_file, temp_output_file, "--resolution", "-1"]
        )
        # Should fail with error about invalid resolution
        assert result.exit_code != 0

    def test_add_h3_invalid_resolution_too_high(self, buildings_test_file, temp_output_file):
        """Test adding H3 with invalid resolution (too high)."""
        runner = CliRunner()
        result = runner.invoke(
            add, ["h3", buildings_test_file, temp_output_file, "--resolution", "16"]
        )
        # Should fail with error about invalid resolution
        assert result.exit_code != 0

    def test_add_h3_core_function_invalid_resolution(self, buildings_test_file, temp_output_file):
        """Test core add_h3_column function with invalid resolution (covers line 51)."""
        import click

        from geoparquet_io.core.add_h3_column import add_h3_column

        # Test resolution too high (bypassing CLI validation)
        with pytest.raises(click.BadParameter) as exc_info:
            add_h3_column(
                input_parquet=buildings_test_file,
                output_parquet=temp_output_file,
                h3_resolution=16,
                h3_column_name="h3_cell",
                verbose=False,
            )
        assert "H3 resolution must be between 0 and 15" in str(exc_info.value)

        # Test resolution too low
        with pytest.raises(click.BadParameter) as exc_info:
            add_h3_column(
                input_parquet=buildings_test_file,
                output_parquet=temp_output_file,
                h3_resolution=-1,
                h3_column_name="h3_cell",
                verbose=False,
            )
        assert "H3 resolution must be between 0 and 15" in str(exc_info.value)

    # Note: add admin-divisions tests are skipped because they require a countries file
    # and network access. These should be tested separately with appropriate test data.
    @pytest.mark.skip(reason="Requires countries file and network access")
    def test_add_admin_divisions(self, places_test_file, temp_output_file):
        """Test adding admin divisions (skipped - requires countries file)."""
        pass


class TestRemoteWriteSupport:
    """Tests for remote write functionality."""

    def test_remote_url_detection(self):
        """Test that remote URLs are correctly detected."""
        from geoparquet_io.core.common import is_remote_url

        # Test S3 URLs
        assert is_remote_url("s3://bucket/file.parquet")
        assert is_remote_url("s3://my-bucket/path/to/file.parquet")

        # Test GCS URLs
        assert is_remote_url("gs://bucket/file.parquet")

        # Test Azure URLs
        assert is_remote_url("az://container/file.parquet")

        # Test HTTP/HTTPS URLs
        assert is_remote_url("https://example.com/data.parquet")
        assert is_remote_url("http://example.com/data.parquet")

        # Test local paths (should return False)
        assert not is_remote_url("local.parquet")
        assert not is_remote_url("/path/to/file.parquet")
        assert not is_remote_url("./relative/path.parquet")

    def test_write_with_remote_output_creates_temp_file(self, buildings_test_file):
        """Test that remote outputs trigger temp file creation."""
        from unittest.mock import MagicMock, patch

        import duckdb

        from geoparquet_io.core.common import write_parquet_with_metadata

        # Mock the upload function to avoid actual upload
        mock_upload = MagicMock()

        with patch("geoparquet_io.core.upload.upload", mock_upload):
            # Create a mock DuckDB connection
            con = duckdb.connect()
            con.execute("INSTALL spatial; LOAD spatial;")

            # Simple query to read the test file
            from geoparquet_io.core.common import safe_file_url

            input_url = safe_file_url(buildings_test_file, False)
            query = f"SELECT * FROM '{input_url}' LIMIT 10"

            # Remote S3 output
            remote_output = "s3://test-bucket/output.parquet"

            # This should create a temp file and attempt to upload
            write_parquet_with_metadata(
                con=con, query=query, output_file=remote_output, verbose=False
            )

            # Verify upload was called
            assert mock_upload.called
            # Check that the source was a local temp file
            call_args = mock_upload.call_args
            assert call_args is not None
            source_path = str(call_args[1]["source"])  # Keyword arg 'source'
            assert source_path.endswith(".parquet")
            # Destination should be the remote URL
            assert call_args[1]["destination"] == remote_output
