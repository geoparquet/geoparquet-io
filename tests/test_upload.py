"""
Tests for upload functionality.
"""

from pathlib import Path

from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.upload import parse_object_store_url


class TestUploadUrlParsing:
    """Test suite for object store URL parsing."""

    def test_parse_s3_url_with_prefix(self):
        """Test parsing S3 URL with prefix."""
        bucket_url, prefix = parse_object_store_url("s3://my-bucket/path/to/data/")
        assert bucket_url == "s3://my-bucket"
        assert prefix == "path/to/data/"

    def test_parse_s3_url_without_prefix(self):
        """Test parsing S3 URL without prefix."""
        bucket_url, prefix = parse_object_store_url("s3://my-bucket")
        assert bucket_url == "s3://my-bucket"
        assert prefix == ""

    def test_parse_s3_url_with_file(self):
        """Test parsing S3 URL with file path."""
        bucket_url, prefix = parse_object_store_url("s3://my-bucket/path/file.parquet")
        assert bucket_url == "s3://my-bucket"
        assert prefix == "path/file.parquet"

    def test_parse_gcs_url(self):
        """Test parsing GCS URL."""
        bucket_url, prefix = parse_object_store_url("gs://my-bucket/path/to/data/")
        assert bucket_url == "gs://my-bucket"
        assert prefix == "path/to/data/"

    def test_parse_azure_url(self):
        """Test parsing Azure URL."""
        bucket_url, prefix = parse_object_store_url("az://myaccount/mycontainer/path/to/data/")
        assert bucket_url == "az://myaccount/mycontainer"
        assert prefix == "path/to/data/"

    def test_parse_azure_url_minimal(self):
        """Test parsing Azure URL with just account and container."""
        bucket_url, prefix = parse_object_store_url("az://myaccount/mycontainer")
        assert bucket_url == "az://myaccount/mycontainer"
        assert prefix == ""

    def test_parse_https_url(self):
        """Test parsing HTTPS URL."""
        bucket_url, prefix = parse_object_store_url("https://example.com/data/")
        assert bucket_url == "https://example.com/data/"
        assert prefix == ""


class TestUploadDryRun:
    """Test suite for upload dry-run mode."""

    def test_upload_single_file_dry_run(self, places_test_file):
        """Test dry-run mode for single file upload."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "upload",
                places_test_file,
                "s3://test-bucket/path/output.parquet",
                "--dry-run",
            ],
        )

        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output
        assert "Would upload:" in result.output
        assert "Source:" in result.output
        assert "Size:" in result.output
        assert "Destination:" in result.output
        assert "Target key:" in result.output
        assert places_test_file in result.output
        assert "s3://test-bucket/path/output.parquet" in result.output

    def test_upload_single_file_dry_run_with_profile(self, places_test_file):
        """Test dry-run mode with AWS profile."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "upload",
                places_test_file,
                "s3://test-bucket/data.parquet",
                "--profile",
                "test-profile",
                "--dry-run",
            ],
        )

        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output
        assert "AWS Profile: test-profile" in result.output

    def test_upload_directory_dry_run(self, temp_output_dir):
        """Test dry-run mode for directory upload."""
        # Create some test files
        test_dir = Path(temp_output_dir) / "test_files"
        test_dir.mkdir()

        for i in range(5):
            (test_dir / f"file_{i}.parquet").write_text(f"test content {i}")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "upload",
                str(test_dir),
                "s3://test-bucket/dataset/",
                "--dry-run",
            ],
        )

        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output
        assert "Would upload 5 file(s)" in result.output
        assert "Source:" in result.output
        assert "Destination:" in result.output
        assert "Files that would be uploaded:" in result.output
        # Check that some files are listed
        assert "file_0.parquet" in result.output

    def test_upload_directory_with_pattern_dry_run(self, temp_output_dir):
        """Test dry-run mode with pattern filtering."""
        # Create mixed file types
        test_dir = Path(temp_output_dir) / "test_files"
        test_dir.mkdir()

        for i in range(3):
            (test_dir / f"data_{i}.parquet").write_text(f"parquet {i}")
            (test_dir / f"info_{i}.json").write_text(f"json {i}")
            (test_dir / f"readme_{i}.txt").write_text(f"text {i}")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "upload",
                str(test_dir),
                "s3://test-bucket/dataset/",
                "--pattern",
                "*.json",
                "--dry-run",
            ],
        )

        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output
        assert "Would upload 3 file(s)" in result.output
        assert "Pattern:     *.json" in result.output
        # Should only show JSON files
        assert "info_0.json" in result.output
        # Should not show parquet or txt files
        assert "data_0.parquet" not in result.output
        assert "readme_0.txt" not in result.output

    def test_upload_directory_truncates_long_list(self, temp_output_dir):
        """Test that dry-run truncates long file lists."""
        # Create more than 10 files
        test_dir = Path(temp_output_dir) / "test_files"
        test_dir.mkdir()

        for i in range(15):
            (test_dir / f"file_{i:02d}.parquet").write_text(f"test {i}")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "upload",
                str(test_dir),
                "s3://test-bucket/dataset/",
                "--dry-run",
            ],
        )

        assert result.exit_code == 0
        assert "Would upload 15 file(s)" in result.output
        # Should show truncation message
        assert "and 5 more file(s)" in result.output

    def test_upload_empty_directory_dry_run(self, temp_output_dir):
        """Test dry-run with empty directory."""
        test_dir = Path(temp_output_dir) / "empty"
        test_dir.mkdir()

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "upload",
                str(test_dir),
                "s3://test-bucket/dataset/",
                "--dry-run",
            ],
        )

        assert result.exit_code == 0
        assert "No files found" in result.output

    def test_upload_directory_pattern_no_match(self, temp_output_dir):
        """Test dry-run with pattern that matches no files."""
        test_dir = Path(temp_output_dir) / "test_files"
        test_dir.mkdir()

        # Create only parquet files
        for i in range(3):
            (test_dir / f"data_{i}.parquet").write_text(f"test {i}")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "upload",
                str(test_dir),
                "s3://test-bucket/dataset/",
                "--pattern",
                "*.csv",  # No CSV files exist
                "--dry-run",
            ],
        )

        assert result.exit_code == 0
        assert "No files found" in result.output
