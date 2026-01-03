"""
Tests for upload functionality.
"""

from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.upload import (
    _create_s3_store_with_endpoint,
    _setup_store_and_kwargs,
    parse_object_store_url,
)


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


class TestS3EndpointConfiguration:
    """Test suite for S3 endpoint configuration."""

    def test_create_s3_store_with_endpoint_https(self):
        """Test creating S3Store with HTTPS endpoint."""
        with patch("geoparquet_io.core.upload.S3Store") as mock_store:
            _create_s3_store_with_endpoint(
                bucket_url="s3://my-bucket/data.parquet",
                s3_endpoint="minio.example.com:9000",
                s3_region="us-west-2",
                s3_use_ssl=True,
            )

            mock_store.assert_called_once_with(
                "my-bucket",
                endpoint="https://minio.example.com:9000",
                region="us-west-2",
            )

    def test_create_s3_store_with_endpoint_http(self):
        """Test creating S3Store with HTTP endpoint (no SSL)."""
        with patch("geoparquet_io.core.upload.S3Store") as mock_store:
            _create_s3_store_with_endpoint(
                bucket_url="s3://my-bucket/data.parquet",
                s3_endpoint="minio.local:9000",
                s3_region=None,  # Should default to us-east-1
                s3_use_ssl=False,
            )

            mock_store.assert_called_once_with(
                "my-bucket",
                endpoint="http://minio.local:9000",
                region="us-east-1",
            )

    def test_setup_store_with_custom_endpoint(self):
        """Test _setup_store_and_kwargs uses S3Store for custom endpoint."""
        with patch("geoparquet_io.core.upload.S3Store") as mock_s3store:
            with patch("geoparquet_io.core.upload.obs.store.from_url") as mock_from_url:
                _setup_store_and_kwargs(
                    bucket_url="s3://my-bucket",
                    profile=None,
                    chunk_concurrency=12,
                    chunk_size=None,
                    s3_endpoint="custom.endpoint.com",
                    s3_region="eu-west-1",
                    s3_use_ssl=True,
                )

                # Should use S3Store, not from_url
                mock_s3store.assert_called_once()
                mock_from_url.assert_not_called()

    def test_setup_store_without_endpoint_uses_from_url(self):
        """Test _setup_store_and_kwargs uses from_url when no custom endpoint."""
        with patch("geoparquet_io.core.upload.S3Store") as mock_s3store:
            with patch("geoparquet_io.core.upload.obs.store.from_url") as mock_from_url:
                _setup_store_and_kwargs(
                    bucket_url="s3://my-bucket",
                    profile=None,
                    chunk_concurrency=12,
                    chunk_size=None,
                    # No s3_endpoint
                )

                # Should use from_url, not S3Store
                mock_from_url.assert_called_once_with("s3://my-bucket")
                mock_s3store.assert_not_called()

    def test_setup_store_returns_kwargs(self):
        """Test _setup_store_and_kwargs returns correct kwargs."""
        with patch("geoparquet_io.core.upload.obs.store.from_url"):
            store, kwargs = _setup_store_and_kwargs(
                bucket_url="s3://my-bucket",
                profile=None,
                chunk_concurrency=24,
                chunk_size=16 * 1024 * 1024,
            )

            assert kwargs["max_concurrency"] == 24
            assert kwargs["chunk_size"] == 16 * 1024 * 1024


class TestUploadCLIS3Options:
    """Test suite for S3 endpoint CLI options."""

    def test_upload_with_s3_endpoint_dry_run(self, places_test_file):
        """Test dry-run mode with S3 endpoint options."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "upload",
                places_test_file,
                "s3://test-bucket/data.parquet",
                "--s3-endpoint",
                "minio.example.com:9000",
                "--s3-no-ssl",
                "--dry-run",
            ],
        )

        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output

    def test_upload_with_s3_region_dry_run(self, places_test_file):
        """Test dry-run mode with S3 region option."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "upload",
                places_test_file,
                "s3://test-bucket/data.parquet",
                "--s3-region",
                "eu-west-1",
                "--dry-run",
            ],
        )

        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output
