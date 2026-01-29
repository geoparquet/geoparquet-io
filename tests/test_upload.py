"""
Tests for upload functionality.
"""

import importlib
import os
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

# Use importlib to get the actual module (avoids namespace collision with cli group)
main_module = importlib.import_module("geoparquet_io.cli.main")
cli = main_module.cli
from geoparquet_io.core.upload import (  # noqa: E402
    _check_azure_credentials,
    _check_gcs_credentials,
    _check_s3_credentials,
    _setup_store_and_kwargs,
    _try_infer_region_from_bucket,
    check_credentials,
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
        with patch.object(main_module, "check_credentials", return_value=(True, "")):
            result = runner.invoke(
                cli,
                [
                    "publish",
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
        # Mock credential check to pass (since test-profile doesn't exist)
        with patch.object(main_module, "check_credentials", return_value=(True, "")):
            result = runner.invoke(
                cli,
                [
                    "publish",
                    "upload",
                    places_test_file,
                    "s3://test-bucket/data.parquet",
                    "--aws-profile",
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
        with patch.object(main_module, "check_credentials", return_value=(True, "")):
            result = runner.invoke(
                cli,
                [
                    "publish",
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
        with patch.object(main_module, "check_credentials", return_value=(True, "")):
            result = runner.invoke(
                cli,
                [
                    "publish",
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
        with patch.object(main_module, "check_credentials", return_value=(True, "")):
            result = runner.invoke(
                cli,
                [
                    "publish",
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
        with patch.object(main_module, "check_credentials", return_value=(True, "")):
            result = runner.invoke(
                cli,
                [
                    "publish",
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
        with patch.object(main_module, "check_credentials", return_value=(True, "")):
            result = runner.invoke(
                cli,
                [
                    "publish",
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


class TestRegionInference:
    """Test suite for region inference from bucket names."""

    def test_infer_us_west_2_from_bucket(self):
        """Test inferring us-west-2 region from bucket name."""
        result = _try_infer_region_from_bucket("us-west-2.opendata.source.coop")
        assert result == "us-west-2"

    def test_infer_eu_central_1_from_bucket(self):
        """Test inferring eu-central-1 region from bucket name."""
        result = _try_infer_region_from_bucket("eu-central-1.example.com")
        assert result == "eu-central-1"

    def test_no_region_in_bucket_name(self):
        """Test returns None when no region in bucket name."""
        result = _try_infer_region_from_bucket("my-normal-bucket")
        assert result is None

    def test_no_region_in_regular_domain(self):
        """Test returns None for regular domain bucket name."""
        result = _try_infer_region_from_bucket("example.com")
        assert result is None


class TestCredentialChecking:
    """Test suite for credential checking functionality."""

    def test_check_credentials_with_env_vars(self):
        """Test credential checking passes with environment variables."""
        with patch.dict(
            "os.environ", {"AWS_ACCESS_KEY_ID": "key", "AWS_SECRET_ACCESS_KEY": "secret"}
        ):
            ok, hint = check_credentials("s3://bucket/path")
            assert ok is True
            assert hint == ""

    def test_check_credentials_without_env_vars(self):
        """Test credential checking fails without environment variables or default profile."""
        with patch.dict("os.environ", {}, clear=True):
            # Also mock the default profile fallback to return no credentials
            with patch(
                "geoparquet_io.core.upload._load_aws_credentials_from_profile",
                return_value=(None, None, None),
            ):
                ok, hint = check_credentials("s3://bucket/path")
                assert ok is False
                assert "S3 credentials not found" in hint

    def test_check_credentials_with_default_profile_fallback(self):
        """Test credential checking falls back to default profile in ~/.aws/credentials."""
        with patch.dict("os.environ", {}, clear=True):
            # Mock the default profile to return valid credentials
            with patch(
                "geoparquet_io.core.upload._load_aws_credentials_from_profile",
                return_value=("access_key", "secret_key", "us-west-2"),
            ):
                ok, hint = check_credentials("s3://bucket/path")
                assert ok is True
                assert hint == ""

    def test_check_credentials_http_always_ok(self):
        """Test credential checking passes for HTTP URLs."""
        ok, hint = check_credentials("https://example.com/file.parquet")
        assert ok is True
        assert hint == ""


class TestS3EndpointConfiguration:
    """Test suite for S3 endpoint configuration."""

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

    def test_setup_store_for_s3_uses_s3store(self):
        """Test _setup_store_and_kwargs uses S3Store for S3 URLs."""
        with patch("geoparquet_io.core.upload.S3Store") as mock_s3store:
            with patch("geoparquet_io.core.upload.obs.store.from_url") as mock_from_url:
                _setup_store_and_kwargs(
                    bucket_url="s3://my-bucket",
                    profile=None,
                    chunk_concurrency=12,
                    chunk_size=None,
                )

                # Should use S3Store for S3 URLs to handle credentials properly
                mock_s3store.assert_called_once()
                mock_from_url.assert_not_called()

    def test_setup_store_returns_kwargs(self):
        """Test _setup_store_and_kwargs returns correct kwargs."""
        with patch("geoparquet_io.core.upload.S3Store"):
            store, kwargs = _setup_store_and_kwargs(
                bucket_url="s3://my-bucket",
                profile=None,
                chunk_concurrency=24,
                chunk_size=16 * 1024 * 1024,
            )

            assert kwargs["max_concurrency"] == 24
            assert kwargs["chunk_size"] == 16 * 1024 * 1024

    def test_setup_store_for_non_s3_uses_from_url(self):
        """Test _setup_store_and_kwargs uses from_url for non-S3 URLs."""
        with patch("geoparquet_io.core.upload.S3Store") as mock_s3store:
            with patch("geoparquet_io.core.upload.obs.store.from_url") as mock_from_url:
                _setup_store_and_kwargs(
                    bucket_url="gs://my-bucket",
                    profile=None,
                    chunk_concurrency=12,
                    chunk_size=None,
                )

                # Should use from_url for non-S3 URLs
                mock_from_url.assert_called_once_with("gs://my-bucket")
                mock_s3store.assert_not_called()


class TestUploadCLIS3Options:
    """Test suite for S3 endpoint CLI options."""

    def test_upload_with_s3_endpoint_dry_run(self, places_test_file):
        """Test dry-run mode with S3 endpoint options."""
        runner = CliRunner()
        with patch.object(main_module, "check_credentials", return_value=(True, "")):
            result = runner.invoke(
                cli,
                [
                    "publish",
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
        with patch.object(main_module, "check_credentials", return_value=(True, "")):
            result = runner.invoke(
                cli,
                [
                    "publish",
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


class TestUploadEventLoopCompatibility:
    """Test suite for event loop compatibility (issue #157)."""

    def test_upload_from_running_event_loop(self, places_test_file):
        """Test that upload works when called from within a running event loop.

        This verifies the fix for issue #157: asyncio.run() cannot be called
        from a running event loop.
        """
        import asyncio

        async def call_upload_from_async():
            # This should NOT raise RuntimeError about asyncio.run()
            runner = CliRunner()
            with patch.object(main_module, "check_credentials", return_value=(True, "")):
                result = runner.invoke(
                    cli,
                    [
                        "publish",
                        "upload",
                        places_test_file,
                        "s3://bucket/test.parquet",
                        "--dry-run",
                    ],
                )
            return result

        # Run the test from within an event loop
        result = asyncio.run(call_upload_from_async())
        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output

    def test_directory_upload_from_running_event_loop(self, temp_output_dir):
        """Test that directory upload works when called from within a running event loop."""
        import asyncio
        from pathlib import Path

        # Create some test files
        test_dir = Path(temp_output_dir) / "test_files"
        test_dir.mkdir()
        for i in range(3):
            (test_dir / f"file_{i}.parquet").write_text(f"test content {i}")

        async def call_upload_from_async():
            runner = CliRunner()
            with patch.object(main_module, "check_credentials", return_value=(True, "")):
                result = runner.invoke(
                    cli,
                    [
                        "publish",
                        "upload",
                        str(test_dir),
                        "s3://bucket/dataset/",
                        "--dry-run",
                    ],
                )
            return result

        result = asyncio.run(call_upload_from_async())
        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output
        assert "Would upload 3 file(s)" in result.output


class TestCredentialValidationFunctions:
    """Tests for cloud credential validation functions."""

    def test_check_s3_credentials_with_env_vars(self):
        """Test S3 credential detection from environment variables."""
        with patch.dict(
            os.environ, {"AWS_ACCESS_KEY_ID": "test_key", "AWS_SECRET_ACCESS_KEY": "test_secret"}
        ):
            found, hint = _check_s3_credentials()
            assert found is True
            assert hint == ""

    def test_check_s3_credentials_missing_no_profile(self):
        """Test S3 credential check fails with helpful hint when no credentials."""
        with patch.dict(os.environ, {}, clear=True):
            with patch(
                "geoparquet_io.core.upload._load_aws_credentials_from_profile",
                return_value=(None, None, None),
            ):
                found, hint = _check_s3_credentials()
                assert found is False
                assert "S3 credentials not found" in hint
                assert "AWS_ACCESS_KEY_ID" in hint
                assert "aws configure" in hint

    def test_check_s3_credentials_with_profile_found(self):
        """Test S3 credential check with valid profile."""
        with patch(
            "geoparquet_io.core.upload._load_aws_credentials_from_profile",
            return_value=("access_key", "secret_key", "us-west-2"),
        ):
            found, hint = _check_s3_credentials(profile="myprofile")
            assert found is True
            assert hint == ""

    def test_check_s3_credentials_with_profile_not_found(self):
        """Test S3 credential check fails with profile-specific hint."""
        with patch(
            "geoparquet_io.core.upload._load_aws_credentials_from_profile",
            return_value=(None, None, None),
        ):
            found, hint = _check_s3_credentials(profile="myprofile")
            assert found is False
            assert "myprofile" in hint
            assert "~/.aws/credentials" in hint
            assert "[myprofile]" in hint

    def test_check_s3_credentials_falls_back_to_default_profile(self):
        """Test S3 credential check falls back to default profile."""
        with patch.dict(os.environ, {}, clear=True):
            with patch(
                "geoparquet_io.core.upload._load_aws_credentials_from_profile",
                return_value=("default_key", "default_secret", None),
            ):
                found, hint = _check_s3_credentials()
                assert found is True
                assert hint == ""

    def test_check_gcs_credentials_with_service_account_key(self):
        """Test GCS credential detection with service account key file."""
        import tempfile

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            key_file = f.name

        try:
            with patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": key_file}):
                found, hint = _check_gcs_credentials()
                assert found is True
                assert hint == ""
        finally:
            Path(key_file).unlink(missing_ok=True)

    def test_check_gcs_credentials_missing(self):
        """Test GCS credential check fails with helpful hint."""
        with patch.dict(os.environ, {}, clear=True):
            found, hint = _check_gcs_credentials()
            assert found is False
            assert "GCS credentials not found" in hint
            assert "GOOGLE_APPLICATION_CREDENTIALS" in hint
            assert "gcloud auth" in hint

    def test_check_gcs_credentials_file_doesnt_exist(self):
        """Test GCS credential check fails when file doesn't exist."""
        with patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": "/nonexistent/key.json"}):
            found, hint = _check_gcs_credentials()
            assert found is False
            assert "GCS credentials not found" in hint

    def test_check_azure_credentials_with_account_key(self):
        """Test Azure credential detection with storage account key."""
        with patch.dict(os.environ, {"AZURE_STORAGE_ACCOUNT_KEY": "test_key"}):
            found, hint = _check_azure_credentials()
            assert found is True
            assert hint == ""

    def test_check_azure_credentials_with_sas_token(self):
        """Test Azure credential detection with SAS token."""
        with patch.dict(os.environ, {"AZURE_STORAGE_SAS_TOKEN": "test_token"}):
            found, hint = _check_azure_credentials()
            assert found is True
            assert hint == ""

    def test_check_azure_credentials_with_client_id(self):
        """Test Azure credential detection with client ID."""
        with patch.dict(os.environ, {"AZURE_CLIENT_ID": "test_client_id"}):
            found, hint = _check_azure_credentials()
            assert found is True
            assert hint == ""

    def test_check_azure_credentials_missing(self):
        """Test Azure credential check fails with helpful hint."""
        with patch.dict(os.environ, {}, clear=True):
            found, hint = _check_azure_credentials()
            assert found is False
            assert "Azure credentials not found" in hint
            assert "AZURE_STORAGE_ACCOUNT_KEY" in hint
            assert "az login" in hint
