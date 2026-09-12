"""
Tests for upload functionality.
"""

import importlib
import itertools
import os
import re
import threading
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

# Use importlib to get the actual module (avoids namespace collision with cli group)
main_module = importlib.import_module("geoparquet_io.cli.main")
cli = main_module.cli
# `check_credentials` is patched where `gpio publish upload` resolves it, which is
# the module that owns the command (Deep Review 3.2 moved it out of `cli.main`).
publish_module = importlib.import_module("geoparquet_io.cli.commands.publish")
upload_module = importlib.import_module("geoparquet_io.core.upload")
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
        with patch.object(publish_module, "check_credentials", return_value=(True, "")):
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
        with patch.object(publish_module, "check_credentials", return_value=(True, "")):
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
        with patch.object(publish_module, "check_credentials", return_value=(True, "")):
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
        with patch.object(publish_module, "check_credentials", return_value=(True, "")):
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
        with patch.object(publish_module, "check_credentials", return_value=(True, "")):
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
        with patch.object(publish_module, "check_credentials", return_value=(True, "")):
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
        with patch.object(publish_module, "check_credentials", return_value=(True, "")):
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
        """Test credential checking fails when the whole chain comes up empty."""
        with patch.dict("os.environ", {}, clear=True):
            with patch(
                "geoparquet_io.core.upload.resolve_aws_credentials",
                return_value=None,
            ):
                ok, hint = check_credentials("s3://bucket/path")
                assert ok is False
                assert "S3 credentials not found" in hint

    def test_check_credentials_with_default_profile_fallback(self):
        """Test credential checking passes on anything the chain resolves."""
        with patch.dict("os.environ", {}, clear=True):
            with patch(
                "geoparquet_io.core.upload.resolve_aws_credentials",
                return_value={"access_key_id": "key", "secret_access_key": "secret"},
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
    """Test suite for S3 endpoint configuration.

    resolve_aws_credentials is patched out: these tests are about store
    construction, and the real resolver walks botocore's full chain, which
    would execute ambient config (an assume-role or credential_process
    profile) from a unit test.
    """

    @pytest.fixture(autouse=True)
    def _no_ambient_credentials(self):
        with patch("geoparquet_io.core.upload.resolve_aws_credentials", return_value=None):
            yield

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

    def test_setup_store_for_gcs_uses_from_url(self):
        """GCS keeps obstore's own from_url; only S3 and Azure are built by hand."""
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


class TestAzureStoreConstruction:
    """Azure stores are built explicitly, not through ``obs.store.from_url`` (#864).

    obstore's ``az://`` convention is ``az://<container>/<path>`` with the account
    taken from the environment, so handing it gpio's ``az://<account>/<container>``
    URL either refuses to build ("Account must be specified"), panics in Rust once
    ``AZURE_STORAGE_ACCOUNT_NAME`` is set, or silently reads the account segment as
    the container. These tests construct real ``AzureStore`` objects -- construction
    needs no network and no credentials -- so the obstore contract is exercised, not
    a mock of it.
    """

    AZURE_ENV_VARS = (
        "AZURE_STORAGE_ACCOUNT_NAME",
        "AZURE_STORAGE_ACCOUNT_KEY",
        "AZURE_STORAGE_ACCESS_KEY",
        "AZURE_STORAGE_MASTER_KEY",
        "AZURE_STORAGE_SAS_TOKEN",
        "AZURE_STORAGE_SAS_KEY",
        "AZURE_CONTAINER_NAME",
    )

    def _clear_azure_env(self, monkeypatch):
        for name in self.AZURE_ENV_VARS:
            monkeypatch.delenv(name, raising=False)

    def test_azure_store_is_built_from_the_url_with_no_env_and_no_credentials(self, monkeypatch):
        """``az://account/container/prefix`` builds an AzureStore with those parts."""
        from obstore.store import AzureStore

        from geoparquet_io.core.upload import _build_azure_store

        self._clear_azure_env(monkeypatch)

        store = _build_azure_store("az://myaccount/mycontainer/some/prefix")

        assert isinstance(store, AzureStore)
        assert store.config["account_name"] == "myaccount"
        assert store.config["container_name"] == "mycontainer"
        assert store.prefix == "some/prefix"

    def test_azure_store_without_a_prefix_has_no_prefix(self, monkeypatch):
        """The bucket URL gpio hands the store carries no prefix; keys do."""
        from geoparquet_io.core.upload import _build_azure_store

        self._clear_azure_env(monkeypatch)

        store = _build_azure_store("az://myaccount/mycontainer")

        assert store.config["account_name"] == "myaccount"
        assert store.config["container_name"] == "mycontainer"
        assert store.prefix is None

    def test_account_in_the_url_wins_over_the_environment(self, monkeypatch):
        """The URL is the authority for the account; env only fills what it omits."""
        from geoparquet_io.core.upload import _build_azure_store

        self._clear_azure_env(monkeypatch)
        monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_NAME", "envaccount")

        store = _build_azure_store("az://urlaccount/mycontainer")

        assert store.config["account_name"] == "urlaccount"

    def test_azure_url_without_a_container_is_rejected(self, monkeypatch):
        """``az://account`` alone names no container; say so instead of guessing.

        The refusal is an ``InvalidParameterError`` so the CLI boundary renders
        it as a clean error rather than a raw traceback.
        """
        from geoparquet_io.core.exceptions import InvalidParameterError
        from geoparquet_io.core.upload import _build_azure_store

        self._clear_azure_env(monkeypatch)

        with pytest.raises(InvalidParameterError, match=r"az://<account>/<container>"):
            _build_azure_store("az://myaccount")

        with pytest.raises(InvalidParameterError, match=r"az://<account>/<container>"):
            parse_object_store_url("az://myaccount")

    @pytest.mark.parametrize(
        "url",
        [
            "abfs://container@account.dfs.core.windows.net/out.parquet",
            "abfss://container@account.dfs.core.windows.net/out.parquet",
            "azure://account/container/out.parquet",
        ],
    )
    def test_parse_object_store_url_refuses_azure_alias_schemes_by_name(self, url):
        """``abfs[s]://`` and ``azure://`` are refused with the supported spelling.

        These spellings order the account and container differently (or bury them
        in a host name), so parsing them account-first would target the wrong
        place. They used to fall into the generic ``Unsupported URL scheme``
        ValueError -- a raw traceback at the CLI.
        """
        from geoparquet_io.core.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError, match=r"az://<account>/<container>"):
            parse_object_store_url(url)

    def test_parse_object_store_url_refuses_unknown_schemes_without_a_traceback(self):
        """A scheme gpio does not speak is an InvalidParameterError, not a ValueError."""
        from geoparquet_io.core.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError, match="unsupported URL scheme"):
            parse_object_store_url("ftp://host/file.parquet")

    def test_setup_store_builds_an_azure_store_instead_of_calling_from_url(self, monkeypatch):
        """The upload path routes az:// through the explicit builder."""
        from obstore.store import AzureStore

        self._clear_azure_env(monkeypatch)

        with patch("geoparquet_io.core.upload.obs.store.from_url") as mock_from_url:
            store, kwargs = _setup_store_and_kwargs(
                bucket_url="az://myaccount/mycontainer",
                profile=None,
                chunk_concurrency=12,
                chunk_size=None,
            )

        mock_from_url.assert_not_called()
        assert isinstance(store, AzureStore)
        assert store.config["account_name"] == "myaccount"
        assert store.config["container_name"] == "mycontainer"
        assert kwargs["max_concurrency"] == 12

    def test_upload_to_azure_puts_the_key_into_the_accounts_container(self, monkeypatch, tmp_path):
        """``publish upload`` reaches obstore with an AzureStore and a bare key."""
        from obstore.store import AzureStore

        from geoparquet_io.core.upload import upload

        self._clear_azure_env(monkeypatch)
        source = tmp_path / "data.parquet"
        source.write_bytes(b"parquet-bytes")

        with patch("geoparquet_io.core.upload.obs.put") as mock_put:
            upload(source, "az://myaccount/mycontainer/dataset/data.parquet")

        store = mock_put.call_args.args[0]
        assert isinstance(store, AzureStore)
        assert store.config["account_name"] == "myaccount"
        assert store.config["container_name"] == "mycontainer"
        assert store.prefix is None
        assert mock_put.call_args.args[1] == "dataset/data.parquet"


class TestUploadCLIRefusesUnparseableAzureUrls:
    """``gpio publish upload`` answers a bad Azure URL with a message, not a traceback."""

    @pytest.mark.parametrize(
        "destination",
        [
            "abfs://container@account.dfs.core.windows.net/out.parquet",
            "abfss://container@account.dfs.core.windows.net/out.parquet",
            "azure://account/container/out.parquet",
            "az://acctonly",
        ],
    )
    def test_publish_upload_refuses_with_a_clean_error(self, destination, tmp_path):
        """Exit non-zero, the supported ``az://`` form named, and no raw traceback."""
        source = tmp_path / "data.parquet"
        source.write_bytes(b"parquet-bytes")

        runner = CliRunner()
        with patch.object(publish_module, "check_credentials", return_value=(True, "")):
            result = runner.invoke(
                cli, ["publish", "upload", str(source), destination], catch_exceptions=False
            )

        assert result.exit_code != 0
        assert "az://<account>/<container>" in result.output
        assert "Traceback" not in result.output


class TestUploadCLIS3Options:
    """Test suite for S3 endpoint CLI options."""

    def test_upload_with_s3_endpoint_dry_run(self, places_test_file):
        """Test dry-run mode with S3 endpoint options."""
        runner = CliRunner()
        with patch.object(publish_module, "check_credentials", return_value=(True, "")):
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
        with patch.object(publish_module, "check_credentials", return_value=(True, "")):
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
            with patch.object(publish_module, "check_credentials", return_value=(True, "")):
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
            with patch.object(publish_module, "check_credentials", return_value=(True, "")):
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
                "geoparquet_io.core.upload.resolve_aws_credentials",
                return_value=None,
            ):
                found, hint = _check_s3_credentials()
                assert found is False
                assert "S3 credentials not found" in hint
                assert "AWS_ACCESS_KEY_ID" in hint
                assert "aws configure" in hint

    def test_check_s3_credentials_with_profile_found(self):
        """Test S3 credential check with valid profile."""
        with patch(
            "geoparquet_io.core.upload.resolve_aws_credentials",
            return_value={"access_key_id": "access_key", "secret_access_key": "secret_key"},
        ) as mock_resolve:
            found, hint = _check_s3_credentials(profile="myprofile")
            assert found is True
            assert hint == ""
            mock_resolve.assert_called_once_with("myprofile")

    def test_check_s3_credentials_with_profile_not_found(self):
        """Test S3 credential check fails with profile-specific hint."""
        with patch(
            "geoparquet_io.core.upload.resolve_aws_credentials",
            return_value=None,
        ):
            found, hint = _check_s3_credentials(profile="myprofile")
            assert found is False
            assert "myprofile" in hint
            assert "~/.aws/credentials" in hint
            assert "[myprofile]" in hint

    def test_check_s3_credentials_falls_back_to_default_profile(self):
        """Test S3 credential check passes on the chain's default-profile result."""
        with patch.dict(os.environ, {}, clear=True):
            with patch(
                "geoparquet_io.core.upload.resolve_aws_credentials",
                return_value={
                    "access_key_id": "default_key",
                    "secret_access_key": "default_secret",
                },
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

    @pytest.mark.parametrize(
        "env_var",
        [
            "AZURE_STORAGE_ACCOUNT_KEY",
            "AZURE_STORAGE_ACCESS_KEY",
            "AZURE_STORAGE_MASTER_KEY",
            "AZURE_STORAGE_SAS_TOKEN",
            "AZURE_STORAGE_SAS_KEY",
            "AZURE_STORAGE_TOKEN",
            "AZURE_STORAGE_CLIENT_ID",
            "AZURE_CLIENT_ID",
        ],
    )
    def test_check_azure_credentials_accepts_every_env_var_obstore_honours(self, env_var):
        """Each credential variable obstore's AzureStore reads passes the gate alone.

        The gate used to hard-fail unless one of exactly three variables was set,
        blocking uploads that obstore itself would have authenticated -- e.g. with
        only ``AZURE_STORAGE_ACCESS_KEY`` (the documented alias) in the
        environment.
        """
        with patch.dict(os.environ, {env_var: "credential-value"}, clear=True):
            found, hint = _check_azure_credentials()
            assert found is True
            assert hint == ""

    def test_check_azure_credentials_accepts_azure_cli_opt_in(self):
        """``AZURE_USE_AZURE_CLI=true`` is a credential source, not a missing one."""
        with patch.dict(os.environ, {"AZURE_USE_AZURE_CLI": "true"}, clear=True):
            found, hint = _check_azure_credentials()
            assert found is True
            assert hint == ""

    def test_check_azure_credentials_azure_cli_false_is_not_a_credential(self):
        """An explicit ``AZURE_USE_AZURE_CLI=false`` does not pass the gate."""
        with patch.dict(os.environ, {"AZURE_USE_AZURE_CLI": "false"}, clear=True):
            found, hint = _check_azure_credentials()
            assert found is False

    def test_check_azure_credentials_missing(self):
        """Test Azure credential check fails with helpful hint."""
        with patch.dict(os.environ, {}, clear=True):
            found, hint = _check_azure_credentials()
            assert found is False
            assert "Azure credentials not found" in hint
            assert "AZURE_STORAGE_ACCOUNT_KEY" in hint
            assert "az login" in hint
            # "az login" alone is not enough for obstore -- the opt-in must be named.
            assert "AZURE_USE_AZURE_CLI=true" in hint


class TestDirectoryUploadReportsWhatReachedTheStore:
    """A directory upload must report the files that actually arrived (#1019).

    Every test here drives the real ``gpio publish upload`` command with
    ``obs.put`` replaced by a recording stub, then checks the printed summary
    against the keys the stub actually received -- not against itself.
    """

    @staticmethod
    def _make_files(tmp_path, count=10):
        source = tmp_path / "dataset"
        source.mkdir()
        for i in range(count):
            (source / f"part-{i:02d}.parquet").write_bytes(b"x" * 100)
        return source

    @staticmethod
    def _run(source, fake_put, extra_args=(), destination="s3://example-bucket/out/"):
        runner = CliRunner()
        with (
            patch.object(publish_module, "check_credentials", return_value=(True, "")),
            patch.object(upload_module.obs, "put", fake_put),
            patch.object(upload_module, "_setup_store_and_kwargs", lambda *a, **k: (object(), {})),
        ):
            return runner.invoke(
                cli,
                ["publish", "upload", str(source), destination, *extra_args],
            )

    @staticmethod
    def _summary_counts(output):
        """The three counts the summary claims: (uploaded, failed, not attempted)."""
        uploaded = re.search(r"✓ (\d+)/(\d+) file\(s\) uploaded successfully", output)
        failed = re.search(r"✗ (\d+) file\(s\) failed", output)
        skipped = re.search(r"⊘ (\d+) file\(s\) not attempted", output)
        assert uploaded, f"no summary line in output:\n{output}"
        return (
            int(uploaded.group(1)),
            int(failed.group(1)) if failed else 0,
            int(skipped.group(1)) if skipped else 0,
        )

    def test_fail_fast_does_not_count_stopped_files_as_uploaded(self, tmp_path):
        """The success count is what arrived, not ``total - errors`` (#1019).

        The very first file to reach the store fails, on a single worker, so
        nothing else is ever tried. Before the fix this printed
        ``9/10 file(s) uploaded successfully`` over an empty bucket.
        """
        source = self._make_files(tmp_path)
        arrived = []
        calls = itertools.count()
        lock = threading.Lock()

        def fake_put(store, key, path, **kwargs):
            with lock:
                nth = next(calls)
            if nth == 0:
                raise RuntimeError("AccessDenied: bucket is read-only")
            arrived.append(Path(path).name)

        result = self._run(source, fake_put, ["--fail-fast", "--max-files", "1"])

        assert arrived == []
        assert self._summary_counts(result.output) == (0, 1, 9)
        assert "⊘ 9 file(s) not attempted (stopped on first error)" in result.output
        assert result.exit_code == 1

    def test_fail_fast_lets_in_flight_uploads_finish_and_counts_them(self, tmp_path):
        """Cancellation reaches files that never started, never files in flight.

        The gate makes every non-failing upload finish *after* the failure has
        been signalled, so a file counted as uploaded here can only be one that
        was in flight when ``--fail-fast`` tripped.
        """
        source = self._make_files(tmp_path)
        started, arrived = [], []
        calls = itertools.count()
        lock = threading.Lock()
        failure_landed = threading.Event()

        def fake_put(store, key, path, **kwargs):
            with lock:
                nth = next(calls)
                started.append(Path(path).name)
            if nth == 1:
                failure_landed.set()
                raise RuntimeError("AccessDenied: bucket is read-only")
            assert failure_landed.wait(timeout=30), "the failing upload never ran"
            arrived.append(Path(path).name)

        result = self._run(source, fake_put, ["--fail-fast", "--max-files", "2"])

        # Two workers, so exactly two files were in flight when the failure
        # landed; the survivor could only finish after it, and must be counted.
        assert len(started) == 2
        assert arrived == [started[0]]
        assert self._summary_counts(result.output) == (1, 1, 8)
        assert result.exit_code == 1

    def test_continue_on_error_attempts_every_file_and_still_exits_non_zero(self, tmp_path):
        """Without ``--fail-fast`` nothing is skipped, but errors must still fail."""
        source = self._make_files(tmp_path)
        arrived = []

        def fake_put(store, key, path, **kwargs):
            if Path(path).name == "part-00.parquet":
                raise RuntimeError("AccessDenied: bucket is read-only")
            arrived.append(Path(path).name)

        result = self._run(source, fake_put, ["--max-files", "2"])

        assert len(arrived) == 9
        assert self._summary_counts(result.output) == (9, 1, 0)
        assert "not attempted" not in result.output
        assert result.exit_code == 1

    def test_a_directory_upload_that_failed_entirely_exits_non_zero(self, tmp_path):
        """Nothing reached the bucket, so ``cmd && echo ok`` must not print ok."""
        source = self._make_files(tmp_path)
        arrived = []

        def fake_put(store, key, path, **kwargs):
            raise RuntimeError("AccessDenied: bucket is read-only")

        result = self._run(source, fake_put)

        assert arrived == []
        assert self._summary_counts(result.output) == (0, 10, 0)
        assert "10 of 10 file(s) failed to upload" in result.output
        assert result.exit_code == 1

    def test_a_fully_successful_directory_upload_still_exits_zero(self, tmp_path):
        """The happy path is unchanged: no failure lines, exit 0."""
        source = self._make_files(tmp_path)
        arrived = []

        def fake_put(store, key, path, **kwargs):
            arrived.append(Path(path).name)

        result = self._run(source, fake_put, ["--max-files", "4"])

        assert len(arrived) == 10
        assert self._summary_counts(result.output) == (10, 0, 0)
        assert "failed" not in result.output
        assert result.exit_code == 0

    def test_the_error_names_the_destination_and_the_counts(self, tmp_path):
        """A script's operator needs to know where the gap is, and how big.

        A realistic prefix, several segments deep: the first version of this
        error routed the destination through the presigned-URL sanitizer, which
        keeps a *filename* and elides the path before it -- so a directory URL,
        ending in ``/``, came out as ``s3://bucket/datasets/.../``. The fixture
        it was tested with sat exactly at the threshold where nothing is elided.
        """
        source = self._make_files(tmp_path, count=4)
        destination = "s3://example-bucket/datasets/overture/2025-01/buildings/"

        def fake_put(store, key, path, **kwargs):
            if Path(path).name == "part-00.parquet":
                raise RuntimeError("AccessDenied: bucket is read-only")

        result = self._run(source, fake_put, ["--max-files", "2"], destination=destination)

        assert destination in result.output, result.output
        assert "..." not in result.output.split("Error:")[-1]
        assert "1 of 4 file(s) failed to upload" in result.output
        assert result.exit_code == 1

    @pytest.mark.parametrize(
        ("url", "expected"),
        [
            # A directory URL keeps its whole path: there is no filename to elide to.
            (
                "s3://bucket/datasets/overture/2025-01/buildings/",
                "s3://bucket/datasets/overture/2025-01/buildings/",
            ),
            # A file URL still elides the middle and keeps the filename.
            ("s3://bucket/a/b/c/d/file.parquet", "s3://bucket/a/.../file.parquet"),
            # And the query string -- where presigned credentials live -- always goes.
            (
                "s3://bucket/a/b/c/d/?X-Amz-Signature=secret",
                "s3://bucket/a/b/c/d/",
            ),
        ],
    )
    def test_sanitizing_a_directory_url_keeps_its_path(self, url, expected):
        from geoparquet_io.core.exceptions import sanitize_url_for_logging

        assert sanitize_url_for_logging(url) == expected

    def test_a_partition_to_a_remote_folder_exits_non_zero_when_a_file_is_missing(self, tmp_path):
        """The raise reaches every directory writer, not only ``publish upload``.

        ``gpio partition <scheme> in.parquet s3://…/`` writes locally and then
        uploads the directory through the same ``_upload_directory_sync``. On
        ``main`` a partial upload there printed ``Created N partition(s) in
        s3://…`` and exited 0.
        """
        arrived = []

        def fake_put(store, key, path, **kwargs):
            if not arrived:
                arrived.append(Path(path).name)
                raise RuntimeError("AccessDenied: bucket is read-only")
            arrived.append(Path(path).name)

        runner = CliRunner()
        with (
            patch.object(upload_module.obs, "put", fake_put),
            patch.object(upload_module, "_setup_store_and_kwargs", lambda *a, **k: (object(), {})),
        ):
            result = runner.invoke(
                cli,
                [
                    "partition",
                    "quadkey",
                    "tests/data/buildings_test.parquet",
                    "s3://example-bucket/datasets/buildings/",
                    "--resolution",
                    "8",
                    "--partition-resolution",
                    "3",
                    "--force",
                ],
            )

        assert arrived, "the partition never reached the upload"
        assert result.exit_code != 0, result.output
        assert "failed to upload" in result.output
        assert "Created" not in result.output.split("Error:")[-1]
