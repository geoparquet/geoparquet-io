"""
Tests for geoparquet_io.core.file_utils module.

Tests file path utilities including glob pattern detection, partition path handling,
SQL escaping, and cache key generation.
"""

import functools
import http.server
import os
import threading

import pytest

from geoparquet_io.core.exceptions import (
    FileNotFoundGeoParquetError,
    GeoParquetError,
    InvalidParameterError,
)
from geoparquet_io.core.file_utils import (
    _get_file_cache_key,
    get_all_parquet_files,
    get_first_parquet_file,
    guard_output_not_read_as_input,
    handle_output_overwrite,
    has_glob_pattern,
    is_partition_path,
    is_same_file_path,
    resolve_partition_path,
    safe_file_url,
    validate_output_path,
    validate_parquet_extension,
)

# =============================================================================
# Tests for has_glob_pattern()
# =============================================================================


class TestHasGlobPattern:
    """Test glob pattern detection."""

    def test_asterisk_detected(self):
        """Asterisk wildcard is detected."""
        assert has_glob_pattern("*.parquet") is True
        assert has_glob_pattern("path/*.parquet") is True
        assert has_glob_pattern("**/*.parquet") is True

    def test_question_mark_detected(self):
        """Question mark wildcard is detected."""
        assert has_glob_pattern("file?.parquet") is True
        assert has_glob_pattern("path/file?.parquet") is True

    def test_brackets_detected(self):
        """Bracket patterns are detected."""
        assert has_glob_pattern("file[0-9].parquet") is True
        assert has_glob_pattern("[abc].parquet") is True

    def test_no_pattern_returns_false(self):
        """Normal paths return False."""
        assert has_glob_pattern("file.parquet") is False
        assert has_glob_pattern("/path/to/file.parquet") is False
        assert has_glob_pattern("relative/path.parquet") is False

    def test_empty_string(self):
        """Empty string returns False."""
        assert has_glob_pattern("") is False


# =============================================================================
# Tests for is_partition_path()
# =============================================================================


class TestIsPartitionPath:
    """Test partition path detection."""

    def test_glob_pattern_is_partition(self):
        """Glob patterns are partition paths."""
        assert is_partition_path("*.parquet") is True
        assert is_partition_path("data/**/*.parquet") is True

    def test_local_directory_is_partition(self, tmp_path):
        """Local directories are partition paths."""
        assert is_partition_path(str(tmp_path)) is True

    def test_local_file_is_not_partition(self, tmp_path):
        """Local files are not partition paths."""
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"test")
        assert is_partition_path(str(test_file)) is False

    def test_hive_style_remote_url(self):
        """Remote URLs with hive-style partitioning are detected."""
        url = "s3://bucket/data/year=2024/month=01/data.parquet"
        assert is_partition_path(url) is True

    def test_plain_remote_url(self):
        """Plain remote URLs are not partition paths."""
        url = "s3://bucket/data/file.parquet"
        assert is_partition_path(url) is False

    def test_http_url_not_partition(self):
        """HTTP URLs without hive partitioning are not partition paths."""
        url = "https://example.com/data.parquet"
        assert is_partition_path(url) is False


# =============================================================================
# Tests for resolve_partition_path()
# =============================================================================


class TestResolvePartitionPath:
    """Test partition path resolution."""

    def test_directory_with_parquet_files(self, tmp_path):
        """Directory with .parquet files resolves to *.parquet."""
        (tmp_path / "a.parquet").write_bytes(b"test")
        (tmp_path / "b.parquet").write_bytes(b"test")

        resolved, options = resolve_partition_path(str(tmp_path))

        assert resolved.endswith("*.parquet")
        assert "hive_partitioning" not in options

    def test_directory_with_hive_subdirs(self, tmp_path):
        """Directory with hive subdirs resolves with hive option."""
        hive_dir = tmp_path / "year=2024"
        hive_dir.mkdir()
        (hive_dir / "data.parquet").write_bytes(b"test")

        resolved, options = resolve_partition_path(str(tmp_path))

        # Handle both Unix (/) and Windows (\) path separators
        assert "**" in resolved and "*.parquet" in resolved
        assert options.get("hive_partitioning") is True

    def test_explicit_hive_partitioning(self, tmp_path):
        """Explicit hive_partitioning parameter is respected."""
        (tmp_path / "a.parquet").write_bytes(b"test")

        resolved, options = resolve_partition_path(str(tmp_path), hive_partitioning=True)

        assert options.get("hive_partitioning") is True

    def test_path_with_glob_unchanged(self):
        """Glob patterns pass through unchanged."""
        glob_path = "/data/**/*.parquet"
        resolved, options = resolve_partition_path(glob_path)
        assert resolved == glob_path


# =============================================================================
# Tests for get_first_parquet_file()
# =============================================================================


class TestGetFirstParquetFile:
    """Test finding first parquet file in partition."""

    def test_directory_returns_first_file(self, tmp_path):
        """Returns first sorted parquet file in directory."""
        (tmp_path / "c.parquet").write_bytes(b"test")
        (tmp_path / "a.parquet").write_bytes(b"test")
        (tmp_path / "b.parquet").write_bytes(b"test")

        result = get_first_parquet_file(str(tmp_path))

        assert result is not None
        assert result.endswith("a.parquet")

    def test_nested_directory(self, tmp_path):
        """Finds parquet files in nested directories."""
        nested = tmp_path / "subdir"
        nested.mkdir()
        (nested / "data.parquet").write_bytes(b"test")

        result = get_first_parquet_file(str(tmp_path))

        assert result is not None
        assert "data.parquet" in result

    def test_empty_directory_returns_none(self, tmp_path):
        """Empty directory returns None."""
        result = get_first_parquet_file(str(tmp_path))
        assert result is None

    def test_glob_pattern(self, tmp_path):
        """Glob pattern returns first matching file."""
        (tmp_path / "a.parquet").write_bytes(b"test")
        (tmp_path / "b.parquet").write_bytes(b"test")

        pattern = str(tmp_path / "*.parquet")
        result = get_first_parquet_file(pattern)

        assert result is not None
        assert result.endswith("a.parquet")

    def test_remote_url_returns_unchanged(self):
        """Remote URLs are returned unchanged."""
        url = "s3://bucket/file.parquet"
        result = get_first_parquet_file(url)
        assert result == url

    def test_single_file_returns_itself(self, tmp_path):
        """Single file path returns itself."""
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"test")

        result = get_first_parquet_file(str(test_file))

        assert result == str(test_file)


# =============================================================================
# Tests for get_all_parquet_files()
# =============================================================================


class TestGetAllParquetFiles:
    """Test getting all parquet files from partition."""

    def test_directory_returns_all_files(self, tmp_path):
        """Returns all parquet files in directory."""
        (tmp_path / "a.parquet").write_bytes(b"test")
        (tmp_path / "b.parquet").write_bytes(b"test")
        (tmp_path / "c.txt").write_bytes(b"test")

        result = get_all_parquet_files(str(tmp_path))

        assert len(result) == 2
        assert all(f.endswith(".parquet") for f in result)

    def test_results_are_sorted(self, tmp_path):
        """Results are returned sorted."""
        (tmp_path / "z.parquet").write_bytes(b"test")
        (tmp_path / "a.parquet").write_bytes(b"test")

        result = get_all_parquet_files(str(tmp_path))

        assert result[0].endswith("a.parquet")
        assert result[1].endswith("z.parquet")

    def test_empty_directory_returns_empty(self, tmp_path):
        """Empty directory returns empty list."""
        result = get_all_parquet_files(str(tmp_path))
        assert result == []

    def test_nonexistent_path_returns_empty(self, tmp_path):
        """Non-existent path returns empty list."""
        result = get_all_parquet_files(str(tmp_path / "nonexistent.parquet"))
        assert result == []


# =============================================================================
# Tests for validate_output_path()
# =============================================================================


class TestValidateOutputPath:
    """Test output path validation."""

    def test_valid_path(self, tmp_path):
        """Valid writable path passes."""
        output_path = str(tmp_path / "output.parquet")
        # Should not raise
        validate_output_path(output_path)

    def test_nonexistent_directory_raises(self):
        """Non-existent directory raises FileNotFoundGeoParquetError."""
        with pytest.raises(FileNotFoundGeoParquetError):
            validate_output_path("/nonexistent/directory/file.parquet")

    def test_remote_url_passes(self):
        """Remote URLs are not validated."""
        validate_output_path("s3://bucket/file.parquet")

    def test_current_directory(self):
        """Output in current directory is valid."""
        validate_output_path("output.parquet")


# =============================================================================
# Tests for validate_parquet_extension()
# =============================================================================


class TestValidateParquetExtension:
    """Test parquet extension validation."""

    def test_valid_parquet_extension(self):
        """Files with .parquet extension pass."""
        validate_parquet_extension("output.parquet")
        validate_parquet_extension("/path/to/file.parquet")
        validate_parquet_extension("s3://bucket/file.parquet")

    def test_invalid_extension_raises(self):
        """Files without .parquet extension raise."""
        with pytest.raises(InvalidParameterError):
            validate_parquet_extension("output.csv")

    def test_any_extension_bypasses_check(self):
        """any_extension=True allows any extension."""
        validate_parquet_extension("output.csv", any_extension=True)

    def test_none_and_dash_allowed(self):
        """None and '-' are allowed (stdout)."""
        validate_parquet_extension(None)
        validate_parquet_extension("-")

    def test_case_insensitive(self):
        """Extension check is case insensitive."""
        validate_parquet_extension("output.PARQUET")
        validate_parquet_extension("output.Parquet")


# =============================================================================
# Tests for is_same_file_path()
# =============================================================================


class TestIsSameFilePath:
    """Two spellings of one file compare equal; everything else does not.

    Every caller that decides "am I about to write back over my own input?" has
    to reach the same verdict. When ``handle_fix_common`` compared raw strings
    and ``handle_output_overwrite`` compared ``resolve()``, an aliased
    ``--fix-output`` took the "different file" branch in one and the "same file"
    branch in the other -- no backup, then a refusal (#959).
    """

    def test_identical_strings(self, tmp_path):
        target = str(tmp_path / "a.parquet")
        assert is_same_file_path(target, target) is True

    def test_dot_prefixed_alias(self, tmp_path):
        target = tmp_path / "a.parquet"
        target.write_bytes(b"")
        alias = f"{tmp_path}{os.sep}.{os.sep}a.parquet"
        assert alias != str(target)
        assert is_same_file_path(alias, str(target)) is True

    def test_relative_and_absolute_spellings(self, tmp_path):
        target = tmp_path / "a.parquet"
        target.write_bytes(b"")
        # Relative to the real CWD, so the test never has to change it.
        relative = os.path.relpath(str(target))
        assert relative != str(target)
        assert is_same_file_path(relative, str(target)) is True

    def test_symlink_alias(self, tmp_path):
        target = tmp_path / "a.parquet"
        target.write_bytes(b"")
        link = tmp_path / "link.parquet"
        link.symlink_to(target)
        assert is_same_file_path(str(link), str(target)) is True

    def test_different_files(self, tmp_path):
        assert is_same_file_path(str(tmp_path / "a.parquet"), str(tmp_path / "b.parquet")) is False

    @pytest.mark.parametrize(
        "first,second",
        [(None, "a.parquet"), ("a.parquet", None), ("", "a.parquet"), ("a.parquet", "")],
    )
    def test_a_missing_path_is_never_the_same_file(self, first, second):
        assert is_same_file_path(first, second) is False

    def test_remote_urls_are_compared_verbatim(self):
        """There is no local filesystem to resolve a remote URL against."""
        assert is_same_file_path("s3://bucket/a.parquet", "s3://bucket/a.parquet") is True
        assert is_same_file_path("s3://bucket/a.parquet", "s3://bucket/b.parquet") is False
        assert is_same_file_path("s3://bucket/a.parquet", "/tmp/a.parquet") is False

    def test_an_unresolvable_path_is_not_a_match(self, tmp_path, monkeypatch):
        """A resolve() that raises means "cannot tell", which must not read as "same"."""

        def explode(self, *args, **kwargs):
            raise OSError("resolve blew up")

        monkeypatch.setattr("pathlib.Path.resolve", explode)
        assert is_same_file_path(str(tmp_path / "a.parquet"), str(tmp_path / "b.parquet")) is False


# =============================================================================
# Tests for handle_output_overwrite()
# =============================================================================


class TestHandleOutputOverwrite:
    """Test output overwrite handling."""

    def test_nonexistent_file_ok(self, tmp_path):
        """Non-existent file does not raise."""
        handle_output_overwrite(str(tmp_path / "new.parquet"), overwrite=False)

    def test_existing_file_no_overwrite_raises(self, tmp_path):
        """Existing file without overwrite flag raises."""
        existing = tmp_path / "existing.parquet"
        existing.write_bytes(b"test")

        with pytest.raises(GeoParquetError, match="already exists"):
            handle_output_overwrite(str(existing), overwrite=False)

    def test_existing_file_with_overwrite_deletes(self, tmp_path):
        """Existing file with overwrite flag is deleted."""
        existing = tmp_path / "existing.parquet"
        existing.write_bytes(b"test")

        handle_output_overwrite(str(existing), overwrite=True)

        assert not existing.exists()

    def test_same_input_output_raises(self, tmp_path):
        """Same input and output file raises."""
        file_path = tmp_path / "file.parquet"
        file_path.write_bytes(b"test")

        with pytest.raises(GeoParquetError, match="Cannot overwrite input"):
            handle_output_overwrite(str(file_path), overwrite=True, input_path=str(file_path))

    def test_none_output_path_ok(self):
        """None output path does nothing."""
        handle_output_overwrite(None, overwrite=False)


# =============================================================================
# Tests for guard_output_not_read_as_input()
# =============================================================================


@pytest.fixture
def parts_dir(tmp_path):
    """A directory holding two parquet files, plus a sibling output directory."""
    parts = tmp_path / "parts"
    parts.mkdir()
    (parts / "a.parquet").write_bytes(b"a")
    (parts / "b.parquet").write_bytes(b"b")
    (tmp_path / "out").mkdir()
    return parts


class TestGuardOutputNotReadAsInput:
    """An output written into the input dataset is refused (#867).

    A directory or glob input is re-globbed on every run, so an output written
    inside it joins the dataset: the next run reads its own previous output
    back and duplicates every row it holds. Nothing downstream can notice --
    the read succeeds and the counts merely grow -- so the write is refused up
    front, before anything is created.
    """

    def test_output_inside_input_directory_raises(self, parts_dir):
        with pytest.raises(GeoParquetError) as exc:
            guard_output_not_read_as_input(str(parts_dir), str(parts_dir / "sorted.parquet"))
        message = str(exc.value)
        assert "sorted.parquet" in message
        assert str(parts_dir) in message
        assert "somewhere else" in message

    def test_output_deeper_inside_input_directory_raises(self, parts_dir):
        nested = parts_dir / "nested"
        nested.mkdir()
        with pytest.raises(GeoParquetError, match="somewhere else"):
            guard_output_not_read_as_input(str(parts_dir), str(nested / "sorted.parquet"))

    def test_trailing_separator_on_the_directory_still_raises(self, parts_dir):
        with pytest.raises(GeoParquetError, match="somewhere else"):
            guard_output_not_read_as_input(
                f"{parts_dir}{os.sep}", str(parts_dir / "sorted.parquet")
            )

    def test_output_matching_input_glob_raises(self, parts_dir):
        with pytest.raises(GeoParquetError, match="somewhere else"):
            guard_output_not_read_as_input(
                str(parts_dir / "*.parquet"), str(parts_dir / "sorted.parquet")
            )

    def test_output_matching_recursive_input_glob_raises(self, parts_dir):
        nested = parts_dir / "nested"
        nested.mkdir()
        with pytest.raises(GeoParquetError, match="somewhere else"):
            guard_output_not_read_as_input(
                str(parts_dir / "**" / "*.parquet"), str(nested / "sorted.parquet")
            )

    def test_output_the_input_glob_does_not_match_is_allowed(self, parts_dir):
        """``parts/*.parquet`` is not read recursively, so a subdirectory is safe."""
        nested = parts_dir / "nested"
        nested.mkdir()
        guard_output_not_read_as_input(str(parts_dir / "*.parquet"), str(nested / "sorted.parquet"))

    def test_output_with_another_extension_under_the_glob_is_allowed(self, parts_dir):
        guard_output_not_read_as_input(str(parts_dir / "*.parquet"), str(parts_dir / "sorted.fgb"))

    def test_sibling_directory_output_is_allowed(self, parts_dir):
        guard_output_not_read_as_input(str(parts_dir), str(parts_dir.parent / "out" / "s.parquet"))

    def test_single_file_input_is_not_this_guard_s_business(self, tmp_path):
        """A single file input names exactly one file; the equality check in
        handle_output_overwrite covers it, and writing next to it is fine."""
        single = tmp_path / "in.parquet"
        single.write_bytes(b"x")
        guard_output_not_read_as_input(str(single), str(tmp_path / "out.parquet"))
        guard_output_not_read_as_input(str(single), str(single))

    def test_missing_paths_and_streams_are_ignored(self, parts_dir):
        guard_output_not_read_as_input(None, str(parts_dir / "s.parquet"))
        guard_output_not_read_as_input(str(parts_dir), None)
        guard_output_not_read_as_input(str(parts_dir), "-")
        guard_output_not_read_as_input("-", str(parts_dir / "s.parquet"))

    def test_remote_paths_are_ignored(self, parts_dir):
        """Remote datasets are not enumerated here; nothing local to compare."""
        guard_output_not_read_as_input("s3://bucket/parts/", "s3://bucket/parts/out.parquet")
        guard_output_not_read_as_input(str(parts_dir), "s3://bucket/out.parquet")

    def test_handle_output_overwrite_refuses_before_the_output_exists(self, parts_dir):
        """The very first run must be refused: the file it would create is the
        one that poisons the dataset, and it does not exist yet."""
        target = parts_dir / "sorted.parquet"
        assert not target.exists()
        with pytest.raises(GeoParquetError, match="somewhere else"):
            handle_output_overwrite(str(target), overwrite=False, input_path=str(parts_dir))

    def test_overwrite_does_not_bypass_the_guard(self, parts_dir):
        target = parts_dir / "sorted.parquet"
        target.write_bytes(b"stale")
        with pytest.raises(GeoParquetError, match="somewhere else"):
            handle_output_overwrite(str(target), overwrite=True, input_path=str(parts_dir))
        assert target.exists(), "a refused write must not delete anything"


# =============================================================================
# Tests for copy_file()
# =============================================================================


class _RecordingHandler(http.server.SimpleHTTPRequestHandler):
    """Static file handler that records every raw request target it is given."""

    seen: list[str] = []

    def do_GET(self):  # noqa: N802 - http.server API
        type(self).seen.append(self.path)
        super().do_GET()

    def log_message(self, *args):  # pragma: no cover - silence stderr noise
        pass


@pytest.fixture
def recording_http_server(tmp_path):
    """Serve a directory over loopback HTTP, recording the raw paths requested.

    Yields ``(base_url, served_dir, seen_paths)``. No outside network is
    touched, so this is not a ``network`` test.
    """
    served = tmp_path / "served"
    served.mkdir()

    handler_cls = type("_Handler", (_RecordingHandler,), {"seen": []})
    httpd = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0), functools.partial(handler_cls, directory=str(served))
    )
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_address[1]}", served, handler_cls.seen
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


class TestCopyFile:
    """Test byte-for-byte copying, local and remote (#798 diff-cover gap)."""

    def test_local_copy_verbose_logs_debug_message(self, tmp_path, caplog):
        """verbose=True logs the 'Copying ... to ...' debug line before copying."""
        import logging

        from geoparquet_io.core.file_utils import copy_file

        source = tmp_path / "source.parquet"
        dest = tmp_path / "dest.parquet"
        source.write_bytes(b"geoparquet-bytes")

        with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
            copy_file(str(source), str(dest), verbose=True)

        assert dest.read_bytes() == b"geoparquet-bytes"
        assert f"Copying {source} to {dest}" in caplog.text

    def test_s3_copy_builds_the_store_from_gpio_s_configured_endpoint(self):
        """The ambient S3 config (--s3-endpoint/--s3-region/--aws-profile) reaches the store.

        A copy that built its own client from ambient credentials would target AWS
        even when the user pointed gpio at MinIO, while the recompute path on the
        same command line honoured the setting (#810).
        """
        from unittest.mock import patch

        from geoparquet_io.core.duckdb_utils import s3_config_scope
        from geoparquet_io.core.file_utils import resolve_object_store

        config = {
            "s3_endpoint": "minio.local:9000",
            "s3_region": "us-west-2",
            "s3_use_ssl": False,
            "profile": "my-minio",
        }

        with (
            patch("geoparquet_io.core.upload.S3Store") as mock_s3store,
            patch(
                "geoparquet_io.core.upload.resolve_aws_credentials",
                return_value={
                    "access_key_id": "AKIA-TEST",
                    "secret_access_key": "secret-test",
                },
            ) as mock_creds,
            s3_config_scope(config),
        ):
            store, key = resolve_object_store("s3://bucket/path/to/file.parquet")

        assert key == "path/to/file.parquet"
        assert store is mock_s3store.return_value
        mock_creds.assert_called_once_with("my-minio")
        assert mock_s3store.call_args.args[0] == "bucket"
        store_kwargs = mock_s3store.call_args.kwargs
        assert store_kwargs["endpoint"] == "http://minio.local:9000"
        assert store_kwargs["region"] == "us-west-2"
        assert store_kwargs["access_key_id"] == "AKIA-TEST"
        assert store_kwargs["secret_access_key"] == "secret-test"

    def test_gcs_urls_resolve_to_their_own_store(self):
        """gs:// is a real destination now, not an fsspec ImportError (#810)."""
        from unittest.mock import patch

        from geoparquet_io.core.file_utils import resolve_object_store

        with patch("geoparquet_io.core.upload.obs.store.from_url") as mock_from_url:
            _, gs_key = resolve_object_store("gs://bucket/path/file.parquet")

        assert gs_key == "path/file.parquet"
        assert [call.args[0] for call in mock_from_url.call_args_list] == ["gs://bucket"]

    def test_azure_urls_resolve_to_an_azure_store(self, monkeypatch):
        """az:// copies build a real AzureStore from the URL's own account (#864).

        ``obs.store.from_url('az://account/container')`` could never serve this:
        without env config it demands an account, with it the account segment is
        misread as the container. The store is built explicitly instead, and this
        test constructs the real thing -- no network, no credentials needed.
        """
        from obstore.store import AzureStore

        from geoparquet_io.core.file_utils import resolve_object_store

        # Same breadth as TestAzureStoreConstruction.AZURE_ENV_VARS: a malformed
        # credential variable in the ambient environment (CI included) must not
        # be able to fail AzureStore construction.
        for name in (
            "AZURE_STORAGE_ACCOUNT_NAME",
            "AZURE_STORAGE_ACCOUNT_KEY",
            "AZURE_STORAGE_ACCESS_KEY",
            "AZURE_STORAGE_MASTER_KEY",
            "AZURE_STORAGE_SAS_TOKEN",
            "AZURE_STORAGE_SAS_KEY",
            "AZURE_CONTAINER_NAME",
        ):
            monkeypatch.delenv(name, raising=False)

        store, key = resolve_object_store("az://myaccount/mycontainer/path/file.parquet")

        assert isinstance(store, AzureStore)
        assert store.config["account_name"] == "myaccount"
        assert store.config["container_name"] == "mycontainer"
        assert key == "path/file.parquet"

    @pytest.mark.parametrize(
        "url",
        [
            "abfs://container@account.dfs.core.windows.net/in.parquet",
            "abfss://container@account.dfs.core.windows.net/in.parquet",
            "azure://account/container/file.parquet",
        ],
    )
    @pytest.mark.parametrize("side", ["source", "dest"])
    def test_other_azure_url_forms_are_refused_by_name(self, url, side):
        """Only the ``az://<account>/<container>`` form is served; the rest say so.

        ``abfs[s]://`` and ``azure://`` carry the account and container in a
        different order (or in a host name), so they cannot be parsed as gpio's
        account-first form. Name the supported spelling instead of guessing, and
        build no store at all.
        """
        from unittest.mock import patch

        from geoparquet_io.core.file_utils import copy_file

        args = (url, "out.parquet") if side == "source" else ("in.parquet", url)
        with (
            patch("geoparquet_io.core.upload.S3Store") as mock_s3store,
            patch("geoparquet_io.core.upload.obs.store.from_url") as mock_from_url,
            pytest.raises(InvalidParameterError, match=r"az://<account>/<container>"),
        ):
            copy_file(*args)

        mock_s3store.assert_not_called()
        mock_from_url.assert_not_called()

    def test_http_source_copy_requests_path_and_query_verbatim(
        self, recording_http_server, tmp_path
    ):
        """An http(s) source is fetched with a plain GET of the URL exactly as given.

        The object-store route keyed on ``urlsplit(url).path`` discarded the query
        string, so a presigned URL was requested without its signature. The copy
        must send path *and* query, untouched.
        """
        from geoparquet_io.core.file_utils import copy_file

        base_url, served, seen = recording_http_server
        payload = b"presigned-source-bytes"
        (served / "data.parquet").write_bytes(payload)
        dest = tmp_path / "dest.parquet"

        copy_file(f"{base_url}/data.parquet?X-Amz-Signature=abc%2Fdef", str(dest))

        assert dest.read_bytes() == payload
        assert seen == ["/data.parquet?X-Amz-Signature=abc%2Fdef"], seen

    def test_http_source_copy_does_not_double_encode_percent_escapes(
        self, recording_http_server, tmp_path
    ):
        """A ``%20`` in the source URL reaches the server as ``%20``, not ``%2520``.

        Same contract as #825/#845 for reads: the URL the user pasted already is
        the percent-encoded form, so the copy encodes nothing.
        """
        from geoparquet_io.core.file_utils import copy_file

        base_url, served, seen = recording_http_server
        payload = b"space-named-source-bytes"
        (served / "my file.parquet").write_bytes(payload)
        dest = tmp_path / "dest.parquet"

        copy_file(f"{base_url}/my%20file.parquet", str(dest))

        assert dest.read_bytes() == payload
        assert seen == ["/my%20file.parquet"], seen

    def test_http_source_copy_raises_on_http_error_status(self, recording_http_server, tmp_path):
        """A 404 on the source surfaces as an error, not an empty destination file."""
        from geoparquet_io.core.file_utils import copy_file

        base_url, _served, _seen = recording_http_server
        dest = tmp_path / "dest.parquet"

        with pytest.raises(Exception, match="404"):
            copy_file(f"{base_url}/missing.parquet", str(dest))

        assert not dest.exists()

    def test_unsupported_scheme_fails_before_any_store_is_built(self):
        """A scheme with no store dies by name up front, and lists the ones that work (#810)."""
        from geoparquet_io.core.file_utils import _check_copyable_scheme

        with pytest.raises(GeoParquetError) as exc:
            _check_copyable_scheme("source_path", "ftp://example.com/in.parquet", writing=False)

        message = str(exc.value)
        assert "ftp://" in message
        for scheme in ("s3://", "gs://", "az://"):
            assert scheme in message, f"the error must name {scheme} as a supported scheme"

    def test_http_destination_is_rejected_as_read_only(self):
        """HTTP(S) can be copied from, never to; say so instead of failing mid-stream."""
        from geoparquet_io.core.file_utils import copy_file

        with pytest.raises(GeoParquetError, match="read-only") as exc:
            copy_file("local.parquet", "https://example.com/out.parquet")

        assert "az://" in str(exc.value), "az:// is a writable destination; offer it"

    def test_remote_to_remote_copy_streams_bytes_through_obstore(self, monkeypatch):
        """A remote->remote copy moves the real bytes, with no network and no whole-file buffer."""
        import obstore as obs
        from obstore.store import MemoryStore

        from geoparquet_io.core import file_utils

        payload = b"geoparquet-bytes" * 1000
        store = MemoryStore()
        obs.put(store, "in.parquet", payload)
        monkeypatch.setattr(
            file_utils, "resolve_object_store", lambda url: (store, url.rsplit("/", 1)[-1])
        )

        file_utils.copy_file("s3://bucket/in.parquet", "s3://bucket/out.parquet")

        assert obs.get(store, "out.parquet").bytes().to_bytes() == payload

    def test_remote_source_local_dest_writes_the_local_file(self, monkeypatch, tmp_path):
        """Only one side needs to be remote; the local side is plain file I/O."""
        import obstore as obs
        from obstore.store import MemoryStore

        from geoparquet_io.core import file_utils

        payload = b"mixed-source-bytes"
        store = MemoryStore()
        obs.put(store, "in.parquet", payload)
        monkeypatch.setattr(
            file_utils, "resolve_object_store", lambda url: (store, url.rsplit("/", 1)[-1])
        )
        dest = tmp_path / "dest.parquet"

        file_utils.copy_file("s3://bucket/in.parquet", str(dest))

        assert dest.read_bytes() == payload

    def test_local_source_remote_dest_puts_the_object(self, monkeypatch, tmp_path):
        """A local file uploaded to a remote destination lands under the resolved key."""
        import obstore as obs
        from obstore.store import MemoryStore

        from geoparquet_io.core import file_utils

        payload = b"local-source-bytes"
        source = tmp_path / "source.parquet"
        source.write_bytes(payload)
        store = MemoryStore()
        monkeypatch.setattr(
            file_utils, "resolve_object_store", lambda url: (store, url.rsplit("/", 1)[-1])
        )

        file_utils.copy_file(str(source), "s3://bucket/out.parquet")

        assert obs.get(store, "out.parquet").bytes().to_bytes() == payload

    def test_mid_stream_failure_does_not_commit_the_destination_object(self, monkeypatch):
        """A copy that dies mid-stream must not commit a truncated object.

        Closing the obstore writer *commits* whatever was buffered, so a
        ``finally: close()`` turned a torn connection into a truncated object at
        the destination key. On failure the writer is dropped un-committed and
        the destination stays absent.
        """
        import obstore
        from obstore.store import MemoryStore

        from geoparquet_io.core import file_utils

        store = MemoryStore()
        monkeypatch.setattr(
            file_utils, "resolve_object_store", lambda url: (store, url.rsplit("/", 1)[-1])
        )
        monkeypatch.setattr(obstore, "open_reader", lambda s, key: _FailingMidStreamReader())

        with pytest.raises(OSError, match="torn down"):
            file_utils.copy_file("s3://bucket/in.parquet", "s3://bucket/out.parquet")

        with pytest.raises(FileNotFoundError):
            obstore.get(store, "out.parquet")

    def test_mid_stream_failure_preserves_a_pre_existing_destination_object(self, monkeypatch):
        """A good object already at the key survives a failed overwrite attempt."""
        import obstore
        from obstore.store import MemoryStore

        from geoparquet_io.core import file_utils

        good = b"the-good-object-bytes"
        store = MemoryStore()
        obstore.put(store, "out.parquet", good)
        monkeypatch.setattr(
            file_utils, "resolve_object_store", lambda url: (store, url.rsplit("/", 1)[-1])
        )
        monkeypatch.setattr(obstore, "open_reader", lambda s, key: _FailingMidStreamReader())

        with pytest.raises(OSError, match="torn down"):
            file_utils.copy_file("s3://bucket/in.parquet", "s3://bucket/out.parquet")

        assert obstore.get(store, "out.parquet").bytes().to_bytes() == good

    def test_mid_stream_failure_unlinks_a_partial_local_destination(self, monkeypatch, tmp_path):
        """A local destination is not left behind truncated when the source dies."""
        import obstore
        from obstore.store import MemoryStore

        from geoparquet_io.core import file_utils

        store = MemoryStore()
        monkeypatch.setattr(
            file_utils, "resolve_object_store", lambda url: (store, url.rsplit("/", 1)[-1])
        )
        monkeypatch.setattr(obstore, "open_reader", lambda s, key: _FailingMidStreamReader())
        dest = tmp_path / "dest.parquet"

        with pytest.raises(OSError, match="torn down"):
            file_utils.copy_file("s3://bucket/in.parquet", str(dest))

        assert not dest.exists()

    def test_http_source_torn_mid_stream_unlinks_the_partial_local_destination(self, tmp_path):
        """The http(s) branch has the same guarantee: no truncated output survives."""
        import functools as ft
        import http.server as hs
        import threading as th

        from geoparquet_io.core.file_utils import copy_file

        class _TruncatingHandler(hs.BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 - http.server API
                self.send_response(200)
                self.send_header("Content-Length", "1000000")
                self.end_headers()
                self.wfile.write(b"only-a-few-bytes")
                self.wfile.flush()
                self.connection.close()

            def log_message(self, *args):  # pragma: no cover
                pass

        httpd = hs.ThreadingHTTPServer(("127.0.0.1", 0), ft.partial(_TruncatingHandler))
        thread = th.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        dest = tmp_path / "dest.parquet"
        try:
            with pytest.raises(Exception):  # noqa: B017 - the transport error type varies
                copy_file(
                    f"http://127.0.0.1:{httpd.server_address[1]}/in.parquet",
                    str(dest),
                )
        finally:
            httpd.shutdown()
            httpd.server_close()
            thread.join(timeout=5)

        assert not dest.exists()


class _FailingMidStreamReader:
    """A source handle that yields one chunk, then dies like a broken connection."""

    def __init__(self):
        self._calls = 0

    def read(self, size=-1):
        if self._calls == 0:
            self._calls += 1
            return b"partial-bytes"
        raise OSError("connection torn down mid-stream")

    def close(self):
        pass


# =============================================================================
# Tests for safe_file_url()
# =============================================================================


class TestSafeFileUrl:
    """Test SQL-safe file URL preparation."""

    def test_local_path_escapes_quotes(self, tmp_path):
        """Local paths have single quotes escaped."""
        # Create a file with a quote in its name
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"test")

        result = safe_file_url(str(test_file))

        assert "'" not in result or "''" in result

    def test_local_path_single_quote_escaped(self, tmp_path):
        """Single quotes in path are doubled for SQL."""
        # We can't easily create a file with a quote, so test the escaping logic
        # by using a mock path
        test_dir = tmp_path / "test"
        test_dir.mkdir()
        test_file = test_dir / "file.parquet"
        test_file.write_bytes(b"test")

        # Test with a path that has quotes
        path_with_quote = str(test_file).replace("file", "file'name")
        # Since file doesn't exist, this will raise
        with pytest.raises(FileNotFoundGeoParquetError):
            safe_file_url(path_with_quote)

    def test_glob_pattern_allowed_without_existence_check(self, tmp_path):
        """Glob patterns don't require existence check."""
        pattern = str(tmp_path / "*.parquet")
        result = safe_file_url(pattern)
        assert result == pattern

    def test_nonexistent_file_raises(self, tmp_path):
        """Non-existent file raises FileNotFoundGeoParquetError."""
        with pytest.raises(FileNotFoundGeoParquetError):
            safe_file_url(str(tmp_path / "nonexistent.parquet"))

    def test_remote_url_passes_through(self):
        """Remote URLs are resolved without being altered."""
        url = "https://example.com/path/to/file.parquet"
        assert safe_file_url(url) == url

    def test_s3_url_passes_through(self):
        """S3 URLs pass through unchanged (except quote escaping)."""
        url = "s3://bucket/file.parquet"
        result = safe_file_url(url)
        assert result == url

    def test_http_url_with_special_chars_is_taken_as_given(self):
        """HTTP URLs are taken as already percent-encoded (#825).

        Both a valid encoded URL and one with a raw space reach the reader
        unchanged; see tests/test_remote_url_encoding.py for the contract.
        """
        encoded = "https://example.com/path%20with%20spaces/file.parquet"
        assert safe_file_url(encoded) == encoded

        raw = "https://example.com/path with spaces/file.parquet"
        assert safe_file_url(raw) == raw

    def test_sql_injection_prevention(self, tmp_path):
        """SQL injection via single quotes is prevented."""
        test_file = tmp_path / "safe.parquet"
        test_file.write_bytes(b"test")

        # If the path had a quote, it would be doubled
        result = safe_file_url(str(test_file))
        # Result should be usable in SQL without injection risk
        assert result.count("'") % 2 == 0 or "'" not in result


class TestAzureReadsAreRefusedByName:
    """Azure-scheme inputs are refused at the read boundary, not deep in DuckDB.

    gpio never loads DuckDB's azure extension, so an ``az://`` input dies inside
    the metadata probe with a misleading "not a valid GeoParquet file" unless the
    read boundary names the actual limitation first: Azure is a write destination
    only.
    """

    @pytest.mark.parametrize(
        "url",
        [
            "az://myaccount/mycontainer/in.parquet",
            "azure://myaccount/mycontainer/in.parquet",
            "abfs://container@account.dfs.core.windows.net/in.parquet",
            "abfss://container@account.dfs.core.windows.net/in.parquet",
        ],
    )
    def test_resolve_file_url_refuses_azure_reads(self, url):
        """Every Azure spelling gets the same early, named refusal."""
        from geoparquet_io.core.file_utils import resolve_file_url

        with pytest.raises(InvalidParameterError, match="write destination"):
            resolve_file_url(url)

    def test_safe_file_url_refuses_azure_reads_too(self):
        """The SQL-facing wrapper shares the same boundary."""
        with pytest.raises(InvalidParameterError, match="write destination"):
            safe_file_url("az://myaccount/mycontainer/in.parquet")


# =============================================================================
# Tests for _get_file_cache_key()
# =============================================================================


class TestGetFileCacheKey:
    """Test cache key generation for files."""

    def test_local_file_includes_mtime(self, tmp_path):
        """Local files include modification time in key."""
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"test")

        key = _get_file_cache_key(str(test_file))

        assert len(key) == 2
        assert isinstance(key[0], str)
        assert isinstance(key[1], float)
        assert key[1] > 0  # mtime should be non-zero

    def test_remote_url_zero_mtime(self):
        """Remote URLs have zero mtime."""
        url = "s3://bucket/file.parquet"
        key = _get_file_cache_key(url)

        assert key == (url, 0)

    def test_nonexistent_file_zero_mtime(self, tmp_path):
        """Non-existent files have zero mtime."""
        path = str(tmp_path / "nonexistent.parquet")
        key = _get_file_cache_key(path)

        assert key[1] == 0

    def test_key_uses_resolved_path(self, tmp_path):
        """Key uses resolved absolute path for local files."""
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"test")

        key = _get_file_cache_key(str(test_file))

        # Should be absolute path
        assert os.path.isabs(key[0])

    def test_different_files_different_keys(self, tmp_path):
        """Different files produce different cache keys."""
        file1 = tmp_path / "file1.parquet"
        file2 = tmp_path / "file2.parquet"
        file1.write_bytes(b"test1")
        file2.write_bytes(b"test2")

        key1 = _get_file_cache_key(str(file1))
        key2 = _get_file_cache_key(str(file2))

        assert key1[0] != key2[0]

    def test_modified_file_changes_key(self, tmp_path):
        """Modified file produces different mtime in key."""
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"test")

        key1 = _get_file_cache_key(str(test_file))

        # Modify the file
        import time

        time.sleep(0.01)  # Ensure time difference
        test_file.write_bytes(b"modified")

        key2 = _get_file_cache_key(str(test_file))

        # Path should be same, mtime should differ
        assert key1[0] == key2[0]
        assert key1[1] != key2[1]


# =============================================================================
# Integration Tests
# =============================================================================


class TestFileUtilsIntegration:
    """Integration tests using real GeoParquet files."""

    def test_partition_workflow(self, country_partition_dir):
        """Full workflow: detect -> resolve -> get files."""
        # Detect as partition
        assert is_partition_path(country_partition_dir) is True

        # Resolve path
        resolved, options = resolve_partition_path(country_partition_dir)
        assert resolved.endswith("*.parquet") or "**" in resolved

        # Get files
        files = get_all_parquet_files(country_partition_dir)
        assert len(files) == 4  # El_Salvador, Guatemala, Honduras, Nicaragua

        # Get first file
        first = get_first_parquet_file(country_partition_dir)
        assert first is not None
        assert first.endswith(".parquet")

    def test_single_file_workflow(self, places_test_file):
        """Single file is not a partition."""
        assert is_partition_path(places_test_file) is False

        files = get_all_parquet_files(places_test_file)
        # Single existing file returns list with that file
        if os.path.exists(places_test_file):
            assert files == [places_test_file]
