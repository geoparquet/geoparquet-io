"""Tests for core exception classes."""

import pytest

from geoparquet_io.core.exceptions import (
    ExtensionUnavailableError,
    FileNotFoundGeoParquetError,
    GeometryError,
    GeoParquetError,
    InvalidParameterError,
    PartitionError,
    RemoteAccessError,
    ValidationError,
    is_unpublished_extension_error,
)
from geoparquet_io.core.partition.common import PartitionAnalysisError

# The two DuckDB 1.5.5 errors these branches must tell apart, verbatim. Both say
# "Failed to download extension"; only the first says the registry answered.
_NOT_PUBLISHED = (
    'HTTP Error: Failed to download extension "geography" at URL '
    '"http://community-extensions.duckdb.org/v1.5.5/osx_arm64/geography.duckdb_extension.gz" '
    "(HTTP 404)"
)
_OFFLINE = (
    'IO Error: Failed to download extension "geography" at URL '
    '"http://community-extensions.duckdb.org/v1.5.5/osx_arm64/geography.duckdb_extension.gz" '
    "(ERROR Could not establish connection)"
)


class TestCoreExceptions:
    """Test that core exceptions are framework-agnostic."""

    def test_base_exception_inherits_from_exception(self):
        """GeoParquetError should inherit from Exception, not click exceptions."""
        assert issubclass(GeoParquetError, Exception)
        # Should NOT inherit from click
        import click

        assert not issubclass(GeoParquetError, click.ClickException)

    def test_file_not_found_error(self):
        with pytest.raises(FileNotFoundGeoParquetError) as exc_info:
            raise FileNotFoundGeoParquetError("test.parquet")
        assert "test.parquet" in str(exc_info.value)

    def test_extension_unavailable_error(self):
        """ExtensionUnavailableError names the extension and DuckDB version (issue #491)."""
        exc = ExtensionUnavailableError("geography", "1.5.5")
        assert isinstance(exc, GeoParquetError)
        assert exc.name == "geography"
        assert exc.duckdb_version == "1.5.5"
        message = str(exc)
        assert "geography" in message
        assert "1.5.5" in message

    def test_extension_unavailable_error_with_detail(self):
        exc = ExtensionUnavailableError("geography", "1.5.5", "HTTP 404")
        assert "HTTP 404" in str(exc)

    def test_extension_unavailable_error_names_the_feature(self):
        """The failing command is named so users know what stopped working (#737)."""
        exc = ExtensionUnavailableError("geography", "1.5.5", feature="gpio add s2")
        assert "gpio add s2" in str(exc)

    def test_geography_hint_offers_a5_and_never_a_forbidden_downgrade(self):
        """The 404 branch must be actionable without violating the pin (#778).

        The floor is duckdb>=1.5.6, a release 'geography' is published for
        on mainstream platforms, so a 404 now points at a platform the
        registry has no builds for (Windows ARM, musl, source builds) or an
        intercepting proxy — not at the version, and not primarily at this
        machine's network. Either way the hint must not tell a user to
        install duckdb 1.5.1: that is below the floor, so it leaves
        `uv pip check` failing and any `uv sync` silently reverting it.
        `gpio add a5` is the substitute without S2.
        """
        message = str(ExtensionUnavailableError("geography", "1.5.5", _NOT_PUBLISHED))

        assert "a5" in message
        assert "mainstream platforms" in message
        assert "no community builds" in message
        assert "is not published for this one" in message
        # The 404 branch must not repeat the connectivity branch's diagnosis.
        assert "check network access" not in message
        # Never recommend a DuckDB the pin forbids.
        assert "duckdb==1.5.1" not in message
        assert "pip install" not in message
        # No promised timeline: "pending" claimed a republication nobody filed.
        assert "pending" not in message.lower()

    def test_geography_hint_is_absent_when_the_download_merely_failed(self):
        """An offline user must not be told to wait for an upstream release (#778).

        Both DuckDB errors say "Failed to download extension"; only the 404 says
        the registry answered and lacks this build. Diagnosing on the shared
        phrase told users behind a proxy that S2 was unpublished.
        """
        message = str(ExtensionUnavailableError("geography", "1.5.5", _OFFLINE))

        assert "published for every DuckDB gpio supports" not in message
        assert "is not published for this one" not in message
        assert "reachable" in message
        assert "proxy" in message

    def test_unpublished_flag_tracks_the_underlying_error(self):
        assert ExtensionUnavailableError("geography", "1.5.5", _NOT_PUBLISHED).unpublished
        assert not ExtensionUnavailableError("geography", "1.5.5", _OFFLINE).unpublished
        assert not ExtensionUnavailableError("geography", "1.5.5").unpublished

    def test_no_detail_claims_neither_cause(self):
        """With no underlying error there is no evidence for either diagnosis."""
        message = str(ExtensionUnavailableError("geography", "1.5.5"))

        assert "may not be published" in message
        assert "proxy" not in message
        assert "published for every DuckDB gpio supports" not in message

    def test_extension_unavailable_error_hint_is_extension_specific(self):
        """Other community extensions must not inherit the geography guidance."""
        message = str(ExtensionUnavailableError("h3", "1.5.5", _NOT_PUBLISHED))

        assert "published for every DuckDB gpio supports" not in message
        assert "a5" not in message
        assert "h3" in message

    def test_is_unpublished_extension_error_walks_the_cause_chain(self):
        """gpio raises `from e`, so the 404 can sit one link down the chain."""
        cause = RuntimeError(_NOT_PUBLISHED)
        exc = ExtensionUnavailableError("geography", "1.5.5")
        exc.__cause__ = cause

        assert is_unpublished_extension_error(exc)
        assert is_unpublished_extension_error(_NOT_PUBLISHED)
        assert not is_unpublished_extension_error(_OFFLINE)
        assert not is_unpublished_extension_error(None)

    def test_file_not_found_error_with_detail(self):
        with pytest.raises(FileNotFoundGeoParquetError) as exc_info:
            raise FileNotFoundGeoParquetError("test.parquet", "no read permission")
        assert "test.parquet" in str(exc_info.value)
        assert "no read permission" in str(exc_info.value)

    def test_file_not_found_error_sanitizes_presigned_s3_url(self):
        """Presigned S3 URLs with credentials should be sanitized in error message."""
        presigned_url = (
            "s3://bucket/data/file.parquet"
            "?AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE"
            "&Signature=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
            "&Expires=1234567890"
        )
        exc = FileNotFoundGeoParquetError(presigned_url)

        # Original path preserved for programmatic access
        assert "AWSAccessKeyId" in exc.path
        assert "Signature" in exc.path

        # Error message should NOT contain credentials
        error_msg = str(exc)
        assert "AWSAccessKeyId" not in error_msg
        assert "AKIAIOSFODNN7EXAMPLE" not in error_msg
        assert "Signature" not in error_msg
        assert "wJalrXUtnFEMI" not in error_msg

        # But should still identify the file
        assert "s3://bucket/data/file.parquet" in error_msg

    def test_file_not_found_error_sanitizes_gcs_signed_url(self):
        """GCS signed URLs with credentials should be sanitized."""
        signed_url = (
            "gs://bucket/path/file.parquet"
            "?X-Goog-Algorithm=GOOG4-RSA-SHA256"
            "&X-Goog-Credential=service-account%40project.iam.gserviceaccount.com"
            "&X-Goog-Signature=abc123secret"
        )
        exc = FileNotFoundGeoParquetError(signed_url, "bucket not found")

        # Credentials NOT in error message
        assert "X-Goog-Credential" not in str(exc)
        assert "X-Goog-Signature" not in str(exc)
        assert "abc123secret" not in str(exc)

        # File path and detail still present
        assert "gs://bucket/path/file.parquet" in str(exc)
        assert "bucket not found" in str(exc)

    def test_file_not_found_error_preserves_local_paths(self):
        """Local file paths without query strings should pass through unchanged."""
        local_path = "/home/user/data/file.parquet"
        exc = FileNotFoundGeoParquetError(local_path)

        assert exc.path == local_path
        assert local_path in str(exc)

    def test_invalid_parameter_error(self):
        with pytest.raises(InvalidParameterError) as exc_info:
            raise InvalidParameterError("resolution", "must be between 1-15")
        assert "resolution" in str(exc_info.value)
        assert "must be between 1-15" in str(exc_info.value)

    def test_remote_access_error(self):
        with pytest.raises(RemoteAccessError):
            raise RemoteAccessError("s3://bucket/file", "Access denied")

    def test_remote_access_error_sanitizes_url(self):
        """URLs with credentials should be sanitized."""
        exc = RemoteAccessError(
            "s3://bucket/path/to/file.parquet?AWSAccessKeyId=SECRET", "Access denied"
        )
        # Query string should be stripped
        assert "AWSAccessKeyId" not in exc.url
        assert "SECRET" not in exc.url
        # URL is truncated but filename preserved: s3://bucket/path/.../file.parquet
        assert "..." in exc.url
        assert "bucket" in exc.url
        assert exc.url.endswith("file.parquet")

    def test_geometry_error(self):
        with pytest.raises(GeometryError):
            raise GeometryError("No geometry column found")

    def test_partition_error(self):
        with pytest.raises(PartitionError):
            raise PartitionError("Invalid partition scheme")

    def test_partition_error_carries_no_result_by_default(self):
        """Most partition failures are about one file and have nothing to report."""
        assert PartitionError("Invalid partition scheme").result is None

    def test_partition_error_can_carry_the_run_that_failed(self):
        """A directory sub-partition run fails *partly*: the survivors still matter.

        `ops.sub_partition_by_*` raises once the run finishes, so the caller needs
        what succeeded as well as what did not (#811).
        """
        run = {"processed": 2, "skipped": 0, "errors": [{"file": "a.parquet", "error": "boom"}]}

        exc = PartitionError("1 of 3 file(s) failed to sub-partition", result=run)

        assert exc.result == run
        assert str(exc) == "1 of 3 file(s) failed to sub-partition"

    def test_validation_error(self):
        with pytest.raises(ValidationError):
            raise ValidationError("File does not conform to GeoParquet spec")

    def test_exception_hierarchy(self):
        """All specific exceptions should inherit from GeoParquetError."""
        assert issubclass(FileNotFoundGeoParquetError, GeoParquetError)
        assert issubclass(InvalidParameterError, GeoParquetError)
        assert issubclass(RemoteAccessError, GeoParquetError)
        assert issubclass(GeometryError, GeoParquetError)
        assert issubclass(PartitionError, GeoParquetError)
        assert issubclass(ValidationError, GeoParquetError)

    def test_partition_analysis_error_in_hierarchy(self):
        """PartitionAnalysisError should inherit from PartitionError."""
        assert issubclass(PartitionAnalysisError, PartitionError)
        assert issubclass(PartitionAnalysisError, GeoParquetError)
        # Should work with handle_core_exception via PartitionError handling
        exc = PartitionAnalysisError("Bad partition strategy")
        assert exc.message == "Bad partition strategy"

    def test_exception_message_attribute(self):
        """All exceptions should have a message attribute."""
        exc = GeoParquetError("test message")
        assert exc.message == "test message"

        exc2 = InvalidParameterError("param", "reason")
        assert "param" in exc2.message
        assert exc2.param_name == "param"
        assert exc2.reason == "reason"
