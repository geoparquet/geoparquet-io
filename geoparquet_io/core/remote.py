"""
Remote URL handling for S3, GCS, Azure, and HTTP/HTTPS.

This module provides utilities for detecting and working with remote URLs
in cloud object storage (S3, GCS, Azure) and HTTP/HTTPS endpoints.
"""

from __future__ import annotations

import os
import warnings
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, fields, replace
from pathlib import Path
from typing import Any

from geoparquet_io.core.exceptions import (
    FailureKind,
    InvalidParameterError,
    classify_failure,
    fatal_status_message,
    sanitize_url_for_logging,
)
from geoparquet_io.core.logging_config import debug, info, progress, success
from geoparquet_io.core.sizing import bytes_to_mb

#: Every URL scheme gpio reads or writes, and the store family it belongs to.
#: The one scheme table (#1367): :func:`is_remote_url` and every family test
#: below read it, and nothing outside this module spells a scheme out.
#: Matching is case-sensitive, as DuckDB's own is.
REMOTE_SCHEMES = {
    "http": "http",
    "https": "http",
    "s3": "s3",
    "s3a": "s3",
    "gs": "gcs",
    "gcs": "gcs",
    "az": "azure",
    "azure": "azure",
    "abfs": "azure",
    "abfss": "azure",
}


def url_scheme(path) -> str | None:
    """The scheme of a remote URL as written (``"s3a"``, ``"https"``), else None.

    None for a local path, ``file://``, an unknown scheme and anything that is
    not a string.
    """
    if not isinstance(path, str):
        return None
    scheme, separator, _ = path.partition("://")
    return scheme if separator and scheme in REMOTE_SCHEMES else None


def remote_family(path) -> str | None:
    """The store family of a remote URL: ``http``, ``s3``, ``gcs`` or ``azure``."""
    scheme = url_scheme(path)
    return REMOTE_SCHEMES[scheme] if scheme else None


#: The one spelling gpio's object-store code (upload, copy_file) addresses each
#: family by: ``s3a://`` is ``s3://`` and ``gcs://`` is ``gs://`` everywhere.
#: Azure is addressed only as ``az://<account>/<container>/<path>`` (#864); the
#: ``azure://`` and ``abfs[s]://`` spellings put the account and container
#: elsewhere and are refused by name, never respelled.
STORE_SCHEME_FOR_FAMILY = {"s3": "s3", "gcs": "gs", "azure": "az"}


def is_http_url(path) -> bool:
    """Whether *path* is an ``http://`` or ``https://`` URL."""
    return remote_family(path) == "http"


def is_account_first_azure_url(path) -> bool:
    """Whether *path* is an Azure URL in the one form gpio parses, ``az://``."""
    return is_azure_url(path) and url_scheme(path) == STORE_SCHEME_FOR_FAMILY["azure"]


def canonical_store_url(url: str) -> str:
    """*url* under its family's store spelling: ``s3a://b/k`` -> ``s3://b/k``,
    ``gcs://b/k`` -> ``gs://b/k``. Azure, HTTP(S) and local paths come back as
    given."""
    family = remote_family(url)
    if family is None or family == "azure":
        return url
    store_scheme = STORE_SCHEME_FOR_FAMILY.get(family)
    if store_scheme is None:
        return url
    return f"{store_scheme}://{url.partition('://')[2]}"


@contextmanager
def open_url(path, mode: str = "rb"):
    """Open a local path or a remote URL as a file object (fsspec underneath).

    The one place gpio hands a URL to fsspec, so a filesystem picked by URL
    prefix cannot grow up beside the scheme table (#1367 T2e).
    """
    import fsspec

    with fsspec.open(path, mode) as handle:
        yield handle


def has_http_scheme(url: str) -> bool:
    """Whether *url* parses with an ``http``/``https`` scheme, in any case.

    For a service endpoint (CARTO's SQL API) that must be handed to an HTTP
    client and nothing else: an empty, ``file://`` or object-store scheme fails.
    """
    from urllib.parse import urlparse

    return REMOTE_SCHEMES.get(urlparse(url).scheme) == "http"


def is_remote_url(path):
    """
    Check if path is a remote URL that DuckDB can read.

    Supports:
    - HTTP/HTTPS: http://, https://
    - AWS S3: s3://, s3a://
    - Azure: az://, azure://, abfs://, abfss://
    - Google Cloud Storage: gs://, gcs://

    Args:
        path: File path or URL to check

    Returns:
        bool: True if path is a remote URL, False otherwise
    """
    return url_scheme(path) is not None


def is_s3_url(path):
    """
    Check if path is an S3 URL.

    Args:
        path: File path or URL to check

    Returns:
        bool: True if path is S3
    """
    return remote_family(path) == "s3"


def is_azure_url(path):
    """
    Check if path is an Azure Blob Storage URL.

    Args:
        path: File path or URL to check

    Returns:
        bool: True if path is Azure
    """
    return remote_family(path) == "azure"


def is_gcs_url(path):
    """
    Check if path is a Google Cloud Storage URL.

    Args:
        path: File path or URL to check

    Returns:
        bool: True if path is GCS
    """
    return remote_family(path) == "gcs"


def needs_httpfs(path):
    """
    Check if path requires httpfs extension (S3, Azure, GCS).

    HTTP/HTTPS work without httpfs, but cloud storage protocols need it.

    Args:
        path: File path or URL to check

    Returns:
        bool: True if httpfs extension is needed
    """
    return remote_family(path) not in (None, "http")


def protocol_label(path, default: str = "HTTP") -> str:
    """The upper-cased scheme a message names a URL by (``S3``), else *default*."""
    return path.split("://")[0].upper() if "://" in path else default


def display_path(path):
    """*path* as a message may show it: a remote URL with its secrets stripped."""
    return sanitize_url_for_logging(path) if is_remote_url(path) else path


def resolve_s3_config(
    s3_endpoint: str | None = None,
    s3_region: str | None = None,
    s3_no_ssl: bool = False,
    aws_profile: str | None = None,
) -> dict:
    """Resolve S3 configuration from explicit params with env var fallbacks.

    Resolution order (highest wins):
    1. Explicit params
    2. AWS env vars (AWS_ENDPOINT_URL, AWS_REGION/AWS_DEFAULT_REGION, AWS_PROFILE)
    3. Defaults (no endpoint, no region, SSL on, no profile)

    ``profile_from_env`` is True when the profile is only the exported
    ``AWS_PROFILE``. The AWS SDK lets env keys beat an exported ``AWS_PROFILE``
    but not ``--profile``, so the DuckDB secret needs to know which one it got.
    """
    endpoint = s3_endpoint or os.environ.get("AWS_ENDPOINT_URL") or None
    region = s3_region or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    profile = aws_profile or os.environ.get("AWS_PROFILE")

    use_ssl = True
    bare_endpoint = endpoint
    if endpoint:
        if endpoint.startswith("http://"):
            use_ssl = False
            bare_endpoint = endpoint[len("http://") :]
        elif endpoint.startswith("https://"):
            bare_endpoint = endpoint[len("https://") :]

    if s3_no_ssl:
        use_ssl = False

    return {
        "s3_endpoint": bare_endpoint,
        "s3_region": region,
        "s3_use_ssl": use_ssl,
        "profile": profile,
        "profile_from_env": bool(profile) and not aws_profile,
    }


def validate_profile_for_urls(profile, *urls):
    """
    Validate that profile parameter is only used with S3 URLs.

    The --profile flag sets AWS credentials for S3 operations. Using it with
    other cloud providers (GCS, Azure) would be confusing since they use
    different authentication mechanisms.

    Args:
        profile: AWS profile name or None
        *urls: Variable number of file paths to validate

    Raises:
        InvalidParameterError: If profile is used with non-S3 remote URLs

    Example:
        validate_profile_for_urls(profile, input_file, output_file)
    """
    if not profile:
        return

    for url in urls:
        if url and is_remote_url(url) and not is_s3_url(url):
            protocol = protocol_label(url, "unknown")
            raise InvalidParameterError(
                "profile",
                f"only valid for S3 URLs, but got {protocol} URL: {sanitize_url_for_logging(url)}. "
                f"For {protocol} authentication, use environment variables or default credentials.",
            )


# ---------------------------------------------------------------------------
# The remote-access options object (#1436)
# ---------------------------------------------------------------------------


#: Click parameter -> ``RemoteOptions`` field, for every remote-access flag.
#: ``--aws-profile`` is spelled ``profile`` here and ``--s3-no-ssl`` is the
#: *negation* of ``s3_use_ssl``, inverted by :meth:`RemoteOptions.from_cli`. A
#: parameter absent from this table is not a remote-access flag and is ignored.
_CLI_SPELLINGS = {
    "aws_profile": "profile",
    "s3_endpoint": "s3_endpoint",
    "s3_region": "s3_region",
    "s3_no_ssl": "s3_use_ssl",
}

#: One warning per deprecated spelling per process, keyed ``"<entry point>.<keyword>"``.
_warned_spellings: set[str] = set()


@dataclass(frozen=True, kw_only=True)
class RemoteOptions:
    """How gpio reaches an object store, as one value (``gpio.RemoteOptions``, #1436).

    The second per-concern options object, after
    :class:`~geoparquet_io.core.parquet_writer.WriteOptions` (#761). Every CLI
    command that reads or writes a remote path builds exactly one from its
    flags and the root group's (:meth:`from_cli` plus
    ``cli.decorators.pack_remote_options``), and the Python API takes one
    directly, so a credential cannot be accepted by one door and dropped on the
    way to the store (#627, #663).

    Every field defaults to ``None``, which means "not asked": :meth:`resolved`
    then falls back to the AWS environment variables and finally to gpio's
    defaults (no endpoint, no region, SSL on, no profile). A field is asked for
    exactly when it is not None, so :meth:`merge` can let a command's own flag
    beat the root group's without a second sentinel.

    Attributes:
        profile: AWS profile name, as ``--aws-profile``. S3 only;
            :func:`validate_profile_for_urls` refuses it for another store.
        s3_endpoint: Custom S3-compatible endpoint, as ``--s3-endpoint``
            (e.g. ``"minio.example.com:9000"``). A scheme is stripped by
            :meth:`resolved`, which reads ``http://`` as ``s3_use_ssl=False``.
        s3_region: S3 region, as ``--s3-region``.
        s3_use_ssl: Whether to speak HTTPS to the endpoint. ``--s3-no-ssl`` is
            its negation; None means "not asked", which resolves to True.
    """

    profile: str | None = None
    s3_endpoint: str | None = None
    s3_region: str | None = None
    s3_use_ssl: bool | None = None

    @classmethod
    def pick(cls, values: Mapping[str, Any]) -> RemoteOptions:
        """The options named in *values*; other keys are ignored, absent fields stay unset."""
        return cls(
            **{
                f.name: values[f.name]
                for f in fields(cls)
                if f.name in values and values[f.name] is not None
            }
        )

    @classmethod
    def from_cli(cls, **params: Any) -> RemoteOptions:
        """The one ``RemoteOptions`` a CLI command builds from its Click parameters.

        ``--aws-profile`` is ``profile`` and ``--s3-no-ssl`` is the negation of
        ``s3_use_ssl``: the flag's ``False`` is "not asked", so it leaves the
        field unset rather than asking for SSL. Every other parameter is
        ignored -- ``profile`` above all, because ``gpio check --profile`` is a
        row-group size band and ``gpio benchmark --profile`` turns profiling on.
        Neither is a credential, and reading them here would hand an AWS
        profile named ``"balanced"`` to the credential chain.
        """
        values: dict[str, Any] = {}
        for name, value in params.items():
            field = _CLI_SPELLINGS.get(name)
            if field is None:
                continue
            # `--s3-no-ssl` absent is False, which is "not asked", not "use SSL":
            # asking would make an unset command flag beat the root group's.
            values[field] = (False if value else None) if name == "s3_no_ssl" else value
        return cls.pick(values)

    def merge(self, other: RemoteOptions | None) -> RemoteOptions:
        """This value with *other*'s asked-for fields on top.

        How a command's own ``--aws-profile`` beats the root group's: the root
        options are the base and the command's are *other*. A None field of
        *other* was not asked for and leaves this one as it is.
        """
        if other is None:
            return self
        return replace(self, **{name: getattr(other, name) for name in other.asked()})

    def asked(self) -> frozenset[str]:
        """The names of the fields that were asked for (not None)."""
        return frozenset(f.name for f in fields(self) if getattr(self, f.name) is not None)

    def __bool__(self) -> bool:
        """Whether anything was asked for at all."""
        return bool(self.asked())

    def resolved(self) -> dict:
        """The S3 config dict :func:`resolve_s3_config` builds from these options.

        The one bridge to the ambient config every DuckDB connection and the
        obstore store read, so an endpoint given here and an exported
        ``AWS_ENDPOINT_URL`` are reconciled in one place.
        """
        return resolve_s3_config(
            s3_endpoint=self.s3_endpoint,
            s3_region=self.s3_region,
            s3_no_ssl=self.s3_use_ssl is False,
            aws_profile=self.profile,
        )

    @contextmanager
    def activate(self) -> Iterator[dict]:
        """Make these options the ambient S3 config for the duration of the block.

        Yields the resolved config, as ``cli._shared._activate_s3`` did, so a
        caller that has to hand the individual values to a non-DuckDB client
        (obstore, in ``upload``) reads them from the same resolution.
        """
        from geoparquet_io.core.duckdb_utils import s3_config_scope

        config = self.resolved()
        with s3_config_scope(config):
            yield config


#: Deprecated API keyword -> ``RemoteOptions`` field. ``gpio.read_partition``
#: spells the profile ``aws_profile`` and everything else spells it ``profile``;
#: the warning has to name the keyword the caller actually wrote.
_KEYWORD_FIELDS = {
    "profile": "profile",
    "aws_profile": "profile",
    "s3_endpoint": "s3_endpoint",
    "s3_region": "s3_region",
    "s3_use_ssl": "s3_use_ssl",
}


def remote_options_from_keywords(
    where: str,
    remote: RemoteOptions | None,
    **legacy: Any,
) -> RemoteOptions:
    """*remote*, or one built from the deprecated per-keyword spellings.

    The migration path off the loose credential keywords (#1436). An entry point
    that used to take ``profile=``/``s3_endpoint=``/``s3_region=``/``s3_use_ssl=``
    keeps taking them: each is folded into the returned ``RemoteOptions`` and
    warned about once per process, under the horizon "removed in the next
    minor". Naming a field both in *remote* and as a keyword is a ``TypeError``,
    as ``Table.write`` already refuses a write field set twice.

    ``s3_use_ssl=True`` is the old signature's default, not a request, so it is
    not treated as asked for; ``s3_use_ssl=False`` is.

    Args:
        where: The entry point's name, for the warning and the error message.
        remote: The new spelling, or None.
        **legacy: The old keywords, by their old names.

    Returns:
        The one ``RemoteOptions`` the entry point should pass on.
    """
    asked = {
        name: value
        for name, value in legacy.items()
        if value is not None and not (name == "s3_use_ssl" and value is True)
    }
    base = remote or RemoteOptions()
    if not asked:
        return base

    fields_asked = {_KEYWORD_FIELDS[name]: value for name, value in asked.items()}
    both = sorted(fields_asked.keys() & base.asked())
    if both:
        raise TypeError(
            f"{where}() got {', '.join(both)} both in remote= and as a keyword; "
            "set each field in one place"
        )
    _warn_deprecated_spellings(where, asked)
    return base.merge(RemoteOptions.pick(fields_asked))


def _warn_deprecated_spellings(where: str, asked: Iterable[str]) -> None:
    """Warn once per process for each deprecated keyword *where* was given.

    The replacement names the ``RemoteOptions`` *field*, which is not always the
    old keyword: ``gpio.read_partition(aws_profile=...)`` becomes
    ``remote=gpio.RemoteOptions(profile=...)``.
    """
    for name in sorted(asked):
        key = f"{where}.{name}"
        if key in _warned_spellings:
            continue
        _warned_spellings.add(key)
        warnings.warn(
            f"{where}({name}=...) is deprecated and will be removed in the next "
            f"minor release; pass "
            f"remote=gpio.RemoteOptions({_KEYWORD_FIELDS[name]}=...) instead",
            DeprecationWarning,
            stacklevel=4,
        )


def show_remote_read_message(file_path, verbose=False):
    """
    Show consistent message when reading from remote files.

    Args:
        file_path: Path to check (local or remote)
        verbose: If True, show detailed message
    """
    if not is_remote_url(file_path):
        return

    protocol = protocol_label(file_path)
    if verbose:
        info(f"Reading from {protocol}: {sanitize_url_for_logging(file_path)}")
    else:
        info(f"Reading from {protocol} (network operations may take time)...")


#: The remedy for a 401/403, per object store: a fact about the store, while *which*
#: store a path is on is `is_s3_url` / `is_azure_url` / `is_gcs_url`'s decision.
_S3_CREDENTIALS = (
    "S3: check AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or configure ~/.aws/credentials."
)
_AZURE_CREDENTIALS = (
    "Azure: check AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY, "
    "or set AZURE_STORAGE_SAS_TOKEN for SAS token auth."
)
_GCS_CREDENTIALS = "GCS: check GOOGLE_APPLICATION_CREDENTIALS points to service account JSON."

#: The guidance for the failures that are not an HTTP status, keyed by the kind
#: ``classify_failure`` reports -- the same classification everything else uses.
_REMOTE_HINTS = {
    FailureKind.TIMEOUT: (
        "Connection timed out:\n  - Check network connectivity\n"
        "  - File may be very large - try a smaller file first\n"
        "  - Remote server may be slow or overloaded"
    ),
    FailureKind.UNREACHABLE: (
        "Cannot connect to remote server:\n  - Check network connectivity\n"
        "  - Verify the hostname/URL is correct\n  - Server may be down or unreachable"
    ),
}
_GENERIC_REMOTE_HINT = (
    "Remote file access failed:\n  - Check network connectivity\n"
    "  - Verify file URL and access permissions"
)


def _store_credentials(file_path: str) -> str:
    if is_s3_url(file_path):
        return _S3_CREDENTIALS
    if is_azure_url(file_path):
        return _AZURE_CREDENTIALS
    if is_gcs_url(file_path):
        return _GCS_CREDENTIALS
    return "The file may be private or require authentication."


def get_remote_error_hint(error: BaseException, file_path: str = "") -> str:
    """Guidance for a failed remote read, from the one failure classification.

    Args:
        error: The exception DuckDB (or another library) raised.
        file_path: The remote file path/URL that failed

    Returns:
        str: User-friendly message with troubleshooting hints. A 401/403/404 uses
        the shared status sentence (:func:`~geoparquet_io.core.exceptions.fatal_status_message`)
        with the object store's own credential remedy.
    """
    verdict = classify_failure(error)
    if verdict.kind is FailureKind.HTTP_STATUS and verdict.status is not None:
        message = fatal_status_message(
            verdict.status,
            subject="the remote file",
            credentials=_store_credentials(file_path),
        )
        if message is not None and verdict.status == 404 and file_path:
            return f"{message}\n  - URL: {sanitize_url_for_logging(file_path)}"
        if message is not None:
            return message
    return _REMOTE_HINTS.get(verdict.kind, _GENERIC_REMOTE_HINT)


def upload_if_remote(local_path, remote_path, profile=None, is_directory=False, verbose=False):
    """
    Upload local file/dir to remote path if remote_path is a remote URL.

    Args:
        local_path: Local file or directory path to upload
        remote_path: Remote URL or local path
        profile: AWS profile name (S3 only, optional)
        is_directory: Whether local_path is a directory
        verbose: Whether to print verbose output

    Returns:
        bool: True if upload was performed, False if not remote
    """
    if not is_remote_url(remote_path):
        return False

    from geoparquet_io.core.upload import upload

    if verbose:
        # Calculate size for progress indication
        if is_directory:
            total_size = sum(
                os.path.getsize(os.path.join(dirpath, filename))
                for dirpath, _, filenames in os.walk(local_path)
                for filename in filenames
            )
        else:
            total_size = os.path.getsize(local_path)

        size_mb = bytes_to_mb(total_size)
        progress(f"Uploading {size_mb:.1f} MB to {sanitize_url_for_logging(remote_path)}...")

    pattern = "*.parquet" if is_directory else None
    upload(
        source=Path(local_path),
        destination=remote_path,
        remote=RemoteOptions(profile=profile),
        pattern=pattern,
        dry_run=False,
    )

    if verbose:
        success(f"Successfully uploaded to {sanitize_url_for_logging(remote_path)}")

    return True


@contextmanager
def remote_write_context(output_path, is_directory=False, verbose=False):
    """
    Context manager for remote writes with automatic temp file/dir cleanup.

    Yields actual write path (temp for remote, original for local).
    Handles cleanup automatically on exit.

    Args:
        output_path: Output path (local or remote URL)
        is_directory: Whether output is a directory (for partitioning)
        verbose: Whether to print verbose output

    Yields:
        tuple: (actual_write_path, is_remote)
            - actual_write_path: Path to write to (temp for remote, original for local)
            - is_remote: Boolean indicating if output is remote

    Example:
        with remote_write_context('s3://bucket/file.parquet', verbose=True) as (path, is_remote):
            # Write to path
            write_file(path)
            # Cleanup and upload handled automatically
    """
    is_remote = is_remote_url(output_path)
    if not is_remote:
        yield output_path, False
        return
    from geoparquet_io.core.file_utils import scratch_dir, scratch_file

    context = scratch_dir() if is_directory else scratch_file()
    with context as temp_path:
        if verbose:
            debug(f"Remote output detected: {output_path}")
            debug(f"Writing to temporary path: {temp_path}")
        yield temp_path, True
