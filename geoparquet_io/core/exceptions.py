"""
Framework-agnostic exceptions for geoparquet-io core modules.

Core modules must NOT import click or raise click exceptions.
These exceptions are converted to click exceptions at the CLI boundary
by the cli/exception_handler.py module.

Exception Hierarchy:
    GeoParquetError (base)
    ├── FileNotFoundGeoParquetError - file/path not found
    ├── InvalidParameterError - invalid function argument
    ├── RemoteAccessError - S3/GCS/Azure access issues
    ├── GeometryError - geometry column issues
    ├── PartitionError - partitioning failures
    └── ValidationError - spec validation failures
"""

from __future__ import annotations

import enum
import json
import re
import socket
import xml.etree.ElementTree  # nosec B405 -- only classifies ParseError; never parses XML
from typing import NamedTuple, TypeGuard


def sanitize_url_for_logging(url: str) -> str:
    """Remove credentials and query params from URL for safe logging.

    Presigned URLs contain credentials in query parameters that should
    not be logged. This function strips the query string and truncates
    long paths for readability while preserving the filename.

    Args:
        url: URL that may contain sensitive query parameters

    Returns:
        Sanitized URL safe for logging, with long paths truncated but
        filename preserved (e.g., "s3://bucket/prefix/.../file.parquet")
    """
    if not url:
        return url
    # Remove query string (may contain presigned credentials)
    if "?" in url:
        url = url.split("?")[0]
    # Truncate very long paths but keep filename for debugging. A directory
    # URL ends in "/" and has no filename to keep: the elision would leave
    # `s3://bucket/datasets/.../`, which names nothing a user can act on --
    # the destination of a partial upload came out that way (#1025 review).
    # The credentials, if any, were in the query string, already gone.
    parts = url.split("/")
    if len(parts) > 5 and parts[-1]:
        return "/".join(parts[:4]) + "/..." + "/" + parts[-1]
    return url


#: A URL embedded in free text. Quotes are excluded from the body because
#: DuckDB writes URLs inside single quotes -- ``HTTP GET error on '<url>'`` --
#: and the closing quote is not part of the URL.
_URL_IN_TEXT = re.compile(r"[A-Za-z][A-Za-z0-9+.\-]*://[^\s'\"<>]+")

#: A complete ANSI escape sequence, removed whole so no readable tail is left
#: behind when the introducing ESC goes.
_ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")

#: C0/C1 control characters, minus tab and newline, which error text legitimately
#: uses. ``\r`` is not spared: on a terminal it rewinds the line and can hide
#: what was printed before it.
_CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b-\x1f\x7f-\x9f]")


#: ``name=value`` / ``name: value`` credential pairs in free text. A server's
#: error body often echoes the request it rejected, ``api_key=...`` included,
#: and that text is not inside a URL, so the URL pass cannot see it.
_CREDENTIAL_PAIR = re.compile(
    r"(?i)\b((?:[a-z0-9_.-]*(?:api[_-]?key|apikey|token|secret|password|passwd|signature|"
    r"credential|authorization))(?:\s*[=:]\s*)|key=)"
    r"(?:(?:bearer|basic)\s+)?(?:\"[^\"]*\"|'[^']*'|[^\s&'\"<>,;]+)"
)


#: ``scheme://user:password@host`` userinfo. The password is replaced.
_URL_USERINFO = re.compile(r"(://[^/\s:@'\"]+:)[^/\s@'\"]+@")


def redact_credentials(text: str) -> str:
    """Replace credential values in free text, leaving everything else intact.

    The single redaction both the error path (`clean_error_text`)
    and the log path (``logging_config.warn`` / ``error``) use: ``name=value``
    / ``name: value`` credential pairs, and the password of ``scheme://user:pw@host``.
    URLs are otherwise untouched, so a log line still names the file it is about.
    """
    if not text:
        return text
    text = _URL_USERINFO.sub(r"\1***@", text)
    return _CREDENTIAL_PAIR.sub(lambda m: f"{m.group(1)}***", text)


def clean_error_text(text: str) -> str:
    """The one policy for text that reaches a user, a log line or a gpio error.

    Three things can ride into an error message from outside gpio, and none is
    safe to echo verbatim:

    * **Credentials in URLs.** DuckDB names the URL it failed on, query string and
      all -- ``HTTP GET error on 'https://bucket/x.parquet?X-Amz-Signature=...'``.
      The query string (where a presigned URL's signature and an Azure SAS ``sig=``
      ride) is removed; the rest of the URL is kept, because a gpio error that
      names the destination on purpose must not elide it.
    * **Echoed credential pairs.** ``api_key=...``, ``token: ...``,
      ``password=...``, ``user:pw@host`` in the text itself (a server body quoting
      the request) have their value replaced by ``***`` (:func:`redact_credentials`).
    * **Control characters.** DuckDB quotes the offending value back in a
      conversion or parse error, so a value a *file* chose reaches the terminal.
      ANSI escapes in it can rewrite the line, so an input could author a clean
      ``Error:`` line of its own choosing. They are stripped; tab and newline stay.
    """
    if not text:
        return text
    text = _URL_IN_TEXT.sub(lambda m: m.group(0).split("?")[0], text)
    text = redact_credentials(text)
    text = _ANSI_ESCAPE.sub("", text)
    return _CONTROL_CHARS.sub("", text)


class GeoParquetError(Exception):
    """Base exception for all geoparquet-io errors."""

    def __init__(self, message: str) -> None:
        # The one place a gpio error's text is cleaned: whatever a caller
        # interpolated into it (an httpx / OWSLib / DuckDB message that names
        # the request URL, credentials and all) is sanitized here, once, so
        # ``str(error)`` is safe for the CLI and the API alike (#1034).
        self.message = clean_error_text(message)
        super().__init__(self.message)


class FileNotFoundGeoParquetError(GeoParquetError):
    """Raised when a required file or path is not found."""

    def __init__(self, path: str, detail: str | None = None) -> None:
        self.path = path  # Original path for programmatic access
        msg = f"File not found: {path}"
        if detail:
            msg = f"{msg} - {detail}"
        super().__init__(msg)


class InvalidParameterError(GeoParquetError):
    """Raised when a function parameter has an invalid value."""

    def __init__(self, param_name: str, reason: str) -> None:
        self.param_name = param_name
        self.reason = clean_error_text(reason)
        super().__init__(f"Invalid parameter '{param_name}': {self.reason}")


class RemoteAccessError(GeoParquetError):
    """Raised when remote file access (S3/GCS/Azure) fails."""

    def __init__(self, url: str, reason: str) -> None:
        self.url = clean_error_text(url)
        self.reason = clean_error_text(reason)
        super().__init__(f"Remote access error for {self.url}: {self.reason}")


class GeometryError(GeoParquetError):
    """Raised when geometry column operations fail."""

    pass


class PartitionError(GeoParquetError):
    """Raised when partitioning operations fail.

    Attributes:
        result: The run this failure came out of, when there is one. A directory
            sub-partition run (`ops.sub_partition_by_*`) processes many files and
            raises only at the end, so the caller needs what succeeded as well as
            what failed (#811). ``None`` for a single-file partition failure,
            which has no partial run to report.
    """

    def __init__(self, message: str, result: dict | None = None) -> None:
        super().__init__(message)
        self.result = result


class ValidationError(GeoParquetError):
    """Raised when GeoParquet validation fails."""

    pass


# DuckDB's own text for "this extension is not built/published for your DuckDB".
# The HTTP status is the whole signal. Verified against DuckDB 1.5.5:
#
#   unpublished  HTTPException: 'HTTP Error: Failed to download extension
#                "geography" at URL "..." (HTTP 404)'
#   offline      IOException:   'IO Error: Failed to download extension
#                "..." at URL "..." (ERROR Could not establish connection)'
#
# Both say "Failed to download extension", so that phrase decides nothing -- it
# was matching an offline user's error and telling them to wait for an upstream
# republication that has nothing to do with their problem (#778). Only the 404
# means the registry answered and does not have this build.
#
# DuckDB 2.0 adds a second shape: the community a5 build downloads and loads,
# then its init refuses because a5 for 2.0+ is "distributed from the Query.Farm
# extension repository, not the DuckDB community repository". Equally not a
# network problem, and equally not published where gpio installs from.
_NOT_PUBLISHED_SIGNATURES = ("http 404", "not the duckdb community repository")


def is_unpublished_extension_error(exc: BaseException | str | None) -> bool:
    """True when `exc` carries DuckDB's "not published for this version" signature.

    Accepts an exception (whose ``__cause__``/``__context__`` chain is walked) or
    the raw error text. Deliberately does NOT match gpio's own wording, which is
    present on every :class:`ExtensionUnavailableError` regardless of cause.
    """
    if exc is None:
        return False
    if isinstance(exc, str):
        blob = exc.lower()
    else:
        parts: list[str] = []
        seen: set[int] = set()
        cur: BaseException | None = exc
        while cur is not None and id(cur) not in seen:
            seen.add(id(cur))
            parts.append(str(cur))
            cur = cur.__cause__ or cur.__context__
        blob = " ".join(parts).lower()
    return any(sig in blob for sig in _NOT_PUBLISHED_SIGNATURES)


# Extension-specific guidance, shown only when the extension really is
# unpublished. Only add an entry when we know something the generic message
# cannot say.
_UNPUBLISHED_EXTENSION_HINTS = {
    "a5": (
        "For DuckDB 2.0 and later, a5 moved to the Query.Farm extension "
        "repository, and gpio installs only from the DuckDB community repository. "
        "Use DuckDB 1.5 for the a5 commands."
    ),
    "geography": (
        "'geography' is published for gpio's whole DuckDB range on mainstream "
        "platforms (Linux/macOS amd64+arm64, Windows amd64), so a 404 here "
        "means a platform the registry carries no community builds for "
        "(Windows ARM, musl/Alpine, a source-built DuckDB) or a proxy "
        "answering in the registry's place. For a hierarchical cell index "
        "meanwhile, use `gpio add a5` or `gpio partition a5`."
    ),
}


#: The extension name inside DuckDB's download failure, e.g. ``Failed to
#: download extension "geography" at URL "..." (HTTP 404)``.
_EXTENSION_NAME_IN_ERROR = re.compile(r'extension\s+"([^"]+)"')


def unpublished_extension_hint(exc: BaseException | str | None) -> str | None:
    """gpio's guidance for DuckDB's "not published for this version" 404, or ``None``.

    The same shape as
    :func:`~geoparquet_io.core.duckdb_utils.spill_space_hint`: a paragraph to
    append to DuckDB's own message, or ``None`` when this is not that failure.

    It exists because the 404 arrives as an ``HTTPException``, which hangs below
    ``IOException`` and so is otherwise classified as "the input file is
    unreachable" -- true of the extension registry, useless to a user who named
    a perfectly good file. :class:`ExtensionUnavailableError` already says the
    right thing wherever the ``INSTALL`` is wrapped, but three sites are not
    (``core/duckdb_utils._install_and_load_extension``,
    ``core/extract_bigquery``, ``core/partition/admin_hierarchical``), and their
    failures reach the CLI raw.
    """
    if not is_unpublished_extension_error(exc):
        return None
    match = _EXTENSION_NAME_IN_ERROR.search(exc if isinstance(exc, str) else str(exc))
    if match is None:
        return None
    name = match.group(1)
    listing = f"https://community-extensions.duckdb.org/extensions/{name}.html"
    hint = (
        f"This is the extension registry answering, not your input file: community "
        f"extensions are built per DuckDB release, and '{name}' is not published for "
        f"this one (see {listing})."
    )
    extra = _UNPUBLISHED_EXTENSION_HINTS.get(name)
    return f"{hint} {extra}" if extra else hint


class ExtensionUnavailableError(GeoParquetError):
    """Raised when a required DuckDB community extension cannot be installed/loaded.

    Community extensions (e.g. ``geography`` for S2 support) are built per
    DuckDB release. When a new DuckDB version ships before an extension has
    been rebuilt for it, ``INSTALL ... FROM community`` fails with an opaque
    HTTP 404. This exception surfaces an actionable message instead.
    """

    def __init__(
        self,
        name: str,
        duckdb_version: str,
        detail: str | None = None,
        feature: str | None = None,
    ) -> None:
        self.name = name
        self.duckdb_version = duckdb_version
        self.feature = feature
        self.unpublished = is_unpublished_extension_error(detail)
        subject = f"'{feature}' requires" if feature else "This operation requires"
        listing = f"https://community-extensions.duckdb.org/extensions/{name}.html"
        msg = (
            f"{subject} the DuckDB community extension '{name}', which could not "
            f"be loaded for DuckDB {duckdb_version}."
        )
        if self.unpublished:
            # DuckDB said "not published for this version" outright.
            msg += (
                f" Community extensions are built per DuckDB release, and '{name}' "
                f"is not published for this one (see {listing})."
            )
            hint = _UNPUBLISHED_EXTENSION_HINTS.get(name)
            if hint:
                msg = f"{msg} {hint}"
        elif detail:
            # A failure that is NOT a 404: do not blame the extension registry.
            msg += (
                " This is not the 'not published for this DuckDB version' 404, so "
                "check that https://community-extensions.duckdb.org is reachable "
                "from here (proxy, firewall, offline)."
            )
        else:
            # No underlying error to reason from: state the possibility, claim nothing.
            msg += (
                f" Community extensions are built per DuckDB release, so '{name}' "
                f"may not be published for this version yet (see {listing})."
            )
        if detail:
            msg = f"{msg} Original error: {detail}"
        super().__init__(msg)


class OptionalDependencyError(GeoParquetError):
    """Raised when an optional Python package a command needs is not installed.

    The commands backed by optional extras (``simplify``, ``raster`` — see
    ADR-0007) import their packages lazily through
    :mod:`geoparquet_io.core.optional_deps`, so ``import geoparquet_io`` and
    every other command keep working without them. The message carries the
    exact install commands for the missing package.
    """


class BatchTooLargeError(GeoParquetError):
    """Raised when a server refused a page of features because of its size.

    This typically happens when a server's actual payload limit is lower than
    its advertised maxRecordCount: the page comes back as a non-JSON body, as
    an ArcGIS JSON error envelope (code 500, "Error performing query
    operation"), or as a 5xx that survives every same-size retry. The caller
    should retry the same offset with a smaller batch.
    """

    def __init__(self, url: str, batch_size: int, reason: str) -> None:
        self.url = clean_error_text(url)
        self.batch_size = batch_size
        self.reason = clean_error_text(reason)
        super().__init__(f"Batch size {batch_size} too large for {self.url}: {self.reason}")


# ---------------------------------------------------------------------------
# Failure classification: one table, keyed on exception type and structured
# fields (#1020, #1034). The message of an HTTP or DuckDB failure embeds the
# request URL, which embeds the user's own layer name, filter and API key, so a
# substring test on it lets input decide the verdict. Text is read in two bounded
# places only (see `classify_failure`). Every service and the CLI ask here.
# ---------------------------------------------------------------------------


class FailureKind(enum.Enum):
    """What kind of failure a caught exception was."""

    #: Happened on this machine (memory, interrupt, a bad query of our own);
    #: no response exists to classify and retrying cannot help.
    LOCAL = "local"
    #: The request timed out.
    TIMEOUT = "timeout"
    #: No connection was made: refused, DNS, reset, protocol error.
    UNREACHABLE = "unreachable"
    #: The server answered with an HTTP status (see ``FailureClass.status``).
    HTTP_STATUS = "http_status"
    #: The server answered but the body could not be parsed.
    MALFORMED_RESPONSE = "malformed_response"
    #: The file is not Parquet at all (wrong format, truncated, no magic bytes).
    NOT_PARQUET = "not_parquet"
    #: Not recognised; the caller keeps its generic message.
    UNKNOWN = "unknown"


class FailureClass(NamedTuple):
    """The verdict of :func:`classify_failure`."""

    kind: FailureKind
    status: int | None = None


def _status_of(exc: BaseException) -> int | None:
    """The HTTP status a library exception carries as a structured field.

    ``duckdb.HTTPException.status_code`` reports 0 when the request produced no
    status of its own (some auth failures do this), so 0 reads as "none".
    """
    response = getattr(exc, "response", None)
    code = getattr(response, "status_code", None)
    if code is None:
        code = getattr(exc, "status_code", None)
    if isinstance(code, int) and not isinstance(code, bool) and code > 0:
        return code
    return None


#: The phrases DuckDB and pyarrow use for "this is not a Parquet file". Neither
#: library gives a structured field for it, so this one predicate reads the text;
#: every "is it Parquet" question in gpio asks :func:`classify_failure` for
#: :attr:`FailureKind.NOT_PARQUET` instead of keeping its own copy.
_NOT_PARQUET_TEXT = re.compile(
    r"no magic bytes found|not a parquet file|magic bytes not found|too small to be a parquet"
    r"|smaller than the minimum file footer",
    re.IGNORECASE,
)

#: A quoted run or a URL: user-chosen text inside a DuckDB message (the path, the
#: layer, the SQL). Removed before :func:`_duckdb_io_verdict` reads the rest, so
#: a file or table *named* "forbidden" cannot decide the verdict (#1020).
_USER_TEXT_IN_MESSAGE = re.compile(r"'[^']*'|\"[^\"]*\"|" + _URL_IN_TEXT.pattern)
_HTTP_STATUS_TEXT = re.compile(r"\(HTTP (\d{3})\)|\bHTTP(?: error)?[ :]+(\d{3})\b", re.IGNORECASE)


def _duckdb_io_verdict(exc: BaseException) -> FailureClass | None:
    """DuckDB's remote-read failure, read from its text -- the one place that is done.

    A remote ``read_parquet`` fails as a plain ``IOException`` whose only account
    of what happened is its message ("... (HTTP 403)", "Timeout was reached").
    That is the one place gpio reads exception text for an HTTP verdict, it is
    confined to DuckDB's IO exception types, and the user's own path/URL/SQL is
    cut out of the text first.
    """
    import duckdb

    if not isinstance(exc, (duckdb.IOException, duckdb.HTTPException)):
        return None
    text = _USER_TEXT_IN_MESSAGE.sub(" ", str(exc))
    match = _HTTP_STATUS_TEXT.search(text)
    if match:
        return FailureClass(FailureKind.HTTP_STATUS, int(match.group(1) or match.group(2)))
    lowered = text.lower()
    if "unauthorized" in lowered:
        return FailureClass(FailureKind.HTTP_STATUS, 401)
    if "forbidden" in lowered or "access denied" in lowered:
        return FailureClass(FailureKind.HTTP_STATUS, 403)
    if "not found" in lowered or "does not exist" in lowered:
        return FailureClass(FailureKind.HTTP_STATUS, 404)
    if "timeout" in lowered or "timed out" in lowered:
        return FailureClass(FailureKind.TIMEOUT)
    if "unable to connect" in lowered or "connection" in lowered:
        return FailureClass(FailureKind.UNREACHABLE)
    return None


def _is_not_parquet(exc: BaseException) -> bool:
    import duckdb
    import pyarrow as pa

    return isinstance(exc, (duckdb.Error, pa.ArrowException, OSError)) and bool(
        _NOT_PARQUET_TEXT.search(str(exc))
    )


def _classify_one(exc: BaseException) -> FailureClass:
    import duckdb

    if isinstance(exc, (MemoryError, KeyboardInterrupt, SystemExit)):
        return FailureClass(FailureKind.LOCAL)
    if _is_not_parquet(exc):
        return FailureClass(FailureKind.NOT_PARQUET)
    if isinstance(exc, _local_duckdb_errors(duckdb)):
        return FailureClass(FailureKind.LOCAL)

    status = _status_of(exc)
    if status is not None:
        return FailureClass(FailureKind.HTTP_STATUS, status)

    # httpx / requests are matched by class lineage, not imported: this module
    # classifies their errors, it is not an HTTP client.
    if (
        isinstance(exc, TimeoutError)
        or _derives_from(exc, "httpx", "TimeoutException")
        or (_derives_from(exc, "requests", "Timeout"))
    ):
        return FailureClass(FailureKind.TIMEOUT)
    if (
        isinstance(exc, (ConnectionError, socket.gaierror))
        or _derives_from(exc, "httpx", "TransportError")
        or _derives_from(exc, "requests", "ConnectionError")
    ):
        return FailureClass(FailureKind.UNREACHABLE)
    if isinstance(exc, _MALFORMED_RESPONSE_TYPES) or _derives_from(exc, "lxml", "XMLSyntaxError"):
        return FailureClass(FailureKind.MALFORMED_RESPONSE)
    verdict = _duckdb_io_verdict(exc)
    if verdict is not None:
        return verdict
    return FailureClass(FailureKind.UNKNOWN)


def _derives_from(exc: BaseException, package: str, class_name: str) -> bool:
    """Whether ``exc`` is ``<package>...<class_name>`` or a subclass, by name, without importing it."""
    return any(
        base.__name__ == class_name and base.__module__.split(".")[0] == package
        for base in type(exc).__mro__
    )


def _local_duckdb_errors(duckdb) -> tuple[type[BaseException], ...]:
    return (
        duckdb.OutOfMemoryException,
        duckdb.InterruptException,
        duckdb.BinderException,
        duckdb.CatalogException,
        duckdb.ParserException,
        duckdb.InvalidInputException,
    )


_MALFORMED_RESPONSE_TYPES: tuple[type[BaseException], ...] = (
    xml.etree.ElementTree.ParseError,
    json.JSONDecodeError,
    UnicodeDecodeError,
)


def classify_failure(exc: BaseException, *, follow_chain: bool = True) -> FailureClass:
    """Classify a caught exception by its type and structured fields.

    Unless ``follow_chain=False``, the exception and its ``__cause__`` / ``__context__`` chain are walked
    and the first recognised link decides, so a library error wrapped by
    another still classifies. Message text is read in two bounded places only:
    the Parquet-magic phrases (:attr:`FailureKind.NOT_PARQUET`) and the
    status/timeout wording of a DuckDB IO failure, which has no structured field
    (see :func:`_duckdb_io_verdict`; the user's own path, URL and SQL are cut
    out first, so the same exception classifies the same way everywhere).
    """
    seen: set[int] = set()
    cur: BaseException | None = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        verdict = _classify_one(cur)
        if verdict.kind is not FailureKind.UNKNOWN:
            return verdict
        cur = (cur.__cause__ or cur.__context__) if follow_chain else None
    return FailureClass(FailureKind.UNKNOWN)


#: The statuses that are the *caller's* problem and never worth a retry, and
#: the sentence each one gets. One mapping for WFS, ArcGIS and Carto: they used
#: to carry three, each with its own wording. Lives here (not in ``http_retry``)
#: because it classifies a status into gpio's vocabulary; ``http_retry`` only
#: decides whether to try again.
FATAL_STATUS_MESSAGES: dict[int, str] = {
    401: "Authentication required for {subject}. {credentials}",
    403: "Access denied to {subject}. {credentials}",
    404: "{subject} not found (404). Check the name and the URL.",
}


def is_auth_status(status: int | None) -> TypeGuard[int]:
    """Whether ``status`` means "this needs a credential" (401 or 403).

    ``FailureClass.status`` is only ever set for an HTTP status, so a verdict's
    status can be passed as is.
    """
    return status in (401, 403)


def fatal_status_message(status: int, *, subject: str, credentials: str) -> str | None:
    """The message for a 401/403/404, or ``None`` for any other status.

    Args:
        status: HTTP status the server answered with.
        subject: What was asked for, e.g. ``"the WFS service"`` or
            ``"table 'cities'"``; its first letter is capitalized when it
            opens the 404 sentence.
        credentials: The service-specific remedy for 401/403 (which flag or
            variable carries the credential).
    """
    template = FATAL_STATUS_MESSAGES.get(status)
    if template is None:
        return None
    if status == 404:
        subject = subject[:1].upper() + subject[1:]
    return template.format(subject=subject, credentials=credentials).strip()


def status_message(status: int, *, subject: str, credentials: str, reason: str = "") -> str:
    """The sentence for any HTTP status: the fatal one, else ``HTTP error <n> <reason> for <subject>``.

    The one wording every service shares; ``reason`` is the response's reason
    phrase (never ``str(exc)``, which spells out the full request URL).
    """
    return fatal_status_message(status, subject=subject, credentials=credentials) or (
        f"HTTP error {status} {reason}".rstrip() + f" for {subject}"
    )


def not_parquet_file_error(path: str | None) -> GeoParquetError:
    """The one answer to "this file is not Parquet", naming the file.

    Every site that finds out (``duckdb_metadata``'s readers, ``inspect``,
    ``check``) raises or returns this, so the wording is one, not one per place.
    """
    subject = f"The file '{path}'" if path else "This file"
    if not path or not path.lower().endswith(".parquet"):
        return GeoParquetError(
            f"{subject} is not a Parquet file; this command reads .parquet files. "
            "Use 'gpio convert' to create a proper GeoParquet file."
        )
    return GeoParquetError(
        f"{subject} has a .parquet extension but is not a valid "
        f"Parquet file. It may be a different format (CSV, JSON, etc.) that was "
        f"incorrectly named. Check the file contents or use 'gpio convert' to "
        f"create a proper GeoParquet file."
    )
