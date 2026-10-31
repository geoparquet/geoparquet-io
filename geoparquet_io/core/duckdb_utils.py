"""
DuckDB connection management utilities.

This module provides functions for creating and managing DuckDB connections
with appropriate extensions loaded for GeoParquet operations.

``import duckdb`` is deferred into the functions that touch it (#1307): the
embeddable core modules (see docs/guide/embedding.md) import this module for
``quote_identifier``/``sql_literal``/``sql_path``-style helpers, and a
host application without DuckDB installed must be able to import them and fail
only when a connection is actually asked for.
"""

from __future__ import annotations

import atexit
import datetime
import errno
import functools
import inspect
import itertools
import json
import os
import re
import shutil
import tempfile
import threading
import uuid
import weakref
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, BinaryIO, TypeVar

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa

from geoparquet_io.core.edges import (
    PLANAR,
    geoarrow_edge_type,
    geoarrow_type_edges,
    is_geodesic,
    without_edges,
)
from geoparquet_io.core.exceptions import ExtensionUnavailableError, ValidationError
from geoparquet_io.core.logging_config import debug, warn
from geoparquet_io.core.parquet_schema import root_schema_columns
from geoparquet_io.core.remote import is_s3_url, needs_httpfs
from geoparquet_io.core.sizing import nudged_size

# Per-bucket cache for S3 buckets that require authentication
# Buckets not in this set are accessed without credentials (works for public buckets)
_s3_cache_lock = threading.Lock()
_s3_buckets_needing_auth: set[str] = set()

# Ambient S3 config — set once at CLI/API boundary via s3_config_scope(),
# automatically picked up by get_duckdb_connection().
# Uses ContextVar for proper isolation across concurrent async/threaded scopes.
_active_s3_config: ContextVar[dict | None] = ContextVar("_active_s3_config", default=None)


@contextmanager
def s3_config_scope(s3_config: dict):
    """Set ambient S3 config for all get_duckdb_connection() calls within this scope.

    Thread-safe and async-safe via contextvars. Nested scopes merge configs,
    with inner scopes taking precedence. Config is automatically restored on exit.
    """
    current = _active_s3_config.get() or {}
    merged = {**current, **s3_config}
    token = _active_s3_config.set(merged)
    try:
        yield
    finally:
        _active_s3_config.reset(token)


_F = TypeVar("_F", bound=Callable[..., Any])


def uses_aws_profile(func: _F) -> _F:
    """Run ``func`` with its ``profile`` (or ``write.profile``) as the ambient AWS profile.

    The profile goes into :func:`s3_config_scope`, which DuckDB's
    ``credential_chain`` secret and the obstore store read explicitly. Nothing is
    written to ``os.environ``, so a call with ``profile="prod"`` cannot change
    the credentials of a later call (#1055). A ``None`` profile leaves any outer
    scope as it is, and so does a call with no S3 URL among its arguments: an
    AWS profile means nothing to a local, https, gs or az path, and scoping it
    there would only load the ``aws`` extension for nothing.

    The profile counts as explicit, so it wins over env keys (see
    :func:`_resolve_s3_settings`). The scope is a ContextVar, so a worker thread
    started inside ``func`` does not see it. Copy the context
    (``contextvars.copy_context().run``) if a worker opens an S3 connection.
    """
    signature = inspect.signature(func)
    if "profile" not in signature.parameters and "write" not in signature.parameters:
        raise TypeError(f"{func.__qualname__} has no 'profile' or 'write' parameter")

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        arguments = signature.bind_partial(*args, **kwargs).arguments
        # A driver that takes the write options (parquet_writer.WriteOptions)
        # carries the profile there instead of in a parameter of its own.
        profile = arguments.get("profile") or getattr(arguments.get("write"), "profile", None)
        if not profile or not _any_s3_url(arguments.values()):
            return func(*args, **kwargs)
        with s3_config_scope({"profile": profile, "profile_from_env": False}):
            return func(*args, **kwargs)

    return wrapper  # type: ignore[return-value]


def _any_s3_url(values: Iterable[Any]) -> bool:
    """Whether any value, or any item of a list/tuple value, is an S3 URL."""
    for value in values:
        items = value if isinstance(value, (list, tuple)) else (value,)
        if any(is_s3_url(item) for item in items):
            return True
    return False


# The providers a secret with a named PROFILE tries. duckdb-aws reads PROFILE
# without CHAIN as "the config-file provider only" (aws_secret.cpp), which drops
# SSO and credential_process profiles that the SDK default chain plus
# AWS_PROFILE used to resolve. Naming them keeps those working. env is left out
# on purpose: only an explicit profile (--aws-profile, profile=) gets here, and
# the AWS CLI lets an explicit --profile win over env keys. An exported
# AWS_PROFILE does not get here (see _resolve_s3_settings), so env keys still
# beat it, as they do in the AWS SDK.
#
# Known limit: for an assume-role profile (role_arn plus source_profile),
# duckdb-aws signs the STS AssumeRole call with the SDK default chain, not the
# source_profile keys. No CHAIN value changes that today.
_NAMED_PROFILE_CHAIN = "config;process;sso"


def _resolve_s3_settings(
    s3_endpoint: str | None, s3_region: str | None, s3_use_ssl: bool | None
) -> dict:
    """The S3 settings a connection gets: explicit kwargs over the ambient scope.

    The one place they are resolved. Both the ``SET s3_*`` statements and the
    ``credential_chain`` secret are built from this dict, so they cannot
    disagree: DuckDB reads a secret's endpoint and region over ``SET``, so a
    secret built from anything else silently wins.

    ``profile`` is only an explicit one. A profile that is just the exported
    ``AWS_PROFILE`` is left to the SDK default chain, which reads it itself
    after env keys, so the secret keeps the AWS SDK order and main's behavior.
    """
    ambient = _active_s3_config.get() or {}
    explicit_profile = None if ambient.get("profile_from_env") else ambient.get("profile")
    return {
        "profile": explicit_profile,
        "endpoint": s3_endpoint if s3_endpoint is not None else ambient.get("s3_endpoint"),
        "region": s3_region if s3_region is not None else ambient.get("s3_region"),
        "use_ssl": s3_use_ssl if s3_use_ssl is not None else ambient.get("s3_use_ssl"),
    }


def _s3_secret_sql(settings: Mapping[str, Any]) -> str:
    """The ``CREATE SECRET`` statement for the AWS credential chain.

    ``settings`` is the dict from :func:`_resolve_s3_settings`. The secret
    carries the same endpoint, URL style, SSL flag and region the ``SET``
    statements apply, because a ``credential_chain`` secret otherwise defaults
    its endpoint to ``s3.amazonaws.com`` and its region to the profile's, and
    both beat ``SET``. A region left unset is still filled by DuckDB from
    ``s3_region``, ``AWS_REGION`` or the profile, in that order.
    """
    params = ["TYPE s3", "PROVIDER credential_chain", "VALIDATION 'none'"]
    profile = settings.get("profile")
    if profile:
        params.append(f"CHAIN '{_NAMED_PROFILE_CHAIN}'")
        params.append(f"PROFILE {sql_literal(profile)}")
    endpoint = settings.get("endpoint")
    if endpoint:
        params.append(f"ENDPOINT {sql_literal(endpoint)}")
        params.append("URL_STYLE 'path'")
        params.append(f"USE_SSL {'false' if settings.get('use_ssl') is False else 'true'}")
    region = settings.get("region")
    if region:
        params.append(f"REGION {sql_literal(region)}")
    body = ",\n    ".join(params)
    return f"CREATE OR REPLACE SECRET (\n    {body}\n);"


def _create_s3_secret(con, settings: Mapping[str, Any], *, required: bool) -> None:
    """Load ``aws`` and create the credential chain secret on ``con``.

    ``required`` is the caller's ``use_s3_auth``: a private bucket read needs
    the secret, so a missing ``aws`` extension is an error. A secret that only
    an ambient profile asked for is best effort. The connection may never read
    S3 at all (gs, az), and an offline machine must not fail on ``INSTALL aws``.
    """
    try:
        _install_and_load_extension(con, "aws")
    except Exception as e:
        if required:
            raise
        debug(f"Skipping the AWS profile secret, the aws extension is unavailable: {e}")
        return
    con.execute(_s3_secret_sql(settings))


def _apply_s3_settings(con, settings: Mapping[str, Any]) -> None:
    """``SET`` the resolved endpoint and region for reads that use no secret."""
    endpoint = settings.get("endpoint")
    if endpoint:
        con.execute(f"SET s3_endpoint={sql_literal(endpoint)};")
        con.execute("SET s3_url_style='path';")
        ssl_value = "false" if settings.get("use_ssl") is False else "true"
        con.execute(f"SET s3_use_ssl={ssl_value};")

    region = settings.get("region")
    if region:
        con.execute(f"SET s3_region={sql_literal(region)};")


def get_active_s3_config() -> dict:
    """Return the ambient S3 config set by :func:`s3_config_scope`, or ``{}``.

    Lets a non-DuckDB remote path (obstore in ``file_utils.copy_file``) read the
    same ``--s3-endpoint``/``--s3-region``/``--s3-no-ssl``/``--aws-profile``
    settings ``get_duckdb_connection()`` applies, rather than building a second
    channel or falling back to ambient credentials (#810).
    """
    return dict(_active_s3_config.get() or {})


def _extract_bucket_name(path: str) -> str:
    """Extract bucket name from S3 URL."""
    # s3://bucket-name/path -> bucket-name
    path_without_protocol = path.split("://", 1)[1]
    return path_without_protocol.split("/")[0]


def _needs_s3_auth(exception: Exception) -> bool:
    """Detect if exception indicates S3 bucket requires authentication."""
    from geoparquet_io.core.exceptions import classify_failure, is_auth_status

    # 401/403 without credentials means we need to authenticate
    return is_auth_status(classify_failure(exception).status)


def _add_bucket_needing_auth(bucket: str) -> None:
    """Thread-safe add to S3 auth cache."""
    with _s3_cache_lock:
        _s3_buckets_needing_auth.add(bucket)


def _bucket_needs_auth(bucket: str) -> bool:
    """Thread-safe check if bucket requires authentication."""
    with _s3_cache_lock:
        return bucket in _s3_buckets_needing_auth


def _clear_s3_cache() -> None:
    """Clear S3 access cache (useful for testing)."""
    with _s3_cache_lock:
        _s3_buckets_needing_auth.clear()


def _escape_sql_string(value: str) -> str:
    """
    Escape single quotes for SQL string literals.

    SQL standard escaping: single quotes are doubled ('→'').
    This prevents SQL injection when interpolating user-provided
    strings (like file paths) into SQL queries.

    Args:
        value: The string value to escape

    Returns:
        String with single quotes escaped for safe SQL interpolation
    """
    return value.replace("'", "''")


def _require_str(value: object) -> str:
    """``value`` unchanged if it is a ``str``; a ``TypeError`` otherwise.

    For the literal builders whose input must already be text: ``sql_literal``
    renders anything through ``str()``, which would turn a ``bytes`` payload
    into the characters ``b'...'`` instead of failing.
    """
    if not isinstance(value, str):
        raise TypeError(f"expected str, got {type(value).__name__}")
    return value


def sql_literal(value: object) -> str:
    """Render a Python value as a DuckDB SQL literal -- the one value-literal builder.

    - ``bool`` -> ``TRUE``/``FALSE``; ``int`` -> its digits.
    - ``float`` -> an explicit DOUBLE: a bare ``0.699999988079071`` is typed
      DECIMAL by DuckDB and never equals the REAL column it came from, and
      ``nan``/``inf`` need the cast to be literals at all.
    - anything else -> ``str(value)`` as a single-quoted string, escaped by
      :func:`_escape_sql_string`. A NUL byte is spliced in as ``chr(0)``:
      DuckDB 1.5's lexer ends a quoted string at a NUL ("unterminated quoted
      string"), while 2.0 accepts it; the splice reads the same on both.
    """
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"CAST('{value!r}' AS DOUBLE)"
    text = _escape_sql_string(str(value))
    if "\x00" in text:
        return " || chr(0) || ".join(f"'{piece}'" for piece in text.split("\x00"))
    return f"'{text}'"


_SETTING_KEY_RE = re.compile(r"[a-z_]+")


def restore_duckdb_settings(
    con: duckdb.DuckDBPyConnection, saved: Mapping[str, object], verbose: bool = False
) -> None:
    """Put back session settings a write overrode, as ``current_setting`` read them.

    The connection belongs to the caller, not to the write: partition loops
    finalize N files on one connection, and the Python API holds a connection
    across operations, so a setting left behind throttles everything after it.
    A value is restored exactly when DuckDB's display of it round-trips; a
    displayed size that does not was either the engine default (RESET) or one
    the caller set (re-SET half a displayed step up, which reads back the same).

    ``saved`` keys are setting names from gpio's own code, never user input.
    """
    import duckdb

    for key, value in saved.items():
        if not _SETTING_KEY_RE.fullmatch(key):
            raise ValueError(f"Not a DuckDB setting name: {key!r}")
        try:
            _set_setting(con, key, value)
            if _current_setting(con, key) == value:
                continue
            con.execute(f"RESET {key}")
            nudged = nudged_size(value) if isinstance(value, str) else None
            if _current_setting(con, key) != value and nudged:
                _set_setting(con, key, nudged)
        except duckdb.Error as e:  # pragma: no cover - defensive
            if verbose:
                debug(f"Could not restore DuckDB setting {key}: {e}")


# What DuckDB's Python client raises when a SIGINT stops a running query
# (measured on 1.5.6). It is a plain RuntimeError, not an InterruptException.
_QUERY_INTERRUPTED = "Query interrupted"


def is_interrupt(exc: BaseException) -> bool:
    """Whether ``exc`` is the user stopping the run (Ctrl-C), not a failure.

    A guard that turns errors into verdicts must re-raise these. DuckDB
    reports a SIGINT during a query as ``RuntimeError("Query interrupted")``,
    which ``except Exception`` would otherwise swallow and carry on past.

    The cause and context chain is searched too, since a caller that wraps
    errors (``apply_all_fixes`` raises ``GeoParquetError(...) from e``) would
    otherwise hide the interrupt. The walk is bounded and stops on a cycle.
    """
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen and len(seen) < _MAX_CHAIN:
        if _is_interrupt_itself(current):
            return True
        seen.add(id(current))
        current = current.__cause__ or current.__context__
    return False


_MAX_CHAIN = 32


def _is_interrupt_itself(exc: BaseException) -> bool:
    # KeyboardInterrupt first: this always runs inside an exception handler,
    # and in a duckdb-less vendoring host the import below would mask the
    # interrupt with a ModuleNotFoundError.
    if isinstance(exc, KeyboardInterrupt):
        return True
    try:
        import duckdb
    except ModuleNotFoundError:
        return isinstance(exc, RuntimeError) and str(exc) == _QUERY_INTERRUPTED

    if isinstance(exc, duckdb.InterruptException):
        return True
    return isinstance(exc, (RuntimeError, duckdb.Error)) and str(exc) == _QUERY_INTERRUPTED


def disable_geoparquet_conversion(con: duckdb.DuckDBPyConnection) -> None:
    """Read a GeoParquet WKB column as the BLOB it stores, not as GEOMETRY.

    With conversion on, DuckDB presents a WKB column as GEOMETRY and decodes
    each value only when something reads it, so a check that counts values
    passes bytes it never looked at, and the first query that does decode one
    fails on a zero-length or malformed value with no row to point at (#1082).
    A checker owns its connection and wants the bytes, so it turns this off.
    Columns stored with the native Parquet GEOMETRY type stay GEOMETRY.
    """
    con.execute("SET enable_geoparquet_conversion = false")


_VERSION_PARTS = re.compile(r"v?(\d+)(?:\.(\d+))?")


def _parse_duckdb_version(version: str) -> tuple[int | None, int | None]:
    """``(major, minor)`` from a DuckDB version string, ``None`` where unreadable.

    Accepts ``"2.0.0"``, ``"2"``, ``"2.0.0-dev123"`` and ``"v2.0.0"``; anything
    that does not start with a number (``"unknown"``, ``""``) is ``(None, None)``.
    """
    match = _VERSION_PARTS.match(version.strip()) if isinstance(version, str) else None
    if match is None:
        return None, None
    major, minor = match.groups()
    return int(major), (int(minor) if minor is not None else None)


@dataclass(frozen=True)
class DuckDBCapabilities:
    """What the installed DuckDB can do, where 1.5 and 2.0 differ.

    Built once by :func:`duckdb_capabilities`; every version-dependent branch
    in gpio reads a field here instead of ``duckdb.__version__`` (#1367). An
    unreadable version leaves ``major``/``minor`` ``None`` and every 2.0
    feature ``False``: the 1.5 code paths are the conservative ones.

    Attributes:
        version: ``duckdb.__version__`` as installed (reported in
            :class:`ExtensionUnavailableError` and the benchmark environment
            block, ``core/benchmark.py:get_environment_info``).
        major, minor: Parsed from ``version``; ``None`` when not numeric.
        is_v2: ``major >= 2``.
        core_geometry: DuckDB's core ships GEOMETRY and the serialize /
            deserialize functions, so read-only inspect paths skip the spatial
            extension (``get_duckdb_connection(load_spatial=None)``,
            ``inspect_utils`` previews, #1353).
        native_geojson_writer: ``COPY ... (FORMAT GEOJSON)`` exists
            (the native fast path in ``geojson_stream``, #1349).
        a5_from_query_farm: the community ``a5`` is a stub that points at the
            Query.Farm repository, which publishes the real build for 2.0+
            (the routing open PR #1360 adds to ``load_community_extension``).
            No reader yet by design: it is the hook #1360 is retargeted onto
            once this lands, so that PR reads the owner instead of the version.
    """

    version: str
    major: int | None
    minor: int | None
    is_v2: bool
    core_geometry: bool
    native_geojson_writer: bool
    a5_from_query_farm: bool


def _capabilities_for(version: str) -> DuckDBCapabilities:
    """The capabilities a given version string implies (pure; no DuckDB call)."""
    major, minor = _parse_duckdb_version(version)
    is_v2 = major is not None and major >= 2
    return DuckDBCapabilities(
        version=version,
        major=major,
        minor=minor,
        is_v2=is_v2,
        core_geometry=is_v2,
        native_geojson_writer=is_v2,
        a5_from_query_farm=is_v2,
    )


@functools.cache
def duckdb_capabilities() -> DuckDBCapabilities:
    """The installed DuckDB's :class:`DuckDBCapabilities`, computed once per process.

    The one reader of ``duckdb.__version__`` in gpio (``tests/
    test_duckdb_capabilities.py`` fails on any other). Test hook: a test uses
    ``pretend_duckdb_capabilities`` / ``pretend_duckdb_version`` from
    ``tests/test_duckdb_capabilities.py``, which patch ``_capabilities_for`` and
    call ``duckdb_capabilities.cache_clear()`` around the patch; it never
    patches ``duckdb.__version__``.
    """
    import duckdb

    return _capabilities_for(str(duckdb.__version__))


def complete_without_fetch(result) -> None:
    """Close an open ``execute()`` result without fetching a row from it.

    On DuckDB 2.0 a result stays open until a fetch returns no rows, blocking
    ``sql()``/``register()`` on the connection; ``complete()`` runs the rest
    of the statement to completion (keeping a write's effect, unlike
    ``abort()``) and discards the rows unconverted, so a probe whose rows
    cannot be fetched (a GEOMETRY that fails to serialize on 1.5+) still
    leaves nothing open. DuckDB 1.5 has no such method and needs none: the
    next statement replaces the result there.

    Decided per object, and this is the only place that asks: :func:`fetch_one`
    (the result API guard's required spelling for a one-row read) is also
    handed ``sqlite3`` cursors -- GeoPackage metadata in ``format_writers`` --
    which have no ``complete()`` on any DuckDB, so a client-wide answer would
    make every such read raise on 2.0. (A ``result_complete`` capability field
    was tried and removed for exactly that reason.)
    """
    complete = getattr(result, "complete", None)
    if complete is not None:
        complete()


def resolve_load_spatial(load_spatial: bool | None) -> bool:
    """Whether a connection opened with ``load_spatial`` gets the spatial extension.

    ``None`` is the auto mode: spatial only where DuckDB's own core lacks
    GEOMETRY (``core_geometry``, #1353). :func:`get_duckdb_connection` decides
    through this, and a caller that has to know what its auto-mode connection
    carries resolves it here once and opens with the answer.
    """
    if load_spatial is None:
        return not duckdb_capabilities().core_geometry
    return load_spatial


def fetch_one(result):
    """``result.fetchone()`` for an ``execute()`` result, leaving nothing open.

    On DuckDB 2.0 a result stays open until a fetch returns no rows, and while
    it is open ``sql()``, ``register()``, ``create_function()`` and any
    relation read on the connection raise ``ResourceInUseException``. A single
    ``fetchone()`` on a one-row query (a count, a ``LIMIT 1``, a setting) gets
    its row and never sees the end, so it leaves exactly that open result.

    After a row comes back, the rest is run to completion with ``complete()``,
    not cancelled with ``abort()``: ``abort()`` fails an open transaction when
    the statement may write, and a ``COPY`` or ``INSERT ... RETURNING`` read
    for its first row must keep its effect. DuckDB 1.5 has neither method
    (the next statement replaced the result there), so nothing is called.
    """
    row = result.fetchone()
    if row is not None:
        complete_without_fetch(result)
    return row


def _current_setting(con: duckdb.DuckDBPyConnection, key: str) -> object:
    return fetch_one(con.execute(f"SELECT current_setting('{key}')"))[0]


def _set_setting(con: duckdb.DuckDBPyConnection, key: str, value: object) -> None:
    if isinstance(value, str):
        con.execute(f"SET {key} = {sql_literal(value)}")
    else:
        con.execute(f"SET {key} = {value}")


def pin_duckdb_settings(con: duckdb.DuckDBPyConnection, settings: Mapping[str, int | bool]) -> None:
    """SET settings a write needs for its duration; the caller restores them.

    The pair of :func:`restore_duckdb_settings`: ``settings`` keys are names from
    gpio's own code, never user input.
    """
    for key, value in settings.items():
        if not _SETTING_KEY_RE.fullmatch(key):
            raise ValueError(f"Not a DuckDB setting name: {key!r}")
        _set_setting(con, key, str(value).lower() if isinstance(value, bool) else int(value))


def sql_path(path: str | os.PathLike[str]) -> str:
    """Return a RAW file path as a complete, quoted SQL string literal.

    ``sql_path("/data/it's.parquet")`` yields ``'/data/it''s.parquet'`` --
    quotes included, ready to drop straight into ``FROM {sql_path(p)}`` or
    ``TO {sql_path(p)}``. The quotes are part of the result on purpose: a call
    site that writes ``FROM '{p}'`` by hand is one that can forget the escape,
    and forgetting it is what made ``gpio add admin-divisions`` die with a
    ``ParserException`` on an output path containing an apostrophe -- *after*
    it had already written the file (issue #718).

    The argument must be **RAW**: the path as the user typed it, or as
    :func:`~geoparquet_io.core.file_utils.resolve_file_url` resolved it -- never
    an already-escaped value; escaping is not idempotent, so a second pass
    doubles the doubling. No guard can catch that automatically -- a filename
    may legally contain two apostrophes in a row -- so the rule is structural: a
    path is escaped exactly once, here, and the result is used whole. Slicing
    the quotes off it (``sql_path(p)[1:-1]``) to quote it again by hand is the
    shape the ``sql_escape_hand_quote`` guard refuses: with a NUL byte the
    literal is ``'a' || chr(0) || 'b'``, not one quoted span.

    For a non-path SQL value use :func:`sql_literal`, which supplies the quotes
    too; for an identifier use :func:`quote_identifier`. A path is a string
    literal like any other, so this is ``sql_literal`` on its ``fspath``.

    The bare escape under both, ``_escape_sql_string``, is private to this
    module: a call site that adds its own quotes around it is one that can
    forget them, and gets no NUL handling.

    A ``PathLike`` is accepted as well as a ``str``: callers hold ``Path``
    objects just as often, and ``str.replace`` on a ``Path`` is the completely
    unrelated *filesystem rename*, so coercing here is what keeps every call
    site from having to remember ``str()``.

    Args:
        path: RAW, unescaped file path or URL, as ``str`` or ``PathLike``.

    Returns:
        The path as a quoted, escaped SQL string literal.
    """
    return sql_literal(_require_str(os.fspath(path)))


def build_kv_metadata_clause(pairs: Mapping[str, str] | None) -> str | None:
    """Build a DuckDB ``KV_METADATA {...}`` clause from RAW key/value pairs.

    Every key and value is escaped exactly once here, so callers must pass raw
    strings. Returns ``None`` when there is nothing to write, so a caller can
    simply skip appending the option.

    Keys are quoted as well as escaped: an unquoted key containing ``:`` (an
    ``ARROW:schema`` payload, say) makes DuckDB's parser reject the whole COPY.

    Keys and values must already be ``str``. Nothing is coerced here: every
    caller decodes Parquet's ``bytes`` KV payloads before it gets this far, and
    ``str(b'{"a": 1}')`` would quietly write the literal ``b'{"a": 1}'`` into the
    file rather than fail. A non-``str`` raises instead.
    """
    if not pairs:
        return None
    body = ", ".join(
        f"{sql_literal(_require_str(key))}: {sql_literal(_require_str(value))}"
        for key, value in pairs.items()
    )
    return f"KV_METADATA {{{body}}}"


def quote_identifier(name: str) -> str:
    """
    Quote a SQL identifier for safe use in DuckDB queries.

    Escapes embedded double quotes by doubling them, then wraps in double quotes.
    This handles column/table names with spaces, special characters, reserved words,
    or uppercase letters that need to be preserved.

    The input must be a **bare** identifier, exactly as it appears in the file's
    schema (e.g. from ``primary_geometry_column`` or ``table.column_names``).
    The function is deliberately not idempotent: quoting an already-quoted name
    yields an identifier that literally contains double quotes, so never pass it
    something already quoted.

    For a SQL *string literal* (e.g. ``WHERE path_in_schema = '...'``) use
    :func:`sql_literal` instead; the two escapes are not interchangeable.

    Args:
        name: The identifier (column name, table name, etc.) to quote

    Returns:
        A safely quoted identifier string

    Raises:
        ValueError: If the name is empty or contains a NUL byte. Both are legal
            Parquet field names but have no quoted SQL spelling: ``""`` is a
            zero-length delimited identifier and a NUL truncates the identifier
            inside DuckDB's parser, so emitting either produces unparsable SQL
            rather than an actionable error.
    """
    if not name:
        raise ValueError(
            "cannot quote an empty SQL identifier: DuckDB rejects the "
            'zero-length delimited identifier ""'
        )
    if "\x00" in name:
        raise ValueError(
            "cannot quote a SQL identifier containing a NUL byte: DuckDB's "
            "parser truncates the identifier there"
        )
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


# SQL keywords that could be dangerous in a user-supplied WHERE clause.
# These could modify data or database structure.
#
# REPLACE is deliberately absent: ``REPLACE(str, from, to)`` is a standard
# scalar function in DuckDB, BigQuery Standard SQL and Carto/Postgres alike, so
# blocking it rejects ordinary filters such as ``REPLACE(zip,'-','')='19104'``.
# A function call cannot modify data; the statement gate below is what actually
# stops DML. TRUNCATE is kept by the opposite half of the same argument: none of
# those three backends spells its scalar truncation ``TRUNCATE`` (they all use
# ``trunc``/``TRUNC``), so the word only ever shows up as the DDL statement, and
# keeping it costs no legitimate clause.
DANGEROUS_SQL_KEYWORDS = [
    "DROP",
    "DELETE",
    "INSERT",
    "UPDATE",
    "CREATE",
    "ALTER",
    "TRUNCATE",
    "EXEC",
    "EXECUTE",
    "MERGE",
    "GRANT",
    "REVOKE",
]


def where_condition_fragment(where_clause: str) -> str:
    """Return ``(<clause>\\n)`` -- the clause as one AND-able boolean condition.

    A newline is placed before the closing paren so a trailing ``--`` comment in
    the clause cannot swallow the paren -- or whatever the caller appends next
    (another AND-ed condition, ``LIMIT``, ``USING SAMPLE``, a JOIN, ...).
    Callers are responsible for validating the clause with
    :func:`validate_where_clause` first.
    """
    return f"({where_clause}\n)"


def where_sql_fragment(where_clause: str | None) -> str:
    """Return a `` WHERE (...)`` fragment for ``where_clause`` (empty if None)."""
    if not where_clause:
        return ""
    return f" WHERE {where_condition_fragment(where_clause)}"


def _dangerous_keywords_in(where_clause: str) -> list[str]:
    """Return the blocklisted keywords that appear as real SQL words.

    Keywords inside string literals (``name = 'Grant County'``), inside quoted
    identifiers, or inside comments are *data*, not statements, and must not be
    flagged. DuckDB's own lexer is used to find the word tokens, which handles
    every literal form the SQL dialect has (``'...'``, ``E'...'``, ``$$...$$``)
    plus ``--`` and ``/* */`` comments. If the lexer is unavailable, fall back to
    a conservative whole-word regex over the raw clause.
    """
    try:
        import duckdb

        tokens = duckdb.tokenize(where_clause)
        word_types = {duckdb.token_type.keyword, duckdb.token_type.identifier}
        words = set()
        for position, token_type in tokens:
            if token_type not in word_types:
                continue
            match = re.match(r"\w+", where_clause[position:])
            if match:
                words.add(match.group(0).upper())
        return [kw for kw in DANGEROUS_SQL_KEYWORDS if kw in words]
    except Exception:  # pragma: no cover - lexer is part of the duckdb API
        upper_clause = where_clause.upper()
        return [kw for kw in DANGEROUS_SQL_KEYWORDS if re.search(rf"\b{kw}\b", upper_clause)]


def validate_where_clause(where_clause: str) -> None:
    """
    Validate that a user-supplied WHERE clause is a single filter expression.

    Two checks run:

    1. A blocklist of keywords that could modify data or database structure,
       matched against real SQL word tokens only (see
       :func:`_dangerous_keywords_in`).
    2. A parser-based statement gate. The clause is composed into the same
       ``WHERE (<clause>\\n)`` shape the callers emit and handed to DuckDB's
       parser; anything that parses as more than one statement is rejected.
       DuckDB's ``execute()`` runs multi-statement strings, so a clause that
       smuggles in a second statement could ``COPY`` data out or ``ATTACH`` a
       database (gpio #612). Counting parsed statements -- rather than scanning
       the text for ``;`` -- is what makes the gate hold: dollar quoting
       (``$$'$$``), block comments, line comments and ``E'\\''`` escapes all hide
       a ``;`` from a hand-rolled quote-state walker but not from the parser.

    Note: this is a safety net for *trusted* input, not a security boundary. It
    stops a clause from becoming extra statements; it cannot stop abuse that
    stays inside one expression (e.g. reading a local file through a scalar
    function). Untrusted input needs parameterized queries or a real allowlist.

    Args:
        where_clause: The WHERE clause string to validate

    Raises:
        ValidationError: If dangerous SQL keywords are found, if the clause does
            not parse, or if it composes into more than one statement.
    """
    found_keywords = _dangerous_keywords_in(where_clause)
    if found_keywords:
        raise ValidationError(
            f"WHERE clause contains potentially dangerous SQL keywords: {', '.join(found_keywords)}. "
            "Only SELECT-style filtering expressions are allowed in --where. "
            "Run data-modifying statements against the source system directly."
        )

    try:
        import duckdb
    except ModuleNotFoundError:
        # A duckdb-less vendoring host (#1307) keeps the keyword blocklist
        # above; the parser gate needs the engine that would run the query.
        return

    probe_query = f"SELECT 1 WHERE {where_condition_fragment(where_clause)}"
    try:
        statements = duckdb.extract_statements(probe_query)
    except Exception as e:
        raise ValidationError(
            f"WHERE clause is not a single filtering expression: it could not be parsed "
            f"as SQL. {e}. Note that --where is parsed with DuckDB's SQL dialect even "
            "when the query runs on another backend."
        ) from e

    if len(statements) > 1:
        raise ValidationError(
            f"WHERE clause is not a single filtering expression: it resolves to "
            f"{len(statements)} SQL statements, separated by a ';' (possibly hidden in a "
            "comment or a quoted string). Run additional statements against the source "
            "system directly."
        )


def build_spatial_join_condition(
    input_geom_col: str,
    target_geom_col: str,
    input_bbox_col: str | None = None,
    target_bbox_col: str | None = None,
    input_alias: str = "a",
    target_alias: str = "b",
    input_geom_sql: str | None = None,
    input_is_point: bool = False,
) -> str:
    """Build the ON-clause condition for a spatial join between two tables.

    The precise predicate is always ``ST_Intersects(target_geom, input_geom)``.
    When *both* sides expose a bbox covering column, a cheap bounding-box
    overlap test is ANDed in front of it: DuckDB evaluates the four numeric
    comparisons first and only runs the expensive geometry intersection on the
    surviving candidate pairs. Because bbox overlap is a necessary condition for
    geometry intersection, the result is identical to ST_Intersects alone -- the
    pre-filter changes the query plan and its memory use, never the output.

    Why the pre-filter is kept -- it is a *memory-safety* mechanism, not merely a
    speed-up:

    - A *bare* ``ST_Intersects(...)`` ON clause is the only shape DuckDB's
      ``SPATIAL_JOIN`` operator matches. ``SPATIAL_JOIN`` builds an in-memory,
      non-spilling R-tree; on large real-polygon inputs (e.g. remote Overture
      admin boundaries at >=1M features) that index exhausts memory and OOMs --
      the same "effectively hangs" failure first seen when the pre-filter was
      dropped in #457 (see PR #460).
    - ANDing the bbox-overlap test in front drops the plan to a streaming
      ``BLOCKWISE_NL_JOIN`` driving the LEFT JOIN, which runs in bounded memory
      and completes on those inputs.

    Small, local benchmarks (#538) showed the bare ``SPATIAL_JOIN`` finishing
    faster and suggested it was the fast path to prefer. A >=1M-feature
    validation (#545) showed that does not hold at scale, where the bare
    predicate OOMs. The pre-filter is therefore retained on the explicit-bbox
    (1.x) path for bounded memory; do NOT restore a bare predicate there on the
    strength of small-scale timings. The earlier "~27-73x faster / SPATIAL_JOIN
    is the fast path" rationale is withdrawn.

    Two paths still emit a bare predicate because no usable bbox pair exists: a
    reprojected non-CRS84 input whose stored bbox is in the source CRS (#525),
    and native-geometry inputs (they carry no 1.x bbox covering). Those paths
    share the same ``SPATIAL_JOIN`` OOM risk at scale; whether they need their
    own memory-bounded strategy -- and whether deriving a bbox from the geometry
    (once #462's direction) helps or hurts -- is tracked in #550, not decided
    here.

    All identifiers are passed through :func:`quote_identifier`, so column names
    sourced from untrusted file metadata cannot break out of the predicate.

    Args:
        input_geom_col: Geometry column on the input (left) table.
        target_geom_col: Geometry column on the target (right) table.
        input_bbox_col: Optional bbox covering column on the input table.
        target_bbox_col: Optional bbox covering column on the target table.
        input_alias: SQL alias for the input table (default "a").
        target_alias: SQL alias for the target table (default "b").
        input_geom_sql: A pre-built input-geometry expression (e.g. an
            ``ST_Transform`` reprojecting a non-CRS84 input, #525). It replaces
            ``input_alias.input_geom_col`` and drops the bbox pre-filter, since
            the stored input bbox is in the source CRS.
        input_is_point: The input geometry is one POINT per row (the join point
            ``gpio process aggregate admin`` builds). With a target bbox, the
            pre-filter is then point-in-bbox on that point -- comparable even
            when the input has no bbox of its own.

    Returns:
        A SQL boolean expression for use directly after ``ON``.
    """
    qt_geom = quote_identifier(target_geom_col)

    if input_is_point:
        point = input_geom_sql or f"{input_alias}.{quote_identifier(input_geom_col)}"
        intersects = f"ST_Intersects({target_alias}.{qt_geom}, {point})"
        if not target_bbox_col:
            return intersects
        qt_bbox = quote_identifier(target_bbox_col)
        return (
            "(\n"
            "        -- Fast point-in-bbox pre-filter (cheap; eliminates most candidate pairs)\n"
            f"        {target_alias}.{qt_bbox}.xmin <= ST_X({point}) AND\n"
            f"        {target_alias}.{qt_bbox}.xmax >= ST_X({point}) AND\n"
            f"        {target_alias}.{qt_bbox}.ymin <= ST_Y({point}) AND\n"
            f"        {target_alias}.{qt_bbox}.ymax >= ST_Y({point})\n"
            "    )\n"
            "    -- Precise check only runs on the bbox matches\n"
            f"    AND {intersects}"
        )

    # ``input_geom_sql`` is a pre-built input-geometry expression (e.g. wrapped in
    # ST_Transform to reproject a non-CRS84 input, #525). When supplied, the
    # stored input bbox is in the *source* CRS and so cannot be used for the
    # overlap pre-filter — fall back to the precise predicate alone.
    if input_geom_sql is not None:
        intersects = f"ST_Intersects({target_alias}.{qt_geom}, {input_geom_sql})"
        return intersects

    qi_geom = quote_identifier(input_geom_col)
    intersects = f"ST_Intersects({target_alias}.{qt_geom}, {input_alias}.{qi_geom})"

    # A one-sided bbox cannot form an overlap test, so fall back to the
    # precise predicate alone.
    if not (input_bbox_col and target_bbox_col):
        return intersects

    qi_bbox = quote_identifier(input_bbox_col)
    qt_bbox = quote_identifier(target_bbox_col)
    return (
        "(\n"
        "        -- Fast bbox-overlap pre-filter (cheap; eliminates most candidate pairs)\n"
        f"        {input_alias}.{qi_bbox}.xmin <= {target_alias}.{qt_bbox}.xmax AND\n"
        f"        {input_alias}.{qi_bbox}.xmax >= {target_alias}.{qt_bbox}.xmin AND\n"
        f"        {input_alias}.{qi_bbox}.ymin <= {target_alias}.{qt_bbox}.ymax AND\n"
        f"        {input_alias}.{qi_bbox}.ymax >= {target_alias}.{qt_bbox}.ymin\n"
        "    )\n"
        "    -- Precise check only runs on the bbox matches\n"
        f"    AND {intersects}"
    )


# The input-row id build_spatial_join_query carries through a one-match join.
SPATIAL_JOIN_INPUT_ROW = "__gpio_input_row"


def build_spatial_join_query(
    *,
    input_relation: str,
    target_relation: str,
    target_select: str,
    condition: str,
    match_order: Sequence[str],
    input_alias: str = "a",
    target_alias: str = "b",
) -> str:
    """Build the ``SELECT`` that LEFT JOINs every input row to one target it meets.

    The query keeps every input column (``<input_alias>.*``), adds
    ``target_select`` (target-side expressions, aliased by the caller), and
    joins ``ON condition`` -- normally :func:`build_spatial_join_condition`.
    An input row that meets no target is kept, with NULL target columns.

    Exactly one output row per input row, in input order (#543): a feature
    that intersects two target polygons is never emitted twice. Of the
    targets a row meets, the one kept is the first by ``match_order`` -- SQL
    expressions over the target alias, compared in order, ascending, NULLs
    last. That is the tiebreak, and it is deterministic: rows that tie on
    every ``match_order`` key carry the same target values, so which of them
    survives cannot show in the output. The admin commands pass the admin code
    columns, so a feature in the overlap of two admin polygons goes to the
    smaller code. It is a cheap key on purpose: an intersection-area tiebreak
    would run ``ST_Intersection`` on every matched row, and the admin join
    point is a POINT with no area. Caveat for polygon inputs (``add
    admin-divisions``, ``partition admin`` join the full geometry): a polygon
    straddling a border goes to the smaller code, not to the region holding
    most of its area.

    The dedup is spill-safe at the scale the admin commands target. Input rows
    are numbered with ``row_number() OVER ()`` -- a streaming window, with no
    ``PARTITION BY`` to buffer -- and the survivor is picked with ``DISTINCT
    ON``, which DuckDB plans as a hash aggregate (it spills) followed by a sort
    on the row number (it spills). The ``QUALIFY row_number() OVER (PARTITION
    BY ...)`` form that #461 retired buffers every row in a window that does
    not spill, and is not used.

    Args:
        input_relation: The input side: a table name, ``read_parquet(...)``
            call, or parenthesised subquery.
        target_relation: The target side, in the same forms.
        target_select: The target-side SELECT items, e.g. ``b."country" AS c``.
        condition: The ON condition.
        match_order: The tiebreak keys (at least one).
        input_alias: SQL alias for the input (default "a").
        target_alias: SQL alias for the target (default "b").

    Returns:
        A SELECT statement (no trailing semicolon).

    Raises:
        ValueError: An empty ``match_order``.
    """
    if not match_order:
        raise ValueError("build_spatial_join_query needs a match_order to pick the kept match")

    row = SPATIAL_JOIN_INPUT_ROW
    keys = [f"__gpio_match_{i}" for i in range(len(match_order))]
    key_select = ", ".join(f"{expr} AS {key}" for expr, key in zip(match_order, keys, strict=True))
    key_order = ", ".join(f"{key} ASC NULLS LAST" for key in keys)
    return (
        f"SELECT * EXCLUDE ({row}, {', '.join(keys)}) FROM (\n"
        f"    SELECT DISTINCT ON ({row}) * FROM (\n"
        f"        SELECT {input_alias}.*, {target_select}, {key_select}\n"
        f"        FROM (SELECT *, row_number() OVER () AS {row} FROM {input_relation}) {input_alias}\n"
        f"        LEFT JOIN {target_relation} {target_alias}\n"
        f"        ON {condition}\n"
        "    )\n"
        f"    ORDER BY {row}, {key_order}\n"
        ")"
    )


# Strategy labels returned by spatial_join_strategy(), kept in sync with the
# predicate decision in build_spatial_join_condition().
SPATIAL_JOIN_NATIVE = "native"
SPATIAL_JOIN_BBOX_PREFILTER = "bbox_prefilter"
SPATIAL_JOIN_NO_BBOX = "no_bbox"


def spatial_join_strategy(
    has_native_geometry: bool,
    input_bbox_col: str | None,
    target_bbox_col: str | None,
    input_geom_rewritten: bool = False,
) -> str:
    """Classify which spatial-join strategy :func:`build_spatial_join_condition` uses.

    Lets callers print a status message that matches the predicate that actually
    runs (issue #538), instead of misreporting the native-geometry fast path as a
    degraded "no bbox" fallback.

    Args:
        has_native_geometry: True when the input exposes a native geometry type
            (GeoParquet 2.x / geo-typed Parquet) rather than a 1.x bbox covering.
        input_bbox_col: Input-side bbox covering column, or ``None``.
        target_bbox_col: Target-side bbox covering column, or ``None``.
        input_geom_rewritten: True when the input geometry is rewritten in the ON
            clause (e.g. an ``ST_Transform`` reprojecting a non-CRS84 input, issue
            #525). :func:`build_spatial_join_condition` then drops the bbox
            pre-filter because the stored (source-CRS) input bbox is incomparable
            to the target bbox, so the emitted predicate is a bare ``ST_Intersects``
            even though ``input_bbox_col`` is populated. The classifier must mirror
            that or it misreports a reprojected 1.x-with-bbox input as a
            bbox-prefilter join (issue #538).

    Returns:
        - ``SPATIAL_JOIN_NATIVE``: native-geometry input. The ON clause is a bare
          ``ST_Intersects``, which DuckDB's ``SPATIAL_JOIN`` operator recognizes
          and accelerates. This is the fast path, not a fallback.
        - ``SPATIAL_JOIN_BBOX_PREFILTER``: 1.x input with a bbox covering on both
          sides; the cheap bbox-overlap test is ANDed in front of ``ST_Intersects``.
        - ``SPATIAL_JOIN_NO_BBOX``: no bbox pre-filter is applied — either a 1.x
          input without a bbox column, or a reprojected input whose stored bbox is
          incomparable. The precise check runs against every candidate.
    """
    if has_native_geometry:
        return SPATIAL_JOIN_NATIVE
    if input_geom_rewritten:
        return SPATIAL_JOIN_NO_BBOX
    if input_bbox_col and target_bbox_col:
        return SPATIAL_JOIN_BBOX_PREFILTER
    return SPATIAL_JOIN_NO_BBOX


_SPATIAL_JOIN_NOTES = {
    SPATIAL_JOIN_NATIVE: "-- Using native geometry with DuckDB SPATIAL_JOIN",
    SPATIAL_JOIN_BBOX_PREFILTER: "-- Using bbox columns for optimized spatial join",
    SPATIAL_JOIN_NO_BBOX: "-- Using full geometry intersection (no bbox optimization)",
}


def print_spatial_join_dry_run(
    strategy: str,
    query: str,
    output_parquet: str,
    compression: str | None,
    compression_level: int | None,
    row_group_rows: int,
) -> None:
    """Print the dry run of a spatial-join ``COPY`` (``add admin-divisions``/``country-codes``).

    The strategy note says which predicate runs (#538); the COPY itself is
    printed by the one display body, ``core.compression.print_copy_dry_run``,
    with the row-group count the caller's funnel resolves.
    """
    from geoparquet_io.core.compression import print_copy_dry_run
    from geoparquet_io.core.logging_config import info

    info(_SPATIAL_JOIN_NOTES[strategy])
    print_copy_dry_run(query, output_parquet, compression, compression_level, row_group_rows)
    info("-- Original metadata would also be preserved in the output file")


def _install_and_load_extension(con, name: str) -> None:
    """INSTALL (best-effort) then LOAD a DuckDB extension.

    An INSTALL failure alone is tolerated silently: parallel installs race
    with each other (https://github.com/duckdb/duckdb/issues/12589), and a
    cached copy of the extension may load fine even when the extension
    directory is not writable. But if the LOAD then also fails, the install
    error is what explains it (issue #574), so surface it as a warning
    before propagating the LOAD error instead of hiding it behind an opaque
    'Extension "..." not found'.
    """
    install_error = None
    try:
        con.execute(f"INSTALL {name};")
    except Exception as e:
        install_error = e
    try:
        con.execute(f"LOAD {name};")
    except Exception:
        if install_error is not None:
            warn(f"could not install the DuckDB '{name}' extension: {install_error}")
        raise


#: Marks the scratch directories gpio hands to DuckDB. The prefix is not
#: decoration: :func:`sweep_orphaned_spill_dirs` matches on it, so only a
#: directory gpio minted itself is ever a candidate for removal.
_SPILL_DIR_PREFIX = "gpio-spill-"

#: The exact shape :func:`spill_directory` mints, with the owning pid captured.
#: Anything else under the base -- a cached dataset, another tool's scratch, a
#: hand-made directory that merely starts with the prefix -- does not match and
#: is never touched.
_SPILL_DIR_RE = re.compile(rf"^{re.escape(_SPILL_DIR_PREFIX)}(\d+)-[0-9a-f]{{12}}$")


# Keep the marker open for the owning process's lifetime. A lock is independent
# of the pid namespace from which a sibling sweeps the shared volume.
_SPILL_LOCK_NAME = ".gpio-owner.lock"
_spill_locks: dict[str, BinaryIO] = {}
_spill_locks_guard = threading.Lock()


def _spill_lock(path: str) -> BinaryIO | None:
    """Take a nonblocking ownership lock; None means Windows (no fcntl)."""
    try:
        import fcntl
    except ImportError:
        return None
    marker = open(os.path.join(path, _SPILL_LOCK_NAME), "a+b")
    try:
        fcntl.flock(marker, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BaseException:
        marker.close()
        raise
    return marker


@contextmanager
def _spill_base_lock(base: str | os.PathLike):
    """Serialize publishing a marker and sweeping on a shared spill volume."""
    try:
        import fcntl
    except ImportError:
        yield
        return
    # Lock the base directory itself: a sidecar lock would outlive every spill
    # and violate callers' scratch cleanup guarantees. This also works when
    # the base is read-only (scanning it still must be best effort).
    descriptor = os.open(base, os.O_RDONLY)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        os.close(descriptor)


def _release_spill_lock(path: str) -> None:
    # Runs as a connection's weakref finalizer, so the cyclic GC can call it on
    # any thread at any allocation -- including one inside spill_directory that
    # holds _spill_locks_guard and the base flock. Taking a lock here would then
    # wait on its own thread forever and stall every sibling process sharing the
    # base (the CI matrix hang). So it takes none: the dict pop is atomic, and
    # whoever pops a path owns its cleanup.
    marker = _spill_locks.pop(path, None)
    if marker is not None:
        match = _SPILL_DIR_RE.match(os.path.basename(path))
        if match is not None and int(match[1]) == os.getpid():
            shutil.rmtree(path, ignore_errors=True)
        marker.close()


def _release_spill_locks() -> None:
    for path in list(_spill_locks):
        _release_spill_lock(path)


atexit.register(_release_spill_locks)


def _process_is_alive(pid: int) -> bool:
    """Whether ``pid`` is still running. Answers True whenever it cannot tell.

    Only ever used to decide *not* to delete something, so the unsure answer has
    to be "alive": a false "dead" would take a running sibling's scratch space
    away mid-query, while a false "alive" merely leaves one more directory for
    the next run (or the OS tmp reaper) to collect.
    """
    try:
        import psutil

        return psutil.pid_exists(pid)
    except Exception:  # psutil is optional on vendored hosts (#1330): unsure means alive
        return True


def orphaned_spill_dirs(base_dir: str | os.PathLike) -> list[str]:
    """Spill directories under ``base_dir`` whose owning process has exited.

    On POSIX, a held marker lock protects a live owner across pid namespaces.
    Legacy leaves without a marker retain the conservative pid check. Windows
    has no fcntl and always uses that check.
    """
    found: list[str] = []
    try:
        entries = list(os.scandir(base_dir))
    except OSError:
        return found
    for entry in entries:
        match = _SPILL_DIR_RE.match(entry.name)
        if match is None:
            continue
        try:
            if not entry.is_dir(follow_symlinks=False):
                continue
        except OSError:  # pragma: no cover - raced with another sweeper
            continue
        if os.path.isfile(os.path.join(entry.path, _SPILL_LOCK_NAME)):
            try:
                marker = _spill_lock(entry.path)
            except OSError:
                continue
            if marker is not None:
                marker.close()
                found.append(entry.path)
                continue
        if not _process_is_alive(int(match.group(1))):
            found.append(entry.path)
    return found


def sweep_orphaned_spill_dirs(base_dir: str | os.PathLike) -> list[str]:
    """Remove the spill directories under ``base_dir`` that nobody owns any more.

    DuckDB removes its temp directory when the *query* completes, not when the
    process dies. A run killed mid-spill (SIGINT, SIGTERM, SIGKILL) therefore
    orphans a directory that can hold gigabytes, however carefully the caller
    closes the connection in a ``finally``. DuckDB's own fixed ``.tmp`` made
    that self-limiting by accident -- the next run reuses the same path and the
    same ``duckdb_temp_storage_*.tmp`` filenames -- so the unique names gpio
    needs for correctness would otherwise turn a bounded leak into a growing
    one. This is the other half of that trade.

    Best effort by design: a leftover owned by another user, or on a read-only
    volume, is skipped rather than raised, because failing to tidy up must never
    fail the run that was only trying to start.

    Returns:
        The paths actually removed, for callers that report what they freed.
    """
    removed: list[str] = []
    try:
        with _spill_base_lock(base_dir):
            for path in orphaned_spill_dirs(base_dir):
                marker = None
                try:
                    # Reacquire and hold through removal: listing is a snapshot.
                    marker = _spill_lock(path)
                    match = _SPILL_DIR_RE.match(os.path.basename(path))
                    if marker is None and (match is None or _process_is_alive(int(match[1]))):
                        continue
                    shutil.rmtree(path)
                except OSError:
                    continue
                finally:
                    if marker is not None:
                        marker.close()
                removed.append(path)
    except OSError:
        pass  # Missing/read-only base: sweeping must never fail the caller.
    return removed


def spill_directory(base_dir: str | os.PathLike | None = None) -> str:
    """Path to a private directory DuckDB may spill intermediate results into.

    On POSIX the directory is created with a locked ownership marker before
    returning. DuckDB may spill later; the lock already protects it from sibling
    sweepers, including those in a different pid namespace. Empty directories
    are removed at process exit. Windows keeps lazy creation and pid checks.

    Every call returns a fresh path, and that uniqueness is a correctness
    requirement rather than tidiness. DuckDB names its spill files after the
    block size alone (``duckdb_temp_storage_S192K-0.tmp``): nothing in the name
    identifies the connection, the database or the process. Two connections
    pointed at one directory therefore write over each other's blocks, and the
    loser fails with ``IO Error: Could not read enough bytes from file`` or, worse,
    reads back another query's bytes as its own. That applies to two gpio
    processes as much as to two connections inside one, so a shared constant --
    ``/tmp/gpio-spill``, an admin cache directory -- is never a safe answer.

    Args:
        base_dir: Volume to spill onto. Defaults to the OS temp directory, which
            follows ``TMPDIR``: that is the knob for a machine whose root volume
            is too small to hold a large sort's spill. On POSIX a missing base
            is created. When permissions prevent creation, the uncreated path
            is returned; DuckDB reports the unusable volume if it needs to spill.
            Windows always creates its directory lazily.

    Returns:
        An absolute path, safe to hand to ``SET temp_directory``.
    """
    base = os.fspath(base_dir) if base_dir is not None else tempfile.gettempdir()
    sweep_orphaned_spill_dirs(base)
    path = os.path.join(base, f"{_SPILL_DIR_PREFIX}{os.getpid()}-{uuid.uuid4().hex[:12]}")
    try:
        import fcntl  # noqa: F401 -- availability selects the Windows fallback
    except ImportError:
        return path
    try:
        os.makedirs(base, exist_ok=True)
        with _spill_locks_guard, _spill_base_lock(base):
            # DuckDB can remove a completed spill's directory. Close its obsolete
            # descriptor before retaining another, so repeated connections stay bounded.
            for old_path in list(_spill_locks):
                if not os.path.exists(old_path):
                    # A finalizer may pop it meanwhile; see _release_spill_lock.
                    stale = _spill_locks.pop(old_path, None)
                    if stale is not None:
                        stale.close()
            os.makedirs(path, mode=0o700, exist_ok=False)
            try:
                marker = _spill_lock(path)
            except BaseException:
                shutil.rmtree(path, ignore_errors=True)
                raise
            if marker is not None:
                _spill_locks[path] = marker
    except OSError as exc:
        if exc.errno not in (errno.EACCES, errno.EROFS) or os.path.exists(path):
            raise
        # No directory could be created, hence no spill can be swept. Preserve
        # the lazy factory contract: reads that never spill can still succeed;
        # DuckDB reports the unusable volume only if it actually needs to spill.
    return path


#: The DuckDB failures whose cause is the *input*, not gpio.
#:
#: This is gpio's one answer to "is this DuckDB error news about the user's
#: file, or a bug in something we generated?" -- the question the CLI asks
#: before it replaces a traceback with an error line (#983), and the same
#: question ``api/`` has to answer without Click, which is why the taxonomy
#: lives in ``core/`` rather than at the boundary that happens to consume it.
#:
#: Enumerated one by one rather than named by a base class, because DuckDB's
#: hierarchy does not split along this line. Measured against duckdb 1.5.5:
#: ``InvalidInputException`` sits under ``ProgrammingError`` next to
#: ``ParserException``, ``BinderException`` and ``CatalogException``, which are
#: its exact opposite, and ``duckdb.Error``'s only direct subclass is
#: ``DatabaseError`` -- so every shorthand available is either too wide or
#: splits the wrong set.
#:
#: * ``InvalidInputException`` -- the file's own bytes or metadata are
#:   unreadable. A ``geo`` block with no ``columns`` object raises this, as do
#:   all four of #983's reproductions and DuckDB's curved-WKB refusal (#988).
#: * ``IOException`` -- missing, unreachable or unreadable. ``HTTPException``
#:   hangs below it, so a remote URL that will not fetch is covered too; both
#:   are properties of the path the user named.
#:
#: What is deliberately *not* here is everything gpio itself can get wrong.
#: ``ParserException``, ``BinderException`` and ``CatalogException`` are decided
#: by the SQL text and the schema before a single row is read: they reproduce on
#: every file, so they are never news about one of them, and always a bug in a
#: query we generated -- the defect class behind #700, #718 and #944. Those keep
#: their traceback.
#:
#: ``ConversionException`` is not here either, and that is a decision rather
#: than an oversight: no reproduction reaches an unowned boundary with one.
#: ``core/convert.py`` already owns the live conversion site with a better
#: message, and the one refusal known to carry "Conversion Error" in its *text*
#: -- #988's curved WKB -- is raised as an ``InvalidInputException`` and so is
#: covered by the first entry anyway. gpio writes every ``CAST`` it runs, so
#: whether such a failure is the data's fault or gpio's choice of target type is
#: not decidable from the exception, and membership here has to be decidable.
#:
#: Deferred like every other duckdb touch in this module (#1307): the tuple is
#: built on first attribute access through the module ``__getattr__`` below, so
#: ``from ... import INPUT_FILE_DUCKDB_ERRORS`` still works while a plain
#: ``import geoparquet_io.core.duckdb_utils`` never imports duckdb.
def _input_file_duckdb_errors() -> tuple[type[BaseException], ...]:
    cached = globals().get("INPUT_FILE_DUCKDB_ERRORS")
    if cached is None:
        import duckdb

        cached = (
            duckdb.InvalidInputException,
            duckdb.IOException,
        )
        # Cache into the module namespace so every later access -- attribute or
        # :func:`is_input_file_duckdb_error` -- reads one tuple, and a test that
        # monkeypatches ``duckdb_utils.INPUT_FILE_DUCKDB_ERRORS`` is honored.
        globals()["INPUT_FILE_DUCKDB_ERRORS"] = cached
    return cached


def __getattr__(name: str):
    if name == "INPUT_FILE_DUCKDB_ERRORS":
        return _input_file_duckdb_errors()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def is_input_file_duckdb_error(exc: BaseException) -> bool:
    """True when `exc` is a DuckDB failure the *input* caused, not gpio.

    Tests the exception itself and not its ``__cause__`` chain, unlike
    :func:`spill_space_hint` and
    :func:`~geoparquet_io.core.exceptions.is_unpublished_extension_error`. Those
    two match on *text*, which survives being re-raised inside a wrapper, so
    they have to look down the chain to find it. This matches on *class*, and a
    class that has been wrapped by a gpio frame is no longer unowned: the
    wrapper is gpio saying what it thinks happened, and its own class is the
    answer to use.
    """
    return isinstance(exc, _input_file_duckdb_errors())


#: DuckDB's own words when the *spill volume* fills up. The message it prints is
#: an "Out of Memory Error" that never mentions ``TMPDIR``, so it sends users
#: after the one knob that cannot help them; this is the phrase that tells the
#: disk shortage apart from a real memory one.
_SPILL_EXHAUSTED_SIGNATURE = "max_temp_directory_size"


def spill_space_hint(exc: BaseException | str | None) -> str | None:
    """gpio's guidance for DuckDB's "ran out of spill space" error, or ``None``.

    Accepts an exception (whose ``__cause__``/``__context__`` chain is walked)
    or the raw error text, mirroring
    :func:`~geoparquet_io.core.exceptions.is_unpublished_extension_error`.
    """
    if exc is None:
        return None
    if isinstance(exc, str):
        blob = exc
    else:
        parts: list[str] = []
        seen: set[int] = set()
        cur: BaseException | None = exc
        while cur is not None and id(cur) not in seen:
            seen.add(id(cur))
            parts.append(str(cur))
            cur = cur.__cause__ or cur.__context__
        blob = " ".join(parts)
    if _SPILL_EXHAUSTED_SIGNATURE not in blob:
        return None
    return (
        "This is a disk shortage, not a memory one, whatever the wording says: "
        "DuckDB spills intermediate results to a scratch directory and that "
        f"volume ran out of room. gpio spills under the system temp directory "
        f"(currently {tempfile.gettempdir()}); the admin-boundary commands spill "
        "onto the admin cache volume instead.\n"
        "\n"
        "Point TMPDIR (%TEMP% on Windows) at a volume with room to spare:\n"
        "\n"
        "    TMPDIR=/path/with/space gpio ...\n"
        "\n"
        "Check first whether /tmp is RAM-backed: systemd mounts a tmpfs there by "
        "default on Fedora, Arch and openSUSE, as do Kubernetes' "
        "'emptyDir: {medium: Memory}' and 'docker run --tmpfs /tmp'. Spilling "
        "onto a tmpfs spends the very memory the spill was meant to save, so "
        "TMPDIR needs to name real storage."
    )


def _ceiling_based_limits() -> tuple[str | None, int | None]:
    """The default ``memory_limit`` and thread cap for a connection that names neither.

    Deferred import: :mod:`geoparquet_io.core.memory_limits` owns the ceiling and
    the share of it gpio takes (one detector, one number -- #1156), and it imports
    this module for its connections and setting helpers. Importing it at module
    scope here would close that cycle.
    """
    from geoparquet_io.core.memory_limits import _connection_limit

    return _connection_limit(None)


def _apply_connection_limits(
    con: duckdb.DuckDBPyConnection, memory_limit: str | None, threads: int | None
) -> None:
    """Bound a freshly opened connection: the memory limit, and the threads it affords.

    An explicit value wins; otherwise the ceiling-based default, so that DuckDB
    work with no write to wrap -- a spatial join into a temp table, a format
    export, a disk-rewrite COPY -- is bounded too (#1174). The limit alone is not
    enough: spread over every core it leaves each thread too little to spill with,
    and DuckDB raises ``OutOfMemoryException`` instead (measured at a 244MB limit:
    a 24M-row Hilbert scan failed on 64 threads and spilled on one). So the
    default comes with the cap it affords, as a bounded statement's does.

    The cap only ever comes *down* from what DuckDB already has, and only when
    gpio chose both numbers: a caller who named the limit or the thread count
    owns them, and that keeps the cap idempotent for
    :func:`memory_limits.open_bounded_connection`, which passes both.
    """
    limit, cap = (memory_limit, None) if memory_limit is not None else _ceiling_based_limits()
    _apply_memory_limit(con, limit, None if threads is not None else cap)


def _apply_memory_limit(
    con: duckdb.DuckDBPyConnection,
    limit: str | None,
    thread_cap: int | None,
    verbose: bool = False,
) -> None:
    """SET ``memory_limit``, then lower ``threads`` to ``thread_cap`` -- the one place gpio does.

    Every bounded connection goes through here: one freshly opened
    (:func:`_apply_connection_limits`), one write scoped on a caller's
    connection (``memory_limits.scoped_write_memory_limit``) and an already-open
    connection held to a limit (``memory_limits.bound_open_connection``). Either
    half is skipped when its value is ``None``. The cap only ever brings
    ``threads`` *down* from what the session has: a thread count is never raised
    to match a limit.
    """
    if limit is not None:
        con.execute(f"SET memory_limit = {sql_literal(str(limit))};")
        if verbose:
            debug(f"DuckDB memory limit: {limit}")
    if thread_cap is not None and thread_cap < int(str(_current_setting(con, "threads"))):
        con.execute(f"SET threads = {thread_cap}")
        if verbose:
            debug(f"DuckDB threads: {thread_cap} (for the memory limit)")


_PROFILE_ENV_VAR = "GPIO_DUCKDB_PROFILE"

# Values a user reaching for an on/off switch would type. GPIO_DUCKDB_PROFILE
# names a directory, so one of these is only honoured when that directory
# already exists; otherwise `=1` would quietly create a ./1/ directory.
_PROFILE_FLAG_LIKE = frozenset({"1", "0", "true", "false", "yes", "no", "on", "off"})

# Distinguishes connections opened within one process; itertools.count is
# safe under threads.
_profile_connection_ids = itertools.count(1)


def _profile_file_prefix() -> str:
    """A per-connection filename prefix that does not collide across processes.

    Microsecond time plus PID plus a random token: the PID alone repeats
    across containers (often PID 1), and second resolution repeats within a
    run. The connection counter keeps the names in opening order.
    """
    stamp = datetime.datetime.now().strftime("%Y%m%dT%H%M%S%f")
    token = uuid.uuid4().hex[:6]
    return f"gpio-profile-{stamp}-p{os.getpid()}-{token}-c{next(_profile_connection_ids)}"


def _ensure_profile_directory(directory: str) -> OSError | None:
    """Create *directory* if missing and probe that a file can be written.

    Returns the ``OSError`` that made it unusable, or ``None`` when usable.
    """
    try:
        os.makedirs(directory, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=directory, prefix=".gpio-profile-probe-"):
            pass
    except OSError as exc:
        return exc
    return None


def _profile_directory() -> str | None:
    """The usable ``GPIO_DUCKDB_PROFILE`` directory, or ``None`` (profiling off).

    The path is resolved to an absolute path here, once, so a later
    ``os.chdir`` cannot move it. An unusable path (or a flag-like value such
    as ``1``/``true`` that is not an existing directory) warns and returns
    ``None`` rather than raising: DuckDB fails *every* query on a connection
    whose ``profiling_output`` cannot be written (an InternalException at fetch
    time), so profiling must be refused up front — it may never break the
    actual work.
    """
    raw = os.environ.get(_PROFILE_ENV_VAR, "").strip()
    if not raw:
        return None
    if raw.lower() in _PROFILE_FLAG_LIKE and not os.path.isdir(raw):
        warn(
            f"GPIO_DUCKDB_PROFILE={raw!r} looks like an on/off flag, but it names the "
            "directory to write DuckDB profiles into (e.g. GPIO_DUCKDB_PROFILE=profiles); "
            "continuing without DuckDB profiling"
        )
        return None
    directory = os.path.abspath(raw)
    exc = _ensure_profile_directory(directory)
    if exc is not None:
        warn(
            f"GPIO_DUCKDB_PROFILE={raw!r} is not a writable directory ({exc}); "
            "continuing without DuckDB profiling"
        )
        return None
    return directory


class ProfiledConnection:
    """A DuckDB connection whose queries each write their own JSON profile.

    DuckDB's ``profiling_output`` setting names *one* file, and each profiled
    statement overwrites it, so a fixed path would keep only the last query's
    profile. This wrapper re-points ``profiling_output`` at a fresh file in
    the ``GPIO_DUCKDB_PROFILE`` directory before every ``execute``,
    ``executemany`` and ``sql`` call; a ``SET`` writes no profile itself, so
    the rotation never clobbers anything, and a name already on disk is
    skipped, so no existing file is overwritten by a rotation.

    What gets captured is decided by DuckDB, not by this wrapper: DuckDB
    writes a statement's profile only when its result is *fully consumed*
    (``fetchall()``, ``.arrow().read_all()``, a ``COPY``, a ``fetchone()``
    read until it returns ``None``). A result read with a single bare
    ``fetchone()``, or abandoned when the next statement runs, writes no
    profile. gpio reads single rows through :func:`fetch_one`, which on DuckDB
    2.0 completes the result, so there those reads are profiled too. DDL and
    ``SET`` write none either. So a command's profile directory holds its
    result-draining queries — usually the heavy ones — not every statement it
    ran. The profiled SQL is the file's ``query_name`` on DuckDB 1.x and its
    ``query.sql`` on 2.0, which nests the rest under ``query``, ``operator``
    and ``io``.

    A lazy relation from ``sql()`` executes when it is fetched, and DuckDB
    writes its profile to whichever file is current *then*: fetched straight
    away it gets its own file, but fetched after another statement it
    replaces that statement's profile (DuckDB overwrites its output file, and
    no hook runs before a relation executes).

    Before each rotation the directory is re-checked (and re-created if it
    was deleted). If it can no longer be written, profiling is switched off
    on this connection with a single warning, so profiling never breaks the
    work. A directory removed, or a disk filled, *during* a statement still
    fails that statement at fetch time; it is not retried, because the
    statement's side effects (a ``COPY`` file, say) have already happened.

    ``cursor()`` returns a profiled cursor (a cursor is a new DuckDB session
    and would not inherit the setting). Everything else delegates to the
    wrapped connection. It is a proxy, not a ``DuckDBPyConnection`` subclass:
    ``isinstance(con, duckdb.DuckDBPyConnection)`` is False for it, and it
    cannot be handed to a DuckDB API that takes ``connection=``. gpio does
    neither, which is why returning it from :func:`get_duckdb_connection` is
    safe.
    """

    def __init__(self, con: duckdb.DuckDBPyConnection, directory: str):
        self._con = con
        self._directory = directory
        self._prefix = _profile_file_prefix()
        self._statement_ids = itertools.count(1)
        self._enabled = True
        # Order matters: DuckDB rejects a .json profiling_output until
        # enable_profiling is 'json'. The window between the two SETs is safe
        # because SET statements are themselves never profiled.
        con.execute("SET enable_profiling = 'json';")
        self._rotate_profile_output()

    def _directory_usable(self) -> bool:
        if os.path.isdir(self._directory) and os.access(self._directory, os.W_OK):
            return True
        exc = _ensure_profile_directory(self._directory)
        if exc is None:
            return True
        self._enabled = False
        self._con.execute("PRAGMA disable_profiling;")
        warn(
            f"GPIO_DUCKDB_PROFILE directory {self._directory!r} is no longer writable "
            f"({exc}); DuckDB profiling is off for the rest of this connection"
        )
        return False

    def _rotate_profile_output(self) -> None:
        if not self._enabled or not self._directory_usable():
            return
        while True:
            name = f"{self._prefix}-q{next(self._statement_ids):04d}.json"
            target = os.path.join(self._directory, name)
            if not os.path.exists(target):
                break
        self._con.execute(f"SET profiling_output = {sql_path(target)};")

    def execute(self, *args, **kwargs):
        self._rotate_profile_output()
        return self._con.execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        self._rotate_profile_output()
        return self._con.executemany(*args, **kwargs)

    def sql(self, *args, **kwargs):
        self._rotate_profile_output()
        return self._con.sql(*args, **kwargs)

    def cursor(self):
        cur = self._con.cursor()
        if not self._enabled:
            return cur
        return ProfiledConnection(cur, self._directory)

    def __getattr__(self, name):
        return getattr(self._con, name)

    def __enter__(self):
        self._con.__enter__()
        return self

    def __exit__(self, *exc_info):
        return self._con.__exit__(*exc_info)


def load_spatial_extension(con) -> None:
    """INSTALL and LOAD spatial on ``con``, with gpio's session settings.

    The one place that knows loading spatial also means setting
    ``geometry_always_xy`` (the setting is registered by the extension, so it
    cannot be set before the LOAD). Used by :func:`get_duckdb_connection` and
    by paths that opened without spatial (``load_spatial=None`` where
    ``duckdb_capabilities().core_geometry``) but then hit a function outside DuckDB's core
    set and load it on demand.
    """
    _install_and_load_extension(con, "spatial")
    # DuckDB 1.5+: ensure lon/lat = x/y axis order globally.
    # Replaces per-call always_xy := true in ST_Transform.
    con.execute("SET geometry_always_xy = true;")


def get_duckdb_connection(
    load_spatial=True,
    load_httpfs=None,
    use_s3_auth=False,
    threads=None,
    s3_endpoint=None,
    s3_region=None,
    s3_use_ssl=None,
    temp_directory=None,
    memory_limit=None,
):
    """
    Create a DuckDB connection with necessary extensions loaded.

    By default, S3 access uses no credentials, which works for public buckets.
    DuckDB automatically handles region detection for public S3 buckets.

    When use_s3_auth=True, loads the aws extension and configures credential
    discovery for private S3 buckets.

    Args:
        load_spatial: Whether to load spatial extension (default: True).
                    If None, loads it only when DuckDB's core lacks the
                    GEOMETRY type (``not duckdb_capabilities().core_geometry``, #1353): the
                    auto mode for read-only paths that need nothing beyond
                    DuckDB core's serialize/deserialize set, mirroring
                    load_httpfs's None-means-auto convention.
        load_httpfs: Whether to load httpfs extension for S3/Azure/GCS.
                    If None (default), auto-detects based on usage.
        use_s3_auth: Whether to configure AWS credential chain for S3 (default: False).
                    Only needed for private buckets.
        threads: Number of threads for DuckDB to use (default: None = all cores,
                less the cap the default memory limit affords). Limiting threads
                is useful for parallel test execution to prevent CPU saturation
                when multiple pytest workers create connections. A value given
                here is used as given: naming it opts out of that cap.
        temp_directory: Volume for DuckDB to spill intermediate results onto.
                    Defaults to a private directory under the OS temp directory
                    (see :func:`spill_directory`); pass a path to put the spill on
                    another volume, e.g. beside a very large output. Never share
                    one path between connections -- ``spill_directory(that_path)``
                    gives each its own leaf under the volume you chose.
        memory_limit: DuckDB memory limit (e.g. "8GB"). Used exactly as given.
                    Left unset, the connection takes gpio's ceiling-based default
                    (``memory_limits.default_memory_limit``: half the process's
                    cgroup cap or physical RAM, whichever is lower) rather than
                    DuckDB's own ~80% of host RAM, which is blind to a container
                    or Slurm job cgroup and allocates outside its own accounting,
                    so the cap is reached before DuckDB ever spills (#1156,
                    #1174). Either way DuckDB spills to temp_directory rather
                    than crashing. A ceiling that cannot be read leaves DuckDB's
                    own default in place. The ceiling-based default also caps
                    ``threads`` to what it affords (``_apply_connection_limits``);
                    a value given here is used as given.

    Environment:
        GPIO_DUCKDB_PROFILE: a directory path (resolved to an absolute path
            when the connection opens). When set, the connection runs with
            ``enable_profiling = 'json'`` and writes one profile file per
            fully consumed query into that directory (created if missing),
            named ``gpio-profile-<time>-p<pid>-<token>-c<n>-q<n>.json``; see
            :class:`ProfiledConnection` for which statements DuckDB profiles.
            An unusable path, or a flag-like value such as ``1``/``true`` that
            is not an existing directory, warns and profiling is skipped; the
            work itself is not failed by profiling.

    Returns:
        duckdb.DuckDBPyConnection | ProfiledConnection: Configured connection
        with extensions loaded. When GPIO_DUCKDB_PROFILE names a usable
        directory it is a :class:`ProfiledConnection`, a proxy that delegates
        to the DuckDB connection (it is not a subclass). The return is left
        unannotated because the many helpers typed ``duckdb.DuckDBPyConnection``
        that receive it use only the methods the proxy forwards.
    """
    import duckdb

    config = {}
    if threads is not None:
        config["threads"] = threads
    con = duckdb.connect(config=config) if config else duckdb.connect()

    # Enable large buffer size for Arrow export to handle datasets with >2GB of
    # string/binary data (e.g., large WKB geometry columns). Without this,
    # DuckDB fails with "Arrow Appender: The maximum total string size for
    # regular string buffers is 2147483647" errors.
    con.execute("SET arrow_large_buffer_size = true;")

    # Spill-to-disk. DuckDB's own default is the *relative* path ".tmp", so a
    # spill lands wherever the process happens to be running: a read-only working
    # directory turns a large sort into a hard failure, and a small root volume
    # into a mid-sort ENOSPC, however much room the input and output volumes have.
    # Decide it here, once, for all connections -- and decide it now, because
    # DuckDB refuses to move a temp directory that has already been used
    # ("Cannot switch temporary directory after the current one has been used"),
    # so a per-write override cannot be applied to the connections gpio shares
    # across writes (a partition loop finalizes N files on one connection).
    effective_temp_dir = temp_directory if temp_directory is not None else spill_directory()
    if temp_directory is None and effective_temp_dir in _spill_locks:
        weakref.finalize(con, _release_spill_lock, effective_temp_dir)
    con.execute(f"SET temp_directory = {sql_path(effective_temp_dir)};")
    _apply_connection_limits(con, memory_limit, threads)

    # Always load spatial extension by default (core use case). None is the
    # auto mode: spatial only where DuckDB's own core lacks GEOMETRY (#1353).
    if resolve_load_spatial(load_spatial):
        load_spatial_extension(con)

    # S3 config, resolved once (explicit kwargs over the ambient scope). The
    # SET statements and the secret below are both built from it.
    s3_settings = _resolve_s3_settings(s3_endpoint, s3_region, s3_use_ssl)

    # Load httpfs for cloud storage support
    if load_httpfs:
        _install_and_load_extension(con, "httpfs")

        # Only configure AWS credentials when asked for: use_s3_auth (private
        # buckets) or a scoped AWS profile, which only the credential chain can
        # honor. Public buckets work without any secret.
        if use_s3_auth or s3_settings["profile"]:
            _create_s3_secret(con, s3_settings, required=use_s3_auth)

    _apply_s3_settings(con, s3_settings)

    # GPIO_DUCKDB_PROFILE=<dir>: wrap the connection (after all setup, so the
    # setup statements above are not profiled) to write one JSON profile per
    # query into <dir>. An unusable directory warned and returned None.
    profile_dir = _profile_directory()
    if profile_dir is not None:
        return ProfiledConnection(con, profile_dir)

    return con


# Query Farm's community extensions (a5) POST usage telemetry when the
# extension is LOADed. The 1.5.5-era a5 build fires that request from a
# *detached* std::thread, so it keeps running after LOAD returns and races
# process teardown: if the process exits while the thread is inside the TLS
# handshake, OpenSSL's global locks have already been freed and the thread
# dereferences a null rwlock, killing the process with SIGSEGV (issue #779).
# The output file is already written by then, so the only symptom is a
# non-zero exit code -- which still fails scripts and CI. Measured at ~8% of
# `gpio add a5` invocations on duckdb 1.5.5; 0% on 1.5.1, whose a5 build used
# a blocking std::async instead.
#
# The extension opts out on the mere *presence* of this variable, whatever its
# value, so an existing setting is left untouched.
_TELEMETRY_OPT_OUT_VAR = "QUERY_FARM_TELEMETRY_OPT_OUT"


def _opt_out_of_extension_telemetry() -> None:
    """Disable community-extension load-time telemetry before LOAD.

    Must run before the LOAD statement: the extension reads the environment
    while it initialises. Uses ``setdefault`` so a user who has deliberately
    set the variable keeps their own value.
    """
    os.environ.setdefault(_TELEMETRY_OPT_OUT_VAR, "1")


def load_community_extension(con, name: str, feature: str | None = None) -> None:
    """INSTALL and LOAD a DuckDB community extension with a clear error message.

    Community extensions (e.g. ``geography`` for S2 support) are built per
    DuckDB release. When a newer DuckDB version ships before the extension has
    been rebuilt for it, ``INSTALL ... FROM community`` fails with an opaque
    HTTP 404. Translate that into an actionable :class:`ExtensionUnavailableError`
    so users understand the extension simply isn't published for their DuckDB
    version yet.

    Args:
        con: An open DuckDB connection.
        name: Community extension name (e.g. "geography", "h3").
        feature: What the caller was trying to do (e.g. "gpio add s2"), named in
            the error so the user knows which command is unavailable.

    Raises:
        ExtensionUnavailableError: If the extension cannot be installed or loaded.
    """
    import duckdb

    _opt_out_of_extension_telemetry()
    try:
        con.execute(f"INSTALL {name} FROM community;")
        con.execute(f"LOAD {name};")
    except duckdb.Error as e:
        raise ExtensionUnavailableError(
            name, duckdb_capabilities().version, str(e), feature=feature
        ) from e


def require_community_extension(name: str, feature: str | None = None) -> None:
    """Fail fast when a community extension a command depends on is unavailable.

    Commands that cannot run without a community extension call this before
    reading input or writing anything, so an unpublished extension surfaces as
    one clear message instead of an error partway through a pipeline (#737).

    The check uses a throwaway in-memory connection; a successful ``INSTALL``
    is cached by DuckDB, so the cost after the first run is negligible.

    Args:
        name: Community extension name (e.g. "geography").
        feature: What the caller was trying to do (e.g. "gpio add s2").

    Raises:
        ExtensionUnavailableError: If the extension cannot be installed or loaded.
    """
    import duckdb

    con = duckdb.connect()
    try:
        load_community_extension(con, name, feature=feature)
    finally:
        con.close()


def get_duckdb_connection_for_s3(
    path: str,
    load_spatial: bool | None = True,
    s3_endpoint: str | None = None,
    s3_region: str | None = None,
    s3_use_ssl: bool | None = None,
) -> duckdb.DuckDBPyConnection | ProfiledConnection:
    """
    Get DuckDB connection configured for S3 access.

    For S3 paths, uses no credentials by default (works for public buckets).
    If a bucket is known to require auth (from previous attempts), uses
    credential chain. Results are cached per bucket.

    Args:
        path: S3 path to access (used to determine bucket and access mode)
        load_spatial: Whether to load spatial extension (default: True; None
            means only when DuckDB's core lacks GEOMETRY, resolved by
            :func:`get_duckdb_connection`)
        s3_endpoint: Custom S3 endpoint (e.g., 'minio.local:9000')
        s3_region: S3 region for custom endpoints
        s3_use_ssl: Whether to use SSL for the S3 endpoint

    Returns:
        duckdb.DuckDBPyConnection | ProfiledConnection: Configured connection
        with appropriate S3 access (a ProfiledConnection proxy when
        GPIO_DUCKDB_PROFILE is set; see :func:`get_duckdb_connection`)
    """

    s3_kwargs = {"s3_endpoint": s3_endpoint, "s3_region": s3_region, "s3_use_ssl": s3_use_ssl}

    # Non-S3 paths: use standard connection
    if not is_s3_url(path):
        return get_duckdb_connection(
            load_spatial=load_spatial, load_httpfs=needs_httpfs(path), **s3_kwargs
        )

    bucket = _extract_bucket_name(path)

    # If we know this bucket needs auth, use credential chain
    if _bucket_needs_auth(bucket):
        return get_duckdb_connection(
            load_spatial=load_spatial, load_httpfs=True, use_s3_auth=True, **s3_kwargs
        )

    # Try without credentials first (works for public buckets)
    con = get_duckdb_connection(
        load_spatial=load_spatial, load_httpfs=True, use_s3_auth=False, **s3_kwargs
    )
    try:
        # Lightweight test query - DuckDB handles glob patterns natively
        fetch_one(con.execute(f"SELECT 1 FROM read_parquet({sql_path(path)}) LIMIT 1"))
        return con
    except Exception as e:
        con.close()
        if _needs_s3_auth(e):
            # This bucket requires authentication - cache and retry
            _add_bucket_needing_auth(bucket)
            return get_duckdb_connection(
                load_spatial=load_spatial, load_httpfs=True, use_s3_auth=True, **s3_kwargs
            )
        raise


class _DuckDBSchemaWrapper:
    """Wrapper to provide PyArrow-like interface for DuckDB schema info."""

    def __init__(self, schema_info):
        self._columns = root_schema_columns(schema_info)

    def __len__(self):
        return len(self._columns)

    def field(self, i):
        return _DuckDBFieldWrapper(self._columns[i])


class _DuckDBFieldWrapper:
    """Wrapper to provide PyArrow-like interface for a DuckDB column."""

    def __init__(self, col_info):
        self.name = col_info.get("name", "")


_TIMESTAMPTZ = "TIMESTAMP WITH TIME ZONE"
_UTC = datetime.UTC


def _as_subquery(sql: str) -> str:
    """``sql`` ready to sit inside parentheses: its own lines, no trailing ``;``.

    On its own lines so a trailing ``--`` comment cannot swallow the closing
    parenthesis.
    """
    return f"(\n{sql.rstrip().rstrip(';')}\n)"


def describe_query(con, sql: str, params=None) -> list[tuple[str, str]]:
    """``(name, DuckDB type)`` for each output column of ``sql``, without a scan.

    The DuckDB-type view of :func:`query_schema`: for the questions only
    DuckDB's own type names answer (GEOMETRY vs GEOGRAPHY vs BLOB, a GeoArrow
    STRUCT's nesting depth, TIMESTAMP WITH TIME ZONE, an ENUM), which an Arrow
    schema has already translated away. ``DESCRIBE`` binds the query and never
    crosses Arrow, so no column type can make it fail. Raises whatever the
    bind raises; a caller that tolerates a miss catches it itself.
    """
    rows = con.execute(f"DESCRIBE {_as_subquery(sql)}", params).fetchall()
    return [(row[0], str(row[1])) for row in rows]


def geom_from_wkb_sql(expr: str) -> str:
    """SQL decoding the WKB bytes ``expr`` yields into a GEOMETRY.

    The WKB codec is spelled here and nowhere else (#1367 concept 7). Whether a
    column *needs* decoding is ``geometry_detection.resolve_geometry_read``'s
    question; this is only the spelling, for a caller that already knows it
    holds WKB. Malformed bytes raise; wrap the result in ``TRY(...)`` to read
    them as NULL.
    """
    return f"ST_GeomFromWKB({expr})"


def as_wkb_sql(expr: str) -> str:
    """SQL encoding the GEOMETRY ``expr`` yields as WKB bytes (the 1.x carrier)."""
    return f"ST_AsWKB({expr})"


#: A DuckDB type name (``DESCRIBE`` / ``TYPEOF`` / a relation's ``.types``) that is
#: a top-level GEOMETRY or GEOGRAPHY: bare, or ``GEOMETRY('EPSG:3857')`` once it
#: carries a CRS. Anchored at both ends so a STRUCT or LIST type that merely
#: mentions GEOMETRY is not matched. The CRS is a SQL string literal, so an
#: embedded ``'`` arrives doubled (``O''Brien``).
_DUCKDB_GEO_TYPE = re.compile(
    r"^(GEOMETRY|GEOGRAPHY)(?:\(\s*'((?:[^']|'')*)'\s*\))?$", re.IGNORECASE | re.DOTALL
)


def parse_duckdb_geo_type(type_name: object) -> tuple[str, str | None] | None:
    """``("GEOMETRY"|"GEOGRAPHY", crs or None)`` for a DuckDB type name, else None.

    The DuckDB-type twin of :func:`native_geo_type` (which reads *Parquet*
    logical types): the one answer to "is this DESCRIBE/TYPEOF type a geometry,
    and with which CRS?" (#1367 concept 10).
    """
    match = _DUCKDB_GEO_TYPE.match(str(type_name or "").strip())
    if match is None:
        return None
    crs = match.group(2).replace("''", "'") if match.group(2) else None
    return match.group(1).upper(), crs


def duckdb_type_is_geometry(type_name: object, *, allow_crs: bool = True) -> bool:
    """Whether a DuckDB type name is GEOMETRY, with or without a CRS (not GEOGRAPHY).

    ``allow_crs=False`` answers only for a CRS-less ``GEOMETRY``. It is for a
    caller deciding whether to wrap the column in ``ST_AsWKB`` before an Arrow
    export: a CRS-typed ``GEOMETRY('EPSG:4326')`` already crosses Arrow as
    ``geoarrow.wkb`` carrying that CRS, and the wrap would drop it
    (``api.table.read_partition``, the streaming write strategy).
    """
    parsed = parse_duckdb_geo_type(type_name)
    if parsed is None or parsed[0] != "GEOMETRY":
        return False
    return allow_crs or parsed[1] is None


#: DuckDB's signed and unsigned fixed-width integer type names.
_DUCKDB_INTEGER_TYPES = frozenset(
    {
        "TINYINT",
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "HUGEINT",
        "UTINYINT",
        "USMALLINT",
        "UINTEGER",
        "UBIGINT",
        "UHUGEINT",
    }
)
#: Numeric names without parameters; REAL and NUMERIC alias FLOAT and DECIMAL.
_DUCKDB_NUMERIC_TYPES = _DUCKDB_INTEGER_TYPES | {"FLOAT", "REAL", "DOUBLE", "DECIMAL", "NUMERIC"}


def duckdb_type_is_integer(type_name: object) -> bool:
    """Whether a type is a DuckDB fixed-width integer, including HUGEINT variants."""
    base = str(type_name or "").strip().upper()
    return base in _DUCKDB_INTEGER_TYPES


def duckdb_type_is_numeric(type_name: object) -> bool:
    """Whether a DuckDB type name (DESCRIBE / TYPEOF output) is a numeric type.

    Case-insensitive, and a parameterized ``DECIMAL(18,3)`` counts. The one
    answer for the coordinate-column test in ``convert`` and the metric-column
    test in ``process aggregate``, which used to keep two lists that disagreed
    (one lacked ``REAL``, the other ``DECIMAL`` without its prefix test).
    """
    base = str(type_name or "").split("(", 1)[0].strip().upper()
    return base in _DUCKDB_NUMERIC_TYPES


def duckdb_type_contains_geometry(type_name: object) -> bool:
    """Whether a DuckDB type name mentions GEOMETRY at any depth (``STRUCT(g GEOMETRY)``, ``GEOMETRY[]``).

    The nested question beside :func:`duckdb_type_is_geometry`: the GeoJSON
    writers render a nested geometry property differently, so the native writer
    declines any property type that holds one. A plain case-insensitive substring
    test, as the native-writer gate has always used (a struct field merely named
    ``geometry_x`` also counts, which only sends the write to the hand-built writer).
    """
    return "GEOMETRY" in str(type_name or "").upper()


def file_relation(url: str) -> str:
    """The one ``read_parquet`` spelling a whole-file read goes through.

    ``hive_partitioning=false``: a file under a ``key=value`` directory is one
    file, not a partition whose key would become a column of the rewrite.
    """
    return f"read_parquet({sql_path(url)}, hive_partitioning=false)"


def file_schema(url: str, *, geoparquet_conversion: bool = True) -> pa.Schema:
    """A Parquet file's Arrow schema, local or remote: :func:`query_schema` of
    :func:`file_relation` (#1367 concept 17).

    The one "what columns does this file have" probe in DuckDB terms, for a
    caller that cannot use ``pq.read_schema`` (a URL). ``geoparquet_conversion
    =False`` reads the columns as stored (geometry as a BLOB): DuckDB's
    GeoParquet reader rejects a ``crs: null`` block outright, which is what the
    null-CRS repair has to read past. ``url`` is RAW (#718).
    """
    con = get_duckdb_connection(load_spatial=geoparquet_conversion, load_httpfs=needs_httpfs(url))
    try:
        if not geoparquet_conversion:
            con.execute("SET enable_geoparquet_conversion = false")
        return query_schema(con, f"SELECT * FROM {file_relation(url)}")
    finally:
        con.close()


def query_schema(con, sql: str, params=None) -> pa.Schema:
    """The Arrow schema ``sql`` produces, without reading a row (#1367 concept 17).

    The one schema probe: every "what does this query/relation output" question
    gpio asks in Arrow terms comes here. Output names are the ones a written
    file gets (duplicates de-duplicated as ``a``, ``a_1``), and the fields carry
    their extension metadata (CRS, edges).

    DuckDB 1.5 cannot export a VARIANT column over the Arrow C interface
    (``OSError: Not implemented Error: Unsupported Arrow type VARIANT``), so a
    file DuckDB wrote with VARIANT properties used to kill every write path at
    this probe (#1352, #1362). On that one failure it is re-run with a sentinel
    non-struct field standing in for each VARIANT column, so the bbox-covering
    gates still see the column exists and judge a covering declared over it
    exactly as 2.0's real export does. Any other failure raises.
    """
    try:
        return _zero_row_schema(con, f"SELECT * FROM {_as_subquery(sql)} LIMIT 0", params)
    except OSError as exc:
        if "Unsupported Arrow type VARIANT" not in str(exc):
            raise
        return _query_schema_without_variant(con, sql, params)


def _zero_row_schema(con, sql: str, params=None) -> pa.Schema:
    # read_all() drains the (empty) reader, so the connection is free afterwards.
    return con.sql(sql, params=params).arrow().read_all().schema


_VARIANT_TYPE_KEYWORD = re.compile(r"\bVARIANT\b")


def _type_contains_variant(type_str: str) -> bool:
    """Whether a DESCRIBE-rendered DuckDB type carries the ``VARIANT`` keyword.

    Case-sensitive and word-bounded on the uppercase keyword ``DESCRIBE``
    renders. Nested matching is load-bearing — ``STRUCT(a VARIANT)`` and
    ``VARIANT[]`` cannot cross Arrow on DuckDB 1.5 any more than a top-level
    ``VARIANT`` — while a struct *field* merely named ``variant_id`` has no
    word boundary before the ``_`` and a lowercase ``variant`` never matches.
    Shared by the probe fallback below and ``inspect``'s preview: a substring
    match here dropped such a struct from the fallback schema and rendered it
    as JSON text in previews (#1352 review).
    """
    return _VARIANT_TYPE_KEYWORD.search(type_str) is not None


def _query_schema_without_variant(con, sql: str, params=None) -> pa.Schema:
    """The probe's schema with a sentinel standing in for every VARIANT column.

    ``DESCRIBE`` never crosses Arrow, so it names the columns safely; a type
    whose rendering carries the ``VARIANT`` keyword (top-level, or nested in a
    STRUCT/LIST) is exactly the set DuckDB 1.5 refuses to export. Each such
    column stays in the schema as a sentinel non-struct ``binary`` field rather
    than being dropped: ``strip_illegal_bbox_covering`` treats a covering
    naming a column *absent* from the schema as somebody else's question and
    keeps it, so dropping the column let a ``covering.bbox`` declared over a
    VARIANT column ship untouched on 1.5 while 2.0's real export strips it. The
    sentinel gives both versions the same verdict. This path runs only after a
    real VARIANT broke the export.
    """
    import pyarrow as pa

    described = describe_query(con, f"SELECT * FROM {_as_subquery(sql)} LIMIT 0", params)
    kept = [
        quote_identifier(name) for name, type_ in described if not _type_contains_variant(type_)
    ]
    exported = (
        _zero_row_schema(con, f"SELECT {', '.join(kept)} FROM {_as_subquery(sql)} LIMIT 0", params)
        if kept
        else pa.schema([])
    )
    exported_fields = iter(exported)
    return pa.schema(
        [
            pa.field(name, pa.binary()) if _type_contains_variant(type_) else next(exported_fields)
            for name, type_ in described
        ]
    )


def _output_types(con, query: str, params=None) -> list[str]:
    """The SQL type of each output column of ``query`` (a view of :func:`describe_query`)."""
    return [type_ for _, type_ in describe_query(con, query, params)]


def _without_timestamptz(
    con, query: str, params, column_types: Sequence[str] | None
) -> tuple[str, list[int]]:
    """``query`` with its TIMESTAMPTZ columns as UTC TIMESTAMPs, and their positions.

    ``column_types`` spares the DESCRIBE when the caller already knows them.
    Positional references (``#n``) keep duplicate output names, such as two bare
    ``MIN(...)``, apart. The query sits on its own lines so a trailing ``--``
    comment cannot swallow the closing parenthesis.
    """
    if column_types is None:
        column_types = _output_types(con, query, params)
    positions = [i for i, type_ in enumerate(column_types) if type_ == _TIMESTAMPTZ]
    if not positions:
        return query, positions
    select = ", ".join(
        f"timezone('UTC', #{i + 1})" if i in positions else f"#{i + 1}"
        for i in range(len(column_types))
    )
    return f"SELECT {select} FROM (\n{query.rstrip().rstrip(';')}\n)", positions


def _session_zone(con) -> datetime.tzinfo:
    """The connection's TimeZone setting as a tzinfo, UTC when Python cannot resolve it."""
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

    (name,) = fetch_one(con.execute("SELECT current_setting('TimeZone')"))
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        return _UTC


def _utc_as_zone(value, zone: datetime.tzinfo):
    """A naive UTC value as an aware datetime in ``zone``, the way ``fetchone()`` gives it.

    Infinities stay as ``fetchone()`` clamps them (naive ``datetime.max``/``min``).
    A BC or year 10000+ value is DuckDB's UTC text, and keeps the ``+00`` that
    ``fetchone()`` gives it, so read back as a literal it means the same instant
    in any session time zone. A value too close to year 1 or 9999 to shift into
    ``zone`` stays in UTC, where ``fetchone()`` raises.
    """
    if isinstance(value, str):
        return f"{value}+00"
    if not isinstance(value, datetime.datetime) or value in (
        datetime.datetime.max,
        datetime.datetime.min,
    ):
        return value
    aware = value.replace(tzinfo=_UTC)
    try:
        return aware.astimezone(zone)
    except OverflowError:  # within hours of year 1 or 9999
        return aware


def _restore_zones(rows: list[tuple], positions: list[int], zone) -> list[tuple]:
    if not positions:
        return rows
    return [
        tuple(_utc_as_zone(v, zone) if i in positions else v for i, v in enumerate(row))
        for row in rows
    ]


def fetch_rows(
    con, query: str, params=None, *, column_types: Sequence[str] | None = None
) -> list[tuple]:
    """Run ``query`` and return every row as ``fetchall()`` would, without needing pytz.

    Use this instead of ``fetchall()`` whenever a row can carry a user column's
    value. DuckDB's Python conversion of a TIMESTAMP WITH TIME ZONE imports
    ``pytz``, which gpio does not depend on. Those columns are fetched as UTC
    TIMESTAMPs and made aware in the session time zone with ``zoneinfo``. Every
    other column goes through ``fetchall()`` untouched. A TIMESTAMPTZ nested in
    a STRUCT, LIST or MAP still needs pytz, as it does with ``fetchall()``.

    Finding those columns costs a DESCRIBE of ``query``. A caller that already
    knows the output types passes them as ``column_types`` to skip it.
    """
    rewritten, positions = _without_timestamptz(con, query, params, column_types)
    zone = _session_zone(con) if positions else None
    return _restore_zones(con.execute(rewritten, params).fetchall(), positions, zone)


def fetch_row(
    con, query: str, params=None, *, column_types: Sequence[str] | None = None
) -> tuple | None:
    """The first row of ``query`` as :func:`fetch_rows` builds it, or None when it is empty."""
    rewritten, positions = _without_timestamptz(con, query, params, column_types)
    zone = _session_zone(con) if positions else None
    row = fetch_one(con.execute(rewritten, params))
    if row is None:
        return None
    return _restore_zones([row], positions, zone)[0]


def _get_query_columns(con, query: str) -> list[str]:
    """Output column names of ``query`` (a view of :func:`describe_query`)."""
    return [name for name, _ in describe_query(con, query)]


def _get_query_column_type(con, query: str, column_name: str) -> str | None:
    """The DuckDB type of one output column of ``query``, or None if absent or unbindable."""
    try:
        return next((t for name, t in describe_query(con, query) if name == column_name), None)
    except Exception:
        return None


# Clauses that must not follow a trailing ORDER BY for it to be safely strippable
# (they change which/how many rows the query returns at all).
_ORDER_BY_TAIL_STOPWORDS = re.compile(r"(?i)\b(limit|offset|union|except|intersect|fetch)\b")

# Constructs the scanner below cannot reason about. It tracks '' and "" state and
# nothing else, which is the hand-rolled quote walker #657 replaced on the
# --where path: line comments, block comments, dollar quoting and E'' escapes all
# hide (or fake) text from it. Measured against this scanner, each one mangles
# the query --
#     SELECT * FROM t -- keep order by x   ->  SELECT * FROM t --
#     SELECT * FROM t /* order by x */     ->  SELECT * FROM t /*
#     SELECT $$order by x$$ AS s FROM t    ->  SELECT $$
#     ... WHERE s = E'\'order by x'         ->  ... WHERE s = E'\'
# -- and the last is the dangerous one: it stays *valid* SQL with a different
# WHERE clause, so the caller would read a different row set instead of failing
# loudly.
#
# There is no public DuckDB AST to gate this properly (extract_statements only
# splits statements), so the scanner stays, and any query carrying one of these
# is left alone: the optimization is skipped and the caller still works.
_UNSCANNABLE_SQL = re.compile(r"--|/\*|\$\$|\$[A-Za-z_][A-Za-z0-9_]*\$|(?<![A-Za-z0-9_])[eE]'")


def _strip_trailing_order_by(query: str) -> str:
    """Drop a trailing top-level ``ORDER BY`` clause from a query.

    For a caller whose own result does not depend on row order. Two use it:

    * row-group sizing samples rows to estimate bytes-per-row, and a ``LIMIT``
      layered over an ``ORDER BY`` forces DuckDB to scan (and sort) the *entire*
      source -- re-downloading remote inputs and recomputing expensive ordering
      keys (e.g. ``ST_Hilbert``) just to size row groups. Without the ordering
      the sample's ``LIMIT`` pushes down, so DuckDB stops after a few row groups.
    * the derived-stats scan computes ``bbox`` and ``geometry_types``, both
      aggregates over every row, and DuckDB keeps the ``ORDER_BY`` operator in
      the plan of an ordered subquery -- so every write whose carried stats had
      to be recomputed sorted its input twice, once to measure and once to
      ``COPY`` (#1177).

    Returns the query unchanged when no strippable top-level trailing ORDER BY is
    found, so the caller still works -- it just skips the speed-up. The same is
    true for a query carrying any construct this scanner cannot track (see
    ``_UNSCANNABLE_SQL``): skipping the speed-up is always safe, guessing is not.
    An ``ORDER BY`` a ``LIMIT``/``OFFSET`` depends on decides *which* rows exist,
    so ``_ORDER_BY_TAIL_STOPWORDS`` keeps that one too.
    """
    if _UNSCANNABLE_SQL.search(query):
        return query

    depth = 0
    in_single = in_double = False
    last_order_by = -1
    i, n = 0, len(query)
    while i < n:
        ch = query[i]
        if in_single:
            in_single = ch != "'"
        elif in_double:
            in_double = ch != '"'
        elif ch == "'":
            in_single = True
        elif ch == '"':
            in_double = True
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0 and ch in "oO":
            match = re.match(r"order\s+by\b", query[i:], re.IGNORECASE)
            prev = query[i - 1] if i else " "
            if match and not (prev.isalnum() or prev == "_"):
                last_order_by = i
                i += match.end()
                continue
        i += 1

    if last_order_by == -1:
        return query
    # Bail if anything follows the ORDER BY that would alter which rows come back.
    if _ORDER_BY_TAIL_STOPWORDS.search(query[last_order_by:]):
        return query
    return query[:last_order_by].rstrip()


# Maps GeoArrow encoding name → number of flatten() calls needed to reach list[struct].
# bracket_depth in DuckDB type string = flatten_depth + 1 for non-point types.
_GEOARROW_FLATTEN_DEPTH = {
    "point": -1,  # plain STRUCT(x, y), no list wrapper
    "linestring": 0,
    "multipoint": 0,
    "polygon": 1,
    "multilinestring": 1,
    "multipolygon": 2,
}


def _geoarrow_coord_exprs(quoted_geom: str, encoding: str) -> tuple:
    """Return (xmin, ymin, xmax, ymax, centroid_x, centroid_y) SQL expressions
    for a GeoArrow native geometry column, for per-row use in a SELECT list.

    Uses list_transform + flatten to extract coordinates, then list_min/max/avg.
    Unknown encodings fall back to depth 0 (linestring-style, no flatten).
    """
    depth = _GEOARROW_FLATTEN_DEPTH.get(encoding.lower(), 0)

    if depth == -1:
        x = f"{quoted_geom}.x"
        y = f"{quoted_geom}.y"
        return x, y, x, y, x, y

    flat = quoted_geom
    for _ in range(depth):
        flat = f"flatten({flat})"

    x_arr = f"list_transform({flat}, lambda p: p.x)"
    y_arr = f"list_transform({flat}, lambda p: p.y)"
    x_min = f"list_min({x_arr})"
    x_max = f"list_max({x_arr})"
    y_min = f"list_min({y_arr})"
    y_max = f"list_max({y_arr})"
    return (
        x_min,
        y_min,
        x_max,
        y_max,
        f"({x_min} + {x_max}) / 2.0",
        f"({y_min} + {y_max}) / 2.0",
    )


def _wrap_query_with_wkb_conversion(query: str, geometry_column: str | Iterable[str] | None) -> str:
    """Wrap *query* so each named geometry column comes out as WKB, in place.

    The one rule (#1367 HANDOFFS row 75): DuckDB's Arrow export hands geometry
    over in its own format, so a write or a stream converts it with
    ``ST_AsWKB``. ``SELECT * REPLACE`` keeps the query's column order and
    needs no column discovery; the copy this replaces fell back to
    ``EXCLUDE`` + append without a connection, moving the geometry last.

    Args:
        query: Original SQL query
        geometry_column: One geometry column, several, or ``None`` to leave
            the query as it is
    """
    if not geometry_column:
        return query
    columns = [geometry_column] if isinstance(geometry_column, str) else list(geometry_column)
    if not columns:
        return query
    replacements = ", ".join(
        f"{as_wkb_sql(quote_identifier(name))} AS {quote_identifier(name)}" for name in columns
    )
    return f"SELECT * REPLACE ({replacements}) FROM ({query})"


def _wrap_query_with_blob_conversion(
    query: str,
    geometry_column: str,
    con=None,
    *,
    secondary_columns: Iterable[str] | None = None,
) -> str:
    """
    Wrap query to convert geometry column(s) to plain binary BLOB.

    Unlike _wrap_query_with_wkb_conversion which produces WKB that DuckDB still
    recognizes as spatial, this casts to BLOB to produce truly plain binary data.
    Used for GeoParquet v1.x where we need plain binary WKB without geoarrow
    extension types in the Parquet schema.

    Args:
        query: Original SQL SELECT query
        geometry_column: Name of the primary geometry column to convert
        con: Optional DuckDB connection to verify column exists
        secondary_columns: Other columns a v1.x file declares as geometry. A file
            may carry more than one geometry column, and every one of them that
            DuckDB reads as GEOMETRY would otherwise be written back as a native
            Parquet GEOMETRY logical type (#712). Only names that DuckDB actually
            typed as GEOMETRY are cast: `geo["columns"]` names columns by
            *declaration*, so a column stored as plain BLOB would reach
            ST_AsWKB(BLOB), which does not bind and would abort the whole write,
            while ST_AsWKB(VARCHAR) does bind and would silently reinterpret a
            text column as WKT. Requires ``con`` — without it the types are
            unknown and the secondaries are left alone.

    Returns:
        str: Wrapped query with BLOB conversion, or original query if there is
        nothing to convert
    """
    # If connection provided, check which columns exist in the query output
    col_info: dict[str, str] = {}
    if con is not None:
        try:
            col_info = dict(describe_query(con, query))
        except Exception:
            col_info = {}

    targets: list[str] = []
    if col_info:
        # Only a real GEOMETRY needs ST_AsWKB, and the same test as the
        # secondaries below. "Not a STRUCT" was too loose: a BLOB primary is
        # already the correct 1.x carrier and ST_AsWKB(BLOB) does not bind, so
        # the whole write aborted; worse, ST_AsWKB(VARCHAR) *does* bind and
        # silently reinterprets the text as WKT.
        if duckdb_type_is_geometry(col_info.get(geometry_column)):
            targets.append(geometry_column)
    else:
        # DESCRIBE failed, so we know nothing about the types. Convert the
        # primary and let DuckDB reject it if it is not geometry -- the
        # historical behaviour, and the only safe guess without type info.
        targets.append(geometry_column)

    for name in secondary_columns or ():
        if name in targets or name == geometry_column:
            continue
        # A STRUCT is a bbox-style column, not geometry: the owner excludes it.
        if duckdb_type_is_geometry(col_info.get(name)):
            targets.append(name)

    if not targets:
        return query

    # Cast to BLOB to produce plain binary without geoarrow extension type
    # Use SELECT * REPLACE to preserve column order
    replacements = ", ".join(
        f"{as_wkb_sql(quote_identifier(col))}::BLOB AS {quote_identifier(col)}" for col in targets
    )
    return f"""
        WITH __arrow_source AS ({query})
        SELECT * REPLACE ({replacements})
        FROM __arrow_source
    """


# ---------------------------------------------------------------------------
# Registering an Arrow table: the one step that hands DuckDB in-memory geometry
# ---------------------------------------------------------------------------

_EXT_NAME = b"ARROW:extension:name"
_EXT_META = b"ARROW:extension:metadata"


def geoarrow_extension(field) -> tuple[str | None, dict]:
    """A field's GeoArrow extension name and parsed metadata, in either carrier.

    pyarrow resolves the type when ``geoarrow.pyarrow`` is imported and leaves
    it as storage plus field metadata when it is not; DuckDB reads both.
    """
    ext_type = field.type
    name = getattr(ext_type, "extension_name", None)
    if name is not None:
        raw = ext_type.__arrow_ext_serialize__()
    else:
        metadata = field.metadata or {}
        name = metadata.get(_EXT_NAME, b"").decode("utf-8") or None
        raw = metadata.get(_EXT_META)
    if not name or not name.startswith("geoarrow."):
        return None, {}
    try:
        parsed = json.loads(raw) if raw else {}
    except ValueError:
        parsed = {}
    return name, parsed if isinstance(parsed, dict) else {}


def _storage(column):
    """A column's storage chunks, whether or not its extension type is resolved."""
    import pyarrow as pa

    if isinstance(column.type, pa.ExtensionType):
        return pa.chunked_array(
            [chunk.storage for chunk in column.chunks], column.type.storage_type
        )
    return column


def _planar_column(field, column, name: str, parsed: dict) -> tuple:
    """``(field, column)`` relabelled planar, over the same buffers and CRS.

    A resolved GeoArrow type stays resolved (its ``with_edge_type``); one that
    pyarrow left as storage plus field metadata loses the ``edges`` key there.
    """
    import pyarrow as pa

    if hasattr(field.type, "with_edge_type"):
        target = field.type.with_edge_type(geoarrow_edge_type(PLANAR))
        chunks = [pa.ExtensionArray.from_storage(target, c.storage) for c in column.chunks]
        return field.with_type(target), pa.chunked_array(chunks, target)
    storage = _storage(column)
    metadata = dict(field.metadata or {})
    metadata[_EXT_NAME] = name.encode("utf-8")
    metadata[_EXT_META] = json.dumps(without_edges(parsed)).encode("utf-8")
    return pa.field(field.name, storage.type, field.nullable, metadata), storage


def _planar_view(table, columns: Iterable[str] | None = None) -> tuple:
    """``table`` with each non-planar GeoArrow column relabelled planar.

    Zero-copy: only the ``edges`` value changes. ``columns`` limits it to
    those names. Returns the view and the input fields it relabelled, keyed
    by name. A field whose metadata does not parse is left as it is.
    """
    wanted = None if columns is None else set(columns)
    originals: dict = {}
    for index, field in enumerate(table.schema):
        if wanted is not None and field.name not in wanted:
            continue
        # The type only, by design: DuckDB never reads a ``geo`` block, so which
        # columns it refuses is a type question, relabelled even when a block
        # says planar or the value is a typo.
        name, parsed = geoarrow_extension(field)
        if not is_geodesic(geoarrow_type_edges(field.type, parsed)):
            continue
        originals[field.name] = field
        planar_field, column = _planar_column(field, table.column(index), name, parsed)
        table = table.set_column(index, planar_field, column)
    return table, originals


class RegisteredArrow:
    """What :func:`register_arrow` registered, and how to give the edges back.

    DuckDB has no GEOGRAPHY type: a registered column is a planar GEOMETRY
    there. An operation that passes a geometry column through unchanged calls
    :meth:`restore_edges` on its result. One that computes new geometry in
    planar math (an aggregation's cells, a reprojection, a planar repair or
    simplification) must not: the edges would then describe shapes that were
    never built on the sphere.
    """

    def __init__(self, name: str, fields: dict) -> None:
        self.name = name
        self.fields = fields

    def read_back(self, con, query: str, metadata: dict | None):
        """``query``'s rows as an Arrow table that passes the input's rows through.

        For a query that reorders or filters the registered rows and keeps their
        geometry (a sort): the result gets the caller's input ``metadata`` --
        its ``geo`` key is the write facade's witness for the table, which
        decides the output's block when it is written -- and every pass-through
        geometry column its registered field back (:meth:`restore_edges`).
        """
        result = con.sql(query).arrow().read_all()
        if metadata:
            result = result.replace_schema_metadata(metadata)
        return self.restore_edges(result)

    @property
    def edges(self) -> dict[str, str]:
        """``{column: edges}`` for every column that was registered as planar."""
        return {
            col: edges
            for col, field in self.fields.items()
            if (edges := geoarrow_type_edges(field.type, geoarrow_extension(field)[1]))
        }

    def restore_edges(self, result, columns: Iterable[str] | None = None):
        """``result`` with each pass-through geometry column given its input field back.

        Restores the input's exact field (type, CRS, edges, field metadata) on
        every registered non-planar column that ``result`` still holds as WKB
        bytes; ``columns`` limits it to those names. A column that came back as
        anything but binary (text, a struct) is left alone.

        It matches by name only. The caller must know that each restored name
        still holds the input column's geometry. A query that puts other data
        under a registered name (``SELECT pt AS geometry``) would get the input
        field, edges and all. Nothing in the result tells the two apart (same
        storage, often the same CRS), so pass ``columns`` or skip the restore.
        """
        import pyarrow as pa

        names = self.fields if columns is None else [c for c in columns if c in self.fields]
        for col in names:
            if col not in result.column_names:
                continue
            original = self.fields[col]
            index = result.schema.get_field_index(col)
            storage = _storage(result.column(index))
            if not (pa.types.is_binary(storage.type) or pa.types.is_large_binary(storage.type)):
                continue
            target = original.type
            is_ext = isinstance(target, pa.ExtensionType)
            storage = storage.cast(target.storage_type if is_ext else target)
            if is_ext:
                storage = pa.chunked_array(
                    [pa.ExtensionArray.from_storage(target, c) for c in storage.chunks], target
                )
            result = result.set_column(index, original, storage)
        return result


def register_arrow(con, name: str, table) -> RegisteredArrow:
    """Register the Arrow ``table`` on ``con`` as ``name``, non-planar edges and all.

    The one way gpio hands an in-memory table to DuckDB (#1253). DuckDB 1.5
    refuses a ``geoarrow.wkb`` column whose ``edges`` is anything but
    ``"planar"`` ("Can't import non-planar edges") and has no setting to accept
    one. So each such column is registered as a planar view of the same
    buffers, and the returned :class:`RegisteredArrow` keeps the input fields
    for :meth:`RegisteredArrow.restore_edges`. The caller's table is unchanged.

    Every non-planar string is relabelled, not only a GeoParquet edge
    algorithm, because DuckDB rejects them all. Which values gpio honours is
    the restore side's question (``edges.GEOGRAPHY_EDGE_ALGORITHMS``).
    """
    view, registered = planar_view(table, name)
    if registered.fields:
        debug(
            f"Registering {', '.join(sorted(registered.fields))} on DuckDB as planar "
            "GEOMETRY: DuckDB cannot import non-planar edges (#1253)"
        )
    con.register(name, view)
    return registered


def planar_view(table, name: str = "table", columns: Iterable[str] | None = None) -> tuple:
    """``(view, RegisteredArrow)``: the planar view DuckDB can read, and the way back.

    For the paths that hand DuckDB a table some other way than ``register``
    (a temp Parquet file read back with ``read_parquet``, a write strategy):
    DuckDB reads a GEOGRAPHY column as planar GEOMETRY, so the result needs
    the same restore. ``columns`` limits the relabel to those names.
    """
    view, fields = _planar_view(table, columns)
    return view, RegisteredArrow(name, fields)


def computed_column_query(source, column_name, sql_expression, replace_column=None):
    """The SELECT that adds (or replaces) one computed column on ``source``.

    ``source`` is a SQL-ready relation. The column name is quoted here (it may
    hold colons, as in ``metrics:area``), and ``replace_column`` is dropped with
    EXCLUDE so the new column takes its place.
    """
    quoted_col = quote_identifier(column_name)
    if replace_column:
        return f"""
        SELECT
            * EXCLUDE ({quote_identifier(replace_column)}),
            {sql_expression} AS {quoted_col}
        FROM {source}
    """
    return f"""
        SELECT
            *,
            {sql_expression} AS {quoted_col}
        FROM {source}
    """
