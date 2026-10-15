"""
DuckDB connection management utilities.

This module provides functions for creating and managing DuckDB connections
with appropriate extensions loaded for GeoParquet operations.
"""

import datetime
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
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, TypeVar

import duckdb

from geoparquet_io.core.exceptions import ExtensionUnavailableError, ValidationError
from geoparquet_io.core.logging_config import debug, warn
from geoparquet_io.core.parquet_schema import root_schema_columns
from geoparquet_io.core.remote import is_s3_url, needs_httpfs

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
    """Run ``func`` with its ``profile`` argument as the ambient AWS profile.

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
    if "profile" not in signature.parameters:
        raise TypeError(f"{func.__qualname__} has no 'profile' parameter")

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        arguments = signature.bind_partial(*args, **kwargs).arguments
        profile = arguments.get("profile")
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
        params.append(f"PROFILE '{_escape_sql_string(profile)}'")
    endpoint = settings.get("endpoint")
    if endpoint:
        params.append(f"ENDPOINT '{_escape_sql_string(endpoint)}'")
        params.append("URL_STYLE 'path'")
        params.append(f"USE_SSL {'false' if settings.get('use_ssl') is False else 'true'}")
    region = settings.get("region")
    if region:
        params.append(f"REGION '{_escape_sql_string(region)}'")
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
        con.execute(f"SET s3_endpoint='{_escape_sql_string(endpoint)}';")
        con.execute("SET s3_url_style='path';")
        ssl_value = "false" if settings.get("use_ssl") is False else "true"
        con.execute(f"SET s3_use_ssl={ssl_value};")

    region = settings.get("region")
    if region:
        con.execute(f"SET s3_region='{_escape_sql_string(region)}';")


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
    error_str = str(exception).lower()
    # 403 without credentials means we need to authenticate
    auth_indicators = ["403", "forbidden", "access denied", "unauthorized"]
    return any(ind in error_str for ind in auth_indicators)


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


_SETTING_KEY_RE = re.compile(r"[a-z_]+")
# A size as current_setting() displays it: "2.7 GiB", "953.6 MiB".
_DISPLAYED_SIZE_RE = re.compile(r"^(\d+)\.(\d+) ([KMGT]iB)$")


def _nudged_size(displayed: str) -> str | None:
    """A size that DuckDB displays back as ``displayed``; None if it is not a size.

    ``current_setting`` truncates a size to one decimal, so writing the display
    string back lands a step low ("2.7 GiB" reads back "2.6 GiB") and a loop of
    save/restore drifts down. Half a displayed step up reads back unchanged.
    """
    match = _DISPLAYED_SIZE_RE.match(displayed)
    if not match:
        return None
    whole, frac, unit = match.groups()
    return f"{whole}.{frac}5{unit}"


def _displayed_size_max(displayed: str) -> str | None:
    """The largest size DuckDB would display as ``displayed``; None if it is not a size.

    The same truncation seen from the other side: "7.4 GiB" is any size from
    7.4 GiB up to 7.5 GiB, so a *comparison* against a number gpio itself set has
    to use the top of that range. Against the bottom, the 8.0GB limit gpio puts
    on a connection reads back as 7.4 GiB = 7.95e9 bytes and looks like a
    stricter limit somebody else chose. :func:`_nudged_size` takes the middle of
    the range instead, because a restore has to read back unchanged rather than
    err high.
    """
    match = _DISPLAYED_SIZE_RE.match(displayed)
    if not match:
        return None
    whole, frac, unit = match.groups()
    step = 10 ** -len(frac)
    return f"{float(f'{whole}.{frac}') + step:.{len(frac)}f}{unit}"


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
    for key, value in saved.items():
        if not _SETTING_KEY_RE.fullmatch(key):
            raise ValueError(f"Not a DuckDB setting name: {key!r}")
        try:
            _set_setting(con, key, value)
            if _current_setting(con, key) == value:
                continue
            con.execute(f"RESET {key}")
            nudged = _nudged_size(value) if isinstance(value, str) else None
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
    if isinstance(exc, (KeyboardInterrupt, duckdb.InterruptException)):
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


def _current_setting(con: duckdb.DuckDBPyConnection, key: str) -> object:
    return con.execute(f"SELECT current_setting('{key}')").fetchone()[0]


def _set_setting(con: duckdb.DuckDBPyConnection, key: str, value: object) -> None:
    if isinstance(value, str):
        con.execute(f"SET {key} = '{_escape_sql_string(value)}'")
    else:
        con.execute(f"SET {key} = {value}")


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
    :func:`~geoparquet_io.core.file_utils.resolve_file_url` resolved it. It must
    NOT be a :func:`~geoparquet_io.core.file_utils.safe_file_url` result, which
    is already escaped; escaping is not idempotent, so a second pass doubles the
    doubling. No guard can catch that automatically -- a filename may legally
    contain two apostrophes in a row -- so the rule is structural: a path is
    escaped exactly once, either by ``safe_file_url`` (which also resolves
    remote URLs and checks existence, and whose bare result the caller quotes)
    or by ``sql_path``, never by both.

    For a non-path SQL string literal use :func:`_escape_sql_string` and supply
    your own quotes; for an identifier use :func:`quote_identifier`.

    A ``PathLike`` is accepted as well as a ``str``: callers hold ``Path``
    objects just as often, and ``str.replace`` on a ``Path`` is the completely
    unrelated *filesystem rename*, so coercing here is what keeps every call
    site from having to remember ``str()``.

    Args:
        path: RAW, unescaped file path or URL, as ``str`` or ``PathLike``.

    Returns:
        The path as a quoted, escaped SQL string literal.
    """
    return f"'{_escape_sql_string(os.fspath(path))}'"


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
        f"'{_escape_sql_string(key)}': '{_escape_sql_string(value)}'"
        for key, value in pairs.items()
    )
    return f"KV_METADATA {{{body}}}"


def validate_compression_level(value: int) -> int:
    """Validate a compression level before interpolating it into SQL.

    ``COMPRESSION_LEVEL`` takes no parameter binding, so the value is formatted
    straight into the ``COPY … (…)`` option list. The CLI constrains it with
    ``click.IntRange(1, 22)``, but ``write_parquet_with_metadata`` and the write
    strategies are public entry points that a Python caller reaches directly,
    where nothing has checked it. DuckDB's ``execute`` runs multi-statement
    strings, so an unvalidated value is an injection surface as well as a
    confusing error.

    ``bool`` is rejected explicitly: it is an ``int`` subclass, and
    ``COMPRESSION_LEVEL True`` is not something a caller meant.

    Args:
        value: Candidate ZSTD compression level.

    Returns:
        The level as an ``int``.

    Raises:
        ValueError: If the value is not an integer in DuckDB's 1-22 range.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            f"Invalid compression_level {value!r}; expected an integer between 1 and 22."
        )
    if not 1 <= value <= 22:
        raise ValueError(
            f"Invalid compression_level {value}; expected an integer between 1 and 22."
        )
    return value


def quote_identifier(name: str) -> str:
    """
    Quote a SQL identifier for safe use in DuckDB queries.

    Escapes embedded double quotes by doubling them, then wraps in double quotes.
    This handles column/table names with spaces, special characters, reserved words,
    or uppercase letters that need to be preserved.

    The input must be a **bare** identifier, exactly as it appears in the file's
    schema (e.g. from ``find_primary_geometry_column`` or ``table.column_names``).
    The function is deliberately not idempotent: quoting an already-quoted name
    yields an identifier that literally contains double quotes, so never pass it
    something already quoted.

    For a SQL *string literal* (e.g. ``WHERE path_in_schema = '...'``) use
    :func:`_escape_sql_string` instead; the two escapes are not interchangeable.

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


def free_column_name(base: str, column_names) -> str:
    """``base``, or ``base_1``/``base_2``/... when ``column_names`` already takes it.

    A computed column aliased ``AS base`` next to ``SELECT *`` does not fail
    when the source already has that name: DuckDB silently renames one of the
    two (``base_1``), and everything that goes on to say ``base`` then binds to
    the *other* column. That is how a computed bbox came to be written under a
    name its ``covering`` did not point at (#1079), and how the geometry built
    from a CSV's WKT column came to be the source's own VARCHAR column of that
    name (#1176). Picking the free name up front makes the alias and every later
    reference one variable, agreeing by construction.

    The comparison is case-insensitive because DuckDB binds identifiers that
    way: a source column ``BBOX`` takes the name ``bbox`` just as surely, even
    though Parquet itself would keep the two apart.

    Args:
        base: The name the computed column would like
        column_names: The names it is emitted beside

    Returns:
        ``base`` when it is free, otherwise the first free ``base_<n>``
    """
    taken = {str(name).lower() for name in column_names}
    candidate = base
    suffix = 0
    while candidate.lower() in taken:
        suffix += 1
        candidate = f"{base}_{suffix}"
    return candidate


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

    Returns:
        A SQL boolean expression for use directly after ``ON``.
    """
    qt_geom = quote_identifier(target_geom_col)

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
    except Exception:  # pragma: no cover - psutil is a hard dependency
        return True


def orphaned_spill_dirs(base_dir: str | os.PathLike) -> list[str]:
    """Spill directories under ``base_dir`` whose owning process has exited.

    Ownership is decided by the pid in the name, never by age: a gpio run that
    takes six hours must not have its scratch space swept out from under it.
    Pid reuse can only make this *more* conservative -- a recycled pid reads as
    alive, so the leftover simply survives another round.

    The one case pids cannot decide is two *pid namespaces* sharing one spill
    volume: point ``TMPDIR`` at the same bind-mounted directory from two
    containers and each sees the other's pids as absent. Give each container its
    own ``TMPDIR`` (the default already does) if you share a volume between them.
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
        if _process_is_alive(int(match.group(1))):
            continue
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
    for path in orphaned_spill_dirs(base_dir):
        try:
            shutil.rmtree(path)
        except OSError:
            continue
        removed.append(path)
    return removed


def spill_directory(base_dir: str | os.PathLike | None = None) -> str:
    """Path to a private directory DuckDB may spill intermediate results into.

    The path is *not* created. DuckDB creates its temp directory the first time
    a query exceeds ``memory_limit``, and removes it again once the query
    completes -- so a connection that never spills leaves nothing behind, and an
    ordinary run, error or not, cleans up after itself. A run *killed* mid-spill
    does not, which is why every call first sweeps ``base_dir`` for the leavings
    of processes that are gone (see :func:`sweep_orphaned_spill_dirs`). The
    sweep is best effort and never raises.

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
            is too small to hold a large sort's spill. Not validated here --
            DuckDB creates the leaf lazily, so a base that is missing, read-only
            or not a directory surfaces as an ``IO Error`` naming the path at
            the first spill rather than at this call. Callers that name a base
            create it first.

    Returns:
        An absolute path, safe to hand to ``SET temp_directory``.
    """
    base = os.fspath(base_dir) if base_dir is not None else tempfile.gettempdir()
    sweep_orphaned_spill_dirs(base)
    return os.path.join(base, f"{_SPILL_DIR_PREFIX}{os.getpid()}-{uuid.uuid4().hex[:12]}")


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
INPUT_FILE_DUCKDB_ERRORS = (
    duckdb.InvalidInputException,
    duckdb.IOException,
)


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
    return isinstance(exc, INPUT_FILE_DUCKDB_ERRORS)


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
    if limit is not None:
        con.execute(f"SET memory_limit = '{_escape_sql_string(str(limit))}';")
    if cap is None or threads is not None:
        return
    if cap < int(str(_current_setting(con, "threads"))):
        con.execute(f"SET threads = {cap}")


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
    read until it returns ``None``). A result read with a single
    ``fetchone()``, or abandoned when the next statement runs, writes no
    profile. DDL and ``SET`` write none either. So a command's profile
    directory holds its result-draining queries — usually the heavy ones —
    not every statement it ran.

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

    def __init__(self, con: "duckdb.DuckDBPyConnection", directory: str):
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
        self._con.execute(f"SET profiling_output = '{_escape_sql_string(target)}';")

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
        load_spatial: Whether to load spatial extension (default: True)
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
    safe_temp_dir = _escape_sql_string(str(effective_temp_dir))
    con.execute(f"SET temp_directory = '{safe_temp_dir}';")
    _apply_connection_limits(con, memory_limit, threads)

    # Always load spatial extension by default (core use case)
    if load_spatial:
        _install_and_load_extension(con, "spatial")
        # DuckDB 1.5+: ensure lon/lat = x/y axis order globally.
        # Replaces per-call always_xy := true in ST_Transform.
        con.execute("SET geometry_always_xy = true;")

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

    _opt_out_of_extension_telemetry()
    try:
        con.execute(f"INSTALL {name} FROM community;")
        con.execute(f"LOAD {name};")
    except duckdb.Error as e:
        raise ExtensionUnavailableError(name, duckdb.__version__, str(e), feature=feature) from e


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
    con = duckdb.connect()
    try:
        load_community_extension(con, name, feature=feature)
    finally:
        con.close()


def get_duckdb_connection_for_s3(
    path: str,
    load_spatial: bool = True,
    s3_endpoint: str | None = None,
    s3_region: str | None = None,
    s3_use_ssl: bool | None = None,
) -> "duckdb.DuckDBPyConnection | ProfiledConnection":
    """
    Get DuckDB connection configured for S3 access.

    For S3 paths, uses no credentials by default (works for public buckets).
    If a bucket is known to require auth (from previous attempts), uses
    credential chain. Results are cached per bucket.

    Args:
        path: S3 path to access (used to determine bucket and access mode)
        load_spatial: Whether to load spatial extension (default: True)
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
    if not path.startswith(("s3://", "s3a://")):
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
        con.execute(f"SELECT 1 FROM read_parquet({sql_path(path)}) LIMIT 1").fetchone()
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
_UTC = datetime.timezone.utc


def _output_types(con, query: str, params=None) -> list[str]:
    """The SQL type of each output column of ``query``. DESCRIBE binds it without a scan."""
    return [column[1] for column in con.execute(f"DESCRIBE {query}", params).fetchall()]


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

    (name,) = con.execute("SELECT current_setting('TimeZone')").fetchone()
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
    row = con.execute(rewritten, params).fetchone()
    if row is None:
        return None
    return _restore_zones([row], positions, zone)[0]


def _get_query_columns(con, query: str) -> list[str]:
    """
    Get column names from a SQL query without executing it fully.

    Args:
        con: DuckDB connection
        query: SQL query to analyze

    Returns:
        List of column names in the query result
    """
    result = con.execute(f"DESCRIBE ({query})").fetchall()
    return [row[0] for row in result]


def _get_query_column_type(con, query: str, column_name: str) -> str | None:
    """Return the DuckDB type string for a named column in a query, or None."""
    try:
        rows = con.execute(f"DESCRIBE ({query})").fetchall()
        for row in rows:
            if row[0] == column_name:
                return row[1]
    except Exception:
        pass
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

    x_arr = f"list_transform({flat}, p -> p.x)"
    y_arr = f"list_transform({flat}, p -> p.y)"
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


def _wrap_query_with_wkb_conversion(query: str, geometry_column: str, con=None) -> str:
    """
    Wrap a query to convert geometry column to WKB format.

    Args:
        query: Original SQL query
        geometry_column: Name of the geometry column
        con: Optional DuckDB connection for column discovery

    Returns:
        Modified query with geometry converted to WKB
    """
    quoted_geom = quote_identifier(geometry_column)

    if con:
        try:
            columns = _get_query_columns(con, query)
            # In the query's own order: appending the geometry moved a column
            # after it (a bbox) ahead of it in the written file.
            is_geom = [c.lower() == geometry_column.lower() for c in columns]
            select = [
                f"ST_AsWKB({quoted_geom}) AS {quoted_geom}" if geom else quote_identifier(c)
                for c, geom in zip(columns, is_geom, strict=True)
            ]
            if any(is_geom) and not all(is_geom):
                return f"SELECT {', '.join(select)} FROM ({query})"
        except Exception:
            pass

    # Fallback: select all and convert geometry
    return (
        f"SELECT * EXCLUDE({quoted_geom}), ST_AsWKB({quoted_geom}) AS {quoted_geom} FROM ({query})"
    )


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
            rows = con.execute(f"DESCRIBE ({query})").fetchall()
            col_info = {row[0]: row[1] for row in rows}
        except Exception:
            col_info = {}

    targets: list[str] = []
    if col_info:
        # Only a real GEOMETRY needs ST_AsWKB, and the same test as the
        # secondaries below. "Not a STRUCT" was too loose: a BLOB primary is
        # already the correct 1.x carrier and ST_AsWKB(BLOB) does not bind, so
        # the whole write aborted; worse, ST_AsWKB(VARCHAR) *does* bind and
        # silently reinterprets the text as WKT.
        if col_info.get(geometry_column, "").upper().startswith("GEOMETRY"):
            targets.append(geometry_column)
    else:
        # DESCRIBE failed, so we know nothing about the types. Convert the
        # primary and let DuckDB reject it if it is not geometry -- the
        # historical behaviour, and the only safe guess without type info.
        targets.append(geometry_column)

    for name in secondary_columns or ():
        if name in targets or name == geometry_column:
            continue
        # A STRUCT is a bbox-style column, not geometry: startswith() excludes it.
        if col_info.get(name, "").upper().startswith("GEOMETRY"):
            targets.append(name)

    if not targets:
        return query

    # Cast to BLOB to produce plain binary without geoarrow extension type
    # Use SELECT * REPLACE to preserve column order
    replacements = ", ".join(
        f"ST_AsWKB({quote_identifier(col)})::BLOB AS {quote_identifier(col)}" for col in targets
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


def _geoarrow_extension(field) -> tuple[str | None, dict]:
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


def geoarrow_field_edges(field) -> str | None:
    """The edges a GeoArrow field declares, lower-case, in either carrier.

    ``None`` when the field is not GeoArrow or declares no edges (planar is
    the GeoArrow default, so a planar type serializes without the key). The
    one reader of a field's edges: ``arrow_geo_metadata`` re-exports it.

    The type only, by design. DuckDB never reads a ``geo`` block, so which
    columns it refuses is a type question, and the register path relabels on
    this answer even when a block says planar or the value is a typo. What a
    table declares once its block is weighed is
    ``write_funnels.table_nonplanar_edges``.
    """
    edges = _geoarrow_extension(field)[1].get("edges")
    return edges.lower() if isinstance(edges, str) else None


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
        import geoarrow.pyarrow as ga

        target = field.type.with_edge_type(ga.EdgeType.PLANAR)
        chunks = [pa.ExtensionArray.from_storage(target, c.storage) for c in column.chunks]
        return field.with_type(target), pa.chunked_array(chunks, target)
    storage = _storage(column)
    metadata = dict(field.metadata or {})
    metadata[_EXT_NAME] = name.encode("utf-8")
    metadata[_EXT_META] = json.dumps({k: v for k, v in parsed.items() if k != "edges"}).encode(
        "utf-8"
    )
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
        if geoarrow_field_edges(field) in (None, "planar"):
            continue
        name, parsed = _geoarrow_extension(field)
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

    @property
    def edges(self) -> dict[str, str]:
        """``{column: edges}`` for every column that was registered as planar."""
        return {col: geoarrow_field_edges(field) or "planar" for col, field in self.fields.items()}

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
    the restore side's question (``geo_metadata.GEOGRAPHY_EDGE_ALGORITHMS``).
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
