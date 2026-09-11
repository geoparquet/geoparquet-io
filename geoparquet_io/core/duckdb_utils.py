"""
DuckDB connection management utilities.

This module provides functions for creating and managing DuckDB connections
with appropriate extensions loaded for GeoParquet operations.
"""

import os
import re
import shutil
import tempfile
import threading
import uuid
from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from contextvars import ContextVar

import duckdb

from geoparquet_io.core.logging_config import warn
from geoparquet_io.core.parquet_schema import root_schema_columns

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
    from geoparquet_io.core.exceptions import ValidationError

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
        threads: Number of threads for DuckDB to use (default: None = all cores).
                Limiting threads is useful for parallel test execution to prevent
                CPU saturation when multiple pytest workers create connections.
        temp_directory: Volume for DuckDB to spill intermediate results onto.
                    Defaults to a private directory under the OS temp directory
                    (see :func:`spill_directory`); pass a path to put the spill on
                    another volume, e.g. beside a very large output. Never share
                    one path between connections -- ``spill_directory(that_path)``
                    gives each its own leaf under the volume you chose.
        memory_limit: DuckDB memory limit (e.g. "8GB"). Opt-in: DuckDB's own
                    default (roughly 80% of RAM) is the right cap for most work,
                    and a lower one only pushes queries to disk that fit in RAM.
                    Once set, DuckDB spills to temp_directory rather than crashing.

    Returns:
        duckdb.DuckDBPyConnection: Configured connection with extensions loaded
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
    if memory_limit is not None:
        safe_memory_limit = _escape_sql_string(str(memory_limit))
        con.execute(f"SET memory_limit = '{safe_memory_limit}';")

    # Always load spatial extension by default (core use case)
    if load_spatial:
        _install_and_load_extension(con, "spatial")
        # DuckDB 1.5+: ensure lon/lat = x/y axis order globally.
        # Replaces per-call always_xy := true in ST_Transform.
        con.execute("SET geometry_always_xy = true;")

    # Load httpfs for cloud storage support
    if load_httpfs:
        _install_and_load_extension(con, "httpfs")

        # Only configure AWS credentials if explicitly requested (for private buckets)
        # Public buckets work without any secret - DuckDB handles them automatically
        if use_s3_auth:
            _install_and_load_extension(con, "aws")
            con.execute("""
                CREATE OR REPLACE SECRET (
                    TYPE s3,
                    PROVIDER credential_chain,
                    VALIDATION 'none'
                );
            """)

    # Apply S3 config: explicit kwargs override ambient config
    ambient = _active_s3_config.get() or {}
    eff_endpoint = s3_endpoint if s3_endpoint is not None else ambient.get("s3_endpoint")
    eff_region = s3_region if s3_region is not None else ambient.get("s3_region")
    eff_use_ssl = s3_use_ssl if s3_use_ssl is not None else ambient.get("s3_use_ssl")

    if eff_endpoint:
        safe_endpoint = _escape_sql_string(eff_endpoint)
        con.execute(f"SET s3_endpoint='{safe_endpoint}';")
        con.execute("SET s3_url_style='path';")
        ssl_value = "true" if eff_use_ssl is not False else "false"
        con.execute(f"SET s3_use_ssl={ssl_value};")

    if eff_region:
        safe_region = _escape_sql_string(eff_region)
        con.execute(f"SET s3_region='{safe_region}';")

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
    from geoparquet_io.core.exceptions import ExtensionUnavailableError

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
) -> "duckdb.DuckDBPyConnection":
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
        duckdb.DuckDBPyConnection: Configured connection with appropriate S3 access
    """
    from geoparquet_io.core.remote import needs_httpfs

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
            other_cols = [
                quote_identifier(c) for c in columns if c.lower() != geometry_column.lower()
            ]
            if other_cols:
                cols_str = ", ".join(other_cols)
                return f"SELECT {cols_str}, ST_AsWKB({quoted_geom}) AS {quoted_geom} FROM ({query})"
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
