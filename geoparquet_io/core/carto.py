"""Carto SQL API to GeoParquet conversion.

Extracts data from Carto SQL API endpoints using DuckDB's ST_Read
for efficient GeoJSON parsing.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import NamedTuple
from urllib.parse import quote, urlparse

import duckdb
import pyarrow as pa

from geoparquet_io.core.column_selection import (
    resolve_columns_against_schema,
    split_column_list,
)
from geoparquet_io.core.common import (
    InvalidParameterError,
    get_duckdb_connection,
    write_geoparquet_table,
)
from geoparquet_io.core.crs_utils import parse_crs_string_to_projjson
from geoparquet_io.core.duckdb_utils import (
    quote_identifier,
    sql_path,
    validate_where_clause,
    where_condition_fragment,
)
from geoparquet_io.core.exceptions import sanitize_error_message
from geoparquet_io.core.geometry_repair import repair_arrow_table_geometry
from geoparquet_io.core.logging_config import (
    configure_verbose,
    debug,
    info,
    progress,
    success,
    warn,
)

# Default timeout and retry settings
DEFAULT_TIMEOUT = 120  # seconds
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 2.0  # seconds

# A failed fetch is diagnosed with one extra request (see _probe_request_status).
# Cap how long that may take so a long --timeout is not paid a second time just
# to learn a status code.
STATUS_PROBE_TIMEOUT = 30.0  # seconds

# How much of a 4xx response body the probe reads to explain the failure. Carto
# answers a bad query with a short JSON error; the cap keeps a body that is
# not short from becoming a download.
PROBE_DETAIL_BYTES = 2048

# The longest a server's Retry-After is honoured for, so a hostile or confused
# header cannot park the client for an hour.
MAX_RETRY_AFTER = 60.0  # seconds

# Statuses that mean "try again": the server is asking for it (429), asked the
# client to resend (408), or is itself failing (5xx).
_RETRYABLE_STATUSES = {408, 429}

# Failures that happened on this machine, not at the server: the query never
# produced a response to classify, so there is nothing to probe for and no
# retry that could help. Everything else that reaches the classifier is an
# I/O failure whose status is worth asking about.
_LOCAL_FAILURES: tuple[type[BaseException], ...] = (
    duckdb.OutOfMemoryException,
    duckdb.InterruptException,
    duckdb.BinderException,
    duckdb.CatalogException,
    duckdb.ParserException,
    duckdb.InvalidInputException,
    MemoryError,
)

# Environment variable for API key
CARTO_API_KEY_ENV = "CARTO_API_KEY"

# The fatal HTTP statuses, and what each one means for a Carto extraction.
# Keyed on the status the transport reported -- never on text found in an
# exception message, which embeds the request URL and so the user's own SQL
# (#1020).
_FATAL_STATUS_HINTS = {
    404: ("Table '{table}' not found. Check the table name and ensure it is publicly accessible."),
    401: (
        "Unauthorized access to table '{table}'. "
        "Set CARTO_API_KEY environment variable or check permissions."
    ),
    403: "Access forbidden to table '{table}'. Check permissions.",
}


class CartoError(Exception):
    """Carto-specific error."""

    pass


class _CartoStatus(NamedTuple):
    """What the transport reported about one failed fetch attempt.

    Args:
        code: The HTTP status the server answered with, or None when the
            request never got one. That absence -- connection refused, DNS
            failure, a timeout -- *is* the retryable class.
        timed_out: Whether the request timed out. Known from the exception
            *type* when the probe times out, and from the *clock* when the data
            request does -- an attempt that ran for the whole ``--timeout``
            before failing timed out whatever its message says, and is not
            probed: the probe would re-run the query that was too heavy the
            first time.
        local: The failure happened on this machine -- out of memory, an
            interrupt, a binder error -- before any response existed to
            classify. Nothing to probe, nothing a retry would change.
        detail: For a 4xx, the start of the server's own explanation
            (``column "foo" does not exist``), sanitized. None otherwise.
        retry_after: The server's ``Retry-After`` in seconds, when it sent one.
    """

    code: int | None
    timed_out: bool
    local: bool = False
    detail: str | None = None
    retry_after: float | None = None


def _validate_carto_url(url: str) -> str:
    """Validate and normalize Carto SQL API URL.

    Args:
        url: Carto SQL API URL

    Returns:
        Normalized URL ending with /api/v2/sql

    Raises:
        InvalidParameterError: If URL is not a valid Carto SQL API endpoint
    """
    url = url.rstrip("/")
    parsed = urlparse(url)

    # Restrict to http(s) — rejects empty, file://, and other schemes so the
    # URL can be safely handed to urllib.request.urlopen (see _carto_sql_json).
    if parsed.scheme not in ("http", "https"):
        raise InvalidParameterError(
            "url",
            f"Invalid URL: {url}. Must include an http:// or https:// scheme",
        )

    # Accept URLs ending with /api/v2/sql or /api/v1/sql
    if url.endswith("/api/v2/sql") or url.endswith("/api/v1/sql"):
        return url

    # Try to construct the URL if user gave base domain
    if not parsed.path or parsed.path == "/":
        # User gave just the domain, append standard path
        return f"{url}/api/v2/sql"

    raise InvalidParameterError(
        "url",
        f"Invalid Carto SQL API URL: {url}. "
        "Expected format: https://account.carto.com/api/v2/sql or https://account.carto.com",
    )


def _validate_table_name(table_name: str) -> str:
    """Validate table name to prevent SQL injection.

    Args:
        table_name: Table name to validate

    Returns:
        Validated table name

    Raises:
        InvalidParameterError: If table name contains dangerous characters
    """
    # Basic validation - table names should be alphanumeric with underscores
    # Allow schema-qualified names (schema.table)
    import re

    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$", table_name):
        raise InvalidParameterError(
            "table_name",
            f"Invalid table name: {table_name}. "
            "Table names must be alphanumeric with underscores, optionally schema-qualified.",
        )
    return table_name


def _build_carto_query(
    table_name: str,
    columns: list[str] | None = None,
    where: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    limit: int | None = None,
    include_geom: bool = True,
) -> str:
    """Build SQL query for Carto API.

    Args:
        table_name: Name of the table to query (will be quoted for safety)
        columns: Columns to select (None = all)
        where: SQL WHERE clause (user-provided, passed through)
        bbox: Bounding box filter (minx, miny, maxx, maxy)
        limit: Maximum rows to return
        include_geom: When True (geometry extraction), force-include ``the_geom``
            in an explicit column list and honor the ``bbox`` spatial filter.
            When False (plain/tabular extraction), neither applies since the
            table has no geometry column.

    Returns:
        SQL query string

    Note:
        The table_name is quoted using PostgreSQL identifier quoting.
        The where clause is validated upstream in ``carto_to_table()`` via
        ``validate_where_clause`` before it reaches this function.
    """
    # Validate and quote table name to prevent SQL injection
    _validate_table_name(table_name)
    quoted_table = quote_identifier(table_name)

    # Column selection - quote each column name
    if columns:
        # Always include the_geom for geometry extraction
        if include_geom and "the_geom" not in columns:
            columns = [*columns, "the_geom"]
        col_str = ", ".join(quote_identifier(c) for c in columns)
    else:
        col_str = "*"

    sql = f"SELECT {col_str} FROM {quoted_table}"

    # Build WHERE clause
    conditions = []
    if where:
        # Already validated upstream in carto_to_table() via validate_where_clause.
        # where_condition_fragment() closes the paren on its own line so a trailing
        # '--' in the clause cannot comment out the bbox condition or the LIMIT.
        conditions.append(where_condition_fragment(where))
    if bbox and include_geom:
        minx, miny, maxx, maxy = bbox
        # Use ST_Intersects with ST_MakeEnvelope for spatial filter
        # the_geom is quoted for safety
        conditions.append(
            f"ST_Intersects({quote_identifier('the_geom')}, "
            f"ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy}, 4326))"
        )

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    if limit is not None:
        sql += f" LIMIT {limit}"

    return sql


def _build_carto_count_query(
    table_name: str,
    where: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
) -> str:
    """Build the COUNT(*) query for a Carto table with the same filters as the scan.

    Note:
        The where clause is validated upstream in ``carto_to_table()`` via
        ``validate_where_clause`` before it reaches this function, and is wrapped
        with :func:`where_condition_fragment` so a trailing ``--`` comment cannot
        swallow the bbox condition that follows it.
    """
    _validate_table_name(table_name)
    quoted_table = quote_identifier(table_name)

    sql = f"SELECT COUNT(*) as count FROM {quoted_table}"

    conditions = []
    if where:
        conditions.append(where_condition_fragment(where))
    if bbox:
        minx, miny, maxx, maxy = bbox
        conditions.append(
            f"ST_Intersects({quote_identifier('the_geom')}, "
            f"ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy}, 4326))"
        )

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    return sql


def _get_row_count(
    url: str,
    table_name: str,
    where: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    api_key: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> int:
    """Get row count from Carto table with optional filters.

    Args:
        url: Carto SQL API URL
        table_name: Table name
        where: Optional WHERE clause
        bbox: Optional bounding box filter
        api_key: Optional API key for authenticated requests
        timeout: Request timeout in seconds

    Returns:
        Number of rows matching the filter
    """
    sql = _build_carto_count_query(table_name, where=where, bbox=bbox)

    full_url = f"{url}?q={quote(sql)}"
    if api_key:
        full_url += f"&api_key={quote(api_key)}"

    conn = get_duckdb_connection()
    conn.execute(f"SET http_timeout = {int(timeout * 1000)}")  # DuckDB uses milliseconds
    result = conn.execute(
        f"SELECT rows[1].count FROM read_json_auto({sql_path(full_url)})"
    ).fetchone()

    return int(result[0]) if result else 0


def _geometry_column_from_fields(fields: object) -> str | None:
    """Return the first geometry column from a Carto ``fields`` schema block.

    Carto's SQL API returns a ``fields`` object mapping each column name to a
    descriptor whose ``type`` is ``"geometry"`` for spatial columns. This pure
    helper inspects that mapping so callers can decide between geometry and
    plain/tabular extraction without a second request.

    Args:
        fields: The ``fields`` value from a Carto SQL API JSON response.

    Returns:
        Name of the first column whose type is ``"geometry"``, or None if the
        schema has no geometry column (or is malformed).
    """
    if not isinstance(fields, dict):
        return None
    for col_name, descriptor in fields.items():
        if isinstance(descriptor, dict) and descriptor.get("type") == "geometry":
            return str(col_name)
    return None


def _column_names_from_fields(fields: object) -> list[str] | None:
    """Return every column name in a Carto ``fields`` schema block, in order.

    The same block :func:`_geometry_column_from_fields` reads, so checking
    ``--include-cols`` against the table costs no extra request (#980).

    Returns:
        The column names, or None if the block is missing or malformed -- which
        callers must read as "no schema to check against", not "no columns".
    """
    if not isinstance(fields, dict):
        return None
    return [str(name) for name in fields]


def _carto_sql_json(
    url: str,
    sql: str,
    api_key: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict:
    """Run a SQL query against the Carto API and return the parsed JSON response.

    Used for lightweight metadata/detection probes (schema, existence checks).
    Bulk data is fetched via DuckDB in :func:`_fetch_with_retry` instead.

    Raises:
        CartoError: On request/parse failure or an API error payload.
    """
    full_url = f"{url}?q={quote(sql)}"
    if api_key:
        full_url += f"&api_key={quote(api_key)}"

    try:
        # url scheme is constrained to http(s) by _validate_carto_url upstream.
        request = urllib.request.Request(full_url, headers={"User-Agent": "geoparquet-io"})
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310  # nosec B310
            payload = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, ValueError) as e:
        raise CartoError(f"Carto SQL request failed: {e}") from e

    if not isinstance(payload, dict):
        raise CartoError("Unexpected Carto SQL API response (not a JSON object)")
    if payload.get("error"):
        raise CartoError(f"Carto API error: {payload['error']}")
    return payload


def _probe_table_schema(
    url: str,
    table_name: str,
    api_key: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> object:
    """Fetch a Carto table's ``fields`` schema block with one bounded request.

    Issues ``SELECT * FROM <table> LIMIT 0`` — schema only, no rows.

    Raises:
        CartoError: If the probe request fails.
    """
    _validate_table_name(table_name)
    quoted_table = quote_identifier(table_name)
    payload = _carto_sql_json(url, f"SELECT * FROM {quoted_table} LIMIT 0", api_key, timeout)
    return payload.get("fields")


def _detect_geometry_column(
    url: str,
    table_name: str,
    api_key: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> str | None:
    """Probe the Carto SQL API for a geometry-typed column.

    Note:
        Carto attaches ``the_geom``/``the_geom_webmercator`` (type ``geometry``)
        to nearly every managed table, even purely tabular ones where those
        columns are entirely NULL. A column reported here therefore does *not*
        guarantee the table actually holds geometry — confirm with
        :func:`_geometry_column_has_values`.

    Returns:
        Name of the first geometry-typed column, or None if the schema has none.

    Raises:
        CartoError: If the probe request fails.
    """
    return _geometry_column_from_fields(_probe_table_schema(url, table_name, api_key, timeout))


def _geometry_column_has_values(
    url: str,
    table_name: str,
    geom_col: str,
    api_key: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> bool:
    """Probe whether a geometry column holds any non-NULL value.

    Carto attaches an often entirely-NULL ``the_geom`` to managed tabular
    tables, so the schema alone misclassifies them as spatial. A bounded
    ``... WHERE <geom> IS NOT NULL LIMIT 1`` probe confirms real geometry
    (returns instantly for spatial tables; ~sub-second even for large all-NULL
    tables since Postgres stops at the first match / table statistics).

    Raises:
        CartoError: If the probe request fails.
    """
    _validate_table_name(table_name)
    quoted_table = quote_identifier(table_name)
    quoted_geom = quote_identifier(geom_col)
    sql = f"SELECT 1 AS has_geom FROM {quoted_table} WHERE {quoted_geom} IS NOT NULL LIMIT 1"
    payload = _carto_sql_json(url, sql, api_key, timeout)
    return bool(payload.get("rows"))


def _detect_table_shape(
    url: str,
    table_name: str,
    api_key: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> tuple[bool, list[str] | None]:
    """Decide whether a Carto table should be extracted as geometry.

    Two-step: (1) find a geometry-typed column in the schema; (2) confirm it
    actually holds non-NULL geometry. Both are needed because Carto adds an
    often-empty ``the_geom`` to managed tabular tables. If detection is
    inconclusive (network error, etc.) we fall back to the geometry path so the
    normal extraction and error handling apply.

    Returns:
        ``(has_geometry, column_names)``. The step-1 probe already carries the
        whole schema, so its column names come back with the decision and
        ``--include-cols`` can be checked against them for free (#980).
        ``column_names`` is None when that probe failed, i.e. when there is no
        schema to check against.
    """
    try:
        fields = _probe_table_schema(url, table_name, api_key, timeout)
    except CartoError as e:
        debug(f"Geometry detection inconclusive ({e}); assuming geometry present")
        return True, None

    columns = _column_names_from_fields(fields)
    geom_col = _geometry_column_from_fields(fields)
    if not geom_col:
        debug("No geometry-typed column in schema; extracting as plain table")
        return False, columns

    try:
        populated = _geometry_column_has_values(url, table_name, geom_col, api_key, timeout)
    except CartoError as e:
        debug(f"Geometry detection inconclusive ({e}); assuming geometry present")
        return True, columns

    if populated:
        debug(f"Detected populated geometry column: {geom_col}")
        return True, columns
    debug(f"Geometry column {geom_col!r} is entirely NULL; extracting as plain table")
    return False, columns


def _probe_schema_columns(
    url: str,
    table_name: str,
    api_key: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> list[str] | None:
    """Read a table's column names for the ``--include-cols`` check, tolerating a failure.

    :func:`_detect_table_shape` gets these for free from the probe it already
    runs. ``--geometry``/``--no-geometry`` skips that probe, so this pays for the
    one bounded ``SELECT * ... LIMIT 0`` itself -- and only for ``--include-cols``,
    where the round-trip earns itself: that list becomes the SELECT list, so an
    unresolved name comes back as an opaque ``ST_Read`` failure on an HTTP error
    body *and* burns the retry budget first. ``--exclude-cols`` needs no probe at
    all; :func:`_apply_column_exclusions` checks it against the fetched table.

    Returns:
        The column names, or None when there is no schema to check against --
        never an extraction failure, matching how every other call site treats
        a probe that could not answer.
    """
    try:
        return _column_names_from_fields(_probe_table_schema(url, table_name, api_key, timeout))
    except CartoError as e:
        debug(f"Schema probe for the column check failed ({e}); skipping it")
        return None


def _apply_column_exclusions(
    table: pa.Table,
    exclude_list: list[str] | None,
    *,
    protect_geometry: bool,
) -> pa.Table:
    """Drop the ``--exclude-cols`` columns from the table that was actually fetched.

    The names are resolved against ``table.column_names`` -- the exact post-fetch
    set, after Carto's ``the_geom`` has become ``geometry`` -- so a case mismatch
    or a typo is an error rather than a filter that silently matches nothing
    (#991). Resolving here rather than against the source schema also costs
    nothing and needs nothing: no probe is involved, so the check holds under
    ``--geometry``/``--no-geometry`` too.

    Args:
        table: The fetched table, after any rename
        exclude_list: Column names to drop, or None
        protect_geometry: Refuse to drop ``geometry``, which GeoParquet output
            requires. Only the geometry path has such a column; on the tabular
            path ``geometry`` is not a column at all, so naming it is an error
            like any other name the table does not carry.

    Returns:
        The table without the excluded columns.

    Raises:
        InvalidParameterError: If a name is absent from the fetched table.
    """
    exclude_list = resolve_columns_against_schema(
        exclude_list, table.column_names, "--exclude-cols"
    )
    if exclude_list and protect_geometry and "geometry" in exclude_list:
        exclude_list = [col for col in exclude_list if col != "geometry"]
        warn("Cannot exclude 'geometry' column - it is required for GeoParquet output")
    if not exclude_list:
        return table

    exclude_set = set(exclude_list)
    table = table.select([col for col in table.column_names if col not in exclude_set])
    debug(f"Excluded columns: {exclude_set}")
    return table


def _create_empty_geoparquet_table(geoparquet_version: str | None = None) -> pa.Table:
    """Create an empty table with proper GeoParquet metadata.

    Args:
        geoparquet_version: GeoParquet version string (default: "1.1.0")

    Returns:
        Empty PyArrow table with valid GeoParquet metadata
    """
    version = geoparquet_version or "1.1.0"
    crs = parse_crs_string_to_projjson("OGC:CRS84")

    geo_metadata = {
        "version": version,
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "crs": crs,
                "geometry_types": [],
            }
        },
    }

    table = pa.table({"geometry": pa.array([], type=pa.binary())})
    new_metadata = {b"geo": json.dumps(geo_metadata).encode("utf-8")}
    return table.replace_schema_metadata(new_metadata)


def _status_from_duckdb_error(exc: BaseException) -> int | None:
    """Read the HTTP status off a DuckDB exception, when it carries a usable one.

    ``duckdb.HTTPException`` exposes ``status_code``, but reports 0 whenever the
    request produced no status of its own -- including, in practice, some auth
    failures -- so 0 is read as "no status" rather than as a code. The
    ``ST_Read`` path raises a plain ``IOException`` that carries no status at
    all; :func:`_probe_request_status` is how that one gets a number.

    Returns:
        The status, or None when the exception does not carry a real one.
    """
    code = getattr(exc, "status_code", None)
    if isinstance(code, int) and not isinstance(code, bool) and code > 0:
        return code
    return None


def _probe_request_status(full_url: str, timeout: float) -> _CartoStatus:
    """Ask the server what status this exact request gets.

    ``ST_Read`` fetches through GDAL, which reports only ``IO Error: Could not
    open GDAL dataset at: <url>`` no matter what the server said -- so the
    status has to be asked for separately. One streamed GET reads the response
    headers without pulling the body down.

    This costs a request, but only on a failure, and the failures it has to tell
    apart are the cheap ones: a missing table, a rejected key, a server already
    erroring. The expensive case is a transient blip on a large query, where
    Carto re-runs it to produce the headers; ``STATUS_PROBE_TIMEOUT`` bounds how
    long that may take before the probe gives up and the failure stays
    retryable.

    Returns:
        The status, or ``_CartoStatus(None, ...)`` when the request produced no
        status. A probe that cannot answer must say so rather than guess: no
        status means the retryable class.
    """
    import httpx

    try:
        with httpx.stream(
            "GET",
            full_url,
            timeout=min(timeout, STATUS_PROBE_TIMEOUT),
            follow_redirects=True,
        ) as response:
            code = response.status_code
            # A 4xx is the one class where the body is worth a bounded read:
            # Carto puts the reason there ("column ... does not exist"), and a
            # bare "HTTP 400" throws away the only thing the user can act on.
            detail = _read_probe_detail(response) if 400 <= code < 500 and code != 429 else None
            return _CartoStatus(code, False, detail=detail, retry_after=_retry_after(response))
    except httpx.TimeoutException:
        return _CartoStatus(None, True)
    except Exception as probe_error:  # noqa: BLE001 - any failure means "no status"
        debug(f"Status probe failed ({probe_error}); treating the failure as retryable")
        return _CartoStatus(None, False)


def _read_probe_detail(response) -> str | None:
    """The first ``PROBE_DETAIL_BYTES`` of a 4xx body, made safe to print.

    Carto's SQL API answers ``{"error": ["column \\"foo\\" does not exist"]}``;
    the messages are unwrapped when the body is that shape, and the raw text is
    used when it is anything else.
    """
    try:
        chunk = next(response.iter_bytes(PROBE_DETAIL_BYTES), b"")
    except Exception:  # noqa: BLE001 - the detail is a courtesy, never the verdict
        return None
    text = chunk.decode("utf-8", "replace").strip()
    try:
        payload = json.loads(text)
    except ValueError:
        payload = None
    if isinstance(payload, dict) and isinstance(payload.get("error"), list):
        text = "; ".join(str(item) for item in payload["error"])
    return sanitize_error_message(text) or None


def _retry_after(response) -> float | None:
    """A ``Retry-After`` given in seconds, capped; None when absent or a date."""
    raw = response.headers.get("Retry-After")
    if raw and raw.strip().isdigit():
        return min(float(raw), MAX_RETRY_AFTER)
    return None


def _classify_fetch_failure(
    exc: BaseException, full_url: str, timeout: float, elapsed: float = 0.0
) -> _CartoStatus:
    """Decide what a failed fetch attempt was, without reading the message.

    In order: a failure that happened here rather than at the server is local;
    an attempt that ran out the whole ``timeout`` timed out, whatever the
    message says, and is not probed (the probe would re-run the query that was
    already too heavy); a status DuckDB itself reported is used as-is (the CSV
    path goes through httpfs, which has one); and only the ``ST_Read`` path,
    whose exception carries no status at all, is probed.
    """
    if isinstance(exc, _LOCAL_FAILURES):
        return _CartoStatus(None, False, local=True)
    if elapsed >= timeout:
        return _CartoStatus(None, True)
    code = _status_from_duckdb_error(exc)
    if code is not None:
        return _CartoStatus(code, False)
    return _probe_request_status(full_url, timeout)


def _is_retryable(status: _CartoStatus) -> bool:
    """No status (refused, DNS, timed out), 408/429, or a 5xx: worth another go."""
    if status.local:
        return False
    code = status.code
    return code is None or code in _RETRYABLE_STATUSES or code >= 500


def _fatal_status_error(
    status: _CartoStatus, table_name: str, fmt: str, exc: BaseException
) -> CartoError | None:
    """Build the fatal error for a classified failure, or None when it is retryable.

    Fatal, and said plainly: a 4xx other than 408/429 (no amount of retrying
    changes a missing table or a bad key -- Carto's own explanation is quoted
    when the probe could read it); a failure that happened on this machine; and
    a server that answered 2xx/3xx to a request gpio could not then read, which
    means the response, not the connection, is the problem -- retrying it three
    times and reporting "Carto returned HTTP 200" helps nobody.
    """
    if _is_retryable(status):
        return None
    reason = sanitize_error_message(str(exc))
    if status.local:
        return CartoError(
            f"Reading table '{table_name}' from Carto failed before any response could be "
            f"classified -- this is a local failure, not a Carto one: {reason}"
        )
    if status.code is not None and status.code < 400:
        return CartoError(
            f"Carto answered HTTP {status.code} for table '{table_name}', but the response "
            f"could not be read as {fmt}. Retrying would fetch the same response; the "
            f"query or the format is the problem: {reason}"
        )
    code = status.code
    if code is None:  # pragma: no cover - _is_retryable already answered for no status
        return None
    hint = _FATAL_STATUS_HINTS.get(code)
    message = (
        hint.format(table=table_name)
        if hint is not None
        else f"Carto request for table '{table_name}' failed with HTTP {code}."
    )
    if status.detail:
        message += f" Carto said: {status.detail}"
    return CartoError(message)


def _retry_warning(status: _CartoStatus, attempt: int, max_retries: int, delay: float) -> str:
    """Word the retry warning from the classified status, not from the message."""
    suffix = f"(attempt {attempt + 1}/{max_retries}), retrying in {delay:.1f}s..."
    if status.timed_out:
        return f"Request timed out {suffix}"
    if status.code is None:
        return f"Could not reach Carto {suffix}"
    return f"Carto returned HTTP {status.code} {suffix}"


def _fetch_with_retry(
    url: str,
    table_name: str,
    sql: str,
    fmt: str = "GeoJSON",
    api_key: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_delay: float = DEFAULT_RETRY_DELAY,
) -> pa.Table:
    """Fetch data from Carto with retry logic for transient failures.

    Args:
        url: Carto SQL API URL
        table_name: Table name (for error messages)
        sql: SQL query to execute
        fmt: Carto response format. ``"GeoJSON"`` (default) parses geometry via
            DuckDB ``ST_Read``; ``"csv"`` reads geometry-less tables via
            ``read_csv_auto`` for plain/tabular extraction.
        api_key: Optional API key
        timeout: Request timeout in seconds
        max_retries: Number of retry attempts
        retry_delay: Base delay between retries (exponential backoff)

    Returns:
        PyArrow Table from DuckDB

    Raises:
        CartoError: On fatal errors or exhausted retries
    """
    # Construct full URL with the requested response format
    fmt_param = "GeoJSON" if fmt == "GeoJSON" else "csv"
    full_url = f"{url}?q={quote(sql)}&format={fmt_param}"
    if api_key:
        full_url += f"&api_key={quote(api_key)}"

    # GeoJSON is parsed with ST_Read; CSV (geometry-less) with read_csv_auto.
    if fmt == "GeoJSON":
        read_expr = f"ST_Read({sql_path(full_url)})"
    else:
        read_expr = f"read_csv_auto({sql_path(full_url)})"

    debug(f"Request URL: {full_url[:100]}...")

    last_exception: Exception | None = None

    for attempt in range(max_retries):
        started = time.monotonic()
        try:
            conn = get_duckdb_connection()
            conn.execute("SET allow_asterisks_in_http_paths = true")
            conn.execute(f"SET http_timeout = {int(timeout * 1000)}")  # milliseconds

            # The clock that decides "timed out" starts at the request, not at
            # the connection: loading the spatial extension on a cold macOS
            # runner took longer than a 2 s --timeout and read as one.
            started = time.monotonic()
            table = conn.execute(f"SELECT * FROM {read_expr}").arrow().read_all()
            return table

        except Exception as e:
            last_exception = e

            # Classify on the HTTP status the transport reported -- never on the
            # exception message, which embeds full_url and therefore the user's
            # own table name, WHERE clause and LIMIT (#1020).
            status = _classify_fetch_failure(
                e, full_url, timeout, elapsed=time.monotonic() - started
            )

            fatal = _fatal_status_error(status, table_name, fmt_param, e)
            if fatal is not None:
                raise fatal from e

            # Retryable: a 408/429/5xx, or no status at all (refused, DNS, timeout).
            if attempt < max_retries - 1:
                delay = retry_delay * (2**attempt)  # Exponential backoff
                if status.retry_after is not None:
                    # The server said how long it wants; a 429 answered with more
                    # requests on our own schedule is the wrong direction.
                    delay = max(delay, status.retry_after)
                warn(_retry_warning(status, attempt, max_retries, delay))
                time.sleep(delay)
            elif status.timed_out:
                raise CartoError(
                    f"Request timed out after {max_retries} attempts. "
                    "The table may be too large. Try using --limit or --where to reduce the result set, "
                    "or increase --timeout."
                ) from e

    # All retries exhausted. The DuckDB message names the URL it failed on, key
    # and all; sanitized rather than echoed.
    raise CartoError(
        f"Failed to fetch data from Carto after {max_retries} attempts: "
        f"{sanitize_error_message(str(last_exception))}"
    ) from last_exception


def carto_to_table(
    url: str,
    table_name: str,
    *,
    where: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    limit: int | None = None,
    include_cols: str | None = None,
    exclude_cols: str | None = None,
    api_key: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
    max_retries: int = DEFAULT_MAX_RETRIES,
    geoparquet_version: str | None = None,
    verbose: bool = False,
    repair_geometry: bool = True,
    geometry: bool | None = None,
) -> pa.Table:
    """Extract data from Carto SQL API to PyArrow Table.

    For tables with a geometry column, DuckDB's ``ST_Read`` parses Carto's
    GeoJSON output and the result carries GeoParquet ``geo`` metadata. For
    geometry-less (tabular) tables, the data is fetched as CSV and returned as a
    plain table with **no** ``geo`` metadata, mirroring gpio's file-conversion
    behavior for non-spatial inputs.

    Args:
        url: Carto SQL API URL (e.g., https://phl.carto.com/api/v2/sql)
        table_name: Name of the table to query
        where: SQL WHERE clause for filtering
        bbox: Bounding box filter as (minx, miny, maxx, maxy) in WGS84.
            Ignored for geometry-less tables.
        limit: Maximum number of rows to return
        include_cols: Comma-separated column names to include
        exclude_cols: Comma-separated column names to exclude (applied after fetch)
        api_key: API key for authenticated requests (or set CARTO_API_KEY env var)
        timeout: Request timeout in seconds (default: 120)
        max_retries: Number of retry attempts for transient failures (default: 3)
        geoparquet_version: GeoParquet version for metadata (default: "1.1.0")
        verbose: Enable verbose output
        repair_geometry: Repair invalid geometry with ST_MakeValid (geometry
            tables only; default: True).
        geometry: Extraction mode. ``None`` (default) auto-detects from the
            table schema; ``True`` forces geometry extraction (GeoParquet);
            ``False`` forces plain/tabular extraction (no ``geo`` metadata).

    Returns:
        PyArrow Table. For geometry tables, a WKB geometry column named
        'geometry' with GeoParquet metadata; for tabular tables, a plain table.

    Raises:
        CartoError: If the Carto API request fails
        InvalidParameterError: If URL or table name is invalid
    """
    configure_verbose(verbose)

    # Validate --where before it is interpolated into any query, and before any
    # network probe/request is made (gpio #612 parity). This is the single
    # choke point every carto_to_table caller (geometry and plain/tabular
    # extraction alike) funnels through.
    if where:
        validate_where_clause(where)

    # Validate URL
    url = _validate_carto_url(url)
    debug(f"Carto URL: {url}")

    # Get API key from parameter or environment
    effective_api_key = api_key or os.environ.get(CARTO_API_KEY_ENV)
    if effective_api_key:
        debug("Using API key for authentication")

    # Parse column lists. A blank entry is rejected here, not only by the Click
    # callback: the Python API never goes through Click, and `` `` reached
    # quote_identifier() through _build_carto_query as a raw ValueError (#980).
    include_list = split_column_list(include_cols, "--include-cols")
    exclude_list = split_column_list(exclude_cols, "--exclude-cols")

    # Decide between geometry and plain/tabular extraction. The shape probe
    # carries the schema, so on the default path the --include-cols check is free.
    schema_columns: list[str] | None = None
    if geometry is None:
        has_geometry, schema_columns = _detect_table_shape(
            url, table_name, effective_api_key, timeout
        )
    else:
        # Forcing the mode skips that probe. --include-cols buys its own rather
        # than losing the check to an unrelated flag (#991); --exclude-cols needs
        # no schema here, since it is checked against the fetched table below.
        has_geometry = geometry
        if include_list:
            schema_columns = _probe_schema_columns(url, table_name, effective_api_key, timeout)

    # --include-cols becomes the SELECT list, so it names *source* columns and is
    # the only list a schema can answer for ahead of the fetch.
    if schema_columns is not None:
        include_list = resolve_columns_against_schema(
            include_list, schema_columns, "--include-cols"
        )

    if not has_geometry:
        if bbox:
            warn("Ignoring --bbox: table has no geometry column (tabular extraction)")
        return _carto_plain_table(
            url,
            table_name,
            where=where,
            limit=limit,
            include_list=include_list,
            exclude_list=exclude_list,
            api_key=effective_api_key,
            timeout=timeout,
            max_retries=max_retries,
        )

    return _carto_geo_table(
        url,
        table_name,
        where=where,
        bbox=bbox,
        limit=limit,
        include_list=include_list,
        exclude_list=exclude_list,
        api_key=effective_api_key,
        timeout=timeout,
        max_retries=max_retries,
        geoparquet_version=geoparquet_version,
        repair_geometry=repair_geometry,
    )


def _carto_geo_table(
    url: str,
    table_name: str,
    *,
    where: str | None,
    bbox: tuple[float, float, float, float] | None,
    limit: int | None,
    include_list: list[str] | None,
    exclude_list: list[str] | None,
    api_key: str | None,
    timeout: float,
    max_retries: int,
    geoparquet_version: str | None,
    repair_geometry: bool,
) -> pa.Table:
    """Extract a Carto table with geometry to a GeoParquet-ready PyArrow Table."""
    # Get row count for progress
    try:
        total_count = _get_row_count(url, table_name, where, bbox, api_key, timeout)
        info(f"Table: {table_name}")
        info(f"Total rows matching filter: {total_count:,}")
    except Exception as e:
        debug(f"Could not get row count: {e}")
        total_count = None

    if total_count == 0:
        warn("No rows match the specified filters")
        return _create_empty_geoparquet_table(geoparquet_version)

    # Build query
    sql = _build_carto_query(
        table_name=table_name,
        columns=include_list,
        where=where,
        bbox=bbox,
        limit=limit,
    )
    debug(f"SQL: {sql}")

    # Fetch data with retry logic
    progress("Fetching data from Carto...")
    table = _fetch_with_retry(
        url=url,
        table_name=table_name,
        sql=sql,
        fmt="GeoJSON",
        api_key=api_key,
        timeout=timeout,
        max_retries=max_retries,
    )

    if table.num_rows == 0:
        warn("Query returned no rows")
        return _create_empty_geoparquet_table(geoparquet_version)

    debug(f"Received {table.num_rows:,} rows")

    # Rename 'geom' to 'geometry' for consistency
    # DuckDB ST_Read uses 'geom' by default
    col_names = table.column_names
    if "geom" in col_names:
        idx = col_names.index("geom")
        col_names[idx] = "geometry"
        table = table.rename_columns(col_names)

    # Remove OGC_FID if present (added by ST_Read)
    if "OGC_FID" in table.column_names:
        cols_to_keep = [c for c in table.column_names if c != "OGC_FID"]
        table = table.select(cols_to_keep)

    # Apply column exclusions, resolved against the table just fetched -- the
    # rename above is why that is the only set that answers exactly (#991).
    table = _apply_column_exclusions(table, exclude_list, protect_geometry=True)

    # Add CRS metadata (Carto uses WGS84)
    version = geoparquet_version or "1.1.0"
    crs = parse_crs_string_to_projjson("OGC:CRS84")
    if crs:
        geo_metadata = {
            "version": version,
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "crs": crs,
                    "geometry_types": [],  # Mixed types possible
                }
            },
        }

        existing_metadata = table.schema.metadata or {}
        new_metadata = {**existing_metadata, b"geo": json.dumps(geo_metadata).encode("utf-8")}
        table = table.replace_schema_metadata(new_metadata)

    # Repair invalid geometry (issue #506). Helper preserves schema metadata.
    if table.num_rows > 0:
        table, _ = repair_arrow_table_geometry(table, "geometry", repair=repair_geometry)

    success(f"Extracted {table.num_rows:,} features")
    return table


def _carto_plain_table(
    url: str,
    table_name: str,
    *,
    where: str | None,
    limit: int | None,
    include_list: list[str] | None,
    exclude_list: list[str] | None,
    api_key: str | None,
    timeout: float,
    max_retries: int,
) -> pa.Table:
    """Extract a geometry-less Carto table to a plain PyArrow Table.

    Fetches via the SQL API's CSV format (no geometry required) and returns a
    table with no GeoParquet ``geo`` metadata. ``bbox`` does not apply here.
    """
    # Get row count for progress (no spatial filter for tabular tables)
    try:
        total_count = _get_row_count(url, table_name, where, None, api_key, timeout)
        info(f"Table: {table_name} (no geometry — extracting as plain table)")
        info(f"Total rows matching filter: {total_count:,}")
    except Exception as e:
        debug(f"Could not get row count: {e}")

    # Build query without geometry/bbox
    sql = _build_carto_query(
        table_name=table_name,
        columns=include_list,
        where=where,
        limit=limit,
        include_geom=False,
    )
    debug(f"SQL: {sql}")

    # Fetch data with retry logic (CSV format)
    progress("Fetching data from Carto...")
    table = _fetch_with_retry(
        url=url,
        table_name=table_name,
        sql=sql,
        fmt="csv",
        api_key=api_key,
        timeout=timeout,
        max_retries=max_retries,
    )

    if table.num_rows == 0:
        warn("Query returned no rows")

    debug(f"Received {table.num_rows:,} rows")

    # Apply column exclusions. Nothing was renamed on this path, so the fetched
    # columns are the source ones -- and 'geometry' is not among them, so naming
    # it is an error here rather than something to protect.
    table = _apply_column_exclusions(table, exclude_list, protect_geometry=False)

    success(f"Extracted {table.num_rows:,} rows (plain table, no geometry)")
    return table


def convert_carto_to_geoparquet(
    url: str,
    table_name: str,
    output_file: str,
    *,
    where: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    limit: int | None = None,
    include_cols: str | None = None,
    exclude_cols: str | None = None,
    api_key: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
    max_retries: int = DEFAULT_MAX_RETRIES,
    skip_hilbert: bool = False,
    skip_bbox: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    geoparquet_version: str | None = None,
    overwrite: bool = False,
    verbose: bool = False,
    repair_geometry: bool = True,
    geometry: bool | None = None,
) -> None:
    """Extract Carto table and save as optimized GeoParquet or plain Parquet.

    Geometry tables are written as optimized GeoParquet (Hilbert-sorted, bbox
    column). Geometry-less (tabular) tables are written as plain Parquet with no
    ``geo`` metadata, and Hilbert/bbox steps are skipped since they require
    geometry.

    Args:
        url: Carto SQL API URL
        table_name: Name of the table to query
        output_file: Output Parquet file path
        where: SQL WHERE clause for filtering
        bbox: Bounding box filter as (minx, miny, maxx, maxy). Ignored for
            geometry-less tables.
        limit: Maximum rows to extract
        include_cols: Comma-separated columns to include
        exclude_cols: Comma-separated columns to exclude
        api_key: API key for authenticated requests (or set CARTO_API_KEY env var)
        timeout: Request timeout in seconds (default: 120)
        max_retries: Number of retry attempts (default: 3)
        skip_hilbert: Skip Hilbert curve sorting (geometry tables only)
        skip_bbox: Skip adding bbox column (geometry tables only)
        compression: Compression algorithm
        compression_level: Compression level
        row_group_size_mb: Row group size in MB
        row_group_rows: Row group size in rows
        geoparquet_version: GeoParquet version
        overwrite: Overwrite existing file
        verbose: Enable verbose output
        repair_geometry: Repair invalid geometry with ST_MakeValid (default: True).
            When False, invalid geometry is preserved and a warning reports the count.
        geometry: Extraction mode. ``None`` (default) auto-detects from the
            table schema; ``True`` forces GeoParquet output; ``False`` forces
            plain/tabular Parquet output.
    """
    configure_verbose(verbose)

    # Check output file
    output_path = Path(output_file)
    if output_path.exists() and not overwrite:
        raise CartoError(f"Output file exists: {output_file}\nUse --overwrite to replace it.")

    # Fetch data
    table = carto_to_table(
        url=url,
        table_name=table_name,
        where=where,
        bbox=bbox,
        limit=limit,
        include_cols=include_cols,
        exclude_cols=exclude_cols,
        api_key=api_key,
        timeout=timeout,
        max_retries=max_retries,
        geoparquet_version=geoparquet_version,
        verbose=verbose,
        repair_geometry=repair_geometry,
        geometry=geometry,
    )

    # Plain/tabular tables carry no 'geo' metadata; Hilbert/bbox don't apply.
    is_geo = bool(table.schema.metadata and b"geo" in table.schema.metadata)

    # Apply Hilbert ordering (unless skipped, geometry only)
    if is_geo and not skip_hilbert and table.num_rows > 0:
        progress("Applying Hilbert curve ordering...")
        from geoparquet_io.core.hilbert_order import hilbert_order_table

        table = hilbert_order_table(table, geometry_column="geometry")
        debug("Hilbert sort complete")

    # Add bbox column (unless skipped, geometry only)
    if is_geo and not skip_bbox and table.num_rows > 0:
        progress("Adding bbox column...")
        from geoparquet_io.core.add.bbox import add_bbox_table

        table = add_bbox_table(table, geometry_column="geometry")
        debug("Bbox column added")

    # Write output. For plain/tabular tables pass an empty geometry_column so
    # write_geoparquet_table skips its name-based auto-detection (which would
    # otherwise mistake a Carto 'the_geom' string column for geometry) and emits
    # plain Parquet with no 'geo' metadata key.
    progress(f"Writing to {output_file}...")
    write_geoparquet_table(
        table,
        output_file,
        geometry_column=None if is_geo else "",
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        geoparquet_version=geoparquet_version if is_geo else None,
    )

    noun = "features" if is_geo else "rows"
    success(f"Wrote {table.num_rows:,} {noun} to {output_file}")
