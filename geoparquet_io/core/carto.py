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
from urllib.parse import quote, urlparse

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

# Environment variable for API key
CARTO_API_KEY_ENV = "CARTO_API_KEY"


class CartoError(Exception):
    """Carto-specific error."""

    pass


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
        try:
            conn = get_duckdb_connection()
            conn.execute("SET allow_asterisks_in_http_paths = true")
            conn.execute(f"SET http_timeout = {int(timeout * 1000)}")  # milliseconds

            table = conn.execute(f"SELECT * FROM {read_expr}").arrow().read_all()
            return table

        except Exception as e:
            last_exception = e
            error_msg = str(e).lower()

            # Non-retryable errors - fail immediately
            if "404" in error_msg or "not found" in error_msg:
                raise CartoError(
                    f"Table '{table_name}' not found. Check the table name and ensure "
                    f"it is publicly accessible."
                ) from e

            if "401" in error_msg or "unauthorized" in error_msg:
                raise CartoError(
                    f"Unauthorized access to table '{table_name}'. "
                    "Set CARTO_API_KEY environment variable or check permissions."
                ) from e

            if "403" in error_msg or "forbidden" in error_msg:
                raise CartoError(
                    f"Access forbidden to table '{table_name}'. Check permissions."
                ) from e

            # Retryable errors
            if attempt < max_retries - 1:
                delay = retry_delay * (2**attempt)  # Exponential backoff
                if "timeout" in error_msg:
                    warn(
                        f"Request timed out (attempt {attempt + 1}/{max_retries}), retrying in {delay:.1f}s..."
                    )
                elif "connection" in error_msg or "network" in error_msg:
                    warn(
                        f"Connection error (attempt {attempt + 1}/{max_retries}), retrying in {delay:.1f}s..."
                    )
                else:
                    warn(
                        f"Request failed (attempt {attempt + 1}/{max_retries}): {e}, retrying in {delay:.1f}s..."
                    )
                time.sleep(delay)
            else:
                # Final attempt failed
                if "timeout" in error_msg:
                    raise CartoError(
                        f"Request timed out after {max_retries} attempts. "
                        "The table may be too large. Try using --limit or --where to reduce the result set, "
                        "or increase --timeout."
                    ) from e

    # All retries exhausted
    raise CartoError(
        f"Failed to fetch data from Carto after {max_retries} attempts: {last_exception}"
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
    exclude_set = set(split_column_list(exclude_cols, "--exclude-cols") or ())

    # Decide between geometry and plain/tabular extraction.
    schema_columns: list[str] | None = None
    if geometry is None:
        has_geometry, schema_columns = _detect_table_shape(
            url, table_name, effective_api_key, timeout
        )
    else:
        has_geometry = geometry

    # --include-cols becomes the SELECT list, so it is checked against the
    # schema the probe above already read — for free, and only when it ran.
    # --exclude-cols is not: it is applied to the *fetched* table, where
    # ``the_geom`` has become ``geometry``, so a Carto-schema check would reject
    # the documented ``--exclude-cols geometry``.
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
            exclude_set=exclude_set,
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
        exclude_set=exclude_set,
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
    exclude_set: set[str],
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

    # Apply column exclusions (but never exclude geometry)
    if exclude_set:
        # Prevent excluding the geometry column - it's required for GeoParquet
        if "geometry" in exclude_set:
            exclude_set = exclude_set - {"geometry"}
            warn("Cannot exclude 'geometry' column - it is required for GeoParquet output")
        if exclude_set:
            cols_to_keep = [c for c in table.column_names if c not in exclude_set]
            table = table.select(cols_to_keep)
            debug(f"Excluded columns: {exclude_set}")

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
    exclude_set: set[str],
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

    # Apply column exclusions (no geometry to protect here)
    if exclude_set:
        cols_to_keep = [c for c in table.column_names if c not in exclude_set]
        table = table.select(cols_to_keep)
        debug(f"Excluded columns: {exclude_set}")

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
