"""
WFS (Web Feature Service) to GeoParquet conversion.

This module provides functionality to download features from OGC WFS services
and convert them to GeoParquet format. Supports WFS 1.0.0, 1.1.0, and 2.0.0.

Key features:
- Fast extraction via Python httpx download + DuckDB local parsing (~10x faster)
- Server-side bbox filtering
- CRS negotiation with EPSG variant handling
- Hilbert curve sorting and bbox column generation
- Auto-pagination and parallel workers for large datasets
- Auto-tile mode to bypass server startIndex limits
"""

from __future__ import annotations

import json
import re
import tempfile
import time
import xml.etree.ElementTree as ET  # nosec B405 - the one parse here (_is_empty_gml_collection) refuses any body declaring a DTD, so no entity expansion is possible; stdlib ET never resolves external entities regardless
from dataclasses import dataclass
from pathlib import Path
from typing import Final
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import duckdb
import pyarrow as pa

# Public API
__all__ = [
    "DEFAULT_WFS_PAGE_SIZE",
    "EmptyLayerError",
    "LayerNotFoundError",
    "WFSAuthenticationError",
    "WFSError",
    "WFSLayerInfo",
    "WFS_VERSIONS",
    "convert_wfs_to_geoparquet",
    "get_layer_info",
    "get_wfs_capabilities",
    "list_available_layers",
    "negotiate_wfs_version",
    "wfs_to_table",
]

from geoparquet_io.core.common import (
    _cast_table_to_schema,
    _compute_unified_schema,
    write_geoparquet_table,
)
from geoparquet_io.core.crs_utils import parse_crs_string_to_projjson
from geoparquet_io.core.duckdb_utils import (
    get_duckdb_connection,
    quote_identifier,
    sql_path,
)
from geoparquet_io.core.geometry_repair import repair_arrow_table_geometry
from geoparquet_io.core.http_retry import (
    get_shared_http_client as _get_shared_http_client_base,
)
from geoparquet_io.core.http_retry import (
    reset_http_client as _reset_http_client_base,
)
from geoparquet_io.core.logging_config import (
    configure_verbose,
    debug,
    info,
    progress,
    success,
    warn,
)
from geoparquet_io.core.reproject import reproject_table

# Maximum JSON object size for DuckDB parsing (1GB)
_MAX_JSON_OBJECT_SIZE = 1073741824


class WFSError(Exception):
    """Exception raised for WFS-related errors."""

    pass


class EmptyLayerError(WFSError):
    """Raised when a WFS layer has 0 features."""

    def __init__(self, typename: str) -> None:
        self.typename = typename
        super().__init__(
            f"No features returned from WFS service for layer '{typename}'.\n"
            "Check that the layer exists and is not empty."
        )


class LayerNotFoundError(WFSError):
    """Raised when a WFS layer does not exist in the service."""

    def __init__(self, typename: str, available: list[str] | None = None) -> None:
        self.typename = typename
        self.available = available or []
        hint = (
            f"\nAvailable layers (first 10): {', '.join(self.available[:10])}"
            if self.available
            else ""
        )
        super().__init__(f"Layer '{typename}' not found in WFS service.{hint}")


class WFSAuthenticationError(WFSError):
    """Raised when WFS service requires authentication or access is denied."""

    def __init__(self, url: str, status_code: int, message: str) -> None:
        self.url = url
        self.status_code = status_code
        super().__init__(message)


# What gpio requests when a service advertises no output formats at all, which
# is also what it requested unconditionally before issue #818.
DEFAULT_OUTPUT_FORMAT: Final[str] = "application/json"

# GeoJSON output format identifiers (in preference order)
GEOJSON_FORMATS = [
    "application/json",
    "json",
    "geojson",
    "application/geo+json",
    "application/vnd.geo+json",
]

# GML output format identifiers (fallback, in preference order).
#
# Servers spell these with wildly inconsistent whitespace and version suffixes
# (``application/gml+xml;version=3.2`` from NextGIS vs
# ``application/gml+xml; version=3.1`` from GeoServer), so matching is done on a
# whitespace-stripped, lowercased form and falls back to a prefix match — see
# _detect_best_output_format.
GML_FORMATS = [
    "gml3",
    "text/xml; subtype=gml/3.1.1",
    "application/gml+xml; version=3.1",
    "gml32",
    "text/xml; subtype=gml/3.2",
    "application/gml+xml; version=3.2",
    "application/gml+xml",
    "gml2",
    "text/xml; subtype=gml/2.1.2",
]

# Schema-metadata key under which the CRS declared by the server in its GeoJSON
# response (the FeatureCollection ``crs`` member) is carried up from the fetch
# layer to wfs_to_table. This is the authoritative statement of which CRS the
# server actually honored — far more reliable than guessing from a bbox. See
# https://github.com/geoparquet/geoparquet-io/issues/499
_SERVER_CRS_METADATA_KEY = b"_wfs_server_crs"


@dataclass
class WFSLayerInfo:
    """WFS layer/feature type metadata."""

    typename: str
    title: str | None
    crs_list: list[str]
    default_crs: str | None
    bbox: tuple[float, float, float, float] | None
    geometry_column: str
    available_formats: list[str]
    sortable_attribute: str | None = None


# Default timeout for HTTP requests (seconds)
DEFAULT_TIMEOUT = 60.0


def _get_shared_http_client(timeout: float = DEFAULT_TIMEOUT):
    """Get shared HTTP client from http_retry module."""
    return _get_shared_http_client_base(timeout=timeout)


def _reset_http_client():
    """Reset shared HTTP client from http_retry module."""
    _reset_http_client_base()


def _make_request(
    url: str,
    params: dict | None = None,
    max_retries: int = 3,
    retry_delay: float = 1.0,
    accept: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> bytes:
    """
    Make HTTP GET request with retry logic.

    Returns raw bytes to handle both JSON and XML responses.

    Args:
        url: Request URL
        params: Query parameters
        max_retries: Number of retry attempts
        retry_delay: Base delay between retries (exponential backoff)
        accept: Accept header value (e.g., "application/json")
        timeout: Request timeout in seconds

    Returns:
        Response content as bytes
    """
    import httpx

    last_exception: Exception | None = None

    headers = {
        "Accept-Encoding": "gzip, deflate",
    }
    if accept:
        headers["Accept"] = accept

    # Build full URL for logging
    request_desc = url
    if params:
        param_summary = ", ".join(f"{k}={v}" for k, v in list(params.items())[:5])
        if len(params) > 5:
            param_summary += f", ... ({len(params)} params)"
        request_desc = f"{url}?{param_summary}"

    for attempt in range(max_retries):
        try:
            debug(f"HTTP GET: {request_desc[:100]}{'...' if len(request_desc) > 100 else ''}")
            start_time = time.time()
            client = _get_shared_http_client(timeout=timeout)
            response = client.get(url, params=params, headers=headers)
            elapsed = time.time() - start_time
            response.raise_for_status()
            content = bytes(response.content)
            debug(f"HTTP OK: {len(content):,} bytes in {elapsed:.1f}s")
            return content
        except httpx.RemoteProtocolError as e:
            last_exception = e
            warn(f"HTTP protocol error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                _reset_http_client()
                time.sleep(retry_delay * (attempt + 1))
        except httpx.TimeoutException as e:
            last_exception = e
            warn(f"HTTP timeout after {timeout}s (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
        except httpx.NetworkError as e:
            last_exception = e
            warn(f"HTTP network error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            if status == 429 or (500 <= status < 600):
                last_exception = e
                warn(f"HTTP {status} (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    retry_after = e.response.headers.get("Retry-After")
                    delay = (
                        float(retry_after)
                        if retry_after and retry_after.isdigit()
                        else retry_delay * (attempt + 1)
                    )
                    time.sleep(delay)
                    continue
            elif status == 401:
                raise WFSAuthenticationError(
                    url, 401, "Authentication required. WFS server requires credentials."
                ) from e
            elif status == 403:
                raise WFSAuthenticationError(
                    url, 403, "Access denied. Check your permissions for this WFS service."
                ) from e
            elif status == 404:
                raise WFSError(f"WFS service not found (404). Check the URL: {url}") from e
            raise WFSError(f"HTTP error {status}: {e}") from e

    raise WFSError(f"Request failed after {max_retries} attempts: {last_exception}")


def _clean_service_url(url: str) -> str:
    """
    Clean WFS service URL by removing GetCapabilities parameters.

    Some URLs come with ?service=WFS&request=GetCapabilities which
    interferes with subsequent requests.

    Args:
        url: Input URL (may include query parameters)

    Returns:
        Clean base URL for the WFS service
    """
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)

    # Remove WFS request-control params that shouldn't persist, in any casing:
    # WFS KVP keys are case-insensitive per the OGC spec, so ``typeName``
    # (the canonical 1.1 casing users copy from a browser URL) must be
    # stripped just like ``typename``, or it survives next to the
    # ``typeNames`` gpio sets and the server picks a layer from conflicting
    # duplicate keys. Everything else (apikeys, tokens, vendor params) stays.
    wfs_control_keys = {
        "service",
        "request",
        "version",
        "typename",
        "typenames",
        "outputformat",
        "srsname",
        "count",
        "maxfeatures",
        "startindex",
        "resulttype",
    }
    params = {k: v for k, v in params.items() if k.lower() not in wfs_control_keys}

    # Rebuild URL
    new_query = urlencode(params, doseq=True) if params else ""
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            new_query,
            "",
        )
    )


def _merge_query_params(url: str, params: dict) -> str:
    """
    Merge additional query params into a URL that may already have a query string.

    Building the final URL by naive string concatenation (``f"{url}?{...}"``)
    corrupts any pre-existing query string (e.g. ``?apikey=mykey``) by adding
    a second ``?``, which has no delimiter meaning in a query string — the
    server sees a single, garbled query and everything after the first ``?``
    (including the real ``apikey`` value) is misparsed (Issue #828).

    New params take precedence over any same-named param already present in
    ``url`` (WFS control params like ``service``/``request``/``version``
    should always reflect the request being built, not a stale value carried
    over from the caller's URL).

    Args:
        url: Base URL, with or without an existing query string.
        params: Additional query params to merge in.

    Returns:
        URL with a single, correctly merged query string.
    """
    parsed = urlparse(url)
    existing_params = parse_qs(parsed.query, keep_blank_values=True)
    # dict values from parse_qs are lists; flatten before merging with new
    # scalar values so urlencode doesn't emit e.g. "apikey=['mykey']".
    merged = {k: v[0] if len(v) == 1 else v for k, v in existing_params.items()}
    merged.update(params)

    new_query = urlencode(merged, doseq=True)
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            new_query,
            "",
        )
    )


def get_wfs_capabilities(service_url: str, version: str = "1.1.0"):
    """
    Get WFS capabilities using OWSLib.

    Args:
        service_url: WFS service URL
        version: WFS version (1.0.0 or 1.1.0)

    Returns:
        OWSLib WebFeatureService object
    """
    try:
        from owslib.wfs import WebFeatureService
    except ImportError as e:
        raise WFSError(
            "owslib is required for WFS extraction. Install with: pip install owslib"
        ) from e

    clean_url = _clean_service_url(service_url)

    try:
        wfs = WebFeatureService(url=clean_url, version=version)
        return wfs
    except Exception as e:
        error_msg = str(e).lower()
        if "connection" in error_msg or "timeout" in error_msg:
            raise WFSError(f"Could not connect to WFS service: {clean_url}\nError: {e}") from e
        elif "xml" in error_msg or "parse" in error_msg:
            raise WFSError(
                f"Invalid WFS response from: {clean_url}\n"
                f"The server may not be a valid WFS service. Error: {e}"
            ) from e
        else:
            raise WFSError(f"Failed to get WFS capabilities: {e}") from e


# WFS versions in preference order (newest first)
WFS_VERSIONS = ["2.0.0", "1.1.0", "1.0.0"]

# Single source of truth for the default page size used when paginating WFS
# requests. Every entry point references it -- the CLI option
# (`gpio extract wfs --page-size`), the core functions (`wfs_to_table`,
# `convert_wfs_to_geoparquet`, `convert_wfs_layers_to_directory`,
# `fetch_all_features_duckdb`, `_fetch_with_spatial_tiles`) and the Python API
# wrappers (`ops.from_wfs`, `ops.from_wfs_layers`, `Table.from_wfs`) -- so their
# defaults cannot drift apart again.
#
# Note: the CLI option is typed `click.IntRange(1000, 500000)`, so any new
# value for this constant must fall inside that range.
DEFAULT_WFS_PAGE_SIZE: Final[int] = 100_000


def negotiate_wfs_version(service_url: str, preferred_version: str = "auto"):
    """
    Negotiate the best WFS version to use with the server.

    Args:
        service_url: WFS service URL
        preferred_version: "auto" to try all versions, or specific version

    Returns:
        Tuple of (negotiated_version, wfs_capabilities_object)

    Raises:
        WFSError: If no supported version works
    """
    if preferred_version != "auto":
        # User specified version, use it directly
        wfs = get_wfs_capabilities(service_url, preferred_version)
        return preferred_version, wfs

    # Auto-negotiate: try versions in preference order
    errors = []
    for version in WFS_VERSIONS:
        try:
            debug(f"Trying WFS version {version}...")
            wfs = get_wfs_capabilities(service_url, version)
            info(f"Using WFS version {version}")
            return version, wfs
        except WFSError as e:
            errors.append(f"  {version}: {e}")
            continue

    # All versions failed
    error_details = "\n".join(errors)
    raise WFSError(
        f"Could not connect to WFS service with any supported version.\n"
        f"Tried: {', '.join(WFS_VERSIONS)}\n"
        f"Errors:\n{error_details}"
    )


def _normalize_crs(crs: str) -> str:
    """
    Normalize CRS string to consistent EPSG format.

    Handles variants like:
    - EPSG:4326
    - urn:ogc:def:crs:EPSG::4326
    - http://www.opengis.net/def/crs/EPSG/0/4326

    Returns:
        Normalized EPSG string (e.g., "EPSG:4326")
    """
    import re

    # Already in simple format
    if re.match(r"^EPSG:\d+$", crs, re.IGNORECASE):
        return crs.upper()

    # URN format: urn:ogc:def:crs:EPSG::4326
    urn_match = re.search(r"EPSG::?(\d+)", crs, re.IGNORECASE)
    if urn_match:
        return f"EPSG:{urn_match.group(1)}"

    # HTTP format: http://www.opengis.net/def/crs/EPSG/0/4326
    http_match = re.search(r"EPSG/\d+/(\d+)", crs, re.IGNORECASE)
    if http_match:
        return f"EPSG:{http_match.group(1)}"

    # CRS84 is equivalent to EPSG:4326 (axis order differs but we handle that)
    if "CRS84" in crs.upper() or "CRS:84" in crs.upper():
        return "EPSG:4326"

    # Return as-is if no pattern matches
    return crs


def _crs_matches(crs1: str, crs2: str) -> bool:
    """Check if two CRS strings represent the same coordinate system."""
    return _normalize_crs(crs1) == _normalize_crs(crs2)


def _with_server_crs(table: pa.Table, server_crs: str | None) -> pa.Table:
    """Attach the server-declared CRS to a table's schema metadata.

    Carries the CRS the server reported in its response up through the
    fetch/concat/type-inference pipeline so wfs_to_table can trust it instead of
    guessing from coordinates. The source is the GeoJSON ``crs`` member or, for
    a GML body, the CRS GDAL reports on the geometry column. No-op when
    ``server_crs`` is None.
    """
    if not server_crs:
        return table
    metadata = dict(table.schema.metadata or {})
    metadata[_SERVER_CRS_METADATA_KEY] = server_crs.encode()
    return table.replace_schema_metadata(metadata)


def _read_server_crs(table: pa.Table) -> str | None:
    """Read the server-declared CRS previously attached via _with_server_crs."""
    metadata = table.schema.metadata or {}
    value = metadata.get(_SERVER_CRS_METADATA_KEY)
    return value.decode() if value else None


def _estimate_crs_from_bbox(
    bbox: tuple[float, float, float, float],
) -> str | None:
    """
    Estimate CRS from coordinate ranges (rough heuristic).

    This is a best-effort detection for common mismatches.
    Returns None if unable to determine.

    Args:
        bbox: (xmin, ymin, xmax, ymax)

    Returns:
        Estimated EPSG code or None
    """
    xmin, ymin, xmax, ymax = bbox

    # WGS84 / EPSG:4326: lon in [-180, 180], lat in [-90, 90]
    if -180 <= xmin <= 180 and -180 <= xmax <= 180 and -90 <= ymin <= 90 and -90 <= ymax <= 90:
        return "EPSG:4326"

    # ETRS89-LAEA / EPSG:3035: European extent roughly 2M-7M x 1M-5.5M
    # Check this BEFORE Web Mercator since it's more specific
    if 2_000_000 < xmin < 7_500_000 and 2_000_000 < xmax < 7_500_000:
        if 1_000_000 < ymin < 5_500_000 and 1_000_000 < ymax < 5_500_000:
            return "EPSG:3035"

    # Web Mercator / EPSG:3857: typically ±20M meters, centered at 0,0
    # Uses wider ranges than EPSG:3035
    if abs(xmin) > 1_000_000 or abs(xmax) > 1_000_000:
        if abs(xmin) < 21_000_000 and abs(xmax) < 21_000_000:
            if abs(ymin) < 21_000_000 and abs(ymax) < 21_000_000:
                return "EPSG:3857"

    return None


def _validate_crs_coordinates(
    table: pa.Table,
    requested_crs: str,
    strict: bool = False,
) -> tuple[bool, str | None]:
    """
    Validate that table coordinates match the requested CRS.

    Computes bbox from geometries and checks if coordinates are in expected range.

    Args:
        table: PyArrow table with geometry column
        requested_crs: CRS that was requested from WFS
        strict: If True, raise error on mismatch; if False, warn and return detected CRS

    Returns:
        Tuple of (is_valid, detected_crs_if_mismatch)
    """
    if table.num_rows == 0:
        return True, None

    try:
        # Extract bbox from geometry column using DuckDB
        # Geometry is stored as WKB, so we need ST_GeomFromWKB to convert
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            con.register("data", table)
            result = con.execute("""
                SELECT
                    MIN(ST_XMin(ST_GeomFromWKB(geometry))) as xmin,
                    MIN(ST_YMin(ST_GeomFromWKB(geometry))) as ymin,
                    MAX(ST_XMax(ST_GeomFromWKB(geometry))) as xmax,
                    MAX(ST_YMax(ST_GeomFromWKB(geometry))) as ymax
                FROM data
                WHERE geometry IS NOT NULL
            """).fetchone()

            if result is None or result[0] is None:
                return True, None

            bbox = (result[0], result[1], result[2], result[3])
        finally:
            con.close()

        # Validate coordinates match requested CRS
        normalized_crs = _normalize_crs(requested_crs)
        xmin, ymin, xmax, ymax = bbox
        detected = _estimate_crs_from_bbox(bbox)

        # Check for mismatch between requested and detected CRS
        mismatch = False
        if normalized_crs == "EPSG:4326":
            # WGS84 coordinates should be in valid range
            if abs(xmin) > 180 or abs(xmax) > 180 or abs(ymin) > 90 or abs(ymax) > 90:
                mismatch = True
        elif normalized_crs == "EPSG:3857":
            # Web Mercator should have large meter values, not small degree values
            if abs(xmin) < 1000 and abs(xmax) < 1000 and abs(ymin) < 1000 and abs(ymax) < 1000:
                mismatch = True
        elif normalized_crs == "EPSG:3035":
            # LAEA Europe should be in specific range
            if not (2_000_000 < xmin < 7_500_000 and 1_000_000 < ymin < 5_500_000):
                mismatch = True
        elif detected and detected != normalized_crs:
            # The bbox heuristic can reliably distinguish coordinate *categories*
            # (geographic degrees vs. projected meters) but NOT one projected CRS
            # from another — e.g. EPSG:22174 (POSGAR 98 / Argentina 4) and
            # EPSG:3857 both use large metric coordinates. Only treat this as a
            # mismatch when the categories disagree; otherwise trust the
            # server-honored CRS rather than relabel it on a guess, which silently
            # corrupts downstream reprojection (issue #499).
            requested_geographic = _is_geographic_crs(normalized_crs)
            detected_geographic = detected == "EPSG:4326"
            if requested_geographic != detected_geographic:
                mismatch = True

        if mismatch:
            msg = (
                f"Coordinate mismatch: requested {requested_crs} but got bbox "
                f"[{xmin:.2f}, {ymin:.2f}, {xmax:.2f}, {ymax:.2f}]. "
            )
            if detected:
                msg += f"Coordinates look like {detected}. Server may have ignored srsName."
            else:
                msg += "Server may have returned data in a different CRS."

            if strict:
                raise WFSError(msg)
            else:
                warn(msg)
                return False, detected

        return True, None

    except WFSError:
        raise
    except Exception as e:
        debug(f"CRS validation skipped due to error: {e}")
        return True, None


def _reconcile_source_crs(
    table: pa.Table,
    *,
    source_crs: str,
    requested_crs: str,
    output_crs: str | None,
    strict_crs: bool,
    origin: str,
) -> tuple[pa.Table, str]:
    """Reconcile a known source CRS against the requested CRS.

    ``source_crs`` is the CRS the data is actually in (from the server's GeoJSON
    ``crs`` member, or — as a last resort — guessed from coordinates). When it
    matches what we requested, the data is trusted as-is. When it genuinely
    differs, the server ignored ``srsName``; we reproject to ``output_crs`` if
    one was given, raise under ``strict_crs``, else label the output with the
    real ``source_crs`` rather than silently mislabeling it (issue #499).
    """
    if _crs_matches(source_crs, requested_crs):
        return table, requested_crs

    lead = "Server declared" if origin == "server" else "Coordinates look like"
    base_msg = (
        f"{lead} {source_crs} but {requested_crs} was requested; server may have ignored srsName."
    )

    if strict_crs:
        raise WFSError(base_msg)

    if output_crs:
        info(f"{base_msg} Reprojecting from {source_crs} to {output_crs}.")
        try:
            table = reproject_table(table, target_crs=output_crs, source_crs=source_crs)
        except Exception as e:
            raise WFSError(f"Failed to reproject from {source_crs} to {output_crs}: {e}") from e
        return table, output_crs

    warn(f"{base_msg} Labeling output with {source_crs}.")
    return table, source_crs


def _resolve_crs_for_output(
    table: pa.Table,
    requested_crs: str,
    output_crs: str | None,
    strict_crs: bool,
) -> tuple[pa.Table, str]:
    """Decide the CRS to label (and optionally reproject) the fetched data to.

    Trust order:
      1. The CRS the server declared in its response — authoritative.
         When present we never second-guess it with coordinate heuristics.
      2. A bbox-coordinate guess — last resort, only when the server declared
         nothing. The guess cannot tell two projected CRSs apart, so it is used
         conservatively (see _validate_crs_coordinates).

    Returns ``(table, crs)`` where ``table`` may have been reprojected.
    """
    server_crs = _read_server_crs(table)
    if server_crs:
        debug(f"Server declared CRS {server_crs} in its response")
        return _reconcile_source_crs(
            table,
            source_crs=server_crs,
            requested_crs=requested_crs,
            output_crs=output_crs,
            strict_crs=strict_crs,
            origin="server",
        )

    debug(
        f"Server declared no CRS in its response; inferring from "
        f"coordinates and trusting the requested {requested_crs} unless they clearly disagree."
    )
    crs_valid, detected_crs = _validate_crs_coordinates(table, requested_crs, strict=strict_crs)
    if crs_valid or not detected_crs:
        return table, requested_crs
    # A real mismatch under strict_crs already raised inside
    # _validate_crs_coordinates, so reconciliation here is only ever reached in
    # non-strict mode — pass strict_crs=False to keep that the single source of
    # truth and avoid a redundant raise.
    return _reconcile_source_crs(
        table,
        source_crs=detected_crs,
        requested_crs=requested_crs,
        output_crs=output_crs,
        strict_crs=False,
        origin="detected",
    )


def _detect_geometry_column(wfs, typename: str) -> str:
    """
    Detect geometry column name from DescribeFeatureType.

    Args:
        wfs: OWSLib WebFeatureService object
        typename: Layer typename

    Returns:
        Geometry column name (default: "geometry")
    """
    try:
        schema = wfs.get_schema(typename)
        if schema and "geometry" in schema:
            return str(schema.get("geometry_column", "geometry"))
        # Check for common geometry column names in properties
        if schema and "properties" in schema:
            for prop_name, prop_type in schema["properties"].items():
                if any(
                    geom in str(prop_type).lower()
                    for geom in ["geometry", "point", "line", "polygon", "multi"]
                ):
                    return str(prop_name)
    except Exception:
        pass  # Fall back to default

    return "geometry"


def _detect_sortable_attribute(wfs, typename: str) -> str | None:
    """
    Detect a sortable (non-geometry) attribute from DescribeFeatureType.

    GeoServer requires a sortBy parameter for stable pagination on layers
    without a primary key. This function finds the first non-geometry
    attribute that can be used for sorting.

    Args:
        wfs: OWSLib WebFeatureService object
        typename: Layer typename

    Returns:
        First sortable attribute name, or None if none found
    """
    try:
        schema = wfs.get_schema(typename)
        if schema and "properties" in schema:
            geometry_col = schema.get("geometry_column", "geometry")
            for prop_name, prop_type in schema["properties"].items():
                # Skip geometry columns
                if prop_name == geometry_col:
                    continue
                prop_type_str = str(prop_type).lower()
                if any(
                    geom in prop_type_str
                    for geom in ["geometry", "point", "line", "polygon", "multi", "curve"]
                ):
                    continue
                # Found a non-geometry attribute
                return str(prop_name)
    except Exception:
        pass
    return None


def _lookup_parameter_ci(parameters: dict, name: str) -> list | None:
    """Read an operation parameter's allowed values, ignoring capitalization.

    OWSLib keys ``operation.parameters`` by the literal ``name`` attribute in
    the capabilities XML. The WFS 2.0 spec writes ``outputFormat``, but NextGIS
    and deegree publish ``OutputFormat``, so a case-sensitive lookup reported
    *no* advertised formats for those servers — after which gpio fell back to
    demanding GeoJSON from a GML-only service.
    See https://github.com/geoparquet/geoparquet-io/issues/818
    """
    if not parameters:
        return None
    wanted = name.lower()
    for key, value in parameters.items():
        if str(key).lower() != wanted:
            continue
        if isinstance(value, dict) and value.get("values"):
            return list(value["values"])
    return None


def get_layer_info(service_url: str, typename: str, version: str = "1.1.0") -> WFSLayerInfo:
    """
    Get metadata for a specific WFS layer.

    Args:
        service_url: WFS service URL
        typename: Layer typename (with or without namespace prefix)
        version: WFS version

    Returns:
        WFSLayerInfo dataclass with layer metadata
    """
    wfs = get_wfs_capabilities(service_url, version)

    # Find the layer (handle namespace variations)
    layer = None
    matched_typename = typename

    if typename in wfs.contents:
        layer = wfs.contents[typename]
    else:
        # Try without namespace prefix
        short_name = typename.split(":")[-1] if ":" in typename else typename
        for key in wfs.contents:
            if key.endswith(f":{short_name}") or key == short_name:
                layer = wfs.contents[key]
                matched_typename = key
                break

    if layer is None:
        available = list(wfs.contents.keys())[:10]
        raise LayerNotFoundError(typename, available)

    # Extract CRS list
    crs_list = []
    default_crs = None

    if hasattr(layer, "crsOptions") and layer.crsOptions:
        crs_list = [str(crs) for crs in layer.crsOptions]
        default_crs = crs_list[0] if crs_list else None

    # Extract bounding box
    bbox = None
    if hasattr(layer, "boundingBoxWGS84") and layer.boundingBoxWGS84:
        bbox = tuple(layer.boundingBoxWGS84)

    # Detect geometry column
    geometry_column = _detect_geometry_column(wfs, matched_typename)

    # Get available output formats
    available_formats = []

    # Method 1: WFS 1.1.0+ - Check operations metadata parameters
    if hasattr(wfs, "operations"):
        for op in wfs.operations:
            if op.name == "GetFeature" and hasattr(op, "parameters"):
                values = _lookup_parameter_ci(op.parameters, "outputFormat")
                if values:
                    available_formats = list(values)
                    break

    # Method 2: Legacy attribute (some OWSLib versions)
    if not available_formats and hasattr(wfs, "getfeature_output_formats"):
        available_formats = list(wfs.getfeature_output_formats)

    # Method 3: Fall back to capabilities XML parsing (WFS 1.0.0 style)
    if not available_formats and hasattr(wfs, "capabilities") and wfs.capabilities:
        try:
            from owslib.util import nspath_eval

            ns = wfs.namespaces
            getfeature = wfs.capabilities.find(
                nspath_eval("wfs:Capability/wfs:Request/wfs:GetFeature", ns)
            )
            if getfeature is not None:
                for fmt in getfeature.findall(nspath_eval("wfs:ResultFormat/*", ns)):
                    available_formats.append(fmt.tag.split("}")[-1])
        except Exception:
            pass

    # Detect sortable attribute for stable pagination (Issue #488)
    sortable_attribute = _detect_sortable_attribute(wfs, matched_typename)

    return WFSLayerInfo(
        typename=matched_typename,
        title=getattr(layer, "title", None),
        crs_list=crs_list,
        default_crs=default_crs,
        bbox=bbox,
        geometry_column=geometry_column,
        available_formats=available_formats,
        sortable_attribute=sortable_attribute,
    )


def list_available_layers(service_url: str, version: str = "1.1.0") -> list[dict]:
    """
    List available layers in a WFS service.

    Args:
        service_url: WFS service URL
        version: WFS version

    Returns:
        List of dicts with layer info (name, typename, title, abstract, bbox)
    """
    wfs = get_wfs_capabilities(service_url, version)

    layers = []
    for typename, layer in wfs.contents.items():
        layers.append(
            {
                "name": typename,  # Alias for consistency with CLI
                "typename": typename,
                "title": getattr(layer, "title", None),
                "abstract": getattr(layer, "abstract", None),
                "bbox": tuple(layer.boundingBoxWGS84)
                if hasattr(layer, "boundingBoxWGS84") and layer.boundingBoxWGS84
                else None,
            }
        )

    return layers


def _normalize_output_format(fmt: str) -> str:
    """Fold an outputFormat string to a comparable form.

    Servers differ only cosmetically: ``application/gml+xml;version=3.2`` and
    ``application/gml+xml; version=3.2`` are the same format, and case is not
    significant in a media type. Lowercasing and dropping all whitespace makes
    those variants compare equal.
    """
    return re.sub(r"\s+", "", fmt).lower()


def _first_matching_format(
    available: list[str], available_norm: list[str], preferred: list[str]
) -> str | None:
    """Return the caller's spelling of the most preferred format on offer.

    Two passes: exact (normalized) equality first, so a server advertising both
    ``gml3`` and ``gml32`` still gets the preference order below honored, then a
    parameter match so an unlisted version suffix (``…;version=3.2.1``) still
    resolves to its family instead of being missed.

    The second pass is deliberately not a plain prefix test: that would read
    ``application/json-seq`` (JSON *text sequences*, which are not a
    FeatureCollection) as ``application/json``, and ``gml3-something`` as
    ``gml3``. Only a media-type parameter -- the part after ``;`` -- may differ.
    """
    for want in preferred:
        want_norm = _normalize_output_format(want)
        for i, got in enumerate(available_norm):
            if got == want_norm:
                return available[i]
    for want in preferred:
        want_norm = _normalize_output_format(want)
        for i, got in enumerate(available_norm):
            if got.startswith(want_norm) and got[len(want_norm) :].startswith(";"):
                return available[i]
    return None


# C0 controls and DEL. h11 refuses to put any of these in a header value, and
# a WFS outputFormat is a media type or a bare token -- never a control byte.
_CONTROL_CHARACTERS: Final = re.compile(r"[\x00-\x1f\x7f]")


def _sanitize_output_format(fmt: str) -> str:
    """Drop control characters from a format string the server advertised.

    A capabilities document is server-controlled text, and a value such as
    ``application/json;\\r\\nX-Evil: 1`` matches ``application/json`` on the
    normalized comparison (which drops whitespace) while being returned in the
    server's own spelling. Echoed back unchanged it is not an injection -- h11
    rejects an illegal header value before it reaches the socket -- but the user
    pays three retries to reach an opaque "Illegal header value" (issue #838).

    Sanitizing here, at the single point where a negotiated format is produced,
    rather than inside ``_accept_header_for``, is deliberate: the same value is
    also sent back as the ``outputFormat=`` query parameter, where urlencode
    would percent-encode the control bytes into the URL instead of rejecting
    them.
    """
    return _CONTROL_CHARACTERS.sub("", fmt)


def _detect_best_output_format(available_formats: list[str]) -> str | None:
    """
    Detect the best output format from available formats.

    Prefers GeoJSON for faster parsing, falls back to GML.

    The two ways of not finding a match are answered differently, because they
    say different things (issue #818):

    * The server advertised formats and none is one gpio can read. Returning
      ``None`` omits ``outputFormat`` from the request so the server answers in
      its own default. Naming a format the server has just said it does not
      support is how gpio used to demand GeoJSON from a GML-only service, and
      picking the first advertised one instead is no better — on GeoServer that
      can be ``SHAPE-ZIP``.
    * The server advertised nothing at all, so there is nothing to honor. gpio
      keeps asking for ``application/json``, which is what it has always sent
      and what every GeoJSON-capable service answers. A server that ignores the
      parameter and replies with GML is now parsed on the response content type
      rather than rejected, so this costs nothing when the guess is wrong.

    Args:
        available_formats: List of format strings from capabilities

    Returns:
        Best format string to request, or None to let the server decide
    """
    if not available_formats:
        return DEFAULT_OUTPUT_FORMAT

    available_norm = [_normalize_output_format(f) for f in available_formats]

    # GeoJSON is preferred - faster to parse - then GML.
    matched = _first_matching_format(
        available_formats, available_norm, GEOJSON_FORMATS
    ) or _first_matching_format(available_formats, available_norm, GML_FORMATS)
    return _sanitize_output_format(matched) if matched is not None else None


def _negotiate_crs(layer_info: WFSLayerInfo, output_crs: str | None = None) -> str:
    """
    Negotiate the best CRS to request from the WFS server.

    Strategy:
    1. If output_crs specified and supported -> use it
    2. Try EPSG:4326 variants (most universal)
    3. Fall back to server default

    Args:
        layer_info: Layer metadata with CRS list
        output_crs: User-requested output CRS

    Returns:
        CRS string to use in requests
    """
    crs_list = layer_info.crs_list

    # If user specified CRS, check if supported
    if output_crs:
        for crs in crs_list:
            if _crs_matches(crs, output_crs):
                debug(f"Using requested CRS: {crs}")
                return crs
        warn(f"Requested CRS '{output_crs}' not in layer's CRS list. Using server default.")

    # Try EPSG:4326 variants
    for crs in crs_list:
        if _crs_matches(crs, "EPSG:4326"):
            debug(f"Using WGS84: {crs}")
            return crs

    # Fall back to default
    if layer_info.default_crs:
        debug(f"Using server default CRS: {layer_info.default_crs}")
        return layer_info.default_crs

    # Last resort
    if crs_list:
        debug(f"Using first available CRS: {crs_list[0]}")
        return crs_list[0]

    return "EPSG:4326"


def _determine_bbox_strategy(
    bbox_mode: str,
    layer_info: WFSLayerInfo,
) -> bool:
    """
    Determine whether to use server-side bbox filtering.

    Unlike BigQuery, WFS doesn't easily expose row counts, so auto mode
    defaults to server-side filtering (conservative for remote services).

    Args:
        bbox_mode: "auto", "server", or "local"
        layer_info: Layer metadata (reserved for future use)

    Returns:
        True if server-side filtering should be used
    """
    # layer_info reserved for future use (e.g., checking server capabilities)
    _ = layer_info

    if bbox_mode == "server":
        debug("Using server-side bbox filter (forced by --bbox-mode server)")
        return True
    if bbox_mode == "local":
        debug("Using local bbox filter (forced by --bbox-mode local)")
        return False

    # Auto mode: default to server-side for WFS
    # WFS servers typically handle spatial filtering efficiently
    debug("Using server-side bbox filter (auto mode for WFS)")
    return True


def _is_urn_crs(crs: str) -> bool:
    """Check if CRS string is in URN format (urn:ogc:def:crs:EPSG::4326)."""
    return crs.lower().startswith("urn:")


def _is_geographic_crs(crs: str) -> bool:
    """Check if CRS is a geographic coordinate system (lat/lon, not projected).

    Uses pyproj for authoritative detection across all geographic CRSs rather
    than a hardcoded list — an allowlist misclassifies valid geographic systems
    outside it (e.g. EPSG:4171 RGF93) as projected, which would flag a false
    coordinate mismatch and relabel correct data to EPSG:4326. Falls back to a
    small known set only when pyproj cannot resolve the CRS.
    """
    normalized = _normalize_crs(crs)
    try:
        from pyproj import CRS as _PyprojCRS

        return bool(_PyprojCRS.from_user_input(normalized).is_geographic)
    except Exception:
        # pyproj unavailable or CRS unrecognized — fall back to a known set.
        geographic_codes = {
            "EPSG:4326",  # WGS84
            "EPSG:4269",  # NAD83
            "EPSG:4267",  # NAD27
            "EPSG:4258",  # ETRS89
        }
        return normalized in geographic_codes or "CRS84" in crs.upper()


def _needs_axis_swap(crs: str, version: str, axis_order: str = "auto") -> bool:
    """
    Determine if bbox coordinates need axis order swap (lat,lon instead of lon,lat).

    Per OGC spec:
    - WFS 1.0.0: Always lon,lat (XY)
    - WFS 1.1.0+: Depends on CRS definition
      - EPSG:4326 (simple): lon,lat (common practice, though spec says lat,lon)
      - urn:ogc:def:crs:EPSG::4326: lat,lon per spec (YX)
      - CRS:84: Always lon,lat (XY) by definition

    Args:
        crs: Coordinate reference system string
        version: WFS version
        axis_order: "auto" (detect), "xy" (force lon,lat), "latlon" (force lat,lon)

    Returns:
        True if bbox needs to be swapped to lat,lon order
    """
    if axis_order == "xy":
        return False
    if axis_order == "latlon":
        return True

    # WFS 1.0.0 always uses XY order
    if version == "1.0.0":
        return False

    # CRS:84 is explicitly XY ordered
    if "CRS84" in crs.upper() or "CRS:84" in crs.upper():
        return False

    # For WFS 1.1.0+: URN format with geographic CRS uses lat,lon
    if _is_urn_crs(crs) and _is_geographic_crs(crs):
        return True

    return False


def _build_bbox_param(
    bbox: tuple[float, float, float, float],
    crs: str,
    version: str = "1.1.0",
    axis_order: str = "auto",
) -> str:
    """
    Build WFS bbox parameter string with correct axis order.

    IMPORTANT: Bbox is always sent in WGS84 (EPSG:4326) regardless of output CRS,
    because many WFS servers (especially GeoServer) only accept bbox in WGS84.
    The bbox coordinates are expected to be in lon,lat (WGS84) order.

    WFS 1.0.0: xmin,ymin,xmax,ymax (always XY, no CRS suffix)
    WFS 1.1.0+: xmin,ymin,xmax,ymax,EPSG:4326 (always WGS84)

    Args:
        bbox: Bounding box tuple (xmin, ymin, xmax, ymax) in WGS84 lon,lat order
        crs: Output CRS (ignored for bbox - we always use WGS84)
        version: WFS version
        axis_order: "auto" (detect from CRS), "xy" (force lon,lat), "latlon" (force lat,lon)

    Returns:
        Bbox parameter string in WGS84
    """
    xmin, ymin, xmax, ymax = bbox

    if version == "1.0.0":
        return f"{xmin},{ymin},{xmax},{ymax}"

    # Always use WGS84 for bbox - most WFS servers expect this regardless of output CRS
    # Use simple EPSG:4326 format (not URN) to ensure XY axis order
    return f"{xmin},{ymin},{xmax},{ymax},EPSG:4326"


def _validate_identifier(name: str) -> str:
    """
    Validate and sanitize a SQL identifier (column name).

    Args:
        name: Column name to validate

    Returns:
        Validated column name

    Raises:
        WFSError: If the name contains invalid characters
    """
    # Only allow alphanumeric, underscore, and standard identifier characters
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
        # Check for dangerous patterns
        if '"' in name or "'" in name or ";" in name or "--" in name:
            raise WFSError(f"Invalid geometry column name '{name}': contains unsafe characters")
        # Allow dots for qualified names but escape them
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_\.]*$", name):
            raise WFSError(f"Invalid geometry column name '{name}': must be a valid identifier")
    return name


def _build_local_bbox_filter(
    bbox: tuple[float, float, float, float],
    geometry_column: str,
) -> str:
    """
    Build DuckDB SQL filter for local bbox filtering.

    Args:
        bbox: Bounding box tuple (xmin, ymin, xmax, ymax)
        geometry_column: Name of geometry column

    Returns:
        DuckDB ST_Intersects SQL condition

    Raises:
        WFSError: If geometry column name is invalid
    """
    # Validate column name to prevent SQL injection
    safe_column = _validate_identifier(geometry_column)

    xmin, ymin, xmax, ymax = bbox
    wkt = f"POLYGON(({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))"
    # Fetched features carry geometry as WKB (BLOB), so decode it before the
    # spatial predicate — ST_Intersects has no BLOB overload.
    # _validate_identifier already rejects `"`, so quote_identifier is a no-op
    # for every name that reaches here -- it is used anyway so the quoting rule
    # is enforced at the interpolation site rather than by a distant allowlist
    # that a later edit could loosen (#936).
    return (
        f"ST_Intersects(ST_GeomFromWKB({quote_identifier(safe_column)}), ST_GeomFromText('{wkt}'))"
    )


def _reproject_bbox_for_local_filter(
    bbox: tuple[float, float, float, float], source_crs: str, target_crs: str
) -> tuple[float, float, float, float]:
    """Reproject a filter bbox from ``source_crs`` into ``target_crs``.

    The local bbox filter compares fetched geometries against this bbox. The
    bbox is expressed in the requested CRS, but when the server honors a
    different CRS the geometries are in *that* CRS — comparing the two as-is
    drops valid rows (issue #499). Aligning the bbox to the geometries' CRS
    avoids reprojecting the whole geometry column just to filter. On failure we
    fall back to the original bbox rather than crash the extraction.
    """
    try:
        from pyproj import Transformer

        transformer = Transformer.from_crs(
            _normalize_crs(source_crs), _normalize_crs(target_crs), always_xy=True
        )
        xmin, ymin, xmax, ymax = transformer.transform_bounds(*bbox)
        return (xmin, ymin, xmax, ymax)
    except Exception as e:
        warn(f"Could not reproject bbox from {source_crs} to {target_crs} for local filter: {e}")
        return bbox


def _get_feature_count(
    service_url: str,
    typename: str,
    version: str = "1.1.0",
    bbox: tuple[float, float, float, float] | None = None,
    crs: str | None = None,
    axis_order: str = "auto",
    warn_on_failure: bool = True,
) -> int | None:
    """
    Try to get feature count using resultType=hits.

    This is a WFS 1.1.0+ feature that returns count without fetching data.

    Args:
        service_url: WFS service URL
        typename: Layer typename
        version: WFS version
        bbox: Optional bounding box filter (xmin, ymin, xmax, ymax)
        crs: CRS for bbox parameter
        axis_order: Bbox axis order ("auto", "xy", "latlon")
        warn_on_failure: Log a warning when the probe fails. Pass False for
            speculative probes that have a fallback by construction (see the
            2.0.0-then-negotiated-version probe in ``wfs_to_table``), where a
            failure is an expected answer rather than a loss of capability.

    Returns:
        Feature count, or None if the server does not report one *or* the probe
        failed. A failed probe is logged as a warning (unless suppressed via
        ``warn_on_failure``) so the resulting loss of auto-tiling / pagination
        is visible in the logs.
    """
    if version == "1.0.0":
        return None  # resultType=hits not supported in WFS 1.0.0

    import httpx

    clean_url = _clean_service_url(service_url)
    params = {
        "service": "WFS",
        "version": version,
        "request": "GetFeature",
        "typeNames" if version == "2.0.0" else "typeName": typename,
        "resultType": "hits",
    }

    if bbox and crs:
        params["bbox"] = _build_bbox_param(bbox, crs, version, axis_order)

    # Merge the WFS params into the URL instead of passing params= to the
    # HTTP layer: httpx *replaces* a URL's existing query when params= is
    # given, which would drop e.g. an apikey the service URL carries — the
    # probe then 403s, the failure degrades to count=None, and pagination /
    # auto-tiling silently disengage (issues #828, #678).
    url = _merge_query_params(clean_url, params)

    try:
        content = _make_request(url)
    except (WFSError, httpx.HTTPError, OSError) as e:
        # Expected failure modes: the service is down, unreachable, rejecting us,
        # or timing out. Degrade to "count unknown" rather than aborting the
        # extraction, but say so — a silent None disables auto-tiling and
        # pagination, which is how servers return silently truncated results
        # (issue #678). Programming errors are not caught: they must surface.
        detail = f"{type(e).__name__}: {e}"
        if not warn_on_failure:
            debug(
                f"Speculative feature count probe failed for '{typename}' "
                f"at WFS {version}: {detail}"
            )
            return None
        warn(
            f"Feature count probe (resultType=hits) failed for '{typename}' "
            f"at WFS {version}: {detail}. Continuing without a "
            "server feature count — auto-tiling and pagination cannot engage, "
            "so results from a server that caps responses may be incomplete."
        )
        return None

    # Parse XML response to find numberOfFeatures
    match = re.search(rb'numberOfFeatures="(\d+)"', content)
    if match:
        return int(match.group(1))

    # Try alternative attribute name
    match = re.search(rb'numberMatched="(\d+)"', content)
    if match:
        return int(match.group(1))

    # Server answered but reported no count — a normal outcome, not a failure.
    debug(f"WFS {version} hits response for '{typename}' carried no feature count")
    return None


def _infer_column_types(table: pa.Table) -> pa.Table:
    """
    Infer and cast string columns to appropriate types.

    WFS servers often serialize all values as quoted strings in JSON,
    causing DuckDB to infer them as VARCHAR. This function detects
    columns that can be safely cast to int64, float64, or bool.

    Args:
        table: PyArrow table with potentially mis-typed string columns

    Returns:
        Table with string columns cast to inferred types
    """
    if table.num_rows == 0:
        return table

    con = get_duckdb_connection()
    try:
        con.register("_wfs_data", table)

        new_columns = []
        for field in table.schema:
            col_name = field.name

            # Only process string/large_string columns
            if field.type not in (pa.string(), pa.large_string()):
                new_columns.append((col_name, table[col_name]))
                continue

            # Check type compatibility using TRY_CAST
            # Quote column name to handle reserved words/special chars
            quoted_col = quote_identifier(col_name)

            try:
                stats = con.execute(f"""
                    SELECT
                        COUNT(*) AS total,
                        COUNT({quoted_col}) AS non_null,
                        SUM(CASE WHEN TRY_CAST({quoted_col} AS BIGINT)::VARCHAR = {quoted_col}
                            THEN 1 ELSE 0 END) AS is_int,
                        SUM(CASE WHEN TRY_CAST({quoted_col} AS DOUBLE) IS NOT NULL
                            THEN 1 ELSE 0 END) AS is_float,
                        SUM(CASE WHEN LOWER({quoted_col}) IN ('true', 'false', '1', '0')
                            THEN 1 ELSE 0 END) AS is_bool
                    FROM _wfs_data
                """).fetchone()

                total, non_null, is_int, is_float, is_bool = stats

                # Skip columns with all nulls
                if non_null == 0:
                    new_columns.append((col_name, table[col_name]))
                    continue

                # Determine target type (order matters: int > float > bool > string)
                if is_int == non_null:
                    # All non-null values are valid integers
                    casted = (
                        con.execute(f"""
                        SELECT CAST({quoted_col} AS BIGINT) FROM _wfs_data
                    """)
                        .arrow()
                        .read_all()
                        .column(0)
                    )
                    new_columns.append((col_name, casted))
                elif is_float == non_null and is_int < non_null:
                    # All non-null values are valid floats (but not all integers)
                    casted = (
                        con.execute(f"""
                        SELECT CAST({quoted_col} AS DOUBLE) FROM _wfs_data
                    """)
                        .arrow()
                        .read_all()
                        .column(0)
                    )
                    new_columns.append((col_name, casted))
                elif is_bool == non_null:
                    # All non-null values are valid booleans
                    casted = (
                        con.execute(f"""
                        SELECT CASE
                            WHEN LOWER({quoted_col}) IN ('true', '1') THEN TRUE
                            WHEN LOWER({quoted_col}) IN ('false', '0') THEN FALSE
                            ELSE NULL
                        END
                        FROM _wfs_data
                    """)
                        .arrow()
                        .read_all()
                        .column(0)
                    )
                    new_columns.append((col_name, casted))
                else:
                    # Keep as string
                    new_columns.append((col_name, table[col_name]))

            except Exception as e:
                # On any error, keep original column
                debug(f"Type inference failed for column '{col_name}': {e}")
                new_columns.append((col_name, table[col_name]))

        con.unregister("_wfs_data")
        return pa.table(dict(new_columns))
    finally:
        con.close()


def _probe_properties_type(
    con: duckdb.DuckDBPyConnection, response_path: str, max_object_size: int
) -> tuple[str, bool]:
    """
    Probe the type of feature.properties to determine if unnest() will work.

    Empty properties ({}) become MAP(VARCHAR, JSON), null properties become JSON,
    missing properties key raises an error - none can be unnested.
    Only STRUCT types support unnest().

    Args:
        con: DuckDB connection with spatial extension loaded
        response_path: RAW path to the GeoJSON file; escaped here by ``sql_path``
        max_object_size: Maximum JSON object size for read_json_auto

    Returns:
        Tuple of (props_type string, can_unnest_props bool)

    See: https://github.com/geoparquet/geoparquet-io/issues/441
    """
    try:
        props_type_result = con.execute(f"""
            WITH features AS (
                SELECT unnest(features) AS feature
                FROM read_json_auto({sql_path(response_path)}, maximum_object_size={max_object_size})
            )
            SELECT typeof(feature.properties) AS props_type
            FROM features
            LIMIT 1
        """).fetchone()
        props_type = props_type_result[0] if props_type_result else "NULL"
    except duckdb.BinderException as e:
        # Missing 'properties' key in feature struct (malformed GeoJSON per RFC 7946)
        if "properties" in str(e):
            props_type = "MISSING"
        else:
            raise

    can_unnest_props = props_type.startswith("STRUCT")
    return props_type, can_unnest_props


def _build_wfs_feature_query(
    response_path: str, extract_fid: bool, can_unnest_props: bool, max_object_size: int
) -> str:
    """
    Build SQL query to extract WFS features from GeoJSON.

    Args:
        response_path: RAW path to the GeoJSON file; escaped here by ``sql_path``
        extract_fid: If True, include feature.id as _wfs_fid column
        can_unnest_props: If True, unnest properties into columns; otherwise geometry-only
        max_object_size: Maximum JSON object size for read_json_auto

    Returns:
        SQL query string
    """
    fid_col = "feature.id AS _wfs_fid," if extract_fid else ""
    fid_select = "_wfs_fid," if extract_fid else ""

    if can_unnest_props:
        # Normal path: unnest properties into separate columns
        return f"""
            WITH features AS (
                SELECT unnest(features) AS feature
                FROM read_json_auto({sql_path(response_path)}, maximum_object_size={max_object_size})
            ),
            extracted AS (
                SELECT
                    {fid_col}
                    ST_AsWKB(ST_GeomFromGeoJSON(feature.geometry)) AS geometry,
                    feature.properties AS props
                FROM features
            )
            SELECT {fid_select} geometry, unnest(props) FROM extracted
        """
    else:
        # Fallback: properties are empty/null/missing (MAP, JSON, or MISSING type),
        # return geometry-only table
        return f"""
            WITH features AS (
                SELECT unnest(features) AS feature
                FROM read_json_auto({sql_path(response_path)}, maximum_object_size={max_object_size})
            )
            SELECT
                {fid_col}
                ST_AsWKB(ST_GeomFromGeoJSON(feature.geometry)) AS geometry
            FROM features
        """


def _read_count_and_server_crs(
    con: duckdb.DuckDBPyConnection, response_path: str
) -> tuple[int, str | None]:
    """Read the feature count and the server-declared CRS in a single pass.

    A WFS GeoJSON response is one FeatureCollection object, so we read it once
    as raw text and pull out both things we need before parsing features:

    * the ``features`` array length — DuckDB can't UNNEST an empty array, so we
      must branch on the count;
    * the optional top-level ``crs`` member. WFS servers (e.g. GeoServer) echo
      the CRS they actually honored there, e.g. ``{"crs": {"properties":
      {"name": "urn:ogc:def:crs:EPSG::22174"}}}``. This is authoritative — when
      present we never have to guess the CRS from coordinate ranges.

    ``json_extract_string`` returns NULL (rather than raising) when the ``crs``
    member is absent (RFC 7946 GeoJSON has none) or has an unexpected shape, so
    we transparently fall back to the coordinate heuristic. Reading once here
    replaces a separate count query and a separate CRS query with one scan.

    See https://github.com/geoparquet/geoparquet-io/issues/499
    """
    row = con.execute(f"""
        SELECT
            json_array_length(content, '$.features') AS cnt,
            json_extract_string(content, '$.crs.properties.name') AS crs_name
        FROM read_text({sql_path(response_path)})
    """).fetchone()
    if not row:
        return 0, None
    feature_count = row[0] or 0
    server_crs = _normalize_crs(str(row[1])) if row[1] else None
    return feature_count, server_crs


# Content types that carry a GML FeatureCollection. WFS 1.x servers -- and
# NextGIS at every version -- answer GetFeature with GML under one of these,
# regardless of the outputFormat that was requested, so the *response* type is
# what decides how gpio parses. An ows:ExceptionReport arrives under the same
# types, which is why the GML parse falls back to the error-page message rather
# than the content type deciding on its own.
_GML_CONTENT_TYPE_MARKERS: Final[tuple[str, ...]] = ("gml", "xml")

# Content types that are never a feature response: HTML and plain-text error
# pages returned with 200 OK.
_ERROR_PAGE_CONTENT_TYPE_MARKERS: Final[tuple[str, ...]] = ("html", "text/plain")

# GDAL reports a GML layer's CRS in the DuckDB column type, e.g.
# GEOMETRY('EPSG:4326'). That is the server's own statement of the CRS it
# returned, so it plays the same role the GeoJSON `crs` member does.
_GEOMETRY_TYPE_CRS_RE: Final = re.compile(r"GEOMETRY\('([^']+)'\)", re.IGNORECASE)


def _classify_wfs_content_type(content_type: str) -> str:
    """Decide how to parse a WFS response body from its Content-Type.

    Returns ``"json"``, ``"gml"``, or ``"error"``. An empty or unrecognized
    content type is treated as JSON, preserving the previous behavior for
    servers that send ``application/octet-stream`` for GeoJSON.
    """
    if not content_type:
        return "json"
    if "json" in content_type or "javascript" in content_type:
        return "json"
    if any(marker in content_type for marker in _ERROR_PAGE_CONTENT_TYPE_MARKERS):
        return "error"
    if any(marker in content_type for marker in _GML_CONTENT_TYPE_MARKERS):
        return "gml"
    return "json"


def _error_page_error(content_type: str, preview: str) -> WFSError:
    """Build the "server returned an error page" failure with a body preview.

    The wording is format-neutral because this is raised from the GML path too,
    where gpio deliberately requested GML: naming JSON there told the user the
    server had failed to send a format gpio never asked for (issue #838).
    """
    return WFSError(
        f"Expected a feature collection (GeoJSON or GML) but got {content_type}. "
        f"Server may have returned an error page:\n{preview}"
    )


def _read_body_preview(path: str) -> str:
    """Read the first 500 characters of a downloaded response body."""
    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            return handle.read(500)
    except OSError:  # pragma: no cover - the file was just written
        return ""


# An empty FeatureCollection is a few hundred bytes of markup; a body larger
# than this holds features and can skip the extra XML parse.
_EMPTY_GML_SNIFF_MAX_BYTES: Final = 1024 * 1024

# FeatureCollection children that carry no features: bounds, metadata, or a
# featureMembers wrapper with nothing inside (GeoServer's GML 3.1 empty shape).
_EMPTY_GML_METADATA_CHILDREN: Final = frozenset({"boundedBy", "metadata"})


def _local_name(tag: str) -> str:
    """Strip the ``{namespace}`` prefix ElementTree puts on a qualified tag."""
    return tag.rpartition("}")[2]


# Every entity-expansion attack (billion laughs, quadratic blowup) needs one of
# these; a WFS FeatureCollection needs neither.
_DTD_MARKERS: Final = (b"<!doctype", b"<!entity")


def _declares_a_dtd(body: bytes) -> bool:
    """True when ``body`` declares a doctype or an entity, in any casing.

    The scan is over ASCII bytes, so it only holds for ASCII-compatible
    encodings -- which is why ``_xml_body_must_be_refused`` rejects UTF-16 and
    UTF-32 bodies before relying on it.
    """
    lowered = body.lower()
    return any(marker in lowered for marker in _DTD_MARKERS)


# UTF-16/UTF-32 byte-order marks (the UTF-32 BE mark starts with NUL bytes and
# is also caught by the prolog NUL scan; it is listed for completeness).
_UTF16_UTF32_BOMS: Final = (b"\xff\xfe", b"\xfe\xff", b"\x00\x00\xfe\xff")

# How far into the body to look for NUL bytes. The XML prolog and root-element
# open tag of any real response fall well inside this window, and in a UTF-16
# or UTF-32 body every ASCII character in that window carries NUL padding.
_XML_PROLOG_SNIFF_BYTES: Final = 256


def _xml_body_must_be_refused(body: bytes) -> bool:
    """True when an XML body may not be parsed -- or handed to GDAL -- at all.

    Two shapes are refused, neither of which a legitimate WFS GetFeature
    response can have:

    - **A DTD** (``<!DOCTYPE``/``<!ENTITY`` in any casing). Every
      entity-expansion attack (billion laughs, quadratic blowup) needs one --
      a couple of kilobytes of nested ``<!ENTITY>`` definitions expand to
      gigabytes on an expat built without amplification protection -- and GML
      uses XML Schema, never a DTD (issue #840).
    - **A UTF-16/UTF-32 body**: a byte-order mark, or NUL bytes in the prolog
      region. expat auto-detects those encodings, so a UTF-16 payload carries
      no ASCII ``<!doctype`` bytes for the marker scan to find yet still gets
      its entities expanded; real servers send UTF-8/ASCII XML.
    """
    if body.startswith(_UTF16_UTF32_BOMS) or b"\x00" in body[:_XML_PROLOG_SNIFF_BYTES]:
        return True
    return _declares_a_dtd(body)


def _is_empty_gml_collection(body: bytes) -> bool:
    """True when ``body`` is a GML FeatureCollection holding zero features.

    A zero-feature collection is a legitimate WFS answer (an empty bbox), but
    GDAL cannot open one -- and on Linux builds ``ST_Read`` *segfaults* on it
    rather than raising -- so the body must be recognized as empty before GDAL
    ever sees it. Anything that is not XML, not a FeatureCollection, or has any
    feature-bearing child goes to GDAL, keeping the error-page WFSError for
    genuinely broken bodies.

    A body ``_xml_body_must_be_refused`` rejects (a DTD, or a UTF-16/UTF-32
    encoding that would smuggle one past the ASCII scan) never reaches the
    parse below. ``_parse_gml_response`` refuses such bodies with a WFSError
    before this sniff is consulted; the check is repeated here so the parse
    stays safe for any caller.
    """
    if len(body) > _EMPTY_GML_SNIFF_MAX_BYTES or _xml_body_must_be_refused(body):
        return False
    try:
        # The body reaching this line declares no DTD and is ASCII-compatible
        # (both checked above), so no entity expansion is possible; stdlib ET
        # never resolves external entities regardless; the body is capped at
        # 1MB; and the parse only gates an empty-collection check.
        root = ET.fromstring(body)  # nosec B314
    except ET.ParseError:
        return False
    if _local_name(root.tag) != "FeatureCollection":
        return False
    for child in root:
        name = _local_name(child.tag)
        if name in _EMPTY_GML_METADATA_CHILDREN:
            continue
        if name == "featureMembers" and len(child) == 0:
            continue
        return False
    return True


def _gml_geometry_column(
    con: duckdb.DuckDBPyConnection, tmp_path: str, content_type: str
) -> tuple[str, list[str], str | None] | None:
    """Describe a GML file and return (geometry column, other columns, CRS).

    ``ST_Read`` raises ``IOException`` when GDAL cannot open the body at all,
    which is exactly what an ``ows:ExceptionReport`` looks like. That case is
    reported with the same "error page" message (and body preview) the JSON path
    has always used, so a genuinely broken response still reads the same way.

    A zero-feature FeatureCollection also fails to open -- GDAL finds no layer
    to build -- but with its own "contains no layers" message, returned as
    ``None`` for the caller to turn into an empty table. That is only a
    fallback: ``_is_empty_gml_collection`` recognizes empty bodies before GDAL
    is invoked at all, because Linux builds segfault here instead of raising.
    """
    try:
        described = con.execute(f"DESCRIBE SELECT * FROM ST_Read({sql_path(tmp_path)})").fetchall()
    except duckdb.IOException as e:
        if "contains no layers" in str(e):
            return None
        raise _error_page_error(content_type, _read_body_preview(tmp_path)) from e

    geometry_column: str | None = None
    server_crs: str | None = None
    other_columns: list[str] = []
    for row in described:
        name, column_type = str(row[0]), str(row[1])
        if geometry_column is None and column_type.upper().startswith("GEOMETRY"):
            geometry_column = name
            match = _GEOMETRY_TYPE_CRS_RE.search(column_type)
            server_crs = _normalize_crs(match.group(1)) if match else None
        else:
            other_columns.append(name)

    if geometry_column is None:
        raise _error_page_error(content_type, _read_body_preview(tmp_path))

    return geometry_column, other_columns, server_crs


def _parse_gml_response(
    con: duckdb.DuckDBPyConnection, tmp_path: str, extract_fid: bool, content_type: str
) -> pa.Table:
    """Parse a GML FeatureCollection into the same shape the GeoJSON path returns.

    Geometry lands in a WKB column named ``geometry`` and every server attribute
    is kept, so everything downstream (repair, CRS validation, type inference,
    tile deduplication) works unchanged.
    """
    # A DTD or a UTF-16/UTF-32 body is refused outright: no legitimate WFS
    # GetFeature response has either shape (GML uses XML Schema; servers send
    # UTF-8), and falling through to GDAL is not a safe default -- a DOCTYPE'd
    # *empty* FeatureCollection is a zero-layer GML dataset, exactly the shape
    # Linux ST_Read segfaults on instead of raising.
    body = Path(tmp_path).read_bytes()
    if _xml_body_must_be_refused(body):
        raise _error_page_error(content_type, _read_body_preview(tmp_path))

    # Zero-feature collection: same shape the JSON path returns for an empty
    # ``features`` array (geometry-only, no _wfs_fid), so pagination and tiled
    # fetches see an empty page, not a failure. Decided from the bytes, before
    # ST_Read -- Linux GDAL segfaults on a zero-layer GML dataset. The
    # ``described is None`` branch is the fallback for platforms that raise.
    if _is_empty_gml_collection(body):
        debug("Empty GML response, returning empty table")
        return pa.table({"geometry": pa.array([], type=pa.binary())})

    described = _gml_geometry_column(con, tmp_path, content_type)
    if described is None:
        debug("Empty GML response, returning empty table")
        return pa.table({"geometry": pa.array([], type=pa.binary())})
    geometry_column, other_columns, server_crs = described

    # OGC_FID is synthesized by GDAL as a per-file row number, not server data.
    # Keeping it would give every page of a paginated fetch the same ids.
    excluded = [geometry_column]
    if "OGC_FID" in other_columns:
        excluded.append("OGC_FID")

    select_parts = [f"ST_AsWKB({quote_identifier(geometry_column)}) AS geometry"]
    # gml:id is the GML equivalent of a GeoJSON feature id, used to deduplicate
    # features that straddle two spatial tiles. GDAL synthesizes gml_id = ''
    # (not NULL) for features with no gml:id; NULLIF sends those to the
    # geometry-bytes dedup fallback instead of collapsing them onto one key.
    if extract_fid and "gml_id" in other_columns:
        select_parts.append("NULLIF(gml_id, '') AS _wfs_fid")
        excluded.append("gml_id")
    exclude_list = ", ".join(quote_identifier(c) for c in excluded)
    select_parts.append(f"* EXCLUDE({exclude_list})")

    query = f"SELECT {', '.join(select_parts)} FROM ST_Read({sql_path(tmp_path)})"
    table = con.execute(query).arrow().read_all()
    return _with_server_crs(table, server_crs)


def _accept_header_for(output_format: str | None) -> str:
    """Choose the Accept header for a requested outputFormat.

    WFS outputFormat values are not always media types (``GML2``, ``gml32``), so
    only a value that looks like one is echoed into Accept; anything else asks
    for whatever the server sends rather than risking a 406.

    ``None`` means the caller decided not to name an ``outputFormat`` at all, so
    Accept must not name one either — asking for JSON there would contradict the
    request and could earn a 406 from a server that cannot produce it.
    """
    if not output_format:
        return "*/*"
    return output_format if "/" in output_format else "*/*"


def _fetch_wfs_page(
    url: str,
    extract_fid: bool = False,
    max_retries: int = 3,
    retry_delay: float = 2.0,
    output_format: str | None = None,
) -> pa.Table:
    """
    Fetch a WFS page via httpx and parse with DuckDB locally.

    Downloads the response using Python httpx (thread-safe, ~10x faster than
    DuckDB httpfs for JSON), streams it to a temp file, then parses it with
    DuckDB's spatial extension. Includes automatic retry for transient network
    errors.

    The parser is chosen from the *response* Content-Type, not from
    ``output_format``: servers such as NextGIS ignore an outputFormat they don't
    support and answer with GML at HTTP 200, so trusting the request would make
    gpio reject a perfectly good FeatureCollection (issue #818).

    Args:
        url: Full WFS GetFeature URL with all parameters
        extract_fid: If True, extract the feature id as _wfs_fid column (for dedup)
        max_retries: Number of retry attempts for transient errors
        retry_delay: Base delay between retries (exponential backoff)
        output_format: The outputFormat requested in ``url``, used only to set
            the Accept header

    Returns:
        PyArrow Table with geometry column (WKB) and all properties
    """
    import httpx

    last_exception: Exception | None = None
    accept = _accept_header_for(output_format)

    for attempt in range(max_retries):
        start_time = time.time()
        if attempt > 0:
            debug(f"Retry {attempt}/{max_retries - 1}: {url[:80]}...")
        else:
            debug(f"Fetching: {url[:100]}...")

        # Stream the response into a temp *directory*, not a temp file: GDAL
        # writes a .gfs schema sidecar next to a schema-less GML body, and only
        # a directory cleanup is guaranteed to take it with it.
        tmp_dir = tempfile.TemporaryDirectory(prefix="gpio-wfs-")
        tmp_path = None
        content_type = ""
        body_kind = "json"
        total_bytes = 0
        try:
            client = _get_shared_http_client(timeout=600)

            with client.stream(
                "GET",
                url,
                headers={"Accept": accept, "Accept-Encoding": "gzip, deflate"},
            ) as response:
                # Check HTTP status
                if response.status_code == 400:
                    body = response.read().decode("utf-8", errors="replace")
                    if "startindex" in body.lower():
                        raise WFSError(
                            f"Server rejected paginated request (startIndex limit): {body.strip()}"
                        )
                    raise WFSError(f"WFS request failed with HTTP 400: {body[:500]}")
                response.raise_for_status()

                # Dispatch on the content-type the server actually sent. HTML and
                # text/plain are never feature responses (error pages returned
                # with 200 OK); XML/GML may be either a FeatureCollection or an
                # ows:ExceptionReport, which the GML parser tells apart.
                content_type = response.headers.get("content-type", "").lower()
                body_kind = _classify_wfs_content_type(content_type)
                if body_kind == "error":
                    preview = response.read().decode("utf-8", errors="replace")[:500]
                    raise _error_page_error(content_type, preview)

                suffix = ".gml" if body_kind == "gml" else ".json"
                tmp_path = str(Path(tmp_dir.name) / f"response{suffix}")
                with open(tmp_path, "wb") as tmp_file:
                    for chunk in response.iter_bytes(chunk_size=65536):
                        tmp_file.write(chunk)
                        total_bytes += len(chunk)

            # Success - break out of retry loop
            download_time = time.time() - start_time
            size_mb = total_bytes / (1024 * 1024)
            debug(f"Downloaded {size_mb:.1f} MB in {download_time:.1f}s")
            break

        except httpx.HTTPStatusError as e:
            tmp_dir.cleanup()
            raise WFSError(f"WFS request failed with HTTP {e.response.status_code}") from e
        except (httpx.RequestError, httpx.RemoteProtocolError) as e:
            # Transient network error - retry
            last_exception = e
            tmp_dir.cleanup()
            if attempt < max_retries - 1:
                delay = retry_delay * (attempt + 1)
                warn(f"Network error (attempt {attempt + 1}/{max_retries}): {e}")
                warn(f"Retrying in {delay:.1f}s...")
                _reset_http_client()
                time.sleep(delay)
                continue
            raise WFSError(f"Failed to fetch WFS data after {max_retries} attempts: {e}") from e
        except WFSError:
            tmp_dir.cleanup()
            raise
    else:
        # Exhausted retries without success
        raise WFSError(f"Failed to fetch WFS data after {max_retries} attempts: {last_exception}")

    con = None
    try:
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)

        if body_kind == "gml":
            parse_start = time.time()
            table = _parse_gml_response(con, tmp_path, extract_fid, content_type)
            debug(
                f"Parsed {table.num_rows:,} GML rows in {time.time() - parse_start:.1f}s "
                f"(total: {time.time() - start_time:.1f}s)"
            )
            return table

        # Read the feature count (DuckDB can't UNNEST empty JSON arrays) and the
        # authoritative CRS the server reported in its GeoJSON response, if any,
        # in a single scan over the response.
        feature_count, server_crs = _read_count_and_server_crs(con, tmp_path)

        if feature_count == 0:
            debug("Empty response, returning empty table")
            empty = pa.table({"geometry": pa.array([], type=pa.binary())})
            return _with_server_crs(empty, server_crs)

        # Detect property type and build extraction query
        props_type, can_unnest_props = _probe_properties_type(con, tmp_path, _MAX_JSON_OBJECT_SIZE)
        if not can_unnest_props:
            debug(f"Properties type is {props_type}, cannot unnest - returning geometry-only table")

        parse_start = time.time()
        query = _build_wfs_feature_query(
            tmp_path, extract_fid, can_unnest_props, _MAX_JSON_OBJECT_SIZE
        )

        result = con.execute(query)
        table = result.arrow().read_all()

        parse_time = time.time() - parse_start
        total_time = time.time() - start_time
        debug(f"Parsed {table.num_rows:,} rows in {parse_time:.1f}s (total: {total_time:.1f}s)")
        return _with_server_crs(table, server_crs)
    except WFSError:
        raise
    except Exception as e:
        raise WFSError(f"Failed to parse WFS response: {e}") from e
    finally:
        if con:
            con.close()
        tmp_dir.cleanup()


def _generate_tile_grid(
    bbox: tuple[float, float, float, float],
    num_tiles: int,
) -> list[tuple[float, float, float, float]]:
    """
    Generate a grid of tile bboxes covering the given bbox.

    Uses aspect-ratio-aware layout so tiles are roughly square in
    geographic coordinates.
    """
    import math

    if num_tiles <= 1:
        return [bbox]

    xmin, ymin, xmax, ymax = bbox
    width = xmax - xmin
    height = ymax - ymin

    if width <= 0 or height <= 0:
        return [bbox]

    aspect = width / height
    cols = max(1, round(math.sqrt(num_tiles * aspect)))
    rows = max(1, math.ceil(num_tiles / cols))

    dx = width / cols
    dy = height / rows

    tiles = []
    for r in range(rows):
        for c in range(cols):
            tiles.append(
                (
                    xmin + c * dx,
                    ymin + r * dy,
                    xmin + (c + 1) * dx,
                    ymin + (r + 1) * dy,
                )
            )

    return tiles


def _refine_tiles_adaptive(
    tiles: list[tuple[float, float, float, float]],
    service_url: str,
    typename: str,
    version: str,
    crs: str,
    axis_order: str,
    max_per_tile: int,
    max_depth: int = 8,
) -> list[tuple[float, float, float, float]]:
    """
    Recursively subdivide tiles that exceed the feature count limit.

    For each tile, probes the server with resultType=hits + bbox to check
    the feature count. Tiles over max_per_tile are split into 4 quadrants.
    """

    def _subdivide(
        bbox: tuple[float, float, float, float], depth: int
    ) -> list[tuple[float, float, float, float]]:
        if depth >= max_depth:
            return [bbox]

        count = _get_feature_count(
            service_url,
            typename,
            version,
            bbox=bbox,
            crs=crs,
            axis_order=axis_order,
        )

        if count is not None and count == 0:
            return []

        if count is None or count <= max_per_tile:
            return [bbox]

        xmin, ymin, xmax, ymax = bbox
        mx = (xmin + xmax) / 2
        my = (ymin + ymax) / 2

        quadrants = [
            (xmin, ymin, mx, my),
            (mx, ymin, xmax, my),
            (xmin, my, mx, ymax),
            (mx, my, xmax, ymax),
        ]

        result = []
        for q in quadrants:
            result.extend(_subdivide(q, depth + 1))
        return result

    refined = []
    for tile in tiles:
        refined.extend(_subdivide(tile, 0))
    return refined


def _deduplicate_tiles(table: pa.Table) -> pa.Table:
    """
    Deduplicate features that appear in multiple tiles.

    Uses _wfs_fid column if present (from GeoJSON feature.id).
    For features with NULL fid, falls back to geometry bytes deduplication.
    Always drops the _wfs_fid column from output.
    """
    if table.num_rows == 0:
        if "_wfs_fid" in table.column_names:
            return table.drop(["_wfs_fid"])
        return table

    con = get_duckdb_connection(load_spatial=False, load_httpfs=False)

    try:
        con.register("tile_data", table)

        if "_wfs_fid" in table.column_names:
            all_cols = [c for c in table.column_names if c != "_wfs_fid"]
            col_list = ", ".join(quote_identifier(c) for c in all_cols)
            # Deduplicate by fid when present, fall back to geometry for NULL fids
            query = f"""
                SELECT {col_list}
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY "_wfs_fid") AS _rn
                    FROM tile_data
                    WHERE "_wfs_fid" IS NOT NULL
                ) WHERE _rn = 1
                UNION ALL
                SELECT {col_list}
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY geometry) AS _rn
                    FROM tile_data
                    WHERE "_wfs_fid" IS NULL
                ) WHERE _rn = 1
            """
        else:
            all_cols = table.column_names
            col_list = ", ".join(quote_identifier(c) for c in all_cols)
            query = f"""
                SELECT {col_list}
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY geometry) AS _rn
                    FROM tile_data
                ) WHERE _rn = 1
            """

        result = con.execute(query)
        return result.arrow().read_all()
    finally:
        con.close()


def _fetch_with_spatial_tiles(
    service_url: str,
    typename: str,
    version: str,
    total_count: int,
    startindex_limit: int,
    layer_bbox: tuple[float, float, float, float],
    crs: str,
    max_workers: int = 1,
    page_size: int = DEFAULT_WFS_PAGE_SIZE,
    axis_order: str = "auto",
    max_features: int | None = None,
    sort_by: str | None = None,
    output_format: str | None = None,
) -> pa.Table:
    """
    Fetch a large WFS dataset by subdividing into spatial tiles.

    Used when the server caps startIndex, making full pagination impossible.
    Subdivides the layer bbox into tiles, fetches each tile separately,
    deduplicates features on tile boundaries, and combines results.
    """
    import math

    max_per_tile = startindex_limit
    num_tiles = max(2, math.ceil(total_count / (max_per_tile * 0.8)))

    progress(f"Auto-tiling: splitting {total_count:,} features into ~{num_tiles} spatial tiles...")

    tiles = _generate_tile_grid(layer_bbox, num_tiles)

    progress(f"Refining {len(tiles)} tiles (checking feature counts per tile)...")
    tiles = _refine_tiles_adaptive(
        tiles,
        service_url,
        typename,
        version,
        crs=crs,
        axis_order=axis_order,
        max_per_tile=max_per_tile,
    )

    progress(f"Fetching {len(tiles)} tiles...")

    all_tables = []
    for i, tile_bbox in enumerate(tiles):
        debug(f"Tile {i + 1}/{len(tiles)}: bbox={tile_bbox}")
        tile_table = fetch_all_features_duckdb(
            service_url,
            typename,
            version,
            max_features=max_features,
            bbox=tile_bbox,
            crs=crs,
            max_workers=max_workers,
            page_size=page_size,
            axis_order=axis_order,
            extract_fid=True,
            sort_by=sort_by,
            output_format=output_format,
        )
        if tile_table.num_rows > 0:
            all_tables.append(tile_table)
        progress(f"Tile {i + 1}/{len(tiles)}: {tile_table.num_rows:,} features")

    if not all_tables:
        raise EmptyLayerError(typename)

    # All tiles share one server CRS; capture before schema unification drops it.
    server_crs = _read_server_crs(all_tables[0])

    # Unify schemas to handle type mismatches across tiles (e.g., int64 vs decimal128)
    if len(all_tables) > 1:
        schemas = [t.schema for t in all_tables]
        unified_schema = _compute_unified_schema(schemas)
        all_tables = [
            _cast_table_to_schema(t, unified_schema, page_info=f"tile {i + 1}")
            for i, t in enumerate(all_tables)
        ]

    combined = pa.concat_tables(all_tables)
    before_dedup = combined.num_rows

    combined = _deduplicate_tiles(combined)
    after_dedup = combined.num_rows

    if before_dedup != after_dedup:
        debug(
            f"Deduplicated: {before_dedup:,} → {after_dedup:,} ({before_dedup - after_dedup:,} duplicates)"
        )

    return _with_server_crs(_infer_column_types(combined), server_crs)


def _probe_startindex_limit(
    service_url: str,
    typename: str,
    version: str,
    crs: str | None = None,
    axis_order: str = "auto",
    output_format: str | None = None,
) -> int | None:
    """
    Probe a WFS server to discover any startIndex limit.

    Some servers (e.g., PDOK) reject requests with startIndex above a
    threshold (e.g., 50,000). This sends a lightweight probe request
    to detect that limit before attempting full pagination.

    Returns the limit value if detected, or None if no limit found *or* the
    probe failed. A failed probe is logged as a warning: a silent None here
    means pagination proceeds past a cap the server would have rejected, which
    is the same silently-truncated-results failure mode as a lost feature count
    (issue #678).
    """
    import httpx

    probe_offset = 50001
    url = _build_wfs_url(
        service_url,
        typename,
        version,
        max_features=1,
        start_index=probe_offset,
        crs=crs,
        axis_order=axis_order,
        output_format=output_format,
    )

    try:
        client = _get_shared_http_client(timeout=30)
        response = client.get(url, headers={"Accept-Encoding": "gzip, deflate"})
        if response.status_code == 400:
            body = response.text.lower()
            if "startindex" in body:
                import re as _re

                match = _re.search(r"startindex.*?(\d[\d.,]+)", body)
                if match:
                    limit_str = match.group(1).replace(",", "").replace(".", "")
                    try:
                        return int(limit_str)
                    except ValueError:
                        pass
                return 50000
        return None
    except (WFSError, httpx.HTTPError, OSError) as e:
        # Same contract as _get_feature_count: expected transport failures
        # degrade to "no limit known" but say so, because that answer silently
        # re-enables unbounded pagination. Programming errors still surface.
        warn(
            f"startIndex limit probe failed for '{typename}' at WFS {version}: "
            f"{type(e).__name__}: {e}. Continuing without a known startIndex "
            "cap — pagination past a server's limit may return truncated or "
            "rejected pages."
        )
        return None


def _build_wfs_url(
    service_url: str,
    typename: str,
    version: str = "1.1.0",
    max_features: int | None = None,
    start_index: int | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    crs: str | None = None,
    axis_order: str = "auto",
    sort_by: str | None = None,
    output_format: str | None = None,
) -> str:
    """Build a WFS GetFeature URL with pagination support.

    ``output_format`` is the format negotiated from GetCapabilities. When it is
    None the ``outputFormat`` parameter is omitted entirely, so the server
    replies in its own default — the only thing that works on a service which
    advertises no format gpio recognizes (issue #818).
    """
    clean_url = _clean_service_url(service_url)

    params = {
        "service": "WFS",
        "version": version,
        "request": "GetFeature",
        "typeNames" if version == "2.0.0" else "typeName": typename,
    }

    if output_format:
        params["outputFormat"] = output_format

    if max_features is not None:
        # WFS 2.0 uses count, WFS 1.x uses maxFeatures
        if version == "2.0.0":
            params["count"] = str(max_features)
        else:
            params["maxFeatures"] = str(max_features)

    if start_index is not None and start_index > 0 and version != "1.0.0":
        params["startIndex"] = str(start_index)

    # Always include srsName when CRS is specified (Issue #405)
    # This tells the server what CRS to return data in, independent of bbox
    # Note: WFS 1.0.0 uses SRS in bbox only, srsName is 1.1.0+ parameter
    if crs and version != "1.0.0":
        params["srsName"] = crs

    if bbox and crs:
        params["bbox"] = _build_bbox_param(bbox, crs, version, axis_order)

    # sortBy is required for stable pagination on PK-less layers (Issue #488)
    if sort_by and version != "1.0.0":
        params["sortBy"] = sort_by

    return _merge_query_params(clean_url, params)


def _single_fetch_mode(
    service_url: str,
    typename: str,
    version: str,
    max_features: int | None,
    bbox: tuple[float, float, float, float] | None,
    crs: str | None,
    axis_order: str,
    extract_fid: bool,
    output_format: str | None = None,
) -> pa.Table:
    """
    Fetch all features in a single request.

    Used when the dataset is small enough to fit in one request.
    """
    url = _build_wfs_url(
        service_url,
        typename,
        version,
        max_features=max_features,
        bbox=bbox,
        crs=crs,
        axis_order=axis_order,
        output_format=output_format,
    )
    table = _fetch_wfs_page(url, extract_fid=extract_fid, output_format=output_format)
    if extract_fid:
        return table
    # _infer_column_types rebuilds the schema and drops metadata; re-attach the
    # server-declared CRS captured by _fetch_wfs_page.
    return _with_server_crs(_infer_column_types(table), _read_server_crs(table))


def _sequential_pagination_mode(
    service_url: str,
    typename: str,
    version: str,
    effective_total: int,
    page_size: int,
    bbox: tuple[float, float, float, float] | None,
    crs: str | None,
    axis_order: str,
    extract_fid: bool,
    sort_by: str | None = None,
    output_format: str | None = None,
) -> dict[int, pa.Table]:
    """
    Fetch features using sequential adaptive pagination.

    Adapts page size dynamically if server caps maxFeatures.
    """
    results: dict[int, pa.Table] = {}
    offset = 0
    page_num = 0
    server_page_size = page_size

    while offset < effective_total:
        remaining = effective_total - offset
        count = min(server_page_size, remaining)
        url = _build_wfs_url(
            service_url,
            typename,
            version,
            max_features=count,
            start_index=offset,
            bbox=bbox,
            crs=crs,
            axis_order=axis_order,
            sort_by=sort_by,
            output_format=output_format,
        )
        try:
            table = _fetch_wfs_page(url, extract_fid=extract_fid, output_format=output_format)
        except Exception as e:
            raise WFSError(f"Failed to fetch page {page_num + 1} (offset {offset}): {e}") from e

        if table.num_rows == 0:
            break

        results[page_num] = table

        # Detect server-side maxFeatures cap
        if table.num_rows < count and server_page_size > table.num_rows:
            server_page_size = table.num_rows
            debug(f"Server caps response to {server_page_size} features per request")

        offset += table.num_rows
        page_num += 1
        debug(
            f"Page {page_num}: {table.num_rows:,} features (offset {offset:,}/{effective_total:,})"
        )

    return results


def _parallel_pagination_mode(
    service_url: str,
    typename: str,
    version: str,
    effective_total: int,
    page_size: int,
    num_pages: int,
    max_workers: int,
    bbox: tuple[float, float, float, float] | None,
    crs: str | None,
    axis_order: str,
    extract_fid: bool,
    sort_by: str | None = None,
    output_format: str | None = None,
) -> dict[int, pa.Table]:
    """
    Fetch features using parallel pagination.

    Pre-builds page URLs and fetches concurrently using ThreadPoolExecutor.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Build page URLs
    pages: list[tuple[int, int, str]] = []
    for i in range(num_pages):
        start = i * page_size
        remaining = effective_total - start
        count = min(page_size, remaining)
        if count <= 0:
            break
        url = _build_wfs_url(
            service_url,
            typename,
            version,
            max_features=count,
            start_index=start,
            bbox=bbox,
            crs=crs,
            axis_order=axis_order,
            sort_by=sort_by,
            output_format=output_format,
        )
        pages.append((i, start, url))

    # Fetch pages in parallel
    results: dict[int, pa.Table] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_page = {
            executor.submit(_fetch_wfs_page, url, extract_fid, output_format=output_format): (
                page_num,
                start,
            )
            for page_num, start, url in pages
        }

        for future in as_completed(future_to_page):
            page_num, start = future_to_page[future]
            try:
                table = future.result()
                results[page_num] = table
                debug(f"Page {page_num + 1}/{num_pages}: {table.num_rows:,} features")
            except Exception as e:
                raise WFSError(f"Failed to fetch page {page_num + 1} (offset {start}): {e}") from e

    return results


def fetch_all_features_duckdb(
    service_url: str,
    typename: str,
    version: str = "1.1.0",
    max_features: int | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    crs: str | None = None,
    max_workers: int = 1,
    page_size: int = DEFAULT_WFS_PAGE_SIZE,
    axis_order: str = "auto",
    extract_fid: bool = False,
    sort_by: str | None = None,
    total_count: int | None = None,
    output_format: str | None = None,
) -> pa.Table:
    """
    Fetch WFS features via Python httpx download and DuckDB local parsing.

    Downloads GeoJSON with Python httpx (~10x faster than DuckDB httpfs), then
    parses locally with DuckDB's spatial extension.

    Supports multiple modes:
    - Single request (max_workers=1, small dataset): One request for all features
    - Sequential pagination (max_workers=1, large dataset): Paginated requests
    - Parallel pagination (max_workers>1): Concurrent paginated requests,
      recommended for large datasets (100k+ features) — typically 5-6x faster

    Args:
        service_url: WFS service URL
        typename: Layer typename
        version: WFS version
        max_features: Maximum features to fetch (None = all)
        bbox: Optional bounding box filter
        crs: CRS for bbox parameter
        max_workers: Number of parallel requests (1-10). Use 4-8 for large datasets.
        page_size: Features per page when paginating (default: 100000)
        axis_order: Bbox axis order ("auto", "xy", "latlon")
        extract_fid: If True, extract WFS feature IDs for deduplication
        sort_by: Attribute to sort by for stable pagination (auto-detected if None)
        total_count: Pre-fetched feature count (avoids re-querying at this version).
            Pass this when the caller already has a trusted count from WFS 2.0.0.
        output_format: outputFormat to request, as negotiated from
            GetCapabilities. None omits the parameter so the server answers in
            its own default format (issue #818).

    Returns:
        PyArrow Table with geometry (WKB) and all properties
    """
    # Use provided count or query at this version
    if total_count is None:
        total_count = _get_feature_count(
            service_url, typename, version, bbox=bbox, crs=crs, axis_order=axis_order
        )
    if max_features and total_count:
        total_count = min(total_count, max_features)

    # Single request for small datasets or when count is unknown
    needs_pagination = (
        total_count is not None and version != "1.0.0" and (max_features or total_count) > page_size
    )

    if not needs_pagination:
        if total_count:
            progress(f"Fetching {total_count:,} features...")
        else:
            progress("Fetching features...")

        table = _single_fetch_mode(
            service_url,
            typename,
            version,
            max_features,
            bbox,
            crs,
            axis_order,
            extract_fid,
            output_format=output_format,
        )
        expected = max_features or total_count
        if expected and table.num_rows < expected and version != "1.0.0":
            # Server returned fewer than expected — likely has a maxFeatures cap.
            # Fall through to adaptive pagination to fetch the rest.
            needs_pagination = True
            page_size = table.num_rows or page_size
        else:
            return table

    # Paginated mode — sequential (max_workers=1) or parallel
    effective_total = max_features if max_features else total_count

    # Guard: pagination requires a known count
    if effective_total is None:
        warn("Cannot determine feature count; falling back to single request mode.")
        return _single_fetch_mode(
            service_url,
            typename,
            version,
            max_features,
            bbox,
            crs,
            axis_order,
            extract_fid,
            output_format=output_format,
        )

    # Probe for server-side startIndex limit (skip for tile fetches)
    if not extract_fid:
        startindex_limit = _probe_startindex_limit(
            service_url,
            typename,
            version,
            crs=crs,
            axis_order=axis_order,
            output_format=output_format,
        )
        if startindex_limit is not None:
            max_reachable = startindex_limit + page_size
            if effective_total > max_reachable:
                raise WFSError(
                    f"Server limits startIndex to {startindex_limit:,}, so only "
                    f"{max_reachable:,} of {effective_total:,} features are reachable via pagination.\n\n"
                    f"Options:\n"
                    f"  1. Use --auto-tile to automatically subdivide into spatial tiles\n"
                    f"  2. Use --limit {max_reachable} to fetch only what's reachable\n"
                    f"  3. Use --bbox to spatially filter to a smaller region\n"
                    f"  4. Download the dataset directly from the provider's bulk download service"
                )

    num_pages = (effective_total + page_size - 1) // page_size
    actual_workers = min(max_workers, num_pages)

    progress(
        f"Fetching {effective_total:,} features in {num_pages} pages "
        f"using {actual_workers} {'worker' if actual_workers == 1 else 'workers'}..."
    )

    # Fetch pages using appropriate mode
    if actual_workers == 1:
        results = _sequential_pagination_mode(
            service_url,
            typename,
            version,
            effective_total,
            page_size,
            bbox,
            crs,
            axis_order,
            extract_fid,
            sort_by=sort_by,
            output_format=output_format,
        )
    else:
        results = _parallel_pagination_mode(
            service_url,
            typename,
            version,
            effective_total,
            page_size,
            num_pages,
            actual_workers,
            bbox,
            crs,
            axis_order,
            extract_fid,
            sort_by=sort_by,
            output_format=output_format,
        )

    # Combine tables in order
    if not results:
        raise EmptyLayerError(typename)

    tables = [results[i] for i in sorted(results.keys())]

    # All pages come from the same layer/request, so they share one server CRS.
    # Capture it before schema unification, which drops metadata.
    server_crs = _read_server_crs(tables[0])

    # Unify schemas to handle type mismatches across pages (e.g., int64 vs decimal128)
    if len(tables) > 1:
        schemas = [t.schema for t in tables]
        unified_schema = _compute_unified_schema(schemas)
        tables = [
            _cast_table_to_schema(t, unified_schema, page_info=f"page {i + 1}")
            for i, t in enumerate(tables)
        ]

    combined = pa.concat_tables(tables)
    debug(f"Combined {len(tables)} pages: {combined.num_rows:,} total features")

    # Skip type inference for tile fetches — the tiling orchestrator handles it
    if extract_fid:
        return _with_server_crs(combined, server_crs)
    return _with_server_crs(_infer_column_types(combined), server_crs)


def _looks_like_server_cap(count: int) -> bool:
    """Check if a feature count looks like a server-imposed cap.

    Server caps are typically round numbers like 1000000, 500000, 100000, etc.
    Parallel workers may drift slightly past exact caps due to network retries
    (e.g., 1,000,002 instead of 1,000,000), so we allow ±0.1% tolerance around
    common cap values (issue #503).
    """
    if count <= 0:
        return False
    # Check for common cap values with ±0.1% tolerance for parallel-worker drift
    common_caps = {1000000, 500000, 100000, 50000, 10000}
    for cap in common_caps:
        if abs(count - cap) <= cap * 0.001:
            return True
    # Check if it's a round number (divisible by 10000 with at least 5 digits)
    if count >= 10000 and count % 10000 == 0:
        return True
    return False


def _get_tiling_bbox(
    user_bbox: tuple[float, float, float, float] | None,
    use_server_bbox: bool,
    layer_info: WFSLayerInfo,
) -> tuple[float, float, float, float] | None:
    """Get the bounding box to use for spatial tiling.

    Returns the user's bbox if provided and server-side filtering is enabled,
    otherwise falls back to the layer's advertised bbox from capabilities.
    Returns None if no bbox is available.
    """
    if user_bbox and use_server_bbox:
        return user_bbox
    if layer_info.bbox:
        return layer_info.bbox
    return None


def wfs_to_table(
    service_url: str,
    typename: str,
    version: str = "1.1.0",
    bbox: tuple[float, float, float, float] | None = None,
    bbox_mode: str = "auto",
    output_crs: str | None = None,
    limit: int | None = None,
    max_workers: int = 1,
    page_size: int = DEFAULT_WFS_PAGE_SIZE,
    axis_order: str = "auto",
    strict_crs: bool = False,
    verbose: bool = False,
    auto_tile: bool = True,
    sort_by: str | None = None,
    repair_geometry: bool = True,
) -> pa.Table:
    """
    Fetch WFS layer as PyArrow Table.

    Uses DuckDB's native HTTP streaming for 10x+ faster extraction:
    - HTTP response is streamed directly in C++ (no Python buffering)
    - JSON parsing happens in DuckDB (faster than Python json)
    - Geometry conversion happens in-database (no temp files)

    For very large datasets (1M+ features), use max_workers > 1 to enable
    parallel pagination, which splits the request into smaller chunks.

    Args:
        service_url: WFS service URL
        typename: Layer typename
        version: WFS version (1.0.0, 1.1.0, or 2.0.0)
        bbox: Bounding box filter (xmin, ymin, xmax, ymax)
        bbox_mode: Bbox strategy ("auto", "server", "local")
        output_crs: Guarantee output in this CRS. If the server returns a
            different CRS than requested, data is reprojected automatically from
            the server's actual CRS. (e.g., "EPSG:4326")
        limit: Maximum features to fetch
        max_workers: Parallel requests for large datasets (default: 1 = single request)
        page_size: Features per page when using parallel mode (default: 100000)
        axis_order: Bbox axis order ("auto", "xy", "latlon")
        strict_crs: If True, fail when the server returns a different CRS than
            requested. If False and output_crs is set, reproject from the
            server's actual CRS; otherwise warn and label the output with the
            server's actual CRS. The CRS the server declares in its GeoJSON
            response is authoritative — gpio never guesses from coordinates when
            the server states which CRS it used (issue #499).
        verbose: Enable debug output
        sort_by: Attribute to sort by for stable pagination. If None, auto-detected from
            DescribeFeatureType. Required for layers without a primary key.
        repair_geometry: Repair invalid geometry with ST_MakeValid (default: True).
            When False, invalid geometry is preserved and a warning reports the count.

    Returns:
        PyArrow Table with GeoParquet-compatible geometry
    """
    configure_verbose(verbose)

    # Handle auto version negotiation
    if version == "auto":
        version, _ = negotiate_wfs_version(service_url)

    # Get layer info
    info("Connecting to WFS service...")
    layer_info = get_layer_info(service_url, typename, version)

    debug(f"Layer: {layer_info.typename}")
    debug(f"Title: {layer_info.title}")
    debug(f"Available CRS: {len(layer_info.crs_list)} options")
    debug(f"Available formats: {layer_info.available_formats}")

    # Negotiate CRS
    crs = _negotiate_crs(layer_info, output_crs)

    # Detect the best output format we can actually parse. None means the
    # server advertises nothing we recognize, so we omit outputFormat and take
    # whatever it sends -- which is how GML-only services work (issue #818).
    output_format = _detect_best_output_format(layer_info.available_formats)
    debug(f"Using output format: {output_format or 'server default (none requested)'}")

    # Auto-detect sortBy attribute for stable pagination (Issue #488)
    # GeoServer requires sortBy for pagination on layers without a primary key
    effective_sort_by = sort_by
    if effective_sort_by is None and version != "1.0.0":
        effective_sort_by = layer_info.sortable_attribute
        if effective_sort_by:
            debug(f"Auto-detected sort attribute: {effective_sort_by}")

    # Determine bbox strategy
    use_server_bbox = True
    if bbox:
        use_server_bbox = _determine_bbox_strategy(bbox_mode, layer_info)

    # Get expected feature count for cap detection
    # Try WFS 2.0.0 first for count query since it often has higher/no limits
    expected_count = None
    if auto_tile and version != "1.0.0":
        # Try WFS 2.0.0 count first (higher limits), fall back to requested
        # version. The 2.0.0 attempt is speculative — a 1.1.0-only server
        # rejecting it is an expected answer, not a degradation, so it stays
        # quiet; the fallback below is the probe whose failure actually costs
        # us auto-tiling, and it warns.
        expected_count = _get_feature_count(
            service_url, layer_info.typename, "2.0.0", warn_on_failure=False
        )
        if not expected_count:
            expected_count = _get_feature_count(service_url, layer_info.typename, version)
        if expected_count:
            debug(f"Server reports {expected_count:,} features")

    # Check for startIndex limit (proactive tiling for pagination limits)
    use_tiling = False
    tiling_bbox: tuple[float, float, float, float] | None = None
    tiling_total = 0
    tiling_limit = 0
    if auto_tile and version != "1.0.0" and expected_count:
        startindex_limit = _probe_startindex_limit(
            service_url,
            layer_info.typename,
            version,
            crs=crs,
            axis_order=axis_order,
            output_format=output_format,
        )
        if startindex_limit:
            effective = min(limit, expected_count) if limit else expected_count
            if effective > startindex_limit + page_size:
                tiling_bbox = _get_tiling_bbox(bbox, use_server_bbox, layer_info)
                if tiling_bbox:
                    use_tiling = True
                    tiling_total = effective
                    tiling_limit = startindex_limit
                    warn(
                        f"Server has startIndex limit of {startindex_limit:,}. "
                        f"Auto-tiling {effective:,} features..."
                    )

    if use_tiling and tiling_bbox is not None:
        table = _fetch_with_spatial_tiles(
            service_url=service_url,
            typename=layer_info.typename,
            version=version,
            total_count=tiling_total,
            startindex_limit=tiling_limit,
            layer_bbox=tiling_bbox,
            crs=crs,
            max_workers=max_workers,
            page_size=page_size,
            axis_order=axis_order,
            max_features=limit,
            sort_by=effective_sort_by,
            output_format=output_format,
        )
    else:
        # Normal fetch — pass expected_count so we don't re-query at this version
        # (some servers return different counts by WFS version; issue #503)
        table = fetch_all_features_duckdb(
            service_url=service_url,
            typename=layer_info.typename,
            version=version,
            max_features=limit,
            bbox=bbox if use_server_bbox else None,
            crs=crs,
            max_workers=max_workers,
            page_size=page_size,
            axis_order=axis_order,
            sort_by=effective_sort_by,
            total_count=expected_count,
            output_format=output_format,
        )

        # Check for maxFeatures cap (reactive tiling)
        # If we got fewer features than expected and the count looks like a server cap,
        # retry with spatial tiling to bypass the cap
        if (
            auto_tile
            and version != "1.0.0"
            and expected_count
            and table.num_rows < expected_count
            and _looks_like_server_cap(table.num_rows)
            and not limit  # Don't retry if user specified a limit
        ):
            tiling_bbox = _get_tiling_bbox(bbox, use_server_bbox, layer_info)
            if tiling_bbox:
                warn(
                    f"Server capped response at {table.num_rows:,} features "
                    f"(expected {expected_count:,}). Auto-tiling to fetch all..."
                )
                # Use a synthetic "startindex_limit" equal to the cap for tile sizing
                table = _fetch_with_spatial_tiles(
                    service_url=service_url,
                    typename=layer_info.typename,
                    version=version,
                    total_count=expected_count,
                    startindex_limit=table.num_rows,  # Use the cap as the limit
                    layer_bbox=tiling_bbox,
                    crs=crs,
                    max_workers=max_workers,
                    page_size=page_size,
                    axis_order=axis_order,
                    max_features=limit,
                    sort_by=effective_sort_by,
                    output_format=output_format,
                )

    if table.num_rows == 0:
        if bbox:
            # Empty results with bbox filter is valid - just no features in that area
            warn(f"No features found in bbox for layer '{typename}'. Writing empty file.")
        else:
            # Empty results without bbox likely indicates a problem
            raise EmptyLayerError(typename)

    # Apply local bbox filter if needed
    if bbox and not use_server_bbox:
        debug("Applying local bbox filter...")
        # The DuckDB roundtrip below drops Arrow schema metadata, which would
        # discard the server-declared CRS and silently fall back to the bbox
        # heuristic (issue #499). Capture it first and re-attach afterwards.
        server_crs = _read_server_crs(table)
        # The bbox is in the requested CRS, but the geometries are in the CRS the
        # server actually returned. When those differ, align the bbox to the
        # geometries' CRS so the filter doesn't drop valid rows (issue #499).
        filter_bbox = bbox
        if server_crs and not _crs_matches(server_crs, crs):
            filter_bbox = _reproject_bbox_for_local_filter(bbox, crs, server_crs)
        filter_sql = _build_local_bbox_filter(filter_bbox, "geometry")
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            con.register("features", table)
            filtered = con.execute(f"SELECT * FROM features WHERE {filter_sql}").arrow()
            table = _with_server_crs(filtered.read_all(), server_crs)
            debug(f"After local filter: {table.num_rows:,} features")
        finally:
            con.close()

    # Repair invalid geometry (issue #506) before CRS resolution, so the rest
    # of the pipeline — and any downstream consumer like PMTiles — sees valid
    # geometry. The helper repairs on native GEOMETRY in DuckDB and restores the
    # table's schema metadata (including the server-CRS marker, issue #499).
    if table.num_rows > 0:
        table, _ = repair_arrow_table_geometry(table, "geometry", repair=repair_geometry)

    # Resolve the CRS to label/reproject to. Trust the server's declared CRS
    # when it gave one; otherwise fall back to a coordinate-range guess.
    table, crs = _resolve_crs_for_output(table, crs, output_crs, strict_crs)

    # Drop the internal server-CRS marker unconditionally so it never leaks into
    # the output file, regardless of whether geo metadata is written below.
    existing_meta = dict(table.schema.metadata or {})
    existing_meta.pop(_SERVER_CRS_METADATA_KEY, None)

    # Add CRS metadata to schema
    projjson = parse_crs_string_to_projjson(_normalize_crs(crs))
    if projjson:
        geo_meta = {
            "version": "1.0.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "crs": projjson,
                }
            },
        }
        existing_meta[b"geo"] = json.dumps(geo_meta).encode("utf-8")

    table = table.replace_schema_metadata(existing_meta)

    success(f"Fetched {table.num_rows:,} features from WFS")
    return table


def convert_wfs_to_geoparquet(
    service_url: str,
    typename: str,
    output_file: str,
    version: str = "1.1.0",
    bbox: tuple[float, float, float, float] | None = None,
    bbox_mode: str = "auto",
    output_crs: str | None = None,
    limit: int | None = None,
    max_workers: int = 1,
    page_size: int = DEFAULT_WFS_PAGE_SIZE,
    axis_order: str = "auto",
    strict_crs: bool = False,
    skip_hilbert: bool = False,
    skip_bbox: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    geoparquet_version: str | None = None,
    overwrite: bool = False,
    verbose: bool = False,
    auto_tile: bool = True,
    sort_by: str | None = None,
    repair_geometry: bool = True,
) -> None:
    """
    Extract WFS layer and save as optimized GeoParquet.

    Args:
        service_url: WFS service URL
        typename: Layer typename
        output_file: Output GeoParquet file path
        version: WFS version
        bbox: Bounding box filter
        bbox_mode: Bbox strategy
        output_crs: Guarantee output in this CRS (reprojects if server returns different)
        limit: Maximum features
        max_workers: Parallel requests for large datasets (default: 1)
        page_size: Features per page when using parallel mode (default: 100000)
        axis_order: Bbox axis order ("auto", "xy", "latlon")
        strict_crs: If True, fail when the server returns a different CRS than
            requested. If False and output_crs is set, reproject from the
            server's actual CRS; otherwise warn and label with the server's
            actual CRS. The server's declared CRS is trusted over any coordinate
            guess (issue #499).
        skip_hilbert: Skip Hilbert curve sorting
        skip_bbox: Skip adding bbox column
        compression: Compression algorithm
        compression_level: Compression level
        row_group_size_mb: Row group size in MB
        row_group_rows: Row group size in rows
        geoparquet_version: GeoParquet version
        overwrite: Overwrite existing file
        verbose: Enable debug output
        auto_tile: Automatically subdivide into spatial tiles when the server caps
            responses (maxFeatures or startIndex limits). Disabling this accepts
            silently truncated results (default: True)
        sort_by: Attribute to sort by for stable pagination. If None, auto-detected.
        repair_geometry: Repair invalid geometry with ST_MakeValid (default: True).
            When False, invalid geometry is preserved and a warning reports the count.
    """
    configure_verbose(verbose)

    # Check output file
    output_path = Path(output_file)
    if output_path.exists() and not overwrite:
        raise WFSError(f"Output file exists: {output_file}\nUse --overwrite to replace it.")

    # Fetch data
    table = wfs_to_table(
        service_url,
        typename,
        version=version,
        bbox=bbox,
        bbox_mode=bbox_mode,
        output_crs=output_crs,
        limit=limit,
        max_workers=max_workers,
        page_size=page_size,
        axis_order=axis_order,
        strict_crs=strict_crs,
        verbose=verbose,
        auto_tile=auto_tile,
        sort_by=sort_by,
        repair_geometry=repair_geometry,
    )

    # Apply Hilbert ordering (unless skipped)
    if not skip_hilbert and table.num_rows > 0:
        progress("Applying Hilbert curve ordering...")
        from geoparquet_io.core.hilbert_order import hilbert_order_table

        table = hilbert_order_table(table, geometry_column="geometry")
        debug("Hilbert sort complete")

    # Add bbox column (unless skipped)
    if not skip_bbox and table.num_rows > 0:
        progress("Adding bbox column...")
        from geoparquet_io.core.add.bbox import add_bbox_table

        table = add_bbox_table(table, geometry_column="geometry")
        debug("Bbox column added")

    # Write output
    progress(f"Writing to {output_file}...")
    write_geoparquet_table(
        table,
        output_file,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        geoparquet_version=geoparquet_version,
    )

    success(f"Wrote {table.num_rows:,} features to {output_file}")


def _extract_single_layer(
    service_url: str,
    typename: str,
    output_dir: Path,
    layer_index: int,
    total_layers: int,
    **kwargs,
) -> tuple[str, Path, int | None, str | None]:
    """
    Extract a single WFS layer to a file in output_dir.

    Returns (typename, output_path, row_count, error_message).
    error_message is None on success.
    """
    # Sanitize typename for filename (replace : and / with _)
    safe_name = typename.replace(":", "_").replace("/", "_")
    output_path = output_dir / f"{safe_name}.parquet"

    try:
        progress(f"[{layer_index + 1}/{total_layers}] Starting {typename}...")
        convert_wfs_to_geoparquet(
            service_url=service_url,
            typename=typename,
            output_file=str(output_path),
            **kwargs,
        )
        # Read back row count for summary
        import pyarrow.parquet as pq

        meta = pq.read_metadata(output_path)
        row_count = meta.num_rows
        return (typename, output_path, row_count, None)
    except Exception as e:
        return (typename, output_path, None, str(e))


def convert_wfs_layers_to_directory(
    service_url: str,
    typenames: list[str],
    output_dir: str | Path,
    parallel_layers: int = 1,
    max_workers: int = 1,
    page_size: int = DEFAULT_WFS_PAGE_SIZE,
    version: str = "1.1.0",
    bbox: tuple[float, float, float, float] | None = None,
    bbox_mode: str = "auto",
    output_crs: str | None = None,
    limit: int | None = None,
    axis_order: str = "auto",
    strict_crs: bool = False,
    skip_hilbert: bool = False,
    skip_bbox: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    geoparquet_version: str | None = None,
    overwrite: bool = False,
    verbose: bool = False,
    auto_tile: bool = True,
    sort_by: str | None = None,
    repair_geometry: bool = True,
) -> dict[str, Path]:
    """
    Extract multiple WFS layers in parallel to a directory.

    Each layer is saved as a separate GeoParquet file named after the typename.

    Args:
        service_url: WFS service URL
        typenames: List of layer typenames to extract
        output_dir: Directory to write output files
        parallel_layers: Number of layers to extract concurrently (default: 1)
        max_workers: Per-layer pagination workers (default: 1)
        page_size: Features per page (default: 100000)
        version: WFS version
        bbox: Optional bounding box filter (applied to all layers)
        bbox_mode: Bbox strategy
        output_crs: Output CRS (applied to all layers)
        limit: Maximum features per layer
        axis_order: Bbox axis order
        strict_crs: Fail on CRS mismatch
        skip_hilbert: Skip Hilbert ordering
        skip_bbox: Skip bbox column
        compression: Compression algorithm
        compression_level: Compression level
        row_group_size_mb: Row group size in MB
        row_group_rows: Row group size in rows
        geoparquet_version: GeoParquet version
        overwrite: Overwrite existing files
        verbose: Enable debug output
        auto_tile: Auto-tile when the server caps responses (maxFeatures or
            startIndex limits). Disabling this accepts silently truncated
            results (default: True)
        sort_by: Sort attribute for stable pagination
        repair_geometry: Repair invalid geometry with ST_MakeValid (default: True)

    Returns:
        Dict mapping typename to output file path (only successful extractions)

    Raises:
        WFSError: If output_dir exists and is not a directory, or if all extractions fail
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    configure_verbose(verbose)

    output_path = Path(output_dir)

    # Create output directory if needed
    if output_path.exists():
        if not output_path.is_dir():
            raise WFSError(f"Output path exists but is not a directory: {output_dir}")
    else:
        output_path.mkdir(parents=True)

    # Check for existing files if not overwriting
    if not overwrite:
        existing = []
        for typename in typenames:
            safe_name = typename.replace(":", "_").replace("/", "_")
            file_path = output_path / f"{safe_name}.parquet"
            if file_path.exists():
                existing.append(str(file_path))
        if existing:
            raise WFSError(
                "Output files already exist:\n  "
                + "\n  ".join(existing)
                + "\nUse --overwrite to replace them."
            )

    progress(
        f"Extracting {len(typenames)} layers to {output_dir} "
        f"(parallel_layers={parallel_layers}, workers={max_workers})..."
    )

    # Build kwargs for each layer extraction
    kwargs = {
        "version": version,
        "bbox": bbox,
        "bbox_mode": bbox_mode,
        "output_crs": output_crs,
        "limit": limit,
        "max_workers": max_workers,
        "page_size": page_size,
        "axis_order": axis_order,
        "strict_crs": strict_crs,
        "skip_hilbert": skip_hilbert,
        "skip_bbox": skip_bbox,
        "compression": compression,
        "compression_level": compression_level,
        "row_group_size_mb": row_group_size_mb,
        "row_group_rows": row_group_rows,
        "geoparquet_version": geoparquet_version,
        "overwrite": overwrite,
        "verbose": verbose,
        "auto_tile": auto_tile,
        "sort_by": sort_by,
        "repair_geometry": repair_geometry,
    }

    results: dict[str, Path] = {}
    errors: list[tuple[str, str]] = []
    total_rows = 0

    actual_parallel = min(parallel_layers, len(typenames))

    if actual_parallel == 1:
        # Sequential mode
        for i, typename in enumerate(typenames):
            result_typename, path, rows, error = _extract_single_layer(
                service_url, typename, output_path, i, len(typenames), **kwargs
            )
            if error:
                errors.append((result_typename, error))
                warn(f"Failed to extract {result_typename}: {error}")
            else:
                results[result_typename] = path
                total_rows += rows or 0
    else:
        # Parallel mode
        with ThreadPoolExecutor(max_workers=actual_parallel) as executor:
            futures = {
                executor.submit(
                    _extract_single_layer,
                    service_url,
                    typename,
                    output_path,
                    i,
                    len(typenames),
                    **kwargs,
                ): typename
                for i, typename in enumerate(typenames)
            }

            for future in as_completed(futures):
                result_typename, path, rows, error = future.result()
                if error:
                    errors.append((result_typename, error))
                    warn(f"Failed to extract {result_typename}: {error}")
                else:
                    results[result_typename] = path
                    total_rows += rows or 0

    # Summary
    if errors:
        warn(f"Completed with {len(errors)} failures:")
        for typename, error in errors:
            warn(f"  {typename}: {error}")

    if not results:
        raise WFSError(f"All {len(typenames)} layer extractions failed")

    success(
        f"Extracted {len(results)}/{len(typenames)} layers ({total_rows:,} total features) to {output_dir}"
    )
    return results
