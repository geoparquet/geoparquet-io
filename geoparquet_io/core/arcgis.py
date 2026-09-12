"""
ArcGIS Feature Service to GeoParquet conversion.

This module provides functionality to download features from ArcGIS REST API
endpoints (FeatureServer/MapServer) and convert them to GeoParquet format.
"""

from __future__ import annotations

import json
import os
import tempfile
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.column_selection import (
    resolve_columns_against_schema,
    split_column_list,
)
from geoparquet_io.core.common import _cast_table_to_schema, write_geoparquet_table
from geoparquet_io.core.crs_utils import _extract_crs_identifier, parse_crs_string_to_projjson
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
from geoparquet_io.core.exceptions import (
    BatchTooLargeError,
    GeoParquetError,
    InvalidParameterError,
    RemoteAccessError,
)
from geoparquet_io.core.geo_metadata import _compute_geometry_types
from geoparquet_io.core.geometry_repair import repair_arrow_table_geometry
from geoparquet_io.core.http_retry import (
    DEFAULT_TIMEOUT,
    make_request_with_retry,
)
from geoparquet_io.core.logging_config import configure_verbose, debug, progress, success, warn
from geoparquet_io.core.remote import setup_aws_profile_if_needed

# ArcGIS Online token endpoint
ARCGIS_ONLINE_TOKEN_URL = "https://www.arcgis.com/sharing/rest/generateToken"

# Default page size for feature downloads (ArcGIS typical max is 2000)
DEFAULT_PAGE_SIZE = 2000

# Map ArcGIS WKID codes to EPSG codes for special cases
WKID_TO_EPSG = {
    102100: 3857,  # Web Mercator
    102113: 3785,  # Legacy Web Mercator
}


@dataclass
class ArcGISAuth:
    """Authentication configuration for ArcGIS services."""

    token: str | None = None
    token_file: str | None = None
    username: str | None = None
    password: str | None = None
    portal_url: str | None = None


@dataclass
class ArcGISLayerInfo:
    """Metadata about an ArcGIS layer."""

    name: str
    geometry_type: str
    spatial_reference: dict
    fields: list[dict]
    max_record_count: int
    total_count: int


# Adaptive batch size fallback sequence
BATCH_SIZE_FALLBACKS = [1000, 500, 100, 50, 10, 1]


@contextmanager
def _gdal_geojson_size_limit_lifted():
    """Lift GDAL's per-feature GeoJSON object size limit around an ST_Read.

    GDAL's GeoJSON/ESRIJSON drivers cap a single feature object at 200 MB by
    default (``OGR_GEOJSON_MAX_OBJ_SIZE``, in megabytes) and raise an
    ``IOException`` ("GeoJSON object too complex/large") when a feature exceeds
    it. Large, complex polygon layers — e.g. national admin boundaries with
    detailed coastlines — blow past this even at ``batch_size=1``, so the
    batch-size fallback ladder cannot recover. GDAL reads the option from the
    process environment; "0" removes the limit. See issue #517.

    Scoped rather than set once: this is a process-global environment variable,
    and permanently lifting GDAL's safety cap for an embedding application (or
    for every later, unrelated gpio call) is not ours to do.
    """
    previous = os.environ.get("OGR_GEOJSON_MAX_OBJ_SIZE")
    os.environ["OGR_GEOJSON_MAX_OBJ_SIZE"] = "0"
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("OGR_GEOJSON_MAX_OBJ_SIZE", None)
        else:
            os.environ["OGR_GEOJSON_MAX_OBJ_SIZE"] = previous


def _get_reduced_batch_size(current_batch: int) -> int | None:
    """
    Get the next smaller batch size from the fallback sequence.

    Args:
        current_batch: Current batch size that failed

    Returns:
        Next smaller batch size, or None if already at minimum (1)
    """
    if current_batch <= 1:
        return None

    # Find the first fallback smaller than current batch
    for fallback in BATCH_SIZE_FALLBACKS:
        if fallback < current_batch:
            return fallback

    # current_batch is very small but > 1, try 1
    return 1


def _make_request(
    method: str,
    url: str,
    params: dict | None = None,
    data: dict | None = None,
    max_retries: int = 3,
    retry_delay: float = 1.0,
    timeout: float = DEFAULT_TIMEOUT,
    batch_size: int | None = None,
) -> dict:
    """
    Make HTTP request with retry logic.

    Delegates to shared http_retry module for connection pooling and error handling.

    Args:
        method: HTTP method ("GET" or "POST")
        url: Request URL
        params: Query parameters
        data: POST data
        max_retries: Number of retry attempts
        retry_delay: Base delay between retries
        timeout: Request timeout in seconds
        batch_size: Current batch size (for BatchTooLargeError context)

    Returns:
        Parsed JSON response

    Raises:
        RemoteAccessError: For HTTP errors
        BatchTooLargeError: When server returns non-JSON (batch too large)
    """
    return make_request_with_retry(
        method=method,
        url=url,
        params=params,
        data=data,
        max_retries=max_retries,
        retry_delay=retry_delay,
        timeout=timeout,
        parse_json=True,
        batch_size=batch_size,
    )


def _handle_arcgis_response(data: dict, context: str) -> dict:
    """Handle ArcGIS REST API response and check for errors."""
    if "error" in data:
        error = data["error"]
        code = error.get("code", "Unknown")
        message = error.get("message", "Unknown error")
        details = error.get("details", [])

        if code in (498, 499):
            raise GeoParquetError(f"{context}: Invalid or expired token. Please re-authenticate.")
        else:
            detail_str = "; ".join(details) if details else ""
            raise GeoParquetError(f"{context}: Error {code} - {message}. {detail_str}")

    return data


def generate_token(
    username: str,
    password: str,
    portal_url: str | None = None,
    verbose: bool = False,
    timeout: float = DEFAULT_TIMEOUT,
) -> str:
    """
    Generate authentication token via ArcGIS REST API.

    Args:
        username: ArcGIS username
        password: ArcGIS password
        portal_url: Enterprise portal URL (default: ArcGIS Online)
        verbose: Whether to print debug output
        timeout: Request timeout in seconds

    Returns:
        Authentication token string

    Raises:
        GeoParquetError: If token generation fails
    """
    token_url = portal_url or ARCGIS_ONLINE_TOKEN_URL

    if verbose:
        debug(f"Generating token from {token_url}")

    data = {
        "username": username,
        "password": password,
        "referer": "geoparquet-io",
        "f": "json",
        "expiration": 60,  # 60 minutes
    }

    result = _make_request("POST", token_url, data=data, timeout=timeout)
    result = _handle_arcgis_response(result, "Token generation")

    if "token" not in result:
        raise GeoParquetError("Token generation failed: no token in response")

    if verbose:
        debug("Token generated successfully")

    return result["token"]


def resolve_token(
    auth: ArcGISAuth,
    service_url: str,
    verbose: bool = False,
    timeout: float = DEFAULT_TIMEOUT,
) -> str | None:
    """
    Resolve authentication token from various sources.

    Priority:
    1. Direct token parameter
    2. Token file (read from file path)
    3. Username/password (generate token via ArcGIS REST API)

    Args:
        auth: ArcGISAuth configuration
        service_url: Service URL (used to detect enterprise portal)
        verbose: Whether to print debug output
        timeout: Request timeout in seconds (used for token generation)

    Returns:
        Token string, or None if no auth provided
    """
    # Priority 1: Direct token
    if auth.token:
        if verbose:
            debug("Using direct token")
        return auth.token

    # Priority 2: Token file
    if auth.token_file:
        if verbose:
            debug(f"Reading token from file: {auth.token_file}")
        try:
            import fsspec

            with fsspec.open(auth.token_file, mode="rt") as f:
                return f.read().strip()
        except OSError as e:
            raise GeoParquetError(f"Failed to read token file: {e}") from e

    # Priority 3: Username/password
    if auth.username and auth.password:
        # Try to detect enterprise portal from service URL
        portal_url = auth.portal_url
        if not portal_url and "/arcgis/" in service_url.lower():
            # Enterprise server pattern: https://server.example.com/arcgis/rest/services/...
            # Token URL: https://server.example.com/arcgis/tokens/generateToken
            import re

            match = re.match(r"(https?://[^/]+/arcgis)", service_url, re.IGNORECASE)
            if match:
                portal_url = f"{match.group(1)}/tokens/generateToken"
                if verbose:
                    debug(f"Detected enterprise portal: {portal_url}")

        return generate_token(auth.username, auth.password, portal_url, verbose, timeout=timeout)

    return None


def _add_token_to_params(params: dict, token: str | None) -> dict:
    """Add authentication token to request parameters."""
    if token:
        return {**params, "token": token}
    return params


def validate_arcgis_url(url: str) -> tuple[str, int | None]:
    """
    Validate and parse ArcGIS Feature Service URL.

    Expected formats:
    - https://services.arcgis.com/.../FeatureServer/0
    - https://server.example.com/arcgis/rest/services/.../MapServer/0

    Args:
        url: ArcGIS service URL

    Returns:
        Tuple of (base_url, layer_id) where layer_id may be None

    Raises:
        InvalidParameterError: If URL is invalid
    """
    import re

    url = url.rstrip("/")

    # Check for ImageServer (raster - not supported)
    if "/ImageServer" in url:
        raise InvalidParameterError(
            "url",
            "ImageServer (raster) services are not supported. "
            "This command only supports vector services (FeatureServer or MapServer). "
            "ImageServer provides raster/imagery data which cannot be converted to GeoParquet.",
        )

    # Check for FeatureServer or MapServer
    if "/FeatureServer" not in url and "/MapServer" not in url:
        raise InvalidParameterError(
            "url",
            "Invalid ArcGIS URL. Expected format: https://services.arcgis.com/.../FeatureServer/0. "
            "The URL must point to a vector layer in a FeatureServer or MapServer. "
            "Make sure the URL includes /FeatureServer/ or /MapServer/ and a layer ID (e.g., /0).",
        )

    # Extract layer ID
    match = re.search(r"/(FeatureServer|MapServer)/(\d+)$", url)
    if match:
        return url, int(match.group(2))

    # URL ends with FeatureServer or MapServer without layer ID
    raise InvalidParameterError(
        "url",
        f"Missing layer ID. You must specify which layer to download by adding the layer ID "
        f"(e.g., {url}/0). To see available layers, open {url}?f=json in a browser.",
    )


def get_layer_info(
    service_url: str,
    token: str | None = None,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    verbose: bool = False,
    timeout: float = DEFAULT_TIMEOUT,
) -> ArcGISLayerInfo:
    """
    Fetch layer metadata from ArcGIS REST service.

    Args:
        service_url: Full layer URL (e.g., .../FeatureServer/0)
        token: Optional authentication token
        where: SQL WHERE clause for counting features (default: "1=1" = all)
        bbox: Bounding box filter (xmin, ymin, xmax, ymax) in WGS84
        verbose: Whether to print debug output
        timeout: Request timeout in seconds

    Returns:
        ArcGISLayerInfo with layer metadata
    """
    if verbose:
        debug(f"Fetching layer info from {service_url}")

    params = _add_token_to_params({"f": "json"}, token)
    data = _make_request("GET", service_url, params=params, timeout=timeout)
    data = _handle_arcgis_response(data, "Layer info")

    # Get feature count (using the WHERE and bbox filters)
    count = get_feature_count(
        service_url, where=where, bbox=bbox, token=token, verbose=verbose, timeout=timeout
    )

    # Servers advertise the layer SR in different places: top-level
    # spatialReference, extent.spatialReference, or sourceSpatialReference.
    # An empty dict (not a silent 4326 default) marks "no advertised SR" so
    # --output-crs native can fail honestly instead of resolving to the wrong CRS.
    spatial_reference = (
        data.get("spatialReference")
        or data.get("extent", {}).get("spatialReference")
        or data.get("sourceSpatialReference")
        or {}
    )

    return ArcGISLayerInfo(
        name=data.get("name", "Unknown"),
        geometry_type=data.get("geometryType", "esriGeometryPoint"),
        spatial_reference=spatial_reference,
        fields=data.get("fields", []),
        max_record_count=data.get("maxRecordCount", 1000),
        total_count=count,
    )


def get_feature_count(
    service_url: str,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    token: str | None = None,
    verbose: bool = False,
    timeout: float = DEFAULT_TIMEOUT,
) -> int:
    """
    Get total feature count from ArcGIS service.

    Args:
        service_url: Full layer URL
        where: WHERE clause filter
        bbox: Bounding box filter (xmin, ymin, xmax, ymax) in WGS84
        token: Optional authentication token
        verbose: Whether to print debug output
        timeout: Request timeout in seconds

    Returns:
        Feature count
    """
    query_url = f"{service_url}/query"
    params = {
        "where": where,
        "returnCountOnly": "true",
        "f": "json",
    }

    # Add bbox filter if provided
    if bbox:
        xmin, ymin, xmax, ymax = bbox
        params["geometry"] = f"{xmin},{ymin},{xmax},{ymax}"
        params["geometryType"] = "esriGeometryEnvelope"
        params["spatialRel"] = "esriSpatialRelIntersects"
        params["inSR"] = "4326"

    params = _add_token_to_params(params, token)

    data = _make_request("GET", query_url, params=params, timeout=timeout)
    data = _handle_arcgis_response(data, "Feature count")

    count = data.get("count", 0)
    if verbose:
        debug(f"Total feature count: {count}")

    return count


def fetch_features_page(
    service_url: str,
    offset: int,
    limit: int,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    out_fields: str = "*",
    token: str | None = None,
    output_wkid: int | None = None,
    max_allowable_offset: float | None = None,
    verbose: bool = False,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict:
    """
    Fetch a single page of features as GeoJSON.

    Args:
        service_url: Full layer URL
        offset: Starting position for results (0-based)
        limit: Number of records to return
        where: WHERE clause filter
        bbox: Bounding box filter (xmin, ymin, xmax, ymax) in WGS84
        out_fields: Comma-separated field names or "*" for all
        token: Optional authentication token
        output_wkid: Optional output WKID (e.g. 25830). When set, requests
            EsriJSON (f=json) with outSR so the server reprojects before delivery.
        max_allowable_offset: Optional server-side geometry generalization
            tolerance, in the units of the output CRS (degrees on the default
            WGS84 path). Reduces vertices per feature for very large geometries.
        verbose: Whether to print debug output
        timeout: Request timeout in seconds. Raise for layers whose large or
            complex geometries take the server minutes to serialize per page.

    Returns:
        GeoJSON FeatureCollection dict, or EsriJSON dict when output_wkid is set
    """
    query_url = f"{service_url}/query"
    params = {
        "where": where,
        "outFields": out_fields,
        "returnGeometry": "true",
        "f": "geojson",
        "resultOffset": str(offset),
        "resultRecordCount": str(limit),
    }

    # Server-side geometry generalization (Douglas-Peucker). Honored on both the
    # GeoJSON and EsriJSON paths. ArcGIS measures the tolerance in outSR units and
    # falls back to the layer's source units when outSR is unset, so anchor outSR
    # (to 4326 on the default GeoJSON path) to keep the unit well-defined. The
    # output_wkid branch below overrides this with the requested SR when present.
    if max_allowable_offset is not None:
        params["maxAllowableOffset"] = str(max_allowable_offset)
        params.setdefault("outSR", "4326")

    # Add bbox filter if provided (spatial query)
    if bbox:
        xmin, ymin, xmax, ymax = bbox
        params["geometry"] = f"{xmin},{ymin},{xmax},{ymax}"
        params["geometryType"] = "esriGeometryEnvelope"
        params["spatialRel"] = "esriSpatialRelIntersects"
        params["inSR"] = "4326"  # WGS84

    # Native-CRS path: ArcGIS honors outSR only for EsriJSON (f=json), not GeoJSON.
    if output_wkid is not None:
        params["f"] = "json"
        params["outSR"] = str(output_wkid)

    params = _add_token_to_params(params, token)

    data = _make_request("GET", query_url, params=params, timeout=timeout, batch_size=limit)

    # GeoJSON responses don't have the standard error format
    # Check if we got features or an error
    if "error" in data:
        _handle_arcgis_response(data, "Feature query")

    return data


def fetch_all_features(
    service_url: str,
    layer_info: ArcGISLayerInfo,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    out_fields: str = "*",
    max_features: int | None = None,
    token: str | None = None,
    batch_size: int | None = None,
    max_workers: int = 1,
    output_wkid: int | None = None,
    max_allowable_offset: float | None = None,
    verbose: bool = False,
    timeout: float = DEFAULT_TIMEOUT,
) -> Generator[dict, None, None]:
    """
    Generator that yields pages of GeoJSON features.

    Handles pagination using resultOffset/resultRecordCount.

    Args:
        service_url: Full layer URL
        layer_info: Layer metadata
        where: WHERE clause filter
        bbox: Bounding box filter (xmin, ymin, xmax, ymax) in WGS84
        out_fields: Comma-separated field names or "*" for all
        max_features: Maximum total features to return (limit)
        token: Optional authentication token
        batch_size: Custom batch size (default: server's maxRecordCount)
        max_workers: Number of concurrent requests (1 = sequential, 2-3 recommended)
        verbose: Whether to print debug output
        timeout: Per-request timeout in seconds

    Yields:
        GeoJSON FeatureCollection dicts for each page
    """
    # Validate max_workers
    if max_workers < 1:
        raise ValueError("max_workers must be at least 1")
    if max_workers > 10:
        warn(
            f"max_workers={max_workers} may trigger rate limits. "
            f"Recommended range: 1-10 (2-3 for best balance)"
        )

    # Sanitize and validate batch_size
    sanitized_batch = batch_size if batch_size and batch_size > 0 else DEFAULT_PAGE_SIZE

    # Determine batch size (respect server limit)
    max_batch = min(
        sanitized_batch,
        layer_info.max_record_count or DEFAULT_PAGE_SIZE,
    )

    # Apply user limit to total
    total = layer_info.total_count
    if max_features is not None:
        total = min(total, max_features)

    if max_workers == 1:
        # Sequential fetching with adaptive batch size
        offset = 0
        fetched = 0
        effective_batch = max_batch  # May be reduced on BatchTooLargeError

        while offset < total:
            # Adjust batch size for last page if limit applies
            remaining = total - offset
            current_batch = min(effective_batch, remaining)

            end = min(offset + current_batch, total)
            progress(f"Fetching features {offset + 1}-{end} of {total}...")

            try:
                page = fetch_features_page(
                    service_url,
                    offset,
                    current_batch,
                    where,
                    bbox=bbox,
                    out_fields=out_fields,
                    token=token,
                    output_wkid=output_wkid,
                    max_allowable_offset=max_allowable_offset,
                    verbose=verbose,
                    timeout=timeout,
                )
            except BatchTooLargeError as e:
                # Server couldn't handle batch size - reduce and retry
                new_batch = _get_reduced_batch_size(current_batch)
                if new_batch is None:
                    # Already at minimum batch size, can't reduce further
                    raise GeoParquetError(
                        f"Server cannot handle even batch_size=1. "
                        f"This layer may have geometry too complex to download. "
                        f"Original error: {e.reason}"
                    ) from e

                warn(
                    f"Server returned error for batch_size={current_batch}. "
                    f"Reducing to {new_batch} and retrying..."
                )
                effective_batch = new_batch
                # Don't increment offset - retry same position with smaller batch
                continue

            features = page.get("features", [])
            if not features:
                break

            yield page

            fetched += len(features)
            offset += current_batch

            # Safety check: if server returned fewer than expected, adjust
            if len(features) < current_batch and offset < total:
                offset = fetched

            # Stop if we've hit the user limit
            if max_features is not None and fetched >= max_features:
                break

        if verbose:
            debug(f"Fetched {fetched} features total")

    else:
        # Parallel fetching with ThreadPoolExecutor
        from concurrent.futures import ThreadPoolExecutor

        fetched = 0
        batch_start = 0
        effective_batch = max_batch  # May be reduced on BatchTooLargeError

        # Reuse a single thread pool for all batches (more efficient)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while batch_start < total:
                # Submit max_workers requests in parallel
                futures = []

                for i in range(max_workers):
                    offset = batch_start + (i * effective_batch)
                    if offset >= total:
                        break

                    remaining = total - offset
                    current_batch = min(effective_batch, remaining)
                    end = min(offset + current_batch, total)

                    # Print progress message synchronously (avoid race condition)
                    progress(f"Fetching features {offset + 1}-{end} of {total}...")

                    future = executor.submit(
                        fetch_features_page,
                        service_url,
                        offset,
                        current_batch,
                        where,
                        bbox=bbox,
                        out_fields=out_fields,
                        token=token,
                        output_wkid=output_wkid,
                        max_allowable_offset=max_allowable_offset,
                        verbose=False,  # Disable per-request verbose to avoid race conditions
                        timeout=timeout,
                    )
                    futures.append((offset, current_batch, future))

                # Collect results in order
                results = []
                batch_too_large = False
                failed_batch_size = None
                failed_at_index = None

                for idx, (offset, req_batch_size, future) in enumerate(futures):
                    try:
                        page = future.result()
                        results.append((offset, page))
                    except BatchTooLargeError:
                        # Mark for retry with smaller batch
                        batch_too_large = True
                        failed_batch_size = req_batch_size
                        failed_at_index = idx
                        break
                    except Exception as e:
                        # If one request fails, propagate the error (fail-fast)
                        raise RemoteAccessError(
                            service_url, f"Failed to fetch features at offset {offset}: {e}"
                        ) from e

                if batch_too_large:
                    # Cancel/drain remaining futures to avoid duplicate requests
                    from concurrent.futures import CancelledError

                    for remaining_idx in range(failed_at_index + 1, len(futures)):
                        _, _, remaining_future = futures[remaining_idx]
                        remaining_future.cancel()
                        # Drain any that couldn't be cancelled
                        try:
                            remaining_future.result(timeout=0.1)
                        except (CancelledError, BatchTooLargeError, Exception):
                            pass  # Expected - just draining

                    # Clear for retry
                    futures.clear()
                    results.clear()

                    # Reduce batch size and restart from batch_start
                    new_batch = _get_reduced_batch_size(failed_batch_size or effective_batch)
                    if new_batch is None:
                        raise GeoParquetError(
                            "Server cannot handle even batch_size=1. "
                            "This layer may have geometry too complex to download."
                        )
                    warn(
                        f"Server returned error for batch_size={failed_batch_size}. "
                        f"Reducing to {new_batch} and retrying..."
                    )
                    effective_batch = new_batch
                    # Don't increment batch_start - retry from same position
                    continue

                # Sort by offset to maintain order
                results.sort(key=lambda x: x[0])

                # Yield pages in sequential order
                for _offset, page in results:
                    features = page.get("features", [])
                    if not features:
                        continue

                    # Check limit before yielding
                    if max_features is not None and fetched >= max_features:
                        break

                    yield page
                    fetched += len(features)

                # Move to next batch
                batch_start += max_workers * effective_batch

                # Stop if we've hit the user limit
                if max_features is not None and fetched >= max_features:
                    break

        if verbose:
            debug(f"Fetched {fetched} features total using {max_workers} workers")


def _wkid_from_spatial_reference(spatial_ref: dict) -> int | None:
    """Return the WKID from an ArcGIS spatial reference dict, or None.

    Prefers latestWkid (the current EPSG-aligned code) over the legacy
    Esri-specific wkid, e.g. {"wkid": 102100, "latestWkid": 3857} -> 3857.
    """
    return spatial_ref.get("latestWkid") or spatial_ref.get("wkid")


def _normalize_wkid(wkid: int) -> int:
    """Map legacy Esri WKIDs to their EPSG equivalents (e.g. 102100 -> 3857)."""
    return WKID_TO_EPSG.get(wkid, wkid)


def _parse_crs_to_wkid(value: str) -> int:
    """Parse a CRS string to an integer WKID.

    Accepts 'EPSG:25830' (any case), URN forms, or a bare '25830'.
    """
    cleaned = value.strip()
    if cleaned.isdigit():
        return int(cleaned)
    identifier = _extract_crs_identifier(cleaned)
    if identifier is None:
        raise ValueError(f"Cannot parse CRS '{value}'. Use 'EPSG:<code>' or a numeric code.")
    authority, code = identifier
    if authority != "EPSG":
        raise ValueError(f"Unsupported CRS authority '{authority}' in '{value}'. Use EPSG codes.")
    if not isinstance(code, int):
        raise ValueError(f"Cannot parse CRS '{value}'. Use 'EPSG:<code>' or a numeric code.")
    return code


def _projjson_from_wkid(wkid: int) -> dict | None:
    """Resolve an ArcGIS WKID to PROJJSON, trying the EPSG then ESRI authority.

    Many ArcGIS layers advertise Esri-authority WKIDs (e.g. 102039) that are
    not valid EPSG codes; tagging those as EPSG would write CRS metadata no
    consumer can resolve.

    Returns None, silently, when neither authority knows the code. The caller
    decides what that means -- a sibling carrier of the same spatial reference
    may still resolve it, and when none does the answer is to refuse, not to
    warn and continue (#1039).
    """
    code = _normalize_wkid(wkid)
    try:
        from pyproj import CRS
        from pyproj.exceptions import CRSError
    except ImportError:
        return {"id": {"authority": "EPSG", "code": code}}
    for authority in ("EPSG", "ESRI"):
        try:
            return CRS.from_authority(authority, code).to_json_dict()
        except CRSError:
            continue
    return None


def _projjson_from_wkt(wkt: str) -> dict | None:
    """Resolve an ArcGIS WKT (or WKT2) spatial reference to PROJJSON, or None.

    ArcGIS supplies ``wkt`` for spatial references no authority registers --
    custom projections above all -- and often alongside a vendor WKID. PROJJSON
    does not require an ``id``, so an unregistered CRS still round-trips as a
    complete description, which is strictly better than declaring the column
    CRS-less.

    Not :func:`~geoparquet_io.core.crs_utils.parse_crs_string_to_projjson`:
    that one reads an ``AUTHORITY:CODE`` identifier out of its argument and
    returns None for every WKT string, which is why the WKT fallback below it
    never resolved anything before #1039.

    pyproj is a hard dependency, imported unguarded here as it is in
    :func:`_resolve_native_wkid` below. :func:`_projjson_from_wkid`'s
    ``ImportError`` guard predates that being true.
    """
    from pyproj import CRS
    from pyproj.exceptions import CRSError

    try:
        return CRS.from_wkt(wkt).to_json_dict()
    except (CRSError, TypeError):
        return None


def _wkid_candidates(spatial_ref: dict) -> list[int]:
    """Every WKID an ArcGIS spatial reference offers, best first, deduplicated.

    ``latestWkid`` is the modern EPSG-aligned code and wins when both resolve,
    but "present" and "resolvable" are different questions: a server can send a
    vendor ``latestWkid`` beside a perfectly good legacy ``wkid``.
    """
    candidates: list[int] = []
    for key in ("latestWkid", "wkid"):
        raw = spatial_ref.get(key)
        # Some services quote the code. ``bool`` is an ``int`` subclass and is
        # never a WKID.
        if isinstance(raw, str) and raw.strip().isdigit():
            raw = int(raw)
        if not isinstance(raw, int) or isinstance(raw, bool):
            continue
        code = _normalize_wkid(raw)
        if code not in candidates:
            candidates.append(code)
    return candidates


def _resolve_native_wkid(spatial_ref: dict) -> int:
    """Resolve a layer's advertised native SR to an EPSG/ESRI WKID for outSR.

    Prefers an advertised WKID that some authority actually resolves. Falls back
    to deriving an EPSG code from an advertised WKT (common for layers that omit
    a WKID, and the only rescue for one whose WKID no authority knows). Raises
    when the SR is absent or its WKT maps to no EPSG code.
    """
    candidates = _wkid_candidates(spatial_ref)
    for wkid in candidates:
        if _projjson_from_wkid(wkid) is not None:
            return wkid

    wkt = spatial_ref.get("wkt")
    if wkt:
        from pyproj import CRS
        from pyproj.exceptions import CRSError

        try:
            epsg = CRS.from_wkt(wkt).to_epsg()
        except CRSError:
            epsg = None
        if epsg is not None:
            return epsg
        raise GeoParquetError(
            "--output-crs native could not resolve the layer's WKT spatial reference "
            "to an EPSG code. Pass an explicit --output-crs EPSG:<code> instead."
        )

    if candidates:
        # The layer advertises a WKID; it is just one nothing resolves, and it
        # would have been sent as outSR and then written as no CRS at all.
        raise _unresolvable_output_crs(spatial_ref)

    raise GeoParquetError(
        "--output-crs native requested but the layer advertises no spatial reference."
    )


def _extract_crs_from_spatial_reference(spatial_ref: dict) -> dict | None:
    """Resolve an ArcGIS spatial reference to PROJJSON, or None when none of it does.

    An ArcGIS ``spatialReference`` object describes one CRS through up to three
    carriers -- ``latestWkid`` (the modern EPSG-aligned code), ``wkid`` (which
    may be an Esri-authority or legacy code), and ``wkt``/``wkt2`` (the full
    definition, and all an unregistered CRS has). Each is tried in turn, because
    any one of them resolving is enough and the first one present is not always
    the one that does: before #1039 a WKID nothing knew ended the search, with
    the WKT beside it never read.

    None means the service described a CRS gpio cannot write down. It is not
    "no CRS": callers must refuse rather than omit the key, because GeoParquet
    reads an absent ``crs`` as OGC:CRS84.
    """
    for wkid in _wkid_candidates(spatial_ref):
        projjson = _projjson_from_wkid(wkid)
        if projjson is not None:
            return projjson

    for key in ("wkt2", "wkt"):
        wkt = spatial_ref.get(key)
        if wkt:
            projjson = _projjson_from_wkt(wkt)
            if projjson is not None:
                return projjson

    if not spatial_ref:
        # An empty spatial reference is not an unresolvable one: ArcGIS's own
        # default is WGS84, and that is a CRS gpio can state.
        return parse_crs_string_to_projjson("EPSG:4326")

    return None


def _unresolvable_output_crs(spatial_ref: dict) -> InvalidParameterError:
    """The refusal for an output CRS gpio has no way to write down.

    Refusing is the point, and it is not the same choice as warning louder.
    GeoParquet spells "the CRS is OGC:CRS84" as an *absent* ``crs`` key, so a
    file written with the CRS left out does not record "unknown" -- it asserts
    longitude/latitude over projected easting/northing, and ``gpio check spec``
    then fails the file gpio has just written (#1039). Writing ``crs: null``
    instead would stop the lie but still discard a CRS the service knew, leaving
    data nothing downstream can reproject; and ``_resolve_native_wkid``, the
    other half of the same question, has always refused rather than guess.
    """
    codes = " / ".join(
        f"{key}={spatial_ref[key]}" for key in ("latestWkid", "wkid") if spatial_ref.get(key)
    )
    wkt_note = (
        " and the WKT supplied beside it does not parse"
        if (spatial_ref.get("wkt") or spatial_ref.get("wkt2"))
        else " and no WKT was supplied for it"
    )
    return InvalidParameterError(
        "output_crs",
        f"the output coordinates would be in {codes}, which resolves under neither the "
        f"EPSG nor the ESRI authority{wkt_note}, so gpio cannot describe the CRS it "
        "would be writing. A GeoParquet file with no `crs` key does not mean 'unknown' "
        "-- it means OGC:CRS84 -- so writing this would label projected coordinates as "
        "longitude/latitude. Pass --output-crs EPSG:<code> with a code that resolves, "
        "reproject the data yourself, or omit --output-crs to fetch WGS84 lon/lat from "
        "the service.",
    )


def _resolve_field_selection(
    layer_info: ArcGISLayerInfo,
    include_list: list[str] | None,
    exclude_list: list[str] | None,
) -> tuple[list[str] | None, list[str] | None]:
    """Check the requested field names against the layer's advertised fields.

    ``include_list`` becomes ``outFields`` and so names *service* fields;
    ``exclude_list`` is applied to the downloaded table, whose columns are
    ``geometry`` plus those same fields (see
    :func:`_build_schema_from_layer_info`) — hence the extra allowed name.

    ``*`` is ArcGIS's own spelling for "every field" in ``outFields`` and passes
    through untouched; it is not a field name, so there is nothing to resolve.

    Returns:
        Both lists in the layer's own spelling of each name.
    """
    field_names = [str(field["name"]) for field in layer_info.fields]
    if include_list != ["*"]:
        include_list = resolve_columns_against_schema(include_list, field_names, "--include-cols")
    exclude_list = resolve_columns_against_schema(
        exclude_list, ["geometry", *field_names], "--exclude-cols"
    )
    return include_list, exclude_list


def _build_schema_from_layer_info(layer_info: ArcGISLayerInfo) -> pa.Schema:
    """
    Build a fixed PyArrow schema from ArcGIS layer metadata.

    This prevents schema mismatches when different batches infer different
    types for the same field (e.g., nulls in batch 1 vs actual values in batch 2).

    Args:
        layer_info: ArcGIS layer metadata containing field definitions

    Returns:
        PyArrow schema with geometry column + attribute fields
    """
    # Map esriFieldType to PyArrow types
    # Reference: https://developers.arcgis.com/rest/services-reference/enterprise/fields/
    TYPE_MAPPING = {
        "esriFieldTypeSmallInteger": pa.int16(),
        "esriFieldTypeInteger": pa.int32(),
        "esriFieldTypeBigInteger": pa.int64(),
        "esriFieldTypeSingle": pa.float32(),
        "esriFieldTypeDouble": pa.float64(),
        "esriFieldTypeString": pa.string(),
        "esriFieldTypeDate": pa.timestamp("ms"),
        "esriFieldTypeDateOnly": pa.date32(),
        "esriFieldTypeTimeOnly": pa.time32("ms"),
        "esriFieldTypeOID": pa.int64(),
        "esriFieldTypeGeometry": pa.binary(),  # Shouldn't appear in fields, but handle defensively
        "esriFieldTypeBlob": pa.binary(),
        "esriFieldTypeGUID": pa.string(),
        "esriFieldTypeGlobalID": pa.string(),
        "esriFieldTypeXML": pa.string(),
    }

    fields = []

    # Geometry column always comes first (WKB binary)
    # Some features may have null geometries (attributes without spatial data)
    fields.append(pa.field("geometry", pa.binary(), nullable=True))

    # Add attribute fields based on layer metadata
    for field_info in layer_info.fields:
        field_name = field_info["name"]
        field_type = field_info["type"]
        nullable = field_info.get("nullable", True)

        # Map esriFieldType to PyArrow type
        if field_type in TYPE_MAPPING:
            pa_type = TYPE_MAPPING[field_type]
        else:
            # Unknown type - fallback to string with warning
            warn(
                f"Unknown ArcGIS field type '{field_type}' for field '{field_name}'. "
                f"Falling back to string type."
            )
            pa_type = pa.string()

        fields.append(pa.field(field_name, pa_type, nullable=nullable))

    return pa.schema(fields)


def _json_doc_to_table(doc: dict, exclude: str, suffix: str, con=None) -> pa.Table:
    """Write a JSON document to a temp file and convert it via DuckDB ST_Read.

    Shared mechanics for the GeoJSON and EsriJSON page converters.

    Args:
        doc: JSON-serializable document (GeoJSON FeatureCollection or raw
            EsriJSON page)
        exclude: Comma-separated columns for the SQL EXCLUDE clause
        suffix: Temp file suffix (controls GDAL driver detection)
        con: Optional DuckDB connection to reuse across pages; a fresh one is
            created and closed when omitted.

    Returns:
        PyArrow Table with WKB geometry column
    """
    owns_con = con is None
    if owns_con:
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    temp_file = tempfile.gettempdir() + f"/arcgis_page_{uuid.uuid4()}{suffix}"

    try:
        with open(temp_file, "w") as f:
            json.dump(doc, f)

        query = f"""
            SELECT
                ST_AsWKB(geom) as geometry,
                * EXCLUDE ({exclude})
            FROM ST_Read({sql_path(temp_file)})
        """

        # Allow arbitrarily large/complex single features through GDAL's parser
        # (issue #517) — must be in effect while ST_Read reads the temp file.
        with _gdal_geojson_size_limit_lifted():
            return con.execute(query).arrow().read_all()

    finally:
        if owns_con:
            con.close()
        if os.path.exists(temp_file):
            os.unlink(temp_file)


def _geojson_page_to_table(features: list[dict], con=None) -> pa.Table | None:
    """
    Convert a page of GeoJSON features to PyArrow Table with WKB geometry.

    Uses DuckDB's spatial extension for geometry conversion.
    This function is designed to handle a single page (~2000 features)
    to keep memory usage low.

    Args:
        features: List of GeoJSON feature dicts (typically one page)
        con: Optional DuckDB connection to reuse

    Returns:
        PyArrow Table with WKB geometry column, or None if no features
    """
    if not features:
        return None

    # DuckDB ST_Read adds OGC_FID for GeoJSON, which we exclude
    return _json_doc_to_table(
        {"type": "FeatureCollection", "features": features},
        exclude="geom, OGC_FID",
        suffix=".geojson",
        con=con,
    )


def _esrijson_page_to_table(page: dict, con=None) -> pa.Table | None:
    """Convert a page of EsriJSON features to a PyArrow Table with WKB geometry.

    Unlike GeoJSON, GDAL's ESRIJSON driver needs the whole response object
    (geometryType / spatialReference / fields / features), so the raw page is
    written verbatim. `attributes` are flattened to columns by the driver.
    GDAL may or may not add OGC_FID for EsriJSON; extra columns are kept here
    and dropped later by _cast_table_to_schema.

    Returns a PyArrow Table with WKB geometry column, or None if no features.
    """
    if not page.get("features"):
        return None

    return _json_doc_to_table(page, exclude="geom", suffix=".json", con=con)


def _stream_features_to_parquet(
    service_url: str,
    layer_info: ArcGISLayerInfo,
    output_path: str,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    out_fields: str = "*",
    max_features: int | None = None,
    token: str | None = None,
    batch_size: int | None = None,
    max_workers: int = 1,
    output_wkid: int | None = None,
    max_allowable_offset: float | None = None,
    verbose: bool = False,
    timeout: float = DEFAULT_TIMEOUT,
) -> tuple[int, dict | None]:
    """
    Stream features from ArcGIS to a Parquet file page by page.

    This is memory-efficient as it only keeps one page (~2000 features)
    in memory at a time. The output is a raw parquet file without
    Hilbert ordering or bbox column (those are applied in a second pass).

    Args:
        service_url: ArcGIS Feature Service URL
        layer_info: Layer metadata
        output_path: Path to write the parquet file
        where: SQL WHERE clause filter
        bbox: Bounding box filter (xmin, ymin, xmax, ymax) in WGS84
        out_fields: Comma-separated field names or "*" for all
        max_features: Maximum total features to return (limit)
        token: Optional authentication token
        batch_size: Custom batch size for pagination
        max_workers: Number of concurrent requests (1 = sequential, 2-3 recommended)
        output_wkid: Preserve native CRS. When set (e.g. 25830), fetches
            EsriJSON (f=json) with outSR; default None fetches GeoJSON in WGS84.
        verbose: Whether to print debug output
        timeout: Per-request timeout in seconds

    Returns:
        Tuple of (number of features written, the server's returned
        spatialReference dict or None). The spatialReference is only populated
        on the EsriJSON path (output_wkid set).
    """
    # Build fixed schema from layer metadata upfront to prevent type mismatches
    # between batches (issue #290)
    target_schema = _build_schema_from_layer_info(layer_info)

    # Filter schema to match requested fields (if out_fields specified)
    if out_fields != "*":
        requested_fields = {f.strip().lower() for f in out_fields.split(",")}
        # Always include geometry
        filtered_fields = [target_schema.field("geometry")]
        # Add only requested attribute fields (case-insensitive match)
        for field in target_schema:
            if field.name != "geometry" and field.name.lower() in requested_fields:
                filtered_fields.append(field)
        target_schema = pa.schema(filtered_fields)

    debug(f"Built schema from layer metadata: {len(target_schema)} fields")

    writer = None
    total_rows = 0
    page_count = 0
    detected_sr: dict | None = None

    # One DuckDB connection for all page conversions (a fresh connection per
    # page would re-load the spatial extension thousands of times)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)

    try:
        for page in fetch_all_features(
            service_url,
            layer_info,
            where,
            bbox=bbox,
            out_fields=out_fields,
            max_features=max_features,
            token=token,
            batch_size=batch_size,
            max_workers=max_workers,
            output_wkid=output_wkid,
            max_allowable_offset=max_allowable_offset,
            verbose=verbose,
            timeout=timeout,
        ):
            features = page.get("features", [])
            if not features:
                continue

            # Capture the server-returned spatial reference once (output_wkid path)
            if output_wkid is not None and detected_sr is None:
                detected_sr = page.get("spatialReference")

            # Convert this page to Arrow table
            if output_wkid is not None:
                page_table = _esrijson_page_to_table(page, con=con)
            else:
                page_table = _geojson_page_to_table(features, con=con)
            if page_table is None:
                continue

            page_count += 1

            # Reconcile this page to the authoritative metadata schema using the
            # shared helper from core/common.py (same code path as the WFS extractor):
            # it reorders columns, fills missing fields with typed nulls, drops extras,
            # and casts each column to the metadata type with page context in errors.
            # The metadata schema stays authoritative (e.g. int32 declared fields are
            # narrowed back from DuckDB's int64 inference); promotion is intentionally
            # NOT used here because, unlike WFS, ArcGIS has an authoritative schema.
            try:
                page_table = _cast_table_to_schema(
                    page_table, target_schema, page_info=f"batch {page_count}"
                )
            except ValueError as e:
                # Cast failure (e.g. a value that overflows a declared narrower type)
                raise GeoParquetError(
                    f"Failed to cast batch {page_count} to target schema. "
                    f"The page's values could not be reconciled with the "
                    f"layer's declared field types. {e}"
                ) from e

            # Initialize writer on first page using the reconciled page's schema.
            # _cast_table_to_schema produces nullable columns (it rebuilds the table),
            # so we seed the writer from the reconciled table rather than the metadata
            # schema to avoid a nullability mismatch. Column types still match metadata;
            # geo metadata is (re)derived in the second pass, not from this writer.
            if writer is None:
                writer = pq.ParquetWriter(output_path, page_table.schema)

            # Write this page
            writer.write_table(page_table)
            total_rows += page_table.num_rows

            # Free memory from this page
            del page_table

        debug(f"Streamed {total_rows} features in {page_count} pages to temp file")
        return total_rows, detected_sr

    finally:
        con.close()
        if writer is not None:
            writer.close()


def _resolve_geometry_types(table: pa.Table, esri_geometry_type: str, verbose: bool) -> list[str]:
    """Geometry types for the geo block, read from the fetched WKB (#928).

    The layer's advertised ``geometryType`` does not describe the file. It is
    coarser than the data (Esri's ``esriGeometryPolyline`` covers both
    LineString and MultiLineString, and says nothing about Z/M), it does not
    cover every Esri type, and it used to fall back to ``"Geometry"`` -- which the
    GeoParquet schema does not allow, so unmapped layers wrote files that
    failed validation on their own metadata.

    Computing from the data instead is what every gpio write path does, gives
    the spec's " Z"/" M"/" ZM" suffixes for free, and keeps the declaration
    consistent with the statistics a reader checks it against.

    The declared type is no longer a fallback either (#962). Falling back
    whenever the compute came back empty put the invented value straight back on
    three reachable paths, because an empty result does not mean "the data was
    fine and said nothing": ``unique_geometry_types`` is all-or-nothing, so one
    malformed row empties the answer for a whole column of good geometry; an
    all-NULL column has nothing to describe; and ``--exclude-cols geometry``
    leaves no column to read. In each case the layer's coarse advertisement
    (``esriGeometryPolygon`` -> MultiPolygon, over data holding a single
    Polygon or no geometry at all) named a type the file does not contain.
    ``[]`` -- the spec's way of saying the types are not known -- is the honest
    answer wherever the data does not supply one, and gpio's own
    geometry_types-vs-statistics check would flag the guess anyway.

    The two ways of arriving at ``[]`` are still worth telling apart for the
    user: unreadable geometry is a data-quality problem worth chasing, an empty
    column is not. They cannot be told apart by catching, because
    ``_compute_geometry_types`` swallows the geoarrow error itself, so the
    non-NULL geometries are counted first.
    """
    geometry = table.column("geometry") if "geometry" in table.column_names else None
    if geometry is None:
        # The user dropped the column with --exclude-cols; the service may well
        # have returned geometry. Blaming the fetch here would point at the
        # wrong thing and give the user nothing to act on.
        warn(
            "The geometry column was excluded, so there is nothing to describe; "
            "declaring geometry_types as [] (types not known)."
        )
        return []
    if geometry.null_count == len(geometry):
        warn("Fetched data holds no geometries; declaring geometry_types as [] (types not known).")
        return []

    computed = _compute_geometry_types(table, "geometry", verbose)
    if not computed:
        warn(
            "Fetched geometry could not be read, so geometry_types is declared as [] "
            f"rather than guessed from the layer's {esri_geometry_type}."
        )
    return computed


def arcgis_to_table(
    service_url: str,
    auth: ArcGISAuth | None = None,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    include_cols: str | None = None,
    exclude_cols: str | None = None,
    limit: int | None = None,
    batch_size: int | None = None,
    max_workers: int = 1,
    output_crs: str | None = None,
    max_allowable_offset: float | None = None,
    verbose: bool = False,
    repair_geometry: bool = True,
    timeout: float = DEFAULT_TIMEOUT,
) -> pa.Table:
    """
    Convert ArcGIS Feature Service to PyArrow Table.

    Uses a memory-efficient two-pass approach:
    1. Stream features page-by-page to a temp parquet file
    2. Read the parquet file back as an Arrow table

    This keeps memory usage low during download (only one page at a time),
    while still producing a complete Arrow table for further processing.

    Server-side filtering is applied to minimize data transfer:
    - where: SQL WHERE clause pushed to server
    - bbox: Spatial filter pushed to server
    - include_cols: Field selection pushed to server (outFields)
    - limit: Row limit applied during pagination

    Args:
        service_url: ArcGIS Feature Service URL (with layer ID)
        auth: Optional authentication configuration
        where: SQL WHERE clause filter
        bbox: Bounding box filter (xmin, ymin, xmax, ymax) in WGS84
        include_cols: Comma-separated column names to include (server-side)
        exclude_cols: Comma-separated column names to exclude (client-side after download)
        limit: Maximum number of features to return
        batch_size: Custom batch size for pagination
        max_workers: Number of concurrent requests (1 = sequential, 2-3 recommended)
        output_crs: Preserve native CRS instead of reprojecting to WGS84. Use
            "native" for the layer's advertised SR, or an EPSG code (e.g.
            "EPSG:25830"). Default None fetches GeoJSON in WGS84 (CRS84).
        max_allowable_offset: Server-side geometry generalization tolerance, in
            the units of the output CRS (degrees on the default WGS84 path).
            Reduces vertices per feature for very large geometries.
        verbose: Whether to print debug output
        timeout: Per-request HTTP timeout in seconds (default 60). Increase for
            FeatureServer layers whose large/complex geometries take the server
            minutes to serialize a single page.

    Returns:
        PyArrow Table with WKB geometry column
    """
    configure_verbose(verbose)

    # Resolve and validate the requested output CRS before any network work
    # so a bad value fails fast with a clean error. "native" needs the layer
    # metadata and is resolved after get_layer_info below.
    output_wkid: int | None = None
    if output_crs is not None and output_crs != "native":
        try:
            output_wkid = _normalize_wkid(_parse_crs_to_wkid(output_crs))
        except ValueError as e:
            raise InvalidParameterError("output_crs", str(e)) from e
        # Parseable is not resolvable. A code no authority knows would be sent
        # as outSR and then have no PROJJSON to write, which used to mean the
        # file was written with no `crs` key -- i.e. claiming CRS84 (#1039).
        # Checked here so a multi-gigabyte download is not spent to reach it.
        if _projjson_from_wkid(output_wkid) is None:
            raise _unresolvable_output_crs({"wkid": output_wkid})

    # A generalization tolerance must be positive; reject bad values before any
    # network work so the failure is fast and clean.
    if max_allowable_offset is not None and max_allowable_offset <= 0:
        raise InvalidParameterError(
            "max_allowable_offset", "must be a positive number (units of the output CRS)."
        )

    # Split the column lists before any network work. A blank entry is rejected
    # here, not only by the Click callback: the Python API never goes through
    # Click, and ``include_cols="name,,pop"`` went to the server verbatim as
    # ``outFields=name,,pop``, with no local error at all (#980).
    include_list = split_column_list(include_cols, "--include-cols")
    exclude_list = split_column_list(exclude_cols, "--exclude-cols")

    # Validate URL
    service_url, layer_id = validate_arcgis_url(service_url)

    # Resolve authentication
    token = resolve_token(auth, service_url, verbose, timeout=timeout) if auth else None

    # Get layer info (with WHERE and bbox filters applied to count)
    layer_info = get_layer_info(
        service_url, token=token, where=where, bbox=bbox, verbose=verbose, timeout=timeout
    )
    debug(f"Layer: {layer_info.name}")
    debug(f"Geometry type: {layer_info.geometry_type}")
    debug(f"Total features matching filter: {layer_info.total_count}")

    # Check the requested names against the layer metadata just fetched, so a
    # typo fails here rather than being handed to the server (--include-cols) or
    # silently excluding nothing (--exclude-cols). ``_build_schema_from_layer_info``
    # builds the downloaded table as "geometry" plus these fields, which is why
    # --exclude-cols is checked against that column too.
    include_list, exclude_list = _resolve_field_selection(layer_info, include_list, exclude_list)

    # Resolve "native" to the layer's advertised SR
    if output_crs == "native":
        output_wkid = _resolve_native_wkid(layer_info.spatial_reference)

    if layer_info.total_count == 0:
        filters_applied = where != "1=1" or bbox is not None
        if filters_applied:
            filter_desc = []
            if where != "1=1":
                filter_desc.append(f"where='{where}'")
            if bbox:
                filter_desc.append(f"bbox={bbox}")
            warn(f"No features match filter: {', '.join(filter_desc)}")
        else:
            warn("Layer has no features")
        # Return empty table with geometry column
        return pa.table({"geometry": pa.array([], type=pa.binary())})

    # Determine outFields for server-side column selection
    out_fields = ",".join(include_list) if include_list else "*"
    if include_list:
        debug(f"Requesting fields: {out_fields}")

    # Pass 1: Stream features to temp parquet file (memory-efficient)
    temp_parquet = tempfile.gettempdir() + f"/arcgis_stream_{uuid.uuid4()}.parquet"

    try:
        progress("Streaming features to temp file...")
        total_rows, detected_sr = _stream_features_to_parquet(
            service_url=service_url,
            layer_info=layer_info,
            output_path=temp_parquet,
            where=where,
            bbox=bbox,
            out_fields=out_fields,
            max_features=limit,
            token=token,
            batch_size=batch_size,
            max_workers=max_workers,
            output_wkid=output_wkid,
            max_allowable_offset=max_allowable_offset,
            verbose=verbose,
            timeout=timeout,
        )

        if total_rows == 0:
            raise GeoParquetError("No features returned from service")

        # Pass 2: Read temp parquet file back as Arrow table
        progress("Reading temp file...")
        table = pq.read_table(temp_parquet)

        # Apply client-side column exclusion if specified
        if exclude_list:
            cols_to_exclude = set(exclude_list)
            # Keep geometry column unless explicitly excluded
            cols_to_keep = [name for name in table.column_names if name not in cols_to_exclude]
            if cols_to_keep:
                table = table.select(cols_to_keep)
                debug(f"Excluded columns: {cols_to_exclude}")

        # Repair invalid geometry (issue #506) before the geo block is built:
        # ST_MakeValid can change a geometry's type (a bowtie Polygon comes back
        # as a MultiPolygon), so the types have to be read off the repaired data
        # that is actually written. The helper preserves schema metadata, so it
        # is equally safe on either side of the block.
        if table.num_rows > 0:
            table, _ = repair_arrow_table_geometry(table, "geometry", repair=repair_geometry)

        # Add CRS to metadata.
        # Default (GeoJSON path): always CRS84 (WGS84 lon/lat) because we request
        # f=geojson, which per RFC 7946 is always WGS84 regardless of the native SR.
        # Native path (EsriJSON + outSR): tag the SR the server actually returned.
        if output_wkid is None:
            crs = parse_crs_string_to_projjson("OGC:CRS84")
        else:
            # returned_wkid is None when the server omitted spatialReference on
            # every page; we can't observe a mismatch in that case, so we skip the
            # warning and fall back to tagging with the requested WKID below.
            # Read through _wkid_candidates so a quoted code ("26918") is
            # compared as the integer it is, not reported as a mismatch with
            # itself.
            candidates = _wkid_candidates(detected_sr or {})
            returned_wkid = candidates[0] if candidates else None
            if returned_wkid is not None and returned_wkid != output_wkid:
                warn(
                    f"Requested output CRS WKID {output_wkid} but server returned "
                    f"WKID {returned_wkid}. Tagging output with the returned CRS "
                    f"(coordinates were not reprojected)."
                )
            returned_sr = detected_sr or {"wkid": output_wkid}
            crs = _extract_crs_from_spatial_reference(returned_sr)
            if crs is None:
                # Never fall through to a geo block with no `crs` key: that is
                # GeoParquet's spelling of OGC:CRS84, not of "unknown" (#1039).
                raise _unresolvable_output_crs(returned_sr)

        geo_metadata = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "crs": crs,
                    "geometry_types": _resolve_geometry_types(
                        table, layer_info.geometry_type, verbose
                    ),
                }
            },
        }

        # Update table schema with geo metadata
        existing_metadata = table.schema.metadata or {}
        new_metadata = {**existing_metadata, b"geo": json.dumps(geo_metadata).encode("utf-8")}
        table = table.replace_schema_metadata(new_metadata)

        success(f"Converted {table.num_rows} features")
        return table

    finally:
        # Clean up temp file (best-effort: on Windows pyarrow may still hold
        # the file handle after read_table, raising PermissionError on unlink)
        if os.path.exists(temp_parquet):
            try:
                os.unlink(temp_parquet)
            except OSError:
                debug(f"Could not remove temp file {temp_parquet}")


def convert_arcgis_to_geoparquet(
    service_url: str,
    output_file: str,
    token: str | None = None,
    token_file: str | None = None,
    username: str | None = None,
    password: str | None = None,
    portal_url: str | None = None,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    include_cols: str | None = None,
    exclude_cols: str | None = None,
    limit: int | None = None,
    output_crs: str | None = None,
    max_allowable_offset: float | None = None,
    skip_hilbert: bool = False,
    skip_bbox: bool = False,
    max_workers: int = 1,
    batch_size: int | None = None,
    compression: str = "ZSTD",
    compression_level: int = 15,
    verbose: bool = False,
    geoparquet_version: str | None = None,
    profile: str | None = None,
    row_group_size_mb: int | None = None,
    row_group_rows: int | None = None,
    overwrite: bool = False,
    repair_geometry: bool = True,
    timeout: float = DEFAULT_TIMEOUT,
) -> None:
    """
    Convert ArcGIS Feature Service to GeoParquet file.

    Main CLI entry point for ArcGIS to GeoParquet conversion.

    Server-side filtering options (pushed to ArcGIS for efficiency):
    - where: SQL WHERE clause
    - bbox: Spatial bounding box filter
    - include_cols: Select specific fields to download
    - limit: Maximum number of features to download

    Args:
        service_url: ArcGIS Feature Service URL
        output_file: Output file path (local or remote)
        token: Direct authentication token
        token_file: Path to file containing token
        username: ArcGIS username (requires password)
        password: ArcGIS password (requires username)
        portal_url: Enterprise portal URL for token generation
        where: SQL WHERE clause filter (pushed to server)
        bbox: Bounding box filter (xmin,ymin,xmax,ymax in WGS84, pushed to server)
        include_cols: Comma-separated columns to include (pushed to server)
        exclude_cols: Comma-separated columns to exclude (applied client-side)
        limit: Maximum number of features to return
        output_crs: Preserve native CRS. "native" uses the layer's advertised SR;
            or pass an EPSG code (e.g. "EPSG:25830"). Default None -> WGS84 (f=geojson).
        max_allowable_offset: Server-side geometry generalization tolerance, in
            output-CRS units (degrees on the default WGS84 path).
        skip_hilbert: Skip Hilbert spatial ordering
        skip_bbox: Skip adding bbox column for spatial query optimization
        max_workers: Number of concurrent requests (1 = sequential, 2-3 recommended)
        batch_size: Features per request (default: server's maxRecordCount, auto-reduces on error)
        compression: Compression codec (ZSTD, GZIP, etc.)
        compression_level: Compression level
        verbose: Whether to print verbose output
        geoparquet_version: GeoParquet version to write
        profile: AWS profile for S3 output
        row_group_size_mb: Row group size in MB (mutually exclusive with row_group_rows)
        row_group_rows: Row group size in number of rows (mutually exclusive with row_group_size_mb)
        timeout: Per-request HTTP timeout in seconds (default 60). Increase for
            layers with very large/complex geometries that the server is slow to
            serialize.
    """
    configure_verbose(verbose)

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, output_file)

    # Check if output file exists and overwrite is False
    if not overwrite and Path(output_file).exists():
        raise GeoParquetError(
            f"Output file already exists: {output_file}. Use --overwrite to replace it."
        )

    # Build auth config
    auth = None
    if any([token, token_file, username, password]):
        auth = ArcGISAuth(
            token=token,
            token_file=token_file,
            username=username,
            password=password,
            portal_url=portal_url,
        )

    # Convert to Arrow table with server-side filtering
    table = arcgis_to_table(
        service_url=service_url,
        auth=auth,
        where=where,
        bbox=bbox,
        include_cols=include_cols,
        exclude_cols=exclude_cols,
        limit=limit,
        batch_size=batch_size,
        max_workers=max_workers,
        output_crs=output_crs,
        max_allowable_offset=max_allowable_offset,
        verbose=verbose,
        repair_geometry=repair_geometry,
        timeout=timeout,
    )

    # Apply Hilbert ordering if not skipped
    if not skip_hilbert and table.num_rows > 0:
        progress("Applying Hilbert spatial ordering...")
        from geoparquet_io.core.hilbert_order import hilbert_order_table

        table = hilbert_order_table(table)

    # Add bbox column for spatial query optimization
    if not skip_bbox and table.num_rows > 0:
        progress("Adding bbox column for spatial query optimization...")
        from geoparquet_io.core.add.bbox import add_bbox_table

        table = add_bbox_table(table, bbox_column_name="bbox", geometry_column="geometry")

    # Write to GeoParquet
    progress(f"Writing to {output_file}...")
    write_geoparquet_table(
        table,
        output_file,
        geometry_column="geometry",
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        geoparquet_version=geoparquet_version,
        verbose=verbose,
        profile=profile,
    )

    success(f"Converted {table.num_rows} features to {output_file}")
