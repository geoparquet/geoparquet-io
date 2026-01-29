"""
ArcGIS Feature Service to GeoParquet conversion.

This module provides functionality to download features from ArcGIS REST API
endpoints (FeatureServer/MapServer) and convert them to GeoParquet format.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
import uuid
from collections.abc import Generator
from dataclasses import dataclass

import click
import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.common import (
    get_duckdb_connection,
    parse_crs_string_to_projjson,
    setup_aws_profile_if_needed,
    write_geoparquet_table,
)
from geoparquet_io.core.logging_config import configure_verbose, debug, progress, success, warn

# ArcGIS Online token endpoint
ARCGIS_ONLINE_TOKEN_URL = "https://www.arcgis.com/sharing/rest/generateToken"

# Default page size for feature downloads (ArcGIS typical max is 2000)
DEFAULT_PAGE_SIZE = 2000

# Map ArcGIS WKID codes to EPSG codes for special cases
WKID_TO_EPSG = {
    102100: 3857,  # Web Mercator
    102113: 3785,  # Legacy Web Mercator
}

# Map ArcGIS geometry types to GeoJSON types
ARCGIS_GEOM_TYPES = {
    "esriGeometryPoint": "Point",
    "esriGeometryMultipoint": "MultiPoint",
    "esriGeometryPolyline": "MultiLineString",
    "esriGeometryPolygon": "MultiPolygon",
    "esriGeometryEnvelope": "Polygon",
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


def _get_http_client():
    """Get HTTP client for making requests."""
    try:
        import httpx

        return httpx.Client(timeout=60.0, follow_redirects=True)
    except ImportError as e:
        raise click.ClickException(
            "httpx is required for ArcGIS conversion. Install with: pip install httpx"
        ) from e


def _make_request(
    method: str,
    url: str,
    params: dict | None = None,
    data: dict | None = None,
    max_retries: int = 3,
    retry_delay: float = 1.0,
) -> dict:
    """Make HTTP request with retry logic."""
    import httpx

    last_exception = None

    for attempt in range(max_retries):
        try:
            with _get_http_client() as client:
                if method == "GET":
                    response = client.get(url, params=params)
                else:
                    response = client.post(url, data=data)
                response.raise_for_status()
                return response.json()
        except httpx.TimeoutException as e:
            last_exception = e
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
        except httpx.NetworkError as e:
            last_exception = e
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            # Retry on 429 (rate limited) or 5xx (server errors)
            if status == 429 or (500 <= status < 600):
                last_exception = e
                if attempt < max_retries - 1:
                    # Honor Retry-After header if present
                    retry_after = e.response.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        delay = float(retry_after)
                    else:
                        delay = retry_delay * (attempt + 1)
                    time.sleep(delay)
                    continue
            elif status == 401:
                raise click.ClickException(
                    "Authentication required. Use --token or --username/--password."
                ) from None
            elif status == 403:
                raise click.ClickException(
                    "Access denied. Check your credentials and service permissions."
                ) from None
            elif status == 404:
                raise click.ClickException(
                    f"Service not found (404). Check the URL: {url}"
                ) from None
            raise click.ClickException(f"HTTP error {status}: {e}") from e

    raise click.ClickException(f"Request failed after {max_retries} attempts: {last_exception}")


def _handle_arcgis_response(data: dict, context: str) -> dict:
    """Handle ArcGIS REST API response and check for errors."""
    if "error" in data:
        error = data["error"]
        code = error.get("code", "Unknown")
        message = error.get("message", "Unknown error")
        details = error.get("details", [])

        if code in (498, 499):
            raise click.ClickException(
                f"{context}: Invalid or expired token. Please re-authenticate."
            )
        else:
            detail_str = "; ".join(details) if details else ""
            raise click.ClickException(f"{context}: Error {code} - {message}. {detail_str}")

    return data


def generate_token(
    username: str,
    password: str,
    portal_url: str | None = None,
    verbose: bool = False,
) -> str:
    """
    Generate authentication token via ArcGIS REST API.

    Args:
        username: ArcGIS username
        password: ArcGIS password
        portal_url: Enterprise portal URL (default: ArcGIS Online)
        verbose: Whether to print debug output

    Returns:
        Authentication token string

    Raises:
        click.ClickException: If token generation fails
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

    result = _make_request("POST", token_url, data=data)
    result = _handle_arcgis_response(result, "Token generation")

    if "token" not in result:
        raise click.ClickException("Token generation failed: no token in response")

    if verbose:
        debug("Token generated successfully")

    return result["token"]


def resolve_token(
    auth: ArcGISAuth,
    service_url: str,
    verbose: bool = False,
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
            raise click.ClickException(f"Failed to read token file: {e}") from e

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

        return generate_token(auth.username, auth.password, portal_url, verbose)

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
        click.ClickException: If URL is invalid
    """
    import re

    url = url.rstrip("/")

    # Check for ImageServer (raster - not supported)
    if "/ImageServer" in url:
        raise click.ClickException(
            f"ImageServer (raster) services are not supported: {url}\n"
            "This command only supports vector services (FeatureServer or MapServer).\n"
            "ImageServer provides raster/imagery data which cannot be converted to GeoParquet."
        )

    # Check for FeatureServer or MapServer
    if "/FeatureServer" not in url and "/MapServer" not in url:
        raise click.ClickException(
            f"Invalid ArcGIS URL: {url}\n\n"
            "Expected format: https://services.arcgis.com/.../FeatureServer/0\n\n"
            "The URL must point to a vector layer in a FeatureServer or MapServer.\n"
            "Make sure the URL includes:\n"
            "  - /FeatureServer/ or /MapServer/ in the path\n"
            "  - A layer ID at the end (e.g., /0, /1, /2)"
        )

    # Extract layer ID
    match = re.search(r"/(FeatureServer|MapServer)/(\d+)$", url)
    if match:
        return url, int(match.group(2))

    # URL ends with FeatureServer or MapServer without layer ID
    raise click.ClickException(
        f"Missing layer ID in URL: {url}\n\n"
        f"You must specify which layer to download by adding the layer ID.\n"
        f"For example: {url}/0\n\n"
        f"To see available layers, open this URL in a browser:\n"
        f"  {url}?f=json"
    )


def get_layer_info(
    service_url: str,
    token: str | None = None,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    verbose: bool = False,
) -> ArcGISLayerInfo:
    """
    Fetch layer metadata from ArcGIS REST service.

    Args:
        service_url: Full layer URL (e.g., .../FeatureServer/0)
        token: Optional authentication token
        where: SQL WHERE clause for counting features (default: "1=1" = all)
        bbox: Bounding box filter (xmin, ymin, xmax, ymax) in WGS84
        verbose: Whether to print debug output

    Returns:
        ArcGISLayerInfo with layer metadata
    """
    if verbose:
        debug(f"Fetching layer info from {service_url}")

    params = _add_token_to_params({"f": "json"}, token)
    data = _make_request("GET", service_url, params=params)
    data = _handle_arcgis_response(data, "Layer info")

    # Get feature count (using the WHERE and bbox filters)
    count = get_feature_count(service_url, where=where, bbox=bbox, token=token, verbose=verbose)

    return ArcGISLayerInfo(
        name=data.get("name", "Unknown"),
        geometry_type=data.get("geometryType", "esriGeometryPoint"),
        spatial_reference=data.get("spatialReference", {"wkid": 4326}),
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
) -> int:
    """
    Get total feature count from ArcGIS service.

    Args:
        service_url: Full layer URL
        where: WHERE clause filter
        bbox: Bounding box filter (xmin, ymin, xmax, ymax) in WGS84
        token: Optional authentication token
        verbose: Whether to print debug output

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

    data = _make_request("GET", query_url, params=params)
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
    verbose: bool = False,
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
        verbose: Whether to print debug output

    Returns:
        GeoJSON FeatureCollection dict
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

    # Add bbox filter if provided (spatial query)
    if bbox:
        xmin, ymin, xmax, ymax = bbox
        params["geometry"] = f"{xmin},{ymin},{xmax},{ymax}"
        params["geometryType"] = "esriGeometryEnvelope"
        params["spatialRel"] = "esriSpatialRelIntersects"
        params["inSR"] = "4326"  # WGS84

    params = _add_token_to_params(params, token)

    data = _make_request("GET", query_url, params=params)

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
    verbose: bool = False,
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
        verbose: Whether to print debug output

    Yields:
        GeoJSON FeatureCollection dicts for each page
    """
    # Determine batch size (respect server limit)
    max_batch = min(
        batch_size or DEFAULT_PAGE_SIZE,
        layer_info.max_record_count or DEFAULT_PAGE_SIZE,
    )

    # Apply user limit to total
    total = layer_info.total_count
    if max_features is not None:
        total = min(total, max_features)

    offset = 0
    fetched = 0

    while offset < total:
        # Adjust batch size for last page if limit applies
        remaining = total - offset
        current_batch = min(max_batch, remaining)

        end = min(offset + current_batch, total)
        progress(f"Fetching features {offset + 1}-{end} of {total}...")

        page = fetch_features_page(
            service_url,
            offset,
            current_batch,
            where,
            bbox=bbox,
            out_fields=out_fields,
            token=token,
            verbose=verbose,
        )

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


def _extract_crs_from_spatial_reference(spatial_ref: dict) -> dict | None:
    """Extract CRS as PROJJSON from ArcGIS spatial reference."""
    # ArcGIS uses WKID (Well-Known ID) which maps to EPSG codes
    wkid = spatial_ref.get("wkid") or spatial_ref.get("latestWkid")

    if wkid:
        # Handle special WKIDs
        epsg_code = WKID_TO_EPSG.get(wkid, wkid)
        return parse_crs_string_to_projjson(f"EPSG:{epsg_code}")

    # Fall back to WKT if provided
    wkt = spatial_ref.get("wkt")
    if wkt:
        return parse_crs_string_to_projjson(wkt)

    # Default to WGS84
    return parse_crs_string_to_projjson("EPSG:4326")


def _geojson_page_to_table(
    features: list[dict],
) -> pa.Table | None:
    """
    Convert a page of GeoJSON features to PyArrow Table with WKB geometry.

    Uses DuckDB's spatial extension for geometry conversion.
    This function is designed to handle a single page (~2000 features)
    to keep memory usage low.

    Args:
        features: List of GeoJSON feature dicts (typically one page)

    Returns:
        PyArrow Table with WKB geometry column, or None if no features
    """
    if not features:
        return None

    # Create a temporary GeoJSON string for DuckDB to parse
    geojson_collection = json.dumps(
        {
            "type": "FeatureCollection",
            "features": features,
        }
    )

    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    temp_file = tempfile.gettempdir() + f"/arcgis_page_{uuid.uuid4()}.geojson"

    try:
        with open(temp_file, "w") as f:
            f.write(geojson_collection)

        # Read GeoJSON and convert geometry to WKB
        query = f"""
            SELECT
                ST_AsWKB(geom) as geometry,
                * EXCLUDE (geom)
            FROM ST_Read('{temp_file}')
        """

        table = con.execute(query).fetch_arrow_table()
        return table

    finally:
        con.close()
        if os.path.exists(temp_file):
            os.unlink(temp_file)


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
    verbose: bool = False,
) -> int:
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
        verbose: Whether to print debug output

    Returns:
        Number of features written
    """
    writer = None
    total_rows = 0
    page_count = 0

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
            verbose=verbose,
        ):
            features = page.get("features", [])
            if not features:
                continue

            # Convert this page to Arrow table
            page_table = _geojson_page_to_table(features)
            if page_table is None:
                continue

            page_count += 1

            # Initialize writer with schema from first page
            if writer is None:
                writer = pq.ParquetWriter(output_path, page_table.schema)

            # Write this page
            writer.write_table(page_table)
            total_rows += page_table.num_rows

            # Free memory from this page
            del page_table

        debug(f"Streamed {total_rows} features in {page_count} pages to temp file")
        return total_rows

    finally:
        if writer is not None:
            writer.close()


def arcgis_to_table(
    service_url: str,
    auth: ArcGISAuth | None = None,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    include_cols: str | None = None,
    exclude_cols: str | None = None,
    limit: int | None = None,
    batch_size: int | None = None,
    verbose: bool = False,
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
        verbose: Whether to print debug output

    Returns:
        PyArrow Table with WKB geometry column
    """
    configure_verbose(verbose)

    # Validate URL
    service_url, layer_id = validate_arcgis_url(service_url)

    # Resolve authentication
    token = resolve_token(auth, service_url, verbose) if auth else None

    # Get layer info (with WHERE and bbox filters applied to count)
    layer_info = get_layer_info(service_url, token=token, where=where, bbox=bbox, verbose=verbose)
    debug(f"Layer: {layer_info.name}")
    debug(f"Geometry type: {layer_info.geometry_type}")
    debug(f"Total features matching filter: {layer_info.total_count}")

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
    out_fields = "*"
    if include_cols:
        # Always include geometry-related fields
        fields = [f.strip() for f in include_cols.split(",")]
        out_fields = ",".join(fields)
        debug(f"Requesting fields: {out_fields}")

    # Pass 1: Stream features to temp parquet file (memory-efficient)
    temp_parquet = tempfile.gettempdir() + f"/arcgis_stream_{uuid.uuid4()}.parquet"

    try:
        progress("Streaming features to temp file...")
        total_rows = _stream_features_to_parquet(
            service_url=service_url,
            layer_info=layer_info,
            output_path=temp_parquet,
            where=where,
            bbox=bbox,
            out_fields=out_fields,
            max_features=limit,
            token=token,
            batch_size=batch_size,
            verbose=verbose,
        )

        if total_rows == 0:
            raise click.ClickException("No features returned from service")

        # Pass 2: Read temp parquet file back as Arrow table
        progress("Reading temp file...")
        table = pq.read_table(temp_parquet)

        # Apply client-side column exclusion if specified
        if exclude_cols:
            cols_to_exclude = {c.strip() for c in exclude_cols.split(",")}
            # Keep geometry column unless explicitly excluded
            cols_to_keep = [name for name in table.column_names if name not in cols_to_exclude]
            if cols_to_keep:
                table = table.select(cols_to_keep)
                debug(f"Excluded columns: {cols_to_exclude}")

        # Add CRS to metadata
        crs = _extract_crs_from_spatial_reference(layer_info.spatial_reference)
        if crs:
            geo_metadata = {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {
                    "geometry": {
                        "encoding": "WKB",
                        "crs": crs,
                        "geometry_types": [
                            ARCGIS_GEOM_TYPES.get(layer_info.geometry_type, "Geometry")
                        ],
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
        # Clean up temp file
        if os.path.exists(temp_parquet):
            os.unlink(temp_parquet)


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
    skip_hilbert: bool = False,
    skip_bbox: bool = False,
    compression: str = "ZSTD",
    compression_level: int = 15,
    verbose: bool = False,
    geoparquet_version: str | None = None,
    profile: str | None = None,
    row_group_size_mb: int | None = None,
    row_group_rows: int | None = None,
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
        skip_hilbert: Skip Hilbert spatial ordering
        skip_bbox: Skip adding bbox column for spatial query optimization
        compression: Compression codec (ZSTD, GZIP, etc.)
        compression_level: Compression level
        verbose: Whether to print verbose output
        geoparquet_version: GeoParquet version to write
        profile: AWS profile for S3 output
        row_group_size_mb: Row group size in MB (mutually exclusive with row_group_rows)
        row_group_rows: Row group size in number of rows (mutually exclusive with row_group_size_mb)
    """
    configure_verbose(verbose)

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, output_file)

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
        verbose=verbose,
    )

    # Apply Hilbert ordering if not skipped
    if not skip_hilbert and table.num_rows > 0:
        progress("Applying Hilbert spatial ordering...")
        from geoparquet_io.core.hilbert_order import hilbert_order_table

        table = hilbert_order_table(table)

    # Add bbox column for spatial query optimization
    if not skip_bbox and table.num_rows > 0:
        progress("Adding bbox column for spatial query optimization...")
        from geoparquet_io.core.add_bbox_column import add_bbox_table

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
