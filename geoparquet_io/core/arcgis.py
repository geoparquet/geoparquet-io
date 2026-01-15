"""
ArcGIS Feature Service to GeoParquet conversion.

This module provides functionality to download features from ArcGIS REST API
endpoints (FeatureServer/MapServer) and convert them to GeoParquet format.
"""

from __future__ import annotations

import json
import time
import urllib.parse
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generator

import click
import pyarrow as pa

from geoparquet_io.core.common import (
    get_duckdb_connection,
    parse_crs_string_to_projjson,
    setup_aws_profile_if_needed,
    write_geoparquet_table,
)
from geoparquet_io.core.logging_config import configure_verbose, debug, progress, success, warn

if TYPE_CHECKING:
    pass

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
    except ImportError:
        raise click.ClickException(
            "httpx is required for ArcGIS conversion. Install with: pip install httpx"
        )


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
            if status == 401:
                raise click.ClickException(
                    "Authentication required. Use --token or --username/--password."
                )
            elif status == 403:
                raise click.ClickException(
                    "Access denied. Check your credentials and service permissions."
                )
            elif status == 404:
                raise click.ClickException(f"Service not found (404). Check the URL: {url}")
            raise click.ClickException(f"HTTP error {status}: {e}")

    raise click.ClickException(f"Request failed after {max_retries} attempts: {last_exception}")


def _handle_arcgis_response(data: dict, context: str) -> dict:
    """Handle ArcGIS REST API response and check for errors."""
    if "error" in data:
        error = data["error"]
        code = error.get("code", "Unknown")
        message = error.get("message", "Unknown error")
        details = error.get("details", [])

        if code in (498, 499):
            raise click.ClickException(f"{context}: Invalid or expired token. Please re-authenticate.")
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
            with open(auth.token_file) as f:
                return f.read().strip()
        except OSError as e:
            raise click.ClickException(f"Failed to read token file: {e}")

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
    verbose: bool = False,
) -> ArcGISLayerInfo:
    """
    Fetch layer metadata from ArcGIS REST service.

    Args:
        service_url: Full layer URL (e.g., .../FeatureServer/0)
        token: Optional authentication token
        verbose: Whether to print debug output

    Returns:
        ArcGISLayerInfo with layer metadata
    """
    if verbose:
        debug(f"Fetching layer info from {service_url}")

    params = _add_token_to_params({"f": "json"}, token)
    data = _make_request("GET", service_url, params=params)
    data = _handle_arcgis_response(data, "Layer info")

    # Get feature count
    count = get_feature_count(service_url, token=token, verbose=verbose)

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
    token: str | None = None,
    verbose: bool = False,
) -> int:
    """
    Get total feature count from ArcGIS service.

    Args:
        service_url: Full layer URL
        where: WHERE clause filter
        token: Optional authentication token
        verbose: Whether to print debug output

    Returns:
        Feature count
    """
    query_url = f"{service_url}/query"
    params = _add_token_to_params(
        {
            "where": where,
            "returnCountOnly": "true",
            "f": "json",
        },
        token,
    )

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
        token: Optional authentication token
        verbose: Whether to print debug output

    Returns:
        GeoJSON FeatureCollection dict
    """
    query_url = f"{service_url}/query"
    params = _add_token_to_params(
        {
            "where": where,
            "outFields": "*",
            "returnGeometry": "true",
            "f": "geojson",
            "resultOffset": str(offset),
            "resultRecordCount": str(limit),
        },
        token,
    )

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

    total = layer_info.total_count
    offset = 0
    fetched = 0

    while offset < total:
        end = min(offset + max_batch, total)
        progress(f"Fetching features {offset + 1}-{end} of {total}...")

        page = fetch_features_page(
            service_url, offset, max_batch, where, token=token, verbose=verbose
        )

        features = page.get("features", [])
        if not features:
            break

        yield page

        fetched += len(features)
        offset += max_batch

        # Safety check: if server returned fewer than expected, adjust
        if len(features) < max_batch and offset < total:
            offset = fetched

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


def _geojson_features_to_table(
    features: list[dict],
    verbose: bool = False,
) -> pa.Table:
    """
    Convert GeoJSON features to PyArrow Table with WKB geometry.

    Uses DuckDB's spatial extension for geometry conversion.

    Args:
        features: List of GeoJSON feature dicts
        verbose: Whether to print debug output

    Returns:
        PyArrow Table with WKB geometry column
    """
    if not features:
        raise click.ClickException("No features to convert")

    # Create a temporary GeoJSON string for DuckDB to parse
    geojson_collection = json.dumps({
        "type": "FeatureCollection",
        "features": features,
    })

    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)

    try:
        # Use DuckDB's ST_Read to parse GeoJSON and convert geometry to WKB
        # Write to temp file because ST_Read expects a file path
        import tempfile
        import uuid

        temp_file = tempfile.gettempdir() + f"/arcgis_temp_{uuid.uuid4()}.geojson"

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
            import os
            if os.path.exists(temp_file):
                os.unlink(temp_file)

    finally:
        con.close()


def arcgis_to_table(
    service_url: str,
    auth: ArcGISAuth | None = None,
    where: str = "1=1",
    batch_size: int | None = None,
    verbose: bool = False,
) -> pa.Table:
    """
    Convert ArcGIS Feature Service to PyArrow Table.

    Main function for converting ArcGIS services to in-memory Arrow tables.

    Args:
        service_url: ArcGIS Feature Service URL (with layer ID)
        auth: Optional authentication configuration
        where: SQL WHERE clause filter
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

    # Get layer info
    layer_info = get_layer_info(service_url, token, verbose)
    debug(f"Layer: {layer_info.name}")
    debug(f"Geometry type: {layer_info.geometry_type}")
    debug(f"Total features: {layer_info.total_count}")

    if layer_info.total_count == 0:
        warn("Layer has no features")
        # Return empty table with geometry column
        return pa.table({"geometry": pa.array([], type=pa.binary())})

    # Fetch all features (paginated)
    all_features = []
    for page in fetch_all_features(
        service_url, layer_info, where, token, batch_size, verbose
    ):
        all_features.extend(page.get("features", []))

    if not all_features:
        raise click.ClickException("No features returned from service")

    # Convert to Arrow table
    progress("Converting to Arrow table...")
    table = _geojson_features_to_table(all_features, verbose)

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
                    "geometry_types": [ARCGIS_GEOM_TYPES.get(layer_info.geometry_type, "Geometry")],
                }
            },
        }

        # Update table schema with geo metadata
        existing_metadata = table.schema.metadata or {}
        new_metadata = {**existing_metadata, b"geo": json.dumps(geo_metadata).encode("utf-8")}
        table = table.replace_schema_metadata(new_metadata)

    success(f"Converted {table.num_rows} features")
    return table


def convert_arcgis_to_geoparquet(
    service_url: str,
    output_file: str,
    token: str | None = None,
    token_file: str | None = None,
    username: str | None = None,
    password: str | None = None,
    portal_url: str | None = None,
    where: str = "1=1",
    skip_hilbert: bool = False,
    compression: str = "ZSTD",
    compression_level: int = 15,
    verbose: bool = False,
    geoparquet_version: str | None = None,
    profile: str | None = None,
) -> None:
    """
    Convert ArcGIS Feature Service to GeoParquet file.

    Main CLI entry point for ArcGIS to GeoParquet conversion.

    Args:
        service_url: ArcGIS Feature Service URL
        output_file: Output file path (local or remote)
        token: Direct authentication token
        token_file: Path to file containing token
        username: ArcGIS username (requires password)
        password: ArcGIS password (requires username)
        portal_url: Enterprise portal URL for token generation
        where: SQL WHERE clause filter
        skip_hilbert: Skip Hilbert spatial ordering
        compression: Compression codec (ZSTD, GZIP, etc.)
        compression_level: Compression level
        verbose: Whether to print verbose output
        geoparquet_version: GeoParquet version to write
        profile: AWS profile for S3 output
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

    # Convert to Arrow table
    table = arcgis_to_table(
        service_url=service_url,
        auth=auth,
        where=where,
        verbose=verbose,
    )

    # Apply Hilbert ordering if not skipped
    if not skip_hilbert and table.num_rows > 0:
        progress("Applying Hilbert spatial ordering...")
        from geoparquet_io.core.hilbert_order import hilbert_order_table

        table = hilbert_order_table(table)

    # Write to GeoParquet
    progress(f"Writing to {output_file}...")
    write_geoparquet_table(
        table,
        output_file,
        geometry_column="geometry",
        compression=compression,
        compression_level=compression_level,
        geoparquet_version=geoparquet_version,
        verbose=verbose,
        profile=profile,
    )

    success(f"Converted {table.num_rows} features to {output_file}")
