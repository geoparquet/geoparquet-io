#!/usr/bin/env python3

import click
import mercantile

from geoparquet_io.core.common import (
    check_bbox_structure,
    find_primary_geometry_column,
    get_crs_display_name,
    get_duckdb_connection,
    get_parquet_metadata,
    needs_httpfs,
    safe_file_url,
    setup_aws_profile_if_needed,
    validate_profile_for_urls,
    write_parquet_with_metadata,
)
from geoparquet_io.core.constants import DEFAULT_QUADKEY_COLUMN_NAME, DEFAULT_QUADKEY_RESOLUTION
from geoparquet_io.core.duckdb_metadata import get_column_names, get_geo_metadata
from geoparquet_io.core.logging_config import configure_verbose, debug, info, success, warn


def _is_geographic_crs(crs_info: dict | str | None) -> bool | None:
    """
    Check if CRS is geographic (lat/long) vs projected.

    Returns:
        True if geographic, False if projected, None if unknown
    """
    if crs_info is None:
        return None

    if isinstance(crs_info, str):
        crs_upper = crs_info.upper()
        # Common geographic CRS codes
        if any(
            code in crs_upper for code in ["4326", "CRS84", "CRS:84", "OGC:CRS84", "4269", "4267"]
        ):
            return True
        return None

    if isinstance(crs_info, dict):
        # Check PROJJSON type
        crs_type = crs_info.get("type", "")
        if crs_type == "GeographicCRS":
            return True
        if crs_type == "ProjectedCRS":
            return False

        # Check EPSG code
        crs_id = crs_info.get("id", {})
        if isinstance(crs_id, dict):
            code = crs_id.get("code")
            if code in [4326, 4269, 4267]:  # Common geographic codes
                return True

    return None


def _validate_crs_for_quadkey(input_parquet: str, geom_col: str, verbose: bool) -> None:
    """
    Validate that the file's CRS is geographic (WGS84/CRS84).

    Quadkeys require lat/lon coordinates. Raises ClickException if CRS is projected.
    """
    safe_url = safe_file_url(input_parquet, verbose=False)

    # Get CRS from GeoParquet metadata
    geo_meta = get_geo_metadata(safe_url)
    if not geo_meta:
        # No metadata - assume WGS84 (common default)
        if verbose:
            debug("No GeoParquet metadata found, assuming WGS84 coordinates")
        return

    columns_meta = geo_meta.get("columns", {})
    if geom_col not in columns_meta:
        if verbose:
            debug(f"Geometry column '{geom_col}' not found in metadata, assuming WGS84")
        return

    crs_info = columns_meta[geom_col].get("crs")

    # No CRS specified means default (WGS84)
    if crs_info is None:
        if verbose:
            debug("No CRS specified in metadata, using default WGS84")
        return

    is_geographic = _is_geographic_crs(crs_info)

    if is_geographic is False:
        crs_name = get_crs_display_name(crs_info)
        raise click.ClickException(
            f"Quadkeys require geographic coordinates (lat/lon), but this file uses "
            f"a projected CRS: {crs_name}\n\n"
            f"To fix this, reproject to WGS84 first:\n"
            f"  gpio reproject {input_parquet} reprojected.parquet --dst-crs EPSG:4326\n\n"
            f"Then run the quadkey command on the reprojected file."
        )

    if verbose and is_geographic:
        debug("CRS validated as geographic (lat/lon coordinates)")


def _lat_lon_to_quadkey(lat: float, lon: float, level: int) -> str:
    """Convert latitude and longitude to a quadkey string using mercantile."""
    tile = mercantile.tile(lon, lat, level)
    return mercantile.quadkey(tile)


def add_quadkey_column(
    input_parquet,
    output_parquet,
    quadkey_column_name=DEFAULT_QUADKEY_COLUMN_NAME,
    resolution=DEFAULT_QUADKEY_RESOLUTION,
    use_centroid=False,
    dry_run=False,
    verbose=False,
    compression="ZSTD",
    compression_level=None,
    row_group_size_mb=None,
    row_group_rows=None,
    profile=None,
    geoparquet_version=None,
):
    """
    Add a quadkey column to a GeoParquet file.

    Computes quadkey tile IDs based on geometry location. By default, uses the
    bbox column midpoint if available, otherwise falls back to geometry centroid.

    Args:
        input_parquet: Path to the input parquet file (local or remote URL)
        output_parquet: Path to the output parquet file (local or remote URL)
        quadkey_column_name: Name for the quadkey column (default: 'quadkey')
        resolution: Quadkey zoom level (0-23). Default: 13
        use_centroid: Force using geometry centroid even if bbox exists
        dry_run: Whether to print SQL commands without executing them
        verbose: Whether to print verbose output
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
    """
    configure_verbose(verbose)

    # Validate resolution
    if not 0 <= resolution <= 23:
        raise click.BadParameter(f"Resolution must be between 0 and 23, got {resolution}")

    # Validate profile is only used with S3
    validate_profile_for_urls(profile, input_parquet, output_parquet)

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, input_parquet, output_parquet)

    # Get safe URL for input file
    input_url = safe_file_url(input_parquet, verbose)

    # Get geometry column
    geom_col = find_primary_geometry_column(input_parquet, verbose)

    # Validate CRS is geographic (quadkeys require lat/lon)
    _validate_crs_for_quadkey(input_parquet, geom_col, verbose)

    # Check if column already exists (skip in dry-run)
    if not dry_run:
        column_names = get_column_names(input_url)
        if quadkey_column_name in column_names:
            raise click.ClickException(
                f"Column '{quadkey_column_name}' already exists in the file. "
                f"Please choose a different name."
            )

    # Determine whether to use bbox or centroid
    use_bbox = False
    if not use_centroid:
        bbox_info = check_bbox_structure(input_parquet, verbose)
        if bbox_info["has_bbox_column"]:
            use_bbox = True
            bbox_col = bbox_info["bbox_column_name"]
            if verbose:
                debug(f"Using bbox column '{bbox_col}' for quadkey calculation")
        else:
            warn("No bbox column found - using geometry centroid for quadkey calculation")
            info("Tip: Add a bbox column with 'gpio add bbox' for faster computation")

    # Dry-run mode header
    if dry_run:
        warn("\n=== DRY RUN MODE - SQL Commands that would be executed ===\n")
        info(f"-- Input file: {input_url}")
        info(f"-- Output file: {output_parquet}")
        info(f"-- Geometry column: {geom_col}")
        info(f"-- New column: {quadkey_column_name}")
        info(f"-- Resolution (zoom level): {resolution}")
        method = "bbox midpoint" if use_bbox else "geometry centroid"
        info(f"-- Calculation method: {method}")
        return

    # Get metadata before processing
    metadata, _ = get_parquet_metadata(input_parquet, verbose)

    if verbose:
        debug(f"Adding quadkey column '{quadkey_column_name}' at resolution {resolution}...")

    # Create DuckDB connection with httpfs if needed
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_parquet))

    try:
        # Register Python UDF for quadkey generation
        con.create_function(
            "lat_lon_to_quadkey",
            _lat_lon_to_quadkey,
            ["DOUBLE", "DOUBLE", "INTEGER"],
            "VARCHAR",
        )

        # Build the SQL expression based on calculation method
        if use_bbox:
            lat_expr = f"(({bbox_col}.ymin + {bbox_col}.ymax) / 2.0)"
            lon_expr = f"(({bbox_col}.xmin + {bbox_col}.xmax) / 2.0)"
        else:
            lat_expr = f"ST_Y(ST_Centroid({geom_col}))"
            lon_expr = f"ST_X(ST_Centroid({geom_col}))"

        # Build SELECT query with new column
        query = f"""
            SELECT *,
                   lat_lon_to_quadkey({lat_expr}, {lon_expr}, {resolution}) AS {quadkey_column_name}
            FROM '{input_url}'
        """

        if verbose:
            debug(f"Query: {query}")

        # Prepare quadkey metadata for GeoParquet spec
        quadkey_metadata = {
            "covering": {"quadkey": {"column": quadkey_column_name, "resolution": resolution}}
        }

        # Write output with metadata
        write_parquet_with_metadata(
            con,
            query,
            output_parquet,
            original_metadata=metadata,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            verbose=verbose,
            profile=profile,
            geoparquet_version=geoparquet_version,
            custom_metadata=quadkey_metadata,
        )

        success(
            f"Successfully added quadkey column '{quadkey_column_name}' "
            f"(zoom level {resolution}) to: {output_parquet}"
        )

    finally:
        con.close()


if __name__ == "__main__":
    add_quadkey_column()
