#!/usr/bin/env python3

import os

import click
import duckdb

from geoparquet_io.core.common import (
    add_bbox,
    check_bbox_structure,
    find_primary_geometry_column,
    get_dataset_bounds,
    get_duckdb_connection,
    get_parquet_metadata,
    get_remote_error_hint,
    is_remote_url,
    needs_httpfs,
    safe_file_url,
    setup_aws_profile_if_needed,
    show_remote_read_message,
    validate_profile_for_urls,
    write_parquet_with_metadata,
)
from geoparquet_io.core.logging_config import debug, info, success, warn
from geoparquet_io.core.partition_reader import require_single_file


def _prepare_working_file(input_parquet, add_bbox_flag, verbose):
    """Prepare working file, adding bbox if needed.

    Returns:
        tuple: (working_parquet, temp_file_created, temp_file_path_or_none)
    """
    import shutil
    import tempfile

    input_bbox_info = check_bbox_structure(input_parquet, verbose)

    if add_bbox_flag and not input_bbox_info["has_bbox_column"]:
        info("\nAdding bbox column to enable fast bounds calculation...")
        temp_fd, temp_file = tempfile.mkstemp(suffix=".parquet")
        os.close(temp_fd)
        shutil.copy2(input_parquet, temp_file)
        add_bbox(temp_file, "bbox", verbose)
        success("Added bbox column for optimized processing")
        return temp_file, True, temp_file

    if input_bbox_info["status"] != "optimal":
        warn(
            "\nWarning: Input file could benefit from bbox optimization:\n"
            + input_bbox_info["message"]
        )
        if not add_bbox_flag:
            info("Tip: Run this command with --add-bbox to enable fast bounds calculation")

    return input_parquet, False, None


def _cleanup_temp_file(temp_file, verbose):
    """Clean up temporary file if it exists."""
    if temp_file and os.path.exists(temp_file):
        try:
            os.remove(temp_file)
            if verbose:
                debug("Cleaned up temporary file")
        except OSError as e:
            if verbose:
                warn(f"Warning: Could not remove temporary file: {e}")


def hilbert_order(
    input_parquet,
    output_parquet,
    geometry_column="geometry",
    add_bbox_flag=False,
    verbose=False,
    compression="ZSTD",
    compression_level=None,
    row_group_size_mb=None,
    row_group_rows=None,
    profile=None,
    geoparquet_version=None,
):
    """
    Reorder a GeoParquet file using Hilbert curve ordering.

    Args:
        input_parquet: Path to input GeoParquet file (local or remote URL)
        output_parquet: Path to output file (local or remote URL)
        geometry_column: Name of geometry column (default: 'geometry')
        add_bbox_flag: Add bbox column before sorting if not present
        verbose: Print verbose output
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
    """
    # Check for partition input (not supported)
    require_single_file(input_parquet, "sort hilbert")

    working_parquet, temp_file_created, temp_file = _prepare_working_file(
        input_parquet, add_bbox_flag, verbose
    )

    validate_profile_for_urls(profile, input_parquet, output_parquet)
    setup_aws_profile_if_needed(profile, input_parquet, output_parquet)
    show_remote_read_message(working_parquet, verbose)

    safe_url = safe_file_url(working_parquet, verbose)
    metadata, _ = get_parquet_metadata(input_parquet, verbose)

    if geometry_column == "geometry":
        geometry_column = find_primary_geometry_column(working_parquet, verbose)
    if verbose:
        debug(f"Using geometry column: {geometry_column}")

    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(working_parquet))

    if verbose:
        debug("Calculating dataset bounds for Hilbert ordering...")

    bounds = get_dataset_bounds(working_parquet, geometry_column, verbose=verbose)
    if not bounds:
        raise click.ClickException("Could not calculate dataset bounds")

    xmin, ymin, xmax, ymax = bounds
    if verbose:
        debug(f"Dataset bounds: ({xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f})")
        debug("Reordering data using Hilbert curve...")

    order_query = f"""
        SELECT * FROM '{safe_url}'
        ORDER BY ST_Hilbert({geometry_column},
            ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax})))
    """

    try:
        write_parquet_with_metadata(
            con,
            order_query,
            output_parquet,
            original_metadata=metadata,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            verbose=verbose,
            profile=profile,
            geoparquet_version=geoparquet_version,
        )
        if verbose:
            debug("Hilbert ordering completed successfully")
        if add_bbox_flag and temp_file_created:
            success("Output includes bbox column and metadata for optimal performance")
        if verbose:
            debug(f"Successfully wrote ordered data to: {output_parquet}")
    except duckdb.IOException as e:
        con.close()
        if is_remote_url(input_parquet):
            hints = get_remote_error_hint(str(e), input_parquet)
            raise click.ClickException(
                f"Failed to read remote file.\n\n{hints}\n\nOriginal error: {str(e)}"
            ) from e
        raise
    finally:
        _cleanup_temp_file(temp_file, verbose)


if __name__ == "__main__":
    hilbert_order()
