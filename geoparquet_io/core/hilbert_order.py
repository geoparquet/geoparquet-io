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

    Takes an input GeoParquet file and creates a new file with rows ordered
    by their position along a Hilbert space-filling curve. Applies best practices:
    - Configurable compression (default ZSTD)
    - Configurable row group sizes
    - bbox covering metadata
    - Preserves CRS from original file
    - Supports remote inputs/outputs (S3, GCS, Azure)

    Args:
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
    """
    # Check input file bbox structure
    input_bbox_info = check_bbox_structure(input_parquet, verbose)

    # Track if we're using a temporary file with bbox added
    temp_file_created = False
    working_parquet = input_parquet

    # If add_bbox is requested and input doesn't have bbox, add it first
    if add_bbox_flag and not input_bbox_info["has_bbox_column"]:
        click.echo(
            click.style("\nAdding bbox column to enable fast bounds calculation...", fg="cyan")
        )
        # Create a temporary file with bbox column
        import tempfile

        temp_fd, temp_file = tempfile.mkstemp(suffix=".parquet")
        os.close(temp_fd)  # Close the file descriptor

        # Copy input to temp file and add bbox
        import shutil

        shutil.copy2(input_parquet, temp_file)

        # Add bbox to the temp file
        add_bbox(temp_file, "bbox", verbose)
        click.echo(click.style("✓ Added bbox column for optimized processing", fg="green"))

        working_parquet = temp_file
        temp_file_created = True

        # Update bbox info for the working file
        input_bbox_info = check_bbox_structure(working_parquet, verbose)

    elif input_bbox_info["status"] != "optimal":
        # Show warning if not optimal and not adding bbox
        click.echo(
            click.style(
                "\nWarning: Input file could benefit from bbox optimization:\n"
                + input_bbox_info["message"],
                fg="yellow",
            )
        )
        if not add_bbox_flag:
            click.echo(
                click.style(
                    "💡 Tip: Run this command with --add-bbox to enable fast bounds calculation",
                    fg="cyan",
                )
            )

    # Validate profile is only used with S3
    validate_profile_for_urls(profile, input_parquet, output_parquet)

    # Setup AWS profile if needed (for DuckDB to read from S3)
    setup_aws_profile_if_needed(profile, input_parquet, output_parquet)

    # Show remote read message
    show_remote_read_message(working_parquet, verbose)

    safe_url = safe_file_url(working_parquet, verbose)

    # Get metadata from original file (use original, not temp)
    metadata, schema = get_parquet_metadata(input_parquet, verbose)

    # Use specified geometry column or find primary one
    if geometry_column == "geometry":
        geometry_column = find_primary_geometry_column(working_parquet, verbose)

    if verbose:
        click.echo(f"Using geometry column: {geometry_column}")

    # Create DuckDB connection with httpfs if needed for remote files
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(working_parquet))

    if verbose:
        click.echo("Calculating dataset bounds for Hilbert ordering...")

    # Get dataset bounds using common function (will be fast if bbox column exists)
    bounds = get_dataset_bounds(working_parquet, geometry_column, verbose=verbose)

    if not bounds:
        raise click.ClickException("Could not calculate dataset bounds")

    xmin, ymin, xmax, ymax = bounds

    if verbose:
        click.echo(f"Dataset bounds: ({xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f})")
        click.echo("Reordering data using Hilbert curve...")

    # Build SELECT query for Hilbert ordering (without COPY wrapper)
    # The write_parquet_with_metadata function will add the COPY wrapper
    order_query = f"""
        SELECT *
        FROM '{safe_url}'
        ORDER BY ST_Hilbert(
            {geometry_column},
            ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))
        )
    """

    try:
        # Use the common write function with metadata preservation
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
            click.echo("Hilbert ordering completed successfully")

        # If we added bbox temporarily and it's now in the output, note it
        if add_bbox_flag and temp_file_created:
            click.echo(
                click.style(
                    "✓ Output includes bbox column and metadata for optimal performance", fg="green"
                )
            )

        if verbose:
            click.echo(f"Successfully wrote ordered data to: {output_parquet}")

    except duckdb.IOException as e:
        con.close()
        if is_remote_url(input_parquet):
            hints = get_remote_error_hint(str(e), input_parquet)
            raise click.ClickException(
                f"Failed to read remote file.\n\n{hints}\n\nOriginal error: {str(e)}"
            ) from e
        raise
    finally:
        # Clean up temporary file if created
        if temp_file_created and os.path.exists(temp_file):
            try:
                os.remove(temp_file)
                if verbose:
                    click.echo("Cleaned up temporary file")
            except Exception as e:
                if verbose:
                    click.echo(f"Warning: Could not remove temporary file: {e}")


if __name__ == "__main__":
    hilbert_order()
