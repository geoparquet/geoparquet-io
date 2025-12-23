#!/usr/bin/env python3

import os
import tempfile

import click

from geoparquet_io.core.add_quadkey_column import add_quadkey_column
from geoparquet_io.core.common import (
    get_duckdb_connection,
    get_parquet_metadata,
    needs_httpfs,
    safe_file_url,
    setup_aws_profile_if_needed,
    validate_profile_for_urls,
    write_parquet_with_metadata,
)
from geoparquet_io.core.constants import DEFAULT_QUADKEY_COLUMN_NAME, DEFAULT_QUADKEY_RESOLUTION
from geoparquet_io.core.duckdb_metadata import get_column_names, get_usable_columns
from geoparquet_io.core.logging_config import configure_verbose, debug, progress, success


def sort_by_quadkey(
    input_parquet,
    output_parquet,
    quadkey_column_name=DEFAULT_QUADKEY_COLUMN_NAME,
    resolution=DEFAULT_QUADKEY_RESOLUTION,
    use_centroid=False,
    remove_quadkey_column=False,
    verbose=False,
    compression="ZSTD",
    compression_level=None,
    row_group_size_mb=None,
    row_group_rows=None,
    profile=None,
    geoparquet_version=None,
):
    """
    Sort a GeoParquet file by quadkey column.

    If the quadkey column doesn't exist and using the default column name, it will
    be auto-added at the specified resolution. If using a custom --quadkey-name and
    the column is missing, an error is raised.

    Args:
        input_parquet: Path to input GeoParquet file (local or remote URL)
        output_parquet: Path to output file (local or remote URL)
        quadkey_column_name: Name of the quadkey column to sort by (default: 'quadkey')
        resolution: Resolution for auto-adding quadkey column (0-23). Default: 13
        use_centroid: Use geometry centroid when auto-adding quadkey column
        remove_quadkey_column: Exclude quadkey column from output after sorting
        verbose: Print verbose output
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
    """
    configure_verbose(verbose)

    # Validate profile is only used with S3
    validate_profile_for_urls(profile, input_parquet, output_parquet)

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, input_parquet, output_parquet)

    safe_url = safe_file_url(input_parquet, verbose)

    # Check if quadkey column exists
    column_names = get_column_names(safe_url)
    column_exists = quadkey_column_name in column_names
    using_default_name = quadkey_column_name == DEFAULT_QUADKEY_COLUMN_NAME

    # Track if we created a temporary file with quadkey
    temp_file = None
    actual_input = input_parquet

    if not column_exists:
        if not using_default_name:
            # Custom name specified but column doesn't exist - error
            raise click.ClickException(
                f"Quadkey column '{quadkey_column_name}' not found in input file.\n"
                f"Run 'gpio add quadkey --quadkey-name {quadkey_column_name}' to add it first."
            )

        # Auto-add quadkey column using default name
        if verbose:
            debug(
                f"Quadkey column '{quadkey_column_name}' not found. "
                f"Auto-adding at resolution {resolution}..."
            )

        # Create temporary file for quadkey-enriched data
        temp_dir = tempfile.gettempdir()
        temp_file = os.path.join(temp_dir, f"quadkey_enriched_{os.path.basename(input_parquet)}")

        try:
            add_quadkey_column(
                input_parquet=input_parquet,
                output_parquet=temp_file,
                quadkey_column_name=quadkey_column_name,
                resolution=resolution,
                use_centroid=use_centroid,
                dry_run=False,
                verbose=verbose,
                compression="ZSTD",
                compression_level=15,
                row_group_size_mb=None,
                row_group_rows=None,
                profile=profile,
            )
            actual_input = temp_file
            if verbose:
                debug(f"Quadkey column added successfully at resolution {resolution}")
        except Exception as e:
            if temp_file and os.path.exists(temp_file):
                os.remove(temp_file)
            raise click.ClickException(f"Failed to add quadkey column: {str(e)}") from e

    elif verbose:
        debug(f"Using existing quadkey column '{quadkey_column_name}'")

    # Get metadata from input file (use actual_input in case we added quadkey)
    actual_safe_url = safe_file_url(actual_input, verbose)
    metadata, _ = get_parquet_metadata(actual_input, verbose)

    # Get usable columns for building SELECT clause
    usable_cols = get_usable_columns(actual_safe_url)
    existing_columns = [c["name"] for c in usable_cols]

    try:
        # Create DuckDB connection
        con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(actual_input))

        # Build SELECT clause - exclude quadkey if requested
        if remove_quadkey_column:
            select_cols = [f'"{col}"' for col in existing_columns if col != quadkey_column_name]
            select_clause = ", ".join(select_cols)
            progress(f"Sorting by '{quadkey_column_name}' (will be removed from output)")
        else:
            select_clause = "*"
            progress(f"Sorting by '{quadkey_column_name}'")

        # Build sort query
        query = f"""
            SELECT {select_clause}
            FROM '{actual_safe_url}'
            ORDER BY "{quadkey_column_name}"
        """

        if verbose:
            debug(f"Sort query: {query}")

        # Write output with metadata preservation
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
        )

        if remove_quadkey_column:
            success(f"Sorted by quadkey and removed column to: {output_parquet}")
        else:
            success(f"Sorted by quadkey to: {output_parquet}")

    finally:
        con.close()
        # Clean up temp file if we created one
        if temp_file and os.path.exists(temp_file):
            if verbose:
                debug("Cleaning up temporary quadkey-enriched file...")
            os.remove(temp_file)


if __name__ == "__main__":
    sort_by_quadkey()
