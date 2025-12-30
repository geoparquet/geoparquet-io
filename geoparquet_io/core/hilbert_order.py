#!/usr/bin/env python3

from __future__ import annotations

import os

import click
import duckdb
import pyarrow as pa

from geoparquet_io.core.common import (
    add_bbox,
    find_primary_geometry_column,
    get_bbox_advice,
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
from geoparquet_io.core.stream_io import open_input, write_output
from geoparquet_io.core.streaming import (
    find_geometry_column_from_table,
    is_stdin,
    should_stream_output,
)


def hilbert_order_table(
    table: pa.Table,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Reorder an Arrow Table using Hilbert curve ordering.

    This is the table-centric version for the Python API.

    Args:
        table: Input PyArrow Table
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with rows reordered by Hilbert curve
    """
    # Find geometry column
    geom_col = geometry_column or find_geometry_column_from_table(table)
    if not geom_col:
        geom_col = "geometry"

    # Register table and execute query
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        con.register("__input_table", table)

        # Check if geometry column is BLOB (needs conversion)
        columns_info = con.execute("DESCRIBE __input_table").fetchall()
        geom_is_blob = any(col[0] == geom_col and "BLOB" in col[1].upper() for col in columns_info)

        if geom_is_blob and geom_col in table.column_names:
            other_cols = [c for c in table.column_names if c != geom_col]
            col_defs = other_cols + [f'ST_GeomFromWKB("{geom_col}") AS "{geom_col}"']
            view_query = (
                f"CREATE VIEW __input_view AS SELECT {', '.join(col_defs)} FROM __input_table"
            )
            con.execute(view_query)
            source_ref = "__input_view"
        else:
            source_ref = "__input_table"

        # Calculate dataset bounds
        bounds_result = con.execute(f"""
            SELECT
                MIN(ST_XMin("{geom_col}")) as xmin,
                MIN(ST_YMin("{geom_col}")) as ymin,
                MAX(ST_XMax("{geom_col}")) as xmax,
                MAX(ST_YMax("{geom_col}")) as ymax
            FROM {source_ref}
        """).fetchone()

        if not bounds_result or any(v is None for v in bounds_result):
            raise ValueError("Could not calculate dataset bounds from table")

        xmin, ymin, xmax, ymax = bounds_result

        # Get non-geometry columns
        other_cols = [f'"{c}"' for c in table.column_names if c != geom_col]
        select_cols = ", ".join(other_cols) if other_cols else ""

        # Build Hilbert ordering query with geometry converted back to WKB
        if select_cols:
            query = f"""
                SELECT {select_cols},
                       ST_AsWKB("{geom_col}") AS "{geom_col}"
                FROM {source_ref}
                ORDER BY ST_Hilbert("{geom_col}",
                    ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax})))
            """
        else:
            query = f"""
                SELECT ST_AsWKB("{geom_col}") AS "{geom_col}"
                FROM {source_ref}
                ORDER BY ST_Hilbert("{geom_col}",
                    ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax})))
            """
        result = con.execute(query).fetch_arrow_table()

        # Preserve metadata
        if table.schema.metadata:
            result = result.replace_schema_metadata(table.schema.metadata)

        return result
    finally:
        con.close()


def _prepare_working_file(input_parquet, add_bbox_flag, verbose):
    """Prepare working file, adding bbox if needed.

    Returns:
        tuple: (working_parquet, temp_file_created, temp_file_path_or_none)
    """
    import shutil
    import tempfile

    bbox_advice = get_bbox_advice(input_parquet, "bounds_calculation", verbose)

    if add_bbox_flag and not bbox_advice["has_bbox_column"]:
        info("\nAdding bbox column to enable fast bounds calculation...")
        temp_fd, temp_file = tempfile.mkstemp(suffix=".parquet")
        os.close(temp_fd)
        shutil.copy2(input_parquet, temp_file)
        add_bbox(temp_file, "bbox", verbose)
        success("Added bbox column for optimized processing")
        return temp_file, True, temp_file

    if bbox_advice["needs_warning"]:
        warn(f"\nWarning: {bbox_advice['message']}")
        if not add_bbox_flag:
            for suggestion in bbox_advice["suggestions"]:
                info(f"Tip: {suggestion}")

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
    input_parquet: str,
    output_parquet: str | None = None,
    geometry_column: str = "geometry",
    add_bbox_flag: bool = False,
    verbose: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    profile: str | None = None,
    geoparquet_version: str | None = None,
) -> None:
    """
    Reorder a GeoParquet file using Hilbert curve ordering.

    Supports Arrow IPC streaming:
    - Input "-" reads from stdin
    - Output "-" or None (with piped stdout) streams to stdout

    Args:
        input_parquet: Path to input GeoParquet file (local, remote URL, or "-" for stdin)
        output_parquet: Path to output file, "-" for stdout, or None for auto-detect
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
    # Check for streaming mode (stdin input or stdout output)
    is_streaming = is_stdin(input_parquet) or should_stream_output(output_parquet)

    if is_streaming:
        _hilbert_order_streaming(
            input_parquet,
            output_parquet,
            geometry_column,
            verbose,
            compression,
            compression_level,
            row_group_size_mb,
            row_group_rows,
            profile,
            geoparquet_version,
        )
        return

    # File-based mode
    _hilbert_order_file_based(
        input_parquet,
        output_parquet,
        geometry_column,
        add_bbox_flag,
        verbose,
        compression,
        compression_level,
        row_group_size_mb,
        row_group_rows,
        profile,
        geoparquet_version,
    )


def _hilbert_order_streaming(
    input_path: str,
    output_path: str | None,
    geometry_column: str,
    verbose: bool,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
    geoparquet_version: str | None,
) -> None:
    """Handle streaming input/output for hilbert_order."""
    # Suppress verbose when streaming to stdout
    if should_stream_output(output_path):
        verbose = False

    with open_input(input_path, verbose=verbose) as (source, metadata, is_stream, con):
        # Get column names from query result (works with both table names and read_parquet)
        sample = con.execute(f"SELECT * FROM {source} LIMIT 0").description
        col_names = [col[0] for col in sample]

        # Find geometry column
        geom_col = geometry_column
        if geom_col == "geometry" or geom_col not in col_names:
            for name in ["geometry", "geom", "the_geom", "wkb_geometry"]:
                if name in col_names:
                    geom_col = name
                    break

        if verbose:
            debug(f"Using geometry column: {geom_col}")
            debug("Calculating dataset bounds for Hilbert ordering...")

        # Calculate dataset bounds
        bounds_result = con.execute(f"""
            SELECT
                MIN(ST_XMin("{geom_col}")) as xmin,
                MIN(ST_YMin("{geom_col}")) as ymin,
                MAX(ST_XMax("{geom_col}")) as xmax,
                MAX(ST_YMax("{geom_col}")) as ymax
            FROM {source}
        """).fetchone()

        if not bounds_result or any(v is None for v in bounds_result):
            raise click.ClickException("Could not calculate dataset bounds")

        xmin, ymin, xmax, ymax = bounds_result
        if verbose:
            debug(f"Dataset bounds: ({xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f})")
            debug("Reordering data using Hilbert curve...")

        # Build Hilbert ordering query
        query = f"""
            SELECT * FROM {source}
            ORDER BY ST_Hilbert("{geom_col}",
                ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax})))
        """

        if verbose:
            debug(f"Streaming hilbert query: {query}")

        # Write output
        write_output(
            con,
            query,
            output_path,
            original_metadata=metadata,
            geometry_column=geom_col,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            verbose=verbose,
            profile=profile,
            geoparquet_version=geoparquet_version,
        )

        if not should_stream_output(output_path):
            success(f"Successfully reordered data using Hilbert curve to: {output_path}")


def _hilbert_order_file_based(
    input_parquet: str,
    output_parquet: str | None,
    geometry_column: str,
    add_bbox_flag: bool,
    verbose: bool,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
    geoparquet_version: str | None,
) -> None:
    """Handle file-based hilbert_order operation."""
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
