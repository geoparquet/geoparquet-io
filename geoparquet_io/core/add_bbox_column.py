#!/usr/bin/env python3

from __future__ import annotations

import pyarrow as pa

from geoparquet_io.core.common import (
    add_computed_column,
    check_bbox_structure,
    detect_geoparquet_file_type,
    find_primary_geometry_column,
    get_duckdb_connection,
)
from geoparquet_io.core.logging_config import progress, success, warn
from geoparquet_io.core.partition_reader import require_single_file
from geoparquet_io.core.stream_io import execute_transform
from geoparquet_io.core.streaming import (
    find_geometry_column_from_table,
    is_stdin,
    should_stream_output,
)


def _build_bbox_sql(geometry_column: str, bbox_column_name: str = "bbox") -> str:
    """Build SQL expression for bbox struct column."""
    return f"""STRUCT_PACK(
        xmin := ST_XMin("{geometry_column}"),
        ymin := ST_YMin("{geometry_column}"),
        xmax := ST_XMax("{geometry_column}"),
        ymax := ST_YMax("{geometry_column}")
    ) AS "{bbox_column_name}" """


def add_bbox_table(
    table: pa.Table,
    bbox_column_name: str = "bbox",
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Add a bbox struct column to an Arrow Table.

    This is the table-centric version for the Python API.

    Args:
        table: Input PyArrow Table
        bbox_column_name: Name for the bbox column (default: 'bbox')
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with bbox column added
    """
    # Find geometry column
    geom_col = geometry_column or find_geometry_column_from_table(table)
    if not geom_col:
        geom_col = "geometry"

    # Check if bbox column already exists
    if bbox_column_name in table.column_names:
        # Drop existing column (replace behavior)
        idx = table.column_names.index(bbox_column_name)
        table = table.remove_column(idx)

    # Register table and execute query
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        con.register("__input_table", table)

        # Check if geometry column is BLOB (needs conversion)
        columns_info = con.execute("DESCRIBE __input_table").fetchall()
        geom_is_blob = any(col[0] == geom_col and "BLOB" in col[1].upper() for col in columns_info)

        if geom_is_blob and geom_col in table.column_names:
            # Create view with geometry conversion
            other_cols = [c for c in table.column_names if c != geom_col]
            col_defs = other_cols + [f'ST_GeomFromWKB("{geom_col}") AS "{geom_col}"']
            view_query = (
                f"CREATE VIEW __input_view AS SELECT {', '.join(col_defs)} FROM __input_table"
            )
            con.execute(view_query)
            source_ref = "__input_view"
        else:
            source_ref = "__input_table"

        # Build query to add bbox column
        bbox_expr = f"""STRUCT_PACK(
            xmin := ST_XMin("{geom_col}"),
            ymin := ST_YMin("{geom_col}"),
            xmax := ST_XMax("{geom_col}"),
            ymax := ST_YMax("{geom_col}")
        )"""

        # Get non-geometry columns
        other_cols = [f'"{c}"' for c in table.column_names if c != geom_col]
        select_cols = ", ".join(other_cols) if other_cols else ""

        # Build SELECT with geometry converted back to WKB
        if select_cols:
            query = f"""
                SELECT {select_cols},
                       ST_AsWKB("{geom_col}") AS "{geom_col}",
                       {bbox_expr} AS "{bbox_column_name}"
                FROM {source_ref}
            """
        else:
            query = f"""
                SELECT ST_AsWKB("{geom_col}") AS "{geom_col}",
                       {bbox_expr} AS "{bbox_column_name}"
                FROM {source_ref}
            """
        result = con.execute(query).fetch_arrow_table()

        # Preserve metadata
        if table.schema.metadata:
            result = result.replace_schema_metadata(table.schema.metadata)

        return result
    finally:
        con.close()


def _make_add_bbox_query(
    source: str,
    geometry_column: str,
    bbox_column_name: str,
    replace_existing: bool = False,
) -> str:
    """Build query to add bbox column to a source."""
    bbox_expr = f"""STRUCT_PACK(
        xmin := ST_XMin("{geometry_column}"),
        ymin := ST_YMin("{geometry_column}"),
        xmax := ST_XMax("{geometry_column}"),
        ymax := ST_YMax("{geometry_column}")
    )"""

    if replace_existing:
        return f'SELECT * EXCLUDE ("{bbox_column_name}"), {bbox_expr} AS "{bbox_column_name}" FROM {source}'
    else:
        return f'SELECT *, {bbox_expr} AS "{bbox_column_name}" FROM {source}'


def add_bbox_column(
    input_parquet: str,
    output_parquet: str | None = None,
    bbox_column_name: str = "bbox",
    dry_run: bool = False,
    verbose: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    profile: str | None = None,
    force: bool = False,
    geoparquet_version: str | None = None,
) -> None:
    """
    Add a bbox struct column to a GeoParquet file.

    Supports Arrow IPC streaming:
    - Input "-" reads from stdin
    - Output "-" or None (with piped stdout) streams to stdout

    Checks for existing bbox columns before adding. If a bbox column already exists:

    - **With covering metadata**: Informs user and exits successfully (no action needed)
    - **Without metadata**: Suggests using `gpio add bbox-metadata` command
    - **With --force**: Replaces the existing bbox column

    Args:
        input_parquet: Path to the input parquet file (local, remote URL, or "-" for stdin)
        output_parquet: Path to output file, "-" for stdout, or None for auto-detect
        bbox_column_name: Name for the bbox column (default: 'bbox')
        dry_run: Whether to print SQL commands without executing them
        verbose: Whether to print verbose output
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        profile: AWS profile name (S3 only, optional)
        force: Whether to replace an existing bbox column
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)

    Note:
        Bbox covering metadata is automatically added when the file is written.
    """
    # Check for streaming mode (stdin input or stdout output)
    is_streaming = is_stdin(input_parquet) or should_stream_output(output_parquet)

    if is_streaming and not dry_run:
        _add_bbox_streaming(
            input_parquet,
            output_parquet,
            bbox_column_name,
            verbose,
            compression,
            compression_level,
            row_group_size_mb,
            row_group_rows,
            profile,
            force,
            geoparquet_version,
        )
        return

    # File-based mode
    _add_bbox_file_based(
        input_parquet,
        output_parquet,
        bbox_column_name,
        dry_run,
        verbose,
        compression,
        compression_level,
        row_group_size_mb,
        row_group_rows,
        profile,
        force,
        geoparquet_version,
    )


def _add_bbox_streaming(
    input_path: str,
    output_path: str | None,
    bbox_column_name: str,
    verbose: bool,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
    force: bool,
    geoparquet_version: str | None,
) -> None:
    """Handle streaming input/output for add_bbox."""
    # Suppress verbose when streaming to stdout
    if should_stream_output(output_path):
        verbose = False

    def make_query(source: str, con) -> str:
        """Build the add bbox query for streaming source."""
        # Get column names from query result (works with both table names and read_parquet)
        sample = con.execute(f"SELECT * FROM {source} LIMIT 0").description
        col_names = [col[0] for col in sample]

        # Find geometry column from common names
        geom_col = None
        for name in ["geometry", "geom", "the_geom", "wkb_geometry"]:
            if name in col_names:
                geom_col = name
                break
        if not geom_col:
            geom_col = "geometry"

        return _make_add_bbox_query(source, geom_col, bbox_column_name, replace_existing=force)

    execute_transform(
        input_path,
        output_path,
        make_query,
        verbose=verbose,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        profile=profile,
        geoparquet_version=geoparquet_version,
    )

    if not should_stream_output(output_path):
        success(f"Successfully added bbox column '{bbox_column_name}' to: {output_path}")


def _add_bbox_file_based(
    input_parquet: str,
    output_parquet: str | None,
    bbox_column_name: str,
    dry_run: bool,
    verbose: bool,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
    force: bool,
    geoparquet_version: str | None,
) -> None:
    """Handle file-based add_bbox operation."""
    # Check for partition input (not supported)
    require_single_file(input_parquet, "add bbox")

    # Check for parquet-geo-only input and warn user (skip in dry-run mode)
    if not dry_run:
        file_type_info = detect_geoparquet_file_type(input_parquet, verbose)
        if file_type_info["file_type"] == "parquet_geo_only":
            warn(
                "Note: Input file uses native Parquet geometry types without GeoParquet metadata. "
                "Bbox column is not required for spatial statistics as native geo types provide "
                "row group statistics. Proceeding with bbox addition anyway."
            )

    # Check for existing bbox column (skip in dry-run mode)
    replace_column = None
    if not dry_run:
        bbox_info = check_bbox_structure(input_parquet, verbose)
        existing_bbox_col = bbox_info.get("bbox_column_name")

        if bbox_info["status"] == "optimal":
            if force:
                replace_column = _handle_existing_bbox_force(bbox_column_name, existing_bbox_col)
            else:
                progress(
                    f"File already has bbox column '{existing_bbox_col}' with covering metadata."
                )
                progress("Use --force to replace the existing bbox column.")
                return

        elif bbox_info["status"] == "suboptimal":
            if force:
                replace_column = _handle_existing_bbox_force(bbox_column_name, existing_bbox_col)
            else:
                progress(f"File has bbox column '{existing_bbox_col}' but lacks covering metadata.")
                progress("Run 'gpio add bbox-metadata' to add metadata, or use --force to replace.")
                return

    # Get geometry column for the SQL expression
    geom_col = find_primary_geometry_column(input_parquet, verbose)

    # Define the SQL expression (the only unique part)
    sql_expression = f"""STRUCT_PACK(
        xmin := ST_XMin({geom_col}),
        ymin := ST_YMin({geom_col}),
        xmax := ST_XMax({geom_col}),
        ymax := ST_YMax({geom_col})
    )"""

    # Use the generic helper for all boilerplate
    add_computed_column(
        input_parquet=input_parquet,
        output_parquet=output_parquet,
        column_name=bbox_column_name,
        sql_expression=sql_expression,
        extensions=None,
        dry_run=dry_run,
        verbose=verbose,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        dry_run_description="Bounding box struct (xmin, ymin, xmax, ymax)",
        profile=profile,
        replace_column=replace_column,
        geoparquet_version=geoparquet_version,
    )

    if not dry_run:
        success(f"Successfully added bbox column '{bbox_column_name}' to: {output_parquet}")


def _handle_existing_bbox_force(bbox_column_name: str, existing_bbox_col: str) -> str | None:
    """Handle force mode when bbox column exists. Returns column to replace or None."""
    if bbox_column_name == existing_bbox_col:
        progress(f"Replacing existing bbox column '{existing_bbox_col}'...")
        return existing_bbox_col
    else:
        warn(
            f"Warning: Adding '{bbox_column_name}' alongside existing "
            f"'{existing_bbox_col}'. File will have 2 bbox columns."
        )
        return None


if __name__ == "__main__":
    add_bbox_column()
