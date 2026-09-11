#!/usr/bin/env python3

from __future__ import annotations

import os

import duckdb
import pyarrow as pa

from geoparquet_io.core.common import (
    add_bbox,
    get_bbox_advice,
    get_dataset_bounds,
    get_parquet_metadata,
    write_parquet_with_metadata,
)
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, quote_identifier, sql_path
from geoparquet_io.core.exceptions import RemoteAccessError
from geoparquet_io.core.file_utils import handle_output_overwrite, resolve_file_url
from geoparquet_io.core.geo_metadata import DEFAULT_GEOPARQUET_VERSION
from geoparquet_io.core.geometry_detection import (
    STANDARD_GEOMETRY_NAMES,
    find_primary_geometry_column,
)
from geoparquet_io.core.logging_config import debug, info, success, warn
from geoparquet_io.core.parquet_writer import (
    DEFAULT_ROW_GROUP_ROWS,
    resolve_output_geoparquet_version,
    resolve_row_group_rows,
)
from geoparquet_io.core.partition.reader import require_single_file
from geoparquet_io.core.remote import (
    get_remote_error_hint,
    is_remote_url,
    needs_httpfs,
    setup_aws_profile_if_needed,
    show_remote_read_message,
    validate_profile_for_urls,
)
from geoparquet_io.core.stream_io import open_input, write_output
from geoparquet_io.core.streaming import (
    find_geometry_column_from_table,
    is_stdin,
    should_stream_output,
)


def _resolve_output_version(
    input_parquet: str,
    geoparquet_version: str | None,
    verbose: bool,
    profile: str | None = None,
) -> str | None:
    """The GeoParquet version this run will actually write, or None if unknowable.

    Whether ``gpio sort hilbert`` and ``gpio sort str`` advise
    ``--geoparquet-version 2.0``, and whether they offer their row-group
    guidance at all, both hang off this answer -- so it has to be the *write's*
    answer. It asks the write facade the same question the write asks it, with
    the same two witnesses (the input file, then its carried metadata) and the
    same ``or DEFAULT_GEOPARQUET_VERSION`` fallback that
    ``write_parquet_with_metadata`` applies.

    It used to re-implement that decision instead, reading only the carried
    ``geo`` key. Once the facade started consulting the file, the two answers
    diverged on exactly the input class the facade exists to fix: for a
    native-geo-only input this said "1.1" and advised switching to 2.0 over a
    file it was writing as 2.0 -- #738's bug arriving from the other side --
    while the row-group guidance, gated on 2.0, could never fire for it.

    Returns None rather than a default when the version cannot be established:
    a stdin stream (whose version the writer resolves from the stream's own
    metadata, not from anything visible here) or an input that will not open.
    Callers use None to stay silent. Defaulting instead is what produced the
    original wrong warning -- a failed read is not evidence of a 1.1 input.
    The facade cannot make that distinction on its own: it swallows a failed
    read and returns None, which the ``or`` below would turn straight back into
    the guess. So the read stays here, as the gate on saying anything at all.
    """
    if geoparquet_version:
        return geoparquet_version
    if is_stdin(input_parquet):
        return None

    # Authenticate before probing: the probe reads the input, and without the
    # profile a remote read fails, which previously degraded to "1.1" and
    # printed the very warning this function exists to suppress.
    if profile:
        setup_aws_profile_if_needed(profile, input_parquet, None)

    try:
        metadata, _ = get_parquet_metadata(input_parquet, verbose)
    except Exception as e:  # noqa: BLE001 - any read failure means "stay quiet"
        debug(f"Could not read {input_parquet} to resolve output version: {e}")
        return None
    return (
        resolve_output_geoparquet_version(
            None,
            input_file=input_parquet,
            original_metadata=metadata,
            verbose=verbose,
        )
        or DEFAULT_GEOPARQUET_VERSION
    )


def hilbert_order_table(
    table: pa.Table,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Reorder an Arrow Table using Hilbert curve ordering.

    This is the table-centric version for the Python API.

    Empty or NULL geometries are placed at the end of the result since they
    have no spatial extent and cannot be assigned a Hilbert curve index.

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
    quoted_geom = quote_identifier(geom_col)

    # Register table and execute query
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        con.register("__input_table", table)

        # Check if geometry column is BLOB (needs conversion)
        columns_info = con.execute("DESCRIBE __input_table").fetchall()
        geom_is_blob = any(col[0] == geom_col and "BLOB" in col[1].upper() for col in columns_info)

        if geom_is_blob and geom_col in table.column_names:
            # Quote column names to handle special characters (colons, spaces, etc.)
            other_cols = [quote_identifier(c) for c in table.column_names if c != geom_col]
            col_defs = other_cols + [f"ST_GeomFromWKB({quoted_geom}) AS {quoted_geom}"]
            view_query = (
                f"CREATE VIEW __input_view AS SELECT {', '.join(col_defs)} FROM __input_table"
            )
            con.execute(view_query)
            source_ref = "__input_view"
        else:
            source_ref = "__input_table"

        # Count empty/null geometries to handle them separately
        # ST_IsEmpty returns TRUE for empty geometries, NULL for NULL geometries
        empty_count_result = con.execute(f"""
            SELECT COUNT(*) FROM {source_ref}
            WHERE {quoted_geom} IS NULL OR ST_IsEmpty({quoted_geom})
        """).fetchone()
        empty_count = empty_count_result[0] if empty_count_result else 0

        if empty_count > 0:
            warn(
                f"Found {empty_count} empty/null geometries. "
                "These will be placed at the end of the sorted output."
            )

        # Calculate dataset bounds from non-empty geometries only
        bounds_result = con.execute(f"""
            SELECT
                MIN(ST_XMin({quoted_geom})) as xmin,
                MIN(ST_YMin({quoted_geom})) as ymin,
                MAX(ST_XMax({quoted_geom})) as xmax,
                MAX(ST_YMax({quoted_geom})) as ymax
            FROM {source_ref}
            WHERE {quoted_geom} IS NOT NULL AND NOT ST_IsEmpty({quoted_geom})
        """).fetchone()

        # If all geometries are empty/null, return table unchanged
        if not bounds_result or any(v is None for v in bounds_result):
            warn("All geometries are empty or null. Returning table without Hilbert ordering.")
            return table

        xmin, ymin, xmax, ymax = bounds_result

        # Get non-geometry columns
        other_cols = [quote_identifier(c) for c in table.column_names if c != geom_col]
        select_cols = ", ".join(other_cols) if other_cols else ""

        # Build column selection with WKB conversion
        if select_cols:
            wkb_select = f"{select_cols}, ST_AsWKB({quoted_geom}) AS {quoted_geom}"
        else:
            wkb_select = f"ST_AsWKB({quoted_geom}) AS {quoted_geom}"

        # Build Hilbert ordering query that handles empty geometries:
        # 1. Non-empty geometries are ordered by Hilbert curve
        # 2. Empty/null geometries are appended at the end
        query = f"""
            WITH non_empty AS (
                SELECT {wkb_select}
                FROM {source_ref}
                WHERE {quoted_geom} IS NOT NULL AND NOT ST_IsEmpty({quoted_geom})
                ORDER BY ST_Hilbert({quoted_geom},
                    ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax})))
            ),
            empty_or_null AS (
                SELECT {wkb_select}
                FROM {source_ref}
                WHERE {quoted_geom} IS NULL OR ST_IsEmpty({quoted_geom})
            )
            SELECT * FROM non_empty
            UNION ALL
            SELECT * FROM empty_or_null
        """
        result = con.execute(query).arrow().read_all()

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
    overwrite: bool = False,
    memory_limit: str | None = None,
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
        row_group_rows: Exact number of rows per row group. When neither this
            nor ``row_group_size_mb`` is given, the sort default
            (``DEFAULT_ROW_GROUP_ROWS``) applies.
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
        memory_limit: DuckDB memory limit for streaming writes (e.g., '2GB', '512MB')
    """
    row_group_rows = resolve_row_group_rows(row_group_rows, row_group_size_mb)
    effective_version = _resolve_output_version(input_parquet, geoparquet_version, verbose, profile)
    if effective_version == "1.1":
        warn(
            "Hilbert sorting to GeoParquet v1.1 provides no spatial filter pushdown benefit. "
            "Consider using --geoparquet-version 2.0 to enable native geo_bbox row group statistics."
        )

    if (
        row_group_rows
        and row_group_rows > DEFAULT_ROW_GROUP_ROWS
        and effective_version in ("2.0", "parquet-geo-only")
    ):
        info(
            "For optimal spatial filter pushdown with Hilbert sorting, consider using "
            f"--row-group-size between 10,000 and {DEFAULT_ROW_GROUP_ROWS:,}. Smaller row "
            "groups create tighter bounding boxes that enable more row group skipping."
        )

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
            memory_limit,
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
        overwrite,
        memory_limit,
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
    memory_limit: str | None = None,
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
            for name in STANDARD_GEOMETRY_NAMES:
                if name in col_names:
                    geom_col = name
                    break
        quoted_geom = quote_identifier(geom_col)

        if verbose:
            debug(f"Using geometry column: {geom_col}")
            debug("Calculating dataset bounds for Hilbert ordering...")

        # Count empty/null geometries
        empty_count_result = con.execute(f"""
            SELECT COUNT(*) FROM {source}
            WHERE {quoted_geom} IS NULL OR ST_IsEmpty({quoted_geom})
        """).fetchone()
        empty_count = empty_count_result[0] if empty_count_result else 0

        if empty_count > 0:
            warn(
                f"Found {empty_count} empty/null geometries. "
                "These will be placed at the end of the sorted output."
            )

        # Calculate dataset bounds from non-empty geometries only
        bounds_result = con.execute(f"""
            SELECT
                MIN(ST_XMin({quoted_geom})) as xmin,
                MIN(ST_YMin({quoted_geom})) as ymin,
                MAX(ST_XMax({quoted_geom})) as xmax,
                MAX(ST_YMax({quoted_geom})) as ymax
            FROM {source}
            WHERE {quoted_geom} IS NOT NULL AND NOT ST_IsEmpty({quoted_geom})
        """).fetchone()

        # If all geometries are empty/null, pass through unchanged
        if not bounds_result or any(v is None for v in bounds_result):
            warn("All geometries are empty or null. Writing file without Hilbert ordering.")
            # Pass through data unchanged
            passthrough_query = f"SELECT * FROM {source}"
            write_output(
                con,
                passthrough_query,
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
                memory_limit=memory_limit,
            )
            if not should_stream_output(output_path):
                success(f"Wrote data without Hilbert ordering to: {output_path}")
            return

        xmin, ymin, xmax, ymax = bounds_result
        if verbose:
            debug(f"Dataset bounds: ({xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f})")
            debug("Reordering data using Hilbert curve...")

        # Build Hilbert ordering query with empty geometry handling
        # Non-empty geometries are ordered by Hilbert curve, empty/null appended at end
        query = f"""
            WITH non_empty AS (
                SELECT * FROM {source}
                WHERE {quoted_geom} IS NOT NULL AND NOT ST_IsEmpty({quoted_geom})
                ORDER BY ST_Hilbert({quoted_geom},
                    ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax})))
            ),
            empty_or_null AS (
                SELECT * FROM {source}
                WHERE {quoted_geom} IS NULL OR ST_IsEmpty({quoted_geom})
            )
            SELECT * FROM non_empty
            UNION ALL
            SELECT * FROM empty_or_null
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
            memory_limit=memory_limit,
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
    overwrite: bool = False,
    memory_limit: str | None = None,
) -> None:
    """Handle file-based hilbert_order operation."""
    # Check if output file exists and handle overwrite (fixes issue #278)
    handle_output_overwrite(output_parquet, overwrite, input_parquet)

    # Check for partition input (not supported)
    require_single_file(input_parquet, "sort hilbert")

    working_parquet, temp_file_created, temp_file = _prepare_working_file(
        input_parquet, add_bbox_flag, verbose
    )

    validate_profile_for_urls(profile, input_parquet, output_parquet)
    setup_aws_profile_if_needed(profile, input_parquet, output_parquet)
    show_remote_read_message(working_parquet, verbose)

    working_url = resolve_file_url(working_parquet, verbose)
    # Read the metadata of the file the query actually reads. With --add-bbox
    # that is the working copy, whose `geo` block `add_bbox` just extended with
    # the `covering` describing the column it wrote. Reading the *original*
    # input here threw that covering away before the write could carry it, so
    # the output shipped a bbox column nothing declared (#738).
    metadata, _ = get_parquet_metadata(working_parquet, verbose)

    if geometry_column == "geometry":
        geometry_column = find_primary_geometry_column(working_parquet, verbose)
    if verbose:
        debug(f"Using geometry column: {geometry_column}")

    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(working_parquet))

    if verbose:
        debug("Calculating dataset bounds for Hilbert ordering...")

    # Count empty/null geometries
    empty_count_result = con.execute(f"""
        SELECT COUNT(*) FROM {sql_path(working_url)}
        WHERE {quote_identifier(geometry_column)} IS NULL OR ST_IsEmpty({quote_identifier(geometry_column)})
    """).fetchone()
    empty_count = empty_count_result[0] if empty_count_result else 0

    if empty_count > 0:
        warn(
            f"Found {empty_count} empty/null geometries. "
            "These will be placed at the end of the sorted output."
        )

    # Get bounds (empty geometries naturally excluded since ST_XMin returns NULL for them)
    bounds = get_dataset_bounds(working_parquet, geometry_column, verbose=verbose)

    # If all geometries are empty/null, copy file unchanged
    if not bounds:
        warn("All geometries are empty or null. Writing file without Hilbert ordering.")
        passthrough_query = f"SELECT * FROM {sql_path(working_url)}"
        write_parquet_with_metadata(
            con,
            passthrough_query,
            output_parquet,
            original_metadata=metadata,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            verbose=verbose,
            profile=profile,
            geoparquet_version=geoparquet_version,
            input_file=input_parquet,
            memory_limit=memory_limit,
        )
        con.close()
        if temp_file_created and temp_file:
            _cleanup_temp_file(temp_file, verbose)
        success(f"Wrote data without Hilbert ordering to: {output_parquet}")
        return

    xmin, ymin, xmax, ymax = bounds
    if verbose:
        debug(f"Dataset bounds: ({xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f})")
        debug("Reordering data using Hilbert curve...")

    # Build Hilbert ordering query with empty geometry handling
    # Non-empty geometries are ordered by Hilbert curve, empty/null appended at end
    order_query = f"""
        WITH non_empty AS (
            SELECT * FROM {sql_path(working_url)}
            WHERE {quote_identifier(geometry_column)} IS NOT NULL AND NOT ST_IsEmpty({quote_identifier(geometry_column)})
            ORDER BY ST_Hilbert({quote_identifier(geometry_column)},
                ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax})))
        ),
        empty_or_null AS (
            SELECT * FROM {sql_path(working_url)}
            WHERE {quote_identifier(geometry_column)} IS NULL OR ST_IsEmpty({quote_identifier(geometry_column)})
        )
        SELECT * FROM non_empty
        UNION ALL
        SELECT * FROM empty_or_null
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
            input_file=input_parquet,
            memory_limit=memory_limit,
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
            raise RemoteAccessError(input_parquet, f"{hints}\n\nOriginal error: {str(e)}") from e
        raise
    finally:
        _cleanup_temp_file(temp_file, verbose)


if __name__ == "__main__":
    hilbert_order()
