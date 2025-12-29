"""Core reprojection logic for GeoParquet files."""

from __future__ import annotations

import json
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa

from geoparquet_io.core.common import (
    _extract_crs_identifier,
    check_bbox_structure,
    extract_crs_from_parquet,
    find_primary_geometry_column,
    get_duckdb_connection,
    needs_httpfs,
    parse_crs_string_to_projjson,
    remote_write_context,
    safe_file_url,
    setup_aws_profile_if_needed,
    upload_if_remote,
    validate_compression_settings,
    validate_profile_for_urls,
    write_parquet_with_metadata,
)
from geoparquet_io.core.logging_config import debug, info, success
from geoparquet_io.core.stream_io import write_output
from geoparquet_io.core.streaming import is_stdin, read_arrow_stream, should_stream_output

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass
class ReprojectResult:
    """Result of a reprojection operation."""

    output_path: Path
    source_crs: str
    target_crs: str
    feature_count: int


def _detect_geometry_column_from_table(table: pa.Table) -> str:
    """Detect geometry column from table metadata.

    Args:
        table: PyArrow Table with geo metadata

    Returns:
        Geometry column name, defaults to 'geometry'
    """
    if table.schema.metadata and b"geo" in table.schema.metadata:
        try:
            geo_meta = json.loads(table.schema.metadata[b"geo"].decode("utf-8"))
            if "primary_column" in geo_meta:
                return geo_meta["primary_column"]
        except (json.JSONDecodeError, KeyError):
            pass
    return "geometry"


def _detect_crs_from_table(table: pa.Table, geom_col: str) -> str:
    """Detect CRS from table metadata.

    Args:
        table: PyArrow Table with geo metadata
        geom_col: Geometry column name

    Returns:
        CRS string like "EPSG:4326"
    """
    if table.schema.metadata and b"geo" in table.schema.metadata:
        try:
            geo_meta = json.loads(table.schema.metadata[b"geo"].decode("utf-8"))
            columns = geo_meta.get("columns", {})
            if geom_col in columns:
                crs_info = columns[geom_col].get("crs")
                if crs_info:
                    identifier = _extract_crs_identifier(crs_info)
                    if identifier:
                        authority, code = identifier
                        return f"{authority}:{code}"
        except (json.JSONDecodeError, KeyError):
            pass
    # Default to WGS84 per GeoParquet spec
    return "EPSG:4326"


def reproject_table(
    table: pa.Table,
    target_crs: str = "EPSG:4326",
    source_crs: str | None = None,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Reproject an Arrow Table to a different CRS.

    This is the table-centric version for the Python API.

    Args:
        table: Input PyArrow Table with geometry column
        target_crs: Target CRS (default: EPSG:4326)
        source_crs: Source CRS. If None, detected from table metadata.
        geometry_column: Geometry column name. If None, detected from metadata.

    Returns:
        New table with reprojected geometry
    """
    # Detect geometry column
    geom_col = geometry_column or _detect_geometry_column_from_table(table)

    # Detect or use source CRS
    effective_source_crs = source_crs or _detect_crs_from_table(table, geom_col)

    # Create connection and register table
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        con.register("__input_table", table)

        # Check if geometry column is BLOB type (needs conversion to GEOMETRY)
        geom_is_blob = False
        if geom_col in table.column_names:
            col_idx = table.column_names.index(geom_col)
            col_type = str(table.schema.field(col_idx).type)
            geom_is_blob = "large_binary" in col_type.lower() or "binary" in col_type.lower()

        # Create view with geometry conversion if needed
        source_table = "__input_table"
        if geom_is_blob:
            other_cols = [c for c in table.column_names if c != geom_col]
            col_defs = other_cols + [f'ST_GeomFromWKB("{geom_col}") AS "{geom_col}"']
            view_query = (
                f"CREATE VIEW __input_view AS SELECT {', '.join(col_defs)} FROM __input_table"
            )
            con.execute(view_query)
            source_table = "__input_view"

        # Build reprojection query
        query = f"""
            SELECT
                * EXCLUDE ("{geom_col}"),
                ST_Transform(
                    "{geom_col}",
                    '{effective_source_crs}',
                    '{target_crs}',
                    always_xy := true
                ) AS "{geom_col}"
            FROM {source_table}
        """

        result = con.execute(query).fetch_arrow_table()

        # Update geo metadata with new CRS
        if table.schema.metadata:
            new_metadata = dict(table.schema.metadata)
            if b"geo" in new_metadata:
                try:
                    geo_meta = json.loads(new_metadata[b"geo"].decode("utf-8"))
                    target_crs_projjson = parse_crs_string_to_projjson(target_crs, con)
                    if geom_col in geo_meta.get("columns", {}):
                        geo_meta["columns"][geom_col]["crs"] = target_crs_projjson
                    new_metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")
                    result = result.replace_schema_metadata(new_metadata)
                except (json.JSONDecodeError, KeyError):
                    pass
            else:
                result = result.replace_schema_metadata(table.schema.metadata)

        return result
    finally:
        con.close()


def _detect_source_crs(input_url: str, verbose: bool) -> str:
    """Detect source CRS from GeoParquet metadata.

    Args:
        input_url: Safe URL to input file
        verbose: Whether to print verbose output

    Returns:
        CRS string like "EPSG:4326"
    """
    # Try to get CRS from GeoParquet metadata
    crs_info = extract_crs_from_parquet(input_url, verbose=verbose)

    if crs_info:
        identifier = _extract_crs_identifier(crs_info)
        if identifier:
            authority, code = identifier
            return f"{authority}:{code}"

    # Default to WGS84 per GeoParquet spec (missing CRS = WGS84)
    if verbose:
        debug("No CRS found in metadata, assuming EPSG:4326 (WGS84)")
    return "EPSG:4326"


def _get_bbox_column_name(input_url: str, verbose: bool) -> str | None:
    """Get bbox column name if it exists.

    Args:
        input_url: Safe URL to input file
        verbose: Whether to print verbose output

    Returns:
        Bbox column name or None
    """
    bbox_info = check_bbox_structure(input_url, verbose=verbose)
    if bbox_info.get("has_bbox_column"):
        return bbox_info.get("bbox_column_name")
    return None


def reproject_impl(
    input_parquet: str,
    output_parquet: str | None = None,
    target_crs: str = "EPSG:4326",
    source_crs: str | None = None,
    overwrite: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    verbose: bool = False,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    on_progress: Callable[[str], None] | None = None,
) -> ReprojectResult:
    """
    Reproject a GeoParquet file to a different CRS using DuckDB.

    Args:
        input_parquet: Path to input GeoParquet file (local or remote URL)
        output_parquet: Path to output file. If None, generates name from input.
        target_crs: Target CRS (default: EPSG:4326)
        source_crs: Override source CRS. If None, detected from metadata.
        overwrite: If True and output_parquet is None, overwrite input file
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        verbose: Whether to print verbose output
        profile: AWS profile name for S3 operations
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
        on_progress: Optional callback for progress messages

    Returns:
        ReprojectResult with information about the operation

    Raises:
        ValueError: If CRS parsing fails or invalid parameters provided
        FileNotFoundError: If input file doesn't exist
        RuntimeError: If reprojection operation fails
    """

    def log(msg: str) -> None:
        if on_progress:
            on_progress(msg)
        elif verbose:
            info(msg)

    # Validate profile usage
    validate_profile_for_urls(profile, input_parquet, output_parquet)

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, input_parquet, output_parquet)

    # Get safe URL for input
    input_url = safe_file_url(input_parquet, verbose=verbose)

    # Create DuckDB connection with spatial extension
    con = get_duckdb_connection(
        load_spatial=True,
        load_httpfs=needs_httpfs(input_parquet),
    )

    try:
        # Detect geometry column
        geom_col = find_primary_geometry_column(input_parquet, verbose=verbose)
        log(f"Geometry column: {geom_col}")

        # Detect source CRS from metadata
        detected_crs = _detect_source_crs(input_url, verbose)

        # Use override if provided, otherwise use detected
        if source_crs is not None:
            info(f"Detected CRS: {detected_crs}")
            info(f"Overriding with source CRS: {source_crs}")
            effective_source_crs = source_crs
        else:
            effective_source_crs = detected_crs
            log(f"Source CRS: {effective_source_crs}")

        log(f"Target CRS: {target_crs}")

        # Get feature count
        count = con.execute(f"SELECT COUNT(*) FROM '{input_url}'").fetchone()[0]
        log(f"Features: {count:,}")

        # Check for existing bbox column to exclude (will be regenerated)
        bbox_col = _get_bbox_column_name(input_url, verbose)
        exclude_cols = [geom_col]
        if bbox_col:
            exclude_cols.append(bbox_col)
            if verbose:
                debug(f"Excluding bbox column '{bbox_col}' (will be regenerated)")
        exclude_clause = ", ".join(exclude_cols)

        # Build SQL query with ST_Transform
        # Use always_xy := true since GeoParquet uses lon/lat (x/y) axis order
        log("Reprojecting...")
        query = f"""
            SELECT
                * EXCLUDE ({exclude_clause}),
                ST_Transform(
                    {geom_col},
                    '{effective_source_crs}',
                    '{target_crs}',
                    always_xy := true
                ) AS {geom_col}
            FROM '{input_url}'
        """

        # Determine output path
        if output_parquet:
            out_path = Path(output_parquet).resolve()
        elif overwrite:
            out_path = Path(input_parquet).resolve()
        else:
            # Generate output name: input_epsg_4326.parquet
            input_path = Path(input_parquet)
            target_suffix = target_crs.replace(":", "_").lower()
            out_path = input_path.parent / f"{input_path.stem}_{target_suffix}.parquet"

        log(f"Output: {out_path}")

        # Validate compression settings
        compression, compression_level, _ = validate_compression_settings(
            compression, compression_level, verbose
        )

        # Get target CRS as PROJJSON for metadata
        target_crs_projjson = parse_crs_string_to_projjson(target_crs, con)

        # Handle in-place overwrite
        is_overwrite = str(out_path) == str(Path(input_parquet).resolve())

        if is_overwrite:
            # Write to temp file first, then replace
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                tmp_path = Path(tmp.name)

            try:
                write_parquet_with_metadata(
                    con,
                    query,
                    str(tmp_path),
                    original_metadata=None,
                    compression=compression,
                    compression_level=compression_level,
                    verbose=verbose,
                    profile=profile,
                    geoparquet_version=geoparquet_version,
                    input_crs=target_crs_projjson,
                )
                # Replace original with temp file
                shutil.move(str(tmp_path), str(out_path))
            except Exception:
                if tmp_path.exists():
                    tmp_path.unlink()
                raise
        else:
            # Write directly to output
            with remote_write_context(str(out_path), verbose=verbose) as (
                actual_output,
                is_remote,
            ):
                write_parquet_with_metadata(
                    con,
                    query,
                    actual_output,
                    original_metadata=None,
                    compression=compression,
                    compression_level=compression_level,
                    verbose=verbose,
                    profile=profile,
                    geoparquet_version=geoparquet_version,
                    input_crs=target_crs_projjson,
                )

                if is_remote:
                    upload_if_remote(
                        actual_output,
                        str(out_path),
                        profile=profile,
                        is_directory=False,
                        verbose=verbose,
                    )

        if verbose:
            success(f"Reprojected {count:,} features from {effective_source_crs} to {target_crs}")

        return ReprojectResult(
            output_path=out_path,
            source_crs=effective_source_crs,
            target_crs=target_crs,
            feature_count=count,
        )

    finally:
        con.close()


def _reproject_streaming(
    input_path: str,
    output_path: str | None,
    target_crs: str,
    source_crs: str | None,
    compression: str,
    compression_level: int | None,
    verbose: bool,
    profile: str | None,
    geoparquet_version: str | None,
) -> None:
    """Handle streaming input/output for reproject."""
    import os

    import pyarrow.parquet as pq

    # Suppress verbose when streaming to stdout
    if should_stream_output(output_path):
        verbose = False

    temp_input_file = None

    try:
        # If reading from stdin, write to temp file first
        if is_stdin(input_path):
            if verbose:
                debug("Reading Arrow IPC stream from stdin...")
            table = read_arrow_stream()
            temp_fd, temp_input_file = tempfile.mkstemp(suffix=".parquet")
            os.close(temp_fd)
            pq.write_table(table, temp_input_file)
            working_file = temp_input_file
        else:
            working_file = input_path

        # Get safe URL
        working_url = safe_file_url(working_file, verbose=False)

        # Create connection
        con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(working_file))

        try:
            # Detect geometry column
            geom_col = find_primary_geometry_column(working_file, verbose=False)

            # Detect source CRS
            detected_crs = _detect_source_crs(working_url, verbose=False)
            effective_source_crs = source_crs if source_crs else detected_crs

            # Check for existing bbox column to exclude
            bbox_col = _get_bbox_column_name(working_url, verbose=False)
            exclude_cols = [geom_col]
            if bbox_col:
                exclude_cols.append(bbox_col)
            exclude_clause = ", ".join(exclude_cols)

            # Build reprojection query
            query = f"""
                SELECT
                    * EXCLUDE ({exclude_clause}),
                    ST_Transform(
                        {geom_col},
                        '{effective_source_crs}',
                        '{target_crs}',
                        always_xy := true
                    ) AS {geom_col}
                FROM '{working_url}'
            """

            # Get original metadata for preservation
            from geoparquet_io.core.common import get_parquet_metadata

            metadata, _ = get_parquet_metadata(working_file, verbose=False)

            # Get target CRS projjson for metadata
            target_crs_projjson = parse_crs_string_to_projjson(target_crs, con)

            # Write output using stream_io
            write_output(
                con,
                query,
                output_path,
                original_metadata=metadata,
                compression=compression,
                compression_level=compression_level,
                verbose=verbose,
                profile=profile,
                geoparquet_version=geoparquet_version,
                input_crs=target_crs_projjson,
            )

            if not should_stream_output(output_path):
                success(f"Reprojected from {effective_source_crs} to {target_crs}: {output_path}")

        finally:
            con.close()

    finally:
        # Clean up temp input file
        if temp_input_file and os.path.exists(temp_input_file):
            os.remove(temp_input_file)


def reproject(
    input_parquet: str,
    output_parquet: str | None = None,
    target_crs: str = "EPSG:4326",
    source_crs: str | None = None,
    overwrite: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    verbose: bool = False,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    on_progress: Callable[[str], None] | None = None,
) -> ReprojectResult | None:
    """
    Reproject a GeoParquet file to a different CRS.

    Supports Arrow IPC streaming:
    - Input "-" reads from stdin
    - Output "-" or None (with piped stdout) streams to stdout

    Args:
        input_parquet: Path to input file (local, remote URL, or "-" for stdin)
        output_parquet: Path to output file, "-" for stdout, or None for auto-detect
        target_crs: Target CRS (default: EPSG:4326)
        source_crs: Override source CRS. If None, detected from metadata.
        overwrite: If True and output_parquet is None, overwrite input file
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        verbose: Whether to print verbose output
        profile: AWS profile name for S3 operations
        geoparquet_version: GeoParquet version to write
        on_progress: Optional callback for progress messages

    Returns:
        ReprojectResult with information, or None for streaming output
    """
    # Check for streaming mode
    is_streaming = is_stdin(input_parquet) or should_stream_output(output_parquet)

    if is_streaming:
        _reproject_streaming(
            input_parquet,
            output_parquet,
            target_crs,
            source_crs,
            compression,
            compression_level,
            verbose,
            profile,
            geoparquet_version,
        )
        return None

    # Use the original implementation for file-based mode
    return reproject_impl(
        input_parquet,
        output_parquet,
        target_crs,
        source_crs,
        overwrite,
        compression,
        compression_level,
        verbose,
        profile,
        geoparquet_version,
        on_progress,
    )
