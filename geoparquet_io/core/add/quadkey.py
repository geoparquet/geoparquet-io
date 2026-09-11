#!/usr/bin/env python3

from __future__ import annotations

import mercantile
import pyarrow as pa

from geoparquet_io.core.common import (
    get_bbox_advice,
    get_parquet_metadata,
    write_parquet_with_metadata,
)
from geoparquet_io.core.constants import DEFAULT_QUADKEY_COLUMN_NAME, DEFAULT_QUADKEY_RESOLUTION
from geoparquet_io.core.crs_utils import (
    crs_string_from_geo_meta,
    crs_string_from_table,
    get_crs_display_name,
    parse_geo_metadata_from_schema,
    source_crs_string,
    transform_geom_sql,
)
from geoparquet_io.core.duckdb_metadata import get_column_names, get_geo_metadata
from geoparquet_io.core.duckdb_utils import (
    get_duckdb_connection,
    quote_identifier,
    sql_path,
)
from geoparquet_io.core.exceptions import GeoParquetError, InvalidParameterError
from geoparquet_io.core.file_utils import handle_output_overwrite, resolve_file_url
from geoparquet_io.core.geometry_detection import (
    STANDARD_GEOMETRY_NAMES,
    find_primary_geometry_column,
)
from geoparquet_io.core.logging_config import (
    configure_verbose,
    debug,
    info,
    progress,
    success,
    warn,
)
from geoparquet_io.core.remote import (
    _sanitize_url_for_logging,
    is_remote_url,
    needs_httpfs,
    setup_aws_profile_if_needed,
    validate_profile_for_urls,
)
from geoparquet_io.core.stream_io import open_input, write_output
from geoparquet_io.core.streaming import (
    find_geometry_column_from_table,
    is_stdin,
    should_stream_output,
)


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


def _validate_crs_from_geo_metadata(
    geo_meta: dict | None,
    geom_col: str,
    verbose: bool,
    source_description: str = "data",
) -> None:
    """
    Validate CRS from geo metadata dictionary.

    Common helper used by file-based, streaming, and table-based paths.

    Args:
        geo_meta: Parsed geo metadata dict (from GeoParquet schema)
        geom_col: Name of the geometry column
        verbose: Whether to print debug output
        source_description: Description for error messages (e.g., "file", "stream", "table")

    Raises:
        GeoParquetError: If CRS is detected as projected
    """
    if not geo_meta:
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
        raise GeoParquetError(
            f"Quadkeys require geographic coordinates (lat/lon), but this {source_description} "
            f"uses a projected CRS: {crs_name}\n\n"
            f"Reproject to WGS84 first using:\n"
            f"  gpio convert reproject <input> <output> --dst-crs EPSG:4326"
        )

    if verbose and is_geographic:
        debug("CRS validated as geographic (lat/lon coordinates)")


def _validate_crs_for_quadkey(input_parquet: str, geom_col: str, verbose: bool) -> None:
    """
    Validate that the file's CRS is geographic (WGS84/CRS84).

    Quadkeys require lat/lon coordinates. Raises ClickException if CRS is projected.
    """

    # Get CRS from GeoParquet metadata
    geo_meta = get_geo_metadata(input_parquet)
    _validate_crs_from_geo_metadata(geo_meta, geom_col, verbose, source_description="file")


def _parse_geo_metadata_from_schema(metadata: dict | None) -> dict | None:
    """Parse geo metadata from schema metadata bytes dict (shared helper)."""
    return parse_geo_metadata_from_schema(metadata)


def _streaming_source_crs(
    input_path: str, metadata: dict | None, geom_col: str, verbose: bool
) -> str | None:
    """Detect the streaming input's CRS as an ``"AUTH:CODE"`` transform string.

    Returns ``None`` for CRS84/default/CRS-less input (no reprojection needed).

    - File input streamed to stdout (``gpio add quadkey file.parquet -``): detect
      from the file, covering both the GeoParquet ``geo`` metadata and the
      parquet-geo native geometry type — same as the file-based path (#530).
    - True stdin: detect from the ``geo`` metadata carried on the Arrow stream.
    """
    if not is_stdin(input_path):
        return source_crs_string(input_path, verbose)
    geo_meta = _parse_geo_metadata_from_schema(metadata)
    return crs_string_from_geo_meta(geo_meta, geom_col)


def _lat_lon_to_quadkey(lat: float, lon: float, level: int) -> str:
    """Convert latitude and longitude to a quadkey string using mercantile."""
    tile = mercantile.tile(lon, lat, level)
    return mercantile.quadkey(tile)


def add_quadkey_table(
    table: pa.Table,
    quadkey_column_name: str = DEFAULT_QUADKEY_COLUMN_NAME,
    resolution: int = DEFAULT_QUADKEY_RESOLUTION,
    use_centroid: bool = False,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Add a quadkey column to an Arrow Table.

    This is the table-centric version for the Python API.

    Args:
        table: Input PyArrow Table
        quadkey_column_name: Name for the quadkey column (default: 'quadkey')
        resolution: Quadkey zoom level (0-23). Default: 13
        use_centroid: Force using geometry centroid even if bbox exists
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with quadkey column added

    Raises:
        ValueError: If resolution is not an integer between 0 and 23
        GeoParquetError: If CRS is detected as projected (quadkeys require lat/lon)
    """
    # Validate resolution before any DuckDB operations
    resolution = int(resolution)
    if resolution < 0 or resolution > 23:
        raise ValueError(f"resolution must be between 0 and 23 inclusive, got {resolution}")

    # Find geometry column
    geom_col = geometry_column or find_geometry_column_from_table(table)
    if not geom_col:
        geom_col = "geometry"

    # Quadkeys require lon/lat degrees. Reproject a known non-CRS84 CRS (#525);
    # otherwise keep the guard (rejects projected input we can't identify, and
    # passes through default/unknown which is assumed WGS84).
    geo_meta = _parse_geo_metadata_from_schema(table.schema.metadata)
    source_crs = crs_string_from_table(table, geom_col)
    needs_transform = source_crs is not None
    if not needs_transform:
        _validate_crs_from_geo_metadata(
            geo_meta, geom_col, verbose=False, source_description="table"
        )

    # Check if bbox column exists. A stored bbox is in the input CRS, so it can't
    # be used when we need to reproject — fall back to the (reprojected) centroid.
    use_bbox = False
    bbox_col = None
    if not use_centroid and not needs_transform:
        for name in ["bbox", "bounds", "bounding_box"]:
            if name in table.column_names:
                use_bbox = True
                bbox_col = name
                break

    # Register table and execute query using context manager for safe cleanup
    with get_duckdb_connection(load_spatial=True, load_httpfs=False) as con:
        # Ensure ST_Transform (CRS-aware keying) emits lon/lat order.
        con.execute("SET geometry_always_xy = true;")

        # Register Python UDF
        con.create_function(
            "lat_lon_to_quadkey",
            _lat_lon_to_quadkey,
            ["DOUBLE", "DOUBLE", "INTEGER"],
            "VARCHAR",
        )

        con.register("__input_table", table)

        # Check if geometry column is BLOB (needs conversion)
        columns_info = con.execute("DESCRIBE __input_table").fetchall()
        geom_is_blob = any(col[0] == geom_col and "BLOB" in col[1].upper() for col in columns_info)

        if geom_is_blob and geom_col in table.column_names:
            # Quote column names to handle special characters (colons, spaces, etc.)
            other_cols = [quote_identifier(c) for c in table.column_names if c != geom_col]
            col_defs = other_cols + [
                f"ST_GeomFromWKB({quote_identifier(geom_col)}) AS {quote_identifier(geom_col)}"
            ]
            view_query = (
                f"CREATE VIEW __input_view AS SELECT {', '.join(col_defs)} FROM __input_table"
            )
            con.execute(view_query)
            source_ref = "__input_view"
        else:
            source_ref = "__input_table"

        # Build lat/lon expressions (reproject to lon/lat when source is non-CRS84)
        if use_bbox and bbox_col:
            lat_expr = (
                f"(({quote_identifier(bbox_col)}.ymin + {quote_identifier(bbox_col)}.ymax) / 2.0)"
            )
            lon_expr = (
                f"(({quote_identifier(bbox_col)}.xmin + {quote_identifier(bbox_col)}.xmax) / 2.0)"
            )
        else:
            geom_ref = transform_geom_sql(quote_identifier(geom_col), source_crs)
            lat_expr = f"ST_Y(ST_Centroid({geom_ref}))"
            lon_expr = f"ST_X(ST_Centroid({geom_ref}))"

        # Get non-geometry columns
        other_cols = [quote_identifier(c) for c in table.column_names if c != geom_col]
        select_cols = ", ".join(other_cols) if other_cols else ""

        # Build SELECT with geometry converted back to WKB
        if select_cols:
            query = f"""
                SELECT {select_cols},
                       ST_AsWKB({quote_identifier(geom_col)}) AS {quote_identifier(geom_col)},
                       lat_lon_to_quadkey({lat_expr}, {lon_expr}, {resolution}) AS {quote_identifier(quadkey_column_name)}
                FROM {source_ref}
            """
        else:
            query = f"""
                SELECT ST_AsWKB({quote_identifier(geom_col)}) AS {quote_identifier(geom_col)},
                       lat_lon_to_quadkey({lat_expr}, {lon_expr}, {resolution}) AS {quote_identifier(quadkey_column_name)}
                FROM {source_ref}
            """
        result = con.execute(query).arrow().read_all()

        # Preserve metadata
        if table.schema.metadata:
            result = result.replace_schema_metadata(table.schema.metadata)

        return result


def add_quadkey_column(
    input_parquet: str,
    output_parquet: str | None = None,
    quadkey_column_name: str = DEFAULT_QUADKEY_COLUMN_NAME,
    resolution: int = DEFAULT_QUADKEY_RESOLUTION,
    use_centroid: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    overwrite: bool = False,
    memory_limit: str | None = None,
    allow_schema_diff: bool = False,
    hive_input: bool | None = None,
) -> None:
    """
    Add a quadkey column to a GeoParquet file.

    Computes quadkey tile IDs based on geometry location. By default, uses the
    bbox column midpoint if available, otherwise falls back to geometry centroid.

    Supports Arrow IPC streaming:
    - Input "-" reads from stdin
    - Output "-" or None (with piped stdout) streams to stdout

    Args:
        input_parquet: Path to the input parquet file (local, remote URL, or "-" for stdin)
        output_parquet: Path to output file, "-" for stdout, or None for auto-detect
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
        memory_limit: DuckDB memory limit for the write (e.g., '2GB', '512MB')
        allow_schema_diff: Read a multi-file input as the union of its columns
            (DuckDB ``union_by_name``) rather than the first file's schema
        hive_input: Read a multi-file input with hive partitioning on, so the
            ``key=value`` directory names come through as columns
    """
    # Check for streaming mode (stdin input or stdout output)
    is_streaming = is_stdin(input_parquet) or should_stream_output(output_parquet)

    if is_streaming and not dry_run:
        _add_quadkey_streaming(
            input_parquet,
            output_parquet,
            quadkey_column_name,
            resolution,
            use_centroid,
            verbose,
            compression,
            compression_level,
            row_group_size_mb,
            row_group_rows,
            profile,
            geoparquet_version,
            memory_limit=memory_limit,
        )
        return

    # File-based mode
    _add_quadkey_file_based(
        input_parquet,
        output_parquet,
        quadkey_column_name,
        resolution,
        use_centroid,
        dry_run,
        verbose,
        compression,
        compression_level,
        row_group_size_mb,
        row_group_rows,
        profile,
        geoparquet_version,
        overwrite,
        memory_limit=memory_limit,
        allow_schema_diff=allow_schema_diff,
        hive_input=hive_input,
    )


def _add_quadkey_streaming(
    input_path: str,
    output_path: str | None,
    quadkey_column_name: str,
    resolution: int,
    use_centroid: bool,
    verbose: bool,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
    geoparquet_version: str | None,
    memory_limit: str | None,
) -> None:
    """Handle streaming input/output for add_quadkey."""
    # Suppress verbose when streaming to stdout
    if should_stream_output(output_path):
        verbose = False

    # Validate resolution
    if not 0 <= resolution <= 23:
        raise InvalidParameterError("resolution", f"must be between 0 and 23, got {resolution}")

    with open_input(input_path, verbose=verbose) as (source, metadata, is_stream, con):
        # Ensure ST_Transform (CRS-aware keying) emits lon/lat order.
        con.execute("SET geometry_always_xy = true;")

        # Register Python UDF for quadkey generation
        con.create_function(
            "lat_lon_to_quadkey",
            _lat_lon_to_quadkey,
            ["DOUBLE", "DOUBLE", "INTEGER"],
            "VARCHAR",
        )

        # Get column names from query result (works with both table names and read_parquet)
        sample = con.execute(f"SELECT * FROM {source} LIMIT 0").description
        col_names = [col[0] for col in sample]

        # Find geometry column
        geom_col = None
        for name in STANDARD_GEOMETRY_NAMES:
            if name in col_names:
                geom_col = name
                break
        if not geom_col:
            geom_col = "geometry"

        # Quadkeys require lon/lat degrees. Reproject a known non-CRS84 CRS (#525,
        # #530); otherwise keep the guard (rejects projected input we can't
        # identify, passes through default/unknown which is assumed WGS84).
        source_crs = _streaming_source_crs(input_path, metadata, geom_col, verbose)
        needs_transform = source_crs is not None
        if not needs_transform:
            geo_meta = _parse_geo_metadata_from_schema(metadata)
            _validate_crs_from_geo_metadata(
                geo_meta, geom_col, verbose, source_description="stream"
            )

        # Check for bbox column. A stored bbox is in the input CRS, so it can't be
        # used when we need to reproject — fall back to the (reprojected) centroid.
        bbox_col = None
        if not use_centroid and not needs_transform:
            for name in ["bbox", "bounds", "bounding_box"]:
                if name in col_names:
                    bbox_col = name
                    break

        # Build lat/lon expressions (reproject to lon/lat when source is non-CRS84)
        if bbox_col:
            lat_expr = (
                f"(({quote_identifier(bbox_col)}.ymin + {quote_identifier(bbox_col)}.ymax) / 2.0)"
            )
            lon_expr = (
                f"(({quote_identifier(bbox_col)}.xmin + {quote_identifier(bbox_col)}.xmax) / 2.0)"
            )
        else:
            geom_ref = transform_geom_sql(quote_identifier(geom_col), source_crs)
            lat_expr = f"ST_Y(ST_Centroid({geom_ref}))"
            lon_expr = f"ST_X(ST_Centroid({geom_ref}))"

        query = f"""
            SELECT *,
                   lat_lon_to_quadkey({lat_expr}, {lon_expr}, {resolution}) AS {quote_identifier(quadkey_column_name)}
            FROM {source}
        """

        if verbose:
            debug(f"Streaming quadkey query: {query}")

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
            success(
                f"Successfully added quadkey column '{quadkey_column_name}' "
                f"(zoom level {resolution}) to: {output_path}"
            )


def _quadkey_from_clause(path: str, allow_schema_diff: bool, hive_input: bool | None) -> str:
    """The FROM target for the add, RAW path escaped exactly once by sql_path.

    Options are attached only when a caller asked for them, so the ordinary
    single-file add keeps reading through a bare path literal exactly as it
    always has. `sort quadkey` is the caller that needs them: its auto-add step
    is the FIRST read of a directory, so a union or a hive setting that only
    reached the sort afterwards would arrive one read too late (#867).
    """
    options = []
    if hive_input:
        options.append("hive_partitioning=true")
    if allow_schema_diff:
        options.append("union_by_name=true")
    if not options:
        return sql_path(path)
    return f"read_parquet({sql_path(path)}, {', '.join(options)})"


def _add_quadkey_file_based(
    input_parquet: str,
    output_parquet: str | None,
    quadkey_column_name: str,
    resolution: int,
    use_centroid: bool,
    dry_run: bool,
    verbose: bool,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
    geoparquet_version: str | None,
    overwrite: bool,
    memory_limit: str | None,
    allow_schema_diff: bool = False,
    hive_input: bool | None = None,
) -> None:
    """Handle file-based add_quadkey operation."""
    configure_verbose(verbose)

    # Check if output file exists and handle overwrite (fixes issue #278)
    handle_output_overwrite(output_parquet, overwrite)

    # Validate resolution
    if not 0 <= resolution <= 23:
        raise InvalidParameterError("resolution", f"must be between 0 and 23, got {resolution}")

    # Validate profile is only used with S3
    validate_profile_for_urls(profile, input_parquet, output_parquet)

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, input_parquet, output_parquet)

    # RAW path: shown to the user in the dry-run header below, and escaped at
    # the SQL boundary by sql_path (#802).
    input_file_path = resolve_file_url(input_parquet, verbose)

    # Get geometry column
    geom_col = find_primary_geometry_column(input_parquet, verbose)

    # Quadkeys require lon/lat degrees. Reproject a known non-CRS84 CRS (#525);
    # otherwise keep the guard (rejects projected input we can't identify, and
    # passes through default/unknown which is assumed WGS84).
    source_crs = source_crs_string(input_parquet, verbose)
    needs_transform = source_crs is not None
    if not needs_transform:
        _validate_crs_for_quadkey(input_parquet, geom_col, verbose)

    # Check if column already exists (skip in dry-run)
    if not dry_run:
        column_names = get_column_names(input_parquet)
        if quadkey_column_name in column_names:
            raise GeoParquetError(
                f"Column '{quadkey_column_name}' already exists in the file. "
                f"Please choose a different name."
            )

    # Determine whether to use bbox or centroid. A stored bbox column is in the
    # input CRS, so it can't be used when we need to reproject — fall back to the
    # (reprojected) centroid in that case.
    use_bbox = False
    bbox_col = None
    if not use_centroid and not needs_transform:
        bbox_advice = get_bbox_advice(input_parquet, "bounds_calculation", verbose)
        if bbox_advice["has_bbox_column"]:
            use_bbox = True
            bbox_col = bbox_advice["bbox_column_name"]
            if verbose:
                debug(f"Using bbox column '{bbox_col}' for quadkey calculation")
        elif bbox_advice["needs_warning"]:
            warn(bbox_advice["message"] + " - using geometry centroid for quadkey calculation")
            for suggestion in bbox_advice["suggestions"]:
                info(f"Tip: {suggestion}")

    # Dry-run mode header
    if dry_run:
        warn("\n=== DRY RUN MODE - SQL Commands that would be executed ===\n")
        display_input = (
            _sanitize_url_for_logging(input_file_path)
            if is_remote_url(input_file_path)
            else input_file_path
        )
        display_output = (
            _sanitize_url_for_logging(output_parquet)
            if is_remote_url(output_parquet)
            else output_parquet
        )
        info(f"-- Input file: {display_input}")
        info(f"-- Output file: {display_output}")
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
    # Ensure ST_Transform (CRS-aware keying) emits lon/lat order.
    con.execute("SET geometry_always_xy = true;")

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
            quoted_bbox = quote_identifier(bbox_col)
            lat_expr = f"(({quoted_bbox}.ymin + {quoted_bbox}.ymax) / 2.0)"
            lon_expr = f"(({quoted_bbox}.xmin + {quoted_bbox}.xmax) / 2.0)"
        else:
            geom_ref = transform_geom_sql(quote_identifier(geom_col), source_crs)
            lat_expr = f"ST_Y(ST_Centroid({geom_ref}))"
            lon_expr = f"ST_X(ST_Centroid({geom_ref}))"

        # Build SELECT query with new column
        query = f"""
            SELECT *,
                   lat_lon_to_quadkey({lat_expr}, {lon_expr}, {resolution}) AS {quote_identifier(quadkey_column_name)}
            FROM {_quadkey_from_clause(input_file_path, allow_schema_diff, hive_input)}
        """

        if verbose:
            debug(f"Query: {query}")

        if not dry_run:
            progress(f"Adding quadkey column '{quadkey_column_name}' (zoom level {resolution})...")

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
            # The rows this write reads are the input's, so the input is the
            # witness auto mode resolves the output version from. Without it a
            # native-geo-only input was re-encoded as 1.1 WKB and lost the CRS
            # its GEOMETRY logical type carried -- which `gpio sort quadkey`
            # then read back as its data (#993).
            input_file=input_parquet,
            memory_limit=memory_limit,
        )

        success(
            f"Successfully added quadkey column '{quadkey_column_name}' "
            f"(zoom level {resolution}) to: {output_parquet}"
        )

    finally:
        con.close()


if __name__ == "__main__":
    add_quadkey_column()
