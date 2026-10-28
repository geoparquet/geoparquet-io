#!/usr/bin/env python3

from __future__ import annotations

import pyarrow as pa

from geoparquet_io.core.common import add_computed_column
from geoparquet_io.core.constants import DEFAULT_A5_COLUMN_NAME
from geoparquet_io.core.crs_utils import (
    crs_string_from_table,
    source_crs_string,
    transform_geom_sql,
)
from geoparquet_io.core.duckdb_utils import (
    get_duckdb_connection,
    load_community_extension,
    quote_identifier,
    register_arrow,
)
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.file_utils import handle_output_overwrite
from geoparquet_io.core.geometry_detection import (
    STANDARD_GEOMETRY_NAMES,
    find_primary_geometry_column,
)
from geoparquet_io.core.geometry_repair import warn_planar_math, warn_planar_math_for_file
from geoparquet_io.core.logging_config import configure_verbose, debug, progress, success
from geoparquet_io.core.partition.reader import require_single_file
from geoparquet_io.core.stream_io import execute_transform
from geoparquet_io.core.streaming import (
    find_geometry_column_from_table,
    is_stdin,
    should_stream_output,
)


def add_a5_table(
    table: pa.Table,
    a5_column_name: str = DEFAULT_A5_COLUMN_NAME,
    resolution: int = 15,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Add an A5 cell ID column to an Arrow Table.

    This is the table-centric version for the Python API.

    Args:
        table: Input PyArrow Table
        a5_column_name: Name for the A5 column (default: 'a5_cell')
        resolution: A5 resolution level (0-30). Default: 15
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with A5 column added
    """
    # Find geometry column
    geom_col = geometry_column or find_geometry_column_from_table(table)
    if not geom_col:
        geom_col = "geometry"

    # Validate resolution
    if not 0 <= resolution <= 30:
        raise ValueError(f"A5 resolution must be between 0 and 30, got {resolution}")

    # A5 keying expects lon/lat degrees; reproject non-CRS84 input first (#525).
    source_crs = crs_string_from_table(table, geom_col)

    # Register table and execute query
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        # Ensure ST_Transform (CRS-aware keying) emits lon/lat order.
        con.execute("SET geometry_always_xy = true;")

        # Load A5 extension
        load_community_extension(con, "a5")

        warn_planar_math("add_a5", table, geom_col)
        registered = register_arrow(con, "__input_table", table)

        # Check if geometry column is BLOB (needs conversion)
        columns_info = con.execute("DESCRIBE __input_table").fetchall()
        geom_is_blob = any(col[0] == geom_col and "BLOB" in col[1].upper() for col in columns_info)

        if geom_is_blob and geom_col in table.column_names:
            # Create view with geometry conversion
            # Quote all column names for safety with special characters
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

        # Build A5 column query (reproject to lon/lat when source is non-CRS84)
        geom_ref = transform_geom_sql(quote_identifier(geom_col), source_crs)
        a5_expr = f"""a5_lonlat_to_cell(
            ST_X(ST_Centroid({geom_ref})),
            ST_Y(ST_Centroid({geom_ref})),
            {resolution}
        )"""

        # Get non-geometry columns
        other_cols = [quote_identifier(c) for c in table.column_names if c != geom_col]
        select_cols = ", ".join(other_cols) if other_cols else ""

        # Build SELECT with geometry converted back to WKB
        if select_cols:
            query = f"""
                SELECT {select_cols},
                       ST_AsWKB({quote_identifier(geom_col)}) AS {quote_identifier(geom_col)},
                       {a5_expr} AS {quote_identifier(a5_column_name)}
                FROM {source_ref}
            """
        else:
            query = f"""
                SELECT ST_AsWKB({quote_identifier(geom_col)}) AS {quote_identifier(geom_col)},
                       {a5_expr} AS {quote_identifier(a5_column_name)}
                FROM {source_ref}
            """
        result = con.execute(query).arrow().read_all()

        # Preserve metadata
        if table.schema.metadata:
            result = result.replace_schema_metadata(table.schema.metadata)

        result = registered.restore_edges(result)
        return result
    finally:
        con.close()


def _make_add_a5_query(
    source: str,
    geometry_column: str,
    a5_column_name: str,
    resolution: int,
    source_crs: str | None = None,
) -> str:
    """Build query to add A5 column to a source.

    ``source_crs`` reprojects a non-CRS84 input to lon/lat before centroid keying
    (#525); ``None`` (CRS84 / CRS-less) leaves the geometry untouched.
    """
    geom_ref = transform_geom_sql(quote_identifier(geometry_column), source_crs)
    a5_expr = f"""a5_lonlat_to_cell(
        ST_X(ST_Centroid({geom_ref})),
        ST_Y(ST_Centroid({geom_ref})),
        {resolution}
    )"""
    return f"SELECT *, {a5_expr} AS {quote_identifier(a5_column_name)} FROM {source}"


def add_a5_column(
    input_parquet: str,
    output_parquet: str | None = None,
    a5_column_name: str = DEFAULT_A5_COLUMN_NAME,
    a5_resolution: int = 15,
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
) -> None:
    """
    Add an A5 cell ID column to a GeoParquet file.

    Supports Arrow IPC streaming:
    - Input "-" reads from stdin
    - Output "-" or None (with piped stdout) streams to stdout

    Computes A5 cell IDs based on geometry centroids using the A5
    discrete global grid system. The cell ID is stored as a UBIGINT
    (unsigned 64-bit integer) for efficient storage and indexing.

    Args:
        input_parquet: Path to the input parquet file (local, remote URL, or "-" for stdin)
        output_parquet: Path to output file, "-" for stdout, or None for auto-detect
        a5_column_name: Name for the A5 column (default: 'a5_cell')
        a5_resolution: A5 resolution level (0-30)
                      Res 10: ~40.7 km², Res 15: ~39.4 m², Res 20: ~38.5 mm²,
                      Res 25: ~38 μm², Res 30: ~37 nm²
                      Default: 15 (good balance for most use cases)
        dry_run: Whether to print SQL commands without executing them
        verbose: Whether to print verbose output
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
        memory_limit: DuckDB memory limit for the write (e.g., '2GB', '512MB')
    """
    # Configure logging verbosity
    configure_verbose(verbose)

    # Validate resolution
    if not 0 <= a5_resolution <= 30:
        raise InvalidParameterError("resolution", f"must be between 0 and 30, got {a5_resolution}")

    warn_planar_math_for_file("add_a5", input_parquet)

    # Check for streaming mode (stdin input or stdout output)
    is_streaming = is_stdin(input_parquet) or should_stream_output(output_parquet)

    if is_streaming and not dry_run:
        _add_a5_streaming(
            input_parquet,
            output_parquet,
            a5_column_name,
            a5_resolution,
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
    # Check if output file exists and handle overwrite (fixes issue #278)
    handle_output_overwrite(output_parquet, overwrite, input_parquet)

    # Check for partition input (not supported)
    require_single_file(input_parquet, "add a5")

    # Get geometry column for the SQL expression
    geom_col = find_primary_geometry_column(input_parquet, verbose)

    # A5 keying expects lon/lat degrees; reproject non-CRS84 input first (#525).
    geom_ref = transform_geom_sql(
        quote_identifier(geom_col), source_crs_string(input_parquet, verbose, geom_col)
    )

    # Define the A5 SQL expression
    sql_expression = f"""a5_lonlat_to_cell(
        ST_X(ST_Centroid({geom_ref})),
        ST_Y(ST_Centroid({geom_ref})),
        {a5_resolution}
    )"""

    # Prepare A5 metadata for GeoParquet spec
    a5_metadata = {"covering": {"a5": {"column": a5_column_name, "resolution": a5_resolution}}}

    if not dry_run:
        progress(f"Adding A5 column '{a5_column_name}' (resolution {a5_resolution})...")

    # Use the generic helper
    add_computed_column(
        input_parquet=input_parquet,
        output_parquet=output_parquet,
        column_name=a5_column_name,
        sql_expression=sql_expression,
        extensions=["a5"],  # Load A5 extension from DuckDB community
        dry_run=dry_run,
        verbose=verbose,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        dry_run_description=f"A5 cell ID at resolution {a5_resolution} (~{_get_resolution_size(a5_resolution)})",
        custom_metadata=a5_metadata,
        profile=profile,
        geoparquet_version=geoparquet_version,
        memory_limit=memory_limit,
    )

    if not dry_run:
        success(
            f"Successfully added A5 column '{a5_column_name}' "
            f"(resolution {a5_resolution}) to: {output_parquet}"
        )


def _add_a5_streaming(
    input_path: str,
    output_path: str | None,
    a5_column_name: str,
    resolution: int,
    verbose: bool,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
    geoparquet_version: str | None,
    memory_limit: str | None,
) -> None:
    """Handle streaming input/output for add_a5."""
    # Suppress verbose when streaming to stdout
    if should_stream_output(output_path):
        verbose = False

    def make_query(source: str, con, source_crs: str | None = None) -> str:
        """Build the add A5 query for streaming source.

        ``source_crs`` is the streamed input's CRS (from its Arrow geo metadata),
        used to reproject non-CRS84 input to lon/lat before keying (#525).
        """
        # Load A5 extension
        load_community_extension(con, "a5")

        # Get column names from query result
        sample = con.execute(f"SELECT * FROM {source} LIMIT 0").description
        col_names = [col[0] for col in sample]

        # Find geometry column from common names
        geom_col = None
        for name in STANDARD_GEOMETRY_NAMES:
            if name in col_names:
                geom_col = name
                break
        if not geom_col:
            geom_col = "geometry"

        if verbose:
            debug(f"Using geometry column: {geom_col}")

        return _make_add_a5_query(source, geom_col, a5_column_name, resolution, source_crs)

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
        memory_limit=memory_limit,
    )

    if not should_stream_output(output_path):
        success(
            f"Successfully added A5 column '{a5_column_name}' "
            f"(resolution {resolution}) to: {output_path}"
        )


def _get_resolution_size(resolution):
    """Get approximate cell size for a given A5 resolution."""
    sizes = {
        0: "42.5M km²",
        1: "10.6M km²",
        2: "2.7M km²",
        3: "666K km²",
        4: "167K km²",
        5: "41.7K km²",
        6: "10.4K km²",
        7: "2,600 km²",
        8: "650 km²",
        9: "163 km²",
        10: "40.7 km²",
        11: "10.2 km²",
        12: "2.5 km²",
        13: "630 m²",
        14: "158 m²",
        15: "39.4 m²",
        16: "9.9 m²",
        17: "2.5 m²",
        18: "616 mm²",
        19: "154 mm²",
        20: "38.5 mm²",
        21: "9.6 mm²",
        22: "2.4 mm²",
        23: "0.6 mm²",
        24: "0.15 mm²",
        25: "38 μm²",
        26: "9.5 μm²",
        27: "2.4 μm²",
        28: "0.6 μm²",
        29: "0.15 μm²",
        30: "37 nm²",
    }
    return sizes.get(resolution, f"resolution {resolution}")
