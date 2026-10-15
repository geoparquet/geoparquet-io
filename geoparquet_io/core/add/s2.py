#!/usr/bin/env python3

"""
Add S2 cell ID column to GeoParquet files.

Uses DuckDB's geography extension to compute S2 cell IDs from geometry centroids.
S2 (Google's Spherical Geometry library) provides a hierarchical spatial index
that divides Earth's surface into cells at various levels (0-30).
"""

from __future__ import annotations

import pyarrow as pa

from geoparquet_io.core.common import add_computed_column
from geoparquet_io.core.constants import (
    DEFAULT_S2_COLUMN_NAME,
    DEFAULT_S2_LEVEL,
)
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
    require_community_extension,
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

# Named in the error when 'geography' is unavailable, so the message says which
# command stopped working rather than just which extension is missing (#737).
_S2_FEATURE = "gpio add s2"


def _load_geography_extension(con):
    """Load DuckDB geography extension for S2 support."""
    load_community_extension(con, "geography", feature=_S2_FEATURE)


def _create_geometry_view(con, table, geom_col):
    """Create view with BLOB-to-geometry conversion if needed.

    Returns:
        str: Source reference ('__input_table' or '__input_view')
    """
    columns_info = con.execute("DESCRIBE __input_table").fetchall()
    geom_is_blob = any(col[0] == geom_col and "BLOB" in col[1].upper() for col in columns_info)

    if geom_is_blob and geom_col in table.column_names:
        # Create view with geometry conversion
        # Quote all column names for safety with special characters
        other_cols = [quote_identifier(c) for c in table.column_names if c != geom_col]
        col_defs = other_cols + [
            f"ST_GeomFromWKB({quote_identifier(geom_col)}) AS {quote_identifier(geom_col)}"
        ]
        view_query = f"CREATE VIEW __input_view AS SELECT {', '.join(col_defs)} FROM __input_table"
        con.execute(view_query)
        return "__input_view"
    return "__input_table"


def _build_s2_select_query(table, source_ref, geom_col, s2_column_name, level, geom_ref=None):
    """Build SELECT query to add S2 column to table.

    Args:
        geom_ref: SQL geometry expression for keying (defaults to the raw column;
            pass a reprojected expression for non-CRS84 input).

    Returns:
        str: Complete SELECT query with S2 expression
    """
    if geom_ref is None:
        geom_ref = quote_identifier(geom_col)

    # Build S2 cell expression (keyed on lon/lat, reprojected when needed)
    s2_expr = f"""s2_cell_token(
        s2_cell_parent(
            s2_cellfromlonlat(
                ST_X(ST_Centroid({geom_ref})),
                ST_Y(ST_Centroid({geom_ref}))
            ),
            {level}
        )
    )"""

    # Build column list
    other_cols = [quote_identifier(c) for c in table.column_names if c != geom_col]
    select_cols = ", ".join(other_cols) if other_cols else ""

    # Build SELECT query
    if select_cols:
        return f"""
            SELECT {select_cols},
                   ST_AsWKB({quote_identifier(geom_col)}) AS {quote_identifier(geom_col)},
                   {s2_expr} AS {quote_identifier(s2_column_name)}
            FROM {source_ref}
        """
    else:
        return f"""
            SELECT ST_AsWKB({quote_identifier(geom_col)}) AS {quote_identifier(geom_col)},
                   {s2_expr} AS {quote_identifier(s2_column_name)}
            FROM {source_ref}
        """


def add_s2_table(
    table: pa.Table,
    s2_column_name: str = DEFAULT_S2_COLUMN_NAME,
    level: int = DEFAULT_S2_LEVEL,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Add an S2 cell ID column to an Arrow Table.

    This is the table-centric version for the Python API.

    Args:
        table: Input PyArrow Table
        s2_column_name: Name for the S2 column (default: 's2_cell')
        level: S2 level (0-30). Default: 13 (~1.2 km² cells)
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with S2 column added
    """
    # Validate level
    if not 0 <= level <= 30:
        raise ValueError(f"S2 level must be between 0 and 30, got {level}")

    # Nothing here works without 'geography'; say so before any DuckDB work (#737).
    require_community_extension("geography", feature=_S2_FEATURE)

    # Find geometry column
    geom_col = geometry_column or find_geometry_column_from_table(table)
    if not geom_col:
        geom_col = "geometry"

    # S2 keying expects lon/lat degrees; reproject non-CRS84 input first (#525).
    source_crs = crs_string_from_table(table, geom_col)

    # Register table and execute query
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        # Ensure ST_Transform (CRS-aware keying) emits lon/lat order.
        con.execute("SET geometry_always_xy = true;")

        _load_geography_extension(con)
        warn_planar_math("add_s2", table, geom_col)
        registered = register_arrow(con, "__input_table", table)

        source_ref = _create_geometry_view(con, table, geom_col)
        geom_ref = transform_geom_sql(quote_identifier(geom_col), source_crs)
        query = _build_s2_select_query(
            table, source_ref, geom_col, s2_column_name, level, geom_ref=geom_ref
        )
        result = con.execute(query).arrow().read_all()

        # Preserve metadata
        if table.schema.metadata:
            result = result.replace_schema_metadata(table.schema.metadata)

        result = registered.restore_edges(result)
        return result
    finally:
        con.close()


def _make_add_s2_query(
    source: str,
    geometry_column: str,
    s2_column_name: str,
    level: int,
    source_crs: str | None = None,
) -> str:
    """Build query to add S2 column to a source.

    ``source_crs`` reprojects a non-CRS84 input to lon/lat before centroid keying
    (#525); ``None`` (CRS84 / CRS-less) leaves the geometry untouched.
    """
    geom_ref = transform_geom_sql(quote_identifier(geometry_column), source_crs)
    # s2_cellfromlonlat returns a cell at level 30, use s2_cell_parent to get desired level
    s2_expr = f"""s2_cell_token(
        s2_cell_parent(
            s2_cellfromlonlat(
                ST_X(ST_Centroid({geom_ref})),
                ST_Y(ST_Centroid({geom_ref}))
            ),
            {level}
        )
    )"""
    return f"SELECT *, {s2_expr} AS {quote_identifier(s2_column_name)} FROM {source}"


def add_s2_column(
    input_parquet: str,
    output_parquet: str | None = None,
    s2_column_name: str = DEFAULT_S2_COLUMN_NAME,
    s2_level: int = DEFAULT_S2_LEVEL,
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
    Add an S2 cell ID column to a GeoParquet file.

    Supports Arrow IPC streaming:
    - Input "-" reads from stdin
    - Output "-" or None (with piped stdout) streams to stdout

    Computes S2 cell IDs based on geometry centroids using Google's S2
    hierarchical spherical grid system. The cell ID is stored as a
    token (hex string) for maximum portability.

    Args:
        input_parquet: Path to the input parquet file (local, remote URL, or "-" for stdin)
        output_parquet: Path to output file, "-" for stdout, or None for auto-detect
        s2_column_name: Name for the S2 column (default: 's2_cell')
        s2_level: S2 level (0-30)
                  Level 8: ~1,250 km², Level 13: ~1.2 km², Level 18: ~4,500 m²
                  Default: 13 (good balance for most use cases)
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

    # Validate level
    if not 0 <= s2_level <= 30:
        raise InvalidParameterError("level", f"must be between 0 and 30, got {s2_level}")

    # Fail before reading the input (or draining stdin) when the community
    # extension that computes S2 cells is unavailable for this DuckDB (#737).
    require_community_extension("geography", feature=_S2_FEATURE)

    warn_planar_math_for_file("add_s2", input_parquet)

    # Check for streaming mode (stdin input or stdout output)
    is_streaming = is_stdin(input_parquet) or should_stream_output(output_parquet)

    if is_streaming and not dry_run:
        _add_s2_streaming(
            input_parquet,
            output_parquet,
            s2_column_name,
            s2_level,
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
    require_single_file(input_parquet, "add s2")

    # Get geometry column for the SQL expression
    geom_col = find_primary_geometry_column(input_parquet, verbose)

    # S2 keying expects lon/lat degrees; reproject non-CRS84 input first (#525).
    geom_ref = transform_geom_sql(
        quote_identifier(geom_col), source_crs_string(input_parquet, verbose, geom_col)
    )

    # Define the S2 SQL expression (using token/string format for portability)
    # s2_cellfromlonlat returns a cell at level 30, use s2_cell_parent to get desired level
    sql_expression = f"""s2_cell_token(
        s2_cell_parent(
            s2_cellfromlonlat(
                ST_X(ST_Centroid({geom_ref})),
                ST_Y(ST_Centroid({geom_ref}))
            ),
            {s2_level}
        )
    )"""

    # Prepare S2 metadata for GeoParquet spec
    s2_metadata = {"covering": {"s2": {"column": s2_column_name, "level": s2_level}}}

    if not dry_run:
        progress(f"Adding S2 column '{s2_column_name}' (level {s2_level})...")

    # Use the generic helper
    add_computed_column(
        input_parquet=input_parquet,
        output_parquet=output_parquet,
        column_name=s2_column_name,
        sql_expression=sql_expression,
        extensions=["geography"],  # Load geography extension from DuckDB community
        dry_run=dry_run,
        verbose=verbose,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        dry_run_description=f"S2 cell ID at level {s2_level} (~{_get_level_size(s2_level)})",
        custom_metadata=s2_metadata,
        profile=profile,
        geoparquet_version=geoparquet_version,
        memory_limit=memory_limit,
    )

    if not dry_run:
        success(
            f"Successfully added S2 column '{s2_column_name}' "
            f"(level {s2_level}) to: {output_parquet}"
        )


def _add_s2_streaming(
    input_path: str,
    output_path: str | None,
    s2_column_name: str,
    level: int,
    verbose: bool,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
    geoparquet_version: str | None,
    memory_limit: str | None,
) -> None:
    """Handle streaming input/output for add_s2."""
    # Suppress verbose when streaming to stdout
    if should_stream_output(output_path):
        verbose = False

    def make_query(source: str, con, source_crs: str | None = None) -> str:
        """Build the add S2 query for streaming source.

        ``source_crs`` is the streamed input's CRS (from its Arrow geo metadata),
        used to reproject non-CRS84 input to lon/lat before keying (#525).
        """
        # Load geography extension
        _load_geography_extension(con)

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

        return _make_add_s2_query(source, geom_col, s2_column_name, level, source_crs)

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
            f"Successfully added S2 column '{s2_column_name}' (level {level}) to: {output_path}"
        )


def _get_level_size(level):
    """Get approximate cell size for a given S2 level."""
    # S2 cell areas at various levels (approximate, varies by location)
    # Formula: Earth surface ~510M km², 6 base cells, each level divides by 4
    sizes = {
        0: "85M km²",
        1: "21M km²",
        2: "5.3M km²",
        3: "1.3M km²",
        4: "324,000 km²",
        5: "81,000 km²",
        6: "20,000 km²",
        7: "5,100 km²",
        8: "1,250 km²",
        9: "313 km²",
        10: "78 km²",
        11: "20 km²",
        12: "4.9 km²",
        13: "1.2 km²",
        14: "0.31 km²",
        15: "77,000 m²",
        16: "19,000 m²",
        17: "4,800 m²",
        18: "1,200 m²",
        19: "300 m²",
        20: "75 m²",
        21: "19 m²",
        22: "4.7 m²",
        23: "1.2 m²",
        24: "0.29 m²",
        25: "0.07 m²",
        26: "0.02 m²",
        27: "0.005 m²",
        28: "0.001 m²",
        29: "0.0003 m²",
        30: "0.00007 m²",
    }
    return sizes.get(level, f"level {level}")


if __name__ == "__main__":
    add_s2_column()
