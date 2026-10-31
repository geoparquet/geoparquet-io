"""What is left of the old grab-bag after the split.

This module grew to 4,138 lines with 50 importers, and the deferred
(function-level) imports written across ``core/`` to dodge cycles through it
were the symptom. Everything that answered a single question moved out to a
module named for that question -- ``arrow_geo_metadata``, ``arrow_types``,
``bbox_structure``, ``compression``, ``derive_geo_from_file``, ``file_type``,
``format_writers``, ``sizing``, ``write_funnels`` -- and the compatibility
re-exports that kept the old import paths alive are gone: every caller now
imports from the owner. The ``core-common-split-layers`` import-linter contract
keeps it that way -- this module is the top of that stack, so nothing below it
may import it back -- and ``tests/test_common_split_guards.py`` refuses a new
re-export shim.

What remains is the stage-C residue, each block with a home already named in
#1010's plan: ``get_parquet_metadata``, ``add_computed_column``/``add_bbox`` and
``should_skip_bbox``. The geometry-type, dimension and bounds helpers went to
their owner, ``geo_metadata`` (#1367 concept 5).
Nothing new belongs here.
"""

import os

import pyarrow.parquet as pq

from geoparquet_io.core.duckdb_metadata import get_column_names
from geoparquet_io.core.duckdb_utils import (
    _DuckDBSchemaWrapper,
    fetch_one,
    get_duckdb_connection,
    load_community_extension,
    quote_identifier,
    sql_path,
)
from geoparquet_io.core.exceptions import (
    GeoParquetError,
    InvalidParameterError,
)
from geoparquet_io.core.file_utils import (
    get_first_parquet_file,
    is_partition_path,
    resolve_file_url,
)
from geoparquet_io.core.geo_metadata import build_bbox_covering
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import (
    debug,
    info,
    progress,
    success,
    warn,
)
from geoparquet_io.core.remote import (
    display_path,
    is_remote_url,
    needs_httpfs,
)
from geoparquet_io.core.write_funnels import write_parquet_with_metadata


def should_skip_bbox(geoparquet_version):
    """Check if bbox column should be skipped for this GeoParquet version.

    For GeoParquet 2.0 and parquet-geo-only, bbox columns are not needed because
    native Parquet geo types provide row group statistics for spatial filtering.

    Args:
        geoparquet_version: Version string (e.g., "1.1", "2.0", "parquet-geo-only")

    Returns:
        bool: True if bbox should be skipped, False if bbox should be added
    """
    return geoparquet_version in ("2.0", "parquet-geo-only", "1.1-geoarrow")


def get_parquet_metadata(parquet_file, verbose=False):
    """Get Parquet file metadata using DuckDB for kv_metadata and PyArrow for schema.

    For partitioned datasets (directories or glob patterns), reads metadata
    from the first file.

    Returns:
        tuple: (kv_metadata dict, PyArrow schema)

    Note: Uses DuckDB for metadata extraction but returns PyArrow schema for
    backward compatibility with code that expects schema.field() methods.
    """

    from geoparquet_io.core.duckdb_metadata import get_kv_metadata

    # For partitions, use first file for metadata
    file_to_check = parquet_file
    if is_partition_path(parquet_file):
        first_file = get_first_parquet_file(parquet_file)
        if first_file:
            file_to_check = first_file

    # Get key-value metadata (returns dict like {b'geo': b'...'})
    kv_metadata = get_kv_metadata(file_to_check)

    # Get PyArrow schema for backward compatibility
    # (some code uses schema.field(i).name patterns)
    if is_remote_url(file_to_check):
        # For remote files, use a DuckDB-based approach to read schema
        from geoparquet_io.core.duckdb_metadata import get_schema_info

        schema_info = get_schema_info(file_to_check)
        # Create a simple object that mimics PyArrow schema for basic usage
        schema = _DuckDBSchemaWrapper(schema_info)
    else:
        # Closed before returning, not left to the collector. `check compression
        # --fix` and `check row-group --fix` call this on the file they then
        # rewrite in place, and Windows refuses to rename over a path this
        # process still holds open (#1032).
        with pq.ParquetFile(file_to_check) as pf:
            schema = pf.schema_arrow

    if verbose and kv_metadata:
        debug("\nParquet metadata key-value pairs:")
        for key, value in kv_metadata.items():
            key_str = key.decode("utf-8") if isinstance(key, bytes) else key
            debug(f"{key_str}: {value}")

    return kv_metadata, schema


def add_computed_column(
    input_parquet,
    output_parquet,
    column_name,
    sql_expression,
    extensions=None,
    dry_run=False,
    verbose=False,
    compression=None,
    compression_level=None,
    row_group_size_mb=None,
    row_group_rows=None,
    dry_run_description=None,
    custom_metadata=None,
    profile=None,
    replace_column=None,
    geoparquet_version=None,
    memory_limit: str | None = None,
):
    """
    Add a computed column to a GeoParquet file using SQL expression.

    Handles all boilerplate for adding columns derived from existing data:
    - Input validation
    - Schema checking
    - DuckDB connection and extension loading
    - Query execution
    - Metadata preservation
    - Dry-run support
    - Remote input/output support

    Args:
        input_parquet: Path to input file (local or remote URL)
        output_parquet: Path to output file (local or remote URL)
        column_name: Name for the new column
        sql_expression: SQL expression to compute column value
        extensions: DuckDB extensions to load beyond 'spatial' (e.g., ['h3'])
        dry_run: Whether to print SQL without executing
        verbose: Whether to print verbose output
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        dry_run_description: Optional description for dry-run output
        custom_metadata: Optional dict with custom metadata (e.g., H3 info)
        profile: AWS profile name (S3 only, optional)
        replace_column: Name of existing column to replace (uses EXCLUDE in query)
        memory_limit: DuckDB memory limit for the write (e.g., '2GB', '512MB');
            None auto-detects based on available system/container memory

    Example:
        add_computed_column(
            'input.parquet', 'output.parquet',
            column_name='h3_cell',
            sql_expression="h3_latlng_to_cell(ST_Y(ST_Centroid(geometry)), "
                          "ST_X(ST_Centroid(geometry)), 9)",
            extensions=['h3'],
            custom_metadata={'covering': {'h3': {'column': 'h3_cell', 'resolution': 9}}}
        )
    """
    # RAW path: the dry-run header below shows it to the user, and every SQL
    # interpolation escapes it through sql_path (#802).
    input_path = resolve_file_url(input_parquet, verbose)

    # Get geometry column (for reference)
    geom_col = find_primary_geometry_column(input_parquet, verbose)

    # Dry-run mode header
    if dry_run:
        warn("\n=== DRY RUN MODE - SQL Commands that would be executed ===\n")
        display_input = display_path(input_path)
        display_output = display_path(output_parquet)
        info(f"-- Input file: {display_input}")
        info(f"-- Output file: {display_output}")
        info(f"-- Geometry column: {geom_col}")
        info(f"-- New column: {column_name}")
        if dry_run_description:
            info(f"-- Description: {dry_run_description}")
        progress("")

    # Check if column already exists (skip in dry-run or when replacing)
    if not dry_run:
        from geoparquet_io.core.duckdb_metadata import get_column_names

        # Only check for column collision if not replacing
        if not replace_column:
            column_names = get_column_names(input_parquet)
            if column_name in column_names:
                raise InvalidParameterError(
                    "column_name",
                    f"Column '{column_name}' already exists in the file. "
                    f"Please choose a different name.",
                )

        # Get metadata before processing
        metadata, _ = get_parquet_metadata(input_parquet, verbose)

        if verbose:
            if replace_column:
                debug(f"Replacing column '{replace_column}' with '{column_name}'...")
            else:
                debug(f"Adding column '{column_name}'...")

    # Create DuckDB connection with httpfs if needed
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_parquet))
    # Ensure ST_Transform (used by CRS-aware grid keying) emits lon/lat order.
    con.execute("SET geometry_always_xy = true;")

    # Load additional extensions if specified
    if extensions:
        for ext in extensions:
            if verbose and not dry_run:
                debug(f"Loading DuckDB extension: {ext}")
            load_community_extension(con, ext)

    # Get total count (skip in dry-run)
    if not dry_run:
        total_count = fetch_one(con.execute(f"SELECT COUNT(*) FROM {sql_path(input_path)}"))[0]
        progress(f"Processing {total_count:,} features...")

    # Quote column name to handle special characters (e.g., colons in "metrics:area")
    quoted_col = quote_identifier(column_name)

    # Use EXCLUDE to drop existing column when replacing
    if replace_column:
        query = f"""
        SELECT
            * EXCLUDE ({quote_identifier(replace_column)}),
            {sql_expression} AS {quoted_col}
        FROM {sql_path(input_path)}
    """
    else:
        query = f"""
        SELECT
            *,
            {sql_expression} AS {quoted_col}
        FROM {sql_path(input_path)}
    """

    if dry_run:
        from geoparquet_io.core.compression import print_copy_dry_run
        from geoparquet_io.core.parquet_writer import resolve_row_group_rows_for_query

        rows = resolve_row_group_rows_for_query(con, query, row_group_rows, row_group_size_mb)
        info("-- Main query:")
        print_copy_dry_run(query, output_parquet, compression, compression_level, rows)
        info("-- This query creates a new parquet file with the computed column added")
        info("-- The covering metadata is updated only if the output has a bbox column")
        con.close()
        return

    # Execute the query using existing write helper
    if verbose:
        debug(f"Creating column '{column_name}'...")

    # Closed deterministically, success or not: the caller may os.replace() the
    # input right after this returns, which Windows refuses while a reader
    # holds it (#1032), and a failed write removes its staging file the same way.
    try:
        write_parquet_with_metadata(
            con,
            query,
            output_parquet,
            original_metadata=metadata,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            custom_metadata=custom_metadata,
            verbose=verbose,
            profile=profile,
            geoparquet_version=geoparquet_version,
            # The precondition: the query above is `SELECT *` plus one computed
            # column, so the rows this write reads are `input_parquet`'s own.
            input_file=input_parquet,
            memory_limit=memory_limit,
        )
    finally:
        con.close()


def add_bbox(parquet_file, bbox_column_name="bbox", verbose=False):
    """
    Add a bbox struct column to a GeoParquet file in-place.

    Internal helper function used by --add-bbox flags in other commands
    (hilbert_order). Modifies the file in-place by
    writing to a temporary file and replacing the original.

    Raises an error if the bbox column already exists.

    Args:
        parquet_file: Path to the parquet file (will be modified in-place)
        bbox_column_name: Name for the bbox column (default: 'bbox')
        verbose: Whether to print verbose output

    Returns:
        bool: True if bbox was added successfully

    Raises:
        click.ClickException: If column already exists or operation fails
    """

    # Check if column already exists using DuckDB
    column_names = get_column_names(parquet_file)

    if bbox_column_name in column_names:
        raise InvalidParameterError(
            "bbox_column_name",
            f"Column '{bbox_column_name}' already exists in the file. "
            f"Please choose a different name.",
        )

    # Get geometry column for SQL expression
    geom_col = find_primary_geometry_column(parquet_file, verbose)

    if verbose:
        debug(f"Adding bbox column for geometry column: {geom_col}")

    # Define SQL expression
    quoted_geom_col = quote_identifier(geom_col)
    sql_expression = f"""STRUCT_PACK(
        xmin := ST_XMin({quoted_geom_col}),
        ymin := ST_YMin({quoted_geom_col}),
        xmax := ST_XMax({quoted_geom_col}),
        ymax := ST_YMax({quoted_geom_col})
    )"""

    # Create temporary file path
    temp_file = parquet_file + ".tmp"

    try:
        # Use add_computed_column to write to temp file
        # Declare the covering explicitly rather than leaving a writer to infer
        # it from the column's name. gpio computed this column from the geometry
        # in this same statement, so the assertion "these values bound that
        # geometry" is one we can actually make here -- and nowhere else (#738).
        # `custom_metadata` is the existing route for that, and it is already
        # version-gated: strip_unsupported_covering drops the key for 1.0 output.
        bbox_covering = {"covering": {"bbox": build_bbox_covering(bbox_column_name)}}

        add_computed_column(
            input_parquet=parquet_file,
            output_parquet=temp_file,
            column_name=bbox_column_name,
            sql_expression=sql_expression,
            extensions=None,
            dry_run=False,
            verbose=verbose,
            compression=None,
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=None,
            dry_run_description=None,
            custom_metadata=bbox_covering,
        )

        # Replace original file with updated file
        os.replace(temp_file, parquet_file)

        if verbose:
            success(f"Successfully added bbox column '{bbox_column_name}'")

        return True

    except Exception as e:
        # Clean up temporary file if something goes wrong
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise GeoParquetError(f"Failed to add bbox: {str(e)}") from e
