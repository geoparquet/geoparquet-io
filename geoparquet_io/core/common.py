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

from geoparquet_io.core.column_selection import has_column
from geoparquet_io.core.duckdb_metadata import get_usable_column_names
from geoparquet_io.core.duckdb_utils import (
    _DuckDBSchemaWrapper,
    computed_column_query,
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
from geoparquet_io.core.geo_metadata import bbox_struct_sql, build_bbox_covering
from geoparquet_io.core.geometry_detection import primary_geometry_column, require_geometry_column
from geoparquet_io.core.logging_config import (
    debug,
    info,
    progress,
    success,
    warn,
)
from geoparquet_io.core.parquet_writer import WriteOptions
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
    *,
    from_clause: str | None = None,
    connection_setup=None,
    report_count: bool = True,
    progress_message: str | None = None,
):
    """Add a SQL-derived column through the shared file write funnel.

    Resolves input paths and geometry, checks for existing columns, loads
    dependencies, reports progress and preserves metadata. Dry runs print the
    SQL without writing; write settings go to write_parquet_with_metadata.

    Args:
        sql_expression: SQL expression yielding the new column value
        extensions: Additional DuckDB extensions beyond spatial
        dry_run_description: Description shown before the SQL preview
        custom_metadata: Additional metadata (for example index covering)
        replace_column: Existing column to exclude from the SELECT
        from_clause: Optional SQL source, including reader options
        connection_setup: Optional callable(con) loading computation dependencies
        report_count: Report the input feature count before writing
        progress_message: Optional computation-specific progress message
        memory_limit: DuckDB write limit; None auto-detects available memory
    """
    # RAW path: the dry-run header below shows it to the user, and every SQL
    # interpolation escapes it through sql_path (#802).
    input_path = resolve_file_url(input_parquet, verbose)

    # For the dry-run header only (no SQL here reads it), so None stays None.
    geom_col = primary_geometry_column(input_parquet, verbose=verbose)

    source = from_clause or sql_path(input_path)

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
        from geoparquet_io.core.index_ops import ensure_index_column

        if not replace_column:
            ensure_index_column(None, input_parquet, column_name, None, reject_existing=True)

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

    if connection_setup is not None:
        connection_setup(con)

    # Load additional extensions if specified
    if extensions:
        for ext in extensions:
            if verbose and not dry_run:
                debug(f"Loading DuckDB extension: {ext}")
            load_community_extension(con, ext)

    # Get total count (skip in dry-run)
    if not dry_run and report_count:
        total_count = fetch_one(con.execute(f"SELECT COUNT(*) FROM {source}"))[0]
        progress(f"Processing {total_count:,} features...")

    query = computed_column_query(source, column_name, sql_expression, replace_column)

    # Handle dry-run display
    if dry_run:
        from geoparquet_io.core.compression import print_copy_dry_run
        from geoparquet_io.core.parquet_writer import resolve_row_group_rows_for_query

        rows = resolve_row_group_rows_for_query(con, query, row_group_rows, row_group_size_mb)
        info("-- Main query:")
        print_copy_dry_run(query, output_parquet, compression, compression_level, rows)
        info("-- This query creates a new parquet file with the computed column added")
        covering = (custom_metadata or {}).get("covering")
        if covering:
            info(f"-- The covering metadata is set to: {covering}")
        else:
            info("-- The covering metadata is updated only if the output has a bbox column")
        con.close()
        return

    if progress_message:
        progress(progress_message)

    # Execute the query using existing write helper
    if verbose:
        debug(f"Creating column '{column_name}'...")

    # Closed deterministically, success or not: the caller may os.replace() the
    # input right after this returns, which Windows refuses while a reader
    # holds it (#1032), and a failed write removes its staging file the same way.
    write = WriteOptions.pick(locals())
    try:
        write_parquet_with_metadata(
            con,
            query,
            output_parquet,
            original_metadata=metadata,
            custom_metadata=custom_metadata,
            verbose=verbose,
            # The precondition: the query above is `SELECT *` plus one computed
            # column, so the rows this write reads are `input_parquet`'s own.
            input_file=input_parquet,
            **write.funnel_kwargs(),
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

    # Check if column already exists (as a query can name it)
    if has_column(get_usable_column_names(parquet_file), bbox_column_name):
        raise InvalidParameterError(
            "bbox_column_name",
            f"Column '{bbox_column_name}' already exists in the file. "
            f"Please choose a different name.",
        )

    geom_col = primary_geometry_column(parquet_file, verbose=verbose)
    geom_col = require_geometry_column(geom_col, parquet_file)  # the SQL needs one

    if verbose:
        debug(f"Adding bbox column for geometry column: {geom_col}")

    # Define SQL expression
    quoted_geom_col = quote_identifier(geom_col)
    sql_expression = bbox_struct_sql(quoted_geom_col, geom_col)

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
