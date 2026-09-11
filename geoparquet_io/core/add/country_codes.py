#!/usr/bin/env python3

from typing import TYPE_CHECKING

from geoparquet_io.core.common import (
    check_bbox_structure,
    get_bbox_advice,
    get_dataset_bounds,
    get_parquet_metadata,
    write_parquet_with_metadata,
)
from geoparquet_io.core.duckdb_utils import (
    SPATIAL_JOIN_BBOX_PREFILTER,
    SPATIAL_JOIN_NATIVE,
    build_spatial_join_condition,
    get_duckdb_connection,
    quote_identifier,
    spatial_join_strategy,
    sql_path,
)
from geoparquet_io.core.exceptions import GeoParquetError, InvalidParameterError
from geoparquet_io.core.file_utils import resolve_file_url
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import debug, info, progress, success, warn
from geoparquet_io.core.remote import _sanitize_url_for_logging, is_remote_url

if TYPE_CHECKING:
    import duckdb


def find_country_code_column(con, countries_source, is_subquery=False):
    """
    Find the country code column in a countries dataset.

    Args:
        con: DuckDB connection
        countries_source: Either a RAW file path or a subquery
        is_subquery: Whether countries_source is a subquery (True) or file path (False)

    Returns:
        str: The name of the country code column

    Raises:
        InvalidParameterError: If no suitable country code column is found
    """
    # Build appropriate query based on source type
    if is_subquery:
        columns_query = f"SELECT * FROM {countries_source} LIMIT 0;"
    else:
        columns_query = f"SELECT * FROM {sql_path(countries_source)} LIMIT 0;"

    countries_columns = [col[0] for col in con.execute(columns_query).description]

    # Define possible country code column names in priority order
    country_code_options = [
        "admin:country_code",
        "country_code",
        "country",
        "ISO_A2",
        "ISO_A3",
        "ISO3",
        "ISO2",
    ]

    # Find the first matching column
    for col in country_code_options:
        if col in countries_columns:
            return col

    # If no column found, raise an error
    raise InvalidParameterError(
        "countries_parquet",
        f"Could not find country code column in countries file. "
        f"Expected one of: {', '.join(country_code_options)}",
    )


def find_subdivision_code_column(con, countries_source, is_subquery=False):
    """
    Find the subdivision code column in a countries dataset.

    Args:
        con: DuckDB connection
        countries_source: Either a RAW file path or a subquery
        is_subquery: Whether countries_source is a subquery (True) or file path (False)

    Returns:
        str or None: The name of the subdivision code column, or None if not found
    """
    # Build appropriate query based on source type
    if is_subquery:
        columns_query = f"SELECT * FROM {countries_source} LIMIT 0;"
    else:
        columns_query = f"SELECT * FROM {sql_path(countries_source)} LIMIT 0;"

    countries_columns = [col[0] for col in con.execute(columns_query).description]

    # Define possible subdivision code column names in priority order
    subdivision_code_options = [
        "admin:subdivision_code",
        "subdivision_code",
        "region",
        "state",
        "province",
    ]

    # Find the first matching column
    for col in subdivision_code_options:
        if col in countries_columns:
            return col

    # Subdivision is optional, return None if not found
    return None


def _handle_bbox_optimization(file_path, bbox_info, add_bbox_flag, file_label, verbose):
    """Handle bbox structure warning and optionally add bbox."""
    if bbox_info["status"] == "optimal":
        return bbox_info

    warn(f"\nWarning: {file_label} could benefit from bbox optimization:\n" + bbox_info["message"])

    if not add_bbox_flag:
        info(
            f"💡 Tip: Run this command with --add-bbox to automatically add bbox optimization to the {file_label.lower()}"
        )
        return bbox_info

    if not bbox_info["has_bbox_column"]:
        progress(f"Adding bbox column to {file_label.lower()}...")
        from geoparquet_io.core.common import add_bbox

        add_bbox(file_path, "bbox", verbose)
        success(f"✓ Added bbox column and metadata to {file_label.lower()}")
    elif not bbox_info["has_bbox_metadata"]:
        progress(f"Adding bbox metadata to {file_label.lower()}...")
        from geoparquet_io.core.add.bbox_metadata import add_bbox_metadata

        add_bbox_metadata(file_path, verbose)

    return check_bbox_structure(file_path, verbose)


def _build_select_clause(country_code_col, subdivision_code_col, using_default):
    """Build the SELECT clause for country and subdivision codes."""
    # Country code selection
    if country_code_col == "admin:country_code":
        country_select = f"b.{quote_identifier(country_code_col)}"
    else:
        country_select = f'b.{quote_identifier(country_code_col)} as "admin:country_code"'

    # Subdivision code selection
    if not subdivision_code_col:
        return country_select

    if using_default and subdivision_code_col == "region":
        subdivision_select = (
            ", CASE WHEN b.region LIKE '%-%' THEN split_part(b.region, '-', 2) "
            'ELSE b.region END as "admin:subdivision_code"'
        )
    elif subdivision_code_col == "admin:subdivision_code":
        subdivision_select = f", b.{quote_identifier(subdivision_code_col)}"
    else:
        subdivision_select = (
            f', b.{quote_identifier(subdivision_code_col)} as "admin:subdivision_code"'
        )

    return country_select + subdivision_select


def _build_spatial_join_query(
    input_path,
    countries_source,
    select_clause,
    input_geom_col,
    countries_geom_col,
    input_bbox_col,
    countries_bbox_col,
):
    """Build the spatial join query based on bbox availability.

    Identifiers are quoted via the shared :func:`build_spatial_join_condition`
    helper, so a maliciously-named geometry/bbox column from an untrusted input
    or ``--countries-parquet`` cannot inject SQL. ``input_path`` is RAW and is
    quoted and escaped here by :func:`sql_path` (#802); ``countries_source`` is
    already a SQL reference (a table name or a ``sql_path`` literal).
    """
    join_condition = build_spatial_join_condition(
        input_geom_col, countries_geom_col, input_bbox_col, countries_bbox_col
    )
    return f"""
    SELECT
        a.*,
        {select_clause}
    FROM {sql_path(input_path)} a
    LEFT JOIN {countries_source} b
    ON {join_condition}
"""


def _build_filter_table_sql(table_name, source_path, bbox_col, bounds):
    """Build SQL to create filtered countries table from bounds.

    ``source_path`` is the RAW countries path/URL; :func:`sql_path` quotes and
    escapes it here (#802).
    """
    xmin, ymin, xmax, ymax = bounds
    q_bbox = quote_identifier(bbox_col)
    if isinstance(xmin, str):  # placeholder values
        return f"""CREATE TEMP TABLE {table_name} AS
SELECT * FROM {sql_path(source_path)}
WHERE {q_bbox}.xmin <= {xmax}
  AND {q_bbox}.xmax >= {xmin}
  AND {q_bbox}.ymin <= {ymax}
  AND {q_bbox}.ymax >= {ymin};"""
    return f"""CREATE TEMP TABLE {table_name} AS
SELECT * FROM {sql_path(source_path)}
WHERE {q_bbox}.xmin <= {xmax:.6f}
  AND {q_bbox}.xmax >= {xmin:.6f}
  AND {q_bbox}.ymin <= {ymax:.6f}
  AND {q_bbox}.ymax >= {ymin:.6f};"""


def _print_dry_run_bounds_info(input_bbox_col, input_path, input_geom_col):
    """Print dry-run info for bounds calculation step.

    This one echoes *SQL*, so the path is escaped -- the printed statement has
    to be runnable. The prose headers elsewhere show the raw path (#802).
    """
    info("-- Step 1: Calculate bounding box of input data to filter remote countries")
    if input_bbox_col:
        q_input_bbox = quote_identifier(input_bbox_col)
        bounds_sql = f"SELECT MIN({q_input_bbox}.xmin) as xmin, ... FROM {sql_path(input_path)};"
    else:
        q_input_geom = quote_identifier(input_geom_col)
        bounds_sql = (
            f"SELECT MIN(ST_XMin({q_input_geom})) as xmin, ... FROM {sql_path(input_path)};"
        )
    progress(bounds_sql)
    progress("")
    warn("-- Calculating actual bounds...")


def _get_bounds_for_filtering(input_parquet, input_geom_col, dry_run, verbose):
    """Get dataset bounds, handling dry-run mode."""
    bounds = get_dataset_bounds(input_parquet, input_geom_col, verbose=(verbose and not dry_run))

    if not bounds:
        if dry_run:
            warn("-- Note: Could not calculate actual bounds")
            return ("<xmin>", "<ymin>", "<xmax>", "<ymax>")
        raise GeoParquetError("Could not calculate dataset bounds")

    if dry_run:
        success(f"-- Bounds calculated: {bounds}")
    elif verbose:
        debug(f"Input bbox: {bounds}")
    return bounds


def _create_filtered_countries_table(
    con, countries_table, default_countries_path, countries_bbox_col, bounds, dry_run, verbose
):
    """Create the filtered countries temporary table."""
    if dry_run:
        progress("")
        info("-- Step 2: Create filtered countries table")

    create_table_sql = _build_filter_table_sql(
        countries_table, default_countries_path, countries_bbox_col, bounds
    )

    if dry_run:
        progress(create_table_sql)
        progress("")
    else:
        if verbose:
            debug("Creating temporary table with filtered countries...")
        con.execute(create_table_sql)
        if verbose:
            count = con.execute(f"SELECT COUNT(*) FROM {countries_table}").fetchone()[0]
            debug(f"Loaded {count} countries overlapping with input data")


def _print_dry_run_header(
    input_path,
    countries_path,
    output_parquet,
    input_geom_col,
    countries_geom_col,
    input_bbox_col,
    countries_bbox_col,
):
    """Print dry-run mode header information.

    ``input_path`` and ``countries_path`` are RAW: this is prose for a human, so
    showing ``o''brien/data.parquet`` for ``o'brien/data.parquet`` is a bug --
    it is confusing to read and wrong to copy-paste (#802).
    """
    warn("\n=== DRY RUN MODE - SQL Commands that would be executed ===\n")
    display_input = (
        _sanitize_url_for_logging(input_path) if is_remote_url(input_path) else input_path
    )
    display_countries = (
        _sanitize_url_for_logging(countries_path)
        if is_remote_url(countries_path)
        else countries_path
    )
    display_output = (
        _sanitize_url_for_logging(output_parquet)
        if is_remote_url(output_parquet)
        else output_parquet
    )
    info(f"-- Input file: {display_input}")
    info(f"-- Countries file: {display_countries}")
    info(f"-- Output file: {display_output}")
    info(f"-- Geometry columns: {input_geom_col} (input), {countries_geom_col} (countries)")
    info(
        f"-- Bbox columns: {input_bbox_col or 'none'} (input), {countries_bbox_col or 'none'} (countries)\n"
    )


def _get_countries_config(countries_parquet, using_default, verbose):
    """Get the RAW countries path, its geometry column, and its bbox column.

    The path is returned unescaped. It is shown to the user in the dry-run
    header and handed to helpers that escape their own argument, so escaping it
    here would mean every consumer had to know it was pre-escaped -- the shape
    that produced #718's crashes (#802). The default Overture URL was already
    returned raw, so this also makes the two branches agree.
    """
    if using_default:
        from geoparquet_io.core.overture import get_overture_divisions_url

        return get_overture_divisions_url(verbose=verbose), "geometry", "bbox"

    countries_path = resolve_file_url(countries_parquet, verbose)
    countries_geom_col = find_primary_geometry_column(countries_parquet, verbose)
    countries_bbox_info = check_bbox_structure(countries_parquet, verbose)
    return countries_path, countries_geom_col, countries_bbox_info["bbox_column_name"]


def _determine_code_columns(
    con, countries_path, countries_source, countries_table, using_default, dry_run, verbose
):
    """Determine country and subdivision code columns."""
    if using_default:
        country_code_col = "country"
        subdivision_code_col = "region"
        if verbose and not dry_run:
            debug(f"Using country code column: {country_code_col} (default countries file)")
            debug(f"Using subdivision code column: {subdivision_code_col} (default countries file)")
        return country_code_col, subdivision_code_col

    if dry_run:
        return "admin:country_code", None

    country_code_col = find_country_code_column(con, countries_path, is_subquery=False)
    if verbose:
        debug(f"Using country code column: {country_code_col}")

    # `_setup_countries_source` returns a SQL reference -- a quoted literal or a
    # temp-table name -- while both finders build the literal themselves when
    # is_subquery is False; passing countries_source straight through produced
    # `FROM ''/path''` and a parser error for every non-default --countries
    # file. Hand over the same RAW path the country-code finder above gets, and
    # only use the table name when the source really is the filtered temp table.
    is_filtered_table = countries_source == countries_table
    subdivision_code_col = find_subdivision_code_column(
        con,
        countries_table if is_filtered_table else countries_path,
        is_subquery=is_filtered_table,
    )
    if subdivision_code_col and verbose:
        debug(f"Using subdivision code column: {subdivision_code_col}")

    return country_code_col, subdivision_code_col


def _print_dry_run_query(
    query,
    output_parquet,
    compression,
    compression_level,
    using_default,
    input_bbox_col,
    countries_bbox_col,
    has_native_geometry=False,
):
    """Print the dry-run query output."""
    final_step = "3" if using_default else "1"
    info(f"-- Step {final_step}: Main spatial join query")

    strategy = spatial_join_strategy(has_native_geometry, input_bbox_col, countries_bbox_col)
    if strategy == SPATIAL_JOIN_NATIVE:
        info("-- Using native geometry with DuckDB SPATIAL_JOIN")
    elif strategy == SPATIAL_JOIN_BBOX_PREFILTER:
        info("-- Using bbox columns for optimized spatial join")
    else:
        info("-- Using full geometry intersection (no bbox optimization)")

    compression_str = (
        f"{compression}:{compression_level}"
        if compression in ["GZIP", "ZSTD", "BROTLI"]
        else compression
    )
    duckdb_compression = compression.lower() if compression != "UNCOMPRESSED" else "uncompressed"

    # sql_path: the printed query has to be valid SQL the user can paste, and
    # output_parquet is a raw CLI argument that may contain an apostrophe (#718).
    display_query = f"""COPY ({query.strip()})
TO {sql_path(output_parquet)}
(FORMAT PARQUET, COMPRESSION '{duckdb_compression}');"""
    progress(display_query)

    info(f"\n-- Note: Using {compression_str} compression")
    info("-- Original metadata would also be preserved in the output file")


def _output_has_subdivision(con, output_parquet):
    """Report whether the written file carries a subdivision column.

    The countries file is not the authority: the join is ``SELECT a.*``, so an
    input that already carried ``admin:subdivision_code`` keeps it even when the
    countries file has none. Asking the output is the only answer that is true
    in both directions (#672).
    """
    described = con.execute(f"SELECT * FROM {sql_path(output_parquet)} LIMIT 0").description
    return "admin:subdivision_code" in {column[0] for column in described}


def _print_output_stats(con, output_parquet):
    """Query the written file and print the per-column counts."""
    output_path = str(output_parquet)
    has_subdivision = _output_has_subdivision(con, output_path)

    subdivision_selects = (
        """,
        COUNT(CASE WHEN "admin:subdivision_code" IS NOT NULL THEN 1 END)
            as features_with_subdivision,
        COUNT(DISTINCT "admin:subdivision_code") as unique_subdivisions"""
        if has_subdivision
        else ""
    )
    stats_query = f"""
    SELECT
        COUNT(*) as total_features,
        COUNT(CASE WHEN "admin:country_code" IS NOT NULL THEN 1 END) as features_with_country,
        COUNT(DISTINCT "admin:country_code") as unique_countries{subdivision_selects}
    FROM {sql_path(output_path)};
    """
    stats = con.execute(stats_query).fetchone()
    total, with_country, unique_countries = stats[0], stats[1], stats[2]
    with_subdivision, unique_subdivisions = (stats[3], stats[4]) if has_subdivision else (0, 0)

    progress("\nResults:")
    progress(f"- Added country codes to {with_country:,} of {total:,} features")
    if with_subdivision > 0:
        progress(f"- Added subdivision codes to {with_subdivision:,} of {total:,} features")
    progress(f"- Found {unique_countries:,} unique countries")
    if unique_subdivisions > 0:
        progress(f"- Found {unique_subdivisions:,} unique subdivisions")


def _print_results_summary(con, output_parquet):
    """Print the results summary after processing.

    The output file is already written by the time this runs, so nothing here
    may decide the exit status. A summary that cannot be read -- a remote output
    this connection has no credentials for, a path DuckDB will not re-open -- is
    reported as a warning and the write still counts as the success it was
    (#672).
    """
    try:
        _print_output_stats(con, output_parquet)
    except Exception as exc:  # noqa: BLE001 - the file is written; a summary is never worth failing for
        warn(f"Could not summarize the output file: {exc}")

    success(f"\nSuccessfully wrote output to: {output_parquet}")


def _setup_default_countries(
    con,
    input_parquet,
    input_path,
    input_geom_col,
    input_bbox_col,
    default_countries_path,
    countries_bbox_col,
    countries_table,
    dry_run,
    verbose,
):
    """Setup filtered countries table for default Overture dataset."""
    if dry_run:
        _print_dry_run_bounds_info(input_bbox_col, input_path, input_geom_col)

    if verbose and not dry_run:
        debug("Calculating bounding box of input data to filter remote countries file...")

    bounds = _get_bounds_for_filtering(input_parquet, input_geom_col, dry_run, verbose)

    _create_filtered_countries_table(
        con, countries_table, default_countries_path, countries_bbox_col, bounds, dry_run, verbose
    )


def _prepare_bbox_columns(
    input_parquet, countries_parquet, using_default, add_bbox_flag, dry_run, verbose
):
    """Prepare and optionally optimize bbox columns for input and countries files.

    For GeoParquet 2.0 / parquet-geo files with native geometry types,
    skip bbox pre-filtering entirely as native geometry row group statistics
    are faster than manual bbox filtering.

    Returns ``(input_bbox_col, countries_bbox_col, has_native_geometry)``.
    """
    # Check if input file has native geometry (2.0 / parquet-geo)
    input_bbox_advice = get_bbox_advice(input_parquet, "spatial_filtering", verbose)

    # For native geometry files, skip bbox pre-filtering
    if input_bbox_advice["skip_bbox_prefilter"]:
        if verbose:
            debug("Input has native geometry - skipping bbox pre-filter (native stats are faster)")
        return None, None, True

    # For 1.x files, use bbox optimization if available
    input_bbox_info = check_bbox_structure(input_parquet, verbose)
    input_bbox_col = input_bbox_info["bbox_column_name"]

    if using_default:
        countries_bbox_col = "bbox"
    else:
        countries_bbox_info = check_bbox_structure(countries_parquet, verbose)
        countries_bbox_col = countries_bbox_info["bbox_column_name"]

    if not dry_run:
        # Show warning and suggest options for 1.x files without bbox
        if input_bbox_advice["needs_warning"]:
            warn(f"\nWarning: {input_bbox_advice['message']}")
            if not add_bbox_flag:
                for suggestion in input_bbox_advice["suggestions"]:
                    info(f"💡 Tip: {suggestion}")

        # Handle bbox optimization if --add-bbox flag is used
        if add_bbox_flag and not input_bbox_info["has_bbox_column"]:
            input_bbox_info = _handle_bbox_optimization(
                input_parquet, input_bbox_info, add_bbox_flag, "Input file", verbose
            )
            input_bbox_col = input_bbox_info["bbox_column_name"]

        if not using_default:
            countries_bbox_info = check_bbox_structure(countries_parquet, verbose)
            countries_bbox_info = _handle_bbox_optimization(
                countries_parquet, countries_bbox_info, add_bbox_flag, "Countries file", verbose
            )
            countries_bbox_col = countries_bbox_info["bbox_column_name"]

    return input_bbox_col, countries_bbox_col, False


def _setup_countries_source(
    con,
    using_default,
    countries_path,
    input_parquet,
    input_path,
    input_geom_col,
    input_bbox_col,
    countries_bbox_col,
    dry_run,
    verbose,
):
    """Setup countries source - either filtered table or direct file reference.

    Returns a SQL *reference*: a temp-table name, or the RAW countries path
    turned into a quoted literal by :func:`sql_path` (#802).
    """
    countries_table = "filtered_countries"

    if using_default:
        from geoparquet_io.core.overture import get_overture_divisions_url

        default_countries_path = get_overture_divisions_url(verbose=verbose)
        _setup_default_countries(
            con,
            input_parquet,
            input_path,
            input_geom_col,
            input_bbox_col,
            default_countries_path,
            countries_bbox_col,
            countries_table,
            dry_run,
            verbose,
        )
        return countries_table
    return sql_path(countries_path)


def _create_duckdb_connection(using_default: bool) -> "duckdb.DuckDBPyConnection":
    """Create and configure DuckDB connection.

    The default countries source is the remote Overture release on S3, so that
    path needs httpfs and the bucket's region. A user-supplied countries file is
    read from local disk and needs neither.
    """
    if using_default:
        return get_duckdb_connection(load_httpfs=True, s3_region="us-west-2")
    return get_duckdb_connection(load_httpfs=False)


def _print_bbox_status(
    input_bbox_col, countries_bbox_col, verbose, dry_run, has_native_geometry=False
):
    """Print a status line describing which spatial-join strategy will run.

    Native-geometry inputs take the bare-ST_Intersects SPATIAL_JOIN fast path, so
    the old "No bbox columns available..." line misreported them as a degraded
    fallback (issue #538). Only 1.x files genuinely lacking a bbox get that warning.
    """
    if dry_run:
        return
    strategy = spatial_join_strategy(has_native_geometry, input_bbox_col, countries_bbox_col)
    if strategy == SPATIAL_JOIN_NATIVE:
        progress("Using native geometry with DuckDB SPATIAL_JOIN...")
    elif strategy == SPATIAL_JOIN_BBOX_PREFILTER:
        if verbose:
            debug("Using bbox columns for initial filtering...")
    else:
        progress("No bbox columns available, using full geometry intersection...")


def add_country_codes(
    input_parquet,
    countries_parquet,
    output_parquet,
    add_bbox_flag,
    dry_run,
    verbose,
    compression="ZSTD",
    compression_level=None,
    row_group_size_mb=None,
    row_group_rows=None,
):
    """Add country ISO codes to a GeoParquet file based on spatial intersection."""
    # RAW paths throughout: they are shown to the user in the dry-run header,
    # and every SQL interpolation escapes at its own boundary via sql_path (#802).
    input_path = resolve_file_url(input_parquet, verbose)
    using_default = countries_parquet is None

    countries_path, countries_geom_col, _ = _get_countries_config(
        countries_parquet, using_default, verbose
    )

    if using_default and not dry_run:
        info("\nNo countries file specified, using default from Overture Maps")
        info(
            "This will filter the remote file to only the area of your data, but may take longer than using a local file."
        )

    input_geom_col = find_primary_geometry_column(input_parquet, verbose)
    input_bbox_col, countries_bbox_col, has_native_geometry = _prepare_bbox_columns(
        input_parquet, countries_parquet, using_default, add_bbox_flag, dry_run, verbose
    )

    if dry_run:
        _print_dry_run_header(
            input_path,
            countries_path,
            output_parquet,
            input_geom_col,
            countries_geom_col,
            input_bbox_col,
            countries_bbox_col,
        )

    metadata = None if dry_run else get_parquet_metadata(input_parquet, verbose)[0]

    if not dry_run and verbose:
        debug(f"Using geometry columns: {input_geom_col} (input), {countries_geom_col} (countries)")

    # Context-managed: the dry-run path returns early, and a leaked DuckDB
    # handle keeps the input file open (breaks cleanup on Windows).
    with _create_duckdb_connection(using_default) as con:
        if not dry_run:
            total_count = con.execute(f"SELECT COUNT(*) FROM {sql_path(input_path)}").fetchone()[0]
            progress(f"Processing {total_count:,} input features...")

        countries_table = "filtered_countries"
        countries_source = _setup_countries_source(
            con,
            using_default,
            countries_path,
            input_parquet,
            input_path,
            input_geom_col,
            input_bbox_col,
            countries_bbox_col,
            dry_run,
            verbose,
        )

        country_code_col, subdivision_code_col = _determine_code_columns(
            con, countries_path, countries_source, countries_table, using_default, dry_run, verbose
        )

        select_clause = _build_select_clause(country_code_col, subdivision_code_col, using_default)
        _print_bbox_status(
            input_bbox_col, countries_bbox_col, verbose, dry_run, has_native_geometry
        )

        query = _build_spatial_join_query(
            input_path,
            countries_source,
            select_clause,
            input_geom_col,
            countries_geom_col,
            input_bbox_col,
            countries_bbox_col,
        )

        if dry_run:
            _print_dry_run_query(
                query,
                output_parquet,
                compression,
                compression_level,
                using_default,
                input_bbox_col,
                countries_bbox_col,
                has_native_geometry,
            )
            return

        if verbose:
            debug("Performing spatial join with country boundaries...")

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
            # The join keeps the input's own geometry column, so the input is
            # the witness auto mode resolves the output version from (#993).
            input_file=input_path,
        )

        _print_results_summary(con, output_parquet)


if __name__ == "__main__":
    add_country_codes()
