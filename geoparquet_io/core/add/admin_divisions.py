#!/usr/bin/env python3

"""
Add admin division columns from multiple datasets.

This module extends the add_country_codes functionality to support
multiple admin datasets with hierarchical level support.
"""

from geoparquet_io.core.admin_datasets import AdminDatasetFactory
from geoparquet_io.core.common import (
    check_bbox_structure,
    get_bbox_advice,
    get_parquet_metadata,
    write_parquet_with_metadata,
)
from geoparquet_io.core.crs_utils import (
    reproject_to_source_sql,
    source_crs_string,
    transform_geom_sql,
)
from geoparquet_io.core.duckdb_utils import (
    SPATIAL_JOIN_BBOX_PREFILTER,
    SPATIAL_JOIN_NATIVE,
    build_spatial_join_condition,
    quote_identifier,
    spatial_join_strategy,
    sql_path,
)
from geoparquet_io.core.file_utils import handle_output_overwrite, resolve_file_url
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import debug, info, progress, success, warn
from geoparquet_io.core.partition.reader import require_single_file
from geoparquet_io.core.remote import _sanitize_url_for_logging, is_remote_url


def _admin_reprojected(source_crs, admin_bbox_col) -> bool:
    """Whether the admin side is reprojected into the input CRS (#525).

    Only when there is a non-CRS84 input *and* the admin dataset has a bbox to
    base a pre-filter on. Reprojecting the (small) admin side instead of the
    large input keeps the join's bbox pre-filter usable (both sides share the
    input CRS) — see :func:`reproject_to_source_sql`.
    """
    return bool(source_crs) and bool(admin_bbox_col)


def _build_admin_subquery(
    dataset,
    levels,
    boundary_columns,
    admin_table_ref,
    admin_geom_col,
    admin_bbox_col,
    admin_where_clauses,
    source_crs=None,
):
    """Build admin data subquery with filters.

    When ``source_crs`` is set (non-CRS84 input) and the admin data has a bbox,
    the admin polygons are reprojected into the input's CRS and a matching
    source-CRS bbox struct is synthesized, so the downstream join keeps its cheap
    bbox pre-filter without transforming the input per row (#525). The admin
    ``WHERE`` extent filter still runs on the original (CRS84) bbox before the
    reprojection.
    """
    admin_where_clause = ""
    if admin_where_clauses:
        admin_where_clause = "WHERE " + " AND ".join(admin_where_clauses)

    # Build column list for subquery - handle struct access
    subquery_cols = []
    for i, col in enumerate(boundary_columns):
        if "[" in col or "(" in col:
            subquery_cols.append(f"{col} as _col_{i}")
        else:
            subquery_cols.append(quote_identifier(col))
    subquery_cols_str = ", ".join(subquery_cols)

    geom_select = quote_identifier(admin_geom_col)

    if _admin_reprojected(source_crs, admin_bbox_col):
        radmin = reproject_to_source_sql(geom_select, source_crs)
        bbox_q = quote_identifier(admin_bbox_col)
        # Reproject the polygon (M rows, cheap) and rebuild its bbox in the input
        # CRS so the join can pre-filter against the input's stored bbox.
        bbox_struct = (
            f"struct_pack(xmin := ST_XMin({radmin}), xmax := ST_XMax({radmin}), "
            f"ymin := ST_YMin({radmin}), ymax := ST_YMax({radmin})) AS {bbox_q}"
        )
        return f"""(
        SELECT {radmin} AS {geom_select}, {bbox_struct}, {subquery_cols_str}
        FROM {admin_table_ref}
        {admin_where_clause}
    )"""

    bbox_select = quote_identifier(admin_bbox_col) if admin_bbox_col else geom_select

    return f"""(
        SELECT {geom_select}, {bbox_select}, {subquery_cols_str}
        FROM {admin_table_ref}
        {admin_where_clause}
    )"""


def _build_admin_select_clause(dataset, levels, partition_columns, prefix=None):
    """Build SELECT clause for admin columns with transformations."""
    use_coalesce = prefix == "vecorel"
    admin_select_parts = []
    for i, (level, col) in enumerate(zip(levels, partition_columns, strict=True)):
        output_col_name = dataset.get_output_column_name(level, prefix=prefix)
        col_transform = dataset.get_column_transform(level)

        if col_transform:
            expr = col_transform
        elif "[" in col or "(" in col:
            expr = f"b._col_{i}"
        else:
            expr = f"b.{quote_identifier(col)}"

        if use_coalesce:
            expr = f"COALESCE({expr}, 'ZZ')"

        admin_select_parts.append(f"{expr} as {quote_identifier(output_col_name)}")

    return ", ".join(admin_select_parts)


def _format_input_ref(input_source, is_table_ref=False):
    """Format input reference for SQL.

    A DuckDB table/CTE name (``is_table_ref=True``) is emitted bare; a RAW file
    path or URL goes through :func:`sql_path`, which quotes and escapes it. The
    caller passes the flag explicitly rather than sniffing for a ``_gpio_``
    prefix, so a real path beginning with ``_gpio_`` can never be emitted
    unquoted (see todo 017).

    ``input_source`` alternates between a path and a table name as the per-level
    chain advances, so the path it carries must be RAW: escaping it earlier
    would mean the escape survives into the branch that emits a table name
    (#802).
    """
    if is_table_ref:
        return input_source
    return sql_path(input_source)


def _build_spatial_join_query(
    input_source,
    admin_subquery,
    admin_select_clause,
    input_bbox_col,
    admin_bbox_col,
    input_geom_col,
    admin_geom_col,
    *,
    is_table_ref=False,
    source_crs=None,
):
    """Build the per-level admin spatial join query.

    A plain streaming ``LEFT JOIN`` (the original design): for each input feature
    it attaches the admin columns of the polygon(s) it intersects. This is the
    only memory-scalable shape — a DuckDB ``LEFT JOIN`` spills to disk, whereas
    the ``QUALIFY ROW_NUMBER()`` window that previously de-duplicated overlapping
    matches buffers every row and OOMs on large inputs (the window operator does
    not spill). Per-level caches are non-overlapping, so a feature normally
    matches exactly one polygon per level; a feature straddling overlapping
    polygons (rare border slivers) is emitted once per match.

    All identifiers are quoted via the shared :func:`build_spatial_join_condition`
    helper / :func:`quote_identifier`, so column names sourced from untrusted file
    metadata cannot break out of the generated SQL. ``is_table_ref`` marks
    ``input_source`` as a DuckDB table/CTE name (per-level chaining) vs a file path.
    """
    input_ref = _format_input_ref(input_source, is_table_ref)

    # CRS handling for a non-CRS84 input (#525):
    #  - If the admin side was reprojected into the input CRS (it has a bbox), the
    #    join runs entirely in the input CRS, so the input geometry is left
    #    untouched and the cheap bbox pre-filter is kept (avoids the #460 hang).
    #  - Otherwise (admin has no bbox) reproject the input geometry inline; without
    #    a bbox there is no pre-filter to preserve either way.
    input_geom_sql = None
    if source_crs and not _admin_reprojected(source_crs, admin_bbox_col):
        input_geom_sql = transform_geom_sql(f"a.{quote_identifier(input_geom_col)}", source_crs)

    # Shared, fully-quoted ON clause (bbox pre-filter + ST_Intersects).
    join_clause = "ON " + build_spatial_join_condition(
        input_geom_col,
        admin_geom_col,
        input_bbox_col,
        admin_bbox_col,
        input_geom_sql=input_geom_sql,
    )

    return f"""
    SELECT
        a.*,
        {admin_select_clause}
    FROM {input_ref} a
    LEFT JOIN {admin_subquery} b
    {join_clause}
"""


def _add_extent_filter(
    con,
    input_source,
    input_bbox_col,
    input_geom_col,
    admin_bbox_col,
    verbose,
    is_table_ref=False,
    source_crs=None,
):
    """Add bbox extent filter to admin where clauses.

    The admin boundaries are in OGC:CRS84, so the extent must be in CRS84 too.
    For a non-CRS84 input the stored bbox is in the source CRS and can't be used —
    compute the extent over the reprojected geometry instead (#525).
    """
    if not admin_bbox_col:
        return None

    input_ref = _format_input_ref(input_source, is_table_ref)

    if input_bbox_col and not source_crs:
        q_input_bbox = quote_identifier(input_bbox_col)
        extent_query = f"""
            SELECT
                MIN({q_input_bbox}.xmin) as xmin,
                MAX({q_input_bbox}.xmax) as xmax,
                MIN({q_input_bbox}.ymin) as ymin,
                MAX({q_input_bbox}.ymax) as ymax
            FROM {input_ref}
        """
    else:
        geom_ref = transform_geom_sql(quote_identifier(input_geom_col), source_crs)
        extent_query = f"""
            SELECT
                MIN(ST_XMin({geom_ref})) as xmin,
                MAX(ST_XMax({geom_ref})) as xmax,
                MIN(ST_YMin({geom_ref})) as ymin,
                MAX(ST_YMax({geom_ref})) as ymax
            FROM {input_ref}
        """

    extent = con.execute(extent_query).fetchone()
    if extent and all(v is not None for v in extent):
        xmin, xmax, ymin, ymax = extent
        q_admin_bbox = quote_identifier(admin_bbox_col)
        extent_filter = f"""
            ({q_admin_bbox}.xmin <= {xmax} AND
             {q_admin_bbox}.xmax >= {xmin} AND
             {q_admin_bbox}.ymin <= {ymax} AND
             {q_admin_bbox}.ymax >= {ymin})
        """
        if verbose:
            debug(
                f"Filtering admin boundaries to input extent: ({xmin:.2f}, {ymin:.2f}, {xmax:.2f}, {ymax:.2f})"
            )
        return extent_filter
    return None


def _handle_bbox_optimization(input_parquet, input_bbox_info, add_bbox_flag, verbose):
    """Handle bbox optimization if needed."""
    # Skip for native geometry files - they use native stats instead of bbox pre-filtering
    if input_bbox_info.get("status") == "native":
        return input_bbox_info

    if input_bbox_info["status"] != "optimal":
        warn(
            "\nWarning: Input file could benefit from bbox optimization:\n"
            + input_bbox_info["message"]
        )
        if add_bbox_flag and not input_bbox_info["has_bbox_column"]:
            progress("Adding bbox column to input file...")
            from geoparquet_io.core.common import add_bbox

            add_bbox(input_parquet, "bbox", verbose)
            success("✓ Added bbox column and metadata to input file")
            return check_bbox_structure(input_parquet, verbose)
    return input_bbox_info


def _print_dry_run_header(
    input_path,
    admin_source,
    output_parquet,
    input_geom_col,
    admin_geom_col,
    input_bbox_col,
    admin_bbox_col,
):
    """Print dry-run mode header.

    ``input_path`` is RAW: this is prose for a human, and showing the SQL-escaped
    ``o''brien/data.parquet`` for ``o'brien/data.parquet`` is confusing to read
    and wrong to copy-paste (#802).
    """
    warn("\n=== DRY RUN MODE - SQL Commands that would be executed ===\n")
    display_input = (
        _sanitize_url_for_logging(input_path) if is_remote_url(input_path) else input_path
    )
    display_admin = (
        _sanitize_url_for_logging(admin_source) if is_remote_url(admin_source) else admin_source
    )
    display_output = (
        _sanitize_url_for_logging(output_parquet)
        if is_remote_url(output_parquet)
        else output_parquet
    )
    info(f"-- Input file: {display_input}")
    info(f"-- Admin dataset: {display_admin}")
    info(f"-- Output file: {display_output}")
    info(f"-- Geometry columns: {input_geom_col} (input), {admin_geom_col} (admin)")
    info(
        f"-- Bbox columns: {input_bbox_col or 'none'} (input), {admin_bbox_col or 'none'} (admin)\n"
    )


def _get_result_stats(con, output_parquet, dataset, levels, verbose, prefix=None):
    """Get statistics about the results."""
    output_col_names = [dataset.get_output_column_name(level, prefix=prefix) for level in levels]
    admin_cols_check = " OR ".join(
        [f"{quote_identifier(col)} IS NOT NULL" for col in output_col_names]
    )

    stats_query = f"""
    SELECT
        COUNT(*) as total_features,
        COUNT(CASE WHEN {admin_cols_check} THEN 1 END) as features_with_admin
    FROM {sql_path(output_parquet)};
    """

    stats = con.execute(stats_query).fetchone()
    total_features = stats[0]
    features_with_admin = stats[1]

    unique_counts = []
    for level, output_col in zip(levels, output_col_names, strict=True):
        count_query = f"""
        SELECT COUNT(DISTINCT {quote_identifier(output_col)}) as unique_count
        FROM {sql_path(output_parquet)}
        WHERE {quote_identifier(output_col)} IS NOT NULL;
        """
        result = con.execute(count_query).fetchone()
        unique_counts.append((level, result[0]))

    return total_features, features_with_admin, unique_counts


def _warn_if_row_count_changed(input_rows, output_rows):
    """Warn when the per-level admin join did not preserve the row count.

    The per-level caches are built to be non-overlapping precisely so a plain
    LEFT JOIN is row-preserving (see ``_build_level_cache_query``). When that
    breaks — a maritime polygon slipping back in, overlapping territories, a
    custom ``--admin-source`` — nothing errors: the output just silently gains
    duplicate features. Naming both counts turns the whole class of bug into a
    visible one, at the cost of a metadata-only ``COUNT(*)``.
    """
    if input_rows is None or output_rows is None or input_rows == output_rows:
        return
    verb = "duplicated" if output_rows > input_rows else "dropped"
    warn(
        f"Admin join {verb} rows: {input_rows} input features, {output_rows} in the "
        "output. Overlapping admin polygons emit one row per match; check the "
        "admin dataset for overlapping boundaries."
    )


def _count_rows(con, source, is_table_ref=False):
    """Row count for a file/URL or a temp table, or None if it cannot be read."""
    try:
        return con.execute(
            f"SELECT COUNT(*) FROM {_format_input_ref(source, is_table_ref)}"
        ).fetchone()[0]
    except Exception:
        return None


def _setup_dataset_and_columns(
    input_parquet, dataset_name, dataset_source, levels, verbose, no_cache=False
):
    """Setup dataset and get column information."""
    from geoparquet_io.core.admin_datasets import get_or_cache_dataset

    dataset = AdminDatasetFactory.create(dataset_name, dataset_source, verbose)

    if verbose:
        debug(f"\nUsing admin dataset: {dataset.get_dataset_name()}")
        debug(f"Adding admin levels: {', '.join(levels)}")

    dataset.validate_levels(levels)
    partition_columns = dataset.get_partition_columns(levels)

    # RAW path: it is shown in the dry-run header, and it is also chained
    # through ``current_source``, which alternately holds a temp-table name --
    # so the escape has to happen at the SQL boundary, not here (#802).
    input_path = resolve_file_url(input_parquet, verbose)

    # Per-level datasets resolve sources per-level in add_admin_divisions_multi
    if dataset.supports_per_level_sources():
        admin_source = None
    else:
        admin_source = get_or_cache_dataset(dataset, no_cache=no_cache, verbose=verbose)

    if verbose and admin_source:
        debug(f"Data source: {admin_source}")

    input_geom_col = find_primary_geometry_column(input_parquet, verbose)
    admin_geom_col = dataset.get_geometry_column()

    # Check if we should skip bbox pre-filtering (for native geometry files)
    input_bbox_advice = get_bbox_advice(input_parquet, "spatial_filtering", verbose)
    if input_bbox_advice["skip_bbox_prefilter"]:
        if verbose:
            debug("Input has native geometry - skipping bbox pre-filter (native stats are faster)")
        input_bbox_info = {"status": "native", "bbox_column_name": None, "has_bbox_column": False}
        input_bbox_col = None
    else:
        input_bbox_info = check_bbox_structure(input_parquet, verbose)
        input_bbox_col = input_bbox_info["bbox_column_name"]

    admin_bbox_col = dataset.get_bbox_column()

    return (
        dataset,
        partition_columns,
        input_path,
        admin_source,
        input_geom_col,
        admin_geom_col,
        input_bbox_info,
        input_bbox_col,
        admin_bbox_col,
    )


def _setup_duckdb_connection():
    """Create and configure DuckDB connection for memory-bounded admin joins.

    Large admin spatial joins (e.g. a multi-million-feature input against
    Overture) are run under the same memory discipline DuckDB needs to reliably
    spill rather than OOM (see todo 013):

    - ``temp_directory`` enables spill-to-disk, onto the admin cache volume
      (which already holds the multi-GB cached dataset, so it has the room) in a
      private leaf per connection -- DuckDB's spill filenames carry no
      connection identity, so two runs sharing the cache dir itself would
      overwrite each other's blocks.
    - ``threads = 1`` is required for memory control: parallel spatial-join /
      window operators each grab memory and cannot coordinate spilling, so they
      OOM even with a temp directory set (DuckDB #8270). Single-threaded
      execution spills predictably. This applies to every per-level
      ``CREATE TEMP TABLE`` join, not just the final write.
    - ``preserve_insertion_order = false`` lets operators stream/flush to disk
      instead of buffering the whole result to preserve order. Safe here:
      threads=1 keeps order within the single pipeline, and admin output order
      is not contractual anyway.
    """
    from geoparquet_io.core.admin_datasets import get_cache_dir
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection, spill_directory

    temp_dir = get_cache_dir()
    temp_dir.mkdir(parents=True, exist_ok=True)
    con = get_duckdb_connection(
        load_spatial=True,
        load_httpfs=True,
        temp_directory=spill_directory(temp_dir),
        threads=1,
    )
    con.execute("SET preserve_insertion_order = false")
    # Reproject (CRS-aware admin join, #525) emits lon/lat order to match CRS84.
    con.execute("SET geometry_always_xy = true")
    return con


def _build_admin_where_clauses_list(
    con,
    dataset,
    levels,
    input_source,
    input_bbox_col,
    input_geom_col,
    admin_bbox_col,
    verbose,
    dry_run,
    is_table_ref=False,
    source_crs=None,
    admin_source=None,
):
    """Build WHERE clauses for admin boundaries."""
    admin_where_clauses = []
    # The admin source decides how much filtering the clause has to do: a
    # pre-filtered cache needs only the subtype clause, a raw release needs the
    # level's whole predicate (#819).
    subtype_filter = dataset.get_subtype_filter(levels, source=admin_source)
    if subtype_filter:
        admin_where_clauses.append(subtype_filter)
        if verbose and not dry_run:
            debug(f"Filtering admin boundaries: {subtype_filter}")

    extent_filter = _add_extent_filter(
        con,
        input_source,
        input_bbox_col,
        input_geom_col,
        admin_bbox_col,
        verbose and not dry_run,
        is_table_ref=is_table_ref,
        source_crs=source_crs,
    )
    if extent_filter:
        admin_where_clauses.append(extent_filter)

    return admin_where_clauses


def _report_join_strategy(
    has_native_geometry, input_bbox_col, admin_bbox_col, verbose, input_geom_rewritten=False
):
    """Print a status line describing which spatial-join strategy will run.

    Native-geometry inputs take the bare-ST_Intersects SPATIAL_JOIN fast path, so
    the old "No bbox columns available, using full geometry intersection..." line
    misreported them as a degraded fallback (issue #538). Only 1.x files that
    genuinely lack a bbox column get that warning now.

    A reprojected (non-CRS84) input also gets a bare ST_Intersects — its stored
    bbox is in the source CRS and is dropped from the predicate (#525) — so
    ``input_geom_rewritten`` keeps the message from claiming a bbox pre-filter
    that the emitted SQL does not contain.
    """
    strategy = spatial_join_strategy(
        has_native_geometry, input_bbox_col, admin_bbox_col, input_geom_rewritten
    )
    if strategy == SPATIAL_JOIN_NATIVE:
        progress("Using native geometry with DuckDB SPATIAL_JOIN...")
    elif strategy == SPATIAL_JOIN_BBOX_PREFILTER:
        if verbose:
            debug("Using bbox columns for initial filtering...")
    else:
        progress("No bbox columns available, using full geometry intersection...")


def _build_query_components(
    con,
    dataset,
    levels,
    partition_columns,
    input_source,
    admin_source,
    admin_geom_col,
    admin_bbox_col,
    input_geom_col,
    input_bbox_col,
    verbose,
    dry_run,
    prefix=None,
    is_table_ref=False,
    source_crs=None,
    has_native_geometry=False,
):
    """Build all query components."""
    # Use provided admin_source (may be cached local path or remote URL)
    # Quote the path for SQL safety
    read_options = dataset.get_read_parquet_options()
    admin_table_ref = (
        f"read_parquet({sql_path(admin_source)}, "
        f"{', '.join([f'{k}={v}' for k, v in read_options.items()])})"
        if read_options
        else sql_path(admin_source)
    )

    admin_where_clauses = _build_admin_where_clauses_list(
        con,
        dataset,
        levels,
        input_source,
        input_bbox_col,
        input_geom_col,
        admin_bbox_col,
        verbose,
        dry_run,
        is_table_ref=is_table_ref,
        source_crs=source_crs,
        admin_source=admin_source,
    )

    admin_select_clause = _build_admin_select_clause(
        dataset, levels, partition_columns, prefix=prefix
    )
    admin_subquery = _build_admin_subquery(
        dataset,
        levels,
        partition_columns,
        admin_table_ref,
        admin_geom_col,
        admin_bbox_col,
        admin_where_clauses,
        source_crs=source_crs,
    )

    if not dry_run:
        input_geom_rewritten = bool(source_crs) and not _admin_reprojected(
            source_crs, admin_bbox_col
        )
        _report_join_strategy(
            has_native_geometry,
            input_bbox_col,
            admin_bbox_col,
            verbose,
            input_geom_rewritten=input_geom_rewritten,
        )

    query = _build_spatial_join_query(
        input_source,
        admin_subquery,
        admin_select_clause,
        input_bbox_col,
        admin_bbox_col,
        input_geom_col,
        admin_geom_col,
        is_table_ref=is_table_ref,
        source_crs=source_crs,
    )

    return query, admin_source


def _handle_dry_run_mode(
    dry_run,
    input_path,
    admin_source,
    output_parquet,
    input_geom_col,
    admin_geom_col,
    input_bbox_col,
    admin_bbox_col,
    query,
    compression,
    compression_level,
    has_native_geometry=False,
    input_geom_rewritten=False,
):
    """Handle dry-run mode output."""
    if not dry_run:
        return False

    _print_dry_run_header(
        input_path,
        admin_source,
        output_parquet,
        input_geom_col,
        admin_geom_col,
        input_bbox_col,
        admin_bbox_col,
    )

    info("-- Main spatial join query")
    strategy = spatial_join_strategy(
        has_native_geometry, input_bbox_col, admin_bbox_col, input_geom_rewritten
    )
    if strategy == SPATIAL_JOIN_NATIVE:
        info("-- Using native geometry with DuckDB SPATIAL_JOIN")
    elif strategy == SPATIAL_JOIN_BBOX_PREFILTER:
        info("-- Using bbox columns for optimized spatial join")
    else:
        info("-- Using full geometry intersection (no bbox optimization)")

    if compression in ["GZIP", "ZSTD", "BROTLI"]:
        compression_str = f"{compression}:{compression_level}"
    else:
        compression_str = compression

    duckdb_compression = compression.lower() if compression != "UNCOMPRESSED" else "uncompressed"
    display_query = f"""COPY ({query.strip()})
TO {sql_path(output_parquet)}
(FORMAT PARQUET, COMPRESSION '{duckdb_compression}');"""
    progress(display_query)

    info(f"\n-- Note: Using {compression_str} compression")
    info("-- Original metadata would also be preserved in the output file")
    return True


def _execute_per_level_joins(
    con,
    dataset,
    levels,
    partition_columns,
    input_source,
    admin_geom_col,
    admin_bbox_col,
    input_geom_col,
    input_bbox_col,
    output_parquet,
    metadata,
    dry_run,
    verbose,
    compression,
    compression_level,
    row_group_size_mb,
    row_group_rows,
    profile,
    geoparquet_version,
    prefix,
    no_cache,
    extra_kv=None,
    source_crs=None,
    has_native_geometry=False,
    memory_limit=None,
):
    """Run separate spatial joins per admin level for per-level-source datasets.

    Each level joins against its own cache file, chaining results through DuckDB
    temp tables. Per-level caches are non-overlapping, so each plain LEFT JOIN
    normally preserves the row count.
    """
    current_source = input_source
    # Cheap (metadata-only) baseline for the row-preservation check below.
    input_rows = None if dry_run else _count_rows(con, input_source)

    for i, (level, col) in enumerate(zip(levels, partition_columns, strict=True)):
        level_admin_source = dataset.get_source_for_level(level, no_cache=no_cache)
        is_last = i == len(levels) - 1
        # Level 0 reads the input file; later levels read the chained temp table.
        is_table_ref = i != 0

        level_query, _ = _build_query_components(
            con,
            dataset,
            [level],
            [col],
            current_source,
            level_admin_source,
            admin_geom_col,
            admin_bbox_col,
            input_geom_col,
            input_bbox_col,
            verbose,
            dry_run,
            prefix=prefix,
            is_table_ref=is_table_ref,
            source_crs=source_crs,
            has_native_geometry=has_native_geometry,
        )

        if dry_run:
            output_col = dataset.get_output_column_name(level, prefix)
            progress(f"\n-- Step {i + 1}: Add {output_col} from {level} boundaries")
            progress(f"-- Source: {level_admin_source}")
            progress(level_query)
            if not is_last:
                current_source = f"_gpio_admin_step_{i}"
            continue

        if verbose:
            debug(f"Spatial join: adding {level} from {level_admin_source}...")

        if is_last:
            write_parquet_with_metadata(
                con,
                level_query,
                output_parquet,
                original_metadata=metadata,
                compression=compression,
                compression_level=compression_level,
                row_group_size_mb=row_group_size_mb,
                row_group_rows=row_group_rows,
                verbose=verbose,
                profile=profile,
                geoparquet_version=geoparquet_version,
                extra_kv_metadata=extra_kv,
                # Every level is `SELECT a.*` over the one before it, so the
                # geometry column the last write emits is the input's own --
                # the witness auto mode needs to keep a native-geo-only input
                # native rather than rewriting it as 1.1 WKB (#993).
                input_file=input_source,
                memory_limit=memory_limit,
            )
        else:
            temp_table = f"_gpio_admin_step_{i}"
            con.execute(f"CREATE OR REPLACE TEMP TABLE {temp_table} AS {level_query}")
            current_source = temp_table

    if dry_run:
        return None, None, None

    total_features, features_with_admin, unique_counts = _get_result_stats(
        con, output_parquet, dataset, levels, verbose, prefix=prefix
    )
    _warn_if_row_count_changed(input_rows, total_features)
    return total_features, features_with_admin, unique_counts


def add_admin_divisions_multi(
    input_parquet: str,
    output_parquet: str,
    dataset_name: str,
    levels: list[str],
    dataset_source: str | None = None,
    add_bbox_flag: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    overwrite: bool = False,
    prefix: str | None = None,
    no_cache: bool = False,
    vecorel: bool = False,
    memory_limit: str | None = None,
):
    """
    Add admin division columns from a multi-level admin dataset.

    Args:
        input_parquet: Input GeoParquet file (local or remote URL)
        output_parquet: Output GeoParquet file (local or remote URL)
        dataset_name: Name of admin dataset ("current", "gaul", "overture")
        levels: List of hierarchical levels to add as columns
        dataset_source: Optional custom path/URL to admin dataset
        add_bbox_flag: Automatically add bbox column if missing
        dry_run: Show SQL without executing
        verbose: Enable verbose output
        compression: Compression type
        compression_level: Compression level
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        profile: AWS profile name (S3 only, optional)
        prefix: Optional column name prefix (default: dataset name, use "admin" for admin: format)
        no_cache: Skip local cache and use remote dataset directly
        memory_limit: DuckDB memory limit for the write (e.g. '2GB')
    """
    # When vecorel mode is active, override prefix to use Vecorel column names
    effective_prefix = "vecorel" if vecorel else prefix

    # Check if output file exists and handle overwrite (fixes issue #278)
    handle_output_overwrite(output_parquet, overwrite, input_parquet)

    # Check for partition input (not supported)
    require_single_file(input_parquet, "add admin-divisions")

    # Setup dataset and columns
    (
        dataset,
        partition_columns,
        input_path,
        admin_source,
        input_geom_col,
        admin_geom_col,
        input_bbox_info,
        input_bbox_col,
        admin_bbox_col,
    ) = _setup_dataset_and_columns(
        input_parquet, dataset_name, dataset_source, levels, verbose, no_cache=no_cache
    )

    # Admin boundaries are OGC:CRS84; reproject a non-CRS84 input before the join
    # (otherwise ST_Intersects errors on the CRS mismatch or matches nothing, #525).
    source_crs = source_crs_string(input_parquet, verbose)

    # Get metadata before processing (skip in dry-run)
    metadata = None
    if not dry_run:
        metadata, _ = get_parquet_metadata(input_parquet, verbose)
        input_bbox_info = _handle_bbox_optimization(
            input_parquet, input_bbox_info, add_bbox_flag, verbose
        )
        input_bbox_col = input_bbox_info["bbox_column_name"]

        if verbose:
            debug(f"Using geometry columns: {input_geom_col} (input), {admin_geom_col} (admin)")

    # Native-geometry inputs take the bare-ST_Intersects SPATIAL_JOIN fast path
    # (issue #538); _setup_dataset_and_columns flags them via status == "native".
    has_native_geometry = input_bbox_info.get("status") == "native"

    # Create DuckDB connection with ambient S3 config from dataset
    from geoparquet_io.core.duckdb_utils import s3_config_scope

    with s3_config_scope(dataset.get_s3_config()):
        con = _setup_duckdb_connection()
        try:
            # Get total input count (skip in dry-run)
            if not dry_run:
                total_count = con.execute(
                    f"SELECT COUNT(*) FROM {sql_path(input_path)}"
                ).fetchone()[0]
                progress(f"Processing {total_count:,} input features...")

            # Build Vecorel metadata if requested (applied to whichever write path runs)
            extra_kv = None
            if vecorel:
                from geoparquet_io.core.constants import (
                    VECOREL_ADMIN_SCHEMA,
                    build_collection_metadata,
                )

                extra_kv = build_collection_metadata([VECOREL_ADMIN_SCHEMA], metadata)

            if dataset.supports_per_level_sources():
                total_features, features_with_admin, unique_counts = _execute_per_level_joins(
                    con,
                    dataset,
                    levels,
                    partition_columns,
                    input_path,
                    admin_geom_col,
                    admin_bbox_col,
                    input_geom_col,
                    input_bbox_col,
                    output_parquet,
                    metadata,
                    dry_run,
                    verbose,
                    compression,
                    compression_level,
                    row_group_size_mb,
                    row_group_rows,
                    profile,
                    geoparquet_version,
                    effective_prefix,
                    no_cache,
                    extra_kv=extra_kv,
                    source_crs=source_crs,
                    has_native_geometry=has_native_geometry,
                    memory_limit=memory_limit,
                )
                if dry_run:
                    return
            else:
                query, admin_source = _build_query_components(
                    con,
                    dataset,
                    levels,
                    partition_columns,
                    input_path,
                    admin_source,
                    admin_geom_col,
                    admin_bbox_col,
                    input_geom_col,
                    input_bbox_col,
                    verbose,
                    dry_run,
                    prefix=effective_prefix,
                    source_crs=source_crs,
                    has_native_geometry=has_native_geometry,
                )

                if _handle_dry_run_mode(
                    dry_run,
                    input_path,
                    admin_source,
                    output_parquet,
                    input_geom_col,
                    admin_geom_col,
                    input_bbox_col,
                    admin_bbox_col,
                    query,
                    compression,
                    compression_level,
                    has_native_geometry=has_native_geometry,
                    input_geom_rewritten=(
                        bool(source_crs) and not _admin_reprojected(source_crs, admin_bbox_col)
                    ),
                ):
                    return

                if verbose:
                    debug("Performing spatial join with admin boundaries...")

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
                    extra_kv_metadata=extra_kv,
                    # The join is `SELECT a.*` plus admin columns, so the
                    # geometry reaching the output is the input's own (#993).
                    input_file=input_path,
                    memory_limit=memory_limit,
                )

                total_features, features_with_admin, unique_counts = _get_result_stats(
                    con, output_parquet, dataset, levels, verbose, prefix=effective_prefix
                )
        finally:
            con.close()

    # Ensure Vecorel-required id column exists
    if vecorel:
        from geoparquet_io.core.constants import ensure_vecorel_columns

        ensure_vecorel_columns(output_parquet, verbose)

    progress("\nResults:")
    progress(
        f"- Added admin division data to {features_with_admin:,} of {total_features:,} features"
    )
    for level, count in unique_counts:
        progress(f"- Found {count:,} unique {level} values")

    success(f"\nSuccessfully wrote output to: {output_parquet}")
