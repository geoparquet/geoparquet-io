#!/usr/bin/env python3

"""
Hierarchical admin partition functionality.

This module provides partitioning by administrative boundaries through a two-step process:
1. Spatial join with remote admin boundaries dataset to add admin columns
2. Partition the enriched data by those admin columns
"""

from __future__ import annotations

import os
import shutil

import duckdb

from geoparquet_io.core.admin_datasets import (
    AdminJoinStep,
    admin_column_ref,
    admin_input_reprojected,
    open_admin_join_connection,
    resolve_admin_dataset,
    run_admin_join,
)
from geoparquet_io.core.bbox_structure import check_bbox_structure
from geoparquet_io.core.common import get_parquet_metadata
from geoparquet_io.core.crs_utils import (
    source_crs_string,
)
from geoparquet_io.core.duckdb_utils import (
    SPATIAL_JOIN_BBOX_PREFILTER,
    fetch_one,
    fetch_rows,
    query_schema,
    quote_identifier,
    spatial_join_strategy,
    sql_path,
)
from geoparquet_io.core.exceptions import PartitionError
from geoparquet_io.core.file_utils import output_is_kept, resolve_file_url
from geoparquet_io.core.geometry_detection import (
    primary_geometry_column,
    require_geometry_column,
)
from geoparquet_io.core.logging_config import debug, progress, success, warn
from geoparquet_io.core.parquet_writer import WriteOptions, resolve_output_geoparquet_version
from geoparquet_io.core.partition.common import raise_if_no_rows, sanitize_filename
from geoparquet_io.core.partition.staging import (
    PartitionWriteOptions,
    check_output_collision,
    create_staging_dir,
    finalize_partition_file,
    iter_staging_partitions,
    make_partition_aliases,
    run_partitioned_copy,
)
from geoparquet_io.core.streaming import is_stdin, read_stdin_to_temp_file


def _get_input_file_info(input_parquet, verbose):
    """Get input file info (RAW path, geometry column, bbox column).

    The path is returned unescaped: it is chained through ``current_source``,
    which alternately holds a temp-table name, so the escape belongs at the SQL
    boundary (:func:`sql_path`) rather than in this return value (#802).
    """
    input_path = resolve_file_url(input_parquet, verbose)
    input_geom_col = require_geometry_column(
        primary_geometry_column(input_parquet, verbose=verbose), input_parquet
    )
    input_bbox_info = check_bbox_structure(input_parquet, verbose)
    input_bbox_col = input_bbox_info["bbox_column_name"]

    return input_path, input_geom_col, input_bbox_col


def _report_join_strategy(input_bbox_col, admin_bbox_col, source_crs):
    """Name the predicate the join runs, as the owner classifies it (#538).

    ``partition admin`` keeps an input's bbox column whatever its geometry
    encoding, so its join never takes the native-geometry path; the bbox
    pre-filter goes only when the input geometry is rewritten (#525).
    """
    strategy = spatial_join_strategy(
        False,
        input_bbox_col,
        admin_bbox_col,
        admin_input_reprojected(source_crs, admin_bbox_col),
    )
    if strategy == SPATIAL_JOIN_BBOX_PREFILTER:
        debug("  → Using bbox columns for optimized spatial join")
    else:
        debug("  → Using full geometry intersection (no bbox optimization)")


def _build_admin_select_for_partitioning(levels, boundary_columns, dataset=None, vecorel=False):
    """Build admin SELECT clause for partitioning.

    In normal mode the admin columns get temporary internal names
    (``_admin_<level>``) used only to drive the partition split. In Vecorel
    mode they are named with Vecorel-compliant column names (e.g.
    ``admin:country_code``) and any dataset column transform (such as stripping
    the country prefix from Overture region codes) is applied, so the columns
    can be written into the output partitions.
    """
    admin_select_parts = []
    output_column_names = []
    for i, (level, col) in enumerate(zip(levels, boundary_columns, strict=True)):
        if vecorel:
            output_col = dataset.get_output_column_name(level, prefix="vecorel")
            col_transform = dataset.get_column_transform(level)
        else:
            output_col = f"_admin_{level}"  # Temporary internal name
            col_transform = None
        output_column_names.append(output_col)

        expr = col_transform or admin_column_ref(i, col)

        admin_select_parts.append(f"{expr} as {quote_identifier(output_col)}")

    return ", ".join(admin_select_parts), output_column_names


def _enrich_with_admin(
    con,
    dataset,
    levels,
    enriched_table,
    input_path,
    input_geom_col,
    input_bbox_col,
    admin_bbox_col,
    vecorel,
    verbose,
    source_crs=None,
    memory_limit=None,
):
    """Fill ``enriched_table`` with the input plus its admin columns.

    The join is :func:`run_admin_join`'s: for a dataset with per-level sources
    (Overture's land-only caches, see
    ``OvertureAdminDataset._build_level_cache_query``) one join per level,
    each reading the one before; for any other, one join of every level. Each
    join keeps one admin region per input feature
    (:func:`~geoparquet_io.core.duckdb_utils.build_spatial_join_query`, #543),
    so ``enriched_table`` has exactly the input's rows; the per-level caches
    are about cost -- smaller, pre-filtered polygons to join against.

    The last join materializes the whole input into the temp table, the
    largest piece of DuckDB work in the command -- bigger than the split that
    follows it. Like every chained join, it runs under the same memory policy
    as every gpio write (``--write-memory`` when given, else the
    ceiling-based default): left unbounded it sized itself for the host and
    was measured at 14.3 GiB on 12 threads while ``--write-memory 700MB`` was
    in force (#1174).

    Returns the list of output admin column names.
    """
    output_column_names = []

    def select_for(step_levels, columns):
        select_clause, names = _build_admin_select_for_partitioning(
            step_levels, columns, dataset=dataset, vecorel=vecorel
        )
        output_column_names.extend(names)
        return select_clause

    def report(step: AdminJoinStep) -> None:
        if verbose and step.per_level:
            debug(
                f"  → Level {step.index + 1}/{step.count}: "
                f"joining {step.levels[0]} from {step.source}"
            )

    run_admin_join(
        con,
        dataset,
        levels,
        input_path=input_path,
        input_geom_col=input_geom_col,
        input_bbox_col=input_bbox_col,
        admin_bbox_col=admin_bbox_col,
        select_for=select_for,
        final_table=enriched_table,
        source_crs=source_crs,
        memory_limit=memory_limit,
        verbose=verbose,
        on_step=report,
    )
    return output_column_names


def _verify_enrichment_results(con, enriched_table, output_column_names):
    """Verify enrichment results and return stats.

    Also warns when features match a coarser level but are NULL at a finer level:
    they pass the "matched" (any-level) check but are excluded from every
    partition by the all-levels-NOT-NULL filter, so they would otherwise vanish
    silently (#480).
    """
    any_clause = " OR ".join(
        [f"{quote_identifier(col)} IS NOT NULL" for col in output_column_names]
    )
    all_clause = " AND ".join(
        [f"{quote_identifier(col)} IS NOT NULL" for col in output_column_names]
    )
    stats_query = f"""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN {any_clause} THEN 1 END) as with_admin,
            COUNT(CASE WHEN {all_clause} THEN 1 END) as with_all_admin
        FROM {enriched_table}
    """
    total_count, with_admin_count, with_all_count = fetch_one(con.execute(stats_query))

    success(f"  ✓ Matched {with_admin_count:,} of {total_count:,} features to admin boundaries")

    if with_admin_count == 0:
        raise PartitionError(
            "No features matched to admin boundaries. Check that input data and boundaries "
            "are in compatible CRS and overlap geographically."
        )

    dropped = with_admin_count - with_all_count
    if dropped > 0:
        warn(
            f"  ⚠️  {dropped:,} feature(s) matched a coarser admin level but are missing a finer "
            "level; they will not appear in any partition (incomplete admin hierarchy)."
        )

    return total_count, with_admin_count


def _get_original_columns(con, input_path):
    """Get original column names from the (RAW) input file path."""
    return query_schema(con, f"SELECT * FROM {sql_path(input_path)}").names


def _build_admin_staging_select(
    enriched_table, output_column_names, original_cols, vecorel, aliases
):
    """Build the SELECT for the single admin partitioned COPY.

    Keeps the original columns (plus the admin columns in Vecorel mode, so each
    partition is Vecorel-compliant) and adds one aliased partition key per level
    (``aliases``, guaranteed not to collide with a real column). PARTITION_BY
    drops those aliases from the written files, so the admin columns are dropped
    from non-Vecorel output exactly as before.
    """
    keep_cols = list(original_cols)
    if vecorel:
        keep_cols += list(output_column_names)
    keep_sql = ", ".join(quote_identifier(col) for col in keep_cols)

    alias_sql = ", ".join(
        f"{quote_identifier(col)} AS {alias}"
        for col, alias in zip(output_column_names, aliases, strict=True)
    )
    where_sql = " AND ".join(f"{quote_identifier(col)} IS NOT NULL" for col in output_column_names)
    return f"SELECT {keep_sql}, {alias_sql} FROM {enriched_table} WHERE {where_sql}"


def _finalize_admin_partition(
    con,
    values,
    partition_dir,
    levels,
    output_folder,
    hive,
    filename_prefix,
    overwrite,
    metadata,
    verbose,
    vecorel,
    write_options,
    seen_outputs,
):
    """Rewrite one staging partition into its final nested location."""
    folder_parts = [
        f"{level}={sanitize_filename(str(value))}" if hive else sanitize_filename(str(value))
        for level, value in zip(levels, values, strict=True)
    ]
    partition_folder = os.path.join(output_folder, *folder_parts)

    safe_last = sanitize_filename(str(values[-1]))
    filename = (
        f"{filename_prefix}_{safe_last}.parquet" if filename_prefix else f"{safe_last}.parquet"
    )
    output_file = os.path.join(partition_folder, filename)
    check_output_collision(seen_outputs, output_file, tuple(values))

    os.makedirs(partition_folder, exist_ok=True)
    if verbose and not output_is_kept(output_file, overwrite):
        debug(f"  → Creating: {'/'.join(folder_parts)}")

    created = finalize_partition_file(
        con, partition_dir, output_file, metadata, overwrite, verbose, write_options
    )

    # Ensure Vecorel schema compliance (id column + non-nullable columns) — DuckDB
    # cannot write non-nullable Parquet columns, so this rewrites in place.
    if created and vecorel:
        from geoparquet_io.core.constants import ensure_vecorel_columns

        ensure_vecorel_columns(output_file, verbose)

    return created


def _create_all_partitions(
    con,
    enriched_table,
    output_column_names,
    levels,
    output_folder,
    hive,
    filename_prefix,
    overwrite,
    metadata,
    input_parquet,
    verbose,
    original_cols,
    write: WriteOptions,
    vecorel=False,
    extra_kv=None,
):
    """Create all partition files in a single pass.

    Routes the enriched rows into a staging dir with one DuckDB
    ``COPY ... PARTITION_BY`` (no per-combination re-scan, issue #478), then
    rewrites each (small) staging partition into its final nested file with the
    correct per-partition metadata.

    ``input_parquet`` is the user's own file, and it is here because the
    version this run writes, and the CRS and edges each partition declares, have
    to come from it, not from the staging file each partition write actually
    reads. See ``PartitionWriteOptions``.
    Unlike the index-adding partition drivers, the admin join reads the user's
    file directly -- the enriched table is ``a.*`` plus the admin columns, with
    only the join predicate reprojected -- so this file is a truthful witness
    for the geometry that will be written.
    """
    # Aliases must not collide with kept columns (originals + admin columns).
    aliases = make_partition_aliases(
        len(output_column_names), list(original_cols) + list(output_column_names)
    )
    write_options = PartitionWriteOptions(
        geoparquet_version=resolve_output_geoparquet_version(
            write.geoparquet_version, input_file=input_parquet, verbose=verbose
        ),
        input_file=input_parquet,
        write=write,
        extra_kv_metadata=extra_kv,
    )

    staging_dir = create_staging_dir(output_folder)
    partition_count = 0
    seen_outputs: dict[str, tuple] = {}
    try:
        select_sql = _build_admin_staging_select(
            enriched_table, output_column_names, original_cols, vecorel, aliases
        )
        run_partitioned_copy(con, select_sql, aliases, staging_dir, verbose, write.memory_limit)

        for values, partition_dir in iter_staging_partitions(staging_dir):
            if _finalize_admin_partition(
                con,
                values,
                partition_dir,
                levels,
                output_folder,
                hive,
                filename_prefix,
                overwrite,
                metadata,
                verbose,
                vecorel,
                write_options,
                seen_outputs,
            ):
                partition_count += 1
            # Incremental cleanup caps peak staging disk at ~one partition.
            shutil.rmtree(partition_dir, ignore_errors=True)
    finally:
        shutil.rmtree(staging_dir, ignore_errors=True)

    return partition_count


def partition_by_admin_hierarchical(
    input_parquet: str,
    output_folder: str | None,
    dataset_name: str,
    levels: list[str],
    hive: bool = False,
    overwrite: bool = False,
    preview: bool = False,
    preview_limit: int = 15,
    verbose: bool = False,
    force: bool = False,
    skip_analysis: bool = False,
    filename_prefix: str | None = None,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    compression: str | None = None,
    compression_level: int | None = None,
    row_group_size_mb: int | None = None,
    row_group_rows: int | None = None,
    memory_limit: str | None = None,
    vecorel: bool = False,
) -> int:
    """
    Partition a GeoParquet file by administrative boundaries.

    Supports Arrow IPC streaming for input:
    - Input "-" reads from stdin (output is always a directory)

    This performs a two-step operation:
    1. Spatial join with remote admin boundaries to add admin columns
    2. Partition the enriched data by those admin columns

    Args:
        input_parquet: Input GeoParquet file (local, remote URL, or "-" for stdin)
        output_folder: Output directory for partitioned files
        dataset_name: Name of admin dataset ("gaul", "overture")
        levels: List of hierarchical levels to partition by
        hive: Use Hive-style partitioning
        overwrite: Overwrite existing partition files
        preview: Preview partitions without creating files
        preview_limit: Number of partitions to show in preview
        verbose: Enable verbose output
        force: Force partitioning even if analysis detects issues
        skip_analysis: Skip partition strategy analysis
        filename_prefix: Prefix for output filenames
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write
        compression: Compression codec (default: ZSTD)
        compression_level: Compression level (default: 15)
        row_group_size_mb: Row group size in MB (mutually exclusive with row_group_rows)
        row_group_rows: Row group size in number of rows (mutually exclusive with row_group_size_mb)
        memory_limit: DuckDB memory limit for write operations (e.g., "2GB")
        vecorel: Output Vecorel-compliant admin columns (admin:country_code,
            admin:subdivision_code) in each partition along with Vecorel
            collection metadata. Expects the Overture dataset with
            country,region levels.

    Returns:
        Number of partitions created
    """
    # Handle stdin input
    stdin_temp_file = None
    actual_input = input_parquet

    if is_stdin(input_parquet):
        stdin_temp_file = read_stdin_to_temp_file(verbose)
        actual_input = stdin_temp_file

    try:
        # Nothing to partition: stop before fetching remote admin
        # boundaries and spatially joining against nothing (#823).
        raise_if_no_rows(actual_input)

        # Setup dataset and get input file info. --write-memory also bounds
        # the dataset's cache downloads (#1156).
        dataset, _boundary_columns, _ = resolve_admin_dataset(
            dataset_name, levels, cache=False, memory_limit=memory_limit, verbose=verbose
        )
        input_path, input_geom_col, input_bbox_col = _get_input_file_info(actual_input, verbose)

        # Admin boundaries are OGC:CRS84; reproject a non-CRS84 input before the
        # join so ST_Intersects neither errors on a CRS mismatch nor silently
        # matches nothing (#525).
        source_crs = source_crs_string(actual_input, verbose, input_geom_col)

        admin_bbox_col = dataset.get_bbox_column()

        # Use ambient S3 config from dataset, applied via get_duckdb_connection
        from geoparquet_io.core.duckdb_utils import s3_config_scope

        with s3_config_scope(dataset.get_s3_config()):
            con = open_admin_join_connection(spill_to_cache=dataset.supports_per_level_sources())

            # STEP 1: Spatial join to create enriched data with admin columns
            progress("\n📍 Step 1/2: Performing spatial join with admin boundaries...")

            enriched_table = "_enriched_with_admin"

            if verbose:
                _report_join_strategy(input_bbox_col, admin_bbox_col, source_crs)

            output_column_names = _enrich_with_admin(
                con,
                dataset,
                levels,
                enriched_table,
                input_path,
                input_geom_col,
                input_bbox_col,
                admin_bbox_col,
                vecorel,
                verbose,
                source_crs=source_crs,
                memory_limit=memory_limit,
            )

            # Verify enrichment results
            _verify_enrichment_results(con, enriched_table, output_column_names)

            # STEP 2: Partition the enriched data
            progress(f"\n📁 Step 2/2: Partitioning by {' → '.join(levels)}...")

            # Preview mode
            if preview:
                _preview_hierarchical_partitions(
                    con,
                    enriched_table,
                    output_column_names,
                    levels,
                    preview_limit,
                    verbose,
                )
                return 0

            # Get metadata from input for preservation
            metadata, _ = get_parquet_metadata(actual_input, verbose)

            # Build Vecorel collection metadata if requested
            extra_kv = None
            if vecorel:
                from geoparquet_io.core.constants import (
                    VECOREL_ADMIN_SCHEMA,
                    build_collection_metadata,
                )

                extra_kv = build_collection_metadata([VECOREL_ADMIN_SCHEMA], metadata)

            # Create output directory
            os.makedirs(output_folder, exist_ok=True)

            # Get original columns (exclude temporary admin columns)
            original_cols = _get_original_columns(con, input_path)

            # Create each partition
            partition_count = _create_all_partitions(
                con,
                enriched_table,
                output_column_names,
                levels,
                output_folder,
                hive,
                filename_prefix,
                overwrite,
                metadata,
                actual_input,
                verbose,
                original_cols,
                WriteOptions.pick(locals()),
                vecorel,
                extra_kv,
            )

        success(f"\n✓ Created {partition_count} partition(s) in {output_folder}")

        return partition_count
    finally:
        # Clean up stdin temp file
        if stdin_temp_file and os.path.exists(stdin_temp_file):
            os.remove(stdin_temp_file)


def _get_preview_partitions(con, table_name, partition_columns, level_names):
    """Query partition statistics for preview."""
    group_by_cols = ", ".join([quote_identifier(col) for col in partition_columns])
    select_cols = ", ".join(
        [
            f"{quote_identifier(col)} as {quote_identifier(name)}"
            for col, name in zip(partition_columns, level_names, strict=True)
        ]
    )

    query = f"""
        SELECT
            {select_cols},
            COUNT(*) as record_count
        FROM {table_name}
        WHERE {" AND ".join([f"{quote_identifier(col)} IS NOT NULL" for col in partition_columns])}
        GROUP BY {group_by_cols}
        ORDER BY record_count DESC
    """

    return fetch_rows(con, query)


def _display_preview_header(level_names, all_partitions, total_records, limit):
    """Display preview header and stats."""
    progress(f"\n📊 Partition Preview ({' → '.join(level_names)}):")
    progress(f"  Total partitions: {len(all_partitions)}")
    progress(f"  Total records: {total_records:,}")
    progress(f"\n  Top {min(limit, len(all_partitions))} partitions by size:")

    header_parts = [f"{name:<25}" for name in level_names]
    header_parts.append(f"{'Records':>15}")
    header_parts.append(f"{'%':>8}")
    header = "  ".join(header_parts)
    progress(f"\n  {header}")
    progress(f"  {'-' * len(header)}")
    return header


def _display_preview_rows(all_partitions, limit, total_records):
    """Display preview rows."""
    for i, row in enumerate(all_partitions):
        if i >= limit:
            break

        values = row[:-1]
        count = row[-1]
        percentage = (count / total_records) * 100

        row_parts = [f"{str(val):<25}" for val in values]
        row_parts.append(f"{count:>15,}")
        row_parts.append(f"{percentage:>7.1f}%")
        progress(f"  {'  '.join(row_parts)}")


def _display_preview_summary(all_partitions, limit, total_records, header):
    """Display preview summary if more exist."""
    if len(all_partitions) > limit:
        remaining = len(all_partitions) - limit
        remaining_records = sum(row[-1] for row in all_partitions[limit:])
        remaining_pct = (remaining_records / total_records) * 100
        progress(f"  {'-' * len(header)}")
        progress(
            f"  ... and {remaining} more partition(s) with {remaining_records:,} records ({remaining_pct:.1f}%)"
        )
        progress("\n  Use --preview-limit to show more partitions")


def _preview_hierarchical_partitions(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    partition_columns: list[str],
    level_names: list[str],
    limit: int,
    verbose: bool,
) -> None:
    """Preview hierarchical partitions without creating files."""
    all_partitions = _get_preview_partitions(con, table_name, partition_columns, level_names)

    if len(all_partitions) == 0:
        warn("\n⚠️  No partitions would be created (no features with admin boundaries)")
        return

    total_records = sum(row[-1] for row in all_partitions)
    header = _display_preview_header(level_names, all_partitions, total_records, limit)
    _display_preview_rows(all_partitions, limit, total_records)
    _display_preview_summary(all_partitions, limit, total_records, header)
