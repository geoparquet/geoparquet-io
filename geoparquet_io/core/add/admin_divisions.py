#!/usr/bin/env python3

"""
Add admin division columns from multiple datasets.

The one "which admin region is this feature in" implementation: every admin
dataset, with hierarchical level support.
"""

from dataclasses import dataclass, field

import duckdb

from geoparquet_io.core.admin_datasets import (
    AdminDataset,
    AdminJoinStep,
    admin_column_ref,
    admin_input_reprojected,
    open_admin_join_connection,
    resolve_admin_dataset,
    run_admin_join,
)
from geoparquet_io.core.bbox_structure import check_bbox_structure, get_bbox_advice
from geoparquet_io.core.common import get_parquet_metadata
from geoparquet_io.core.crs_utils import (
    source_crs_string,
)
from geoparquet_io.core.duckdb_utils import (
    SPATIAL_JOIN_BBOX_PREFILTER,
    SPATIAL_JOIN_NATIVE,
    fetch_one,
    print_spatial_join_dry_run,
    quote_identifier,
    s3_config_scope,
    spatial_join_strategy,
    sql_path,
)
from geoparquet_io.core.file_utils import handle_output_overwrite, resolve_file_url
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import debug, info, progress, success, warn
from geoparquet_io.core.parquet_writer import resolve_row_group_rows_for_query
from geoparquet_io.core.partition.reader import require_single_file
from geoparquet_io.core.remote import display_path
from geoparquet_io.core.write_funnels import write_parquet_with_metadata


def _build_admin_select_clause(dataset, levels, partition_columns, prefix=None):
    """Build SELECT clause for admin columns with transformations."""
    use_coalesce = prefix == "vecorel"
    admin_select_parts = []
    for i, (level, col) in enumerate(zip(levels, partition_columns, strict=True)):
        output_col_name = dataset.get_output_column_name(level, prefix=prefix)
        col_transform = dataset.get_column_transform(level)

        expr = col_transform or admin_column_ref(i, col)
        if use_coalesce:
            expr = f"COALESCE({expr}, 'ZZ')"

        admin_select_parts.append(f"{expr} as {quote_identifier(output_col_name)}")

    return ", ".join(admin_select_parts)


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
    display_input = display_path(input_path)
    display_admin = display_path(admin_source)
    display_output = display_path(output_parquet)
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

    stats = fetch_one(con.execute(stats_query))
    total_features = stats[0]
    features_with_admin = stats[1]

    unique_counts = []
    for level, output_col in zip(levels, output_col_names, strict=True):
        count_query = f"""
        SELECT COUNT(DISTINCT {quote_identifier(output_col)}) as unique_count
        FROM {sql_path(output_parquet)}
        WHERE {quote_identifier(output_col)} IS NOT NULL;
        """
        result = fetch_one(con.execute(count_query))
        unique_counts.append((level, result[0]))

    return total_features, features_with_admin, unique_counts


def _setup_dataset_and_columns(
    input_parquet, dataset_name, dataset_source, levels, verbose, no_cache=False, memory_limit=None
):
    """Setup dataset and get column information."""

    # --write-memory also bounds the dataset's cache downloads (#1156), the
    # first of which may run right here.
    dataset, partition_columns, admin_source = resolve_admin_dataset(
        dataset_name,
        levels,
        cache=True,
        source_path=dataset_source,
        no_cache=no_cache,
        memory_limit=memory_limit,
        verbose=verbose,
    )

    # RAW path: it is shown in the dry-run header, and run_admin_join puts it
    # through sql_path at the SQL boundary (#802).
    input_path = resolve_file_url(input_parquet, verbose)

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


@dataclass
class _AdminAddRun:
    """What ``add admin-divisions`` does around each step of :func:`run_admin_join`."""

    dataset: AdminDataset
    prefix: str | None
    dry_run: bool
    verbose: bool
    input_path: str
    output_parquet: str
    input_geom_col: str
    input_bbox_col: str | None
    admin_bbox_col: str | None
    has_native_geometry: bool
    input_rewritten: bool
    write_kwargs: dict = field(default_factory=dict)
    con: duckdb.DuckDBPyConnection | None = None

    def select_for(self, levels, columns):
        return _build_admin_select_clause(self.dataset, levels, columns, prefix=self.prefix)

    def report(self, step: AdminJoinStep) -> None:
        """The dry run's SQL, or the join's status line, for one step."""
        if self.dry_run:
            self._print_dry_run(step)
            return
        _report_join_strategy(
            self.has_native_geometry,
            self.input_bbox_col,
            self.admin_bbox_col,
            self.verbose,
            input_geom_rewritten=self.input_rewritten,
        )
        if not self.verbose:
            return
        if step.per_level:
            debug(f"Spatial join: adding {step.levels[0]} from {step.source}...")
        else:
            debug("Performing spatial join with admin boundaries...")

    def _print_dry_run(self, step: AdminJoinStep) -> None:
        if step.per_level:
            output_col = self.dataset.get_output_column_name(step.levels[0], self.prefix)
            progress(
                f"\n-- Step {step.index + 1}: Add {output_col} from {step.levels[0]} boundaries"
            )
            progress(f"-- Source: {step.source}")
            progress(step.query)
            return
        _print_dry_run_header(
            self.input_path,
            step.source,
            self.output_parquet,
            self.input_geom_col,
            self.dataset.get_geometry_column(),
            self.input_bbox_col,
            self.admin_bbox_col,
        )
        info("-- Main spatial join query")
        strategy = spatial_join_strategy(
            self.has_native_geometry, self.input_bbox_col, self.admin_bbox_col, self.input_rewritten
        )
        print_spatial_join_dry_run(
            strategy,
            step.query,
            self.output_parquet,
            self.write_kwargs["compression"],
            self.write_kwargs["compression_level"],
            resolve_row_group_rows_for_query(
                self.con,
                step.query,
                self.write_kwargs["row_group_rows"],
                self.write_kwargs["row_group_size_mb"],
            ),
        )

    def write(self, step: AdminJoinStep) -> None:
        """The last step: the join, written through the funnel."""
        write_parquet_with_metadata(self.con, step.query, self.output_parquet, **self.write_kwargs)


def add_admin_divisions_multi(
    input_parquet: str,
    output_parquet: str,
    dataset_name: str,
    levels: list[str],
    dataset_source: str | None = None,
    add_bbox_flag: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
    compression: str | None = None,
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
        _partition_columns,
        input_path,
        admin_source,
        input_geom_col,
        admin_geom_col,
        input_bbox_info,
        input_bbox_col,
        admin_bbox_col,
    ) = _setup_dataset_and_columns(
        input_parquet,
        dataset_name,
        dataset_source,
        levels,
        verbose,
        no_cache=no_cache,
        memory_limit=memory_limit,
    )

    # Admin boundaries are OGC:CRS84; reproject a non-CRS84 input before the join
    # (otherwise ST_Intersects errors on the CRS mismatch or matches nothing, #525).
    source_crs = source_crs_string(input_parquet, verbose, input_geom_col)

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

    run = _AdminAddRun(
        dataset=dataset,
        prefix=effective_prefix,
        dry_run=dry_run,
        verbose=verbose,
        input_path=input_path,
        output_parquet=output_parquet,
        input_geom_col=input_geom_col,
        input_bbox_col=input_bbox_col,
        admin_bbox_col=admin_bbox_col,
        has_native_geometry=has_native_geometry,
        input_rewritten=admin_input_reprojected(source_crs, admin_bbox_col),
        write_kwargs={
            "original_metadata": metadata,
            "compression": compression,
            "compression_level": compression_level,
            "row_group_size_mb": row_group_size_mb,
            "row_group_rows": row_group_rows,
            "verbose": verbose,
            "profile": profile,
            "geoparquet_version": geoparquet_version,
            # The join is `SELECT a.*` plus admin columns at every level, so
            # the geometry reaching the output is the input's own -- the
            # witness auto mode needs to keep a native-geo-only input native
            # rather than rewriting it as 1.1 WKB (#993).
            "input_file": input_path,
            "memory_limit": memory_limit,
        },
    )

    # Create DuckDB connection with ambient S3 config from dataset
    with s3_config_scope(dataset.get_s3_config()):
        con = open_admin_join_connection(spill_to_cache=True)
        run.con = con
        try:
            # Get total input count (skip in dry-run)
            if not dry_run:
                total_count = fetch_one(
                    con.execute(f"SELECT COUNT(*) FROM {sql_path(input_path)}")
                )[0]
                progress(f"Processing {total_count:,} input features...")

            # Build Vecorel metadata if requested (applied to whichever write path runs)
            if vecorel:
                from geoparquet_io.core.constants import (
                    VECOREL_ADMIN_SCHEMA,
                    build_collection_metadata,
                )

                run.write_kwargs["extra_kv_metadata"] = build_collection_metadata(
                    [VECOREL_ADMIN_SCHEMA], metadata
                )

            run_admin_join(
                con,
                dataset,
                levels,
                input_path=input_path,
                input_geom_col=input_geom_col,
                input_bbox_col=input_bbox_col,
                admin_bbox_col=admin_bbox_col,
                select_for=run.select_for,
                final_step=run.write,
                admin_source=admin_source,
                source_crs=source_crs,
                no_cache=no_cache,
                memory_limit=memory_limit,
                verbose=verbose,
                dry_run=dry_run,
                on_step=run.report,
            )
            if dry_run:
                return

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
