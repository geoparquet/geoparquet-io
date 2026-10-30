"""Shared drivers for spatial index enrichment and partitioning.

A5 cells are produced by the installed DuckDB community extension. Different
DuckDB/extension builds can return different A5 IDs (#1361); the driver must
never translate IDs to imitate another build. Golden tests pin each build.
"""

from __future__ import annotations

import math
import os
import tempfile
import uuid
from collections.abc import Callable
from dataclasses import dataclass

from geoparquet_io.core.bbox_structure import get_bbox_advice
from geoparquet_io.core.compression import print_copy_dry_run
from geoparquet_io.core.crs_utils import (
    crs_string_from_table,
    source_crs_string,
    transform_geom_sql,
)
from geoparquet_io.core.duckdb_metadata import get_column_names
from geoparquet_io.core.duckdb_utils import (
    describe_query,
    get_duckdb_connection,
    load_community_extension,
    query_schema,
    quote_identifier,
    register_arrow,
    sql_path,
    uses_aws_profile,
)
from geoparquet_io.core.exceptions import GeoParquetError, InvalidParameterError, PartitionError
from geoparquet_io.core.file_utils import handle_output_overwrite, resolve_file_url
from geoparquet_io.core.geo_metadata import sanitized_carried_geo, sanitized_carried_geo_of_file
from geoparquet_io.core.geometry_detection import (
    detect_geometry_column_from_names,
    find_primary_geometry_column,
)
from geoparquet_io.core.geometry_repair import warn_planar_math, warn_planar_math_for_file
from geoparquet_io.core.logging_config import (
    configure_verbose,
    debug,
    info,
    progress,
    success,
    warn,
)
from geoparquet_io.core.partition.common import preview_partition
from geoparquet_io.core.partition.reader import require_single_file
from geoparquet_io.core.remote import (
    validate_profile_for_urls,
)
from geoparquet_io.core.streaming import (
    find_geometry_column_from_table,
    is_stdin,
    should_stream_output,
)


@dataclass(frozen=True)
class IndexSpec:
    name: str
    column_name: str
    min_resolution: int
    max_resolution: int
    expression: Callable[[str, str, int | str], str]
    base_cells: int
    subdivision: int
    extension: str | None
    size: Callable[[int], str]

    default_resolution: int = 0
    register: Callable | None = None
    resolution_function: str = ""
    parent_template: str = ""
    longitude_first: bool = False
    label: str = ""
    unit: str = "resolution"
    extension_preflight: bool = False
    bbox_midpoint: bool = False
    crs_validator: Callable | None = None
    prefix_partition: bool = False
    coerce_resolution: bool = False
    wrap_auto_error: bool = True
    column_option: str = ""
    display_unit: str = ""
    multiple_files: bool = False
    preview_sql: bool = True
    report_count: bool = True


def preflight_index(spec, command, *, directory=False):
    from geoparquet_io.core.duckdb_utils import require_community_extension

    if spec.extension_preflight or (directory and spec.extension):
        require_community_extension(spec.extension, feature=f"gpio {command} {spec.name}")


def validate_index_crs(spec, geo_meta, geom_col, source_crs, description, verbose=False):
    """Apply the spec's CRS requirement only when no transform is available."""
    if spec.crs_validator is not None and source_crs is None:
        spec.crs_validator(geo_meta, geom_col, verbose, source_description=description)


def table_bbox_column(spec, table, source_crs, use_centroid):
    if not spec.bbox_midpoint or use_centroid or source_crs is not None:
        return None
    from geoparquet_io.core.arrow_geo_metadata import _detect_bbox_column_from_table

    return _detect_bbox_column_from_table(table)


def load_index(con, spec, verbose=False, *, feature=None):
    if spec.extension:
        if verbose:
            debug(f"Loading DuckDB extension: {spec.extension}")
        feature = feature or (f"gpio add {spec.name}" if spec.extension_preflight else None)
        kwargs = {"feature": feature} if feature else {}
        load_community_extension(con, spec.extension, **kwargs)
    if spec.register is not None:
        spec.register(con)


def index_expression(spec, geom_col, resolution, source_crs=None, bbox_col=None):
    if bbox_col:
        bbox = quote_identifier(bbox_col)
        lat = f"(({bbox}.ymin + {bbox}.ymax) / 2.0)"
        lon = f"(({bbox}.xmin + {bbox}.xmax) / 2.0)"
    else:
        geom = transform_geom_sql(quote_identifier(geom_col), source_crs)
        lat, lon = f"ST_Y(ST_Centroid({geom}))", f"ST_X(ST_Centroid({geom}))"
    return spec.expression(lon, lat, resolution)


def validate_index_resolution(spec, resolution, table=False, parameter=None):
    if not spec.min_resolution <= resolution <= spec.max_resolution:
        bounds = (
            f"must be between {spec.min_resolution} and {spec.max_resolution}, got {resolution}"
        )
        if table:
            if spec.coerce_resolution:
                raise ValueError(
                    f"resolution must be between {spec.min_resolution} and {spec.max_resolution} inclusive, got {resolution}"
                )
            raise ValueError(f"{spec.label} {spec.unit} {bounds}")
        raise InvalidParameterError(parameter or spec.unit, bounds)


def add_index_table(spec, table, column_name, resolution, geometry_column=None, use_centroid=False):
    if spec.coerce_resolution:
        resolution = int(resolution)
    validate_index_resolution(spec, resolution, table=True)
    preflight_index(spec, "add")
    geom_col = geometry_column or find_geometry_column_from_table(table)
    if geom_col is None:
        raise GeoParquetError("No geometry column found")
    source_crs = crs_string_from_table(table, geom_col)
    validate_index_crs(
        spec, sanitized_carried_geo(table.schema.metadata), geom_col, source_crs, "table"
    )
    bbox_col = table_bbox_column(spec, table, source_crs, use_centroid)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        con.execute("SET geometry_always_xy = true;")
        load_index(con, spec)
        warn_planar_math(f"add_{spec.name}", table, geom_col)
        registered = register_arrow(con, "__input_table", table)
        columns_info = describe_query(con, "SELECT * FROM __input_table")
        other_cols = [quote_identifier(c) for c in table.column_names if c != geom_col]
        source = "__input_table"
        if any(c[0] == geom_col and "BLOB" in c[1].upper() for c in columns_info):
            definitions = other_cols + [
                f"ST_GeomFromWKB({quote_identifier(geom_col)}) AS {quote_identifier(geom_col)}"
            ]
            con.execute(
                f"CREATE VIEW __input_view AS SELECT {', '.join(definitions)} FROM {source}"
            )
            source = "__input_view"
        columns = other_cols + [
            f"ST_AsWKB({quote_identifier(geom_col)}) AS {quote_identifier(geom_col)}",
            f"{index_expression(spec, geom_col, resolution, source_crs, bbox_col)} AS {quote_identifier(column_name)}",
        ]
        result = con.sql(f"SELECT {', '.join(columns)} FROM {source}").arrow().read_all()
        if table.schema.metadata:
            result = result.replace_schema_metadata(table.schema.metadata)
        return registered.restore_edges(result)
    finally:
        con.close()


def make_index_query(
    spec, source, geometry_column, column_name, resolution, source_crs=None, bbox_col=None
):
    expression = index_expression(spec, geometry_column, resolution, source_crs, bbox_col)
    return f"SELECT *, {expression} AS {quote_identifier(column_name)} FROM {source}"


@uses_aws_profile
def add_index_column(
    spec,
    input_parquet,
    output_parquet,
    column_name,
    resolution,
    dry_run=False,
    verbose=False,
    compression=None,
    compression_level=None,
    row_group_size_mb=None,
    row_group_rows=None,
    profile=None,
    geoparquet_version=None,
    overwrite=False,
    memory_limit=None,
    use_centroid=False,
    allow_schema_diff=False,
    hive_input=None,
):
    configure_verbose(verbose)
    validate_index_resolution(spec, resolution)
    preflight_index(spec, "add")
    warn_planar_math_for_file(f"add_{spec.name}", input_parquet)
    if (is_stdin(input_parquet) or should_stream_output(output_parquet)) and not dry_run:
        return add_index_streaming(
            spec,
            input_parquet,
            output_parquet,
            column_name,
            resolution,
            verbose,
            compression,
            compression_level,
            row_group_size_mb,
            row_group_rows,
            profile,
            geoparquet_version,
            memory_limit,
            use_centroid=use_centroid,
        )
    handle_output_overwrite(output_parquet, overwrite, input_parquet)
    if not spec.multiple_files:
        require_single_file(input_parquet, f"add {spec.name}")
    validate_profile_for_urls(profile, input_parquet, output_parquet)
    geom_col = find_primary_geometry_column(input_parquet, verbose)
    input_path = resolve_file_url(input_parquet, verbose)
    source_crs = source_crs_string(input_parquet, verbose, geom_col)

    validate_index_crs(
        spec, sanitized_carried_geo_of_file(input_parquet), geom_col, source_crs, "file", verbose
    )
    bbox_col = None
    if spec.bbox_midpoint and not use_centroid and source_crs is None:
        advice = get_bbox_advice(input_parquet, "bounds_calculation", verbose)
        bbox_col = advice["bbox_column_name"] if advice["has_bbox_column"] else None
        if advice["needs_warning"] and not bbox_col:
            warn(advice["message"] + f" - using geometry centroid for {spec.label} calculation")
            for suggestion in advice["suggestions"]:
                info(f"Tip: {suggestion}")
    from geoparquet_io.core.common import add_computed_column

    def preview_details():
        if spec.preview_sql:
            info(
                f"-- Description: {spec.label} cell ID at {spec.unit} {resolution} (~{spec.size(resolution)})"
            )
            from geoparquet_io.core.parquet_writer import resolve_row_group_rows_for_query

            query = build_query(source)
            with get_duckdb_connection(load_spatial=True, load_httpfs=False) as con:
                rows = resolve_row_group_rows_for_query(
                    con, query, row_group_rows, row_group_size_mb
                )
            info("-- Main query:")
            print_copy_dry_run(query, output_parquet, compression, compression_level, rows)
        else:
            info(f"-- Resolution ({spec.display_unit or spec.unit}): {resolution}")
            info(f"-- Calculation method: {'bbox midpoint' if bbox_col else 'geometry centroid'}")

    def build_query(source):
        return make_index_query(
            spec, source, geom_col, column_name, resolution, source_crs, bbox_col
        )

    source = _index_from_clause(input_path, allow_schema_diff, hive_input)
    add_computed_column(
        input_parquet,
        output_parquet,
        column_name,
        index_expression(spec, geom_col, resolution, source_crs, bbox_col),
        dry_run=dry_run,
        verbose=verbose,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        profile=profile,
        geoparquet_version=geoparquet_version,
        memory_limit=memory_limit,
        custom_metadata={"covering": {spec.name: {"column": column_name, spec.unit: resolution}}},
        from_clause=source,
        query_builder=build_query,
        connection_setup=lambda con: load_index(con, spec, verbose=verbose),
        dry_run_details=preview_details,
        report_count=spec.report_count,
        progress_message=f"Adding {spec.label} column '{column_name}' ({spec.display_unit or spec.unit} {resolution})...",
    )
    if dry_run:
        return
    success(
        f"Successfully added {spec.label} column '{column_name}' ({spec.display_unit or spec.unit} {resolution}) to: {output_parquet}"
    )


def add_index_streaming(
    spec,
    input_path,
    output_path,
    column_name,
    resolution,
    verbose,
    compression,
    compression_level,
    row_group_size_mb,
    row_group_rows,
    profile,
    geoparquet_version,
    memory_limit,
    use_centroid=False,
):
    """Build the index query through the shared streaming driver."""
    from geoparquet_io.core.stream_io import execute_transform

    validate_index_resolution(spec, resolution)

    def make_query(source, con, source_crs, metadata):
        con.execute("SET geometry_always_xy = true;")
        load_index(con, spec, verbose=verbose and not should_stream_output(output_path))
        schema = query_schema(con, f"SELECT * FROM {source}").empty_table()
        geom_col = detect_geometry_column_from_names(schema.column_names)
        if geom_col is None:
            raise GeoParquetError("No geometry column found")
        validate_index_crs(
            spec,
            sanitized_carried_geo(metadata),
            geom_col,
            source_crs,
            "stream",
            verbose and not should_stream_output(output_path),
        )
        schema = schema.replace_schema_metadata(metadata)
        bbox_col = table_bbox_column(spec, schema, source_crs, use_centroid)
        return make_index_query(
            spec, source, geom_col, column_name, resolution, source_crs, bbox_col
        )

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
        unit = spec.display_unit or spec.unit
        success(
            f"Successfully added {spec.label} column '{column_name}' "
            f"({unit} {resolution}) to: {output_path}"
        )


def _index_from_clause(path: str, allow_schema_diff: bool, hive_input: bool | None) -> str:
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


def ensure_index_column(
    spec,
    input_parquet,
    column_name,
    resolution,
    verbose=False,
    use_centroid=False,
    profile=None,
    *,
    reject_existing=False,
    missing_error=None,
    allow_schema_diff=False,
    hive_input=None,
):
    """Return (working path, resolved column, temporary path), reusing existing cells."""
    is_table = hasattr(input_parquet, "column_names")
    columns = input_parquet.column_names if is_table else get_column_names(input_parquet)
    resolved = next((c for c in columns if c.lower() == column_name.lower()), None)
    if reject_existing:
        if resolved:
            raise InvalidParameterError(
                "column_name",
                f"Column '{column_name}' already exists in the file. Please choose a different name.",
            )
        return None
    if resolved:
        if verbose:
            debug(f"Using existing {spec.label} column '{resolved}'")
        return input_parquet, resolved, None
    if missing_error is not None:
        raise missing_error
    if verbose:
        debug(f"{spec.label} column '{column_name}' not found. Adding it now...")
    if is_table:
        return (
            add_index_table(
                spec, input_parquet, column_name, resolution, use_centroid=use_centroid
            ),
            column_name,
            None,
        )
    temp = os.path.join(
        tempfile.gettempdir(),
        f"{spec.name}_enriched_{uuid.uuid4().hex}.parquet",
    )
    try:
        add_index_column(
            spec,
            input_parquet,
            temp,
            column_name,
            resolution,
            verbose=verbose,
            compression=None,
            compression_level=None,
            use_centroid=use_centroid,
            profile=profile,
            allow_schema_diff=allow_schema_diff,
            hive_input=hive_input,
        )
        if verbose:
            debug(f"{spec.label} column added successfully at {spec.unit} {resolution}")
        return temp, column_name, temp
    except Exception as e:
        if os.path.exists(temp):
            os.remove(temp)
        raise PartitionError(f"Failed to add {spec.label} column: {str(e)}") from e


def clamp_resolution(resolution, minimum, maximum):
    return max(minimum, min(resolution, maximum))


def calculate_index_resolution(
    spec,
    total_rows,
    target_rows_per_partition,
    max_partitions=10000,
    min_resolution=None,
    max_resolution=None,
    verbose=False,
    probe=None,
):
    """Global fallback model; retain each index's pre-consolidation cell counts."""
    minimum = spec.min_resolution if min_resolution is None else min_resolution
    maximum = spec.max_resolution if max_resolution is None else max_resolution
    if total_rows == 0:
        return minimum
    target = total_rows / target_rows_per_partition
    if target > max_partitions:
        if verbose:
            warn(
                f"Target partition count ({target:.0f}) exceeds max ({max_partitions}), "
                f"adjusting target to {max_partitions}"
            )
        target = max_partitions
    if probe is not None:
        probed = probe(target, minimum, maximum)
        if probed is not None:
            return probed
    estimate = 0
    if target >= spec.base_cells:
        estimate = round(math.log(target / spec.base_cells) / math.log(spec.subdivision))
    resolution = clamp_resolution(estimate, minimum, maximum)
    if verbose:
        count = spec.base_cells * spec.subdivision**resolution
        label = spec.label.capitalize()
        info(
            f"{label} auto-resolution: {resolution} "
            f"(~{count:.0f} partitions, ~{total_rows / count:.0f} rows/partition)"
        )
    return resolution


def get_index_spec(name):
    from geoparquet_io.core.add.a5 import SPEC as a5
    from geoparquet_io.core.add.h3 import SPEC as h3
    from geoparquet_io.core.add.quadkey import SPEC as quadkey
    from geoparquet_io.core.add.s2 import SPEC as s2

    specs = {spec.name: spec for spec in (h3, a5, s2, quadkey)}
    if name not in specs:
        raise ValueError(
            f"Unsupported spatial index type: {name}. Supported types: h3, quadkey, a5, s2"
        )
    return specs[name]


def preview_index(input_parquet, column_name, preview_limit, verbose, prefix_length=None):
    """Run partition preview and analysis."""
    from geoparquet_io.core.partition.common import (
        PartitionAnalysisError,
        analyze_partition_strategy,
    )

    try:
        analyze_partition_strategy(
            input_parquet=input_parquet,
            column_name=column_name,
            column_prefix_length=prefix_length,
            verbose=True,
        )
    except PartitionAnalysisError:
        pass
    except Exception as e:
        warn(f"\nAnalysis error: {e}")

    progress("\n" + "=" * 70)
    preview_partition(
        input_parquet=input_parquet,
        column_name=column_name,
        column_prefix_length=prefix_length,
        limit=preview_limit,
        verbose=verbose,
    )


def partition_index(
    spec,
    input_parquet,
    output_folder,
    column_name,
    resolution,
    hive=False,
    overwrite=False,
    preview=False,
    preview_limit=15,
    verbose=False,
    keep_column=None,
    force=False,
    skip_analysis=False,
    filename_prefix=None,
    profile=None,
    geoparquet_version=None,
    compression=None,
    compression_level=None,
    row_group_size_mb=None,
    row_group_rows=None,
    memory_limit=None,
    auto=False,
    target_rows=100000,
    max_partitions=10000,
    partition_resolution=None,
    use_centroid=False,
):
    from geoparquet_io.core.partition.auto_resolution import calculate_auto_resolution
    from geoparquet_io.core.partition.common import (
        calculate_partition_stats,
        partition_by_column,
        raise_if_no_rows,
    )
    from geoparquet_io.core.streaming import read_stdin_to_temp_file

    validate_partition_arguments(spec, resolution, partition_resolution, auto)
    preflight_index(spec, "partition")
    configure_verbose(verbose)

    # Handle stdin input first (before resolution calculation)
    stdin_temp_file = None
    actual_input = input_parquet

    if is_stdin(input_parquet):
        stdin_temp_file = read_stdin_to_temp_file(verbose)
        actual_input = stdin_temp_file

    try:
        raise_if_no_rows(actual_input)
    except Exception:
        if stdin_temp_file and os.path.exists(stdin_temp_file):
            os.remove(stdin_temp_file)
        raise

    # Calculate auto resolution if requested
    if auto:
        try:
            resolution = calculate_auto_resolution(
                input_parquet=actual_input,
                spatial_index_type=spec.name,
                target_rows_per_partition=target_rows,
                max_partitions=max_partitions,
                min_resolution=spec.min_resolution,
                max_resolution=spec.max_resolution,
                verbose=verbose,
                profile=profile,
            )
            if spec.prefix_partition:
                partition_resolution = resolution
                info(f"Auto-calculated {spec.name} {spec.display_unit}: {resolution}")
            else:
                info(f"Auto-calculated {spec.label} {spec.unit}: {resolution}")
        except Exception as e:
            if stdin_temp_file and os.path.exists(stdin_temp_file):
                os.remove(stdin_temp_file)
            if not spec.wrap_auto_error:
                raise
            raise PartitionError(f"Auto-resolution calculation failed: {str(e)}") from e

    try:
        validate_index_resolution(spec, resolution)
        if spec.prefix_partition:
            validate_partition_resolutions(resolution, partition_resolution, spec)
    except Exception:
        if stdin_temp_file and os.path.exists(stdin_temp_file):
            os.remove(stdin_temp_file)
        raise

    if keep_column is None:
        keep_column = hive

    try:
        working_parquet, column_name, temp_file = ensure_index_column(
            spec, actual_input, column_name, resolution, verbose, use_centroid, profile
        )

        if preview:
            try:
                preview_index(
                    working_parquet, column_name, preview_limit, verbose, partition_resolution
                )
            finally:
                if temp_file and os.path.exists(temp_file):
                    os.remove(temp_file)
            return

        if spec.prefix_partition:
            progress(
                f"Partitioning by {spec.name} at {spec.unit} {partition_resolution} (column: '{column_name}')"
            )
        else:
            progress(
                f"Partitioning by {spec.label} cells at {spec.unit} {resolution} (column: '{column_name}')"
            )

        try:
            num_partitions = partition_by_column(
                input_parquet=working_parquet,
                output_folder=output_folder,
                column_name=column_name,
                column_prefix_length=partition_resolution,
                hive=hive,
                overwrite=overwrite,
                verbose=verbose,
                keep_partition_column=keep_column,
                force=force,
                skip_analysis=skip_analysis,
                filename_prefix=filename_prefix,
                profile=profile,
                geoparquet_version=geoparquet_version,
                compression=compression,
                compression_level=compression_level,
                row_group_size_mb=row_group_size_mb,
                row_group_rows=row_group_rows,
                memory_limit=memory_limit,
            )

            total_size_mb, avg_size_mb = calculate_partition_stats(output_folder, num_partitions)
            success(
                f"\nCreated {num_partitions} partition(s) in {output_folder} "
                f"(total: {total_size_mb:.2f} MB, avg: {avg_size_mb:.2f} MB)"
            )
        finally:
            if temp_file and os.path.exists(temp_file):
                if verbose:
                    debug(f"Cleaning up temporary {spec.label}-enriched file...")
                os.remove(temp_file)
    finally:
        # Clean up stdin temp file
        if stdin_temp_file and os.path.exists(stdin_temp_file):
            os.remove(stdin_temp_file)


def validate_partition_arguments(spec, resolution, partition_resolution=None, auto=False):
    if spec.prefix_partition:
        if auto and (resolution is not None or partition_resolution is not None):
            raise InvalidParameterError(
                "auto", "cannot specify --resolution or --partition-resolution with --auto"
            )
        if not auto and (resolution is None or partition_resolution is None):
            raise InvalidParameterError(
                "resolution",
                "must specify either --auto or both --resolution and --partition-resolution",
            )
    else:
        if auto and resolution is not None:
            raise InvalidParameterError("auto", f"cannot specify both --auto and --{spec.unit}")
        if not auto and resolution is None:
            raise InvalidParameterError(spec.unit, f"must specify either --{spec.unit} or --auto")


def validate_partition_resolutions(resolution, partition_resolution, spec):
    """Validate the column and prefix resolutions using the index bounds."""
    validate_index_resolution(spec, resolution)
    validate_index_resolution(spec, partition_resolution, parameter="partition_resolution")
    if partition_resolution > resolution:
        raise InvalidParameterError(
            "partition_resolution",
            f"cannot exceed column resolution ({resolution}), got {partition_resolution}",
        )
