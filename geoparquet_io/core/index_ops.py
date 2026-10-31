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
from geoparquet_io.core.edges import is_nonplanar, representative_point_sql
from geoparquet_io.core.exceptions import GeoParquetError, InvalidParameterError, PartitionError
from geoparquet_io.core.file_utils import handle_output_overwrite, resolve_file_url
from geoparquet_io.core.geo_metadata import sanitized_carried_geo, sanitized_carried_geo_of_file
from geoparquet_io.core.geometry_detection import (
    detect_geometry_column_from_names,
    find_primary_geometry_column,
)
from geoparquet_io.core.geometry_repair import nonplanar_edges_of_arrow_table
from geoparquet_io.core.logging_config import (
    configure_verbose,
    debug,
    info,
    progress,
    success,
    warn,
)
from geoparquet_io.core.parquet_writer import WriteOptions
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
from geoparquet_io.core.write_funnels import collect_nonplanar_edges


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
    report_count: bool = True
    # How a written cell id is stored: an integer type, or a string matching
    # ``cell_pattern`` (a grid index's cell column, as overview detection reads it).
    cell_integer: bool = False
    cell_pattern: str = ""

    @property
    def bounds_text(self) -> str:
        """The valid range as help text, for example ``0-15``."""
        return f"{self.min_resolution}-{self.max_resolution}"


def preflight_index(spec, command, *, directory=False):
    from geoparquet_io.core.duckdb_utils import require_community_extension

    if spec.extension_preflight or (directory and spec.extension):
        require_community_extension(spec.extension, feature=f"gpio {command} {spec.name}")


def validate_index_crs(spec, geo_meta, geom_col, source_crs, description, verbose=False):
    """Apply the spec's CRS requirement only when no transform is available."""
    if spec.crs_validator is not None and source_crs is None:
        spec.crs_validator(geo_meta, geom_col, verbose, source_description=description)


def _keys_from_bbox(spec, use_centroid, source_crs, edges) -> bool:
    """A stored bbox's midpoint keys the cell only for planar lon/lat input.

    A bbox is in the input CRS, so not when the driver reprojects; and a planar
    box's midpoint is not on a spherical shape, so not for non-planar edges
    (#1293).
    """
    return (
        spec.bbox_midpoint and not use_centroid and source_crs is None and not is_nonplanar(edges)
    )


def table_bbox_column(spec, table, source_crs, use_centroid, edges=None):
    if not _keys_from_bbox(spec, use_centroid, source_crs, edges):
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


def index_point(geom_sql, source_crs=None, bbox_col=None, edges=None) -> tuple[str, str]:
    """The ``(lon, lat)`` SQL every cell key takes, for the driver and the probe.

    A bbox midpoint when ``bbox_col`` is set (see :func:`_keys_from_bbox`);
    otherwise ``edges.representative_point_sql`` of ``geom_sql`` (a GEOMETRY
    expression in the input CRS) reprojected to lon/lat, with the column's
    ``edges``, which the caller read once per input (#1293).
    """
    if bbox_col:
        bbox = quote_identifier(bbox_col)
        return f"(({bbox}.xmin + {bbox}.xmax) / 2.0)", f"(({bbox}.ymin + {bbox}.ymax) / 2.0)"
    point = representative_point_sql(transform_geom_sql(geom_sql, source_crs), edges)
    return f"ST_X({point})", f"ST_Y({point})"


def index_expression(spec, geom_col, resolution, source_crs=None, bbox_col=None, edges=None):
    """The one cell expression: ``spec.expression`` keyed from :func:`index_point`."""
    lon, lat = index_point(quote_identifier(geom_col), source_crs, bbox_col, edges)
    return spec.expression(lon, lat, resolution)


def file_key_inputs(spec, input_parquet, geom_col, source_crs, use_centroid=False, verbose=False):
    """The driver's one read of how a file's cells are keyed: ``(edges, bbox_col, advice)``.

    ``edges`` is the column's (``collect_nonplanar_edges``, which refuses a glob
    whose files disagree); ``bbox_col`` the planar bbox column a quadkey keys
    from, when :func:`_keys_from_bbox` allows one; ``advice`` the bbox advice
    behind that answer (``None`` when no bbox was wanted), for the caller that
    tells the user. ``add`` and the ``--auto`` probe both read it, so the probe
    counts the cells the partition writes.
    """
    edges = collect_nonplanar_edges(input_parquet).get(geom_col)
    if not _keys_from_bbox(spec, use_centroid, source_crs, edges):
        return edges, None, None
    advice = get_bbox_advice(input_parquet, "bounds_calculation", verbose)
    return edges, advice["bbox_column_name"] if advice["has_bbox_column"] else None, advice


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
    edges = nonplanar_edges_of_arrow_table(table, geom_col)
    bbox_col = table_bbox_column(spec, table, source_crs, use_centroid, edges)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        con.execute("SET geometry_always_xy = true;")
        load_index(con, spec)
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
            f"{index_expression(spec, geom_col, resolution, source_crs, bbox_col, edges)} AS {quote_identifier(column_name)}",
        ]
        result = con.sql(f"SELECT {', '.join(columns)} FROM {source}").arrow().read_all()
        if table.schema.metadata:
            result = result.replace_schema_metadata(table.schema.metadata)
        return registered.restore_edges(result)
    finally:
        con.close()


@uses_aws_profile
def add_index_column(
    spec,
    input_parquet,
    output_parquet,
    column_name,
    resolution,
    dry_run=False,
    verbose=False,
    write: WriteOptions | None = None,
    overwrite=False,
    use_centroid=False,
    allow_schema_diff=False,
    hive_input=None,
):
    configure_verbose(verbose)
    write = write or WriteOptions()
    validate_index_resolution(spec, resolution)
    preflight_index(spec, "add")
    if (is_stdin(input_parquet) or should_stream_output(output_parquet)) and not dry_run:
        return add_index_streaming(
            spec,
            input_parquet,
            output_parquet,
            column_name,
            resolution,
            verbose,
            write,
            use_centroid=use_centroid,
        )
    handle_output_overwrite(output_parquet, overwrite, input_parquet)
    if not spec.multiple_files:
        require_single_file(input_parquet, f"add {spec.name}")
    validate_profile_for_urls(write.profile, input_parquet, output_parquet)
    geom_col = find_primary_geometry_column(input_parquet, verbose)
    input_path = resolve_file_url(input_parquet, verbose)
    source_crs = source_crs_string(input_parquet, verbose, geom_col)

    validate_index_crs(
        spec, sanitized_carried_geo_of_file(input_parquet), geom_col, source_crs, "file", verbose
    )
    edges, bbox_col, advice = file_key_inputs(
        spec, input_parquet, geom_col, source_crs, use_centroid, verbose
    )
    if advice and advice["needs_warning"] and not bbox_col:
        warn(advice["message"] + f" - using geometry centroid for {spec.label} calculation")
        for suggestion in advice["suggestions"]:
            info(f"Tip: {suggestion}")
    from geoparquet_io.core.common import add_computed_column

    description = f"{spec.label} cell ID at {spec.unit} {resolution} (~{spec.size(resolution)})"
    if spec.bbox_midpoint:
        method = "bbox midpoint" if bbox_col else "geometry centroid"
        description += f"\n-- Calculation method: {method}"
    source = _index_from_clause(input_path, allow_schema_diff, hive_input)
    add_computed_column(
        input_parquet,
        output_parquet,
        column_name,
        index_expression(spec, geom_col, resolution, source_crs, bbox_col, edges),
        dry_run=dry_run,
        verbose=verbose,
        custom_metadata={"covering": {spec.name: {"column": column_name, spec.unit: resolution}}},
        from_clause=source,
        connection_setup=lambda con: load_index(con, spec, verbose=verbose),
        dry_run_description=description,
        report_count=spec.report_count,
        progress_message=f"Adding {spec.label} column '{column_name}' ({spec.display_unit or spec.unit} {resolution})...",
        **write.funnel_kwargs(),
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
    write: WriteOptions,
    use_centroid=False,
):
    """Build the index query through the shared streaming driver."""
    from geoparquet_io.core.duckdb_utils import computed_column_query
    from geoparquet_io.core.stream_io import execute_transform

    validate_index_resolution(spec, resolution)

    def make_query(source, con, source_crs, edges, metadata):
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
        column_edges = edges.get(geom_col)
        bbox_col = table_bbox_column(spec, schema, source_crs, use_centroid, column_edges)
        return computed_column_query(
            source,
            column_name,
            index_expression(spec, geom_col, resolution, source_crs, bbox_col, column_edges),
        )

    execute_transform(
        input_path,
        output_path,
        make_query,
        verbose=verbose,
        input_aware=True,
        **write.funnel_kwargs(),
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
            write=WriteOptions(profile=profile),
            use_centroid=use_centroid,
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
    write: WriteOptions | None = None,
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

    write = write or WriteOptions()
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
                use_centroid=use_centroid,
                input_parquet=actual_input,
                spatial_index_type=spec.name,
                target_rows_per_partition=target_rows,
                max_partitions=max_partitions,
                min_resolution=spec.min_resolution,
                max_resolution=spec.max_resolution,
                verbose=verbose,
                profile=write.profile,
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
            spec, actual_input, column_name, resolution, verbose, use_centroid, write.profile
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
                write=write,
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
