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

import mercantile

from geoparquet_io.core.bbox_structure import get_bbox_advice
from geoparquet_io.core.common import add_computed_column, get_parquet_metadata
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
    require_community_extension,
    sql_path,
    uses_aws_profile,
)
from geoparquet_io.core.exceptions import GeoParquetError, InvalidParameterError, PartitionError
from geoparquet_io.core.file_utils import handle_output_overwrite, resolve_file_url
from geoparquet_io.core.geo_metadata import sanitized_carried_geo
from geoparquet_io.core.geometry_detection import (
    STANDARD_GEOMETRY_NAMES,
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
    display_path,
    needs_httpfs,
    validate_profile_for_urls,
)
from geoparquet_io.core.stream_io import open_input, write_output
from geoparquet_io.core.streaming import (
    find_geometry_column_from_table,
    is_stdin,
    should_stream_output,
)
from geoparquet_io.core.write_funnels import write_parquet_with_metadata


@dataclass(frozen=True)
class IndexSpec:
    name: str
    column_name: str
    min_resolution: int
    max_resolution: int
    expression: Callable[[str, str, int], str]
    base_cells: int
    subdivision: int
    extension: str | None
    size: Callable[[int], str]

    @property
    def label(self):
        return "quadkey" if self.name == "quadkey" else self.name.upper()

    @property
    def unit(self):
        return "level" if self.name == "s2" else "resolution"


def resolve_standard_column(columns, candidates=STANDARD_GEOMETRY_NAMES):
    """Resolve common names case-insensitively, retaining schema spelling."""
    names = {name.lower(): name for name in columns}
    return next((names[name.lower()] for name in candidates if name.lower() in names), None)


def load_index(con, spec):
    if spec.extension:
        kwargs = {"feature": "gpio add s2"} if spec.name == "s2" else {}
        load_community_extension(con, spec.extension, **kwargs)
    else:
        con.create_function(
            "lat_lon_to_quadkey", lat_lon_to_quadkey, ["DOUBLE", "DOUBLE", "INTEGER"], "VARCHAR"
        )


def lat_lon_to_quadkey(lat: float, lon: float, level: int) -> str:
    return str(mercantile.quadkey(mercantile.tile(lon, lat, level)))


def index_expression(spec, geom_col, resolution, source_crs=None, bbox_col=None):
    if bbox_col:
        bbox = quote_identifier(bbox_col)
        lat = f"(({bbox}.ymin + {bbox}.ymax) / 2.0)"
        lon = f"(({bbox}.xmin + {bbox}.xmax) / 2.0)"
    else:
        geom = transform_geom_sql(quote_identifier(geom_col), source_crs)
        lat, lon = f"ST_Y(ST_Centroid({geom}))", f"ST_X(ST_Centroid({geom}))"
    return spec.expression(lon, lat, resolution)


def validate_index_resolution(spec, resolution, table=False):
    if not spec.min_resolution <= resolution <= spec.max_resolution:
        bounds = (
            f"must be between {spec.min_resolution} and {spec.max_resolution}, got {resolution}"
        )
        if table:
            if spec.name == "quadkey":
                raise ValueError(f"resolution must be between 0 and 23 inclusive, got {resolution}")
            raise ValueError(f"{spec.label} {spec.unit} {bounds}")
        raise InvalidParameterError(spec.unit, bounds)


def add_index_table(spec, table, column_name, resolution, geometry_column=None, use_centroid=False):
    if spec.name == "quadkey":
        resolution = int(resolution)
    validate_index_resolution(spec, resolution, table=True)
    if spec.name == "s2":
        require_community_extension("geography", feature="gpio add s2")
    geom_col = geometry_column or find_geometry_column_from_table(table) or "geometry"
    source_crs = crs_string_from_table(table, geom_col)
    bbox_col = None
    if spec.name == "quadkey" and source_crs is None:
        from geoparquet_io.core.add.quadkey import _validate_crs_from_geo_metadata

        _validate_crs_from_geo_metadata(
            sanitized_carried_geo(table.schema.metadata),
            geom_col,
            False,
            source_description="table",
        )
        if not use_centroid:
            bbox_col = resolve_standard_column(
                table.column_names, ("bbox", "bounds", "bounding_box")
            )
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
    if spec.name == "quadkey":
        warn_planar_math_for_file("add_quadkey", input_parquet)
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
        return _write_quadkey_file(
            input_parquet,
            output_parquet,
            column_name,
            resolution,
            use_centroid,
            dry_run,
            verbose,
            compression,
            compression_level,
            row_group_size_mb,
            row_group_rows,
            profile,
            geoparquet_version,
            overwrite,
            memory_limit,
            allow_schema_diff,
            hive_input,
        )
    configure_verbose(verbose)

    validate_index_resolution(spec, resolution)
    if spec.name == "s2":
        require_community_extension("geography", feature="gpio add s2")

    warn_planar_math_for_file(f"add_{spec.name}", input_parquet)

    # Check for streaming mode (stdin input or stdout output)
    is_streaming = is_stdin(input_parquet) or should_stream_output(output_parquet)

    if is_streaming and not dry_run:
        add_index_streaming(
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
            memory_limit=memory_limit,
        )
        return

    # File-based mode
    # Check if output file exists and handle overwrite (fixes issue #278)
    handle_output_overwrite(output_parquet, overwrite, input_parquet)

    # Check for partition input (not supported)
    require_single_file(input_parquet, f"add {spec.name}")

    # Get geometry column for the SQL expression
    geom_col = find_primary_geometry_column(input_parquet, verbose)

    # Cell keying expects lon/lat degrees (#525).
    geom_ref = transform_geom_sql(
        quote_identifier(geom_col), source_crs_string(input_parquet, verbose, geom_col)
    )

    sql_expression = spec.expression(
        f"ST_X(ST_Centroid({geom_ref}))", f"ST_Y(ST_Centroid({geom_ref}))", resolution
    )
    index_metadata = {"covering": {spec.name: {"column": column_name, spec.unit: resolution}}}

    if not dry_run:
        progress(f"Adding {spec.label} column '{column_name}' ({spec.unit} {resolution})...")

    # Use the generic helper
    add_computed_column(
        input_parquet=input_parquet,
        output_parquet=output_parquet,
        column_name=column_name,
        sql_expression=sql_expression,
        extensions=[spec.extension],
        dry_run=dry_run,
        verbose=verbose,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        dry_run_description=f"{spec.label} cell ID at {spec.unit} {resolution} (~{spec.size(resolution)})",
        custom_metadata=index_metadata,
        profile=profile,
        geoparquet_version=geoparquet_version,
        memory_limit=memory_limit,
    )

    if not dry_run:
        success(
            f"Successfully added {spec.label} column '{column_name}' "
            f"({spec.unit} {resolution}) to: {output_parquet}"
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
    """Stream an index column, retaining quadkey's input and metadata policies."""
    from geoparquet_io.core.stream_io import _detect_transform_source_crs

    if should_stream_output(output_path):
        verbose = False
    if spec.name == "quadkey":
        validate_index_resolution(spec, resolution)
    stream_output = spec.name != "quadkey" and should_stream_output(output_path)
    with open_input(input_path, verbose=verbose, stream_output=stream_output) as (
        source,
        metadata,
        is_stream,
        con,
    ):
        if spec.name == "quadkey":
            con.execute("SET geometry_always_xy = true;")
        load_index(con, spec)
        columns = query_schema(con, f"SELECT * FROM {source}").names
        geom_col = resolve_standard_column(columns) or "geometry"
        bbox_col = None
        if spec.name == "quadkey":
            from geoparquet_io.core.add.quadkey import (
                _streaming_source_crs,
                _validate_crs_from_geo_metadata,
            )

            source_crs = _streaming_source_crs(input_path, metadata, geom_col, verbose)
            if source_crs is None:
                _validate_crs_from_geo_metadata(
                    sanitized_carried_geo(metadata),
                    geom_col,
                    verbose,
                    source_description="stream",
                )
                if not use_centroid:
                    bbox_col = resolve_standard_column(columns, ("bbox", "bounds", "bounding_box"))
        else:
            source_crs = _detect_transform_source_crs(input_path, metadata, is_stream)
            if verbose:
                debug(f"Using geometry column: {geom_col}")
        query = make_index_query(
            spec, source, geom_col, column_name, resolution, source_crs, bbox_col
        )
        if verbose and spec.name == "quadkey":
            debug(f"Streaming quadkey query: {query}")
        write_output(
            con,
            query,
            output_path,
            original_metadata=metadata,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            verbose=verbose,
            profile=profile,
            geoparquet_version=geoparquet_version,
            memory_limit=memory_limit,
        )
    if not should_stream_output(output_path):
        unit = "zoom level" if spec.name == "quadkey" else spec.unit
        success(
            f"Successfully added {spec.label} column '{column_name}' "
            f"({unit} {resolution}) to: {output_path}"
        )


def _quadkey_from_clause(path: str, allow_schema_diff: bool, hive_input: bool | None) -> str:
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


@uses_aws_profile
def _write_quadkey_file(
    input_parquet: str,
    output_parquet: str | None,
    quadkey_column_name: str,
    resolution: int,
    use_centroid: bool,
    dry_run: bool,
    verbose: bool,
    compression: str | None,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
    geoparquet_version: str | None,
    overwrite: bool,
    memory_limit: str | None,
    allow_schema_diff: bool = False,
    hive_input: bool | None = None,
) -> None:
    """Write quadkey using its bbox and multi-file policies."""
    from geoparquet_io.core.add.quadkey import _validate_crs_for_quadkey

    configure_verbose(verbose)

    # Check if output file exists and handle overwrite (fixes issue #278)
    handle_output_overwrite(output_parquet, overwrite)

    # Validate resolution
    if not 0 <= resolution <= 23:
        raise InvalidParameterError("resolution", f"must be between 0 and 23, got {resolution}")

    # Validate profile is only used with S3
    validate_profile_for_urls(profile, input_parquet, output_parquet)

    # RAW path: shown to the user in the dry-run header below, and escaped at
    # the SQL boundary by sql_path (#802).
    input_file_path = resolve_file_url(input_parquet, verbose)

    # Get geometry column
    geom_col = find_primary_geometry_column(input_parquet, verbose)

    # Quadkeys require lon/lat degrees. Reproject a known non-CRS84 CRS (#525);
    # otherwise keep the guard (rejects projected input we can't identify, and
    # passes through default/unknown which is assumed WGS84).
    source_crs = source_crs_string(input_parquet, verbose, geom_col)
    needs_transform = source_crs is not None
    if not needs_transform:
        _validate_crs_for_quadkey(input_parquet, geom_col, verbose)

    # Check if column already exists (skip in dry-run)
    if not dry_run:
        column_names = get_column_names(input_parquet)
        if quadkey_column_name in column_names:
            raise GeoParquetError(
                f"Column '{quadkey_column_name}' already exists in the file. "
                f"Please choose a different name."
            )

    # Determine whether to use bbox or centroid. A stored bbox column is in the
    # input CRS, so it can't be used when we need to reproject — fall back to the
    # (reprojected) centroid in that case.
    use_bbox = False
    bbox_col = None
    if not use_centroid and not needs_transform:
        bbox_advice = get_bbox_advice(input_parquet, "bounds_calculation", verbose)
        if bbox_advice["has_bbox_column"]:
            use_bbox = True
            bbox_col = bbox_advice["bbox_column_name"]
            if verbose:
                debug(f"Using bbox column '{bbox_col}' for quadkey calculation")
        elif bbox_advice["needs_warning"]:
            warn(bbox_advice["message"] + " - using geometry centroid for quadkey calculation")
            for suggestion in bbox_advice["suggestions"]:
                info(f"Tip: {suggestion}")

    # Dry-run mode header
    if dry_run:
        warn("\n=== DRY RUN MODE - SQL Commands that would be executed ===\n")
        display_input = display_path(input_file_path)
        display_output = display_path(output_parquet)
        info(f"-- Input file: {display_input}")
        info(f"-- Output file: {display_output}")
        info(f"-- Geometry column: {geom_col}")
        info(f"-- New column: {quadkey_column_name}")
        info(f"-- Resolution (zoom level): {resolution}")
        method = "bbox midpoint" if use_bbox else "geometry centroid"
        info(f"-- Calculation method: {method}")
        return

    # Get metadata before processing
    metadata, _ = get_parquet_metadata(input_parquet, verbose)

    if verbose:
        debug(f"Adding quadkey column '{quadkey_column_name}' at resolution {resolution}...")

    # Create DuckDB connection with httpfs if needed
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_parquet))
    # Ensure ST_Transform (CRS-aware keying) emits lon/lat order.
    con.execute("SET geometry_always_xy = true;")

    try:
        spec = get_index_spec("quadkey")
        load_index(con, spec)
        source = _quadkey_from_clause(input_file_path, allow_schema_diff, hive_input)
        query = make_index_query(
            spec,
            source,
            geom_col,
            quadkey_column_name,
            resolution,
            source_crs,
            bbox_col if use_bbox else None,
        )

        if verbose:
            debug(f"Query: {query}")

        if not dry_run:
            progress(f"Adding quadkey column '{quadkey_column_name}' (zoom level {resolution})...")

        # Prepare quadkey metadata for GeoParquet spec
        quadkey_metadata = {
            "covering": {"quadkey": {"column": quadkey_column_name, "resolution": resolution}}
        }

        # Write output with metadata
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
            custom_metadata=quadkey_metadata,
            # The precondition: this query is `SELECT *` plus the quadkey
            # column, so the rows it writes are `input_parquet`'s own.
            input_file=input_parquet,
            memory_limit=memory_limit,
        )

        success(
            f"Successfully added quadkey column '{quadkey_column_name}' "
            f"(zoom level {resolution}) to: {output_parquet}"
        )

    finally:
        con.close()


def ensure_index_column(
    spec, input_parquet, column_name, resolution, verbose=False, use_centroid=False, profile=None
):
    """Return (working path, resolved column, temporary path), reusing existing cells."""
    columns = get_column_names(input_parquet)
    resolved = resolve_standard_column(columns, (column_name,))
    if resolved:
        if verbose:
            debug(f"Using existing {spec.label} column '{resolved}'")
        return input_parquet, resolved, None
    if verbose:
        debug(
            f"{spec.label.capitalize() if spec.name == 'quadkey' else spec.label} column '{column_name}' not found. Adding it now..."
        )
    temp = os.path.join(
        tempfile.gettempdir(),
        f"{spec.name}_enriched_{uuid.uuid4().hex}_{os.path.basename(input_parquet)}",
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
            profile=profile if spec.name == "quadkey" else None,
        )
        if verbose:
            debug(
                f"{spec.label.capitalize() if spec.name == 'quadkey' else spec.label} column added successfully at {spec.unit} {resolution}"
            )
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
    estimate = 0
    if target >= spec.base_cells:
        if spec.name == "h3":
            estimate = round(math.log(target / spec.base_cells) / math.log(spec.subdivision))
        else:
            estimate = round(math.log2(target / spec.base_cells) / 2)
    resolution = clamp_resolution(estimate, minimum, maximum)
    if verbose:
        count = spec.base_cells * spec.subdivision**resolution
        label = "Quadkey" if spec.name == "quadkey" else spec.label
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

    if spec.name == "s2":
        if auto and resolution is not None:
            raise InvalidParameterError("auto", "cannot specify both --auto and --level")
        if not auto and resolution is None:
            raise InvalidParameterError("level", "must specify either --level or --auto")
        require_community_extension("geography", feature="gpio partition s2")
    configure_verbose(verbose)

    # Handle stdin input first (before resolution calculation)
    stdin_temp_file = None
    actual_input = input_parquet

    if is_stdin(input_parquet):
        stdin_temp_file = read_stdin_to_temp_file(verbose)
        actual_input = stdin_temp_file

    if spec.name == "quadkey":
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

    if spec.name == "s2":
        raise_if_no_rows(actual_input)

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
            if spec.name == "quadkey":
                partition_resolution = resolution
                info(f"Auto-calculated quadkey zoom level: {resolution}")
            else:
                info(f"Auto-calculated {spec.label} {spec.unit}: {resolution}")
        except Exception as e:
            if stdin_temp_file and os.path.exists(stdin_temp_file):
                os.remove(stdin_temp_file)
            if spec.name == "s2":
                raise
            raise PartitionError(f"Auto-resolution calculation failed: {str(e)}") from e

    # Validate resolved resolution
    if not spec.min_resolution <= resolution <= spec.max_resolution:
        if stdin_temp_file and os.path.exists(stdin_temp_file):
            os.remove(stdin_temp_file)
        validate_index_resolution(spec, resolution)

    if spec.name == "quadkey":
        validate_partition_resolutions(resolution, partition_resolution)

    if keep_column is None:
        keep_column = hive

    try:
        # Nothing to partition: stop before rewriting the whole input just
        # to add an index column to it (#823).
        raise_if_no_rows(actual_input)

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

        if spec.name == "quadkey":
            progress(
                f"Partitioning by quadkey at resolution {partition_resolution} (column: '{column_name}')"
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


def validate_partition_resolutions(resolution, partition_resolution):
    """Validate resolution parameters."""
    if not 0 <= resolution <= 23:
        raise InvalidParameterError("resolution", f"must be between 0 and 23, got {resolution}")
    if not 0 <= partition_resolution <= 23:
        raise InvalidParameterError(
            "partition_resolution",
            f"must be between 0 and 23, got {partition_resolution}",
        )
    if partition_resolution > resolution:
        raise InvalidParameterError(
            "partition_resolution",
            f"cannot exceed column resolution ({resolution}), got {partition_resolution}",
        )
