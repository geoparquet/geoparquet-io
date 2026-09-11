#!/usr/bin/env python3

"""Sort-Tile-Recursive ordering for spatially compact Parquet row groups."""

from __future__ import annotations

import math
import uuid

import duckdb
import pyarrow as pa

from geoparquet_io.core.common import get_parquet_metadata, write_parquet_with_metadata
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, quote_identifier, sql_path
from geoparquet_io.core.exceptions import InvalidParameterError, RemoteAccessError
from geoparquet_io.core.file_utils import handle_output_overwrite, resolve_file_url
from geoparquet_io.core.geo_metadata import parse_geo_metadata
from geoparquet_io.core.geometry_detection import (
    STANDARD_GEOMETRY_NAMES,
    find_primary_geometry_column,
)

# These three helpers are private to hilbert_order.py but are shared by convention
# between the two spatial-sort commands: both run the same prepare/write/cleanup
# pipeline. Extracting them (with the rest of the duplicated pipeline) into a
# core/sort_common.py is tracked in #776, because it also rewrites the sibling
# `sort hilbert` command and this project keeps cross-cutting refactors in their
# own PR.
from geoparquet_io.core.hilbert_order import (
    _cleanup_temp_file,
    _prepare_working_file,
    _resolve_output_version,
)
from geoparquet_io.core.logging_config import configure_verbose, debug, info, success, warn
from geoparquet_io.core.parquet_writer import (
    DEFAULT_ROW_GROUP_ROWS,
    resolve_row_group_rows,
)
from geoparquet_io.core.partition.reader import require_single_file
from geoparquet_io.core.remote import (
    get_remote_error_hint,
    is_remote_url,
    needs_httpfs,
    setup_aws_profile_if_needed,
    show_remote_read_message,
    validate_profile_for_urls,
)
from geoparquet_io.core.stream_io import open_input, write_output
from geoparquet_io.core.streaming import (
    find_geometry_column_from_table,
    is_stdin,
    should_stream_output,
)

# STR's strip count is derived from the same number the writer sizes row groups
# with, so its fallback tracks the sort default rather than the general write
# default: a bare `gpio sort str` and `gpio sort str --row-group-size-mb ...`
# then build the same ordering (#775).
DEFAULT_STR_TILE_SIZE = DEFAULT_ROW_GROUP_ROWS


def _validate_tile_size(tile_size: int) -> None:
    if tile_size < 1:
        raise InvalidParameterError("tile_size", "must be at least 1")


def _str_layout(row_count: int, tile_size: int) -> tuple[int, int, int]:
    """Return ``(tile_count, strip_count, strip_size)`` for a 2D STR pack.

    ``strip_size`` is rounded up to a whole number of tiles. Deriving it as
    ``ceil(row_count / strip_count)`` instead leaves a fractional tile at each
    strip boundary, so the row group that spans the boundary covers two strips
    and its bounding box stretches across the whole X extent.
    """
    _validate_tile_size(tile_size)
    tile_count = max(1, math.ceil(row_count / tile_size))
    strip_count = max(1, math.ceil(math.sqrt(tile_count)))
    strip_size = tile_size * max(1, math.ceil(tile_count / strip_count))
    return tile_count, strip_count, strip_size


def _str_order_query(
    source: str,
    geometry_column: str,
    row_count: int,
    tile_size: int,
    output_expressions: list[str] | None = None,
) -> str:
    """Build the STR leaf-packing query used by all input/output paths."""
    _, _, strip_size = _str_layout(row_count, tile_size)
    geom = quote_identifier(geometry_column)
    suffix = uuid.uuid4().hex
    input_order = quote_identifier(f"__gpio_str_input_order_{suffix}")
    center_x = quote_identifier(f"__gpio_str_x_{suffix}")
    center_y = quote_identifier(f"__gpio_str_y_{suffix}")
    strip = quote_identifier(f"__gpio_str_strip_{suffix}")
    selected = (
        ", ".join(output_expressions)
        if output_expressions
        else f"* EXCLUDE ({input_order}, {center_x}, {center_y}, {strip})"
    )
    empty_selected = (
        ", ".join(output_expressions) if output_expressions else f"* EXCLUDE ({input_order})"
    )

    return f"""
        WITH indexed AS (
            SELECT *, row_number() OVER () AS {input_order}
            FROM {source}
        ),
        spatial AS (
            SELECT
                *,
                (ST_XMin({geom}) + ST_XMax({geom})) / 2.0 AS {center_x},
                (ST_YMin({geom}) + ST_YMax({geom})) / 2.0 AS {center_y}
            FROM indexed
            WHERE {geom} IS NOT NULL AND NOT ST_IsEmpty({geom})
        ),
        x_ranked AS (
            SELECT
                *,
                floor(
                    (row_number() OVER (
                        ORDER BY {center_x}, {center_y}, {input_order}
                    ) - 1) / {strip_size}
                )::BIGINT AS {strip}
            FROM spatial
        ),
        ordered AS (
            SELECT *
            FROM x_ranked
            ORDER BY
                {strip},
                CASE WHEN {strip} % 2 = 0 THEN {center_y} ELSE -{center_y} END,
                {center_x},
                {input_order}
        ),
        empty_or_null AS (
            SELECT *
            FROM indexed
            WHERE {geom} IS NULL OR ST_IsEmpty({geom})
        )
        SELECT {selected} FROM ordered
        UNION ALL
        SELECT {empty_selected} FROM empty_or_null
    """


def _count_spatial_rows(con: duckdb.DuckDBPyConnection, source: str, geometry_column: str) -> int:
    geom = quote_identifier(geometry_column)
    result = con.execute(f"""
        SELECT count(*)
        FROM {source}
        WHERE {geom} IS NOT NULL AND NOT ST_IsEmpty({geom})
    """).fetchone()
    return int(result[0]) if result else 0


def _geometry_source(
    con: duckdb.DuckDBPyConnection,
    source: str,
    geometry_column: str,
) -> str:
    """Return a queryable source whose geometry column has DuckDB GEOMETRY type."""
    columns = con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
    geometry_type = next(
        (column[1] for column in columns if column[0] == geometry_column),
        None,
    )
    if geometry_type is None or "BLOB" not in geometry_type.upper():
        return source

    geom = quote_identifier(geometry_column)
    view_name = quote_identifier(f"__gpio_str_geometry_{uuid.uuid4().hex}")
    con.execute(
        f"CREATE TEMP VIEW {view_name} AS "
        f"SELECT * REPLACE (ST_GeomFromWKB({geom}) AS {geom}) FROM {source}"
    )
    return view_name


def str_order_table(
    table: pa.Table,
    geometry_column: str | None = None,
    tile_size: int = DEFAULT_STR_TILE_SIZE,
) -> pa.Table:
    """Reorder an Arrow Table with 2D Sort-Tile-Recursive leaf packing.

    Geometry envelope centers are sorted into X strips. Each strip is sorted on
    Y, alternating direction between strips so adjacent strips stay close. Empty
    and NULL geometries are placed at the end.

    ``tile_size`` does not pack rows into tiles directly: it only selects the
    number of X strips, as ``ceil(sqrt(ceil(num_rows / tile_size)))``. That is a
    coarse control -- nearby ``tile_size`` values often produce an identical
    ordering -- so treat it as "roughly the rows I intend to put in a row
    group", not as an exact tile capacity.
    """
    _validate_tile_size(tile_size)
    geom_col = geometry_column or find_geometry_column_from_table(table) or "geometry"
    geom = quote_identifier(geom_col)

    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        con.register("__input_table", table)
        source = _geometry_source(con, "__input_table", geom_col)

        row_count = _count_spatial_rows(con, source, geom_col)
        if row_count == 0:
            warn("All geometries are empty or null. Returning table without STR ordering.")
            return table

        empty_count = table.num_rows - row_count
        if empty_count:
            warn(
                f"Found {empty_count} empty/null geometries. "
                "These will be placed at the end of the sorted output."
            )

        output_expressions = [
            f"ST_AsWKB({geom}) AS {geom}" if column == geom_col else quote_identifier(column)
            for column in table.column_names
        ]
        query = _str_order_query(
            source,
            geom_col,
            row_count,
            tile_size,
            output_expressions=output_expressions,
        )
        result = con.execute(query).arrow().read_all()
        if table.schema.metadata:
            result = result.replace_schema_metadata(table.schema.metadata)
        return result
    finally:
        con.close()


def str_order(
    input_parquet: str,
    output_parquet: str | None = None,
    geometry_column: str = "geometry",
    add_bbox_flag: bool = False,
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
    """Reorder a GeoParquet file with Sort-Tile-Recursive packing."""
    configure_verbose(verbose)
    # ``resolve_row_group_rows`` validates the value the user actually
    # typed -- the error names --row-group-size rather than the internal
    # tile_size it is derived into -- and every sort subcommand goes through it,
    # so all four reject a non-positive request the same way.
    row_group_rows = resolve_row_group_rows(row_group_rows, row_group_size_mb)
    tile_size = DEFAULT_STR_TILE_SIZE if row_group_rows is None else row_group_rows
    _validate_tile_size(tile_size)
    effective_version = _resolve_output_version(input_parquet, geoparquet_version, verbose, profile)
    if effective_version == "1.1":
        warn(
            "STR sorting to GeoParquet v1.1 provides no spatial filter pushdown benefit. "
            "Consider using --geoparquet-version 2.0 to enable native geo_bbox row group statistics."
        )

    if row_group_size_mb is not None and row_group_rows is None:
        info(
            f"STR falls back to {DEFAULT_STR_TILE_SIZE:,} rows per tile when --row-group-size-mb "
            "is used, because the row count of a byte-sized group is not known before writing. "
            "Set --row-group-size to choose the strip count yourself."
        )

    if is_stdin(input_parquet) or should_stream_output(output_parquet):
        if add_bbox_flag:
            # Same pre-existing gap as `sort hilbert`: the streaming path has no
            # bbox step. Say so instead of silently dropping the request.
            warn("--add-bbox is ignored in streaming mode; no bbox column will be added.")
        _str_order_streaming(
            input_parquet,
            output_parquet,
            geometry_column,
            tile_size,
            verbose,
            compression,
            compression_level,
            row_group_size_mb,
            row_group_rows,
            profile,
            geoparquet_version,
            memory_limit,
        )
        return

    _str_order_file_based(
        input_parquet,
        output_parquet,
        geometry_column,
        tile_size,
        add_bbox_flag,
        verbose,
        compression,
        compression_level,
        row_group_size_mb,
        row_group_rows,
        profile,
        geoparquet_version,
        overwrite,
        memory_limit,
    )


def _str_order_streaming(
    input_path: str,
    output_path: str | None,
    geometry_column: str,
    tile_size: int,
    verbose: bool,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
    geoparquet_version: str | None,
    memory_limit: str | None,
) -> None:
    if should_stream_output(output_path):
        verbose = False

    with open_input(input_path, verbose=verbose) as (source, metadata, _is_stream, con):
        column_names = [
            column[0] for column in con.execute(f"SELECT * FROM {source} LIMIT 0").description
        ]
        geom_col = geometry_column
        if geom_col == "geometry" or geom_col not in column_names:
            # The stream carries its own `geo` metadata, so prefer the file's
            # declared primary column over the conventional-name guess. Without
            # this a file whose primary column is e.g. `footprint` binder-errors
            # in stream mode while working in file mode.
            geo_meta = parse_geo_metadata(metadata) or {}
            primary_column = geo_meta.get("primary_column")
            if primary_column in column_names:
                geom_col = primary_column
            else:
                geom_col = next(
                    (name for name in STANDARD_GEOMETRY_NAMES if name in column_names), geom_col
                )

        source = _geometry_source(con, source, geom_col)
        row_count = _count_spatial_rows(con, source, geom_col)
        reordered = row_count > 0
        if not reordered:
            warn("All geometries are empty or null. Writing file without STR ordering.")
            query = f"SELECT * FROM {source}"
        else:
            query = _str_order_query(source, geom_col, row_count, tile_size)
            if verbose:
                _, strip_count, strip_size = _str_layout(row_count, tile_size)
                debug(
                    f"STR layout: {row_count:,} spatial rows, {strip_count:,} strips, "
                    f"about {strip_size:,} rows per strip"
                )

        write_output(
            con,
            query,
            output_path,
            original_metadata=metadata,
            geometry_column=geom_col,
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
            if reordered:
                success(f"Successfully reordered data using STR to: {output_path}")
            else:
                success(f"Wrote data without STR ordering to: {output_path}")


def _str_order_file_based(
    input_parquet: str,
    output_parquet: str | None,
    geometry_column: str,
    tile_size: int,
    add_bbox_flag: bool,
    verbose: bool,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
    geoparquet_version: str | None,
    overwrite: bool,
    memory_limit: str | None,
) -> None:
    handle_output_overwrite(output_parquet, overwrite, input_parquet)
    require_single_file(input_parquet, "sort str")
    working_parquet, temp_file_created, temp_file = _prepare_working_file(
        input_parquet, add_bbox_flag, verbose
    )
    validate_profile_for_urls(profile, input_parquet, output_parquet)
    setup_aws_profile_if_needed(profile, input_parquet, output_parquet)
    show_remote_read_message(working_parquet, verbose)

    source = sql_path(resolve_file_url(working_parquet, verbose))
    metadata, _ = get_parquet_metadata(working_parquet, verbose)
    if geometry_column == "geometry":
        geometry_column = find_primary_geometry_column(working_parquet, verbose)

    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(working_parquet))
    try:
        geometry_source = _geometry_source(con, source, geometry_column)
        row_count = _count_spatial_rows(con, geometry_source, geometry_column)
        reordered = row_count > 0
        if not reordered:
            warn("All geometries are empty or null. Writing file without STR ordering.")
            query = f"SELECT * FROM {geometry_source}"
        else:
            query = _str_order_query(geometry_source, geometry_column, row_count, tile_size)
            if verbose:
                tile_count, strip_count, strip_size = _str_layout(row_count, tile_size)
                debug(
                    f"STR layout: {row_count:,} spatial rows, {tile_count:,} tiles, "
                    f"{strip_count:,} strips, about {strip_size:,} rows per strip"
                )

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
            input_file=input_parquet,
            memory_limit=memory_limit,
        )
        if add_bbox_flag and temp_file_created:
            success("Output includes bbox column and metadata for optimal performance")
        if reordered:
            success(f"Successfully reordered data using STR to: {output_parquet}")
        else:
            success(f"Wrote data without STR ordering to: {output_parquet}")
    except duckdb.IOException as exc:
        if is_remote_url(input_parquet):
            hints = get_remote_error_hint(str(exc), input_parquet)
            raise RemoteAccessError(input_parquet, f"{hints}\n\nOriginal error: {exc}") from exc
        raise
    finally:
        con.close()
        _cleanup_temp_file(temp_file, verbose)
