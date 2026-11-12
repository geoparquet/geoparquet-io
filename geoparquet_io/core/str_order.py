#!/usr/bin/env python3

"""Sort-Tile-Recursive ordering for spatially compact Parquet row groups."""

from __future__ import annotations

import math
import uuid

import duckdb
import pyarrow as pa

from geoparquet_io.core.common import get_parquet_metadata
from geoparquet_io.core.duckdb_utils import (
    as_wkb_sql,
    describe_query,
    fetch_one,
    get_duckdb_connection,
    quote_identifier,
    register_arrow,
    sql_path,
    uses_aws_profile,
)
from geoparquet_io.core.exceptions import InvalidParameterError, RemoteAccessError
from geoparquet_io.core.file_utils import (
    atomic_write,
    handle_output_overwrite,
    resolve_file_url,
    unlink_released_file,
)
from geoparquet_io.core.geo_metadata import sanitized_carried_geo
from geoparquet_io.core.geometry_detection import (
    create_geometry_view,
    resolve_geometry_column,
    resolve_geometry_read,
)

# These helpers are private to hilbert_order.py but are shared by convention
# between the two spatial-sort commands: both run the same prepare/write
# pipeline, and the scratch copy is removed by its owner (file_utils). Extracting them (with the rest of the duplicated pipeline) into a
# core/sort_common.py is tracked in #776, because it also rewrites the sibling
# `sort hilbert` command and this project keeps cross-cutting refactors in their
# own PR.
from geoparquet_io.core.hilbert_order import (
    _advised_output_version,
    _prepare_working_file,
)
from geoparquet_io.core.logging_config import configure_verbose, debug, info, success, warn
from geoparquet_io.core.parquet_writer import (
    DEFAULT_ROW_GROUP_ROWS,
    WriteOptions,
    resolve_row_group_rows,
)
from geoparquet_io.core.partition.reader import require_single_file
from geoparquet_io.core.remote import (
    get_remote_error_hint,
    is_remote_url,
    needs_httpfs,
    show_remote_read_message,
    validate_profile_for_urls,
)
from geoparquet_io.core.stream_io import open_input, write_output
from geoparquet_io.core.streaming import (
    is_stdin,
    should_stream_output,
)
from geoparquet_io.core.write_funnels import write_parquet_with_metadata

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
    key_expr: str,
    row_count: int,
    tile_size: int,
    output_expressions: list[str] | None = None,
    *,
    has_empty: bool = False,
) -> str:
    """Build the STR leaf-packing query used by all input/output paths.

    ``key_expr`` is the owner's read expression for the geometry column
    (``geometry_detection.resolve_geometry_read(...).expr``), not the quoted
    column: a GeoArrow coordinate struct is no argument to ``ST_IsEmpty`` or
    ``ST_XMin``, and the owner hands those its representative point -- the
    centre of the value's coordinate extent, which is the STR key -- instead of
    dying on it (#1438).
    """
    _, _, strip_size = _str_layout(row_count, tile_size)
    geom = key_expr
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

    query = f"""
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
        )
        SELECT {selected} FROM ordered
    """
    # A second reference materializes all indexed rows, even when the tail is empty.
    if has_empty:
        query += f"""
            UNION ALL
            SELECT {empty_selected} FROM indexed
            WHERE {geom} IS NULL OR ST_IsEmpty({geom})
        """
    return query


def _str_row_counts(con: duckdb.DuckDBPyConnection, source: str, key_expr: str) -> tuple[int, int]:
    """``(rows with a geometry to sort, all rows)``, counted through ``key_expr``."""
    geom = key_expr
    result = fetch_one(
        con.execute(f"""
        SELECT count(*) FILTER (WHERE {geom} IS NOT NULL AND NOT ST_IsEmpty({geom})), count(*)
        FROM {source}
    """)
    )
    return (int(result[0]), int(result[1])) if result else (0, 0)


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
    geom_col = resolve_geometry_column(table, geometry_column, where="the table")
    geom = quote_identifier(geom_col)

    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        registered = register_arrow(con, "__input_table", table)
        source = create_geometry_view(con, "__input_table", geom_col)
        # The owner's read of the column as the view leaves it: GEOMETRY after
        # a WKB decode, a coordinate struct when it is GeoArrow-native (#1438).
        read = resolve_geometry_read(con, source, geom_col, relation=True)

        row_count, total_count = _str_row_counts(con, source, read.expr)
        if row_count == 0:
            warn("All geometries are empty or null. Returning table without STR ordering.")
            return table

        empty_count = table.num_rows - row_count
        if empty_count:
            warn(
                f"Found {empty_count} empty/null geometries. "
                "These will be placed at the end of the sorted output."
            )

        # A GeoArrow-native column goes out as itself: it never became a
        # GEOMETRY to encode back, and ST_AsWKB has no reading of a coordinate
        # struct (#1438).
        selected_geom = geom if read.kind == "geoarrow" else as_wkb_sql(geom)
        output_expressions = [
            f"{selected_geom} AS {geom}" if column == geom_col else quote_identifier(column)
            for column in table.column_names
        ]
        query = _str_order_query(
            source,
            read.expr,
            row_count,
            tile_size,
            output_expressions=output_expressions,
            has_empty=row_count < total_count,
        )
        return registered.read_back(con, query, table.schema.metadata)
    finally:
        con.close()


def str_order(
    input_parquet: str,
    output_parquet: str | None = None,
    geometry_column: str | None = None,
    add_bbox_flag: bool = False,
    verbose: bool = False,
    compression: str | None = None,
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
    effective_version = _advised_output_version(input_parquet, geoparquet_version, verbose, profile)
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

    write = WriteOptions.pick(locals())
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
            write,
        )
        return

    _str_order_file_based(
        input_parquet,
        output_parquet,
        geometry_column,
        tile_size,
        add_bbox_flag,
        verbose,
        write,
        overwrite,
    )


def _str_order_streaming(
    input_path: str,
    output_path: str | None,
    geometry_column: str | None,
    tile_size: int,
    verbose: bool,
    write: WriteOptions,
) -> None:
    if should_stream_output(output_path):
        verbose = False

    with open_input(input_path, verbose=verbose) as (source, metadata, _is_stream, con):
        described = describe_query(con, f"SELECT * FROM {source}")
        # The stream carries its own `geo` metadata, so prefer the file's
        # declared primary column over the conventional-name guess. Without
        # this a file whose primary column is e.g. `footprint` binder-errors
        # in stream mode while working in file mode. A named column is the
        # answer or an error (#1367).
        geom_col = resolve_geometry_column(
            described, geometry_column, geo=sanitized_carried_geo(metadata), where=input_path
        )

        source = create_geometry_view(con, source, geom_col)
        # The owner's read expression for the STR key: a GeoArrow coordinate
        # struct is no argument to ST_IsEmpty or ST_XMin (#1438).
        key_expr = resolve_geometry_read(con, source, geom_col, relation=True).expr
        row_count, total_count = _str_row_counts(con, source, key_expr)
        reordered = row_count > 0
        if not reordered:
            warn("All geometries are empty or null. Writing file without STR ordering.")
            query = f"SELECT * FROM {source}"
        else:
            query = _str_order_query(
                source, key_expr, row_count, tile_size, has_empty=row_count < total_count
            )
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
            verbose=verbose,
            **write.funnel_kwargs(),
        )
        if not should_stream_output(output_path):
            if reordered:
                success(f"Successfully reordered data using STR to: {output_path}")
            else:
                success(f"Wrote data without STR ordering to: {output_path}")


@uses_aws_profile
def _str_order_file_based(
    input_parquet: str,
    output_parquet: str | None,
    geometry_column: str | None,
    tile_size: int,
    add_bbox_flag: bool,
    verbose: bool,
    write: WriteOptions,
    overwrite: bool,
) -> None:
    handle_output_overwrite(output_parquet, overwrite, input_parquet)
    require_single_file(input_parquet, "sort str")
    working_parquet, temp_file_created, temp_file = _prepare_working_file(
        input_parquet, add_bbox_flag, verbose
    )
    validate_profile_for_urls(write.remote_access().profile, input_parquet, output_parquet)
    show_remote_read_message(working_parquet, verbose)

    source = sql_path(resolve_file_url(working_parquet, verbose))
    metadata, _ = get_parquet_metadata(working_parquet, verbose)
    geometry_column = resolve_geometry_column(working_parquet, geometry_column, verbose=verbose)

    assert output_parquet is not None  # file mode has a destination
    with atomic_write(
        output_parquet, profile=write.remote.profile if write.remote else None
    ) as staged:
        con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(working_parquet))
        try:
            geometry_source = create_geometry_view(con, source, geometry_column)
            # The owner's read expression for the STR key: a GeoArrow coordinate
            # struct is no argument to ST_IsEmpty or ST_XMin (#1438).
            key_expr = resolve_geometry_read(
                con, geometry_source, geometry_column, relation=True
            ).expr
            row_count, total_count = _str_row_counts(con, geometry_source, key_expr)
            reordered = row_count > 0
            if not reordered:
                warn("All geometries are empty or null. Writing file without STR ordering.")
                query = f"SELECT * FROM {geometry_source}"
            else:
                query = _str_order_query(
                    geometry_source,
                    key_expr,
                    row_count,
                    tile_size,
                    has_empty=row_count < total_count,
                )
                if verbose:
                    tile_count, strip_count, strip_size = _str_layout(row_count, tile_size)
                    debug(
                        f"STR layout: {row_count:,} spatial rows, {tile_count:,} tiles, "
                        f"{strip_count:,} strips, about {strip_size:,} rows per strip"
                    )

            write_parquet_with_metadata(
                con,
                query,
                staged,
                original_metadata=metadata,
                verbose=verbose,
                input_file=input_parquet,
                **write.funnel_kwargs(),
            )
            if add_bbox_flag and temp_file_created:
                success("Output includes bbox column and metadata for optimal performance")
            if reordered:
                success(f"Successfully reordered data using STR to: {output_parquet}")
            else:
                success(f"Wrote data without STR ordering to: {output_parquet}")
        except duckdb.IOException as exc:
            if is_remote_url(input_parquet):
                hints = get_remote_error_hint(exc, input_parquet)
                raise RemoteAccessError(input_parquet, f"{hints}\n\nOriginal error: {exc}") from exc
            raise
        finally:
            con.close()
            unlink_released_file(temp_file)
