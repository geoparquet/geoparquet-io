#!/usr/bin/env python3

"""Sort-Tile-Recursive ordering for spatially compact Parquet row groups."""

from __future__ import annotations

import math
import uuid

import duckdb
import pyarrow as pa

from geoparquet_io.core.duckdb_utils import (
    as_wkb_sql,
    fetch_one,
    get_duckdb_connection,
    quote_identifier,
    register_arrow,
)
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.geometry_detection import (
    create_geometry_view,
    resolve_geometry_column,
    resolve_geometry_read,
)
from geoparquet_io.core.logging_config import configure_verbose, debug, info, warn
from geoparquet_io.core.parquet_writer import (
    DEFAULT_ROW_GROUP_ROWS,
    WriteOptions,
    resolve_row_group_rows,
)
from geoparquet_io.core.sort_common import advised_output_version, run_sort

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
    effective_version = advised_output_version(input_parquet, geoparquet_version, verbose, profile)

    if row_group_size_mb is not None and row_group_rows is None:
        info(
            f"STR falls back to {DEFAULT_STR_TILE_SIZE:,} rows per tile when --row-group-size-mb "
            "is used, because the row count of a byte-sized group is not known before writing. "
            "Set --row-group-size to choose the strip count yourself."
        )

    run_sort(
        input_parquet,
        output_parquet,
        command="sort str",
        label="STR",
        effective_version=effective_version,
        build_query=lambda con, source, geom_col, verbose, working: _str_query(
            con, source, geom_col, tile_size, verbose, working
        ),
        geometry_column=geometry_column,
        add_bbox_flag=add_bbox_flag,
        verbose=verbose,
        write=WriteOptions.pick(locals()),
        overwrite=overwrite,
    )


def _str_query(con, source, geom_col, tile_size, verbose, working):
    source = create_geometry_view(con, source, geom_col)
    key_expr = resolve_geometry_read(con, source, geom_col, relation=True).expr
    row_count, total_count = _str_row_counts(con, source, key_expr)
    if not row_count:
        warn("All geometries are empty or null. Writing file without STR ordering.")
        return f"SELECT * FROM {source}", False
    if verbose:
        tile_count, strip_count, strip_size = _str_layout(row_count, tile_size)
        debug(
            f"STR layout: {row_count:,} spatial rows, {tile_count:,} tiles, "
            f"{strip_count:,} strips, about {strip_size:,} rows per strip"
        )
    return _str_order_query(
        source, key_expr, row_count, tile_size, has_empty=row_count < total_count
    ), True
