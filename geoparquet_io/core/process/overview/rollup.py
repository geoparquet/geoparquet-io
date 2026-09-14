#!/usr/bin/env python3
"""Rollup SQL builders for `gpio process overview`.

Grid rollups walk the true cell hierarchy (``a5_cell_to_parent`` /
``h3_cell_to_parent``) and regenerate parent geometry from the parent cell id
via the same scheme templates the aggregate engine uses. Admin rollups collapse
region codes to their ISO country prefix and attach cached Overture country
polygons.

Rollup exactness: ``count``, ``sum_*``, ``min_*``, ``max_*`` and breakdown
``count_*`` columns roll up exactly. ``avg_*`` is count-weighted over the
children that carry a value (``SUM(avg * count) / SUM(count) FILTER (avg IS
NOT NULL)``), which is exact when the underlying metric had no NULLs --
documented caveat.
"""

from __future__ import annotations

import gc

from geoparquet_io.core.duckdb_utils import get_duckdb_connection, quote_identifier, sql_path
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.file_utils import resolve_file_url
from geoparquet_io.core.partition.admin_hierarchical import _setup_admin_dataset
from geoparquet_io.core.process.aggregate.by_a5 import A5_SCHEME
from geoparquet_io.core.process.aggregate.by_h3 import H3_SCHEME
from geoparquet_io.core.process.aggregate.common import geometry_to_geom_expr
from geoparquet_io.core.process.aggregate.grid_common import GridScheme, wrap_grid_geometry
from geoparquet_io.core.process.overview.detect import (
    AggregateInfo,
    detect_aggregate_info,
)

GRID_SCHEMES: dict[str, GridScheme] = {"a5": A5_SCHEME, "h3": H3_SCHEME}
GRID_PARENT_TEMPLATES = {
    "a5": "a5_cell_to_parent({cell}, {level})",
    "h3": "h3_cell_to_parent({cell}, {level})",
}


def admin_parent_expr(cell_column: str) -> str:
    """Rolled-up admin code for a region row: 'US-CA' -> 'US'; the NULL-cell
    'unassigned' bucket flows through unchanged."""
    qcol = quote_identifier(cell_column)
    return f"CASE WHEN {qcol} = 'unassigned' THEN 'unassigned' ELSE split_part({qcol}, '-', 1) END"


def build_rollup_agg_parts(info: AggregateInfo) -> list[str]:
    """Aggregate SELECT expressions for count + every rollup column, in order."""
    # SUM(BIGINT) widens to HUGEINT in DuckDB; cast back so count stays BIGINT.
    parts = ["CAST(SUM(count) AS BIGINT) AS count"]
    for col in info.rollup_columns:
        qcol = quote_identifier(col.name)
        if col.func == "sum":
            expr = f"SUM({qcol})"
            if col.cast_to_bigint:
                expr = f"CAST({expr} AS BIGINT)"
        elif col.func == "avg":
            # Count-weighted mean. Children with a NULL avg contribute nothing
            # to the numerator, so they must not count in the denominator
            # either -- otherwise they dilute the parent mean.
            expr = f"SUM({qcol} * count) / NULLIF(SUM(count) FILTER (WHERE {qcol} IS NOT NULL), 0)"
        elif col.func == "min":
            expr = f"MIN({qcol})"
        else:  # max
            expr = f"MAX({qcol})"
        parts.append(f"{expr} AS {qcol}")
    return parts


def build_grid_rollup_sql(info: AggregateInfo, source_sql: str, level: int) -> str:
    """Roll a grid aggregate up to ``level`` and regenerate parent geometry."""
    scheme = GRID_SCHEMES[info.scheme]
    qcol = quote_identifier(info.cell_column)
    parent = GRID_PARENT_TEMPLATES[info.scheme].format(cell=qcol, level=level)
    # NULL cell ids (the 'unassigned' bucket) must not reach the parent
    # function; they group into their own NULL parent row.
    keyed = (
        f"SELECT *, CASE WHEN {qcol} IS NULL THEN NULL ELSE {parent} END AS __parent "
        f"FROM ({source_sql})"
    )
    agg_parts = [f"__parent AS {qcol}", *build_rollup_agg_parts(info)]
    agg_sql = f"SELECT {', '.join(agg_parts)} FROM ({keyed}) GROUP BY __parent"
    return wrap_grid_geometry(agg_sql, scheme, info.cell_column, info.out_geometry)


def build_admin_rollup_sql(
    info: AggregateInfo,
    source_sql: str,
    country_ref: str | None,
    country_code_col: str | None,
    country_geom_expr: str | None,
) -> str:
    """Roll a region-level admin aggregate up to country level.

    ``country_ref`` (a FROM-able relation for the per-level country cache) may
    be None only when ``info.out_geometry`` is ``none``. Countries split across
    multiple cache rows are unioned (``ST_Union_Agg``); the 'unassigned' bucket
    keeps a NULL geometry via the LEFT JOIN.
    """
    qcol = quote_identifier(info.cell_column)
    agg_parts = [
        f"__parent AS {qcol}",
        "__parent AS admin_name",
        *build_rollup_agg_parts(info),
    ]
    keyed = f"SELECT *, {admin_parent_expr(info.cell_column)} AS __parent FROM ({source_sql})"
    agg_sql = f"SELECT {', '.join(agg_parts)} FROM ({keyed}) GROUP BY __parent"
    if info.out_geometry == "none":
        return agg_sql

    countries = (
        f"SELECT {country_code_col} AS __country_code, "
        f"ST_Union_Agg({country_geom_expr}) AS __country_geom "
        f"FROM {country_ref} GROUP BY {country_code_col}"
    )
    polygon = "ST_AsWKB(c.__country_geom)"
    centroid = "ST_AsWKB(ST_Centroid(c.__country_geom))"
    if info.out_geometry == "polygon":
        geom_cols = f"{polygon} AS geometry"
    elif info.out_geometry == "centroid":
        geom_cols = f"{centroid} AS geometry"
    else:  # both
        geom_cols = f"{polygon} AS geometry, {centroid} AS centroid"
    return (
        f"SELECT r.*, {geom_cols} FROM ({agg_sql}) r "
        f"LEFT JOIN ({countries}) c ON r.{qcol} = c.__country_code"
    )


def get_admin_country_context(con, verbose: bool = False) -> tuple[str, str, str]:
    """Return (country_ref, code_col, geom_expr) for the Overture country cache."""

    dataset, _boundary_columns = _setup_admin_dataset("overture", verbose, ["country"])
    path = dataset.get_source_for_level("country")
    country_ref = f"read_parquet({sql_path(resolve_file_url(path, verbose))})"
    code_col = quote_identifier(dataset.get_level_column_mapping()["country"])
    geom_expr = geometry_to_geom_expr(con, country_ref, dataset.get_geometry_column())
    return country_ref, code_col, geom_expr


def validate_level(info: AggregateInfo, level: int | str) -> int | str:
    """Validate one requested overview level against the input's base level."""
    if info.scheme == "admin":
        if level != "country":
            raise InvalidParameterError(
                "levels",
                f"invalid admin overview level '{level}': a region-level "
                "aggregate can only roll up to 'country'",
            )
        return level
    try:
        level_int = int(level)
    except (TypeError, ValueError):
        raise InvalidParameterError(
            "levels", f"invalid {info.scheme} level '{level}': expected an integer"
        ) from None
    scheme = GRID_SCHEMES[info.scheme]
    if not scheme.min_resolution <= level_int < int(info.base_level):
        raise InvalidParameterError(
            "levels",
            f"level {level_int} is not coarser than the input's base resolution "
            f"{info.base_level} (valid: {scheme.min_resolution}-{int(info.base_level) - 1})",
        )
    return level_int


def build_level_sql(con, info: AggregateInfo, source_sql: str, level: int | str) -> str:
    """Build the full rollup SQL for one validated overview level."""
    if info.scheme == "admin":
        if info.out_geometry == "none":
            return build_admin_rollup_sql(info, source_sql, None, None, None)
        country_ref, code_col, geom_expr = get_admin_country_context(con)
        return build_admin_rollup_sql(info, source_sql, country_ref, code_col, geom_expr)
    return build_grid_rollup_sql(info, source_sql, int(level))


def rollup_table(
    table, level: int | str, cell_column: str | None = None, scheme: str | None = None
):
    """Roll an in-memory aggregate Arrow table up to ``level``.

    In-memory counterpart of one `gpio process overview` level, backing
    ``Table.overview``. Returns a new Arrow table.
    """
    con = get_duckdb_connection(load_spatial=True, load_httpfs=True)
    try:
        con.execute("SET geometry_always_xy = true")
        con.register("__overview_input", table)
        # detect_aggregate_info installs/loads the grid extension while probing
        # the base level, so no extra ensure_grid_extension call is needed.
        info = detect_aggregate_info(con, "__overview_input", cell_column, scheme)
        level = validate_level(info, level)
        sql = build_level_sql(con, info, "SELECT * FROM __overview_input", level)
        return con.execute(sql).arrow().read_all()
    finally:
        con.close()
        # Release GDAL/spatial native handles before the next spatial connection
        # opens; leaked native state can segfault sibling xdist tests.
        gc.collect()
