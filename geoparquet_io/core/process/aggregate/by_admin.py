#!/usr/bin/env python3
"""Admin-region aggregation for `gpio process aggregate admin`."""

from __future__ import annotations

import gc

from geoparquet_io.core.admin_datasets import (
    admin_column_ref,
    build_admin_join_query,
    open_admin_join_connection,
    resolve_admin_dataset,
)
from geoparquet_io.core.compression import resolve_process_compression_level
from geoparquet_io.core.crs_utils import extract_crs_from_parquet
from geoparquet_io.core.duckdb_utils import (
    as_wkb_sql,
    describe_query,
    geom_from_wkb_sql,
    quote_identifier,
    s3_config_scope,
    validate_where_clause,
    where_sql_fragment,
)
from geoparquet_io.core.edges import PLANAR, representative_point_sql
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.file_utils import resolve_file_url
from geoparquet_io.core.geometry_detection import (
    primary_geometry_column,
)
from geoparquet_io.core.logging_config import configure_verbose, debug, info, success
from geoparquet_io.core.process.aggregate.common import (
    VALID_OUT_GEOMETRY,
    MetricSpec,
    aggregate_source_relation,
    build_breakdown_pivot,
    build_metric_select,
    parse_breakdown_metric,
    reject_pct_cell_metrics,
    resolve_metric_column_types,
    validate_agg_columns,
    validate_breakdown_metric,
    validate_metric_nodata,
)
from geoparquet_io.core.process.aggregate.grid_common import (
    _RESERVED_INTERNAL,
    _resolve_bbox_column_for_file,
    _validate_bucket_point_args,
    _validate_keying_columns_for_file,
    bucket_point_expr,
    needed_source_columns,
    source_select_list,
)
from geoparquet_io.core.process.aggregate.options import BUCKET_POINT_GEOMETRY
from geoparquet_io.core.write_funnels import write_aggregate_table


def _get_admin_ref(dataset, level: str) -> str:
    """Return an SQL table-reference for the admin dataset at the given level.

    For datasets with per-level caches (Overture), the pre-filtered
    level-specific cache file; for others, the dataset's own source. Either
    way :meth:`AdminDataset.table_reference` builds it, from a RAW path (#802).
    """
    ref: str = dataset.table_reference(dataset.source_for_level(level))
    return ref


def _build_joined_sql(
    input_url: str,
    input_pt_expr: str,
    admin_ref: str,
    code_col: str,
    name_col: str,
    admin_geom_col: str,
    admin_bbox_col: str | None = None,
    where: str | None = None,
    select_list: str = "*, ",
) -> str:
    """Build the spatial-join SQL tagging each input feature with its admin region.

    The admin geometry column is GEOMETRY type in all supported datasets, so it
    is used directly in ST_Intersects.  ``input_pt_expr`` is a SQL expression
    yielding the per-feature POINT used for the join (geometry centroid by
    default; bbox center or an existing point column for --bucket-point, #567).
    The admin geometry is stored as WKB (ST_AsWKB) so that _wrap_admin_geometry
    can treat __admin_geom uniformly as WKB binary.

    When admin_bbox_col is provided, a cheap point-in-bbox prefilter is added
    to the ON clause so ST_Intersects is only evaluated for admin polygons whose
    bounding box contains the feature point. The owner quotes every identifier
    in it (#942 class), and keeps one admin region per input feature -- the
    smallest admin code where polygons overlap -- so no feature is counted in
    two regions (#543, see :func:`build_spatial_join_query`).

    ``where`` is applied to the inner input scan, so the spatial join, metrics,
    and breakdowns all see only the filtered rows (#568). The caller validates
    the clause. Hive partition columns are visible to it (#612); see
    :func:`aggregate_source_relation`. ``select_list`` is the prebuilt
    passthrough part of the inner SELECT (see ``source_select_list``), ending in
    ``", "``: it names only the columns the aggregation reads, so the rest are
    neither scanned nor carried through the join, and it is built against the
    relation so a geometry-less bbox-only input never trips a binder error on a
    column it does not have.
    """
    boundary_columns = list(dict.fromkeys((code_col, name_col)))
    code_ref = admin_column_ref(0, code_col)
    name_ref = admin_column_ref(boundary_columns.index(name_col), name_col)
    return build_admin_join_query(
        input_relation=f"""(
            SELECT {select_list}{input_pt_expr} AS __cen
            FROM {aggregate_source_relation(input_url)}
            {where_sql_fragment(where)}
        )""",
        admin_table_ref=admin_ref,
        admin_select_clause=(
            f"{code_ref} AS __admin_code, "
            f"{name_ref} AS __admin_name, "
            f"{as_wkb_sql(f'b.{quote_identifier(admin_geom_col)}')} AS __admin_geom"
        ),
        input_geom_col="__cen",
        admin_geom_col=admin_geom_col,
        input_bbox_col=None,
        admin_bbox_col=admin_bbox_col,
        boundary_columns=boundary_columns,
        input_alias="s",
        input_is_point=True,
    )


def _build_agg_sql(
    joined_sql: str,
    metrics: list[MetricSpec],
    breakdown_select: str,
    nodata_values: list[str] | None = None,
    column_types: dict[str, str] | None = None,
) -> str:
    """Build the GROUP BY aggregation on top of the spatial join."""
    agg_parts = [
        "COALESCE(__admin_code, 'unassigned') AS admin_code",
        "ANY_VALUE(__admin_name) AS admin_name",
        "ANY_VALUE(__admin_geom) AS __admin_geom",
        "COUNT(*) AS count",
    ]
    metric_select = build_metric_select(
        metrics, nodata_values=nodata_values, column_types=column_types
    )
    if metric_select:
        agg_parts.append(metric_select)
    if breakdown_select:
        agg_parts.append(breakdown_select)
    return (
        f"SELECT {', '.join(agg_parts)} FROM ({joined_sql}) "
        f"GROUP BY COALESCE(__admin_code, 'unassigned')"
    )


def _wrap_admin_geometry(agg_sql: str, out_geometry: str) -> str:
    """Add geometry/centroid columns derived from the per-region admin polygon (WKB)."""
    if out_geometry == "none":
        return f"SELECT a.* EXCLUDE (__admin_geom) FROM ({agg_sql}) a"

    geom_wkb = "a.__admin_geom"  # WKB binary stored in __admin_geom
    # The admin polygons are planar, so their centroid is the planar point.
    point = representative_point_sql(geom_from_wkb_sql(geom_wkb), PLANAR)
    centroid = as_wkb_sql(point)
    if out_geometry == "polygon":
        geom_cols = f"{geom_wkb} AS geometry"
    elif out_geometry == "centroid":
        geom_cols = f"{centroid} AS geometry"
    else:  # both
        geom_cols = f"{geom_wkb} AS geometry, {centroid} AS centroid"
    return f"SELECT a.* EXCLUDE (__admin_geom), {geom_cols} FROM ({agg_sql}) a"


def aggregate_by_admin(
    input_parquet: str,
    output_parquet: str,
    level: str = "country",
    metric: str | None = None,
    breakdown: str | None = None,
    breakdown_limit: int = 20,
    out_geometry: str = "polygon",
    dataset: str = "overture",
    compression: str | None = None,
    compression_level: int | None = None,
    geoparquet_version: str | None = None,
    verbose: bool = False,
    show_sql: bool = False,
    where: str | None = None,
    metric_nodata: str | None = None,
    bucket_point: str = BUCKET_POINT_GEOMETRY,
    bbox_column: str | None = None,
    breakdown_metric: str | None = None,
    memory_limit: str | None = None,
) -> None:
    """Aggregate input features by administrative region.

    Spatially joins each input feature's centroid against admin boundary polygons
    and rolls up per-region statistics (count + optional metrics and breakdowns).
    Features whose centroid falls outside every region form one ``unassigned``
    bucket (``admin_code='unassigned'``, null admin_name, null geometry).

    Args:
        input_parquet: Path to the input GeoParquet file.
        output_parquet: Path for the output aggregated GeoParquet file.
        level: Administrative level to aggregate at (e.g. ``country``, ``region``).
        metric: Comma-separated ``func:column`` metric specs (e.g. ``sum:area``).
        breakdown: Column name for pivot-style breakdown counts.
        breakdown_limit: Maximum number of breakdown values to pivot (default 20).
        out_geometry: One of ``polygon``, ``centroid``, ``both``, ``none``.
        dataset: Admin boundary dataset to use (default ``overture``).
        compression: Parquet compression codec (default ``ZSTD``).
        compression_level: Optional compression level.
        geoparquet_version: GeoParquet spec version to write.
        verbose: Enable verbose debug logging.
        show_sql: Log the final SQL query.
        where: DuckDB WHERE clause filtering input rows before aggregation.
        metric_nodata: NoData sentinel value(s) mapped to NULL in metric columns.
        bucket_point: Where the per-feature join point comes from: ``geometry``
            (centroid, default), ``bbox`` (center of a bbox covering column,
            skips reading the geometry column), or the name of an existing
            point column.
        bbox_column: Bbox covering column for ``bucket_point='bbox'``
            (auto-detected when omitted).
        breakdown_metric: What each breakdown column holds: None/``count``
            (default), or ``sum:col``/``min:col``/``max:col`` for a weighted
            pivot named ``<func>_<col>_<value>``.
        memory_limit: DuckDB memory limit for the join and aggregation (e.g.
            ``"8GB"``). Default: half the process's memory ceiling.
    """
    compression_level = resolve_process_compression_level(compression, compression_level)
    configure_verbose(verbose)
    if out_geometry not in VALID_OUT_GEOMETRY:
        raise InvalidParameterError(
            "out_geometry",
            f"Invalid value '{out_geometry}'. Valid: {', '.join(sorted(VALID_OUT_GEOMETRY))}",
        )
    if where:
        validate_where_clause(where)
    bd_metric = parse_breakdown_metric(breakdown_metric)
    validate_breakdown_metric(breakdown, bd_metric)
    metrics, nodata_values = validate_metric_nodata(metric, metric_nodata, bd_metric)
    # An admin region is not a grid cell and has no cell area, so `pct_cell`
    # cannot mean anything here -- say so before the dataset setup (#1181).
    reject_pct_cell_metrics(metrics)
    _validate_bucket_point_args(bucket_point, bbox_column)
    if bucket_point == "bbox":
        bbox_column = _resolve_bbox_column_for_file(input_parquet, bbox_column, verbose)
    # Fail on a bad keying column before the (possibly network-bound) dataset setup.
    _validate_keying_columns_for_file(
        input_parquet,
        bucket_point,
        bbox_column,
        verbose,
        needed_source_columns(metrics, breakdown, bd_metric),
    )

    admin_dataset, _boundary_columns, _ = resolve_admin_dataset(
        dataset, [level], cache=False, verbose=verbose
    )
    mapping = admin_dataset.get_level_column_mapping()
    code_col = mapping[level]
    # Overture per-level caches store only the code column (e.g. ISO-2 country code
    # like "FR"), not a separate human-readable name.  admin_name will therefore
    # equal the code value.  For richer datasets that expose a name column, override
    # name_col here when get_level_column_mapping returns distinct name keys.
    name_col = code_col
    admin_geom_col = admin_dataset.get_geometry_column()
    admin_bbox_col = admin_dataset.get_bbox_column()

    input_url = resolve_file_url(input_parquet, verbose)
    # None when the input has no geometry column: the bbox and point-column
    # keying modes never read it (bucket_point_expr refuses it in geometry mode).
    geom_col = primary_geometry_column(input_parquet, verbose=verbose)

    # Opened in the dataset's scope, so its public read is not signed with an
    # outer AWS profile and configure_s3's endpoint is not beaten by a secret.
    with s3_config_scope(admin_dataset.get_s3_config()):
        con = open_admin_join_connection(
            spill_to_cache=False, materialize=True, memory_limit=memory_limit, verbose=verbose
        )
    try:
        admin_dataset.configure_s3(con)

        read_rel = aggregate_source_relation(input_url)
        if metrics or breakdown or bd_metric:
            # Clear error (not a DuckDB binder error) for missing metric/breakdown
            # columns -- especially `--metric count`, a no-op request since count
            # is always emitted. Checked before _get_admin_ref so a typo fails now
            # rather than after downloading the admin boundary cache, and before
            # the type resolution below so a missing column reports as missing.
            # The internal aliases are subtracted for the same reason the grid
            # path subtracts them: the narrowed projection drops a requested name
            # that collides with one, so validating against it would pass a column
            # the query then cannot bind.
            cols = {name for name, _ in describe_query(con, f"SELECT * FROM {read_rel}")} - set(
                _RESERVED_INTERNAL
            )
            validate_agg_columns(cols, metrics, breakdown, bd_metric)

        admin_ref = _get_admin_ref(admin_dataset, level)

        # Resolve metric column types from the input so sentinel literals match the
        # column's actual precision (REAL vs DOUBLE, #613) and non-numeric metric
        # columns fail up-front instead of mid-query.
        column_types = (
            resolve_metric_column_types(
                con, f"SELECT * FROM {read_rel}", metrics + ([bd_metric] if bd_metric else [])
            )
            if nodata_values
            else None
        )
        # Admin boundaries are OGC:CRS84; reproject a non-CRS84 input so ST_Intersects
        # does not fail on a CRS mismatch and the join point lands correctly (#525).
        source_crs = extract_crs_from_parquet(input_parquet, verbose, geometry_column=geom_col)
        input_pt_expr, exclude_cols = bucket_point_expr(
            con, read_rel, geom_col, source_crs, bucket_point, bbox_column
        )
        joined_sql = _build_joined_sql(
            input_url,
            input_pt_expr,
            admin_ref,
            code_col,
            name_col,
            admin_geom_col,
            admin_bbox_col,
            where=where,
            select_list=source_select_list(
                con,
                read_rel,
                needed_source_columns(metrics, breakdown, bd_metric),
                (*exclude_cols, *_RESERVED_INTERNAL),
            ),
        )

        # When a breakdown is requested, materialize the spatial join once so that
        # resolve_breakdown_values and the aggregation both read from the same temp
        # table rather than re-running the expensive ST_Intersects join twice. The
        # join point has done its work by then -- the table is one row per input
        # feature, so a POINT per row is pure weight.
        breakdown_select = ""
        if breakdown:
            con.execute(
                f"CREATE TEMP TABLE __agg_joined AS SELECT * EXCLUDE (__cen) FROM ({joined_sql})"
            )
            joined_ref = "SELECT * FROM __agg_joined"
            breakdown_select = build_breakdown_pivot(
                con,
                joined_ref,
                breakdown,
                breakdown_limit,
                metrics=metrics,
                spec=bd_metric,
                nodata_values=nodata_values,
                column_types=column_types,
            )
            agg_sql = _build_agg_sql(
                joined_ref, metrics, breakdown_select, nodata_values, column_types
            )
        else:
            agg_sql = _build_agg_sql(joined_sql, metrics, "", nodata_values, column_types)
        final_sql = _wrap_admin_geometry(agg_sql, out_geometry)

        if show_sql or verbose:
            debug(final_sql)
        result = con.sql(final_sql).arrow().read_all()

        # Derive unassigned count from the already-materialized result table to
        # avoid re-executing the (potentially expensive) spatial join a second time.
        codes = result.column("admin_code").to_pylist()
        if "unassigned" in codes:
            unassigned_count = result.column("count")[codes.index("unassigned")].as_py()
        else:
            unassigned_count = 0
        if unassigned_count:
            info(f"{unassigned_count} feature(s) fell outside all admin regions (-> 'unassigned')")
    finally:
        con.close()
        # Release GDAL/spatial native handles before the next spatial connection
        # opens; leaked native state can segfault sibling xdist tests.
        gc.collect()

    write_aggregate_table(
        result,
        output_parquet,
        out_geometry,
        compression=compression,
        compression_level=compression_level,
        geoparquet_version=geoparquet_version,
        verbose=verbose,
    )
    success(f"Aggregated to {result.num_rows} admin regions -> {output_parquet}")
