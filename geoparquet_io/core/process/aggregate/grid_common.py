#!/usr/bin/env python3
"""Shared grid-cell aggregation engine for `gpio process aggregate <grid>`.

A :class:`GridScheme` captures the few SQL fragments and parameters that differ
between discrete global grid systems (a5, h3, ...). Everything else -- reading the
source, geometry-type detection, the GROUP BY + metric + breakdown assembly, the
NULL-cell guard, unassigned logging, and writing -- is shared here so each scheme
module stays a thin descriptor.
"""

from __future__ import annotations

import gc
from dataclasses import dataclass

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.common import write_geoparquet_table
from geoparquet_io.core.crs_utils import (
    crs_transform_sql_expr,
    extract_crs_from_parquet,
    extract_crs_from_table,
    is_geographic_crs,
)
from geoparquet_io.core.duckdb_utils import (
    _escape_sql_string,
    get_duckdb_connection,
    load_community_extension,
    quote_identifier,
    sql_path,
    validate_where_clause,
    where_sql_fragment,
)
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.file_utils import resolve_file_url
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import configure_verbose, debug, info, success, warn
from geoparquet_io.core.process.aggregate.common import (
    VALID_OUT_GEOMETRY,
    aggregate_source_relation,
    antimeridian_aware_bbox,
    build_breakdown_column_names,
    build_breakdown_select,
    build_metric_select,
    geometry_to_geom_expr,
    resolve_breakdown_values,
    resolve_metric_column_types,
    validate_agg_columns,
    validate_metric_nodata,
)
from geoparquet_io.core.remote import needs_httpfs


@dataclass(frozen=True)
class GridScheme:
    """Per-grid SQL fragments and parameters for the shared aggregation engine.

    Templates use ``str.format`` placeholders:

    - ``key_template``: ``{pt}`` (a POINT GEOMETRY expression), ``{res}`` -> cell id
    - ``boundary_template``: ``{cell}`` -> the cell's GEOMETRY polygon, with the
      longitudes exactly as the grid library reports them (see
      :data:`_NEEDS_SEAM_REPAIR`); the shared builder owns everything after that
    - ``latlng_template``: ``{cell}`` -> per-row centroid intermediate
    - ``centroid_wkb_template``: ``{ll}`` (centroid intermediate alias) -> WKB point

    ``name`` doubles as the ``calculate_auto_resolution`` index type and the noun
    used in log messages.
    """

    name: str
    extension: str
    min_resolution: int
    max_resolution: int
    default_column: str
    key_template: str
    boundary_template: str
    latlng_template: str
    centroid_wkb_template: str


# Cell rings and the antimeridian
# -------------------------------
# A cell ring is a closed curve on the sphere; drawing it in the plane forces a
# choice of where to cut. The two grid libraries make opposite ones, and both
# need repairing:
#
# - ``h3_cell_to_boundary_wkb`` wraps every vertex into [-180, 180], so a cell
#   straddling the antimeridian comes back with vertices at both +179.x and
#   -179.x. Read as a planar ring that is a cell spanning 359 degrees, which a
#   renderer draws as a band across the whole map.
# - ``a5_cell_to_boundary`` keeps the ring contiguous but lets longitudes run
#   past the valid range (267.0 is a real value it returns), which fails
#   GeoParquet's coordinate-range check for a geographic CRS.
#
# The repair below is shared by every scheme so the invariant is stated once:
# **the polygon written for a cell is contiguous, simple, and lies inside
# [-180, 180]**, as a MultiPolygon cut at the antimeridian when it has to be
# (RFC 7946 section 3.1.9).
#
# It is skipped entirely for the rings that are already fine -- the overwhelming
# majority -- because a torn ring necessarily has a segment jumping more than
# 180 degrees of longitude, which forces the planar width past 180. So a ring no
# wider than 180 degrees and inside the valid range needs nothing done to it.
_NEEDS_SEAM_REPAIR = (
    "(ST_XMax({g}) - ST_XMin({g}) > 180.0 OR ST_XMin({g}) < -180.0 OR ST_XMax({g}) > 180.0)"
)

# The exterior ring as an open DOUBLE[2][] (the closing repeat is sliced off).
_CELL_RING = (
    "list_slice(list_transform(ST_Dump(ST_Points(ST_ExteriorRing({g}))), "
    "p -> [ST_X(p.geom), ST_Y(p.geom)]), 1, -2)"
)

# Cumulative unwrapping: each vertex is placed relative to the PREVIOUS one, and
# the 360-degree steps accumulate along the ring. Unwrapping relative to the
# first vertex instead is only correct while every vertex stays within 180
# degrees of it, which polar rings do not -- there the `> 180` test fires on
# vertices that were never wrapped and tears a ring that was intact.
_UNWRAP_STEPS = (
    "list_transform({r}, (p, i) -> CASE WHEN i = 1 THEN 0.0 "
    "WHEN p[1] - {r}[i - 1][1] > 180.0 THEN -360.0 "
    "WHEN p[1] - {r}[i - 1][1] < -180.0 THEN 360.0 ELSE 0.0 END)"
)
_UNWRAP_RING = "list_transform({r}, (p, i) -> [p[1] + list_sum(list_slice({s}, 1, i)), p[2]])"

# Longitude winding of the closed ring, as a multiple of 360. Zero for an
# ordinary cell -- the unwrapped ring closes on itself. Plus or minus 360 for a
# ring that encircles a pole: it genuinely spans every longitude, so no amount
# of unwrapping closes it and it needs an explicit seam instead.
_RING_WINDING = "(360.0 * round((({u})[-1][1] - ({u})[1][1]) / 360.0))"

_RING_LONS = "list_transform({r}, p -> p[1])"
_RING_LON_SPAN = "(list_max(" + _RING_LONS + ") - list_min(" + _RING_LONS + "))"

# The pole a ring encircles is the one its vertices sit next to.
_RING_POLE = "CASE WHEN list_avg(list_transform({r}, p -> p[2])) > 0 THEN 90.0 ELSE -90.0 END"

# Explicit seam for a pole-enclosing ring: carry on past the last vertex to
# where the first one comes round again, run up to the pole, back along it, and
# down to the first vertex. That closes the ring in the plane with the polar cap
# included, and the cut below then splits it at the antimeridian.
_POLE_SEAM_RING = (
    "list_concat({u}, [[{u}[1][1] + {w}, {u}[1][2]], "
    "[{u}[1][1] + {w}, " + _RING_POLE.format(r="{u}") + "], "
    "[{u}[1][1], " + _RING_POLE.format(r="{u}") + "], {u}[1]])"
)

# Slide the finished ring by whole turns so its westmost vertex lands in
# [-180, 180). Everything east of it is then below +540, which is what lets the
# cut below need only the one eastern box.
_NORMALIZE_RING = (
    "list_transform({r}, p -> "
    "[p[1] - 360.0 * floor((list_min(" + _RING_LONS + ") + 180.0) / 360.0), p[2]])"
)

_MAKE_RING_POLYGON = "ST_MakePolygon(ST_MakeLine(list_transform({r}, p -> ST_Point(p[1], p[2]))))"

# RFC 7946 section 3.1.9: a polygon crossing the antimeridian is cut at it and
# written as a MultiPolygon. ST_CollectionExtract(..., 3) drops the degenerate
# line/point pieces a cut can leave when a ring only grazes the seam.
_CUT_AT_ANTIMERIDIAN = (
    "ST_CollectionExtract(ST_Union("
    "ST_Intersection({g}, ST_MakeEnvelope(-180.0, -90.0, 180.0, 90.0)), "
    "ST_Translate(ST_Intersection({g}, ST_MakeEnvelope(180.0, -90.0, 540.0, 90.0)), -360.0, 0.0)"
    "), 3)"
)

# Internal column aliases used while building the aggregation. Any input column
# with one of these names is dropped from the SELECT * passthrough so a generated
# column can never be shadowed by a same-named user column. ("__geom" is kept
# reserved for inputs that carry a stale column from earlier gpio versions.)
_RESERVED_INTERNAL = (
    "__geom",
    "__pt",
    "__key",
    "__bnd",
    "__ll",
    "__ring",
    "__uring",
    "__fring",
)

# --bucket-point mode keywords; any other value names an existing point column.
BUCKET_POINT_GEOMETRY = "geometry"
BUCKET_POINT_BBOX = "bbox"

# Struct fields a bbox covering column must expose.
_BBOX_STRUCT_FIELDS = frozenset({"xmin", "ymin", "xmax", "ymax"})


def _relation_columns(con, relation: str) -> set[str]:
    """Column names exposed by ``relation``."""
    return {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()}


def build_exclude_clause(
    con: duckdb.DuckDBPyConnection, relation: str, columns: tuple[str, ...]
) -> str:
    """Return an `` EXCLUDE (...)`` clause dropping the ``columns`` that actually
    exist in ``relation``; empty string when none do.

    Checking existence keeps the clause safe for inputs that lack a column —
    e.g. attribute+bbox-only files with no geometry column at all (#567).
    """
    cols = _relation_columns(con, relation)
    drop: list[str] = []
    for name in columns:
        if name in cols and name not in drop:
            drop.append(name)
    return f" EXCLUDE ({', '.join(quote_identifier(c) for c in drop)})" if drop else ""


def _exclude_reserved(con, relation: str, extra: tuple[str, ...] = ()) -> str:
    """Return an `` EXCLUDE (...)`` clause dropping input columns that would collide
    with the internal aliases (or names in ``extra``); empty string if none clash."""
    return build_exclude_clause(con, relation, (*extra, *_RESERVED_INTERNAL))


def _validate_bucket_point_args(bucket_point: str, bbox_column: str | None) -> None:
    """Reject option combinations that would silently do the wrong thing."""
    if not bucket_point:
        raise InvalidParameterError(
            "bucket-point",
            "bucket point must be 'geometry', 'bbox', or the name of an existing "
            "point column; got an empty string",
        )
    if bbox_column and bucket_point != BUCKET_POINT_BBOX:
        raise InvalidParameterError(
            "bucket-point", "a bbox column only applies when the bucket point is 'bbox'"
        )


def _validate_bbox_struct_column(con, relation: str, bbox_column: str) -> None:
    """Ensure ``bbox_column`` exists in ``relation`` and is a bbox covering struct."""
    if bbox_column not in _relation_columns(con, relation):
        raise InvalidParameterError(
            "bbox-column", f"bbox column '{bbox_column}' not found in the input"
        )
    qbox = quote_identifier(bbox_column)
    try:
        fields = {
            row[0] for row in con.execute(f"DESCRIBE SELECT {qbox}.* FROM {relation}").fetchall()
        }
    except duckdb.Error:  # not a struct -- .* expansion does not bind
        fields = set()
    missing = _BBOX_STRUCT_FIELDS - fields
    if missing:
        raise InvalidParameterError(
            "bbox-column",
            f"column '{bbox_column}' is not a bbox covering struct: expected "
            f"xmin/ymin/xmax/ymax fields, missing {'/'.join(sorted(missing))}",
        )


def _validate_point_column(con, relation: str, bucket_point: str) -> None:
    """Ensure a point-column ``bucket_point`` names an existing column."""
    if bucket_point in _relation_columns(con, relation):
        return
    hint = ""
    lowered = bucket_point.lower()
    if lowered in (BUCKET_POINT_BBOX, BUCKET_POINT_GEOMETRY):
        hint = f" (mode keywords are lowercase — did you mean '{lowered}'?)"
    raise InvalidParameterError(
        "bucket-point",
        f"point column '{bucket_point}' not found in the input{hint}; the bucket "
        "point must be 'geometry', 'bbox', or the name of an existing point column",
    )


def _bbox_center_lon_sql(qbox: str, source_crs) -> str:
    """Longitude of the bbox center, wraparound-aware for the antimeridian.

    GeoJSON/GeoParquet coverings encode an antimeridian crossing as
    ``xmin > xmax`` (Fiji: xmin=179.9, xmax=-179.9); the naive midpoint would
    land near lon 0. For those rows take the +360-shifted midpoint and wrap
    values > 180 back into (-180, 180]. Only geographic CRSs get this
    treatment: in a projected CRS ``xmin > xmax`` cannot encode a dateline
    crossing, so the plain midpoint is always correct there.
    """
    plain = f"({qbox}.xmin + {qbox}.xmax) / 2.0"
    if not is_geographic_crs(source_crs):
        return plain
    shifted = f"(({qbox}.xmin + {qbox}.xmax + 360.0) / 2.0)"
    wrapped = f"CASE WHEN {shifted} > 180.0 THEN {shifted} - 360.0 ELSE {shifted} END"
    return f"CASE WHEN {qbox}.xmin > {qbox}.xmax THEN {wrapped} ELSE {plain} END"


def bucket_point_expr(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    geom_col: str,
    source_crs: dict | str | None,
    bucket_point: str,
    bbox_column: str | None,
) -> tuple[str, tuple[str, ...]]:
    """Build the keying-point expression for a source relation.

    Returns ``(pt_expr, exclude_columns)``. ``pt_expr`` yields a lon/lat POINT
    (reprojected from a non-CRS84 ``source_crs``, #525). In ``bbox`` and
    point-column modes the main geometry column is excluded from the passthrough
    SELECT so Parquet projection pushdown never reads its column chunks (#567).
    The bbox/point column is validated against ``relation`` so a typo fails with
    a clear error instead of a late binder error.
    """
    _validate_bucket_point_args(bucket_point, bbox_column)
    if bucket_point == BUCKET_POINT_GEOMETRY:
        geom_expr = crs_transform_sql_expr(
            geometry_to_geom_expr(con, relation, geom_col), source_crs
        )
        return f"ST_Centroid({geom_expr})", ()
    if bucket_point == BUCKET_POINT_BBOX:
        if not bbox_column:
            raise InvalidParameterError(
                "bbox-column",
                "bucket point 'bbox' requires a bbox column name (none given or detected)",
            )
        _validate_bbox_struct_column(con, relation, bbox_column)
        qbox = quote_identifier(bbox_column)
        lon = _bbox_center_lon_sql(qbox, source_crs)
        pt = f"ST_Point({lon}, ({qbox}.ymin + {qbox}.ymax) / 2.0)"
        # The bbox covering column is stored in the file's CRS, same as geometry.
        return crs_transform_sql_expr(pt, source_crs), (geom_col,)
    # Any other value names an existing (point) geometry column. ST_Centroid is a
    # no-op for points and keeps non-point columns keyable rather than erroring.
    _validate_point_column(con, relation, bucket_point)
    point_expr = crs_transform_sql_expr(
        geometry_to_geom_expr(con, relation, bucket_point), source_crs
    )
    return f"ST_Centroid({point_expr})", (geom_col,)


def read_grid_source_sql(
    con,
    input_url: str,
    geom_col: str,
    source_crs=None,
    where: str | None = None,
    bucket_point: str = BUCKET_POINT_GEOMETRY,
    bbox_column: str | None = None,
) -> str:
    """Source relation exposing the original columns plus a keying POINT ``__pt``.

    Detects whether the input geometry column is read as GEOMETRY (real GeoParquet)
    or BLOB (plain WKB) so it works on both. Grid keying expects lon/lat, so a
    non-CRS84 ``source_crs`` is reprojected to OGC:CRS84 before keying (#525); a
    CRS-less / already-CRS84 input is left untouched.

    ``where`` is applied to this source scan, so keying, metrics, and breakdowns
    all see only the filtered rows (#568). The caller validates the clause. Hive
    partition columns are visible to it (#612); see
    :func:`aggregate_source_relation`.

    ``bucket_point`` selects where ``__pt`` comes from: the geometry centroid
    (default), the center of a bbox covering column, or an existing point column
    (#567) -- the latter two skip reading the geometry column entirely.
    """
    read_rel = aggregate_source_relation(input_url)
    pt_expr, exclude = bucket_point_expr(
        con, read_rel, geom_col, source_crs, bucket_point, bbox_column
    )
    return (
        f"SELECT *{_exclude_reserved(con, read_rel, exclude)}, {pt_expr} AS __pt "
        f"FROM {read_rel}{where_sql_fragment(where)}"
    )


def build_grid_query(
    con,
    scheme: GridScheme,
    source_sql: str,
    resolution: int,
    cell_column: str,
    metric: str | None,
    breakdown: str | None,
    breakdown_limit: int,
    out_geometry: str,
    metric_nodata: str | None = None,
) -> str:
    """Build the full grid aggregation SQL from a source relation exposing ``__pt``."""
    metrics, nodata_values = validate_metric_nodata(metric, metric_nodata)
    if metrics or breakdown:
        # Fail with a clear message (not a DuckDB binder error) when a requested
        # metric/breakdown column doesn't exist -- especially `--metric count`,
        # which is a no-op request since count is always emitted. Runs before the
        # type resolution below so a missing column reports as missing, not as a
        # non-numeric metric.
        cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM ({source_sql})").fetchall()}
        validate_agg_columns(cols, metrics, breakdown)
    # Resolve metric column types so sentinel literals match the column's actual
    # precision (REAL vs DOUBLE, #613) and non-numeric columns fail up-front.
    column_types = resolve_metric_column_types(con, source_sql, metrics) if nodata_values else None

    key_expr = scheme.key_template.format(pt="__pt", res=resolution)
    keyed_sql = f"SELECT *, {key_expr} AS __key FROM ({source_sql})"

    # Materialize the keyed relation once when a breakdown is requested so that
    # resolve_breakdown_values and the aggregation both read from the same temp
    # table rather than re-running the key-assignment expression twice.
    breakdown_select = ""
    if breakdown:
        con.execute(f"CREATE TEMP TABLE __agg_keyed AS {keyed_sql}")
        keyed_ref = "SELECT * FROM __agg_keyed"
        top_values, has_other = resolve_breakdown_values(con, keyed_ref, breakdown, breakdown_limit)
        colmap = build_breakdown_column_names(top_values, reserved={"count_other"})
        breakdown_select = build_breakdown_select(breakdown, colmap, has_other)
    else:
        keyed_ref = keyed_sql

    agg_parts = [f"__key AS {quote_identifier(cell_column)}", "COUNT(*) AS count"]
    metric_select = build_metric_select(
        metrics, nodata_values=nodata_values, column_types=column_types
    )
    if metric_select:
        agg_parts.append(metric_select)
    if breakdown_select:
        agg_parts.append(breakdown_select)
    agg_sql = f"SELECT {', '.join(agg_parts)} FROM ({keyed_ref}) GROUP BY __key"

    return wrap_grid_geometry(agg_sql, scheme, cell_column, out_geometry)


def _cell_boundary_sql(inner_sql: str, scheme: GridScheme, qcol: str) -> str:
    """Attach the per-cell boundary polygon ``__bnd``, NULL-guarded so a scheme's
    cell function is never called on the NULL cell id of the unassigned bucket."""
    boundary = scheme.boundary_template.format(cell=qcol)
    return (
        f"SELECT *, CASE WHEN {qcol} IS NULL THEN NULL ELSE {boundary} END AS __bnd "
        f"FROM ({inner_sql})"
    )


def _seam_repair_sql(base_sql: str) -> str:
    """Add the seam-repair intermediates for the rings that need one.

    Three nested projections rather than one expression, so each step is named
    and DuckDB evaluates it only for the rows the previous CASE selected: cells
    whose ring is already contiguous and in range keep a NULL ``__ring`` and
    never pay for any of this. See the module comment above
    :data:`_NEEDS_SEAM_REPAIR` for what the steps mean.
    """
    ring = (
        f"SELECT *, CASE WHEN {_NEEDS_SEAM_REPAIR.format(g='__bnd')} "
        f"THEN {_CELL_RING.format(g='__bnd')} END AS __ring FROM ({base_sql})"
    )
    unwrapped = (
        f"SELECT *, {_UNWRAP_RING.format(r='__ring', s=_UNWRAP_STEPS.format(r='__ring'))} "
        f"AS __uring FROM ({ring})"
    )
    # Unwrapping is applied only when it narrows the ring. A polar ring spans
    # more than 180 degrees for real, and shifting its vertices would widen it
    # towards 360 -- there the original ring was right all along.
    guarded = (
        f"CASE WHEN {_RING_LON_SPAN.format(r='__uring')} < {_RING_LON_SPAN.format(r='__ring')} "
        f"THEN __uring ELSE __ring END"
    )
    winding = _RING_WINDING.format(u="__uring")
    closed = (
        f"CASE WHEN __ring IS NULL THEN NULL "
        f"WHEN {winding} <> 0.0 THEN {_POLE_SEAM_RING.format(u='__uring', w=winding)} "
        f"ELSE list_append({guarded}, ({guarded})[1]) END"
    )
    return (
        f"SELECT * EXCLUDE (__uring), {_NORMALIZE_RING.format(r=closed)} AS __fring "
        f"FROM ({unwrapped})"
    )


def _repaired_poly_wkb() -> str:
    """WKB of the cell polygon: the raw boundary, or the repaired ring, cut at
    the antimeridian into a MultiPolygon when it still crosses."""
    poly = _MAKE_RING_POLYGON.format(r="__fring")
    return (
        "CASE WHEN __bnd IS NULL THEN NULL "
        "WHEN __ring IS NULL THEN ST_AsWKB(__bnd) "
        f"WHEN list_max({_RING_LONS.format(r='__fring')}) > 180.0 "
        f"THEN ST_AsWKB({_CUT_AT_ANTIMERIDIAN.format(g=poly)}) "
        f"ELSE ST_AsWKB({poly}) END"
    )


def wrap_grid_geometry(
    agg_sql: str, scheme: GridScheme, cell_column: str, out_geometry: str
) -> str:
    """Add geometry/centroid columns derived from the grid cell id.

    Rows whose cell id is NULL (features with empty/NULL geometry that could not be
    assigned a cell) get NULL geometry throughout.

    The polygon is antimeridian-safe: contiguous, simple, and inside [-180, 180],
    as a MultiPolygon where the cell crosses the seam. The centroid comes from the
    scheme's own cell-centre function, which reports lon/lat in range, so polygon
    and centroid share one frame -- see the module comment above
    :data:`_NEEDS_SEAM_REPAIR`.
    """
    if out_geometry == "none":
        return agg_sql

    qcol = quote_identifier(cell_column)
    latlng = scheme.latlng_template.format(cell=qcol)
    centroid = (
        f"CASE WHEN {qcol} IS NULL THEN NULL "
        f"ELSE {scheme.centroid_wkb_template.format(ll='__ll')} END"
    )
    ll_sql = (
        f"SELECT *, CASE WHEN {qcol} IS NULL THEN NULL ELSE {latlng} END AS __ll FROM ({agg_sql})"
    )
    if out_geometry == "centroid":
        # No polygon is written, so the cell boundary is never built.
        return f"SELECT a.* EXCLUDE (__ll), {centroid} AS geometry FROM ({ll_sql}) a"

    poly = _repaired_poly_wkb()
    geom_cols = (
        f"{poly} AS geometry"
        if out_geometry == "polygon"
        else f"{poly} AS geometry, {centroid} AS centroid"
    )
    repaired = _seam_repair_sql(_cell_boundary_sql(ll_sql, scheme, qcol))
    return f"SELECT a.* EXCLUDE (__bnd, __ll, __ring, __fring), {geom_cols} FROM ({repaired}) a"


def _resolve_resolution(
    scheme,
    input_parquet,
    resolution,
    auto,
    target_per_cell,
    max_cells,
    verbose,
    where: str | None = None,
):
    """Resolve the explicit or auto resolution and validate against scheme bounds.

    ``where`` is forwarded to the auto-resolution sizing so --auto picks the grid
    from the *filtered* row count, not the raw file size (#568).
    """
    from geoparquet_io.core.partition import auto_resolution as _auto_resolution

    if auto and resolution is not None:
        raise InvalidParameterError("resolution", "Pass either --resolution or --auto, not both")
    if not auto and resolution is None:
        raise InvalidParameterError(
            "resolution", f"{scheme.name.upper()} aggregation requires --resolution or --auto"
        )
    if auto:
        resolution = _auto_resolution.calculate_auto_resolution(
            input_parquet,
            scheme.name,
            target_rows_per_partition=target_per_cell,
            max_partitions=max_cells,
            verbose=verbose,
            where=where,
        )
        if verbose:
            debug(f"Auto-selected {scheme.name} resolution {resolution}")
    if not scheme.min_resolution <= resolution <= scheme.max_resolution:
        raise InvalidParameterError(
            "resolution",
            f"{scheme.name.upper()} resolution must be "
            f"{scheme.min_resolution}-{scheme.max_resolution}, got {resolution}",
        )
    return resolution


def _resolve_bbox_column_for_file(
    input_parquet: str, bbox_column: str | None, verbose: bool
) -> str:
    """Return the bbox covering column to key from, auto-detecting when not given.

    Detection consults the file's GeoParquet ``covering.bbox`` metadata first,
    falling back to naming conventions (see ``check_bbox_structure``).
    """
    from geoparquet_io.core.common import check_bbox_structure

    if bbox_column:
        return bbox_column
    detected = check_bbox_structure(input_parquet, verbose).get("bbox_column_name")
    if not detected:
        raise InvalidParameterError(
            "bucket-point",
            "bucket_point='bbox' requires a bbox covering column, but none was "
            "detected. Pass bbox_column or use bucket_point='geometry'.",
        )
    return detected


def _validate_bbox_column_in_table(table, bbox_column: str) -> None:
    """Ensure an explicit table-path ``bbox_column`` exists and is a bbox struct."""
    import pyarrow as pa

    try:
        field = table.schema.field(bbox_column)
    except KeyError:
        raise InvalidParameterError(
            "bbox-column", f"bbox column '{bbox_column}' not found in the table"
        ) from None
    if not pa.types.is_struct(field.type) or not _BBOX_STRUCT_FIELDS.issubset(
        {f.name for f in field.type}
    ):
        raise InvalidParameterError(
            "bbox-column",
            f"column '{bbox_column}' is not a bbox covering struct with xmin/ymin/xmax/ymax fields",
        )


def _resolve_bbox_column_for_table(table, bbox_column: str | None) -> str:
    """Table-path variant of bbox column resolution (Arrow schema detection)."""
    from geoparquet_io.core.common import _detect_bbox_column_from_table

    if bbox_column:
        _validate_bbox_column_in_table(table, bbox_column)
        return bbox_column
    detected = _detect_bbox_column_from_table(table)
    if not detected:
        raise InvalidParameterError(
            "bucket-point",
            "bucket_point='bbox' requires a bbox covering column, but none was "
            "detected. Pass bbox_column or use bucket_point='geometry'.",
        )
    return detected


def _warn_files_missing_column(con, input_path: str, column: str) -> None:
    """Warn when a glob input has files that lack the keying ``column``.

    With ``union_by_name=true`` those files' rows get NULL for the column, so
    all of their features silently land in the unassigned bucket. Detection
    (and up-front validation) only sees the merged schema, hence this check.

    ``input_path`` is a RAW path: this function does its own escaping (#718).
    """
    if not any(ch in input_path for ch in "*?["):
        return
    col_lit = _escape_sql_string(column)
    try:
        total, with_col = con.execute(
            f"SELECT count(DISTINCT file_name), "
            f"count(DISTINCT file_name) FILTER (WHERE name = '{col_lit}') "
            f"FROM parquet_schema({sql_path(input_path)})"
        ).fetchone()
    except duckdb.Error:  # pragma: no cover - best-effort diagnostics only
        return
    if total and with_col < total:
        warn(
            f"{total - with_col} of {total} input files lack column '{column}'; "
            f"their rows have no keying value and will be counted as unassigned"
        )


def _validate_keying_columns_for_file(
    input_parquet: str, bucket_point: str, bbox_column: str | None, verbose: bool
) -> None:
    """Validate the bbox/point keying column against the file schema up front.

    Runs before any expensive work (the --auto probe, grid extension install,
    admin dataset setup) so a typo'd or wrongly-shaped column fails immediately
    with a clear error rather than a late binder error. Also warns when a glob
    input is heterogeneous (some files lack the keying column).
    """
    if bucket_point == BUCKET_POINT_GEOMETRY:
        return
    input_url = resolve_file_url(input_parquet, verbose=False)
    relation = f"read_parquet({sql_path(input_url)}, hive_partitioning=false, union_by_name=true)"
    con = get_duckdb_connection(load_spatial=False, load_httpfs=needs_httpfs(input_parquet))
    try:
        if bucket_point == BUCKET_POINT_BBOX and bbox_column:
            _validate_bbox_struct_column(con, relation, bbox_column)
            _warn_files_missing_column(con, input_parquet, bbox_column)
        elif bucket_point != BUCKET_POINT_BBOX:
            _validate_point_column(con, relation, bucket_point)
            _warn_files_missing_column(con, input_parquet, bucket_point)
    finally:
        con.close()


def _unassigned_reason(bucket_point: str, bbox_column: str | None) -> str:
    """Describe why rows had no keying point, per bucket-point mode.

    When keying came from a bbox or point column, the geometry itself may be
    perfectly intact -- do not blame it.
    """
    if bucket_point == BUCKET_POINT_BBOX:
        return f"NULL '{bbox_column}' bbox value"
    if bucket_point != BUCKET_POINT_GEOMETRY:
        return f"NULL/empty '{bucket_point}' point"
    return "NULL/empty geometry"


def _validate_out_geometry(out_geometry: str) -> None:
    if out_geometry not in VALID_OUT_GEOMETRY:
        raise InvalidParameterError(
            "out_geometry",
            f"Invalid value '{out_geometry}'. Valid: {', '.join(sorted(VALID_OUT_GEOMETRY))}",
        )


def _result_geo_bbox(con, result) -> list[float] | None:
    """RFC 7946 bbox for a finished grid result, or None if it cannot be taken.

    A cell cut at the antimeridian puts parts at both -180 and +180, and a plain
    min/max over that reads as global coverage. Best-effort: a bbox is metadata,
    so a failure here leaves it to the writer rather than losing the output.
    """
    con.register("__agg_result", result)
    try:
        return antimeridian_aware_bbox(con, "__agg_result", "geometry")
    except duckdb.Error as exc:  # pragma: no cover - defensive
        debug(f"Could not compute an antimeridian-aware bbox: {exc}")
        return None
    finally:
        con.unregister("__agg_result")


def aggregate_grid_file(
    scheme: GridScheme,
    input_parquet: str,
    output_parquet: str,
    *,
    resolution: int | None = None,
    auto: bool = False,
    target_per_cell: int = 10000,
    max_cells: int = 500000,
    metric: str | None = None,
    breakdown: str | None = None,
    breakdown_limit: int = 20,
    out_geometry: str = "polygon",
    cell_column: str | None = None,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    geoparquet_version: str | None = None,
    verbose: bool = False,
    show_sql: bool = False,
    where: str | None = None,
    metric_nodata: str | None = None,
    bucket_point: str = BUCKET_POINT_GEOMETRY,
    bbox_column: str | None = None,
) -> None:
    """Aggregate a GeoParquet file into grid cells. Writes the output file."""
    configure_verbose(verbose)
    cell_column = cell_column or scheme.default_column
    _validate_out_geometry(out_geometry)
    if where:
        validate_where_clause(where)
    # Validate metric/nodata pairing before any expensive setup (--auto scanning,
    # CRS reads, connection + community-extension install).
    validate_metric_nodata(metric, metric_nodata)
    _validate_bucket_point_args(bucket_point, bbox_column)
    if bucket_point == BUCKET_POINT_BBOX:
        bbox_column = _resolve_bbox_column_for_file(input_parquet, bbox_column, verbose)
    _validate_keying_columns_for_file(input_parquet, bucket_point, bbox_column, verbose)
    resolution = _resolve_resolution(
        scheme, input_parquet, resolution, auto, target_per_cell, max_cells, verbose, where=where
    )

    input_url = resolve_file_url(input_parquet, verbose)
    geom_col = find_primary_geometry_column(input_parquet, verbose) or "geometry"
    source_crs = extract_crs_from_parquet(input_parquet, verbose)

    con = get_duckdb_connection(load_spatial=True, load_httpfs=True)
    try:
        load_community_extension(con, scheme.extension, feature=f"{scheme.name} aggregation")
        con.execute("SET geometry_always_xy = true")

        source_sql = read_grid_source_sql(
            con,
            input_url,
            geom_col,
            source_crs,
            where=where,
            bucket_point=bucket_point,
            bbox_column=bbox_column,
        )
        final_sql = build_grid_query(
            con,
            scheme,
            source_sql,
            resolution,
            cell_column,
            metric,
            breakdown,
            breakdown_limit,
            out_geometry,
            metric_nodata=metric_nodata,
        )
        if show_sql or verbose:
            debug(final_sql)
        result = con.execute(final_sql).arrow().read_all()
        # The cells are keyed in lon/lat, so the output is always geographic --
        # whatever the input CRS was -- which is what makes the wrap form
        # readable (see `antimeridian_aware_bbox`).
        geo_bbox = None if out_geometry == "none" else _result_geo_bbox(con, result)
    finally:
        con.close()
        # Release GDAL/spatial native handles before the next spatial connection
        # opens; leaked native state can segfault sibling xdist tests.
        gc.collect()

    # Report features that had no assignable cell (no keying point).
    ids = result.column(cell_column).to_pylist()
    if None in ids:
        unassigned = result.column("count")[ids.index(None)].as_py()
        info(
            f"{unassigned} features had no assignable {scheme.name} cell "
            f"({_unassigned_reason(bucket_point, bbox_column)})"
        )

    if out_geometry == "none":
        if compression_level is not None:
            pq.write_table(
                result,
                output_parquet,
                compression=compression,
                compression_level=compression_level,
            )
        else:
            pq.write_table(result, output_parquet, compression=compression)
    else:
        write_geoparquet_table(
            result,
            output_parquet,
            geometry_column="geometry",
            compression=compression,
            compression_level=compression_level,
            geoparquet_version=geoparquet_version,
            verbose=verbose,
            geo_bbox=geo_bbox,
        )
    success(f"Aggregated to {result.num_rows} {scheme.name} cells -> {output_parquet}")


def aggregate_grid_table(
    scheme: GridScheme,
    table,
    *,
    resolution: int,
    metric: str | None = None,
    breakdown: str | None = None,
    breakdown_limit: int = 20,
    out_geometry: str = "polygon",
    cell_column: str | None = None,
    geometry_column: str | None = None,
    where: str | None = None,
    metric_nodata: str | None = None,
    bucket_point: str = BUCKET_POINT_GEOMETRY,
    bbox_column: str | None = None,
) -> pa.Table:
    """Aggregate an in-memory Arrow table into grid cells. Returns a new Arrow table."""
    cell_column = cell_column or scheme.default_column
    _validate_out_geometry(out_geometry)
    if where:
        validate_where_clause(where)
    # Validate metric/nodata pairing before connection setup and extension install.
    validate_metric_nodata(metric, metric_nodata)
    _validate_bucket_point_args(bucket_point, bbox_column)
    if bucket_point == BUCKET_POINT_BBOX:
        bbox_column = _resolve_bbox_column_for_table(table, bbox_column)
    if not scheme.min_resolution <= resolution <= scheme.max_resolution:
        raise InvalidParameterError(
            "resolution",
            f"{scheme.name.upper()} resolution must be "
            f"{scheme.min_resolution}-{scheme.max_resolution}, got {resolution}",
        )

    geom_col = geometry_column or "geometry"
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        load_community_extension(con, scheme.extension, feature=f"{scheme.name} aggregation")
        con.execute("SET geometry_always_xy = true")
        con.register("__agg_input", table)
        source_crs = extract_crs_from_table(table, geom_col)
        pt_expr, exclude = bucket_point_expr(
            con, "__agg_input", geom_col, source_crs, bucket_point, bbox_column
        )
        source_sql = (
            f"SELECT *{_exclude_reserved(con, '__agg_input', (geom_col, *exclude))}, "
            f"{pt_expr} AS __pt FROM __agg_input{where_sql_fragment(where)}"
        )
        final_sql = build_grid_query(
            con,
            scheme,
            source_sql,
            resolution,
            cell_column,
            metric,
            breakdown,
            breakdown_limit,
            out_geometry,
            metric_nodata=metric_nodata,
        )
        return con.execute(final_sql).arrow().read_all()
    finally:
        con.close()
        # Release GDAL/spatial native handles before the next spatial connection
        # opens; leaked native state can segfault sibling xdist tests.
        gc.collect()
