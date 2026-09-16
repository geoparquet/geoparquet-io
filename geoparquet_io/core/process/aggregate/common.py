#!/usr/bin/env python3
"""Shared aggregation spec parsing and SQL builders for `gpio process aggregate`."""

from __future__ import annotations

import math
import re
import string
from dataclasses import dataclass

from geoparquet_io.core.duckdb_utils import quote_identifier, sql_path
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.logging_config import warn

VALID_METRIC_FUNCS = {"sum", "avg", "min", "max"}
VALID_OUT_GEOMETRY = {"polygon", "centroid", "both", "none"}

# Strict SQL-safe numeric literal: ASCII digits only (float() also accepts
# Unicode digits, underscores, inf/Infinity -- none of which are valid SQL).
_NUMERIC_TOKEN_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$", re.ASCII)
_NAN_TOKEN_RE = re.compile(r"^[+-]?nan$", re.IGNORECASE | re.ASCII)

# DuckDB numeric type names (DESCRIBE output). REAL columns report as FLOAT.
_NUMERIC_SQL_TYPES = {
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
    "UHUGEINT",
    "FLOAT",
    "REAL",
    "DOUBLE",
}


def _is_numeric_sql_type(col_type: str) -> bool:
    t = col_type.upper()
    return t in _NUMERIC_SQL_TYPES or t.startswith(("DECIMAL", "NUMERIC"))


def aggregate_source_relation(input_url: str) -> str:
    """``read_parquet`` expression for the input scan of an aggregation.

    ``input_url`` is a RAW path or URL; ``sql_path`` quotes and escapes it here,
    so callers must not pre-escape it (#802).

    Hive partitioning is left to DuckDB's auto-detection (the default) rather
    than forced off, so ``--where "year = 2025"`` can filter on a partition
    column of a hive-style glob/directory -- the documented use case, which the
    forced ``hive_partitioning=false`` broke with a Binder error while the
    ``--auto`` row-count path (a bare ``FROM 'url'``, auto-detecting) accepted
    it (gpio #612).

    Partition columns cannot leak into the output: every aggregation projects a
    fixed column list (bucket id, count, metrics, breakdown pivots, geometry),
    so a passthrough column added by the scan is dropped by the GROUP BY.
    """
    return f"read_parquet({sql_path(input_url)}, union_by_name=true)"


def geometry_to_geom_expr(con, relation: str, geom_col: str) -> str:
    """Return a SQL expression yielding a GEOMETRY for ``geom_col`` in ``relation``.

    DuckDB 1.5 reads a GeoParquet geometry column as a ``GEOMETRY`` type, so it can
    be used directly. In-memory Arrow tables and plain WKB-blob Parquet expose the
    column as ``BLOB``, which must be decoded with ``ST_GeomFromWKB``. This inspects
    the actual column type so callers get a GEOMETRY either way.

    ``relation`` must be usable in a FROM clause (e.g. ``read_parquet('...')`` or a
    registered relation name). ``con`` must have the spatial extension loaded.
    """
    qcol = quote_identifier(geom_col)
    rows = con.execute(f"DESCRIBE SELECT {qcol} FROM {relation}").fetchall()
    col_type = (rows[0][1] if rows else "").upper()
    if "GEOMETRY" in col_type:
        return qcol
    # BLOB/BINARY (and anything unrecognized) is treated as WKB. Wrap in TRY so a
    # single malformed value becomes NULL (-> unassigned bucket) instead of
    # aborting the whole aggregation.
    return f"TRY(ST_GeomFromWKB({qcol}))"


@dataclass(frozen=True)
class MetricSpec:
    """A single numeric rollup: ``func`` over ``column`` -> ``output_name``."""

    func: str
    column: str
    output_name: str


def parse_metrics(metric_str: str | None, param: str = "metric") -> list[MetricSpec]:
    """Parse a --metric string into MetricSpec entries.

    Accepts comma-separated ``func:column`` pairs. A bare ``column`` with no
    ``func:`` prefix defaults to ``sum`` (a total is the common viz intent).
    ``param`` names the flag in error messages.
    """
    if not metric_str:
        return []
    specs: list[MetricSpec] = []
    for raw in metric_str.split(","):
        entry = raw.strip()
        if not entry:
            continue
        if ":" in entry:
            func, _, column = entry.partition(":")
            func = func.strip().lower()
            column = column.strip()
        else:
            func = "sum"
            column = entry
        if func not in VALID_METRIC_FUNCS:
            raise InvalidParameterError(
                param,
                f"Unknown metric function '{func}'. "
                f"Valid functions: {', '.join(sorted(VALID_METRIC_FUNCS))}",
            )
        if not column:
            raise InvalidParameterError(param, f"Metric '{entry}' is missing a column name")
        specs.append(MetricSpec(func=func, column=column, output_name=f"{func}_{column}"))
    return specs


def parse_breakdown_metric(spec_str: str | None) -> MetricSpec | None:
    """Parse a --breakdown-metric string into the spec each pivot column carries.

    None, empty, or the literal ``count`` is the default: the pivot stays
    ``COUNT(*)`` and its columns ``count_<value>``. Otherwise one ``func:column``
    (a bare column is ``sum``, as for --metric) with ``func`` in sum, min, max.
    ``avg`` is refused because a per-category mean cannot be rolled up to a
    coarser overview level without the category's own count, which the output
    does not carry (#1100).
    """
    if spec_str is None or not spec_str.strip() or spec_str.strip().lower() == "count":
        return None
    specs = parse_metrics(spec_str, param="breakdown-metric")
    if len(specs) != 1:
        raise InvalidParameterError(
            "breakdown-metric",
            f"'{spec_str.strip()}' names more than one metric. A breakdown carries "
            'exactly one aggregate, e.g. "sum:peak".',
        )
    spec = specs[0]
    if spec.func == "avg":
        raise InvalidParameterError(
            "breakdown-metric",
            "'avg' cannot be a breakdown metric: a per-category mean has no "
            "per-category count to roll up by in overviews. Use sum, min or max, or "
            "--metric avg:<column> for a per-cell mean.",
        )
    return spec


def validate_breakdown_metric(breakdown: str | None, spec: MetricSpec | None) -> None:
    """A breakdown metric needs a breakdown to pivot. Cheap; run before any setup."""
    if spec is not None and not breakdown:
        raise InvalidParameterError(
            "breakdown-metric",
            "A breakdown metric needs a breakdown to pivot: pass --breakdown <column> "
            "as well, or drop the breakdown metric and use --metric for a per-cell "
            "rollup.",
        )


def breakdown_prefix(spec: MetricSpec | None) -> str:
    """Column-name prefix every pivot column of a breakdown shares: ``count`` for a
    plain count, else the metric's output name (``sum_peak``), a prefix
    ``overview/detect.py`` already rolls up."""
    return spec.output_name if spec is not None else "count"


def parse_metric_nodata(nodata_str: str | None) -> list[str]:
    """Parse a --metric-nodata string into validated numeric literals.

    Accepts comma-separated finite numbers (e.g. ``"-999"`` or ``"-999,-9999"``)
    plus the special token ``nan`` (a common float nodata encoding), which is
    normalized to lowercase ``"nan"`` and rendered as a typed NaN literal at SQL
    build time. Numeric tokens are validated against a strict ASCII literal
    pattern (``float()`` alone also accepts Unicode digits, underscores and
    inf/Infinity, none of which are safe to splice into SQL) and preserved
    verbatim so integer sentinels stay integer literals.
    """
    if nodata_str is None:
        return []
    values: list[str] = []
    for raw in nodata_str.split(","):
        token = raw.strip()
        if not token:
            continue
        if _NAN_TOKEN_RE.match(token):
            values.append("nan")
            continue
        if not _NUMERIC_TOKEN_RE.match(token) or not math.isfinite(float(token)):
            raise InvalidParameterError(
                "metric-nodata",
                f"NoData sentinel '{token}' is not a finite number. "
                'Pass comma-separated numeric values (e.g. "-999" or "-999,-9999"); '
                '"nan" is also accepted for NaN sentinels.',
            )
        values.append(token)
    if not values:
        raise InvalidParameterError(
            "metric-nodata",
            "No NoData sentinel values given. "
            'Pass comma-separated numeric values, e.g. "-999" or "-999,-9999".',
        )
    return values


def validate_metric_nodata(
    metric: str | None,
    metric_nodata: str | None,
    breakdown_metric: MetricSpec | None = None,
) -> tuple[list[MetricSpec], list[str]]:
    """Parse and cross-validate the metric and metric-nodata parameters together.

    Shared by the grid and admin aggregation paths (CLI and Python API), so the
    wording stays flag-neutral. Returns ``(metrics, nodata_values)``.

    Sentinels also apply to a ``--breakdown-metric`` column, so that counts as a
    metric for the "sentinels need something to affect" check (#1100).
    """
    metrics = parse_metrics(metric)
    nodata_values = parse_metric_nodata(metric_nodata)
    if nodata_values and not metrics and breakdown_metric is None:
        raise InvalidParameterError(
            "metric-nodata",
            "NoData sentinels require at least one metric (they only affect metric columns)",
        )
    return metrics, nodata_values


def resolve_metric_column_types(con, select_sql: str, metrics: list[MetricSpec]) -> dict[str, str]:
    """Resolve the DuckDB type of each metric column via a cheap DESCRIBE bind.

    ``select_sql`` must be a SELECT statement exposing the metric columns.
    Returns ``{column: TYPE}`` (uppercase) keyed by the metric's *own* spelling
    of the name: DESCRIBE reports the physical spelling (``Height`` for a
    ``sum:HEIGHT`` request, which :func:`validate_agg_columns` accepts), and a
    lookup by the requested spelling would then miss the REAL cast and the
    non-numeric rejection (#613, #1104). Resolution failures (e.g. a missing
    column) return empty info so the original error surfaces from the real
    query instead of an opaque bind error here.
    """
    if not metrics:
        return {}
    columns = sorted({m.column for m in metrics})
    col_list = ", ".join(quote_identifier(c) for c in columns)
    try:
        rows = con.execute(f"DESCRIBE SELECT {col_list} FROM ({select_sql})").fetchall()
    except Exception:  # noqa: BLE001 - typing is best-effort; real query reports errors
        return {}
    # One DESCRIBE row per selected column, in SELECT order.
    return {c: str(row[1]).upper() for c, row in zip(columns, rows, strict=True)}


def _nodata_literal(token: str, col_type: str | None) -> str:
    """Render one validated sentinel token as a SQL literal matched to ``col_type``.

    - ``nan`` becomes a typed NaN cast (DuckDB evaluates ``NaN = NaN`` as TRUE,
      so NULLIF/IN work).
    - For REAL (float32) columns, the literal is cast to REAL so the comparison
      happens at float32 precision; a bare DOUBLE literal like -3.4028235e+38
      would never equal the widened REAL value (#613).
    - Everything else keeps the validated token verbatim (integer sentinels stay
      integer literals; fractional sentinels never round onto integer columns).
    """
    is_real = col_type in ("FLOAT", "REAL")
    if token == "nan":
        return f"CAST('nan' AS {'REAL' if is_real else 'DOUBLE'})"
    if is_real:
        return f"CAST({token} AS REAL)"
    return token


def _nodata_wrapped_column(
    column: str, nodata_values: list[str], col_type: str | None = None
) -> str:
    """SQL expression mapping sentinel values of ``column`` to NULL."""
    qcol = quote_identifier(column)
    literals = [_nodata_literal(tok, col_type) for tok in nodata_values]
    if len(literals) == 1:
        return f"NULLIF({qcol}, {literals[0]})"
    in_list = ", ".join(literals)
    return f"CASE WHEN {qcol} IN ({in_list}) THEN NULL ELSE {qcol} END"


def _aggregated_column_expr(
    column: str,
    nodata_values: list[str] | None,
    column_types: dict[str, str] | None,
) -> str:
    """The expression an aggregate reads ``column`` through.

    Bare quoted identifier, or -- when sentinels are configured -- the column
    with its sentinel values mapped to NULL so sum/avg/min/max ignore them
    (#566). Shared by the per-cell metrics and the per-category breakdown pivots
    so both honour ``--metric-nodata`` the same way.
    """
    if not nodata_values:
        return quote_identifier(column)
    col_type = (column_types or {}).get(column)
    if col_type is not None and not _is_numeric_sql_type(col_type):
        raise InvalidParameterError(
            "metric-nodata",
            f"NoData sentinels apply only to numeric metric columns; "
            f"column '{column}' has type {col_type}",
        )
    return _nodata_wrapped_column(column, nodata_values, col_type)


def build_metric_select(
    metrics: list[MetricSpec],
    nodata_values: list[str] | None = None,
    column_types: dict[str, str] | None = None,
) -> str:
    """Build the comma-joined aggregate expressions for the SELECT (no leading comma).

    When ``nodata_values`` is given, each metric column is wrapped so sentinel
    values become NULL before aggregation (#566) -- sum/avg/min/max then ignore
    them, while the separate ``COUNT(*)`` still counts every feature.

    ``column_types`` (from :func:`resolve_metric_column_types`) lets sentinel
    literals be cast to the column's actual type and rejects sentinel use on
    non-numeric metric columns up-front instead of mid-query.
    """
    parts = []
    for m in metrics:
        col_expr = _aggregated_column_expr(m.column, nodata_values, column_types)
        parts.append(f"{m.func.upper()}({col_expr}) AS {quote_identifier(m.output_name)}")
    return ", ".join(parts)


# DuckDB folds identifiers case-insensitively, but only over ASCII: "HEIGHT"
# binds to a `Height` column while "STRASSE" does not bind to `straße`. Column
# lookups here follow the same rule so validation accepts exactly what the
# generated SQL would bind -- no more, no less.
_ASCII_LOWER = str.maketrans(string.ascii_uppercase, string.ascii_lowercase)


def _fold(name: str) -> str:
    return name.translate(_ASCII_LOWER)


def _has_column(available: set[str], name: str) -> bool:
    """Whether ``name`` resolves to a column of the input, as DuckDB would resolve it."""
    return name in available or any(_fold(c) == _fold(name) for c in available)


def _format_available(available: set[str]) -> str:
    """Render the column list for an error message.

    The internal ``__``-prefixed aliases the aggregation adds to its source
    relation (``__geom`` and friends) are not columns a user can request, so
    they are left out.
    """
    names = sorted(c for c in available if not c.startswith("__"))
    return ", ".join(names) if names else "(none)"


def validate_agg_columns(
    available: set[str],
    metrics: list[MetricSpec],
    breakdown: str | None,
    breakdown_metric: MetricSpec | None = None,
) -> None:
    """Check that requested metric/breakdown columns exist in the input.

    Raises a clear InvalidParameterError instead of letting the generated SQL
    fail with a DuckDB binder error. The common trap is ``--metric count``:
    ``count`` is emitted automatically for every bucket, so a missing literal
    ``count`` column gets a dedicated explanation. A file that really has a
    ``count`` column (e.g. re-aggregating an aggregate) is still accepted.

    Names are matched the way DuckDB matches them, so ``sum:HEIGHT`` is a valid
    request against a ``Height`` column.
    """
    for m in metrics:
        if _has_column(available, m.column):
            continue
        if _fold(m.column) == "count":
            raise InvalidParameterError(
                "metric",
                "'count' does not need to be requested: every output row "
                "automatically includes a count column (COUNT(*) of features per "
                "bucket). Use --metric for numeric rollups of existing columns "
                '(e.g. "sum:area"), or --breakdown <column> for per-category counts.',
            )
        raise InvalidParameterError(
            "metric",
            f"Metric column '{m.column}' not found in input. "
            f"Available columns: {_format_available(available)}",
        )
    if breakdown and not _has_column(available, breakdown):
        raise InvalidParameterError(
            "breakdown",
            f"Breakdown column '{breakdown}' not found in input. "
            f"Available columns: {_format_available(available)}",
        )
    if breakdown_metric is not None and not _has_column(available, breakdown_metric.column):
        raise InvalidParameterError(
            "breakdown-metric",
            f"Breakdown metric column '{breakdown_metric.column}' not found in input. "
            f"Available columns: {_format_available(available)}",
        )


_UNSAFE_CHARS = re.compile(r"[^0-9a-zA-Z]+")


def sanitize_value_for_column(value: object) -> str:
    """Turn a data value into a safe column-name fragment."""
    if value is None:
        return "null"
    cleaned = _UNSAFE_CHARS.sub("_", str(value).strip().lower()).strip("_")
    # Bounded: a 100 KB category value must not become a 100 KB column name in
    # the Parquet schema. The collision loop below keeps truncated names unique.
    return cleaned[:_MAX_VALUE_FRAGMENT] or "value"


_MAX_VALUE_FRAGMENT = 64


def _unique_name(base: str, used: set[str]) -> str:
    name, suffix = base, 2
    while name in used:
        name = f"{base}_{suffix}"
        suffix += 1
    used.add(name)
    return name


def build_breakdown_column_names(
    values: list, reserved: set[str] | None = None, prefix: str = "count"
) -> list[tuple[object, str]]:
    """Map each raw value to a unique ``<prefix>_<sanitized>`` column name.

    ``prefix`` is :func:`breakdown_prefix`. Collisions (distinct values that
    sanitize to the same fragment, or that hit a name in ``reserved``) get a
    numeric suffix, so two categories are never merged into one column and a
    pivot never lands on a ``--metric`` output name (#1100). The remainder
    bucket is reserved here too, so ``--breakdown-limit`` cannot merge it with
    a category called ``other``.
    """
    used = set(reserved or set()) | {f"{prefix}_other"}
    mapping: list[tuple[object, str]] = []
    for value in values:
        base = f"{prefix}_{sanitize_value_for_column(value)}"
        name = _unique_name(base, used)
        if name != base and base in (reserved or set()):
            warn(
                f"Breakdown column {base!r} for value {value!r} is written as {name!r}: "
                f"{base!r} is already a --metric output column"
            )
        mapping.append((value, name))
    return mapping


def breakdown_other_name(colmap: list[tuple[object, str]], reserved: set[str], prefix: str) -> str:
    """The remainder bucket's column name, clear of the pivots and the metric names."""
    return _unique_name(f"{prefix}_other", set(reserved) | {name for _, name in colmap})


def sql_literal(value: object) -> str:
    """Render a Python value as a safe DuckDB SQL literal.

    A float is written as an explicit DOUBLE: a bare ``0.699999988079071`` is
    typed DECIMAL by DuckDB and never equals the REAL column it came from, so
    the category's rows would land in no pivot at all. ``nan``/``inf`` need the
    cast to be literals at all. A NUL byte cannot sit inside a quoted string,
    so it is spliced in as ``chr(0)``.
    """
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"CAST('{value!r}' AS DOUBLE)"
    text = str(value).replace("'", "''")
    if "\x00" in text:
        return " || chr(0) || ".join(f"'{piece}'" for piece in text.split("\x00"))
    return f"'{text}'"


def resolve_breakdown_values(con, source_sql: str, column: str, limit: int) -> tuple[list, bool]:
    """Find the top-N most frequent values of ``column`` in the source.

    Returns (top_values, has_other). NULL is treated as its own value here; it is
    rolled into the remainder bucket by build_breakdown_select unless it makes
    the cut. A column whose values cannot be spelled as a SQL literal (BLOB,
    STRUCT, LIST, MAP) is refused: its pivots would silently match no rows.
    """
    if limit < 1:
        raise InvalidParameterError("breakdown-limit", f"must be at least 1, got {limit}")
    qcol = quote_identifier(column)
    (_, col_type, *_rest), *_ = con.execute(
        f"DESCRIBE SELECT {qcol} FROM ({source_sql})"
    ).fetchall()
    if not _is_scalar_sql_type(str(col_type)):
        raise InvalidParameterError(
            "breakdown",
            f"column '{column}' has type {col_type}; a breakdown needs a column of "
            "strings, numbers, booleans or dates",
        )
    rows = con.execute(
        f"SELECT {qcol} AS v, COUNT(*) AS n FROM ({source_sql}) "
        f"GROUP BY 1 ORDER BY n DESC, v LIMIT {limit + 1}"
    ).fetchall()
    top = [r[0] for r in rows[:limit]]
    has_other = len(rows) > limit
    return top, has_other


def _is_scalar_sql_type(col_type: str) -> bool:
    upper = col_type.upper()
    return not (
        upper.startswith(("BLOB", "STRUCT", "MAP", "UNION", "BIT"))
        or upper.endswith("[]")
        or "[" in upper
    )


def _breakdown_other_condition(qcol: str, value_colmap: list[tuple[object, str]]) -> str:
    """WHERE condition matching every row the kept-value pivots did not match."""
    kept_non_null = [v for v, _ in value_colmap if v is not None]
    in_list = ", ".join(sql_literal(v) for v in kept_non_null)
    if any(v is None for v, _ in value_colmap):
        # NULL is explicitly kept, so "other" is NOT(kept values including NULL).
        kept_conds = ([f"{qcol} IN ({in_list})"] if kept_non_null else []) + [f"{qcol} IS NULL"]
        return f"NOT ({' OR '.join(kept_conds)})"
    if not kept_non_null:
        # No non-null values kept (shouldn't happen, but handle gracefully).
        return "TRUE"
    # NULL is not explicitly kept, so it belongs in "other". It needs spelling
    # out: `NULL NOT IN (...)` is NULL, not TRUE.
    return f"{qcol} NOT IN ({in_list}) OR {qcol} IS NULL"


def build_breakdown_select(
    column: str,
    value_colmap: list[tuple[object, str]],
    has_other: bool,
    *,
    spec: MetricSpec | None = None,
    other_name: str | None = None,
    nodata_values: list[str] | None = None,
    column_types: dict[str, str] | None = None,
) -> str:
    """Build one filtered aggregate per kept value, plus the remainder bucket.

    ``spec`` chooses what each pivot holds: None gives ``COUNT(*) FILTER (...)
    AS count_<value>``, a spec gives ``SUM(peak) FILTER (...) AS
    sum_peak_<value>`` (#1100). A metric pivot over a category with no rows, or
    only NULL/sentinel values, is NULL, as ``--metric sum:`` reports for the
    same cell. ``nodata_values``/``column_types`` apply ``--metric-nodata`` to
    the metric column the way :func:`build_metric_select` does.
    """
    qcol = quote_identifier(column)
    if spec is None:
        agg_of = lambda cond: f"COUNT(*) FILTER (WHERE {cond})"  # noqa: E731
    else:
        value_expr = _aggregated_column_expr(spec.column, nodata_values, column_types)
        agg_of = lambda cond: f"{spec.func.upper()}({value_expr}) FILTER (WHERE {cond})"  # noqa: E731
    parts = [
        f"{agg_of(f'{qcol} IS NULL' if value is None else f'{qcol} = {sql_literal(value)}')} "
        f"AS {quote_identifier(colname)}"
        for value, colname in value_colmap
    ]
    if has_other:
        name = other_name or f"{breakdown_prefix(spec)}_other"
        parts.append(
            f"{agg_of(_breakdown_other_condition(qcol, value_colmap))} AS {quote_identifier(name)}"
        )
    return ", ".join(parts)


def build_breakdown_pivot(
    con,
    source_sql: str,
    breakdown: str,
    limit: int,
    *,
    metrics: list[MetricSpec],
    spec: MetricSpec | None,
    nodata_values: list[str] | None,
    column_types: dict[str, str] | None,
) -> str:
    """The whole ``--breakdown`` SELECT fragment: top-N values, names, aggregates.

    Shared by the grid and admin engines so the two cannot drift on naming or
    on what a pivot holds.
    """
    reserved = {m.output_name for m in metrics}
    prefix = breakdown_prefix(spec)
    top_values, has_other = resolve_breakdown_values(con, source_sql, breakdown, limit)
    colmap = build_breakdown_column_names(top_values, reserved, prefix)
    return build_breakdown_select(
        breakdown,
        colmap,
        has_other,
        spec=spec,
        other_name=breakdown_other_name(colmap, reserved, prefix),
        nodata_values=nodata_values,
        column_types=column_types,
    )


# Grid-cell output and the antimeridian
# -------------------------------------
# A cell that straddles the antimeridian is written cut into a MultiPolygon with
# parts at both -180 and +180 (RFC 7946 3.1.9). Plain min/max over such a
# dataset reports a bbox of [-180, ..., 180, ...] -- true, but it claims the
# whole globe for data that only touches the seam. RFC 7946 5.2, which
# GeoParquet's `bbox` follows, writes the crossing extent as xmin > xmax
# instead; gpio's own validator already reads it that way (#876).
_ANTIMERIDIAN_EPS = 1e-6

_PLAIN_EXTENT_SQL = """
SELECT min(ST_XMin(g)), max(ST_XMax(g)), min(ST_YMin(g)), max(ST_YMax(g))
FROM (SELECT {geom_expr} AS g FROM {relation} WHERE {qcol} IS NOT NULL)
"""

# Widest longitude gap between the parts, found with a running max over the
# sorted part extents -- the standard interval merge. The parts, not the whole
# geometries: a cut cell's own extent already spans -180 to 180 and would hide
# the gap that makes the crossing visible.
_WIDEST_LON_GAP_SQL = """
WITH parts AS (
    SELECT ST_XMin(p) AS lo, ST_XMax(p) AS hi
    FROM (SELECT UNNEST(ST_Dump({geom_expr})).geom AS p FROM {relation} WHERE {qcol} IS NOT NULL)
), merged AS (
    SELECT lo, max(hi) OVER (ORDER BY lo ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev
    FROM parts
)
SELECT lo, prev FROM merged WHERE prev IS NOT NULL AND lo > prev ORDER BY lo - prev DESC LIMIT 1
"""


def antimeridian_aware_bbox(con, relation: str, geometry_column: str) -> list[float] | None:
    """The ``geo`` bbox for a lon/lat relation, in RFC 7946 wrap form when it crosses.

    Returns ``[xmin, ymin, xmax, ymax]`` with ``xmin > xmax`` when the data
    leaves a longitude gap wider than the one the plain extent implies -- the
    shape of a dataset that sits astride the antimeridian rather than spanning
    the globe. Returns the plain extent otherwise, and None when the relation
    holds no geometry (the caller then leaves the bbox to the writer).

    ``relation`` must already be a FROM-able expression and ``geometry_column``
    a column of it holding WKB or GEOMETRY. Longitudes are assumed: the caller
    establishes a geographic CRS, since ``xmin > xmax`` cannot mean a crossing
    in a projected one.
    """
    qcol = quote_identifier(geometry_column)
    geom_expr = geometry_to_geom_expr(con, relation, geometry_column)
    params = {"relation": relation, "qcol": qcol, "geom_expr": geom_expr}
    xmin, xmax, ymin, ymax = con.execute(_PLAIN_EXTENT_SQL.format(**params)).fetchone()
    if xmin is None:
        return None
    plain = [xmin, ymin, xmax, ymax]
    # Only data reaching both edges can be hiding a crossing; anything else is
    # already reported tightly and must not pay for the part-level scan.
    if xmin > -180.0 + _ANTIMERIDIAN_EPS or xmax < 180.0 - _ANTIMERIDIAN_EPS:
        return plain
    gap = con.execute(_WIDEST_LON_GAP_SQL.format(**params)).fetchone()
    if gap is None or (gap[0] - gap[1]) <= (xmin + 360.0) - xmax:
        return plain
    return [gap[0], ymin, gap[1], ymax]
