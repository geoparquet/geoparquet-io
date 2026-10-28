"""Do a bbox column's four numbers actually cover the geometry beside them?

``bbox_structure`` answers whether the column is *there* and whether the ``geo``
block *declares* it. Neither question touches the numbers, so a file whose bbox
column was left behind by an operation that rewrote geometry -- a simplify, a
reprojection, a repair -- passes both while describing geometry it no longer
has. This module asks the remaining question, row by row: is each row's bbox an
envelope that contains that row's geometry?

The ways to be wrong are not equally bad, and each finding carries its own
severity (:mod:`~geoparquet_io.core.check_verdict`) rather than treating
"mismatch" as one finding:

* **Undersized is a failure.** A covering exists so a reader can prune row
  groups and pages without decoding geometry. A bbox smaller than its geometry
  makes that pruning drop rows that genuinely intersect the query window, and
  nothing anywhere raises: the reader returns less data than the file holds.
* **A NULL bbox, a bbox with a NULL or NaN field, and a geometry value that is
  not WKB are failures** for the same reason: every comparison against NULL or
  NaN is false, so a covering-based filter drops the row, and a value that does
  not parse is one no reader can use.
* **Oversized is a warning.** The reader over-fetches and re-filters on the real
  geometry, so the answer stays right. So is a bbox beside a NULL or empty
  geometry: there is nothing for it to describe.

An antimeridian-crossing bbox (xmin > xmax) on a geographic CRS is read the way
``gpio check spec`` reads the file-level ``bbox`` (RFC 7946, 5.2): longitudes in
[xmin, 180] or [-180, xmax] are inside it, judged per vertex. On a projected CRS
xmin > xmax is simply wrong, and is reported as undersized.

Cost. Comparing means decoding geometry, and a 7-million-row file is ordinary,
so the default is a sample (:data:`DEFAULT_BBOX_SAMPLE_ROWS`) drawn across the
whole file rather than its first rows -- the realistic corruptions are spread
through the file, and a head-of-file sample would miss a partition whose tail
was rewritten. The sample bounds the *decode* cost, not the I/O: drawing rows
from across the file means reading the geometry and bbox columns of every row
group, so on a remote file the sampled check still transfers both columns
whole. The sample is seeded, so the same file gets the same verdict every run;
a passing sample is evidence, not a guarantee -- ``all_rows=True`` is the
exhaustive form, named after the ``--sample-files``/``--all-files`` pair that
already scopes partitioned checks.
"""

from __future__ import annotations

import sys
from typing import TypedDict, cast

import duckdb

from geoparquet_io.core.bbox_structure import check_bbox_structure
from geoparquet_io.core.check_verdict import (
    FAILURE_GLYPH,
    ISSUE_SEVERITY_KEY,
    PASS_GLYPH,
    SEVERITY_KEY,
    WARNING_GLYPH,
    Verdict,
    could_not_run,
    worst,
)
from geoparquet_io.core.crs_utils import extract_crs_from_parquet, is_geographic_crs
from geoparquet_io.core.duckdb_metadata import get_geo_metadata, get_schema_info
from geoparquet_io.core.duckdb_utils import (
    disable_geoparquet_conversion,
    get_duckdb_connection,
    is_interrupt,
    quote_identifier,
    sql_path,
)
from geoparquet_io.core.file_utils import resolve_file_url
from geoparquet_io.core.geometry_detection import (
    GeometryRead,
    find_primary_geometry_column,
    resolve_geometry_read,
)
from geoparquet_io.core.logging_config import debug, error, info, progress, success, warn
from geoparquet_io.core.parquet_schema import root_schema_columns, schema_direct_children
from geoparquet_io.core.remote import needs_httpfs

#: Rows compared when the caller does not say otherwise.
#:
#: Large enough that a corruption affecting even a low single-digit percentage
#: of rows is seen with near-certainty (a 1%-corrupt file hides from a 5,000-row
#: sample with probability 0.99**5000, far below 1e-21), small enough that the
#: geometry decoding and envelope arithmetic stay a fixed cost however large the
#: file is. Reading is not fixed: the sample is drawn from every row group, so
#: the geometry and bbox columns are read in full.
DEFAULT_BBOX_SAMPLE_ROWS = 5000

#: The sample's seed, so the same file gets the same verdict on every run.
BBOX_SAMPLE_SEED = 1239

#: Offending rows named in the report. Enough to start debugging, bounded so a
#: wholly-corrupt 7M-row file does not print 7M row numbers.
MAX_BBOX_EXAMPLES = 5

#: The name the comparison is reported under when it could not run.
BBOX_VALUES_CHECK = "Bbox value comparison"

#: Relative slack allowed when the bbox struct is stored as float64.
#:
#: Four ULP of double precision. A bbox computed from the same float64
#: coordinates the geometry holds should agree exactly; the four ULP cover an
#: intermediate rounding (a reprojection, a bounds aggregation) without
#: admitting anything a reader would notice.
_FLOAT64_RELATIVE_TOLERANCE = 4 * sys.float_info.epsilon

#: Relative slack allowed when the bbox struct is stored as float32.
#:
#: Four ULP of single precision. The GeoParquet spec lets a covering be stored
#: as float32 and asks writers to round outward so the box still covers; many
#: do not, and round to nearest instead, which puts a bound up to half an ULP
#: *inside* the geometry. That is an artefact of the column's own storage, not a
#: wrong bbox, so the slack has to be the storage type's -- the alternative is
#: failing every float32 covering ever written.
_FLOAT32_RELATIVE_TOLERANCE = 4 * 1.1920928955078125e-07

#: Absolute slack for coordinates at or near zero, where a relative tolerance
#: collapses to nothing. 1e-9 degrees is about 0.1 mm at the equator.
_ABSOLUTE_TOLERANCE_FLOOR = 1e-9

#: DuckDB's ``read_parquet`` refuses ``file_row_number`` when the file already
#: has a column of that name, so such a file is numbered the slow way instead.
_ROW_NUMBER_COLUMN = "file_row_number"

#: The bbox field types a covering may use; anything else is not compared.
_FLOAT_FIELD_TYPES = frozenset({"float", "double"})

#: Names the comparison's own columns go by, kept apart from the file's.
_ROW = "__gpio_bbox_row"
_BOX = "__gpio_bbox_value"

#: The repair, for every finding here: the column has to be recomputed, which
#: is a rewrite rather than a metadata edit, so ``check bbox --fix`` cannot do it.
BBOX_REGENERATE_HINT = "Regenerate the column: gpio add bbox <input> <output> --force"

#: The caller did not say which bbox column to compare; look it up.
_LOOK_UP = object()


class BboxValueReport(TypedDict, total=False):
    """What :func:`check_bbox_covers_geometry` found.

    ``passed`` is False for any finding, a warning included; ``severity`` says
    how bad, and ``issue_severity`` labels each issue (see ``check_verdict``).
    """

    passed: bool
    severity: str
    issue_severity: dict[str, str]
    #: Set, with ``error``, when the comparison was asked for and could not run.
    could_not_run: list[str]
    #: Whether any row was actually compared.
    checked: bool
    #: Why not, when ``checked`` is False.
    skip_reason: str | None
    bbox_column_name: str | None
    geometry_column: str | None
    #: The geometry column's declared ``edges`` ("planar" when not declared).
    edges: str
    total_rows: int
    checked_rows: int
    #: True when ``checked_rows`` is a random sample rather than every row.
    sampled: bool
    undersized_rows: int
    oversized_rows: int
    null_bbox_rows: int
    nan_bbox_rows: int
    #: Geometry values that are not WKB: zero-length or unparsable bytes.
    unparsable_geometry_rows: int
    #: Rows with NULL or empty geometry: nothing to compare.
    absent_geometry_rows: int
    #: Of those, rows that still carry a bbox.
    bbox_without_geometry_rows: int
    #: Rows whose bbox was read as antimeridian-crossing (xmin > xmax).
    wrapped_rows: int
    worst_shortfall: float | None
    worst_excess: float | None
    undersized_examples: list[dict]
    oversized_examples: list[dict]
    null_bbox_examples: list[int]
    unparsable_examples: list[int]
    issues: list[str]
    recommendations: list[str]


def _empty_report(skip_reason, bbox_column=None, geometry_column=None) -> dict:
    """The report for a file with nothing to compare: a pass, with the reason."""
    return {
        "passed": True,
        SEVERITY_KEY: Verdict.PASS.label,
        ISSUE_SEVERITY_KEY: {},
        "checked": False,
        "skip_reason": skip_reason,
        "bbox_column_name": bbox_column,
        "geometry_column": geometry_column,
        "edges": "planar",
        "total_rows": 0,
        "checked_rows": 0,
        "sampled": False,
        "undersized_rows": 0,
        "oversized_rows": 0,
        "null_bbox_rows": 0,
        "nan_bbox_rows": 0,
        "unparsable_geometry_rows": 0,
        "absent_geometry_rows": 0,
        "bbox_without_geometry_rows": 0,
        "wrapped_rows": 0,
        "worst_shortfall": None,
        "worst_excess": None,
        "undersized_examples": [],
        "oversized_examples": [],
        "null_bbox_examples": [],
        "unparsable_examples": [],
        "issues": [],
        "recommendations": [],
    }


def _could_not_compare(reason, bbox_column, geometry_column) -> dict:
    """The report for a comparison that was asked for and could not run: a failure."""
    return {
        **_empty_report(reason, bbox_column, geometry_column),
        **could_not_run(BBOX_VALUES_CHECK, reason),
    }


def _bbox_field_types(schema_info, bbox_column: str) -> set[str]:
    """The lower-cased types of the bbox struct's direct children."""
    return {
        str(child.get("type", "")).lower()
        for index, column in enumerate(schema_info)
        if column.get("name") == bbox_column
        for child in schema_direct_children(schema_info, index)
    }


def _relative_tolerance(field_types: set[str]) -> float:
    """The slack the bbox column's own storage type forces.

    A float32 covering cannot name a float64 coordinate, so comparing it at
    double precision would fail every file that stores one. Anything that is
    not float32 is held to the double-precision figure.
    """
    return _FLOAT32_RELATIVE_TOLERANCE if "float" in field_types else _FLOAT64_RELATIVE_TOLERANCE


def _picked_rows(source: str, row_expr: str, geom: str, bbox: str, sample_clause: str) -> str:
    """Each row's number, raw geometry and bbox -- numbered *before* the sample.

    The geometry keeps its own column name so :class:`GeometryRead`'s
    expressions apply to it unchanged. Numbering in an inner query means a
    sampled row still carries its position in the file, whichever way the row
    number is computed.
    """
    return f"""
        SELECT * FROM (
            SELECT {row_expr} AS {_ROW}, {geom}, {bbox} AS {_BOX} FROM {source}
        ) {sample_clause}
    """


def _x_extent_sql(wrapped: str) -> tuple[str, str]:
    """(x shortfall, x excess) for one row, antimeridian-aware.

    An ordinary bbox is compared with the geometry's envelope. A wrapped one
    (xmin > xmax on a geographic CRS) covers [xmin, 180] and [-180, xmax], the
    reading ``check spec`` gives the file-level ``bbox``
    (``validate._x_within_sql``): the shortfall is how far the worst vertex
    lies outside both, and the excess is measured on the vertices shifted onto
    one continuous range, [xmin, xmax + 360]. A segment whose interior bulges
    across the gap between two in-range vertices is undecidable from its
    vertices; RFC 7946 tells producers to cut such geometries at the
    antimeridian.
    """
    xs = "[ST_X(p.geom) FOR p IN ST_Dump(ST_Points(geom))]"
    wrapped_short = (
        "list_max([CASE WHEN x >= bxmin OR x <= bxmax THEN 0 "
        f"ELSE least(bxmin - x, x - bxmax) END FOR x IN {xs}])"
    )
    shifted = f"[CASE WHEN x <= bxmax THEN x + 360 ELSE x END FOR x IN {xs}]"
    wrapped_over = f"greatest(list_min({shifted}) - bxmin, (bxmax + 360) - list_max({shifted}))"
    short = (
        f"CASE WHEN {wrapped} THEN {wrapped_short} ELSE greatest(bxmin - gxmin, gxmax - bxmax) END"
    )
    over = (
        f"CASE WHEN {wrapped} THEN {wrapped_over} ELSE greatest(gxmin - bxmin, bxmax - gxmax) END"
    )
    return short, over


def _classified_rows(read: GeometryRead, geographic: bool) -> str:
    """Per-row categories over ``picked``: which rows are comparable, and which are findings.

    A bbox is read as wrapped only on a geographic CRS and only when both of
    its x bounds are longitudes; anything else with xmin > xmax is undersized.

    The categories are exclusive, in this order: a geometry value that is not
    WKB; no geometry (NULL or empty); a NULL bbox (the struct or any field); a
    NaN in the bbox; and the rows that can be compared. ``read`` supplies the
    one safe way to decode the column (#1256): a bad value -- zero-length,
    unparsable, or the one bad entry of a dictionary-encoded page -- is NULL in
    ``geom`` and TRUE in ``bad``, and every other row is still read.
    """
    box = _BOX
    fields = [f"{box}.{name}" for name in ("xmin", "ymin", "xmax", "ymax")]
    any_null = " OR ".join([f"{box} IS NULL", *(f"{f} IS NULL" for f in fields)])
    any_nan = " OR ".join(f"isnan({f})" for f in fields)
    bad = f"({read.empty_sql} OR {read.unparsable_sql})"
    return f"""
        SELECT {_ROW} AS row_index, geom, bad, absent, bbox_null, bbox_nan,
               NOT (bad OR absent OR bbox_null OR bbox_nan) AS comparable,
               {"TRUE" if geographic else "FALSE"} AND bxmin > bxmax
                   AND bxmin <= 180 AND bxmax >= -180 AS wrapped,
               bxmin, bymin, bxmax, bymax
        FROM (
            SELECT {_ROW}, geom, bad,
                   NOT bad AND (raw_null OR coalesce(ST_IsEmpty(geom), FALSE)) AS absent,
                   ({any_null}) AS bbox_null,
                   coalesce({any_nan}, FALSE) AS bbox_nan,
                   {box}.xmin AS bxmin, {box}.ymin AS bymin,
                   {box}.xmax AS bxmax, {box}.ymax AS bymax
            FROM (
                SELECT {_ROW}, {box}, {read.quoted} IS NULL AS raw_null,
                       coalesce({bad}, FALSE) AS bad, {read.expr} AS geom
                FROM picked
            )
        )
    """


def _comparison_query(
    *,
    picked: str,
    classified: str,
    relative_tolerance: float,
    max_examples: int,
) -> str:
    """The one query the whole check runs: per-row deltas, aggregated in DuckDB.

    Aggregated server-side rather than fetched row by row so ``all_rows=True``
    on a 7-million-row file stays a fixed amount of client memory. The offending
    rows come back through ``arg_max``, which keeps only the worst
    ``max_examples`` -- the rows most worth looking at, and a bounded list.
    """
    tol = f"{relative_tolerance!r}::DOUBLE"
    floor = f"{_ABSOLUTE_TOLERANCE_FLOOR!r}::DOUBLE"
    limit = max_examples
    short_x, over_x = _x_extent_sql("wrapped")
    undersized = "comparable AND shortfall_excess > 0"
    oversized = "comparable AND shortfall_excess <= 0 AND excess_excess > 0"
    null_bbox = "bbox_null AND NOT bad AND NOT absent"
    nan_bbox = "bbox_nan AND NOT bbox_null AND NOT bad AND NOT absent"
    return f"""
    WITH picked AS ({picked}),
    classified AS ({classified}),
    bounds AS (
        SELECT *,
               CASE WHEN comparable THEN ST_XMin(geom) END AS gxmin,
               CASE WHEN comparable THEN ST_YMin(geom) END AS gymin,
               CASE WHEN comparable THEN ST_XMax(geom) END AS gxmax,
               CASE WHEN comparable THEN ST_YMax(geom) END AS gymax
        FROM classified
    ),
    deltas AS (
        SELECT *,
               CASE WHEN comparable THEN {short_x} END AS short_x,
               greatest(bymin - gymin, gymax - bymax) AS short_y,
               CASE WHEN comparable THEN {over_x} END AS over_x,
               greatest(gymin - bymin, bymax - gymax) AS over_y,
               greatest({floor}, {tol} * greatest(abs(gxmin), abs(gxmax),
                                                   abs(bxmin), abs(bxmax))) AS tol_x,
               greatest({floor}, {tol} * greatest(abs(gymin), abs(gymax),
                                                   abs(bymin), abs(bymax))) AS tol_y
        FROM bounds
    ),
    verdict AS (
        SELECT *,
               greatest(short_x, short_y) AS shortfall,
               greatest(over_x, over_y) AS excess,
               greatest(short_x - tol_x, short_y - tol_y) AS shortfall_excess,
               greatest(over_x - tol_x, over_y - tol_y) AS excess_excess
        FROM deltas
    )
    SELECT
        count(*) AS checked_rows,
        count(*) FILTER (WHERE absent) AS absent_geometry_rows,
        count(*) FILTER (WHERE absent AND NOT bbox_null) AS bbox_without_geometry_rows,
        count(*) FILTER (WHERE bad) AS unparsable_geometry_rows,
        count(*) FILTER (WHERE {null_bbox}) AS null_bbox_rows,
        count(*) FILTER (WHERE {nan_bbox}) AS nan_bbox_rows,
        count(*) FILTER (WHERE {undersized}) AS undersized_rows,
        count(*) FILTER (WHERE {oversized}) AS oversized_rows,
        count(*) FILTER (WHERE comparable AND wrapped) AS wrapped_rows,
        max(shortfall) FILTER (WHERE {undersized}) AS worst_shortfall,
        max(excess) FILTER (WHERE {oversized}) AS worst_excess,
        arg_max({{'row': row_index, 'shortfall': shortfall}}, shortfall_excess, {limit})
            FILTER (WHERE {undersized}) AS undersized_examples,
        arg_max({{'row': row_index, 'excess': excess}}, excess_excess, {limit})
            FILTER (WHERE {oversized}) AS oversized_examples,
        arg_min(row_index, row_index, {limit}) FILTER (WHERE {null_bbox}) AS null_bbox_examples,
        arg_min(row_index, row_index, {limit}) FILTER (WHERE bad) AS unparsable_examples
    FROM verdict
    """


#: The query's scalar columns, in order, then its example lists.
_COUNT_KEYS = (
    "checked_rows",
    "absent_geometry_rows",
    "bbox_without_geometry_rows",
    "unparsable_geometry_rows",
    "null_bbox_rows",
    "nan_bbox_rows",
    "undersized_rows",
    "oversized_rows",
    "wrapped_rows",
    "worst_shortfall",
    "worst_excess",
)
_EXAMPLE_KEYS = (
    "undersized_examples",
    "oversized_examples",
    "null_bbox_examples",
    "unparsable_examples",
)

#: Each finding: (count key, severity, how it reads after "N of M checked rows").
_FINDINGS = (
    (
        "unparsable_geometry_rows",
        Verdict.FAILURE,
        "hold a geometry value that is not valid WKB — no reader can decode it, "
        "and its bbox cannot be checked",
    ),
    (
        "undersized_rows",
        Verdict.FAILURE,
        "have a bbox that does not cover its geometry (worst shortfall {worst_shortfall:.6g}) "
        "— a reader's spatial filter silently drops them",
    ),
    (
        "null_bbox_rows",
        Verdict.FAILURE,
        "have a NULL bbox (or a NULL bbox field) beside a non-NULL geometry — every "
        "covering-based filter drops them",
    ),
    (
        "nan_bbox_rows",
        Verdict.FAILURE,
        "have a bbox no spatial filter can match: the bbox contains NaN",
    ),
    (
        "oversized_rows",
        Verdict.WARNING,
        "have a bbox larger than their geometry (worst excess {worst_excess:.6g}) — readers "
        "over-fetch, but results stay correct",
    ),
    (
        "bbox_without_geometry_rows",
        Verdict.WARNING,
        "have a bbox beside a NULL or empty geometry, which has no extent for it to describe",
    ),
)


def _describe_findings(report: dict) -> None:
    """Fill ``issues``, their severities and ``recommendations``; set the verdict."""
    checked = report["checked_rows"]
    levels: dict[str, str] = {}
    verdicts = []
    for key, verdict, text in _FINDINGS:
        count = report[key]
        if not count:
            continue
        issue = f"{count:,} of {checked:,} checked rows " + text.format(**report)
        report["issues"].append(issue)
        levels[issue] = verdict.label
        verdicts.append(verdict)

    overall = worst(verdicts)
    report["passed"] = overall is Verdict.PASS
    report[SEVERITY_KEY] = overall.label
    report[ISSUE_SEVERITY_KEY] = levels
    if overall is not Verdict.PASS:
        report["recommendations"].append(BBOX_REGENERATE_HINT)


def _scope_phrase(report: dict) -> str:
    """How much of the file was compared, with a nudge when it was a sample."""
    phrase = f"{report['checked_rows']:,} of {report['total_rows']:,} rows checked"
    if report["sampled"]:
        phrase += " (a random sample; --all-rows checks every row)"
    return phrase


def _examples_phrase(examples: list[dict], key: str, label: str) -> str:
    """The offending rows as ``row 12 (short by 1.5), row 7 (short by 0.2)``, worst first."""
    return ", ".join(f"row {item['row']} ({label} {item[key]:.6g})" for item in examples)


def _rows_phrase(rows: list[int]) -> str:
    return ", ".join(f"row {row}" for row in rows)


def _report_skip(report: dict) -> None:
    """Say why nothing was compared: a skip, or a comparison that could not run."""
    if report[SEVERITY_KEY] == Verdict.FAILURE.label:
        error(f"{FAILURE_GLYPH} Bbox values not compared: {report['skip_reason']}")
    else:
        info(f"ℹ️  Bbox values not compared: {report['skip_reason']}")


def _report_failures(report: dict) -> None:
    if report["unparsable_geometry_rows"]:
        error(
            f"{FAILURE_GLYPH} {report['unparsable_geometry_rows']:,} rows hold a geometry "
            "value that is not valid WKB"
        )
        info(f"   First: {_rows_phrase(report['unparsable_examples'])}")
    if report["undersized_rows"]:
        error(
            f"{FAILURE_GLYPH} {report['undersized_rows']:,} rows where the bbox does not "
            "cover its geometry"
        )
        info(
            "   Readers prune on the covering, so these rows are missing from spatial "
            "query results with no error anywhere"
        )
        info(
            f"   Worst: {_examples_phrase(report['undersized_examples'], 'shortfall', 'short by')}"
        )
    if report["null_bbox_rows"]:
        error(
            f"{FAILURE_GLYPH} {report['null_bbox_rows']:,} rows have a NULL bbox (or NULL bbox "
            "field) beside a non-NULL geometry"
        )
        info(f"   First: {_rows_phrase(report['null_bbox_examples'])}")
    if report["nan_bbox_rows"]:
        error(f"{FAILURE_GLYPH} {report['nan_bbox_rows']:,} rows have a bbox that contains NaN")


def _report_warnings(report: dict) -> None:
    if report["oversized_rows"]:
        warn(
            f"{WARNING_GLYPH} {report['oversized_rows']:,} rows have a bbox larger than their "
            "geometry (over-fetch only; results stay correct)"
        )
        info(f"   Worst: {_examples_phrase(report['oversized_examples'], 'excess', 'over by')}")
    if report["bbox_without_geometry_rows"]:
        warn(
            f"{WARNING_GLYPH} {report['bbox_without_geometry_rows']:,} rows have a bbox beside "
            "a NULL or empty geometry"
        )


def _report_notes(report: dict) -> None:
    """The readings a verdict depends on, so neither is ever invisible."""
    if report["wrapped_rows"]:
        info(
            f"   {report['wrapped_rows']:,} bbox values cross the antimeridian (xmin > xmax), "
            "read as RFC 7946 5.2 wrap-around"
        )
    if report["edges"] != "planar":
        info(
            f"   Edges are {report['edges']}: each bbox is compared with the planar envelope "
            "of its geometry's vertices, which a geodesic edge can bulge past"
        )


def _report_findings(report: dict) -> None:
    """Print the section ``gpio check bbox`` shows for this comparison."""
    progress("\nBbox vs geometry:")
    if not report["checked"]:
        _report_skip(report)
        return

    scope = _scope_phrase(report)
    if report["passed"]:
        success(f"{PASS_GLYPH} {scope}: every bbox covers its geometry")
    else:
        info(f"   {scope}")
    _report_failures(report)
    _report_warnings(report)
    _report_notes(report)
    if not report["passed"]:
        info(f"   {BBOX_REGENERATE_HINT}")


def check_bbox_covers_geometry(
    parquet_file: str,
    *,
    sample_rows: int = DEFAULT_BBOX_SAMPLE_ROWS,
    all_rows: bool = False,
    max_examples: int = MAX_BBOX_EXAMPLES,
    verbose: bool = False,
    quiet: bool = False,
    bbox_column=_LOOK_UP,
    geo_meta: dict | None = None,
) -> BboxValueReport:
    """Compare each row's bbox against its geometry's real envelope.

    Args:
        parquet_file: Path to the parquet file
        sample_rows: Rows to compare; 0 skips the comparison entirely. Ignored
            when ``all_rows`` is set, and ignored for a file that has fewer
            rows than this, which is compared whole. Bounds the geometry
            decoding, not the read: see the module docstring.
        all_rows: Compare every row rather than a sample
        max_examples: Offending rows to name per category
        verbose: Print additional diagnostics
        quiet: Suppress all output (for multi-file batch mode)
        bbox_column: The bbox column, when the caller already found it (None
            for "the file has none"); looked up when not given
        geo_meta: The file's parsed ``geo`` block, when the caller already read
            it -- names the primary geometry column and its ``edges``

    Returns:
        A :class:`BboxValueReport`. A file with no bbox column, no geometry
        column, or nothing requested comes back ``checked=False, passed=True``:
        the absence of a bbox column is a verdict ``check_bbox_structure``
        already reaches, and this check must not double-report it. A
        comparison that could not run is a failure carrying the reason.
    """
    report = _check(
        parquet_file,
        sample_rows=sample_rows,
        all_rows=all_rows,
        max_examples=max_examples,
        verbose=verbose,
        bbox_column=bbox_column,
        geo_meta=geo_meta,
    )
    if not quiet:
        _report_findings(report)
    return cast(BboxValueReport, report)


def _check(
    parquet_file, *, sample_rows, all_rows, max_examples, verbose, bbox_column, geo_meta
) -> dict:
    if not all_rows and sample_rows <= 0:
        return _empty_report("row comparison not requested (--sample-rows 0)")

    if bbox_column is _LOOK_UP:
        bbox_column = check_bbox_structure(parquet_file, verbose).get("bbox_column_name")
    if not bbox_column:
        return _empty_report("file has no bbox column to compare")

    if geo_meta is None:
        geo_meta = get_geo_metadata(parquet_file) or {}
    schema_info = get_schema_info(parquet_file)
    field_types = _bbox_field_types(schema_info, bbox_column)
    if not field_types <= _FLOAT_FIELD_TYPES:
        return _empty_report(
            f"bbox fields are {', '.join(sorted(field_types))}, not FLOAT or DOUBLE",
            bbox_column,
        )
    geometry_column = geo_meta.get("primary_column") or find_primary_geometry_column(
        parquet_file, verbose
    )
    # Presence, not just a name: the detector falls back to the conventional
    # name, so a file with a bbox column and no geometry at all answers
    # "geometry" and the query would fail to bind.
    if not geometry_column or geometry_column not in {
        column.get("name") for column in root_schema_columns(schema_info)
    }:
        return _empty_report("file has no geometry column to compare", bbox_column)

    column_meta = (geo_meta.get("columns") or {}).get(geometry_column) or {}
    geographic = is_geographic_crs(
        extract_crs_from_parquet(parquet_file, geometry_column=geometry_column)
    )
    raw_url = resolve_file_url(parquet_file, verbose)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(parquet_file))
    try:
        # The stored bytes, not DuckDB's lazy GEOMETRY view of them: a bad
        # value is then a row to count, not an error mid-scan (#1082).
        disable_geoparquet_conversion(con)
        report = _run_comparison(
            con,
            raw_url,
            schema_info,
            bbox_column,
            geometry_column,
            sample_rows=sample_rows,
            all_rows=all_rows,
            max_examples=max_examples,
            relative_tolerance=_relative_tolerance(field_types),
            geographic=geographic,
            verbose=verbose,
        )
    except duckdb.Error as e:
        if is_interrupt(e):
            raise
        return _could_not_compare(str(e), bbox_column, geometry_column)
    finally:
        con.close()
    report["edges"] = column_meta.get("edges") or "planar"
    return report


def _row_source(schema_info, raw_url: str) -> tuple[str, str]:
    """``(table expression, row-index expression)`` for numbering the file's rows.

    ``file_row_number`` is the cheap answer -- the reader already knows it --
    but DuckDB refuses the option on a file that has a column of that name, so
    such a file is numbered with a window function instead. That is a
    materializing scan; it is also the only way to name a row in a file that
    took the name.
    """
    path = sql_path(raw_url)
    if any(column.get("name") == _ROW_NUMBER_COLUMN for column in schema_info):
        return f"read_parquet({path})", "row_number() OVER () - 1"
    return f"read_parquet({path}, file_row_number=true)", quote_identifier(_ROW_NUMBER_COLUMN)


def _run_comparison(
    con,
    raw_url: str,
    schema_info,
    bbox_column: str,
    geometry_column: str,
    *,
    sample_rows: int,
    all_rows: bool,
    max_examples: int,
    relative_tolerance: float,
    geographic: bool,
    verbose: bool,
) -> dict:
    """Build and run the comparison, and turn its one row into a report."""
    read = resolve_geometry_read(con, raw_url, geometry_column)
    if read.kind == "geoarrow":
        # A legitimate encoding, just not one decoded here: a skip, not a failure.
        return _empty_report(
            "geometry column is not WKB or native GEOMETRY (a GeoArrow encoding)",
            bbox_column,
            geometry_column,
        )
    if not read.readable:
        return _could_not_compare(read.problem, bbox_column, geometry_column)

    source, row_expr = _row_source(schema_info, raw_url)
    (total_rows,) = con.execute(f"SELECT count(*) FROM {source}").fetchone()
    sampled = not all_rows and total_rows > sample_rows
    # Seeded, so the same file gets the same verdict every run.
    sample_clause = (
        f"USING SAMPLE reservoir({sample_rows} ROWS) REPEATABLE ({BBOX_SAMPLE_SEED})"
        if sampled
        else ""
    )

    query = _comparison_query(
        picked=_picked_rows(
            source, row_expr, read.quoted, quote_identifier(bbox_column), sample_clause
        ),
        classified=_classified_rows(read, geographic),
        relative_tolerance=relative_tolerance,
        max_examples=max_examples,
    )
    if verbose:
        debug(f"Comparing bbox '{bbox_column}' against geometry '{geometry_column}'")
        debug(query)

    row = con.execute(query).fetchone()
    report: dict = {
        **_empty_report(None, bbox_column, geometry_column),
        "checked": True,
        "total_rows": total_rows,
        "sampled": sampled,
    }
    report.update(zip(_COUNT_KEYS, row[: len(_COUNT_KEYS)], strict=True))
    for key, value in zip(_EXAMPLE_KEYS, row[len(_COUNT_KEYS) :], strict=True):
        report[key] = list(value or [])
    _describe_findings(report)
    return report
