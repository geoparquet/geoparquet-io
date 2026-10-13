"""Do a bbox column's four numbers actually cover the geometry beside them?

``bbox_structure`` answers whether the column is *there* and whether the ``geo``
block *declares* it. Neither question touches the numbers, so a file whose bbox
column was left behind by an operation that rewrote geometry -- a simplify, a
reprojection, a repair -- passes both while describing geometry it no longer
has. This module asks the remaining question, row by row: is each row's bbox an
envelope that contains that row's geometry?

The two ways to be wrong are not equally bad, and the severities here follow
that rather than treating "mismatch" as one finding:

* **Undersized is a correctness failure.** A covering exists so a reader can
  prune row groups and pages without decoding geometry. A bbox smaller than its
  geometry makes that pruning drop rows that genuinely intersect the query
  window, and nothing anywhere raises: the reader simply returns less data than
  the file holds and cannot tell.
* **Oversized is a performance cost.** The reader over-fetches and re-filters
  on the real geometry, so the answer stays right. That is a warning, not a
  failure.

A NULL bbox beside a non-NULL geometry is counted with the failures for the
same reason an undersized one is: every comparison against NULL is NULL, so a
covering-based filter drops the row.

Cost. Comparing means decoding geometry, and a 7-million-row file is ordinary,
so the default is a sample (:data:`DEFAULT_BBOX_SAMPLE_ROWS`) drawn across the
whole file rather than its first rows -- the realistic corruptions are spread
through the file, and a head-of-file sample would miss a partition whose tail
was rewritten. ``all_rows=True`` is the exhaustive form, named after the
``--sample-files``/``--all-files`` pair that already scopes partitioned checks.
"""

import sys
from typing import TypedDict

from geoparquet_io.core.bbox_structure import check_bbox_structure
from geoparquet_io.core.duckdb_metadata import get_schema_info
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, quote_identifier, sql_path
from geoparquet_io.core.file_utils import resolve_file_url
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import debug, error, info, progress, success, warn
from geoparquet_io.core.parquet_schema import root_schema_columns, schema_direct_children
from geoparquet_io.core.remote import needs_httpfs

#: Rows compared when the caller does not say otherwise.
#:
#: Large enough that a corruption affecting even a low single-digit percentage
#: of rows is seen with near-certainty (a 1%-corrupt file hides from a 5,000-row
#: sample with probability 0.99**5000, far below 1e-21), small enough that the
#: envelope arithmetic is a fixed cost no matter how large the file is.
DEFAULT_BBOX_SAMPLE_ROWS = 5000

#: Offending rows named in the report. Enough to start debugging, bounded so a
#: wholly-corrupt 7M-row file does not print 7M row numbers.
MAX_BBOX_EXAMPLES = 5

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

#: The repair, for every finding here: the column has to be recomputed, which
#: is a rewrite rather than a metadata edit, so ``check bbox --fix`` cannot do it.
BBOX_REGENERATE_HINT = "Regenerate the column: gpio add bbox <input> <output> --force"


class BboxValueReport(TypedDict, total=False):
    """What :func:`check_bbox_covers_geometry` found."""

    #: False only when an undersized or NULL bbox was seen.
    passed: bool
    #: Whether any row was actually compared.
    checked: bool
    #: Why not, when ``checked`` is False.
    skip_reason: str | None
    bbox_column_name: str | None
    geometry_column: str | None
    total_rows: int
    checked_rows: int
    #: True when ``checked_rows`` is a random sample rather than every row.
    sampled: bool
    undersized_rows: int
    oversized_rows: int
    null_bbox_rows: int
    #: Rows with NULL or empty geometry: nothing to compare, not a failure.
    absent_geometry_rows: int
    worst_shortfall: float | None
    worst_excess: float | None
    undersized_examples: list[dict]
    oversized_examples: list[dict]
    null_bbox_examples: list[int]
    issues: list[str]
    recommendations: list[str]


def _empty_report(skip_reason, bbox_column=None, geometry_column=None) -> BboxValueReport:
    """The report for a file with nothing to compare: a pass, with the reason."""
    return {
        "passed": True,
        "checked": False,
        "skip_reason": skip_reason,
        "bbox_column_name": bbox_column,
        "geometry_column": geometry_column,
        "total_rows": 0,
        "checked_rows": 0,
        "sampled": False,
        "undersized_rows": 0,
        "oversized_rows": 0,
        "null_bbox_rows": 0,
        "absent_geometry_rows": 0,
        "worst_shortfall": None,
        "worst_excess": None,
        "undersized_examples": [],
        "oversized_examples": [],
        "null_bbox_examples": [],
        "issues": [],
        "recommendations": [],
    }


def _relative_tolerance(schema_info, bbox_column: str) -> float:
    """The slack the bbox column's own storage type forces.

    A float32 covering cannot name a float64 coordinate, so comparing it at
    double precision would fail every file that stores one. Anything that is
    not float32 is held to the double-precision figure.
    """
    field_types = {
        str(child.get("type", "")).upper()
        for index, column in enumerate(schema_info)
        if column.get("name") == bbox_column
        for child in schema_direct_children(schema_info, index)
    }
    return _FLOAT32_RELATIVE_TOLERANCE if "FLOAT" in field_types else _FLOAT64_RELATIVE_TOLERANCE


def _geometry_expression(con, source: str, geometry_column: str) -> str | None:
    """A GEOMETRY-typed expression for the geometry column, or None if unreadable.

    DuckDB hands a GeoParquet column back either as native ``GEOMETRY`` or as
    the WKB ``BLOB`` it is stored as, so the column is asked which it is before
    being wrapped -- ``ST_GeomFromWKB`` binds only against the latter. A
    1.1-geoarrow file's nested geometry is neither, and there is nothing to
    compare against: those return None rather than a broken query.
    """
    quoted = quote_identifier(geometry_column)
    (column_type,) = con.execute(
        f"SELECT column_type FROM (DESCRIBE SELECT {quoted} FROM {source})"
    ).fetchone()
    upper = column_type.upper()
    if upper.startswith("GEOMETRY"):
        return quoted
    if "BLOB" in upper or "BINARY" in upper:
        # TRY so one unreadable WKB value is counted as a row with no geometry
        # rather than aborting the whole check.
        return f"TRY(ST_GeomFromWKB({quoted}))"
    return None


def _comparison_query(source: str, row_expr: str, geom_expr: str, bbox_expr: str, **parts) -> str:
    """The one query the whole check runs: per-row deltas, aggregated in DuckDB.

    Aggregated server-side rather than fetched row by row so ``all_rows=True``
    on a 7-million-row file stays a fixed amount of client memory. The offending
    rows come back through ``arg_max``, which keeps only the worst
    ``max_examples`` -- the rows most worth looking at, and a bounded list.
    """
    tolerance = parts["relative_tolerance"]
    floor = _ABSOLUTE_TOLERANCE_FLOOR
    limit = parts["max_examples"]
    undersized = "comparable AND shortfall_excess > 0"
    oversized = "comparable AND shortfall_excess <= 0 AND excess_excess > 0"
    null_bbox = "bbox_null AND NOT geom_absent"
    return f"""
    WITH sampled AS (
        SELECT {row_expr} AS row_index, {geom_expr} AS geom, {bbox_expr} AS box
        FROM {source}
        {parts["sample_clause"]}
    ),
    bounds AS (
        SELECT row_index,
               box IS NULL AS bbox_null,
               geom IS NULL OR ST_IsEmpty(geom) AS geom_absent,
               ST_XMin(geom) AS gxmin, ST_YMin(geom) AS gymin,
               ST_XMax(geom) AS gxmax, ST_YMax(geom) AS gymax,
               box.xmin AS bxmin, box.ymin AS bymin,
               box.xmax AS bxmax, box.ymax AS bymax
        FROM sampled
    ),
    deltas AS (
        SELECT row_index, bbox_null, geom_absent,
               NOT geom_absent AND NOT bbox_null AS comparable,
               greatest(bxmin - gxmin, gxmax - bxmax) AS short_x,
               greatest(bymin - gymin, gymax - bymax) AS short_y,
               greatest(gxmin - bxmin, bxmax - gxmax) AS over_x,
               greatest(gymin - bymin, bymax - gymax) AS over_y,
               greatest({floor!r}::DOUBLE, {tolerance!r}::DOUBLE
                        * greatest(abs(gxmin), abs(gxmax), abs(bxmin), abs(bxmax))) AS tol_x,
               greatest({floor!r}::DOUBLE, {tolerance!r}::DOUBLE
                        * greatest(abs(gymin), abs(gymax), abs(bymin), abs(bymax))) AS tol_y
        FROM bounds
    ),
    verdict AS (
        SELECT row_index, bbox_null, geom_absent, comparable,
               greatest(short_x, short_y) AS shortfall,
               greatest(over_x, over_y) AS excess,
               greatest(short_x - tol_x, short_y - tol_y) AS shortfall_excess,
               greatest(over_x - tol_x, over_y - tol_y) AS excess_excess
        FROM deltas
    )
    SELECT
        count(*) AS checked_rows,
        count(*) FILTER (WHERE geom_absent) AS absent_geometry_rows,
        count(*) FILTER (WHERE {null_bbox}) AS null_bbox_rows,
        count(*) FILTER (WHERE {undersized}) AS undersized_rows,
        count(*) FILTER (WHERE {oversized}) AS oversized_rows,
        max(shortfall) FILTER (WHERE {undersized}) AS worst_shortfall,
        max(excess) FILTER (WHERE {oversized}) AS worst_excess,
        arg_max({{'row': row_index, 'shortfall': shortfall}}, shortfall_excess, {limit})
            FILTER (WHERE {undersized}) AS undersized_examples,
        arg_max({{'row': row_index, 'excess': excess}}, excess_excess, {limit})
            FILTER (WHERE {oversized}) AS oversized_examples,
        arg_min(row_index, row_index, {limit}) FILTER (WHERE {null_bbox}) AS null_bbox_examples
    FROM verdict
    """


def _describe_findings(report: BboxValueReport) -> None:
    """Fill ``issues`` and ``recommendations`` from the counts, and set ``passed``."""
    checked = report["checked_rows"]
    issues = report["issues"]

    if report["undersized_rows"]:
        issues.append(
            f"{report['undersized_rows']:,} of {checked:,} checked rows have a bbox that "
            f"does not cover its geometry (worst shortfall {report['worst_shortfall']:.6g}) — "
            "a reader's spatial filter silently drops them"
        )
    if report["null_bbox_rows"]:
        issues.append(
            f"{report['null_bbox_rows']:,} of {checked:,} checked rows have a NULL bbox beside "
            "a non-NULL geometry — every covering-based filter drops them"
        )
    if report["oversized_rows"]:
        issues.append(
            f"{report['oversized_rows']:,} of {checked:,} checked rows have a bbox larger than "
            f"their geometry (worst excess {report['worst_excess']:.6g}) — readers over-fetch, "
            "but results stay correct"
        )

    report["passed"] = not (report["undersized_rows"] or report["null_bbox_rows"])
    if not report["passed"]:
        report["recommendations"].append(BBOX_REGENERATE_HINT)


def _scope_phrase(report: BboxValueReport) -> str:
    """How much of the file was compared, with a nudge when it was a sample."""
    phrase = f"{report['checked_rows']:,} of {report['total_rows']:,} rows"
    if report["sampled"]:
        phrase += " (random sample; --all-rows checks every row)"
    return phrase


def _examples_phrase(examples: list[dict], key: str, label: str) -> str:
    """The offending rows as ``row 12 (short by 1.5), row 7 (short by 0.2)``, worst first."""
    return ", ".join(f"row {item['row']} ({label} {item[key]:.6g})" for item in examples)


def _report_findings(report: BboxValueReport) -> None:
    """Print the section ``gpio check bbox`` shows for this comparison."""
    progress("\nBbox vs geometry:")
    scope = _scope_phrase(report)

    if report["passed"] and not report["oversized_rows"]:
        success(f"✓ {scope} checked: every bbox covers its geometry")
    elif report["passed"]:
        warn(f"⚠️  {scope} checked: {report['oversized_rows']:,} bbox values larger than needed")

    if report["undersized_rows"]:
        error(
            f"❌ {scope} checked: {report['undersized_rows']:,} where the bbox "
            "does not cover its geometry"
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
            f"❌ {report['null_bbox_rows']:,} checked rows have a NULL bbox beside a "
            "non-NULL geometry"
        )
        info(f"   First: {', '.join(f'row {row}' for row in report['null_bbox_examples'])}")
    if report["oversized_rows"]:
        warn(
            f"⚠️  {report['oversized_rows']:,} checked rows have a bbox larger than their "
            "geometry (over-fetch only; results stay correct)"
        )
        info(f"   Worst: {_examples_phrase(report['oversized_examples'], 'excess', 'over by')}")
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
) -> BboxValueReport:
    """Compare each row's bbox against its geometry's real envelope.

    Args:
        parquet_file: Path to the parquet file
        sample_rows: Rows to compare; 0 skips the comparison entirely. Ignored
            when ``all_rows`` is set, and ignored for a file that has fewer
            rows than this, which is compared whole.
        all_rows: Compare every row rather than a sample
        max_examples: Offending rows to name per category
        verbose: Print additional diagnostics
        quiet: Suppress all output (for multi-file batch mode)

    Returns:
        A :class:`BboxValueReport`. A file with no bbox column, no geometry
        column, or nothing requested comes back ``checked=False, passed=True``:
        the absence of a bbox column is a verdict ``check_bbox_structure``
        already reaches, and this check must not double-report it.
    """
    if not all_rows and sample_rows <= 0:
        return _empty_report("row comparison not requested")

    bbox_info = check_bbox_structure(parquet_file, verbose)
    bbox_column = bbox_info.get("bbox_column_name")
    if not bbox_column:
        return _empty_report("file has no bbox column to compare")

    schema_info = get_schema_info(parquet_file)
    geometry_column = find_primary_geometry_column(parquet_file, verbose)
    # Presence, not just a name: the detector falls back to the conventional
    # name, so a file with a bbox column and no geometry at all answers
    # "geometry" and the query would fail to bind.
    if not geometry_column or geometry_column not in {
        column.get("name") for column in root_schema_columns(schema_info)
    }:
        return _empty_report("file has no geometry column to compare", bbox_column)

    raw_url = resolve_file_url(parquet_file, verbose)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(parquet_file))
    try:
        report = _run_comparison(
            con,
            raw_url,
            schema_info,
            bbox_column,
            geometry_column,
            sample_rows=sample_rows,
            all_rows=all_rows,
            max_examples=max_examples,
            verbose=verbose,
        )
    finally:
        con.close()

    if not quiet and report["checked"]:
        _report_findings(report)
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
    verbose: bool,
) -> BboxValueReport:
    """Build and run the comparison, and turn its one row into a report."""
    source, row_expr = _row_source(schema_info, raw_url)
    geom_expr = _geometry_expression(con, source, geometry_column)
    if geom_expr is None:
        return _empty_report(
            "geometry column is not WKB or native GEOMETRY", bbox_column, geometry_column
        )

    (total_rows,) = con.execute(f"SELECT count(*) FROM {source}").fetchone()
    sampled = not all_rows and total_rows > sample_rows
    sample_clause = f"USING SAMPLE {sample_rows} ROWS" if sampled else ""

    query = _comparison_query(
        source,
        row_expr,
        geom_expr,
        quote_identifier(bbox_column),
        sample_clause=sample_clause,
        relative_tolerance=_relative_tolerance(schema_info, bbox_column),
        max_examples=max_examples,
    )
    if verbose:
        debug(f"Comparing bbox '{bbox_column}' against geometry '{geometry_column}'")
        debug(query)

    row = con.execute(query).fetchone()
    report: BboxValueReport = {
        "passed": True,
        "checked": True,
        "skip_reason": None,
        "bbox_column_name": bbox_column,
        "geometry_column": geometry_column,
        "total_rows": total_rows,
        "checked_rows": row[0],
        "sampled": sampled,
        "absent_geometry_rows": row[1],
        "null_bbox_rows": row[2],
        "undersized_rows": row[3],
        "oversized_rows": row[4],
        "worst_shortfall": row[5],
        "worst_excess": row[6],
        "undersized_examples": list(row[7] or []),
        "oversized_examples": list(row[8] or []),
        "null_bbox_examples": list(row[9] or []),
        "issues": [],
        "recommendations": [],
    }
    _describe_findings(report)
    return report
