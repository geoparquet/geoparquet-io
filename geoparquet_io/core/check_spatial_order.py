#!/usr/bin/env python3


from math import ceil, isfinite, sqrt
from statistics import mean

import duckdb

from geoparquet_io.core.bbox_structure import check_bbox_structure
from geoparquet_io.core.check_verdict import (
    ISSUE_SEVERITY_KEY,
    SEVERITY_KEY,
    Verdict,
    could_not_run,
    is_could_not_run,
    result_verdict,
    worst,
)
from geoparquet_io.core.duckdb_metadata import (
    get_per_row_group_bbox_stats,
    get_per_row_group_native_geo_stats,
    get_per_row_group_weights,
)
from geoparquet_io.core.duckdb_utils import (
    disable_geoparquet_conversion,
    get_duckdb_connection,
    is_interrupt,
    sql_path,
)
from geoparquet_io.core.file_utils import resolve_file_url
from geoparquet_io.core.geometry_detection import (
    find_primary_geometry_column,
    resolve_geometry_read,
)
from geoparquet_io.core.logging_config import debug, progress, warn
from geoparquet_io.core.remote import needs_httpfs

# The ordering verdict, per the Portolan spec (portolan-sdi/portolan-spec#188):
# efficiency is the expected byte skip rate of a query window over what the
# same row-group count achieves on an ideal tiling. The window's centre falls
# in a row group with probability proportional to its rows and spans
# _DEFAULT_QUERY_FRACTION of each dimension of that group's box; each group
# weighs its compressed bytes. Both the bar and the floors are the spec's, so
# nothing on the CLI or API overrides them.
SPATIAL_ORDER_MIN_EFFICIENCY = 0.70
#: A pass needs this many row groups; a clear failure does not (the spec's
#: PORTO-FMT-050/044).
SPATIAL_ORDER_MIN_ROW_GROUPS = 8
#: Under this efficiency a file fails from two row groups up: boxes that each
#: cover most of the extent are plain at any count (PORTO-FMT-059).
SPATIAL_ORDER_OBVIOUS_FAIL_EFFICIENCY = 0.25
#: Under this many compressed bytes (summed over the row groups, footer-only)
#: nothing about order is judged: pruning saves a reader almost nothing
#: (PORTO-FMT-058).
SPATIAL_ORDER_MIN_FILE_BYTES = 5_000_000

_SKIP_RATE_THRESHOLD = 0.5
_DEFAULT_QUERY_FRACTION = 0.1
#: The windows the result reports read shares for, beside the verdict's 10%.
_REPORT_FRACTIONS = (0.01, _DEFAULT_QUERY_FRACTION, 0.25)
#: A reference skip rate at or under this is zero: one row group, or an extent
#: that is a single point. Float noise, not a layout.
_ZERO_SKIP_TOLERANCE = 1e-9

#: The check's name in a "could not run" line.
SPATIAL_ORDER_CHECK = "Spatial order check"


def _bboxes_overlap(bbox1: dict, bbox2: dict) -> bool:
    """Check if two bounding boxes overlap.

    Two bounding boxes overlap if they share any interior area.
    Boxes that only touch at edges or corners are not considered overlapping.

    Args:
        bbox1: First bbox dict with xmin, ymin, xmax, ymax
        bbox2: Second bbox dict with xmin, ymin, xmax, ymax

    Returns:
        True if bboxes overlap, False otherwise
    """
    # Boxes overlap if they overlap in BOTH X and Y dimensions
    # X overlap: bbox1.xmax > bbox2.xmin AND bbox2.xmax > bbox1.xmin
    # Y overlap: bbox1.ymax > bbox2.ymin AND bbox2.ymax > bbox1.ymin
    x_overlap = bbox1["xmax"] > bbox2["xmin"] and bbox2["xmax"] > bbox1["xmin"]
    y_overlap = bbox1["ymax"] > bbox2["ymin"] and bbox2["ymax"] > bbox1["ymin"]
    return x_overlap and y_overlap


def _calculate_consecutive_avg(con, raw_url, read, row_limit, verbose):
    """Calculate average distance between consecutive features.

    Rows holding no geometry (NULL or zero-length) are left out before the
    pairing, so they neither pair up nor fail the decode (#1082).
    """
    query = f"""
    WITH numbered AS (
        SELECT ROW_NUMBER() OVER () as id, {read.expr} as geom
        FROM (SELECT * FROM {sql_path(raw_url)} {row_limit})
        WHERE {read.present_sql}
    )
    SELECT AVG(ST_Distance(a.geom, b.geom)) as avg_dist
    FROM numbered a JOIN numbered b ON b.id = a.id + 1;
    """
    if verbose:
        progress("Calculating average distance between consecutive features...")
    result = con.execute(query).fetchone()
    avg = result[0] if result else None
    if verbose:
        debug(f"Average distance between consecutive features: {avg}")
    return avg


def _calculate_random_avg(con, raw_url, read, row_limit, random_sample_size, verbose):
    """Calculate average distance between random pairs of features."""
    query = f"""
    WITH sample AS (
        SELECT {read.expr} as geom
        FROM (SELECT * FROM {sql_path(raw_url)} {row_limit})
        WHERE {read.present_sql}
    ),
    random_pairs AS (
        SELECT a.geom as geom1, b.geom as geom2
        FROM (SELECT geom FROM sample ORDER BY random() LIMIT {random_sample_size}) a,
             (SELECT geom FROM sample ORDER BY random() LIMIT {random_sample_size}) b
        WHERE a.geom != b.geom
    )
    SELECT AVG(ST_Distance(geom1, geom2)) as avg_dist FROM random_pairs;
    """
    if verbose:
        progress(f"Calculating average distance between {random_sample_size} random pairs...")
    result = con.execute(query).fetchone()
    avg = result[0] if result else None
    if verbose:
        debug(f"Average distance between random features: {avg}")
    return avg


def _errored_results(reason: str) -> dict:
    """The verdict for a sampling check that could not read the geometry.

    A failure (:func:`~geoparquet_io.core.check_verdict.could_not_run`), not a
    fixable ordering problem. ``error`` carries the reason so a caller can tell
    "could not check" from "badly ordered".
    """
    return could_not_run(
        SPATIAL_ORDER_CHECK,
        reason,
        ratio=None,
        consecutive_avg=None,
        random_avg=None,
        method="sampling",
    )


def _build_results_dict(ratio, consecutive_avg, random_avg):
    """Build structured results dictionary for sampling method."""
    passed = ratio is not None and ratio < 0.5
    issues = []
    recommendations = []
    if ratio is not None and ratio >= 0.5:
        issues.append(f"Poor spatial ordering (ratio: {ratio:.2f})")
        recommendations.append("Apply Hilbert spatial ordering for better query performance")
    return {
        "passed": passed,
        "ratio": ratio,
        "consecutive_avg": consecutive_avg,
        "random_avg": random_avg,
        "method": "sampling",
        "issues": issues,
        "recommendations": recommendations,
        "judged": ratio is not None,
        "fix_available": not passed,
    }


def _get_row_limit_clause(con, raw_url, limit_rows, verbose):
    """Determine row limit clause based on total rows."""
    total_rows = con.execute(f"SELECT COUNT(*) FROM {sql_path(raw_url)}").fetchone()[0]
    if verbose:
        debug(f"Total rows in file: {total_rows:,}")

    if total_rows > limit_rows:
        if verbose:
            debug(f"Limiting analysis to first {limit_rows:,} rows")
        return f"LIMIT {limit_rows}"
    return ""


def _print_standalone_results(ratio, consecutive_avg, random_avg):
    """Print results when running as standalone command (not from check_all)."""
    progress("\nResults:")
    debug(f"Average distance between consecutive features: {consecutive_avg}")
    debug(f"Average distance between random features: {random_avg}")
    progress(f"Ratio (consecutive / random): {ratio}")

    if ratio is not None and ratio < 0.5:
        progress("=> Data seems strongly spatially clustered.")
    elif ratio is not None:
        progress("=> Data might not be strongly clustered (or is partially clustered).")


def _print_bbox_stats_results(ratio, overlap_count, total_pairs, passed, judged, metrics):
    """Print bbox-stats results when running as standalone command."""
    progress("\nResults:")
    debug(f"Row group pairs analyzed: {total_pairs}")
    debug(f"Overlapping pairs: {overlap_count}")
    progress(f"Overlap ratio: {ratio:.2f}")
    if metrics:
        progress(_locality_summary(metrics))

    # `passed` is the final verdict, so the printed message cannot contradict
    # the structured result -- and a withheld verdict is not a pass.
    if not judged:
        progress("=> Spatial ordering not judged.")
    elif passed:
        progress("=> Data appears well spatially ordered.")
    else:
        progress("=> Data may benefit from spatial ordering (queries prune few row groups).")


def _locality_summary(metrics: dict) -> str:
    """One line with every number the verdict rests on, the absolute cost first.

    The spec's reporting rules (PORTO-FMT-060/061): lead with the share of the
    bytes a typical query reads, then the achievable rate, the efficiency, the
    1% and 25% window profile, and the area sum.
    """
    efficiency = metrics["skip_rate_efficiency"]
    efficiency_text = "undefined" if efficiency is None else f"{efficiency:.2f}"
    area_sum = metrics["bbox_area_sum"]
    area_text = "" if area_sum is None else f", area sum={area_sum:.2f}"
    profile = metrics.get("read_profile") or ()
    profile_text = ""
    if len(profile) == len(_REPORT_FRACTIONS):
        profile_text = f"; windows at 1% and 25% read {profile[0]:.0%} and {profile[2]:.0%}"
    return (
        f"A typical query reads {1.0 - metrics['estimated_skip_rate']:.0%} of the bytes, "
        f"against {1.0 - metrics['ideal_skip_rate']:.0%} for an ideal tiling "
        f"(efficiency {efficiency_text}){area_text}{profile_text}"
    )


def spatial_verdict_withheld(result: dict) -> bool:
    """Whether a spatial-order result reached no verdict at all.

    The one predicate every layer asks. ``passed`` in a result means "no
    failure found", so a consumer that only cares about failure needs nothing
    else; one that prints a check mark or counts outcomes asks this first.
    """
    return not result.get("judged", True)


def _spatial_order_verdict(
    num_row_groups: int, efficiency: float | None, file_bytes: float | None
) -> tuple[bool, bool, list[str]]:
    """The verdict: ``(passed, judged, warnings)``, per the spec's floors.

    Under the 5 MB size floor nothing is judged (PORTO-FMT-058). At eight or
    more row groups the 0.70 bar decides (PORTO-FMT-050). Below eight, a file
    under the 0.25 obvious-fail bar still fails (PORTO-FMT-059); otherwise the
    pass is withheld (PORTO-FMT-044) and ``warnings`` says why there is no
    verdict.
    """
    if file_bytes is not None and file_bytes < SPATIAL_ORDER_MIN_FILE_BYTES:
        return (
            True,
            False,
            [
                f"Spatial ordering not judged: the row groups hold "
                f"{file_bytes / 1_000_000:.1f} MB, under the "
                f"{SPATIAL_ORDER_MIN_FILE_BYTES / 1_000_000:.0f} MB floor at which "
                "ordering matters for reads"
            ],
        )
    if num_row_groups == 0:
        return True, False, ["Spatial ordering not judged: no row-group statistics"]
    if num_row_groups >= SPATIAL_ORDER_MIN_ROW_GROUPS:
        if efficiency is None:
            return (
                True,
                False,
                [
                    "Spatial ordering not judged: the efficiency is undefined "
                    "for an extent no query can miss"
                ],
            )
        return efficiency >= SPATIAL_ORDER_MIN_EFFICIENCY, True, []
    if (
        num_row_groups > 1
        and efficiency is not None
        and efficiency < SPATIAL_ORDER_OBVIOUS_FAIL_EFFICIENCY
    ):
        return False, True, []
    return (
        True,
        False,
        [
            f"Spatial ordering not judged below {SPATIAL_ORDER_MIN_ROW_GROUPS} row groups "
            f"({num_row_groups} in this file); smaller row groups "
            "(--row-group-size, 2,048 minimum) would make it judgeable"
        ],
    )


def check_spatial_order_bbox_stats(
    parquet_file: str,
    verbose: bool = False,
    return_results: bool = False,
    quiet: bool = False,
) -> float | dict:
    """Check spatial ordering using row group bbox statistics.

    This method is faster than sampling because it only reads row group metadata
    instead of actual geometry data. It checks if consecutive row groups have
    overlapping bounding boxes, which indicates poor spatial ordering.

    Args:
        parquet_file: Path to parquet file
        verbose: Print additional information
        return_results: If True, return structured results dict
        quiet: If True, suppress all output

    Returns:
        ratio (float) if return_results=False, or dict if return_results=True
    """

    bbox_col_name = check_bbox_structure(parquet_file, verbose).get("bbox_column_name")
    if not bbox_col_name:
        raise ValueError(
            f"File {parquet_file} does not have a bbox column. "
            "Use the sampling-based method instead."
        )

    if verbose:
        debug(f"Using bbox column: {bbox_col_name}")

    row_group_bboxes = get_per_row_group_bbox_stats(parquet_file, bbox_col_name)

    if verbose:
        debug(f"Analyzing {len(row_group_bboxes)} row groups")

    return _check_spatial_order_from_row_group_bboxes(
        row_group_bboxes, parquet_file, verbose, return_results, quiet, method="bbox_stats"
    )


def _check_spatial_order_from_row_group_bboxes(
    row_group_bboxes: list[dict],
    parquet_file: str,
    verbose: bool = False,
    return_results: bool = False,
    quiet: bool = False,
    method: str = "native_geo_bbox",
) -> float | dict:
    """Check spatial ordering from row group bboxes.

    Shared logic for checking spatial order from pre-fetched row group bboxes.
    Used by both bbox column method and native geo_bbox stats method.

    Args:
        row_group_bboxes: List of dicts with row_group_id, xmin, ymin, xmax, ymax
        parquet_file: Path to parquet file (for logging)
        verbose: Print additional information
        return_results: If True, return structured results dict
        quiet: If True, suppress all output
        method: Method label for results dict ("bbox_stats" or "native_geo_bbox")

    Returns:
        ratio (float) if return_results=False, or dict if return_results=True
    """
    if len(row_group_bboxes) <= 1:
        if verbose:
            debug("Only one or zero row groups - assuming well ordered")
        ratio = 0.0
        overlap_count = 0
        total_pairs = 0
    else:
        overlap_count = 0
        for i in range(len(row_group_bboxes) - 1):
            bbox1 = row_group_bboxes[i]
            bbox2 = row_group_bboxes[i + 1]
            if _bboxes_overlap(bbox1, bbox2):
                overlap_count += 1
                if verbose:
                    debug(f"Row groups {bbox1['row_group_id']} and {bbox2['row_group_id']} overlap")

        total_pairs = len(row_group_bboxes) - 1
        ratio = overlap_count / total_pairs if total_pairs > 0 else 0.0

        if verbose:
            debug(f"Overlapping pairs: {overlap_count}/{total_pairs}")

    # The verdict is the expected byte skip rate relative to what this
    # row-group count allows -- not the consecutive-pair overlap above, which
    # is ~1.0 for a perfectly ordered file and so cannot decide anything
    # (#755). Row and byte weights come from the footer; a bbox whose stats
    # are not finite cannot be trusted, so the file falls back to "no
    # statistics" rather than a silent pass.
    finite_bboxes = _finite_bboxes(row_group_bboxes)
    weights = _row_group_weights(parquet_file, finite_bboxes)
    metrics: dict = {}
    if finite_bboxes and len(finite_bboxes) == len(row_group_bboxes):
        metrics = _spatial_locality_metrics(
            finite_bboxes, rows=weights["rows"], sizes=weights["bytes"]
        )
    efficiency: float | None = metrics.get("skip_rate_efficiency")

    passed, judged, warnings = _spatial_order_verdict(
        len(finite_bboxes) if metrics else 0, efficiency, weights["file_bytes"]
    )

    avg_area_ratio: float | None = metrics.get("avg_bbox_area_ratio")
    avg_skip_rate: float | None = metrics.get("estimated_skip_rate")
    ideal_skip_rate: float | None = metrics.get("ideal_skip_rate")

    if verbose and metrics:
        debug(_locality_summary(metrics) + f", consecutive overlap={ratio:.2f}")

    issues = []
    recommendations = []
    if not passed:
        issues.append(
            f"Poor spatial ordering: a typical query reads {1.0 - avg_skip_rate:.0%} of the "
            f"file's bytes, against {1.0 - ideal_skip_rate:.0%} achievable with "
            f"{len(finite_bboxes)} row groups"
        )
        largest = metrics.get("largest_row_groups") or []
        if largest:
            named = ", ".join(str(i) for i in largest)
            issues.append(f"The largest row-group boxes against the extent: {named}")
        recommendations.append("Apply Hilbert spatial ordering for better query performance")

    if not quiet and not return_results and not verbose:
        _print_bbox_stats_results(ratio, overlap_count, total_pairs, passed, judged, metrics)

    if return_results:
        return {
            "passed": passed,
            "ratio": ratio,
            "overlap_count": overlap_count,
            "total_pairs": total_pairs,
            "method": method,
            "issues": issues,
            "recommendations": recommendations,
            "warnings": warnings,
            "judged": judged,
            "num_row_groups": len(finite_bboxes),
            "file_bytes": weights["file_bytes"],
            "fix_available": judged and not passed,
            "estimated_skip_rate": avg_skip_rate,
            "ideal_skip_rate": ideal_skip_rate,
            "skip_rate_efficiency": efficiency,
            "avg_bbox_area_ratio": avg_area_ratio,
            "bbox_area_sum": metrics.get("bbox_area_sum"),
            "read_profile": metrics.get("read_profile"),
            "largest_row_groups": metrics.get("largest_row_groups"),
        }

    return ratio


def _finite_bboxes(row_group_bboxes: list[dict]) -> list[dict]:
    """The bboxes whose four corners are finite numbers.

    A NaN or infinite statistic would propagate through every min/max and
    silently score an efficiency of 1.00; dropping to the no-statistics path
    is honest instead.
    """
    return [
        b
        for b in row_group_bboxes
        if all(isfinite(float(b[k])) for k in ("xmin", "ymin", "xmax", "ymax"))
    ]


def _row_group_weights(parquet_file: str, bboxes: list[dict]) -> dict:
    """Rows and compressed bytes aligned with ``bboxes``, plus the file total.

    ``file_bytes`` sums every row group, not only the boxed ones, because the
    spec's size floor is about the file. A footer that cannot be read leaves
    uniform weights and no size, which judges the layout but never the floor.
    """
    try:
        per_group = get_per_row_group_weights(parquet_file)
    except Exception:
        per_group = {}
    if not per_group:
        uniform = [1.0] * len(bboxes)
        return {"rows": uniform, "bytes": list(uniform), "file_bytes": None}
    rows = [max(per_group.get(b["row_group_id"], {}).get("rows", 0.0), 1.0) for b in bboxes]
    sizes = [max(per_group.get(b["row_group_id"], {}).get("bytes", 0.0), 1.0) for b in bboxes]
    return {
        "rows": rows,
        "bytes": sizes,
        "file_bytes": float(sum(group["bytes"] for group in per_group.values())),
    }


def check_spatial_order(
    parquet_file: str,
    random_sample_size: int,
    limit_rows: int,
    verbose: bool,
    return_results: bool = False,
    quiet: bool = False,
) -> float | dict | None:
    """Check if a GeoParquet file is spatially ordered.

    Automatically detects if the file has a bbox column (GeoParquet 2.0+) and uses
    the faster bbox-stats method. Falls back to sampling method for older files.

    Args:
        parquet_file: Path to parquet file
        random_sample_size: Number of rows in each random sample (sampling method only)
        limit_rows: Max number of rows to analyze (sampling method only)
        verbose: Print additional information
        return_results: If True, return structured results dict
        quiet: If True, suppress all output (for multi-file batch mode)

    Returns:
        ratio (float) if return_results=False, or dict if return_results=True
    """

    raw_url = resolve_file_url(parquet_file, verbose)

    # Try bbox-stats method first (faster)
    bbox_col_name = check_bbox_structure(parquet_file, verbose).get("bbox_column_name")
    if bbox_col_name:
        if verbose:
            debug(f"Using bbox-stats method (bbox column: {bbox_col_name})")
        try:
            return check_spatial_order_bbox_stats(
                parquet_file, verbose=verbose, return_results=return_results, quiet=quiet
            )
        except (ValueError, KeyError, IndexError) as e:
            # ValueError: Invalid bbox column structure
            # KeyError: Missing expected bbox fields (xmin, ymin, xmax, ymax)
            # IndexError: Empty or malformed row group stats
            if verbose:
                warn(f"Bbox-stats method failed: {e}, falling back to sampling")
            # Fall through to try native geo_bbox stats

    # Try native geo_bbox stats (GeoParquet 2.0 / parquet-geo-only)
    geometry_column = find_primary_geometry_column(parquet_file, verbose)
    native_geo_stats = get_per_row_group_native_geo_stats(parquet_file, geometry_column)
    if native_geo_stats:
        if verbose:
            debug(f"Using native geo_bbox stats ({len(native_geo_stats)} row groups)")
        try:
            return _check_spatial_order_from_row_group_bboxes(
                native_geo_stats, parquet_file, verbose, return_results, quiet
            )
        except (ValueError, KeyError, IndexError) as e:
            if verbose:
                warn(f"Native geo_bbox method failed: {e}, falling back to sampling")
            # Fall through to sampling method

    # Fall back to sampling method
    if verbose or not quiet:
        warn(
            "No bbox column or native geo_bbox stats found - using slower sampling method, "
            "which compares consecutive-feature distances and does not measure row-group "
            "pruning. Add a bbox column with 'gpio add bbox' so the footer-based check can run."
        )

    geometry_column = find_primary_geometry_column(parquet_file, verbose)
    if verbose:
        debug(f"Using geometry column: {geometry_column}")
        debug("Using sampling method")

    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(parquet_file))
    try:
        disable_geoparquet_conversion(con)
        averages = _sample_distances(
            con, raw_url, geometry_column, limit_rows, random_sample_size, verbose
        )
    except duckdb.Error as e:
        if is_interrupt(e):
            # The user stopping the run (Ctrl-C), not a verdict on the file.
            raise
        averages = f"could not read geometry column {geometry_column!r} ({e})"
    finally:
        con.close()

    if isinstance(averages, str):
        # A caller asking for results presents the verdict itself.
        if return_results:
            return _errored_results(averages)
        warn(f"Cannot check spatial order of {parquet_file}, {averages}")
        return None

    consecutive_avg, random_avg = averages
    ratio = consecutive_avg / random_avg if consecutive_avg and random_avg else None

    if not verbose and not quiet:
        _print_standalone_results(ratio, consecutive_avg, random_avg)

    if return_results:
        return _build_results_dict(ratio, consecutive_avg, random_avg)

    return ratio


def _sample_distances(con, raw_url, geometry_column, limit_rows, random_sample_size, verbose):
    """(consecutive avg, random avg), or a sentence saying why the column cannot be read."""
    read = resolve_geometry_read(con, raw_url, geometry_column)
    if not read.readable:
        return read.problem
    row_limit = _get_row_limit_clause(con, raw_url, limit_rows, verbose)
    consecutive_avg = _calculate_consecutive_avg(con, raw_url, read, row_limit, verbose)
    random_avg = _calculate_random_avg(con, raw_url, read, row_limit, random_sample_size, verbose)
    return consecutive_avg, random_avg


def _compute_data_extent(row_group_bboxes: list[dict]) -> dict:
    """Compute the total spatial extent across all row group bboxes.

    Args:
        row_group_bboxes: List of dicts with xmin, ymin, xmax, ymax keys.

    Returns:
        Dict with xmin, ymin, xmax, ymax for the full extent.

    Raises:
        ValueError: If row_group_bboxes is empty.
    """
    if not row_group_bboxes:
        raise ValueError("No row group bboxes provided")
    return {
        "xmin": min(b["xmin"] for b in row_group_bboxes),
        "ymin": min(b["ymin"] for b in row_group_bboxes),
        "xmax": max(b["xmax"] for b in row_group_bboxes),
        "ymax": max(b["ymax"] for b in row_group_bboxes),
    }


def _axis_overlap_share(a0: float, a1: float, g0: float, g1: float, half: float) -> float:
    """Chance a centre coordinate drawn uniformly on ``[a0, a1]`` lands in the
    target ``[g0, g1]`` grown by ``half`` on each side.

    A span with no width puts all its probability on ``a0`` (the spec's
    single-coordinate rule), which is also what makes a zero-size axis judge
    on the other axis alone.
    """
    g0, g1 = g0 - half, g1 + half
    if a1 <= a0:
        return 1.0 if g0 <= a0 <= g1 else 0.0
    return min(1.0, max(0.0, (min(a1, g1) - max(a0, g0)) / (a1 - a0)))


def _weighted_skip_rate(
    boxes: list[dict],
    rows: list[float] | None = None,
    sizes: list[float] | None = None,
    query_fraction: float = _DEFAULT_QUERY_FRACTION,
) -> float:
    """Expected share of the file's bytes a query window skips.

    The window's centre falls in box ``i`` with probability ``rows_i / N``,
    uniform within the box, and the window spans ``query_fraction`` of each
    dimension of that box, so the query works where the data is and at its
    scale: one far outlier can neither hide a shuffled file behind a stretched
    extent nor fail a sorted one. Each box then counts by its share of the
    bytes, because what a reader saves is bytes, not a count of row groups.
    A closed form over the boxes: O(n^2), footer only, no seed, the same
    number from every implementation (the spec's PORTO-FMT-050).
    """
    if not boxes:
        return 0.0
    rows = rows if rows is not None else [1.0] * len(boxes)
    sizes = sizes if sizes is not None else list(rows)
    total_rows = float(sum(rows))
    total_size = float(sum(sizes))
    read = 0.0
    for j, target in enumerate(boxes):
        hit = 0.0
        for i, source in enumerate(boxes):
            half_w = query_fraction * (source["xmax"] - source["xmin"]) / 2.0
            half_h = query_fraction * (source["ymax"] - source["ymin"]) / 2.0
            fx = _axis_overlap_share(
                source["xmin"], source["xmax"], target["xmin"], target["xmax"], half_w
            )
            fy = _axis_overlap_share(
                source["ymin"], source["ymax"], target["ymin"], target["ymax"], half_h
            )
            hit += rows[i] / total_rows * fx * fy
        read += sizes[j] / total_size * hit
    rate = 1.0 - read
    # Accumulated float dust on an exact 0 or 1 stays an exact 0 or 1.
    if abs(rate) <= _ZERO_SKIP_TOLERANCE:
        return 0.0
    return min(1.0, max(0.0, rate))


def _full_tiling_bboxes(extent: dict, num_row_groups: int) -> list[dict]:
    """The reference layout: the near-square grid the spec pins (PORTO-FMT-053).

    ``ceil(sqrt(n))`` columns, ``ceil(n / cols)`` rows, laid row by row, with
    the last row's cells stretched so it too spans the full width -- the grid
    covers the whole extent, every cell filled. Scored with uniform rows and
    bytes.
    """
    if num_row_groups <= 0:
        return []
    cols = ceil(sqrt(num_row_groups))
    rows = ceil(num_row_groups / cols)
    height = (extent["ymax"] - extent["ymin"]) / rows
    cells: list[dict] = []
    for row in range(rows):
        in_row = cols if row < rows - 1 else num_row_groups - cols * (rows - 1)
        width = (extent["xmax"] - extent["xmin"]) / in_row
        for col in range(in_row):
            cells.append(
                {
                    "xmin": extent["xmin"] + col * width,
                    "ymin": extent["ymin"] + row * height,
                    "xmax": extent["xmin"] + (col + 1) * width,
                    "ymax": extent["ymin"] + (row + 1) * height,
                }
            )
    return cells


def _spatial_locality_metrics(
    row_group_bboxes: list[dict],
    rows: list[float] | None = None,
    sizes: list[float] | None = None,
    query_fraction: float = _DEFAULT_QUERY_FRACTION,
) -> dict:
    """The pruning-efficiency numbers for a row-group layout, from the footer only.

    Shared by the spatial-order check and ``check_spatial_pushdown_readiness``.
    ``skip_rate_efficiency`` is None when the reference itself skips nothing
    (one row group, or an extent that is a single point) and ``bbox_area_sum``
    when the extent has no area: undefined, not zero. ``read_profile`` carries
    the expected read share at the 1%, 10% and 25% windows, and
    ``largest_row_groups`` the ids of the up-to-three boxes largest against
    the extent, so a failure can say whether one outlier or the whole file is
    at fault (PORTO-FMT-060/061).
    """
    extent = _compute_data_extent(row_group_bboxes)
    estimated = _weighted_skip_rate(row_group_bboxes, rows, sizes, query_fraction)
    ideal_boxes = _full_tiling_bboxes(extent, len(row_group_bboxes))
    ideal = _weighted_skip_rate(ideal_boxes, query_fraction=query_fraction)
    # A reference that skips nothing leaves nothing to fall short of:
    # undefined, not zero. The tolerance absorbs the ulp a box-equals-extent
    # hit probability can carry.
    efficiency = min(estimated / ideal, 1.0) if ideal > _ZERO_SKIP_TOLERANCE else None
    avg_area_ratio = _compute_avg_bbox_area_ratio(row_group_bboxes, extent)
    extent_area = (extent["xmax"] - extent["xmin"]) * (extent["ymax"] - extent["ymin"])

    def _area(box: dict) -> float:
        return (box["xmax"] - box["xmin"]) * (box["ymax"] - box["ymin"])

    ranked = sorted(row_group_bboxes, key=_area, reverse=True)[:3]
    return {
        "avg_bbox_area_ratio": avg_area_ratio,
        # 0/0 on an extent with no area: undefined, never "vanishingly tight".
        "bbox_area_sum": avg_area_ratio * len(row_group_bboxes) if extent_area > 0 else None,
        "estimated_skip_rate": estimated,
        "ideal_skip_rate": ideal,
        "skip_rate_efficiency": efficiency,
        "read_profile": tuple(
            1.0 - _weighted_skip_rate(row_group_bboxes, rows, sizes, fraction)
            for fraction in _REPORT_FRACTIONS
        ),
        "largest_row_groups": [box.get("row_group_id") for box in ranked],
    }


def _compute_avg_bbox_area_ratio(row_group_bboxes: list[dict], extent: dict) -> float:
    """Compute average ratio of row group bbox area to total extent area.

    Lower values mean tighter row group bboxes (better spatial locality).

    Args:
        row_group_bboxes: List of row group bbox dicts.
        extent: The total data extent dict.

    Returns:
        Average area ratio (0.0 to 1.0). Returns 0.0 if extent area is zero.
    """
    extent_area = (extent["xmax"] - extent["xmin"]) * (extent["ymax"] - extent["ymin"])
    if extent_area <= 0 or not row_group_bboxes:
        return 0.0
    ratios = [
        (rg["xmax"] - rg["xmin"]) * (rg["ymax"] - rg["ymin"]) / extent_area
        for rg in row_group_bboxes
    ]
    return mean(ratios)


def check_spatial_pushdown_readiness(
    parquet_file: str,
    verbose: bool = False,
    query_fraction: float = _DEFAULT_QUERY_FRACTION,
) -> dict:
    """Check how well a file supports spatial filter pushdown.

    Evaluates whether the file has per-row-group bbox statistics (a covering
    bbox column or native GeoParquet 2.0 statistics), measures spatial
    locality, and estimates what share of the file a typical regional query
    skips. Deterministic: the same closed form the ordering check uses.

    Args:
        parquet_file: Path to the GeoParquet file.
        verbose: If True, log detailed progress.
        query_fraction: Fraction of each dimension the query window spans.

    Returns:
        Dict with keys:
            has_geo_bbox (bool): Whether file has per-RG bbox stats.
            num_row_groups (int): Number of row groups carrying statistics.
            estimated_skip_rate (float): Expected fraction skippable.
            avg_bbox_area_ratio (float): Average RG bbox area / extent area.
            passed (bool): True if skip rate >= 0.5 (good pushdown readiness).
            issues (list[str]): Problems found.
            recommendations (list[str]): Suggestions for improvement.
    """

    bbox_col_name = check_bbox_structure(parquet_file, verbose).get("bbox_column_name")
    row_group_bboxes: list[dict] = []
    if bbox_col_name:
        if verbose:
            debug(f"Using bbox column: {bbox_col_name}")
        row_group_bboxes = get_per_row_group_bbox_stats(parquet_file, bbox_col_name)
    if not row_group_bboxes:
        geometry_column = find_primary_geometry_column(parquet_file, verbose)
        row_group_bboxes = get_per_row_group_native_geo_stats(parquet_file, geometry_column) or []
    row_group_bboxes = _finite_bboxes(row_group_bboxes)

    if not row_group_bboxes:
        if verbose:
            debug("No geo_bbox column found, pushdown not possible")
        return {
            "has_geo_bbox": False,
            "num_row_groups": 0,
            "estimated_skip_rate": 0.0,
            "avg_bbox_area_ratio": 0.0,
            "passed": False,
            "issues": [
                "File has no geo_bbox column. "
                "Spatial filter pushdown requires per-row-group bbox stats (GeoParquet 2.0+)."
            ],
            "recommendations": [
                "Add bbox column with 'gpio add bbox' and upgrade to GeoParquet 2.0"
            ],
        }

    if verbose:
        debug(f"Found {len(row_group_bboxes)} row groups with bbox stats")
    metrics: dict = {}
    if len(row_group_bboxes) > 1:
        weights = _row_group_weights(parquet_file, row_group_bboxes)
        metrics = _spatial_locality_metrics(
            row_group_bboxes,
            rows=weights["rows"],
            sizes=weights["bytes"],
            query_fraction=query_fraction,
        )
    return pushdown_readiness_from_locality(len(row_group_bboxes), metrics)


def pushdown_readiness_from_locality(num_row_groups: int, metrics: dict) -> dict:
    """The pushdown-readiness verdict from locality numbers already computed.

    ``metrics`` is a ``_spatial_locality_metrics`` result, or any dict carrying
    its keys -- the spatial-order payload does, so ``check spatial`` reaches
    this verdict from the same numbers without a second footer read.
    """
    if num_row_groups <= 1:
        return {
            "has_geo_bbox": True,
            "num_row_groups": num_row_groups,
            "estimated_skip_rate": 0.0,
            "avg_bbox_area_ratio": 0.0,
            "passed": False,  # Can't skip any row groups with only 0-1 row groups
            "issues": ["Single row group provides no pushdown benefit"],
            "recommendations": ["Consider using smaller row groups for spatial queries"],
        }

    avg_area_ratio = metrics["avg_bbox_area_ratio"]
    avg_skip_rate = metrics["estimated_skip_rate"]
    issues: list[str] = []
    recommendations: list[str] = []

    # Absolute, unlike the ordering verdict: see _SKIP_RATE_THRESHOLD.
    passed = avg_skip_rate >= _SKIP_RATE_THRESHOLD

    if not passed:
        issues.append(
            f"Low spatial filter pushdown efficiency (estimated skip rate: {avg_skip_rate:.0%})"
        )
        recommendations.append(
            "Apply Hilbert spatial ordering with 'gpio sort hilbert' to improve pushdown"
        )

    if avg_area_ratio > 0.5:
        issues.append(
            f"Row group bboxes are large relative to data extent (avg ratio: {avg_area_ratio:.2f})"
        )
        recommendations.append(
            "Spatially sorting and re-partitioning may produce tighter row group bboxes"
        )

    return {
        "has_geo_bbox": True,
        "num_row_groups": num_row_groups,
        "estimated_skip_rate": avg_skip_rate,
        "ideal_skip_rate": metrics["ideal_skip_rate"],
        "skip_rate_efficiency": metrics["skip_rate_efficiency"],
        "avg_bbox_area_ratio": avg_area_ratio,
        "bbox_area_sum": metrics["bbox_area_sum"],
        "passed": passed,
        "issues": issues,
        "recommendations": recommendations,
    }


_NATIVE_GEO_FILE_TYPES = ("geoparquet_v2", "parquet_geo_only")


def pushdown_verdict(pushdown: dict, *, native_geo: bool) -> Verdict:
    """How much :func:`check_spatial_pushdown_readiness`'s finding matters.

    Advice where it is advice: a multi-row-group file whose row groups a
    regional query cannot skip, or a 1.x file with no bbox column to skip on,
    is a ``WARNING``. Not where it is not: a file with native geo types carries
    per-row-group statistics without a bbox column (``check bbox`` tells such
    a file to *drop* one), and a single row group has nothing to skip -- its
    size is ``check row-group``'s question. Both are ``PASS``, shown as
    information.
    """
    if not pushdown.get("has_geo_bbox"):
        return Verdict.PASS if native_geo else Verdict.WARNING
    if pushdown.get("num_row_groups", 0) <= 1 or pushdown.get("passed"):
        return Verdict.PASS
    return Verdict.WARNING


def with_pushdown(parquet_file: str, spatial: dict, verbose: bool = False) -> dict:
    """``spatial`` (a :func:`check_spatial_order` result) with pushdown readiness folded in.

    ``gpio check spatial`` judges both: the pushdown result is kept under
    ``"pushdown"`` with its own severity, and when it is a warning its issues
    join the result's, labelled as warnings, so ``--strict`` sees what stdout
    marks ``⚠``. A spatial check that could not run is returned unchanged; a
    pushdown measurement that cannot be taken is left out (``None``) rather
    than guessed.
    """
    from geoparquet_io.core.file_type import detect_geoparquet_file_type

    if is_could_not_run(spatial):
        return spatial
    try:
        pushdown = check_spatial_pushdown_readiness(parquet_file, verbose=verbose)
        file_type = detect_geoparquet_file_type(parquet_file)["file_type"]
    except Exception as e:
        if is_interrupt(e):
            raise
        debug(f"Pushdown readiness could not be measured: {e}")
        return {**spatial, "pushdown": None}
    verdict = pushdown_verdict(pushdown, native_geo=file_type in _NATIVE_GEO_FILE_TYPES)
    pushdown = {**pushdown, SEVERITY_KEY: verdict.label}
    merged = {**spatial, "pushdown": pushdown}
    if verdict is Verdict.PASS:
        return merged
    issues = list(pushdown.get("issues", []))
    merged["issues"] = [*spatial.get("issues", []), *issues]
    merged[ISSUE_SEVERITY_KEY] = {
        **(spatial.get(ISSUE_SEVERITY_KEY) or {}),
        **dict.fromkeys(issues, verdict.label),
    }
    merged["recommendations"] = [
        *spatial.get("recommendations", []),
        *pushdown.get("recommendations", []),
    ]
    merged[SEVERITY_KEY] = worst([result_verdict(spatial), verdict]).label
    return merged
