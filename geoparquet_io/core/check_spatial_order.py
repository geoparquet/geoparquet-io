#!/usr/bin/env python3

"""Spatial-order check: does this file's row-group layout let a reader prune?

The verdict is the expected fraction of row groups a query window can skip,
relative to the same expectation for a full tiling of the extent into as many
cells, as defined by the Portolan spec (specs/portolan/formats.md, "Pruning
efficiency") and enforced by its validator.
"""

import math
from statistics import mean

from geoparquet_io.core.bbox_structure import _bbox_column_from_covering
from geoparquet_io.core.duckdb_metadata import (
    get_geo_metadata,
    get_per_row_group_bbox_stats,
    get_per_row_group_native_geo_stats,
    has_bbox_column,
)
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, quote_identifier, sql_path
from geoparquet_io.core.file_utils import resolve_file_url
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import debug, progress, warn
from geoparquet_io.core.remote import needs_httpfs

#: The spatial-order verdict, as the Portolan spec defines it
#: (specs/portolan/formats.md, "Pruning efficiency"): a layout passes when its
#: expected skip rate reaches this fraction of what a full tiling of the extent
#: into the same number of row groups achieves.
SPATIAL_ORDER_MIN_EFFICIENCY = 0.70

#: Below this many row groups the footer check does not decide (spec, "Footer
#: check"): the numbers are reported and the verdict is withheld.
SPATIAL_ORDER_MIN_ROW_GROUPS = 8

#: Pushdown readiness is absolute -- will queries actually prune? -- where the
#: ordering verdict above is relative to the row-group count.
_SKIP_RATE_THRESHOLD = 0.5

#: The query window is this fraction of the extent in each dimension.
_DEFAULT_QUERY_FRACTION = 0.1

#: A reference skip rate at or under this is zero: a box equal to the extent
#: evaluates to a hit probability of 1 up to an ulp either way.
_ZERO_SKIP_TOLERANCE = 1e-9

_WITHHELD_VERDICT = f"not judged below {SPATIAL_ORDER_MIN_ROW_GROUPS} row groups"


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


def _calculate_consecutive_avg(con, raw_url, geometry_column, row_limit, verbose):
    """Calculate average distance between consecutive features."""
    quoted_geom = quote_identifier(geometry_column)
    query = f"""
    WITH numbered AS (
        SELECT ROW_NUMBER() OVER () as id, {quoted_geom} as geom
        FROM {sql_path(raw_url)} {row_limit}
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


def _calculate_random_avg(con, raw_url, geometry_column, row_limit, random_sample_size, verbose):
    """Calculate average distance between random pairs of features."""
    quoted_geom = quote_identifier(geometry_column)
    query = f"""
    WITH sample AS (SELECT {quoted_geom} as geom FROM {sql_path(raw_url)} {row_limit}),
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
        progress(f"=> Spatial ordering {_WITHHELD_VERDICT}.")
    elif passed:
        progress("=> Data appears well spatially ordered.")
    else:
        progress("=> Data may benefit from spatial ordering (queries prune few row groups).")


def _locality_summary(metrics: dict) -> str:
    """One line with every number the verdict rests on, plus the area sum."""
    efficiency = metrics["skip_rate_efficiency"]
    efficiency_text = "undefined" if efficiency is None else f"{efficiency:.2f}"
    area_sum = metrics["bbox_area_sum"]
    area_text = "" if area_sum is None else f", area sum={area_sum:.2f}"
    return (
        f"Locality: skip_rate={metrics['estimated_skip_rate']:.2%} of an achievable "
        f"{metrics['ideal_skip_rate']:.2%} (efficiency {efficiency_text}){area_text}, "
        f"area_ratio={metrics['avg_bbox_area_ratio']:.4f}"
    )


def spatial_verdict_withheld(result: dict) -> bool:
    """Whether a spatial-order result reached no verdict at all.

    The one predicate every layer asks. ``passed`` in a result means "no
    failure found", so a consumer that only cares about failure needs nothing
    else; one that prints a check mark or counts outcomes asks this first.
    """
    return not result.get("judged", True)


def _spatial_order_verdict(
    num_row_groups: int, efficiency: float | None
) -> tuple[bool, bool, list[str]]:
    """The verdict: ``(passed, judged, warnings)``.

    ``judged`` is False below ``SPATIAL_ORDER_MIN_ROW_GROUPS`` row groups and
    when the efficiency is undefined; ``passed`` is then True, because no
    failure was found, and ``warnings`` says why there is no verdict and what
    would make the file judgeable.
    """
    if num_row_groups == 0:
        return True, False, ["Spatial ordering not judged: no row-group statistics"]
    if num_row_groups < SPATIAL_ORDER_MIN_ROW_GROUPS:
        return (
            True,
            False,
            [
                f"Spatial ordering {_WITHHELD_VERDICT} ({num_row_groups} in this file); "
                "smaller row groups (--row-group-size, 2,048 minimum) would make it judgeable"
            ],
        )
    if efficiency is None:
        return (
            True,
            False,
            [
                "Spatial ordering not judged: the efficiency is undefined for an extent no query can miss"
            ],
        )
    return efficiency >= SPATIAL_ORDER_MIN_EFFICIENCY, True, []


def _bbox_column_name(parquet_file: str) -> str | None:
    """The bbox column the covering metadata names, else the first conventional one.

    The covering is the authoritative pointer: a file can carry a stale or
    secondary bbox column earlier in its schema, or name its real one
    ``bounding_box``, and the name-suffix heuristic would read the wrong
    column -- or none -- for every verdict built on it.
    """
    covering = _bbox_column_from_covering(get_geo_metadata(parquet_file))
    if covering:
        return covering
    has_bbox, name = has_bbox_column(parquet_file)
    return name if has_bbox else None


def check_spatial_order_bbox_stats(
    parquet_file: str,
    verbose: bool = False,
    return_results: bool = False,
    quiet: bool = False,
) -> float | dict:
    """Check spatial ordering from the row-group statistics of the bbox column.

    Reads the footer only. The verdict is the pruning efficiency of the
    row-group boxes; see ``SPATIAL_ORDER_MIN_EFFICIENCY``.

    Args:
        parquet_file: Path to parquet file
        verbose: Print additional information
        return_results: If True, return structured results dict
        quiet: If True, suppress all output

    Returns:
        ratio (float) if return_results=False, or dict if return_results=True

    Raises:
        ValueError: no bbox column, or one without row-group statistics.
    """
    bbox_col_name = _bbox_column_name(parquet_file)
    if not bbox_col_name:
        raise ValueError(
            f"File {parquet_file} does not have a bbox column. "
            "Use the sampling-based method instead."
        )

    if verbose:
        debug(f"Using bbox column: {bbox_col_name}")

    row_group_bboxes = get_per_row_group_bbox_stats(parquet_file, bbox_col_name)
    if not row_group_bboxes:
        raise ValueError(f"bbox column '{bbox_col_name}' carries no row-group statistics")

    if verbose:
        debug(f"Analyzing {len(row_group_bboxes)} row groups with bbox statistics")

    return _check_spatial_order_from_row_group_bboxes(
        row_group_bboxes,
        parquet_file,
        verbose,
        return_results,
        quiet,
        method="bbox_stats",
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
        ratio (float) if return_results=False, or dict if return_results=True.
        In the dict ``passed`` means "no failure found" and ``judged`` whether a
        verdict was reached at all: below ``SPATIAL_ORDER_MIN_ROW_GROUPS`` row
        groups ``judged`` is False, ``passed`` is True, the numbers are still
        reported and ``warnings`` says why there is no verdict.
        ``num_row_groups`` counts the row groups that carry statistics.
    """
    if len(row_group_bboxes) <= 1:
        if verbose:
            debug("Only one or zero row groups - no consecutive pairs to compare")
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

    # The verdict is the expected skip rate relative to what this row-group
    # count allows -- not the consecutive-pair overlap above, which is ~1.0
    # for a perfectly ordered file and so cannot decide anything (#755).
    metrics: dict = {}
    if row_group_bboxes:
        metrics = _spatial_locality_metrics(row_group_bboxes)
    efficiency: float | None = metrics.get("skip_rate_efficiency")

    passed, judged, warnings = _spatial_order_verdict(len(row_group_bboxes), efficiency)

    avg_area_ratio: float | None = metrics.get("avg_bbox_area_ratio")
    avg_skip_rate: float | None = metrics.get("estimated_skip_rate")
    ideal_skip_rate: float | None = metrics.get("ideal_skip_rate")

    if verbose and metrics:
        debug(_locality_summary(metrics) + f", consecutive overlap={ratio:.2f}")

    issues = []
    recommendations = []
    if not passed:
        issues.append(
            f"Poor spatial ordering: queries can skip {avg_skip_rate:.0%} of row groups, "
            f"against {ideal_skip_rate:.0%} achievable with {len(row_group_bboxes)} row groups"
        )
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
            "num_row_groups": len(row_group_bboxes),
            "fix_available": judged and not passed,
            "estimated_skip_rate": avg_skip_rate,
            "ideal_skip_rate": ideal_skip_rate,
            "skip_rate_efficiency": efficiency,
            "avg_bbox_area_ratio": avg_area_ratio,
            "bbox_area_sum": metrics.get("bbox_area_sum"),
        }

    return ratio


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
    bbox_col_name = _bbox_column_name(parquet_file)
    if bbox_col_name:
        if verbose:
            debug(f"Using bbox-stats method (bbox column: {bbox_col_name})")
        try:
            return check_spatial_order_bbox_stats(
                parquet_file,
                verbose=verbose,
                return_results=return_results,
                quiet=quiet,
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
                native_geo_stats,
                parquet_file,
                verbose,
                return_results,
                quiet,
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
        row_limit = _get_row_limit_clause(con, raw_url, limit_rows, verbose)

        consecutive_avg = _calculate_consecutive_avg(
            con, raw_url, geometry_column, row_limit, verbose
        )
        random_avg = _calculate_random_avg(
            con, raw_url, geometry_column, row_limit, random_sample_size, verbose
        )

        ratio = consecutive_avg / random_avg if consecutive_avg and random_avg else None

        if not verbose and not quiet:
            _print_standalone_results(ratio, consecutive_avg, random_avg)

        if return_results:
            return _build_results_dict(ratio, consecutive_avg, random_avg)

        return ratio
    finally:
        con.close()


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


def _axis_hit_probability(
    lo: float, hi: float, ext_lo: float, ext_hi: float, window: float
) -> float:
    """Probability that a window of this width, placed uniformly, overlaps [lo, hi].

    The window's low edge is uniform on [ext_lo, ext_hi - window]; it overlaps
    the box when that edge lies in [lo - window, hi]. If the extent has no
    room for the window on this axis (zero width, or a window at least as wide
    as the extent) every placement hits every box.
    """
    span = ext_hi - ext_lo - window
    if span <= 0:
        return 1.0
    overlap = min(hi, ext_hi - window) - max(lo - window, ext_lo)
    # Clamped: a box spanning the extent evaluates to 1 up to an ulp either way.
    return min(1.0, max(0.0, overlap / span))


def _hit_probability(box: dict, extent: dict, query_fraction: float) -> float:
    """Probability that a uniformly placed query window overlaps this box."""
    width = (extent["xmax"] - extent["xmin"]) * query_fraction
    height = (extent["ymax"] - extent["ymin"]) * query_fraction
    return _axis_hit_probability(
        lo=box["xmin"], hi=box["xmax"], ext_lo=extent["xmin"], ext_hi=extent["xmax"], window=width
    ) * _axis_hit_probability(
        lo=box["ymin"], hi=box["ymax"], ext_lo=extent["ymin"], ext_hi=extent["ymax"], window=height
    )


def _expected_skip_rate(row_group_bboxes: list[dict], extent: dict, query_fraction: float) -> float:
    """Expected fraction of row groups a random query window can skip.

    The closed form of what a sample of windows estimates: the window's
    lower-left corner is uniform over the extent (less the window), and a row
    group is skipped when the window misses its box. Deterministic, O(n),
    footer-only, no seed. Shared with portolan-spec#188 and rashid#174.
    """
    hit = mean(_hit_probability(b, extent, query_fraction) for b in row_group_bboxes)
    return max(0.0, 1.0 - hit)


def _full_tiling_bboxes(extent: dict, num_row_groups: int) -> list[dict]:
    """The reference layout: ``num_row_groups`` cells covering the whole extent.

    Not the best layout possible -- clustered data beats it, which is why the
    efficiency is capped -- but the one every implementation judges against.

    A near-square grid of ``cols = ceil(sqrt(n))`` columns whose last row holds
    the remainder, stretched to the full width so nothing is left uncovered. A
    grid that leaves its spare cells empty lets windows in the gap skip every
    box, which inflated the reference by 13% at n=3 and 4% at n=5.
    """
    cols = math.ceil(math.sqrt(num_row_groups))
    rows = math.ceil(num_row_groups / cols)
    height = (extent["ymax"] - extent["ymin"]) / rows
    boxes = []
    for row in range(rows):
        cells = min(cols, num_row_groups - row * cols)
        width = (extent["xmax"] - extent["xmin"]) / cells
        for col in range(cells):
            boxes.append(
                {
                    "xmin": extent["xmin"] + col * width,
                    "xmax": extent["xmin"] + (col + 1) * width,
                    "ymin": extent["ymin"] + row * height,
                    "ymax": extent["ymin"] + (row + 1) * height,
                }
            )
    return boxes


def _spatial_locality_metrics(
    row_group_bboxes: list[dict],
    query_fraction: float = _DEFAULT_QUERY_FRACTION,
) -> dict:
    """The pruning-efficiency numbers for a row-group layout, from the footer only.

    Shared by the spatial-order check and ``check_spatial_pushdown_readiness``.
    ``skip_rate_efficiency`` is None when the reference itself skips nothing
    (one row group, or an extent with neither width nor height) and
    ``bbox_area_sum`` when the extent has no area: undefined, not zero.
    ``bbox_area_sum`` is the same expectation with a zero-size window; it keeps
    seeing oversized boxes after the efficiency saturates at high counts.
    """
    extent = _compute_data_extent(row_group_bboxes)
    estimated = _expected_skip_rate(row_group_bboxes, extent, query_fraction)
    ideal_boxes = _full_tiling_bboxes(extent, len(row_group_bboxes))
    ideal = _expected_skip_rate(ideal_boxes, extent, query_fraction)
    # A reference that skips nothing (one row group, or a window as large as
    # the extent) leaves nothing to fall short of: undefined, not zero. The
    # tolerance absorbs the ulp a box-equals-extent hit probability can carry.
    efficiency = min(estimated / ideal, 1.0) if ideal > _ZERO_SKIP_TOLERANCE else None
    avg_area_ratio = _compute_avg_bbox_area_ratio(row_group_bboxes, extent)
    extent_area = (extent["xmax"] - extent["xmin"]) * (extent["ymax"] - extent["ymin"])
    return {
        "avg_bbox_area_ratio": avg_area_ratio,
        # 0/0 on an extent with no area: undefined, never "vanishingly tight".
        "bbox_area_sum": avg_area_ratio * len(row_group_bboxes) if extent_area > 0 else None,
        "estimated_skip_rate": estimated,
        "ideal_skip_rate": ideal,
        "skip_rate_efficiency": efficiency,
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

    Evaluates whether the file has geo_bbox metadata per row group, measures
    spatial locality, and estimates what percentage of row groups a typical
    regional query could skip.

    Args:
        parquet_file: Path to the GeoParquet file.
        verbose: If True, log detailed progress.
        query_fraction: Fraction of each dimension the query window spans.

    Returns:
        Dict with keys:
            has_geo_bbox (bool): Whether file has per-RG bbox stats (a bbox
                column named by the covering, a conventional one, or native
                GeoParquet 2.0 statistics).
            num_row_groups (int): Number of row groups carrying statistics.
            estimated_skip_rate (float): Expected fraction of RGs skippable.
            avg_bbox_area_ratio (float): Average RG bbox area / total extent area.
            passed (bool): True if skip rate >= 0.5 (good pushdown readiness).
            issues (list[str]): Problems found.
            recommendations (list[str]): Suggestions for improvement.
    """

    bbox_col_name = _bbox_column_name(parquet_file)
    row_group_bboxes: list[dict] = []
    if bbox_col_name:
        if verbose:
            debug(f"Using bbox column: {bbox_col_name}")
        row_group_bboxes = get_per_row_group_bbox_stats(parquet_file, bbox_col_name)
    if not row_group_bboxes:
        geometry_column = find_primary_geometry_column(parquet_file, verbose)
        row_group_bboxes = get_per_row_group_native_geo_stats(parquet_file, geometry_column) or []

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
        metrics = _spatial_locality_metrics(row_group_bboxes, query_fraction=query_fraction)
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
