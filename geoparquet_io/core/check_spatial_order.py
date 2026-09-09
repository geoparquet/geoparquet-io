#!/usr/bin/env python3


import math
import random as _random
from statistics import mean

from geoparquet_io.core.duckdb_utils import get_duckdb_connection, quote_identifier, sql_path
from geoparquet_io.core.file_utils import resolve_file_url
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import debug, progress
from geoparquet_io.core.remote import needs_httpfs

# Consecutive-pair bbox overlap. Kept as a reported statistic, no longer a
# verdict: on Hilbert-sorted data consecutive row groups are spatially adjacent
# by construction, so their boxes touch and this runs at ~1.0 for a PERFECTLY
# ordered file. Measured on ideal tilings: 0.75 at 13 row groups, 0.88 at 59,
# 0.96 at 589. It cannot tell "every row group covers the whole country" from
# "row groups tile the country perfectly but neighbours touch" (#755).
_OVERLAP_RATIO_THRESHOLD = 0.3

# Ordering verdict: the estimated skip rate as a fraction of what an ideal grid
# tiling of the same extent and row-group count achieves on the same queries.
# Relative because the achievable skip rate depends on the row-group count -- two
# row groups can never skip more than ~50%, 589 should skip ~98% -- so any
# absolute cutoff is wrong at one end or the other.
#
# 0.65 sits in a gap measured on real data, not only synthetic. Across 206 files
# with five or more row groups from every catalog in the Portolan registry, the
# efficiencies were 0.000 for one genuinely unsorted file, then a cluster of eight
# under-sorted files between 0.534 and 0.699, then nothing until 0.722, rising to a
# median of 0.979. Every file in that 0.53-0.70 cluster reached 0.87-0.97 after a
# re-sort, so the bar separates "could be fixed by sorting" from "already as good
# as its row-group count allows" rather than flagging the merely imperfect.
#
# The synthetic corpus agrees: the worst well-sorted Hilbert file scores 0.603 at
# five row groups, 0.759 at eight and 0.946 at fifty-nine (60 runs per count),
# while unsorted data scores 0.000 at every count.
_SKIP_RATE_EFFICIENCY_THRESHOLD = 0.7

# Below this many row groups the comparison is too noisy to fail a file on, so
# the verdict is withheld. Measured on the same well-sorted data, a PERFECTLY
# sorted file scores as low as 0.105 at two row groups and 0.295 at three (10th
# percentiles 0.479 and 0.455): a grid of two or three cells is a poor model of
# what a sort can do to clustered data, so a low score there says more about the
# reference than about the file. Five matches the floor Portolan's formats.md
# already sets for its footer check. The metrics are still computed and reported
# at every count -- only the verdict is withheld.
_MIN_VERDICT_ROW_GROUPS = 5

# Pushdown readiness verdict: ABSOLUTE, and deliberately a different question
# from ordering. "Is this sorted as well as it could be?" and "will queries
# actually prune well?" have different answers for a file with few row groups,
# and both are worth saying.
_SKIP_RATE_THRESHOLD = 0.5

_DEFAULT_NUM_SAMPLES = 20
_DEFAULT_QUERY_FRACTION = 0.1
_DEFAULT_SEED = 42


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


def _print_bbox_stats_results(ratio, overlap_count, total_pairs, passed):
    """Print bbox-stats results when running as standalone command."""
    progress("\nResults:")
    debug(f"Row group pairs analyzed: {total_pairs}")
    debug(f"Overlapping pairs: {overlap_count}")
    progress(f"Overlap ratio: {ratio:.2f}")

    # `passed` is the final verdict (overlap ratio plus the secondary locality
    # check), so the printed message cannot contradict the structured result
    if passed:
        progress("=> Data appears well spatially ordered.")
    else:
        progress("=> Data may benefit from spatial ordering (high row group overlap).")


def check_spatial_order_bbox_stats(
    parquet_file: str,
    verbose: bool = False,
    return_results: bool = False,
    quiet: bool = False,
    num_samples: int = _DEFAULT_NUM_SAMPLES,
    query_fraction: float = _DEFAULT_QUERY_FRACTION,
    seed: int = _DEFAULT_SEED,
    efficiency_threshold: float = _SKIP_RATE_EFFICIENCY_THRESHOLD,
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
    from geoparquet_io.core.duckdb_metadata import (
        get_per_row_group_bbox_stats,
        has_bbox_column,
    )

    has_bbox, bbox_col_name = has_bbox_column(parquet_file)
    if not has_bbox or not bbox_col_name:
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
        row_group_bboxes,
        parquet_file,
        verbose,
        return_results,
        quiet,
        method="bbox_stats",
        num_samples=num_samples,
        query_fraction=query_fraction,
        seed=seed,
        efficiency_threshold=efficiency_threshold,
    )


def _check_spatial_order_from_row_group_bboxes(
    row_group_bboxes: list[dict],
    parquet_file: str,
    verbose: bool = False,
    return_results: bool = False,
    quiet: bool = False,
    method: str = "native_geo_bbox",
    num_samples: int = _DEFAULT_NUM_SAMPLES,
    query_fraction: float = _DEFAULT_QUERY_FRACTION,
    seed: int = _DEFAULT_SEED,
    efficiency_threshold: float = _SKIP_RATE_EFFICIENCY_THRESHOLD,
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

    # The verdict is how well this layout prunes, relative to what its row-group
    # count allows -- not the consecutive-pair overlap above, which is ~1.0 for a
    # perfectly ordered file and so cannot decide anything (#755). Metrics are
    # computed for every file, not only suspect ones, so a passing file can show
    # how good it is rather than only that it escaped a flag.
    metrics: dict = {}
    if len(row_group_bboxes) >= 2:
        metrics = _spatial_locality_metrics(
            row_group_bboxes,
            num_samples=num_samples,
            query_fraction=query_fraction,
            seed=seed,
        )
    if len(row_group_bboxes) >= _MIN_VERDICT_ROW_GROUPS:
        passed = metrics["skip_rate_efficiency"] >= efficiency_threshold
    else:
        # Too few row groups to judge: the ideal-tiling reference is noisy enough
        # there that a well-sorted file can score badly, so measuring the layout
        # would be measuring the row-group count. The numbers are still reported.
        # Whether few row groups is a good LAYOUT is
        # check_spatial_pushdown_readiness's question, and it answers separately.
        passed = True

    avg_area_ratio: float | None = metrics.get("avg_bbox_area_ratio")
    avg_skip_rate: float | None = metrics.get("estimated_skip_rate")
    ideal_skip_rate: float | None = metrics.get("ideal_skip_rate")
    efficiency: float | None = metrics.get("skip_rate_efficiency")

    if verbose and metrics:
        debug(
            f"Locality: skip_rate={avg_skip_rate:.2%} of an achievable "
            f"{ideal_skip_rate:.2%} (efficiency {efficiency:.2f}), "
            f"area_ratio={avg_area_ratio:.4f}, consecutive overlap={ratio:.2f}"
        )

    issues = []
    recommendations = []
    if not passed:
        issues.append(
            f"Poor spatial ordering: queries can skip {avg_skip_rate:.0%} of row groups, "
            f"against {ideal_skip_rate:.0%} achievable with {len(row_group_bboxes)} row groups"
        )
        recommendations.append("Apply Hilbert spatial ordering for better query performance")

    if not quiet and not return_results and not verbose:
        _print_bbox_stats_results(ratio, overlap_count, total_pairs, passed)

    if return_results:
        return {
            "passed": passed,
            "ratio": ratio,
            "overlap_count": overlap_count,
            "total_pairs": total_pairs,
            "method": method,
            "issues": issues,
            "recommendations": recommendations,
            "fix_available": not passed,
            "estimated_skip_rate": avg_skip_rate,
            "ideal_skip_rate": ideal_skip_rate,
            "skip_rate_efficiency": efficiency,
            "avg_bbox_area_ratio": avg_area_ratio,
        }

    return ratio


def check_spatial_order(
    parquet_file: str,
    random_sample_size: int,
    limit_rows: int,
    verbose: bool,
    return_results: bool = False,
    quiet: bool = False,
    num_samples: int = _DEFAULT_NUM_SAMPLES,
    query_fraction: float = _DEFAULT_QUERY_FRACTION,
    seed: int = _DEFAULT_SEED,
    efficiency_threshold: float = _SKIP_RATE_EFFICIENCY_THRESHOLD,
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
    from geoparquet_io.core.duckdb_metadata import (
        get_per_row_group_native_geo_stats,
        has_bbox_column,
    )
    from geoparquet_io.core.logging_config import warn

    raw_url = resolve_file_url(parquet_file, verbose)

    # Try bbox-stats method first (faster)
    has_bbox, bbox_col_name = has_bbox_column(parquet_file)
    if has_bbox and bbox_col_name:
        if verbose:
            debug(f"Using bbox-stats method (bbox column: {bbox_col_name})")
        try:
            return check_spatial_order_bbox_stats(
                parquet_file,
                verbose=verbose,
                return_results=return_results,
                quiet=quiet,
                num_samples=num_samples,
                query_fraction=query_fraction,
                seed=seed,
                efficiency_threshold=efficiency_threshold,
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
                num_samples=num_samples,
                query_fraction=query_fraction,
                seed=seed,
                efficiency_threshold=efficiency_threshold,
            )
        except (ValueError, KeyError, IndexError) as e:
            if verbose:
                warn(f"Native geo_bbox method failed: {e}, falling back to sampling")
            # Fall through to sampling method

    # Fall back to sampling method
    if verbose or not quiet:
        warn(
            "No bbox column or native geo_bbox stats found - using slower sampling method. "
            "For faster checks, add bbox column with 'gpio add bbox' or use GeoParquet 2.0."
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


def _generate_sample_query_bboxes(
    extent: dict,
    num_samples: int = 10,
    query_fraction: float = 0.1,
    seed: int | None = None,
) -> list[dict]:
    """Generate random sample query bboxes within the data extent.

    Each sample covers approximately ``query_fraction`` of the extent in each
    dimension (so the area fraction is roughly query_fraction^2).

    Args:
        extent: Dict with xmin, ymin, xmax, ymax for the full data extent.
        num_samples: Number of sample bboxes to generate.
        query_fraction: Fraction of each dimension the query should span.
        seed: Optional random seed for reproducibility.

    Returns:
        List of bbox dicts with xmin, ymin, xmax, ymax.
    """
    rng = _random.Random(seed)  # nosec B311 - not used for security
    x_range = extent["xmax"] - extent["xmin"]
    y_range = extent["ymax"] - extent["ymin"]
    query_width = x_range * query_fraction
    query_height = y_range * query_fraction

    samples = []
    for _ in range(num_samples):
        x_start = rng.uniform(extent["xmin"], extent["xmax"] - query_width)  # nosec B311
        y_start = rng.uniform(extent["ymin"], extent["ymax"] - query_height)  # nosec B311
        samples.append(
            {
                "xmin": x_start,
                "ymin": y_start,
                "xmax": x_start + query_width,
                "ymax": y_start + query_height,
            }
        )
    return samples


def _compute_skip_rate_for_query(query_bbox: dict, row_group_bboxes: list[dict]) -> float:
    """Compute the fraction of row groups that can be skipped for a query bbox.

    A row group can be skipped if its bbox does not overlap with the query bbox.

    Args:
        query_bbox: The query bbox dict with xmin, ymin, xmax, ymax.
        row_group_bboxes: List of row group bbox dicts.

    Returns:
        Float between 0.0 and 1.0 representing the fraction skippable.
    """
    if not row_group_bboxes:
        return 0.0
    skipped = sum(1 for rg in row_group_bboxes if not _bboxes_overlap(query_bbox, rg))
    return skipped / len(row_group_bboxes)


def _ideal_grid_bboxes(extent: dict, num_row_groups: int) -> list[dict]:
    """The best row-group layout possible for this extent and row-group count.

    A near-square grid tiling: every box the same size, no overlap, no gaps. It is
    the reference the actual layout is judged against, so that "well ordered"
    means "as good as this row-group count allows" rather than a fixed number
    that only makes sense at one count (#755).

    A grid is the right reference because it is what a perfect space-filling-curve
    sort converges to. It is generous where the data is not uniform -- clustered
    data cannot tile evenly -- which is why the threshold is 0.5 rather than
    something near 1.0.
    """
    if num_row_groups <= 0:
        return []
    cols = math.ceil(math.sqrt(num_row_groups))
    rows = math.ceil(num_row_groups / cols)
    width = (extent["xmax"] - extent["xmin"]) / cols
    height = (extent["ymax"] - extent["ymin"]) / rows
    boxes = []
    for i in range(num_row_groups):
        row, col = divmod(i, cols)
        boxes.append(
            {
                "row_group_id": i,
                "xmin": extent["xmin"] + col * width,
                "xmax": extent["xmin"] + (col + 1) * width,
                "ymin": extent["ymin"] + row * height,
                "ymax": extent["ymin"] + (row + 1) * height,
            }
        )
    return boxes


def _spatial_locality_metrics(
    row_group_bboxes: list[dict],
    num_samples: int = _DEFAULT_NUM_SAMPLES,
    query_fraction: float = _DEFAULT_QUERY_FRACTION,
    seed: int = _DEFAULT_SEED,
) -> dict:
    """Measure how well this row-group layout supports pruning.

    One computation shared by the spatial-order check and
    ``check_spatial_pushdown_readiness``, which used to run it twice per CLI
    invocation and then answer with different halves of it.

    Uses only the row-group bounding boxes from the footer -- no data is read.

    Returns:
        Dict with ``avg_bbox_area_ratio``, ``estimated_skip_rate``,
        ``ideal_skip_rate`` and ``skip_rate_efficiency`` (the estimated rate as a
        fraction of the ideal; 1.0 means this layout prunes as well as a perfect
        grid tiling of the same row-group count).
    """
    extent = _compute_data_extent(row_group_bboxes)
    samples = _generate_sample_query_bboxes(
        extent, num_samples=num_samples, query_fraction=query_fraction, seed=seed
    )
    estimated = mean(_compute_skip_rate_for_query(s, row_group_bboxes) for s in samples)
    ideal_boxes = _ideal_grid_bboxes(extent, len(row_group_bboxes))
    ideal = mean(_compute_skip_rate_for_query(s, ideal_boxes) for s in samples)
    # ideal == 0 means no layout could skip anything for these queries (one row
    # group, or a degenerate extent), so there is nothing to fall short of.
    efficiency = estimated / ideal if ideal > 0 else 1.0
    return {
        "avg_bbox_area_ratio": _compute_avg_bbox_area_ratio(row_group_bboxes, extent),
        "estimated_skip_rate": estimated,
        "ideal_skip_rate": ideal,
        "skip_rate_efficiency": min(efficiency, 1.0),
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
    num_samples: int = _DEFAULT_NUM_SAMPLES,
    query_fraction: float = _DEFAULT_QUERY_FRACTION,
    seed: int = _DEFAULT_SEED,
) -> dict:
    """Check how well a file supports spatial filter pushdown.

    Evaluates whether the file has geo_bbox metadata per row group, measures
    spatial locality, and estimates what percentage of row groups a typical
    regional query could skip.

    Args:
        parquet_file: Path to the GeoParquet file.
        verbose: If True, log detailed progress.
        num_samples: Number of random sample queries to evaluate.
        query_fraction: Fraction of each dimension each sample query spans.
        seed: Random seed for reproducible sample queries.

    Returns:
        Dict with keys:
            has_geo_bbox (bool): Whether file has per-RG geo_bbox stats.
            num_row_groups (int): Number of row groups in the file.
            estimated_skip_rate (float): Average fraction of RGs skippable.
            avg_bbox_area_ratio (float): Average RG bbox area / total extent area.
            passed (bool): True if skip rate >= 0.5 (good pushdown readiness).
            issues (list[str]): Problems found.
            recommendations (list[str]): Suggestions for improvement.
    """
    from geoparquet_io.core.duckdb_metadata import (
        get_per_row_group_bbox_stats,
        has_bbox_column,
    )

    has_bbox, bbox_col_name = has_bbox_column(parquet_file)

    issues: list[str] = []
    recommendations: list[str] = []

    if not has_bbox or not bbox_col_name:
        if verbose:
            debug("No geo_bbox column found, pushdown not possible")
        issues.append(
            "File has no geo_bbox column. "
            "Spatial filter pushdown requires per-row-group bbox stats (GeoParquet 2.0+)."
        )
        recommendations.append("Add bbox column with 'gpio add bbox' and upgrade to GeoParquet 2.0")
        return {
            "has_geo_bbox": False,
            "num_row_groups": 0,
            "estimated_skip_rate": 0.0,
            "avg_bbox_area_ratio": 0.0,
            "passed": False,
            "issues": issues,
            "recommendations": recommendations,
        }

    if verbose:
        debug(f"Using bbox column: {bbox_col_name}")

    row_group_bboxes = get_per_row_group_bbox_stats(parquet_file, bbox_col_name)
    num_rgs = len(row_group_bboxes)

    if verbose:
        debug(f"Found {num_rgs} row groups with bbox stats")

    if num_rgs <= 1:
        if verbose:
            debug("Only 0 or 1 row groups, skip rate is trivially 0.0")
        return {
            "has_geo_bbox": True,
            "num_row_groups": num_rgs,
            "estimated_skip_rate": 0.0,
            "avg_bbox_area_ratio": 0.0,
            "passed": False,  # Can't skip any row groups with only 0-1 row groups
            "issues": ["Single row group provides no pushdown benefit"],
            "recommendations": ["Consider using smaller row groups for spatial queries"],
        }

    metrics = _spatial_locality_metrics(
        row_group_bboxes, num_samples=num_samples, query_fraction=query_fraction, seed=seed
    )
    avg_area_ratio = metrics["avg_bbox_area_ratio"]
    avg_skip_rate = metrics["estimated_skip_rate"]

    if verbose:
        debug(f"Average bbox area ratio: {avg_area_ratio:.4f}")
        debug(f"Estimated average skip rate: {avg_skip_rate:.2%}")

    # Deliberately absolute, unlike the ordering check's relative threshold: this
    # answers "will queries actually prune well?", which a two-row-group file
    # fails however perfectly it is sorted. The ordering check answers "is this
    # sorted as well as its row-group count allows?" and passes the same file.
    # Both are true and worth saying (#755).
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
        "num_row_groups": num_rgs,
        "estimated_skip_rate": avg_skip_rate,
        "ideal_skip_rate": metrics["ideal_skip_rate"],
        "skip_rate_efficiency": metrics["skip_rate_efficiency"],
        "avg_bbox_area_ratio": avg_area_ratio,
        "passed": passed,
        "issues": issues,
        "recommendations": recommendations,
    }
