#!/usr/bin/env python3
"""Combined spatial query optimization check.

Evaluates five factors that affect spatial query performance:
1. Native geo types (v2.0 or parquet-geo-only)
2. Per-row-group geo bbox statistics
3. Spatial sorting (e.g. Hilbert curve)
4. Appropriate row group size for spatial queries
5. Efficient compression (ZSTD)
"""

from geoparquet_io.core.file_type import detect_geoparquet_file_type
from geoparquet_io.core.logging_config import error, info, progress, success, warn


def _score_to_level(score):
    """Convert a numeric score (0-5) to an optimization level string.

    Args:
        score: Number of checks that passed (0-5)

    Returns:
        One of 'fully_optimized', 'partially_optimized', or 'not_optimized'
    """
    if score >= 5:
        return "fully_optimized"
    if score >= 3:
        return "partially_optimized"
    return "not_optimized"


def _check_native_geo_types(parquet_file, verbose=False):
    """Check whether the file uses native Parquet geo types (v2.0 or parquet-geo-only).

    Returns:
        dict with 'passed' (bool) and 'detail' (str)
    """
    file_info = detect_geoparquet_file_type(parquet_file, verbose)
    has_native = file_info.get("has_native_geo_types", False)

    if has_native:
        file_type = file_info["file_type"]
        label = "GeoParquet 2.0" if file_type == "geoparquet_v2" else "Parquet geo-only"
        return {"passed": True, "detail": f"Uses native Parquet geo types ({label})"}

    version = file_info.get("geo_version") or "unknown"
    return {
        "passed": False,
        "detail": f"No native Parquet geo types (version {version})",
    }


def _check_geo_bbox_stats(parquet_file, verbose=False):
    """Check whether per-row-group geo bbox statistics are present.

    Returns:
        dict with 'passed' (bool) and 'detail' (str)
    """
    from geoparquet_io.core.metadata_utils import has_parquet_geo_row_group_stats

    stats_info = has_parquet_geo_row_group_stats(parquet_file)
    if stats_info["has_stats"]:
        return {"passed": True, "detail": "Row group geo bbox statistics present"}
    return {
        "passed": False,
        "detail": "No per-row-group geo bbox statistics found",
    }


def _check_spatial_sorting(parquet_file, verbose=False):
    """Check whether data appears spatially sorted.

    Uses the bbox-stats method when available, falls back to sampling.

    Returns:
        dict with 'passed' (bool) and 'detail' (str)
    """
    from geoparquet_io.core.check_spatial_order import check_spatial_order

    try:
        result = check_spatial_order(
            parquet_file,
            random_sample_size=100,
            limit_rows=100000,
            verbose=verbose,
            return_results=True,
            quiet=True,
        )
        # check_spatial_order with return_results=True always returns a dict
        assert isinstance(result, dict)
        ratio = result["ratio"]
        ratio_str = f" (ratio: {ratio:.2f})" if ratio is not None else ""
        if result["passed"]:
            return {"passed": True, "detail": f"Data appears spatially sorted{ratio_str}"}
        return {"passed": False, "detail": f"Data is not spatially sorted{ratio_str}"}
    except Exception:
        return {"passed": False, "detail": "Could not determine spatial ordering"}


def _check_row_group_size(parquet_file, verbose=False):
    """Check whether row group size is appropriate for spatial queries (10k-50k rows).

    Returns:
        dict with 'passed' (bool) and 'detail' (str)
    """
    from geoparquet_io.core.check_parquet_structure import (
        SPATIAL_ROW_COUNT_RANGE,
        get_row_group_stats,
    )

    stats = get_row_group_stats(parquet_file)
    avg_rows = stats["avg_rows_per_group"]
    total_size_mb = stats["total_size"] / (1024 * 1024)

    # Small files with a single row group are always fine
    if total_size_mb < 64 and stats["num_groups"] == 1:
        return {
            "passed": True,
            "detail": f"Small file ({total_size_mb:.1f} MB), single row group is appropriate",
        }

    spatial_low, spatial_high = SPATIAL_ROW_COUNT_RANGE
    # The band is imported rather than repeated: its docstring names this
    # function as one of the things it governs, which was only true by
    # coincidence while the numbers were spelled out here as well.
    return {
        "passed": spatial_low <= avg_rows <= spatial_high,
        "detail": (
            f"Average {avg_rows:,.0f} rows per group "
            f"(optimal {spatial_low // 1000}k-{spatial_high // 1000}k for spatial queries)"
        ),
    }


def _check_compression(parquet_file, verbose=False):
    """Check whether geometry column uses ZSTD compression.

    Returns:
        dict with 'passed' (bool) and 'detail' (str)
    """
    from geoparquet_io.core.check_parquet_structure import (
        _NO_COMPRESSION_INFO,
        check_compression,
    )

    result = check_compression(parquet_file, verbose=False, return_results=True, quiet=True)
    if result is None:
        return {"passed": False, "detail": "No geometry column found"}

    if result.get("geometry_column") and result.get("current_compression") is None:
        # No row groups, so no codec to report -- don't claim ZSTD (#823).
        return {"passed": True, "detail": _NO_COMPRESSION_INFO}

    if result.get("passed"):
        return {
            "passed": True,
            "detail": f"ZSTD compression on geometry column '{result.get('geometry_column')}'",
        }

    current = result.get("current_compression", "unknown")
    return {
        "passed": False,
        "detail": f"{current} compression on geometry column (ZSTD recommended)",
    }


_RECOMMENDATION_MAP = {
    "native_geo_types": "Convert to GeoParquet 2.0 for native geo types: gpio convert geoparquet --geoparquet-version 2.0",
    "geo_bbox_stats": "Use GeoParquet 2.0 or add bbox column: gpio add bbox",
    "spatial_sorting": "Apply Hilbert spatial sorting: gpio sort hilbert",
    "row_group_size": "Re-partition with 10k-50k rows per group for spatial queries",
    "compression": "Re-compress geometry with ZSTD: gpio convert geoparquet --compression zstd",
}


def check_optimization(parquet_file, verbose=False, return_results=False, quiet=False):
    """Run combined spatial query optimization check.

    Evaluates five factors that affect spatial query performance:
    1. Native geo types present (v2.0 or parquet-geo-only)
    2. Per-row-group geo bbox statistics present
    3. Spatial sorting detected (Hilbert or similar)
    4. Row group size appropriate for spatial queries (10k-50k rows)
    5. ZSTD compression on geometry column

    Args:
        parquet_file: Path to parquet file
        verbose: Print additional information
        return_results: If True, return structured results dict
        quiet: If True, suppress all output

    Returns:
        dict if return_results=True, containing:
            - passed: bool (True only when all 5 checks pass)
            - score: int (0-5, number of passing checks)
            - total_checks: int (always 5)
            - level: str ('fully_optimized', 'partially_optimized', 'not_optimized')
            - checks: dict of sub-check results
            - issues: list of issue descriptions
            - recommendations: list of recommendations
    """
    checks = {
        "native_geo_types": _check_native_geo_types(parquet_file, verbose),
        "geo_bbox_stats": _check_geo_bbox_stats(parquet_file, verbose),
        "spatial_sorting": _check_spatial_sorting(parquet_file, verbose),
        "row_group_size": _check_row_group_size(parquet_file, verbose),
        "compression": _check_compression(parquet_file, verbose),
    }

    score = sum(1 for c in checks.values() if c["passed"])
    total_checks = len(checks)
    level = _score_to_level(score)
    passed = score == total_checks

    issues = [c["detail"] for c in checks.values() if not c["passed"]]
    recommendations = [_RECOMMENDATION_MAP[name] for name, c in checks.items() if not c["passed"]]

    if not quiet:
        _print_results(checks, score, total_checks, level, issues, recommendations)

    if return_results:
        return {
            "passed": passed,
            "score": score,
            "total_checks": total_checks,
            "level": level,
            "checks": checks,
            "issues": issues,
            "recommendations": recommendations,
        }


def _print_results(checks, score, total_checks, level, issues, recommendations):
    """Print optimization check results using logging helpers."""
    progress("\nSpatial Query Optimization Check:")
    progress(f"Score: {score}/{total_checks}")

    level_labels = {
        "fully_optimized": "Fully optimized for spatial queries",
        "partially_optimized": "Partially optimized, improvements possible",
        "not_optimized": "Not optimized for spatial queries",
    }

    if level == "fully_optimized":
        success(f"  {level_labels[level]}")
    elif level == "partially_optimized":
        warn(f"  {level_labels[level]}")
    else:
        error(f"  {level_labels[level]}")

    progress("\nChecklist:")
    for name, check in checks.items():
        label = name.replace("_", " ").title()
        if check["passed"]:
            success(f"  [pass] {label}: {check['detail']}")
        else:
            warn(f"  [fail] {label}: {check['detail']}")

    if recommendations:
        progress("\nRecommendations:")
        for rec in recommendations:
            info(f"  - {rec}")
