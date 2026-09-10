#!/usr/bin/env python3


from enum import Enum

from geoparquet_io.core.common import check_bbox_structure, detect_geoparquet_file_type, format_size
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import error, info, progress, success, warn
from geoparquet_io.core.metadata_utils import has_parquet_geo_row_group_stats
from geoparquet_io.core.parquet_writer import DEFAULT_SORT_ROW_GROUP_ROWS

#: What a file with no row groups can be told about its compression: nothing.
#: Shared with ``check_optimization`` so both checks word it the same way (#823).
_NO_COMPRESSION_INFO = "No compression information available (file has no row groups)"

#: The band ``check row-group`` passes or fails a file on.
#:
#: It is deliberately wide, because a *verdict* has to accept the layouts the
#: writers people actually use produce at their own defaults: DuckDB writes
#: 122,880 rows per group, gpio's own general write default lands at 100,352
#: (100,000 rounded up to a multiple of 2,048), and ``gpio sort`` writes 49,152.
#: A pass/fail band that excludes all of those is reporting on the ecosystem
#: rather than on the file in front of it.
#:
#: Be honest about how firm the endpoints are: that argument justifies an upper
#: bound somewhere *above* DuckDB's 122,880, not 200,000 specifically. 200,000
#: is the value this check has always used, kept deliberately so no published
#: file changes status -- inherited, not derived. Only SPATIAL_ROW_COUNT_RANGE
#: below has a measurement behind it.
GENERAL_ROW_COUNT_RANGE = (10_000, 200_000)

#: The narrower band that makes *spatial* filters prune, which the check gives
#: as advice rather than as a verdict.
#:
#: A row group is the unit a spatial predicate skips, so fewer rows per group
#: means tighter per-group bounding boxes and less of the file read. Measured on
#: nine published catalogue files (#775), moving from 100,000 to 50,000 rows per
#: group cut the share of the file a query window covering 10% of each dimension
#: must read from 43-100% down to 10-28%. Nothing measures the top of
#: GENERAL_ROW_COUNT_RANGE the same way, so where the two bands disagree this is
#: the one with evidence behind it -- it is what ``gpio sort`` aims at, what
#: ``gpio check optimization`` scores (it imports this constant rather than
#: repeating the numbers), and what docs/guide/check.md and docs/guide/sort.md
#: quote.
#:
#: Both endpoints are stated in requested rows, and no file can have either of
#: them: the Parquet writer emits row groups in whole 2,048-row vectors. That
#: is the writer's job to reconcile, not this band's -- ``gpio sort`` snaps its
#: request to a whole vector (``align_to_writer_vector``), which maps 10,000 to
#: 10,240 and 50,000 to 49,152, both inside the band. Before #961 it asked for
#: 50,000 and the writer rounded that *up* to 51,200, so ``check optimization``
#: failed a file ``gpio sort`` had just written. The band did not move; the
#: request did.
#:
#: ``align_to_writer_vector`` keeps its own copy of this band's top, because
#: this module imports ``parquet_writer`` and so it cannot import back.
#: ``tests/test_sort_row_group_default.py`` asserts the two agree.
#:
#: The two bands are not rival answers to one question (#795): a file can sit
#: inside the general band and still prune badly, so the messages below say
#: which band they are talking about instead of calling one of them "optimal"
#: full stop.
SPATIAL_ROW_COUNT_RANGE = (10_000, 50_000)


class CheckProfile(str, Enum):
    """
    Profiles for checking parquet file structure
    based on specific use cases

    Attributes:
        web: Parquet file will be queried from the browser directly
    """

    web = "web"


def get_row_group_stats(parquet_file):
    """
    Get basic row group statistics from a parquet file.

    Returns:
        dict: Statistics including:
            - num_groups: Number of row groups
            - total_rows: Total number of rows
            - avg_rows_per_group: Average rows per group
            - total_size: Total file size in bytes
            - avg_group_size: Average group size in bytes
    """
    from geoparquet_io.core.duckdb_metadata import get_row_group_stats_summary

    return get_row_group_stats_summary(parquet_file)


def assess_row_group_size(
    avg_group_size_bytes, total_size_bytes, profile: CheckProfile | None = None
):
    """
    Assess if row group size is optimal.

    Returns:
        tuple: (status, message, color) where status is one of:
            - "optimal"
            - "suboptimal"
            - "poor"
    """
    avg_group_size_mb = avg_group_size_bytes / (1024 * 1024)
    total_size_mb = total_size_bytes / (1024 * 1024)

    if total_size_mb < 64:
        return "optimal", "Row group size is appropriate for small file", "green"

    if profile == CheckProfile.web:
        if avg_group_size_mb < 1.5:
            return (
                "suboptimal",
                "Row group size may be excessively small for queries directly from a web frontend",
                "yellow",
            )
        elif avg_group_size_mb > 128 and avg_group_size_mb <= 256:
            return (
                "suboptimal",
                "Row group size may be excessively large for queries directly from a web frontend",
                "yellow",
            )
        elif avg_group_size_mb > 256:
            return (
                "poor",
                "Row group size is too large for queries directly from a web frontend",
                "red",
            )
        else:
            return (
                "optimal",
                "Row group size could be appropriate for queries directly from a web frontend",
                "green",
            )

    if 64 <= avg_group_size_mb <= 256:
        return "optimal", "Row group size is optimal (64-256 MB)", "green"
    elif 32 <= avg_group_size_mb < 64 or 256 < avg_group_size_mb <= 512:
        return (
            "suboptimal",
            "Row group size is suboptimal. Recommended size is 64-256 MB",
            "yellow",
        )
    else:
        return (
            "poor",
            "Row group size is outside recommended range. Target 64-256 MB for best performance",
            "red",
        )


def _is_small_single_group(total_size_bytes=None, num_groups=None) -> bool:
    """A sub-64 MB file already written as one row group.

    Any row count is fine there and no rewrite can improve it -- splitting one
    group of a small file only costs metadata -- so both the verdict and the
    ``--fix`` trigger exempt it, from this one definition.
    """
    if total_size_bytes is None or num_groups is None:
        return False
    return total_size_bytes / (1024 * 1024) < 64 and num_groups == 1


def row_count_fix_available(avg_rows, total_size_bytes=None, num_groups=None) -> bool:
    """Whether ``check row-group --fix`` has anything to do to this file.

    Deliberately **not** ``assess_row_count(...) != "optimal"``, which is what
    it used to be. The two answer different questions and #972 is the gap
    between them:

    * the *verdict* is ``GENERAL_ROW_COUNT_RANGE``, settled that way on purpose
      (#795, #958) -- a file laid out the way mainstream writers lay files out
      is not wrong, and calling it wrong would fail most published GeoParquet;
    * ``--fix`` writes ``DEFAULT_SORT_ROW_GROUP_ROWS``, which comes from
      ``SPATIAL_ROW_COUNT_RANGE`` -- the narrower band, and the one
      ``check optimization`` scores.

    Triggering off the verdict meant a 100,352-row-group file was "optimal", so
    ``gpio check row-group --fix`` printed "No fix needed" while
    ``gpio check optimization`` on the same file printed ``[fail] Row Group
    Size`` -- #972's transcript, which survived the write-side half of the fix.
    It also made the outcome depend on which subcommand you asked: ``check
    spatial --fix``, ``check compression --fix`` and ``check bbox --fix`` each
    forced a rewrite for their own reasons and so fixed the row groups by
    accident, while the subcommand named after the problem did not.

    So the trigger follows the band ``--fix`` writes into. The verdict does not
    move.

    Args:
        avg_rows: Average rows per row group
        total_size_bytes: Total file size in bytes (optional)
        num_groups: Number of row groups (optional)

    Returns:
        True if a rewrite would move the file into SPATIAL_ROW_COUNT_RANGE
    """
    if _is_small_single_group(total_size_bytes, num_groups):
        return False
    spatial_low, spatial_high = SPATIAL_ROW_COUNT_RANGE
    return not (spatial_low <= avg_rows <= spatial_high)


def assess_row_count(avg_rows, total_size_bytes=None, num_groups=None):
    """
    Assess if average row count per group is optimal.

    Args:
        avg_rows: Average rows per row group
        total_size_bytes: Total file size in bytes (optional, for small file leniency)
        num_groups: Number of row groups (optional, for single group leniency)

    Returns:
        tuple: (status, message, color) where status is one of:
            - "optimal"
            - "suboptimal"
            - "poor"

    Note:
        The verdict is GENERAL_ROW_COUNT_RANGE; SPATIAL_ROW_COUNT_RANGE is
        narrower and is advice. A file inside the first but outside the second
        is still "optimal" -- it is laid out the way mainstream writers lay
        files out -- but the message says so rather than claiming the file has
        nothing left to gain, which is what made the verdict contradict the
        guidelines printed under it (#795).
    """
    general_low, general_high = GENERAL_ROW_COUNT_RANGE
    spatial_low, spatial_high = SPATIAL_ROW_COUNT_RANGE
    general_band = f"{general_low:,}-{general_high:,}"
    spatial_band = f"{spatial_low:,}-{spatial_high:,}"

    # For small files with a single row group, any row count is fine
    if _is_small_single_group(total_size_bytes, num_groups):
        return "optimal", "Row count is appropriate for small file", "green"

    if avg_rows < 2000:
        return (
            "poor",
            f"Row count per group is very low. Target {general_band} rows per group",
            "red",
        )
    elif avg_rows > 1000000:
        return (
            "poor",
            f"Row count per group is very high. Target {general_band} rows per group",
            "red",
        )
    elif general_low <= avg_rows <= general_high:
        if avg_rows > spatial_high:
            return (
                "optimal",
                f"Row count per group is optimal for general use ({general_band}); "
                f"spatial queries prune more with {spatial_band} rows per group",
                "green",
            )
        return "optimal", "Row count per group is optimal", "green"
    else:
        return (
            "suboptimal",
            f"Row count per group is outside recommended range ({general_band})",
            "yellow",
        )


def get_compression_info(parquet_file, column_name=None):
    """
    Get compression information for specified column(s).

    Returns:
        dict: Mapping of column names to their compression algorithms
    """
    from geoparquet_io.core.duckdb_metadata import (
        get_compression_info as duckdb_get_compression_info,
    )

    return duckdb_get_compression_info(parquet_file, column_name)


def check_row_groups(
    parquet_file,
    verbose=False,
    return_results=False,
    quiet=False,
    profile: CheckProfile | None = None,
):
    """Check row group optimization and print results.

    Args:
        parquet_file: Path to parquet file
        verbose: Print additional information
        return_results: If True, return structured results dict instead of only printing
        quiet: If True, suppress all output (for multi-file batch mode)
        profile: Check row groups for specific use case

    Returns:
        dict if return_results=True, containing:
            - passed: bool
            - stats: dict with file statistics
            - size_status: str (optimal/suboptimal/poor)
            - row_status: str (optimal/suboptimal/poor)
            - issues: list of issue descriptions
            - recommendations: list of recommendations
    """
    stats = get_row_group_stats(parquet_file)

    size_status, size_message, size_color = assess_row_group_size(
        stats["avg_group_size"], stats["total_size"], profile=profile
    )
    row_status, row_message, row_color = assess_row_count(
        stats["avg_rows_per_group"], stats["total_size"], stats["num_groups"]
    )

    # Build results dict
    # Pass if row count is optimal (size guidelines are secondary)
    passed = row_status == "optimal"
    issues = []
    recommendations = []

    # Only report size issues if row count is also problematic
    # (Row count is what we optimize for; size is just a guideline)
    if size_status != "optimal" and row_status != "optimal":
        issues.append(size_message)
        recommendations.append("Rewrite with optimal row group size (64-256 MB)")

    if row_status != "optimal":
        issues.append(row_message)
        recommendations.append(
            f"Target {GENERAL_ROW_COUNT_RANGE[0]:,}-{GENERAL_ROW_COUNT_RANGE[1]:,} rows per group"
        )

    # We fix by row count, not size, and --fix writes DEFAULT_SORT_ROW_GROUP_ROWS
    # -- so the trigger is the spatial band, not the verdict's general one (#972).
    fix_available = row_count_fix_available(
        stats["avg_rows_per_group"], stats["total_size"], stats["num_groups"]
    )
    spatial_band = f"{SPATIAL_ROW_COUNT_RANGE[0]:,}-{SPATIAL_ROW_COUNT_RANGE[1]:,}"
    # A file inside the general band but outside the spatial one passes, and
    # still has a fix waiting. Say so, rather than reporting "optimal" and then
    # silently rewriting the file.
    fix_but_optimal = fix_available and row_status == "optimal"
    if fix_but_optimal:
        recommendations.append(
            f"Rewrite with {spatial_band} rows per group so spatial filters prune "
            f"(gpio check row-group --fix writes {DEFAULT_SORT_ROW_GROUP_ROWS:,})"
        )

    results = {
        "passed": passed,
        "stats": stats,
        "size_status": size_status,
        "row_status": row_status,
        "issues": issues,
        "recommendations": recommendations,
        "fix_available": fix_available,
    }

    # Print results (skip if quiet mode)
    if not quiet:
        progress("\nRow Group Analysis:")
        progress(f"Number of row groups: {stats['num_groups']}")

        # Color-based output for size
        size_msg = f"Average group size: {format_size(stats['avg_group_size'])}"
        if size_color == "green":
            success(size_msg)
            success(size_message)
        elif size_color == "yellow":
            warn(size_msg)
            warn(size_message)
        else:
            error(size_msg)
            error(size_message)

        # Color-based output for rows
        row_msg = f"Average rows per group: {stats['avg_rows_per_group']:,.0f}"
        if row_color == "green":
            success(row_msg)
            success(row_message)
        elif row_color == "yellow":
            warn(row_msg)
            warn(row_message)
        else:
            error(row_msg)
            error(row_message)

        if fix_but_optimal:
            warn(
                f"Row groups are larger than the spatial band ({spatial_band}); "
                f"--fix rewrites at {DEFAULT_SORT_ROW_GROUP_ROWS:,} rows per group"
            )

        progress(f"\nTotal file size: {format_size(stats['total_size'])}")

        if size_status != "optimal" or row_status != "optimal" or fix_but_optimal:
            general_low, general_high = GENERAL_ROW_COUNT_RANGE
            spatial_low, spatial_high = SPATIAL_ROW_COUNT_RANGE
            progress("\nRow Group Guidelines:")
            progress("- Optimal size: 64-256 MB per row group")
            progress(
                f"- Optimal rows: {general_low:,}-{general_high:,} rows per group (general use)"
            )
            progress("- Small files (<64 MB): single row group is fine")
            progress(
                f"- Spatial queries: {spatial_low:,}-{spatial_high:,} rows per group with "
                "Hilbert sorting and GeoParquet v2.0 enables optimal row group skipping "
                f"(gpio sort defaults to {DEFAULT_SORT_ROW_GROUP_ROWS:,})"
            )

    if return_results:
        return results


def _check_parquet_geo_only(parquet_file, file_type_info, verbose, return_results, quiet=False):
    """Check parquet-geo-only file (no geo metadata is expected)."""
    bbox_info = check_bbox_structure(parquet_file, verbose)
    stats_info = has_parquet_geo_row_group_stats(parquet_file)

    issues = []
    recommendations = []

    # For parquet-geo-only, bbox column is NOT recommended
    if bbox_info["has_bbox_column"]:
        issues.append(
            f"Bbox column '{bbox_info['bbox_column_name']}' found "
            "(not needed for native Parquet geo types)"
        )
        recommendations.append(
            "Remove bbox column with --fix (native geo types provide row group stats)"
        )

    passed = not bbox_info["has_bbox_column"]

    # Print results (skip if quiet mode)
    if not quiet:
        progress("\nParquet Geo Analysis:")
        success("✓ File uses native Parquet GEOMETRY/GEOGRAPHY types")
        warn("⚠️  No GeoParquet metadata (file uses parquet-geo-only format)")
        info("   Use 'gpio convert --geoparquet-version 2.0' to add GeoParquet 2.0 metadata")

        if bbox_info["has_bbox_column"]:
            warn(
                f"⚠️  Bbox column '{bbox_info['bbox_column_name']}' found "
                "(unnecessary - native geo types have row group stats)"
            )
            info("   Use --fix to remove the bbox column")
        else:
            success("✓ No bbox column (correct for native Parquet geo types)")

        if stats_info["has_stats"]:
            success("✓ Row group statistics available for spatial filtering")

    if return_results:
        return {
            "passed": passed,
            "file_type": "parquet_geo_only",
            "has_geo_metadata": False,
            "has_native_geo_types": True,
            "has_bbox_column": bbox_info["has_bbox_column"],
            "bbox_column_name": bbox_info.get("bbox_column_name"),
            "has_row_group_stats": stats_info["has_stats"],
            "needs_bbox_removal": bbox_info["has_bbox_column"],
            "issues": issues,
            "recommendations": recommendations,
            "fix_available": bbox_info["has_bbox_column"],
        }


def _check_geoparquet_v2(parquet_file, file_type_info, verbose, return_results, quiet=False):
    """Check GeoParquet 2.0 file (a bbox column is optional, but must be declared)."""
    bbox_info = check_bbox_structure(parquet_file, verbose)
    stats_info = has_parquet_geo_row_group_stats(parquet_file)

    issues = []
    recommendations = []

    # `covering` is not in the 2.0 spec text -- it was introduced in 1.1 and
    # dropped in 2.0 in favour of the native statistics -- but 2.0 readers must
    # tolerate unknown fields, and opengeospatial/geoparquet#302 proposes
    # reinstating it (still open). Since files carrying one exist and the
    # motivation holds (native stats prune row groups, a covering also prunes
    # pages within one), a declared bbox column is not a defect here. An
    # *undeclared* one is: it costs bytes and no reader can use it (#738).
    undeclared_bbox = bbox_info["has_bbox_column"] and not bbox_info["has_bbox_metadata"]
    if undeclared_bbox:
        issues.append(
            f"Bbox column '{bbox_info['bbox_column_name']}' is not declared in the "
            "'covering' metadata, so no reader can use it"
        )
        recommendations.append(
            "Declare it with 'gpio add bbox-metadata', or remove the column with --fix"
        )

    passed = not undeclared_bbox

    # Print results (skip if quiet mode)
    if not quiet:
        progress("\nGeoParquet 2.0 Metadata:")
        success(f"✓ Version {file_type_info['geo_version']}")
        success("✓ Uses native Parquet GEOMETRY/GEOGRAPHY types")

        if undeclared_bbox:
            warn(
                f"⚠️  Bbox column '{bbox_info['bbox_column_name']}' found but not declared "
                "in 'covering' metadata"
            )
            info("   As written, it costs file size and no reader can use it.")
            info("   Declare it with 'gpio add bbox-metadata', or use --fix to remove it.")
        elif bbox_info["has_bbox_column"]:
            success(
                f"✓ Bbox covering column '{bbox_info['bbox_column_name']}' declared "
                "(optional in 2.0; enables page-level pruning)"
            )
        else:
            success("✓ No bbox column (native geo statistics are enough for most files)")

        if stats_info["has_stats"]:
            success("✓ Row group statistics available for spatial filtering")

    if return_results:
        return {
            "passed": passed,
            "file_type": "geoparquet_v2",
            "has_geo_metadata": True,
            "version": file_type_info["geo_version"],
            "has_native_geo_types": True,
            "has_bbox_column": bbox_info["has_bbox_column"],
            "bbox_column_name": bbox_info.get("bbox_column_name"),
            "has_row_group_stats": stats_info["has_stats"],
            # Only an *undeclared* bbox column is removable: --fix must not
            # delete a covering the file legitimately declares.
            "needs_bbox_removal": undeclared_bbox,
            "issues": issues,
            "recommendations": recommendations,
            "fix_available": undeclared_bbox,
        }


def _check_geoparquet_v1(parquet_file, file_type_info, verbose, return_results, quiet=False):
    """Check GeoParquet 1.x file (existing logic, bbox IS recommended)."""
    from geoparquet_io.core.duckdb_metadata import get_geo_metadata
    from geoparquet_io.core.geo_metadata import covering_supported

    geo_meta = get_geo_metadata(parquet_file)
    # `get_geo_metadata` is a read-only reader: it hands the block back exactly
    # as the file holds it, so a block that is not a JSON object arrives here
    # verbatim and used to crash with `'list' object has no attribute 'get'`
    # (#968). A validation reader guards and reports the truth rather than
    # sanitizing: a block that declares no version has none, which is what
    # "0.0.0" says and what the outdated-version issue below reports.
    version = geo_meta.get("version", "0.0.0") if isinstance(geo_meta, dict) else "0.0.0"
    bbox_info = check_bbox_structure(parquet_file, verbose)

    # Build results
    issues = []
    recommendations = []

    if version < "1.1.0":
        issues.append(f"GeoParquet version {version} is outdated")
        recommendations.append("Upgrade to version 1.1.0+")

    needs_bbox_column = not bbox_info["has_bbox_column"]
    # 'covering' is 1.1-only, so a 1.0 file with a bbox column is not missing anything
    # it is allowed to have — the "outdated version" issue above is the actionable one.
    needs_bbox_metadata = (
        covering_supported(version)
        and bbox_info["has_bbox_column"]
        and not bbox_info["has_bbox_metadata"]
    )

    if needs_bbox_column:
        issues.append("No bbox column found")
        recommendations.append("Add bbox column for better query performance")

    if needs_bbox_metadata:
        issues.append("Bbox column exists but missing metadata covering")
        recommendations.append("Add bbox covering to metadata")

    # The inverse mismatch: a pre-1.1 file that carries the 1.1-only key anyway.
    # 'gpio check spec' rejects such a file, so don't affirm it here.
    has_illegal_covering = bbox_info["has_bbox_metadata"] and not covering_supported(version)
    if has_illegal_covering:
        issues.append(f"Metadata covering present but version {version} predates 'covering' (1.1)")
        recommendations.append("Upgrade to version 1.1.0+ to keep the bbox covering")

    passed = version >= "1.1.0" and not needs_bbox_column and not needs_bbox_metadata

    # Always suggest v2.0 upgrade for v1.x files
    recommendations.append(
        "Consider upgrading to GeoParquet 2.0 for native spatial stats "
        "and filter pushdown. Run: gpio convert geoparquet input.parquet "
        "output.parquet --geoparquet-version 2.0"
    )

    # Print results (skip if quiet mode)
    if not quiet:
        progress("\nGeoParquet Metadata:")
        if version >= "1.1.0":
            success(f"✓ Version {version}")
        else:
            warn(f"⚠️ Version {version} (upgrade to 1.1.0+ recommended)")

        if bbox_info["has_bbox_column"]:
            if has_illegal_covering:
                error(
                    f"❌ Found bbox column '{bbox_info['bbox_column_name']}' with covering "
                    f"metadata, but 'covering' requires GeoParquet 1.1+ (this file is {version}) "
                    "— run 'gpio check spec' for details"
                )
            elif bbox_info["has_bbox_metadata"]:
                success(
                    f"✓ Found bbox column '{bbox_info['bbox_column_name']}' "
                    "with proper metadata covering"
                )
            elif needs_bbox_metadata:
                warn(
                    f"⚠️  Found bbox column '{bbox_info['bbox_column_name']}' but missing "
                    "bbox covering metadata (add to metadata to help inform clients)"
                )
            else:
                info(
                    f"ℹ️  Found bbox column '{bbox_info['bbox_column_name']}'; the 'covering' "
                    f"key that advertises it needs GeoParquet 1.1+ (this file is {version})"
                )
        else:
            error("❌ No bbox column found (recommended for better performance)")

        info(
            "ℹ️  GeoParquet 2.0 is available, with native spatial stats and filter pushdown. "
            "Run: gpio convert geoparquet input.parquet output.parquet --geoparquet-version 2.0"
        )

    if return_results:
        return {
            "passed": passed,
            "file_type": "geoparquet_v1",
            "has_geo_metadata": True,
            "version": version,
            "has_bbox_column": bbox_info["has_bbox_column"],
            "has_bbox_metadata": bbox_info["has_bbox_metadata"],
            "bbox_column_name": bbox_info.get("bbox_column_name"),
            "needs_bbox_column": needs_bbox_column,
            "needs_bbox_metadata": needs_bbox_metadata,
            "issues": issues,
            "recommendations": recommendations,
            "fix_available": needs_bbox_column or needs_bbox_metadata,
        }


def check_metadata_and_bbox(parquet_file, verbose=False, return_results=False, quiet=False):
    """Check GeoParquet metadata version and bbox structure (version-aware).

    Handles three file types differently:
    - GeoParquet 1.x: Bbox column is recommended for spatial filtering
    - GeoParquet 2.0: Bbox column is NOT recommended (native geo types provide stats)
    - Parquet-geo-only: Bbox column is NOT recommended (native geo types provide stats)

    Args:
        parquet_file: Path to parquet file
        verbose: Print additional information
        return_results: If True, return structured results dict
        quiet: If True, suppress all output (for multi-file batch mode)

    Returns:
        dict if return_results=True, containing:
            - passed: bool
            - file_type: str (geoparquet_v1, geoparquet_v2, parquet_geo_only, unknown)
            - has_geo_metadata: bool
            - version: str (for v1/v2)
            - has_bbox_column: bool
            - bbox_column_name: str or None
            - issues: list of issue descriptions
            - recommendations: list of recommendations
            - fix_available: bool
            - needs_bbox_removal: bool (for v2/parquet-geo-only with bbox)
    """
    # Detect file type first
    file_type_info = detect_geoparquet_file_type(parquet_file, verbose)

    # Handle parquet-geo-only case (no geo metadata is intentional)
    if file_type_info["file_type"] == "parquet_geo_only":
        return _check_parquet_geo_only(parquet_file, file_type_info, verbose, return_results, quiet)

    # Handle GeoParquet 2.0 case
    if file_type_info["file_type"] == "geoparquet_v2":
        return _check_geoparquet_v2(parquet_file, file_type_info, verbose, return_results, quiet)

    # Handle GeoParquet 1.x case
    if file_type_info["file_type"] == "geoparquet_v1":
        return _check_geoparquet_v1(parquet_file, file_type_info, verbose, return_results, quiet)

    # Unknown file type - no geo indicators found
    if not quiet:
        error("\n❌ No GeoParquet metadata found")
    if return_results:
        return {
            "passed": False,
            "file_type": "unknown",
            "has_geo_metadata": False,
            "issues": ["No GeoParquet metadata or native Parquet geo types found"],
            "recommendations": [],
            "fix_available": False,
        }


def check_compression(parquet_file, verbose=False, return_results=False, quiet=False):
    """Check compression settings for geometry column.

    Args:
        parquet_file: Path to parquet file
        verbose: Print additional information
        return_results: If True, return structured results dict
        quiet: If True, suppress all output (for multi-file batch mode)

    Returns:
        dict if return_results=True, containing:
            - passed: bool
            - current_compression: str
            - geometry_column: str
            - issues: list of issue descriptions
            - recommendations: list of recommendations
    """
    primary_col = find_primary_geometry_column(parquet_file, verbose)
    if not primary_col:
        if not quiet:
            error("\n❌ No geometry column found")
        if return_results:
            return {
                "passed": False,
                "current_compression": None,
                "geometry_column": None,
                "issues": ["No geometry column found"],
                "recommendations": [],
                "fix_available": False,
            }
        return

    compression = get_compression_info(parquet_file, primary_col).get(primary_col)
    if compression is None:
        # A file with no row groups -- a zero-row result of a spatial filter, say --
        # has no column chunks, so parquet_metadata() reports no codec for any
        # column. Say so instead of indexing blind (#823). Nothing is wrong with
        # the file and there is nothing to re-compress, so this passes.
        if not quiet:
            progress("\nCompression Analysis:")
            info(f"ℹ️  {_NO_COMPRESSION_INFO}")
        if return_results:
            return {
                "passed": True,
                "current_compression": None,
                "geometry_column": primary_col,
                "issues": [],
                "recommendations": [],
                "fix_available": False,
            }
        return

    passed = compression == "ZSTD"

    issues = []
    recommendations = []
    if not passed:
        issues.append(f"{compression} compression instead of ZSTD")
        recommendations.append("Re-compress with ZSTD for better performance")

    results = {
        "passed": passed,
        "current_compression": compression,
        "geometry_column": primary_col,
        "issues": issues,
        "recommendations": recommendations,
        "fix_available": not passed,
    }

    # Print results (skip if quiet mode)
    if not quiet:
        progress("\nCompression Analysis:")
        if compression == "ZSTD":
            success(f"✓ ZSTD compression on geometry column '{primary_col}'")
        else:
            warn(
                f"⚠️  {compression} compression on geometry column '{primary_col}' (ZSTD recommended)"
            )

    if return_results:
        return results


def check_bloom_filters(parquet_file, verbose=False, return_results=False, quiet=False):
    """Check bloom filter presence on columns.

    Bloom filters enable efficient point lookups on low-cardinality columns
    (city names, land use types, integer ranges). DuckDB 1.5+ automatically
    writes bloom filters for eligible columns.

    Args:
        parquet_file: Path to parquet file
        verbose: Print additional information
        return_results: If True, return structured results dict
        quiet: If True, suppress all output (for multi-file batch mode)

    Returns:
        dict if return_results=True, containing:
            - passed: bool (always True, bloom filters are informational)
            - has_bloom_filters: bool
            - columns_with_bloom_filters: list of column names
            - columns_without_bloom_filters: list of column names
            - bloom_filter_details: list of per-column bloom filter info
    """
    from geoparquet_io.core.duckdb_metadata import get_bloom_filter_info

    bloom_info = get_bloom_filter_info(parquet_file)

    columns_with = [
        entry["column_name"] for entry in bloom_info if entry["row_groups_with_bloom_filter"] > 0
    ]
    columns_without = [
        entry["column_name"] for entry in bloom_info if entry["row_groups_with_bloom_filter"] == 0
    ]
    has_bloom = len(columns_with) > 0

    total_bloom_bytes = sum(entry["total_bloom_filter_bytes"] for entry in bloom_info)

    results = {
        "passed": True,
        "has_bloom_filters": has_bloom,
        "columns_with_bloom_filters": columns_with,
        "columns_without_bloom_filters": columns_without,
        "bloom_filter_details": bloom_info,
        "total_bloom_filter_bytes": total_bloom_bytes,
    }

    if not quiet:
        progress("\nBloom Filter Analysis:")
        if has_bloom:
            success(
                f"Bloom filters found on {len(columns_with)} column(s): {', '.join(columns_with)}"
            )
            info(f"Total bloom filter size: {format_size(total_bloom_bytes)}")
            if verbose:
                for entry in bloom_info:
                    if entry["row_groups_with_bloom_filter"] > 0:
                        info(
                            f"  {entry['column_name']}: "
                            f"{entry['bloom_filter_coverage_pct']}% coverage, "
                            f"{format_size(entry['total_bloom_filter_bytes'])}"
                        )
        else:
            info("No bloom filters detected")
            if verbose:
                info(
                    "Bloom filters speed up point lookups on low-cardinality columns "
                    "(e.g., city names, categories)"
                )

    if return_results:
        return results


def check_all(
    parquet_file,
    verbose=False,
    return_results=False,
    quiet=False,
    profile: CheckProfile | None = None,
):
    """Run all structure checks.

    Args:
        parquet_file: Path to parquet file
        verbose: Print additional information
        return_results: If True, return aggregated results dict
        quiet: If True, suppress all output (for multi-file batch mode)

    Returns:
        dict if return_results=True, containing results from all checks
    """
    row_groups_result = check_row_groups(
        parquet_file, verbose, return_results=True, quiet=quiet, profile=profile
    )
    bbox_result = check_metadata_and_bbox(parquet_file, verbose, return_results=True, quiet=quiet)
    compression_result = check_compression(parquet_file, verbose, return_results=True, quiet=quiet)
    bloom_filter_result = check_bloom_filters(
        parquet_file, verbose, return_results=True, quiet=quiet
    )

    if return_results:
        return {
            "row_groups": row_groups_result,
            "bbox": bbox_result,
            "compression": compression_result,
            "bloom_filters": bloom_filter_result,
        }


if __name__ == "__main__":
    check_all()
