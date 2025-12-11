#!/usr/bin/env python3


import click

from geoparquet_io.core.common import (
    check_bbox_structure,
    detect_geoparquet_file_type,
    find_primary_geometry_column,
    format_size,
)
from geoparquet_io.core.metadata_utils import has_parquet_geo_row_group_stats


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


def assess_row_group_size(avg_group_size_bytes, total_size_bytes):
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


def assess_row_count(avg_rows):
    """
    Assess if average row count per group is optimal.

    Returns:
        tuple: (status, message, color) where status is one of:
            - "optimal"
            - "suboptimal"
            - "poor"
    """
    if avg_rows < 2000:
        return (
            "poor",
            "Row count per group is very low. Target 50,000-200,000 rows per group",
            "red",
        )
    elif avg_rows > 1000000:
        return (
            "poor",
            "Row count per group is very high. Target 50,000-200,000 rows per group",
            "red",
        )
    elif 50000 <= avg_rows <= 200000:
        return "optimal", "Row count per group is optimal", "green"
    else:
        return (
            "suboptimal",
            "Row count per group is outside recommended range (50,000-200,000)",
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


def check_row_groups(parquet_file, verbose=False, return_results=False):
    """Check row group optimization and print results.

    Args:
        parquet_file: Path to parquet file
        verbose: Print additional information
        return_results: If True, return structured results dict instead of only printing

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
        stats["avg_group_size"], stats["total_size"]
    )
    row_status, row_message, row_color = assess_row_count(stats["avg_rows_per_group"])

    # Build results dict
    passed = size_status == "optimal" and row_status == "optimal"
    issues = []
    recommendations = []

    if size_status != "optimal":
        issues.append(size_message)
        recommendations.append("Rewrite with optimal row group size (128-256 MB)")

    if row_status != "optimal":
        issues.append(row_message)
        recommendations.append("Target 50,000-200,000 rows per group")

    results = {
        "passed": passed,
        "stats": stats,
        "size_status": size_status,
        "row_status": row_status,
        "issues": issues,
        "recommendations": recommendations,
        "fix_available": not passed,
    }

    # Print results
    click.echo("\nRow Group Analysis:")
    click.echo(f"Number of row groups: {stats['num_groups']}")

    click.echo(
        click.style(f"Average group size: {format_size(stats['avg_group_size'])}", fg=size_color)
    )
    click.echo(click.style(size_message, fg=size_color))

    click.echo(
        click.style(f"Average rows per group: {stats['avg_rows_per_group']:,.0f}", fg=row_color)
    )
    click.echo(click.style(row_message, fg=row_color))

    click.echo(f"\nTotal file size: {format_size(stats['total_size'])}")

    if size_status != "optimal" or row_status != "optimal":
        click.echo("\nRow Group Guidelines:")
        click.echo("- Optimal size: 64-256 MB per row group")
        click.echo("- Optimal rows: 50,000-200,000 rows per group")
        click.echo("- Small files (<64 MB): single row group is fine")

    if return_results:
        return results


def _check_parquet_geo_only(parquet_file, file_type_info, verbose, return_results):
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

    # Print results
    click.echo("\nParquet Geo Analysis:")
    click.echo(click.style("✓ File uses native Parquet GEOMETRY/GEOGRAPHY types", fg="green"))
    click.echo(
        click.style("⚠️  No GeoParquet metadata (file uses parquet-geo-only format)", fg="yellow")
    )
    click.echo(
        click.style(
            "   Use 'gpio convert --geoparquet-version 2.0' to add GeoParquet 2.0 metadata",
            fg="cyan",
        )
    )

    if bbox_info["has_bbox_column"]:
        click.echo(
            click.style(
                f"⚠️  Bbox column '{bbox_info['bbox_column_name']}' found "
                "(unnecessary - native geo types have row group stats)",
                fg="yellow",
            )
        )
        click.echo(click.style("   Use --fix to remove the bbox column", fg="cyan"))
    else:
        click.echo(
            click.style("✓ No bbox column (correct for native Parquet geo types)", fg="green")
        )

    if stats_info["has_stats"]:
        click.echo(
            click.style("✓ Row group statistics available for spatial filtering", fg="green")
        )

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


def _check_geoparquet_v2(parquet_file, file_type_info, verbose, return_results):
    """Check GeoParquet 2.0 file (bbox not recommended)."""
    bbox_info = check_bbox_structure(parquet_file, verbose)
    stats_info = has_parquet_geo_row_group_stats(parquet_file)

    issues = []
    recommendations = []

    # For v2, bbox column is NOT recommended
    if bbox_info["has_bbox_column"]:
        issues.append(
            f"Bbox column '{bbox_info['bbox_column_name']}' found (not needed for GeoParquet 2.0)"
        )
        recommendations.append(
            "Remove bbox column with --fix (native geo types provide row group stats)"
        )

    passed = not bbox_info["has_bbox_column"]

    # Print results
    click.echo("\nGeoParquet 2.0 Metadata:")
    click.echo(click.style(f"✓ Version {file_type_info['geo_version']}", fg="green"))
    click.echo(click.style("✓ Uses native Parquet GEOMETRY/GEOGRAPHY types", fg="green"))

    if bbox_info["has_bbox_column"]:
        click.echo(
            click.style(
                f"⚠️  Bbox column '{bbox_info['bbox_column_name']}' found (not recommended for 2.0)",
                fg="yellow",
            )
        )
        click.echo(
            click.style(
                "   Native Parquet geo types provide row group stats for spatial filtering.",
                fg="cyan",
            )
        )
        click.echo(click.style("   Use --fix to remove the bbox column", fg="cyan"))
    else:
        click.echo(click.style("✓ No bbox column (correct for GeoParquet 2.0)", fg="green"))

    if stats_info["has_stats"]:
        click.echo(
            click.style("✓ Row group statistics available for spatial filtering", fg="green")
        )

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
            "needs_bbox_removal": bbox_info["has_bbox_column"],
            "issues": issues,
            "recommendations": recommendations,
            "fix_available": bbox_info["has_bbox_column"],
        }


def _check_geoparquet_v1(parquet_file, file_type_info, verbose, return_results):
    """Check GeoParquet 1.x file (existing logic, bbox IS recommended)."""
    from geoparquet_io.core.duckdb_metadata import get_geo_metadata

    geo_meta = get_geo_metadata(parquet_file)
    version = geo_meta.get("version", "0.0.0") if geo_meta else "0.0.0"
    bbox_info = check_bbox_structure(parquet_file, verbose)

    # Build results
    issues = []
    recommendations = []

    if version < "1.1.0":
        issues.append(f"GeoParquet version {version} is outdated")
        recommendations.append("Upgrade to version 1.1.0+")

    needs_bbox_column = not bbox_info["has_bbox_column"]
    needs_bbox_metadata = bbox_info["has_bbox_column"] and not bbox_info["has_bbox_metadata"]

    if needs_bbox_column:
        issues.append("No bbox column found")
        recommendations.append("Add bbox column for better query performance")

    if needs_bbox_metadata:
        issues.append("Bbox column exists but missing metadata covering")
        recommendations.append("Add bbox covering to metadata")

    passed = version >= "1.1.0" and not needs_bbox_column and not needs_bbox_metadata

    # Print results
    click.echo("\nGeoParquet Metadata:")
    version_color = "green" if version >= "1.1.0" else "yellow"
    version_prefix = "✓" if version >= "1.1.0" else "⚠️"
    version_suffix = "" if version >= "1.1.0" else " (upgrade to 1.1.0+ recommended)"

    click.echo(click.style(f"{version_prefix} Version {version}{version_suffix}", fg=version_color))

    if bbox_info["has_bbox_column"]:
        if bbox_info["has_bbox_metadata"]:
            click.echo(
                click.style(
                    f"✓ Found bbox column '{bbox_info['bbox_column_name']}' "
                    "with proper metadata covering",
                    fg="green",
                )
            )
        else:
            click.echo(
                click.style(
                    f"⚠️  Found bbox column '{bbox_info['bbox_column_name']}' but missing "
                    "bbox covering metadata (add to metadata to help inform clients)",
                    fg="yellow",
                )
            )
    else:
        click.echo(
            click.style("❌ No bbox column found (recommended for better performance)", fg="red")
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


def check_metadata_and_bbox(parquet_file, verbose=False, return_results=False):
    """Check GeoParquet metadata version and bbox structure (version-aware).

    Handles three file types differently:
    - GeoParquet 1.x: Bbox column is recommended for spatial filtering
    - GeoParquet 2.0: Bbox column is NOT recommended (native geo types provide stats)
    - Parquet-geo-only: Bbox column is NOT recommended (native geo types provide stats)

    Args:
        parquet_file: Path to parquet file
        verbose: Print additional information
        return_results: If True, return structured results dict

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
        return _check_parquet_geo_only(parquet_file, file_type_info, verbose, return_results)

    # Handle GeoParquet 2.0 case
    if file_type_info["file_type"] == "geoparquet_v2":
        return _check_geoparquet_v2(parquet_file, file_type_info, verbose, return_results)

    # Handle GeoParquet 1.x case
    if file_type_info["file_type"] == "geoparquet_v1":
        return _check_geoparquet_v1(parquet_file, file_type_info, verbose, return_results)

    # Unknown file type - no geo indicators found
    click.echo(click.style("\n❌ No GeoParquet metadata found", fg="red"))
    if return_results:
        return {
            "passed": False,
            "file_type": "unknown",
            "has_geo_metadata": False,
            "issues": ["No GeoParquet metadata or native Parquet geo types found"],
            "recommendations": [],
            "fix_available": False,
        }


def check_compression(parquet_file, verbose=False, return_results=False):
    """Check compression settings for geometry column.

    Args:
        parquet_file: Path to parquet file
        verbose: Print additional information
        return_results: If True, return structured results dict

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
        click.echo(click.style("\n❌ No geometry column found", fg="red"))
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

    compression = get_compression_info(parquet_file, primary_col)[primary_col]
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

    # Print results
    click.echo("\nCompression Analysis:")
    if compression == "ZSTD":
        click.echo(
            click.style(f"✓ ZSTD compression on geometry column '{primary_col}'", fg="green")
        )
    else:
        click.echo(
            click.style(
                f"⚠️  {compression} compression on geometry column '{primary_col}' (ZSTD recommended)",
                fg="yellow",
            )
        )

    if return_results:
        return results


def check_all(parquet_file, verbose=False, return_results=False):
    """Run all structure checks.

    Args:
        parquet_file: Path to parquet file
        verbose: Print additional information
        return_results: If True, return aggregated results dict

    Returns:
        dict if return_results=True, containing results from all checks
    """
    row_groups_result = check_row_groups(parquet_file, verbose, return_results=True)
    bbox_result = check_metadata_and_bbox(parquet_file, verbose, return_results=True)
    compression_result = check_compression(parquet_file, verbose, return_results=True)

    if return_results:
        return {
            "row_groups": row_groups_result,
            "bbox": bbox_result,
            "compression": compression_result,
        }


if __name__ == "__main__":
    check_all()
