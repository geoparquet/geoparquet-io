#!/usr/bin/env python3
"""Comparison and trend analysis for archived benchmark JSON.

New measurements use scripts/profile_command.py and seeded local workloads.
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path


def normalize_matrix(rows: list[dict], version: str) -> dict:
    """Adapt seeded harness rows to the archived summary shape without mixing workloads."""
    if not rows or any(row.get("diagnostic") for row in rows):
        raise ValueError("Expected nonempty, uninstrumented matrix measurements")
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row["command"], row["workload"], row["size"])].append(row)
    benchmarks = []
    for (command, workload, size), runs in grouped.items():
        times = [run["wall_seconds"] for run in runs]
        benchmarks.append(
            {
                "operation": command,
                "file_size": f"{workload}/{size}",
                "avg_time": sum(times) / len(times),
                "min_time": min(times),
                "max_time": max(times),
                "cpu_seconds": min(run["cpu_seconds"] for run in runs),
                "peak_rss_bytes": max(run["peak_rss_bytes"] for run in runs),
                "success_count": len(runs),
                "fail_count": 0,
                "output_sha256": [run.get("output_sha256") for run in runs],
                "raw_output_sha256": [run.get("raw_output_sha256") for run in runs],
            }
        )
    from importlib.metadata import version as package_version

    return {
        "version": version,
        "gpio_version": package_version("geoparquet-io"),
        "iterations": min(len(runs) for runs in grouped.values()),
        "measurement": "seeded-profile-v1",
        "benchmarks": benchmarks,
    }


def load_results(path) -> dict:
    """Accept archived summaries and new raw matrix arrays."""
    with open(path) as source:
        result = json.load(source)
    return normalize_matrix(result, Path(path).stem) if isinstance(result, list) else result


def validate_outputs(base: dict, head: dict):
    """Reject changed comparable bytes before reporting timing improvements.

    Only #1111 PIVOT and #1123 rollup have approved behavioral changes.
    Raw hashes remain in summaries for auditing known nondeterministic fields.
    """
    if base.get("measurement") != "seeded-profile-v1":
        return  # Legacy archives contain no output hashes.
    lookup = {(b["file_size"], b["operation"]): b for b in head["benchmarks"]}
    approved = {
        "overview",
        "aggregate-breakdown",
        "aggregate-admin",
        "aggregate-admin-native",
        "aggregate-admin-reprojected",
    }
    for before in base["benchmarks"]:
        key = (before["file_size"], before["operation"])
        after = lookup.get(key)
        if after is None:
            raise ValueError(f"Missing output measurement: {key}")
        hashes = before.get("output_sha256", []) + after.get("output_sha256", [])
        if not hashes or any(value is None for value in hashes):
            raise ValueError(f"Missing output hashes: {key}")
        if before["operation"] in approved:
            continue
        if len({json.dumps(value, sort_keys=True) for value in hashes}) != 1:
            raise ValueError(f"Output bytes changed: {key}")


def compare_results(file1: str, file2: str):
    """Compare two benchmark result files."""
    results1 = load_results(file1)
    results2 = load_results(file2)

    print(f"\n{'=' * 70}")
    print(f"Comparison: {results1['version']} vs {results2['version']}")
    print(f"{'=' * 70}\n")

    validate_outputs(results1, results2)

    # Build lookup for results2
    lookup2 = {}
    for b in results2["benchmarks"]:
        key = (b["file_size"], b["operation"])
        lookup2[key] = b

    print(
        f"{'Operation':<25} {'File':<8} {results1['version']:<12} {results2['version']:<12} {'Delta':<12}"
    )
    print("-" * 70)

    for b1 in results1["benchmarks"]:
        key = (b1["file_size"], b1["operation"])
        b2 = lookup2.get(key)

        op_name = b1["operation"]
        file_size = b1["file_size"]

        if b1["avg_time"] is None:
            time1_str = "FAILED"
        else:
            time1_str = f"{b1['avg_time']:.3f}s"

        if b2 is None or b2["avg_time"] is None:
            time2_str = "FAILED" if b2 else "N/A"
            delta_str = "N/A"
        else:
            time2_str = f"{b2['avg_time']:.3f}s"

            if b1["avg_time"] is not None and b1["avg_time"] > 0:
                delta = (b2["avg_time"] - b1["avg_time"]) / b1["avg_time"] * 100
                if delta > 0:
                    delta_str = f"+{delta:.1f}% slower"
                elif delta < 0:
                    delta_str = f"{delta:.1f}% faster"
                else:
                    delta_str = "same"
            else:
                delta_str = "N/A"

        print(f"{op_name:<25} {file_size:<8} {time1_str:<12} {time2_str:<12} {delta_str:<12}")

    print()


def analyze_trends(baseline_files: list[str], degradation_threshold: float = 0.05) -> dict:
    """Analyze performance trends across multiple baseline files.

    Args:
        baseline_files: List of baseline JSON files, ordered from oldest to newest
        degradation_threshold: Threshold for detecting degradation (default 5% = 0.05)

    Returns:
        Dictionary with trend analysis results including warnings and statistics
    """
    if len(baseline_files) < 2:
        return {
            "error": "Need at least 2 baselines for trend analysis",
            "baselines_count": len(baseline_files),
        }

    # Load all baselines
    baselines = []
    for filepath in baseline_files:
        try:
            baselines.append(load_results(filepath))
        except Exception as e:
            return {"error": f"Failed to load {filepath}: {e}"}

    # Extract versions for reporting
    versions = [b.get("version", "unknown") for b in baselines]

    # Build time series for each (file_size, operation) pair
    # Store (baseline_index, avg_time) to track gaps
    time_series = {}
    for baseline_idx, baseline in enumerate(baselines):
        for bench in baseline["benchmarks"]:
            if bench["avg_time"] is None or bench["avg_time"] <= 0:
                continue
            key = (bench["file_size"], bench["operation"])
            if key not in time_series:
                time_series[key] = []
            time_series[key].append((baseline_idx, bench["avg_time"]))

    # Detect gradual degradation
    warnings = []
    consistent_improvements = []

    for key, entries in time_series.items():
        if len(entries) < 3:
            continue

        file_size, operation = key

        # Calculate deltas only between consecutive baseline indices
        deltas = []
        for i in range(len(entries) - 1):
            idx1, time1 = entries[i]
            idx2, time2 = entries[i + 1]
            # Only compute delta if baselines are consecutive
            if idx2 == idx1 + 1:
                deltas.append((time2 - time1) / time1)

        if len(deltas) < 2:
            continue

        # Check for consistent degradation (last 2 deltas both exceed threshold)
        # Also verify the last 2 baselines (most recent) both have data
        last_two_indices = [entries[-2][0], entries[-1][0]]
        has_recent_consecutive = (
            last_two_indices[-1] == last_two_indices[-2] + 1
            and last_two_indices[-1] == len(baselines) - 1
        )

        if has_recent_consecutive and all(d > degradation_threshold for d in deltas[-2:]):
            avg_degradation = sum(deltas[-2:]) / 2 * 100
            warnings.append(
                {
                    "file_size": file_size,
                    "operation": operation,
                    "type": "gradual_degradation",
                    "avg_degradation_pct": round(avg_degradation, 1),
                    "releases_affected": 2,
                    "message": f"{operation} ({file_size}): {avg_degradation:.1f}% avg degradation over last 2 releases",
                }
            )

        # Check for consistent improvement
        if has_recent_consecutive and all(d < -degradation_threshold for d in deltas[-2:]):
            avg_improvement = -sum(deltas[-2:]) / 2 * 100
            consistent_improvements.append(
                {
                    "file_size": file_size,
                    "operation": operation,
                    "avg_improvement_pct": round(avg_improvement, 1),
                    "message": f"{operation} ({file_size}): {avg_improvement:.1f}% avg improvement over last 2 releases",
                }
            )

    # Calculate overall statistics (only from consecutive baselines)
    all_deltas = []
    for entries in time_series.values():
        for i in range(len(entries) - 1):
            idx1, time1 = entries[i]
            idx2, time2 = entries[i + 1]
            # Only include deltas from consecutive baselines
            if idx2 == idx1 + 1:
                all_deltas.append((time2 - time1) / time1)

    if all_deltas:
        avg_change = sum(all_deltas) / len(all_deltas) * 100
        max_regression = max(all_deltas) * 100 if all_deltas else 0
        max_improvement = min(all_deltas) * 100 if all_deltas else 0
    else:
        avg_change = 0
        max_regression = 0
        max_improvement = 0

    return {
        "versions": versions,
        "baselines_count": len(baselines),
        "operations_tracked": len(time_series),
        "warnings": warnings,
        "improvements": consistent_improvements,
        "statistics": {
            "avg_change_pct": round(avg_change, 2),
            "max_regression_pct": round(max_regression, 1),
            "max_improvement_pct": round(max_improvement, 1),
        },
    }


def print_trend_analysis(trend_data: dict):
    """Print trend analysis in human-readable format."""
    if "error" in trend_data:
        print(f"\nTrend Analysis Error: {trend_data['error']}")
        return

    print(f"\n{'=' * 70}")
    print("Trend Analysis Across Releases")
    print(f"{'=' * 70}")
    print(f"Versions: {' → '.join(trend_data['versions'])}")
    print(f"Baselines: {trend_data['baselines_count']}")
    print(f"Operations tracked: {trend_data['operations_tracked']}")
    print()

    # Print statistics
    stats = trend_data["statistics"]
    print("Overall Statistics:")
    print(f"  Average change: {stats['avg_change_pct']:+.2f}%")
    print(f"  Max regression: {stats['max_regression_pct']:+.1f}%")
    print(f"  Max improvement: {stats['max_improvement_pct']:+.1f}%")
    print()

    # Print warnings
    if trend_data["warnings"]:
        print(f"⚠️  Gradual Degradation Detected ({len(trend_data['warnings'])} operations):")
        print("-" * 70)
        for w in trend_data["warnings"]:
            print(f"  • {w['message']}")
        print()
    else:
        print("✅ No gradual degradation detected")
        print()

    # Print improvements
    if trend_data["improvements"]:
        print(f"🚀 Consistent Improvements ({len(trend_data['improvements'])} operations):")
        print("-" * 70)
        for imp in trend_data["improvements"]:
            print(f"  • {imp['message']}")
        print()


def main():
    parser = argparse.ArgumentParser(description="Compare archived gpio benchmark results")
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--compare", nargs=2, metavar=("BASE", "HEAD"))
    action.add_argument("--trend", nargs="+")
    action.add_argument("--matrix", help="Normalize raw profile_command JSON for CI summaries")
    parser.add_argument("--version-label", default="unknown")
    parser.add_argument("--trend-threshold", type=float, default=0.05)
    parser.add_argument("--output", "-o")
    args = parser.parse_args()
    if args.matrix:
        if not args.output:
            parser.error("--matrix requires --output")
        result = normalize_matrix(json.loads(Path(args.matrix).read_text()), args.version_label)
        Path(args.output).write_text(json.dumps(result, indent=2) + "\n")
    elif args.compare:
        compare_results(*args.compare)
    else:
        result = analyze_trends(args.trend, args.trend_threshold)
        print_trend_analysis(result)
        if args.output:
            Path(args.output).write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
