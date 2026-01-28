"""Report formatting for benchmark results."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from geoparquet_io.core.benchmark_suite import (
        BenchmarkResult,
        ComparisonResult,
    )


def format_table(results: list[BenchmarkResult]) -> str:
    """Format results as ASCII table.

    Displays RSS memory (which captures PyArrow/DuckDB allocations).
    """
    lines = []
    lines.append(f"{'Operation':<20} {'File':<20} {'Time (s)':<12} {'RSS (MB)':<12} {'Status':<8}")
    lines.append("-" * 72)

    for r in results:
        status = "✓" if r.success else "✗"
        lines.append(
            f"{r.operation:<20} {r.file:<20} {r.time_seconds:<12.3f} "
            f"{r.peak_rss_memory_mb:<12.1f} {status:<8}"
        )

    return "\n".join(lines)


def format_comparison_table(comparisons: list[ComparisonResult]) -> str:
    """Format comparison results as ASCII table."""
    from geoparquet_io.core.benchmark_suite import RegressionStatus

    lines = []
    lines.append(
        f"{'Operation':<20} {'File':<15} {'Time':<10} {'Δ':<8} {'RSS':<10} {'Δ':<8} {'Status':<10}"
    )
    lines.append("-" * 81)

    status_icons = {
        RegressionStatus.OK: "✓ OK",
        RegressionStatus.WARNING: "⚠ WARNING",
        RegressionStatus.FAILURE: "✗ FAILURE",
        RegressionStatus.IMPROVED: "↑ IMPROVED",
    }

    for c in comparisons:
        time_delta_str = f"{c.time_delta_pct:+.0%}"
        memory_delta_str = f"{c.memory_delta_pct:+.0%}"
        status_str = status_icons.get(c.status, str(c.status))

        lines.append(
            f"{c.operation:<20} {c.file:<15} {c.current_time:<10.3f} {time_delta_str:<8} "
            f"{c.current_rss_memory:<10.1f} {memory_delta_str:<8} {status_str:<10}"
        )

    return "\n".join(lines)


# NOTE: Markdown format deferred until needed for PR comments
# def format_markdown(results): ...
# def format_comparison_markdown(comparisons): ...
