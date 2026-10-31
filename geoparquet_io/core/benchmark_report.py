"""Report formatting for benchmark results."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from geoparquet_io.core.benchmark_suite import (
        BenchmarkResult,
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
