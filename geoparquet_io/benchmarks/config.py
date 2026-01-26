"""Configuration for benchmark suite."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RegressionThresholds:
    """Thresholds for regression detection.

    All values are decimals (e.g., 0.10 = 10%).
    """

    time_warning: float = 0.10  # >10% slower = warning
    time_failure: float = 0.25  # >25% slower = failure
    memory_warning: float = 0.20  # >20% more memory = warning
    memory_failure: float = 0.50  # >50% more memory = failure


# Core operations (~10) - run by default
CORE_OPERATIONS: list[str] = [
    "read",
    "write",
    "convert-geojson",
    "convert-gpkg",
    "extract-bbox",
    "extract-columns",
    "reproject",
    "sort-hilbert",
    "add-bbox",
    "partition-quadkey",
]

# Full suite operations - includes core + extras
FULL_OPERATIONS: list[str] = CORE_OPERATIONS + [
    "convert-shapefile",
    "convert-fgb",
    "sort-quadkey",
    "add-h3",
    "add-quadkey",
    "add-country",
    "partition-h3",
    "partition-country",
]

# Default regression thresholds
DEFAULT_THRESHOLDS = RegressionThresholds()

# Memory limit configurations for Docker
MEMORY_LIMITS: dict[str, str] = {
    "constrained": "512m",
    "normal": "4g",
}
