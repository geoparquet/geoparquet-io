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

# Chain operations - multi-step workflows for testing chaining performance
CHAIN_OPERATIONS: list[str] = [
    "chain-extract-bbox-sort",  # Extract → Add BBox → Hilbert Sort
    "chain-convert-optimize",  # Convert → Add BBox → Hilbert Sort
    "chain-filter-reproject-partition",  # BBox Filter → Reproject → Quadkey Partition
]

# Full suite operations - includes core + extras + chains
FULL_OPERATIONS: list[str] = (
    CORE_OPERATIONS
    + [
        "convert-shapefile",
        "convert-fgb",
        "sort-quadkey",
        "add-h3",
        "add-quadkey",
        "add-country",
        "partition-h3",
        "partition-country",
    ]
    + CHAIN_OPERATIONS
)

# Default regression thresholds
DEFAULT_THRESHOLDS = RegressionThresholds()

# Base URL for benchmark data on source.coop
BENCHMARK_DATA_URL = "https://data.source.coop/cholmes/gpio-test/benchmark"

# Benchmark files by size tier
# These are real-world GeoParquet files from public datasets:
# - buildings: Overture Singapore Buildings (polygons, WGS84)
# - places: Overture Singapore Places (points, WGS84)
# - fields: fiboa Slovenia Field Boundaries (polygons, EPSG:3794)
BENCHMARK_FILES: dict[str, dict[str, str]] = {
    "tiny": {
        "buildings": f"{BENCHMARK_DATA_URL}/buildings_tiny.parquet",  # 1K rows, ~178KB
        "places": f"{BENCHMARK_DATA_URL}/places_tiny.parquet",  # 1K rows, ~130KB
    },
    "small": {
        "buildings": f"{BENCHMARK_DATA_URL}/buildings_small.parquet",  # 10K rows, ~2.4MB
        "places": f"{BENCHMARK_DATA_URL}/places_small.parquet",  # 10K rows, ~1.1MB
    },
    "medium": {
        "buildings": f"{BENCHMARK_DATA_URL}/buildings_medium.parquet",  # 100K rows, ~18MB
    },
    "large": {
        "fields": f"{BENCHMARK_DATA_URL}/fields_large.parquet",  # 809K rows, ~176MB
    },
}
