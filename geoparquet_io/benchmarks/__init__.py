"""Benchmark suite for geoparquet-io."""

from geoparquet_io.benchmarks.config import (
    CHAIN_OPERATIONS,
    CORE_OPERATIONS,
    DEFAULT_THRESHOLDS,
    FULL_OPERATIONS,
    MEMORY_LIMITS,
    RegressionThresholds,
)

__all__ = [
    "CORE_OPERATIONS",
    "CHAIN_OPERATIONS",
    "FULL_OPERATIONS",
    "DEFAULT_THRESHOLDS",
    "MEMORY_LIMITS",
    "RegressionThresholds",
]
