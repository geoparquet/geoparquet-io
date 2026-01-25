"""
Write strategy module for GeoParquet output.

This module provides a pluggable architecture for different write strategies,
each with different memory and performance characteristics:

- duckdb-kv (default): Use DuckDB COPY TO with native KV_METADATA for geo metadata
- in-memory: Load entire dataset into memory, apply metadata, write once
- streaming: Stream Arrow RecordBatches directly to ParquetWriter
- disk-rewrite: Write with DuckDB, then rewrite row-group by row-group with PyArrow

Strategies are only used when metadata rewrite is needed (GeoParquet 1.0, 1.1).
For parquet-geo-only and some 2.0 operations, a plain DuckDB COPY TO is used
without any strategy.

Usage:
    from geoparquet_io.core.write_strategies import (
        WriteStrategy,
        WriteStrategyFactory,
    )

    # Get a specific strategy
    strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DUCKDB_KV)
    strategy.write_from_query(con, query, output_path, ...)
"""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

from .base import (
    BaseWriteStrategy,
    WriteStrategy,
    atomic_write,
    needs_metadata_rewrite,
)

if TYPE_CHECKING:
    pass


class WriteStrategyFactory:
    """Factory for creating write strategies."""

    _strategy_classes: dict = {}
    _initialized = False

    @classmethod
    def _ensure_initialized(cls) -> None:
        """Lazily initialize strategy classes to avoid circular imports."""
        if cls._initialized:
            return

        from .arrow_memory import ArrowMemoryStrategy
        from .arrow_streaming import ArrowStreamingStrategy
        from .disk_rewrite import DiskRewriteStrategy
        from .duckdb_kv import DuckDBKVStrategy

        cls._strategy_classes = {
            WriteStrategy.ARROW_MEMORY: ArrowMemoryStrategy,
            WriteStrategy.ARROW_STREAMING: ArrowStreamingStrategy,
            WriteStrategy.DUCKDB_KV: DuckDBKVStrategy,
            WriteStrategy.DISK_REWRITE: DiskRewriteStrategy,
        }
        cls._initialized = True

    @classmethod
    @lru_cache(maxsize=4)
    def get_strategy(cls, strategy: WriteStrategy) -> BaseWriteStrategy:
        """
        Get a strategy instance by enum value.

        Instances are cached for reuse.

        Args:
            strategy: Strategy enum value

        Returns:
            Strategy instance
        """
        cls._ensure_initialized()
        return cls._strategy_classes[strategy]()

    @classmethod
    def list_strategies(cls) -> list[str]:
        """
        List available strategy names.

        Returns:
            List of strategy value strings
        """
        return [s.value for s in WriteStrategy]

    @classmethod
    def clear_cache(cls) -> None:
        """
        Clear the strategy instance cache.

        Useful for testing.
        """
        cls.get_strategy.cache_clear()


__all__ = [
    "WriteStrategy",
    "WriteStrategyFactory",
    "BaseWriteStrategy",
    "atomic_write",
    "needs_metadata_rewrite",
]
