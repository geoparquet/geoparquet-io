"""
Write strategy module for GeoParquet output.

This module provides a pluggable architecture for different write strategies,
each with different memory and performance characteristics:

- in-memory: Load entire dataset into memory, apply metadata, write once
- streaming: Stream Arrow RecordBatches directly to ParquetWriter
- duckdb-kv: Use DuckDB COPY TO with native KV_METADATA for geo metadata
- disk-rewrite: Write with DuckDB, then rewrite row-group by row-group with PyArrow

Usage:
    from geoparquet_io.core.write_strategies import (
        WriteStrategy,
        WriteContext,
        WriteStrategyFactory,
    )

    # Auto-select based on context
    context = WriteContext(estimated_bytes=1_000_000_000, is_remote=False)
    strategy = WriteStrategyFactory.select_strategy(context)
    strategy.write_from_query(con, query, output_path, ...)

    # Explicitly choose a strategy
    strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DUCKDB_KV)
"""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

from .base import (
    BaseWriteStrategy,
    WriteContext,
    WriteStrategy,
    atomic_write,
    needs_metadata_rewrite,
)

if TYPE_CHECKING:
    pass

# Memory threshold constants for auto-selection
# Lowered from 75% to 50% for concurrent workload safety
MEMORY_THRESHOLD_RATIO = 0.50
MEMORY_RESERVED_BUFFER_MB = 512


def _get_available_memory() -> int | None:
    """
    Get available system memory in bytes.

    Returns:
        Available memory in bytes, or None if unavailable
    """
    try:
        import psutil

        return psutil.virtual_memory().available
    except ImportError:
        return None


class WriteStrategyFactory:
    """Factory for creating and selecting write strategies."""

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
            strategy: Strategy enum value (not AUTO)

        Returns:
            Strategy instance

        Raises:
            ValueError: If AUTO is passed (use select_strategy instead)
        """
        if strategy == WriteStrategy.AUTO:
            raise ValueError("Use select_strategy() for AUTO")

        cls._ensure_initialized()
        return cls._strategy_classes[strategy]()

    @classmethod
    def select_strategy(cls, context: WriteContext) -> BaseWriteStrategy:
        """
        Auto-select the best strategy for the given context.

        Selection logic:
        1. If no metadata rewrite needed, use in-memory (fastest)
        2. For remote output, prefer in-memory (simpler temp file handling)
        3. For large files exceeding memory threshold, use duckdb-kv
        4. Default to in-memory for smaller files

        Args:
            context: Write context with file and system information

        Returns:
            Appropriate strategy instance for the context
        """
        cls._ensure_initialized()

        if not context.needs_metadata_rewrite:
            return cls.get_strategy(WriteStrategy.ARROW_MEMORY)

        if context.is_remote:
            return cls.get_strategy(WriteStrategy.ARROW_MEMORY)

        if context.estimated_bytes and context.available_memory_bytes:
            available = context.available_memory_bytes - (MEMORY_RESERVED_BUFFER_MB * 1024 * 1024)
            threshold = available * MEMORY_THRESHOLD_RATIO

            if context.estimated_bytes > threshold:
                return cls.get_strategy(WriteStrategy.DUCKDB_KV)

        return cls.get_strategy(WriteStrategy.ARROW_MEMORY)

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
    "WriteContext",
    "WriteStrategyFactory",
    "BaseWriteStrategy",
    "atomic_write",
    "needs_metadata_rewrite",
    "MEMORY_THRESHOLD_RATIO",
    "MEMORY_RESERVED_BUFFER_MB",
]
