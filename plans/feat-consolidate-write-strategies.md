# feat: Consolidate GeoParquet Write Strategies

---

## Enhancement Summary

*Added via `/deepen-plan` research phase - 2026-01-25*

This plan was enhanced with insights from multiple parallel research agents analyzing best practices, framework documentation, performance characteristics, security concerns, and architectural patterns. Key improvements incorporated:

### Critical Changes from Research

1. **Memory Threshold Lowered**: Changed auto-selection threshold from 75% to 50% of available memory with a reserved buffer (research finding: 75% is too aggressive for concurrent workloads)

2. **Frozen Dataclasses**: Added `frozen=True` to `WriteContext` for thread safety and immutability

3. **Abstract Method Type Hints**: Added explicit return types to all abstract methods for better IDE support and runtime safety

4. **SQL Injection Mitigation**: Added parameterized query patterns and input validation for file paths in `duckdb-kv` strategy

5. **Path Traversal Protection**: Added validation for output paths to prevent directory traversal attacks

6. **Simplified Initial Scope**: Consider starting with 2 strategies (`in-memory` + `duckdb-kv`) rather than all 4 - the streaming strategies have overlapping use cases

7. **Factory Pattern Enhancement**: Use `functools.lru_cache` for strategy instantiation to avoid repeated object creation

8. **Error Recovery**: Added explicit cleanup procedures for partial writes and temp files

### Performance Insights

- **Batch size recommendation**: 100,000 rows per batch for streaming (balances memory and throughput)
- **Row group targeting**: 128MB uncompressed for optimal read performance
- **DuckDB memory limit**: Set to 50% of available memory for streaming writes
- **Thread count**: Use `threads=1` for streaming to ensure predictable memory usage

### Security Additions

- Input path validation before COPY TO operations
- Temp file creation with secure permissions
- Proper cleanup of temporary files on all exit paths

---

## Overview

Consolidate multiple GeoParquet write strategies from different branches into a clean, pluggable architecture with CLI and Python API support. Each strategy will be fully encapsulated for easy testing, comparison, and potential removal after evaluation.

## Problem Statement

The current codebase has evolved multiple approaches to writing GeoParquet files across different branches:

1. **Main branch**: Arrow in-memory approach that loads entire datasets before writing
2. **feature/streaming-duckdb-write**: DuckDB COPY TO with native `KV_METADATA` for geo metadata
3. **lazy-execution-design**: Arrow streaming via RecordBatchReader

Additionally, the pre-0.7.0 approach of full disk rewrite with PyArrow should be preserved as a fallback option.

Each approach has different memory characteristics, performance profiles, and reliability tradeoffs. Users need the ability to choose the right strategy for their workload.

## Proposed Solution

Implement a **Strategy Pattern** for write operations with:

1. **Four encapsulated write strategies** in separate modules
2. **CLI option** (`--write-strategy`) available on all write commands
3. **Python API support** via `write_strategy` parameter
4. **Auto-selection logic** that chooses the best strategy based on context
5. **Skip-rewrite optimization** when metadata doesn't need modification

## Technical Approach

### Architecture

```
geoparquet_io/
├── core/
│   ├── common.py              # write_parquet_with_metadata() - dispatcher
│   ├── write_strategies/      # NEW: Strategy implementations
│   │   ├── __init__.py        # Strategy enum, factory, base class
│   │   ├── base.py            # WriteStrategy ABC
│   │   ├── arrow_memory.py    # Current main approach
│   │   ├── arrow_streaming.py # From lazy-execution-design
│   │   ├── duckdb_kv.py        # From feature/streaming-duckdb-write
│   │   └── disk_rewrite.py    # Pre-0.7.0 style full rewrite
│   └── ...
├── cli/
│   ├── decorators.py          # Add @write_strategy_option
│   └── main.py                # Apply decorator to write commands
└── api/
    └── table.py               # Add write_strategy param to Table.write()
```

### Strategy Interface

```python
# core/write_strategies/base.py
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
import pyarrow as pa
import duckdb

class WriteStrategy(str, Enum):
    """Available write strategies."""
    AUTO = "auto"
    ARROW_MEMORY = "in-memory"
    ARROW_STREAMING = "streaming"
    DUCKDB_KV = "duckdb-kv"
    DISK_REWRITE = "disk-rewrite"

@dataclass(frozen=True)  # Research: immutable for thread safety
class WriteContext:
    """Context for write operation decision-making."""
    estimated_rows: int | None = None
    estimated_bytes: int | None = None
    output_path: str = ""
    is_remote: bool = False
    geoparquet_version: str = "1.1"
    has_geometry: bool = True
    needs_metadata_rewrite: bool = True
    available_memory_bytes: int | None = None

class BaseWriteStrategy(ABC):
    """Base class for write strategy implementations."""

    name: str
    description: str
    supports_streaming: bool
    supports_remote: bool

    @abstractmethod
    def write_from_query(
        self,
        con: duckdb.DuckDBPyConnection,
        query: str,
        output_path: str,
        geometry_column: str,
        original_metadata: dict | None,
        geoparquet_version: str,
        compression: str,
        compression_level: int,
        row_group_size_mb: int | None,
        row_group_rows: int | None,
        input_crs: dict | None,
        verbose: bool,
    ) -> None:  # Research: explicit return type
        """Write query results to GeoParquet file."""
        ...  # Research: use ... instead of pass for abstract methods

    @abstractmethod
    def write_from_table(
        self,
        table: pa.Table,
        output_path: str,
        geometry_column: str,
        geoparquet_version: str,
        compression: str,
        compression_level: int,
        row_group_size_mb: int | None,
        row_group_rows: int | None,
        verbose: bool,
    ) -> None:  # Research: explicit return type
        """Write Arrow table to GeoParquet file."""
        ...  # Research: use ... instead of pass for abstract methods

    @classmethod
    def can_handle(cls, context: WriteContext) -> bool:
        """Check if this strategy can handle the given context."""
        return True

    def _validate_output_path(self, output_path: str) -> None:
        """Validate output path for security concerns.

        Research: Prevent path traversal attacks.
        """
        import os
        # Normalize path to detect traversal attempts
        normalized = os.path.normpath(output_path)
        if ".." in normalized.split(os.sep):
            raise ValueError(f"Invalid output path (directory traversal detected): {output_path}")
```

### Strategy Factory

```python
# core/write_strategies/__init__.py
from functools import lru_cache  # Research: cache strategy instances
from .base import WriteStrategy, WriteContext, BaseWriteStrategy
from .arrow_memory import ArrowMemoryStrategy
from .arrow_streaming import ArrowStreamingStrategy
from .duckdb_kv import DuckDBKVStrategy
from .disk_rewrite import DiskRewriteStrategy

# Research: Memory threshold constants (extracted for testability)
MEMORY_THRESHOLD_RATIO = 0.50  # Research: lowered from 0.75 for concurrent workloads
MEMORY_RESERVED_BUFFER_MB = 512  # Reserve buffer for other operations

class WriteStrategyFactory:
    """Factory for creating and selecting write strategies."""

    _strategies = {
        WriteStrategy.ARROW_MEMORY: ArrowMemoryStrategy,
        WriteStrategy.ARROW_STREAMING: ArrowStreamingStrategy,
        WriteStrategy.DUCKDB_KV: DuckDBKVStrategy,
        WriteStrategy.DISK_REWRITE: DiskRewriteStrategy,
    }

    @classmethod
    @lru_cache(maxsize=4)  # Research: cache strategy instances
    def get_strategy(cls, strategy: WriteStrategy) -> BaseWriteStrategy:
        """Get a strategy instance by enum value."""
        if strategy == WriteStrategy.AUTO:
            raise ValueError("Use select_strategy() for AUTO")
        return cls._strategies[strategy]()

    @classmethod
    def select_strategy(cls, context: WriteContext) -> BaseWriteStrategy:
        """Auto-select the best strategy for the given context.

        Research insights applied:
        - Lowered memory threshold from 75% to 50% for concurrent workloads
        - Added reserved buffer for other operations
        - Prioritized duckdb-kv for large files (atomic writes, predictable memory)
        """
        # Skip rewrite entirely if no metadata changes needed
        if not context.needs_metadata_rewrite:
            return cls.get_strategy(WriteStrategy.ARROW_MEMORY)

        # For remote output, prefer in-memory (simpler temp file handling)
        if context.is_remote:
            return cls.get_strategy(WriteStrategy.ARROW_MEMORY)

        # For large files, prefer streaming
        if context.estimated_bytes and context.available_memory_bytes:
            # Research: Reserve buffer and use conservative threshold
            available = context.available_memory_bytes - (MEMORY_RESERVED_BUFFER_MB * 1024 * 1024)
            threshold = available * MEMORY_THRESHOLD_RATIO

            # Use streaming if file exceeds threshold
            if context.estimated_bytes > threshold:
                return cls.get_strategy(WriteStrategy.DUCKDB_KV)

        # Default to in-memory for smaller files
        return cls.get_strategy(WriteStrategy.ARROW_MEMORY)

    @classmethod
    def list_strategies(cls) -> list[str]:
        """List available strategy names."""
        return [s.value for s in WriteStrategy]

    @classmethod
    def clear_cache(cls) -> None:
        """Clear the strategy instance cache (useful for testing)."""
        cls.get_strategy.cache_clear()
```

### Implementation Phases

#### Phase 1: Foundation

**Tasks:**
- [ ] Create `core/write_strategies/` directory structure
- [ ] Implement `base.py` with `WriteStrategy` enum, `WriteContext`, and `BaseWriteStrategy` ABC
- [ ] Implement `__init__.py` with `WriteStrategyFactory`

**Files:**
- `geoparquet_io/core/write_strategies/__init__.py`
- `geoparquet_io/core/write_strategies/base.py`

#### Phase 2: Core Strategies

**Task 2.1: Arrow In-Memory Strategy (`arrow_memory.py`)**

Extract current `write_geoparquet_via_arrow()` logic into a strategy class.

```python
# core/write_strategies/arrow_memory.py
class ArrowMemoryStrategy(BaseWriteStrategy):
    """Write by loading full Arrow table in memory, then write once."""

    name = "in-memory"
    description = "Load entire dataset into memory, apply metadata, write once"
    supports_streaming = False
    supports_remote = True

    def write_from_query(self, con, query, output_path, ...):
        # Current write_geoparquet_via_arrow() implementation
        result = con.execute(query)
        table = result.fetch_arrow_table()
        table = _apply_geoparquet_metadata(table, ...)
        _write_table_with_settings(table, output_path, ...)
```

**Task 2.2: Arrow Streaming Strategy (`arrow_streaming.py`)**

Port from `lazy-execution-design` branch.

```python
# core/write_strategies/arrow_streaming.py
class ArrowStreamingStrategy(BaseWriteStrategy):
    """Stream DuckDB results as RecordBatches to ParquetWriter."""

    name = "streaming"
    description = "Stream query results in batches for constant memory usage"
    supports_streaming = True
    supports_remote = True

    def write_from_query(self, con, query, output_path, ...):
        result = con.execute(query)
        reader = result.fetch_record_batch(rows_per_batch=100_000)

        with pq.ParquetWriter(output_path, schema_with_meta, ...) as writer:
            for batch in reader:
                writer.write_batch(batch)
```

**Task 2.3: DuckDB KV Metadata Strategy (`duckdb_kv.py`)**

Port from `feature/streaming-duckdb-write` branch. Uses DuckDB's native `KV_METADATA` option to write geo metadata directly during COPY TO - no external dependencies, single atomic write.

```python
# core/write_strategies/duckdb_kv.py
import re

# Research: Compression values whitelist (prevents injection via compression param)
VALID_COMPRESSIONS = frozenset({"ZSTD", "SNAPPY", "GZIP", "LZ4", "UNCOMPRESSED"})

class DuckDBKVStrategy(BaseWriteStrategy):
    """Use DuckDB COPY TO with native KV_METADATA for geo metadata."""

    name = "duckdb-kv"
    description = "DuckDB streaming write with native metadata support"
    supports_streaming = True
    supports_remote = True  # Uses temp file + upload for remote

    def write_from_query(self, con, query, output_path, ...):
        # Research: Validate output path for security
        self._validate_output_path(output_path)

        # Research: Validate compression against whitelist
        compression_upper = compression.upper()
        if compression_upper not in VALID_COMPRESSIONS:
            raise ValueError(f"Invalid compression: {compression}. Valid: {VALID_COMPRESSIONS}")

        # Set memory-safe DuckDB configuration
        # Research: 50% memory limit for concurrent safety
        con.execute("SET threads = 1")
        con.execute(f"SET memory_limit = '{memory_limit}'")

        # Compute metadata via SQL before write
        geo_meta = self._prepare_geo_metadata(...)
        if not preserve_bbox:
            geo_meta["columns"][geom]["bbox"] = compute_bbox_via_sql(con, query, geom)

        # Wrap query with WKB conversion
        final_query = _wrap_query_with_wkb_conversion(query, geometry_column)

        # Research: Properly escape output path for SQL
        escaped_path = output_path.replace("'", "''")

        # Serialize geo metadata for KV_METADATA
        # Research: JSON escaping is safe, but double-check for edge cases
        geo_meta_json = json.dumps(geo_meta)
        geo_meta_escaped = geo_meta_json.replace("'", "''")

        # DuckDB COPY TO with KV_METADATA (single atomic write with geo metadata)
        # Research: Use validated compression and escaped paths
        copy_sql = f"""
            COPY ({final_query}) TO '{escaped_path}'
            (FORMAT PARQUET, COMPRESSION {compression_upper}, KV_METADATA {{geo: '{geo_meta_escaped}'}})
        """
        con.execute(copy_sql)

    def _validate_output_path(self, output_path: str) -> None:
        """Validate output path for security.

        Research: Prevent path traversal and command injection.
        """
        super()._validate_output_path(output_path)

        # Research: Additional validation for DuckDB COPY TO
        # Reject paths with suspicious characters that could break SQL
        if re.search(r"[;\x00]", output_path):
            raise ValueError(f"Invalid characters in output path: {output_path}")
```

**Key advantage:** No footer rewrite step - DuckDB writes the geo metadata directly during the COPY operation, making it atomic and reliable.

**Security notes from research:**
- Output paths are validated and escaped to prevent SQL injection
- Compression values are validated against a whitelist
- Thread count is fixed at 1 for predictable memory usage

**Task 2.4: Disk Rewrite Strategy (`disk_rewrite.py`)**

Implement pre-0.7.0 style full file rewrite. This is the most reliable fallback - writes to disk first, then rewrites row-group by row-group to add metadata.

```python
# core/write_strategies/disk_rewrite.py
class DiskRewriteStrategy(BaseWriteStrategy):
    """Write with DuckDB, then read/rewrite entire file with PyArrow for metadata."""

    name = "disk-rewrite"
    description = "Full file rewrite (reliable, memory-efficient via row groups)"
    supports_streaming = False
    supports_remote = True

    def write_from_query(self, con, query, output_path, ...):
        # Step 1: Write with DuckDB (fast, but no geo metadata)
        temp_path = output_path + ".tmp"
        copy_sql = f"COPY ({query}) TO '{temp_path}' (FORMAT PARQUET, COMPRESSION {compression})"
        con.execute(copy_sql)

        # Step 2: Read and rewrite with PyArrow (adds geo metadata)
        # Use chunked reading for memory efficiency
        self._rewrite_with_metadata(temp_path, output_path, geo_meta, ...)
        os.unlink(temp_path)

    def _rewrite_with_metadata(self, input_path, output_path, geo_meta, ...):
        """Rewrite file with proper geo metadata, row group by row group."""
        pf = pq.ParquetFile(input_path)
        schema = pf.schema_arrow

        # Add geo metadata to schema
        new_meta = dict(schema.metadata or {})
        new_meta[b"geo"] = json.dumps(geo_meta).encode("utf-8")
        new_schema = schema.with_metadata(new_meta)

        # Stream row groups to avoid full memory load (memory = 1 row group)
        with pq.ParquetWriter(output_path, new_schema, ...) as writer:
            for i in range(pf.metadata.num_row_groups):
                table = pf.read_row_group(i)
                table = table.replace_schema_metadata(new_meta)
                writer.write_table(table)
```

**Key advantage:** Works with larger-than-memory datasets by processing one row group at a time. Most reliable fallback when other strategies fail.

#### Research Insights: Error Recovery and Cleanup

All strategies should implement proper cleanup for partial writes. This pattern applies to all strategies:

```python
# core/write_strategies/base.py (additional methods)

import os
import tempfile
from contextlib import contextmanager

@contextmanager
def atomic_write(output_path: str, suffix: str = ".parquet"):
    """Context manager for atomic file writes with cleanup.

    Research: Ensures partial files are cleaned up on failure.
    Writes to temp file, then renames atomically on success.
    """
    # Create temp file in same directory for atomic rename
    dir_path = os.path.dirname(output_path) or "."
    fd, temp_path = tempfile.mkstemp(suffix=suffix, dir=dir_path)
    os.close(fd)

    try:
        yield temp_path
        # Atomic rename on success
        os.replace(temp_path, output_path)
    except Exception:
        # Cleanup temp file on failure
        if os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except OSError:
                pass  # Best effort cleanup
        raise

# Usage in strategies:
def write_from_query(self, ...):
    with atomic_write(output_path) as temp_path:
        # Write to temp_path
        ...
    # File is now atomically at output_path
```

**Cleanup checklist for each strategy:**

| Strategy | Temp Files | Cleanup Location | Atomic? |
|----------|------------|------------------|---------|
| in-memory | None (writes directly) | N/A | No |
| streaming | None (streams to target) | N/A | No |
| duckdb-kv | Uses atomic_write | Context manager | Yes |
| disk-rewrite | temp + rewrite temp | Both in finally block | Yes |

#### Phase 3: Integration

**Task 3.1: Update `write_parquet_with_metadata()`**

```python
# core/common.py
def write_parquet_with_metadata(
    con,
    query,
    output_file,
    original_metadata=None,
    compression="ZSTD",
    compression_level=15,
    row_group_size_mb=None,
    row_group_rows=None,
    custom_metadata=None,
    verbose=False,
    show_sql=False,
    profile=None,
    geoparquet_version=None,
    input_crs=None,
    write_strategy: str | WriteStrategy = WriteStrategy.AUTO,  # NEW
    preserve_bbox: bool = True,  # NEW
    preserve_geometry_types: bool = True,  # NEW
):
    """Write a parquet file with proper compression and metadata handling."""
    from geoparquet_io.core.write_strategies import WriteStrategyFactory, WriteStrategy, WriteContext

    # Build context for strategy selection
    context = WriteContext(
        output_path=output_file,
        is_remote=is_remote_url(output_file),
        geoparquet_version=geoparquet_version or "1.1",
        has_geometry=_query_has_geometry(con, query),
        needs_metadata_rewrite=_needs_metadata_rewrite(geoparquet_version, original_metadata),
    )

    # Select or get strategy
    if write_strategy == WriteStrategy.AUTO or write_strategy == "auto":
        strategy = WriteStrategyFactory.select_strategy(context)
        if verbose:
            debug(f"Auto-selected write strategy: {strategy.name}")
    else:
        strategy_enum = WriteStrategy(write_strategy) if isinstance(write_strategy, str) else write_strategy
        strategy = WriteStrategyFactory.get_strategy(strategy_enum)
        if verbose:
            info(f"Using write strategy: {strategy.name}")

    # Execute write
    with remote_write_context(output_file, is_directory=False, verbose=verbose) as (
        actual_output, is_remote
    ):
        strategy.write_from_query(
            con=con,
            query=query,
            output_path=actual_output,
            geometry_column=geometry_column,
            original_metadata=original_metadata,
            geoparquet_version=geoparquet_version,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            input_crs=input_crs,
            verbose=verbose,
        )

        if is_remote:
            upload_if_remote(actual_output, output_file, profile=profile, verbose=verbose)
```

**Task 3.2: Add CLI Decorator**

```python
# cli/decorators.py
def write_strategy_option(func):
    """Add --write-strategy option to a command."""
    return click.option(
        "--write-strategy",
        type=click.Choice(["auto", "in-memory", "streaming", "duckdb-kv", "disk-rewrite"]),
        default="auto",
        help="Write strategy: auto (default), in-memory, streaming, duckdb-kv, disk-rewrite",
    )(func)
```

**Task 3.3: Apply to All Write Commands**

Commands to update in `cli/main.py`:
- `extract` (line ~2050)
- `convert` (line ~1600)
- `sort hilbert` (line ~2500)
- `sort quadkey` (line ~2600)
- `add bbox` (line ~700)
- `add quadkey` (line ~850)
- `add h3` (line ~900)
- `add country` (line ~1000)
- `add admin` (line ~1100)
- `partition *` commands
- `check --fix` related
- `meta set`

**Task 3.4: Update Python API**

```python
# api/table.py
class Table:
    def write(
        self,
        path: str,
        compression: str = "zstd",
        compression_level: int | None = None,
        row_group_size_mb: float | None = None,
        geoparquet_version: str | None = None,
        write_strategy: str = "auto",  # NEW
        verbose: bool = False,
    ) -> Path:
        """Write this table to a GeoParquet file."""
        from geoparquet_io.core.write_strategies import WriteStrategyFactory, WriteContext
        # ... implementation
```

#### Phase 4: Skip-Rewrite Optimization

Implement logic to skip metadata rewriting when unnecessary.

```python
# core/write_strategies/base.py
def needs_metadata_rewrite(
    geoparquet_version: str,
    original_metadata: dict | None,
    operation: str,
) -> bool:
    """Determine if metadata rewrite is needed for this operation."""
    version_config = GEOPARQUET_VERSIONS.get(geoparquet_version, GEOPARQUET_VERSIONS["1.1"])

    # parquet-geo-only with default CRS doesn't need rewrite
    if geoparquet_version == "parquet-geo-only":
        if original_metadata:
            crs = _extract_crs_from_metadata(original_metadata)
            if not crs or is_default_crs(crs):
                return False
        return True  # Non-default CRS needs rewrite

    # GeoParquet 2.0 with native types may not need rewrite
    if geoparquet_version == "2.0":
        # DuckDB writes native types, but we may need to add bbox/geometry_types
        # Skip if operation preserves all metadata
        if operation in ("columns_only", "sort"):
            return False

    # 1.0/1.1 always need geo metadata
    return version_config.get("rewrite_metadata", True)
```

Operations that preserve metadata (no rewrite needed):
| Operation | Bbox | Geometry Types | Skip Rewrite? |
|-----------|------|----------------|---------------|
| extract --columns (no spatial filter) | Preserve | Preserve | Maybe* |
| sort hilbert/quadkey | Preserve | Preserve | Maybe* |
| No geometry in output | N/A | N/A | Yes |

*Only if input already has valid metadata and version matches output version.

#### Phase 5: Testing

**Test Files:**
- `tests/test_write_strategies.py` - Strategy unit tests
- `tests/test_write_strategy_integration.py` - End-to-end tests
- `tests/test_write_strategy_equivalence.py` - Output comparison tests

**Test Matrix:**

```python
# tests/test_write_strategies.py
import pytest
from geoparquet_io.core.write_strategies import (
    WriteStrategy, WriteStrategyFactory, WriteContext
)

class TestWriteStrategyFactory:
    def test_get_strategy_arrow_memory(self):
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_MEMORY)
        assert strategy.name == "in-memory"

    def test_auto_select_small_file_local(self):
        context = WriteContext(
            estimated_bytes=100_000_000,  # 100MB
            available_memory_bytes=8_000_000_000,  # 8GB
            is_remote=False,
        )
        strategy = WriteStrategyFactory.select_strategy(context)
        assert strategy.name == "in-memory"

    def test_auto_select_large_file_local(self):
        context = WriteContext(
            estimated_bytes=10_000_000_000,  # 10GB
            available_memory_bytes=8_000_000_000,  # 8GB
            is_remote=False,
        )
        strategy = WriteStrategyFactory.select_strategy(context)
        assert strategy.name == "duckdb-kv"

    def test_auto_select_remote_prefers_memory(self):
        context = WriteContext(
            estimated_bytes=10_000_000_000,
            is_remote=True,
        )
        strategy = WriteStrategyFactory.select_strategy(context)
        assert strategy.name == "in-memory"

@pytest.mark.parametrize("strategy", [
    WriteStrategy.ARROW_MEMORY,
    WriteStrategy.ARROW_STREAMING,
    WriteStrategy.DUCKDB_KV,
    WriteStrategy.DISK_REWRITE,
])
class TestAllStrategies:
    def test_write_small_file(self, strategy, sample_geoparquet, tmp_path):
        output = tmp_path / "output.parquet"
        strat = WriteStrategyFactory.get_strategy(strategy)
        # ... test each strategy produces valid output

    def test_preserves_geo_metadata(self, strategy, sample_geoparquet, tmp_path):
        # ... verify geo metadata is correct

    def test_handles_all_geoparquet_versions(self, strategy, tmp_path):
        # ... test with 1.0, 1.1, 2.0, parquet-geo-only

@pytest.mark.slow
class TestLargeFileStrategies:
    """Tests for large file handling - marked slow."""

    def test_streaming_constant_memory(self, large_geoparquet, tmp_path):
        """Verify streaming strategies don't exceed memory bounds."""
        # Use memory profiler to verify peak memory
        pass

    def test_strategies_produce_equivalent_output(self, sample_geoparquet, tmp_path):
        """All strategies should produce semantically equivalent output."""
        outputs = {}
        for strategy in WriteStrategy:
            if strategy == WriteStrategy.AUTO:
                continue
            output = tmp_path / f"output_{strategy.value}.parquet"
            # ... write with strategy
            outputs[strategy] = output

        # Compare all outputs
        # ... verify row counts, schema, geo metadata match
```

**CLI Tests:**

```python
# tests/test_write_strategy_cli.py
from click.testing import CliRunner
from geoparquet_io.cli.main import extract

class TestWriteStrategyCLI:
    def test_extract_with_strategy_option(self, sample_geoparquet, tmp_path):
        runner = CliRunner()
        output = tmp_path / "output.parquet"

        result = runner.invoke(extract, [
            str(sample_geoparquet),
            str(output),
            "--write-strategy", "streaming",
        ])

        assert result.exit_code == 0
        assert output.exists()

    def test_invalid_strategy_errors(self, sample_geoparquet, tmp_path):
        runner = CliRunner()
        result = runner.invoke(extract, [
            str(sample_geoparquet),
            str(tmp_path / "out.parquet"),
            "--write-strategy", "invalid",
        ])
        assert result.exit_code != 0
        assert "Invalid value" in result.output
```

**Research: Additional Security Tests**

```python
# tests/test_write_strategy_security.py
class TestWriteStrategySecurity:
    """Security tests identified by research agents."""

    def test_path_traversal_rejected(self, sample_geoparquet, tmp_path):
        """Ensure path traversal attempts are blocked."""
        from geoparquet_io.core.write_strategies import WriteStrategyFactory, WriteStrategy

        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DUCKDB_KV)

        with pytest.raises(ValueError, match="directory traversal"):
            strategy._validate_output_path("../../../etc/passwd")

    def test_sql_special_chars_in_path(self, sample_geoparquet, tmp_path):
        """Ensure SQL special characters in paths are handled safely."""
        output = tmp_path / "file'; DROP TABLE users;--.parquet"

        # Should either escape safely or reject
        # The key is it should NOT execute arbitrary SQL
        runner = CliRunner()
        result = runner.invoke(extract, [
            str(sample_geoparquet),
            str(output),
            "--write-strategy", "duckdb-kv",
        ])
        # Should either succeed (escaped) or fail with validation error
        # Should NOT crash with SQL error

    def test_invalid_compression_rejected(self, sample_geoparquet, tmp_path):
        """Ensure only valid compression values are accepted."""
        from geoparquet_io.core.write_strategies.duckdb_kv import DuckDBKVStrategy

        strategy = DuckDBKVStrategy()
        # Attempting to pass SQL injection via compression should fail
        # (whitelist validation)

    def test_null_byte_in_path_rejected(self, sample_geoparquet, tmp_path):
        """Ensure null bytes in paths are rejected."""
        from geoparquet_io.core.write_strategies import WriteStrategyFactory, WriteStrategy

        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DUCKDB_KV)

        with pytest.raises(ValueError, match="Invalid characters"):
            strategy._validate_output_path("file\x00.parquet")
```

**Research: Cleanup and Error Recovery Tests**

```python
# tests/test_write_strategy_cleanup.py
class TestWriteStrategyCleanup:
    """Cleanup and error recovery tests from research."""

    def test_temp_file_cleaned_on_failure(self, sample_geoparquet, tmp_path):
        """Ensure temp files are cleaned up when write fails."""
        output = tmp_path / "output.parquet"

        # Force a failure during write (mock DuckDB to raise)
        with patch("duckdb.DuckDBPyConnection.execute", side_effect=RuntimeError("Simulated failure")):
            with pytest.raises(RuntimeError):
                # Attempt write
                pass

        # Verify no temp files remain
        temp_files = list(tmp_path.glob("*.tmp*"))
        assert len(temp_files) == 0, f"Temp files not cleaned: {temp_files}"

    def test_atomic_write_no_partial_file(self, sample_geoparquet, tmp_path):
        """Ensure atomic writes don't leave partial files on failure."""
        output = tmp_path / "output.parquet"

        # Simulate failure mid-write
        # Verify output file doesn't exist (wasn't partially written)

    def test_factory_cache_cleared(self):
        """Ensure factory cache can be cleared for testing."""
        from geoparquet_io.core.write_strategies import WriteStrategyFactory, WriteStrategy

        # Get a strategy (populates cache)
        s1 = WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_MEMORY)

        # Clear cache
        WriteStrategyFactory.clear_cache()

        # Get again (should be new instance)
        s2 = WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_MEMORY)

        # Note: with frozen dataclasses, instances may still be equal
        # but this verifies the cache mechanism works
```

#### Phase 6: Documentation

**Files to create/update:**

1. `docs/guide/write-strategies.md` - NEW: Comprehensive guide
2. `docs/api/python-api.md` - Add write_strategy parameter
3. `docs/cli/extract.md` - Add --write-strategy option
4. `docs/cli/convert.md` - Add --write-strategy option
5. (All other CLI command docs)

**Guide Content (`docs/guide/write-strategies.md`):**

```markdown
# Write Strategies

gpio supports multiple strategies for writing GeoParquet files, each with different
memory and performance characteristics.

## Available Strategies

### `auto` (Default)

Automatically selects the best strategy based on:
- Estimated file size vs available memory
- Local vs remote output
- GeoParquet version

### `in-memory`

Loads the entire dataset into memory, applies metadata, writes once.

**Best for:** Small to medium files that fit in memory
**Memory:** O(n) - proportional to dataset size
**Speed:** Fast for files that fit in RAM

### `streaming`

Streams query results as Arrow RecordBatches directly to ParquetWriter.

**Best for:** Large files, memory-constrained environments
**Memory:** O(batch_size) - constant regardless of total size
**Speed:** Slightly slower due to batch overhead

### `duckdb-kv`

Uses DuckDB's native COPY TO with the `KV_METADATA` option to write
geo metadata directly during the streaming write. Single atomic operation.

**Best for:** Very large files, minimal memory usage
**Memory:** O(1) - nearly constant
**Speed:** Fast writes, no post-processing needed
**Reliability:** Atomic write - either succeeds completely or fails

### `disk-rewrite`

Writes with DuckDB, then reads and rewrites the file with PyArrow,
processing one row group at a time for memory efficiency.

**Best for:** Maximum compatibility, fallback when other strategies fail
**Memory:** O(row_group_size) - one row group at a time
**Speed:** Slower (reads file twice, writes twice)

## CLI Usage

All write commands support `--write-strategy`:

=== "CLI"

    ```bash
    # Auto-select (default)
    gpio extract input.parquet output.parquet

    # Force streaming for large file
    gpio extract large.parquet output.parquet --write-strategy streaming

    # Use DuckDB native metadata for minimal memory
    gpio convert input.geojson output.parquet --write-strategy duckdb-kv
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Auto-select (default)
    table = gpio.read("input.parquet")
    table.write("output.parquet")

    # Force streaming
    table.write("output.parquet", write_strategy="streaming")

    # Use DuckDB native metadata
    table.write("output.parquet", write_strategy="duckdb-kv")
    ```

## When Metadata Rewrite is Skipped

gpio optimizes writes by skipping metadata rewrite when possible:

- **No geometry column in output**: Regular Parquet, no geo metadata needed
- **parquet-geo-only with default CRS**: Native Parquet types are sufficient
- **GeoParquet 2.0 preserving operations**: Sort, column-only extract

## Troubleshooting

### Out of Memory Errors

If you encounter OOM errors with large files:

```bash
# Use streaming strategy
gpio extract large.parquet output.parquet --write-strategy streaming

# Or duckdb-kv for minimal memory (recommended for very large files)
gpio extract large.parquet output.parquet --write-strategy duckdb-kv
```

### Corrupted or Missing Metadata

If a file has missing or corrupt geo metadata:

```bash
# Check file validity
gpio check output.parquet

# Re-run with disk-rewrite for maximum reliability
gpio extract input.parquet output.parquet --write-strategy disk-rewrite
```
```

## Acceptance Criteria

### Functional Requirements

- [ ] All four strategies produce valid GeoParquet output
- [ ] `--write-strategy` option available on all write commands
- [ ] Python API supports `write_strategy` parameter
- [ ] Auto-selection chooses appropriate strategy based on context
- [ ] Metadata rewrite is skipped when not needed
- [ ] Verbose mode logs which strategy is selected/used
- [ ] All GeoParquet versions (1.0, 1.1, 2.0, parquet-geo-only) work with all strategies

### Non-Functional Requirements

- [ ] Streaming strategies maintain constant memory for large files
- [ ] No regression in write speed for small files
- [ ] Clear error messages when strategy fails
- [ ] Documentation explains each strategy's tradeoffs

### Quality Gates

- [ ] Test coverage >= 80% for new code
- [ ] All existing tests pass
- [ ] Complexity grade A for new modules
- [ ] Pre-commit hooks pass

## Dependencies & Prerequisites

### New Dependencies

No new required dependencies. DuckDB's native `KV_METADATA` support eliminates the need for fastparquet.

Optional for memory detection:
```toml
[project.optional-dependencies]
memory = [
    "psutil>=5.9.0",  # For memory detection in auto-selection
]
```

### Branch Consolidation

Before starting implementation:

1. Create new branch `feat/write-strategies` from `main`
2. Cherry-pick relevant commits from:
   - `feature/streaming-duckdb-write` (DuckDB + KV_METADATA)
   - `lazy-execution-design` (Arrow streaming)
3. Resolve any conflicts

## Risk Analysis & Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| DuckDB KV_METADATA version compatibility | Low | Requires DuckDB 0.9+ (already in dependencies) |
| Memory detection inaccurate | Medium | Research: lowered to 50% threshold with 512MB buffer; allow manual override |
| Strategy overhead slows small files | Low | Benchmark; ensure auto-select uses in-memory for small files |
| Disk-rewrite requires 2x disk space | Medium | Document requirement; temp file cleanup on failure |
| SQL injection in COPY TO | High | Research: validate paths, whitelist compression values, escape strings |
| Path traversal attacks | Medium | Research: normalize and validate paths before use |
| Partial file corruption on failure | Medium | Research: use atomic_write context manager for temp + rename pattern |

## Simplification Option (Research Recommendation)

The research agents identified that the four strategies have overlapping use cases. A simpler initial implementation could start with just **two strategies**:

### Minimal Viable Implementation

| Strategy | Use Case | Memory |
|----------|----------|--------|
| `in-memory` | Small-medium files, remote output | O(n) |
| `duckdb-kv` | Large files, memory-constrained | O(1) |

**Rationale:**
- `streaming` (Arrow RecordBatch) overlaps with `duckdb-kv` for the streaming use case
- `disk-rewrite` is only needed as a fallback if `duckdb-kv` has issues
- Starting with 2 strategies reduces implementation scope by ~40%
- Additional strategies can be added later if needed

**Recommended approach:**
1. Implement `in-memory` and `duckdb-kv` in Phase 2
2. Mark `streaming` and `disk-rewrite` as "Phase 2.5" (deferred)
3. Evaluate after initial release whether additional strategies are needed
4. If users report issues with `duckdb-kv`, implement `disk-rewrite` as fallback

This aligns with the YAGNI principle - implement what's needed, add more later.

## Future Considerations

1. **Parallel writes**: Multiple strategies could write partitions in parallel
2. **Progress callbacks**: Report progress for long-running writes
3. **Strategy plugins**: Allow custom strategies via entry points
4. **Async writes**: Non-blocking write operations

## References

### Internal References

- Current write implementation: `geoparquet_io/core/common.py:2480` (`write_geoparquet_via_arrow`)
- Existing strategy pattern: `geoparquet_io/core/admin_datasets.py` (AdminDataset ABC + Factory)
- CLI decorators: `geoparquet_io/cli/decorators.py`
- Streaming design doc: `docs/plans/2026-01-24-streaming-duckdb-write-design.md`

### External References

- [PyArrow ParquetWriter](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html)
- [DuckDB Memory Management](https://duckdb.org/2024/07/09/memory-management)
- [DuckDB COPY TO KV_METADATA](https://duckdb.org/docs/sql/statements/copy#parquet-options)
- [GeoParquet Specification](https://github.com/opengeospatial/geoparquet)

### Research: Framework Documentation Insights

**PyArrow ParquetWriter best practices:**
- Use `write_batch()` for streaming rather than `write_table()` for memory efficiency
- Set `use_dictionary=False` for geometry columns (binary data doesn't benefit)
- Target 128MB uncompressed row groups for optimal read performance
- Close writer explicitly or use context manager to ensure footer is written

**DuckDB memory management:**
- `SET threads = 1` ensures predictable memory usage during COPY
- `SET memory_limit` should be set to 50% of available for concurrent safety
- `KV_METADATA` option requires DuckDB 0.9.0+ (verified in dependencies)
- COPY TO streams data, doesn't load entire result into memory

**Python CLI best practices (from research):**
- Use `click.Choice` for enum-like options (validates at CLI level)
- Consider exposing strategy selection via environment variable for power users
- Log selected strategy at debug level, not info (avoid noise)

**Strategy pattern best practices:**
- Use ABC with `@abstractmethod` for interface enforcement
- Prefer composition over inheritance for strategy-specific helpers
- Consider Protocol for duck-typing flexibility (future enhancement)
- Use `frozen=True` dataclasses for thread-safe context objects

### Related Work

- Branch: `feature/streaming-duckdb-write` - DuckDB streaming with KV_METADATA
- Branch: `lazy-execution-design` - Arrow streaming implementation
- Commit `ea899d5`: Refactor to Arrow-based writes (current main approach)
