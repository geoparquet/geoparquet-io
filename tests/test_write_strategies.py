"""
Tests for write strategy implementations.

Tests the Strategy Pattern for GeoParquet writes including:
- Factory methods
- Auto-selection logic
- Individual strategy implementations
- Security validations
"""

import json
import tempfile
import uuid
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.write_strategies import (
    MEMORY_RESERVED_BUFFER_MB,
    MEMORY_THRESHOLD_RATIO,
    WriteContext,
    WriteStrategy,
    WriteStrategyFactory,
    atomic_write,
    needs_metadata_rewrite,
)


class TestWriteStrategy:
    """Tests for WriteStrategy enum."""

    def test_enum_values(self):
        """All expected strategy values exist."""
        assert WriteStrategy.AUTO.value == "auto"
        assert WriteStrategy.ARROW_MEMORY.value == "in-memory"
        assert WriteStrategy.ARROW_STREAMING.value == "streaming"
        assert WriteStrategy.DUCKDB_KV.value == "duckdb-kv"
        assert WriteStrategy.DISK_REWRITE.value == "disk-rewrite"

    def test_enum_from_string(self):
        """Enum can be created from string values."""
        assert WriteStrategy("auto") == WriteStrategy.AUTO
        assert WriteStrategy("in-memory") == WriteStrategy.ARROW_MEMORY
        assert WriteStrategy("streaming") == WriteStrategy.ARROW_STREAMING
        assert WriteStrategy("duckdb-kv") == WriteStrategy.DUCKDB_KV
        assert WriteStrategy("disk-rewrite") == WriteStrategy.DISK_REWRITE


class TestWriteContext:
    """Tests for WriteContext dataclass."""

    def test_defaults(self):
        """Context has correct defaults."""
        context = WriteContext()
        assert context.estimated_rows is None
        assert context.estimated_bytes is None
        assert context.output_path == ""
        assert context.is_remote is False
        assert context.geoparquet_version == "1.1"
        assert context.has_geometry is True
        assert context.needs_metadata_rewrite is True
        assert context.available_memory_bytes is None

    def test_immutability(self):
        """Context is immutable (frozen dataclass)."""
        context = WriteContext(output_path="test.parquet")
        with pytest.raises(AttributeError):
            context.output_path = "other.parquet"


class TestWriteStrategyFactory:
    """Tests for WriteStrategyFactory."""

    def test_get_strategy_arrow_memory(self):
        """Get in-memory strategy."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_MEMORY)
        assert strategy.name == "in-memory"
        assert strategy.supports_streaming is False
        assert strategy.supports_remote is True

    def test_get_strategy_streaming(self):
        """Get streaming strategy."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_STREAMING)
        assert strategy.name == "streaming"
        assert strategy.supports_streaming is True

    def test_get_strategy_duckdb_kv(self):
        """Get DuckDB KV strategy."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DUCKDB_KV)
        assert strategy.name == "duckdb-kv"
        assert strategy.supports_streaming is True

    def test_get_strategy_disk_rewrite(self):
        """Get disk rewrite strategy."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)
        assert strategy.name == "disk-rewrite"
        assert strategy.supports_streaming is False

    def test_get_strategy_auto_raises(self):
        """AUTO requires select_strategy instead."""
        with pytest.raises(ValueError, match="Use select_strategy"):
            WriteStrategyFactory.get_strategy(WriteStrategy.AUTO)

    def test_list_strategies(self):
        """List all available strategies."""
        strategies = WriteStrategyFactory.list_strategies()
        assert "auto" in strategies
        assert "in-memory" in strategies
        assert "streaming" in strategies
        assert "duckdb-kv" in strategies
        assert "disk-rewrite" in strategies

    def test_cache_clear(self):
        """Cache can be cleared."""
        WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_MEMORY)
        WriteStrategyFactory.clear_cache()


class TestAutoSelection:
    """Tests for automatic strategy selection."""

    def test_auto_select_small_file_local(self):
        """Small local file uses in-memory."""
        context = WriteContext(
            estimated_bytes=100_000_000,  # 100MB
            available_memory_bytes=8_000_000_000,  # 8GB
            is_remote=False,
        )
        strategy = WriteStrategyFactory.select_strategy(context)
        assert strategy.name == "in-memory"

    def test_auto_select_large_file_local(self):
        """Large local file uses duckdb-kv."""
        context = WriteContext(
            estimated_bytes=10_000_000_000,  # 10GB
            available_memory_bytes=8_000_000_000,  # 8GB
            is_remote=False,
        )
        strategy = WriteStrategyFactory.select_strategy(context)
        assert strategy.name == "duckdb-kv"

    def test_auto_select_remote_prefers_memory(self):
        """Remote output prefers in-memory."""
        context = WriteContext(
            estimated_bytes=10_000_000_000,
            is_remote=True,
        )
        strategy = WriteStrategyFactory.select_strategy(context)
        assert strategy.name == "in-memory"

    def test_auto_select_no_metadata_rewrite(self):
        """No metadata rewrite uses in-memory."""
        context = WriteContext(
            needs_metadata_rewrite=False,
            estimated_bytes=10_000_000_000,
            available_memory_bytes=1_000_000,
        )
        strategy = WriteStrategyFactory.select_strategy(context)
        assert strategy.name == "in-memory"

    def test_memory_threshold_calculation(self):
        """Memory threshold is properly calculated."""
        available = 8_000_000_000  # 8GB
        reserved = MEMORY_RESERVED_BUFFER_MB * 1024 * 1024
        threshold = (available - reserved) * MEMORY_THRESHOLD_RATIO

        # File just under threshold
        context = WriteContext(
            estimated_bytes=int(threshold * 0.9),
            available_memory_bytes=available,
            is_remote=False,
        )
        strategy = WriteStrategyFactory.select_strategy(context)
        assert strategy.name == "in-memory"

        # File just over threshold
        context = WriteContext(
            estimated_bytes=int(threshold * 1.1),
            available_memory_bytes=available,
            is_remote=False,
        )
        strategy = WriteStrategyFactory.select_strategy(context)
        assert strategy.name == "duckdb-kv"


class TestAtomicWrite:
    """Tests for atomic_write context manager."""

    def test_successful_write(self):
        """Successful write atomically renames file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.parquet"

            with atomic_write(str(output_path)) as temp_path:
                Path(temp_path).write_text("test content")

            assert output_path.exists()
            assert output_path.read_text() == "test content"

    def test_failed_write_cleanup(self):
        """Failed write cleans up temp file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.parquet"

            with pytest.raises(RuntimeError):
                with atomic_write(str(output_path)) as temp_path:
                    Path(temp_path).write_text("partial")
                    raise RuntimeError("Simulated failure")

            assert not output_path.exists()
            # Temp file should be cleaned up
            temp_files = list(Path(tmpdir).glob("*.parquet*"))
            assert len(temp_files) == 0


class TestNeedsMetadataRewrite:
    """Tests for needs_metadata_rewrite function."""

    def test_parquet_geo_only_no_rewrite(self):
        """parquet-geo-only doesn't need rewrite."""
        assert needs_metadata_rewrite("parquet-geo-only", None) is False

    def test_v1_needs_rewrite(self):
        """GeoParquet 1.x needs rewrite."""
        assert needs_metadata_rewrite("1.1", None) is True
        assert needs_metadata_rewrite("1.0", None) is True

    def test_v2_columns_only_no_rewrite(self):
        """GeoParquet 2.0 with columns_only operation skips rewrite."""
        assert needs_metadata_rewrite("2.0", None, "columns_only") is False

    def test_v2_sort_no_rewrite(self):
        """GeoParquet 2.0 with sort operation skips rewrite."""
        assert needs_metadata_rewrite("2.0", None, "sort") is False


@pytest.fixture
def sample_table():
    """Create a sample PyArrow table with geometry."""
    # Simple point geometries as WKB
    wkb_point = b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@"

    return pa.table(
        {
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "geometry": [wkb_point, wkb_point, wkb_point],
        }
    )


@pytest.fixture
def output_file():
    """Create temp output path with cleanup."""
    tmp_path = Path(tempfile.gettempdir()) / f"test_write_{uuid.uuid4()}.parquet"
    yield str(tmp_path)
    if tmp_path.exists():
        tmp_path.unlink()


class TestArrowMemoryStrategy:
    """Tests for ArrowMemoryStrategy."""

    def test_write_from_table(self, sample_table, output_file):
        """Write table produces valid GeoParquet."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_MEMORY)

        strategy.write_from_table(
            table=sample_table,
            output_path=output_file,
            geometry_column="geometry",
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            verbose=False,
        )

        assert Path(output_file).exists()
        pf = pq.ParquetFile(output_file)
        assert pf.metadata.num_rows == 3

        # Check geo metadata
        metadata = pf.schema_arrow.metadata
        assert b"geo" in metadata


class TestDuckDBKVStrategy:
    """Tests for DuckDBKVStrategy."""

    def test_path_traversal_rejected(self):
        """Path traversal attempts are blocked."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DUCKDB_KV)

        with pytest.raises(ValueError, match="directory traversal"):
            strategy._validate_output_path("../../../etc/passwd")

    def test_null_byte_rejected(self):
        """Null bytes in paths are rejected."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DUCKDB_KV)

        with pytest.raises(ValueError, match="Invalid characters"):
            strategy._validate_output_path("file\x00.parquet")

    def test_semicolon_rejected(self):
        """Semicolons in paths are rejected (SQL injection prevention)."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DUCKDB_KV)

        with pytest.raises(ValueError, match="Invalid characters"):
            strategy._validate_output_path("file;DROP TABLE users;--.parquet")


class TestDiskRewriteStrategy:
    """Tests for DiskRewriteStrategy."""

    def test_write_from_table(self, sample_table, output_file):
        """Write table produces valid GeoParquet."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)

        strategy.write_from_table(
            table=sample_table,
            output_path=output_file,
            geometry_column="geometry",
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            verbose=False,
        )

        assert Path(output_file).exists()
        pf = pq.ParquetFile(output_file)
        assert pf.metadata.num_rows == 3

        # Check geo metadata
        metadata = pf.schema_arrow.metadata
        assert b"geo" in metadata
        geo_meta = json.loads(metadata[b"geo"])
        assert "geometry" in geo_meta["columns"]


@pytest.fixture
def duckdb_connection():
    """Create DuckDB connection with spatial extension."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial")
    yield con
    con.close()


@pytest.fixture
def sample_geoparquet(tmp_path):
    """Create a sample GeoParquet file for testing."""
    # Create sample data with WKB geometry
    wkb_point = b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@"
    table = pa.table(
        {
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "geometry": [wkb_point, wkb_point, wkb_point],
        }
    )

    output_path = tmp_path / f"sample_{uuid.uuid4()}.parquet"

    # Write with geo metadata
    geo_meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
            }
        },
    }

    metadata = {b"geo": json.dumps(geo_meta).encode()}
    schema_with_meta = table.schema.with_metadata(metadata)
    table = table.cast(schema_with_meta)

    pq.write_table(table, output_path)
    return str(output_path)


class TestWriteFromQuery:
    """Tests for writing from DuckDB queries."""

    def test_arrow_memory_write_from_query(self, duckdb_connection, sample_geoparquet, output_file):
        """ArrowMemoryStrategy writes from query correctly."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_MEMORY)

        query = f"SELECT * FROM read_parquet('{sample_geoparquet}')"

        strategy.write_from_query(
            con=duckdb_connection,
            query=query,
            output_path=output_file,
            geometry_column="geometry",
            original_metadata=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            input_crs=None,
            verbose=False,
        )

        assert Path(output_file).exists()
        pf = pq.ParquetFile(output_file)
        assert pf.metadata.num_rows == 3
