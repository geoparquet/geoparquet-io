"""
Tests for write strategy implementations.

Tests the Strategy Pattern for GeoParquet writes including:
- Factory methods
- Individual strategy implementations
- Security validations
"""

import json
import logging
import struct
import sys
import tempfile
import uuid
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.common import get_parquet_metadata
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
from geoparquet_io.core.validate import validate_geoparquet
from geoparquet_io.core.write_funnels import write_parquet_with_metadata
from geoparquet_io.core.write_strategies import (
    WriteStrategy,
    WriteStrategyFactory,
    atomic_write,
    needs_metadata_rewrite,
)


class TestWriteStrategy:
    """Tests for WriteStrategy enum."""

    def test_enum_values(self):
        """All expected strategy values exist."""
        assert WriteStrategy.ARROW_MEMORY.value == "in-memory"
        assert WriteStrategy.ARROW_STREAMING.value == "streaming"
        assert WriteStrategy.DUCKDB_KV.value == "duckdb-kv"
        assert WriteStrategy.DISK_REWRITE.value == "disk-rewrite"

    def test_enum_from_string(self):
        """Enum can be created from string values."""
        assert WriteStrategy("in-memory") == WriteStrategy.ARROW_MEMORY
        assert WriteStrategy("streaming") == WriteStrategy.ARROW_STREAMING
        assert WriteStrategy("duckdb-kv") == WriteStrategy.DUCKDB_KV
        assert WriteStrategy("disk-rewrite") == WriteStrategy.DISK_REWRITE


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

    def test_list_strategies(self):
        """List all available strategies."""
        strategies = WriteStrategyFactory.list_strategies()
        assert "in-memory" in strategies
        assert "streaming" in strategies
        assert "duckdb-kv" in strategies
        assert "disk-rewrite" in strategies

    def test_cache_clear(self):
        """Cache can be cleared."""
        WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_MEMORY)
        WriteStrategyFactory.clear_cache()


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


# SQL constructs the ORDER BY scanner cannot track. Each one is a way to hide
# text from a hand-rolled quote-state walker, and each is named in the #657
# rationale for replacing exactly this technique on the --where path.
UNSCANNABLE_QUERIES = [
    pytest.param("SELECT * FROM t -- keep order by x\n", id="line-comment"),
    pytest.param("SELECT * FROM t /* order by x */", id="block-comment"),
    pytest.param("SELECT $$order by x$$ AS s FROM t", id="dollar-quote"),
    pytest.param("SELECT $tag$order by x$tag$ AS s FROM t", id="tagged-dollar-quote"),
    pytest.param("SELECT * FROM t WHERE s = E'\\'order by x'", id="e-string-escape"),
]


class TestStripTrailingOrderByLeavesUnscannableQueriesAlone:
    """The scanner must skip the optimization rather than guess.

    It tracks '' and "" state and nothing else. Before the guard, every query
    below was truncated mid-construct: three into SQL that no longer parses
    (recovered by the caller's fallback, losing the MB target), and the
    E'' case into SQL that still parses but carries a *different* WHERE
    clause — so the bytes-per-row estimate would silently come from a
    different row set.
    """

    @pytest.mark.parametrize("query", UNSCANNABLE_QUERIES)
    def test_query_is_returned_unchanged(self, query):
        from geoparquet_io.core.write_strategies.row_group_sizing import _strip_trailing_order_by

        assert _strip_trailing_order_by(query) == query, (
            "the scanner cannot track this construct, so it must leave the query alone"
        )

    @pytest.mark.parametrize("query", UNSCANNABLE_QUERIES)
    def test_unstripped_query_still_parses(self, query):
        """Whatever comes back must be runnable — that is the point of bailing."""
        import duckdb

        from geoparquet_io.core.write_strategies.row_group_sizing import _strip_trailing_order_by

        result = _strip_trailing_order_by(query)
        duckdb.extract_statements(result)  # raises if the strip broke the SQL

    def test_ordinary_generated_queries_are_still_optimized(self):
        """The guard must not disable the speed-up for the queries gpio emits."""
        from geoparquet_io.core.write_strategies.row_group_sizing import _strip_trailing_order_by

        query = (
            "SELECT * FROM read_parquet('in.parquet') WHERE code = 'abc' "
            "ORDER BY ST_Hilbert(geometry, ST_Extent(ST_MakeEnvelope(0, 0, 1, 1)))"
        )
        stripped = _strip_trailing_order_by(query)
        assert stripped != query, "a plain generated ORDER BY must still be stripped"
        assert "ORDER BY" not in stripped.upper()
        assert stripped.endswith("'abc'")


class TestStripTrailingOrderBy:
    """Sampling for --row-group-size-mb must not re-run the ordered query."""

    def test_strips_top_level_hilbert_order_by(self):
        from geoparquet_io.core.write_strategies.row_group_sizing import _strip_trailing_order_by

        query = "SELECT * FROM t\n        ORDER BY ST_Hilbert(geometry, ST_Extent(x))\n    "
        stripped = _strip_trailing_order_by(query)
        assert "ORDER BY" not in stripped.upper()
        assert stripped.strip() == "SELECT * FROM t"

    def test_leaves_query_without_order_by(self):
        from geoparquet_io.core.write_strategies.row_group_sizing import _strip_trailing_order_by

        query = "SELECT * FROM t WHERE id > 5"
        assert _strip_trailing_order_by(query) == query

    def test_ignores_order_by_inside_subquery(self):
        from geoparquet_io.core.write_strategies.row_group_sizing import _strip_trailing_order_by

        query = "SELECT * FROM (SELECT a FROM t ORDER BY a) sub"
        assert _strip_trailing_order_by(query) == query

    def test_keeps_order_by_when_limit_follows(self):
        from geoparquet_io.core.write_strategies.row_group_sizing import _strip_trailing_order_by

        query = "SELECT * FROM t ORDER BY a LIMIT 10"
        assert _strip_trailing_order_by(query) == query

    def test_stripped_sample_matches_ordered_row_size(self):
        """The estimate must be unaffected by dropping the ordering."""
        from geoparquet_io.core.write_strategies.row_group_sizing import (
            _resolve_row_group_rows,
            _strip_trailing_order_by,
        )

        con = duckdb.connect()
        base = "SELECT i AS id, i * 2 AS v FROM range(1000) t(i)"
        ordered = f"{base}\n            ORDER BY id"

        rows = _resolve_row_group_rows(con, ordered, 0.001, None, verbose=False)
        con.close()
        assert rows is not None and rows > 0
        assert _strip_trailing_order_by(ordered).strip() == base


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


class TestWriteStrategiesNoGeometry:
    """
    Tests for write strategies when geometry_column is None.

    Regression tests for issue #440: write strategies should write valid plain
    Parquet (no geo metadata) when the table has no geometry column.
    """

    @pytest.fixture
    def non_geo_table(self):
        """Create a plain Arrow table with no geometry."""
        return pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [100.5, 200.0, 300.75],
            }
        )

    @pytest.fixture
    def non_geo_query(self, duckdb_connection):
        """Register a non-geo table and return query string."""
        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [100.5, 200.0, 300.75],
            }
        )
        duckdb_connection.register("non_geo_data", table)
        return "SELECT * FROM non_geo_data"

    def _assert_valid_plain_parquet(self, output_path: str):
        """Assert output is valid plain Parquet with no geo metadata."""
        pf = pq.ParquetFile(output_path)
        assert pf.metadata.num_rows == 3

        # Verify no geo metadata key
        schema_metadata = pf.schema_arrow.metadata or {}
        assert b"geo" not in schema_metadata, "Plain Parquet should have no 'geo' metadata"

        # Verify data integrity
        table = pf.read()
        assert table.column_names == ["id", "name", "value"]
        assert table["id"].to_pylist() == [1, 2, 3]

    def test_in_memory_strategy_no_geometry(self, non_geo_table, output_file):
        """in-memory strategy writes valid plain Parquet when geometry_column=None."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_MEMORY)

        strategy.write_from_table(
            table=non_geo_table,
            output_path=output_file,
            geometry_column=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            verbose=False,
        )

        self._assert_valid_plain_parquet(output_file)

    def test_duckdb_kv_strategy_no_geometry(self, non_geo_table, output_file):
        """duckdb-kv strategy writes valid plain Parquet when geometry_column=None."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DUCKDB_KV)

        strategy.write_from_table(
            table=non_geo_table,
            output_path=output_file,
            geometry_column=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            verbose=False,
        )

        self._assert_valid_plain_parquet(output_file)

    def test_streaming_strategy_no_geometry(self, non_geo_table, output_file):
        """streaming strategy writes valid plain Parquet when geometry_column=None."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_STREAMING)

        strategy.write_from_table(
            table=non_geo_table,
            output_path=output_file,
            geometry_column=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            verbose=False,
        )

        self._assert_valid_plain_parquet(output_file)

    def test_disk_rewrite_strategy_no_geometry(self, non_geo_table, output_file):
        """disk-rewrite strategy writes valid plain Parquet when geometry_column=None."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)

        strategy.write_from_table(
            table=non_geo_table,
            output_path=output_file,
            geometry_column=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            verbose=False,
        )

        self._assert_valid_plain_parquet(output_file)

    def test_in_memory_query_no_geometry(self, duckdb_connection, non_geo_query, output_file):
        """in-memory strategy write_from_query handles geometry_column=None."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_MEMORY)

        strategy.write_from_query(
            con=duckdb_connection,
            query=non_geo_query,
            output_path=output_file,
            geometry_column=None,
            original_metadata=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            input_crs=None,
            verbose=False,
        )

        self._assert_valid_plain_parquet(output_file)

    def test_duckdb_kv_query_no_geometry(self, duckdb_connection, non_geo_query, output_file):
        """duckdb-kv strategy write_from_query handles geometry_column=None."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DUCKDB_KV)

        strategy.write_from_query(
            con=duckdb_connection,
            query=non_geo_query,
            output_path=output_file,
            geometry_column=None,
            original_metadata=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            input_crs=None,
            verbose=False,
        )

        self._assert_valid_plain_parquet(output_file)

    def test_streaming_query_no_geometry(self, duckdb_connection, non_geo_query, output_file):
        """streaming strategy write_from_query handles geometry_column=None."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_STREAMING)

        strategy.write_from_query(
            con=duckdb_connection,
            query=non_geo_query,
            output_path=output_file,
            geometry_column=None,
            original_metadata=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            input_crs=None,
            verbose=False,
        )

        self._assert_valid_plain_parquet(output_file)

    def test_disk_rewrite_query_no_geometry(self, duckdb_connection, non_geo_query, output_file):
        """disk-rewrite strategy write_from_query handles geometry_column=None."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)

        strategy.write_from_query(
            con=duckdb_connection,
            query=non_geo_query,
            output_path=output_file,
            geometry_column=None,
            original_metadata=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            input_crs=None,
            verbose=False,
        )

        self._assert_valid_plain_parquet(output_file)

    def test_disk_rewrite_query_no_geometry_verbose(
        self, duckdb_connection, non_geo_query, output_file, caplog
    ):
        """The verbose row count on the plain-Parquet path opens the file it just wrote.

        Same shape as the geo path's temp-file peek: the handle has to be closed
        before the work directory is torn down, which on Windows is the
        difference between a cleaned-up temp dir and a leaked one.
        """
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)

        with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
            strategy.write_from_query(
                con=duckdb_connection,
                query=non_geo_query,
                output_path=output_file,
                geometry_column=None,
                original_metadata=None,
                geoparquet_version="1.1",
                compression="ZSTD",
                compression_level=15,
                row_group_size_mb=None,
                row_group_rows=None,
                input_crs=None,
                verbose=True,
            )

        assert "Wrote 3 rows" in caplog.text
        self._assert_valid_plain_parquet(output_file)

    def test_compression_options_honored_no_geometry(self, non_geo_table, output_file):
        """Compression options are honored when writing non-geo data."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DUCKDB_KV)

        strategy.write_from_table(
            table=non_geo_table,
            output_path=output_file,
            geometry_column=None,
            geoparquet_version="1.1",
            compression="SNAPPY",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=None,
            verbose=False,
        )

        pf = pq.ParquetFile(output_file)
        # Verify compression was applied (SNAPPY)
        row_group = pf.metadata.row_group(0)
        col_meta = row_group.column(0)
        assert col_meta.compression == "SNAPPY"

    @pytest.fixture
    def empty_non_geo_table(self):
        """Create an empty Arrow table with no geometry."""
        return pa.table(
            {
                "id": pa.array([], type=pa.int64()),
                "name": pa.array([], type=pa.string()),
                "value": pa.array([], type=pa.float64()),
            }
        )

    @pytest.fixture
    def empty_non_geo_query(self, duckdb_connection):
        """Register an empty non-geo table and return query string."""
        table = pa.table(
            {
                "id": pa.array([], type=pa.int64()),
                "name": pa.array([], type=pa.string()),
                "value": pa.array([], type=pa.float64()),
            }
        )
        duckdb_connection.register("empty_non_geo_data", table)
        return "SELECT * FROM empty_non_geo_data"

    def _assert_valid_empty_plain_parquet(self, output_path: str):
        """Assert output is valid empty plain Parquet with no geo metadata."""
        pf = pq.ParquetFile(output_path)
        assert pf.metadata.num_rows == 0

        # Verify no geo metadata key
        schema_metadata = pf.schema_arrow.metadata or {}
        assert b"geo" not in schema_metadata, "Plain Parquet should have no 'geo' metadata"

    def test_streaming_empty_table_no_geometry(self, empty_non_geo_table, output_file):
        """streaming strategy handles empty table with geometry_column=None."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_STREAMING)

        strategy.write_from_table(
            table=empty_non_geo_table,
            output_path=output_file,
            geometry_column=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=None,
            verbose=False,
        )

        self._assert_valid_empty_plain_parquet(output_file)

    def test_streaming_empty_query_no_geometry(
        self, duckdb_connection, empty_non_geo_query, output_file
    ):
        """streaming strategy handles empty query result with geometry_column=None."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_STREAMING)

        strategy.write_from_query(
            con=duckdb_connection,
            query=empty_non_geo_query,
            output_path=output_file,
            geometry_column=None,
            original_metadata=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=None,
            input_crs=None,
            verbose=False,
        )

        self._assert_valid_empty_plain_parquet(output_file)


def _write_points_geoparquet(path: Path, num_rows: int) -> str:
    """Write a small WKB-point GeoParquet file used as a multi-row-group source."""
    geo_meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
    }
    points = [struct.pack("<BI2d", 1, 1, float(i), float(i)) for i in range(num_rows)]
    table = pa.table({"id": list(range(num_rows)), "geometry": points})
    table = table.replace_schema_metadata({b"geo": json.dumps(geo_meta).encode()})
    pq.write_table(table, path)
    return str(path)


class TestDiskRewriteRowGroupCoarsening:
    """The metadata rewrite must be able to MERGE source row groups (#697).

    ``_rewrite_with_metadata`` issued one ``write_table`` per source row group,
    and each of those starts a new row group, so it could only ever make groups
    smaller than the source's. A request larger than the source's groups came
    back as the source's shape, silently.

    These call the helper directly because it is the smallest thing that can
    express the shape assertions. The defect is *not* helper-only: DuckDB's
    ``ROW_GROUP_SIZE`` leaves small source groups alone, so the temporary file
    the rewrite reads still arrives at 10 rows per group and
    ``gpio extract geoparquet --write-strategy disk-rewrite --row-group-size``
    ignored the request end to end. ``test_write_contract`` pins that direction
    across all four strategies.
    """

    GEO_META = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
    }

    @staticmethod
    def _row_group_sizes(path) -> list[int]:
        pf = pq.ParquetFile(str(path))
        return [pf.metadata.row_group(i).num_rows for i in range(pf.metadata.num_row_groups)]

    @pytest.fixture
    def source_of_ten_row_groups(self, tmp_path):
        """400 rows in 40 groups of 10 — the shape from the issue."""
        path = tmp_path / "small_groups.parquet"
        points = [struct.pack("<BI2d", 1, 1, float(i), float(i)) for i in range(400)]
        table = pa.table({"id": list(range(400)), "geometry": points})
        pq.write_table(table, path, row_group_size=10)
        assert self._row_group_sizes(path) == [10] * 40
        return path

    def _rewrite(self, source, out, row_group_rows):
        WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)._rewrite_with_metadata(
            input_path=str(source),
            output_path=str(out),
            geo_meta=self.GEO_META,
            compression="ZSTD",
            compression_level=None,
            verbose=False,
            row_group_rows=row_group_rows,
        )

    def test_request_larger_than_source_groups_coarsens(self, source_of_ten_row_groups, tmp_path):
        """The reproducer: 10-row source groups, request 100."""
        out = tmp_path / "coarsened.parquet"
        self._rewrite(source_of_ten_row_groups, out, 100)

        assert self._row_group_sizes(out) == [100] * 4

    def test_request_smaller_than_source_groups_still_splits(
        self, source_of_ten_row_groups, tmp_path
    ):
        """The direction that already worked must keep working."""
        out = tmp_path / "split.parquet"
        self._rewrite(source_of_ten_row_groups, out, 5)

        assert self._row_group_sizes(out) == [5] * 80

    def test_no_request_preserves_the_source_shape(self, source_of_ten_row_groups, tmp_path):
        """No sizing request means no reshaping."""
        out = tmp_path / "unchanged.parquet"
        self._rewrite(source_of_ten_row_groups, out, None)

        assert self._row_group_sizes(out) == [10] * 40

    def test_uneven_remainder_is_flushed(self, tmp_path):
        """A trailing partial group must still be written, not dropped."""
        source = tmp_path / "uneven.parquet"
        points = [struct.pack("<BI2d", 1, 1, float(i), float(i)) for i in range(25)]
        pq.write_table(
            pa.table({"id": list(range(25)), "geometry": points}), source, row_group_size=5
        )
        out = tmp_path / "uneven_out.parquet"
        self._rewrite(source, out, 10)

        assert self._row_group_sizes(out) == [10, 10, 5]

    @pytest.mark.parametrize(
        ("source_rows", "source_group", "target", "expected"),
        [
            (400, 10, 25, [25] * 16),
            (600, 60, 100, [100] * 6),
            (400, 40, 100, [100] * 4),
            (990, 99, 100, [100] * 9 + [90]),
        ],
        ids=["10s_to_25", "60s_to_100", "40s_to_100", "99s_to_100"],
    )
    def test_overshoot_is_carried_not_flushed_as_a_runt(
        self, tmp_path, source_rows, source_group, target, expected
    ):
        """A batch that overshoots the target must not leave a runt behind it.

        Flushing the whole over-full batch wrote one full group plus its
        remainder, so 10-row sources at ``row_group_rows=25`` came back as
        ``[25, 5, 25, 5, ...]`` -- more groups than asked for, half of them
        undersized, and a worse layout than doing nothing. The remainder is
        carried into the next group instead. Only the final group may be short.
        """
        source = tmp_path / f"src_{source_group}_{target}.parquet"
        points = [struct.pack("<BI2d", 1, 1, float(i), float(i)) for i in range(source_rows)]
        pq.write_table(
            pa.table({"id": list(range(source_rows)), "geometry": points}),
            source,
            row_group_size=source_group,
        )
        out = tmp_path / f"out_{source_group}_{target}.parquet"
        self._rewrite(source, out, target)

        assert self._row_group_sizes(out) == expected
        assert pq.read_table(str(out)).column("id").to_pylist() == list(range(source_rows))

    def test_rows_and_values_survive_coarsening(self, source_of_ten_row_groups, tmp_path):
        """Merging groups must not reorder or lose rows."""
        out = tmp_path / "values.parquet"
        self._rewrite(source_of_ten_row_groups, out, 100)

        assert pq.read_table(str(out)).column("id").to_pylist() == list(range(400))

    @pytest.mark.parametrize("row_group_rows", [100, None], ids=["coarsen", "no_request"])
    def test_verbose_progress_reports_every_ten_source_groups(
        self, source_of_ten_row_groups, tmp_path, caplog, row_group_rows
    ):
        """Both loops report progress; 40 source groups means four reports."""
        out = tmp_path / f"verbose_{row_group_rows}.parquet"
        with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
            WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)._rewrite_with_metadata(
                input_path=str(source_of_ten_row_groups),
                output_path=str(out),
                geo_meta=self.GEO_META,
                compression="ZSTD",
                compression_level=None,
                verbose=True,
                row_group_rows=row_group_rows,
            )

        assert "Rewrote 10/40 row groups" in caplog.text
        assert "Rewrote 40/40 row groups" in caplog.text
        assert self._row_group_sizes(out) == ([100] * 4 if row_group_rows else [10] * 40)

    def test_geo_metadata_is_written_when_coarsening(self, source_of_ten_row_groups, tmp_path):
        """The rewrite's actual job still happens on the merging path."""
        out = tmp_path / "meta.parquet"
        self._rewrite(source_of_ten_row_groups, out, 100)

        metadata = pq.ParquetFile(str(out)).schema_arrow.metadata
        assert json.loads(metadata[b"geo"].decode("utf-8"))["version"] == "1.1.0"


class TestDiskRewriteRowGroupSizing:
    """disk-rewrite must honour row-group sizing requests (issue #689).

    The strategy accepted ``row_group_rows``/``row_group_size_mb`` and used
    neither, so callers silently got DuckDB/PyArrow defaults.
    """

    def _row_group_sizes(self, path: str) -> list[int]:
        pf = pq.ParquetFile(path)
        return [pf.metadata.row_group(i).num_rows for i in range(pf.metadata.num_row_groups)]

    def test_row_group_rows_honored_from_query(self, duckdb_connection, tmp_path, output_file):
        """An explicit rows-per-group request shapes the written row groups."""
        src = _write_points_geoparquet(tmp_path / "src.parquet", 40)
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)

        strategy.write_from_query(
            con=duckdb_connection,
            query=f"SELECT * FROM read_parquet('{src}')",
            output_path=output_file,
            geometry_column="geometry",
            original_metadata=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=10,
            input_crs=None,
            verbose=False,
        )

        sizes = self._row_group_sizes(output_file)
        assert sum(sizes) == 40
        assert len(sizes) == 4, f"expected 4 row groups of 10 rows, got {sizes}"
        assert max(sizes) <= 10

    def test_no_row_group_settings_leaves_default_shape(
        self, duckdb_connection, tmp_path, output_file
    ):
        """Without sizing options the default single-row-group shape is unchanged."""
        src = _write_points_geoparquet(tmp_path / "src.parquet", 40)
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)

        strategy.write_from_query(
            con=duckdb_connection,
            query=f"SELECT * FROM read_parquet('{src}')",
            output_path=output_file,
            geometry_column="geometry",
            original_metadata=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=None,
            input_crs=None,
            verbose=False,
        )

        assert self._row_group_sizes(output_file) == [40]

    def test_row_group_size_mb_splits_row_groups(self, duckdb_connection, tmp_path, output_file):
        """A small MB target produces more, smaller row groups than the default."""
        src = _write_points_geoparquet(tmp_path / "src.parquet", 2000)
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)

        strategy.write_from_query(
            con=duckdb_connection,
            query=f"SELECT * FROM read_parquet('{src}')",
            output_path=output_file,
            geometry_column="geometry",
            original_metadata=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=0.01,
            row_group_rows=None,
            input_crs=None,
            verbose=False,
        )

        sizes = self._row_group_sizes(output_file)
        assert sum(sizes) == 2000
        assert len(sizes) > 1, f"MB target should split row groups, got {sizes}"
        assert max(sizes) < 2000

    def test_write_from_table_honors_row_group_rows(self, tmp_path, output_file):
        """write_from_table forwards sizing through to the written file."""
        src = _write_points_geoparquet(tmp_path / "src.parquet", 40)
        table = pq.read_table(src)
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)

        strategy.write_from_table(
            table=table,
            output_path=output_file,
            geometry_column="geometry",
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=10,
            verbose=False,
        )

        sizes = self._row_group_sizes(output_file)
        assert sum(sizes) == 40
        assert len(sizes) == 4, f"expected 4 row groups of 10 rows, got {sizes}"

    def test_plain_parquet_query_honors_row_group_rows(self, duckdb_connection, output_file):
        """Non-geo writes honour sizing too (they take a separate COPY path).

        This path is a bare DuckDB COPY, which flushes a row group per input
        chunk (chunk size capped at 2048 rows), so a request finer than the
        incoming chunks cannot be honored exactly. The assertion is therefore
        made at chunk scale — the same resolution the duckdb-kv strategy gives
        for non-geo writes.
        """
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)

        strategy.write_from_query(
            con=duckdb_connection,
            query="SELECT i AS id, i * 2 AS v FROM range(10000) t(i)",
            output_path=output_file,
            geometry_column=None,
            original_metadata=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=2048,
            input_crs=None,
            verbose=False,
        )

        sizes = self._row_group_sizes(output_file)
        assert sum(sizes) == 10000
        assert max(sizes) <= 2048, f"expected <=2048 rows per group, got {sizes}"
        assert len(sizes) == 5, f"expected 5 row groups, got {sizes}"

    def test_plain_parquet_table_honors_row_group_size_mb(self, output_file):
        """Non-geo Arrow-table writes convert an MB target to rows per group."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)
        table = pa.table({"id": list(range(5000)), "v": [float(i) for i in range(5000)]})

        strategy.write_from_table(
            table=table,
            output_path=output_file,
            geometry_column=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=0.01,
            row_group_rows=None,
            verbose=False,
        )

        sizes = self._row_group_sizes(output_file)
        assert sum(sizes) == 5000
        assert len(sizes) > 1, f"MB target should split row groups, got {sizes}"

    def test_plain_parquet_table_honors_row_group_rows(self, output_file):
        """Non-geo Arrow-table writes honour sizing too."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)
        table = pa.table({"id": list(range(40)), "v": [float(i) for i in range(40)]})

        strategy.write_from_table(
            table=table,
            output_path=output_file,
            geometry_column=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=10,
            verbose=False,
        )

        sizes = self._row_group_sizes(output_file)
        assert sum(sizes) == 40
        assert len(sizes) == 4, f"expected 4 row groups of 10 rows, got {sizes}"


class TestDuckDBKVWriteConfiguration:
    """The duckdb-kv strategy must not silently change the write it was asked for."""

    @staticmethod
    def _points_query(con, path: str, rows: int = 60000) -> str:
        """A compressible table on disk; returns a query selecting from it."""
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(
            f"""COPY (SELECT (i % 50)::VARCHAR AS cat, 'region_' || (i % 20) AS reg,
                    ST_Point((i % 500) * 0.001, (i % 499) * 0.001) AS geometry
                FROM range({rows}) t(i))
                TO '{path}' (FORMAT PARQUET)"""
        )
        return f"SELECT * FROM read_parquet('{path}')"

    def test_compression_level_reaches_the_writer(self, tmp_path):
        """`--compression-level` must not be dropped on this path.

        `_build_copy_options` never emitted COMPRESSION_LEVEL, so every write
        through this strategy silently fell back to DuckDB's ZSTD default of 3
        while gpio's own default is 15 — materially larger files, with no
        indication the option had been ignored.
        """
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.write_funnels import write_parquet_with_metadata

        con = get_duckdb_connection()
        query = self._points_query(con, str(tmp_path / "src.parquet"))

        sizes = {}
        for level in (1, 22):
            out = tmp_path / f"out{level}.parquet"
            write_parquet_with_metadata(
                con,
                query,
                str(out),
                geoparquet_version="1.1",
                compression="ZSTD",
                compression_level=level,
                verbose=False,
            )
            sizes[level] = out.stat().st_size

        assert sizes[22] < sizes[1], (
            f"compression level ignored: level 1 = {sizes[1]}, level 22 = {sizes[22]}"
        )

    def test_session_settings_are_restored_after_a_write(self, tmp_path):
        """The connection belongs to the caller, not to one write.

        The strategy clamps threads=1, preserve_insertion_order=false and
        memory_limit for its own COPY. Leaving them behind meant one write
        throttled every later query on a shared connection — partition loops
        finalize N files on one connection, and the Python API holds a
        connection across operations.
        """
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.write_funnels import write_parquet_with_metadata

        con = get_duckdb_connection()
        query = self._points_query(con, str(tmp_path / "src.parquet"), rows=500)

        managed = ("threads", "preserve_insertion_order", "memory_limit")

        def snapshot():
            return {
                key: con.execute(f"SELECT current_setting('{key}')").fetchone()[0]
                for key in managed
            }

        before = snapshot()
        write_parquet_with_metadata(
            con, query, str(tmp_path / "out.parquet"), geoparquet_version="1.1", verbose=False
        )

        assert snapshot() == before


class TestCompressionLevelValidation:
    """`COMPRESSION_LEVEL` is formatted into SQL, so the value must be checked."""

    @pytest.mark.parametrize("bad", ["1; DROP TABLE t", 3.5, True, 0, 23, -1, "15", None, object()])
    def test_rejects_values_that_are_not_a_duckdb_level(self, bad):
        from geoparquet_io.core.duckdb_utils import validate_compression_level

        with pytest.raises(ValueError):
            validate_compression_level(bad)

    @pytest.mark.parametrize("level", [1, 15, 22])
    def test_accepts_the_documented_range(self, level):
        from geoparquet_io.core.duckdb_utils import validate_compression_level

        assert validate_compression_level(level) == level

    def test_library_callers_are_checked_not_just_the_cli(self, tmp_path):
        """The CLI has IntRange(1, 22); a Python caller reaches the writer directly."""
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.write_funnels import write_parquet_with_metadata

        con = get_duckdb_connection()
        con.execute("INSTALL spatial; LOAD spatial;")
        src = tmp_path / "src.parquet"
        con.execute(
            f"""COPY (SELECT ST_Point(i * 0.1, i * 0.1) AS geometry FROM range(10) t(i))
                TO '{src}' (FORMAT PARQUET)"""
        )

        with pytest.raises(ValueError, match="compression_level"):
            write_parquet_with_metadata(
                con,
                f"SELECT * FROM read_parquet('{src}')",
                str(tmp_path / "out.parquet"),
                geoparquet_version="1.1",
                compression="ZSTD",
                compression_level="1; DROP TABLE t",
                verbose=False,
            )


class TestDuckDBKVGeoarrowFieldMetadata:
    """geoarrow.wkb carried in field metadata, not in the Arrow type (#727).

    ``geoarrow.pyarrow`` registers its extension types process-globally on
    import, so the same WKB column reaches the writer either as a resolved
    ``geoarrow.wkb`` extension type or as plain ``large_binary`` whose *field*
    metadata carries ``ARROW:extension:name``. DuckDB honours that metadata on
    ``register()`` and presents the column as ``GEOMETRY`` either way, so the
    ``ST_GeomFromWKB`` wrapper has to be skipped in both shapes -- wrapping a
    GEOMETRY is a binder error, which is what ``Table.add_kdtree().write()``
    used to hit.
    """

    @staticmethod
    def _table_with_metadata_only_extension():
        wkb_point = (
            b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@"
        )
        geometry = pa.field(
            "geometry",
            pa.large_binary(),
            metadata={
                b"ARROW:extension:name": b"geoarrow.wkb",
                b"ARROW:extension:metadata": b"{}",
            },
        )
        schema = pa.schema([pa.field("id", pa.int64()), geometry])
        return pa.table(
            {"id": [1, 2, 3], "geometry": [wkb_point, wkb_point, wkb_point]},
            schema=schema,
        )

    def test_write_from_table_with_metadata_only_extension(self, output_file):
        """A metadata-carried geoarrow.wkb column writes without a binder error."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DUCKDB_KV)

        strategy.write_from_table(
            table=self._table_with_metadata_only_extension(),
            output_path=output_file,
            geometry_column="geometry",
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            verbose=False,
        )

        result = pq.read_table(output_file)
        assert result.num_rows == 3
        assert b"geo" in (result.schema.metadata or {})


class TestDuckDBArrowExtensionTypeMapping:
    """Pin the Arrow -> DuckDB type mapping the writers infer rather than ask about.

    ``duckdb_kv.write_from_table`` and ``disk_rewrite.write_from_table`` decide
    whether to wrap the geometry column in ``ST_GeomFromWKB`` by reading the
    Arrow extension name themselves, mirroring what DuckDB does on
    ``register()`` instead of asking DuckDB. That mirror is only correct while
    the mapping below holds, and a silent change to it would turn back into the
    binder error of #727 (wrapping a GEOMETRY) or its inverse (feeding raw WKB
    bytes to a spatial function that wants GEOMETRY).

    So assert the mapping directly, with the same field-metadata carrier shape
    the writers see: an unresolved ``ARROW:extension:name``, which is what
    arrives whenever nothing in the process registered ``geoarrow.pyarrow``.
    """

    WKB_POINT = struct.pack("<BI2d", 1, 1, 1.0, 2.0)

    @staticmethod
    def _duckdb_type_for(extension_name, arrow_type, values):
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection

        metadata = (
            {
                b"ARROW:extension:name": extension_name.encode(),
                b"ARROW:extension:metadata": b"{}",
            }
            if extension_name
            else None
        )
        schema = pa.schema([pa.field("g", arrow_type, metadata=metadata)])
        table = pa.table({"g": pa.array(values, type=arrow_type)}, schema=schema)

        con = get_duckdb_connection(load_spatial=True)
        try:
            con.register("probe", table)
            return con.execute("DESCRIBE SELECT * FROM probe").fetchall()[0][1]
        finally:
            con.close()

    @pytest.mark.parametrize("storage", [pa.binary(), pa.large_binary()])
    def test_geoarrow_wkb_registers_as_geometry(self, storage):
        """Both WKB storage widths become GEOMETRY -- so ST_GeomFromWKB must be skipped."""
        assert self._duckdb_type_for("geoarrow.wkb", storage, [self.WKB_POINT]) == "GEOMETRY"

    def test_ogc_wkb_registers_as_blob(self):
        """``ogc.wkb`` is NOT promoted, so it still needs the ST_GeomFromWKB wrapper.

        This is why the writers compare against ``geoarrow.wkb`` exactly rather
        than testing "has any WKB extension name".
        """
        assert self._duckdb_type_for("ogc.wkb", pa.binary(), [self.WKB_POINT]) == "BLOB"

    def test_geoarrow_wkt_registers_as_varchar(self):
        assert self._duckdb_type_for("geoarrow.wkt", pa.string(), ["POINT (1 2)"]) == "VARCHAR"

    def test_geoarrow_point_registers_as_struct(self):
        """A native nested carrier stays a STRUCT: not GEOMETRY, and not WKB bytes."""
        point_type = pa.struct([pa.field("x", pa.float64()), pa.field("y", pa.float64())])
        duckdb_type = self._duckdb_type_for("geoarrow.point", point_type, [{"x": 1.0, "y": 2.0}])
        assert duckdb_type.startswith("STRUCT")

    def test_unmarked_binary_registers_as_blob(self):
        """No marker means no promotion -- the baseline the wrapper is written for."""
        assert self._duckdb_type_for(None, pa.binary(), [self.WKB_POINT]) == "BLOB"


# ---------------------------------------------------------------------------
# Native Parquet GEOMETRY logical types (#764)
# ---------------------------------------------------------------------------

ALL_STRATEGIES = ["duckdb-kv", "in-memory", "streaming", "disk-rewrite"]
NATIVE_VERSIONS = ["2.0", "parquet-geo-only"]


def _geometry_carriers(path: str) -> dict[str, str]:
    """Physical carrier per geometry column: "native" or the Parquet physical type.

    Read without the spatial extension so the answer is the file's own schema,
    not something DuckDB reconstructed.
    """
    con = get_duckdb_connection(load_spatial=False)
    try:
        rows = con.execute(
            f"SELECT name, type, logical_type FROM parquet_schema({sql_path(path)})"
        ).fetchall()
    finally:
        con.close()
    return {
        name: "native" if (logical and "Geometry" in str(logical)) else str(typ).lower()
        for name, typ, logical in rows
        if name in ("geometry", "geom2")
    }


def _failed_checks(path: str) -> list[str]:
    """Validator failures, minus the one Windows fails for a platform reason.

    On win32 ``native_geo_stats_contains_data_*`` reports every geometry as
    outside its own column's geospatial statistics, for any native GEOMETRY
    column written by any code path (#721, #748). Excused by name and only on
    win32, exactly as ``tests/test_secondary_geometry_carriers.py`` does it, so
    every other check stays enforced everywhere.
    """
    failed = sorted(
        {c.name for c in validate_geoparquet(path).checks if c.status.value == "failed"}
    )
    if sys.platform == "win32":
        failed = [f for f in failed if not f.startswith("native_geo_stats_contains_data")]
    return failed


def _write_via_query(source: str, out: str, version: str, strategy: str, **kwargs) -> None:
    con = get_duckdb_connection(load_spatial=True)
    try:
        metadata, _ = get_parquet_metadata(source)
        write_parquet_with_metadata(
            con,
            f"SELECT * FROM read_parquet({sql_path(source)})",
            out,
            original_metadata=metadata,
            geoparquet_version=version,
            write_strategy=strategy,
            input_file=source,
            **kwargs,
        )
    finally:
        con.close()


class TestNativeGeometryTypeAcrossStrategies:
    """Every strategy gives a 2.0 / parquet-geo-only geometry column a native type.

    Regression suite for #764. Three strategies branch on the target version and
    build a native Parquet GEOMETRY logical type; ``disk-rewrite`` did not, so
    its 2.0 output declared a version whose spec requires that type and carried
    plain BYTE_ARRAY WKB -- two failures from gpio's own ``check spec``. Under
    parquet-geo-only it was worse: the logical type is a column's only geometry
    identity there, and the ``geo`` block the strategy wrote anyway carried
    ``"version": null``, which DuckDB refuses to open at all.
    """

    @pytest.mark.parametrize("strategy", ALL_STRATEGIES)
    @pytest.mark.parametrize("version", NATIVE_VERSIONS)
    def test_primary_column_is_native_and_valid(self, strategy, version, tmp_path):
        """The issue's reproducer: one geometry column, one ordinary file."""
        source = _write_points_geoparquet(tmp_path / "src.parquet", 3)
        out = str(tmp_path / f"{strategy}_{version}.parquet")

        _write_via_query(source, out, version, strategy)

        carriers = _geometry_carriers(out)
        assert carriers == {"geometry": "native"}, (
            f"{strategy} wrote a {carriers['geometry']} primary column in a {version} file"
        )
        assert _failed_checks(out) == []

    @pytest.mark.parametrize("strategy", ALL_STRATEGIES)
    def test_secondary_column_is_native_under_parquet_geo_only(self, strategy, tmp_path):
        """parquet-geo-only writes no ``geo`` block, so every column needs the type (#706).

        The 2.0 half of this lives in ``tests/test_secondary_geometry_carriers.py``;
        parquet-geo-only has nothing to name the secondary by afterwards, which is
        why the names must reach the write as ``geometry_info``.
        """
        source = str(tmp_path / "two_geom.parquet")
        wkb = [struct.pack("<BI2d", 1, 1, float(i), float(i)) for i in range(4)]
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {"encoding": "WKB", "geometry_types": ["Point"]},
                "geom2": {"encoding": "WKB", "geometry_types": ["Point"]},
            },
        }
        table = pa.table({"id": [1, 2], "geometry": wkb[:2], "geom2": wkb[2:]})
        pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(geo).encode()}), source)
        out = str(tmp_path / f"{strategy}_geo_only.parquet")

        _write_via_query(
            source,
            out,
            "parquet-geo-only",
            strategy,
            geometry_info={
                "primary": "geometry",
                "secondary": ["geom2"],
                "metadata": {"geom2": geo["columns"]["geom2"]},
            },
        )

        assert _geometry_carriers(out) == {"geometry": "native", "geom2": "native"}
        assert _failed_checks(out) == []

    @pytest.mark.parametrize("version", NATIVE_VERSIONS)
    def test_disk_rewrite_table_entry_point_is_native(self, version, tmp_path):
        """``write_from_table`` (the ``Table.write()`` path) takes the same branch."""
        source = _write_points_geoparquet(tmp_path / "src.parquet", 3)
        out = str(tmp_path / f"table_{version}.parquet")

        WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE).write_from_table(
            table=pq.read_table(source),
            output_path=out,
            geometry_column="geometry",
            geoparquet_version=version,
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=None,
            verbose=False,
        )

        assert _geometry_carriers(out) == {"geometry": "native"}
        assert _failed_checks(out) == []

    def test_disk_rewrite_parquet_geo_only_writes_no_geo_key(self, tmp_path):
        """parquet-geo-only means no ``geo`` block, not one with ``"version": null``."""
        source = _write_points_geoparquet(tmp_path / "src.parquet", 3)
        out = str(tmp_path / "geo_only.parquet")

        _write_via_query(source, out, "parquet-geo-only", "disk-rewrite")

        assert b"geo" not in (pq.read_schema(out).metadata or {})
        # DuckDB's own reader refused the file this used to write, with
        # "Geoparquet metadata does not have a version".
        con = duckdb.connect()
        try:
            count = con.execute(f"SELECT count(*) FROM read_parquet({sql_path(out)})").fetchone()
        finally:
            con.close()
        assert count[0] == 3

    @pytest.mark.parametrize("version", ["1.0", "1.1"])
    def test_disk_rewrite_v1x_output_is_unchanged(self, version, tmp_path):
        """The version branch must not leak the native type into 1.x output."""
        source = _write_points_geoparquet(tmp_path / "src.parquet", 3)
        out = str(tmp_path / f"v{version}.parquet")

        _write_via_query(source, out, version, "disk-rewrite")

        assert _geometry_carriers(out) == {"geometry": "byte_array"}
        assert _failed_checks(out) == []
        assert json.loads(pq.read_schema(out).metadata[b"geo"])["version"] == f"{version}.0"

    @pytest.mark.parametrize("strategy", ["disk-rewrite", "streaming"])
    def test_verbose_write_names_the_native_columns(self, strategy, tmp_path, caplog):
        """The verbose branch of the native path is code too, and says which columns.

        Both strategies build the native type from the same helper, and both log
        what they are about to write; an f-string that only runs under
        ``--verbose`` is exactly the kind of line that ships broken otherwise.
        """
        source = _write_points_geoparquet(tmp_path / "src.parquet", 3)
        out = str(tmp_path / f"verbose_{strategy}.parquet")

        with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
            _write_via_query(source, out, "2.0", strategy, verbose=True)

        assert "native parquet geometry type" in caplog.text.lower()
        assert "'geometry'" in caplog.text
        assert _geometry_carriers(out) == {"geometry": "native"}

    def test_verbose_write_leaves_no_open_parquet_handle(self, tmp_path, monkeypatch):
        """disk-rewrite deletes its temp file, so nothing may still hold it open.

        The verbose row-count peek at the DuckDB temp file kept a
        ``pq.ParquetFile`` alive in the enclosing frame until ``write_from_query``
        returned -- past the ``os.unlink`` of that very file. POSIX unlinks an
        open file happily; Windows raises ``PermissionError: [WinError 32]``, so
        every ``--verbose`` disk-rewrite write died there.

        Asserted as the invariant rather than as the platform error, so this
        fails on every OS: when the write returns, every ParquetFile it opened
        is closed.
        """
        opened = []
        real_parquet_file = pq.ParquetFile

        def tracking_parquet_file(source, *args, **kwargs):
            handle = real_parquet_file(source, *args, **kwargs)
            # Only the strategy's own scratch directory: those are the files it
            # deletes, and the only ones whose handles have to be closed by the
            # time it returns.
            if "gpio_disk_rewrite_" in str(source):
                opened.append(handle)
            return handle

        monkeypatch.setattr(pq, "ParquetFile", tracking_parquet_file)

        source = _write_points_geoparquet(tmp_path / "src.parquet", 3)
        out = str(tmp_path / "handles.parquet")

        _write_via_query(source, out, "2.0", "disk-rewrite", verbose=True)

        assert opened, "no temp-file ParquetFile opened -- test no longer exercises the path"
        assert [h.closed for h in opened] == [True] * len(opened)

    def test_disk_rewrite_native_type_survives_row_group_coarsening(self, tmp_path):
        """The rewrite is per row group, so a resized write must stay native throughout."""
        source = _write_points_geoparquet(tmp_path / "src.parquet", 25)
        out = str(tmp_path / "coarsened.parquet")

        _write_via_query(source, out, "2.0", "disk-rewrite", row_group_rows=10)

        pf = pq.ParquetFile(out)
        shape = [pf.metadata.row_group(i).num_rows for i in range(pf.metadata.num_row_groups)]
        assert shape == [10, 10, 5]
        assert _geometry_carriers(out) == {"geometry": "native"}
        assert pq.read_table(out).column("id").to_pylist() == list(range(25))
        assert _failed_checks(out) == []


class TestNativeWkbTypeHelpers:
    """The two pieces every native-type write path shares."""

    WKB_POINT = struct.pack("<BI2d", 1, 1, 1.0, 2.0)

    def test_native_wkb_type_omits_a_default_crs(self):
        """A default CRS is carried by omission, the way the geo block omits the key."""
        import geoarrow.pyarrow as ga

        from geoparquet_io.core.geoarrow_encoding import native_wkb_type

        assert native_wkb_type(None).crs == ga.wkb().crs
        assert native_wkb_type({"id": {"authority": "OGC", "code": "CRS84"}}).crs == ga.wkb().crs

    def test_native_wkb_type_carries_a_projected_crs(self):
        """The CRS the geo block declares is what the logical type must carry."""
        from geoparquet_io.core.geoarrow_encoding import native_wkb_type

        crs = {"id": {"authority": "EPSG", "code": 3857}}

        assert json.loads(native_wkb_type(crs).crs.to_json())["id"]["code"] == 3857

    def test_to_geoarrow_column_keeps_an_already_matching_array(self):
        """The common path: plain WKB binary already matches ``geoarrow.wkb``."""
        from geoparquet_io.core.geoarrow_encoding import native_wkb_type
        from geoparquet_io.core.write_strategies.arrow_streaming import to_geoarrow_column

        converted = to_geoarrow_column(
            pa.array([self.WKB_POINT], type=pa.binary()), native_wkb_type(None)
        )

        assert converted.type.extension_name == "geoarrow.wkb"
        assert converted.storage.to_pylist() == [self.WKB_POINT]

    def test_to_geoarrow_column_retypes_when_the_storage_differs(self):
        """A target whose storage differs takes the rebuild path, bytes intact."""
        import geoarrow.pyarrow as ga

        from geoparquet_io.core.write_strategies.arrow_streaming import to_geoarrow_column

        converted = to_geoarrow_column(pa.array([self.WKB_POINT], type=pa.binary()), ga.large_wkb())

        assert converted.type.storage_type == pa.large_binary()
        assert converted.storage.to_pylist() == [self.WKB_POINT]


# ---------------------------------------------------------------------------
# The native GEOMETRY type's CRS agrees with the geo block's (#848)
# ---------------------------------------------------------------------------


def _type_crs(path: str) -> dict[str, tuple[str, int] | None]:
    """``(authority, code)`` carried by each geometry column's Parquet logical type.

    Read back through PyArrow with ``geoarrow.pyarrow`` imported, which is what
    resolves a native GEOMETRY logical type into a typed field carrying its CRS.
    ``None`` means the type carries no CRS, i.e. the file claims the spec default.

    Not comparable by PyArrow type equality: ``WkbType.__eq__`` ignores the CRS,
    so a CRS84 column and an EPSG:3857 one compare equal.
    """
    import geoarrow.pyarrow  # noqa: F401  -- registers the extension type

    schema = pq.read_schema(path)
    return {
        name: _crs_identity(getattr(schema.field(name).type, "crs", None))
        for name in ("geometry", "geom2")
        if name in schema.names
    }


def _block_crs(path: str) -> dict[str, tuple[str, int] | None]:
    """``(authority, code)`` each column's ``geo`` entry declares; ``{}`` with no block."""
    geo = json.loads((pq.read_schema(path).metadata or {}).get(b"geo") or b"{}")
    return {
        name: _crs_identity(entry.get("crs")) for name, entry in (geo.get("columns") or {}).items()
    }


def _crs_identity(crs) -> tuple[str, int] | None:
    """Normalize a PROJJSON dict or a geoarrow CRS object down to its authority code."""
    if crs is None:
        return None
    if not isinstance(crs, dict):
        crs = json.loads(crs.to_json())
    identity = crs.get("id") or {}
    return (identity.get("authority"), identity.get("code")) if identity else None


def _projected_source(tmp_path, *, secondary: bool) -> str:
    """A 1.1 source whose geometry columns each declare their own projected CRS."""
    import pyproj

    columns = {
        "geometry": {
            "encoding": "WKB",
            "geometry_types": ["Point"],
            "crs": pyproj.CRS.from_authority("EPSG", "3857").to_json_dict(),
        }
    }
    points = [struct.pack("<BI2d", 1, 1, float(i) * 1000, float(i) * 1000) for i in range(6)]
    data = {"id": [0, 1, 2], "geometry": points[:3]}
    if secondary:
        columns["geom2"] = {
            "encoding": "WKB",
            "geometry_types": ["Point"],
            "crs": pyproj.CRS.from_authority("EPSG", "27700").to_json_dict(),
        }
        data["geom2"] = points[3:]

    geo_meta = {"version": "1.1.0", "primary_column": "geometry", "columns": columns}
    source = str(tmp_path / "projected.parquet")
    pq.write_table(
        pa.table(data).replace_schema_metadata({b"geo": json.dumps(geo_meta).encode()}), source
    )
    return source


class TestNativeGeometryCrsAcrossStrategies:
    """Every strategy takes the logical type's CRS from the ``geo`` block it just built.

    Regression suite for #848. At 2.0 the CRS lives in both the native Parquet
    GEOMETRY logical type and the ``geo`` block, and ``gpio check spec`` requires
    them to agree. ``duckdb-kv`` and (since #764) ``disk-rewrite`` read each
    column's CRS out of the metadata built for that same file; the two Arrow-side
    strategies built the type from the incoming Arrow field alone, so a write that
    threads no ``input_crs`` -- the ordinary case, since only a reprojection
    supplies one -- produced a bare Geometry type beside a block declaring
    EPSG:3857, failing ``v2_crs_consistency_geometry`` and
    ``v2_crs_in_parquet_type_geometry`` on gpio's own output.

    Under parquet-geo-only there is no block to disagree with and nothing fails
    those two checks -- the type is the column's only geometry identity, so
    dropping the CRS silently relabels projected coordinates as CRS84. That is
    the worse bug of the two, and it is ``coordinates_valid_for_crs`` that
    catches it.
    """

    @pytest.mark.parametrize("strategy", ALL_STRATEGIES)
    @pytest.mark.parametrize("version", NATIVE_VERSIONS)
    def test_primary_crs_reaches_the_logical_type(self, strategy, version, tmp_path):
        """The issue's reproducer: one projected geometry column, no ``input_crs``."""
        source = _projected_source(tmp_path, secondary=False)
        out = str(tmp_path / f"{strategy}_{version}.parquet")

        _write_via_query(source, out, version, strategy)

        assert _type_crs(out) == {"geometry": ("EPSG", 3857)}
        if version == "2.0":
            assert _block_crs(out) == {"geometry": ("EPSG", 3857)}
        assert _failed_checks(out) == []

    @pytest.mark.parametrize("strategy", ALL_STRATEGIES)
    @pytest.mark.parametrize("version", NATIVE_VERSIONS)
    def test_each_column_takes_its_own_crs(self, strategy, version, tmp_path):
        """A secondary column's type carries *its* CRS, not the primary's (#706)."""
        source = _projected_source(tmp_path, secondary=True)
        out = str(tmp_path / f"{strategy}_{version}_two.parquet")

        _write_via_query(
            source,
            out,
            version,
            strategy,
            geometry_info={
                "primary": "geometry",
                "secondary": ["geom2"],
                "metadata": {
                    "geom2": json.loads(pq.read_schema(source).metadata[b"geo"])["columns"]["geom2"]
                },
            },
        )

        assert _type_crs(out) == {"geometry": ("EPSG", 3857), "geom2": ("EPSG", 27700)}
        if version == "2.0":
            assert _block_crs(out) == {"geometry": ("EPSG", 3857), "geom2": ("EPSG", 27700)}
        assert _failed_checks(out) == []

    @pytest.mark.parametrize("strategy", ALL_STRATEGIES)
    def test_a_requested_crs_still_wins_over_the_source_crs(self, strategy, tmp_path):
        """``input_crs`` is the output CRS when a caller threads one, on every strategy.

        The reprojection case, and the guard against fixing one disagreement into
        another: the block applies ``input_crs``, so the type has to follow it
        rather than the CRS the *source* declared.
        """
        import pyproj

        source = _write_points_geoparquet(tmp_path / "src.parquet", 3)
        crs = pyproj.CRS.from_authority("EPSG", "3857").to_json_dict()
        out = str(tmp_path / f"{strategy}_requested.parquet")

        _write_via_query(source, out, "2.0", strategy, input_crs=crs)

        assert _type_crs(out) == {"geometry": ("EPSG", 3857)}
        assert _block_crs(out) == {"geometry": ("EPSG", 3857)}

    def test_an_explicit_default_input_crs_clears_a_projected_field_crs(self, tmp_path):
        """A requested spec-default CRS beats the CRS the Arrow field carries.

        The in-memory table entry point with ``input_crs`` spelling out
        OGC:CRS84: the geo block drops it (the default is declared by omission),
        so the logical type has to come out bare too. Resurrecting the incoming
        field's EPSG:3857 puts a projected CRS on the type beside a block
        claiming the default -- ``v2_crs_consistency_geometry`` fails on gpio's
        own output.
        """
        import geoarrow.pyarrow as ga
        import pyproj

        wkb = [struct.pack("<BI2d", 1, 1, float(i), float(i)) for i in range(3)]
        projected = pyproj.CRS.from_authority("EPSG", "3857").to_json_dict()
        geom = ga.wkb().with_crs(projected).wrap_array(pa.array(wkb, type=pa.binary()))
        table = pa.table({"id": [0, 1, 2], "geometry": geom})
        out = str(tmp_path / "in_memory_default_input_crs.parquet")

        WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_MEMORY).write_from_table(
            table=table,
            output_path=out,
            geometry_column="geometry",
            geoparquet_version="2.0",
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=None,
            verbose=False,
            input_crs=pyproj.CRS.from_authority("OGC", "CRS84").to_json_dict(),
        )

        # Probed on the physical logical type: PyArrow's read-back attaches the
        # default OGC:CRS84 to a bare GEOMETRY type, so ``_type_crs`` cannot
        # tell "bare" from "spelled out".
        con = get_duckdb_connection(load_spatial=False)
        try:
            logical_types = dict(
                con.execute(
                    f"SELECT name, logical_type FROM parquet_schema({sql_path(out)})"
                ).fetchall()
            )
        finally:
            con.close()
        assert "crs=<null>" in str(logical_types["geometry"])
        assert _block_crs(out) == {"geometry": None}
        assert _failed_checks(out) == []

    def test_the_field_crs_fallback_survives_for_unresolved_columns(self):
        """A column no geo block has resolved keeps the CRS its Arrow field carries.

        The guard against the opposite bug: with ``crs_resolved`` False (the
        default), a missing ``input_crs`` must not clear a projected field CRS
        -- that would silently relabel projected coordinates as CRS84.
        """
        import geoarrow.pyarrow as ga
        import pyproj

        from geoparquet_io.core.arrow_geo_metadata import _process_geometry_column_for_version

        projected = pyproj.CRS.from_authority("EPSG", "3857").to_json_dict()
        wkb = [struct.pack("<BI2d", 1, 1, 1.0, 2.0)]
        geom = ga.wkb().with_crs(projected).wrap_array(pa.array(wkb, type=pa.binary()))
        table = pa.table({"geometry": geom})

        processed = _process_geometry_column_for_version(table, "geometry", "2.0", None, False)

        assert _crs_identity(processed.schema.field("geometry").type.crs) == ("EPSG", 3857)

    def test_streaming_table_entry_point_keeps_the_crs_with_no_geo_block(self, tmp_path):
        """``Table.write()`` under parquet-geo-only: the type is the only identity left.

        The streaming strategy builds a block for this version too and drops it
        again, which is the only thing keeping a requested CRS on the column once
        there is no ``geo`` key to read it back out of.

        Asserted on the logical type alone: this entry point still lets the
        *input's* carried ``geo`` key ride into the output, which is the separate
        leak tracked in #773 and is not what this write path decides.
        """
        import pyproj

        crs = pyproj.CRS.from_authority("EPSG", "3857").to_json_dict()
        source = _write_points_geoparquet(tmp_path / "src.parquet", 3)
        out = str(tmp_path / "streaming_table_geo_only.parquet")

        WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_STREAMING).write_from_table(
            table=pq.read_table(source),
            output_path=out,
            geometry_column="geometry",
            geoparquet_version="parquet-geo-only",
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=None,
            verbose=False,
            input_crs=crs,
        )

        assert _type_crs(out) == {"geometry": ("EPSG", 3857)}

    def test_verbose_write_names_the_crs_it_put_in_the_block(self, tmp_path, caplog):
        """The in-memory path logs the CRS it resolved; an f-string only ``--verbose`` runs."""
        import pyproj

        source = _write_points_geoparquet(tmp_path / "src.parquet", 3)
        out = str(tmp_path / "verbose_crs.parquet")

        with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
            _write_via_query(
                source,
                out,
                "2.0",
                "in-memory",
                verbose=True,
                input_crs=pyproj.CRS.from_authority("EPSG", "3857").to_json_dict(),
            )

        assert "added crs to geo metadata" in caplog.text.lower()
        assert _type_crs(out) == {"geometry": ("EPSG", 3857)}


class TestMergeSecondaryGeometryMetadata:
    """The one definition of "give every secondary column a ``geo`` entry"."""

    def test_an_undeclared_secondary_still_gets_the_required_encoding(self):
        """``encoding`` is required by the spec, so a column the input left bare gets WKB."""
        from geoparquet_io.core.write_strategies.base import merge_secondary_geometry_metadata

        geo_meta = {"columns": {"geometry": {"encoding": "WKB"}}}

        merge_secondary_geometry_metadata(geo_meta, {"secondary": ["geom2"], "metadata": {}})

        assert geo_meta["columns"]["geom2"] == {"encoding": "WKB"}

    def test_the_input_metadata_wins_over_the_default(self):
        """A declared secondary keeps its own crs and encoding -- what the native type reads."""
        from geoparquet_io.core.write_strategies.base import merge_secondary_geometry_metadata

        crs = {"id": {"authority": "EPSG", "code": 27700}}
        geo_meta = {"columns": {"geometry": {"encoding": "WKB"}}}

        merge_secondary_geometry_metadata(
            geo_meta,
            {"secondary": ["geom2"], "metadata": {"geom2": {"encoding": "point", "crs": crs}}},
        )

        assert geo_meta["columns"]["geom2"] == {"encoding": "point", "crs": crs}
