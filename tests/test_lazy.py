"""
Tests for the Lazy Execution API (LazyTable class).
"""

from __future__ import annotations

import json
import tempfile
import uuid
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.api.lazy import (
    LazyTable,
    convert_lazy,
    from_arrow,
    from_relation,
    from_table,
    read_lazy,
)
from tests.conftest import safe_unlink

TEST_DATA_DIR = Path(__file__).parent / "data"
BUILDINGS_PARQUET = TEST_DATA_DIR / "buildings_test.parquet"
BUILDINGS_GEOJSON = TEST_DATA_DIR / "buildings_test.geojson"


class TestReadLazy:
    """Tests for gpio.read_lazy() entry point."""

    def test_read_lazy_returns_lazy_table(self):
        """Test that read_lazy() returns a LazyTable instance."""
        if not BUILDINGS_PARQUET.exists():
            pytest.skip("Test data not available")

        table = read_lazy(BUILDINGS_PARQUET)
        assert isinstance(table, LazyTable)

    def test_read_lazy_detects_geometry(self):
        """Test that read_lazy() detects geometry column."""
        if not BUILDINGS_PARQUET.exists():
            pytest.skip("Test data not available")

        table = read_lazy(BUILDINGS_PARQUET)
        assert table.geometry_column == "geometry"

    def test_read_lazy_context_manager(self):
        """Test that LazyTable works as context manager."""
        if not BUILDINGS_PARQUET.exists():
            pytest.skip("Test data not available")

        with read_lazy(BUILDINGS_PARQUET) as table:
            assert isinstance(table, LazyTable)


class TestLazyTableProperties:
    """Tests for LazyTable properties."""

    @pytest.fixture
    def lazy_table(self):
        """Create a LazyTable from test data."""
        if not BUILDINGS_PARQUET.exists():
            pytest.skip("Test data not available")
        return read_lazy(BUILDINGS_PARQUET)

    def test_sql_property(self, lazy_table):
        """Test that .sql property returns SQL string."""
        sql = lazy_table.sql
        assert isinstance(sql, str)
        assert "SELECT" in sql or "select" in sql.lower()

    def test_geometry_column_property(self, lazy_table):
        """Test geometry_column property."""
        assert lazy_table.geometry_column == "geometry"

    def test_crs_property(self, lazy_table):
        """Test crs property returns dict or None."""
        crs = lazy_table.crs
        # CRS can be None or a dict
        assert crs is None or isinstance(crs, dict)


class TestLazyTableTransforms:
    """Tests for LazyTable transform methods."""

    @pytest.fixture
    def lazy_table(self):
        """Create a LazyTable from test data."""
        if not BUILDINGS_PARQUET.exists():
            pytest.skip("Test data not available")
        return read_lazy(BUILDINGS_PARQUET)

    def test_add_bbox_returns_new_instance(self, lazy_table):
        """Test that add_bbox() returns a new LazyTable."""
        result = lazy_table.add_bbox()
        assert isinstance(result, LazyTable)
        assert result is not lazy_table  # Immutable pattern

    def test_add_bbox_sql(self, lazy_table):
        """Test that add_bbox() adds bbox to SQL."""
        result = lazy_table.add_bbox()
        sql = result.sql
        assert "ST_XMin" in sql
        assert "ST_YMin" in sql
        assert "bbox" in sql

    def test_extract_with_bbox(self, lazy_table):
        """Test extract() with bbox filter."""
        result = lazy_table.extract(bbox=(-180, -90, 180, 90))
        assert isinstance(result, LazyTable)
        sql = result.sql
        assert "ST_Intersects" in sql or "ST_MakeEnvelope" in sql

    def test_extract_with_where(self, lazy_table):
        """Test extract() with WHERE clause."""
        result = lazy_table.extract(where="1=1")
        assert isinstance(result, LazyTable)
        sql = result.sql
        assert "1=1" in sql

    def test_extract_with_limit(self, lazy_table):
        """Test extract() with limit."""
        result = lazy_table.extract(limit=10)
        sql = result.sql
        assert "LIMIT 10" in sql

    def test_sort_hilbert(self, lazy_table):
        """Test sort_hilbert() transform."""
        result = lazy_table.sort_hilbert()
        assert isinstance(result, LazyTable)
        sql = result.sql
        assert "ST_Hilbert" in sql

    def test_sort_column(self, lazy_table):
        """Test sort_column() transform."""
        result = lazy_table.sort_column("name")
        sql = result.sql
        assert "ORDER BY" in sql
        assert "name" in sql.lower()

    def test_reproject(self, lazy_table):
        """Test reproject() transform."""
        result = lazy_table.reproject("EPSG:32610")
        assert isinstance(result, LazyTable)
        sql = result.sql
        assert "ST_Transform" in sql
        # CRS should be updated
        assert result.crs is not None
        assert result.crs.get("id", {}).get("code") == 32610

    def test_chained_transforms(self, lazy_table):
        """Test chaining multiple transforms."""
        result = lazy_table.add_bbox().extract(limit=100).sort_hilbert()
        assert isinstance(result, LazyTable)
        sql = result.sql
        # All transforms should be in the SQL
        assert "bbox" in sql.lower()
        assert "LIMIT 100" in sql
        assert "ST_Hilbert" in sql


class TestLazyTableTerminalOps:
    """Tests for LazyTable terminal operations."""

    @pytest.fixture
    def lazy_table(self):
        """Create a LazyTable from test data."""
        if not BUILDINGS_PARQUET.exists():
            pytest.skip("Test data not available")
        return read_lazy(BUILDINGS_PARQUET)

    @pytest.fixture
    def output_file(self):
        """Create a temporary output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_lazy_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_collect_returns_arrow(self, lazy_table):
        """Test that collect() returns Arrow table."""
        result = lazy_table.collect()
        assert isinstance(result, pa.Table)
        assert result.num_rows > 0

    def test_count_returns_int(self, lazy_table):
        """Test that count() returns row count."""
        count = lazy_table.count()
        assert isinstance(count, int)
        assert count > 0

    def test_explain_returns_string(self, lazy_table):
        """Test that explain() returns query plan."""
        plan = lazy_table.explain()
        assert isinstance(plan, str)

    def test_write_creates_file(self, lazy_table, output_file):
        """Test that write() creates a parquet file."""
        result_path = lazy_table.write(output_file)
        assert result_path.exists()
        # Verify it's a valid parquet file
        pf = pq.ParquetFile(output_file)
        assert pf.metadata.num_rows > 0

    def test_write_with_transforms(self, lazy_table, output_file):
        """Test write() after transforms."""
        lazy_table.add_bbox().extract(limit=5).write(output_file)
        pf = pq.ParquetFile(output_file)
        assert pf.metadata.num_rows == 5
        # Check bbox column was added
        schema = pf.schema_arrow
        assert "bbox" in schema.names

    def test_write_has_geo_metadata(self, lazy_table, output_file):
        """Test that write() produces GeoParquet with geo metadata."""
        lazy_table.write(output_file)
        pf = pq.ParquetFile(output_file)
        metadata = pf.schema_arrow.metadata
        assert metadata is not None
        assert b"geo" in metadata
        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        assert "version" in geo_meta
        assert "columns" in geo_meta


class TestFromTable:
    """Tests for from_table() entry point."""

    @pytest.fixture
    def duckdb_connection(self):
        """Create a DuckDB connection with test data."""
        if not BUILDINGS_PARQUET.exists():
            pytest.skip("Test data not available")

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")
        con.execute(f"CREATE TABLE test_table AS SELECT * FROM read_parquet('{BUILDINGS_PARQUET}')")
        yield con
        con.close()

    @pytest.fixture
    def output_file(self):
        """Create a temporary output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_lazy_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_from_table_returns_lazy_table(self, duckdb_connection):
        """Test that from_table() returns LazyTable."""
        table = from_table("test_table", duckdb_connection)
        assert isinstance(table, LazyTable)

    def test_from_table_collect(self, duckdb_connection):
        """Test collect() from from_table()."""
        table = from_table("test_table", duckdb_connection)
        result = table.collect()
        assert isinstance(result, pa.Table)
        assert result.num_rows > 0

    def test_from_table_with_crs(self, duckdb_connection):
        """Test from_table() with explicit CRS."""
        table = from_table("test_table", duckdb_connection, crs="EPSG:4326")
        assert table.crs is not None
        assert table.crs.get("id", {}).get("code") == 4326

    def test_from_table_write(self, duckdb_connection, output_file):
        """Test writing from from_table()."""
        from_table("test_table", duckdb_connection).write(output_file)
        assert Path(output_file).exists()


class TestFromRelation:
    """Tests for from_relation() entry point."""

    @pytest.fixture
    def duckdb_relation(self):
        """Create a DuckDB relation from test data."""
        if not BUILDINGS_PARQUET.exists():
            pytest.skip("Test data not available")

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")
        rel = con.sql(f"SELECT * FROM read_parquet('{BUILDINGS_PARQUET}')")
        yield rel
        con.close()

    def test_from_relation_returns_lazy_table(self, duckdb_relation):
        """Test that from_relation() returns LazyTable."""
        table = from_relation(duckdb_relation)
        assert isinstance(table, LazyTable)

    def test_from_relation_collect(self, duckdb_relation):
        """Test collect() from from_relation()."""
        table = from_relation(duckdb_relation)
        result = table.collect()
        assert isinstance(result, pa.Table)
        assert result.num_rows > 0


class TestFromArrow:
    """Tests for from_arrow() entry point."""

    @pytest.fixture
    def arrow_table(self):
        """Create an Arrow table from test data."""
        if not BUILDINGS_PARQUET.exists():
            pytest.skip("Test data not available")
        return pq.read_table(BUILDINGS_PARQUET)

    @pytest.fixture
    def output_file(self):
        """Create a temporary output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_lazy_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_from_arrow_returns_lazy_table(self, arrow_table):
        """Test that from_arrow() returns LazyTable."""
        table = from_arrow(arrow_table)
        assert isinstance(table, LazyTable)

    def test_from_arrow_collect(self, arrow_table):
        """Test collect() from from_arrow()."""
        table = from_arrow(arrow_table)
        result = table.collect()
        assert isinstance(result, pa.Table)
        assert result.num_rows == arrow_table.num_rows

    def test_from_arrow_with_transforms(self, arrow_table, output_file):
        """Test transforms on from_arrow() table."""
        from_arrow(arrow_table).add_bbox().write(output_file)
        pf = pq.ParquetFile(output_file)
        schema = pf.schema_arrow
        assert "bbox" in schema.names


@pytest.mark.slow
class TestConvertLazy:
    """Tests for convert_lazy() entry point."""

    @pytest.fixture
    def output_file(self):
        """Create a temporary output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_lazy_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_convert_lazy_returns_lazy_table(self):
        """Test that convert_lazy() returns LazyTable."""
        if not BUILDINGS_GEOJSON.exists():
            pytest.skip("Test data not available")

        table = convert_lazy(BUILDINGS_GEOJSON)
        assert isinstance(table, LazyTable)

    def test_convert_lazy_collect(self):
        """Test collect() from convert_lazy()."""
        if not BUILDINGS_GEOJSON.exists():
            pytest.skip("Test data not available")

        table = convert_lazy(BUILDINGS_GEOJSON)
        result = table.collect()
        assert isinstance(result, pa.Table)
        assert result.num_rows > 0

    def test_convert_lazy_write(self, output_file):
        """Test write() from convert_lazy()."""
        if not BUILDINGS_GEOJSON.exists():
            pytest.skip("Test data not available")

        convert_lazy(BUILDINGS_GEOJSON).write(output_file)
        assert Path(output_file).exists()
        pf = pq.ParquetFile(output_file)
        assert pf.metadata.num_rows > 0


class TestLazyTableImmutability:
    """Tests for LazyTable immutability pattern."""

    @pytest.fixture
    def lazy_table(self):
        """Create a LazyTable from test data."""
        if not BUILDINGS_PARQUET.exists():
            pytest.skip("Test data not available")
        return read_lazy(BUILDINGS_PARQUET)

    def test_transforms_return_new_instances(self, lazy_table):
        """Test that all transforms return new instances."""
        t1 = lazy_table.add_bbox()
        t2 = lazy_table.extract(limit=10)
        t3 = lazy_table.sort_hilbert()

        # All should be different instances
        assert t1 is not lazy_table
        assert t2 is not lazy_table
        assert t3 is not lazy_table
        assert t1 is not t2
        assert t2 is not t3

    def test_original_unchanged_after_transform(self, lazy_table):
        """Test that original table is unchanged after transforms."""
        original_sql = lazy_table.sql

        # Apply transforms
        lazy_table.add_bbox()
        lazy_table.extract(limit=10)
        lazy_table.sort_hilbert()

        # Original should be unchanged
        assert lazy_table.sql == original_sql
