"""
Tests for the Python API (fluent Table class and ops module).
"""

from __future__ import annotations

import tempfile
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.api import Table, ops, pipe, read

TEST_DATA_DIR = Path(__file__).parent / "data"
PLACES_PARQUET = TEST_DATA_DIR / "places_test.parquet"


class TestRead:
    """Tests for gpio.read() entry point."""

    def test_read_returns_table(self):
        """Test that read() returns a Table instance."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        table = read(PLACES_PARQUET)
        assert isinstance(table, Table)

    def test_read_preserves_rows(self):
        """Test that read() preserves row count."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        table = read(PLACES_PARQUET)
        assert table.num_rows == 766

    def test_read_detects_geometry(self):
        """Test that read() detects geometry column."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        table = read(PLACES_PARQUET)
        assert table.geometry_column == "geometry"


class TestTable:
    """Tests for the Table class."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    @pytest.fixture
    def output_file(self):
        """Create a temporary output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_api_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        if tmp_path.exists():
            tmp_path.unlink()

    def test_table_repr(self, sample_table):
        """Test Table string representation."""
        repr_str = repr(sample_table)
        assert "Table(" in repr_str
        assert "rows=766" in repr_str
        assert "geometry='geometry'" in repr_str

    def test_to_arrow(self, sample_table):
        """Test converting to PyArrow Table."""
        arrow_table = sample_table.to_arrow()
        assert isinstance(arrow_table, pa.Table)
        assert arrow_table.num_rows == 766

    def test_column_names(self, sample_table):
        """Test getting column names."""
        names = sample_table.column_names
        assert "geometry" in names
        assert "name" in names

    def test_add_bbox(self, sample_table):
        """Test add_bbox() method."""
        result = sample_table.add_bbox()
        assert isinstance(result, Table)
        assert "bbox" in result.column_names
        assert result.num_rows == 766

    def test_add_bbox_custom_name(self, sample_table):
        """Test add_bbox() with custom column name."""
        result = sample_table.add_bbox(column_name="bounds")
        assert "bounds" in result.column_names

    def test_add_quadkey(self, sample_table):
        """Test add_quadkey() method."""
        result = sample_table.add_quadkey(resolution=10)
        assert isinstance(result, Table)
        assert "quadkey" in result.column_names
        assert result.num_rows == 766

    def test_sort_hilbert(self, sample_table):
        """Test sort_hilbert() method."""
        result = sample_table.sort_hilbert()
        assert isinstance(result, Table)
        assert result.num_rows == 766

    def test_extract_columns(self, sample_table):
        """Test extract() with column selection."""
        result = sample_table.extract(columns=["name", "address"])
        assert "name" in result.column_names
        assert "address" in result.column_names
        # geometry is auto-included
        assert "geometry" in result.column_names

    def test_extract_limit(self, sample_table):
        """Test extract() with row limit."""
        result = sample_table.extract(limit=10)
        assert result.num_rows == 10

    def test_chaining(self, sample_table):
        """Test chaining multiple operations."""
        result = sample_table.add_bbox().add_quadkey(resolution=10)
        assert "bbox" in result.column_names
        assert "quadkey" in result.column_names
        assert result.num_rows == 766

    def test_write(self, sample_table, output_file):
        """Test write() method."""
        sample_table.add_bbox().write(output_file)
        assert Path(output_file).exists()

        # Verify output
        loaded = pq.read_table(output_file)
        assert "bbox" in loaded.column_names

    def test_add_h3(self, sample_table):
        """Test add_h3() method."""
        result = sample_table.add_h3()
        assert isinstance(result, Table)
        assert "h3_cell" in result.column_names
        assert result.num_rows == 766

    def test_add_h3_custom_resolution(self, sample_table):
        """Test add_h3() with custom resolution."""
        result = sample_table.add_h3(resolution=5)
        assert "h3_cell" in result.column_names
        assert result.num_rows == 766

    def test_add_h3_custom_column_name(self, sample_table):
        """Test add_h3() with custom column name."""
        result = sample_table.add_h3(column_name="my_h3")
        assert "my_h3" in result.column_names
        assert result.num_rows == 766

    def test_add_kdtree(self, sample_table):
        """Test add_kdtree() method."""
        result = sample_table.add_kdtree()
        assert isinstance(result, Table)
        assert "kdtree_cell" in result.column_names
        assert result.num_rows == 766

    def test_add_kdtree_custom_params(self, sample_table):
        """Test add_kdtree() with custom parameters."""
        result = sample_table.add_kdtree(iterations=5, sample_size=1000)
        assert "kdtree_cell" in result.column_names
        assert result.num_rows == 766

    def test_sort_column(self, sample_table):
        """Test sort_column() method."""
        result = sample_table.sort_column("name")
        assert isinstance(result, Table)
        assert result.num_rows == 766

    def test_sort_column_descending(self, sample_table):
        """Test sort_column() in descending order."""
        result = sample_table.sort_column("name", descending=True)
        assert isinstance(result, Table)
        assert result.num_rows == 766

    def test_sort_quadkey(self, sample_table):
        """Test sort_quadkey() method."""
        result = sample_table.sort_quadkey(resolution=10)
        assert isinstance(result, Table)
        assert result.num_rows == 766
        # Quadkey column should be auto-added
        assert "quadkey" in result.column_names

    def test_sort_quadkey_remove_column(self, sample_table):
        """Test sort_quadkey() with remove_column=True."""
        result = sample_table.sort_quadkey(resolution=10, remove_column=True)
        assert isinstance(result, Table)
        assert result.num_rows == 766
        # Quadkey column should be removed after sorting
        assert "quadkey" not in result.column_names

    def test_reproject(self, sample_table):
        """Test reproject() method."""
        # Reproject to Web Mercator and back to WGS84
        result = sample_table.reproject(target_crs="EPSG:3857")
        assert isinstance(result, Table)
        assert result.num_rows == 766


class TestOps:
    """Tests for the ops module (pure functions)."""

    @pytest.fixture
    def arrow_table(self):
        """Get an Arrow table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return pq.read_table(PLACES_PARQUET)

    def test_add_bbox(self, arrow_table):
        """Test ops.add_bbox()."""
        result = ops.add_bbox(arrow_table)
        assert isinstance(result, pa.Table)
        assert "bbox" in result.column_names

    def test_add_quadkey(self, arrow_table):
        """Test ops.add_quadkey()."""
        result = ops.add_quadkey(arrow_table, resolution=10)
        assert isinstance(result, pa.Table)
        assert "quadkey" in result.column_names

    def test_sort_hilbert(self, arrow_table):
        """Test ops.sort_hilbert()."""
        result = ops.sort_hilbert(arrow_table)
        assert isinstance(result, pa.Table)
        assert result.num_rows == 766

    def test_extract(self, arrow_table):
        """Test ops.extract()."""
        result = ops.extract(arrow_table, limit=10)
        assert isinstance(result, pa.Table)
        assert result.num_rows == 10


class TestPipe:
    """Tests for the pipe() composition helper."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    def test_pipe_empty(self, sample_table):
        """Test pipe with no operations."""
        transform = pipe()
        result = transform(sample_table)
        assert result is sample_table

    def test_pipe_single(self, sample_table):
        """Test pipe with single operation."""
        transform = pipe(lambda t: t.add_bbox())
        result = transform(sample_table)
        assert "bbox" in result.column_names

    def test_pipe_multiple(self, sample_table):
        """Test pipe with multiple operations."""
        transform = pipe(
            lambda t: t.add_bbox(),
            lambda t: t.add_quadkey(resolution=10),
        )
        result = transform(sample_table)
        assert "bbox" in result.column_names
        assert "quadkey" in result.column_names

    def test_pipe_with_ops(self):
        """Test pipe with ops functions on Arrow table."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        arrow_table = pq.read_table(PLACES_PARQUET)
        transform = pipe(
            lambda t: ops.add_bbox(t),
            lambda t: ops.extract(t, limit=10),
        )
        result = transform(arrow_table)
        assert "bbox" in result.column_names
        assert result.num_rows == 10
