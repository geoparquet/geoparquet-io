"""
Tests for the Python API (fluent Table class and ops module).
"""

from __future__ import annotations

import tempfile
import uuid
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.api import Table, convert, ops, pipe, read
from tests.conftest import safe_unlink

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
        safe_unlink(tmp_path)

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


class TestConvert:
    """Tests for gpio.convert() entry point."""

    @pytest.fixture
    def gpkg_file(self):
        """Get path to test GeoPackage file."""
        path = TEST_DATA_DIR / "buildings_test.gpkg"
        if not path.exists():
            pytest.skip("GeoPackage test data not available")
        return str(path)

    @pytest.fixture
    def geojson_file(self):
        """Get path to test GeoJSON file."""
        path = TEST_DATA_DIR / "buildings_test.geojson"
        if not path.exists():
            pytest.skip("GeoJSON test data not available")
        return str(path)

    @pytest.fixture
    def csv_wkt_file(self):
        """Get path to test CSV file with WKT geometry."""
        path = TEST_DATA_DIR / "points_wkt.csv"
        if not path.exists():
            pytest.skip("CSV WKT test data not available")
        return str(path)

    @pytest.fixture
    def output_file(self):
        """Create a temporary output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_convert_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_convert_geopackage_returns_table(self, gpkg_file):
        """Test that convert() returns a Table for GeoPackage input."""
        table = convert(gpkg_file)
        assert isinstance(table, Table)
        assert table.num_rows > 0

    def test_convert_geojson_returns_table(self, geojson_file):
        """Test that convert() returns a Table for GeoJSON input."""
        table = convert(geojson_file)
        assert isinstance(table, Table)
        assert table.num_rows > 0

    def test_convert_csv_with_wkt(self, csv_wkt_file):
        """Test converting CSV with WKT column."""
        table = convert(csv_wkt_file)
        assert isinstance(table, Table)
        assert "geometry" in table.column_names

    def test_convert_detects_geometry_column(self, gpkg_file):
        """Test that convert() detects geometry column."""
        table = convert(gpkg_file)
        assert table.geometry_column == "geometry"

    def test_convert_with_write(self, csv_wkt_file, output_file):
        """Test writing converted data."""
        # Test that convert -> write chain works (CSV has simpler geometry)
        convert(csv_wkt_file).write(output_file)
        assert Path(output_file).exists()

        # Verify output
        loaded = pq.read_table(output_file)
        assert loaded.num_rows > 0
        assert "geometry" in loaded.column_names


class TestTableUpload:
    """Tests for Table.upload() method."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    def test_upload_writes_temp_and_calls_upload(self, sample_table):
        """Test that upload() writes to temp file and calls core upload."""
        with patch("geoparquet_io.core.upload.upload") as mock_upload:
            with patch("geoparquet_io.core.common.setup_aws_profile_if_needed"):
                # Make upload a no-op
                mock_upload.return_value = None

                sample_table.upload("s3://test-bucket/test.parquet")

                # Verify upload was called
                mock_upload.assert_called_once()
                call_args = mock_upload.call_args
                assert call_args.kwargs["destination"] == "s3://test-bucket/test.parquet"

    def test_upload_with_s3_endpoint(self, sample_table):
        """Test upload() with custom S3 endpoint."""
        with patch("geoparquet_io.core.upload.upload") as mock_upload:
            with patch("geoparquet_io.core.common.setup_aws_profile_if_needed"):
                mock_upload.return_value = None

                sample_table.upload(
                    "s3://test-bucket/test.parquet",
                    s3_endpoint="minio.example.com:9000",
                    s3_use_ssl=False,
                )

                call_args = mock_upload.call_args
                assert call_args.kwargs["s3_endpoint"] == "minio.example.com:9000"
                assert call_args.kwargs["s3_use_ssl"] is False

    def test_upload_cleans_up_temp_file(self, sample_table):
        """Test that upload() cleans up temp file even on error."""
        captured_paths = []

        def capture_and_raise(**kwargs):
            captured_paths.append(kwargs["source"])
            raise Exception("Upload failed")

        with patch("geoparquet_io.core.upload.upload") as mock_upload:
            with patch("geoparquet_io.core.common.setup_aws_profile_if_needed"):
                mock_upload.side_effect = capture_and_raise

                with pytest.raises(Exception, match="Upload failed"):
                    sample_table.upload("s3://test-bucket/test.parquet")

                # Verify the temp file path was captured and cleaned up
                assert len(captured_paths) == 1
                temp_path = captured_paths[0]
                assert not Path(temp_path).exists(), "Temp file should be deleted after error"


class TestTableMetadataProperties:
    """Tests for the new metadata properties on Table."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    def test_crs_property(self, sample_table):
        """Test crs property returns CRS or None."""
        crs = sample_table.crs
        # Can be None (OGC:CRS84 default) or a dict/string
        assert crs is None or isinstance(crs, (dict, str))

    def test_bounds_property(self, sample_table):
        """Test bounds property returns tuple."""
        bounds = sample_table.bounds
        assert bounds is not None
        assert isinstance(bounds, tuple)
        assert len(bounds) == 4
        xmin, ymin, xmax, ymax = bounds
        assert xmin < xmax
        assert ymin < ymax

    def test_schema_property(self, sample_table):
        """Test schema property returns PyArrow Schema."""
        import pyarrow as pa

        schema = sample_table.schema
        assert isinstance(schema, pa.Schema)
        assert "geometry" in [field.name for field in schema]

    def test_geoparquet_version_property(self, sample_table):
        """Test geoparquet_version property returns version string."""
        version = sample_table.geoparquet_version
        # Should be a version string like "1.1" or "1.1.0" or None
        assert version is None or isinstance(version, str)
        if version:
            # Accept patched versions like "1.1.0" by checking major.minor
            major_minor = ".".join(version.split(".")[:2])
            assert major_minor in ["1.0", "1.1", "2.0"]


class TestTableInfo:
    """Tests for the info() method."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    def test_info_verbose_returns_none(self, sample_table, capsys):
        """Test info(verbose=True) prints output and returns None."""
        result = sample_table.info(verbose=True)
        assert result is None

        captured = capsys.readouterr()
        assert "Table:" in captured.out
        assert "766" in captured.out
        assert "Geometry:" in captured.out

    def test_info_dict_mode(self, sample_table):
        """Test info(verbose=False) returns dict."""
        info = sample_table.info(verbose=False)
        assert isinstance(info, dict)
        assert info["rows"] == 766
        assert "geometry_column" in info
        assert "crs" in info
        assert "bounds" in info
        assert "geoparquet_version" in info
        assert "column_names" in info


class TestWriteReturnsPath:
    """Tests for write() returning Path."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    @pytest.fixture
    def output_file(self):
        """Create a temporary output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_write_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_write_returns_path(self, sample_table, output_file):
        """Test that write() returns a Path object."""
        result = sample_table.write(output_file)
        assert isinstance(result, Path)
        assert result.exists()
        assert str(result) == output_file


class TestOpsNewFunctions:
    """Tests for the new ops module functions."""

    @pytest.fixture
    def arrow_table(self):
        """Get an Arrow table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return pq.read_table(PLACES_PARQUET)

    def test_add_h3(self, arrow_table):
        """Test ops.add_h3()."""
        result = ops.add_h3(arrow_table, resolution=7)
        assert isinstance(result, pa.Table)
        assert "h3_cell" in result.column_names

    def test_add_kdtree(self, arrow_table):
        """Test ops.add_kdtree()."""
        result = ops.add_kdtree(arrow_table, iterations=5)
        assert isinstance(result, pa.Table)
        assert "kdtree_cell" in result.column_names

    def test_sort_column(self, arrow_table):
        """Test ops.sort_column()."""
        result = ops.sort_column(arrow_table, column="name")
        assert isinstance(result, pa.Table)
        assert result.num_rows == 766

    def test_sort_quadkey(self, arrow_table):
        """Test ops.sort_quadkey()."""
        result = ops.sort_quadkey(arrow_table, resolution=10)
        assert isinstance(result, pa.Table)
        assert result.num_rows == 766

    def test_reproject(self, arrow_table):
        """Test ops.reproject()."""
        result = ops.reproject(arrow_table, target_crs="EPSG:3857")
        assert isinstance(result, pa.Table)
        assert result.num_rows == 766


class TestReadPartition:
    """Tests for the read_partition() function."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    @pytest.fixture
    def partition_dir(self, sample_table):
        """Create a temporary partitioned directory."""
        tmp_dir = Path(tempfile.gettempdir()) / f"test_partition_{uuid.uuid4()}"
        tmp_dir.mkdir(exist_ok=True)

        # Use the full table (766 rows) which is above the minimum threshold
        sample_table.partition_by_quadkey(tmp_dir, overwrite=True, partition_resolution=3)

        yield tmp_dir

        # Cleanup with retry for Windows file locking
        import shutil
        import time

        for attempt in range(3):
            try:
                shutil.rmtree(tmp_dir)
                break
            except OSError:
                time.sleep(0.1 * (attempt + 1))

    def test_read_partition_from_directory(self, partition_dir):
        """Test reading a partitioned directory."""
        from geoparquet_io import read_partition

        table = read_partition(partition_dir)
        assert isinstance(table, Table)
        assert table.num_rows > 0
        assert table.geometry_column == "geometry"
