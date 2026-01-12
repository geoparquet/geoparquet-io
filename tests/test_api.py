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


class TestTableHeadTail:
    """Tests for head() and tail() methods."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    def test_head_default(self, sample_table):
        """Test head() returns first 10 rows by default."""
        result = sample_table.head()
        assert isinstance(result, Table)
        assert result.num_rows == 10
        assert result.geometry_column == sample_table.geometry_column

    def test_head_custom_n(self, sample_table):
        """Test head() with custom n value."""
        result = sample_table.head(25)
        assert result.num_rows == 25

    def test_head_larger_than_table(self, sample_table):
        """Test head() with n larger than table size."""
        result = sample_table.head(10000)
        assert result.num_rows == sample_table.num_rows

    def test_tail_default(self, sample_table):
        """Test tail() returns last 10 rows by default."""
        result = sample_table.tail()
        assert isinstance(result, Table)
        assert result.num_rows == 10
        assert result.geometry_column == sample_table.geometry_column

    def test_tail_custom_n(self, sample_table):
        """Test tail() with custom n value."""
        result = sample_table.tail(25)
        assert result.num_rows == 25

    def test_tail_larger_than_table(self, sample_table):
        """Test tail() with n larger than table size."""
        result = sample_table.tail(10000)
        assert result.num_rows == sample_table.num_rows

    def test_head_zero(self, sample_table):
        """Test head(0) returns empty Table."""
        result = sample_table.head(0)
        assert isinstance(result, Table)
        assert result.num_rows == 0

    def test_head_negative_raises(self, sample_table):
        """Test head(-1) raises ValueError."""
        with pytest.raises(ValueError, match="n must be non-negative"):
            sample_table.head(-1)

    def test_tail_zero(self, sample_table):
        """Test tail(0) returns empty Table."""
        result = sample_table.tail(0)
        assert isinstance(result, Table)
        assert result.num_rows == 0

    def test_tail_negative_raises(self, sample_table):
        """Test tail(-1) raises ValueError."""
        with pytest.raises(ValueError, match="n must be non-negative"):
            sample_table.tail(-1)


class TestTableStats:
    """Tests for stats() method."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    def test_stats_returns_dict(self, sample_table):
        """Test stats() returns a dictionary."""
        result = sample_table.stats()
        assert isinstance(result, dict)

    def test_stats_has_all_columns(self, sample_table):
        """Test stats() includes all columns."""
        result = sample_table.stats()
        for col_name in sample_table.column_names:
            assert col_name in result

    def test_stats_structure(self, sample_table):
        """Test stats() returns expected structure per column."""
        result = sample_table.stats()
        for _col_name, col_stats in result.items():
            assert "nulls" in col_stats
            assert "min" in col_stats
            assert "max" in col_stats
            assert "unique" in col_stats

    def test_stats_geometry_column(self, sample_table):
        """Test stats() handles geometry columns correctly."""
        result = sample_table.stats()
        geom_col = sample_table.geometry_column
        if geom_col:
            assert result[geom_col]["min"] is None
            assert result[geom_col]["max"] is None


class TestTableMetadata:
    """Tests for metadata() method."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    def test_metadata_returns_dict(self, sample_table):
        """Test metadata() returns a dictionary."""
        result = sample_table.metadata()
        assert isinstance(result, dict)

    def test_metadata_has_basic_fields(self, sample_table):
        """Test metadata() includes basic fields."""
        result = sample_table.metadata()
        assert "rows" in result
        assert "columns_count" in result
        assert "geometry_column" in result
        assert "columns" in result

    def test_metadata_rows_match(self, sample_table):
        """Test metadata() rows matches table."""
        result = sample_table.metadata()
        assert result["rows"] == sample_table.num_rows

    def test_metadata_columns_structure(self, sample_table):
        """Test metadata() columns have expected structure."""
        result = sample_table.metadata()
        for col in result["columns"]:
            assert "name" in col
            assert "type" in col
            assert "is_geometry" in col

    def test_metadata_includes_geo_metadata(self, sample_table):
        """Test metadata() includes geo_metadata for GeoParquet files."""
        result = sample_table.metadata()
        # The test file should have geo metadata
        if result.get("geoparquet_version"):
            assert "geo_metadata" in result

    def test_metadata_with_parquet_metadata(self, sample_table):
        """Test metadata() includes parquet metadata when requested."""
        result = sample_table.metadata(include_parquet_metadata=True)
        # If there's any metadata besides 'geo', it should be included
        assert isinstance(result, dict)


class TestTableToGeojson:
    """Tests for to_geojson() method."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        # Use a smaller subset for faster tests
        return read(PLACES_PARQUET).head(10)

    @pytest.fixture
    def output_file(self, tmp_path):
        """Create a temporary output file path using pytest's tmp_path fixture."""
        file_path = tmp_path / f"test_geojson_{uuid.uuid4()}.geojson"
        yield str(file_path)
        safe_unlink(file_path)

    def test_to_geojson_to_file(self, sample_table, output_file):
        """Test to_geojson() writes to file."""
        result = sample_table.to_geojson(output_file)
        assert result == output_file
        assert Path(output_file).exists()

    def test_to_geojson_file_is_valid_json(self, sample_table, output_file):
        """Test to_geojson() produces valid JSON."""
        import json

        sample_table.to_geojson(output_file)
        with open(output_file) as f:
            data = json.load(f)
        assert "type" in data
        assert data["type"] == "FeatureCollection"


class TestCheckResult:
    """Tests for CheckResult class."""

    def test_check_result_import(self):
        """Test CheckResult can be imported."""
        from geoparquet_io.api.check import CheckResult

        result = CheckResult({"passed": True}, check_type="test")
        assert result.passed()

    def test_check_result_passed(self):
        """Test CheckResult.passed() method."""
        from geoparquet_io.api.check import CheckResult

        passing = CheckResult({"passed": True}, check_type="test")
        assert passing.passed()

        failing = CheckResult({"passed": False}, check_type="test")
        assert not failing.passed()

    def test_check_result_failures(self):
        """Test CheckResult.failures() method."""
        from geoparquet_io.api.check import CheckResult

        result = CheckResult({"passed": False, "issues": ["Issue 1", "Issue 2"]}, check_type="test")
        failures = result.failures()
        assert len(failures) == 2
        assert "Issue 1" in failures

    def test_check_result_to_dict(self):
        """Test CheckResult.to_dict() method."""
        from geoparquet_io.api.check import CheckResult

        raw = {"passed": True, "some_data": 123}
        result = CheckResult(raw, check_type="test")
        assert result.to_dict() == raw

    def test_check_result_bool(self):
        """Test CheckResult bool conversion."""
        from geoparquet_io.api.check import CheckResult

        passing = CheckResult({"passed": True}, check_type="test")
        assert bool(passing)

        failing = CheckResult({"passed": False}, check_type="test")
        assert not bool(failing)


class TestTableCheck:
    """Tests for Table check methods."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    def test_check_returns_check_result(self, sample_table):
        """Test check() returns a CheckResult."""
        from geoparquet_io.api.check import CheckResult

        result = sample_table.check()
        assert isinstance(result, CheckResult)
        assert result.check_type == "all"

    def test_check_has_results(self, sample_table):
        """Test check() returns results dict."""
        result = sample_table.check()
        results_dict = result.to_dict()
        assert isinstance(results_dict, dict)

    def test_check_compression_returns_check_result(self, sample_table):
        """Test check_compression() returns a CheckResult."""
        from geoparquet_io.api.check import CheckResult

        result = sample_table.check_compression()
        assert isinstance(result, CheckResult)
        assert result.check_type == "compression"

    def test_check_bbox_returns_check_result(self, sample_table):
        """Test check_bbox() returns a CheckResult."""
        from geoparquet_io.api.check import CheckResult

        result = sample_table.check_bbox()
        assert isinstance(result, CheckResult)
        assert result.check_type == "bbox"

    def test_check_row_groups_returns_check_result(self, sample_table):
        """Test check_row_groups() returns a CheckResult."""
        from geoparquet_io.api.check import CheckResult

        result = sample_table.check_row_groups()
        assert isinstance(result, CheckResult)
        assert result.check_type == "row_groups"


class TestTableAddBboxMetadata:
    """Tests for add_bbox_metadata() method."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    def test_add_bbox_metadata_requires_bbox_column(self, sample_table):
        """Test add_bbox_metadata() raises error if bbox column missing."""
        # Use a non-existent column name to trigger the error
        with pytest.raises(ValueError, match="not found"):
            sample_table.add_bbox_metadata(bbox_column="nonexistent_bbox")

    def test_add_bbox_metadata_with_bbox_column(self, sample_table):
        """Test add_bbox_metadata() works with bbox column."""
        # First add the bbox column, then add metadata
        with_bbox = sample_table.add_bbox()
        with_meta = with_bbox.add_bbox_metadata()
        assert isinstance(with_meta, Table)

        # Check metadata was added
        meta = with_meta.metadata()
        geo_meta = meta.get("geo_metadata", {})
        columns = geo_meta.get("columns", {})
        geom_col = with_meta.geometry_column

        assert geom_col in columns, "Geometry column should be in geo metadata columns"
        covering = columns[geom_col].get("covering")

        # Verify covering structure
        assert covering is not None, "Covering metadata should be present"
        assert isinstance(covering, dict), "Covering should be a dict"
        assert "bbox" in covering, "Covering should have 'bbox' key"

        bbox_paths = covering["bbox"]
        assert isinstance(bbox_paths, dict), "Covering bbox should be a dict"
        assert "xmin" in bbox_paths, "Covering should have xmin path"
        assert "ymin" in bbox_paths, "Covering should have ymin path"
        assert "xmax" in bbox_paths, "Covering should have xmax path"
        assert "ymax" in bbox_paths, "Covering should have ymax path"

        # Each path should be a list like ["bbox", "xmin"]
        for key in ["xmin", "ymin", "xmax", "ymax"]:
            path = bbox_paths[key]
            assert isinstance(path, list), f"Path for {key} should be a list"
            assert len(path) == 2, f"Path for {key} should have 2 elements"


class TestTopLevelExports:
    """Tests for top-level module exports."""

    def test_check_result_exported(self):
        """Test CheckResult is exported from top-level module."""
        from geoparquet_io import CheckResult

        assert CheckResult is not None

    def test_stac_functions_exported(self):
        """Test STAC functions are exported from top-level module."""
        from geoparquet_io import generate_stac, validate_stac

        assert generate_stac is not None
        assert validate_stac is not None
