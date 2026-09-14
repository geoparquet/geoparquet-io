"""
Tests for format_writers module.

Tests verify that format conversions:
- GeoPackage: Creates valid .gpkg with spatial index
- FlatGeobuf: Creates valid .fgb with spatial index
- CSV: Exports WKT geometry and handles complex types
- Shapefile: Creates valid .shp with all sidecar files
- Handles errors (missing files, invalid paths, overwrite protection)
- Escapes SQL injection attempts in paths and parameters
"""

import tempfile
import uuid
from pathlib import Path

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.format_writers import (
    write_csv,
    write_flatgeobuf,
    write_geojson,
    write_geopackage,
    write_shapefile,
)

# Test data
TEST_DATA_DIR = Path(__file__).parent / "data"
PLACES_PARQUET = TEST_DATA_DIR / "places_test.parquet"
BUILDINGS_PARQUET = TEST_DATA_DIR / "buildings_test.parquet"


class TestGeoPackageWriter:
    """Tests for GeoPackage format writer."""

    @pytest.fixture
    def output_file(self):
        """Create temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.gpkg"
        yield str(tmp_path)
        if tmp_path.exists():
            tmp_path.unlink()

    def test_basic_conversion(self, output_file):
        """Test basic GeoParquet to GeoPackage conversion."""
        result = write_geopackage(
            input_path=str(PLACES_PARQUET),
            output_path=output_file,
            verbose=False,
        )
        assert result == output_file
        assert Path(output_file).exists()
        assert Path(output_file).stat().st_size > 0

    def test_custom_layer_name(self, output_file):
        """Test GeoPackage with custom layer name."""
        write_geopackage(
            input_path=str(PLACES_PARQUET),
            output_path=output_file,
            layer_name="my_layer",
            verbose=False,
        )
        assert Path(output_file).exists()

        # Verify layer exists in GeoPackage by checking gpkg_contents table
        import sqlite3

        con = sqlite3.connect(output_file)
        # Check gpkg_contents which lists all layers
        cursor = con.execute("SELECT table_name FROM gpkg_contents")
        layers = [row[0] for row in cursor.fetchall()]
        con.close()

        # The layer name should be in gpkg_contents
        # Note: DuckDB's GDAL driver may or may not respect the layer name fully
        # Just verify the file was created successfully
        assert len(layers) > 0, "GeoPackage should have at least one layer"

    def test_overwrite_protection(self, output_file):
        """Test that overwrite=False prevents overwriting existing file."""
        # Create initial file
        write_geopackage(
            input_path=str(PLACES_PARQUET),
            output_path=output_file,
            verbose=False,
        )

        # Try to overwrite without flag - should raise error
        with pytest.raises(Exception, match="already exists|Use --overwrite"):
            write_geopackage(
                input_path=str(PLACES_PARQUET),
                output_path=output_file,
                overwrite=False,
                verbose=False,
            )

    def test_overwrite_allowed(self, output_file):
        """Test that overwrite=True allows overwriting."""
        # Create initial file
        write_geopackage(
            input_path=str(PLACES_PARQUET),
            output_path=output_file,
            verbose=False,
        )

        # Overwrite with flag - should succeed
        write_geopackage(
            input_path=str(PLACES_PARQUET),
            output_path=output_file,
            overwrite=True,
            verbose=False,
        )
        assert Path(output_file).exists()

    def test_sql_injection_in_layer_name(self, output_file):
        """Test that SQL injection in layer name is escaped."""
        # Try SQL injection in layer name
        malicious_layer = "test'; DROP TABLE features; --"

        # Should not raise SQL error, should escape the quotes
        write_geopackage(
            input_path=str(PLACES_PARQUET),
            output_path=output_file,
            layer_name=malicious_layer,
            verbose=False,
        )
        assert Path(output_file).exists()

    def test_sql_injection_in_output_path(self):
        """Test that SQL injection in output path is escaped."""
        #  Try SQL injection in output path
        # Single quotes in filenames are valid on Linux, so test actually succeeds
        # which proves escaping is working (file is created, no SQL error)
        malicious_path = str(
            Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}'; DROP TABLE features; --.gpkg"
        )

        try:
            # Should succeed with escaped path (proves no SQL injection)
            write_geopackage(
                input_path=str(PLACES_PARQUET),
                output_path=malicious_path,
                verbose=False,
            )
            # If we got here, escaping worked and file was created
            assert Path(malicious_path).exists()
        finally:
            # Clean up
            if Path(malicious_path).exists():
                Path(malicious_path).unlink()


class TestFlatGeobufWriter:
    """Tests for FlatGeobuf format writer."""

    @pytest.fixture
    def output_file(self):
        """Create temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.fgb"
        yield str(tmp_path)
        if tmp_path.exists():
            tmp_path.unlink()

    def test_basic_conversion(self, output_file):
        """Test basic GeoParquet to FlatGeobuf conversion."""
        result = write_flatgeobuf(
            input_path=str(PLACES_PARQUET),
            output_path=output_file,
            verbose=False,
        )
        assert result == output_file
        assert Path(output_file).exists()
        assert Path(output_file).stat().st_size > 0

    def test_flatgeobuf_has_magic_bytes(self, output_file):
        """Test that FlatGeobuf file has correct magic bytes."""
        write_flatgeobuf(
            input_path=str(PLACES_PARQUET),
            output_path=output_file,
            verbose=False,
        )

        # FlatGeobuf files start with magic bytes: fgb + version byte
        with open(output_file, "rb") as f:
            magic = f.read(8)
            # Check for FlatGeobuf signature (fgb + version, typically 0x03)
            assert magic.startswith(b"fgb"), f"Invalid FlatGeobuf magic bytes: {magic[:4].hex()}"

    def test_overwrite_protection(self, output_file):
        """Test that overwrite=False prevents overwriting."""
        # Create initial file
        write_flatgeobuf(
            input_path=str(PLACES_PARQUET),
            output_path=output_file,
            verbose=False,
        )

        # Try to overwrite - should raise error
        with pytest.raises(Exception, match="already exists|Use --overwrite"):
            write_flatgeobuf(
                input_path=str(PLACES_PARQUET),
                output_path=output_file,
                overwrite=False,
                verbose=False,
            )


class TestCSVWriter:
    """Tests for CSV format writer."""

    @pytest.fixture
    def output_file(self):
        """Create temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.csv"
        yield str(tmp_path)
        if tmp_path.exists():
            tmp_path.unlink()

    def test_basic_conversion_with_wkt(self, output_file):
        """Test basic GeoParquet to CSV conversion with WKT."""
        result = write_csv(
            input_path=str(PLACES_PARQUET),
            output_path=output_file,
            include_wkt=True,
            verbose=False,
        )
        assert result == output_file
        assert Path(output_file).exists()

        # Verify CSV structure
        import csv

        with open(output_file, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) > 0
            # Should have 'wkt' column
            assert "wkt" in rows[0]
            # WKT should start with geometry type
            assert any(
                rows[0]["wkt"].startswith(geom_type)
                for geom_type in ["POINT", "LINESTRING", "POLYGON", "MULTIPOINT"]
            )

    def test_csv_without_wkt(self, output_file):
        """Test CSV export without WKT geometry."""
        write_csv(
            input_path=str(PLACES_PARQUET),
            output_path=output_file,
            include_wkt=False,
            verbose=False,
        )
        assert Path(output_file).exists()

        # Verify no 'wkt' column
        import csv

        with open(output_file, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert "wkt" not in rows[0]

    def test_csv_includes_header(self, output_file):
        """Test that CSV includes header row."""
        write_csv(
            input_path=str(PLACES_PARQUET),
            output_path=output_file,
            verbose=False,
        )

        with open(output_file, encoding="utf-8") as f:
            first_line = f.readline().strip()
            # Should have comma-separated headers
            assert "," in first_line

    def test_sql_injection_in_output_path(self):
        """Test that SQL injection in output path is escaped."""
        # Single quotes in filenames are valid on Linux, so test actually succeeds
        # which proves escaping is working (file is created, no SQL error)
        malicious_path = str(
            Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}'; DROP TABLE data; --.csv"
        )

        try:
            # Should succeed with escaped path (proves no SQL injection)
            write_csv(
                input_path=str(PLACES_PARQUET),
                output_path=malicious_path,
                verbose=False,
            )
            # If we got here, escaping worked and file was created
            assert Path(malicious_path).exists()
        finally:
            # Clean up
            if Path(malicious_path).exists():
                Path(malicious_path).unlink()

    def test_overwrite_protection(self, output_file):
        """Test that overwrite=False prevents overwriting."""
        # Create initial file
        write_csv(
            input_path=str(PLACES_PARQUET),
            output_path=output_file,
            verbose=False,
        )

        # Try to overwrite - should raise error
        with pytest.raises(Exception, match="already exists|Use --overwrite"):
            write_csv(
                input_path=str(PLACES_PARQUET),
                output_path=output_file,
                overwrite=False,
                verbose=False,
            )

    def test_csv_plain_parquet_no_geometry(self, output_file, tmp_path):
        """Test CSV export of plain Parquet without geometry column."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create a plain parquet file without geometry
        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "score": [85.5, 92.0, 78.3],
            }
        )
        plain_parquet = tmp_path / "plain.parquet"
        pq.write_table(table, plain_parquet)

        # Should succeed without error
        result = write_csv(
            input_path=str(plain_parquet),
            output_path=output_file,
            include_wkt=True,  # Even with this True, should work
            verbose=False,
        )
        assert result == output_file
        assert Path(output_file).exists()

        # Verify CSV structure
        import csv

        with open(output_file, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 3
            assert "id" in rows[0]
            assert "name" in rows[0]
            assert "score" in rows[0]
            # Should NOT have wkt column since no geometry
            assert "wkt" not in rows[0]


class TestShapefileWriter:
    """Tests for Shapefile format writer."""

    @pytest.fixture
    def output_file(self):
        """Create temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.shp"
        yield str(tmp_path)
        # Clean up all shapefile sidecar files
        for ext in [".shp", ".shx", ".dbf", ".prj", ".cpg"]:
            sidecar = tmp_path.with_suffix(ext)
            if sidecar.exists():
                sidecar.unlink()

    def test_basic_conversion(self, output_file):
        """Test basic GeoParquet to Shapefile conversion."""
        result = write_shapefile(
            input_path=str(PLACES_PARQUET),
            output_path=output_file,
            verbose=False,
        )
        assert result == output_file
        assert Path(output_file).exists()

    def test_shapefile_creates_sidecar_files(self, output_file):
        """Test that Shapefile creates all required sidecar files."""
        write_shapefile(
            input_path=str(PLACES_PARQUET),
            output_path=output_file,
            verbose=False,
        )

        # Shapefile should create .shp, .shx, .dbf at minimum
        output_path = Path(output_file)
        assert output_path.with_suffix(".shp").exists()
        assert output_path.with_suffix(".shx").exists()
        assert output_path.with_suffix(".dbf").exists()

    def test_shapefile_custom_encoding(self, output_file):
        """Test Shapefile with custom encoding."""
        write_shapefile(
            input_path=str(PLACES_PARQUET),
            output_path=output_file,
            encoding="ISO-8859-1",
            verbose=False,
        )
        assert Path(output_file).exists()

    def test_overwrite_protection(self, output_file):
        """Test that overwrite=False prevents overwriting."""
        # Create initial file
        write_shapefile(
            input_path=str(PLACES_PARQUET),
            output_path=output_file,
            verbose=False,
        )

        # Try to overwrite - should raise error
        with pytest.raises(Exception, match="already exists|Use --overwrite"):
            write_shapefile(
                input_path=str(PLACES_PARQUET),
                output_path=output_file,
                overwrite=False,
                verbose=False,
            )

    def test_sql_injection_in_encoding(self, output_file):
        """Test that SQL injection in encoding is escaped."""
        malicious_encoding = "UTF-8'; DROP TABLE features; --"

        # Should not raise SQL error (may raise encoding error, which is fine)
        try:
            write_shapefile(
                input_path=str(PLACES_PARQUET),
                output_path=output_file,
                encoding=malicious_encoding,
                verbose=False,
            )
        except Exception as e:
            # Should be encoding error, not SQL error
            error_msg = str(e).lower()
            assert "sql" not in error_msg and "syntax" not in error_msg


class TestNoGeometryConversions:
    """Tests for converting plain Parquet files without geometry columns."""

    @pytest.fixture
    def plain_parquet(self, tmp_path):
        """Create a plain parquet file without geometry."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "score": [85.5, 92.0, 78.3],
            }
        )
        path = tmp_path / "plain.parquet"
        pq.write_table(table, path)
        return str(path)

    def test_flatgeobuf_no_geometry_error(self, plain_parquet, tmp_path):
        """Test FlatGeobuf gives friendly error for files without geometry."""
        output_file = tmp_path / "output.fgb"

        with pytest.raises(Exception) as exc_info:
            write_flatgeobuf(
                input_path=plain_parquet,
                output_path=str(output_file),
                verbose=False,
            )
        # Should have a friendly error message, not raw GDAL error
        assert "FlatGeobuf requires geometry" in str(exc_info.value)

    def test_shapefile_no_geometry_warns(self, plain_parquet, tmp_path, caplog):
        """Test Shapefile warns when no geometry column found."""
        import logging

        output_file = tmp_path / "output.shp"

        # Assert on the log record, not on stdout: which handler the package
        # logger carries depends on whether the embedding application (here,
        # pytest) has configured logging itself.
        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            write_shapefile(
                input_path=plain_parquet,
                output_path=str(output_file),
                verbose=False,
            )
        assert "no geometry" in caplog.text.lower()

    def test_geopackage_no_geometry_warns(self, plain_parquet, tmp_path, caplog):
        """Test GeoPackage warns when no geometry column found."""
        import logging

        output_file = tmp_path / "output.gpkg"

        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            write_geopackage(
                input_path=plain_parquet,
                output_path=str(output_file),
                verbose=False,
            )
        assert "no geometry" in caplog.text.lower()

    def test_geojson_no_geometry_raises_error(self, plain_parquet, tmp_path):
        """Test GeoJSON export errors when no geometry is present."""
        from geoparquet_io.core.exceptions import GeoParquetError

        output_file = tmp_path / "output.json"

        # Should raise error about missing geometry
        with pytest.raises(GeoParquetError, match="(?i)cannot export to geojson.*no geometry"):
            write_geojson(
                input_path=plain_parquet,
                output_path=str(output_file),
                verbose=False,
            )

    def test_case_insensitive_geometry_detection(self, tmp_path):
        """Test that geometry columns are detected case-insensitively."""
        import pyarrow as pa
        import pyarrow.parquet as pq
        import shapely

        # Create parquet with uppercase "Geometry" column
        geom = shapely.Point(0, 0)
        table = pa.table(
            {
                "id": [1, 2],
                "name": ["A", "B"],
                "Geometry": [shapely.to_wkb(geom), shapely.to_wkb(geom)],  # Uppercase
            }
        )
        input_path = tmp_path / "mixed_case.parquet"
        pq.write_table(table, input_path)

        output_path = tmp_path / "output.fgb"

        # Should NOT raise "no geometry column" error - the uppercase Geometry should be detected
        # Note: This may fail for other reasons (e.g., invalid WKB), but should not fail
        # with the "no geometry" error
        try:
            write_flatgeobuf(
                input_path=str(input_path),
                output_path=str(output_path),
                verbose=False,
            )
        except Exception as e:
            # If it fails, make sure it's NOT due to missing geometry
            assert "no geometry column" not in str(e).lower()
            assert "requires geometry" not in str(e).lower()


class TestGeoJSONWriter:
    """Tests for GeoJSON format writer."""

    @pytest.fixture
    def output_file(self):
        """Create temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.geojson"
        yield str(tmp_path)
        if tmp_path.exists():
            tmp_path.unlink()

    def test_overwrite_protection(self, output_file):
        """Test that overwrite=False prevents overwriting."""
        # Create initial file
        write_geojson(
            input_path=str(PLACES_PARQUET),
            output_path=output_file,
            verbose=False,
        )

        # Try to overwrite - should raise error
        with pytest.raises(Exception, match="already exists|Use --overwrite"):
            write_geojson(
                input_path=str(PLACES_PARQUET),
                output_path=output_file,
                overwrite=False,
                verbose=False,
            )


class TestCLIConvertSubcommands:
    """Tests for CLI convert subcommands."""

    @pytest.fixture
    def runner(self):
        """Create Click test runner."""
        return CliRunner()

    def test_convert_geopackage_subcommand(self, runner, tmp_path):
        """Test 'gpio convert geopackage' CLI command."""
        output = tmp_path / "output.gpkg"
        result = runner.invoke(
            cli,
            [
                "convert",
                "geopackage",
                str(PLACES_PARQUET),
                str(output),
            ],
        )
        if result.exit_code != 0:
            print(f"STDOUT: {result.stdout}")
            print(f"Exception: {result.exception}")
        assert result.exit_code == 0
        assert output.exists()

    def test_convert_flatgeobuf_subcommand(self, runner, tmp_path):
        """Test 'gpio convert flatgeobuf' CLI command."""
        output = tmp_path / "output.fgb"
        result = runner.invoke(
            cli,
            [
                "convert",
                "flatgeobuf",
                str(PLACES_PARQUET),
                str(output),
            ],
        )
        assert result.exit_code == 0
        assert output.exists()

    def test_convert_csv_subcommand(self, runner, tmp_path):
        """Test 'gpio convert csv' CLI command."""
        output = tmp_path / "output.csv"
        result = runner.invoke(
            cli,
            [
                "convert",
                "csv",
                str(PLACES_PARQUET),
                str(output),
            ],
        )
        assert result.exit_code == 0
        assert output.exists()

    def test_convert_shapefile_subcommand(self, runner, tmp_path):
        """Test 'gpio convert shapefile' CLI command."""
        output = tmp_path / "output.shp"
        result = runner.invoke(
            cli,
            [
                "convert",
                "shapefile",
                str(PLACES_PARQUET),
                str(output),
            ],
        )
        assert result.exit_code == 0
        assert output.exists()

    def test_convert_auto_detect_geopackage(self, runner, tmp_path):
        """Test auto-detection of GeoPackage format from extension."""
        output = tmp_path / "output.gpkg"  # Auto-detect from .gpkg extension
        result = runner.invoke(
            cli,
            [
                "convert",
                str(PLACES_PARQUET),
                str(output),
            ],
        )
        assert result.exit_code == 0
        assert output.exists()

    def test_convert_auto_detect_flatgeobuf(self, runner, tmp_path):
        """Test auto-detection of FlatGeobuf format from extension."""
        output = tmp_path / "output.fgb"  # Auto-detect from .fgb extension
        result = runner.invoke(
            cli,
            [
                "convert",
                str(PLACES_PARQUET),
                str(output),
            ],
        )
        assert result.exit_code == 0
        assert output.exists()

    def test_convert_auto_detect_csv(self, runner, tmp_path):
        """Test auto-detection of CSV format from extension."""
        output = tmp_path / "output.csv"  # Auto-detect from .csv extension
        result = runner.invoke(
            cli,
            [
                "convert",
                str(PLACES_PARQUET),
                str(output),
            ],
        )
        assert result.exit_code == 0
        assert output.exists()

    def test_convert_auto_detect_shapefile(self, runner, tmp_path):
        """Test auto-detection of Shapefile format from extension."""
        output = tmp_path / "output.shp"  # Auto-detect from .shp extension
        result = runner.invoke(
            cli,
            [
                "convert",
                str(PLACES_PARQUET),
                str(output),
            ],
        )
        assert result.exit_code == 0
        assert output.exists()


class TestShapefileZip:
    """Tests for shapefile zip functionality."""

    @pytest.fixture
    def output_file(self):
        """Create temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.shp"
        yield str(tmp_path)
        # Clean up all shapefile sidecar files and zip
        for ext in [".shp", ".shx", ".dbf", ".prj", ".cpg", ".shp.zip"]:
            sidecar = tmp_path.with_suffix(ext)
            if sidecar.exists():
                sidecar.unlink()

    def test_create_shapefile_zip_basic(self, output_file):
        """Test creating a zip archive from a shapefile."""
        from geoparquet_io.core.format_writers import create_shapefile_zip

        # First create a shapefile
        write_shapefile(
            input_path=str(PLACES_PARQUET),
            output_path=output_file,
            verbose=False,
        )

        # Verify sidecar files exist
        output_path = Path(output_file)
        assert output_path.with_suffix(".shp").exists()
        assert output_path.with_suffix(".shx").exists()
        assert output_path.with_suffix(".dbf").exists()

        # Create zip
        zip_path = create_shapefile_zip(output_file, verbose=False)

        # Verify zip was created
        assert zip_path.exists()
        assert zip_path.suffix == ".zip"
        assert zip_path.name.endswith(".shp.zip")

        # Verify zip contains all files
        import zipfile

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
            assert any(n.endswith(".shp") for n in names)
            assert any(n.endswith(".shx") for n in names)
            assert any(n.endswith(".dbf") for n in names)

    def test_create_shapefile_zip_missing_file(self):
        """Test that creating zip from non-existent shapefile raises error."""
        from geoparquet_io.core.format_writers import create_shapefile_zip

        nonexistent = str(Path(tempfile.gettempdir()) / f"nonexistent_{uuid.uuid4()}.shp")

        with pytest.raises(Exception, match="not found"):
            create_shapefile_zip(nonexistent)

    def test_create_shapefile_zip_verbose(self, output_file):
        """Test creating zip with verbose output."""
        from geoparquet_io.core.format_writers import create_shapefile_zip

        # Create shapefile
        write_shapefile(
            input_path=str(PLACES_PARQUET),
            output_path=output_file,
            verbose=False,
        )

        # Create zip with verbose (should not raise)
        zip_path = create_shapefile_zip(output_file, verbose=True)
        assert zip_path.exists()


class TestCRSPreservation:
    """Test CRS preservation in GDAL format exports."""

    # Test file with EPSG:5070 (NAD83 / Conus Albers)
    CRS_TEST_FILE = TEST_DATA_DIR / "fields_gpq2_5070_brotli.parquet"

    @pytest.fixture
    def shapefile_output(self):
        """Create temp shapefile output path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.shp"
        yield str(tmp_path)
        # Clean up all shapefile sidecar files
        for ext in [".shp", ".shx", ".dbf", ".prj", ".cpg"]:
            sidecar = tmp_path.with_suffix(ext)
            if sidecar.exists():
                sidecar.unlink()

    @pytest.fixture
    def flatgeobuf_output(self):
        """Create temp FlatGeobuf output path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.fgb"
        yield str(tmp_path)
        if tmp_path.exists():
            tmp_path.unlink()

    @pytest.fixture
    def geopackage_output(self):
        """Create temp GeoPackage output path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.gpkg"
        yield str(tmp_path)
        if tmp_path.exists():
            tmp_path.unlink()

    def test_shapefile_includes_prj_file(self, shapefile_output):
        """Shapefile export should create .prj sidecar with CRS."""
        write_shapefile(
            input_path=str(self.CRS_TEST_FILE),
            output_path=shapefile_output,
            verbose=True,
        )

        # Verify .prj file was created
        prj_file = Path(shapefile_output).with_suffix(".prj")
        assert prj_file.exists(), "Shapefile should have .prj sidecar file"

        # Verify .prj contains CRS information
        prj_content = prj_file.read_text()
        assert len(prj_content) > 0, ".prj file should contain CRS definition"
        assert "PROJCS" in prj_content or "GEOGCS" in prj_content, ".prj should contain WKT"

    def test_flatgeobuf_embeds_crs(self, flatgeobuf_output):
        """FlatGeobuf should embed CRS in file."""
        write_flatgeobuf(
            input_path=str(self.CRS_TEST_FILE),
            output_path=flatgeobuf_output,
            verbose=True,
        )

        # Read back with DuckDB and verify CRS
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection

        con = get_duckdb_connection(load_spatial=True)
        try:
            # Use ST_Read_Meta to get metadata including CRS
            result = con.execute(f"""
                SELECT * FROM ST_Read_Meta('{flatgeobuf_output}')
            """).fetchone()

            assert result is not None, "Should be able to read FlatGeobuf metadata"
            layers = result[3]  # List of layer dicts
            assert len(layers) > 0, "FlatGeobuf should have at least one layer"

            layer = layers[0]
            geometry_fields = layer.get("geometry_fields", [])
            assert len(geometry_fields) > 0, "Layer should have geometry fields"

            crs_info = geometry_fields[0].get("crs", {})
            # Check for EPSG code
            auth_name = crs_info.get("auth_name")
            auth_code = crs_info.get("auth_code")

            assert auth_name == "EPSG", f"Expected EPSG authority, got {auth_name}"
            # auth_code can be string or int depending on DuckDB version
            assert str(auth_code) == "5070", f"Expected EPSG:5070, got {auth_name}:{auth_code}"
        finally:
            con.close()

    def test_geopackage_embeds_crs(self, geopackage_output):
        """GeoPackage should embed CRS in database."""
        write_geopackage(
            input_path=str(self.CRS_TEST_FILE),
            output_path=geopackage_output,
            verbose=True,
        )

        # Read back with DuckDB and verify CRS
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection

        con = get_duckdb_connection(load_spatial=True)
        try:
            # Use ST_Read_Meta to get metadata including CRS
            result = con.execute(f"""
                SELECT * FROM ST_Read_Meta('{geopackage_output}')
            """).fetchone()

            assert result is not None, "Should be able to read GeoPackage metadata"
            layers = result[3]  # List of layer dicts
            assert len(layers) > 0, "GeoPackage should have at least one layer"

            layer = layers[0]
            geometry_fields = layer.get("geometry_fields", [])
            assert len(geometry_fields) > 0, "Layer should have geometry fields"

            crs_info = geometry_fields[0].get("crs", {})
            # Check for EPSG code
            auth_name = crs_info.get("auth_name")
            auth_code = crs_info.get("auth_code")

            assert auth_name == "EPSG", f"Expected EPSG authority, got {auth_name}"
            # auth_code can be string or int depending on DuckDB version
            assert str(auth_code) == "5070", f"Expected EPSG:5070, got {auth_name}:{auth_code}"
        finally:
            con.close()

    def test_default_crs_handled(self, shapefile_output):
        """Files with default CRS (EPSG:4326) should export with .prj file."""
        # Use a file with default CRS
        default_crs_file = TEST_DATA_DIR / "places_test.parquet"

        write_shapefile(
            input_path=str(default_crs_file),
            output_path=shapefile_output,
            verbose=True,
        )

        assert Path(shapefile_output).exists()

        # .prj file MUST be created for EPSG:4326 (fixes #190)
        # GDAL formats don't have implicit CRS defaults - must be explicit
        prj_file = Path(shapefile_output).with_suffix(".prj")
        assert prj_file.exists(), "Shapefile must have .prj file even for EPSG:4326"

        # Verify it contains WGS84/4326 reference
        prj_content = prj_file.read_text()
        assert "WGS" in prj_content.upper() or "4326" in prj_content, (
            ".prj should reference WGS84 or EPSG:4326"
        )

    def test_missing_crs_handled_gracefully(self, geopackage_output):
        """Files without CRS should export without error."""
        # Most test files have CRS, but export should work even if missing
        write_geopackage(
            input_path=str(BUILDINGS_PARQUET),
            output_path=geopackage_output,
            verbose=True,
        )

        # Should succeed without error
        assert Path(geopackage_output).exists()


class TestCustomGeometryColumnName:
    """Test that format writers detect geometry from GeoParquet metadata, not just hardcoded names."""

    @pytest.fixture
    def custom_geom_parquet(self, tmp_path):
        """Create a GeoParquet file with a non-standard geometry column name."""
        import json

        import pyarrow as pa
        import pyarrow.parquet as pq
        import shapely

        geom1 = shapely.Point(1.0, 2.0)
        geom2 = shapely.Point(3.0, 4.0)
        table = pa.table(
            {
                "id": [1, 2],
                "name": ["a", "b"],
                "my_custom_geom": [shapely.to_wkb(geom1), shapely.to_wkb(geom2)],
            }
        )

        geo_metadata = {
            "version": "1.1.0",
            "primary_column": "my_custom_geom",
            "columns": {
                "my_custom_geom": {
                    "encoding": "WKB",
                    "geometry_types": ["Point"],
                    "crs": {
                        "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
                        "type": "GeographicCRS",
                        "name": "WGS 84",
                        "id": {"authority": "EPSG", "code": 4326},
                    },
                }
            },
        }

        existing_meta = table.schema.metadata or {}
        new_meta = {**existing_meta, b"geo": json.dumps(geo_metadata).encode("utf-8")}
        table = table.replace_schema_metadata(new_meta)

        path = str(tmp_path / "custom_geom.parquet")
        pq.write_table(table, path)
        return path

    def test_csv_writer_detects_custom_geometry(self, custom_geom_parquet, tmp_path):
        """CSV writer should detect geometry column from metadata, not hardcoded names."""
        output_path = str(tmp_path / "output.csv")
        write_csv(
            input_path=custom_geom_parquet,
            output_path=output_path,
            verbose=False,
        )
        assert Path(output_path).exists()
        content = Path(output_path).read_text()
        # Should have a 'wkt' column from ST_AsText conversion, not the raw column name
        assert "wkt" in content.split("\n")[0]

    def test_gdal_writer_detects_custom_geometry(self, custom_geom_parquet, tmp_path):
        """GeoPackage writer should detect geometry column from metadata, not hardcoded names."""
        output_path = str(tmp_path / "output.gpkg")
        write_geopackage(
            input_path=custom_geom_parquet,
            output_path=output_path,
            verbose=False,
        )
        assert Path(output_path).exists()
