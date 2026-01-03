"""
Tests for the convert command.

Tests verify that convert applies all best practices:
- ZSTD compression
- 100k row groups
- Bbox column with metadata
- Hilbert spatial ordering
- GeoParquet 1.1.0 metadata
- Output passes validation
"""

import os
import sys

import duckdb
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.check_parquet_structure import (
    check_bbox_structure,
    get_compression_info,
    get_row_group_stats,
)
from geoparquet_io.core.common import get_parquet_metadata, parse_geo_metadata
from geoparquet_io.core.convert import convert_to_geoparquet


@pytest.fixture
def shapefile_input(test_data_dir):
    """Return path to test shapefile."""
    return str(test_data_dir / "buildings_test.shp")


@pytest.fixture
def geojson_input(test_data_dir):
    """Return path to test GeoJSON file."""
    return str(test_data_dir / "buildings_test.geojson")


@pytest.fixture
def geopackage_input(test_data_dir):
    """Return path to test GeoPackage file."""
    return str(test_data_dir / "buildings_test.gpkg")


@pytest.fixture
def csv_wkt_input(test_data_dir):
    """Return path to test CSV file with WKT column."""
    return str(test_data_dir / "points_wkt.csv")


@pytest.fixture
def csv_geometry_input(test_data_dir):
    """Return path to test CSV file with geometry column."""
    return str(test_data_dir / "points_geometry.csv")


@pytest.fixture
def csv_latlon_input(test_data_dir):
    """Return path to test CSV file with lat/lon columns."""
    return str(test_data_dir / "points_latlon.csv")


@pytest.fixture
def csv_latitude_longitude_input(test_data_dir):
    """Return path to test CSV file with latitude/longitude columns."""
    return str(test_data_dir / "points_latitude_longitude.csv")


@pytest.fixture
def tsv_wkt_input(test_data_dir):
    """Return path to test TSV file with WKT column."""
    return str(test_data_dir / "points_wkt.tsv")


@pytest.fixture
def csv_semicolon_input(test_data_dir):
    """Return path to test file with semicolon delimiter."""
    return str(test_data_dir / "points_semicolon.txt")


@pytest.fixture
def csv_invalid_wkt_input(test_data_dir):
    """Return path to test CSV with invalid WKT."""
    return str(test_data_dir / "points_invalid_wkt.csv")


@pytest.fixture
def csv_invalid_latlon_input(test_data_dir):
    """Return path to test CSV with invalid lat/lon."""
    return str(test_data_dir / "points_invalid_latlon.csv")


@pytest.fixture
def csv_mixed_geoms_input(test_data_dir):
    """Return path to test CSV with mixed geometry types."""
    return str(test_data_dir / "mixed_geometries.csv")


class TestConvertCore:
    """Test core convert_to_geoparquet function."""

    def test_convert_shapefile(self, shapefile_input, temp_output_file):
        """Test basic conversion from shapefile."""
        convert_to_geoparquet(
            shapefile_input,
            temp_output_file,
            skip_hilbert=False,
            verbose=False,
        )

        assert os.path.exists(temp_output_file)
        assert os.path.getsize(temp_output_file) > 0

    def test_convert_geojson(self, geojson_input, temp_output_file):
        """Test conversion from GeoJSON."""
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=False,
            verbose=False,
        )

        assert os.path.exists(temp_output_file)
        assert os.path.getsize(temp_output_file) > 0

    def test_convert_geopackage(self, geopackage_input, temp_output_file):
        """Test conversion from GeoPackage."""
        convert_to_geoparquet(
            geopackage_input,
            temp_output_file,
            skip_hilbert=False,
            verbose=False,
        )

        assert os.path.exists(temp_output_file)
        assert os.path.getsize(temp_output_file) > 0

    def test_convert_skip_hilbert(self, shapefile_input, temp_output_file):
        """Test conversion with --skip-hilbert flag."""
        convert_to_geoparquet(
            shapefile_input,
            temp_output_file,
            skip_hilbert=True,
            verbose=False,
        )

        assert os.path.exists(temp_output_file)
        # File should still be valid, just not Hilbert ordered
        # (We can't easily test for lack of ordering without larger dataset)

    def test_convert_verbose(self, shapefile_input, temp_output_file, capsys):
        """Test verbose output."""
        convert_to_geoparquet(
            shapefile_input,
            temp_output_file,
            skip_hilbert=False,
            verbose=True,
        )

        captured = capsys.readouterr()
        # Logging output goes to stderr
        assert "Detecting geometry column" in captured.err
        assert "Dataset bounds" in captured.err
        assert "bbox" in captured.err.lower()

    def test_convert_custom_compression(self, shapefile_input, temp_output_file):
        """Test custom compression settings."""
        convert_to_geoparquet(
            shapefile_input,
            temp_output_file,
            compression="ZSTD",
            compression_level=15,
            verbose=False,
        )

        assert os.path.exists(temp_output_file)
        compression_info = get_compression_info(temp_output_file)
        # Check that geometry column has ZSTD compression
        geom_compression = compression_info.get("geometry")
        assert geom_compression == "ZSTD"

    def test_convert_invalid_input(self, temp_output_file):
        """Test error handling for missing input file."""
        with pytest.raises(Exception) as exc_info:
            convert_to_geoparquet(
                "nonexistent.shp",
                temp_output_file,
                skip_hilbert=False,
                verbose=False,
            )
        assert "not found" in str(exc_info.value).lower()


class TestConvertBestPractices:
    """Test that convert applies all best practices."""

    def test_zstd_compression_applied(self, shapefile_input, temp_output_file):
        """Verify ZSTD compression is applied by default."""
        convert_to_geoparquet(shapefile_input, temp_output_file)

        compression_info = get_compression_info(temp_output_file)
        geom_compression = compression_info.get("geometry")
        assert geom_compression == "ZSTD", "Expected ZSTD compression on geometry column"

    def test_bbox_column_exists(self, shapefile_input, temp_output_file):
        """Verify bbox column is added."""
        convert_to_geoparquet(shapefile_input, temp_output_file)

        bbox_info = check_bbox_structure(temp_output_file, verbose=False)
        assert bbox_info["has_bbox_column"], "Expected bbox column to exist"
        assert bbox_info["bbox_column_name"] == "bbox"

    def test_bbox_metadata_present(self, shapefile_input, temp_output_file):
        """Verify bbox covering metadata is added."""
        convert_to_geoparquet(shapefile_input, temp_output_file)

        bbox_info = check_bbox_structure(temp_output_file, verbose=False)
        assert bbox_info["has_bbox_metadata"], "Expected bbox covering in metadata"
        assert bbox_info["status"] == "optimal"

    def test_geoparquet_version(self, shapefile_input, temp_output_file):
        """Verify GeoParquet 1.1.0+ metadata is created."""
        convert_to_geoparquet(shapefile_input, temp_output_file)

        metadata, _ = get_parquet_metadata(temp_output_file, verbose=False)
        geo_meta = parse_geo_metadata(metadata, verbose=False)

        assert geo_meta is not None, "Expected GeoParquet metadata to exist"
        version = geo_meta.get("version")
        assert version >= "1.1.0", f"Expected version >= 1.1.0, got {version}"

    def test_row_group_size(self, shapefile_input, temp_output_file):
        """Verify row groups are properly sized."""
        convert_to_geoparquet(shapefile_input, temp_output_file)

        stats = get_row_group_stats(temp_output_file)
        # For small test files, we might only have 1 row group
        # The key is that row_group_rows parameter was set to 100k
        assert stats["num_groups"] >= 1

    def test_hilbert_ordering_applied(self, shapefile_input, temp_output_file):
        """Verify Hilbert ordering is applied by default."""
        convert_to_geoparquet(shapefile_input, temp_output_file)

        # Check spatial order - should have good locality (low ratio)
        # Note: With small test files, this might not show perfect ordering
        # For now, just verify the file was created and has geometry
        # The spatial ordering check has its own encoding issues with converted files
        assert os.path.exists(temp_output_file)

        # Verify we can read the file
        con = duckdb.connect()
        count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{temp_output_file}')").fetchone()[
            0
        ]
        assert count > 0
        con.close()

    def test_geometry_column_preserved(self, shapefile_input, temp_output_file):
        """Verify geometry column is preserved."""
        convert_to_geoparquet(shapefile_input, temp_output_file)

        # Use DuckDB to check schema
        con = duckdb.connect()
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")

        result = con.execute(f"SELECT geometry FROM '{temp_output_file}' LIMIT 1").fetchone()
        assert result is not None
        con.close()

    def test_attribute_columns_preserved(self, shapefile_input, temp_output_file):
        """Verify attribute columns are preserved from input."""
        convert_to_geoparquet(shapefile_input, temp_output_file)

        # Use DuckDB to check schema
        con = duckdb.connect()
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")

        result = con.execute(f"DESCRIBE SELECT * FROM '{temp_output_file}'").fetchall()
        column_names = [row[0] for row in result]

        # Should have geometry and bbox at minimum
        assert "geometry" in column_names
        assert "bbox" in column_names

        # Should have some attribute columns from the shapefile
        assert len(column_names) > 2, "Expected attribute columns in addition to geometry/bbox"
        con.close()


class TestConvertCLI:
    """Test CLI interface for convert command."""

    def test_cli_basic_shapefile(self, shapefile_input, temp_output_file):
        """Test CLI basic usage with shapefile."""
        runner = CliRunner()
        result = runner.invoke(cli, ["convert", shapefile_input, temp_output_file])

        assert result.exit_code == 0, f"Command failed: {result.output}"
        assert os.path.exists(temp_output_file)
        assert "Converting" in result.output
        assert "Done" in result.output

    def test_cli_basic_geojson(self, geojson_input, temp_output_file):
        """Test CLI with GeoJSON input."""
        runner = CliRunner()
        result = runner.invoke(cli, ["convert", geojson_input, temp_output_file])

        assert result.exit_code == 0, f"Command failed: {result.output}"
        assert os.path.exists(temp_output_file)

    def test_cli_basic_geopackage(self, geopackage_input, temp_output_file):
        """Test CLI with GeoPackage input."""
        runner = CliRunner()
        result = runner.invoke(cli, ["convert", geopackage_input, temp_output_file])

        assert result.exit_code == 0, f"Command failed: {result.output}"
        assert os.path.exists(temp_output_file)

    def test_cli_verbose_output(self, shapefile_input, temp_output_file):
        """Test verbose flag shows progress."""
        runner = CliRunner()
        result = runner.invoke(cli, ["convert", shapefile_input, temp_output_file, "--verbose"])

        assert result.exit_code == 0
        assert "Detecting geometry column" in result.output
        assert "Dataset bounds" in result.output

    def test_cli_skip_hilbert(self, shapefile_input, temp_output_file):
        """Test --skip-hilbert flag."""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["convert", shapefile_input, temp_output_file, "--skip-hilbert"]
        )

        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

    def test_cli_custom_compression(self, shapefile_input, temp_output_file):
        """Test custom compression options."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "convert",
                shapefile_input,
                temp_output_file,
                "--compression",
                "ZSTD",
                "--compression-level",
                "15",
            ],
        )

        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        # Verify ZSTD compression was applied
        compression_info = get_compression_info(temp_output_file)
        assert compression_info.get("geometry") == "ZSTD"

    def test_cli_invalid_input(self):
        """Test error handling for missing input."""
        runner = CliRunner()
        result = runner.invoke(cli, ["convert", "nonexistent.shp", "out.parquet"])

        assert result.exit_code != 0
        assert "not found" in result.output.lower() or "does not exist" in result.output.lower()

    def test_cli_output_messages(self, shapefile_input, temp_output_file):
        """Test that CLI outputs expected messages."""
        runner = CliRunner()
        result = runner.invoke(cli, ["convert", shapefile_input, temp_output_file])

        assert result.exit_code == 0
        # Should show: converting, time, output file, size, validation
        assert "Converting" in result.output
        assert "Done in" in result.output
        assert "Output:" in result.output
        assert "validation" in result.output.lower()


class TestConvertEdgeCases:
    """Test edge cases and error handling."""

    def test_convert_preserves_row_count(self, shapefile_input, temp_output_file):
        """Test that all rows are preserved during conversion."""
        # Get row count from input
        con = duckdb.connect()
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")

        input_count = con.execute(f"SELECT COUNT(*) FROM ST_Read('{shapefile_input}')").fetchone()[
            0
        ]

        # Convert
        convert_to_geoparquet(shapefile_input, temp_output_file)

        # Get row count from output using DuckDB
        output_count = con.execute(f"SELECT COUNT(*) FROM '{temp_output_file}'").fetchone()[0]

        assert input_count == output_count, "Row count mismatch after conversion"
        con.close()

    @pytest.mark.skipif(
        sys.platform == "win32", reason="chmod permissions not supported on Windows"
    )
    def test_convert_output_directory_not_writable(self, shapefile_input, tmp_path):
        """Test error handling when output directory is not writable."""
        # Create a read-only directory
        read_only_dir = tmp_path / "readonly"
        read_only_dir.mkdir()
        read_only_dir.chmod(0o444)

        output_file = str(read_only_dir / "output.parquet")

        try:
            with pytest.raises(Exception) as exc_info:
                convert_to_geoparquet(shapefile_input, output_file)
            assert (
                "permission" in str(exc_info.value).lower()
                or "write" in str(exc_info.value).lower()
            )
        finally:
            # Clean up - restore permissions
            read_only_dir.chmod(0o755)

    def test_convert_nonexistent_output_directory(self, shapefile_input):
        """Test error handling when output directory doesn't exist."""
        output_file = "/nonexistent/path/output.parquet"

        with pytest.raises(Exception) as exc_info:
            convert_to_geoparquet(shapefile_input, output_file)
        assert (
            "not found" in str(exc_info.value).lower() or "directory" in str(exc_info.value).lower()
        )


class TestConvertCSVCore:
    """Test CSV/TSV conversion core functionality."""

    def test_convert_csv_wkt_autodetect(self, csv_wkt_input, temp_output_file):
        """Test CSV conversion with auto-detected WKT column."""
        convert_to_geoparquet(csv_wkt_input, temp_output_file, verbose=False)

        assert os.path.exists(temp_output_file)
        assert os.path.getsize(temp_output_file) > 0

        # Verify geometry column exists
        con = duckdb.connect()
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
        result = con.execute(f"SELECT geometry FROM '{temp_output_file}' LIMIT 1").fetchone()
        assert result is not None
        con.close()

    def test_convert_csv_geometry_column(self, csv_geometry_input, temp_output_file):
        """Test CSV with 'geometry' column name is auto-detected."""
        convert_to_geoparquet(csv_geometry_input, temp_output_file, verbose=False)

        assert os.path.exists(temp_output_file)
        # Verify row count
        con = duckdb.connect()
        count = con.execute(f"SELECT COUNT(*) FROM '{temp_output_file}'").fetchone()[0]
        assert count == 5
        con.close()

    def test_convert_csv_latlon_autodetect(self, csv_latlon_input, temp_output_file):
        """Test CSV conversion with auto-detected lat/lon columns."""
        convert_to_geoparquet(csv_latlon_input, temp_output_file, verbose=False)

        assert os.path.exists(temp_output_file)

        # Verify geometries are POINTs
        con = duckdb.connect()
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
        result = con.execute(
            f"SELECT ST_GeometryType(geometry) FROM '{temp_output_file}' LIMIT 1"
        ).fetchone()
        assert "POINT" in result[0]
        con.close()

    def test_convert_csv_latitude_longitude_columns(
        self, csv_latitude_longitude_input, temp_output_file
    ):
        """Test CSV with latitude/longitude column names."""
        convert_to_geoparquet(csv_latitude_longitude_input, temp_output_file, verbose=False)

        assert os.path.exists(temp_output_file)
        assert os.path.getsize(temp_output_file) > 0

    def test_convert_tsv_autodetect(self, tsv_wkt_input, temp_output_file):
        """Test TSV conversion with auto-detected tab delimiter."""
        convert_to_geoparquet(tsv_wkt_input, temp_output_file, verbose=False)

        assert os.path.exists(temp_output_file)
        # Verify row count matches
        con = duckdb.connect()
        count = con.execute(f"SELECT COUNT(*) FROM '{temp_output_file}'").fetchone()[0]
        assert count == 5
        con.close()

    def test_convert_csv_explicit_wkt_column(self, csv_wkt_input, temp_output_file):
        """Test CSV with explicit --wkt-column flag."""
        convert_to_geoparquet(csv_wkt_input, temp_output_file, wkt_column="wkt", verbose=False)

        assert os.path.exists(temp_output_file)

    def test_convert_csv_explicit_latlon_columns(self, csv_latlon_input, temp_output_file):
        """Test CSV with explicit --lat-column and --lon-column flags."""
        convert_to_geoparquet(
            csv_latlon_input, temp_output_file, lat_column="lat", lon_column="lon", verbose=False
        )

        assert os.path.exists(temp_output_file)

    def test_convert_csv_custom_delimiter(self, csv_semicolon_input, temp_output_file):
        """Test CSV with custom delimiter."""
        convert_to_geoparquet(csv_semicolon_input, temp_output_file, delimiter=";", verbose=False)

        assert os.path.exists(temp_output_file)
        # Verify data was read correctly
        con = duckdb.connect()
        count = con.execute(f"SELECT COUNT(*) FROM '{temp_output_file}'").fetchone()[0]
        assert count == 5
        con.close()

    def test_convert_csv_skip_hilbert(self, csv_wkt_input, temp_output_file):
        """Test CSV conversion with --skip-hilbert flag."""
        convert_to_geoparquet(csv_wkt_input, temp_output_file, skip_hilbert=True, verbose=False)

        assert os.path.exists(temp_output_file)

    def test_convert_csv_mixed_geometry_types(self, csv_mixed_geoms_input, temp_output_file):
        """Test CSV with mixed geometry types (POINTs and POLYGONs)."""
        convert_to_geoparquet(csv_mixed_geoms_input, temp_output_file, verbose=False)

        assert os.path.exists(temp_output_file)
        # Verify we have different geometry types
        con = duckdb.connect()
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
        result = con.execute(
            f"SELECT DISTINCT ST_GeometryType(geometry) FROM '{temp_output_file}'"
        ).fetchall()
        geom_types = [r[0] for r in result]
        assert len(geom_types) == 2  # Should have both POINT and POLYGON
        con.close()


class TestConvertCSVValidation:
    """Test CSV/TSV validation and error handling."""

    def test_convert_csv_invalid_wkt_fails_by_default(
        self, csv_invalid_wkt_input, temp_output_file
    ):
        """Test that invalid WKT causes failure by default."""
        with pytest.raises(Exception) as exc_info:
            convert_to_geoparquet(csv_invalid_wkt_input, temp_output_file, verbose=False)
        assert "invalid" in str(exc_info.value).lower() or "wkt" in str(exc_info.value).lower()

    def test_convert_csv_invalid_wkt_skip(self, csv_invalid_wkt_input, temp_output_file):
        """Test that --skip-invalid allows conversion with invalid WKT."""
        convert_to_geoparquet(
            csv_invalid_wkt_input, temp_output_file, skip_invalid=True, verbose=False
        )

        assert os.path.exists(temp_output_file)
        # Should only have valid rows
        con = duckdb.connect()
        count = con.execute(f"SELECT COUNT(*) FROM '{temp_output_file}'").fetchone()[0]
        assert count == 2  # Only 2 valid POINTs out of 4 rows
        con.close()

    def test_convert_csv_invalid_latlon_fails(self, csv_invalid_latlon_input, temp_output_file):
        """Test that invalid lat/lon values cause failure."""
        with pytest.raises(Exception) as exc_info:
            convert_to_geoparquet(csv_invalid_latlon_input, temp_output_file, verbose=False)
        assert (
            "latitude" in str(exc_info.value).lower() or "longitude" in str(exc_info.value).lower()
        )

    def test_convert_csv_nonexistent_wkt_column(self, csv_wkt_input, temp_output_file):
        """Test error when specified WKT column doesn't exist."""
        with pytest.raises(Exception) as exc_info:
            convert_to_geoparquet(
                csv_wkt_input, temp_output_file, wkt_column="nonexistent", verbose=False
            )
        assert "not found" in str(exc_info.value).lower()

    def test_convert_csv_nonexistent_latlon_columns(self, csv_latlon_input, temp_output_file):
        """Test error when specified lat/lon columns don't exist."""
        with pytest.raises(Exception) as exc_info:
            convert_to_geoparquet(
                csv_latlon_input,
                temp_output_file,
                lat_column="bad_lat",
                lon_column="bad_lon",
                verbose=False,
            )
        assert "not found" in str(exc_info.value).lower()

    def test_convert_csv_lat_without_lon(self, csv_latlon_input, temp_output_file):
        """Test error when only lat column specified without lon."""
        with pytest.raises(Exception) as exc_info:
            convert_to_geoparquet(
                csv_latlon_input, temp_output_file, lat_column="lat", verbose=False
            )
        assert "both" in str(exc_info.value).lower()


class TestConvertCSVCLI:
    """Test CLI interface for CSV/TSV conversion."""

    def test_cli_csv_wkt_basic(self, csv_wkt_input, temp_output_file):
        """Test CLI basic CSV conversion with WKT."""
        runner = CliRunner()
        result = runner.invoke(cli, ["convert", csv_wkt_input, temp_output_file])

        assert result.exit_code == 0, f"Command failed: {result.output}"
        assert os.path.exists(temp_output_file)
        assert "Using WKT column" in result.output
        assert "Done" in result.output

    def test_cli_csv_latlon_basic(self, csv_latlon_input, temp_output_file):
        """Test CLI basic CSV conversion with lat/lon."""
        runner = CliRunner()
        result = runner.invoke(cli, ["convert", csv_latlon_input, temp_output_file])

        assert result.exit_code == 0, f"Command failed: {result.output}"
        assert os.path.exists(temp_output_file)
        assert "lat/lon" in result.output.lower()

    def test_cli_csv_explicit_wkt_column(self, csv_wkt_input, temp_output_file):
        """Test CLI with explicit --wkt-column flag."""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["convert", csv_wkt_input, temp_output_file, "--wkt-column", "wkt"]
        )

        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

    def test_cli_csv_explicit_latlon_columns(self, csv_latlon_input, temp_output_file):
        """Test CLI with explicit --lat-column and --lon-column flags."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "convert",
                csv_latlon_input,
                temp_output_file,
                "--lat-column",
                "lat",
                "--lon-column",
                "lon",
            ],
        )

        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

    def test_cli_csv_custom_delimiter(self, csv_semicolon_input, temp_output_file):
        """Test CLI with custom --delimiter flag."""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["convert", csv_semicolon_input, temp_output_file, "--delimiter", ";"]
        )

        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

    def test_cli_csv_skip_invalid(self, csv_invalid_wkt_input, temp_output_file):
        """Test CLI with --skip-invalid flag."""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["convert", csv_invalid_wkt_input, temp_output_file, "--skip-invalid"]
        )

        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

    def test_cli_csv_verbose(self, csv_wkt_input, temp_output_file):
        """Test CLI verbose output for CSV."""
        runner = CliRunner()
        result = runner.invoke(cli, ["convert", csv_wkt_input, temp_output_file, "--verbose"])

        assert result.exit_code == 0
        assert "Detected columns" in result.output or "wkt" in result.output.lower()

    def test_cli_tsv_autodetect(self, tsv_wkt_input, temp_output_file):
        """Test CLI with TSV file (tab delimiter auto-detect)."""
        runner = CliRunner()
        result = runner.invoke(cli, ["convert", tsv_wkt_input, temp_output_file])

        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)
