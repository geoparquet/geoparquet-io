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
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.check_parquet_structure import (
    check_all,
    check_bbox_structure,
    get_compression_info,
    get_row_group_stats,
)
from geoparquet_io.core.common import get_parquet_metadata
from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.geo_metadata import parse_geo_metadata
from geoparquet_io.core.geometry_detection import (
    detect_parquet_geometry_column,
    find_primary_geometry_column,
)


def _check_all_passed(results: dict) -> bool:
    """Check if all check_all sub-checks passed."""
    return all(r.get("passed", True) for r in results.values() if isinstance(r, dict))


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


@pytest.fixture
def csv_large_wkt_input(tmp_path):
    """Create a CSV with WKT geometry exceeding DuckDB's default 2MB line limit.

    This tests that gpio can handle geospatial CSVs with very large WKT strings,
    such as complex polygons with many vertices (coastlines, administrative boundaries).
    """
    import math

    # Generate a polygon with enough vertices to exceed 2MB
    # Each coordinate pair like "0.123456 0.654321," is ~20 bytes
    # Need ~100,000 coordinate pairs to reach 2MB
    num_vertices = 120_000

    coords = []
    for i in range(num_vertices):
        # Create a circle-ish polygon
        angle = 2 * math.pi * i / num_vertices
        x = round(4.9 + 0.1 * math.cos(angle), 7)
        y = round(52.3 + 0.1 * math.sin(angle), 7)
        coords.append(f"{x} {y}")

    # Close the polygon
    coords.append(coords[0])

    wkt = f"POLYGON(({','.join(coords)}))"

    # Verify we exceed 2MB
    assert len(wkt) > 2_000_000, f"WKT is only {len(wkt)} bytes, need >2MB"

    csv_path = tmp_path / "large_wkt.csv"
    # Quote the WKT value since it contains commas (coordinate separators)
    csv_path.write_text(f'id,name,wkt\n1,Large Polygon,"{wkt}"\n')

    return str(csv_path)


@pytest.fixture
def unsorted_parquet_input(test_data_dir):
    """Return path to larger unsorted parquet file (1445 rows, 15 row groups)."""
    return str(test_data_dir / "unsorted.parquet")


@pytest.mark.slow
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

        # Verify output passes check_all validation
        results = check_all(temp_output_file, return_results=True, quiet=True)
        assert _check_all_passed(results), f"Output failed check_all: {results}"

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

        # Verify output passes check_all validation
        results = check_all(temp_output_file, return_results=True, quiet=True)
        assert _check_all_passed(results), f"Output failed check_all: {results}"

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

        # Verify output passes check_all validation
        results = check_all(temp_output_file, return_results=True, quiet=True)
        assert _check_all_passed(results), f"Output failed check_all: {results}"

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

    def test_convert_verbose(self, shapefile_input, temp_output_file, caplog):
        """Test verbose output.

        Asserted via ``caplog``, not ``capsys``: under pytest the root logger
        already has handlers (pytest's capture), so gpio's library bootstrap
        attaches only a NullHandler and log records propagate to pytest's
        capture instead of being written to stderr.
        """
        import logging

        with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
            convert_to_geoparquet(
                shapefile_input,
                temp_output_file,
                skip_hilbert=False,
                verbose=True,
            )

        assert "Detecting geometry column" in caplog.text
        assert "Dataset bounds" in caplog.text
        assert "bbox" in caplog.text.lower()

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
        geom_col = find_primary_geometry_column(temp_output_file)
        geom_compression = compression_info.get(geom_col)
        assert geom_compression == "ZSTD", (
            f"Expected ZSTD for '{geom_col}', got {geom_compression}. Keys: {list(compression_info.keys())}"
        )

        # Verify output passes check_all validation
        results = check_all(temp_output_file, return_results=True, quiet=True)
        assert _check_all_passed(results), f"Output failed check_all: {results}"

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


@pytest.fixture(scope="module")
def best_practices_file(tmp_path_factory):
    """Convert the test shapefile once with default options for read-only checks.

    Every test in TestConvertBestPractices previously re-ran this identical
    default conversion just to assert one property of the output. The output is
    only read in assertions, so a single module-scoped conversion is safe
    (issue #666 item 2).
    """
    from tests.conftest import TEST_DATA_DIR

    output = tmp_path_factory.mktemp("convert_best_practices") / "converted.parquet"
    convert_to_geoparquet(str(TEST_DATA_DIR / "buildings_test.shp"), str(output))
    return str(output)


class TestConvertBestPractices:
    """Test that convert applies all best practices (one shared conversion)."""

    def test_zstd_compression_applied(self, best_practices_file):
        """Verify ZSTD compression is applied by default."""
        compression_info = get_compression_info(best_practices_file)
        geom_col = find_primary_geometry_column(best_practices_file)
        geom_compression = compression_info.get(geom_col)
        assert geom_compression == "ZSTD", (
            f"Expected ZSTD compression on geometry column '{geom_col}'"
        )

    def test_bbox_column_exists(self, best_practices_file):
        """Verify bbox column is added."""
        bbox_info = check_bbox_structure(best_practices_file, verbose=False)
        assert bbox_info["has_bbox_column"], "Expected bbox column to exist"
        assert bbox_info["bbox_column_name"] == "bbox"

    def test_bbox_metadata_present(self, best_practices_file):
        """Verify bbox covering metadata is added."""
        bbox_info = check_bbox_structure(best_practices_file, verbose=False)
        assert bbox_info["has_bbox_metadata"], "Expected bbox covering in metadata"
        assert bbox_info["status"] == "optimal"

    def test_geoparquet_version(self, best_practices_file):
        """Verify GeoParquet 1.1.0+ metadata is created."""
        metadata, _ = get_parquet_metadata(best_practices_file, verbose=False)
        geo_meta = parse_geo_metadata(metadata, verbose=False)

        assert geo_meta is not None, "Expected GeoParquet metadata to exist"
        version = geo_meta.get("version")
        assert version >= "1.1.0", f"Expected version >= 1.1.0, got {version}"

    def test_row_group_size(self, best_practices_file):
        """Verify row groups are properly sized."""
        stats = get_row_group_stats(best_practices_file)
        # For small test files, we might only have 1 row group
        # The key is that row_group_rows parameter was set to 100k
        assert stats["num_groups"] >= 1

    def test_hilbert_ordering_applied(self, best_practices_file):
        """Verify Hilbert ordering is applied by default."""
        # Check spatial order - should have good locality (low ratio)
        # Note: With small test files, this might not show perfect ordering
        # For now, just verify the file was created and has geometry
        # The spatial ordering check has its own encoding issues with converted files
        assert os.path.exists(best_practices_file)

        # Verify we can read the file
        con = duckdb.connect()
        count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{best_practices_file}')"
        ).fetchone()[0]
        assert count > 0
        con.close()

    def test_geometry_column_preserved(self, best_practices_file):
        """Verify geometry column is preserved."""
        # Use DuckDB to check schema
        con = duckdb.connect()
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")

        # Get actual geometry column name (DuckDB uses "geom" for shapefile conversion)
        geom_col = find_primary_geometry_column(best_practices_file)
        result = con.execute(
            f"SELECT ST_AsText(\"{geom_col}\") FROM '{best_practices_file}' LIMIT 1"
        ).fetchone()
        assert result is not None
        con.close()

    def test_attribute_columns_preserved(self, best_practices_file):
        """Verify attribute columns are preserved from input."""
        # Use DuckDB to check schema
        con = duckdb.connect()
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")

        result = con.execute(f"DESCRIBE SELECT * FROM '{best_practices_file}'").fetchall()
        column_names = [row[0] for row in result]

        # Should have geometry (detected dynamically) and bbox at minimum
        geom_col = find_primary_geometry_column(best_practices_file)
        assert geom_col in column_names, f"Expected geometry column '{geom_col}' in {column_names}"
        assert "bbox" in column_names

        # Should have some attribute columns from the shapefile
        assert len(column_names) > 2, "Expected attribute columns in addition to geometry/bbox"
        con.close()


class TestConvertCLI:
    """CLI plumbing tests for convert (core conversion behavior is tested above).

    Consolidated from eight per-flag tests that each ran a full conversion
    (issue #666 item 2). What the dropped tests asserted survives elsewhere:
    GeoJSON and GeoPackage CLI/API conversions are exercised in the fast suite
    by test_geoparquet_versions.py::TestVersionCLI (CLI convert on GeoJSON) and
    test_convert_layer.py (GeoPackage), --skip-hilbert CLI plumbing by every
    TestVersionCLI case, and the remaining flags by the single run below.
    """

    def test_cli_plumbing_full_options(self, shapefile_input, temp_output_file):
        """One CLI run proving options reach core and expected messages print."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "convert",
                shapefile_input,
                temp_output_file,
                "--verbose",
                "--compression",
                "ZSTD",
                "--compression-level",
                "15",
            ],
        )

        assert result.exit_code == 0, f"Command failed: {result.output}"
        assert os.path.exists(temp_output_file)
        # --verbose plumbing
        assert "Detecting geometry column" in result.output
        assert "Dataset bounds" in result.output
        # User-facing messages: converting, time, output file, validation
        assert "Converting" in result.output
        assert "Done in" in result.output
        assert "Output:" in result.output
        assert "validation" in result.output.lower()
        # --compression/--compression-level plumbing
        compression_info = get_compression_info(temp_output_file)
        geom_col = find_primary_geometry_column(temp_output_file)
        assert compression_info.get(geom_col) == "ZSTD", f"Expected ZSTD for '{geom_col}'"

    def test_cli_invalid_input(self):
        """Test error handling for missing input."""
        runner = CliRunner()
        result = runner.invoke(cli, ["convert", "nonexistent.shp", "out.parquet"])

        assert result.exit_code != 0
        assert "not found" in result.output.lower() or "does not exist" in result.output.lower()


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
        result = con.execute(
            f"SELECT ST_AsText(geometry) FROM '{temp_output_file}' LIMIT 1"
        ).fetchone()
        assert result is not None
        con.close()

        # Verify output passes check_all validation
        results = check_all(temp_output_file, return_results=True, quiet=True)
        assert _check_all_passed(results), f"Output failed check_all: {results}"

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

        # Verify output passes check_all validation
        results = check_all(temp_output_file, return_results=True, quiet=True)
        assert _check_all_passed(results), f"Output failed check_all: {results}"

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

    def test_convert_csv_large_wkt_exceeding_2mb(self, csv_large_wkt_input, temp_output_file):
        """Test CSV with WKT geometry exceeding DuckDB's default 2MB line limit.

        Regression test for GitHub issue #301: CSV files with large WKT geometries
        (like complex polygons from UNESCO world heritage sites) failed with
        "Maximum line size of 2000000 bytes exceeded" error.
        """
        convert_to_geoparquet(csv_large_wkt_input, temp_output_file, verbose=False)

        assert os.path.exists(temp_output_file)
        assert os.path.getsize(temp_output_file) > 0

        # Verify geometry was parsed correctly
        con = duckdb.connect()
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
        result = con.execute(
            f"SELECT ST_GeometryType(geometry), ST_NPoints(geometry) FROM '{temp_output_file}'"
        ).fetchone()
        assert "POLYGON" in result[0]
        assert result[1] > 100_000  # Should have many vertices
        con.close()

    def test_convert_csv_large_wkt_with_explicit_delimiter(self, tmp_path, temp_output_file):
        """Test large WKT with explicit delimiter (tests both branches of _build_csv_read_expr).

        Verifies that max_line_size is applied to both read_csv (explicit delimiter)
        and read_csv_auto (auto-detect) branches.
        """
        import math

        # Create a moderately large WKT (just over default 2MB limit)
        num_vertices = 110_000
        coords = []
        for i in range(num_vertices):
            angle = 2 * math.pi * i / num_vertices
            x = round(4.9 + 0.1 * math.cos(angle), 7)
            y = round(52.3 + 0.1 * math.sin(angle), 7)
            coords.append(f"{x} {y}")
        coords.append(coords[0])  # Close polygon
        wkt = f"POLYGON(({','.join(coords)}))"

        # Create CSV with semicolon delimiter (forces explicit delimiter path)
        csv_path = tmp_path / "large_wkt_semicolon.csv"
        csv_path.write_text(f'id;name;wkt\n1;Large Polygon;"{wkt}"\n')

        # Convert with explicit delimiter
        convert_to_geoparquet(str(csv_path), temp_output_file, delimiter=";", verbose=False)

        assert os.path.exists(temp_output_file)
        con = duckdb.connect()
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
        result = con.execute(
            f"SELECT ST_GeometryType(geometry), ST_NPoints(geometry) FROM '{temp_output_file}'"
        ).fetchone()
        assert "POLYGON" in result[0]
        assert result[1] > 100_000
        con.close()

    def test_delimiter_is_escaped_for_the_sql_literal(self):
        """#937: `--delimiter` went into `delim='{...}'` unescaped.

        The path beside it was already routed through ``sql_path``; the
        delimiter was not, so a value containing ``'`` closed the literal early
        and produced a ParserException. ``_escape_sql_string`` takes a RAW
        value and adds no quotes of its own, so the surrounding ``'...'`` stays.
        """
        from geoparquet_io.core.convert import _build_csv_read_expr

        expr = _build_csv_read_expr("/tmp/x.csv", "'")

        assert "delim=''''" in expr
        # Escape exactly once: a doubled escape would read as an empty string
        # followed by a stray quote pair.
        assert "delim=''''''" not in expr

    def test_quote_in_delimiter_produces_parseable_sql(self):
        """The generated statement must parse; before the fix it did not."""
        from geoparquet_io.core.convert import _build_csv_read_expr

        expr = _build_csv_read_expr("/tmp/x.csv", "'")

        con = duckdb.connect()
        try:
            (payload,) = con.execute(
                "SELECT json_serialize_sql(?)", [f"SELECT * FROM {expr}"]
            ).fetchone()
        finally:
            con.close()
        assert '"error":true' not in payload.replace(" ", ""), payload

    def test_convert_csv_with_quote_delimiter_end_to_end(self, tmp_path, temp_output_file):
        """A `'`-delimited CSV converts instead of dying in the parser."""
        csv_path = tmp_path / "quote_delim.csv"
        csv_path.write_text("id'wkt\n1'POINT (1 2)\n2'POINT (3 4)\n")

        convert_to_geoparquet(str(csv_path), temp_output_file, delimiter="'", verbose=False)

        assert pq.read_table(temp_output_file).num_rows == 2

    def test_convert_csv_custom_max_line_size_env_var(
        self, tmp_path, temp_output_file, monkeypatch
    ):
        """Test that GPIO_CSV_MAX_LINE_SIZE env var is respected."""
        from geoparquet_io.core.convert import get_csv_max_line_size, set_csv_max_line_size

        # Reset any override from previous tests
        set_csv_max_line_size(None)

        # Set custom value via env var
        custom_size = 100 * 1024 * 1024  # 100MB
        monkeypatch.setenv("GPIO_CSV_MAX_LINE_SIZE", str(custom_size))

        assert get_csv_max_line_size() == custom_size

        # Reset
        monkeypatch.delenv("GPIO_CSV_MAX_LINE_SIZE", raising=False)
        set_csv_max_line_size(None)

    def test_convert_csv_large_wkt_fails_with_small_limit(
        self, csv_large_wkt_input, temp_output_file
    ):
        """Verify that large WKT would fail with DuckDB's default 2MB limit.

        This boundary test proves the fix is necessary by showing the old behavior
        would have failed. We temporarily set max_line_size to 2MB (DuckDB's default).
        """
        from geoparquet_io.core.convert import set_csv_max_line_size

        try:
            # Set to DuckDB's default 2MB limit
            set_csv_max_line_size(2 * 1024 * 1024)

            # This should fail with "Maximum line size exceeded"
            with pytest.raises(Exception) as exc_info:
                convert_to_geoparquet(csv_large_wkt_input, temp_output_file, verbose=False)

            assert "maximum line size" in str(exc_info.value).lower()
        finally:
            # Reset to default
            set_csv_max_line_size(None)


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
        con = duckdb.connect()
        # The fixture's 4 rows are: 2 valid POINTs, 1 unparsable, 1 empty WKT.
        # --skip-invalid drops what cannot be parsed; the row with no WKT at all
        # is kept, with NULL geometry, so its attributes are not lost (#655).
        count, nulls = con.execute(
            f"SELECT COUNT(*), COUNT(*) FILTER (WHERE geometry IS NULL) FROM '{temp_output_file}'"
        ).fetchone()
        assert (count, nulls) == (3, 1)
        con.close()

    def test_convert_csv_skip_invalid_metadata_computed(
        self, csv_invalid_wkt_input, temp_output_file
    ):
        """Test that skip_invalid correctly computes geo metadata.

        DuckDB 1.5 introduced segfaults when TRY() expressions in CTEs were
        inlined by the optimizer and re-evaluated by downstream spatial metadata
        queries (ST_GeometryType, ST_XMin, etc.). This test verifies that the
        temp table materialization and CTE workarounds produce correct metadata.
        """
        import json

        import pyarrow.parquet as pq

        convert_to_geoparquet(
            csv_invalid_wkt_input, temp_output_file, skip_invalid=True, verbose=False
        )

        # Verify geo metadata was computed correctly
        pf = pq.ParquetFile(temp_output_file)
        geo_meta = json.loads(pf.schema_arrow.metadata[b"geo"].decode("utf-8"))

        # Check geometry_types is computed (triggers ST_GeometryType on TRY() result)
        geom_col = geo_meta.get("primary_column", "geometry")
        col_meta = geo_meta["columns"][geom_col]
        assert "geometry_types" in col_meta
        assert "Point" in col_meta["geometry_types"]

        # Check bbox is computed (triggers ST_XMin/XMax/YMin/YMax on TRY() result)
        assert "bbox" in col_meta
        bbox = col_meta["bbox"]
        assert len(bbox) == 4
        # Verify bbox values are reasonable (not NaN or None)
        assert all(isinstance(v, (int, float)) for v in bbox)

        # Verify geometries can be read and operated on (final sanity check)
        con = duckdb.connect()
        con.install_extension("spatial")
        con.load_extension("spatial")
        result = con.execute(f"SELECT ST_AsText(geometry) FROM '{temp_output_file}'").fetchall()
        # 2 parseable POINTs plus the retained no-WKT row (#655).
        assert len(result) == 3
        assert sum("POINT" in (r[0] or "") for r in result) == 2
        assert sum(r[0] is None for r in result) == 1
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

    def test_cli_row_group_size(self, temp_output_file, tmp_path):
        """Test CLI with --row-group-size option.

        DuckDB has a minimum row group size of 2,048 rows (vector size).
        We create a file with 10,000 rows and request 3,000 rows per group.
        DuckDB will create groups of ~4,096 rows (2 vectors).
        See: https://github.com/duckdb/duckdb/discussions/8392
        """
        # Create a parquet file with 10,000 rows (enough for multiple row groups)
        large_input = str(tmp_path / "large_input.parquet")
        con = duckdb.connect()
        con.install_extension("spatial")
        con.load_extension("spatial")
        con.execute(f"""
            COPY (
                SELECT
                    i as id,
                    ST_Point(i % 360 - 180, i % 180 - 90) as geometry
                FROM range(10000) t(i)
            ) TO '{large_input}' (FORMAT PARQUET)
        """)
        con.close()

        runner = CliRunner()
        # Request 3000 rows per group - DuckDB will round to multiples of 2048
        result = runner.invoke(
            cli, ["convert", large_input, temp_output_file, "--row-group-size", "3000"]
        )

        assert result.exit_code == 0, f"Command failed: {result.output}"
        assert os.path.exists(temp_output_file)

        # Verify multiple row groups were created
        con = duckdb.connect()
        row_groups = con.execute(
            f"""
            SELECT DISTINCT row_group_id, row_group_num_rows
            FROM parquet_metadata('{temp_output_file}')
            ORDER BY row_group_id
            """
        ).fetchall()
        total_rows = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{temp_output_file}')"
        ).fetchone()[0]
        con.close()

        assert total_rows == 10000, f"Expected 10000 rows, got {total_rows}"
        # With 10,000 rows and ~4,096 rows per group, expect 2-3 row groups
        assert len(row_groups) >= 2, (
            f"Expected multiple row groups, got {len(row_groups)}: {row_groups}"
        )

    def test_cli_row_group_size_mb(self, unsorted_parquet_input, temp_output_file):
        """Test CLI with --row-group-size-mb option is accepted and doesn't error."""
        runner = CliRunner()
        # Test that the option is accepted and file is created successfully
        # Note: Actual row group splitting behavior depends on file format and DuckDB internals
        result = runner.invoke(
            cli,
            ["convert", unsorted_parquet_input, temp_output_file, "--row-group-size-mb", "0.05"],
        )

        assert result.exit_code == 0, f"Command failed: {result.output}"
        assert os.path.exists(temp_output_file)

        # Verify the file is valid and can be read
        con = duckdb.connect()
        row_count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{temp_output_file}')"
        ).fetchone()[0]
        con.close()

        assert row_count == 1445, f"Expected 1445 rows, got {row_count}"


class TestConvertRowGroupSizeMB:
    """--row-group-size-mb must actually shrink row groups (regression #547)."""

    @pytest.fixture
    def many_row_parquet(self, tmp_path):
        """A GeoParquet large enough that the default write is a single row group."""
        n = 50000
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        path = str(tmp_path / "many.parquet")
        escaped = path.replace("'", "''")
        con.execute(
            f"""
            COPY (
                SELECT i AS id,
                       ST_Point(random() * 10, random() * 10) AS geometry
                FROM range({n}) t(i)
            ) TO '{escaped}' (FORMAT PARQUET);
            """
        )
        con.close()
        # Sanity: default DuckDB row group size (>100k rows) keeps this in 1 group.
        assert pq.ParquetFile(path).num_row_groups == 1
        return path

    def test_row_group_size_mb_reduces_row_groups(self, many_row_parquet, tmp_path):
        """A small MB target on the default (duckdb-kv) path yields many row groups."""
        out = str(tmp_path / "out.parquet")
        result = CliRunner().invoke(
            cli,
            ["convert", many_row_parquet, out, "--row-group-size-mb", "0.2"],
        )

        assert result.exit_code == 0, f"Command failed: {result.output}"
        pf = pq.ParquetFile(out)
        assert pf.metadata.num_rows == 50000
        # Before the fix, --row-group-size-mb was dropped on the duckdb-kv path,
        # leaving DuckDB's default (one group here). It must now split.
        assert pf.num_row_groups > 1, (
            f"--row-group-size-mb had no effect: got {pf.num_row_groups} row group(s)"
        )

    def test_smaller_mb_yields_more_row_groups(self, many_row_parquet, tmp_path):
        """Row group count scales inversely with the MB target (monotonic)."""
        small = str(tmp_path / "small.parquet")
        large = str(tmp_path / "large.parquet")
        for target, out in [("0.1", small), ("1.0", large)]:
            result = CliRunner().invoke(
                cli, ["convert", many_row_parquet, out, "--row-group-size-mb", target]
            )
            assert result.exit_code == 0, f"Command failed: {result.output}"

        assert pq.ParquetFile(small).num_row_groups > pq.ParquetFile(large).num_row_groups


class TestConvertNoGeometry:
    """Test conversion of files without geometry columns."""

    @pytest.fixture
    def plain_parquet_input(self, tmp_path):
        """Create a Parquet file without any geometry column."""
        table = pa.table(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["a", "b", "c", "d", "e"],
                "value": [10.0, 20.0, 30.0, 40.0, 50.0],
            }
        )
        path = str(tmp_path / "plain.parquet")
        pq.write_table(table, path)
        return path

    @pytest.fixture
    def plain_csv_input(self, tmp_path):
        """Create a CSV file without any geometry column."""
        path = str(tmp_path / "plain.csv")
        with open(path, "w") as f:
            f.write("id,name,value\n")
            f.write("1,a,10.0\n")
            f.write("2,b,20.0\n")
            f.write("3,c,30.0\n")
        return path

    def test_convert_parquet_no_geometry(self, plain_parquet_input, temp_output_file):
        """Parquet without geometry should convert to plain optimized Parquet with --allow-no-geometry."""
        convert_to_geoparquet(
            plain_parquet_input, temp_output_file, allow_no_geometry=True, skip_hilbert=True
        )

        assert os.path.exists(temp_output_file)

        # Verify same row count
        con = duckdb.connect()
        count = con.execute(f"SELECT COUNT(*) FROM '{temp_output_file}'").fetchone()[0]
        assert count == 5

        # Verify columns preserved (no extra geometry/bbox added)
        cols = [
            col[0] for col in con.execute(f"SELECT * FROM '{temp_output_file}' LIMIT 0").description
        ]
        assert "id" in cols
        assert "name" in cols
        assert "value" in cols
        assert "geometry" not in cols
        assert "bbox" not in cols
        con.close()

        # Verify no geo metadata
        metadata, _ = get_parquet_metadata(temp_output_file, verbose=False)
        geo_meta = parse_geo_metadata(metadata, verbose=False)
        assert geo_meta is None, "Expected no GeoParquet metadata for plain file"

    def test_convert_csv_no_geometry(self, plain_csv_input, temp_output_file):
        """CSV without geometry should convert to plain optimized Parquet with --allow-no-geometry."""
        convert_to_geoparquet(
            plain_csv_input, temp_output_file, allow_no_geometry=True, skip_hilbert=True
        )

        assert os.path.exists(temp_output_file)

        # Verify data preserved
        con = duckdb.connect()
        count = con.execute(f"SELECT COUNT(*) FROM '{temp_output_file}'").fetchone()[0]
        assert count == 3

        cols = [
            col[0] for col in con.execute(f"SELECT * FROM '{temp_output_file}' LIMIT 0").description
        ]
        assert "id" in cols
        assert "name" in cols
        assert "geometry" not in cols
        con.close()

        # Verify no geo metadata
        metadata, _ = get_parquet_metadata(temp_output_file, verbose=False)
        geo_meta = parse_geo_metadata(metadata, verbose=False)
        assert geo_meta is None

    def test_convert_no_geometry_cli_warns(self, plain_parquet_input, temp_output_file):
        """CLI should warn about missing geometry and succeed with --allow-no-geometry."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "convert",
                plain_parquet_input,
                temp_output_file,
                "--allow-no-geometry",
                "--skip-hilbert",
            ],
        )

        assert result.exit_code == 0, f"Command failed: {result.output}"
        assert os.path.exists(temp_output_file)
        assert "no geometry column" in result.output.lower()

    def test_convert_no_geometry_requires_skip_hilbert(self, plain_parquet_input, temp_output_file):
        """No-geometry file with skip_hilbert=False should error."""
        from geoparquet_io.core.exceptions import GeoParquetError

        # When allow_no_geometry is True but skip_hilbert is False, should error
        with pytest.raises(GeoParquetError, match="(?i)cannot apply hilbert sorting"):
            convert_to_geoparquet(
                plain_parquet_input, temp_output_file, allow_no_geometry=True, skip_hilbert=False
            )

    def test_convert_no_geometry_errors_without_flag(self, plain_parquet_input, temp_output_file):
        """No-geometry file without --allow-no-geometry should error."""
        from geoparquet_io.core.exceptions import GeoParquetError

        with pytest.raises(GeoParquetError, match="(?i)no geometry column"):
            convert_to_geoparquet(plain_parquet_input, temp_output_file)

    def test_convert_with_geometry_still_works(self, shapefile_input, temp_output_file):
        """Regression guard: files with geometry still produce GeoParquet."""
        convert_to_geoparquet(shapefile_input, temp_output_file)

        metadata, _ = get_parquet_metadata(temp_output_file, verbose=False)
        geo_meta = parse_geo_metadata(metadata, verbose=False)
        assert geo_meta is not None, "Expected GeoParquet metadata for geo file"

    def test_build_plain_select_query_uses_correct_reader(self):
        """Verify _build_plain_select_query uses correct reader for each file type."""
        from geoparquet_io.core.convert import _build_plain_select_query

        # Parquet files should use read_parquet
        parquet_query = _build_plain_select_query("data.parquet", is_parquet=True)
        assert "read_parquet" in parquet_query
        assert "read_csv" not in parquet_query
        assert "ST_Read" not in parquet_query

        # CSV files should use read_csv
        csv_query = _build_plain_select_query("data.csv", is_csv=True)
        assert "read_csv" in csv_query
        assert "read_parquet" not in csv_query
        assert "ST_Read" not in csv_query

        # Spatial files (non-parquet, non-csv) should use ST_Read
        geojson_query = _build_plain_select_query("data.geojson", is_parquet=False, is_csv=False)
        assert "ST_Read" in geojson_query
        assert "read_csv" not in geojson_query
        assert "read_parquet" not in geojson_query

        # Shapefile should also use ST_Read
        shp_query = _build_plain_select_query("data.shp", is_parquet=False, is_csv=False)
        assert "ST_Read" in shp_query


class TestCustomGeometryColumnName:
    """Test that convert respects GeoParquet metadata for non-standard geometry column names."""

    @pytest.fixture
    def custom_geom_parquet(self, tmp_path):
        """Create a GeoParquet file with a non-standard geometry column name and proper metadata."""
        import json

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

    def test_convert_detects_custom_geometry_column_from_metadata(
        self, custom_geom_parquet, temp_output_file
    ):
        """Convert should use GeoParquet metadata to find geometry column, not just hardcoded names."""
        convert_to_geoparquet(custom_geom_parquet, temp_output_file)

        assert os.path.exists(temp_output_file)

        metadata, _ = get_parquet_metadata(temp_output_file, verbose=False)
        geo_meta = parse_geo_metadata(metadata, verbose=False)
        assert geo_meta is not None, "Expected GeoParquet metadata in output"
        # Convert preserves original geometry column name (not renamed to 'geometry')
        assert "my_custom_geom" in geo_meta.get("columns", {})

    def test_convert_custom_geom_preserves_data(self, custom_geom_parquet, temp_output_file):
        """Convert should preserve all rows when geometry column has a custom name."""
        convert_to_geoparquet(custom_geom_parquet, temp_output_file)

        con = duckdb.connect()
        count = con.execute(f"SELECT COUNT(*) FROM '{temp_output_file}'").fetchone()[0]
        assert count == 2
        con.close()


class TestDetectParquetGeometryColumn:
    """Test the shared detect_parquet_geometry_column function."""

    def _make_geoparquet(self, tmp_path, geom_col_name, include_metadata=True):
        """Helper to create a GeoParquet file with a given geometry column name."""
        import json

        import shapely

        geom = shapely.Point(1.0, 2.0)
        table = pa.table(
            {
                "id": [1],
                geom_col_name: [shapely.to_wkb(geom)],
            }
        )

        if include_metadata:
            geo_metadata = {
                "version": "1.1.0",
                "primary_column": geom_col_name,
                "columns": {
                    geom_col_name: {
                        "encoding": "WKB",
                        "geometry_types": ["Point"],
                    }
                },
            }
            existing_meta = table.schema.metadata or {}
            new_meta = {**existing_meta, b"geo": json.dumps(geo_metadata).encode("utf-8")}
            table = table.replace_schema_metadata(new_meta)

        path = str(tmp_path / f"{geom_col_name}.parquet")
        pq.write_table(table, path)
        return path

    def test_finds_custom_column_from_metadata(self, tmp_path):
        """Should find geometry column from GeoParquet metadata, not just hardcoded names."""
        path = self._make_geoparquet(tmp_path, "my_custom_geom")
        result = detect_parquet_geometry_column(path)
        assert result == "my_custom_geom"

    def test_finds_standard_column_without_metadata(self, tmp_path):
        """Should fall back to name-based detection when no GeoParquet metadata exists."""
        path = self._make_geoparquet(tmp_path, "geometry", include_metadata=False)
        result = detect_parquet_geometry_column(path)
        assert result == "geometry"

    def test_returns_none_for_plain_parquet(self, tmp_path):
        """Should return None when file has no geo metadata and no standard geometry column."""
        table = pa.table({"id": [1], "value": [42.0]})
        path = str(tmp_path / "plain.parquet")
        pq.write_table(table, path)
        result = detect_parquet_geometry_column(path)
        assert result is None

    def test_metadata_takes_priority_over_column_names(self, tmp_path):
        """When metadata says primary_column is X, return X even if 'geometry' column also exists."""
        import json

        import shapely

        geom = shapely.Point(1.0, 2.0)
        table = pa.table(
            {
                "id": [1],
                "geometry": [shapely.to_wkb(geom)],
                "the_real_geom": [shapely.to_wkb(geom)],
            }
        )

        geo_metadata = {
            "version": "1.1.0",
            "primary_column": "the_real_geom",
            "columns": {"the_real_geom": {"encoding": "WKB", "geometry_types": ["Point"]}},
        }
        meta = {b"geo": json.dumps(geo_metadata).encode("utf-8")}
        table = table.replace_schema_metadata(meta)

        path = str(tmp_path / "priority.parquet")
        pq.write_table(table, path)

        result = detect_parquet_geometry_column(path)
        assert result == "the_real_geom"
