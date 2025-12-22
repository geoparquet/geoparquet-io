"""Tests for the reproject command and core functionality."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import reproject
from geoparquet_io.core.reproject import reproject_impl


@pytest.fixture
def duckdb_conn():
    """Create a DuckDB connection with spatial extension."""
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")
    yield conn
    conn.close()


@pytest.fixture
def create_geoparquet(tmp_path, duckdb_conn):
    """Factory fixture for creating test GeoParquet files with CRS metadata."""

    def _create(filename: str, wkt_geometry: str, crs_epsg: int = 4326) -> Path:
        file_path = tmp_path / filename

        # Use DuckDB COPY with GEOPARQUET_VERSION to create valid GeoParquet
        query = f"SELECT 1 AS id, ST_GeomFromText('{wkt_geometry}') AS geometry"
        duckdb_conn.execute(f"""
            COPY ({query}) TO '{file_path}'
            (FORMAT PARQUET, GEOPARQUET_VERSION 'V1')
        """)

        # Add CRS metadata using pyarrow
        table = pq.read_table(file_path)
        metadata = table.schema.metadata or {}

        # Parse existing geo metadata created by DuckDB
        if b"geo" in metadata:
            geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        else:
            geo_meta = {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {"geometry": {"encoding": "WKB"}},
            }

        # Add CRS to existing metadata
        if "columns" in geo_meta and "geometry" in geo_meta["columns"]:
            geo_meta["columns"]["geometry"]["crs"] = {"id": {"authority": "EPSG", "code": crs_epsg}}

        new_metadata = {**metadata, b"geo": json.dumps(geo_meta).encode()}
        new_schema = table.schema.with_metadata(new_metadata)
        new_table = table.cast(new_schema)
        pq.write_table(new_table, file_path)

        return file_path

    return _create


class TestReprojectCore:
    """Tests for the core reproject_impl function."""

    def test_reproject_utm_to_wgs84(self, create_geoparquet, tmp_path):
        """Test reprojection of a point from UTM Zone 10N to WGS84."""
        # San Francisco area in UTM Zone 10N (EPSG:32610)
        # These coordinates should transform to approximately (-122.4, 37.75)
        utm_x, utm_y = 551000, 4180000
        input_file = create_geoparquet(
            "input_utm.parquet",
            f"POINT({utm_x} {utm_y})",
            crs_epsg=32610,
        )

        output_file = tmp_path / "output_4326.parquet"

        result = reproject_impl(
            input_parquet=str(input_file),
            output_parquet=str(output_file),
            target_crs="EPSG:4326",
        )

        assert result.output_path == output_file
        assert result.feature_count == 1
        assert "32610" in result.source_crs
        assert result.target_crs == "EPSG:4326"

        # Read output and verify coordinates
        conn = duckdb.connect()
        conn.execute("INSTALL spatial; LOAD spatial;")
        coords = conn.execute(f"""
            SELECT ST_X(geometry) as x, ST_Y(geometry) as y
            FROM '{output_file}'
        """).fetchone()
        conn.close()

        x, y = coords
        # Expected: approximately (-122.4, 37.75) in lon/lat order
        assert -123 < x < -122, f"Longitude {x} not in expected range"
        assert 37 < y < 38, f"Latitude {y} not in expected range"


class TestAxisOrder:
    """Tests for verifying correct axis order (lon/lat) in output."""

    def test_output_uses_lonlat_order(self, create_geoparquet, tmp_path):
        """Verify output GeoParquet uses lon/lat (x/y) axis order.

        This is the critical test for axis order. GeoParquet spec requires
        lon/lat order. After reprojection from UTM:
        - X coordinate should be longitude (-180 to 180)
        - Y coordinate should be latitude (-90 to 90)

        If axes were swapped, X would be in latitude range.
        """
        # Use a location where lat/lon are very different magnitudes
        # Sydney, Australia in UTM Zone 56S (EPSG:32756)
        # Approximately (334000, 6252000) -> (151.2, -33.9) lon/lat
        input_file = create_geoparquet(
            "sydney_utm.parquet",
            "POINT(334000 6252000)",
            crs_epsg=32756,
        )

        output_file = tmp_path / "sydney_4326.parquet"
        reproject_impl(input_parquet=str(input_file), output_parquet=str(output_file))

        # Read output coordinates
        conn = duckdb.connect()
        conn.execute("INSTALL spatial; LOAD spatial;")
        coords = conn.execute(f"""
            SELECT ST_X(geometry) as x, ST_Y(geometry) as y
            FROM '{output_file}'
        """).fetchone()
        conn.close()

        x, y = coords

        # Key assertion: X (longitude) should be around 151, not -33
        # If axes were swapped, x would be around -33 (latitude)
        assert abs(x) > 90, (
            f"X coordinate {x} is in latitude range [-90, 90]. "
            "This suggests axis order is wrong (lat/lon instead of lon/lat)."
        )
        assert abs(y) < 90, f"Y coordinate {y} is not in valid latitude range"

        # More specific checks for Sydney
        assert 150 < x < 152, f"Longitude {x} not near Sydney (expected ~151)"
        assert -35 < y < -33, f"Latitude {y} not near Sydney (expected ~-34)"


class TestRoundtripReprojection:
    """Tests for roundtrip reprojection accuracy."""

    def test_roundtrip_preserves_coordinates(self, create_geoparquet, tmp_path):
        """Test that WGS84 -> UTM -> WGS84 preserves coordinates within tolerance."""
        # Start with known WGS84 coordinates
        original_lon, original_lat = -122.4194, 37.7749  # San Francisco

        # Create input in WGS84
        input_4326 = create_geoparquet(
            "sf_4326.parquet",
            f"POINT({original_lon} {original_lat})",
            crs_epsg=4326,
        )

        # Reproject to UTM Zone 10N
        utm_file = tmp_path / "sf_utm.parquet"
        reproject_impl(
            input_parquet=str(input_4326),
            output_parquet=str(utm_file),
            target_crs="EPSG:32610",
        )

        # Update the UTM file to have CRS metadata for the reverse transform
        table = pq.read_table(utm_file)
        metadata = table.schema.metadata or {}
        geo_meta = json.loads(metadata.get(b"geo", b"{}"))
        if "geometry" in geo_meta.get("columns", {}):
            geo_meta["columns"]["geometry"]["crs"] = {"id": {"authority": "EPSG", "code": 32610}}
        new_metadata = {**metadata, b"geo": json.dumps(geo_meta).encode()}
        new_schema = table.schema.with_metadata(new_metadata)
        new_table = table.cast(new_schema)
        pq.write_table(new_table, utm_file)

        # Reproject back to WGS84
        final_file = tmp_path / "sf_roundtrip.parquet"
        reproject_impl(
            input_parquet=str(utm_file),
            output_parquet=str(final_file),
            target_crs="EPSG:4326",
        )

        # Read final coordinates
        conn = duckdb.connect()
        conn.execute("INSTALL spatial; LOAD spatial;")
        coords = conn.execute(f"""
            SELECT ST_X(geometry) as x, ST_Y(geometry) as y
            FROM '{final_file}'
        """).fetchone()
        conn.close()

        final_lon, final_lat = coords

        # Should match within ~1e-6 degrees (sub-meter precision)
        tolerance = 1e-6
        assert abs(final_lon - original_lon) < tolerance, (
            f"Longitude changed: {original_lon} -> {final_lon}"
        )
        assert abs(final_lat - original_lat) < tolerance, (
            f"Latitude changed: {original_lat} -> {final_lat}"
        )


class TestPolygonReprojection:
    """Tests for reprojecting polygon geometries."""

    def test_reproject_polygon(self, create_geoparquet, tmp_path):
        """Test that polygons are correctly reprojected."""
        # Create a square polygon in UTM Zone 10N
        # 1km x 1km square near San Francisco
        input_file = create_geoparquet(
            "square_utm.parquet",
            "POLYGON((550000 4180000, 551000 4180000, 551000 4181000, 550000 4181000, 550000 4180000))",
            crs_epsg=32610,
        )

        output_file = tmp_path / "square_4326.parquet"
        result = reproject_impl(input_parquet=str(input_file), output_parquet=str(output_file))

        assert result.feature_count == 1

        # Read output and verify all vertices are in valid lon/lat range
        conn = duckdb.connect()
        conn.execute("INSTALL spatial; LOAD spatial;")

        # Get bounding box
        bbox = conn.execute(f"""
            SELECT
                ST_XMin(geometry) as xmin,
                ST_XMax(geometry) as xmax,
                ST_YMin(geometry) as ymin,
                ST_YMax(geometry) as ymax
            FROM '{output_file}'
        """).fetchone()
        conn.close()

        xmin, xmax, ymin, ymax = bbox

        # All coordinates should be in valid ranges
        assert -180 <= xmin <= 180, f"xmin {xmin} not in lon range"
        assert -180 <= xmax <= 180, f"xmax {xmax} not in lon range"
        assert -90 <= ymin <= 90, f"ymin {ymin} not in lat range"
        assert -90 <= ymax <= 90, f"ymax {ymax} not in lat range"

        # Should be near San Francisco
        assert -123 < xmin < -122, f"xmin {xmin} not near SF"
        assert 37 < ymin < 38, f"ymin {ymin} not near SF"


class TestOverwriteMode:
    """Tests for in-place overwrite functionality."""

    def test_reproject_overwrite(self, create_geoparquet):
        """Test reprojection with overwrite flag."""
        # Create input file
        input_file = create_geoparquet(
            "to_overwrite.parquet",
            "POINT(551000 4180000)",
            crs_epsg=32610,
        )

        original_path = Path(input_file)
        original_mtime = original_path.stat().st_mtime

        # Reproject in place
        result = reproject_impl(
            input_parquet=str(input_file),
            output_parquet=str(input_file),  # Same as input
            target_crs="EPSG:4326",
        )

        assert result.output_path == original_path
        assert result.feature_count == 1

        # File should have been modified
        new_mtime = original_path.stat().st_mtime
        assert new_mtime >= original_mtime

        # Verify content was reprojected
        conn = duckdb.connect()
        conn.execute("INSTALL spatial; LOAD spatial;")
        coords = conn.execute(f"""
            SELECT ST_X(geometry) as x, ST_Y(geometry) as y
            FROM '{input_file}'
        """).fetchone()
        conn.close()

        x, y = coords
        # Should now be in WGS84 range
        assert -180 <= x <= 180, f"x {x} not in lon range"
        assert -90 <= y <= 90, f"y {y} not in lat range"


class TestSameCRS:
    """Tests for when source and target CRS are the same."""

    def test_reproject_same_crs(self, create_geoparquet, tmp_path):
        """Test behavior when source and target CRS are the same."""
        original_lon, original_lat = -122.4, 37.8

        # Create file already in EPSG:4326
        input_file = create_geoparquet(
            "already_4326.parquet",
            f"POINT({original_lon} {original_lat})",
            crs_epsg=4326,
        )

        output_file = tmp_path / "still_4326.parquet"
        result = reproject_impl(
            input_parquet=str(input_file),
            output_parquet=str(output_file),
            target_crs="EPSG:4326",
        )

        assert result.feature_count == 1

        # Verify coordinates are unchanged
        conn = duckdb.connect()
        conn.execute("INSTALL spatial; LOAD spatial;")
        coords = conn.execute(f"""
            SELECT ST_X(geometry) as x, ST_Y(geometry) as y
            FROM '{output_file}'
        """).fetchone()
        conn.close()

        x, y = coords

        # Should be identical (or very close)
        assert abs(x - original_lon) < 1e-9, f"Longitude changed: {original_lon} -> {x}"
        assert abs(y - original_lat) < 1e-9, f"Latitude changed: {original_lat} -> {y}"


class TestAutoOutputFilename:
    """Tests for automatic output filename generation."""

    def test_auto_output_filename(self, create_geoparquet):
        """Test that output filename is generated correctly when not specified."""
        input_file = create_geoparquet(
            "input.parquet",
            "POINT(551000 4180000)",
            crs_epsg=32610,
        )

        result = reproject_impl(
            input_parquet=str(input_file),
            output_parquet=None,  # Auto-generate
            target_crs="EPSG:4326",
        )

        # Should create input_epsg_4326.parquet in same directory
        expected_name = "input_epsg_4326.parquet"
        assert result.output_path.name == expected_name
        assert result.output_path.exists()

        # Clean up
        if result.output_path.exists():
            result.output_path.unlink()


class TestReprojectCLI:
    """Tests for the CLI reproject command."""

    def test_cli_basic_reproject(self, create_geoparquet, tmp_path):
        """Test basic CLI invocation."""
        input_file = create_geoparquet(
            "cli_input.parquet",
            "POINT(551000 4180000)",
            crs_epsg=32610,
        )
        output_file = tmp_path / "cli_output.parquet"

        runner = CliRunner()
        result = runner.invoke(reproject, [str(input_file), str(output_file)])

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert output_file.exists()
        assert "Reprojected" in result.output

    def test_cli_with_target_crs(self, create_geoparquet, tmp_path):
        """Test CLI with custom target CRS."""
        input_file = create_geoparquet(
            "cli_input2.parquet",
            "POINT(-122.4 37.8)",
            crs_epsg=4326,
        )
        output_file = tmp_path / "cli_output2.parquet"

        runner = CliRunner()
        result = runner.invoke(
            reproject, [str(input_file), str(output_file), "--target-crs", "EPSG:32610"]
        )

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert output_file.exists()
        assert "EPSG:32610" in result.output


class TestMultipleFeatures:
    """Tests for files with multiple features."""

    def test_reproject_multiple_features(self, duckdb_conn, tmp_path):
        """Test reprojection of a file with multiple features."""
        input_file = tmp_path / "multi_features.parquet"

        # Create file with multiple points using GEOPARQUET_VERSION
        query = """
            SELECT
                1 AS id, ST_GeomFromText('POINT(551000 4180000)') AS geometry
            UNION ALL SELECT
                2 AS id, ST_GeomFromText('POINT(552000 4181000)') AS geometry
            UNION ALL SELECT
                3 AS id, ST_GeomFromText('POINT(553000 4182000)') AS geometry
        """
        duckdb_conn.execute(f"""
            COPY ({query}) TO '{input_file}'
            (FORMAT PARQUET, GEOPARQUET_VERSION 'V1')
        """)

        # Add CRS metadata
        table = pq.read_table(input_file)
        metadata = table.schema.metadata or {}

        # Parse existing geo metadata
        if b"geo" in metadata:
            geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        else:
            geo_meta = {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {"geometry": {"encoding": "WKB"}},
            }

        # Add CRS
        if "columns" in geo_meta and "geometry" in geo_meta["columns"]:
            geo_meta["columns"]["geometry"]["crs"] = {"id": {"authority": "EPSG", "code": 32610}}

        new_metadata = {**metadata, b"geo": json.dumps(geo_meta).encode()}
        new_schema = table.schema.with_metadata(new_metadata)
        new_table = table.cast(new_schema)
        pq.write_table(new_table, input_file)

        output_file = tmp_path / "multi_output.parquet"
        result = reproject_impl(input_parquet=str(input_file), output_parquet=str(output_file))

        assert result.feature_count == 3

        # Verify all features are in valid range
        conn = duckdb.connect()
        conn.execute("INSTALL spatial; LOAD spatial;")
        all_coords = conn.execute(f"""
            SELECT id, ST_X(geometry) as x, ST_Y(geometry) as y
            FROM '{output_file}'
            ORDER BY id
        """).fetchall()
        conn.close()

        assert len(all_coords) == 3
        for id_, x, y in all_coords:
            assert -180 <= x <= 180, f"Feature {id_}: x {x} not in lon range"
            assert -90 <= y <= 90, f"Feature {id_}: y {y} not in lat range"
            # All should be near SF
            assert -124 < x < -121, f"Feature {id_}: x {x} not near SF"
            assert 37 < y < 39, f"Feature {id_}: y {y} not near SF"


class TestWithExistingTestFiles:
    """Tests using existing test files in the test data directory."""

    def test_reproject_5070_to_4326(self, fields_5070_file, tmp_path):
        """Test reprojection from EPSG:5070 (NAD83 Conus Albers) to EPSG:4326."""
        output_file = tmp_path / "fields_4326.parquet"

        result = reproject_impl(
            input_parquet=fields_5070_file,
            output_parquet=str(output_file),
            target_crs="EPSG:4326",
        )

        assert result.output_path == output_file
        assert result.feature_count > 0
        assert "5070" in result.source_crs
        assert result.target_crs == "EPSG:4326"

        # Verify output coordinates are in WGS84 range
        conn = duckdb.connect()
        conn.execute("INSTALL spatial; LOAD spatial;")
        bbox = conn.execute(f"""
            SELECT
                MIN(ST_XMin(geometry)) as xmin,
                MAX(ST_XMax(geometry)) as xmax,
                MIN(ST_YMin(geometry)) as ymin,
                MAX(ST_YMax(geometry)) as ymax
            FROM '{output_file}'
        """).fetchone()
        conn.close()

        xmin, xmax, ymin, ymax = bbox
        # All coordinates should be in valid WGS84 ranges
        assert -180 <= xmin <= 180, f"xmin {xmin} not in lon range"
        assert -180 <= xmax <= 180, f"xmax {xmax} not in lon range"
        assert -90 <= ymin <= 90, f"ymin {ymin} not in lat range"
        assert -90 <= ymax <= 90, f"ymax {ymax} not in lat range"
