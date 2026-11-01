"""Tests for FileGDB CRS detection.

DuckDB's ST_Read_Meta returns empty for FileGDB directories (a known limitation).
These tests verify our workaround that iterates through internal .gdbtable files.

These tests require GDAL (ogr2ogr) to be installed.
"""

import os
import shutil
import subprocess
import tempfile

import pytest

from geoparquet_io.core.crs_utils import detect_crs_from_spatial_file
from geoparquet_io.core.duckdb_utils import fetch_one, get_duckdb_connection

# Timeout for ogr2ogr subprocess calls (seconds)
OGR2OGR_TIMEOUT = 60


@pytest.fixture
def filegdb_path():
    """Create a test FileGDB using ogr2ogr.

    We use ogr2ogr because DuckDB's COPY TO with OpenFileGDB driver
    has issues with required parameters.

    Requires GDAL (ogr2ogr) to be installed.
    """
    import uuid

    # Create unique paths to avoid conflicts
    unique_id = uuid.uuid4().hex[:8]
    gdb_path = os.path.join(tempfile.gettempdir(), f"test_filegdb_{unique_id}.gdb")
    geojson_path = os.path.join(tempfile.gettempdir(), f"test_filegdb_source_{unique_id}.geojson")

    # Clean up any existing files
    if os.path.exists(gdb_path):
        shutil.rmtree(gdb_path)

    # Create a GeoJSON source file
    geojson_content = """{
        "type": "FeatureCollection",
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}},
        "features": [
            {"type": "Feature", "geometry": {"type": "Point", "coordinates": [-75.1, 39.9]}, "properties": {"id": 1, "name": "Philadelphia"}},
            {"type": "Feature", "geometry": {"type": "Point", "coordinates": [-73.9, 40.7]}, "properties": {"id": 2, "name": "New York"}}
        ]
    }"""

    with open(geojson_path, "w") as f:
        f.write(geojson_content)

    # Convert to FileGDB using ogr2ogr
    try:
        result = subprocess.run(
            ["ogr2ogr", "-f", "OpenFileGDB", gdb_path, geojson_path, "-a_srs", "EPSG:4326"],
            capture_output=True,
            text=True,
            timeout=OGR2OGR_TIMEOUT,
        )
    except FileNotFoundError:
        if os.path.exists(geojson_path):
            os.remove(geojson_path)
        pytest.skip("ogr2ogr not found - GDAL must be installed to run FileGDB tests")
    except subprocess.TimeoutExpired:
        if os.path.exists(geojson_path):
            os.remove(geojson_path)
        pytest.skip(f"ogr2ogr timed out after {OGR2OGR_TIMEOUT}s")

    if result.returncode != 0:
        # Cleanup on failure
        if os.path.exists(geojson_path):
            os.remove(geojson_path)
        pytest.skip(f"ogr2ogr failed to create FileGDB: {result.stderr}")

    yield gdb_path

    # Cleanup
    if os.path.exists(gdb_path):
        shutil.rmtree(gdb_path)
    if os.path.exists(geojson_path):
        os.remove(geojson_path)


@pytest.mark.slow
class TestFileGDBCrsDetection:
    """Tests for FileGDB CRS detection workaround.

    These tests use ogr2ogr to create test fixtures, which involves
    subprocess calls to external tools.
    """

    def test_detect_crs_from_filegdb_returns_crs(self, filegdb_path):
        """Test that CRS is detected from a FileGDB directory."""
        con = get_duckdb_connection(load_spatial=True)
        try:
            crs = detect_crs_from_spatial_file(filegdb_path, con, verbose=False)

            assert crs is not None, "CRS should be detected from FileGDB"
            # Check it's EPSG:4326
            if "id" in crs:
                assert crs["id"]["authority"] == "EPSG"
                assert crs["id"]["code"] == 4326
            elif "name" in crs:
                assert "4326" in str(crs) or "WGS 84" in str(crs)
        finally:
            con.close()

    def test_detect_crs_from_filegdb_with_trailing_slash(self, filegdb_path):
        """Test that CRS detection works with trailing slash in path."""
        con = get_duckdb_connection(load_spatial=True)
        try:
            crs = detect_crs_from_spatial_file(filegdb_path + "/", con, verbose=False)

            assert crs["id"] == {"authority": "EPSG", "code": 4326}
        finally:
            con.close()

    def test_st_read_meta_returns_empty_for_filegdb_directory(self, filegdb_path):
        """Verify that ST_Read_Meta returns empty for FileGDB directories.

        This documents the underlying DuckDB limitation that our workaround fixes.
        """
        con = get_duckdb_connection(load_spatial=True)
        try:
            result = con.execute(f"SELECT * FROM ST_Read_Meta('{filegdb_path}')").fetchall()

            # This should be empty - that's the bug we're working around
            assert len(result) == 0, "ST_Read_Meta should return empty for FileGDB directories"
        finally:
            con.close()

    def test_st_read_works_for_filegdb_directory(self, filegdb_path):
        """Verify that ST_Read works for FileGDB directories.

        This confirms the data itself is readable, just not the metadata.
        """
        con = get_duckdb_connection(load_spatial=True)
        try:
            result = fetch_one(con.execute(f"SELECT COUNT(*) FROM ST_Read('{filegdb_path}')"))

            assert result[0] > 0, "ST_Read should be able to read FileGDB data"
        finally:
            con.close()

    def test_detect_crs_returns_none_for_nonexistent_path(self):
        """Test that non-existent paths return None gracefully."""
        con = get_duckdb_connection(load_spatial=True)
        try:
            crs = detect_crs_from_spatial_file("/nonexistent/path.gdb", con, verbose=False)

            assert crs is None
        finally:
            con.close()

    def test_detect_crs_handles_non_gdb_directory(self):
        """Test that non-.gdb directories don't trigger FileGDB workaround."""
        con = get_duckdb_connection(load_spatial=True)
        try:
            # Use a regular directory
            crs = detect_crs_from_spatial_file("/tmp", con, verbose=False)

            # Should return None (not crash)
            assert crs is None
        finally:
            con.close()


@pytest.fixture
def multi_layer_filegdb_path():
    """Create a test FileGDB with multiple layers having different CRS.

    This tests the edge case where tables in a FileGDB have different CRS.
    Our implementation returns the first CRS found from a user table.

    Requires GDAL (ogr2ogr) to be installed.
    """
    import uuid

    unique_id = uuid.uuid4().hex[:8]
    gdb_path = os.path.join(tempfile.gettempdir(), f"test_multi_crs_{unique_id}.gdb")

    # Clean up any existing files
    if os.path.exists(gdb_path):
        shutil.rmtree(gdb_path)

    # Create first layer in WGS84 (EPSG:4326)
    geojson_4326 = os.path.join(tempfile.gettempdir(), f"layer_4326_{unique_id}.geojson")
    with open(geojson_4326, "w") as f:
        f.write("""{
            "type": "FeatureCollection",
            "features": [
                {"type": "Feature", "geometry": {"type": "Point", "coordinates": [-75.1, 39.9]}, "properties": {"id": 1}}
            ]
        }""")

    # Create second layer in Web Mercator (EPSG:3857)
    geojson_3857 = os.path.join(tempfile.gettempdir(), f"layer_3857_{unique_id}.geojson")
    with open(geojson_3857, "w") as f:
        f.write("""{
            "type": "FeatureCollection",
            "features": [
                {"type": "Feature", "geometry": {"type": "Point", "coordinates": [-8361663, 4859241]}, "properties": {"id": 1}}
            ]
        }""")

    def cleanup_temp_files():
        for path in [geojson_4326, geojson_3857]:
            if os.path.exists(path):
                os.remove(path)

    # Create FileGDB with first layer
    try:
        result1 = subprocess.run(
            [
                "ogr2ogr",
                "-f",
                "OpenFileGDB",
                gdb_path,
                geojson_4326,
                "-a_srs",
                "EPSG:4326",
                "-nln",
                "layer_wgs84",
            ],
            capture_output=True,
            text=True,
            timeout=OGR2OGR_TIMEOUT,
        )
    except FileNotFoundError:
        cleanup_temp_files()
        pytest.skip("ogr2ogr not found - GDAL must be installed to run FileGDB tests")
    except subprocess.TimeoutExpired:
        cleanup_temp_files()
        pytest.skip(f"ogr2ogr timed out after {OGR2OGR_TIMEOUT}s")

    if result1.returncode != 0:
        cleanup_temp_files()
        pytest.skip(f"ogr2ogr failed to create FileGDB: {result1.stderr}")

    # Append second layer with different CRS
    try:
        result2 = subprocess.run(
            [
                "ogr2ogr",
                "-f",
                "OpenFileGDB",
                "-update",
                "-append",
                gdb_path,
                geojson_3857,
                "-a_srs",
                "EPSG:3857",
                "-nln",
                "layer_mercator",
            ],
            capture_output=True,
            text=True,
            timeout=OGR2OGR_TIMEOUT,
        )
    except FileNotFoundError:
        cleanup_temp_files()
        if os.path.exists(gdb_path):
            shutil.rmtree(gdb_path)
        pytest.skip("ogr2ogr not found - GDAL must be installed to run FileGDB tests")
    except subprocess.TimeoutExpired:
        cleanup_temp_files()
        if os.path.exists(gdb_path):
            shutil.rmtree(gdb_path)
        pytest.skip(f"ogr2ogr timed out after {OGR2OGR_TIMEOUT}s")

    if result2.returncode != 0:
        cleanup_temp_files()
        if os.path.exists(gdb_path):
            shutil.rmtree(gdb_path)
        pytest.skip(f"ogr2ogr failed to append layer: {result2.stderr}")

    yield gdb_path

    # Cleanup
    if os.path.exists(gdb_path):
        shutil.rmtree(gdb_path)
    cleanup_temp_files()


@pytest.mark.slow
class TestFileGDBMultiLayerCrs:
    """Tests for FileGDB with multiple layers having different CRS.

    These tests use ogr2ogr to create test fixtures, which involves
    subprocess calls to external tools.
    """

    def test_multi_layer_filegdb_returns_a_crs(self, multi_layer_filegdb_path):
        """Test that CRS detection returns a valid CRS from multi-layer FileGDB.

        Note: Our implementation returns the first CRS found when iterating
        through .gdbtable files. For FileGDBs with mixed CRS, users should
        specify --crs explicitly or use layer selection.
        """
        con = get_duckdb_connection(load_spatial=True)
        try:
            crs = detect_crs_from_spatial_file(multi_layer_filegdb_path, con, verbose=False)

            # Should return one of the CRS values (either 4326 or 3857)
            assert crs is not None, "Should detect CRS from multi-layer FileGDB"

            # Extract the EPSG code
            if "id" in crs:
                code = crs["id"]["code"]
                assert code in [
                    4326,
                    3857,
                ], f"Expected EPSG:4326 or EPSG:3857, got {code}"
            else:
                # PROJJSON format - check name
                crs_str = str(crs)
                assert (
                    "4326" in crs_str
                    or "3857" in crs_str
                    or "WGS 84" in crs_str
                    or "Mercator" in crs_str
                )
        finally:
            con.close()

    def test_multi_layer_filegdb_has_two_layers(self, multi_layer_filegdb_path):
        """Verify the test fixture actually created two layers."""
        import duckdb

        con = get_duckdb_connection(load_spatial=True)
        try:
            # Count .gdbtable files that aren't system tables
            gdbtable_count = 0
            for f in os.listdir(multi_layer_filegdb_path):
                if f.endswith(".gdbtable"):
                    path = os.path.join(multi_layer_filegdb_path, f)
                    try:
                        result = fetch_one(con.execute(f"SELECT * FROM ST_Read_Meta('{path}')"))
                        if result and result[3]:
                            for layer in result[3]:
                                if not layer.get("name", "").startswith("GDB_"):
                                    gdbtable_count += 1
                    except duckdb.Error:
                        # Skip files that can't be read by DuckDB
                        pass

            assert gdbtable_count >= 2, f"Expected at least 2 user layers, found {gdbtable_count}"
        finally:
            con.close()
