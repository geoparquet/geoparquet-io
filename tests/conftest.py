"""
Pytest configuration and shared fixtures for geoparquet-io tests.
"""

import json
import os
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

# Test data directory
TEST_DATA_DIR = Path(__file__).parent / "data"
PLACES_TEST_FILE = TEST_DATA_DIR / "places_test.parquet"
BUILDINGS_TEST_FILE = TEST_DATA_DIR / "buildings_test.parquet"
COUNTRY_PARTITION_DIR = TEST_DATA_DIR / "country_partition"


@pytest.fixture
def test_data_dir():
    """Return the path to the test data directory."""
    return TEST_DATA_DIR


@pytest.fixture
def places_test_file():
    """Return the path to the places test parquet file."""
    return str(PLACES_TEST_FILE)


@pytest.fixture
def buildings_test_file():
    """Return the path to the buildings test parquet file."""
    return str(BUILDINGS_TEST_FILE)


@pytest.fixture
def temp_output_dir():
    """Create a temporary directory for test outputs."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    # Cleanup after test
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def temp_output_file(temp_output_dir):
    """Create a temporary output file path."""
    return os.path.join(temp_output_dir, "output.parquet")


@contextmanager
def duckdb_connection():
    """
    Context manager for DuckDB connections that ensures proper cleanup.

    Useful for tests to avoid Windows file locking issues.
    """
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
        yield con
    finally:
        con.close()


# Helper functions for GeoParquet version testing


def get_geoparquet_version(parquet_file):
    """
    Extract GeoParquet version from file metadata.

    Args:
        parquet_file: Path to the parquet file

    Returns:
        str: GeoParquet version string (e.g., "1.0.0", "1.1.0", "2.0.0") or None
    """
    pf = pq.ParquetFile(parquet_file)
    metadata = pf.schema_arrow.metadata
    if metadata and b"geo" in metadata:
        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        return geo_meta.get("version")
    return None


def has_native_geo_types(parquet_file):
    """
    Check if file uses Parquet GEOMETRY/GEOGRAPHY logical types.

    Args:
        parquet_file: Path to the parquet file

    Returns:
        bool: True if file has native Parquet geo types
    """
    pf = pq.ParquetFile(parquet_file)
    schema_str = str(pf.metadata.schema)
    return "Geometry" in schema_str or "Geography" in schema_str


def has_geoparquet_metadata(parquet_file):
    """
    Check if file has 'geo' metadata key (GeoParquet metadata).

    Args:
        parquet_file: Path to the parquet file

    Returns:
        bool: True if file has GeoParquet metadata
    """
    pf = pq.ParquetFile(parquet_file)
    metadata = pf.schema_arrow.metadata
    return metadata is not None and b"geo" in metadata


def get_geo_metadata(parquet_file):
    """
    Get the full GeoParquet metadata from a file.

    Args:
        parquet_file: Path to the parquet file

    Returns:
        dict: GeoParquet metadata or None
    """
    pf = pq.ParquetFile(parquet_file)
    metadata = pf.schema_arrow.metadata
    if metadata and b"geo" in metadata:
        return json.loads(metadata[b"geo"].decode("utf-8"))
    return None


# Test data file fixtures
@pytest.fixture
def fields_v2_file(test_data_dir):
    """Return path to the GeoParquet 2.0 test file (CRS84, ZSTD)."""
    return str(test_data_dir / "fields_gpq2_crs84_zstd.parquet")


@pytest.fixture
def fields_geom_type_only_file(test_data_dir):
    """Return path to the Parquet Geo Only test file (CRS84, with bbox, SNAPPY)."""
    return str(test_data_dir / "fields_pgo_crs84_bbox_snappy.parquet")


@pytest.fixture
def fields_geom_type_only_5070_file(test_data_dir):
    """Return path to the Parquet Geo Only test file (EPSG:5070, SNAPPY)."""
    return str(test_data_dir / "fields_pgo_5070_snappy.parquet")


@pytest.fixture
def austria_bbox_covering_file(test_data_dir):
    """Return path to the austria_bbox_covering.parquet test file.

    This file has a non-standard bbox column name ('geometry_bbox')
    that is properly registered in the GeoParquet covering metadata.
    """
    return str(test_data_dir / "austria_bbox_covering.parquet")


@pytest.fixture
def geojson_input(test_data_dir):
    """Return path to the buildings_test.geojson test file."""
    return str(test_data_dir / "buildings_test.geojson")


@pytest.fixture
def gpkg_buildings(test_data_dir):
    """Return path to the buildings_test.gpkg test file."""
    return str(test_data_dir / "buildings_test.gpkg")


@pytest.fixture
def buildings_gpkg_6933(test_data_dir):
    """Return path to the buildings_test_6933.gpkg test file (EPSG:6933)."""
    return str(test_data_dir / "buildings_test_6933.gpkg")


@pytest.fixture
def shapefile_buildings(test_data_dir):
    """Return path to the buildings_test.shp test file."""
    return str(test_data_dir / "buildings_test.shp")


@pytest.fixture
def csv_points_wkt(test_data_dir):
    """Return path to the points_wkt.csv test file."""
    return str(test_data_dir / "points_wkt.csv")


@pytest.fixture
def fields_5070_file(test_data_dir):
    """Return path to the Parquet Geo Only test file (EPSG:5070, SNAPPY)."""
    return str(test_data_dir / "fields_pgo_5070_snappy.parquet")


@pytest.fixture
def unsorted_test_file(test_data_dir):
    """Return path to the unsorted.parquet test file (poor spatial ordering)."""
    return str(test_data_dir / "unsorted.parquet")


@pytest.fixture
def country_partition_dir():
    """Return path to the country partition test directory.

    This directory contains 4 parquet files representing a flat partition:
    - El_Salvador.parquet
    - Guatemala.parquet
    - Honduras.parquet
    - Nicaragua.parquet

    All files have the same schema and GeoParquet 1.1.0 metadata.
    Total: ~5000 rows across 4 files.
    """
    return str(COUNTRY_PARTITION_DIR)
