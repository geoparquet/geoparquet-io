"""
Pytest configuration and shared fixtures for geoparquet-io tests.
"""

import os
import shutil
import tempfile
from pathlib import Path

import pytest

# Test data directory
TEST_DATA_DIR = Path(__file__).parent / "data"
PLACES_TEST_FILE = TEST_DATA_DIR / "places_test.parquet"
BUILDINGS_TEST_FILE = TEST_DATA_DIR / "buildings_test.parquet"


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
