"""
Tests for the list_layers() function.

Tests verify that multi-layer formats (GeoPackage, FileGDB) have their layers
enumerated correctly, and that single-layer formats return None.
"""

import os
import sqlite3
import sys

import pytest
from click.testing import CliRunner

from geoparquet_io.core.duckdb_utils import _escape_sql_string
from geoparquet_io.core.layers import (
    _is_filegdb,
    _is_geopackage,
)
from geoparquet_io.core.layers import (
    list_layers as list_layers_core,
)


@pytest.fixture
def cli_runner():
    """Return a Click test runner."""
    return CliRunner()


@pytest.fixture
def multilayer_gpkg(test_data_dir):
    """Return path to multi-layer GeoPackage with buildings and roads layers."""
    return str(test_data_dir / "multilayer_test.gpkg")


@pytest.fixture
def single_layer_gpkg(test_data_dir):
    """Return path to single-layer GeoPackage (buildings_test.gpkg)."""
    return str(test_data_dir / "buildings_test.gpkg")


@pytest.fixture
def filegdb_path(test_data_dir):
    """Return path to the GDAL test FileGDB with multiple layers."""
    return str(test_data_dir / "testopenfilegdb.gdb")


@pytest.fixture
def geojson_input(test_data_dir):
    """Return path to the buildings_test.geojson test file."""
    return str(test_data_dir / "buildings_test.geojson")


@pytest.fixture
def shapefile_input(test_data_dir):
    """Return path to the buildings_test.shp test file."""
    return str(test_data_dir / "buildings_test.shp")


class TestHelperFunctions:
    """Tests for internal helper functions."""

    def test_is_geopackage_true(self):
        """Should recognize .gpkg extension."""
        assert _is_geopackage("data.gpkg") is True
        assert _is_geopackage("/path/to/file.GPKG") is True
        assert _is_geopackage("file.GpKg") is True

    def test_is_geopackage_false(self):
        """Should reject non-gpkg extensions."""
        assert _is_geopackage("data.gdb") is False
        assert _is_geopackage("data.geojson") is False
        assert _is_geopackage("data.parquet") is False

    def test_is_filegdb_true(self):
        """Should recognize .gdb extension."""
        assert _is_filegdb("data.gdb") is True
        assert _is_filegdb("/path/to/dir.GDB") is True

    def test_is_filegdb_false(self):
        """Should reject non-gdb extensions."""
        assert _is_filegdb("data.gpkg") is False
        assert _is_filegdb("data.sqlite") is False

    def test_escape_sql_string_no_quotes(self):
        """Strings without quotes should pass through unchanged."""
        assert _escape_sql_string("/path/to/file.gpkg") == "/path/to/file.gpkg"

    def test_escape_sql_string_with_single_quote(self):
        """Single quotes should be doubled for SQL escaping."""
        assert _escape_sql_string("O'Brien.gpkg") == "O''Brien.gpkg"
        assert _escape_sql_string("it's data.gpkg") == "it''s data.gpkg"

    def test_escape_sql_string_multiple_quotes(self):
        """Multiple quotes should all be escaped."""
        assert _escape_sql_string("a'b'c.gpkg") == "a''b''c.gpkg"


class TestListLayersGeoPackage:
    """Tests for list_layers() with GeoPackage files."""

    def test_multi_layer_geopackage_returns_list(self, multilayer_gpkg):
        """Multi-layer GeoPackage should return list of layer names."""
        layers = list_layers_core(multilayer_gpkg)

        assert layers is not None
        assert isinstance(layers, list)
        assert len(layers) >= 2
        # Should contain buildings and roads
        assert "buildings" in layers
        assert "roads" in layers

    def test_single_layer_geopackage_returns_none(self, single_layer_gpkg):
        """Single-layer GeoPackage should return None."""
        layers = list_layers_core(single_layer_gpkg)

        # Single-layer formats return None
        assert layers is None

    def test_layer_order_preserved(self, multilayer_gpkg):
        """Layers should be returned in consistent order."""
        layers1 = list_layers_core(multilayer_gpkg)
        layers2 = list_layers_core(multilayer_gpkg)

        assert layers1 == layers2

    def test_geopackage_with_spaces_in_path(self, tmp_path, multilayer_gpkg):
        """Paths with spaces should work correctly."""
        # Create a directory with spaces
        spaced_dir = tmp_path / "path with spaces"
        spaced_dir.mkdir()
        spaced_file = spaced_dir / "test file.gpkg"

        # Copy the test file
        import shutil

        shutil.copy(multilayer_gpkg, spaced_file)

        layers = list_layers_core(str(spaced_file))
        assert layers is not None
        assert len(layers) >= 2

    def test_geopackage_with_apostrophe_in_path(self, tmp_path, multilayer_gpkg):
        """Paths with apostrophes should work (SQL injection prevention)."""
        # Create a directory with apostrophe
        quoted_dir = tmp_path / "O'Brien's data"
        quoted_dir.mkdir()
        quoted_file = quoted_dir / "test.gpkg"

        import shutil

        shutil.copy(multilayer_gpkg, quoted_file)

        # Should not raise SQL injection or parsing errors
        layers = list_layers_core(str(quoted_file))
        assert layers is not None


@pytest.mark.slow
class TestListLayersFileGDB:
    """Tests for list_layers() with FileGDB files.

    Marked slow because FileGDB reading can be slower than GeoPackage.
    """

    def test_filegdb_returns_list(self, filegdb_path):
        """FileGDB with multiple layers should return list of layer names."""
        layers = list_layers_core(filegdb_path)

        assert layers is not None
        assert isinstance(layers, list)
        assert len(layers) > 1  # Should have multiple layers

    def test_filegdb_contains_expected_layers(self, filegdb_path):
        """FileGDB should contain known layers from GDAL test fixture."""
        layers = list_layers_core(filegdb_path)

        # GDAL test FileGDB should have these layers
        assert "point" in layers
        assert "polygon" in layers

    def test_filegdb_not_directory_raises(self, tmp_path):
        """Non-existent .gdb directory should raise FileNotFoundError."""
        fake_gdb = str(tmp_path / "nonexistent.gdb")

        with pytest.raises(FileNotFoundError):
            list_layers_core(fake_gdb)


class TestListLayersSingleLayerFormats:
    """Tests for list_layers() with single-layer formats."""

    def test_geojson_returns_none(self, geojson_input):
        """GeoJSON files (single-layer) should return None."""
        layers = list_layers_core(geojson_input)
        assert layers is None

    def test_shapefile_returns_none(self, shapefile_input):
        """Shapefiles (single-layer) should return None."""
        layers = list_layers_core(shapefile_input)
        assert layers is None

    def test_parquet_returns_none(self, places_test_file):
        """GeoParquet files (single-layer) should return None."""
        layers = list_layers_core(places_test_file)
        assert layers is None


class TestListLayersRemoteURLs:
    """Tests for remote URL handling."""

    def test_s3_url_raises_valueerror(self):
        """S3 URLs should raise ValueError with clear message."""
        with pytest.raises(ValueError, match="Remote URLs are not supported"):
            list_layers_core("s3://bucket/data.gpkg")

    def test_http_url_raises_valueerror(self):
        """HTTP URLs should raise ValueError with clear message."""
        with pytest.raises(ValueError, match="Remote URLs are not supported"):
            list_layers_core("https://example.com/data.gpkg")

    def test_gs_url_raises_valueerror(self):
        """GCS URLs should raise ValueError with clear message."""
        with pytest.raises(ValueError, match="Remote URLs are not supported"):
            list_layers_core("gs://bucket/data.gdb")


class TestListLayersPublicAPI:
    """Tests for the public API: gpio.list_layers()."""

    def test_api_exposed_at_module_level(self):
        """list_layers should be importable from geoparquet_io."""
        import geoparquet_io as gpio

        assert hasattr(gpio, "list_layers")
        assert callable(gpio.list_layers)

    def test_api_returns_same_as_core(self, multilayer_gpkg):
        """Public API should return same result as core function."""
        import geoparquet_io as gpio

        api_result = gpio.list_layers(multilayer_gpkg)
        core_result = list_layers_core(multilayer_gpkg)

        assert api_result == core_result


class TestListLayersCLI:
    """Tests for the CLI command: gpio inspect layers."""

    def test_cli_command_exists(self, cli_runner):
        """CLI command should be accessible."""
        from geoparquet_io.cli.main import cli

        result = cli_runner.invoke(cli, ["inspect", "layers", "--help"])
        assert result.exit_code == 0
        assert "List layers" in result.output

    def test_cli_multilayer_output(self, cli_runner, multilayer_gpkg):
        """CLI should output layer names for multi-layer files."""
        from geoparquet_io.cli.main import cli

        result = cli_runner.invoke(cli, ["inspect", "layers", multilayer_gpkg])
        assert result.exit_code == 0
        assert "Found" in result.output
        assert "buildings" in result.output
        assert "roads" in result.output

    def test_cli_json_output(self, cli_runner, multilayer_gpkg):
        """CLI --json flag should output valid JSON."""
        import json

        from geoparquet_io.cli.main import cli

        result = cli_runner.invoke(cli, ["inspect", "layers", multilayer_gpkg, "--json"])
        assert result.exit_code == 0

        data = json.loads(result.output)
        assert "layers" in data
        assert "count" in data
        assert data["count"] >= 2

    def test_cli_single_layer_output(self, cli_runner, single_layer_gpkg):
        """CLI should indicate when no layers found."""
        from geoparquet_io.cli.main import cli

        result = cli_runner.invoke(cli, ["inspect", "layers", single_layer_gpkg])
        assert result.exit_code == 0
        assert "No layers found" in result.output

    def test_cli_nonexistent_file(self, cli_runner, tmp_path):
        """CLI should show error for nonexistent files."""
        from geoparquet_io.cli.main import cli

        fake_path = str(tmp_path / "nonexistent.gpkg")
        result = cli_runner.invoke(cli, ["inspect", "layers", fake_path])
        assert result.exit_code != 0
        assert "not found" in result.output.lower()


class TestListLayersEdgeCases:
    """Tests for edge cases and error handling."""

    def test_nonexistent_file_raises(self, tmp_path):
        """Nonexistent file should raise FileNotFoundError."""
        fake_path = str(tmp_path / "nonexistent.gpkg")

        with pytest.raises(FileNotFoundError):
            list_layers_core(fake_path)

    def test_empty_path_raises(self):
        """Empty path should raise ValueError."""
        with pytest.raises(ValueError, match="cannot be empty"):
            list_layers_core("")

    def test_directory_without_gdb_extension_returns_none(self, tmp_path):
        """Regular directory (not .gdb) should return None (unrecognized format)."""
        # A regular directory that exists is treated as an unknown format
        # Unknown formats return None (same as single-layer formats)
        result = list_layers_core(str(tmp_path))
        assert result is None

    def test_pathlib_path_input(self, multilayer_gpkg):
        """Should accept pathlib.Path objects."""
        from pathlib import Path

        result = list_layers_core(Path(multilayer_gpkg))
        assert result is not None
        assert isinstance(result, list)

    def test_corrupt_geopackage_raises_valueerror(self, tmp_path):
        """Corrupt GeoPackage (valid SQLite, no gpkg_contents) should raise ValueError."""
        corrupt_gpkg = tmp_path / "corrupt.gpkg"

        # Create a valid SQLite file without gpkg_contents table
        con = sqlite3.connect(str(corrupt_gpkg))
        con.execute("CREATE TABLE dummy (id INTEGER)")
        con.close()

        with pytest.raises(ValueError, match="missing gpkg_contents"):
            list_layers_core(str(corrupt_gpkg))

    def test_empty_geopackage_returns_none(self, tmp_path):
        """GeoPackage with 0 layers should return None."""
        empty_gpkg = tmp_path / "empty.gpkg"

        # Create a valid GeoPackage structure with no feature layers
        con = sqlite3.connect(str(empty_gpkg))
        con.execute("""
            CREATE TABLE gpkg_contents (
                table_name TEXT,
                data_type TEXT,
                identifier TEXT,
                description TEXT,
                last_change TEXT,
                min_x REAL,
                min_y REAL,
                max_x REAL,
                max_y REAL,
                srs_id INTEGER
            )
        """)
        con.close()

        result = list_layers_core(str(empty_gpkg))
        assert result is None

    def test_unicode_path(self, tmp_path, multilayer_gpkg):
        """Paths with unicode characters should work."""
        import shutil

        unicode_dir = tmp_path / "unicode_\u65e5\u672c\u8a9e"
        unicode_dir.mkdir()
        unicode_file = unicode_dir / "test.gpkg"
        shutil.copy(multilayer_gpkg, unicode_file)

        result = list_layers_core(str(unicode_file))
        assert result is not None

    def test_relative_path_normalized(self, multilayer_gpkg):
        """Relative paths should be handled correctly."""
        # Exercise relative-path handling WITHOUT changing the process CWD.
        # xdist workers share one process CWD per worker, so a test that
        # changes it leaks into every test that runs after it on that worker
        # (see the no-cwd-change-in-tests hook in .pre-commit-config.yaml).
        relative_path = os.path.relpath(multilayer_gpkg)
        assert not os.path.isabs(relative_path)
        result = list_layers_core(relative_path)
        assert result is not None

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Symlinks require admin/Developer Mode on Windows",
    )
    def test_symlink_to_geopackage(self, tmp_path, multilayer_gpkg):
        """Symlinks to GeoPackage files should work."""
        symlink = tmp_path / "link.gpkg"
        symlink.symlink_to(multilayer_gpkg)

        result = list_layers_core(str(symlink))
        assert result is not None
        assert len(result) >= 2


class TestListLayersPermissions:
    """Tests for file permission handling."""

    @pytest.mark.skipif(os.name == "nt", reason="Unix-specific permissions")
    def test_unreadable_file_raises(self, tmp_path, multilayer_gpkg):
        """Unreadable file should raise appropriate error."""
        import shutil
        import stat

        unreadable = tmp_path / "unreadable.gpkg"
        shutil.copy(multilayer_gpkg, unreadable)

        # Remove read permissions
        unreadable.chmod(stat.S_IWUSR)

        try:
            with pytest.raises((FileNotFoundError, ValueError, PermissionError)):
                list_layers_core(str(unreadable))
        finally:
            # Restore permissions for cleanup
            unreadable.chmod(stat.S_IRUSR | stat.S_IWUSR)
