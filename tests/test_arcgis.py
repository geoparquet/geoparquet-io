"""
Tests for ArcGIS Feature Service conversion.

Tests use mocked HTTP responses to avoid network dependencies.
Network tests are marked separately for optional integration testing.
"""

import json
import tempfile
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from tests.conftest import safe_unlink


# --- Mock Data Fixtures ---

MOCK_LAYER_INFO = {
    "name": "Test Layer",
    "geometryType": "esriGeometryPoint",
    "spatialReference": {"wkid": 4326, "latestWkid": 4326},
    "fields": [
        {"name": "OBJECTID", "type": "esriFieldTypeOID"},
        {"name": "name", "type": "esriFieldTypeString"},
    ],
    "maxRecordCount": 1000,
}

MOCK_FEATURE_COUNT = {"count": 3}

MOCK_FEATURES_PAGE = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
            "properties": {"OBJECTID": 1, "name": "Point 1"},
        },
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-122.5, 37.9]},
            "properties": {"OBJECTID": 2, "name": "Point 2"},
        },
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-122.6, 38.0]},
            "properties": {"OBJECTID": 3, "name": "Point 3"},
        },
    ],
}


class TestResolveToken:
    """Tests for token resolution."""

    def test_direct_token(self):
        """Test direct token is used as-is."""
        from geoparquet_io.core.arcgis import ArcGISAuth, resolve_token

        auth = ArcGISAuth(token="direct_token")
        result = resolve_token(auth, "https://example.com")
        assert result == "direct_token"

    def test_token_file(self, tmp_path):
        """Test token is read from file."""
        from geoparquet_io.core.arcgis import ArcGISAuth, resolve_token

        token_file = tmp_path / "token.txt"
        token_file.write_text("file_token\n")

        auth = ArcGISAuth(token_file=str(token_file))
        result = resolve_token(auth, "https://example.com")
        assert result == "file_token"

    @patch("geoparquet_io.core.arcgis.generate_token")
    def test_username_password(self, mock_generate):
        """Test token generation from username/password."""
        from geoparquet_io.core.arcgis import ArcGISAuth, resolve_token

        mock_generate.return_value = "generated_token"

        auth = ArcGISAuth(username="user", password="pass")
        result = resolve_token(auth, "https://example.com")

        assert result == "generated_token"
        mock_generate.assert_called_once()

    def test_priority_token_over_file(self, tmp_path):
        """Test direct token takes priority over token file."""
        from geoparquet_io.core.arcgis import ArcGISAuth, resolve_token

        token_file = tmp_path / "token.txt"
        token_file.write_text("file_token")

        auth = ArcGISAuth(token="direct_token", token_file=str(token_file))
        result = resolve_token(auth, "https://example.com")
        assert result == "direct_token"

    def test_no_auth(self):
        """Test None returned when no auth provided."""
        from geoparquet_io.core.arcgis import ArcGISAuth, resolve_token

        auth = ArcGISAuth()
        result = resolve_token(auth, "https://example.com")
        assert result is None


class TestValidateArcgisUrl:
    """Tests for URL validation."""

    def test_valid_feature_server_url(self):
        """Test valid FeatureServer URL."""
        from geoparquet_io.core.arcgis import validate_arcgis_url

        url, layer_id = validate_arcgis_url(
            "https://services.arcgis.com/org/arcgis/rest/services/Test/FeatureServer/0"
        )
        assert "/FeatureServer/0" in url
        assert layer_id == 0

    def test_valid_map_server_url(self):
        """Test valid MapServer URL."""
        from geoparquet_io.core.arcgis import validate_arcgis_url

        url, layer_id = validate_arcgis_url(
            "https://example.com/arcgis/rest/services/Test/MapServer/5"
        )
        assert "/MapServer/5" in url
        assert layer_id == 5

    def test_url_with_trailing_slash(self):
        """Test URL with trailing slash is handled."""
        from geoparquet_io.core.arcgis import validate_arcgis_url

        url, layer_id = validate_arcgis_url(
            "https://services.arcgis.com/org/rest/services/Test/FeatureServer/0/"
        )
        assert layer_id == 0

    def test_invalid_url_no_server_type(self):
        """Test invalid URL without FeatureServer/MapServer."""
        import click

        from geoparquet_io.core.arcgis import validate_arcgis_url

        with pytest.raises(click.ClickException, match="Invalid ArcGIS URL"):
            validate_arcgis_url("https://example.com/rest/services/Test/0")

    def test_invalid_url_no_layer_id(self):
        """Test invalid URL without layer ID."""
        import click

        from geoparquet_io.core.arcgis import validate_arcgis_url

        with pytest.raises(click.ClickException, match="Missing layer ID in URL"):
            validate_arcgis_url("https://example.com/rest/services/Test/FeatureServer")


class TestGenerateToken:
    """Tests for token generation."""

    @patch("geoparquet_io.core.arcgis._make_request")
    def test_successful_generation(self, mock_request):
        """Test successful token generation."""
        from geoparquet_io.core.arcgis import generate_token

        mock_request.return_value = {"token": "new_token", "expires": 3600}

        result = generate_token("user", "pass")

        assert result == "new_token"
        mock_request.assert_called_once()

    @patch("geoparquet_io.core.arcgis._make_request")
    def test_invalid_credentials(self, mock_request):
        """Test error on invalid credentials."""
        import click

        from geoparquet_io.core.arcgis import generate_token

        mock_request.return_value = {
            "error": {"code": 400, "message": "Invalid credentials", "details": []}
        }

        with pytest.raises(click.ClickException, match="Invalid credentials"):
            generate_token("user", "wrong_pass")


class TestGetLayerInfo:
    """Tests for layer info retrieval."""

    @patch("geoparquet_io.core.arcgis._make_request")
    @patch("geoparquet_io.core.arcgis.get_feature_count")
    def test_successful_info(self, mock_count, mock_request):
        """Test successful layer info retrieval."""
        from geoparquet_io.core.arcgis import get_layer_info

        mock_request.return_value = MOCK_LAYER_INFO
        mock_count.return_value = 100

        result = get_layer_info("https://example.com/FeatureServer/0")

        assert result.name == "Test Layer"
        assert result.geometry_type == "esriGeometryPoint"
        assert result.max_record_count == 1000
        assert result.total_count == 100


class TestFetchFeaturesPage:
    """Tests for feature fetching."""

    @patch("geoparquet_io.core.arcgis._make_request")
    def test_fetch_page(self, mock_request):
        """Test fetching a single page of features."""
        from geoparquet_io.core.arcgis import fetch_features_page

        mock_request.return_value = MOCK_FEATURES_PAGE

        result = fetch_features_page(
            "https://example.com/FeatureServer/0",
            offset=0,
            limit=1000,
        )

        assert result["type"] == "FeatureCollection"
        assert len(result["features"]) == 3


class TestCrsExtraction:
    """Tests for CRS handling."""

    def test_wkid_to_epsg(self):
        """Test WKID conversion to EPSG."""
        from geoparquet_io.core.arcgis import _extract_crs_from_spatial_reference

        # Standard EPSG
        result = _extract_crs_from_spatial_reference({"wkid": 4326})
        assert result is not None

        # Web Mercator special case
        result = _extract_crs_from_spatial_reference({"wkid": 102100})
        assert result is not None

    def test_default_crs(self):
        """Test default CRS when no spatial reference."""
        from geoparquet_io.core.arcgis import _extract_crs_from_spatial_reference

        result = _extract_crs_from_spatial_reference({})
        assert result is not None  # Should default to WGS84


class TestCLI:
    """CLI integration tests."""

    @pytest.fixture
    def output_file(self):
        """Create temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_arcgis_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    @patch("geoparquet_io.core.arcgis.convert_arcgis_to_geoparquet")
    def test_basic_command(self, mock_convert, output_file):
        """Test basic CLI command."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "convert",
                "arcgis",
                "https://example.com/FeatureServer/0",
                output_file,
            ],
        )

        assert result.exit_code == 0
        mock_convert.assert_called_once()

    def test_missing_output(self):
        """Test error when output file missing."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "convert",
                "arcgis",
                "https://example.com/FeatureServer/0",
            ],
        )

        assert result.exit_code != 0

    def test_username_without_password(self, output_file):
        """Test error when username provided without password."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "convert",
                "arcgis",
                "https://example.com/FeatureServer/0",
                output_file,
                "--username",
                "user",
            ],
        )

        assert result.exit_code != 0
        assert "password" in result.output.lower() or "password" in str(result.exception).lower()


class TestPythonAPI:
    """Tests for Python API functions."""

    @patch("geoparquet_io.core.arcgis.arcgis_to_table")
    def test_convert_arcgis_function(self, mock_arcgis_to_table):
        """Test convert_arcgis API function."""
        from geoparquet_io.api.table import convert_arcgis

        # Create mock table
        mock_table = pa.table({"geometry": [b"test"], "name": ["Point 1"]})
        mock_arcgis_to_table.return_value = mock_table

        result = convert_arcgis("https://example.com/FeatureServer/0")

        assert result.num_rows == 1
        mock_arcgis_to_table.assert_called_once()

    @patch("geoparquet_io.core.arcgis.arcgis_to_table")
    def test_ops_from_arcgis_function(self, mock_arcgis_to_table):
        """Test ops.from_arcgis function."""
        from geoparquet_io.api import ops

        # Create mock table
        mock_table = pa.table({"geometry": [b"test"], "name": ["Point 1"]})
        mock_arcgis_to_table.return_value = mock_table

        result = ops.from_arcgis("https://example.com/FeatureServer/0")

        assert isinstance(result, pa.Table)
        assert result.num_rows == 1


@pytest.mark.network
class TestNetworkIntegration:
    """Network integration tests (require actual ArcGIS service)."""

    # Small public service for testing
    SMALL_SERVICE = "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/ArcGIS/rest/services/Current_Ice_Jams/FeatureServer/0"

    def test_fetch_layer_info(self):
        """Test fetching real layer info."""
        from geoparquet_io.core.arcgis import get_layer_info

        info = get_layer_info(self.SMALL_SERVICE)
        assert info.name is not None
        assert info.total_count >= 0

    def test_fetch_feature_count(self):
        """Test fetching real feature count."""
        from geoparquet_io.core.arcgis import get_feature_count

        count = get_feature_count(self.SMALL_SERVICE)
        assert isinstance(count, int)
        assert count >= 0

    @pytest.fixture
    def output_file(self):
        """Create temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_arcgis_network_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_full_conversion(self, output_file):
        """Test full conversion of small public service."""
        from geoparquet_io.core.arcgis import convert_arcgis_to_geoparquet

        convert_arcgis_to_geoparquet(
            self.SMALL_SERVICE,
            output_file,
            verbose=True,
            skip_hilbert=True,  # Skip for speed
        )

        assert Path(output_file).exists()
        pf = pq.ParquetFile(output_file)
        assert pf.metadata.num_rows >= 0

    def test_cli_full_conversion(self, output_file):
        """Test CLI full conversion of public service."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "convert",
                "arcgis",
                self.SMALL_SERVICE,
                output_file,
                "--skip-hilbert",
                "-v",
            ],
        )

        # May succeed or fail depending on network
        # We just want to ensure the command runs without crashing
        if result.exit_code == 0:
            assert Path(output_file).exists()

    def test_python_api_conversion(self, output_file):
        """Test Python API conversion."""
        import geoparquet_io as gpio

        table = gpio.convert_arcgis(self.SMALL_SERVICE)
        assert table.num_rows >= 0
        assert "geometry" in table.column_names
