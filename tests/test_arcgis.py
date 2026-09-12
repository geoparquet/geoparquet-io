"""
Tests for ArcGIS Feature Service conversion.

Tests use mocked HTTP responses to avoid network dependencies.
Network tests are marked separately for optional integration testing.
"""

import datetime
import json
import tempfile
import uuid
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from tests.conftest import safe_unlink
from tests.http_transport import FakeTransport, html_reply, json_reply
from tests.native_geo_probes import geo_block_crs_id, spec_problems

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

MOCK_ESRI_FEATURES_PAGE = {
    "geometryType": "esriGeometryPolygon",
    "spatialReference": {"wkid": 25830, "latestWkid": 25830},
    "fields": [
        {"name": "OBJECTID", "type": "esriFieldTypeOID"},
        {"name": "name", "type": "esriFieldTypeString"},
    ],
    "features": [
        {
            "attributes": {"OBJECTID": 1, "name": "Zone 1"},
            "geometry": {
                "rings": [
                    [
                        [442931.3, 4475041.4],
                        [442930.1, 4475006.5],
                        [442896.9, 4475008.9],
                        [442898.3, 4475043.0],
                        [442931.3, 4475041.4],
                    ]
                ]
            },
        }
    ],
}


def stub_arcgis_service(http, *, layer=None, count=None, page=None):
    """Route a whole FeatureServer layer at the fake transport.

    Metadata (`f=json` on the layer URL), the `returnCountOnly` probe and the
    feature pages, so a CLI or API call runs end to end with no network and the
    request it issued stays assertable.
    """
    page = page if page is not None else MOCK_FEATURES_PAGE
    count = count if count is not None else len(page["features"])
    http.respond(
        lambda request: not request.path.endswith("/query"),
        json_reply(layer if layer is not None else MOCK_LAYER_INFO),
    )
    http.respond(
        lambda request: request.params.get("returnCountOnly") == "true",
        json_reply({"count": count}),
    )
    http.respond(lambda request: True, json_reply(page))
    return http


def feature_page_requests(http):
    """The feature-page requests, in order (no metadata, no count probe)."""
    return [
        request
        for request in http.requests
        if request.path.endswith("/query") and request.params.get("returnCountOnly") != "true"
    ]


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
        from geoparquet_io.core.arcgis import validate_arcgis_url
        from geoparquet_io.core.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError, match="(?i)invalid arcgis url"):
            validate_arcgis_url("https://example.com/rest/services/Test/0")

    def test_invalid_url_no_layer_id(self):
        """Test invalid URL without layer ID."""
        from geoparquet_io.core.arcgis import validate_arcgis_url
        from geoparquet_io.core.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError, match="(?i)missing layer id"):
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
        from geoparquet_io.core.arcgis import generate_token
        from geoparquet_io.core.exceptions import GeoParquetError

        mock_request.return_value = {
            "error": {"code": 400, "message": "Invalid credentials", "details": []}
        }

        with pytest.raises(GeoParquetError, match="(?i)invalid credentials"):
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

    @patch("geoparquet_io.core.arcgis._make_request")
    @patch("geoparquet_io.core.arcgis.get_feature_count")
    def test_spatial_reference_from_extent(self, mock_count, mock_request):
        """Many servers advertise the layer SR only under extent.spatialReference."""
        from geoparquet_io.core.arcgis import get_layer_info

        mock_count.return_value = 1
        mock_request.return_value = {
            "name": "Madrid",
            "geometryType": "esriGeometryPolygon",
            "extent": {"spatialReference": {"wkid": 25830, "latestWkid": 25830}},
            "fields": [],
            "maxRecordCount": 1000,
        }

        result = get_layer_info("https://example.com/MapServer/0")

        assert result.spatial_reference == {"wkid": 25830, "latestWkid": 25830}

    @patch("geoparquet_io.core.arcgis._make_request")
    @patch("geoparquet_io.core.arcgis.get_feature_count")
    def test_spatial_reference_from_source_sr(self, mock_count, mock_request):
        from geoparquet_io.core.arcgis import get_layer_info

        mock_count.return_value = 1
        mock_request.return_value = {
            "name": "Layer",
            "geometryType": "esriGeometryPolygon",
            "sourceSpatialReference": {"wkid": 4269},
            "fields": [],
            "maxRecordCount": 1000,
        }

        result = get_layer_info("https://example.com/MapServer/0")

        assert result.spatial_reference == {"wkid": 4269}

    @patch("geoparquet_io.core.arcgis._make_request")
    @patch("geoparquet_io.core.arcgis.get_feature_count")
    def test_spatial_reference_missing_is_empty_not_4326(self, mock_count, mock_request):
        """No advertised SR must not silently default to 4326 (native would lie)."""
        from geoparquet_io.core.arcgis import get_layer_info

        mock_count.return_value = 1
        mock_request.return_value = {
            "name": "Layer",
            "geometryType": "esriGeometryPolygon",
            "fields": [],
            "maxRecordCount": 1000,
        }

        result = get_layer_info("https://example.com/MapServer/0")

        assert result.spatial_reference == {}


class TestFetchFeaturesPage:
    """What a feature-page request actually puts on the wire.

    Driven through ``tests/http_transport.py``: the assertion is the query
    string the server would have received, not a mock's recorded kwargs.
    """

    URL = "https://example.com/FeatureServer/0"

    def test_fetch_page(self, monkeypatch):
        """Test fetching a single page of features."""
        from geoparquet_io.core.arcgis import fetch_features_page

        http = FakeTransport.install(monkeypatch)
        http.respond("/query", json_reply(MOCK_FEATURES_PAGE))

        result = fetch_features_page(self.URL, offset=0, limit=1000)

        assert result["type"] == "FeatureCollection"
        assert len(result["features"]) == 3
        assert http.last.path == "/FeatureServer/0/query"
        assert http.last.params["resultOffset"] == "0"
        assert http.last.params["resultRecordCount"] == "1000"

    def test_fetch_page_default_uses_geojson(self, monkeypatch):
        from geoparquet_io.core.arcgis import fetch_features_page

        http = FakeTransport.install(monkeypatch)
        http.respond("/query", json_reply(MOCK_FEATURES_PAGE))

        fetch_features_page(self.URL, offset=0, limit=1000)

        assert http.last.params["f"] == "geojson"
        assert "outSR" not in http.last.params

    def test_fetch_page_output_wkid_uses_esrijson_and_outsr(self, monkeypatch):
        from geoparquet_io.core.arcgis import fetch_features_page

        http = FakeTransport.install(monkeypatch)
        http.respond("/query", json_reply(MOCK_ESRI_FEATURES_PAGE))

        fetch_features_page(self.URL, offset=0, limit=1000, output_wkid=25830)

        assert http.last.params["f"] == "json"
        assert http.last.params["outSR"] == "25830"

    def test_fetch_page_no_max_allowable_offset_by_default(self, monkeypatch):
        from geoparquet_io.core.arcgis import fetch_features_page

        http = FakeTransport.install(monkeypatch)
        http.respond("/query", json_reply(MOCK_FEATURES_PAGE))

        fetch_features_page(self.URL, offset=0, limit=1000)

        assert "maxAllowableOffset" not in http.last.params

    def test_fetch_page_max_allowable_offset_on_geojson(self, monkeypatch):
        """Generalization tolerance is honored on the default GeoJSON path too."""
        from geoparquet_io.core.arcgis import fetch_features_page

        http = FakeTransport.install(monkeypatch)
        http.respond("/query", json_reply(MOCK_FEATURES_PAGE))

        fetch_features_page(self.URL, offset=0, limit=1000, max_allowable_offset=0.005)

        assert http.last.params["f"] == "geojson"
        assert http.last.params["maxAllowableOffset"] == "0.005"
        # outSR is anchored to 4326 so the tolerance unit is unambiguously degrees.
        assert http.last.params["outSR"] == "4326"

    def test_fetch_page_max_allowable_offset_with_outsr(self, monkeypatch):
        from geoparquet_io.core.arcgis import fetch_features_page

        http = FakeTransport.install(monkeypatch)
        http.respond("/query", json_reply(MOCK_ESRI_FEATURES_PAGE))

        fetch_features_page(
            self.URL, offset=0, limit=1000, output_wkid=25830, max_allowable_offset=0.005
        )

        assert http.last.params["f"] == "json"
        assert http.last.params["outSR"] == "25830"
        assert http.last.params["maxAllowableOffset"] == "0.005"


class TestTimeout:
    """The configurable HTTP timeout (issue #518), read off the request itself.

    ``httpx`` records a per-request timeout in ``request.extensions``, so the
    harness asserts the value that actually reached the transport rather than
    the value handed to an intermediate mock.
    """

    URL = "https://example.com/FeatureServer/0"

    def test_fetch_page_default_timeout(self, monkeypatch):
        """Default timeout matches DEFAULT_TIMEOUT when not specified."""
        from geoparquet_io.core.arcgis import DEFAULT_TIMEOUT, fetch_features_page

        http = FakeTransport.install(monkeypatch)
        http.respond("/query", json_reply(MOCK_FEATURES_PAGE))

        fetch_features_page(self.URL, offset=0, limit=1000)

        assert http.last.timeout == DEFAULT_TIMEOUT

    def test_fetch_page_custom_timeout_threaded_to_request(self, monkeypatch):
        """A custom timeout reaches the underlying HTTP request."""
        from geoparquet_io.core.arcgis import fetch_features_page

        http = FakeTransport.install(monkeypatch)
        http.respond("/query", json_reply(MOCK_FEATURES_PAGE))

        fetch_features_page(self.URL, offset=0, limit=1000, timeout=300.0)

        assert http.last.timeout == 300.0

    def test_make_request_passes_timeout_to_retry_helper(self, monkeypatch):
        """_make_request forwards the timeout all the way to the wire."""
        from geoparquet_io.core.arcgis import _make_request

        http = FakeTransport.install(monkeypatch)
        http.respond("/test", json_reply({"ok": True}))

        assert _make_request("GET", "https://example.com/test", timeout=240.0) == {"ok": True}
        assert http.last.timeout == 240.0

    def test_make_request_with_retry_applies_timeout_per_request(self, monkeypatch):
        """The retry helper applies the timeout to the actual httpx call.

        Regression test: the shared client caches its first timeout, so the
        timeout must also be passed per-request to actually take effect. The
        harness hands out one client for every call, so a timeout applied only
        at construction would not show up on the second request.
        """
        from geoparquet_io.core.http_retry import make_request_with_retry

        http = FakeTransport.install(monkeypatch)
        http.respond("/test", json_reply({"ok": True}))

        make_request_with_retry("GET", "https://example.com/test", timeout=30.0)
        make_request_with_retry("GET", "https://example.com/test", timeout=300.0)

        assert [request.timeout for request in http.requests] == [30.0, 300.0]

    def test_non_json_without_batch_size_is_remote_access_error(self, monkeypatch):
        """A non-paged request that gets HTML must not report a batch problem.

        Regression test for #485: the count and layer-info queries carry no
        batch size, so a WAF/proxy block page was reported as
        "Batch size 0 too large", hiding the real cause.
        """
        from geoparquet_io.core.exceptions import BatchTooLargeError, RemoteAccessError
        from geoparquet_io.core.http_retry import make_request_with_retry

        http = FakeTransport.install(monkeypatch)
        http.respond("/count", html_reply(b"<html>blocked by WAF</html>"))

        with pytest.raises(RemoteAccessError) as exc_info:
            make_request_with_retry("GET", "https://example.com/count")

        assert not isinstance(exc_info.value, BatchTooLargeError)
        assert "Batch size" not in str(exc_info.value)
        assert "text/html" in str(exc_info.value)
        assert len(http.requests) == 1  # not retried

    def test_non_json_with_batch_size_keeps_batch_error(self, monkeypatch):
        """A paged request that gets HTML still raises BatchTooLargeError."""
        from geoparquet_io.core.exceptions import BatchTooLargeError
        from geoparquet_io.core.http_retry import make_request_with_retry

        http = FakeTransport.install(monkeypatch)
        http.respond("/query", html_reply(b"<html>blocked by WAF</html>"))

        with pytest.raises(BatchTooLargeError) as exc_info:
            make_request_with_retry("GET", "https://example.com/query", batch_size=1000)

        assert exc_info.value.batch_size == 1000
        assert len(http.requests) == 1


class TestCrsParsing:
    """Tests for output-crs parsing helpers."""

    def test_parse_crs_to_wkid_epsg_prefix(self):
        from geoparquet_io.core.arcgis import _parse_crs_to_wkid

        assert _parse_crs_to_wkid("EPSG:25830") == 25830

    def test_parse_crs_to_wkid_bare_code(self):
        from geoparquet_io.core.arcgis import _parse_crs_to_wkid

        assert _parse_crs_to_wkid("25830") == 25830

    def test_parse_crs_to_wkid_case_insensitive(self):
        from geoparquet_io.core.arcgis import _parse_crs_to_wkid

        assert _parse_crs_to_wkid("epsg:4148") == 4148

    def test_parse_crs_to_wkid_rejects_garbage(self):
        from geoparquet_io.core.arcgis import _parse_crs_to_wkid

        with pytest.raises(ValueError):
            _parse_crs_to_wkid("not-a-crs")

    def test_parse_crs_to_wkid_rejects_non_epsg_authority(self):
        from geoparquet_io.core.arcgis import _parse_crs_to_wkid

        with pytest.raises(ValueError, match="ESRI"):
            _parse_crs_to_wkid("ESRI:102039")

    def test_parse_crs_to_wkid_urn_form(self):
        from geoparquet_io.core.arcgis import _parse_crs_to_wkid

        assert _parse_crs_to_wkid("urn:ogc:def:crs:EPSG::25830") == 25830

    def test_wkid_from_spatial_reference_prefers_latest_wkid(self):
        from geoparquet_io.core.arcgis import _wkid_from_spatial_reference

        # latestWkid carries the modern EPSG code; legacy wkid is Esri-specific
        assert _wkid_from_spatial_reference({"wkid": 102100, "latestWkid": 3857}) == 3857
        assert _wkid_from_spatial_reference({"latestWkid": 4326}) == 4326
        assert _wkid_from_spatial_reference({"wkid": 25830}) == 25830
        assert _wkid_from_spatial_reference({}) is None

    def test_normalize_wkid_maps_esri_legacy_codes(self):
        from geoparquet_io.core.arcgis import _normalize_wkid

        assert _normalize_wkid(102100) == 3857
        assert _normalize_wkid(25830) == 25830


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

    @patch("geoparquet_io.core.arcgis._stream_features_to_parquet")
    @patch("geoparquet_io.core.arcgis.get_layer_info")
    def test_output_crs_is_wgs84_regardless_of_native_sr(
        self, mock_get_layer, mock_stream, tmp_path
    ):
        """Test that output CRS is always WGS84 even when layer's native SR is 3857 (issue #427).

        ArcGIS f=geojson always returns WGS84 per RFC 7946, so the output metadata
        must reflect that, not the layer's native spatial reference.
        """
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, arcgis_to_table

        # Layer advertises Web Mercator (EPSG:3857), but f=geojson returns WGS84
        layer_info = ArcGISLayerInfo(
            name="Test",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 102100, "latestWkid": 3857},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "name", "type": "esriFieldTypeString", "nullable": True},
            ],
            max_record_count=1000,
            total_count=3,
        )
        mock_get_layer.return_value = layer_info

        # Create a temp parquet with test data that _stream_features_to_parquet would produce
        temp_parquet = str(tmp_path / "temp.parquet")
        test_table = pa.table(
            {
                "geometry": [
                    b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
                ],
                "OBJECTID": [1],
                "name": ["Test Point"],
            }
        )
        pq.write_table(test_table, temp_parquet)

        def mock_stream_side_effect(*args, **kwargs):
            # Copy our test parquet to where arcgis_to_table expects it
            import shutil

            output_path = kwargs.get("output_path") or args[2]
            shutil.copy(temp_parquet, output_path)
            return 1, None

        mock_stream.side_effect = mock_stream_side_effect

        # Call arcgis_to_table which sets CRS metadata
        result = arcgis_to_table("https://example.com/FeatureServer/0")

        # Verify output CRS is WGS84, not 3857
        geo_meta = json.loads(result.schema.metadata[b"geo"])
        crs = geo_meta["columns"]["geometry"]["crs"]

        # CRS84 is WGS84 with lon/lat axis order (matches GeoJSON)
        assert crs["id"]["authority"] == "OGC"
        assert crs["id"]["code"] == "CRS84"


class TestArcgisToTableOutputCrs:
    def _layer(self, wkid, latest):
        from geoparquet_io.core.arcgis import ArcGISLayerInfo

        return ArcGISLayerInfo(
            name="Test",
            geometry_type="esriGeometryPolygon",
            spatial_reference={"wkid": wkid, "latestWkid": latest},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "name", "type": "esriFieldTypeString", "nullable": True},
            ],
            max_record_count=1000,
            total_count=1,
        )

    def _stub_stream(self, tmp_path, detected_sr):
        import shutil

        temp_parquet = str(tmp_path / "temp.parquet")
        pq.write_table(
            pa.table(
                {
                    "geometry": [
                        b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
                    ],
                    "OBJECTID": [1],
                    "name": ["Zone 1"],
                }
            ),
            temp_parquet,
        )

        def side_effect(*args, **kwargs):
            output_path = kwargs.get("output_path") or args[2]
            shutil.copy(temp_parquet, output_path)
            return 1, detected_sr

        return side_effect

    @patch("geoparquet_io.core.arcgis._stream_features_to_parquet")
    @patch("geoparquet_io.core.arcgis.get_layer_info")
    def test_explicit_output_crs_tags_returned_sr(self, mock_layer, mock_stream, tmp_path):
        from geoparquet_io.core.arcgis import arcgis_to_table

        mock_layer.return_value = self._layer(25830, 25830)
        mock_stream.side_effect = self._stub_stream(tmp_path, {"wkid": 25830, "latestWkid": 25830})

        result = arcgis_to_table("https://example.com/FeatureServer/0", output_crs="EPSG:25830")

        crs = json.loads(result.schema.metadata[b"geo"])["columns"]["geometry"]["crs"]
        assert crs["id"]["authority"] == "EPSG"
        assert crs["id"]["code"] == 25830
        assert mock_stream.call_args.kwargs["output_wkid"] == 25830

    @patch("geoparquet_io.core.arcgis._stream_features_to_parquet")
    @patch("geoparquet_io.core.arcgis.get_layer_info")
    def test_native_resolves_to_layer_sr(self, mock_layer, mock_stream, tmp_path):
        from geoparquet_io.core.arcgis import arcgis_to_table

        mock_layer.return_value = self._layer(25830, 25830)
        mock_stream.side_effect = self._stub_stream(tmp_path, {"wkid": 25830, "latestWkid": 25830})

        result = arcgis_to_table("https://example.com/FeatureServer/0", output_crs="native")

        assert mock_stream.call_args.kwargs["output_wkid"] == 25830
        crs = json.loads(result.schema.metadata[b"geo"])["columns"]["geometry"]["crs"]
        assert crs["id"]["code"] == 25830

    @patch("geoparquet_io.core.arcgis._stream_features_to_parquet")
    @patch("geoparquet_io.core.arcgis.get_layer_info")
    def test_no_warning_for_esri_legacy_wkid_alias(self, mock_layer, mock_stream, tmp_path, caplog):
        """Server echoing legacy wkid 102100 for EPSG:3857 must not warn."""
        import logging

        from geoparquet_io.core.arcgis import arcgis_to_table

        mock_layer.return_value = self._layer(102100, 3857)
        mock_stream.side_effect = self._stub_stream(tmp_path, {"wkid": 102100, "latestWkid": 3857})

        with caplog.at_level(logging.WARNING):
            result = arcgis_to_table("https://example.com/FeatureServer/0", output_crs="EPSG:3857")

        assert not any("server returned" in r.message for r in caplog.records)
        crs = json.loads(result.schema.metadata[b"geo"])["columns"]["geometry"]["crs"]
        assert crs["id"]["authority"] == "EPSG"
        assert crs["id"]["code"] == 3857

    @patch("geoparquet_io.core.arcgis._stream_features_to_parquet")
    @patch("geoparquet_io.core.arcgis.get_layer_info")
    def test_native_esri_only_wkid_tags_esri_authority(self, mock_layer, mock_stream, tmp_path):
        """ESRI-authority WKIDs (no EPSG equivalent) must not be tagged as EPSG."""
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, arcgis_to_table

        mock_layer.return_value = ArcGISLayerInfo(
            name="Test",
            geometry_type="esriGeometryPolygon",
            spatial_reference={"wkid": 102039},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "name", "type": "esriFieldTypeString", "nullable": True},
            ],
            max_record_count=1000,
            total_count=1,
        )
        mock_stream.side_effect = self._stub_stream(tmp_path, {"wkid": 102039})

        result = arcgis_to_table("https://example.com/FeatureServer/0", output_crs="native")

        assert mock_stream.call_args.kwargs["output_wkid"] == 102039
        crs = json.loads(result.schema.metadata[b"geo"])["columns"]["geometry"]["crs"]
        assert crs["id"]["authority"] == "ESRI"
        assert int(crs["id"]["code"]) == 102039

    @patch("geoparquet_io.core.arcgis._stream_features_to_parquet")
    @patch("geoparquet_io.core.arcgis.get_layer_info")
    def test_unresolvable_wkid_writes_no_crs(self, mock_layer, mock_stream, tmp_path, caplog):
        """A WKID resolving as neither EPSG nor ESRI must not be tagged as EPSG."""
        import logging

        from geoparquet_io.core.arcgis import ArcGISLayerInfo, arcgis_to_table

        mock_layer.return_value = ArcGISLayerInfo(
            name="Test",
            geometry_type="esriGeometryPolygon",
            spatial_reference={"wkid": 999999},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "name", "type": "esriFieldTypeString", "nullable": True},
            ],
            max_record_count=1000,
            total_count=1,
        )
        mock_stream.side_effect = self._stub_stream(tmp_path, {"wkid": 999999})

        with caplog.at_level(logging.WARNING):
            result = arcgis_to_table("https://example.com/FeatureServer/0", output_crs="native")

        # No fabricated EPSG metadata when the code cannot be resolved.
        metadata = result.schema.metadata or {}
        assert b"geo" not in metadata
        assert any("999999" in r.message for r in caplog.records)

    @patch("geoparquet_io.core.arcgis._stream_features_to_parquet")
    @patch("geoparquet_io.core.arcgis.get_layer_info")
    def test_native_wkt_only_resolves_via_epsg(self, mock_layer, mock_stream, tmp_path):
        """Native SR advertised only as WKT must resolve to its EPSG code."""
        from pyproj import CRS

        from geoparquet_io.core.arcgis import ArcGISLayerInfo, arcgis_to_table

        wkt = CRS.from_epsg(25830).to_wkt()
        mock_layer.return_value = ArcGISLayerInfo(
            name="Test",
            geometry_type="esriGeometryPolygon",
            spatial_reference={"wkt": wkt},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "name", "type": "esriFieldTypeString", "nullable": True},
            ],
            max_record_count=1000,
            total_count=1,
        )
        mock_stream.side_effect = self._stub_stream(tmp_path, {"wkid": 25830, "latestWkid": 25830})

        result = arcgis_to_table("https://example.com/FeatureServer/0", output_crs="native")

        assert mock_stream.call_args.kwargs["output_wkid"] == 25830
        crs = json.loads(result.schema.metadata[b"geo"])["columns"]["geometry"]["crs"]
        assert crs["id"]["code"] == 25830

    @patch("geoparquet_io.core.arcgis._stream_features_to_parquet")
    @patch("geoparquet_io.core.arcgis.get_layer_info")
    def test_native_wkt_only_unresolvable_raises(self, mock_layer, mock_stream, tmp_path):
        """A WKT-only native SR with no EPSG equivalent raises an accurate error."""
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, arcgis_to_table
        from geoparquet_io.core.exceptions import GeoParquetError

        wkt = (
            'LOCAL_CS["Custom",LOCAL_DATUM["Custom",0],UNIT["metre",1.0],'
            'AXIS["X",EAST],AXIS["Y",NORTH]]'
        )
        mock_layer.return_value = ArcGISLayerInfo(
            name="Test",
            geometry_type="esriGeometryPolygon",
            spatial_reference={"wkt": wkt},
            fields=[{"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False}],
            max_record_count=1000,
            total_count=1,
        )

        with pytest.raises(GeoParquetError, match="could not resolve"):
            arcgis_to_table("https://example.com/FeatureServer/0", output_crs="native")

        mock_stream.assert_not_called()

    @patch("geoparquet_io.core.arcgis.get_layer_info")
    def test_invalid_output_crs_raises_before_network(self, mock_layer):
        from geoparquet_io.core.arcgis import arcgis_to_table
        from geoparquet_io.core.exceptions import GeoParquetError

        with pytest.raises(GeoParquetError, match="output_crs"):
            arcgis_to_table("https://example.com/FeatureServer/0", output_crs="ESRI:102100")

        mock_layer.assert_not_called()

    @patch("geoparquet_io.core.arcgis._stream_features_to_parquet")
    @patch("geoparquet_io.core.arcgis.get_layer_info")
    def test_max_allowable_offset_threads_to_stream(self, mock_layer, mock_stream, tmp_path):
        from geoparquet_io.core.arcgis import arcgis_to_table

        mock_layer.return_value = self._layer(25830, 25830)
        mock_stream.side_effect = self._stub_stream(tmp_path, {"wkid": 25830, "latestWkid": 25830})

        arcgis_to_table(
            "https://example.com/FeatureServer/0",
            output_crs="EPSG:25830",
            max_allowable_offset=0.005,
        )

        assert mock_stream.call_args.kwargs["max_allowable_offset"] == 0.005

    @patch("geoparquet_io.core.arcgis.get_layer_info")
    def test_invalid_max_allowable_offset_raises_before_network(self, mock_layer):
        from geoparquet_io.core.arcgis import arcgis_to_table
        from geoparquet_io.core.exceptions import GeoParquetError

        with pytest.raises(GeoParquetError, match="max_allowable_offset"):
            arcgis_to_table("https://example.com/FeatureServer/0", max_allowable_offset=0)

        with pytest.raises(GeoParquetError, match="max_allowable_offset"):
            arcgis_to_table("https://example.com/FeatureServer/0", max_allowable_offset=-1.0)

        mock_layer.assert_not_called()

    @patch("geoparquet_io.core.arcgis._stream_features_to_parquet")
    @patch("geoparquet_io.core.arcgis.get_layer_info")
    def test_server_ignores_outsr_tags_returned_and_warns(
        self, mock_layer, mock_stream, tmp_path, caplog
    ):
        import logging

        from geoparquet_io.core.arcgis import arcgis_to_table

        mock_layer.return_value = self._layer(25830, 25830)
        mock_stream.side_effect = self._stub_stream(tmp_path, {"wkid": 4326, "latestWkid": 4326})

        with caplog.at_level(logging.WARNING):
            result = arcgis_to_table("https://example.com/FeatureServer/0", output_crs="EPSG:25830")

        crs = json.loads(result.schema.metadata[b"geo"])["columns"]["geometry"]["crs"]
        assert crs["id"]["code"] == 4326
        assert any("25830" in r.message and "4326" in r.message for r in caplog.records)


class TestSchemaBuilding:
    """Tests for schema building from layer metadata (issue #290)."""

    def test_basic_types(self):
        """Test mapping of common ArcGIS types to PyArrow types."""
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _build_schema_from_layer_info

        layer_info = ArcGISLayerInfo(
            name="test",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "int_field", "type": "esriFieldTypeInteger", "nullable": True},
                {"name": "double_field", "type": "esriFieldTypeDouble", "nullable": True},
                {"name": "str_field", "type": "esriFieldTypeString", "nullable": True},
                {"name": "date_field", "type": "esriFieldTypeDate", "nullable": True},
            ],
            max_record_count=2000,
            total_count=100,
        )

        schema = _build_schema_from_layer_info(layer_info)

        # Check geometry column
        assert schema.names[0] == "geometry"
        assert schema.field("geometry").type == pa.binary()
        assert schema.field("geometry").nullable  # Can be null for features without spatial data

        # Check attribute columns
        assert schema.field("OBJECTID").type == pa.int64()
        assert not schema.field("OBJECTID").nullable

        assert schema.field("int_field").type == pa.int32()
        assert schema.field("int_field").nullable

        assert schema.field("double_field").type == pa.float64()
        assert schema.field("double_field").nullable

        assert schema.field("str_field").type == pa.string()
        assert schema.field("str_field").nullable

        assert schema.field("date_field").type == pa.timestamp("ms")
        assert schema.field("date_field").nullable

    def test_date_only_and_time_only_types(self):
        """Test DateOnly and TimeOnly type mappings (added in ArcGIS Server 10.9)."""
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _build_schema_from_layer_info

        layer_info = ArcGISLayerInfo(
            name="test",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "date_only", "type": "esriFieldTypeDateOnly", "nullable": True},
                {"name": "time_only", "type": "esriFieldTypeTimeOnly", "nullable": True},
            ],
            max_record_count=2000,
            total_count=100,
        )

        schema = _build_schema_from_layer_info(layer_info)

        assert schema.field("date_only").type == pa.date32()
        assert schema.field("time_only").type == pa.time32("ms")

    def test_all_numeric_types(self):
        """Test all numeric type mappings."""
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _build_schema_from_layer_info

        layer_info = ArcGISLayerInfo(
            name="test",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "small_int", "type": "esriFieldTypeSmallInteger", "nullable": True},
                {"name": "int", "type": "esriFieldTypeInteger", "nullable": True},
                {"name": "big_int", "type": "esriFieldTypeBigInteger", "nullable": True},
                {"name": "single", "type": "esriFieldTypeSingle", "nullable": True},
                {"name": "double", "type": "esriFieldTypeDouble", "nullable": True},
            ],
            max_record_count=2000,
            total_count=100,
        )

        schema = _build_schema_from_layer_info(layer_info)

        assert schema.field("small_int").type == pa.int16()
        assert schema.field("int").type == pa.int32()
        assert schema.field("big_int").type == pa.int64()
        assert schema.field("single").type == pa.float32()
        assert schema.field("double").type == pa.float64()

    def test_special_types(self):
        """Test GUID, GlobalID, Blob, XML types."""
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _build_schema_from_layer_info

        layer_info = ArcGISLayerInfo(
            name="test",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "guid_field", "type": "esriFieldTypeGUID", "nullable": True},
                {"name": "globalid", "type": "esriFieldTypeGlobalID", "nullable": False},
                {"name": "blob_field", "type": "esriFieldTypeBlob", "nullable": True},
                {"name": "xml_field", "type": "esriFieldTypeXML", "nullable": True},
            ],
            max_record_count=2000,
            total_count=100,
        )

        schema = _build_schema_from_layer_info(layer_info)

        # GUIDs map to string
        assert schema.field("guid_field").type == pa.string()
        assert schema.field("guid_field").nullable

        # GlobalID is non-nullable string
        assert schema.field("globalid").type == pa.string()
        assert not schema.field("globalid").nullable

        # Blob maps to binary
        assert schema.field("blob_field").type == pa.binary()
        assert schema.field("blob_field").nullable

        # XML maps to string
        assert schema.field("xml_field").type == pa.string()
        assert schema.field("xml_field").nullable

    def test_unknown_type_fallback(self):
        """Test that unknown types fall back to string with warning."""
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _build_schema_from_layer_info

        layer_info = ArcGISLayerInfo(
            name="test",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "unknown_field", "type": "esriFieldTypeUnknown", "nullable": True},
            ],
            max_record_count=2000,
            total_count=100,
        )

        # Should not raise, should warn and fallback to string
        schema = _build_schema_from_layer_info(layer_info)
        assert schema.field("unknown_field").type == pa.string()
        assert schema.field("unknown_field").nullable

    def test_schema_field_count(self):
        """Test that schema has correct number of fields (geometry + attributes)."""
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _build_schema_from_layer_info

        layer_info = ArcGISLayerInfo(
            name="test",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "field1", "type": "esriFieldTypeString", "nullable": True},
                {"name": "field2", "type": "esriFieldTypeInteger", "nullable": True},
                {"name": "field3", "type": "esriFieldTypeDouble", "nullable": True},
            ],
            max_record_count=2000,
            total_count=100,
        )

        schema = _build_schema_from_layer_info(layer_info)

        # Should have geometry + 3 fields = 4 total
        assert len(schema) == 4
        assert schema.names == ["geometry", "field1", "field2", "field3"]


class TestCLI:
    """CLI integration tests."""

    @pytest.fixture
    def output_file(self):
        """Create temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_arcgis_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_basic_command(self, monkeypatch, output_file):
        """The command runs end to end and writes a spec-clean GeoParquet."""
        http = stub_arcgis_service(FakeTransport.install(monkeypatch))
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "extract",
                "arcgis",
                "https://example.com/FeatureServer/0",
                output_file,
            ],
        )

        assert result.exit_code == 0, result.output
        assert pq.read_metadata(output_file).num_rows == 3
        assert spec_problems(output_file) == []
        assert feature_page_requests(http)[0].params["f"] == "geojson"

    def test_missing_output(self):
        """Test error when output file missing."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "extract",
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
                "extract",
                "arcgis",
                "https://example.com/FeatureServer/0",
                output_file,
                "--username",
                "user",
            ],
        )

        assert result.exit_code != 0
        assert "password" in result.output.lower() or "password" in str(result.exception).lower()


class TestArcgisCliOutputCrs:
    """Tests for the --output-crs CLI option on extract arcgis."""

    def test_cli_passes_output_crs(self, monkeypatch, tmp_path):
        """--output-crs reaches the server as outSR, and the file says so."""
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        http = stub_arcgis_service(FakeTransport.install(monkeypatch), page=MOCK_ESRI_FEATURES_PAGE)
        out = str(tmp_path / "out.parquet")
        result = CliRunner().invoke(
            cli,
            [
                "extract",
                "arcgis",
                "https://example.com/FeatureServer/0",
                out,
                "--output-crs",
                "EPSG:25830",
            ],
        )

        assert result.exit_code == 0, result.output
        page = feature_page_requests(http)[0]
        assert page.params["outSR"] == "25830"
        assert page.params["f"] == "json"  # outSR is honored only for EsriJSON
        assert geo_block_crs_id(out) == {"authority": "EPSG", "code": 25830}

    @patch("geoparquet_io.core.arcgis.get_layer_info")
    def test_cli_invalid_output_crs_fails_cleanly(self, mock_layer, tmp_path):
        """A bad --output-crs must fail with a clean message before any network call."""
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        out = str(tmp_path / "out.parquet")
        result = CliRunner().invoke(
            cli,
            [
                "extract",
                "arcgis",
                "https://example.com/FeatureServer/0",
                out,
                "--output-crs",
                "not-a-crs",
            ],
        )

        assert result.exit_code != 0
        assert "output_crs" in result.output
        assert not isinstance(result.exception, ValueError)
        mock_layer.assert_not_called()

    def test_cli_passes_max_allowable_offset(self, monkeypatch, tmp_path):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        http = stub_arcgis_service(FakeTransport.install(monkeypatch))
        out = str(tmp_path / "out.parquet")
        result = CliRunner().invoke(
            cli,
            [
                "extract",
                "arcgis",
                "https://example.com/FeatureServer/0",
                out,
                "--max-allowable-offset",
                "0.005",
            ],
        )

        assert result.exit_code == 0, result.output
        assert feature_page_requests(http)[0].params["maxAllowableOffset"] == "0.005"

    def test_cli_passes_timeout(self, monkeypatch, tmp_path):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        http = stub_arcgis_service(FakeTransport.install(monkeypatch))
        out = str(tmp_path / "out.parquet")
        result = CliRunner().invoke(
            cli,
            [
                "extract",
                "arcgis",
                "https://example.com/FeatureServer/0",
                out,
                "--timeout",
                "300",
            ],
        )

        assert result.exit_code == 0, result.output
        assert {request.timeout for request in http.requests} == {300.0}

    def test_cli_timeout_defaults_to_60(self, monkeypatch, tmp_path):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        http = stub_arcgis_service(FakeTransport.install(monkeypatch))
        out = str(tmp_path / "out.parquet")
        result = CliRunner().invoke(
            cli,
            [
                "extract",
                "arcgis",
                "https://example.com/FeatureServer/0",
                out,
            ],
        )

        assert result.exit_code == 0, result.output
        assert {request.timeout for request in http.requests} == {60.0}

    @patch("geoparquet_io.core.arcgis.get_layer_info")
    def test_cli_invalid_max_allowable_offset_fails_cleanly(self, mock_layer, tmp_path):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        out = str(tmp_path / "out.parquet")
        result = CliRunner().invoke(
            cli,
            [
                "extract",
                "arcgis",
                "https://example.com/FeatureServer/0",
                out,
                "--max-allowable-offset",
                "0",
            ],
        )

        assert result.exit_code != 0
        assert "max_allowable_offset" in result.output
        mock_layer.assert_not_called()


class TestPythonAPI:
    """Tests for Python API functions."""

    @patch("geoparquet_io.core.arcgis.arcgis_to_table")
    def test_extract_arcgis_function(self, mock_arcgis_to_table):
        """Test extract_arcgis API function."""
        from geoparquet_io.api.table import extract_arcgis

        # Create mock table
        mock_table = pa.table({"geometry": [b"test"], "name": ["Point 1"]})
        mock_arcgis_to_table.return_value = mock_table

        result = extract_arcgis("https://example.com/FeatureServer/0")

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


class TestApiOutputCrs:
    """Tests for output_crs forwarding through the Python API."""

    @patch("geoparquet_io.core.arcgis.arcgis_to_table")
    def test_ops_from_arcgis_forwards_output_crs(self, mock_to_table):
        from geoparquet_io.api import ops

        mock_to_table.return_value = pa.table({"geometry": pa.array([], type=pa.binary())})
        ops.from_arcgis("https://example.com/FeatureServer/0", output_crs="native")

        assert mock_to_table.call_args.kwargs["output_crs"] == "native"

    @patch("geoparquet_io.core.arcgis.arcgis_to_table")
    def test_extract_arcgis_forwards_output_crs(self, mock_to_table):
        from geoparquet_io.api.table import extract_arcgis

        mock_to_table.return_value = pa.table({"geometry": pa.array([], type=pa.binary())})
        extract_arcgis("https://example.com/FeatureServer/0", output_crs="EPSG:25830")

        assert mock_to_table.call_args.kwargs["output_crs"] == "EPSG:25830"

    @patch("geoparquet_io.core.arcgis.arcgis_to_table")
    def test_ops_from_arcgis_forwards_max_allowable_offset(self, mock_to_table):
        from geoparquet_io.api import ops

        mock_to_table.return_value = pa.table({"geometry": pa.array([], type=pa.binary())})
        ops.from_arcgis("https://example.com/FeatureServer/0", max_allowable_offset=0.005)

        assert mock_to_table.call_args.kwargs["max_allowable_offset"] == 0.005

    @patch("geoparquet_io.core.arcgis.arcgis_to_table")
    def test_extract_arcgis_forwards_max_allowable_offset(self, mock_to_table):
        from geoparquet_io.api.table import extract_arcgis

        mock_to_table.return_value = pa.table({"geometry": pa.array([], type=pa.binary())})
        extract_arcgis("https://example.com/FeatureServer/0", max_allowable_offset=0.005)

        assert mock_to_table.call_args.kwargs["max_allowable_offset"] == 0.005


class TestStreamingConversion:
    """Tests for memory-efficient streaming conversion."""

    @pytest.fixture
    def output_file(self):
        """Create temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_arcgis_stream_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_geojson_page_to_table(self):
        """Test converting a single page of GeoJSON features to Arrow table."""
        from geoparquet_io.core.arcgis import _geojson_page_to_table

        features = MOCK_FEATURES_PAGE["features"]
        table = _geojson_page_to_table(features)

        assert table is not None
        assert table.num_rows == 3
        assert "geometry" in table.column_names

    def test_geojson_page_to_table_empty(self):
        """Test empty features returns None."""
        from geoparquet_io.core.arcgis import _geojson_page_to_table

        result = _geojson_page_to_table([])
        assert result is None

    def test_gdal_size_limit_lifted_inside_scope_and_unset_after(self, monkeypatch):
        """The limit is lifted inside the scope and removed again on exit (#517)."""
        import os

        from geoparquet_io.core.arcgis import _gdal_geojson_size_limit_lifted

        monkeypatch.delenv("OGR_GEOJSON_MAX_OBJ_SIZE", raising=False)
        with _gdal_geojson_size_limit_lifted():
            assert os.environ["OGR_GEOJSON_MAX_OBJ_SIZE"] == "0"
        assert "OGR_GEOJSON_MAX_OBJ_SIZE" not in os.environ

    def test_gdal_size_limit_restores_existing_value(self, monkeypatch):
        """A prior nonzero limit is lifted inside, then put back (issue #517)."""
        import os

        from geoparquet_io.core.arcgis import _gdal_geojson_size_limit_lifted

        monkeypatch.setenv("OGR_GEOJSON_MAX_OBJ_SIZE", "200")
        with _gdal_geojson_size_limit_lifted():
            assert os.environ["OGR_GEOJSON_MAX_OBJ_SIZE"] == "0"
        assert os.environ["OGR_GEOJSON_MAX_OBJ_SIZE"] == "200"

    def test_geojson_page_to_table_does_not_leak_size_limit(self, monkeypatch):
        """Converting a page lifts the limit for ST_Read without leaking it (#517)."""
        import os

        from geoparquet_io.core.arcgis import _geojson_page_to_table

        monkeypatch.setenv("OGR_GEOJSON_MAX_OBJ_SIZE", "200")
        table = _geojson_page_to_table(MOCK_FEATURES_PAGE["features"])

        assert table is not None
        assert os.environ["OGR_GEOJSON_MAX_OBJ_SIZE"] == "200", (
            "the GDAL size limit leaked out of the page conversion"
        )

    @patch("geoparquet_io.core.arcgis.fetch_all_features")
    def test_stream_features_to_parquet_single_page(self, mock_fetch, output_file):
        """Test streaming a single page to parquet."""
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _stream_features_to_parquet

        # Mock single page
        mock_fetch.return_value = iter([MOCK_FEATURES_PAGE])

        layer_info = ArcGISLayerInfo(
            name="Test",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "name", "type": "esriFieldTypeString", "nullable": True},
            ],
            max_record_count=1000,
            total_count=3,
        )

        total, _ = _stream_features_to_parquet(
            service_url="https://example.com/FeatureServer/0",
            layer_info=layer_info,
            output_path=output_file,
        )

        assert total == 3
        assert Path(output_file).exists()

        # Verify parquet content
        table = pq.read_table(output_file)
        assert table.num_rows == 3
        assert "geometry" in table.column_names

    @patch("geoparquet_io.core.arcgis.fetch_all_features")
    def test_stream_features_to_parquet_multi_page(self, mock_fetch, output_file):
        """Test streaming multiple pages to parquet."""
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _stream_features_to_parquet

        # Create two pages of features
        page1 = {
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
            ],
        }
        page2 = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-122.6, 38.0]},
                    "properties": {"OBJECTID": 3, "name": "Point 3"},
                },
            ],
        }

        mock_fetch.return_value = iter([page1, page2])

        layer_info = ArcGISLayerInfo(
            name="Test",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "name", "type": "esriFieldTypeString", "nullable": True},
            ],
            max_record_count=2,
            total_count=3,
        )

        total, _ = _stream_features_to_parquet(
            service_url="https://example.com/FeatureServer/0",
            layer_info=layer_info,
            output_path=output_file,
        )

        assert total == 3

        # Verify all rows are present
        table = pq.read_table(output_file)
        assert table.num_rows == 3

    @patch("geoparquet_io.core.arcgis.fetch_all_features")
    def test_stream_features_handles_empty_pages(self, mock_fetch, output_file):
        """Test streaming handles pages with no features."""
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _stream_features_to_parquet

        # First page has features, second is empty
        page1 = MOCK_FEATURES_PAGE
        page2 = {"type": "FeatureCollection", "features": []}

        mock_fetch.return_value = iter([page1, page2])

        layer_info = ArcGISLayerInfo(
            name="Test",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "name", "type": "esriFieldTypeString", "nullable": True},
            ],
            max_record_count=1000,
            total_count=3,
        )

        total, _ = _stream_features_to_parquet(
            service_url="https://example.com/FeatureServer/0",
            layer_info=layer_info,
            output_path=output_file,
        )

        assert total == 3  # Only features from page1

    @patch("geoparquet_io.core.arcgis._stream_features_to_parquet")
    @patch("geoparquet_io.core.arcgis.get_layer_info")
    @patch("geoparquet_io.core.arcgis.validate_arcgis_url")
    def test_arcgis_to_table_cleans_temp_file(self, mock_validate, mock_layer_info, mock_stream):
        """Test that temp file is cleaned up after conversion."""
        import glob
        import os

        from geoparquet_io.core.arcgis import ArcGISLayerInfo, arcgis_to_table

        mock_validate.return_value = ("https://example.com/FeatureServer/0", 0)
        mock_layer_info.return_value = ArcGISLayerInfo(
            name="Test",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[],
            max_record_count=1000,
            total_count=3,
        )

        # Create a real temp file that stream would create
        temp_dir = tempfile.gettempdir()

        def mock_stream_impl(service_url, layer_info, output_path, **kwargs):
            # Write a minimal parquet file
            table = pa.table({"geometry": [b"test1", b"test2", b"test3"], "name": ["a", "b", "c"]})
            pq.write_table(table, output_path)
            return 3, None

        mock_stream.side_effect = mock_stream_impl

        # Count temp files before
        temp_files_before = set(glob.glob(os.path.join(temp_dir, "arcgis_stream_*.parquet")))

        # Run conversion
        result = arcgis_to_table("https://example.com/FeatureServer/0")

        # Count temp files after
        temp_files_after = set(glob.glob(os.path.join(temp_dir, "arcgis_stream_*.parquet")))

        # Should have same number (temp file cleaned up)
        assert temp_files_before == temp_files_after
        assert result.num_rows == 3


class TestStreamOutputCrs:
    @patch("geoparquet_io.core.arcgis.fetch_all_features")
    def test_stream_returns_detected_sr_for_esrijson(self, mock_fetch, tmp_path):
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _stream_features_to_parquet

        layer_info = ArcGISLayerInfo(
            name="Test",
            geometry_type="esriGeometryPolygon",
            spatial_reference={"wkid": 25830, "latestWkid": 25830},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "name", "type": "esriFieldTypeString", "nullable": True},
            ],
            max_record_count=1000,
            total_count=1,
        )
        mock_fetch.return_value = iter([MOCK_ESRI_FEATURES_PAGE])
        out = str(tmp_path / "stream.parquet")

        total_rows, detected_sr = _stream_features_to_parquet(
            "https://example.com/FeatureServer/0",
            layer_info,
            out,
            output_wkid=25830,
        )

        assert total_rows == 1
        assert detected_sr == {"wkid": 25830, "latestWkid": 25830}
        assert mock_fetch.call_args.kwargs["output_wkid"] == 25830

    @patch("geoparquet_io.core.arcgis.fetch_all_features")
    def test_stream_default_returns_no_sr(self, mock_fetch, tmp_path):
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _stream_features_to_parquet

        layer_info = ArcGISLayerInfo(
            name="Test",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "name", "type": "esriFieldTypeString", "nullable": True},
            ],
            max_record_count=1000,
            total_count=3,
        )
        mock_fetch.return_value = iter([MOCK_FEATURES_PAGE])
        out = str(tmp_path / "stream.parquet")

        total_rows, detected_sr = _stream_features_to_parquet(
            "https://example.com/FeatureServer/0", layer_info, out
        )

        assert total_rows == 3
        assert detected_sr is None


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
        """Test CLI full extraction from public service."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "extract",
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
        """Test Python API extraction."""
        import geoparquet_io as gpio

        table = gpio.extract_arcgis(self.SMALL_SERVICE)
        assert table.num_rows >= 0
        assert "geometry" in table.column_names


class TestCastTableToSchema:
    """Schema-reconciliation tests for the shared _cast_table_to_schema helper.

    ArcGIS converged onto core/common._cast_table_to_schema (the same helper WFS
    uses) instead of a bespoke _align_table_to_schema + raw cast. These tests
    preserve the reorder/drop/add coverage of the deleted helper and add the new
    page-context-error and geometry-binary guarantees the convergence provides.
    """

    def test_reorders_columns_to_match_schema(self):
        """Columns are reordered to match the target schema."""
        from geoparquet_io.core.common import _cast_table_to_schema

        # Source table with different column order
        source = pa.table(
            {
                "c": [3, 6],
                "a": [1, 4],
                "b": [2, 5],
            }
        )

        target_schema = pa.schema(
            [
                pa.field("a", pa.int64()),
                pa.field("b", pa.int64()),
                pa.field("c", pa.int64()),
            ]
        )

        result = _cast_table_to_schema(source, target_schema)

        assert result.column_names == ["a", "b", "c"]
        assert result.column("a").to_pylist() == [1, 4]
        assert result.column("b").to_pylist() == [2, 5]
        assert result.column("c").to_pylist() == [3, 6]

    def test_drops_extra_columns(self):
        """Extra columns not in the target schema are dropped."""
        from geoparquet_io.core.common import _cast_table_to_schema

        source = pa.table(
            {
                "a": [1, 2],
                "b": [3, 4],
                "extra": ["x", "y"],  # Not in target schema
            }
        )

        target_schema = pa.schema(
            [
                pa.field("a", pa.int64()),
                pa.field("b", pa.int64()),
            ]
        )

        result = _cast_table_to_schema(source, target_schema)

        assert result.column_names == ["a", "b"]
        assert "extra" not in result.column_names

    def test_adds_missing_columns_with_nulls(self):
        """Missing columns are filled with typed nulls."""
        from geoparquet_io.core.common import _cast_table_to_schema

        source = pa.table(
            {
                "a": [1, 2],
            }
        )

        target_schema = pa.schema(
            [
                pa.field("a", pa.int64()),
                pa.field("missing", pa.string()),
            ]
        )

        result = _cast_table_to_schema(source, target_schema)

        assert result.column_names == ["a", "missing"]
        assert result.column("missing").to_pylist() == [None, None]

    def test_handles_all_mismatches_together(self):
        """Reorder + drop + add + cast in combination."""
        from geoparquet_io.core.common import _cast_table_to_schema

        source = pa.table(
            {
                "z": [9, 10],  # wrong position
                "a": [1, 2],  # should be first
                "extra": ["x", "y"],  # should be dropped
            }
        )

        target_schema = pa.schema(
            [
                pa.field("a", pa.int64()),
                pa.field("missing", pa.float64()),  # should be added as null
                pa.field("z", pa.int64()),
            ]
        )

        result = _cast_table_to_schema(source, target_schema)

        assert result.column_names == ["a", "missing", "z"]
        assert result.column("a").to_pylist() == [1, 2]
        assert result.column("missing").to_pylist() == [None, None]
        assert result.column("z").to_pylist() == [9, 10]
        assert "extra" not in result.column_names

    def test_casts_time_strings_to_time32(self):
        """HH:MM:SS strings (GDAL's TimeOnly shape) cast to a declared time32.

        PyArrow has no built-in string → time cast, so the helper parses these
        explicitly; without that, any layer with a TimeOnly field crashes.
        """
        import datetime

        from geoparquet_io.core.common import _cast_table_to_schema

        source = pa.table(
            {"time_only": pa.array(["14:30:00", "05:06:07", None], type=pa.large_string())}
        )
        target_schema = pa.schema([pa.field("time_only", pa.time32("ms"))])

        result = _cast_table_to_schema(source, target_schema)

        assert result.column("time_only").to_pylist() == [
            datetime.time(14, 30, 0),
            datetime.time(5, 6, 7),
            None,
        ]

    def test_casts_fractional_time_strings_to_time32(self):
        """Fractional seconds survive the string → time32(ms) parse."""
        import datetime

        from geoparquet_io.core.common import _cast_table_to_schema

        source = pa.table({"t": pa.array(["14:30:00.123", "00:00:01"], type=pa.string())})
        target_schema = pa.schema([pa.field("t", pa.time32("ms"))])

        result = _cast_table_to_schema(source, target_schema)

        assert result.column("t").to_pylist() == [
            datetime.time(14, 30, 0, 123000),
            datetime.time(0, 0, 1),
        ]

    def test_casts_all_null_string_column_to_time32(self):
        """An all-null page inferred as string still casts to a declared time32.

        DuckDB's GeoJSON driver infers an all-null TimeOnly column as string, so
        this shape shows up on any sparse page even without --output-crs.
        """
        from geoparquet_io.core.common import _cast_table_to_schema

        source = pa.table({"t": pa.array([None, None], type=pa.string())})
        target_schema = pa.schema([pa.field("t", pa.time32("ms"))])

        result = _cast_table_to_schema(source, target_schema)

        assert result.column("t").to_pylist() == [None, None]
        assert result.schema.field("t").type == pa.time32("ms")

    def test_unparseable_time_string_raises_with_context(self):
        """A non-time string still fails loudly, with the column named."""
        import pytest

        from geoparquet_io.core.common import _cast_table_to_schema

        source = pa.table({"t": pa.array(["not a time"], type=pa.string())})
        target_schema = pa.schema([pa.field("t", pa.time32("ms"))])

        with pytest.raises(ValueError, match="'t'"):
            _cast_table_to_schema(source, target_schema, page_info="batch 2")

    def test_geometry_binary_is_preserved(self):
        """A WKB geometry column stays binary even when attributes are strings.

        Guards against any regression that coerces the geometry column toward
        string when reconciling an ArcGIS page table to the metadata schema.
        """
        from geoparquet_io.core.common import _cast_table_to_schema

        source = pa.table(
            {
                "geometry": pa.array([b"\x01\x02", b"\x03\x04"], type=pa.binary()),
                "name": ["a", "b"],
            }
        )

        target_schema = pa.schema(
            [
                pa.field("geometry", pa.binary()),
                pa.field("name", pa.string()),
            ]
        )

        result = _cast_table_to_schema(source, target_schema)

        assert result.schema.field("geometry").type == pa.binary()
        assert result.column("geometry").to_pylist() == [b"\x01\x02", b"\x03\x04"]

    def test_int32_column_upcast_to_timestamp(self):
        """int32 → timestamp is upcast via int64 instead of crashing (issue #516).

        DuckDB can infer an epoch-zero/null date column as int32 while the layer
        metadata declares timestamp. PyArrow cannot cast int32 → timestamp
        directly (it raises ArrowNotImplementedError), so the helper upcasts to
        int64 first.
        """
        from geoparquet_io.core.common import _cast_table_to_schema

        source = pa.table({"valid_on": pa.array([0, 1000], type=pa.int32())})
        target_schema = pa.schema([pa.field("valid_on", pa.timestamp("ms"))])

        result = _cast_table_to_schema(source, target_schema)

        assert result.schema.field("valid_on").type == pa.timestamp("ms")
        # 0 ms = epoch, 1000 ms = one second past epoch
        assert result.column("valid_on").to_pylist() == [
            datetime.datetime(1970, 1, 1, 0, 0, 0),
            datetime.datetime(1970, 1, 1, 0, 0, 1),
        ]

    def test_int16_column_upcast_to_timestamp(self):
        """Any sub-64-bit integer column is upcast before the timestamp cast."""
        from geoparquet_io.core.common import _cast_table_to_schema

        source = pa.table({"valid_on": pa.array([0, 500], type=pa.int16())})
        target_schema = pa.schema([pa.field("valid_on", pa.timestamp("ms"))])

        result = _cast_table_to_schema(source, target_schema)

        assert result.schema.field("valid_on").type == pa.timestamp("ms")
        assert result.column("valid_on").to_pylist() == [
            datetime.datetime(1970, 1, 1, 0, 0, 0),
            datetime.datetime(1970, 1, 1, 0, 0, 0, 500000),
        ]

    def test_cast_failure_reports_page_context(self):
        """An impossible cast raises ValueError carrying the page/column context.

        This is the new diagnostic the convergence buys: ArcGIS wraps this
        ValueError into a GeoParquetError that names the batch and column,
        replacing the previous generic message.
        """
        from geoparquet_io.core.common import _cast_table_to_schema

        # A non-numeric string cannot be safely cast to int64
        source = pa.table({"amount": ["not_a_number", "also_bad"]})
        target_schema = pa.schema([pa.field("amount", pa.int64())])

        with pytest.raises(ValueError, match="amount") as exc_info:
            _cast_table_to_schema(source, target_schema, page_info="batch 7")

        assert "batch 7" in str(exc_info.value)


class TestSchemaMismatchBetweenBatches:
    """Tests for issue #334: Schema mismatch with mixed-type chunks.

    When extracting from ArcGIS, different pages may have different inferred
    types for the same field (e.g., int64 vs double) due to the actual data
    values in each batch.
    """

    @pytest.fixture
    def output_file(self, tmp_path):
        """Create a temporary output file path."""
        output = tmp_path / f"test_output_{uuid.uuid4()}.parquet"
        yield str(output)
        safe_unlink(output)

    @patch("geoparquet_io.core.arcgis.fetch_all_features")
    def test_handles_int_to_double_type_variance(self, mock_fetch, output_file):
        """Test that double fields with integer values in some batches are handled.

        Reproduces issue #334: When a double field (like TownGlValu) has only
        whole numbers in some batches, DuckDB infers int64, causing schema
        mismatch when cast to the correct float64 from ArcGIS metadata.
        """
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _stream_features_to_parquet

        # Page 1: value field has decimal values (inferred as double)
        page1 = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-72.5, 44.2]},
                    "properties": {"OBJECTID": 1, "TownGlValu": 15000.50},
                },
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-72.6, 44.3]},
                    "properties": {"OBJECTID": 2, "TownGlValu": 25000.75},
                },
            ],
        }

        # Page 2: value field has only whole numbers (may be inferred as int64)
        page2 = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-72.7, 44.4]},
                    "properties": {"OBJECTID": 3, "TownGlValu": 30000},
                },
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-72.8, 44.5]},
                    "properties": {"OBJECTID": 4, "TownGlValu": 40000},
                },
            ],
        }

        mock_fetch.return_value = iter([page1, page2])

        # Layer metadata correctly defines TownGlValu as double
        layer_info = ArcGISLayerInfo(
            name="VT Property Transfers",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "TownGlValu", "type": "esriFieldTypeDouble", "nullable": True},
            ],
            max_record_count=2000,
            total_count=4,
        )

        # This should NOT raise a schema mismatch error
        total, _ = _stream_features_to_parquet(
            service_url="https://example.com/FeatureServer/0",
            layer_info=layer_info,
            output_path=output_file,
        )

        assert total == 4

        # Verify all values are present and correctly typed as float64
        table = pq.read_table(output_file)
        assert table.num_rows == 4
        assert table.schema.field("TownGlValu").type == pa.float64()

    @patch("geoparquet_io.core.arcgis.fetch_all_features")
    def test_handles_null_column_becoming_string(self, mock_fetch, output_file):
        """Test that columns with nulls in early batches are handled.

        When a column has all nulls in batch 1, DuckDB may infer it as null type.
        Later batches with actual values should still work.
        """
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _stream_features_to_parquet

        # Page 1: description is null
        page1 = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-72.5, 44.2]},
                    "properties": {"OBJECTID": 1, "description": None},
                },
            ],
        }

        # Page 2: description has actual values
        page2 = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-72.6, 44.3]},
                    "properties": {"OBJECTID": 2, "description": "Some text"},
                },
            ],
        }

        mock_fetch.return_value = iter([page1, page2])

        layer_info = ArcGISLayerInfo(
            name="Test Layer",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "description", "type": "esriFieldTypeString", "nullable": True},
            ],
            max_record_count=2000,
            total_count=2,
        )

        total, _ = _stream_features_to_parquet(
            service_url="https://example.com/FeatureServer/0",
            layer_info=layer_info,
            output_path=output_file,
        )

        assert total == 2

        table = pq.read_table(output_file)
        assert table.num_rows == 2
        assert table.schema.field("description").type == pa.string()

    @patch("geoparquet_io.core.arcgis.fetch_all_features")
    def test_handles_multiple_type_variance_columns(self, mock_fetch, output_file):
        """Test multiple columns with type variance across batches."""
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _stream_features_to_parquet

        # Page 1: mixed types
        page1 = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-72.5, 44.2]},
                    "properties": {
                        "OBJECTID": 1,
                        "price": 100.50,  # float
                        "count": 10,  # int
                        "rating": None,  # null
                    },
                },
            ],
        }

        # Page 2: different inference
        page2 = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-72.6, 44.3]},
                    "properties": {
                        "OBJECTID": 2,
                        "price": 200,  # whole number (might infer as int)
                        "count": 20,
                        "rating": 4.5,  # now has value
                    },
                },
            ],
        }

        mock_fetch.return_value = iter([page1, page2])

        layer_info = ArcGISLayerInfo(
            name="Test Layer",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "price", "type": "esriFieldTypeDouble", "nullable": True},
                {"name": "count", "type": "esriFieldTypeInteger", "nullable": True},
                {"name": "rating", "type": "esriFieldTypeSingle", "nullable": True},
            ],
            max_record_count=2000,
            total_count=2,
        )

        total, _ = _stream_features_to_parquet(
            service_url="https://example.com/FeatureServer/0",
            layer_info=layer_info,
            output_path=output_file,
        )

        assert total == 2

        table = pq.read_table(output_file)
        assert table.num_rows == 2
        assert table.schema.field("price").type == pa.float64()
        assert table.schema.field("count").type == pa.int32()
        assert table.schema.field("rating").type == pa.float32()

    @patch("geoparquet_io.core.arcgis.fetch_all_features")
    def test_handles_column_order_mismatch(self, mock_fetch, output_file):
        """Test that column order differences between batches are handled.

        DuckDB may return columns in different order than the ArcGIS metadata.
        This should not cause a schema mismatch error.
        """
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _stream_features_to_parquet

        # Properties order differs from layer_info field order
        page1 = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-72.5, 44.2]},
                    # Note: properties order is name, OBJECTID, value
                    "properties": {"name": "First", "OBJECTID": 1, "value": 100.0},
                },
            ],
        }

        mock_fetch.return_value = iter([page1])

        # Layer metadata defines: OBJECTID, name, value (different order!)
        layer_info = ArcGISLayerInfo(
            name="Test Layer",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "name", "type": "esriFieldTypeString", "nullable": True},
                {"name": "value", "type": "esriFieldTypeDouble", "nullable": True},
            ],
            max_record_count=2000,
            total_count=1,
        )

        # This should NOT raise a schema mismatch error
        total, _ = _stream_features_to_parquet(
            service_url="https://example.com/FeatureServer/0",
            layer_info=layer_info,
            output_path=output_file,
        )

        assert total == 1

        table = pq.read_table(output_file)
        assert table.num_rows == 1
        # Verify schema matches target, not source order
        assert table.schema.names == ["geometry", "OBJECTID", "name", "value"]

    @patch("geoparquet_io.core.arcgis.fetch_all_features")
    def test_handles_extra_columns_from_service(self, mock_fetch, output_file):
        """Test that extra columns from service not in metadata are dropped.

        Some ArcGIS services return more fields than listed in metadata.
        We should only keep fields defined in layer metadata.
        """
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _stream_features_to_parquet

        page1 = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-72.5, 44.2]},
                    "properties": {
                        "OBJECTID": 1,
                        "name": "Test",
                        "extra_field": "should_be_dropped",  # Not in metadata
                    },
                },
            ],
        }

        mock_fetch.return_value = iter([page1])

        layer_info = ArcGISLayerInfo(
            name="Test Layer",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "name", "type": "esriFieldTypeString", "nullable": True},
            ],
            max_record_count=2000,
            total_count=1,
        )

        total, _ = _stream_features_to_parquet(
            service_url="https://example.com/FeatureServer/0",
            layer_info=layer_info,
            output_path=output_file,
        )

        assert total == 1

        table = pq.read_table(output_file)
        assert table.num_rows == 1
        # extra_field should NOT be in the output
        assert "extra_field" not in table.schema.names
        assert table.schema.names == ["geometry", "OBJECTID", "name"]

    @patch("geoparquet_io.core.arcgis.fetch_all_features")
    def test_handles_missing_columns_from_service(self, mock_fetch, output_file):
        """Test that missing columns from service are filled with nulls.

        Some ArcGIS services may not return all fields in every response.
        Missing fields should be filled with nulls of the correct type.
        """
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _stream_features_to_parquet

        page1 = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-72.5, 44.2]},
                    "properties": {
                        "OBJECTID": 1,
                        # "name" is missing from this response
                    },
                },
            ],
        }

        mock_fetch.return_value = iter([page1])

        layer_info = ArcGISLayerInfo(
            name="Test Layer",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "name", "type": "esriFieldTypeString", "nullable": True},
            ],
            max_record_count=2000,
            total_count=1,
        )

        total, _ = _stream_features_to_parquet(
            service_url="https://example.com/FeatureServer/0",
            layer_info=layer_info,
            output_path=output_file,
        )

        assert total == 1

        table = pq.read_table(output_file)
        assert table.num_rows == 1
        # name should be present (as null) even though it wasn't in the service response
        assert "name" in table.schema.names
        assert table.column("name")[0].as_py() is None


@pytest.mark.network
class TestRealWorldSchemaMismatch:
    """Integration tests for schema mismatch with real-world ArcGIS services.

    These tests require network access and validate the fix for issue #334
    against actual ArcGIS REST endpoints that exhibited the problem.
    """

    # Vermont property transfer dataset from issue #334
    VT_PROPERTY_SERVICE = (
        "https://services1.arcgis.com/BkFxaEFNwHqX3tAw/arcgis/rest/services/"
        "FS_VCGI_OPENDATA_Cadastral_PTTR_point_WM_v1_view/FeatureServer/0"
    )

    def test_vermont_property_transfers_issue_334(self):
        """Test extraction from VT property transfer dataset that triggered #334.

        This dataset has the TownGlValu field (esriFieldTypeDouble) which caused
        schema mismatches when some batches only had integer values.

        The #334 regression lives in the per-page schema reconciliation inside
        ``_stream_features_to_parquet``; what actually exercises it is crossing
        *multiple batches* over that Double field, not the total row count. We
        drive the core extractor with a small ``batch_size`` (3 pages of 50 =
        150 rows) rather than pulling 5000 rows through the server's 2000-row
        pages. That hits the identical code path with ~30x less live data,
        keeping this live third-party fetch fast and far less prone to the
        transient worker crashes that a large multi-page fetch under parallel
        CI can trigger. (``batch_size`` isn't exposed on the public
        ``extract_arcgis`` API, so we call the core ``arcgis_to_table``, which
        the public API wraps and which owns the #334 fix.)
        """
        from geoparquet_io.core.arcgis import arcgis_to_table

        # 3 small pages (50 each) still span the TownGlValu Double field across
        # batches, which is exactly what triggered the schema mismatch in #334.
        table = arcgis_to_table(
            self.VT_PROPERTY_SERVICE,
            limit=150,
            batch_size=50,
        )

        assert table.num_rows > 0
        assert "geometry" in table.column_names

        # TownGlValu is the esriFieldTypeDouble field from #334; its presence
        # confirms we actually crossed the batches that used to mismatch.
        assert "TownGlValu" in table.column_names

        # Verify the schema is consistent (no schema mismatch error occurred).
        # If we got here without error, the fix is working.
        assert table.schema is not None


class TestBatchSizeReduction:
    """Unit tests for adaptive batch size functionality."""

    def test_get_reduced_batch_size_from_large(self):
        """Test batch size reduction from large value."""
        from geoparquet_io.core.arcgis import _get_reduced_batch_size

        # From 2000, should reduce to 1000
        assert _get_reduced_batch_size(2000) == 1000
        # From 1500, should reduce to 1000
        assert _get_reduced_batch_size(1500) == 1000

    def test_get_reduced_batch_size_from_medium(self):
        """Test batch size reduction from medium values."""
        from geoparquet_io.core.arcgis import _get_reduced_batch_size

        # From 1000, should reduce to 500
        assert _get_reduced_batch_size(1000) == 500
        # From 500, should reduce to 100
        assert _get_reduced_batch_size(500) == 100

    def test_get_reduced_batch_size_from_small(self):
        """Test batch size reduction from small values."""
        from geoparquet_io.core.arcgis import _get_reduced_batch_size

        # From 100, should reduce to 50
        assert _get_reduced_batch_size(100) == 50
        # From 50, should reduce to 10
        assert _get_reduced_batch_size(50) == 10
        # From 10, should reduce to 1
        assert _get_reduced_batch_size(10) == 1

    def test_get_reduced_batch_size_at_minimum(self):
        """Test batch size reduction at minimum returns None."""
        from geoparquet_io.core.arcgis import _get_reduced_batch_size

        # At 1, can't reduce further
        assert _get_reduced_batch_size(1) is None
        # At 0 (edge case), can't reduce
        assert _get_reduced_batch_size(0) is None

    def test_get_reduced_batch_size_unusual_values(self):
        """Test batch size reduction with unusual values."""
        from geoparquet_io.core.arcgis import _get_reduced_batch_size

        # Value between fallbacks should get next lower
        assert _get_reduced_batch_size(750) == 500
        assert _get_reduced_batch_size(5) == 1
        assert _get_reduced_batch_size(2) == 1


class TestBatchTooLargeError:
    """Unit tests for BatchTooLargeError exception."""

    def test_error_attributes(self):
        """Test BatchTooLargeError stores correct attributes."""
        from geoparquet_io.core.exceptions import BatchTooLargeError

        error = BatchTooLargeError(
            url="https://example.com/arcgis/rest/services/test/FeatureServer/0/query",
            batch_size=500,
            reason="Server returned HTML error page",
        )

        assert error.batch_size == 500
        assert "500" in str(error)
        assert "HTML error page" in str(error)

    def test_error_sanitizes_url(self):
        """Test BatchTooLargeError sanitizes sensitive URL params."""
        from geoparquet_io.core.exceptions import BatchTooLargeError

        # URL with query params that might contain credentials
        error = BatchTooLargeError(
            url="https://example.com/arcgis?token=secret123&other=param",
            batch_size=100,
            reason="test",
        )

        # Query params should be stripped
        assert "secret123" not in error.url


class TestAdaptiveBatchWithMock:
    """Tests for adaptive batch size behavior using mocks."""

    def test_batch_reduces_on_json_error(self):
        """Test that batch size reduces when server returns non-JSON."""
        from unittest.mock import patch

        from geoparquet_io.core.arcgis import (
            ArcGISLayerInfo,
            fetch_all_features,
        )
        from geoparquet_io.core.exceptions import BatchTooLargeError

        # Mock layer info
        layer_info = ArcGISLayerInfo(
            name="Test",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[],
            max_record_count=1000,
            total_count=100,
        )

        call_count = 0
        batch_sizes_used = []

        def mock_fetch_page(service_url, offset, limit, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            batch_sizes_used.append(limit)

            # First call with large batch fails
            if limit > 50:
                raise BatchTooLargeError(
                    url=service_url,
                    batch_size=limit,
                    reason="Simulated server error",
                )

            # Smaller batches succeed
            return {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [0, 0]},
                        "properties": {"id": i},
                    }
                    for i in range(offset, min(offset + limit, 100))
                ],
            }

        with patch(
            "geoparquet_io.core.arcgis.fetch_features_page",
            side_effect=mock_fetch_page,
        ):
            pages = list(
                fetch_all_features(
                    "https://example.com/FeatureServer/0",
                    layer_info,
                    batch_size=100,
                )
            )

        # Should have reduced batch size and succeeded
        assert len(pages) > 0
        # First attempt was 100, then reduced to 50
        assert 100 in batch_sizes_used
        assert 50 in batch_sizes_used


@pytest.mark.network
@pytest.mark.slow
@pytest.mark.integration
class TestAdaptiveBatchNetworkIntegration:
    """Network integration tests for adaptive batch size (issue #382)."""

    # Mozambique Admin 3 layer that triggers batch-too-large errors
    MOZ_ADMIN3_SERVICE = (
        "https://www.mozgis.gov.mz/server/rest/services/Data/"
        "UnidadesAdministrativasCenso2017/FeatureServer/3"
    )

    @pytest.fixture
    def output_file(self, tmp_path):
        """Create a temporary output file path."""
        output = tmp_path / f"moz_admin3_{uuid.uuid4()}.parquet"
        yield str(output)
        safe_unlink(output)

    @pytest.fixture
    def expected_feature_count(self):
        """Fetch current feature count from the service dynamically."""
        from geoparquet_io.core.arcgis import get_feature_count

        return get_feature_count(self.MOZ_ADMIN3_SERVICE)

    def test_mozambique_admin3_adaptive_batch_issue_382(self, output_file, expected_feature_count):
        """Test extraction from Mozambique Admin 3 that triggers issue #382.

        This layer has complex polygons. The server's maxRecordCount is 2000,
        but it returns HTML errors for batches larger than ~100 features due
        to payload size limits.

        The adaptive batch size should automatically reduce and succeed.
        """
        from geoparquet_io.core.arcgis import convert_arcgis_to_geoparquet

        # This should succeed with adaptive batch reduction
        convert_arcgis_to_geoparquet(
            self.MOZ_ADMIN3_SERVICE,
            output_file,
            skip_hilbert=True,  # Skip for speed
            verbose=True,
        )

        assert Path(output_file).exists()
        pf = pq.ParquetFile(output_file)
        # Should have all features (count fetched dynamically from service)
        assert pf.metadata.num_rows == expected_feature_count

    def test_cli_batch_size_option(self, output_file, expected_feature_count):
        """Test --batch-size CLI option works."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "extract",
                "arcgis",
                self.MOZ_ADMIN3_SERVICE,
                output_file,
                "--batch-size",
                "50",  # Start with small batch to avoid errors
                "--skip-hilbert",
                "-v",
            ],
        )

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert Path(output_file).exists()
        pf = pq.ParquetFile(output_file)
        assert pf.metadata.num_rows == expected_feature_count


class TestEsriJsonPageToTable:
    """EsriJSON pages are parsed by DuckDB ST_Read (GDAL ESRIJSON driver)."""

    def test_esrijson_page_to_table_parses_geometry_and_attrs(self):
        from geoparquet_io.core.arcgis import _esrijson_page_to_table

        table = _esrijson_page_to_table(MOCK_ESRI_FEATURES_PAGE)

        assert table is not None
        assert table.num_rows == 1
        assert "geometry" in table.column_names
        # Attribute columns are flattened from `attributes`
        assert "OBJECTID" in table.column_names
        assert "name" in table.column_names
        # Geometry is WKB bytes (projected coords preserved, not reprojected)
        assert isinstance(table.column("geometry")[0].as_py(), bytes)

    def test_esrijson_page_to_table_empty(self):
        from geoparquet_io.core.arcgis import _esrijson_page_to_table

        assert _esrijson_page_to_table({"features": []}) is None


class TestConvertOutputCrs:
    @patch("geoparquet_io.core.arcgis.write_geoparquet_table")
    @patch("geoparquet_io.core.arcgis.arcgis_to_table")
    def test_convert_forwards_output_crs(self, mock_to_table, _mock_write, tmp_path):
        from geoparquet_io.core.arcgis import convert_arcgis_to_geoparquet

        mock_to_table.return_value = pa.table({"geometry": pa.array([], type=pa.binary())})
        out = str(tmp_path / "out.parquet")

        convert_arcgis_to_geoparquet(
            "https://example.com/FeatureServer/0",
            out,
            output_crs="EPSG:25830",
            skip_hilbert=True,
            skip_bbox=True,
        )

        assert mock_to_table.call_args.kwargs["output_crs"] == "EPSG:25830"


@pytest.mark.network
@pytest.mark.slow
class TestArcgisOutputCrsLive:
    """Live smoke test against a real ArcGIS server to confirm that
    --output-crs preserves geometries in their native CRS (EPSG:25830)
    rather than reprojecting them to WGS84."""

    def test_madrid_native_25830(self, tmp_path):
        import duckdb

        from geoparquet_io.core.arcgis import convert_arcgis_to_geoparquet
        from geoparquet_io.core.exceptions import GeoParquetError, RemoteAccessError

        out = str(tmp_path / "zp.parquet")
        url = "https://sigma.madrid.es/hosted/rest/services/MOVILIDAD/ZONAS_PEATONALES/MapServer/0"

        # An unavailable server should skip, not fail. A returned-but-wrong
        # CRS is a real bug and must fail (AssertionError propagates below).
        try:
            convert_arcgis_to_geoparquet(
                url,
                out,
                limit=5,
                output_crs="EPSG:25830",
                geoparquet_version="2.0",
            )
        except (RemoteAccessError, GeoParquetError) as exc:
            pytest.skip(f"Madrid server unavailable: {exc}")

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")
        crs = con.execute(f"SELECT ST_CRS(geometry) FROM '{out}' LIMIT 1").fetchone()[0]
        assert "25830" in str(crs)
