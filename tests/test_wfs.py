"""
Tests for WFS (Web Feature Service) extraction.

Tests use mocked HTTP responses to avoid network dependencies.
Network tests are marked separately for optional integration testing.
"""

import logging
import os
import re
import time
from unittest.mock import MagicMock, patch

import pytest

# Module-level imports for WFS functions (avoids per-test imports)
from geoparquet_io.core import wfs as wfs_module
from geoparquet_io.core.wfs import (
    EmptyLayerError,
    LayerNotFoundError,
    WFSAuthenticationError,
    WFSError,
    WFSLayerInfo,
    _build_bbox_param,
    _build_local_bbox_filter,
    _build_wfs_url,
    _detect_best_output_format,
    _detect_sortable_attribute,
    _determine_bbox_strategy,
    _negotiate_crs,
    _normalize_crs,
    _validate_identifier,
    get_layer_info,
    get_wfs_capabilities,
    list_available_layers,
)
from tests.http_transport import FakeTransport, geojson_reply, xml_reply


@pytest.fixture
def offline_wfs_probes():
    """Complete the mock for the WFS entry points so they stop reaching the network.

    Mocking ``negotiate_wfs_version`` / ``get_layer_info`` /
    ``fetch_all_features_duckdb`` is *not* enough to take ``wfs_to_table``
    offline. Before it reaches the mocked fetch, its auto-tiling logic calls
    ``_get_feature_count`` twice (once for WFS 2.0.0, then for the requested
    version), and those calls are unmocked. Against a fake host they resolve to
    real DNS lookups that fail, and ``_get_feature_count`` swallows every
    exception -- so the tests still passed while quietly burning the 1s+2s
    retry backoff twice, ~6s per test.

    ``_probe_startindex_limit`` is the same trap one level down, and it is
    load-bearing for a different caller: ``fetch_all_features_duckdb`` probes
    the server's startIndex support before paging, so tests that call it
    directly (``TestAutoPageSingleWorker``) leak even though they mock the page
    fetcher. It swallows ``httpx.HTTPError`` the same way.

    Stubbing both probes to ``None`` reproduces exactly what the failed requests
    already returned, so no assertion changes meaning. ``_make_request`` is
    additionally replaced with a tripwire, and the recorded calls are asserted
    on teardown so a *future* unmocked network path fails loudly instead of
    silently sleeping.

    Scope note: the tripwire covers only ``_make_request``-mediated traffic.
    ``_fetch_wfs_page`` and ``_probe_startindex_limit`` drive httpx directly and
    bypass it; they are covered by the explicit stubs above, not by the
    tripwire. A new httpx-driven helper would therefore escape silently -- add
    it to this fixture rather than assuming the tripwire caught it.
    """
    escaped_requests: list[str] = []

    def _blocked_request(url, *args, **kwargs):
        escaped_requests.append(url)
        raise AssertionError(f"unmocked WFS HTTP request to {url}")

    with (
        patch.object(wfs_module, "_get_feature_count", return_value=None),
        patch.object(wfs_module, "_probe_startindex_limit", return_value=None),
        patch.object(wfs_module, "_make_request", side_effect=_blocked_request),
    ):
        yield

    assert not escaped_requests, (
        f"wfs_to_table made unmocked HTTP request(s): {escaped_requests}. "
        "Extend the offline_wfs_probes fixture to cover the new call."
    )


# =============================================================================
# Mock WFS Response Data
# =============================================================================

# WFS 1.1.0 GetCapabilities response with 2 feature types (cities, roads)
MOCK_CAPABILITIES_XML = """<?xml version="1.0" encoding="UTF-8"?>
<wfs:WFS_Capabilities
    xmlns:wfs="http://www.opengis.net/wfs"
    xmlns:ows="http://www.opengis.net/ows"
    xmlns:ogc="http://www.opengis.net/ogc"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:gml="http://www.opengis.net/gml"
    version="1.1.0">

    <ows:ServiceIdentification>
        <ows:Title>Mock WFS Server</ows:Title>
        <ows:Abstract>A mock WFS server for testing geoparquet-io</ows:Abstract>
        <ows:ServiceType>WFS</ows:ServiceType>
        <ows:ServiceTypeVersion>1.1.0</ows:ServiceTypeVersion>
    </ows:ServiceIdentification>

    <ows:ServiceProvider>
        <ows:ProviderName>Test Provider</ows:ProviderName>
    </ows:ServiceProvider>

    <ows:OperationsMetadata>
        <ows:Operation name="GetCapabilities">
            <ows:DCP>
                <ows:HTTP>
                    <ows:Get xlink:href="http://mock.wfs.server/wfs"/>
                </ows:HTTP>
            </ows:DCP>
        </ows:Operation>
        <ows:Operation name="DescribeFeatureType">
            <ows:DCP>
                <ows:HTTP>
                    <ows:Get xlink:href="http://mock.wfs.server/wfs"/>
                    <ows:Post xlink:href="http://mock.wfs.server/wfs"/>
                </ows:HTTP>
            </ows:DCP>
        </ows:Operation>
        <ows:Operation name="GetFeature">
            <ows:DCP>
                <ows:HTTP>
                    <ows:Get xlink:href="http://mock.wfs.server/wfs"/>
                    <ows:Post xlink:href="http://mock.wfs.server/wfs"/>
                </ows:HTTP>
            </ows:DCP>
            <ows:Parameter name="outputFormat">
                <ows:Value>text/xml; subtype=gml/3.1.1</ows:Value>
                <ows:Value>application/json</ows:Value>
                <ows:Value>application/geo+json</ows:Value>
            </ows:Parameter>
        </ows:Operation>
    </ows:OperationsMetadata>

    <wfs:FeatureTypeList>
        <wfs:FeatureType>
            <wfs:Name>test:cities</wfs:Name>
            <wfs:Title>Cities</wfs:Title>
            <wfs:Abstract>Major cities dataset for testing</wfs:Abstract>
            <wfs:DefaultSRS>urn:ogc:def:crs:EPSG::4326</wfs:DefaultSRS>
            <wfs:OtherSRS>urn:ogc:def:crs:EPSG::3857</wfs:OtherSRS>
            <wfs:OutputFormats>
                <wfs:Format>text/xml; subtype=gml/3.1.1</wfs:Format>
                <wfs:Format>application/json</wfs:Format>
            </wfs:OutputFormats>
            <ows:WGS84BoundingBox>
                <ows:LowerCorner>-180.0 -90.0</ows:LowerCorner>
                <ows:UpperCorner>180.0 90.0</ows:UpperCorner>
            </ows:WGS84BoundingBox>
        </wfs:FeatureType>
        <wfs:FeatureType>
            <wfs:Name>test:roads</wfs:Name>
            <wfs:Title>Roads</wfs:Title>
            <wfs:Abstract>Road network dataset for testing</wfs:Abstract>
            <wfs:DefaultSRS>urn:ogc:def:crs:EPSG::4326</wfs:DefaultSRS>
            <wfs:OutputFormats>
                <wfs:Format>text/xml; subtype=gml/3.1.1</wfs:Format>
                <wfs:Format>application/json</wfs:Format>
            </wfs:OutputFormats>
            <ows:WGS84BoundingBox>
                <ows:LowerCorner>-125.0 24.0</ows:LowerCorner>
                <ows:UpperCorner>-66.0 50.0</ows:UpperCorner>
            </ows:WGS84BoundingBox>
        </wfs:FeatureType>
    </wfs:FeatureTypeList>

    <ogc:Filter_Capabilities>
        <ogc:Spatial_Capabilities>
            <ogc:GeometryOperands>
                <ogc:GeometryOperand>gml:Envelope</ogc:GeometryOperand>
                <ogc:GeometryOperand>gml:Point</ogc:GeometryOperand>
                <ogc:GeometryOperand>gml:Polygon</ogc:GeometryOperand>
            </ogc:GeometryOperands>
            <ogc:SpatialOperators>
                <ogc:SpatialOperator name="BBOX"/>
                <ogc:SpatialOperator name="Intersects"/>
                <ogc:SpatialOperator name="Within"/>
            </ogc:SpatialOperators>
        </ogc:Spatial_Capabilities>
        <ogc:Scalar_Capabilities>
            <ogc:LogicalOperators/>
            <ogc:ComparisonOperators>
                <ogc:ComparisonOperator>EqualTo</ogc:ComparisonOperator>
                <ogc:ComparisonOperator>NotEqualTo</ogc:ComparisonOperator>
                <ogc:ComparisonOperator>LessThan</ogc:ComparisonOperator>
                <ogc:ComparisonOperator>GreaterThan</ogc:ComparisonOperator>
                <ogc:ComparisonOperator>Like</ogc:ComparisonOperator>
            </ogc:ComparisonOperators>
        </ogc:Scalar_Capabilities>
    </ogc:Filter_Capabilities>
</wfs:WFS_Capabilities>
"""

# GeoJSON FeatureCollection with 3 point features
MOCK_GEOJSON_RESPONSE = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "cities.1",
            "geometry": {"type": "Point", "coordinates": [-122.4194, 37.7749]},
            "properties": {
                "gml_id": "cities.1",
                "name": "San Francisco",
                "population": 884363,
                "country": "USA",
            },
        },
        {
            "type": "Feature",
            "id": "cities.2",
            "geometry": {"type": "Point", "coordinates": [-73.9857, 40.7484]},
            "properties": {
                "gml_id": "cities.2",
                "name": "New York",
                "population": 8336817,
                "country": "USA",
            },
        },
        {
            "type": "Feature",
            "id": "cities.3",
            "geometry": {"type": "Point", "coordinates": [-0.1276, 51.5074]},
            "properties": {
                "gml_id": "cities.3",
                "name": "London",
                "population": 8982000,
                "country": "UK",
            },
        },
    ],
    "totalFeatures": 3,
    "numberMatched": 3,
    "numberReturned": 3,
    "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}},
}

# GML3 response with 1 feature (WFS 1.1.0 default format)
MOCK_GML_RESPONSE = """<?xml version="1.0" encoding="UTF-8"?>
<wfs:FeatureCollection
    xmlns:wfs="http://www.opengis.net/wfs"
    xmlns:gml="http://www.opengis.net/gml"
    xmlns:test="http://mock.wfs.server/test"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    numberOfFeatures="1"
    timeStamp="2026-03-23T12:00:00Z">

    <gml:boundedBy>
        <gml:Envelope srsName="urn:ogc:def:crs:EPSG::4326">
            <gml:lowerCorner>37.7749 -122.4194</gml:lowerCorner>
            <gml:upperCorner>37.7749 -122.4194</gml:upperCorner>
        </gml:Envelope>
    </gml:boundedBy>

    <gml:featureMember>
        <test:cities gml:id="cities.1">
            <gml:boundedBy>
                <gml:Envelope srsName="urn:ogc:def:crs:EPSG::4326">
                    <gml:lowerCorner>37.7749 -122.4194</gml:lowerCorner>
                    <gml:upperCorner>37.7749 -122.4194</gml:upperCorner>
                </gml:Envelope>
            </gml:boundedBy>
            <test:geometry>
                <gml:Point srsName="urn:ogc:def:crs:EPSG::4326">
                    <gml:pos>37.7749 -122.4194</gml:pos>
                </gml:Point>
            </test:geometry>
            <test:name>San Francisco</test:name>
            <test:population>884363</test:population>
            <test:country>USA</test:country>
        </test:cities>
    </gml:featureMember>
</wfs:FeatureCollection>
"""

# Zero-feature GML responses. A WFS 2.0 server answers an empty bbox with a
# self-closing FeatureCollection; a WFS 1.x server (GeoServer) sends one whose
# only child is a null boundedBy. GDAL can open neither ("contains no layers"),
# so the GML path must recognize them as legitimately empty, not as error pages.
MOCK_EMPTY_GML_WFS20 = """<?xml version="1.0" encoding="UTF-8"?>
<wfs:FeatureCollection
    xmlns:wfs="http://www.opengis.net/wfs/2.0"
    xmlns:gml="http://www.opengis.net/gml/3.2"
    numberMatched="0"
    numberReturned="0"
    timeStamp="2026-03-23T12:00:00Z"/>
"""

MOCK_EMPTY_GML_WFS11 = """<?xml version="1.0" encoding="UTF-8"?>
<wfs:FeatureCollection
    xmlns:wfs="http://www.opengis.net/wfs"
    xmlns:gml="http://www.opengis.net/gml"
    numberOfFeatures="0"
    timeStamp="2026-03-23T12:00:00Z">
    <gml:boundedBy>
        <gml:null>unknown</gml:null>
    </gml:boundedBy>
</wfs:FeatureCollection>
"""

# GeoServer's GML 3.1 flavor of the same answer: an empty featureMembers child.
MOCK_EMPTY_GML_FEATUREMEMBERS = """<?xml version="1.0" encoding="UTF-8"?>
<wfs:FeatureCollection
    xmlns:wfs="http://www.opengis.net/wfs"
    xmlns:gml="http://www.opengis.net/gml"
    numberOfFeatures="0"
    timeStamp="2026-03-23T12:00:00Z">
    <gml:featureMembers/>
</wfs:FeatureCollection>
"""

# GML response whose features carry no gml:id at all. GDAL synthesizes
# gml_id = '' (empty string, not NULL) for these, which must not become a
# shared dedup key.
MOCK_GML_IDLESS_RESPONSE = """<?xml version="1.0" encoding="UTF-8"?>
<wfs:FeatureCollection
    xmlns:wfs="http://www.opengis.net/wfs"
    xmlns:gml="http://www.opengis.net/gml"
    xmlns:test="http://mock.wfs.server/test"
    numberOfFeatures="3"
    timeStamp="2026-03-23T12:00:00Z">
    <gml:featureMember>
        <test:cities>
            <test:geometry>
                <gml:Point srsName="urn:ogc:def:crs:EPSG::4326">
                    <gml:pos>37.7749 -122.4194</gml:pos>
                </gml:Point>
            </test:geometry>
            <test:name>San Francisco</test:name>
        </test:cities>
    </gml:featureMember>
    <gml:featureMember>
        <test:cities>
            <test:geometry>
                <gml:Point srsName="urn:ogc:def:crs:EPSG::4326">
                    <gml:pos>40.7128 -74.0060</gml:pos>
                </gml:Point>
            </test:geometry>
            <test:name>New York</test:name>
        </test:cities>
    </gml:featureMember>
    <gml:featureMember>
        <test:cities>
            <test:geometry>
                <gml:Point srsName="urn:ogc:def:crs:EPSG::4326">
                    <gml:pos>51.5074 -0.1278</gml:pos>
                </gml:Point>
            </test:geometry>
            <test:name>London</test:name>
        </test:cities>
    </gml:featureMember>
</wfs:FeatureCollection>
"""

# XSD schema response for DescribeFeatureType
MOCK_DESCRIBE_FEATURE_TYPE = """<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema
    xmlns:xsd="http://www.w3.org/2001/XMLSchema"
    xmlns:gml="http://www.opengis.net/gml"
    xmlns:test="http://mock.wfs.server/test"
    targetNamespace="http://mock.wfs.server/test"
    elementFormDefault="qualified">

    <xsd:import namespace="http://www.opengis.net/gml"
        schemaLocation="http://schemas.opengis.net/gml/3.1.1/base/gml.xsd"/>

    <xsd:complexType name="citiesType">
        <xsd:complexContent>
            <xsd:extension base="gml:AbstractFeatureType">
                <xsd:sequence>
                    <xsd:element name="geometry" type="gml:PointPropertyType"
                        minOccurs="0" maxOccurs="1"/>
                    <xsd:element name="name" type="xsd:string"
                        minOccurs="0" maxOccurs="1"/>
                    <xsd:element name="population" type="xsd:int"
                        minOccurs="0" maxOccurs="1"/>
                    <xsd:element name="country" type="xsd:string"
                        minOccurs="0" maxOccurs="1"/>
                </xsd:sequence>
            </xsd:extension>
        </xsd:complexContent>
    </xsd:complexType>

    <xsd:element name="cities" type="test:citiesType"
        substitutionGroup="gml:_Feature"/>

    <xsd:complexType name="roadsType">
        <xsd:complexContent>
            <xsd:extension base="gml:AbstractFeatureType">
                <xsd:sequence>
                    <xsd:element name="geometry" type="gml:MultiLineStringPropertyType"
                        minOccurs="0" maxOccurs="1"/>
                    <xsd:element name="name" type="xsd:string"
                        minOccurs="0" maxOccurs="1"/>
                    <xsd:element name="highway_type" type="xsd:string"
                        minOccurs="0" maxOccurs="1"/>
                    <xsd:element name="lanes" type="xsd:int"
                        minOccurs="0" maxOccurs="1"/>
                </xsd:sequence>
            </xsd:extension>
        </xsd:complexContent>
    </xsd:complexType>

    <xsd:element name="roads" type="test:roadsType"
        substitutionGroup="gml:_Feature"/>
</xsd:schema>
"""

# Empty FeatureCollection response
MOCK_EMPTY_RESPONSE = {
    "type": "FeatureCollection",
    "features": [],
    "totalFeatures": 0,
    "numberMatched": 0,
    "numberReturned": 0,
    "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}},
}


# =============================================================================
# Pytest Fixtures
# =============================================================================


@pytest.fixture
def mock_capabilities_xml():
    """Return WFS 1.1.0 GetCapabilities XML response.

    Contains 2 feature types:
    - test:cities (Point geometry, global extent)
    - test:roads (LineString geometry, USA extent)

    Supports GML 3.1.1 and JSON output formats.
    """
    return MOCK_CAPABILITIES_XML


@pytest.fixture
def mock_geojson_response():
    """Return GeoJSON FeatureCollection with 3 point features.

    Features represent cities:
    - San Francisco (population: 884363)
    - New York (population: 8336817)
    - London (population: 8982000)

    CRS is EPSG:4326 (WGS84).
    """
    return MOCK_GEOJSON_RESPONSE


@pytest.fixture
def mock_gml_response():
    """Return GML3 response with 1 feature.

    Contains a single city (San Francisco) in GML 3.1.1 format.
    This is the default WFS 1.1.0 GetFeature response format.
    """
    return MOCK_GML_RESPONSE


@pytest.fixture
def mock_describe_feature_type():
    """Return XSD schema response for DescribeFeatureType.

    Describes 2 feature types:
    - cities: Point geometry with name, population, country fields
    - roads: MultiLineString geometry with name, highway_type, lanes fields
    """
    return MOCK_DESCRIBE_FEATURE_TYPE


@pytest.fixture
def mock_empty_response():
    """Return empty GeoJSON FeatureCollection.

    Represents a WFS GetFeature response with no matching features.
    totalFeatures, numberMatched, and numberReturned are all 0.
    """
    return MOCK_EMPTY_RESPONSE


@pytest.fixture
def mock_wfs_url():
    """Return mock WFS server URL."""
    return "http://mock.wfs.server/wfs"


@pytest.fixture
def mock_wfs_responses(
    mock_wfs_url,
    mock_capabilities_xml,
    mock_geojson_response,
    mock_gml_response,
    mock_describe_feature_type,
    mock_empty_response,
):
    """Return dict of all mock WFS responses keyed by request type.

    Useful for setting up comprehensive request mocking.
    """
    return {
        "url": mock_wfs_url,
        "capabilities": mock_capabilities_xml,
        "geojson": mock_geojson_response,
        "gml": mock_gml_response,
        "describe_feature_type": mock_describe_feature_type,
        "empty": mock_empty_response,
    }


# =============================================================================
# Additional Mock Data for Pagination Tests
# =============================================================================

# First page of 2 features (offset 0)
MOCK_GEOJSON_PAGE_1 = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "cities.1",
            "geometry": {"type": "Point", "coordinates": [-122.4194, 37.7749]},
            "properties": {"gml_id": "cities.1", "name": "San Francisco", "population": 884363},
        },
        {
            "type": "Feature",
            "id": "cities.2",
            "geometry": {"type": "Point", "coordinates": [-73.9857, 40.7484]},
            "properties": {"gml_id": "cities.2", "name": "New York", "population": 8336817},
        },
    ],
    "numberMatched": 5,
    "numberReturned": 2,
}

# Second page of 2 features (offset 2)
MOCK_GEOJSON_PAGE_2 = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "cities.3",
            "geometry": {"type": "Point", "coordinates": [-0.1276, 51.5074]},
            "properties": {"gml_id": "cities.3", "name": "London", "population": 8982000},
        },
        {
            "type": "Feature",
            "id": "cities.4",
            "geometry": {"type": "Point", "coordinates": [139.6917, 35.6895]},
            "properties": {"gml_id": "cities.4", "name": "Tokyo", "population": 13960000},
        },
    ],
    "numberMatched": 5,
    "numberReturned": 2,
}

# Third page with 1 feature (offset 4)
MOCK_GEOJSON_PAGE_3 = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "cities.5",
            "geometry": {"type": "Point", "coordinates": [2.3522, 48.8566]},
            "properties": {"gml_id": "cities.5", "name": "Paris", "population": 2161000},
        },
    ],
    "numberMatched": 5,
    "numberReturned": 1,
}

# Capabilities XML with minimal optional fields (missing Abstract, OtherSRS)
MOCK_MINIMAL_CAPABILITIES_XML = """<?xml version="1.0" encoding="UTF-8"?>
<wfs:WFS_Capabilities
    xmlns:wfs="http://www.opengis.net/wfs"
    xmlns:ows="http://www.opengis.net/ows"
    version="1.1.0">
    <wfs:FeatureTypeList>
        <wfs:FeatureType>
            <wfs:Name>minimal:layer</wfs:Name>
            <wfs:DefaultSRS>urn:ogc:def:crs:EPSG::4326</wfs:DefaultSRS>
        </wfs:FeatureType>
    </wfs:FeatureTypeList>
</wfs:WFS_Capabilities>
"""


@pytest.fixture
def mock_geojson_page_1():
    """First page of paginated results."""
    return MOCK_GEOJSON_PAGE_1


@pytest.fixture
def mock_geojson_page_2():
    """Second page of paginated results."""
    return MOCK_GEOJSON_PAGE_2


@pytest.fixture
def mock_geojson_page_3():
    """Third (final) page of paginated results."""
    return MOCK_GEOJSON_PAGE_3


@pytest.fixture
def mock_minimal_capabilities_xml():
    """Minimal capabilities XML with missing optional fields."""
    return MOCK_MINIMAL_CAPABILITIES_XML


# =============================================================================
# Mock-Based Test Classes
# =============================================================================


class TestCapabilityParsing:
    """Tests for OWSLib-based capability parsing via mocked WebFeatureService."""

    def _create_mock_wfs(self):
        """Create a mock OWSLib WebFeatureService object."""
        mock_wfs = MagicMock()

        # Mock layer contents
        cities_layer = MagicMock()
        cities_layer.id = "test:cities"
        cities_layer.title = "Cities"
        cities_layer.crsOptions = [
            "urn:ogc:def:crs:EPSG::4326",
            "urn:ogc:def:crs:EPSG::3857",
        ]
        cities_layer.boundingBoxWGS84 = (-180.0, -90.0, 180.0, 90.0)

        roads_layer = MagicMock()
        roads_layer.id = "test:roads"
        roads_layer.title = "Road Network"
        roads_layer.crsOptions = ["urn:ogc:def:crs:EPSG::4326"]
        roads_layer.boundingBoxWGS84 = (-125.0, 24.0, -66.0, 50.0)

        mock_wfs.contents = {
            "test:cities": cities_layer,
            "test:roads": roads_layer,
        }
        mock_wfs.getfeature_output_formats = [
            "application/json",
            "application/geo+json",
            "text/xml; subtype=gml/3.1.1",
        ]

        return mock_wfs

    @patch("owslib.wfs.WebFeatureService")
    def test_parses_layer_list(self, mock_wfs_class):
        """Test that GetCapabilities returns WFS with layer contents."""
        mock_wfs_class.return_value = self._create_mock_wfs()

        wfs = get_wfs_capabilities("http://mock.wfs.server/wfs")

        # WFS object should have contents with 2 layers
        assert len(wfs.contents) == 2
        assert "test:cities" in wfs.contents
        assert "test:roads" in wfs.contents

    @patch("owslib.wfs.WebFeatureService")
    def test_extracts_layer_info(self, mock_wfs_class):
        """Test extraction of layer info via OWSLib interface."""
        mock_wfs_class.return_value = self._create_mock_wfs()

        wfs = get_wfs_capabilities("http://mock.wfs.server/wfs")

        # Verify cities layer metadata via OWSLib interface
        cities = wfs.contents["test:cities"]
        assert cities.title == "Cities"
        assert "urn:ogc:def:crs:EPSG::4326" in cities.crsOptions
        assert cities.boundingBoxWGS84 == (-180.0, -90.0, 180.0, 90.0)

        # Verify roads layer
        roads = wfs.contents["test:roads"]
        assert roads.boundingBoxWGS84 == (-125.0, 24.0, -66.0, 50.0)

    @patch("owslib.wfs.WebFeatureService")
    def test_extracts_supported_formats(self, mock_wfs_class):
        """Test extraction of supported output formats."""
        mock_wfs_class.return_value = self._create_mock_wfs()

        wfs = get_wfs_capabilities("http://mock.wfs.server/wfs")

        # Check formats via OWSLib interface
        assert "application/json" in wfs.getfeature_output_formats
        assert "application/geo+json" in wfs.getfeature_output_formats

    @patch("owslib.wfs.WebFeatureService")
    def test_handles_connection_error(self, mock_wfs_class):
        """Test handling of connection errors."""
        mock_wfs_class.side_effect = Exception("Connection refused")
        from geoparquet_io.core.wfs import WFSError

        with pytest.raises(WFSError, match="Could not connect|Connection"):
            get_wfs_capabilities("http://mock.wfs.server/wfs")


class TestErrorHandling:
    """Tests for error handling in WFS module."""

    def test_wfs_error_raised_for_invalid_url(self):
        """Test that WFSError is raised for connection failures."""
        from geoparquet_io.core.wfs import WFSError

        # Invalid URL should raise WFSError
        with pytest.raises(WFSError):
            get_wfs_capabilities("http://localhost:99999/invalid")

    @patch("owslib.wfs.WebFeatureService")
    def test_layer_not_found_error(self, mock_wfs_class):
        """Test error when requested layer doesn't exist."""
        mock_wfs = MagicMock()
        # Only "ns:cities" exists, not "nonexistent:data"
        mock_wfs.contents = {"ns:cities": MagicMock()}
        mock_wfs_class.return_value = mock_wfs

        from geoparquet_io.core.wfs import WFSError

        with pytest.raises(WFSError, match="not found"):
            get_layer_info("http://mock.wfs.server/wfs", "nonexistent:data")


class TestWFSExceptionHierarchy:
    """Test typed WFS exception subclasses for downstream consumers."""

    @pytest.mark.parametrize(
        ("exc_class", "args"),
        [
            pytest.param(EmptyLayerError, ("test:layer",), id="empty-layer"),
            pytest.param(LayerNotFoundError, ("test:layer",), id="layer-not-found"),
            pytest.param(
                WFSAuthenticationError,
                ("http://example.com/wfs", 401, "Auth required"),
                id="authentication",
            ),
        ],
    )
    def test_subclass_catchable_as_wfs_error(self, exc_class, args):
        """Each typed subclass is catchable as WFSError (backward compatible)."""
        assert issubclass(exc_class, WFSError)
        with pytest.raises(WFSError) as exc_info:
            raise exc_class(*args)
        assert isinstance(exc_info.value, exc_class)

    def test_empty_layer_error_has_typename(self):
        """EmptyLayerError should expose typename attribute."""
        err = EmptyLayerError("ns:layername")
        assert err.typename == "ns:layername"
        assert "ns:layername" in str(err)
        assert "No features returned" in str(err)

    def test_layer_not_found_error_has_typename_and_available(self):
        """LayerNotFoundError should expose typename and available layers."""
        err = LayerNotFoundError("ns:missing", ["ns:layer1", "ns:layer2"])
        assert err.typename == "ns:missing"
        assert err.available == ["ns:layer1", "ns:layer2"]
        assert "ns:missing" in str(err)
        assert "not found" in str(err)

    def test_layer_not_found_error_with_no_available(self):
        """LayerNotFoundError should work without available layers."""
        err = LayerNotFoundError("ns:missing")
        assert err.typename == "ns:missing"
        assert err.available == []
        assert "ns:missing" in str(err)

    def test_auth_error_has_url_and_status_code(self):
        """WFSAuthenticationError should expose url and status_code."""
        err = WFSAuthenticationError("http://example.com/wfs", 401, "Auth required")
        assert err.url == "http://example.com/wfs"
        assert err.status_code == 401
        assert "Auth required" in str(err)

    @patch("owslib.wfs.WebFeatureService")
    def test_layer_not_found_raises_typed_error(self, mock_wfs_class):
        """LayerNotFoundError is raised when layer doesn't exist."""
        mock_wfs = MagicMock()
        mock_wfs.contents = {"ns:cities": MagicMock(), "ns:roads": MagicMock()}
        mock_wfs_class.return_value = mock_wfs

        with pytest.raises(LayerNotFoundError) as exc_info:
            get_layer_info("http://mock.wfs.server/wfs", "nonexistent:data")

        assert exc_info.value.typename == "nonexistent:data"
        assert "ns:cities" in exc_info.value.available
        assert "ns:roads" in exc_info.value.available


# Helper Data Structures for Tests
# =============================================================================


class MockLayerInfo:
    """Mock WFSLayerInfo for unit testing pure logic functions."""

    def __init__(
        self,
        typename: str = "test:cities",
        title: str | None = "Cities",
        crs_list: list[str] | None = None,
        default_crs: str | None = "urn:ogc:def:crs:EPSG::4326",
        bbox: tuple[float, float, float, float] | None = (-180.0, -90.0, 180.0, 90.0),
        geometry_column: str = "geometry",
    ):
        self.typename = typename
        self.title = title
        self.crs_list = crs_list or ["urn:ogc:def:crs:EPSG::4326", "urn:ogc:def:crs:EPSG::3857"]
        self.default_crs = default_crs
        self.bbox = bbox
        self.geometry_column = geometry_column


class MockCapabilities:
    """Mock WFS capabilities for unit testing."""

    def __init__(
        self,
        supports_bbox: bool = True,
        version: str = "1.1.0",
        max_features: int | None = None,
    ):
        self.supports_bbox = supports_bbox
        self.version = version
        self.max_features = max_features


# =============================================================================
# Unit Tests - Bbox Strategy
# =============================================================================


class TestBboxStrategy:
    """Test _determine_bbox_strategy() pure logic.

    This function decides whether to use server-side or local bbox filtering.
    Unlike BigQuery, WFS doesn't expose row counts easily, so auto mode
    defaults to server-side filtering (conservative for remote services).
    """

    def _create_mock_layer_info(self):
        """Create a mock WFSLayerInfo for testing."""
        return WFSLayerInfo(
            typename="test:cities",
            title="Cities",
            crs_list=["EPSG:4326", "EPSG:3857"],
            default_crs="EPSG:4326",
            bbox=(-180.0, -90.0, 180.0, 90.0),
            geometry_column="geometry",
            available_formats=["application/json"],
        )

    @pytest.mark.parametrize(
        "bbox_mode,expected",
        [
            ("server", True),
            ("local", False),
        ],
    )
    def test_explicit_mode_respected(self, bbox_mode, expected):
        """Explicit server/local mode bypasses auto-detection logic."""
        layer_info = self._create_mock_layer_info()
        result = _determine_bbox_strategy(bbox_mode, layer_info)
        assert result is expected

    def test_auto_mode_defaults_server_for_wfs(self):
        """Auto mode defaults to server-side for WFS (conservative choice).

        Unlike BigQuery, WFS servers don't easily expose row counts.
        Server-side filtering is safer for remote services to avoid
        downloading large datasets unnecessarily.
        """
        layer_info = self._create_mock_layer_info()
        result = _determine_bbox_strategy("auto", layer_info)
        assert result is True  # WFS auto mode defaults to server-side

    def test_auto_mode_still_uses_server_with_different_layer_info(self):
        """Auto mode uses server-side regardless of layer_info (reserved for future)."""
        # layer_info is reserved for future use (e.g., checking server capabilities)
        layer_info = WFSLayerInfo(
            typename="test:layer",
            title=None,
            crs_list=[],
            default_crs=None,
            bbox=None,
            geometry_column="geometry",
            available_formats=[],
        )
        result = _determine_bbox_strategy("auto", layer_info)
        assert result is True  # Still defaults to server-side


# =============================================================================
# Unit Tests - Bbox Filter Construction
# =============================================================================


class TestBboxFilters:
    """Test bbox filter construction functions.

    Tests both:
    - WFS bbox parameter string for server-side filtering (_build_bbox_param)
    - DuckDB SQL expression for local filtering (_build_local_bbox_filter)
    """

    @pytest.mark.parametrize(
        "bbox,crs,version,expected_param",
        [
            # WFS 1.0.0: xmin,ymin,xmax,ymax (no CRS)
            ((-122.5, 37.5, -122.0, 38.0), "EPSG:4326", "1.0.0", "-122.5,37.5,-122.0,38.0"),
            # WFS 1.1.0: xmin,ymin,xmax,ymax,crs
            (
                (-122.5, 37.5, -122.0, 38.0),
                "EPSG:4326",
                "1.1.0",
                "-122.5,37.5,-122.0,38.0,EPSG:4326",
            ),
            # Integer coordinates
            ((-180, -90, 180, 90), "EPSG:4326", "1.1.0", "-180,-90,180,90,EPSG:4326"),
        ],
    )
    def test_server_side_bbox_parameter(self, bbox, crs, version, expected_param):
        """Server-side filtering uses WFS bbox parameter format."""
        result = _build_bbox_param(bbox, crs, version)
        assert result == expected_param

    @pytest.mark.parametrize(
        "bbox,geometry_column",
        [
            ((-122.5, 37.5, -122.0, 38.0), "geometry"),
            ((-122.5, 37.5, -122.0, 38.0), "the_geom"),
            ((-122.5, 37.5, -122.0, 38.0), "geom"),
        ],
    )
    def test_local_bbox_duckdb_filter(self, bbox, geometry_column):
        """Local filtering uses DuckDB ST_Intersects expression."""
        sql = _build_local_bbox_filter(bbox, geometry_column)
        assert f'"{geometry_column}"' in sql
        assert "ST_Intersects" in sql
        assert "ST_GeomFromText" in sql
        assert "POLYGON" in sql

    def test_local_bbox_polygon_is_closed_ring(self):
        """DuckDB POLYGON must be a closed ring (first point == last point)."""
        bbox = (-122.5, 37.5, -122.0, 38.0)
        sql = _build_local_bbox_filter(bbox, "geometry")
        # The polygon should start and end at the same point
        assert "-122.5 37.5" in sql  # First point
        # Count occurrences - should appear twice (start and end)
        assert sql.count("-122.5 37.5") == 2

    def test_bbox_with_crs_suffix(self):
        """WFS 1.1.0 bbox always uses EPSG:4326 regardless of output CRS.

        Many WFS servers (especially GeoServer) only accept bbox in WGS84,
        so we always send bbox with EPSG:4326 suffix regardless of output CRS.
        """
        bbox = (-122.5, 37.5, -122.0, 38.0)
        crs = "urn:ogc:def:crs:EPSG::4326"  # Output CRS (ignored for bbox)
        result = _build_bbox_param(bbox, crs, "1.1.0")
        # Bbox should always end with EPSG:4326, not the output CRS
        assert result.endswith("EPSG:4326")
        assert result == "-122.5,37.5,-122.0,38.0,EPSG:4326"

    def test_invalid_geometry_column_rejected(self):
        """Invalid geometry column names with SQL injection characters are rejected."""
        bbox = (-122.5, 37.5, -122.0, 38.0)
        with pytest.raises(WFSError, match="Invalid geometry column name"):
            _build_local_bbox_filter(bbox, 'geom"; DROP TABLE --')


# =============================================================================
# Unit Tests - Sort By Parameter (Issue #488)
# =============================================================================


class TestSortByParameter:
    """Test sortBy parameter for stable pagination on PK-less layers.

    GeoServer requires sortBy for pagination on layers without a primary key.
    """

    def test_build_wfs_url_includes_sortby_when_provided(self):
        """sortBy parameter is included in URL when specified."""
        url = _build_wfs_url(
            "http://example.com/wfs",
            "layer",
            version="2.0.0",
            max_features=1000,
            start_index=10000,
            sort_by="gid",
        )
        assert "sortBy=gid" in url

    def test_build_wfs_url_no_sortby_when_not_provided(self):
        """sortBy parameter is omitted when not specified."""
        url = _build_wfs_url(
            "http://example.com/wfs",
            "layer",
            version="2.0.0",
            max_features=1000,
            start_index=10000,
        )
        assert "sortBy" not in url

    def test_build_wfs_url_no_sortby_for_wfs_1_0(self):
        """sortBy is not included for WFS 1.0.0 (not supported)."""
        url = _build_wfs_url(
            "http://example.com/wfs",
            "layer",
            version="1.0.0",
            max_features=1000,
            sort_by="gid",
        )
        assert "sortBy" not in url

    def test_detect_sortable_attribute_picks_first_non_geometry(self):
        """_detect_sortable_attribute returns first non-geometry attribute."""
        mock_wfs = MagicMock()
        mock_wfs.get_schema.return_value = {
            "geometry_column": "the_geom",
            "properties": {
                "gid": "int",
                "name": "string",
                "the_geom": "geometry",
            },
        }
        result = _detect_sortable_attribute(mock_wfs, "test:layer")
        assert result == "gid"

    def test_detect_sortable_attribute_skips_geometry_types(self):
        """_detect_sortable_attribute skips columns with geometry types."""
        mock_wfs = MagicMock()
        mock_wfs.get_schema.return_value = {
            "geometry_column": "geom",
            "properties": {
                "geom": "gml:PointPropertyType",
                "boundary": "gml:MultiPolygonPropertyType",
                "id": "int",
            },
        }
        result = _detect_sortable_attribute(mock_wfs, "test:layer")
        assert result == "id"

    def test_detect_sortable_attribute_returns_none_when_no_properties(self):
        """_detect_sortable_attribute returns None if no properties."""
        mock_wfs = MagicMock()
        mock_wfs.get_schema.return_value = {"geometry_column": "geom"}
        result = _detect_sortable_attribute(mock_wfs, "test:layer")
        assert result is None

    def test_detect_sortable_attribute_handles_exception(self):
        """_detect_sortable_attribute returns None on schema fetch error."""
        mock_wfs = MagicMock()
        mock_wfs.get_schema.side_effect = Exception("Schema unavailable")
        result = _detect_sortable_attribute(mock_wfs, "test:layer")
        assert result is None


# =============================================================================
# Unit Tests - Output Format Detection
# =============================================================================


class TestFormatDetection:
    """Test _detect_best_output_format() format preference logic.

    GeoJSON is preferred for speed, with fallbacks to GML variants.
    """

    @pytest.mark.parametrize(
        "available_formats,expected_format",
        [
            # GeoJSON variants (preferred)
            (["application/json", "text/xml; subtype=gml/3.1.1"], "application/json"),
            (["json", "gml3"], "json"),
            (["geojson", "gml2"], "geojson"),
            (["application/geo+json", "application/xml"], "application/geo+json"),
            # GML3 when no JSON
            (
                ["text/xml; subtype=gml/3.1.1", "text/xml; subtype=gml/2.1.2"],
                "text/xml; subtype=gml/3.1.1",
            ),
            (["gml3", "gml2"], "gml3"),
            # GML2 as last resort
            (["text/xml; subtype=gml/2.1.2"], "text/xml; subtype=gml/2.1.2"),
            (["gml2"], "gml2"),
            # Media-type GML variants that differ only in spacing/version from
            # the canonical spellings (NextGIS, deegree). Issue #818.
            (
                ["application/gml+xml;version=3.2"],
                "application/gml+xml;version=3.2",
            ),
            (
                ["application/gml+xml; version=3.2"],
                "application/gml+xml; version=3.2",
            ),
            (
                ["Application/GML+XML;Version=3.2"],
                "Application/GML+XML;Version=3.2",
            ),
            (
                ["text/xml;subtype=gml/3.1.1"],
                "text/xml;subtype=gml/3.1.1",
            ),
            # Mixed-case GeoJSON still wins over GML
            (
                ["Application/GML+XML;version=3.2", "Application/JSON"],
                "Application/JSON",
            ),
            # Unknown format - nothing we can parse, so request nothing
            (["application/x-custom", "unknown/type"], None),
        ],
    )
    def test_format_preference_order(self, available_formats, expected_format):
        """Formats are selected in order: GeoJSON > GML3 > GML2 > nothing."""
        result = _detect_best_output_format(available_formats)
        assert result == expected_format

    def test_unparseable_formats_request_nothing(self):
        """Advertising only formats gpio cannot read means asking for nothing.

        Inventing "GML3", or taking the first advertised format, made gpio name
        an outputFormat the server never offered. Returning None lets
        _build_wfs_url omit the parameter so the server answers in its own
        default format (issue #818).
        """
        assert _detect_best_output_format(["SHAPE-ZIP", "csv"]) is None

    def test_empty_formats_keep_the_historical_json_request(self):
        """A server that advertises nothing is still asked for GeoJSON.

        There is no advertisement to honor here, so gpio keeps sending what it
        sent before issue #818 rather than giving up the fast path for every
        service whose capabilities it cannot parse. A server that ignores the
        parameter and answers with GML is parsed on the response content type,
        so a wrong guess is no longer fatal.
        """
        assert _detect_best_output_format([]) == "application/json"

    @pytest.mark.parametrize(
        "format_string,expected_is_json",
        [
            ("application/json", True),
            ("json", True),
            ("geojson", True),
            ("application/geo+json", True),
            ("gml3", False),
        ],
    )
    def test_geojson_detection(self, format_string, expected_is_json):
        """GeoJSON format detection is case-insensitive."""
        # Test both lowercase and uppercase
        formats_lower = [format_string.lower(), "gml2"]
        formats_upper = [format_string.upper(), "gml2"]

        result_lower = _detect_best_output_format(formats_lower)
        result_upper = _detect_best_output_format(formats_upper)

        if expected_is_json:
            # JSON formats should be selected over GML
            assert "json" in result_lower.lower() or "geo" in result_lower.lower()
            assert "json" in result_upper.lower() or "geo" in result_upper.lower()
        else:
            # Non-JSON GML formats - gml3 should be preferred over gml2
            assert result_lower == format_string.lower()

    def test_gml_version_preference(self):
        """Prefer GML 3.x over GML 2.x for better geometry support."""
        formats = ["gml2", "gml3"]
        result = _detect_best_output_format(formats)
        assert result == "gml3"  # GML3 preferred over GML2


class TestOutputFormatNegotiation:
    """_build_wfs_url requests the format the caller negotiated (issue #818).

    Before this, the builder hardcoded ``outputFormat=application/json`` and the
    format detected from GetCapabilities was computed and thrown away, so a
    GML-only server was always asked for GeoJSON.
    """

    # The exact URL gpio sent before issue #818, kept literal so a refactor of
    # the parameter order cannot silently change what servers receive.
    _LEGACY_JSON_URL = (
        "http://example.com/wfs?service=WFS&version=1.1.0&request=GetFeature"
        "&typeName=layer&outputFormat=application%2Fjson&maxFeatures=10"
    )

    def test_requested_format_is_sent(self):
        """A negotiated GML format reaches the URL."""
        url = _build_wfs_url(
            "http://example.com/wfs",
            "layer",
            version="1.1.0",
            max_features=10,
            output_format="application/gml+xml;version=3.2",
        )
        assert "outputFormat=application%2Fgml%2Bxml%3Bversion%3D3.2" in url

    def test_none_omits_the_parameter(self):
        """No negotiated format means no outputFormat: the server picks."""
        url = _build_wfs_url(
            "http://example.com/wfs",
            "layer",
            version="1.1.0",
            max_features=10,
        )
        assert "outputFormat" not in url

    def test_application_json_reproduces_the_legacy_url(self):
        """Passing the old hardcoded value rebuilds the pre-#818 URL byte for byte."""
        url = _build_wfs_url(
            "http://example.com/wfs",
            "layer",
            version="1.1.0",
            max_features=10,
            output_format="application/json",
        )
        assert url == self._LEGACY_JSON_URL


# WFS 2.0 GetCapabilities that spells the parameter "OutputFormat" (capital O),
# as NextGIS and deegree do. The WFS 2.0 spec spells it "outputFormat", but
# OWSLib keys op.parameters by whatever the XML said. Issue #818.
#
# The advertised version must match the one _nextgis_wfs requests exactly.
# owslib 0.36 raises CapabilitiesError on a mismatch where 0.35 did not, and
# uv.lock resolves 0.36 only on Python >= 3.12 -- so declaring a realistic
# NextGIS "2.0.2" against a "2.0.0" request passes on 3.10/3.11 and fails on
# 3.12/3.13. The capitalization is what this fixture is for; the version string
# is incidental, so it is pinned to the requested one.
MOCK_CAPABILITIES_UPPERCASE_FORMAT_XML = """<?xml version="1.0"?>
<WFS_Capabilities xmlns="http://www.opengis.net/wfs/2.0"
    xmlns:ows="http://www.opengis.net/ows/1.1"
    xmlns:fes="http://www.opengis.net/fes/2.0"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    version="2.0.0">
  <ows:ServiceIdentification>
    <ows:Title>Mock GML-only WFS</ows:Title>
    <ows:ServiceType>WFS</ows:ServiceType>
    <ows:ServiceTypeVersion>2.0.0</ows:ServiceTypeVersion>
  </ows:ServiceIdentification>
  <ows:OperationsMetadata>
    <ows:Operation name="GetFeature">
      <ows:DCP>
        <ows:HTTP>
          <ows:Get xlink:href="http://mock.wfs.server/wfs?"/>
        </ows:HTTP>
      </ows:DCP>
      <ows:Parameter name="OutputFormat">
        <ows:AllowedValues>
          <ows:Value>application/gml+xml;version=3.2</ows:Value>
        </ows:AllowedValues>
      </ows:Parameter>
    </ows:Operation>
  </ows:OperationsMetadata>
  <FeatureTypeList>
    <FeatureType>
      <Name>Vector_Layer__UK_Cities</Name>
      <Title>Vector Layer (UK Cities)</Title>
      <DefaultCRS>EPSG:3857</DefaultCRS>
      <OtherCRS>EPSG:4326</OtherCRS>
      <ows:WGS84BoundingBox>
        <ows:LowerCorner>-7.333284 50.133721</ows:LowerCorner>
        <ows:UpperCorner>1.300013 60.150035</ows:UpperCorner>
      </ows:WGS84BoundingBox>
    </FeatureType>
  </FeatureTypeList>
</WFS_Capabilities>
"""


class TestCapabilityOutputFormatCase:
    """get_layer_info must find outputFormat however the server capitalizes it.

    This is the root cause of issue #818: the lookup was case-sensitive, so a
    server advertising ``OutputFormat`` reported *no* formats at all, and gpio
    then fell back to demanding GeoJSON from a GML-only service.
    """

    def _nextgis_wfs(self):
        from owslib.wfs import WebFeatureService

        wfs = WebFeatureService(
            url="http://mock.wfs.server/wfs",
            version="2.0.0",
            xml=MOCK_CAPABILITIES_UPPERCASE_FORMAT_XML.encode(),
        )
        # DescribeFeatureType would hit the network; the geometry-column and
        # sortable-attribute probes both tolerate a failure here.
        wfs.get_schema = MagicMock(side_effect=Exception("offline"))
        return wfs

    def test_uppercase_output_format_parameter_is_found(self):
        """``<ows:Parameter name="OutputFormat">`` yields the advertised format."""
        wfs = self._nextgis_wfs()
        with patch.object(wfs_module, "get_wfs_capabilities", return_value=wfs):
            layer_info = get_layer_info(
                "http://mock.wfs.server/wfs", "Vector_Layer__UK_Cities", "2.0.0"
            )
        assert layer_info.available_formats == ["application/gml+xml;version=3.2"]

    def test_detected_format_is_the_advertised_gml(self):
        """The advertised GML format survives detection instead of becoming JSON."""
        wfs = self._nextgis_wfs()
        with patch.object(wfs_module, "get_wfs_capabilities", return_value=wfs):
            layer_info = get_layer_info(
                "http://mock.wfs.server/wfs", "Vector_Layer__UK_Cities", "2.0.0"
            )
        assert (
            _detect_best_output_format(layer_info.available_formats)
            == "application/gml+xml;version=3.2"
        )


# =============================================================================
# Unit Tests - CRS Negotiation
# =============================================================================


class TestCRSNegotiation:
    """Test _negotiate_crs() CRS selection logic.

    Strategy:
    1. If --output-crs specified and supported → use it
    2. Try EPSG:4326 variants (most universal)
    3. Fall back to server default
    """

    def _create_layer_info(self, crs_list, default_crs=None):
        """Helper to create WFSLayerInfo with CRS settings."""
        return WFSLayerInfo(
            typename="test:layer",
            title="Test",
            crs_list=crs_list,
            default_crs=default_crs or (crs_list[0] if crs_list else None),
            bbox=None,
            geometry_column="geometry",
            available_formats=["application/json"],
        )

    @pytest.mark.parametrize(
        "crs_list,output_crs,expected",
        [
            # Explicit output_crs respected when available
            (["EPSG:4326", "EPSG:3857"], "EPSG:3857", "EPSG:3857"),
            (["urn:ogc:def:crs:EPSG::4326", "EPSG:3857"], "EPSG:3857", "EPSG:3857"),
            # EPSG:4326 variants matched when no output_crs
            (["urn:ogc:def:crs:EPSG::4326"], None, "urn:ogc:def:crs:EPSG::4326"),
            (["EPSG:4326", "EPSG:3857"], None, "EPSG:4326"),
            (
                ["http://www.opengis.net/def/crs/EPSG/0/4326"],
                None,
                "http://www.opengis.net/def/crs/EPSG/0/4326",
            ),
        ],
    )
    def test_crs_selection_priority(self, crs_list, output_crs, expected):
        """CRS selection follows priority: explicit > EPSG:4326 > server default."""
        layer_info = self._create_layer_info(crs_list)
        result = _negotiate_crs(layer_info, output_crs)
        assert result == expected

    def test_fallback_to_server_default(self):
        """Fall back to default_crs when EPSG:4326 not available."""
        layer_info = self._create_layer_info(
            crs_list=["EPSG:32610", "EPSG:32611"],  # UTM zones, no WGS84
            default_crs="EPSG:32610",
        )
        result = _negotiate_crs(layer_info, None)
        assert result == "EPSG:32610"

    def test_unsupported_output_crs_falls_back(self):
        """When requested CRS not in supported list, fall back to available."""
        layer_info = self._create_layer_info(crs_list=["EPSG:4326"])
        # Request unsupported CRS - should fall back to EPSG:4326
        result = _negotiate_crs(layer_info, "EPSG:2154")  # French Lambert
        assert result == "EPSG:4326"

    @pytest.mark.parametrize(
        "crs_variant,expected_normalized",
        [
            ("EPSG:4326", "EPSG:4326"),
            ("urn:ogc:def:crs:EPSG::4326", "EPSG:4326"),
            ("http://www.opengis.net/def/crs/EPSG/0/4326", "EPSG:4326"),
            ("EPSG:3857", "EPSG:3857"),
            ("urn:ogc:def:crs:EPSG::3857", "EPSG:3857"),
        ],
    )
    def test_crs_variant_normalization(self, crs_variant, expected_normalized):
        """Different CRS URI formats should normalize to same EPSG code."""
        result = _normalize_crs(crs_variant)
        assert result == expected_normalized

    def test_empty_crs_list_uses_default(self):
        """Use default_crs when crs_list is empty."""
        layer_info = self._create_layer_info(crs_list=[], default_crs="EPSG:4326")
        result = _negotiate_crs(layer_info, None)
        assert result == "EPSG:4326"

    def test_no_crs_available_returns_4326(self):
        """When no CRS info at all, default to EPSG:4326."""
        layer_info = self._create_layer_info(crs_list=[], default_crs=None)
        result = _negotiate_crs(layer_info, None)
        assert result == "EPSG:4326"


# =============================================================================
# Unit Tests - Namespace Resolution
# =============================================================================


class TestNamespaceResolution:
    """Test typename namespace matching and sanitization logic."""

    @pytest.mark.parametrize(
        "column_name,should_pass",
        [
            ("geometry", True),
            ("the_geom", True),
            ("geom123", True),
            ("_private_geom", True),
            ('geom"injection', False),
            ("geom;drop", False),
            ("geom--comment", False),
        ],
    )
    def test_validate_identifier(self, column_name, should_pass):
        """Identifier validation catches SQL injection attempts."""
        if should_pass:
            result = _validate_identifier(column_name)
            assert result == column_name
        else:
            with pytest.raises(WFSError, match="Invalid geometry column name"):
                _validate_identifier(column_name)


# =============================================================================
# Integration Tests (require network)
# =============================================================================


@pytest.mark.network
@pytest.mark.slow
class TestWFSIntegration:
    """Integration tests against real WFS services."""

    # Transport for Cairo GeoServer (known to support GeoJSON)
    WFS_URL = "https://data.transportforcairo.com/geoserver/geonode/ows"
    TYPENAME = "geonode:cairo_od_stats"

    @pytest.mark.xfail(
        reason="External WFS service (transportforcairo.com) unreliable",
        raises=WFSError,
        strict=False,
    )
    def test_list_available_layers(self):
        """Test listing layers from real WFS."""

        # Should not raise - just verify it runs
        layers = list_available_layers(self.WFS_URL)
        assert len(layers) > 0

    @pytest.mark.xfail(
        reason="External WFS service (transportforcairo.com) unreliable",
        raises=WFSError,
        strict=False,
    )
    def test_extract_with_limit(self, tmp_path):
        """Test extracting features with limit."""
        from geoparquet_io.core.wfs import convert_wfs_to_geoparquet

        output = tmp_path / "cairo_test.parquet"
        convert_wfs_to_geoparquet(
            self.WFS_URL,
            self.TYPENAME,
            str(output),
            limit=10,
            skip_hilbert=True,
            skip_bbox=True,
        )
        import pyarrow.parquet as pq

        table = pq.read_table(output)
        assert table.num_rows <= 10
        assert "geometry" in table.column_names

    @pytest.mark.xfail(
        reason="External WFS service (transportforcairo.com) unreliable",
        raises=WFSError,
        strict=False,
    )
    def test_extract_with_bbox(self, tmp_path):
        """Test bbox filtering (Cairo region)."""
        from geoparquet_io.core.wfs import convert_wfs_to_geoparquet

        output = tmp_path / "bbox_test.parquet"
        # Cairo bbox (roughly)
        convert_wfs_to_geoparquet(
            self.WFS_URL,
            self.TYPENAME,
            str(output),
            bbox=(31.2, 29.9, 31.3, 30.0),
            bbox_mode="server",
            limit=5,
            skip_hilbert=True,
            skip_bbox=True,
        )
        import pyarrow.parquet as pq

        table = pq.read_table(output)
        assert table.num_rows >= 0  # May be 0 if no data in bbox

    @pytest.mark.xfail(
        reason="External WFS service (transportforcairo.com) unreliable",
        raises=WFSError,
        strict=False,
    )
    def test_python_api(self):
        """Test Python API."""
        from geoparquet_io.api import Table

        table = Table.from_wfs(
            self.WFS_URL,
            self.TYPENAME,
            limit=5,
        )
        assert table.num_rows <= 5


# =============================================================================
# CLI Ergonomics Tests
# =============================================================================


class TestWFSCLIErgonomics:
    """Test CLI option naming consistency with ArcGIS."""

    def test_cli_uses_workers_option(self):
        """WFS CLI should use --workers like ArcGIS, not --max-workers."""
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        # Test that --workers is recognized
        result = runner.invoke(cli, ["extract", "wfs", "--help"])
        assert result.exit_code == 0
        # Should have --workers option
        assert "--workers" in result.output
        # Should NOT have --max-workers option
        assert "--max-workers" not in result.output

    def test_workers_capped_at_10(self):
        """Workers should be capped at 10 in CLI validation."""
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["extract", "wfs", "--help"])
        # Should have --workers option with max of 10
        assert "--workers" in result.output
        # Click displays IntRange as [1<=x<=10]
        assert "[1<=x<=10]" in result.output


# =============================================================================
# DuckDB-Native WFS Fetch Tests
# =============================================================================


@pytest.mark.network
@pytest.mark.slow
class TestDuckDBNativeWFS:
    """Test DuckDB-native WFS fetching using httpfs and read_json_auto."""

    # Use Transport for Cairo GeoServer (known to support GeoJSON)
    WFS_URL = (
        "https://data.transportforcairo.com/geoserver/geonode/ows?"
        "service=WFS&version=1.1.0&request=GetFeature"
        "&typeName=geonode:cairo_od_stats"
        "&outputFormat=application/json"
        "&maxFeatures=10"
    )

    @pytest.mark.network
    @pytest.mark.xfail(reason="requires external WFS endpoint, flaky in CI", strict=False)
    def test_fetch_wfs_page_returns_arrow_table(self):
        """DuckDB-native fetch should return an Arrow table with geometry."""
        from geoparquet_io.core.wfs import _fetch_wfs_page

        table = _fetch_wfs_page(self.WFS_URL)

        # Should return Arrow table
        import pyarrow as pa

        assert isinstance(table, pa.Table)
        # Should have geometry column
        assert "geometry" in table.column_names
        # Should have some rows
        assert table.num_rows > 0
        assert table.num_rows <= 10

    @pytest.mark.network
    @pytest.mark.xfail(reason="requires external WFS endpoint, flaky in CI", strict=False)
    def test_fetch_wfs_page_geometry_is_wkb(self):
        """Geometry should be WKB binary format."""
        from geoparquet_io.core.wfs import _fetch_wfs_page

        table = _fetch_wfs_page(self.WFS_URL)

        # Geometry should be binary (WKB)
        import pyarrow as pa

        geom_type = table.schema.field("geometry").type
        assert pa.types.is_binary(geom_type) or pa.types.is_large_binary(geom_type)


@pytest.mark.network
@pytest.mark.slow
class TestGMLOnlyServerEndToEnd:
    """End-to-end extraction from a real GML-only WFS (issue #818).

    NextGIS advertises only ``application/gml+xml;version=3.2`` and spells the
    capabilities parameter ``OutputFormat``. Before the fix gpio saw no formats,
    demanded GeoJSON, and rejected the GML the server returned.
    """

    WFS_URL = "https://demo.nextgis.com/api/resource/9748/wfs"
    TYPENAME = "Vector_Layer__UK_Cities"

    @pytest.mark.xfail(reason="external demo service, may be unavailable", strict=False)
    def test_extract_from_gml_only_server(self, tmp_path):
        """A GML-only server yields a readable GeoParquet file."""
        import pyarrow.parquet as pq

        from geoparquet_io.core.wfs import convert_wfs_to_geoparquet

        output = tmp_path / "uk_cities.parquet"
        convert_wfs_to_geoparquet(
            self.WFS_URL,
            self.TYPENAME,
            str(output),
            version="2.0.0",
            limit=5,
        )

        assert output.exists()
        table = pq.read_table(output)
        assert table.num_rows > 0
        assert "geometry" in table.column_names


def _mock_wfs_http_response(
    body: bytes,
    content_type: str = "application/json",
    status_code: int = 200,
    sent_headers: dict | None = None,
):
    """Patch httpx.Client.stream to return a canned response.

    Shared factory for the content-type and properties-handling tests, which
    previously each carried a near-identical inline mock class (issue #666
    item 7). The response supports both ``read()`` (error-page validation path)
    and ``iter_bytes()`` (streaming JSON path).

    Pass ``sent_headers`` to have the request headers gpio would have put on the
    wire copied into it, for tests that assert on what was requested rather than
    on what came back.
    """
    import httpx

    # Bound outside the class body: a class body does not see an enclosing
    # function's variable when the same name is assigned inside it.
    response_status = status_code

    def mock_stream(*args, **kwargs):
        if sent_headers is not None:
            sent_headers.update(kwargs.get("headers") or {})

        class MockResponse:
            status_code = response_status
            headers = {"content-type": content_type}

            def raise_for_status(self):
                pass

            def read(self):
                return body

            def iter_bytes(self, chunk_size=None):
                yield body

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        return MockResponse()

    return patch.object(httpx.Client, "stream", mock_stream)


_EMPTY_FEATURE_COLLECTION = b'{"type": "FeatureCollection", "features": []}'


def _billion_laughs_gml(levels: int = 10) -> bytes:
    """A FeatureCollection whose internal DTD expands to ``10**levels`` entities.

    The classic entity-expansion bomb: about a kilobyte of markup that a parser
    with no expansion limit turns into gigabytes. It wears a FeatureCollection
    root on purpose, so nothing but the DTD guard itself can turn it away.
    """
    entities = ['<!ENTITY lol0 "lol">']
    entities += [f'<!ENTITY lol{i} "{f"&lol{i - 1};" * 10}">' for i in range(1, levels + 1)]
    return (
        '<?xml version="1.0"?>\n'
        "<!DOCTYPE wfs:FeatureCollection [\n" + "\n".join(entities) + "\n]>\n"
        '<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0">'
        f"&lol{levels};</wfs:FeatureCollection>"
    ).encode()


class TestContentTypeValidation:
    """Test content-type validation catches error pages returned with 200 OK."""

    @pytest.mark.parametrize(
        ("content_type", "body", "error_match"),
        [
            pytest.param(
                "text/html; charset=utf-8",
                b"<html><body>Error: Service unavailable</body></html>",
                "Expected a feature collection.*text/html",
                id="rejects-html",
            ),
            pytest.param(
                "application/xml",
                b"<ows:ExceptionReport>Service error</ows:ExceptionReport>",
                "Expected a feature collection.*application/xml",
                id="rejects-xml",
            ),
            pytest.param(
                "text/xml",
                b'<?xml version="1.0"?><ows:ExceptionReport '
                b'xmlns:ows="http://www.opengis.net/ows/1.1" version="2.0.0">'
                b'<ows:Exception exceptionCode="InvalidParameterValue" '
                b'locator="outputFormat"><ows:ExceptionText>Unsupported '
                b"outputFormat</ows:ExceptionText></ows:Exception>"
                b"</ows:ExceptionReport>",
                "Expected a feature collection.*text/xml",
                id="rejects-exception-report",
            ),
            pytest.param(
                "text/plain",
                b"Error: Invalid request parameters",
                "Expected a feature collection.*text/plain",
                id="rejects-text-plain",
            ),
            pytest.param(
                "application/json", _EMPTY_FEATURE_COLLECTION, None, id="accepts-application-json"
            ),
            pytest.param(
                "text/javascript",
                _EMPTY_FEATURE_COLLECTION,
                None,
                id="accepts-text-javascript",
            ),
            # A GML FeatureCollection at 200 is a valid answer, not an error
            # page: WFS 1.x servers (and NextGIS at any version) return it under
            # an XML content type. Issue #818.
            pytest.param(
                "text/xml; charset=utf-8",
                MOCK_GML_RESPONSE.encode(),
                None,
                id="accepts-gml-feature-collection",
            ),
            pytest.param(
                "application/gml+xml; version=3.2",
                MOCK_GML_RESPONSE.encode(),
                None,
                id="accepts-gml-media-type",
            ),
        ],
    )
    def test_content_type_validation(self, content_type, body, error_match):
        """Non-parseable content types raise WFSError; JSON and GML parse normally."""
        from geoparquet_io.core.wfs import WFSError, _fetch_wfs_page

        with _mock_wfs_http_response(body, content_type):
            if error_match is not None:
                with pytest.raises(WFSError, match=error_match):
                    _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")
            else:
                # Should not raise; a parseable body yields a table
                _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")


class TestGMLResponseParsing:
    """_fetch_wfs_page parses GML bodies, not just GeoJSON (issue #818).

    NextGIS answers every GetFeature with GML at HTTP 200 regardless of the
    requested outputFormat, so dispatching on the *response* content type is
    what makes those servers usable at all.
    """

    def _fetch_gml(self, content_type="text/xml; charset=utf-8"):
        from geoparquet_io.core.wfs import _fetch_wfs_page

        with _mock_wfs_http_response(MOCK_GML_RESPONSE.encode(), content_type):
            return _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")

    def test_parses_features_with_correct_axis_order(self):
        """The urn srsName in MOCK_GML_RESPONSE means lat/lon in the file, lon/lat out."""
        import duckdb

        table = self._fetch_gml()
        assert table.num_rows == 1

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        con.register("features", table)
        wkt = con.execute("SELECT ST_AsText(ST_GeomFromWKB(geometry)) FROM features").fetchone()[0]
        con.close()
        assert wkt == "POINT (-122.4194 37.7749)"

    def test_keeps_attribute_columns(self):
        """Feature attributes survive the GML path, like they do for GeoJSON."""
        table = self._fetch_gml()
        assert "geometry" in table.column_names
        assert "name" in table.column_names
        assert table.column("name").to_pylist() == ["San Francisco"]

    def test_geometry_column_is_wkb(self):
        """The geometry column is WKB binary, matching the GeoJSON path."""
        import pyarrow as pa

        table = self._fetch_gml()
        geom_type = table.schema.field("geometry").type
        assert pa.types.is_binary(geom_type) or pa.types.is_large_binary(geom_type)

    def test_records_server_declared_crs(self):
        """The CRS GDAL read off the GML is carried up like the GeoJSON crs member."""
        from geoparquet_io.core.wfs import _SERVER_CRS_METADATA_KEY

        table = self._fetch_gml()
        assert table.schema.metadata.get(_SERVER_CRS_METADATA_KEY) == b"EPSG:4326"

    def test_leaves_no_temp_files_behind(self, tmp_path, monkeypatch):
        """GDAL writes a .gfs sidecar for schema-less GML; nothing may survive.

        The GeoJSON path used a NamedTemporaryFile and unlinked exactly that one
        path, which would strand the sidecar. The GML path must use a temporary
        *directory* so every file GDAL creates is removed.

        The assertion is on the ``gpio-wfs-*`` *directory*, not on file suffixes
        directly under the temp root: the response body and its sidecar live one
        level down, so a suffix filter over ``gettempdir()`` sees nothing either
        way and stays green even when the directory is stranded (issue #838).

        Redirecting ``tempfile.tempdir`` at a per-test directory keeps this from
        reading other xdist workers' in-flight fetches as leaks.
        """
        import tempfile

        monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
        self._fetch_gml()

        leaked = sorted(p.name for p in tmp_path.glob("gpio-wfs-*"))
        assert not leaked, f"GML parse leaked temp directories: {leaked}"

    def test_extract_fid_uses_gml_id(self):
        """Tiled fetches dedupe on gml:id, the GML analogue of a GeoJSON feature id."""
        from geoparquet_io.core.wfs import _fetch_wfs_page

        with _mock_wfs_http_response(MOCK_GML_RESPONSE.encode(), "text/xml"):
            table = _fetch_wfs_page("http://mock.wfs/wfs?service=WFS", extract_fid=True)

        assert table.column("_wfs_fid").to_pylist() == ["cities.1"]
        # The id is consumed as the dedup key, not left behind as an attribute.
        assert "gml_id" not in table.column_names

    @pytest.mark.parametrize(
        ("body", "content_type"),
        [
            pytest.param(
                MOCK_EMPTY_GML_WFS20,
                "application/gml+xml; version=3.2",
                id="wfs20-self-closing",
            ),
            pytest.param(
                MOCK_EMPTY_GML_WFS11,
                "text/xml; subtype=gml/3.1.1",
                id="wfs11-null-boundedby",
            ),
            pytest.param(
                MOCK_EMPTY_GML_FEATUREMEMBERS,
                "text/xml; subtype=gml/3.1.1",
                id="wfs11-empty-featuremembers",
            ),
        ],
    )
    def test_empty_collection_returns_empty_table(self, body, content_type):
        """A zero-feature GML FeatureCollection is a valid answer, not an error.

        GDAL cannot open one at all, but it is what a server sends for an empty
        bbox, so the GML path must return the same empty geometry-only table
        the JSON path returns for zero features -- pagination and tiled fetches
        rely on that to stop cleanly.
        """
        import pyarrow as pa

        from geoparquet_io.core.wfs import _fetch_wfs_page

        with _mock_wfs_http_response(body.encode(), content_type):
            table = _fetch_wfs_page("http://mock.wfs/wfs?service=WFS", extract_fid=True)

        assert table.num_rows == 0
        assert table.column_names == ["geometry"]
        assert pa.types.is_binary(table.schema.field("geometry").type)

    @pytest.mark.parametrize(
        "body",
        [
            pytest.param(MOCK_EMPTY_GML_WFS20, id="wfs20-self-closing"),
            pytest.param(MOCK_EMPTY_GML_WFS11, id="wfs11-null-boundedby"),
            pytest.param(MOCK_EMPTY_GML_FEATUREMEMBERS, id="wfs11-empty-featuremembers"),
        ],
    )
    def test_empty_collection_never_reaches_gdal(self, body):
        """An empty collection must be answered without ever calling ST_Read.

        On Linux builds of DuckDB spatial, ST_Read *segfaults* on a zero-layer
        GML dataset instead of raising IOException, so mapping the exception is
        not enough -- the body has to be recognized as empty before GDAL sees
        it.
        """
        from geoparquet_io.core import wfs as wfs_module

        def _tripwire(*args, **kwargs):
            raise AssertionError("empty GML body reached ST_Read")

        with (
            _mock_wfs_http_response(body.encode(), "text/xml"),
            patch.object(wfs_module, "_gml_geometry_column", side_effect=_tripwire),
        ):
            table = wfs_module._fetch_wfs_page("http://mock.wfs/wfs?service=WFS")

        assert table.num_rows == 0

    @pytest.mark.parametrize(
        ("body", "expected"),
        [
            pytest.param(MOCK_EMPTY_GML_WFS20.encode(), True, id="empty-wfs20"),
            pytest.param(MOCK_EMPTY_GML_WFS11.encode(), True, id="empty-wfs11"),
            pytest.param(MOCK_EMPTY_GML_FEATUREMEMBERS.encode(), True, id="empty-featuremembers"),
            pytest.param(MOCK_GML_RESPONSE.encode(), False, id="collection-with-features"),
            pytest.param(
                b'<?xml version="1.0"?><ows:ExceptionReport '
                b'xmlns:ows="http://www.opengis.net/ows/1.1"/>',
                False,
                id="exception-report",
            ),
            pytest.param(b"<html><body>Error</body></html>", False, id="html-error-page"),
            pytest.param(b"not xml at all", False, id="not-xml"),
            # A WFS FeatureCollection never carries a DTD, so any body that
            # declares one is refused before it reaches the parser (issue
            # #840). The fetch path raises a WFSError for these instead of
            # calling this sniff at all; False here keeps the sniff itself
            # safe for any caller.
            pytest.param(
                b'<!DOCTYPE wfs:FeatureCollection [<!ENTITY x "y">]>'
                b'<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0"/>',
                False,
                id="doctype-with-entity",
            ),
            pytest.param(
                b'<!doctype wfs:FeatureCollection [<!entity x "y">]>'
                b'<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0"/>',
                False,
                id="doctype-lowercase",
            ),
            pytest.param(
                b'<!DoCtYpE wfs:FeatureCollection [<!EnTiTy x "y">]>'
                b'<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0"/>',
                False,
                id="doctype-mixed-case",
            ),
            pytest.param(
                b'<!ENTITY x SYSTEM "file:///etc/passwd">'
                b'<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0"/>',
                False,
                id="entity-without-doctype",
            ),
        ],
    )
    def test_is_empty_gml_collection(self, body, expected):
        """Only a featureless FeatureCollection counts as empty; everything else
        goes to GDAL, which keeps the error-page WFSError for broken bodies."""
        from geoparquet_io.core.wfs import _is_empty_gml_collection

        assert _is_empty_gml_collection(body) is expected

    def test_entity_expansion_bomb_is_refused_immediately(self):
        """A billion-laughs body is rejected on the DTD, not parsed (issue #840).

        Without the guard this hands ~1KB of markup to expat and asks it to
        build 10**10 entity expansions. A libexpat with amplification protection
        (>= 2.4) raises quickly, so the guard is what makes the outcome the same
        on a build that has the protection compiled out: refused in constant
        time instead of hanging or exhausting memory. The 1MB body cap does not
        help here -- expansion is what is unbounded, not size.
        """
        from geoparquet_io.core.wfs import _is_empty_gml_collection

        start = time.perf_counter()
        assert _is_empty_gml_collection(_billion_laughs_gml()) is False
        assert time.perf_counter() - start < 1.0, "the DTD was handed to the parser"

    def test_utf16_bomb_never_reaches_the_parser(self):
        """A UTF-16 body must be refused before the ASCII DTD scan can miss it.

        expat auto-detects UTF-16, so an entity bomb encoded that way carries no
        ASCII ``<!doctype``/``<!entity`` bytes for the marker scan to find: it
        sailed past the guard, was parsed, had its entities expanded, and --
        with only text content inside the root -- was even reported as an empty
        collection. The small expansion here stays under libexpat's
        amplification threshold on purpose: on an unfixed build it parses
        cleanly and returns True, so this asserts the refusal, not a parser
        error.
        """
        from geoparquet_io.core.wfs import _is_empty_gml_collection

        body = _billion_laughs_gml(levels=3).decode().encode("utf-16")
        start = time.perf_counter()
        assert _is_empty_gml_collection(body) is False
        assert time.perf_counter() - start < 1.0, "the UTF-16 body was handed to the parser"

    @pytest.mark.parametrize(
        ("body", "expected"),
        [
            pytest.param(b"\xfe\xff\x00<\x00a\x00>", True, id="utf16-be-bom"),
            pytest.param(b"\xff\xfe<\x00a\x00>\x00", True, id="utf16-le-bom"),
            pytest.param(b"\x00\x00\xfe\xff", True, id="utf32-be-bom"),
            pytest.param(b"\xff\xfe\x00\x00", True, id="utf32-le-bom"),
            pytest.param(
                '<?xml version="1.0"?><a/>'.encode("utf-16-le"),
                True,
                id="bomless-utf16-nul-in-prolog",
            ),
            pytest.param(
                b'<?xml version="1.0"?><!DOCTYPE a><a/>',
                True,
                id="doctype",
            ),
            pytest.param(MOCK_EMPTY_GML_WFS20.encode(), False, id="utf8-empty-collection"),
            pytest.param(MOCK_GML_RESPONSE.encode(), False, id="utf8-collection"),
        ],
    )
    def test_xml_body_refusal(self, body, expected):
        """DTDs and UTF-16/UTF-32 bodies are refused; plain UTF-8 GML is not."""
        from geoparquet_io.core.wfs import _xml_body_must_be_refused

        assert _xml_body_must_be_refused(body) is expected

    @pytest.mark.parametrize(
        "body",
        [
            pytest.param(
                b'<?xml version="1.0"?>\n'
                b"<!DOCTYPE wfs:FeatureCollection [<!ELEMENT a EMPTY>]>\n"
                b'<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0"/>',
                id="ascii-doctype-empty-collection",
            ),
            pytest.param(_billion_laughs_gml(), id="ascii-entity-bomb"),
            pytest.param(
                _billion_laughs_gml(levels=3).decode().encode("utf-16"),
                id="utf16-bom-entity-bomb",
            ),
            pytest.param(
                _billion_laughs_gml(levels=3).decode().encode("utf-16-le"),
                id="utf16-bomless-entity-bomb",
            ),
        ],
    )
    def test_refused_xml_raises_wfs_error_without_reaching_gdal(self, body):
        """A DTD or UTF-16 body gets the clean error-page WFSError, never GDAL.

        Two crash routes are closed at once. Falling through to GDAL is not a
        safe default: a DOCTYPE'd *empty* FeatureCollection is a zero-layer GML
        dataset, exactly the shape Linux ST_Read segfaults on instead of
        raising. And a UTF-16 body used to slip past the ASCII DTD scan into
        the sniffer's XML parse, entities and all. No legitimate WFS GetFeature
        response has either shape (GML uses XML Schema, servers send UTF-8), so
        both are refused with the standard error-page WFSError and its body
        preview -- the tripwire proves ST_Read is never invoked.
        """
        from geoparquet_io.core import wfs as wfs_module

        def _tripwire(*args, **kwargs):
            raise AssertionError("refused XML body reached ST_Read")

        with (
            _mock_wfs_http_response(body, "text/xml"),
            patch.object(wfs_module, "_gml_geometry_column", side_effect=_tripwire),
            pytest.raises(wfs_module.WFSError, match="Expected a feature collection"),
        ):
            wfs_module._fetch_wfs_page("http://mock.wfs/wfs?service=WFS")

    def test_idless_features_get_null_fid(self):
        """Features without gml:id must get a NULL _wfs_fid, not GDAL's ''.

        GDAL synthesizes gml_id = '' (empty string) when a feature has no
        gml:id/fid. Passed through as-is, every id-less feature shares the
        dedup key '' and tile deduplication keeps exactly one of them. NULL
        routes them to the geometry-bytes fallback instead, like the GeoJSON
        path's missing feature.id.
        """
        from geoparquet_io.core.wfs import _fetch_wfs_page

        with _mock_wfs_http_response(MOCK_GML_IDLESS_RESPONSE.encode(), "text/xml"):
            table = _fetch_wfs_page("http://mock.wfs/wfs?service=WFS", extract_fid=True)

        assert table.num_rows == 3
        assert table.column("_wfs_fid").to_pylist() == [None, None, None]
        assert "gml_id" not in table.column_names

    def test_idless_features_survive_tile_dedup(self):
        """Three distinct id-less features stay three rows after deduplication."""
        from geoparquet_io.core.wfs import _deduplicate_tiles, _fetch_wfs_page

        with _mock_wfs_http_response(MOCK_GML_IDLESS_RESPONSE.encode(), "text/xml"):
            table = _fetch_wfs_page("http://mock.wfs/wfs?service=WFS", extract_fid=True)

        result = _deduplicate_tiles(table)
        assert result.num_rows == 3
        assert sorted(result.column("name").to_pylist()) == ["London", "New York", "San Francisco"]

    def test_xml_without_features_reads_as_an_error_page(self):
        """An ows:ExceptionReport is XML but has no geometry, so it is an error.

        The content type alone cannot tell a FeatureCollection from an exception
        report -- servers send both as text/xml -- so the GML parser decides,
        and must produce the same message (with a body preview) the JSON path
        gives for an error page.
        """
        from geoparquet_io.core.wfs import WFSError, _fetch_wfs_page

        report = (
            b'<?xml version="1.0"?><ows:ExceptionReport '
            b'xmlns:ows="http://www.opengis.net/ows/1.1"><ows:Exception '
            b'exceptionCode="NoApplicableCode"/></ows:ExceptionReport>'
        )
        with _mock_wfs_http_response(report, "text/xml; subtype=gml/3.1.1"):
            with pytest.raises(WFSError, match="ExceptionReport"):
                _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")


class TestResponseDispatchHelpers:
    """The small decisions that route a WFS response to a parser (issue #818)."""

    @pytest.mark.parametrize(
        ("content_type", "expected"),
        [
            ("application/json", "json"),
            ("text/javascript", "json"),
            ("text/html; charset=utf-8", "error"),
            ("text/plain", "error"),
            ("text/xml; subtype=gml/3.1.1", "gml"),
            ("application/gml+xml; version=3.2", "gml"),
            # No content type at all, and a type gpio has no opinion about:
            # both stay on the JSON path, which is what servers that send
            # application/octet-stream for GeoJSON have always relied on.
            ("", "json"),
            ("application/octet-stream", "json"),
        ],
    )
    def test_classify_content_type(self, content_type, expected):
        from geoparquet_io.core.wfs import _classify_wfs_content_type

        assert _classify_wfs_content_type(content_type) == expected

    @pytest.mark.parametrize(
        ("output_format", "expected"),
        [
            # Nothing negotiated means outputFormat was deliberately left off
            # the request, so Accept must not name a format either.
            (None, "*/*"),
            ("application/gml+xml; version=3.2", "application/gml+xml; version=3.2"),
            # "GML2" and "gml32" are WFS format tokens, not media types. Echoing
            # one into Accept would earn a 406 from a strict server, so ask for
            # anything instead.
            ("GML2", "*/*"),
            ("gml32", "*/*"),
        ],
    )
    def test_accept_header_for(self, output_format, expected):
        from geoparquet_io.core.wfs import _accept_header_for

        assert _accept_header_for(output_format) == expected

    def test_http_400_surfaces_the_server_message(self):
        """A 400 is reported with the server's own body, not a bare status code."""
        from geoparquet_io.core.wfs import WFSError, _fetch_wfs_page

        with _mock_wfs_http_response(b"Unknown typeName", "text/plain", status_code=400):
            with pytest.raises(WFSError, match="HTTP 400.*Unknown typeName"):
                _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")

    def test_http_400_names_the_startindex_limit(self):
        """The startIndex rejection keeps its own message, which pagination reads."""
        from geoparquet_io.core.wfs import WFSError, _fetch_wfs_page

        body = b"Value of startIndex exceeds the maximum"
        with _mock_wfs_http_response(body, "text/plain", status_code=400):
            with pytest.raises(WFSError, match="startIndex limit"):
                _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")


class TestLookupParameterCaseInsensitive:
    """_lookup_parameter_ci reads capabilities however a server capitalizes them."""

    def test_finds_a_differently_cased_key(self):
        from geoparquet_io.core.wfs import _lookup_parameter_ci

        params = {"OutputFormat": {"values": ["application/gml+xml"]}}
        assert _lookup_parameter_ci(params, "outputFormat") == ["application/gml+xml"]

    @pytest.mark.parametrize(
        ("parameters", "reason"),
        [
            ({}, "no parameters at all"),
            ({"resultType": {"values": ["results"]}}, "no such parameter"),
            ({"outputFormat": {"values": []}}, "parameter declares no values"),
            ({"outputFormat": "application/json"}, "parameter is not a value bag"),
        ],
    )
    def test_returns_none_when_there_is_nothing_to_read(self, parameters, reason):
        """Every shape that is not an allowed-value list reads as "unknown"."""
        from geoparquet_io.core.wfs import _lookup_parameter_ci

        assert _lookup_parameter_ci(parameters, "outputFormat") is None, reason


class TestFormatPrefixMatching:
    """A version suffix gpio has not seen still resolves to its format family."""

    @pytest.mark.parametrize(
        "advertised",
        [
            "application/gml+xml;version=3.2.1",
            "text/xml; subtype=gml/3.1.1; charset=utf-8",
        ],
    )
    def test_unlisted_media_type_parameter_still_matches(self, advertised):
        """An extra ``;parameter`` does not hide a format gpio can read."""
        from geoparquet_io.core.wfs import _detect_best_output_format

        assert _detect_best_output_format([advertised]) == advertised

    @pytest.mark.parametrize(
        "advertised",
        [
            # RFC 7464 JSON text sequences: a stream of records, not one
            # FeatureCollection, so reading it as GeoJSON would fail or -- worse
            # -- silently return one record.
            "application/json-seq",
            "application/geo+json-seq",
            "gml3-alternate",
        ],
    )
    def test_a_longer_name_is_not_the_same_format(self, advertised):
        """Matching stops at a media-type parameter; it is not a bare prefix test."""
        from geoparquet_io.core.wfs import _detect_best_output_format

        assert _detect_best_output_format([advertised]) is None


# h11 refuses to put any of these on the wire, so a format carrying one turns
# into three retries and an opaque "Illegal header value" (issue #838).
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")

# A capabilities document is server-controlled text. This value matches
# ``application/json`` once normalized -- whitespace is dropped for the
# comparison -- so without sanitizing it would be echoed back verbatim.
_HOSTILE_FORMAT = "application/json;\r\nX-Evil: 1"


class TestHostileAdvertisedFormat:
    """A control character in an advertised outputFormat never leaves gpio."""

    def test_negotiated_format_has_no_control_characters(self):
        """The value is still matched, but returned without the control bytes."""
        from geoparquet_io.core.wfs import _detect_best_output_format

        negotiated = _detect_best_output_format([_HOSTILE_FORMAT])

        assert negotiated is not None, "normalized matching must still find the format"
        assert negotiated.startswith("application/json")
        assert not _CONTROL_CHARS.search(negotiated)

    def test_request_still_goes_out_with_a_legal_accept_header(self):
        """Sanitizing must not break the request: it is sent, with a legal header."""
        from geoparquet_io.core.wfs import _accept_header_for, _detect_best_output_format

        negotiated = _detect_best_output_format([_HOSTILE_FORMAT])
        accept = _accept_header_for(negotiated)
        assert not _CONTROL_CHARS.search(accept)

        sent: dict = {}
        with _mock_wfs_http_response(
            _EMPTY_FEATURE_COLLECTION, "application/json", sent_headers=sent
        ):
            table = wfs_module._fetch_wfs_page(
                "http://mock.wfs/wfs?service=WFS", output_format=negotiated
            )

        assert table.num_rows == 0
        assert not _CONTROL_CHARS.search(sent["Accept"])

    def test_sanitizing_covers_the_output_format_parameter_too(self):
        """The negotiated value is also the ``outputFormat=`` query parameter.

        Sanitizing at ``_detect_best_output_format`` -- rather than inside
        ``_accept_header_for`` -- is what keeps the control bytes out of the URL
        as well as out of the header.
        """
        from geoparquet_io.core.wfs import _build_wfs_url, _detect_best_output_format

        negotiated = _detect_best_output_format([_HOSTILE_FORMAT])
        url = _build_wfs_url(
            "http://mock.wfs/wfs", "topp:states", "2.0.0", output_format=negotiated
        )

        assert "outputFormat" in url
        assert not _CONTROL_CHARS.search(url)
        assert "%0D" not in url.upper()


_OMIT_PROPERTIES = object()
"""Sentinel: build a feature with no 'properties' key at all (malformed GeoJSON)."""


def _wfs_point_features(props_list, fids=None):
    """Build GeoJSON point features with the given per-feature properties.

    ``_OMIT_PROPERTIES`` omits the 'properties' member entirely (malformed per
    RFC 7946); any other value (including ``None`` and ``{}``) is used as-is.
    """
    features = []
    for i, props in enumerate(props_list):
        feature = {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [i, i]},
        }
        if props is not _OMIT_PROPERTIES:
            feature["properties"] = props
        if fids is not None:
            feature["id"] = fids[i]
        features.append(feature)
    return features


class TestEmptyProperties:
    """Test handling of empty/null properties in GeoJSON features.

    When WFS features have empty properties ({}) or null properties,
    DuckDB infers MAP(VARCHAR, JSON) or JSON types instead of STRUCT.
    The unnest() function doesn't support these types, so we fall back
    to returning geometry-only tables.

    See: https://github.com/geoparquet/geoparquet-io/issues/441
    """

    @pytest.mark.parametrize(
        ("features", "fetch_kwargs", "exact_columns", "required_columns"),
        [
            pytest.param(
                _wfs_point_features([{}, {}]),
                {},
                ["geometry"],
                [],
                id="empty-properties-geometry-only",
            ),
            pytest.param(
                _wfs_point_features([None, None]),
                {},
                ["geometry"],
                [],
                id="null-properties-geometry-only",
            ),
            pytest.param(
                _wfs_point_features([{}, {}], fids=["line.1", "line.2"]),
                {"extract_fid": True},
                ["_wfs_fid", "geometry"],
                [],
                id="empty-properties-extract-fid",
            ),
            pytest.param(
                _wfs_point_features(
                    [{"name": "A", "population": 100}, {"name": "B", "population": 200}]
                ),
                {},
                None,
                ["geometry", "name", "population"],
                id="normal-properties-unnested",
            ),
            pytest.param(
                _wfs_point_features([{"value": 123}, {"value": "text"}]),
                {},
                None,
                ["geometry", "value"],
                id="heterogeneous-value-types",
            ),
            pytest.param(
                _wfs_point_features([_OMIT_PROPERTIES, _OMIT_PROPERTIES]),
                {},
                ["geometry"],
                [],
                id="missing-properties-key-geometry-only",
            ),
        ],
    )
    def test_properties_handling(self, features, fetch_kwargs, exact_columns, required_columns):
        """Each properties shape yields the expected columns (geometry-only fallback or unnest)."""
        import json

        import pyarrow as pa

        from geoparquet_io.core.wfs import _fetch_wfs_page

        geojson = {"type": "FeatureCollection", "features": features}
        with _mock_wfs_http_response(json.dumps(geojson).encode()):
            result = _fetch_wfs_page("http://mock.wfs/wfs?service=WFS", **fetch_kwargs)

        assert isinstance(result, pa.Table)
        assert result.num_rows == 2
        if exact_columns is not None:
            assert result.column_names == exact_columns
        for column in required_columns:
            assert column in result.column_names


@pytest.mark.usefixtures("offline_wfs_probes")
class TestAutoPageSingleWorker:
    """Test that single-worker mode auto-paginates for large datasets.

    These call ``fetch_all_features_duckdb`` directly, which probes the server's
    startIndex support before paging. That probe drives httpx itself, so mocking
    the page fetcher does not cover it -- the three tests here reached
    ``mock.wfs:443`` on every run. Stubbing the probe to ``None`` reproduces what
    the failed request already returned (``_probe_startindex_limit`` swallows
    ``httpx.HTTPError``), so no assertion changes meaning.
    """

    def test_single_worker_paginates_when_count_exceeds_page_size(self):
        """With max_workers=1 and total > page_size, should paginate sequentially."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import fetch_all_features_duckdb

        # Each page must have page_size rows so adaptive pagination doesn't
        # mistake 1-row results for a server-side maxFeatures cap.
        page1 = pa.table(
            {
                "geometry": pa.array([b"\x01\x02"] * 10000, type=pa.binary()),
                "name": pa.array(["a"] * 10000),
            }
        )
        page2 = pa.table(
            {
                "geometry": pa.array([b"\x01\x03"] * 10000, type=pa.binary()),
                "name": pa.array(["b"] * 10000),
            }
        )

        call_count = 0

        def mock_fetch(url, extract_fid=False, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return page1
            return page2

        with (
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=20000),
            patch("geoparquet_io.core.wfs._fetch_wfs_page", side_effect=mock_fetch),
        ):
            result = fetch_all_features_duckdb(
                "https://mock.wfs/wfs",
                "layer",
                max_workers=1,
                page_size=10000,
            )

        assert call_count == 2
        assert result.num_rows == 20000

    def test_single_worker_no_pagination_when_count_within_page_size(self):
        """With max_workers=1 and total <= page_size, should use single request."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import fetch_all_features_duckdb

        mock_table = pa.table(
            {
                "geometry": pa.array([b"\x01\x02"] * 5000, type=pa.binary()),
                "name": pa.array(["a"] * 5000),
            }
        )

        with (
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=5000),
            patch("geoparquet_io.core.wfs._fetch_wfs_page", return_value=mock_table) as mock_fetch,
        ):
            result = fetch_all_features_duckdb(
                "https://mock.wfs/wfs",
                "layer",
                max_workers=1,
                page_size=10000,
            )

        mock_fetch.assert_called_once()
        assert result.num_rows == 5000

    def test_single_worker_no_pagination_when_count_unknown(self):
        """With max_workers=1 and unknown count, should use single request."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import fetch_all_features_duckdb

        mock_table = pa.table(
            {
                "geometry": pa.array([b"\x01\x02"], type=pa.binary()),
                "name": pa.array(["a"]),
            }
        )

        with (
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=None),
            patch("geoparquet_io.core.wfs._fetch_wfs_page", return_value=mock_table) as mock_fetch,
        ):
            result = fetch_all_features_duckdb(
                "https://mock.wfs/wfs",
                "layer",
                max_workers=1,
                page_size=10000,
            )

        mock_fetch.assert_called_once()
        assert result.num_rows == 1

    def test_single_worker_pagination_respects_max_features(self):
        """Auto-pagination should respect max_features limit."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import fetch_all_features_duckdb

        call_count = 0

        def mock_fetch(url, extract_fid=False, **kwargs):
            nonlocal call_count
            call_count += 1
            n = 10000 if call_count == 1 else 5000
            return pa.table(
                {
                    "geometry": pa.array([b"\x01\x02"] * n, type=pa.binary()),
                    "name": pa.array(["a"] * n),
                }
            )

        with (
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=100000),
            patch("geoparquet_io.core.wfs._fetch_wfs_page", side_effect=mock_fetch),
        ):
            result = fetch_all_features_duckdb(
                "https://mock.wfs/wfs",
                "layer",
                max_workers=1,
                max_features=15000,
                page_size=10000,
            )

        assert call_count == 2
        assert result.num_rows == 15000

    # test_parallel_workers_use_httpx_fetcher lived here. It asserted only
    # `mock_fetch.call_count == 2`. Its replacement asserts the page windows
    # that reached the transport and the rows that came back:
    # tests/test_wfs_transport.py::test_parallel_pagination_covers_every_window
    # (this class stubs the count/startIndex probes offline, so the converted
    # test cannot run inside it).

    def test_startindex_limit_raises_clear_error(self):
        """When server has startIndex limit, should raise with actionable guidance."""
        from geoparquet_io.core.wfs import WFSError, fetch_all_features_duckdb

        with (
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=11000000),
            patch("geoparquet_io.core.wfs._probe_startindex_limit", return_value=50000),
        ):
            with pytest.raises(WFSError, match="startIndex.*50,000"):
                fetch_all_features_duckdb(
                    "https://mock.wfs/wfs",
                    "layer",
                    max_workers=1,
                    page_size=10000,
                )

    @pytest.mark.skipif(
        os.environ.get("PYTEST_XDIST_WORKER") is not None,
        reason="DuckDB resource contention crashes pytest-xdist workers",
    )
    def test_startindex_limit_allows_small_datasets(self):
        """When total features fit within the startIndex limit, should proceed."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import fetch_all_features_duckdb

        page = pa.table(
            {
                "geometry": pa.array([b"\x01\x02"], type=pa.binary()),
                "name": pa.array(["a"]),
            }
        )

        with (
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=30000),
            patch("geoparquet_io.core.wfs._probe_startindex_limit", return_value=50000),
            patch("geoparquet_io.core.wfs._fetch_wfs_page", return_value=page),
        ):
            result = fetch_all_features_duckdb(
                "https://mock.wfs/wfs",
                "layer",
                max_workers=1,
                page_size=10000,
            )

        assert result.num_rows > 0

    @pytest.mark.skipif(
        os.environ.get("PYTEST_XDIST_WORKER") is not None,
        reason="DuckDB resource contention crashes pytest-xdist workers",
    )
    def test_no_startindex_limit_proceeds_normally(self):
        """When server has no startIndex limit, pagination should proceed."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import fetch_all_features_duckdb

        page = pa.table(
            {
                "geometry": pa.array([b"\x01\x02"], type=pa.binary()),
                "name": pa.array(["a"]),
            }
        )

        with (
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=20000),
            patch("geoparquet_io.core.wfs._probe_startindex_limit", return_value=None),
            patch("geoparquet_io.core.wfs._fetch_wfs_page", return_value=page),
        ):
            result = fetch_all_features_duckdb(
                "https://mock.wfs/wfs",
                "layer",
                max_workers=1,
                page_size=10000,
            )

        assert result.num_rows > 0


# =============================================================================
# Axis Order Tests (Issue #397)
# =============================================================================


class TestAxisOrder:
    """Test axis order detection and bbox parameter building.

    WFS 1.1.0+ with URN format CRS uses lat,lon (YX) axis order per OGC spec,
    while simple EPSG:4326 format uses lon,lat (XY).
    """

    def test_is_urn_crs_detects_urn_format(self):
        """Should correctly identify URN format CRS strings."""
        from geoparquet_io.core.wfs import _is_urn_crs

        assert _is_urn_crs("urn:ogc:def:crs:EPSG::4326") is True
        assert _is_urn_crs("urn:x-ogc:def:crs:EPSG::4326") is True
        assert _is_urn_crs("URN:OGC:DEF:CRS:EPSG::3857") is True
        assert _is_urn_crs("EPSG:4326") is False
        assert _is_urn_crs("http://www.opengis.net/def/crs/EPSG/0/4326") is False

    def test_is_geographic_crs_detects_geographic_systems(self):
        """Should correctly identify geographic (lat/lon) CRS."""
        from geoparquet_io.core.wfs import _is_geographic_crs

        assert _is_geographic_crs("EPSG:4326") is True
        assert _is_geographic_crs("urn:ogc:def:crs:EPSG::4326") is True
        assert _is_geographic_crs("EPSG:4269") is True  # NAD83
        assert _is_geographic_crs("CRS:84") is True
        assert _is_geographic_crs("EPSG:3857") is False  # Web Mercator (projected)
        assert _is_geographic_crs("EPSG:3035") is False  # LAEA (projected)

    def test_is_geographic_crs_handles_codes_outside_allowlist(self):
        """Geographic CRSs beyond the small allowlist must not be called projected.

        pyproj-backed detection identifies any geographic CRS; a hardcoded list
        would misclassify these and trigger a false coordinate mismatch.
        """
        from geoparquet_io.core.wfs import _is_geographic_crs

        assert _is_geographic_crs("EPSG:4171") is True  # RGF93 (geographic)
        assert _is_geographic_crs("EPSG:4258") is True  # ETRS89 (geographic)
        assert _is_geographic_crs("urn:ogc:def:crs:EPSG::4171") is True
        assert _is_geographic_crs("EPSG:22174") is False  # POSGAR 98 (projected)
        assert _is_geographic_crs("EPSG:25830") is False  # ETRS89 / UTM 30N (projected)

    @pytest.mark.parametrize(
        "crs,version,axis_order,expected_swap",
        [
            # WFS 1.0.0 always uses XY
            ("EPSG:4326", "1.0.0", "auto", False),
            ("urn:ogc:def:crs:EPSG::4326", "1.0.0", "auto", False),
            # WFS 1.1.0+ with simple EPSG format uses XY
            ("EPSG:4326", "1.1.0", "auto", False),
            ("EPSG:4326", "2.0.0", "auto", False),
            # WFS 1.1.0+ with URN format uses YX (lat,lon)
            ("urn:ogc:def:crs:EPSG::4326", "1.1.0", "auto", True),
            ("urn:ogc:def:crs:EPSG::4326", "2.0.0", "auto", True),
            ("urn:x-ogc:def:crs:EPSG::4326", "1.1.0", "auto", True),
            # CRS:84 is always XY by definition
            ("CRS:84", "1.1.0", "auto", False),
            ("urn:ogc:def:crs:OGC:1.3:CRS84", "2.0.0", "auto", False),
            # Projected CRS never swaps
            ("urn:ogc:def:crs:EPSG::3857", "1.1.0", "auto", False),
            # Forced axis order overrides auto-detection
            ("urn:ogc:def:crs:EPSG::4326", "1.1.0", "xy", False),
            ("EPSG:4326", "1.1.0", "latlon", True),
        ],
    )
    def test_needs_axis_swap(self, crs, version, axis_order, expected_swap):
        """Axis swap decision based on CRS format, version, and override."""
        from geoparquet_io.core.wfs import _needs_axis_swap

        result = _needs_axis_swap(crs, version, axis_order)
        assert result is expected_swap

    @pytest.mark.parametrize(
        "bbox,crs,version,axis_order,expected",
        [
            # WFS 1.0.0: no CRS suffix
            ((-122.5, 37.5, -122.0, 38.0), "EPSG:4326", "1.0.0", "auto", "-122.5,37.5,-122.0,38.0"),
            # WFS 1.1.0+: Always EPSG:4326 with XY order (bbox is always WGS84)
            # This ensures compatibility with servers that only accept WGS84 bbox
            (
                (-122.5, 37.5, -122.0, 38.0),
                "EPSG:4326",
                "1.1.0",
                "auto",
                "-122.5,37.5,-122.0,38.0,EPSG:4326",
            ),
            # Even with URN output CRS, bbox uses simple EPSG:4326
            (
                (-122.5, 37.5, -122.0, 38.0),
                "urn:ogc:def:crs:EPSG::4326",
                "1.1.0",
                "auto",
                "-122.5,37.5,-122.0,38.0,EPSG:4326",
            ),
            # WFS 2.0.0 also uses EPSG:4326 for bbox
            (
                (4.82, 50.44, 4.92, 50.48),
                "urn:ogc:def:crs:EPSG::4326",
                "2.0.0",
                "auto",
                "4.82,50.44,4.92,50.48,EPSG:4326",
            ),
            # Projected output CRS still uses WGS84 bbox
            (
                (-122.5, 37.5, -122.0, 38.0),
                "EPSG:3857",
                "1.1.0",
                "auto",
                "-122.5,37.5,-122.0,38.0,EPSG:4326",
            ),
        ],
    )
    def test_build_bbox_param_axis_order(self, bbox, crs, version, axis_order, expected):
        """Bbox parameter string should always use WGS84 for server compatibility."""
        from geoparquet_io.core.wfs import _build_bbox_param

        result = _build_bbox_param(bbox, crs, version, axis_order)
        assert result == expected


# =============================================================================
# Version Negotiation Tests (Issue #312)
# =============================================================================


class TestVersionNegotiation:
    """Test WFS version negotiation and WFS 2.0 support."""

    def test_wfs_versions_list(self):
        """WFS_VERSIONS should be in preference order (newest first)."""
        from geoparquet_io.core.wfs import WFS_VERSIONS

        assert WFS_VERSIONS == ["2.0.0", "1.1.0", "1.0.0"]

    def test_build_wfs_url_uses_count_for_wfs_2(self):
        """WFS 2.0 should use 'count' parameter instead of 'maxFeatures'."""
        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs",
            "test:layer",
            version="2.0.0",
            max_features=100,
        )
        assert "count=100" in url
        assert "maxFeatures" not in url

    def test_build_wfs_url_uses_maxfeatures_for_wfs_1(self):
        """WFS 1.x should use 'maxFeatures' parameter."""
        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs",
            "test:layer",
            version="1.1.0",
            max_features=100,
        )
        assert "maxFeatures=100" in url
        assert "count=" not in url

    def test_build_wfs_url_uses_typenames_for_wfs_20(self):
        """WFS 2.0.0 should use 'typeNames' (plural)."""
        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs",
            "test:layer",
            version="2.0.0",
        )
        assert "typeNames=test%3Alayer" in url or "typeNames=test:layer" in url
        assert "typeName=" not in url

    def test_build_wfs_url_uses_typename_for_wfs_11(self):
        """WFS 1.1.0 should use 'typeName' (singular) per OGC spec."""
        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs",
            "test:layer",
            version="1.1.0",
        )
        assert "typeName=test%3Alayer" in url or "typeName=test:layer" in url
        assert "typeNames=" not in url

    def test_build_wfs_url_uses_typename_for_wfs_10(self):
        """WFS 1.0.0 should use 'typeName' (singular)."""
        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs",
            "test:layer",
            version="1.0.0",
        )
        assert "typeName=test" in url
        assert "typeNames=" not in url

    def test_build_wfs_url_includes_srsname_without_bbox(self):
        """srsName should be included even without bbox filter (Issue #405)."""
        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs",
            "test:layer",
            version="1.1.0",
            crs="EPSG:4326",
            # No bbox provided
        )
        assert "srsName=" in url or "srsname=" in url.lower()

    def test_build_wfs_url_includes_srsname_with_bbox(self):
        """srsName should still work when bbox is also provided."""
        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs",
            "test:layer",
            version="1.1.0",
            bbox=(4.0, 50.0, 5.0, 51.0),
            crs="EPSG:4326",
        )
        # Should have both srsName param AND bbox with CRS suffix
        assert "srsName=" in url or "srsname=" in url.lower()
        assert "bbox=" in url

    def test_build_wfs_url_includes_srsname_wfs_20(self):
        """WFS 2.0.0 should also include srsName parameter."""
        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs",
            "test:layer",
            version="2.0.0",
            crs="EPSG:4326",
        )
        assert "srsName=" in url or "srsname=" in url.lower()

    def test_build_wfs_url_excludes_srsname_wfs_10(self):
        """WFS 1.0.0 should NOT include srsName (uses SRS in bbox only)."""
        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs",
            "test:layer",
            version="1.0.0",
            crs="EPSG:4326",
        )
        # srsName is a 1.1.0+ parameter
        assert "srsName=" not in url

    def test_build_wfs_url_excludes_srsname_when_no_crs(self):
        """srsName should be absent when no CRS specified."""
        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs",
            "test:layer",
            version="1.1.0",
            # No crs provided
        )
        assert "srsName=" not in url


# =============================================================================
# Query Param Merging Tests (Issue #828)
# =============================================================================


class TestBuildWfsUrlQueryParamMerging:
    """A service_url with an existing query param (e.g. an apikey) must not
    be corrupted by a second "?" when WFS GetFeature params are appended."""

    def test_build_wfs_url_preserves_existing_query_param(self):
        """The issue's exact reproduction: an apikey query param must survive
        intact, joined with '&' rather than a second '?'."""
        from urllib.parse import parse_qs, urlparse

        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/geo/wfs?apikey=mykey",
            "layer",
            "2.0.0",
        )

        # Exactly one '?' delimiter in the whole URL.
        assert url.count("?") == 1

        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        assert params["apikey"] == ["mykey"]
        assert params["service"] == ["WFS"]
        assert params["version"] == ["2.0.0"]
        assert params["request"] == ["GetFeature"]
        assert params["typeNames"] == ["layer"]

    def test_build_wfs_url_no_existing_query_param(self):
        """Non-regression: a service_url with no query string still produces
        a single '?' and no stray leading '&'."""
        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs",
            "test:layer",
            version="2.0.0",
        )

        assert url.count("?") == 1
        assert "?&" not in url
        assert url.startswith("https://example.com/wfs?")

    def test_build_wfs_url_merges_multiple_existing_query_params(self):
        """Multiple pre-existing query params must all be preserved and
        merged with the WFS params."""
        from urllib.parse import parse_qs, urlparse

        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs?apikey=mykey&format=json",
            "test:layer",
            version="1.1.0",
        )

        assert url.count("?") == 1
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        assert params["apikey"] == ["mykey"]
        assert params["format"] == ["json"]
        assert params["service"] == ["WFS"]
        assert params["typeName"] == ["test:layer"]

    def test_build_wfs_url_strips_wfs_specific_params_before_merging(self):
        """A service_url that already carries GetCapabilities-style WFS
        params (service/request/version) must not end up with duplicate or
        conflicting keys after merging."""
        from urllib.parse import parse_qs, urlparse

        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs?service=WFS&request=GetCapabilities&apikey=mykey",
            "test:layer",
            version="2.0.0",
        )

        assert url.count("?") == 1
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        # Stale GetCapabilities value must not survive; GetFeature is authoritative.
        assert params["request"] == ["GetFeature"]
        assert params["apikey"] == ["mykey"]

    def test_build_wfs_url_strips_mixed_case_wfs_params(self):
        """WFS KVP keys are case-insensitive per the OGC spec, so the strip
        must be too: ``typeName`` (canonical WFS 1.1 casing users copy from a
        browser URL) surviving next to the ``typeNames`` we set would leave the
        server to pick a layer from conflicting duplicate keys."""
        from urllib.parse import parse_qs, urlparse

        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs"
            "?Version=1.0.0&typeName=roads&Request=GetCapabilities&apikey=mykey",
            "buildings",
            version="2.0.0",
        )

        assert url.count("?") == 1
        params = parse_qs(urlparse(url).query)
        # The apikey survives; every mixed-case control param is replaced by
        # exactly one authoritative value.
        assert params["apikey"] == ["mykey"]
        assert params["typeNames"] == ["buildings"]
        assert params["version"] == ["2.0.0"]
        assert params["request"] == ["GetFeature"]
        lowered = [k.lower() for k in params]
        assert "typename" not in lowered  # typeName=roads must not survive
        assert lowered.count("version") == 1
        assert lowered.count("request") == 1

    def test_build_wfs_url_strips_stale_control_params_case_insensitively(self):
        """A GetFeature URL copied from a browser carries request-control keys
        (outputFormat, count, startIndex, resultType, srsName) that must not
        leak into the request gpio builds — a stale ``resultType=hits`` would
        turn every data request into a count response."""
        from urllib.parse import parse_qs, urlparse

        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs"
            "?resultType=hits&outputFormat=text/xml&startIndex=100"
            "&maxFeatures=5&Count=5&srsName=EPSG:3857&apikey=mykey",
            "buildings",
            version="2.0.0",
        )

        params = parse_qs(urlparse(url).query)
        assert params["apikey"] == ["mykey"]
        lowered = [k.lower() for k in params]
        for stale in ("resulttype", "outputformat", "startindex", "maxfeatures", "srsname"):
            assert stale not in lowered
        assert "count" not in lowered  # no max_features requested → no count param


# =============================================================================
# CRS Validation Tests (Issue #398)
# =============================================================================


class TestCRSValidation:
    """Test CRS coordinate validation and mismatch detection."""

    def test_estimate_crs_from_bbox_detects_wgs84(self):
        """Should detect WGS84 coordinates."""
        from geoparquet_io.core.wfs import _estimate_crs_from_bbox

        # Valid WGS84 bbox
        assert _estimate_crs_from_bbox((-122.5, 37.5, -122.0, 38.0)) == "EPSG:4326"
        assert _estimate_crs_from_bbox((-180, -90, 180, 90)) == "EPSG:4326"
        assert _estimate_crs_from_bbox((4.82, 50.44, 4.92, 50.48)) == "EPSG:4326"

    def test_estimate_crs_from_bbox_detects_laea_europe(self):
        """Should detect EPSG:3035 (LAEA Europe) coordinates."""
        from geoparquet_io.core.wfs import _estimate_crs_from_bbox

        # Typical EPSG:3035 bbox (Belgian buildings in meters)
        bbox = (3817324.31, 2942224.42, 4063861.36, 3100245.97)
        assert _estimate_crs_from_bbox(bbox) == "EPSG:3035"

    def test_estimate_crs_from_bbox_detects_web_mercator(self):
        """Should detect EPSG:3857 (Web Mercator) coordinates."""
        from geoparquet_io.core.wfs import _estimate_crs_from_bbox

        # Web Mercator coordinates (world extent)
        bbox = (-20037508.34, -20037508.34, 20037508.34, 20037508.34)
        assert _estimate_crs_from_bbox(bbox) == "EPSG:3857"

    def test_validate_crs_coordinates_passes_for_valid_wgs84(self):
        """Should pass when coordinates match requested WGS84 CRS."""
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.wfs import _validate_crs_coordinates

        # Create a table with valid WGS84 geometry
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            result = con.execute("""
                SELECT ST_AsWKB(ST_Point(-122.4, 37.8)) as geometry
            """).arrow()
            table = result.read_all()
        finally:
            con.close()

        is_valid, detected = _validate_crs_coordinates(table, "EPSG:4326")
        assert is_valid is True
        assert detected is None

    def test_validate_crs_coordinates_detects_mismatch(self):
        """Should detect when coordinates don't match requested CRS."""
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.wfs import _validate_crs_coordinates

        # Create a table with EPSG:3035 coordinates (not WGS84)
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            result = con.execute("""
                SELECT ST_AsWKB(ST_Point(3900000, 3000000)) as geometry
            """).arrow()
            table = result.read_all()
        finally:
            con.close()

        # Request WGS84 but coordinates are clearly projected
        is_valid, detected = _validate_crs_coordinates(table, "EPSG:4326", strict=False)
        assert is_valid is False
        assert detected == "EPSG:3035"

    def test_validate_crs_coordinates_does_not_override_projected_crs(self):
        """Should NOT override a server-honored projected CRS on a bbox guess (issue #499).

        EPSG:22174 (POSGAR 98 / Argentina 4) and EPSG:3857 both use large metric
        coordinates that the bbox heuristic cannot distinguish. When the server
        honored the requested projected CRS, we must trust it rather than relabel
        it as the heuristic's guess, which silently corrupts downstream reprojection.
        """
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.wfs import _validate_crs_coordinates

        # Real EPSG:22174 coordinates from IDECOR (Córdoba, Argentina)
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            result = con.execute("""
                SELECT ST_AsWKB(ST_Point(4527586.01, 6386852.32)) as geometry
            """).arrow()
            table = result.read_all()
        finally:
            con.close()

        # Requested and server-returned 22174 — heuristic would guess 3857, but
        # both are projected, so there is no real (category) mismatch.
        is_valid, detected = _validate_crs_coordinates(table, "EPSG:22174", strict=False)
        assert is_valid is True
        assert detected is None

    def test_validate_crs_coordinates_detects_projected_data_for_geographic_request(self):
        """Should still flag a real category mismatch: geographic requested, metric data."""
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.wfs import _validate_crs_coordinates

        # Projected (metric) coordinates returned for a non-4326 geographic request
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            result = con.execute("""
                SELECT ST_AsWKB(ST_Point(4527586.01, 6386852.32)) as geometry
            """).arrow()
            table = result.read_all()
        finally:
            con.close()

        # EPSG:4258 (ETRS89) is geographic — metric values are a real mismatch.
        is_valid, detected = _validate_crs_coordinates(table, "EPSG:4258", strict=False)
        assert is_valid is False
        assert detected == "EPSG:3857"

    def test_validate_crs_coordinates_detects_geographic_data_for_projected_request(self):
        """Should flag a real category mismatch: projected requested, degree data."""
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.wfs import _validate_crs_coordinates

        # Degree-range coordinates returned for a projected request
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            result = con.execute("""
                SELECT ST_AsWKB(ST_Point(-62.7, -32.6)) as geometry
            """).arrow()
            table = result.read_all()
        finally:
            con.close()

        # EPSG:22174 is projected — degree values are a real mismatch.
        is_valid, detected = _validate_crs_coordinates(table, "EPSG:22174", strict=False)
        assert is_valid is False
        assert detected == "EPSG:4326"

    def test_validate_crs_coordinates_raises_on_strict_mismatch(self):
        """Should raise error in strict mode when CRS doesn't match."""
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.wfs import WFSError, _validate_crs_coordinates

        # Create a table with projected coordinates
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            result = con.execute("""
                SELECT ST_AsWKB(ST_Point(3900000, 3000000)) as geometry
            """).arrow()
            table = result.read_all()
        finally:
            con.close()

        with pytest.raises(WFSError, match="Coordinate mismatch"):
            _validate_crs_coordinates(table, "EPSG:4326", strict=True)

    def test_validate_crs_coordinates_skips_empty_table(self):
        """Should pass for empty tables without checking."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _validate_crs_coordinates

        # Empty table
        table = pa.table({"geometry": pa.array([], type=pa.binary())})

        is_valid, detected = _validate_crs_coordinates(table, "EPSG:4326")
        assert is_valid is True
        assert detected is None

    def test_reproject_on_crs_mismatch(self):
        """Should reproject data when CRS mismatch detected (issue #407)."""
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.reproject import reproject_table
        from geoparquet_io.core.wfs import _validate_crs_coordinates

        # Create table with EPSG:3035 coordinates (Belgium area)
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            result = con.execute("""
                SELECT ST_AsWKB(ST_Point(3900000, 3000000)) as geometry
            """).arrow()
            table = result.read_all()
        finally:
            con.close()

        # Verify mismatch detection
        is_valid, detected_crs = _validate_crs_coordinates(table, "EPSG:4326", strict=False)
        assert is_valid is False
        assert detected_crs == "EPSG:3035"

        # Reproject to WGS84 (simulates the new wfs_to_table behavior)
        reprojected = reproject_table(table, target_crs="EPSG:4326", source_crs="EPSG:3035")

        # Verify output coordinates are in WGS84 range
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            con.register("data", reprojected)
            result = con.execute("""
                SELECT
                    ST_X(ST_GeomFromWKB(geometry)) as x,
                    ST_Y(ST_GeomFromWKB(geometry)) as y
                FROM data
            """).fetchone()
            x, y = result
        finally:
            con.close()

        # WGS84 coordinates should be in valid range
        assert -180 <= x <= 180, f"X coordinate {x} out of WGS84 range"
        assert -90 <= y <= 90, f"Y coordinate {y} out of WGS84 range"
        # Should be roughly in Western Europe (Belgium area)
        assert 0 < x < 10, f"Expected longitude ~4-6, got {x}"
        assert 45 < y < 55, f"Expected latitude ~50, got {y}"

    @pytest.mark.usefixtures("offline_wfs_probes")
    def test_wfs_to_table_reprojects_on_mismatch(self):
        """wfs_to_table should reproject when output_crs set and server returns different CRS."""
        from unittest.mock import MagicMock, patch

        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.wfs import wfs_to_table

        # Create mock table with EPSG:3035 coordinates
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            result = con.execute("""
                SELECT ST_AsWKB(ST_Point(3900000, 3000000)) as geometry
            """).arrow()
            mock_table = result.read_all()
        finally:
            con.close()

        # Mock the WFS fetching to return our test table
        with (
            patch("geoparquet_io.core.wfs.negotiate_wfs_version") as mock_version,
            patch("geoparquet_io.core.wfs.get_layer_info") as mock_layer_info,
            patch("geoparquet_io.core.wfs.fetch_all_features_duckdb") as mock_fetch,
        ):
            mock_version.return_value = ("2.0.0", MagicMock())
            mock_layer_info.return_value = MagicMock(
                typename="mock:layer",
                title="Mock Layer",
                crs_list=["EPSG:4326", "EPSG:3035"],
                default_crs="EPSG:4326",
                available_formats=["application/json"],
            )
            mock_fetch.return_value = mock_table

            # Call wfs_to_table with output_crs - should trigger reprojection
            result = wfs_to_table(
                service_url="https://mock.wfs.server/wfs",
                typename="mock:layer",
                output_crs="EPSG:4326",
            )

        # Verify result is reprojected to WGS84
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            con.register("data", result)
            coords = con.execute("""
                SELECT
                    ST_X(ST_GeomFromWKB(geometry)) as x,
                    ST_Y(ST_GeomFromWKB(geometry)) as y
                FROM data
            """).fetchone()
            x, y = coords
        finally:
            con.close()

        # Should be in WGS84 range (Western Europe)
        assert -180 <= x <= 180, f"X {x} not in WGS84 range"
        assert -90 <= y <= 90, f"Y {y} not in WGS84 range"

    @pytest.mark.usefixtures("offline_wfs_probes")
    def test_reproject_error_wrapped_in_wfs_error(self):
        """Reprojection errors should be wrapped in WFSError with context."""
        from unittest.mock import MagicMock, patch

        import pyarrow as pa

        from geoparquet_io.core.wfs import WFSError, wfs_to_table

        # Create mock table with invalid geometry
        mock_table = pa.table({"geometry": [b"invalid_wkb"]})

        with (
            patch("geoparquet_io.core.wfs.negotiate_wfs_version") as mock_version,
            patch("geoparquet_io.core.wfs.get_layer_info") as mock_layer_info,
            patch("geoparquet_io.core.wfs.fetch_all_features_duckdb") as mock_fetch,
            patch("geoparquet_io.core.wfs._validate_crs_coordinates") as mock_validate,
        ):
            mock_version.return_value = ("2.0.0", MagicMock())
            mock_layer_info.return_value = MagicMock(
                typename="mock:layer",
                title="Mock Layer",
                crs_list=["EPSG:4326"],
                default_crs="EPSG:4326",
                available_formats=["application/json"],
            )
            mock_fetch.return_value = mock_table
            # Force mismatch detection
            mock_validate.return_value = (False, "EPSG:3035")

            with pytest.raises(WFSError) as exc_info:
                wfs_to_table(
                    service_url="https://mock.wfs.server/wfs",
                    typename="mock:layer",
                    output_crs="EPSG:4326",
                )

            assert "Failed to reproject" in str(exc_info.value)
            assert "EPSG:3035" in str(exc_info.value)
            assert "EPSG:4326" in str(exc_info.value)


class TestServerDeclaredCRS:
    """The server's GeoJSON ``crs`` member is authoritative (issue #499).

    When a WFS server echoes the CRS it actually used in the FeatureCollection
    ``crs`` member, we must read and trust it rather than guessing from the
    bounding box — the bbox heuristic cannot tell two projected CRSs apart.
    """

    def _make_mock_stream(self, geojson_bytes: bytes):
        """Create a mock httpx stream response yielding the given bytes."""
        import httpx

        def mock_stream(*args, **kwargs):
            class MockResponse:
                status_code = 200
                headers = {"content-type": "application/json"}

                def raise_for_status(self):
                    pass

                def iter_bytes(self, chunk_size=None):
                    yield geojson_bytes

                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    pass

            return MockResponse()

        return patch.object(httpx.Client, "stream", mock_stream)

    def _geojson_with_crs(self, crs_name: str | None) -> bytes:
        import json

        feature_collection: dict = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [4527586.01, 6386852.32]},
                    "properties": {"name": "a"},
                }
            ],
        }
        if crs_name is not None:
            feature_collection["crs"] = {"type": "name", "properties": {"name": crs_name}}
        return json.dumps(feature_collection).encode()

    def test_fetch_wfs_page_extracts_server_crs(self):
        """_fetch_wfs_page records the server-declared CRS in schema metadata."""
        from geoparquet_io.core.wfs import _SERVER_CRS_METADATA_KEY, _fetch_wfs_page

        body = self._geojson_with_crs("urn:ogc:def:crs:EPSG::22174")
        with self._make_mock_stream(body):
            table = _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")

        assert table.schema.metadata is not None
        assert table.schema.metadata[_SERVER_CRS_METADATA_KEY] == b"EPSG:22174"

    def test_fetch_wfs_page_no_crs_member(self):
        """No ``crs`` member (RFC 7946 GeoJSON) leaves no server-CRS metadata."""
        from geoparquet_io.core.wfs import _SERVER_CRS_METADATA_KEY, _fetch_wfs_page

        with self._make_mock_stream(self._geojson_with_crs(None)):
            table = _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")

        metadata = table.schema.metadata or {}
        assert _SERVER_CRS_METADATA_KEY not in metadata

    def test_fetch_wfs_page_empty_response_carries_crs(self):
        """An empty FeatureCollection still propagates its declared CRS."""
        import json

        from geoparquet_io.core.wfs import _SERVER_CRS_METADATA_KEY, _fetch_wfs_page

        body = json.dumps(
            {
                "type": "FeatureCollection",
                "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::22174"}},
                "features": [],
            }
        ).encode()
        with self._make_mock_stream(body):
            table = _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")

        assert table.num_rows == 0
        assert (table.schema.metadata or {}).get(_SERVER_CRS_METADATA_KEY) == b"EPSG:22174"

    def test_fetch_wfs_page_unrecognized_crs_shape(self):
        """A ``crs`` member without a name (e.g. a link CRS) yields no metadata.

        ``json_extract_string`` returns NULL rather than raising on an
        unexpected shape, so extraction falls back to the bbox heuristic.
        """
        import json

        from geoparquet_io.core.wfs import _SERVER_CRS_METADATA_KEY, _fetch_wfs_page

        body = json.dumps(
            {
                "type": "FeatureCollection",
                "crs": {"type": "link", "properties": {"href": "http://example/crs"}},
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [4527586.01, 6386852.32]},
                        "properties": {"name": "a"},
                    }
                ],
            }
        ).encode()
        with self._make_mock_stream(body):
            table = _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")

        assert table.num_rows == 1
        assert _SERVER_CRS_METADATA_KEY not in (table.schema.metadata or {})

    def test_read_and_with_server_crs_roundtrip(self):
        """_with_server_crs / _read_server_crs round-trip through metadata."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _read_server_crs, _with_server_crs

        table = pa.table({"geometry": pa.array([], type=pa.binary())})
        assert _read_server_crs(table) is None

        tagged = _with_server_crs(table, "EPSG:22174")
        assert _read_server_crs(tagged) == "EPSG:22174"

        # No-op when CRS is None.
        assert _read_server_crs(_with_server_crs(table, None)) is None

    def test_fetch_all_features_propagates_server_crs(self):
        """Server CRS survives the single-request fetch path."""
        from unittest.mock import patch

        import pyarrow as pa

        from geoparquet_io.core.wfs import (
            _read_server_crs,
            _with_server_crs,
            fetch_all_features_duckdb,
        )

        page = _with_server_crs(
            pa.table({"geometry": pa.array([b"\x00"], type=pa.binary())}),
            "EPSG:22174",
        )
        with (
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=1),
            patch("geoparquet_io.core.wfs._fetch_wfs_page", return_value=page),
            patch("geoparquet_io.core.wfs._infer_column_types", side_effect=lambda t: t),
        ):
            result = fetch_all_features_duckdb("http://mock.wfs/wfs", "layer", version="2.0.0")

        assert _read_server_crs(result) == "EPSG:22174"


class TestResolveCRSForOutput:
    """wfs_to_table CRS resolution: trust the server, guess only as a fallback."""

    def _point_table(self, x: float, y: float):
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection

        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            result = con.execute(f"SELECT ST_AsWKB(ST_Point({x}, {y})) as geometry").arrow()
            return result.read_all()
        finally:
            con.close()

    def test_trusts_matching_server_crs_without_guessing(self):
        """Issue #499: server honored EPSG:22174 → keep it, never guess EPSG:3857."""
        from unittest.mock import patch

        from geoparquet_io.core.wfs import _resolve_crs_for_output, _with_server_crs

        # Argentine Gauss-Krüger coords the bbox heuristic would misread as 3857.
        table = _with_server_crs(self._point_table(4527586.01, 6386852.32), "EPSG:22174")

        with patch("geoparquet_io.core.wfs._validate_crs_coordinates") as mock_validate:
            out_table, crs = _resolve_crs_for_output(table, "EPSG:22174", None, False)

        assert crs == "EPSG:22174"
        # The heuristic validator must not even run when the server declared a CRS.
        mock_validate.assert_not_called()

    def test_server_crs_contradiction_labels_with_server_crs(self):
        """Server ignored srsName (returned 3857 for 22174) and no output_crs → label 3857."""
        from geoparquet_io.core.wfs import _resolve_crs_for_output, _with_server_crs

        table = _with_server_crs(self._point_table(4527586.01, 6386852.32), "EPSG:3857")

        out_table, crs = _resolve_crs_for_output(table, "EPSG:22174", None, False)

        # Authoritative server CRS wins over the requested label; no bbox guess.
        assert crs == "EPSG:3857"
        assert out_table is table

    def test_server_crs_contradiction_reprojects_to_output_crs(self):
        """Contradiction with output_crs set → reproject from the server CRS."""
        from unittest.mock import patch

        from geoparquet_io.core.wfs import _resolve_crs_for_output, _with_server_crs

        table = _with_server_crs(self._point_table(4527586.01, 6386852.32), "EPSG:3857")

        with patch(
            "geoparquet_io.core.wfs.reproject_table", return_value="REPROJECTED"
        ) as mock_reproject:
            out_table, crs = _resolve_crs_for_output(table, "EPSG:22174", "EPSG:4326", False)

        assert crs == "EPSG:4326"
        assert out_table == "REPROJECTED"
        mock_reproject.assert_called_once()
        assert mock_reproject.call_args.kwargs["source_crs"] == "EPSG:3857"
        assert mock_reproject.call_args.kwargs["target_crs"] == "EPSG:4326"

    def test_server_crs_contradiction_strict_raises(self):
        """Contradiction under strict_crs → WFSError, even with no output_crs."""
        from geoparquet_io.core.wfs import WFSError, _resolve_crs_for_output, _with_server_crs

        table = _with_server_crs(self._point_table(4527586.01, 6386852.32), "EPSG:3857")

        with pytest.raises(WFSError, match="server may have ignored srsName"):
            _resolve_crs_for_output(table, "EPSG:22174", None, True)

    def test_falls_back_to_heuristic_without_server_crs(self):
        """No server CRS → fall back to coordinate-range guessing (degrees for 4326)."""
        from unittest.mock import patch

        from geoparquet_io.core.wfs import _resolve_crs_for_output

        # Projected metric coords but WGS84 requested, no server CRS declared.
        table = self._point_table(3900000, 3000000)

        with patch("geoparquet_io.core.wfs.debug") as mock_debug:
            out_table, crs = _resolve_crs_for_output(table, "EPSG:4326", None, False)

        # Heuristic detects projected data and relabels (fallback path preserved).
        assert crs == "EPSG:3035"
        # The fallback to coordinate inference is announced (visible under --verbose).
        assert any("declared no CRS" in str(call.args[0]) for call in mock_debug.call_args_list)

    @pytest.mark.usefixtures("offline_wfs_probes")
    def test_wfs_to_table_trusts_server_crs_end_to_end(self):
        """End-to-end #499 regression: declared EPSG:22174 stays EPSG:22174."""
        import json
        from unittest.mock import MagicMock, patch

        from geoparquet_io.core.crs_utils import parse_crs_string_to_projjson
        from geoparquet_io.core.wfs import _with_server_crs, wfs_to_table

        table = _with_server_crs(self._point_table(4527586.01, 6386852.32), "EPSG:22174")

        with (
            patch("geoparquet_io.core.wfs.negotiate_wfs_version") as mock_version,
            patch("geoparquet_io.core.wfs.get_layer_info") as mock_layer_info,
            patch("geoparquet_io.core.wfs.fetch_all_features_duckdb", return_value=table),
            patch("geoparquet_io.core.wfs._validate_crs_coordinates") as mock_validate,
        ):
            mock_version.return_value = ("2.0.0", MagicMock())
            mock_layer_info.return_value = MagicMock(
                typename="mock:layer",
                title="Mock Layer",
                crs_list=["EPSG:22174"],
                default_crs="EPSG:22174",
                available_formats=["application/json"],
                sortable_attribute=None,
                bbox=None,
            )
            result = wfs_to_table(
                service_url="https://mock.wfs.server/wfs",
                typename="mock:layer",
                output_crs="EPSG:22174",
            )

        mock_validate.assert_not_called()
        # Output metadata must encode EPSG:22174, not a bbox guess.
        geo = json.loads(result.schema.metadata[b"geo"])
        expected = parse_crs_string_to_projjson("EPSG:22174")
        assert geo["columns"]["geometry"]["crs"] == expected

    @pytest.mark.usefixtures("offline_wfs_probes")
    def test_wfs_to_table_preserves_server_crs_through_local_bbox_filter(self):
        """Issue #499: local bbox filtering must not drop the server CRS.

        The local-filter path round-trips the table through DuckDB, which
        strips Arrow schema metadata. If the server-declared CRS is lost there,
        resolution silently falls back to the bbox heuristic — the exact bug
        #499 fixes — so we assert the heuristic validator is never consulted.
        """
        import json
        from unittest.mock import MagicMock, patch

        from geoparquet_io.core.crs_utils import parse_crs_string_to_projjson
        from geoparquet_io.core.wfs import _with_server_crs, wfs_to_table

        table = _with_server_crs(self._point_table(4527586.01, 6386852.32), "EPSG:22174")

        with (
            patch("geoparquet_io.core.wfs.negotiate_wfs_version") as mock_version,
            patch("geoparquet_io.core.wfs.get_layer_info") as mock_layer_info,
            patch("geoparquet_io.core.wfs.fetch_all_features_duckdb", return_value=table),
            patch("geoparquet_io.core.wfs._validate_crs_coordinates") as mock_validate,
        ):
            mock_version.return_value = ("2.0.0", MagicMock())
            mock_layer_info.return_value = MagicMock(
                typename="mock:layer",
                title="Mock Layer",
                crs_list=["EPSG:22174"],
                default_crs="EPSG:22174",
                available_formats=["application/json"],
                sortable_attribute=None,
                bbox=None,
            )
            result = wfs_to_table(
                service_url="https://mock.wfs.server/wfs",
                typename="mock:layer",
                bbox=(4_000_000, 6_000_000, 5_000_000, 7_000_000),
                bbox_mode="local",
            )

        # Server CRS survived the local-filter roundtrip → no heuristic guess.
        mock_validate.assert_not_called()
        assert result.num_rows == 1
        geo = json.loads(result.schema.metadata[b"geo"])
        assert geo["columns"]["geometry"]["crs"] == parse_crs_string_to_projjson("EPSG:22174")

    @pytest.mark.usefixtures("offline_wfs_probes")
    def test_local_bbox_filter_reprojects_bbox_to_server_crs(self):
        """Issue #499: a bbox in the requested CRS must be aligned to the CRS the
        server actually returned, or local filtering drops valid rows.

        Requested EPSG:4326 (bbox in degrees), but the server returns EPSG:22174
        geometry. The degree bbox would not intersect the metric point unless it
        is reprojected into EPSG:22174 first.
        """
        from unittest.mock import MagicMock, patch

        from geoparquet_io.core.wfs import _with_server_crs, wfs_to_table

        # EPSG:22174 point near Córdoba, AR (= -62.7059, -32.6603 in EPSG:4326).
        table = _with_server_crs(self._point_table(4527586.01, 6386852.32), "EPSG:22174")
        deg_bbox = (-62.8059, -32.7603, -62.6059, -32.5603)

        with (
            patch("geoparquet_io.core.wfs.negotiate_wfs_version") as mock_version,
            patch("geoparquet_io.core.wfs.get_layer_info") as mock_layer_info,
            patch("geoparquet_io.core.wfs.fetch_all_features_duckdb", return_value=table),
        ):
            mock_version.return_value = ("2.0.0", MagicMock())
            mock_layer_info.return_value = MagicMock(
                typename="mock:layer",
                title="Mock Layer",
                crs_list=["EPSG:4326"],
                default_crs="EPSG:4326",
                available_formats=["application/json"],
                sortable_attribute=None,
                bbox=None,
            )
            result = wfs_to_table(
                service_url="https://mock.wfs.server/wfs",
                typename="mock:layer",
                bbox=deg_bbox,
                bbox_mode="local",
            )

        # The point is retained because the bbox was reprojected to EPSG:22174.
        assert result.num_rows == 1

    @pytest.mark.usefixtures("offline_wfs_probes")
    def test_wfs_to_table_strips_server_crs_marker_when_no_projjson(self):
        """The internal server-CRS marker never leaks into the output schema.

        Even when no geo metadata is written (projjson unavailable for the
        resolved CRS), the ``_wfs_server_crs`` marker must be removed.
        """
        from unittest.mock import MagicMock, patch

        from geoparquet_io.core.wfs import (
            _SERVER_CRS_METADATA_KEY,
            _with_server_crs,
            wfs_to_table,
        )

        table = _with_server_crs(self._point_table(4527586.01, 6386852.32), "EPSG:22174")

        with (
            patch("geoparquet_io.core.wfs.negotiate_wfs_version") as mock_version,
            patch("geoparquet_io.core.wfs.get_layer_info") as mock_layer_info,
            patch("geoparquet_io.core.wfs.fetch_all_features_duckdb", return_value=table),
            patch("geoparquet_io.core.wfs.parse_crs_string_to_projjson", return_value=None),
        ):
            mock_version.return_value = ("2.0.0", MagicMock())
            mock_layer_info.return_value = MagicMock(
                typename="mock:layer",
                title="Mock Layer",
                crs_list=["EPSG:22174"],
                default_crs="EPSG:22174",
                available_formats=["application/json"],
                sortable_attribute=None,
                bbox=None,
            )
            result = wfs_to_table(
                service_url="https://mock.wfs.server/wfs",
                typename="mock:layer",
                output_crs="EPSG:22174",
            )

        metadata = result.schema.metadata or {}
        assert _SERVER_CRS_METADATA_KEY not in metadata
        assert b"geo" not in metadata


# =============================================================================
# Integration Tests for WFS 2.0 (Issue #312)
# =============================================================================


@pytest.mark.integration
@pytest.mark.network
@pytest.mark.slow
class TestWFS20Integration:
    """Integration tests against real WFS 2.0 servers.

    Uses Helsinki WFS (kartta.hel.fi) which reliably supports WFS 2.0.0.
    """

    HELSINKI_WFS = "https://kartta.hel.fi/ws/geoserver/avoindata/wfs"
    HELSINKI_LAYER = "avoindata:Ajoneuvoliikenne_liikennemaarat_viiva"

    @pytest.mark.xfail(reason="External WFS service may be unavailable")
    def test_version_negotiation_finds_wfs_2(self):
        """Auto negotiation should find WFS 2.0.0 when available."""
        from geoparquet_io.core.wfs import negotiate_wfs_version

        version, wfs = negotiate_wfs_version(self.HELSINKI_WFS)
        assert version in ["2.0.0", "1.1.0", "1.0.0"]

    @pytest.mark.xfail(reason="External WFS service may be unavailable")
    def test_extract_with_wfs_2_version(self):
        """Should successfully extract features using WFS 2.0.0."""
        from geoparquet_io.core.wfs import wfs_to_table

        table = wfs_to_table(
            self.HELSINKI_WFS,
            self.HELSINKI_LAYER,
            version="2.0.0",
            limit=10,
        )

        assert table.num_rows <= 10
        assert "geometry" in table.column_names


# =============================================================================
# Type Inference Tests (Issue #400)
# =============================================================================


class TestInferColumnTypes:
    """Tests for string column type inference.

    WFS servers often return all values as quoted strings in JSON, causing
    DuckDB to infer them as VARCHAR. These tests verify that _infer_column_types
    correctly detects and casts numeric, boolean, and other typed columns.
    """

    def test_infer_types_integer(self):
        """Integer strings should be cast to int64."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"objectid": ["12345", "67890", "11111"]})
        result = _infer_column_types(table)
        assert result.schema.field("objectid").type == pa.int64()
        assert result["objectid"].to_pylist() == [12345, 67890, 11111]

    def test_infer_types_float(self):
        """Float strings should be cast to double."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"value": ["3.14", "2.71", "1.618"]})
        result = _infer_column_types(table)
        assert result.schema.field("value").type == pa.float64()
        assert result["value"].to_pylist() == pytest.approx([3.14, 2.71, 1.618])

    def test_infer_types_string_preserved(self):
        """Non-numeric strings should stay as string."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"name": ["Alice", "Bob", "Charlie"]})
        result = _infer_column_types(table)
        assert result.schema.field("name").type in (pa.string(), pa.large_string())
        assert result["name"].to_pylist() == ["Alice", "Bob", "Charlie"]

    def test_infer_types_with_nulls(self):
        """Null values should be preserved during type inference."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"count": ["100", None, "200"]})
        result = _infer_column_types(table)
        assert result.schema.field("count").type == pa.int64()
        assert result["count"].to_pylist() == [100, None, 200]

    def test_infer_types_prefers_int_over_float(self):
        """Whole numbers should be int64, not float64."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"id": ["1", "2", "3"]})
        result = _infer_column_types(table)
        assert result.schema.field("id").type == pa.int64()

    def test_infer_types_skips_non_string_columns(self):
        """Already-typed columns should not be modified."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table(
            {
                "existing_int": pa.array([1, 2, 3], type=pa.int64()),
                "existing_float": pa.array([1.1, 2.2, 3.3], type=pa.float64()),
            }
        )
        result = _infer_column_types(table)
        assert result.schema.field("existing_int").type == pa.int64()
        assert result.schema.field("existing_float").type == pa.float64()

    def test_infer_types_boolean(self):
        """Boolean strings should be cast to bool."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"active": ["true", "false", "true"]})
        result = _infer_column_types(table)
        assert result.schema.field("active").type == pa.bool_()
        assert result["active"].to_pylist() == [True, False, True]

    def test_infer_types_boolean_variants(self):
        """Various boolean representations should work."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"flag": ["True", "FALSE", "1", "0"]})
        result = _infer_column_types(table)
        assert result.schema.field("flag").type == pa.bool_()
        assert result["flag"].to_pylist() == [True, False, True, False]

    def test_infer_types_empty_table(self):
        """Empty tables should not crash."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"col": pa.array([], type=pa.string())})
        result = _infer_column_types(table)
        assert result.num_rows == 0

    def test_infer_types_all_nulls(self):
        """Column with all nulls should stay string."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"maybe": pa.array([None, None], type=pa.string())})
        result = _infer_column_types(table)
        # Stays string (or large_string) because we can't infer type from nulls
        assert result.schema.field("maybe").type in (pa.string(), pa.large_string())

    def test_infer_types_mixed_numeric_stays_string(self):
        """Mixed numeric and text should stay string."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"mixed": ["123", "abc", "456"]})
        result = _infer_column_types(table)
        assert result.schema.field("mixed").type in (pa.string(), pa.large_string())

    def test_infer_types_negative_numbers(self):
        """Negative numbers should be handled correctly."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table(
            {
                "negative_int": ["-100", "-200", "300"],
                "negative_float": ["-1.5", "2.5", "-3.5"],
            }
        )
        result = _infer_column_types(table)
        assert result.schema.field("negative_int").type == pa.int64()
        assert result.schema.field("negative_float").type == pa.float64()
        assert result["negative_int"].to_pylist() == [-100, -200, 300]

    def test_infer_types_geometry_preserved(self):
        """Binary geometry column should not be touched."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        geom = pa.array([b"\x01\x02\x03"], type=pa.binary())
        table = pa.table(
            {
                "geometry": geom,
                "id": ["1"],
            }
        )
        result = _infer_column_types(table)
        assert result.schema.field("geometry").type == pa.binary()

    def test_infer_types_multiple_columns(self):
        """Multiple columns should all be processed."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table(
            {
                "id": ["1", "2"],
                "value": ["3.14", "2.71"],
                "name": ["foo", "bar"],
                "active": ["true", "false"],
            }
        )
        result = _infer_column_types(table)
        assert result.schema.field("id").type == pa.int64()
        assert result.schema.field("value").type == pa.float64()
        assert result.schema.field("name").type in (pa.string(), pa.large_string())
        assert result.schema.field("active").type == pa.bool_()

    # =========================================================================
    # Edge Case Tests (adversarial review findings)
    # =========================================================================

    def test_infer_types_scientific_notation(self):
        """Scientific notation should cast to float, not int."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"sci": ["1.5e6", "2.0e-3", "3e10"]})
        result = _infer_column_types(table)
        # Scientific notation parses as float
        assert result.schema.field("sci").type == pa.float64()
        assert result["sci"].to_pylist() == pytest.approx([1.5e6, 2.0e-3, 3e10])

    def test_infer_types_leading_zeros_cast_to_float(self):
        """Leading zeros cast to float (not int, since '007' != '7')."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        # Leading zeros: '007' != '7' so fails int check, but DOUBLE works
        table = pa.table({"code": ["007", "008", "009"]})
        result = _infer_column_types(table)
        # DuckDB TRY_CAST to DOUBLE succeeds, so becomes float
        # Note: if you need to preserve leading zeros, don't use type inference
        assert result.schema.field("code").type == pa.float64()
        assert result["code"].to_pylist() == [7.0, 8.0, 9.0]

    def test_infer_types_whitespace_cast_to_float(self):
        """DuckDB trims whitespace before casting, so these become floats."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        # DuckDB trims whitespace during TRY_CAST
        table = pa.table({"padded": [" 123 ", "456", " 789"]})
        result = _infer_column_types(table)
        # Whitespace trimmed, DOUBLE cast succeeds
        assert result.schema.field("padded").type == pa.float64()
        assert result["padded"].to_pylist() == [123.0, 456.0, 789.0]

    def test_infer_types_empty_string_stays_string(self):
        """Empty strings mixed with numbers should stay string."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"maybe_num": ["123", "", "456"]})
        result = _infer_column_types(table)
        # Empty string is not a valid int, stays string
        assert result.schema.field("maybe_num").type in (pa.string(), pa.large_string())

    def test_infer_types_bigint_overflow_becomes_float(self):
        """Numbers exceeding BIGINT range cast to float (with precision loss)."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        # BIGINT max is 9223372036854775807, but DOUBLE can hold larger values
        table = pa.table({"huge": ["99999999999999999999", "1", "2"]})
        result = _infer_column_types(table)
        # BIGINT fails but DOUBLE succeeds (with precision loss)
        assert result.schema.field("huge").type == pa.float64()
        # Note: 99999999999999999999 becomes 1e20 (precision loss is expected)
        assert result["huge"].to_pylist()[0] == pytest.approx(1e20, rel=0.01)

    def test_infer_types_connection_cleanup_on_error(self):
        """Connection should be closed even if processing fails."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        # Test with valid data - connection should be closed after
        table = pa.table({"x": ["1", "2", "3"]})
        result = _infer_column_types(table)
        assert result.schema.field("x").type == pa.int64()
        # If connection leaked, subsequent calls would fail or accumulate
        # Run multiple times to detect leaks
        for _ in range(5):
            result = _infer_column_types(table)
            assert result.schema.field("x").type == pa.int64()


# =============================================================================
# Type Inference Integration Test (Issue #400)
# =============================================================================


@pytest.mark.network
@pytest.mark.slow
class TestTypeInferenceIntegration:
    """Integration test verifying type inference works with real WFS data.

    Uses Belgian WFS which returns string-typed numeric fields.
    """

    # Belgian federal WFS - known to return string-typed numerics
    BELGIUM_WFS = "https://geoservices.wallonie.be/arcgis/services/AMENAGEMENT_TERRITOIRE/TLPE/MapServer/WFSServer"
    BELGIUM_LAYER = "TLPE:TLPE"

    @pytest.mark.xfail(
        reason="External WFS service may be unavailable",
        strict=False,
        raises=(Exception,),
    )
    def test_wfs_type_inference_real_server(self):
        """Real WFS data should have numeric columns properly typed."""
        import pyarrow as pa

        from geoparquet_io.api import ops

        table = ops.from_wfs(
            self.BELGIUM_WFS,
            self.BELGIUM_LAYER,
            limit=50,
        )

        assert table.num_rows > 0
        assert "geometry" in table.column_names

        # Check that at least one numeric-looking column got inferred
        # (exact columns depend on the WFS, but objectid is typically numeric)
        numeric_types = (pa.int64(), pa.float64())
        has_numeric = any(
            field.type in numeric_types for field in table.schema if field.name != "geometry"
        )
        # If no numeric columns found, that's suspicious but not fatal
        # (server may have changed schema)
        if not has_numeric:
            pytest.skip("No numeric columns found in WFS response")


# =============================================================================
# srsName Without Bbox Tests (Issue #405)
# =============================================================================


@pytest.mark.integration
@pytest.mark.network
@pytest.mark.slow
class TestSrsNameWithoutBbox:
    """Integration tests for srsName parameter without bbox filter.

    Issue #405: srsName was only sent when bbox was provided, causing servers
    to return data in their native CRS instead of the requested CRS.
    """

    # Wallonia INSPIRE server - returns EPSG:3035 natively, supports WGS84
    WALLONIA_WFS = "https://geoservices.wallonie.be/geoserver/inspire_bu/ows"
    WALLONIA_LAYER = "inspire_bu:BU.Building_building_emprise"

    @pytest.mark.xfail(
        reason="External WFS service may be unavailable",
        strict=False,
        raises=(Exception,),  # Only xfail on connection/service errors
    )
    def test_wfs_without_bbox_returns_wgs84_coordinates(self):
        """Without bbox, server should still return WGS84 when requested.

        The Wallonia server's native CRS is EPSG:3035 (ETRS89-LAEA).
        If srsName is properly sent, we should get WGS84 coords (~4.3, ~50.7).
        If srsName is missing, we'd get EPSG:3035 coords (~3923264, ~3080361).
        """
        import shapely

        from geoparquet_io.core.wfs import wfs_to_table

        table = wfs_to_table(
            self.WALLONIA_WFS,
            self.WALLONIA_LAYER,
            version="1.1.0",
            limit=5,
            # No bbox - this is the key condition for #405
        )

        assert table.num_rows > 0
        geom = shapely.from_wkb(table.column("geometry")[0].as_py())

        # Get first coordinate
        if hasattr(geom, "exterior"):
            x, y = geom.exterior.coords[0]
        else:
            x, y = geom.coords[0]

        # WGS84 Belgium coords: longitude ~3-6, latitude ~49-52
        # EPSG:3035 coords would be ~3.8M, ~3.0M
        assert -180 <= x <= 180, f"X coord {x} not in WGS84 range, likely EPSG:3035"
        assert -90 <= y <= 90, f"Y coord {y} not in WGS84 range, likely EPSG:3035"

        # More specific check for Belgium
        assert 2.5 <= x <= 6.5, f"X coord {x} not in Belgium longitude range"
        assert 49.0 <= y <= 52.0, f"Y coord {y} not in Belgium latitude range"


@pytest.mark.integration
@pytest.mark.network
@pytest.mark.slow
class TestProjectedCRSPreservedIntegration:
    """Live regression for #499: a server-honored projected CRS must be kept.

    IDECOR (Córdoba, Argentina) serves all layers in EPSG:22174 (an Argentine
    Gauss-Krüger zone). The old bbox heuristic saw the large metric coordinates
    (~4.5M, ~6.4M) and relabeled the output EPSG:3857, silently corrupting any
    downstream reprojection. The geometry was correct, so this can only be
    caught by asserting on the stored CRS metadata, not coordinate ranges.
    """

    IDECOR_WFS = "https://idecor-ws.mapascordoba.gob.ar/geoserver/idecor/wfs"
    IDECOR_LAYER = "idecor:puntos_interes_bv"

    @pytest.mark.xfail(
        reason="External WFS service may be unavailable",
        strict=False,
        raises=(Exception,),  # Only xfail on connection/service errors
    )
    def test_projected_crs_not_relabeled(self):
        """Output metadata must report EPSG:22174, never the bbox-guessed 3857."""
        import json

        from geoparquet_io.core.wfs import wfs_to_table

        table = wfs_to_table(
            self.IDECOR_WFS,
            self.IDECOR_LAYER,
            output_crs="EPSG:22174",
            limit=5,
        )

        assert table.num_rows > 0

        geo = json.loads(table.schema.metadata[b"geo"])
        crs = geo["columns"]["geometry"]["crs"]
        # PROJJSON for EPSG:22174 — the authoritative code must be preserved.
        assert crs["id"]["authority"] == "EPSG"
        assert int(crs["id"]["code"]) == 22174, (
            f"Output mislabeled as {crs['id']}; expected EPSG:22174 (issue #499)"
        )


# =============================================================================
# Spatial Tiling Tests (startIndex limit workaround)
# =============================================================================


class TestGenerateTileGrid:
    """Test grid generation for spatial tiling."""

    def test_correct_count(self):
        """Grid should produce at least the requested number of tiles."""
        from geoparquet_io.core.wfs import _generate_tile_grid

        bbox = (3.0, 50.0, 7.0, 54.0)
        tiles = _generate_tile_grid(bbox, 4)
        assert len(tiles) >= 4

    def test_covers_full_bbox(self):
        """Union of tile bboxes should cover the original bbox."""
        from geoparquet_io.core.wfs import _generate_tile_grid

        bbox = (3.0, 50.0, 7.0, 54.0)
        tiles = _generate_tile_grid(bbox, 6)

        all_xmin = min(t[0] for t in tiles)
        all_ymin = min(t[1] for t in tiles)
        all_xmax = max(t[2] for t in tiles)
        all_ymax = max(t[3] for t in tiles)

        assert all_xmin == pytest.approx(bbox[0])
        assert all_ymin == pytest.approx(bbox[1])
        assert all_xmax == pytest.approx(bbox[2])
        assert all_ymax == pytest.approx(bbox[3])

    def test_single_tile(self):
        """Requesting 1 tile should return the original bbox."""
        from geoparquet_io.core.wfs import _generate_tile_grid

        bbox = (3.0, 50.0, 7.0, 54.0)
        tiles = _generate_tile_grid(bbox, 1)
        assert len(tiles) == 1
        assert tiles[0] == pytest.approx(bbox)

    def test_respects_aspect_ratio(self):
        """Wide bbox should produce more columns than rows."""
        from geoparquet_io.core.wfs import _generate_tile_grid

        wide_bbox = (0.0, 0.0, 10.0, 2.0)
        tiles = _generate_tile_grid(wide_bbox, 10)

        xs = sorted({t[0] for t in tiles} | {t[2] for t in tiles})
        ys = sorted({t[1] for t in tiles} | {t[3] for t in tiles})
        cols = len(xs) - 1
        rows = len(ys) - 1
        assert cols > rows


class TestRefineTilesAdaptive:
    """Test adaptive quadtree refinement of tile grid."""

    def test_splits_oversized_tile(self):
        """Tile with count > limit should be subdivided into 4."""
        from geoparquet_io.core.wfs import _refine_tiles_adaptive

        tiles = [(0.0, 0.0, 1.0, 1.0)]

        def mock_count(service_url, typename, version, bbox=None, crs=None, axis_order="auto"):
            if bbox == (0.0, 0.0, 1.0, 1.0):
                return 100000
            return 20000

        with patch("geoparquet_io.core.wfs._get_feature_count", side_effect=mock_count):
            result = _refine_tiles_adaptive(
                tiles,
                "http://mock/wfs",
                "layer",
                "1.1.0",
                crs="EPSG:4326",
                axis_order="auto",
                max_per_tile=50000,
            )

        assert len(result) == 4

    def test_leaves_small_tiles_alone(self):
        """Tile with count < limit should not be subdivided."""
        from geoparquet_io.core.wfs import _refine_tiles_adaptive

        tiles = [(0.0, 0.0, 1.0, 1.0)]

        with patch("geoparquet_io.core.wfs._get_feature_count", return_value=10000):
            result = _refine_tiles_adaptive(
                tiles,
                "http://mock/wfs",
                "layer",
                "1.1.0",
                crs="EPSG:4326",
                axis_order="auto",
                max_per_tile=50000,
            )

        assert len(result) == 1
        assert result[0] == tiles[0]

    def test_max_depth_prevents_infinite_recursion(self):
        """Recursion should stop at max_depth even if tiles are still too large."""
        from geoparquet_io.core.wfs import _refine_tiles_adaptive

        tiles = [(0.0, 0.0, 1.0, 1.0)]

        with patch("geoparquet_io.core.wfs._get_feature_count", return_value=999999):
            result = _refine_tiles_adaptive(
                tiles,
                "http://mock/wfs",
                "layer",
                "1.1.0",
                crs="EPSG:4326",
                axis_order="auto",
                max_per_tile=50000,
                max_depth=2,
            )

        # depth 0: 1 tile -> 4 (depth 1) -> 16 (depth 2, max)
        assert len(result) == 16

    def test_handles_none_count_gracefully(self):
        """If _get_feature_count returns None, tile should be kept as-is."""
        from geoparquet_io.core.wfs import _refine_tiles_adaptive

        tiles = [(0.0, 0.0, 1.0, 1.0)]

        with patch("geoparquet_io.core.wfs._get_feature_count", return_value=None):
            result = _refine_tiles_adaptive(
                tiles,
                "http://mock/wfs",
                "layer",
                "1.1.0",
                crs="EPSG:4326",
                axis_order="auto",
                max_per_tile=50000,
            )

        assert len(result) == 1


class TestGetFeatureCountWithBbox:
    """_get_feature_count's hits probe, read off the request it issued.

    The probe must pre-merge its WFS params into the URL rather than passing
    ``params=`` to httpx: httpx 0.28 *replaces* a URL's existing query when
    ``params=`` is given, which drops e.g. an apikey (issue #828). Asserting
    the transport's recorded query string tests exactly that, where asserting
    a mock's recorded kwargs only tested how the call was spelled.
    """

    def test_bbox_included_in_request(self, monkeypatch):
        """When bbox provided, it should appear in the hits request."""
        from geoparquet_io.core.wfs import _get_feature_count

        http = FakeTransport.install(monkeypatch)
        http.respond("/wfs", xml_reply(b'<c numberOfFeatures="42"/>'))

        result = _get_feature_count(
            "http://mock/wfs",
            "layer",
            "1.1.0",
            bbox=(4.0, 52.0, 5.0, 53.0),
            crs="EPSG:4326",
        )

        assert result == 42
        assert "bbox" in http.last.params

    def test_no_bbox_backward_compatible(self, monkeypatch):
        """Without bbox, request should not include bbox param."""
        from geoparquet_io.core.wfs import _get_feature_count

        http = FakeTransport.install(monkeypatch)
        http.respond("/wfs", xml_reply(b'<c numberOfFeatures="100"/>'))

        result = _get_feature_count("http://mock/wfs", "layer", "1.1.0")

        assert result == 100
        assert "bbox" not in http.last.params


class TestGetFeatureCountPreservesQueryParams:
    """The resultType=hits probe must keep the service URL's own query params.

    On the issue #828 scenario a server that 403s without its apikey made the
    probe fail, the failure was swallowed into count=None, and pagination /
    auto-tiling silently disengaged — the #678 truncation failure mode.
    """

    def test_hits_probe_url_carries_apikey_and_resulttype(self, monkeypatch):
        """The probe request URL carries BOTH the apikey and resultType=hits."""
        from geoparquet_io.core.wfs import _get_feature_count

        http = FakeTransport.install(monkeypatch)
        http.respond("/geo/wfs", xml_reply(b'<c numberMatched="7"/>'))

        result = _get_feature_count("https://example.com/geo/wfs?apikey=mykey", "layer", "2.0.0")

        assert result == 7
        assert http.last.params == {
            "apikey": "mykey",
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": "layer",
            "resultType": "hits",
        }


class TestGetFeatureCountErrorVisibility:
    """A failed count probe must be visible, not silently degrade auto-tiling (#678)."""

    def test_wfs_error_returns_none_and_warns(self, caplog):
        """A WFSError from the probe returns None but logs a warning naming the cause."""
        from geoparquet_io.core.wfs import _get_feature_count

        with (
            patch(
                "geoparquet_io.core.wfs._make_request",
                side_effect=WFSError("Request failed after 3 attempts: connection refused"),
            ),
            caplog.at_level(logging.WARNING, logger="geoparquet_io"),
        ):
            result = _get_feature_count("http://mock/wfs", "layer", "1.1.0")

        assert result is None
        warnings = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
        assert any("connection refused" in m for m in warnings), warnings
        assert any("count" in m.lower() for m in warnings), warnings

    def test_transport_error_returns_none_and_warns(self, caplog):
        """An httpx transport error is expected — warn and degrade, do not raise."""
        import httpx

        from geoparquet_io.core.wfs import _get_feature_count

        with (
            patch(
                "geoparquet_io.core.wfs._make_request",
                side_effect=httpx.ConnectError("nodename nor servname provided"),
            ),
            caplog.at_level(logging.WARNING, logger="geoparquet_io"),
        ):
            result = _get_feature_count("http://mock/wfs", "layer", "2.0.0")

        assert result is None
        warnings = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
        assert any("nodename nor servname provided" in m for m in warnings), warnings

    def test_programming_error_propagates(self):
        """A TypeError is a bug in gpio, not a server problem — it must surface."""
        from geoparquet_io.core.wfs import _get_feature_count

        with (
            patch(
                "geoparquet_io.core.wfs._make_request",
                side_effect=TypeError("_make_request() got an unexpected keyword argument"),
            ),
            pytest.raises(TypeError),
        ):
            _get_feature_count("http://mock/wfs", "layer", "1.1.0")

    def test_successful_probe_does_not_warn(self, caplog):
        """The happy path is unchanged and silent."""
        from geoparquet_io.core.wfs import _get_feature_count

        with (
            patch("geoparquet_io.core.wfs._make_request", return_value=b'numberMatched="7"'),
            caplog.at_level(logging.WARNING, logger="geoparquet_io"),
        ):
            result = _get_feature_count("http://mock/wfs", "layer", "2.0.0")

        assert result == 7
        assert [r.message for r in caplog.records if r.levelno >= logging.WARNING] == []

    def test_server_without_count_returns_none_without_warning(self, caplog):
        """ "Server reports no count" is a normal answer, distinct from a failed probe."""
        from geoparquet_io.core.wfs import _get_feature_count

        with (
            patch("geoparquet_io.core.wfs._make_request", return_value=b"<wfs:FeatureCollection/>"),
            caplog.at_level(logging.WARNING, logger="geoparquet_io"),
        ):
            result = _get_feature_count("http://mock/wfs", "layer", "1.1.0")

        assert result is None
        assert [r.message for r in caplog.records if r.levelno >= logging.WARNING] == []

    def test_auto_tiling_still_engages_when_probe_succeeds(self):
        """Regression guard: a working probe still drives the auto-tiling decision."""
        from geoparquet_io.core.wfs import _refine_tiles_adaptive

        seen = []

        def fake_request(url, params=None, **kwargs):
            from urllib.parse import parse_qs, urlparse

            # Root tile is over the limit; every quadrant is under it. The
            # probe pre-merges its params into the URL (issue #828).
            query = parse_qs(urlparse(url).query)
            seen.append(query.get("bbox", [""])[0])
            n = 500 if len(seen) == 1 else 10
            return f'numberOfFeatures="{n}"'.encode()

        with patch("geoparquet_io.core.wfs._make_request", side_effect=fake_request):
            tiles = _refine_tiles_adaptive(
                [(0.0, 0.0, 4.0, 4.0)],
                "http://mock/wfs",
                "layer",
                "1.1.0",
                "EPSG:4326",
                "xy",
                max_per_tile=100,
            )

        assert len(tiles) == 4

    def test_warn_on_failure_false_stays_silent(self):
        """Speculative callers can opt out of the warning but still get None."""
        from geoparquet_io.core.wfs import _get_feature_count

        with (
            patch(
                "geoparquet_io.core.wfs._make_request",
                side_effect=WFSError("HTTP error 400: version not supported"),
            ),
            patch("geoparquet_io.core.wfs.warn") as mock_warn,
        ):
            result = _get_feature_count("http://mock/wfs", "layer", "2.0.0", warn_on_failure=False)

        assert result is None
        mock_warn.assert_not_called()


def _mock_layer_info():
    """Minimal layer info for driving wfs_to_table in tests."""
    return MagicMock(
        typename="layer",
        title="Layer",
        crs_list=["EPSG:4326"],
        default_crs="EPSG:4326",
        available_formats=["application/json"],
        bbox=(3.0, 50.0, 7.0, 54.0),
    )


class TestSpeculativeCountProbeIsQuiet:
    """wfs_to_table probes at 2.0.0 first and falls back to the negotiated
    version by construction. A 1.1.0-only server rejecting the 2.0.0 probe is a
    healthy extraction, not a degradation — it must not warn (#678 review)."""

    def _run_wfs_to_table(self, fake_request, caplog):
        import pyarrow as pa

        from geoparquet_io.core.wfs import wfs_to_table

        mock_table = pa.table(
            {
                "geometry": pa.array([b"\x01"], type=pa.binary()),
                "name": pa.array(["a"]),
            }
        )

        with (
            patch("geoparquet_io.core.wfs._make_request", side_effect=fake_request),
            patch(
                "geoparquet_io.core.wfs.negotiate_wfs_version", return_value=("1.1.0", MagicMock())
            ),
            patch("geoparquet_io.core.wfs.get_layer_info", return_value=_mock_layer_info()),
            patch("geoparquet_io.core.wfs._probe_startindex_limit", return_value=None),
            patch("geoparquet_io.core.wfs.fetch_all_features_duckdb", return_value=mock_table),
            caplog.at_level(logging.WARNING, logger="geoparquet_io"),
        ):
            wfs_to_table("http://mock/wfs", "layer", auto_tile=True)

        return [
            r.message
            for r in caplog.records
            if r.levelno >= logging.WARNING and "Feature count probe" in r.message
        ]

    def test_healthy_2_0_0_fallback_does_not_warn(self, caplog):
        """2.0.0 rejected, negotiated 1.1.0 answers: no probe warning at all."""

        def fake_request(url, params=None, **kwargs):
            from urllib.parse import parse_qs, urlparse

            # The probe pre-merges its params into the URL (issue #828).
            query = parse_qs(urlparse(url).query)
            if query.get("version") == ["2.0.0"]:
                raise WFSError("HTTP error 400: version 2.0.0 not supported")
            return b'numberOfFeatures="42"'

        assert self._run_wfs_to_table(fake_request, caplog) == []

    def test_real_probe_failure_still_warns(self, caplog):
        """When the negotiated-version probe fails too, the count is genuinely
        lost — that one must still be loud."""

        def fake_request(url, params=None, **kwargs):
            raise WFSError("Request failed after 3 attempts: connection refused")

        warnings = self._run_wfs_to_table(fake_request, caplog)
        assert len(warnings) == 1, warnings
        assert "connection refused" in warnings[0]
        assert "1.1.0" in warnings[0]


class TestDeduplicateTiles:
    """Test deduplication of features across tile boundaries."""

    def test_removes_duplicates_by_fid(self):
        """Features with same _wfs_fid should be deduplicated."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _deduplicate_tiles

        table = pa.table(
            {
                "_wfs_fid": pa.array(["a", "b", "a", "c"]),
                "geometry": pa.array([b"\x01", b"\x02", b"\x01", b"\x03"], type=pa.binary()),
                "name": pa.array(["x", "y", "x", "z"]),
            }
        )

        result = _deduplicate_tiles(table)
        assert result.num_rows == 3

    def test_drops_fid_column(self):
        """_wfs_fid column should be removed from output."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _deduplicate_tiles

        table = pa.table(
            {
                "_wfs_fid": pa.array(["a", "b"]),
                "geometry": pa.array([b"\x01", b"\x02"], type=pa.binary()),
            }
        )

        result = _deduplicate_tiles(table)
        assert "_wfs_fid" not in result.column_names

    def test_fallback_geometry_dedup_when_no_fid(self):
        """When _wfs_fid is absent, deduplicate by geometry bytes."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _deduplicate_tiles

        table = pa.table(
            {
                "geometry": pa.array([b"\x01", b"\x02", b"\x01"], type=pa.binary()),
                "name": pa.array(["x", "y", "x"]),
            }
        )

        result = _deduplicate_tiles(table)
        assert result.num_rows == 2

    def test_null_fid_uses_geometry_dedup(self):
        """Features with NULL _wfs_fid should be deduplicated by geometry."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _deduplicate_tiles

        # Mix of valid fids and NULLs - NULLs should use geometry dedup
        table = pa.table(
            {
                "_wfs_fid": pa.array(["a", None, None, "b"], type=pa.string()),
                "geometry": pa.array([b"\x01", b"\x02", b"\x02", b"\x03"], type=pa.binary()),
                "name": pa.array(["w", "x", "x", "z"]),
            }
        )

        result = _deduplicate_tiles(table)
        # "a" and "b" kept (unique fids), one of the two NULL rows kept (same geometry)
        assert result.num_rows == 3
        assert "_wfs_fid" not in result.column_names

    def test_all_null_fids_uses_geometry_dedup(self):
        """When all fids are NULL, deduplicate entirely by geometry."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _deduplicate_tiles

        table = pa.table(
            {
                "_wfs_fid": pa.array([None, None, None, None], type=pa.string()),
                "geometry": pa.array([b"\x01", b"\x02", b"\x01", b"\x02"], type=pa.binary()),
                "name": pa.array(["a", "b", "a", "b"]),
            }
        )

        result = _deduplicate_tiles(table)
        assert result.num_rows == 2
        assert "_wfs_fid" not in result.column_names


class TestFetchWithSpatialTiles:
    """Test the spatial tiling orchestrator."""

    def test_combines_tile_results(self):
        """Should fetch each tile and combine results."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _fetch_with_spatial_tiles

        tile1 = pa.table(
            {
                "_wfs_fid": pa.array(["a"]),
                "geometry": pa.array([b"\x01\x02"], type=pa.binary()),
                "name": pa.array(["x"]),
            }
        )
        tile2 = pa.table(
            {
                "_wfs_fid": pa.array(["b"]),
                "geometry": pa.array([b"\x01\x03"], type=pa.binary()),
                "name": pa.array(["y"]),
            }
        )

        call_count = 0

        def mock_fetch(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return tile1 if call_count == 1 else tile2

        with (
            patch(
                "geoparquet_io.core.wfs._generate_tile_grid",
                return_value=[
                    (0.0, 0.0, 0.5, 0.5),
                    (0.5, 0.0, 1.0, 0.5),
                ],
            ),
            patch(
                "geoparquet_io.core.wfs._refine_tiles_adaptive",
                side_effect=lambda tiles, *a, **_kw: tiles,
            ),
            patch("geoparquet_io.core.wfs.fetch_all_features_duckdb", side_effect=mock_fetch),
        ):
            result = _fetch_with_spatial_tiles(
                service_url="http://mock/wfs",
                typename="layer",
                version="1.1.0",
                total_count=100000,
                startindex_limit=50000,
                layer_bbox=(0.0, 0.0, 1.0, 0.5),
                crs="EPSG:4326",
            )

        assert result.num_rows == 2
        assert "_wfs_fid" not in result.column_names

    def test_auto_tile_triggers_tiling(self):
        """wfs_to_table with auto_tile=True should invoke tiling when limit detected."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import wfs_to_table

        mock_table = pa.table(
            {
                "geometry": pa.array([b"\x01"], type=pa.binary()),
                "name": pa.array(["a"]),
            }
        )

        with (
            patch(
                "geoparquet_io.core.wfs.negotiate_wfs_version", return_value=("1.1.0", MagicMock())
            ),
            patch("geoparquet_io.core.wfs.get_layer_info") as mock_info,
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=11000000),
            patch("geoparquet_io.core.wfs._probe_startindex_limit", return_value=50000),
            patch(
                "geoparquet_io.core.wfs._fetch_with_spatial_tiles", return_value=mock_table
            ) as mock_tile,
        ):
            mock_info.return_value = MagicMock(
                typename="layer",
                title="Layer",
                crs_list=["EPSG:4326"],
                default_crs="EPSG:4326",
                available_formats=["application/json"],
                bbox=(3.0, 50.0, 7.0, 54.0),
            )
            wfs_to_table("http://mock/wfs", "layer", auto_tile=True)

        mock_tile.assert_called_once()

    def test_auto_tile_noop_when_no_limit(self):
        """When server has no startIndex limit, should use normal fetch path."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import wfs_to_table

        mock_table = pa.table(
            {
                "geometry": pa.array([b"\x01"], type=pa.binary()),
                "name": pa.array(["a"]),
            }
        )

        with (
            patch(
                "geoparquet_io.core.wfs.negotiate_wfs_version", return_value=("1.1.0", MagicMock())
            ),
            patch("geoparquet_io.core.wfs.get_layer_info") as mock_info,
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=20000),
            patch("geoparquet_io.core.wfs._probe_startindex_limit", return_value=None),
            patch(
                "geoparquet_io.core.wfs.fetch_all_features_duckdb", return_value=mock_table
            ) as mock_fetch,
            patch("geoparquet_io.core.wfs._fetch_with_spatial_tiles") as mock_tile,
        ):
            mock_info.return_value = MagicMock(
                typename="layer",
                title="Layer",
                crs_list=["EPSG:4326"],
                default_crs="EPSG:4326",
                available_formats=["application/json"],
                bbox=(3.0, 50.0, 7.0, 54.0),
            )
            wfs_to_table("http://mock/wfs", "layer", auto_tile=True)

        mock_fetch.assert_called_once()
        mock_tile.assert_not_called()


class TestServerCapDetection:
    """Cap detection must tolerate slight count drift (issue #503).

    Parallel workers retry pages on transient errors, which can nudge the total
    a few features past an exact server cap (e.g. 1,000,002 instead of
    1,000,000). The detector must still recognise these as caps so reactive
    auto-tiling fires.
    """

    def test_exact_common_caps_detected(self):
        from geoparquet_io.core.wfs import _looks_like_server_cap

        for cap in (1000000, 500000, 100000, 50000, 10000):
            assert _looks_like_server_cap(cap) is True

    def test_slight_drift_above_cap_detected(self):
        """The core #503 bug: parallel-worker drift past a round cap."""
        from geoparquet_io.core.wfs import _looks_like_server_cap

        assert _looks_like_server_cap(1000002) is True
        assert _looks_like_server_cap(1000500) is True  # +0.05%

    def test_slight_drift_below_cap_detected(self):
        from geoparquet_io.core.wfs import _looks_like_server_cap

        assert _looks_like_server_cap(999998) is True
        assert _looks_like_server_cap(49997) is True

    def test_outside_tolerance_not_detected(self):
        """Counts beyond ±0.1% of a cap (and not round) are real totals."""
        from geoparquet_io.core.wfs import _looks_like_server_cap

        assert _looks_like_server_cap(1002000) is False  # +0.2%, not round
        assert _looks_like_server_cap(1234567) is False

    def test_round_numbers_still_detected(self):
        """Existing divisible-by-10000 heuristic is preserved."""
        from geoparquet_io.core.wfs import _looks_like_server_cap

        assert _looks_like_server_cap(1230000) is True
        assert _looks_like_server_cap(40000) is True

    def test_zero_and_negative_not_detected(self):
        from geoparquet_io.core.wfs import _looks_like_server_cap

        assert _looks_like_server_cap(0) is False
        assert _looks_like_server_cap(-5) is False

    def test_reactive_tiling_triggers_on_drifted_cap(self):
        """End-to-end: a capped response of 10,005 (drifted from 10,000) with a
        higher 2.0.0 count must trigger reactive spatial tiling (issue #503)."""
        import pyarrow as pa

        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.wfs import wfs_to_table

        # Build a real table whose row count looks like a drifted server cap.
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            capped_table = (
                con.execute("SELECT ST_AsWKB(ST_Point(0, 0)) AS geometry FROM range(10005)")
                .arrow()
                .read_all()
            )
        finally:
            con.close()

        tiled_table = pa.table(
            {
                "geometry": pa.array([b"\x01"], type=pa.binary()),
                "name": pa.array(["full"]),
            }
        )

        with (
            patch(
                "geoparquet_io.core.wfs.negotiate_wfs_version", return_value=("1.1.0", MagicMock())
            ),
            patch("geoparquet_io.core.wfs.get_layer_info") as mock_info,
            # 2.0.0 reports the true count; the capped fetch returns fewer.
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=11000),
            patch("geoparquet_io.core.wfs._probe_startindex_limit", return_value=None),
            patch("geoparquet_io.core.wfs.fetch_all_features_duckdb", return_value=capped_table),
            patch(
                "geoparquet_io.core.wfs._fetch_with_spatial_tiles", return_value=tiled_table
            ) as mock_tile,
        ):
            mock_info.return_value = MagicMock(
                typename="layer",
                title="Layer",
                crs_list=["EPSG:4326"],
                default_crs="EPSG:4326",
                available_formats=["application/json"],
                bbox=(3.0, 50.0, 7.0, 54.0),
            )
            wfs_to_table("http://mock/wfs", "layer", auto_tile=True)

        mock_tile.assert_called_once()


class TestCountThreadedToFetch:
    """The trusted 2.0.0 count must be threaded into the fetch (issue #503).

    Previously ``fetch_all_features_duckdb`` re-queried the count at the
    requested version (e.g. 1.1.0), which on some GeoServer instances returns a
    DIFFERENT (capped) number than 2.0.0 — so pagination stopped early.
    """

    def test_fetch_uses_provided_total_count(self, monkeypatch):
        """When total_count is passed, fetch must not re-query the count."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import fetch_all_features_duckdb

        # Table size must match total_count to avoid fallback to pagination
        small_table = pa.table({"geometry": pa.array([b"\x01"], type=pa.binary())})

        http = FakeTransport.install(monkeypatch)
        http.respond(
            lambda request: True, geojson_reply({"type": "FeatureCollection", "features": []})
        )

        with patch("geoparquet_io.core.wfs._single_fetch_mode", return_value=small_table):
            fetch_all_features_duckdb("http://mock/wfs", "layer", version="1.1.0", total_count=1)

        # No resultType=hits request was ever put on the wire.
        assert http.matching(lambda request: request.params.get("resultType") == "hits") == []

    def test_wfs_to_table_threads_count_into_fetch(self):
        """wfs_to_table must pass the 2.0.0 expected_count as total_count."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import wfs_to_table

        mock_table = pa.table(
            {
                "geometry": pa.array([b"\x01"], type=pa.binary()),
                "name": pa.array(["a"]),
            }
        )

        with (
            patch(
                "geoparquet_io.core.wfs.negotiate_wfs_version", return_value=("1.1.0", MagicMock())
            ),
            patch("geoparquet_io.core.wfs.get_layer_info") as mock_info,
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=20000),
            patch("geoparquet_io.core.wfs._probe_startindex_limit", return_value=None),
            patch(
                "geoparquet_io.core.wfs.fetch_all_features_duckdb", return_value=mock_table
            ) as mock_fetch,
        ):
            mock_info.return_value = MagicMock(
                typename="layer",
                title="Layer",
                crs_list=["EPSG:4326"],
                default_crs="EPSG:4326",
                available_formats=["application/json"],
                bbox=(3.0, 50.0, 7.0, 54.0),
            )
            wfs_to_table("http://mock/wfs", "layer", auto_tile=True)

        # The 2.0.0 count reached the fetch as total_count, so the fetch never
        # re-queried it at the negotiated version.
        assert [call.kwargs.get("total_count") for call in mock_fetch.call_args_list] == [20000]


class TestMultiLayerExtraction:
    """Tests for multi-layer parallel extraction."""

    def test_convert_wfs_layers_creates_directory(self, tmp_path):
        """Multi-layer extraction should create output directory and files."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import convert_wfs_layers_to_directory

        mock_table = pa.table(
            {
                "geometry": pa.array(
                    [
                        b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?"
                    ],
                    type=pa.binary(),
                ),
                "name": pa.array(["test"]),
            }
        )

        output_dir = tmp_path / "output"

        with (
            patch("geoparquet_io.core.wfs.wfs_to_table", return_value=mock_table),
            patch("geoparquet_io.core.wfs.configure_verbose"),
        ):
            results = convert_wfs_layers_to_directory(
                service_url="http://mock/wfs",
                typenames=["layer1", "layer2"],
                output_dir=str(output_dir),
                skip_hilbert=True,
                skip_bbox=True,
            )

        assert len(results) == 2
        assert "layer1" in results
        assert "layer2" in results
        assert output_dir.exists()
        assert (output_dir / "layer1.parquet").exists()
        assert (output_dir / "layer2.parquet").exists()

    def test_convert_wfs_layers_parallel_mode(self, tmp_path):
        """Multi-layer extraction with parallel_layers > 1 should use ThreadPoolExecutor."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import convert_wfs_layers_to_directory

        mock_table = pa.table(
            {
                "geometry": pa.array(
                    [
                        b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?"
                    ],
                    type=pa.binary(),
                ),
                "name": pa.array(["test"]),
            }
        )

        output_dir = tmp_path / "output"

        with (
            patch("geoparquet_io.core.wfs.wfs_to_table", return_value=mock_table),
            patch("geoparquet_io.core.wfs.configure_verbose"),
        ):
            results = convert_wfs_layers_to_directory(
                service_url="http://mock/wfs",
                typenames=["layer1", "layer2", "layer3"],
                output_dir=str(output_dir),
                parallel_layers=3,
                skip_hilbert=True,
                skip_bbox=True,
            )

        assert len(results) == 3

    def test_cli_parses_comma_separated_typenames(self, tmp_path):
        """CLI should parse comma-separated typenames for multi-layer mode."""
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        runner = CliRunner()

        # Just test that the CLI parses correctly - don't actually extract
        # Patch at the import location inside the function
        with patch("geoparquet_io.core.wfs.convert_wfs_layers_to_directory") as mock_extract:
            result = runner.invoke(
                cli,
                [
                    "extract",
                    "wfs",
                    "http://mock/wfs",
                    "layer1,layer2,layer3",
                    str(output_dir),
                    "--skip-hilbert",
                    "--skip-bbox",
                ],
            )

        # Should have called multi-layer function with parsed typenames
        assert result.exit_code == 0
        mock_extract.assert_called_once()
        call_kwargs = mock_extract.call_args[1]
        assert call_kwargs["typenames"] == ["layer1", "layer2", "layer3"]

    def test_cli_rejects_empty_typenames(self, tmp_path):
        """CLI should error (not IndexError) when typename parses to an empty list."""
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "extract",
                "wfs",
                "http://mock/wfs",
                ", ,",  # whitespace/comma-only -> no valid typenames
                str(tmp_path / "out.parquet"),
            ],
        )

        assert result.exit_code != 0
        assert "No valid typename" in result.output

    def test_sanitizes_typename_for_filename(self, tmp_path):
        """Typenames with colons should be sanitized for filenames."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import convert_wfs_layers_to_directory

        mock_table = pa.table(
            {
                "geometry": pa.array(
                    [
                        b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?"
                    ],
                    type=pa.binary(),
                ),
                "name": pa.array(["test"]),
            }
        )

        output_dir = tmp_path / "output"

        with (
            patch("geoparquet_io.core.wfs.wfs_to_table", return_value=mock_table),
            patch("geoparquet_io.core.wfs.configure_verbose"),
        ):
            convert_wfs_layers_to_directory(
                service_url="http://mock/wfs",
                typenames=["ns:layer_name"],
                output_dir=str(output_dir),
                skip_hilbert=True,
                skip_bbox=True,
            )

        # Colons should be replaced with underscores in filename
        assert (output_dir / "ns_layer_name.parquet").exists()


class TestStartIndexProbeErrorVisibility:
    """The sibling probe swallowed failures the same way `_get_feature_count` did.

    `_probe_startindex_limit` detects servers that reject a `startIndex` above
    some cap. Returning a silent `None` on a failed probe means "no cap known",
    which re-enables unbounded pagination against a server that will reject or
    truncate the later pages — the same silently-wrong-output failure mode as a
    lost feature count (#678), reached by a different route.
    """

    def test_transport_error_returns_none_and_warns(self, caplog):
        """A transport failure degrades to "no cap known" but says so."""
        import httpx

        from geoparquet_io.core.wfs import _probe_startindex_limit

        with (
            patch(
                "geoparquet_io.core.wfs._get_shared_http_client",
                side_effect=httpx.ConnectError("nodename nor servname provided"),
            ),
            caplog.at_level(logging.WARNING, logger="geoparquet_io"),
        ):
            result = _probe_startindex_limit("http://mock/wfs", "layer", "2.0.0")

        assert result is None
        warnings = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
        assert any("nodename nor servname provided" in m for m in warnings), warnings
        assert any("startindex" in m.lower() for m in warnings), warnings

    def test_programming_error_propagates(self):
        """A bug inside the probe must surface, not be reported as "no cap"."""
        from geoparquet_io.core.wfs import _probe_startindex_limit

        with patch(
            "geoparquet_io.core.wfs._build_wfs_url",
            side_effect=AttributeError("'NoneType' object has no attribute 'rstrip'"),
        ):
            with pytest.raises(AttributeError, match="rstrip"):
                _probe_startindex_limit("http://mock/wfs", "layer", "2.0.0")

    def test_server_without_a_cap_returns_none_without_warning(self, caplog):
        """A healthy server that imposes no cap is not a failure — stay quiet."""
        from geoparquet_io.core.wfs import _probe_startindex_limit

        response = MagicMock()
        response.status_code = 200
        client = MagicMock()
        client.get.return_value = response

        with (
            patch("geoparquet_io.core.wfs._get_shared_http_client", return_value=client),
            caplog.at_level(logging.WARNING, logger="geoparquet_io"),
        ):
            result = _probe_startindex_limit("http://mock/wfs", "layer", "2.0.0")

        assert result is None
        assert [r.message for r in caplog.records if r.levelno >= logging.WARNING] == []

    def test_detected_cap_is_returned_without_warning(self, caplog):
        """The success path still parses the cap out of the 400 body."""
        from geoparquet_io.core.wfs import _probe_startindex_limit

        response = MagicMock()
        response.status_code = 400
        response.text = "startIndex must not exceed 50,000"
        client = MagicMock()
        client.get.return_value = response

        with (
            patch("geoparquet_io.core.wfs._get_shared_http_client", return_value=client),
            caplog.at_level(logging.WARNING, logger="geoparquet_io"),
        ):
            result = _probe_startindex_limit("http://mock/wfs", "layer", "2.0.0")

        assert result == 50000
        assert [r.message for r in caplog.records if r.levelno >= logging.WARNING] == []
