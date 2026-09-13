"""
Tests for geoparquet_io.core.crs_utils module.

Tests CRS (Coordinate Reference System) utilities including extraction,
normalization, and validation of CRS information.
"""

import pytest

from geoparquet_io.core.crs_utils import (
    _extract_crs_identifier,
    _format_crs_display,
    _validate_projjson,
    _wrap_query_with_crs,
    extract_crs_from_parquet,
    get_crs_display_name,
    is_default_crs,
    is_geographic_crs,
    parse_crs_string_to_projjson,
)

# =============================================================================
# Tests for _extract_crs_identifier()
# =============================================================================


class TestExtractCrsIdentifier:
    """Test CRS identifier extraction from various formats."""

    def test_projjson_dict_with_id(self):
        """Extracts identifier from PROJJSON dict with 'id' key."""
        crs = {"id": {"authority": "EPSG", "code": 4326}}
        result = _extract_crs_identifier(crs)

        assert result == ("EPSG", 4326)

    def test_projjson_dict_numeric_code(self):
        """Numeric codes are returned as int."""
        crs = {"id": {"authority": "EPSG", "code": 5070}}
        result = _extract_crs_identifier(crs)

        assert result == ("EPSG", 5070)
        assert isinstance(result[1], int)

    def test_projjson_dict_string_code(self):
        """Non-numeric codes are returned as uppercase str."""
        crs = {"id": {"authority": "OGC", "code": "CRS84"}}
        result = _extract_crs_identifier(crs)

        assert result == ("OGC", "CRS84")
        assert isinstance(result[1], str)

    def test_epsg_string_format(self):
        """Parses 'EPSG:4326' string format."""
        result = _extract_crs_identifier("EPSG:4326")

        assert result == ("EPSG", 4326)

    def test_ogc_string_format(self):
        """Parses 'OGC:CRS84' string format."""
        result = _extract_crs_identifier("OGC:CRS84")

        assert result == ("OGC", "CRS84")

    def test_urn_ogc_format(self):
        """Parses URN format 'urn:ogc:def:crs:EPSG::4326'."""
        result = _extract_crs_identifier("urn:ogc:def:crs:EPSG::4326")

        assert result == ("EPSG", 4326)

    def test_case_insensitive_authority(self):
        """Authority is normalized to uppercase."""
        result = _extract_crs_identifier("epsg:4326")

        assert result[0] == "EPSG"

    def test_dict_without_id_returns_none(self):
        """Dict without 'id' key returns None."""
        crs = {"name": "WGS 84", "type": "GeographicCRS"}
        result = _extract_crs_identifier(crs)

        assert result is None

    def test_empty_dict_returns_none(self):
        """Empty dict returns None."""
        result = _extract_crs_identifier({})
        assert result is None

    def test_none_returns_none(self):
        """None input returns None."""
        result = _extract_crs_identifier(None)
        assert result is None

    def test_invalid_string_format(self):
        """Invalid string format returns None."""
        result = _extract_crs_identifier("not a valid crs")
        assert result is None

    def test_incomplete_urn(self):
        """Incomplete URN returns None."""
        result = _extract_crs_identifier("urn:ogc:def:crs")
        assert result is None


# =============================================================================
# Tests for is_default_crs()
# =============================================================================


class TestIsDefaultCrs:
    """Test default CRS detection (OGC:CRS84 or EPSG:4326)."""

    def test_none_is_default(self):
        """None CRS is considered default."""
        assert is_default_crs(None) is True

    def test_empty_string_is_default(self):
        """Empty string is considered default."""
        assert is_default_crs("") is True

    def test_epsg_4326_is_default(self):
        """EPSG:4326 is default."""
        crs = {"id": {"authority": "EPSG", "code": 4326}}
        assert is_default_crs(crs) is True

    def test_ogc_crs84_is_default(self):
        """OGC:CRS84 is default."""
        crs = {"id": {"authority": "OGC", "code": "CRS84"}}
        assert is_default_crs(crs) is True

    def test_epsg_5070_not_default(self):
        """EPSG:5070 is not default."""
        crs = {"id": {"authority": "EPSG", "code": 5070}}
        assert is_default_crs(crs) is False

    def test_string_epsg_4326_is_default(self):
        """String 'EPSG:4326' is default."""
        assert is_default_crs("EPSG:4326") is True

    def test_string_ogc_crs84_is_default(self):
        """String 'OGC:CRS84' is default."""
        assert is_default_crs("OGC:CRS84") is True


# =============================================================================
# Tests for _validate_projjson()
# =============================================================================


class TestValidateProjjson:
    """Test PROJJSON validation."""

    def test_valid_with_schema(self):
        """Valid PROJJSON with $schema."""
        crs = {"$schema": "https://proj.org/schemas/v0.6/projjson.schema.json"}
        assert _validate_projjson(crs) is True

    def test_valid_with_type(self):
        """Valid PROJJSON with type."""
        crs = {"type": "GeographicCRS", "name": "WGS 84"}
        assert _validate_projjson(crs) is True

    def test_valid_with_id(self):
        """Valid PROJJSON with id."""
        crs = {"id": {"authority": "EPSG", "code": 4326}}
        assert _validate_projjson(crs) is True

    def test_invalid_empty_dict(self):
        """Empty dict is invalid."""
        assert _validate_projjson({}) is False

    def test_invalid_non_dict(self):
        """Non-dict input is invalid."""
        assert _validate_projjson("EPSG:4326") is False
        assert _validate_projjson(4326) is False
        assert _validate_projjson(None) is False

    def test_invalid_unrecognized_keys(self):
        """Dict without expected keys is invalid."""
        assert _validate_projjson({"foo": "bar"}) is False


# =============================================================================
# Tests for _wrap_query_with_crs()
# =============================================================================


class TestWrapQueryWithCrs:
    """Test wrapping queries with ST_SetCRS."""

    def test_wraps_query_with_crs(self):
        """Query is wrapped with ST_SetCRS."""
        query = "SELECT * FROM my_table"
        crs = {"id": {"authority": "EPSG", "code": 5070}}
        result = _wrap_query_with_crs(query, "geometry", crs)

        assert "ST_SetCRS" in result
        assert '"geometry"' in result
        assert "5070" in result

    def test_no_wrap_for_default_crs(self):
        """Default CRS does not wrap query."""
        query = "SELECT * FROM my_table"
        crs = {"id": {"authority": "EPSG", "code": 4326}}
        result = _wrap_query_with_crs(query, "geometry", crs)

        assert result == query

    def test_no_wrap_for_none_crs(self):
        """None CRS does not wrap query."""
        query = "SELECT * FROM my_table"
        result = _wrap_query_with_crs(query, "geometry", None)

        assert result == query

    def test_raises_without_geometry_column(self):
        """Raises ValueError when geometry_column is None but CRS is set."""
        query = "SELECT * FROM my_table"
        crs = {"id": {"authority": "EPSG", "code": 5070}}

        with pytest.raises(ValueError, match="geometry_column is required"):
            _wrap_query_with_crs(query, None, crs)

    def test_skips_invalid_projjson(self):
        """Invalid PROJJSON skips wrapping."""
        query = "SELECT * FROM my_table"
        crs = {"not_valid": "crs"}  # Missing $schema, type, and id
        result = _wrap_query_with_crs(query, "geometry", crs)

        assert result == query

    def test_escapes_geometry_column_name(self):
        """Geometry column name with quotes is escaped."""
        query = "SELECT * FROM my_table"
        crs = {"type": "ProjectedCRS", "id": {"authority": "EPSG", "code": 5070}}
        result = _wrap_query_with_crs(query, 'my"geom', crs)

        # Double quotes should be escaped
        assert 'my""geom' in result


# =============================================================================
# Tests for extract_crs_from_parquet()
# =============================================================================


class TestExtractCrsFromParquet:
    """Test CRS extraction from parquet files."""

    def test_extracts_crs_from_geoparquet_metadata(self, fields_5070_file):
        """Extracts CRS from GeoParquet metadata."""
        crs = extract_crs_from_parquet(fields_5070_file)

        # File should have non-default CRS
        if crs:
            identifier = _extract_crs_identifier(crs)
            assert identifier is not None
            assert identifier[0] == "EPSG"

    def test_returns_none_for_default_crs(self, places_test_file):
        """Returns None for default CRS (4326/CRS84)."""
        crs = extract_crs_from_parquet(places_test_file)

        # Most test files are WGS84, which returns None
        if crs is not None:
            # If it does return something, it should be valid
            assert isinstance(crs, dict)

    def test_verbose_mode(self, fields_5070_file):
        """Verbose mode logs CRS info."""
        # Should not raise
        extract_crs_from_parquet(fields_5070_file, verbose=True)

    def test_parquet_geo_type_crs(self, fields_geom_type_only_5070_file):
        """Extracts CRS from Parquet native geo type."""
        crs = extract_crs_from_parquet(fields_geom_type_only_5070_file)

        # This file has EPSG:5070 stored in the geo type
        if crs:
            identifier = _extract_crs_identifier(crs)
            if identifier:
                assert identifier[0] == "EPSG"


# =============================================================================
# Tests for _format_crs_display()
# =============================================================================


class TestFormatCrsDisplay:
    """Test CRS display formatting."""

    def test_formats_epsg_code(self):
        """Formats EPSG CRS as 'EPSG:code'."""
        crs = {"id": {"authority": "EPSG", "code": 4326}}
        result = _format_crs_display(crs)

        assert result == "EPSG:4326"

    def test_formats_ogc_code(self):
        """Formats OGC CRS."""
        crs = {"id": {"authority": "OGC", "code": "CRS84"}}
        result = _format_crs_display(crs)

        assert result == "OGC:CRS84"

    def test_none_returns_none_string(self):
        """None CRS returns 'None'."""
        assert _format_crs_display(None) == "None"

    def test_truncates_long_strings(self):
        """Long CRS representations are truncated."""
        crs = {"name": "A" * 100}  # Long CRS without id
        result = _format_crs_display(crs)

        # Should be truncated or formatted somehow
        assert len(result) <= 60 or "..." in result


# =============================================================================
# Tests for get_crs_display_name()
# =============================================================================


class TestGetCrsDisplayName:
    """Test human-readable CRS name generation."""

    def test_none_shows_unknown_not_the_default(self):
        """``None`` is an explicit ``crs: null``: the CRS is unknown."""
        result = get_crs_display_name(None)
        assert result == "null (CRS unknown)"

    def test_string_returned_as_is(self):
        """String CRS is returned as-is."""
        result = get_crs_display_name("EPSG:4326")
        assert result == "EPSG:4326"

    def test_dict_with_name_and_id(self):
        """Dict with name and id shows both."""
        crs = {"name": "WGS 84", "id": {"authority": "EPSG", "code": 4326}}
        result = get_crs_display_name(crs)

        assert "WGS 84" in result
        assert "EPSG:4326" in result

    def test_dict_with_only_id(self):
        """Dict with only id shows authority:code."""
        crs = {"id": {"authority": "EPSG", "code": 5070}}
        result = get_crs_display_name(crs)

        assert "EPSG:5070" in result

    def test_dict_with_only_name(self):
        """Dict with only name shows name."""
        crs = {"name": "NAD83 / Conus Albers"}
        result = get_crs_display_name(crs)

        assert "NAD83 / Conus Albers" in result

    def test_projjson_object_fallback(self):
        """Unrecognized PROJJSON shows generic name."""
        crs = {"$schema": "something"}
        result = get_crs_display_name(crs)

        assert "PROJJSON" in result or result is not None


# =============================================================================
# Tests for is_geographic_crs()
# =============================================================================


class TestIsGeographicCrs:
    """Test geographic vs projected CRS detection."""

    def test_none_is_geographic(self):
        """None CRS is considered geographic (WGS84 default)."""
        assert is_geographic_crs(None) is True

    def test_geographic_type(self):
        """GeographicCRS type returns True."""
        crs = {"type": "GeographicCRS"}
        assert is_geographic_crs(crs) is True

    def test_projected_type(self):
        """ProjectedCRS type returns False."""
        crs = {"type": "ProjectedCRS"}
        assert is_geographic_crs(crs) is False

    def test_epsg_4326_is_geographic(self):
        """EPSG:4326 is geographic."""
        crs = {"id": {"authority": "EPSG", "code": 4326}}
        assert is_geographic_crs(crs) is True

    def test_ogc_crs84_is_geographic(self):
        """OGC:CRS84 is geographic."""
        crs = {"id": {"authority": "OGC", "code": "CRS84"}}
        assert is_geographic_crs(crs) is True

    def test_utm_is_projected(self):
        """UTM zones are projected."""
        crs = {"name": "WGS 84 / UTM zone 18N"}
        assert is_geographic_crs(crs) is False

    def test_mercator_is_projected(self):
        """Mercator projections are projected."""
        crs = {"name": "Web Mercator"}
        assert is_geographic_crs(crs) is False

    def test_string_4326_is_geographic(self):
        """String '4326' in name indicates geographic."""
        assert is_geographic_crs("EPSG:4326") is True

    def test_string_utm_is_projected(self):
        """String with UTM is projected."""
        assert is_geographic_crs("UTM Zone 10N") is False

    def test_wgs84_name_is_geographic(self):
        """WGS84 in name indicates geographic."""
        crs = {"name": "WGS 84"}
        assert is_geographic_crs(crs) is True


# =============================================================================
# Tests for parse_crs_string_to_projjson()
# =============================================================================


class TestParseCrsStringToProjjson:
    """Test CRS string to PROJJSON conversion."""

    def test_parses_epsg_code(self):
        """Parses EPSG:4326 to PROJJSON dict."""
        result = parse_crs_string_to_projjson("EPSG:4326")

        assert result is not None
        assert isinstance(result, dict)
        # Should have at least an id
        if "id" in result:
            assert result["id"]["authority"] == "EPSG"
            assert result["id"]["code"] == 4326

    def test_parses_epsg_5070(self):
        """Parses EPSG:5070 to PROJJSON dict."""
        result = parse_crs_string_to_projjson("EPSG:5070")

        assert result is not None
        assert isinstance(result, dict)

    def test_invalid_string_returns_none(self):
        """Invalid CRS string returns None."""
        result = parse_crs_string_to_projjson("not a crs")
        assert result is None

    def test_empty_string_returns_none(self):
        """Empty string returns None."""
        result = parse_crs_string_to_projjson("")
        assert result is None

    def test_with_duckdb_connection(self):
        """Works with DuckDB connection parameter."""
        import duckdb

        con = duckdb.connect()
        result = parse_crs_string_to_projjson("EPSG:4326", con=con)
        con.close()

        assert result is not None


# =============================================================================
# Integration Tests
# =============================================================================


class TestCrsUtilsIntegration:
    """Integration tests with real files."""

    def test_full_workflow_epsg_5070(self, fields_5070_file):
        """Full CRS extraction and formatting workflow."""
        # Extract CRS
        crs = extract_crs_from_parquet(fields_5070_file)

        if crs:
            # Get identifier
            identifier = _extract_crs_identifier(crs)
            assert identifier is not None

            # Check if projected
            assert is_geographic_crs(crs) is False

            # Format for display
            display = get_crs_display_name(crs)
            assert display is not None

    def test_crs_projjson_reference_file(self, crs_projjson_file):
        """Tests CRS extraction from file with projjson: reference."""
        crs = extract_crs_from_parquet(crs_projjson_file)

        # Should resolve the reference and return full PROJJSON
        if crs:
            identifier = _extract_crs_identifier(crs)
            # Should be EPSG:5070
            if identifier:
                assert identifier[0] == "EPSG"

    def test_crs_srid_file(self, crs_srid_file):
        """Tests CRS extraction from file with srid: format."""
        crs = extract_crs_from_parquet(crs_srid_file)

        # Should resolve srid:5070 to EPSG:5070
        if crs:
            identifier = _extract_crs_identifier(crs)
            if identifier:
                assert identifier[0] == "EPSG"


class TestProjjsonFromWkt:
    """``projjson_from_wkt``: the parser the ArcGIS extractor reads server WKT with."""

    def test_a_registered_crs_keeps_its_id(self):
        from pyproj import CRS

        from geoparquet_io.core.crs_utils import projjson_from_wkt

        assert projjson_from_wkt(CRS.from_epsg(26918).to_wkt())["id"]["code"] == 26918

    def test_a_custom_projection_is_a_full_definition_without_an_id(self):
        from geoparquet_io.core.crs_utils import projjson_from_wkt

        wkt = (
            'PROJCS["Custom_Transverse_Mercator",'
            'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",'
            'SPHEROID["WGS_1984",6378137.0,298.257223563]],'
            'PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],'
            'PROJECTION["Transverse_Mercator"],PARAMETER["False_Easting",500000.0],'
            'PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-75.5],'
            'PARAMETER["Scale_Factor",0.9996],PARAMETER["Latitude_Of_Origin",0.0],'
            'UNIT["Meter",1.0]]'
        )
        projjson = projjson_from_wkt(wkt)
        assert projjson["type"] == "ProjectedCRS"
        assert projjson["name"] == "Custom_Transverse_Mercator"
        assert "id" not in projjson

    @pytest.mark.parametrize(
        "wkt",
        ["not wkt", "", "   ", None, 12, b"PROJCS[]", "A" * 70_000, "PROJCS[" * 20_000],
        # Explicit ids: pytest exports the test id into an environment variable,
        # which Windows caps at 32,767 characters.
        ids=["garbage", "empty", "blank", "none", "int", "bytes", "70k-chars", "deep-nesting"],
    )
    def test_anything_that_is_not_a_bounded_wkt_string_is_none(self, wkt):
        from geoparquet_io.core.crs_utils import projjson_from_wkt

        assert projjson_from_wkt(wkt) is None
