"""Tests for the tests/geo_schema_helpers.py test-support parser."""

from tests.geo_schema_helpers import parse_geometry_type_from_schema


class TestParseGeometryTypeFromSchema:
    """Tests for parse_geometry_type_from_schema function."""

    def test_parses_simple_geometry(self):
        """Test parsing simple Geometry type."""
        schema_str = "required group geometry (Geometry(Point, XY))"
        result = parse_geometry_type_from_schema("geometry", schema_str)
        assert result is not None
        assert result.get("geometry_type") == "Point"
        assert result.get("coordinate_dimension") == "XY"

    def test_parses_geography_with_crs(self):
        """Test parsing Geography type with CRS."""
        schema_str = 'required group geometry (Geography(Polygon, XY, crs="OGC:CRS84"))'
        result = parse_geometry_type_from_schema("geometry", schema_str)
        assert result is not None
        assert result.get("geometry_type") == "Polygon"
        assert result.get("crs") == "OGC:CRS84"

    def test_parses_geography_with_algorithm(self):
        """Test parsing Geography type with algorithm."""
        schema_str = "required group geometry (Geography(Point, XY, algorithm=spherical))"
        result = parse_geometry_type_from_schema("geometry", schema_str)
        assert result is not None
        assert result.get("algorithm") == "spherical"

    def test_parses_xyz_coordinate_dimension(self):
        """Test parsing XYZ coordinate dimension."""
        schema_str = "required group geometry (Geometry(LineString, XYZ))"
        result = parse_geometry_type_from_schema("geometry", schema_str)
        assert result is not None
        assert result.get("geometry_type") == "LineString"
        assert result.get("coordinate_dimension") == "XYZ"

    def test_parses_multipolygon(self):
        """Test parsing MultiPolygon geometry type."""
        schema_str = "required group geometry (Geometry(MultiPolygon, XY))"
        result = parse_geometry_type_from_schema("geometry", schema_str)
        assert result is not None
        assert result.get("geometry_type") == "MultiPolygon"

    def test_returns_none_for_non_geo_field(self):
        """Test returns None for non-geometry field."""
        schema_str = "optional binary name (STRING)"
        result = parse_geometry_type_from_schema("name", schema_str)
        assert result is None

    def test_handles_json_crs(self):
        """Test handling of JSON CRS object."""
        crs_json = '{"type": "PROJCRS", "name": "NAD83"}'
        schema_str = f"required group geometry (Geometry(Polygon, XY, crs={crs_json}))"
        result = parse_geometry_type_from_schema("geometry", schema_str)
        assert result is not None
        assert result["crs"] == {"type": "PROJCRS", "name": "NAD83"}

    def test_parses_geometry_collection(self):
        """Test parsing GeometryCollection type."""
        schema_str = "required group geometry (Geometry(GeometryCollection, XY))"
        result = parse_geometry_type_from_schema("geometry", schema_str)
        assert result is not None
        assert result.get("geometry_type") == "GeometryCollection"
