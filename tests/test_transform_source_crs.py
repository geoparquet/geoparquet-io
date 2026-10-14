"""``transform_source_crs`` is the one owner of "the source CRS of a transform" (#1264).

Before it, four helpers turned a file's CRS into an ``ST_Transform`` source and
disagreed. The authority-code-only helper read a projected PROJJSON with no
``id`` as lon/lat (#1258), the stream reader understood EPSG only (#1261) and
the table readers ignored the GeoArrow field (#1262). These tests pin the
contract every transform path now shares.

- ``None`` only for coordinates already in lon/lat.
- ``"AUTH:CODE"`` when the id resolves in pyproj.
- pyproj's canonical PROJJSON when there is no usable id but the body parses.
- :class:`UnresolvableCRSError` otherwise, never a silent "assume lon/lat".
"""

from __future__ import annotations

import json

import duckdb
import pytest
from pyproj import CRS

from geoparquet_io.core.crs_utils import (
    CRS_ABSENT,
    UnresolvableCRSError,
    crs_authority_code,
    reproject_to_source_sql,
    transform_geom_sql,
    transform_source_crs,
    transform_source_label,
)
from geoparquet_io.core.exceptions import GeoParquetError

TM_PROJ4 = (
    "+proj=tmerc +lat_0=36 +lon_0=139.833333 +k=0.9999 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs"
)
#: A point near Tokyo in the Transverse Mercator above, and where it lands in lon/lat.
TM_XY = (6000.0, -33000.0)
TOKYO_LONLAT = (139.8996, 35.7025)


def _uncoded(crs: CRS) -> dict:
    body = crs.to_json_dict()
    body.pop("id", None)
    return body


@pytest.fixture(scope="module")
def tm_body() -> dict:
    return _uncoded(CRS.from_proj4(TM_PROJ4))


def _transform_point(src: str, xy=TM_XY):
    """Run the SQL ``transform_geom_sql`` builds on one point, return (x, y)."""
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial")
        con.execute("LOAD spatial")
        con.execute("SET geometry_always_xy = true")
        expr = transform_geom_sql(f"ST_Point({xy[0]}, {xy[1]})", src)
        return con.execute(f"SELECT ST_X(g), ST_Y(g) FROM (SELECT {expr} AS g)").fetchone()
    finally:
        con.close()


class TestLonLatInputNeedsNoTransform:
    @pytest.mark.parametrize(
        "crs",
        [
            None,
            "",
            {},
            CRS_ABSENT,
            "EPSG:4326",
            "OGC:CRS84",
            "srid:4326",
            "urn:ogc:def:crs:OGC:1.3:CRS84",
        ],
        ids=repr,
    )
    def test_absent_or_default_is_none(self, crs):
        assert transform_source_crs(crs) is None

    def test_coded_default_projjson_is_none(self):
        assert transform_source_crs(CRS.from_epsg(4326).to_json_dict()) is None

    def test_uncoded_wgs84_twin_is_none(self):
        """An id-less WGS 84 body is pyproj-equal to CRS84 ignoring axis order."""
        assert transform_source_crs(_uncoded(CRS.from_epsg(4326))) is None

    def test_uncoded_wgs84_twin_as_json_string_is_none(self):
        assert transform_source_crs(json.dumps(_uncoded(CRS.from_epsg(4326)))) is None


class TestAuthorityCodes:
    def test_coded_projjson_returns_its_code(self):
        assert transform_source_crs(CRS.from_epsg(5070).to_json_dict()) == "EPSG:5070"

    def test_id_only_dict_returns_its_code(self):
        assert transform_source_crs({"id": {"authority": "EPSG", "code": 5070}}) == "EPSG:5070"

    def test_authority_code_string(self):
        assert transform_source_crs("EPSG:28992") == "EPSG:28992"

    def test_esri_code(self):
        assert transform_source_crs(CRS.from_user_input("ESRI:54009").to_json_dict()) == (
            "ESRI:54009"
        )

    def test_urn_string(self):
        assert transform_source_crs("urn:ogc:def:crs:EPSG::3857") == "EPSG:3857"

    def test_compound_without_id_uses_its_horizontal_component(self):
        compound = CRS.from_user_input("EPSG:3301+EPSG:5705").to_json_dict()
        compound.pop("id", None)
        assert transform_source_crs(compound) == "EPSG:3301"

    def test_unknown_authority_code_string_raises(self):
        with pytest.raises(UnresolvableCRSError, match="EPSG:999999"):
            transform_source_crs("EPSG:999999")


class TestUncodedBodies:
    """The #1258 case: a projected body with no id is reprojected, not read as lon/lat."""

    def test_uncoded_projected_returns_canonical_projjson(self, tm_body):
        src = transform_source_crs(tm_body)
        assert src is not None
        assert src == CRS.from_json_dict(tm_body).to_json()

    def test_uncoded_projected_transforms_to_tokyo(self, tm_body):
        x, y = _transform_point(transform_source_crs(tm_body))
        assert (x, y) == pytest.approx(TOKYO_LONLAT, abs=1e-3)

    def test_json_string_body_matches_dict_body(self, tm_body):
        assert transform_source_crs(json.dumps(tm_body)) == transform_source_crs(tm_body)

    def test_idempotent_on_its_own_output(self, tm_body):
        once = transform_source_crs(tm_body)
        assert transform_source_crs(once) == once

    def test_bound_crs_custom_datum(self):
        bound = _uncoded(
            CRS.from_proj4(
                "+proj=longlat +a=6378206.4 +rf=294.9786982 +towgs84=-100,200,300,0,0,0,0 +no_defs"
            )
        )
        src = transform_source_crs(bound)
        assert src is not None and src.startswith("{")
        assert json.loads(src)["type"] == "BoundCRS"

    def test_bogus_id_with_valid_body_uses_the_body_and_warns(self, tm_body, caplog):
        crs = dict(tm_body, id={"authority": "MAPINFO", "code": 9999})
        with caplog.at_level("WARNING"):
            src = transform_source_crs(crs)
        assert src is not None and "MAPINFO" not in src
        assert "MAPINFO:9999" in caplog.text
        x, y = _transform_point(src)
        assert (x, y) == pytest.approx(TOKYO_LONLAT, abs=1e-3)

    def test_a_quote_in_the_name_survives_one_escape(self, tm_body):
        src = transform_source_crs(dict(tm_body, name="O'Brien grid"))
        assert "O'Brien" in src
        x, y = _transform_point(src)
        assert (x, y) == pytest.approx(TOKYO_LONLAT, abs=1e-3)

    def test_wkt_string_is_accepted(self):
        src = transform_source_crs(CRS.from_proj4(TM_PROJ4).to_wkt())
        x, y = _transform_point(src)
        assert (x, y) == pytest.approx(TOKYO_LONLAT, abs=1e-3)

    def test_coded_wkt_returns_its_code(self):
        assert transform_source_crs(CRS.from_epsg(3857).to_wkt()) == "EPSG:3857"


class TestUnresolvableFailsLoudly:
    @pytest.mark.parametrize(
        "crs",
        [
            "WGS 84",
            "4326",
            "EPSG:12 34",
            'GEOGCS["WGS 84",DATUM["WGS_1984"]]',
            "+proj=tmerc +lat_0=36",
            "{not json",
            {"type": "ProjectedCRS"},
            {"id": {"authority": "EPSG", "code": "'||(SELECT '4326')||'"}},
            {"id": {"authority": "EPSG'||x||'", "code": 3857}},
            12345,
            [1, 2],
            {"id": {"authority": "NOPE", "code": "1"}},
            {"type": "ProjectedCRS", "name": "x", "bad": {1, 2}},
        ],
        ids=repr,
    )
    def test_raises_with_a_reproject_hint(self, crs):
        with pytest.raises(UnresolvableCRSError, match="--src-crs") as excinfo:
            transform_source_crs(crs)
        assert isinstance(excinfo.value, GeoParquetError)

    def test_an_oversized_body_is_refused_before_pyproj(self, tm_body):
        with pytest.raises(UnresolvableCRSError, match="too large"):
            transform_source_crs(dict(tm_body, remarks="x" * 300_000))

    def test_the_message_quotes_the_value_as_data(self):
        with pytest.raises(UnresolvableCRSError) as excinfo:
            transform_source_crs("X:1'||(SELECT 'pwned')||'")
        assert "(SELECT 'pwned')" in str(excinfo.value)


class TestSinks:
    def test_transform_geom_sql_escapes_exactly_once(self, tm_body):
        sql = transform_geom_sql('"g"', dict(tm_body, name="O'Brien grid"))
        assert "O''Brien" in sql and "O''''Brien" not in sql

    def test_transform_geom_sql_leaves_lon_lat_alone(self):
        assert transform_geom_sql('"g"', None) == '"g"'

    def test_reproject_to_source_sql_uses_the_resolver(self, tm_body):
        sql = reproject_to_source_sql('"g"', tm_body)
        assert sql.startswith("ST_Transform(\"g\", 'OGC:CRS84', '{")

    def test_reproject_to_source_sql_raises_on_garbage(self):
        with pytest.raises(UnresolvableCRSError):
            reproject_to_source_sql('"g"', "WGS 84")


class TestAuthorityCodeForComparison:
    """``crs_authority_code`` (the old ``crs_string_for_transform``) stays code-only (#1014)."""

    def test_uncoded_body_has_no_code(self, tm_body):
        assert crs_authority_code(tm_body) is None

    def test_coded_body(self):
        assert crs_authority_code(CRS.from_epsg(5070).to_json_dict()) == "EPSG:5070"

    def test_free_form_is_none(self):
        assert crs_authority_code("WGS 84") is None


class TestCompoundDefaults:
    def test_compound_over_wgs84_without_id_needs_no_transform(self):
        body = CRS.from_user_input("EPSG:4326+3855").to_json_dict()
        body.pop("id", None)
        assert body["type"] == "CompoundCRS"
        assert transform_source_crs(body) is None


class TestLabel:
    def test_lon_lat(self):
        assert transform_source_label(None) == "OGC:CRS84"

    def test_code_is_shown_as_is(self):
        assert transform_source_label("EPSG:3857") == "EPSG:3857"

    def test_projjson_is_named(self, tm_body):
        label = transform_source_label(transform_source_crs(tm_body))
        assert label.endswith("(no authority code)")

    def test_unnamed_projjson(self):
        label = transform_source_label('{"type": "ProjectedCRS"}')
        assert label == "unnamed CRS (no authority code)"

    def test_a_user_value_that_is_not_json_is_shown_bounded(self):
        assert transform_source_label("{" + "x" * 500).endswith("...")
