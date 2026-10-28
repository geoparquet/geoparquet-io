"""File-controlled CRS strings must never reach transform SQL unescaped.

#866/#870 made the Parquet geo logical-type parser carry *arbitrary* free-form
``crs`` strings through (so validation can fail closed on them, instead of
misreading them as the OGC:CRS84 default). Before that, only quote-free
``<authority>:<code>`` strings could come out of the parser, and the raw
``'{source_crs}'`` interpolations in the ST_Transform sinks were safe by
accident. Two layers now enforce what used to be incidental:

1. :func:`transform_source_crs` -- the one resolver every ST_Transform source
   funnels through (#1264) -- only ever returns the strict
   ``<authority>:<code>`` shape or PROJJSON that pyproj wrote itself, and
   raises for anything else. A free-form value stays available for
   validation, but never becomes a transform argument.
2. The ST_Transform sinks (``core/reproject.py``, ``core/geojson_stream.py``)
   escape whatever they interpolate via ``_escape_sql_string``, because the
   reproject path may legitimately pass a PROJJSON *string* to ST_Transform.

The probe values below were verified against the unfixed branch: a file whose
geo type declares ``crs=EPSG:'||(SELECT '4326')||'`` reprojected successfully
(the injected subquery evaluated to ``EPSG:4326``), and ``X:1'||(SELECT
'pwned')||'`` failed with "Could not create projection: X:1PWNED" -- i.e. the
attacker-controlled SQL executed inside the query.
"""

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyproj import CRS as PyprojCRS

from geoparquet_io.core.crs_utils import (
    UnresolvableCRSError,
    crs_authority_code,
    transform_source_crs,
)
from geoparquet_io.core.duckdb_metadata import parse_geometry_logical_type
from geoparquet_io.core.duckdb_utils import _escape_sql_string
from geoparquet_io.core.geojson_stream import _build_feature_query
from geoparquet_io.core.reproject import reproject_impl, reproject_table
from geoparquet_io.core.validate import CheckStatus, validate_geoparquet

#: Values verified to execute as SQL on the unfixed branch (see module docstring).
INJECTION_CRS_VALUES = [
    "EPSG:'||(SELECT '4326')||'",
    "X:1'||(SELECT 'pwned')||'",
]

#: Free-form values a Parquet writer may legally emit; none is an authority code.
FREEFORM_CRS_VALUES = [
    'GEOGCS["WGS 84",DATUM["WGS_1984"]]',
    "EPSG:12 34",  # colon-bearing, but not the quote-free authority-code shape
    "not a coordinate reference system",
]

#: Free-form values pyproj reads as a CRS that is not lon/lat WGS 84. They
#: resolve the way the GeoArrow field reader does, to a code PROJ is sure of
#: or to pyproj's own PROJJSON, and the raw string never reaches SQL.
PARSED_FREEFORM_CRS_VALUES = [
    "+proj=tmerc +lat_0=36",
    "NAD83",
    "+proj=longlat +datum=NAD83",
    "Tokyo",
]

#: Free-form values that pyproj reads as lon/lat WGS 84. They need no transform,
#: so they resolve to ``None`` and never reach SQL (DuckDB ``ST_SetCRS`` writes
#: such strings into the native GEOMETRY type).
LONLAT_FREEFORM_CRS_VALUES = [
    "4326",
    "WGS 84",
    "WGS84",
    "+proj=longlat +datum=WGS84 +no_defs",
]


def _write_geo_type_crs_file(tmp_path, name: str, parquet_crs: str, metadata_crs) -> str:
    """Write a real file whose Parquet geo type carries ``parquet_crs`` verbatim.

    geoarrow-pyarrow writes the CRS string onto the GEOMETRY logical type
    unchanged, which is exactly how a hostile or merely free-form value arrives
    in the wild. ``metadata_crs="ABSENT"`` omits the geo-metadata ``crs`` key so
    detection falls through to the Parquet geo type.
    """
    import geoarrow.pyarrow as ga
    import shapely

    wkb = [bytes(w) for w in shapely.to_wkb(shapely.points([[500000.0, 5000000.0]]))]
    array = ga.wkb().with_crs(parquet_crs).wrap_array(pa.array(wkb, type=pa.binary()))
    col_meta: dict = {"encoding": "WKB", "geometry_types": ["Point"]}
    if metadata_crs != "ABSENT":
        col_meta["crs"] = metadata_crs
    geo = {"version": "2.0.0", "primary_column": "geometry", "columns": {"geometry": col_meta}}
    table = pa.table({"geometry": array})
    table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode("utf-8")})
    out = tmp_path / name
    pq.write_table(table, out)
    return str(out)


# =============================================================================
# Layer 1: the transform choke point only returns the authority-code shape
# =============================================================================


class TestTransformSourceRejectsFreeform:
    @pytest.mark.parametrize("value", INJECTION_CRS_VALUES)
    def test_the_verified_probe_values_raise(self, value):
        with pytest.raises(UnresolvableCRSError):
            transform_source_crs(value)

    @pytest.mark.parametrize("value", FREEFORM_CRS_VALUES)
    def test_free_form_values_raise(self, value):
        with pytest.raises(UnresolvableCRSError):
            transform_source_crs(value)

    @pytest.mark.parametrize("value", LONLAT_FREEFORM_CRS_VALUES)
    def test_lon_lat_free_form_values_need_no_transform(self, value):
        assert transform_source_crs(value) is None

    @pytest.mark.parametrize("value", PARSED_FREEFORM_CRS_VALUES)
    def test_parsed_free_form_values_never_come_back_raw(self, value):
        src = transform_source_crs(value)
        assert src != value
        assert src.startswith("{") or crs_authority_code(src) == src

    @pytest.mark.parametrize(
        "value",
        INJECTION_CRS_VALUES
        + FREEFORM_CRS_VALUES
        + PARSED_FREEFORM_CRS_VALUES
        + LONLAT_FREEFORM_CRS_VALUES,
    )
    def test_free_form_values_name_no_authority_code(self, value):
        assert crs_authority_code(value) is None

    def test_a_projjson_dict_with_an_injected_code_raises(self):
        crs = {"id": {"authority": "EPSG", "code": "'||(SELECT '4326')||'"}}
        assert crs_authority_code(crs) is None
        with pytest.raises(UnresolvableCRSError):
            transform_source_crs(crs)

    def test_a_projjson_dict_with_an_injected_authority_raises(self):
        crs = {"id": {"authority": "EPSG'||x||'", "code": 3857}}
        assert crs_authority_code(crs) is None
        with pytest.raises(UnresolvableCRSError):
            transform_source_crs(crs)

    def test_legitimate_authority_codes_still_pass(self):
        assert transform_source_crs("EPSG:3857") == "EPSG:3857"
        assert transform_source_crs("EPSG:28992") == "EPSG:28992"

    def test_a_legitimate_projjson_dict_still_passes(self):
        crs = {"id": {"authority": "EPSG", "code": 5070}}
        assert transform_source_crs(crs) == "EPSG:5070"


# =============================================================================
# Layer 2: the ST_Transform sinks escape what they interpolate
# =============================================================================


class TestGeojsonFeatureQueryEscapesSourceCrs:
    def test_a_quote_bearing_source_crs_is_escaped_in_the_query(self):
        injected = INJECTION_CRS_VALUES[0]
        query = _build_feature_query(
            "'input.parquet'", "geometry", [], source_crs=injected, repair_geometry=False
        )
        assert f"'{injected}'" not in query  # the raw, executable form
        assert _escape_sql_string(injected) in query  # the inert literal


class TestReprojectInjectionIsInert:
    """End to end: the injected SQL must not execute -- reproject must FAIL.

    On the unfixed branch the first probe file reprojects *successfully*
    because ``(SELECT '4326')`` runs inside the query and evaluates to a valid
    CRS. A clean failure whose message carries the un-evaluated literal is the
    proof the value arrived as data, not SQL.
    """

    def test_file_reproject_of_an_injected_geo_type_crs_fails_cleanly(self, tmp_path):
        path = _write_geo_type_crs_file(
            tmp_path, "inject.parquet", "EPSG:'||(SELECT '4326')||'", "ABSENT"
        )
        out = str(tmp_path / "out.parquet")
        with pytest.raises(Exception) as excinfo:
            reproject_impl(path, out, target_crs="EPSG:3857")
        # The subquery arrived as data: it is still in the message, un-evaluated.
        assert "(SELECT '4326')" in str(excinfo.value)

    def test_table_reproject_of_an_injected_metadata_crs_fails_cleanly(self):
        import shapely

        wkb = [bytes(w) for w in shapely.to_wkb(shapely.points([[500000.0, 5000000.0]]))]
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", "crs": "X:1'||(SELECT 'pwned')||'"}},
        }
        table = pa.table({"geometry": pa.array(wkb, type=pa.binary())})
        table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode("utf-8")})
        with pytest.raises(Exception) as excinfo:
            reproject_table(table, target_crs="EPSG:4326")
        # On the unfixed branch this concatenates to "X:1PWNED" -- the injected
        # SQL executed. Fixed, the quoted subquery survives as an inert literal
        # (the identifier path upper-cases the carried string, hence lower()).
        assert "X:1PWNED" not in str(excinfo.value)
        assert "'||(select 'pwned')||'" in str(excinfo.value).lower()

    def test_a_legitimate_epsg_3857_file_still_reprojects(self, tmp_path):
        path = _write_geo_type_crs_file(
            tmp_path,
            "webmercator.parquet",
            "EPSG:3857",
            PyprojCRS.from_epsg(3857).to_json_dict(),
        )
        out = str(tmp_path / "out.parquet")
        result = reproject_impl(path, out, target_crs="EPSG:4326")
        assert result.feature_count == 1
        assert Path(out).exists()


# =============================================================================
# S3: an unbalanced-brace PROJJSON crs must fail closed, not read as CRS84
# =============================================================================


class TestUnbalancedProjjsonFailsClosed:
    def test_the_parser_carries_the_malformed_value_as_a_string(self):
        result = parse_geometry_logical_type('GeometryType(crs={"type": "GeographicCRS")')
        assert result is not None
        assert "crs" in result
        assert result["crs"] == '{"type": "GeographicCRS"'

    def test_unbalanced_brace_crs_with_absent_metadata_fails_consistency(self, tmp_path):
        """Was PASSED "both are OGC:CRS84" -- a claim the file never made."""
        path = _write_geo_type_crs_file(
            tmp_path, "unbalanced.parquet", '{"type": "GeographicCRS"', "ABSENT"
        )
        result = validate_geoparquet(path, validate_data=False)
        check = next(c for c in result.checks if c.name == "v2_crs_consistency_geometry")
        assert check.status is CheckStatus.FAILED
        assert "GeographicCRS" in (check.details or "")
