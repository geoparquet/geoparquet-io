"""#814: the Parquet geo type's bare ``<authority>:<code>`` CRS must be recognized.

The Parquet geospatial spec lets the ``GEOMETRY``/``GEOGRAPHY`` logical type name
its CRS in four shapes: nothing at all (which the spec defines as OGC:CRS84),
``srid:<id>``, ``projjson:<key>``, and inline PROJJSON — plus the compact
``<authority>:<code>`` form, e.g. ``EPSG:32633``. gpio's parser recognized only
the first four, so a file using the compact form parsed to *no* ``crs`` key,
which reads downstream as the positive claim "this type declares OGC:CRS84".

That made ``_check_v2_crs_consistency`` wrong in both directions: a real mismatch
(geo metadata OGC:CRS84 vs a Parquet type naming EPSG:32633) passed, and a real
match (both naming EPSG:3857) failed. The two prefix forms have the same
``a:b`` shape as the compact one, so the assertions below also pin that
``srid:``/``projjson:`` keep winning over the new branch.

#866 is the same bug one step out: the Parquet spec's ``crs`` field is *free
form*, so ``crs=4326``, ``crs=WGS 84`` or a raw WKT string are all legal values
gpio cannot resolve. Those still fell off the end of the parser as "no crs key",
i.e. as a positive OGC:CRS84 claim. They now come through as unresolved strings
and fail closed, exactly like the unknown authority ``FOO:BAR`` already did.
"""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyproj import CRS as PyprojCRS

from geoparquet_io.core.crs_utils import crs_of
from geoparquet_io.core.duckdb_metadata import (
    get_schema_info,
    parse_geometry_logical_type,
    resolve_crs_reference,
)
from geoparquet_io.core.validate import (
    CheckStatus,
    _check_native_crs_format,
    _check_parquet_geo_only_crs,
    _check_v2_crs_consistency,
    validate_geoparquet,
)

EPSG3857_PROJJSON = PyprojCRS.from_epsg(3857).to_json_dict()
EPSG5070_PROJJSON = PyprojCRS.from_epsg(5070).to_json_dict()
EPSG32633_PROJJSON = PyprojCRS.from_epsg(32633).to_json_dict()
CRS84_ID = {
    "type": "GeographicCRS",
    "name": "WGS 84 (CRS84)",
    "id": {"authority": "OGC", "code": "CRS84"},
}


def _schema(logical_type: str) -> list[dict]:
    return [{"name": "geometry", "logical_type": logical_type}]


def _geo_meta(col_meta: dict) -> dict:
    return {"version": "2.0.0", "primary_column": "geometry", "columns": {"geometry": col_meta}}


# =============================================================================
# The parser: the compact form has to survive parse_geometry_logical_type
# =============================================================================


class TestParserRecognizesTheCompactForm:
    def test_bare_authority_code_is_returned_as_the_crs(self):
        result = parse_geometry_logical_type("GeometryType(crs=EPSG:32633)")
        assert result is not None
        assert result["crs"] == "EPSG:32633"

    def test_compact_form_alongside_positional_params(self):
        result = parse_geometry_logical_type("GeometryType(Point, XY, crs=EPSG:32633)")
        assert result is not None
        assert result["crs"] == "EPSG:32633"
        assert result["geometry_type"] == "Point"
        assert result["coordinate_dimension"] == "XY"

    def test_compact_form_on_a_geography_type_keeps_the_algorithm(self):
        result = parse_geometry_logical_type("GeographyType(crs=OGC:CRS84, algorithm=spherical)")
        assert result is not None
        assert result["crs"] == "OGC:CRS84"
        assert result["algorithm"] == "spherical"

    @pytest.mark.parametrize(
        "logical_type,expected",
        [
            ("GeometryType(crs=srid:5070)", "srid:5070"),
            ("GeometryType(crs=srid:0)", "srid:0"),
            ("GeometryType(crs=projjson:projjson_epsg_5070)", "projjson:projjson_epsg_5070"),
        ],
    )
    def test_prefixed_forms_still_win_over_the_new_branch(self, logical_type, expected):
        """``srid:``/``projjson:`` have the same ``a:b`` shape and keep their meaning."""
        result = parse_geometry_logical_type(logical_type)
        assert result is not None
        assert result["crs"] == expected

    @pytest.mark.parametrize(
        "logical_type",
        [
            "GeometryType(crs=<null>)",
            "GeometryType(crs=)",  # what DuckDB actually emits for a CRS-less type
            "GeographyType(crs=, algorithm=spherical)",
            "GeographyType(algorithm=spherical)",
        ],
    )
    def test_a_type_that_declares_no_crs_still_has_no_crs_key(self, logical_type):
        result = parse_geometry_logical_type(logical_type)
        assert result is not None
        assert "crs" not in result

    def test_inline_projjson_is_still_parsed_into_a_dict(self):
        result = parse_geometry_logical_type(
            'GeometryType(crs={"type": "ProjectedCRS", "id": {"authority": "EPSG", "code": 5070}})'
        )
        assert result is not None
        assert result["crs"]["id"]["code"] == 5070


# =============================================================================
# The resolver: the compact form resolves through pyproj, like srid:
# =============================================================================


class TestResolveCompactForm:
    def test_epsg_authority_code_resolves_to_projjson(self):
        result = resolve_crs_reference("any_file.parquet", "EPSG:32633")
        assert isinstance(result, dict)
        assert result["id"] == {"authority": "EPSG", "code": 32633}

    def test_ogc_crs84_resolves_to_projjson(self):
        result = resolve_crs_reference("any_file.parquet", "OGC:CRS84")
        assert isinstance(result, dict)
        assert str(result.get("id", {}).get("code", "")).upper() == "CRS84"

    def test_srid_still_takes_the_srid_path(self):
        """``srid`` must not be read as an authority name."""
        result = resolve_crs_reference("any_file.parquet", "srid:5070")
        assert isinstance(result, dict)
        assert result.get("id", {}).get("code") == 5070

    def test_uppercase_srid_still_takes_the_srid_path(self):
        """The parser carries ``SRID:5070`` through, so the resolver must match it too."""
        result = resolve_crs_reference("any_file.parquet", "SRID:5070")
        assert isinstance(result, dict)
        assert result.get("id", {}).get("code") == 5070

    @pytest.mark.parametrize("value", ["unknown:format", "EPSG:99999999", "not-a-crs"])
    def test_an_unresolvable_value_is_returned_unchanged(self, value):
        assert resolve_crs_reference("any_file.parquet", value) == value


# =============================================================================
# The verdicts #814 reports as wrong, in both directions
# =============================================================================


class TestV2CrsConsistencyVerdicts:
    def test_metadata_crs84_against_a_compact_utm_type_is_a_mismatch(self):
        """Was PASSED: the unrecognized form read as a positive CRS84 claim."""
        check = _check_v2_crs_consistency(
            _geo_meta({"crs": CRS84_ID}), _schema("GeometryType(crs=EPSG:32633)"), "geometry"
        )
        assert check.status is CheckStatus.FAILED

    def test_omitted_metadata_crs_against_a_compact_utm_type_is_a_mismatch(self):
        """An omitted metadata crs means CRS84, which EPSG:32633 is not."""
        check = _check_v2_crs_consistency(
            _geo_meta({}), _schema("GeometryType(crs=EPSG:32633)"), "geometry"
        )
        assert check.status is CheckStatus.FAILED

    def test_matching_crs_on_both_sides_passes(self):
        """Was FAILED: the same CRS, rejected."""
        check = _check_v2_crs_consistency(
            _geo_meta({"crs": EPSG3857_PROJJSON}),
            _schema("GeometryType(crs=EPSG:3857)"),
            "geometry",
        )
        assert check.status is CheckStatus.PASSED

    def test_compact_crs84_matches_explicit_epsg_4326_metadata(self):
        """CRS84/EPSG:4326 equivalence still holds through the new branch."""
        check = _check_v2_crs_consistency(
            _geo_meta({"crs": PyprojCRS.from_epsg(4326).to_json_dict()}),
            _schema("GeometryType(crs=OGC:CRS84)"),
            "geometry",
        )
        assert check.status is CheckStatus.PASSED

    def test_srid_zero_still_means_unknown_not_an_authority_code(self):
        check = _check_v2_crs_consistency(
            _geo_meta({"crs": None}), _schema("GeometryType(crs=srid:0)"), "geometry"
        )
        assert check.status is CheckStatus.PASSED


class TestSridReferenceInConsistency:
    """Per the Parquet spec, ``srid:<id>`` in the geo context is EPSG:<id>.

    It must resolve like the compact form does, so a file whose metadata carries
    the identical CRS as PROJJSON does not false-fail; ``projjson:<key>`` names
    file metadata this check cannot read, so it stays a conservative mismatch.
    """

    def test_matching_srid_reference_passes(self):
        """Was FAILED: the raw ``srid:5070`` string never equals its own PROJJSON."""
        check = _check_v2_crs_consistency(
            _geo_meta({"crs": EPSG5070_PROJJSON}),
            _schema("GeometryType(crs=srid:5070)"),
            "geometry",
        )
        assert check.status is CheckStatus.PASSED

    def test_mismatching_srid_reference_fails(self):
        check = _check_v2_crs_consistency(
            _geo_meta({"crs": PyprojCRS.from_epsg(4326).to_json_dict()}),
            _schema("GeometryType(crs=srid:5070)"),
            "geometry",
        )
        assert check.status is CheckStatus.FAILED

    def test_uppercase_srid_reference_behaves_like_lowercase(self):
        check = _check_v2_crs_consistency(
            _geo_meta({"crs": EPSG5070_PROJJSON}),
            _schema("GeometryType(crs=SRID:5070)"),
            "geometry",
        )
        assert check.status is CheckStatus.PASSED

    def test_uppercase_srid_zero_still_means_unknown(self):
        check = _check_v2_crs_consistency(
            _geo_meta({"crs": None}), _schema("GeometryType(crs=SRID:0)"), "geometry"
        )
        assert check.status is CheckStatus.PASSED

    def test_unresolvable_srid_reference_still_fails_closed(self):
        check = _check_v2_crs_consistency(
            _geo_meta({"crs": EPSG5070_PROJJSON}),
            _schema("GeometryType(crs=srid:99999999)"),
            "geometry",
        )
        assert check.status is CheckStatus.FAILED

    def test_projjson_reference_is_a_conservative_mismatch(self):
        """The named key cannot be read here, so report a mismatch, never a guess."""
        check = _check_v2_crs_consistency(
            _geo_meta({"crs": EPSG5070_PROJJSON}),
            _schema("GeometryType(crs=projjson:my_crs)"),
            "geometry",
        )
        assert check.status is CheckStatus.FAILED


# =============================================================================
# The parquet-geo-only checks, which now see a CRS where they saw none
# =============================================================================


class TestParquetGeoOnlyChecksSeeTheCompactCrs:
    """A CRS-less type and a compactly-named CRS must no longer report the same."""

    def test_native_crs_format_warns_like_the_other_reference_forms(self):
        """Was PASSED "no CRS (defaults to OGC:CRS84)" — a CRS the file never named."""
        check = _check_native_crs_format(_schema("GeometryType(crs=EPSG:32633)"), "geometry")
        assert check.status is CheckStatus.WARNING
        assert "EPSG:32633" in (check.details or "")

    def test_parquet_geo_only_crs_reports_the_resolved_crs(self):
        check = _check_parquet_geo_only_crs(
            _schema("GeometryType(crs=EPSG:32633)"), "geometry", "any_file.parquet"
        )
        assert check.status is CheckStatus.PASSED
        assert "no CRS" not in check.message

    def test_a_crs_less_type_still_reports_the_default(self):
        check = _check_native_crs_format(_schema("GeometryType(crs=)"), "geometry")
        assert check.status is CheckStatus.PASSED
        assert "defaults to OGC:CRS84" in check.message


# =============================================================================
# End to end on a real file that carries the compact form
# =============================================================================


def _write_compact_crs_file(tmp_path, name: str, parquet_crs: str, metadata_crs) -> str:
    """Write a real file whose Parquet geo type names its CRS as ``authority:code``.

    DuckDB expands an authority code into full PROJJSON when it writes, so the
    compact form has to come from geoarrow-pyarrow, which writes the CRS string
    onto the logical type verbatim.
    """
    import geoarrow.pyarrow as ga
    import shapely

    wkb = [bytes(w) for w in shapely.to_wkb(shapely.points([[500000.0, 5000000.0]]))]
    array = ga.wkb().with_crs(parquet_crs).wrap_array(pa.array(wkb, type=pa.binary()))
    col_meta: dict = {"encoding": "WKB", "geometry_types": ["Point"]}
    if metadata_crs != "ABSENT":  # "ABSENT" omits the key, i.e. the CRS84 default
        col_meta["crs"] = metadata_crs
    geo = {
        "version": "2.0.0",
        "primary_column": "geometry",
        "columns": {"geometry": col_meta},
    }
    table = pa.table({"geometry": array})
    table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode("utf-8")})
    out = tmp_path / name
    pq.write_table(table, out)

    # Non-vacuity: the written file really carries the compact form, not PROJJSON.
    logical = [c["logical_type"] for c in get_schema_info(str(out)) if c["name"] == "geometry"]
    assert logical == [f"GeometryType(crs={parquet_crs})"]
    return str(out)


class TestCompactCrsFileValidatesEndToEnd:
    def test_a_matching_compact_crs_file_passes_consistency(self, tmp_path):
        path = _write_compact_crs_file(
            tmp_path, "compact_match.parquet", "EPSG:32633", EPSG32633_PROJJSON
        )
        result = validate_geoparquet(path, validate_data=False)
        check = next(c for c in result.checks if c.name == "v2_crs_consistency_geometry")
        assert check.status is CheckStatus.PASSED

    def test_a_mismatching_compact_crs_file_fails_consistency(self, tmp_path):
        path = _write_compact_crs_file(tmp_path, "compact_mismatch.parquet", "EPSG:32633", CRS84_ID)
        result = validate_geoparquet(path, validate_data=False)
        check = next(c for c in result.checks if c.name == "v2_crs_consistency_geometry")
        assert check.status is CheckStatus.FAILED
        assert "32633" in (check.details or "") or "UTM" in (check.details or "")

    def test_crs_detection_reads_the_compact_form_off_the_parquet_type(self, tmp_path):
        """A file with no geo metadata still has a real CRS gpio can read."""
        import geoarrow.pyarrow as ga
        import shapely

        wkb = [bytes(w) for w in shapely.to_wkb(shapely.points([[500000.0, 5000000.0]]))]
        array = ga.wkb().with_crs("EPSG:32633").wrap_array(pa.array(wkb, type=pa.binary()))
        out = tmp_path / "compact_no_geo.parquet"
        pq.write_table(pa.table({"geometry": array}), out)

        crs = crs_of(str(out))
        assert isinstance(crs, dict)
        assert crs["id"] == {"authority": "EPSG", "code": 32633}


# =============================================================================
# #866: a free-form crs the parser cannot resolve must never read as CRS84
# =============================================================================

#: Legal Parquet ``crs`` values gpio cannot resolve to a CRS.
#:
#: The Parquet geospatial spec leaves the field free form, so each of these is a
#: value a writer may legitimately emit; none of them is "this type declares no
#: CRS", which is the *only* thing the OGC:CRS84 default may be read from.
FREEFORM_CRS_VALUES = [
    "4326",  # a bare EPSG code, no authority
    "WGS 84",  # a CRS name
    "EPSG:",  # an authority with an empty code
    "EPSG:abc",  # an authority with a code PROJ does not carry
    'GEOGCS["WGS 84",DATUM["WGS_1984"]]',  # WKT1 — note the embedded comma
    'PROJCRS["WGS 84 / UTM zone 33N"]',  # WKT2
]


class TestParserCarriesFreeformCrsThrough:
    @pytest.mark.parametrize("value", FREEFORM_CRS_VALUES)
    def test_the_literal_value_arrives_under_the_crs_key(self, value):
        """Was: no ``crs`` key at all, which callers read as the CRS84 default."""
        result = parse_geometry_logical_type(f"GeometryType(crs={value})")
        assert result is not None
        assert result["crs"] == value

    @pytest.mark.parametrize("value", FREEFORM_CRS_VALUES)
    def test_a_freeform_crs_is_distinguishable_from_declaring_no_crs(self, value):
        declared = parse_geometry_logical_type(f"GeometryType(crs={value})")
        absent = parse_geometry_logical_type("GeometryType(crs=)")
        assert declared is not None and absent is not None
        assert "crs" in declared and "crs" not in absent

    def test_a_freeform_crs_survives_a_trailing_algorithm_parameter(self):
        """The value ends at DuckDB's own ``, algorithm=``, not at the first comma."""
        result = parse_geometry_logical_type(
            'GeographyType(crs=GEOGCS["WGS 84",DATUM["WGS_1984"]], algorithm=spherical)'
        )
        assert result is not None
        assert result["crs"] == 'GEOGCS["WGS 84",DATUM["WGS_1984"]]'
        assert result["algorithm"] == "spherical"

    def test_a_freeform_crs_alongside_positional_params(self):
        result = parse_geometry_logical_type("GeometryType(Point, XY, crs=WGS 84)")
        assert result is not None
        assert result["crs"] == "WGS 84"
        assert result["geometry_type"] == "Point"
        assert result["coordinate_dimension"] == "XY"


class TestFreeformCrsFailsClosedInConsistency:
    @pytest.mark.parametrize("value", FREEFORM_CRS_VALUES)
    def test_an_absent_metadata_crs_is_a_mismatch_not_a_shared_crs84_claim(self, value):
        """Was PASSED "both are OGC:CRS84" — a claim neither carrier ever made."""
        check = _check_v2_crs_consistency(
            _geo_meta({}), _schema(f"GeometryType(crs={value})"), "geometry"
        )
        assert check.status is CheckStatus.FAILED
        assert value in (check.details or "")

    @pytest.mark.parametrize("value", FREEFORM_CRS_VALUES)
    def test_a_named_metadata_crs_is_a_mismatch_showing_the_literal(self, value):
        check = _check_v2_crs_consistency(
            _geo_meta({"crs": EPSG3857_PROJJSON}),
            _schema(f"GeometryType(crs={value})"),
            "geometry",
        )
        assert check.status is CheckStatus.FAILED
        assert value in (check.details or "")
        assert "unrecognized" in (check.details or "")

    @pytest.mark.parametrize("value", FREEFORM_CRS_VALUES)
    def test_an_explicit_crs84_metadata_crs_is_still_a_mismatch(self, value):
        check = _check_v2_crs_consistency(
            _geo_meta({"crs": CRS84_ID}), _schema(f"GeometryType(crs={value})"), "geometry"
        )
        assert check.status is CheckStatus.FAILED


class TestFreeformCrsIsReportedAsAnUnrecognizedForm:
    @pytest.mark.parametrize("value", FREEFORM_CRS_VALUES)
    def test_native_crs_format_warns_with_the_literal(self, value):
        check = _check_native_crs_format(_schema(f"GeometryType(crs={value})"), "geometry")
        assert check.status is CheckStatus.WARNING
        assert value in (check.details or "")

    @pytest.mark.parametrize("value", FREEFORM_CRS_VALUES)
    def test_parquet_geo_only_crs_warns_with_the_literal(self, value):
        """``4326`` must not slip through the is-geographic heuristic as a pass."""
        check = _check_parquet_geo_only_crs(
            _schema(f"GeometryType(crs={value})"), "geometry", "any_file.parquet"
        )
        assert check.status is CheckStatus.WARNING
        assert value in (check.details or "")


class TestRecognizedFormsAreUnchangedByTheFreeformBranch:
    """The four resolvable shapes and the no-CRS shape keep their #851 behavior."""

    @pytest.mark.parametrize(
        "logical_type",
        [
            "GeometryType(crs=<null>)",
            "GeometryType(crs=)",
            "GeographyType(crs=, algorithm=spherical)",
            "GeographyType(crs=<null>, algorithm=spherical)",
            "GeographyType(algorithm=spherical)",
        ],
    )
    def test_no_crs_declared_still_leaves_no_crs_key(self, logical_type):
        result = parse_geometry_logical_type(logical_type)
        assert result is not None
        assert "crs" not in result

    @pytest.mark.parametrize(
        "logical_type,expected",
        [
            ("GeometryType(crs=srid:5070)", "srid:5070"),
            ("GeometryType(crs=srid:0)", "srid:0"),
            ("GeometryType(crs=projjson:my_crs)", "projjson:my_crs"),
            ("GeometryType(crs=EPSG:32633)", "EPSG:32633"),
        ],
    )
    def test_the_reference_forms_still_parse_to_themselves(self, logical_type, expected):
        result = parse_geometry_logical_type(logical_type)
        assert result is not None
        assert result["crs"] == expected

    def test_inline_projjson_still_parses_to_a_dict(self):
        result = parse_geometry_logical_type(
            'GeometryType(crs={"type": "ProjectedCRS", '
            '"id": {"authority": "EPSG", "code": 5070}}, algorithm=planar)'
        )
        assert result is not None
        assert result["crs"]["id"]["code"] == 5070
        assert result["algorithm"] == "planar"


class TestFreeformCrsFileValidatesEndToEnd:
    """A real file, because the parser and the checks must agree on a live schema."""

    def test_a_freeform_crs_against_a_named_metadata_crs_fails(self, tmp_path):
        path = _write_compact_crs_file(
            tmp_path, "freeform_mismatch.parquet", "WGS 84", EPSG3857_PROJJSON
        )
        result = validate_geoparquet(path, validate_data=False)
        check = next(c for c in result.checks if c.name == "v2_crs_consistency_geometry")
        assert check.status is CheckStatus.FAILED
        assert "Schema: WGS 84 (unrecognized" in (check.details or "")

    def test_a_freeform_crs_against_an_absent_metadata_crs_fails(self, tmp_path):
        """The #866 headline: this reported PASSED "both are OGC:CRS84"."""
        path = _write_compact_crs_file(tmp_path, "freeform_absent.parquet", "WGS 84", "ABSENT")
        result = validate_geoparquet(path, validate_data=False)
        check = next(c for c in result.checks if c.name == "v2_crs_consistency_geometry")
        assert check.status is CheckStatus.FAILED
        assert "WGS 84" in (check.details or "")

    def test_inspect_reports_the_freeform_crs_as_a_mismatch_naming_the_literal(self, tmp_path):
        """``gpio inspect`` must render the string, not swallow it or show a dict."""
        from geoparquet_io.core.inspect_utils import _format_crs_for_display, extract_geo_info

        path = _write_compact_crs_file(
            tmp_path, "freeform_inspect.parquet", "WGS 84", EPSG3857_PROJJSON
        )
        info = extract_geo_info(path)
        assert any("WGS 84" in w for w in info["warnings"])
        assert _format_crs_for_display("WGS 84") == "WGS 84"
