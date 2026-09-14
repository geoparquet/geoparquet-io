"""#699: the CRS helpers must distinguish an absent ``crs`` from an explicit null.

Two spec rules govern every assertion here, and gpio has already shipped both
elsewhere (root ``CHANGELOG.md``, "Distinguish an explicit crs: null (CRS
*unknown*) from an omitted crs key", #471, and ``gpio convert reproject
--assume-crs84``):

1. An **omitted** ``crs`` key means the OGC:CRS84 default.
2. An explicit ``"crs": null`` means the CRS is **unknown** -- not the default.

Both shapes collapse to ``None`` under ``col_meta.get("crs")``, so the helpers
take :data:`~geoparquet_io.core.crs_utils.CRS_ABSENT` for the first and ``None``
for the second, and call sites extract with
:func:`~geoparquet_io.core.crs_utils.crs_from_column_meta`.

A third rule covers the CRS84/EPSG:4326 pair: GeoParquet fixes the stored
coordinate order to (x, y) regardless of the CRS's own axis definition, so
OGC:CRS84 and EPSG:4326 describe the same coordinates in every GeoParquet file
and must compare equal in every representation (bare ``id`` stub, authority
string, URN, and full pyproj PROJJSON).
"""

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from pyproj import CRS as PyprojCRS

from geoparquet_io.core.crs_utils import (
    CRS_ABSENT,
    crs_from_column_meta,
    get_crs_display_name,
    is_default_crs,
    is_geographic_crs,
)
from geoparquet_io.core.inspect_utils import _crs_are_equivalent
from geoparquet_io.core.validate import (
    CheckStatus,
    _check_crs_valid,
    _check_epoch_valid,
    _check_v2_crs_consistency,
    _check_v2_crs_in_parquet_type,
    _crs_equals,
    _is_crs84_equivalent,
    _is_ogc_crs84,
    validate_geoparquet,
)

# The bare-``id`` stub form: what a hand-written or minimal writer emits.
CRS84_ID = {
    "type": "GeographicCRS",
    "name": "WGS 84 (CRS84)",
    "id": {"authority": "OGC", "code": "CRS84"},
}
EPSG4326_ID = {
    "type": "GeographicCRS",
    "name": "WGS 84",
    "id": {"authority": "EPSG", "code": 4326},
}
EPSG3857_ID = {
    "type": "ProjectedCRS",
    "name": "WGS 84 / Pseudo-Mercator",
    "id": {"authority": "EPSG", "code": 3857},
}

# The full PROJJSON form: what pyproj (and therefore most real writers) emit.
# Generated rather than hand-written so the test exercises real files, not a
# synthetic stub that only the id fast path ever sees.
CRS84_PROJJSON = PyprojCRS.from_user_input("OGC:CRS84").to_json_dict()
EPSG4326_PROJJSON = PyprojCRS.from_epsg(4326).to_json_dict()
EPSG3857_PROJJSON = PyprojCRS.from_epsg(3857).to_json_dict()

BINARY_HELPERS = pytest.mark.parametrize(
    "compare", [_crs_equals, _crs_are_equivalent], ids=["crs_equals", "crs_are_equivalent"]
)


def _idless(crs: dict) -> dict:
    """A PROJJSON copy with no ``id`` member, so no id fast path can answer."""
    stripped = dict(crs)
    stripped.pop("id", None)
    return stripped


# =============================================================================
# The extraction boundary
# =============================================================================


class TestCrsFromColumnMeta:
    """The distinction has to survive extraction, or no helper can see it."""

    def test_absent_key_yields_the_sentinel(self):
        assert crs_from_column_meta({}) is CRS_ABSENT
        assert crs_from_column_meta({"encoding": "WKB"}) is CRS_ABSENT

    def test_explicit_null_yields_none(self):
        assert crs_from_column_meta({"crs": None}) is None

    def test_present_crs_passes_through_unchanged(self):
        assert crs_from_column_meta({"crs": CRS84_ID}) is CRS84_ID

    def test_sentinel_is_distinct_from_none(self):
        assert CRS_ABSENT is not None

    def test_sentinel_is_falsy_like_the_none_it_replaces(self):
        """Call sites guard with ``if crs:``; the sentinel must not flip them."""
        assert not CRS_ABSENT

    def test_sentinel_repr_is_readable(self):
        assert "CRS_ABSENT" in repr(CRS_ABSENT)

    def test_non_dict_meta_is_treated_as_absent(self):
        assert crs_from_column_meta(None) is CRS_ABSENT

    def test_display_name_renders_the_default_not_the_repr(self):
        """The sentinel reaches validation messages; it must read as the default."""
        assert "CRS84" in get_crs_display_name(CRS_ABSENT)
        assert "CRS_ABSENT" not in get_crs_display_name(CRS_ABSENT)

    def test_display_name_renders_an_explicit_null_as_unknown(self):
        """``None`` reaches the same messages and must not claim the default.

        It renders inside the v2-consistency FAILED detail line; calling an
        explicit null "OGC:CRS84" there makes the failure contradict itself.
        """
        rendered = get_crs_display_name(None)
        assert "unknown" in rendered.lower()
        assert "CRS84" not in rendered

    def test_absent_crs_is_geographic(self):
        """Absent means OGC:CRS84, which is lon/lat."""
        assert is_geographic_crs(CRS_ABSENT) is True

    def test_absent_crs_is_the_default_crs(self):
        assert is_default_crs(CRS_ABSENT) is True


# =============================================================================
# Case (a): the unary helpers
# =============================================================================


class TestUnaryHelpersAbsentVsNull:
    def test_absent_crs_is_the_crs84_default(self):
        assert _is_crs84_equivalent(CRS_ABSENT) is True
        assert _is_ogc_crs84(CRS_ABSENT) is True

    def test_explicit_null_crs_is_unknown_not_crs84(self):
        assert _is_crs84_equivalent(None) is False
        assert _is_ogc_crs84(None) is False

    def test_explicit_crs84_and_epsg4326_are_still_crs84(self):
        for crs in (CRS84_ID, EPSG4326_ID, CRS84_PROJJSON, EPSG4326_PROJJSON):
            assert _is_crs84_equivalent(crs) is True

    def test_other_crs_is_not_crs84(self):
        assert _is_crs84_equivalent(EPSG3857_ID) is False
        assert _is_crs84_equivalent(EPSG3857_PROJJSON) is False


# =============================================================================
# Case (b): absent vs null in the binary helpers
# =============================================================================


class TestBinaryHelpersAbsentVsNull:
    @BINARY_HELPERS
    def test_absent_equals_explicit_crs84(self, compare):
        assert compare(CRS_ABSENT, CRS84_ID) is True
        assert compare(CRS84_ID, CRS_ABSENT) is True

    @BINARY_HELPERS
    def test_absent_equals_explicit_epsg4326(self, compare):
        """Absent resolves to CRS84, which is EPSG:4326 under fixed (x, y) order."""
        assert compare(CRS_ABSENT, EPSG4326_ID) is True
        assert compare(CRS_ABSENT, EPSG4326_PROJJSON) is True

    @BINARY_HELPERS
    def test_absent_equals_absent(self, compare):
        assert compare(CRS_ABSENT, CRS_ABSENT) is True

    @BINARY_HELPERS
    def test_explicit_null_does_not_equal_crs84(self, compare):
        assert compare(None, CRS84_ID) is False
        assert compare(CRS84_ID, None) is False
        assert compare(None, CRS84_PROJJSON) is False

    @BINARY_HELPERS
    def test_absent_does_not_equal_explicit_null(self, compare):
        """Default-CRS84 and unknown-CRS are different claims about the data."""
        assert compare(CRS_ABSENT, None) is False
        assert compare(None, CRS_ABSENT) is False

    @BINARY_HELPERS
    def test_absent_does_not_equal_a_real_other_crs(self, compare):
        assert compare(CRS_ABSENT, EPSG3857_ID) is False

    @BINARY_HELPERS
    def test_absent_does_not_equal_an_empty_crs_value(self, compare):
        """``{}`` / ``""`` name no CRS at all, so they cannot be the default.

        ``is_default_crs`` answers True for any falsy value, so the absent
        branch must not lean on it alone or the two helpers drift apart here.
        """
        assert compare(CRS_ABSENT, {}) is False
        assert compare(CRS_ABSENT, "") is False

    @BINARY_HELPERS
    def test_absent_equals_id_less_crs84_projjson(self, compare):
        """The absent branch must recognize CRS84 with no ``id`` to match on.

        Both helpers claim to resolve absent identically, but each called its
        own "is this the default?" predicate: ``_is_crs84_equivalent`` (which
        falls back to pyproj) versus ``is_default_crs`` (which only reads an
        authority id, so id-less CRS84 PROJJSON came back False).
        """
        assert compare(CRS_ABSENT, _idless(CRS84_PROJJSON)) is True
        assert compare(_idless(CRS84_PROJJSON), CRS_ABSENT) is True

    @BINARY_HELPERS
    def test_absent_equals_the_srid_spelling_of_crs84(self, compare):
        """``SRID:4326`` is a CRS84 spelling one predicate knew and the other did not."""
        assert compare(CRS_ABSENT, "SRID:4326") is True
        assert compare("SRID:4326", CRS_ABSENT) is True

    @BINARY_HELPERS
    def test_absent_still_differs_from_an_id_less_other_crs(self, compare):
        assert compare(CRS_ABSENT, _idless(EPSG3857_PROJJSON)) is False

    @BINARY_HELPERS
    def test_two_unknown_crs_declarations_agree(self, compare):
        """Both helpers must give the same answer; ``_crs_equals`` already said True.

        The call sites compare two declarations *of the same file* (geo metadata
        vs Parquet logical type), so "both say unknown" is a consistent file.
        """
        assert compare(None, None) is True


# =============================================================================
# Case (c): OGC:CRS84 == EPSG:4326 in every representation
# =============================================================================


class TestCrs84EqualsEpsg4326:
    @BINARY_HELPERS
    def test_id_stub_form(self, compare):
        assert compare(CRS84_ID, EPSG4326_ID) is True
        assert compare(EPSG4326_ID, CRS84_ID) is True

    @BINARY_HELPERS
    def test_authority_string_form(self, compare):
        assert compare("OGC:CRS84", "EPSG:4326") is True
        assert compare("EPSG:4326", "OGC:CRS84") is True

    @BINARY_HELPERS
    def test_urn_string_form(self, compare):
        assert compare("urn:ogc:def:crs:OGC:1.3:CRS84", "EPSG:4326") is True

    @BINARY_HELPERS
    def test_full_pyproj_projjson_form(self, compare):
        """Real files carry full PROJJSON, not the ``{"id": ...}`` stub."""
        assert compare(CRS84_PROJJSON, EPSG4326_PROJJSON) is True

    @BINARY_HELPERS
    def test_mixed_stub_and_full_projjson(self, compare):
        assert compare(CRS84_ID, EPSG4326_PROJJSON) is True
        assert compare(CRS84_PROJJSON, EPSG4326_ID) is True

    @BINARY_HELPERS
    def test_mixed_string_and_projjson(self, compare):
        assert compare("OGC:CRS84", EPSG4326_PROJJSON) is True

    @BINARY_HELPERS
    def test_genuinely_different_crs_still_differ(self, compare):
        """The fast path must not become a blanket yes."""
        assert compare(CRS84_ID, EPSG3857_ID) is False
        assert compare(CRS84_PROJJSON, EPSG3857_PROJJSON) is False
        assert compare("EPSG:4326", "EPSG:3857") is False


class TestIdLessProjjsonAxisOrder:
    """Both fallbacks must ignore axis order, for the same spec reason."""

    @BINARY_HELPERS
    def test_lon_lat_vs_lat_lon_without_ids(self, compare):
        lon_lat = _idless(CRS84_PROJJSON)
        lat_lon = _idless(EPSG4326_PROJJSON)
        # Preconditions: no id, so the fast path cannot answer and pyproj runs.
        assert "id" not in lon_lat and "id" not in lat_lon
        assert [a["abbreviation"] for a in lon_lat["coordinate_system"]["axis"]] == ["Lon", "Lat"]
        assert [a["abbreviation"] for a in lat_lon["coordinate_system"]["axis"]] == ["Lat", "Lon"]
        assert compare(lon_lat, lat_lon) is True

    @BINARY_HELPERS
    def test_different_crs_without_ids_still_differ(self, compare):
        assert compare(_idless(CRS84_PROJJSON), _idless(EPSG3857_PROJJSON)) is False


# =============================================================================
# Call sites: an omitted crs must keep validating as CRS84
# =============================================================================


def _schema(logical_type: str) -> list[dict]:
    return [{"name": "geometry", "logical_type": logical_type}]


def _geo_meta(col_meta: dict) -> dict:
    return {"version": "2.0.0", "primary_column": "geometry", "columns": {"geometry": col_meta}}


# A Parquet GEOMETRY logical type with no CRS. Per the Parquet spec that means
# OGC:CRS84, which is why it is the "default" side of every pair below.
SCHEMA_NO_CRS = _schema("GeometryType(crs=<null>)")
# The spec's spelling of "the CRS is unknown" on the Parquet side: "When the
# GeoParquet column-metadata crs is null, the Parquet logical-type crs property
# SHOULD be set to the string srid:0" (conformance row: srid:0 | null | "CRS
# undefined or unknown").
SCHEMA_SRID0 = _schema("GeometryType(crs=srid:0)")
SCHEMA_EPSG3857 = _schema(
    'GeometryType(crs={"type": "ProjectedCRS", "name": "x", '
    '"id": {"authority": "EPSG", "code": 3857}})'
)


class TestOmittedCrsStillValidatesAsCrs84:
    """The load-bearing regression guard: absent ``crs`` is the common, valid case."""

    def test_crs_valid_check_passes_for_omitted_key(self):
        check = _check_crs_valid({"encoding": "WKB"}, "geometry")
        assert check.status is CheckStatus.PASSED
        assert "defaults to OGC:CRS84" in check.message

    def test_crs_valid_check_warns_for_explicit_null(self):
        check = _check_crs_valid({"encoding": "WKB", "crs": None}, "geometry")
        assert check.status is CheckStatus.WARNING

    def test_v2_inline_crs_not_required_when_key_omitted(self):
        check = _check_v2_crs_in_parquet_type(_geo_meta({}), SCHEMA_NO_CRS, "geometry")
        assert check.status is CheckStatus.PASSED

    def test_v2_inline_crs_not_required_for_explicit_crs84(self):
        for crs in (CRS84_ID, EPSG4326_ID, CRS84_PROJJSON):
            check = _check_v2_crs_in_parquet_type(
                _geo_meta({"crs": crs}), SCHEMA_NO_CRS, "geometry"
            )
            assert check.status is CheckStatus.PASSED

    def test_v2_inline_crs_still_required_for_a_real_crs(self):
        check = _check_v2_crs_in_parquet_type(
            _geo_meta({"crs": EPSG3857_ID}), SCHEMA_NO_CRS, "geometry"
        )
        assert check.status is CheckStatus.FAILED

    def test_v2_consistency_passes_when_both_sides_omit_crs(self):
        check = _check_v2_crs_consistency(_geo_meta({}), SCHEMA_NO_CRS, "geometry")
        assert check.status is CheckStatus.PASSED

    @pytest.mark.parametrize("crs", [CRS84_ID, EPSG4326_ID, CRS84_PROJJSON, EPSG4326_PROJJSON])
    def test_v2_consistency_passes_for_explicit_crs84_vs_omitted_schema_crs(self, crs):
        check = _check_v2_crs_consistency(_geo_meta({"crs": crs}), SCHEMA_NO_CRS, "geometry")
        assert check.status is CheckStatus.PASSED

    def test_v2_consistency_fails_when_metadata_crs_is_unknown_but_schema_is_default(self):
        """Explicit null says "unknown"; an omitted Parquet crs says OGC:CRS84."""
        check = _check_v2_crs_consistency(_geo_meta({"crs": None}), SCHEMA_NO_CRS, "geometry")
        assert check.status is CheckStatus.FAILED

    def test_v2_consistency_fails_on_a_real_mismatch(self):
        check = _check_v2_crs_consistency(_geo_meta({"crs": CRS84_ID}), SCHEMA_EPSG3857, "geometry")
        assert check.status is CheckStatus.FAILED

    def test_v2_consistency_passes_on_a_real_match(self):
        check = _check_v2_crs_consistency(
            _geo_meta({"crs": EPSG3857_ID}), SCHEMA_EPSG3857, "geometry"
        )
        assert check.status is CheckStatus.PASSED


class TestUnknownCrsHasAValidTwoZeroRepresentation:
    """``crs: null`` + Parquet ``srid:0`` is the spec's canonical unknown-CRS pair.

    The spec: "When the GeoParquet column-metadata crs is null, the Parquet
    logical-type crs property SHOULD be set to the string srid:0", with a
    conformance row of ``srid:0 | null | "CRS undefined or unknown"``. Now that
    null-vs-absent is a real mismatch, this pairing is the *only* way to declare
    an unknown CRS in a GeoParquet 2.0 file, so it has to validate.
    """

    def test_v2_consistency_passes_for_null_metadata_and_srid_zero_schema(self):
        check = _check_v2_crs_consistency(_geo_meta({"crs": None}), SCHEMA_SRID0, "geometry")
        assert check.status is CheckStatus.PASSED

    def test_v2_consistency_fails_when_only_the_schema_says_unknown(self):
        """An omitted metadata crs claims OGC:CRS84; ``srid:0`` claims unknown."""
        check = _check_v2_crs_consistency(_geo_meta({}), SCHEMA_SRID0, "geometry")
        assert check.status is CheckStatus.FAILED

    @pytest.mark.parametrize("crs", [CRS84_ID, EPSG4326_ID, EPSG3857_ID])
    def test_v2_consistency_fails_when_metadata_names_a_crs_but_schema_says_unknown(self, crs):
        check = _check_v2_crs_consistency(_geo_meta({"crs": crs}), SCHEMA_SRID0, "geometry")
        assert check.status is CheckStatus.FAILED

    def test_null_metadata_still_fails_against_a_named_schema_crs(self):
        """srid:0 is the only Parquet CRS an explicit null may pair with."""
        check = _check_v2_crs_consistency(_geo_meta({"crs": None}), SCHEMA_EPSG3857, "geometry")
        assert check.status is CheckStatus.FAILED

    def test_srid_zero_is_not_read_as_epsg_zero_or_the_default(self):
        """A non-zero srid is a real CRS claim, not the unknown marker."""
        check = _check_v2_crs_consistency(
            _geo_meta({"crs": None}), _schema("GeometryType(crs=srid:4326)"), "geometry"
        )
        assert check.status is CheckStatus.FAILED


class TestEpochResolvesAbsentAndNullLikeEveryOtherCheck:
    """``_check_epoch_valid`` shares the sentinel, so it must keep both answers.

    It is the last check that told absent from null with a private marker; these
    pin the two outcomes across the move onto ``crs_from_column_meta``.
    """

    def test_absent_crs_epoch_fails_on_the_default_datum_ensemble(self):
        """Absent means OGC:CRS84, a datum ensemble, which cannot carry an epoch."""
        check = _check_epoch_valid({"epoch": 2020.0}, "geometry")
        assert check.status is CheckStatus.FAILED

    def test_explicit_null_crs_epoch_warns_because_the_datum_is_unknowable(self):
        check = _check_epoch_valid({"epoch": 2020.0, "crs": None}, "geometry")
        assert check.status is CheckStatus.WARNING
        assert "null" in check.message

    def test_no_epoch_passes_whatever_the_crs_shape(self):
        assert _check_epoch_valid({}, "geometry").status is CheckStatus.PASSED
        assert _check_epoch_valid({"crs": None}, "geometry").status is CheckStatus.PASSED


# =============================================================================
# End to end: a real file that declares an unknown CRS
# =============================================================================


def _write_v2_file(tmp_path, name: str, geom_expr: str, col_meta_update: dict):
    """Write a real GeoParquet 2.0 file: native geo type + hand-set geo metadata.

    ``geom_expr`` sets the CRS carried by the Parquet ``GEOMETRY`` logical type;
    ``col_meta_update`` is merged into the geo metadata's geometry column, so the
    two carriers can be made to agree or disagree on purpose.
    """
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection

    source = Path(__file__).parent / "data" / "buildings_test.parquet"
    out = tmp_path / name

    geo = json.loads(pq.read_metadata(str(source)).metadata[b"geo"].decode("utf-8"))
    geo["version"] = "2.0.0"
    geo["columns"][geo["primary_column"]].update(col_meta_update)
    geo_json = json.dumps(geo).replace("'", "''")

    con = get_duckdb_connection()
    try:
        con.execute(f"""
            COPY (SELECT * REPLACE ({geom_expr} AS geometry) FROM '{source.as_posix()}')
            TO '{out.as_posix()}'
            (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE', KV_METADATA {{geo: '{geo_json}'}})
        """)
    finally:
        con.close()
    return str(out)


def _check_named(result, name: str):
    matches = [c for c in result.checks if c.name == name]
    assert matches, f"{name} not among {sorted({c.name for c in result.checks})}"
    return matches[0]


class TestUnknownCrsFileValidatesEndToEnd:
    """The null-CRS rules, exercised through ``validate_geoparquet`` on real files."""

    def test_null_metadata_with_srid_zero_type_is_a_consistent_file(self, tmp_path):
        """The spec's canonical unknown-CRS 2.0 pairing must not fail validation."""
        path = _write_v2_file(
            tmp_path, "unknown_crs.parquet", "ST_SetCRS(geometry, 'srid:0')", {"crs": None}
        )
        # Non-vacuity: both carriers really say "unknown" in the written file.
        schema = pq.ParquetFile(path).schema_arrow
        assert json.loads(schema.metadata[b"geo"])["columns"]["geometry"]["crs"] is None

        result = validate_geoparquet(path, validate_data=False)
        assert _check_named(result, "v2_crs_consistency_geometry").status is CheckStatus.PASSED

    def test_null_metadata_against_a_crs84_type_is_still_a_mismatch(self, tmp_path):
        """An unknown CRS in the metadata contradicts a CRS84 Parquet type."""
        path = _write_v2_file(tmp_path, "null_vs_crs84.parquet", "geometry", {"crs": None})
        result = validate_geoparquet(path, validate_data=False)
        assert _check_named(result, "v2_crs_consistency_geometry").status is CheckStatus.FAILED

    def test_explicit_null_crs_is_reported_as_unknown_not_as_the_default(self, null_crs_parquet):
        """``gpio check spec`` on a plain 1.x file with ``crs: null`` warns."""
        result = validate_geoparquet(null_crs_parquet, validate_data=False)
        check = _check_named(result, "crs_valid_geometry")
        assert check.status is CheckStatus.WARNING
        assert "null" in check.message
