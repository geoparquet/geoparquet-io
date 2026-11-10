"""The CRS owner (#1367 concept 8): one answer each for the questions the
extractors, the validators and the writers used to answer on their own.

``crs_utils`` owns four questions and these tables pin them:

* ``is_geographic(crs)`` -- pyproj, not a name list. ``None`` and an omitted
  ``crs`` key are the lon/lat default and answer True; a CRS nothing can read
  answers False.
* ``crs_equal(a, b)`` -- the one equality, with the structural-dict fallback
  and axis order ignored (GeoParquet stores x, y).
* ``projjson_for_label(label)`` -- a label to PROJJSON, and a refusal (the
  owner's error, naming the label) for a label nothing resolves: never an
  ``{"id": ...}`` stub.
* ``crs_of(source)`` -- the CRS of a file's or table's primary geometry column.
"""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyproj import CRS

from geoparquet_io.core import crs_utils
from geoparquet_io.core.crs_utils import (
    CRS_ABSENT,
    UnresolvableCRSError,
    crs_equal,
    crs_of,
    is_geographic,
    projjson_for_label,
)


def _projjson(label: str, *, drop_id: bool = False) -> dict:
    found = CRS.from_user_input(label).to_json_dict()
    if drop_id:
        found.pop("id", None)
    return found


# ---------------------------------------------------------------------------
# projjson_for_label
# ---------------------------------------------------------------------------

#: (label, the authority:code the PROJJSON names)
RESOLVING_LABELS = [
    ("EPSG:5070", ("EPSG", 5070)),
    ("epsg:5070", ("EPSG", 5070)),
    ("  EPSG:5070  ", ("EPSG", 5070)),
    ("urn:ogc:def:crs:EPSG::3857", ("EPSG", 3857)),
    ("http://www.opengis.net/def/crs/EPSG/0/3857", ("EPSG", 3857)),
    ("http://www.opengis.net/gml/srs/epsg.xml#31287", ("EPSG", 31287)),
    ("urn:ogc:def:crs,crs:EPSG::4326,crs:EPSG::5773", ("EPSG", 4326)),
    ("OGC:CRS84", ("OGC", "CRS84")),
    ("CRS:84", ("OGC", "CRS84")),
    ("urn:ogc:def:crs:OGC:1.3:CRS84", ("OGC", "CRS84")),
    ("ESRI:102001", ("ESRI", 102001)),
    # the label is read as written: PROJ's own entries, no alias (the legacy
    # Web Mercator WKID/SRID aliases are the extractors' numeric-code policy)
    ("ESRI:102100", ("ESRI", 102100)),
    ("EPSG:900913", ("EPSG", 900913)),
]


@pytest.mark.parametrize(
    ("label", "expected"), RESOLVING_LABELS, ids=[r[0] for r in RESOLVING_LABELS]
)
def test_a_label_resolves_to_the_projjson_it_names(label, expected):
    found = projjson_for_label(label)
    assert isinstance(found, dict)
    assert found["id"]["authority"] == expected[0]
    assert str(found["id"]["code"]) == str(expected[1])


def test_a_wkt_label_resolves_to_a_complete_definition():
    wkt = CRS.from_epsg(32633).to_wkt()
    found = projjson_for_label(wkt)
    assert found["type"] == "ProjectedCRS"
    assert found["id"]["code"] == 32633


@pytest.mark.parametrize(
    "label",
    [
        "junk",
        "",
        "   ",
        "EPSG:99999999",  # shaped like a code, in no database
        "NOSUCH:1234",
        "srid:0",
        "projjson:crs_key",
        "x" * 5000,
        None,
        4326,
        {"id": {"authority": "EPSG", "code": 4326}},
    ],
    ids=[
        "junk",
        "empty",
        "blank",
        "no-such-epsg",
        "no-such-authority",
        "srid",
        "projjson-key",
        "huge",
        "none",
        "int",
        "dict",
    ],
)
def test_a_label_nothing_resolves_is_refused_not_stubbed(label):
    """Approved change 11: the owner's error, never an ``{"id": ...}`` block."""
    with pytest.raises(UnresolvableCRSError) as raised:
        projjson_for_label(label)
    if isinstance(label, str) and label.strip() and len(label) < 100:
        assert label.strip() in str(raised.value)


def test_a_refusal_names_the_label():
    with pytest.raises(UnresolvableCRSError, match="EPSG:99999999"):
        projjson_for_label("EPSG:99999999")


def test_the_projjson_is_a_fresh_dict_each_call():
    first = projjson_for_label("EPSG:5070")
    first["name"] = "scribbled on"
    assert projjson_for_label("EPSG:5070")["name"] != "scribbled on"


def test_the_stub_resolver_is_gone():
    assert not hasattr(crs_utils, "parse_crs_string_to_projjson")


# ---------------------------------------------------------------------------
# is_geographic
# ---------------------------------------------------------------------------

GEOGRAPHIC_CASES = [
    # (id, value, expected)
    ("absent key", CRS_ABSENT, True),
    ("explicit None", None, True),
    ("EPSG:4326", "EPSG:4326", True),
    ("OGC:CRS84", "OGC:CRS84", True),
    ("CRS:84", "CRS:84", True),
    ("EPSG:4979 (3D geographic)", "EPSG:4979", True),
    ("EPSG:4171 RGF93 (not in any old list)", "EPSG:4171", True),
    ("EPSG:4269 NAD83", "EPSG:4269", True),
    ("EPSG:3857", "EPSG:3857", False),
    ("URN 3857", "urn:ogc:def:crs:EPSG::3857", False),
    ("EPSG:5070", "EPSG:5070", False),
    ("projjson 4326", _projjson("EPSG:4326"), True),
    ("projjson 4171", _projjson("EPSG:4171"), True),
    ("projjson 3857", _projjson("EPSG:3857"), False),
    ("projjson 3857 without id", _projjson("EPSG:3857", drop_id=True), False),
    ("id stub 4326", {"id": {"authority": "EPSG", "code": 4326}}, True),
    ("id stub 3857", {"id": {"authority": "EPSG", "code": 3857}}, False),
    ("compound 4326+5773", "EPSG:4326+5773", True),
    ("compound 32633+5773", "EPSG:32633+5773", False),
    ("a pyproj CRS object", CRS.from_epsg(4171), True),
    ("a projected pyproj CRS object", CRS.from_epsg(3857), False),
    # nothing can read these: not geographic (the caller decides what unknown means)
    ("junk string", "not a crs", False),
    ("empty dict", {}, False),
    ("empty string", "", False),
    ("name only", {"name": "WGS 84"}, False),
]


@pytest.mark.parametrize(
    ("value", "expected"), [c[1:] for c in GEOGRAPHIC_CASES], ids=[c[0] for c in GEOGRAPHIC_CASES]
)
def test_is_geographic(value, expected):
    assert is_geographic(value) is expected


def test_is_projected_is_the_definite_other_answer():
    assert crs_utils.is_projected("EPSG:3857") is True
    assert crs_utils.is_projected(_projjson("EPSG:5070")) is True
    assert crs_utils.is_projected("EPSG:4326") is False
    assert crs_utils.is_projected(None) is False
    assert crs_utils.is_projected(CRS_ABSENT) is False
    assert crs_utils.is_projected("not a crs") is False  # unknown is not projected


def test_the_four_is_geographic_copies_are_gone():
    from geoparquet_io.core import extract, wfs
    from geoparquet_io.core.add import quadkey

    assert not hasattr(crs_utils, "is_geographic_crs")
    assert not hasattr(extract, "is_geographic_crs")
    assert not hasattr(quadkey, "_is_geographic_crs")
    assert not hasattr(wfs, "_is_geographic_crs")


# ---------------------------------------------------------------------------
# crs_equal
# ---------------------------------------------------------------------------

EQUAL_PAIRS = [
    ("absent == absent", CRS_ABSENT, CRS_ABSENT, True),
    ("absent == EPSG:4326 (the default)", CRS_ABSENT, "EPSG:4326", True),
    ("absent == OGC:CRS84 projjson", CRS_ABSENT, _projjson("OGC:CRS84"), True),
    ("absent != EPSG:3857", CRS_ABSENT, "EPSG:3857", False),
    ("null == null", None, None, True),
    ("null != absent", None, CRS_ABSENT, False),
    ("null != EPSG:4326", None, "EPSG:4326", False),
    ("CRS84 == 4326 (axis order only)", "OGC:CRS84", "EPSG:4326", True),
    ("CRS84 projjson == 4326 projjson", _projjson("OGC:CRS84"), _projjson("EPSG:4326"), True),
    ("same string", "EPSG:3857", "EPSG:3857", True),
    ("string vs URN", "EPSG:3857", "urn:ogc:def:crs:EPSG::3857", True),
    ("string vs OGC URL", "EPSG:3857", "http://www.opengis.net/def/crs/EPSG/0/3857", True),
    ("CRS:84 vs EPSG:4326", "CRS:84", "EPSG:4326", True),
    ("different strings", "EPSG:3857", "EPSG:5070", False),
    # the pairs the two old copies (validate._crs_equals, inspect_utils._crs_are_equivalent) split on
    ("string vs projjson with id", "EPSG:3857", _projjson("EPSG:3857"), True),
    ("string vs id stub", "EPSG:3857", {"id": {"authority": "EPSG", "code": 3857}}, True),
    ("URN vs projjson", "urn:ogc:def:crs:EPSG::3857", _projjson("EPSG:3857"), True),
    (
        "4326 string vs id-less 4326 projjson",
        "EPSG:4326",
        _projjson("EPSG:4326", drop_id=True),
        True,
    ),
    (
        "CRS84 string vs id-less 4326 projjson",
        "OGC:CRS84",
        _projjson("EPSG:4326", drop_id=True),
        True,
    ),
    (
        "id stub 4326 vs id-less 4326 projjson",
        {"id": {"authority": "EPSG", "code": 4326}},
        _projjson("EPSG:4326", drop_id=True),
        True,
    ),
    ("opaque string equals itself", "custom thing", "custom thing", True),
    ("opaque strings differ", "custom thing", "other thing", False),
    ("structurally identical opaque dicts (approved 9)", {"foo": 1}, {"foo": 1}, True),
    ("different opaque dicts", {"foo": 1}, {"foo": 2}, False),
    ("empty dicts", {}, {}, True),
    (
        "both id-less, same CRS",
        _projjson("EPSG:5070", drop_id=True),
        _projjson("EPSG:5070", drop_id=True),
        True,
    ),
    (
        "both id-less, different CRS",
        _projjson("EPSG:5070", drop_id=True),
        _projjson("EPSG:3857", drop_id=True),
        False,
    ),
    ("ids differ", _projjson("EPSG:5070"), _projjson("EPSG:3857"), False),
    (
        "id-less vs with-id, same CRS",
        _projjson("EPSG:5070", drop_id=True),
        _projjson("EPSG:5070"),
        True,
    ),
    ("garbage dict vs a real CRS", {"foo": 1}, _projjson("EPSG:3857"), False),
]


@pytest.mark.parametrize(
    ("a", "b", "expected"), [p[1:] for p in EQUAL_PAIRS], ids=[p[0] for p in EQUAL_PAIRS]
)
def test_crs_equal(a, b, expected):
    assert crs_equal(a, b) is expected
    assert crs_equal(b, a) is expected  # symmetric


def test_the_equality_copies_are_gone():
    from geoparquet_io.core import inspect_utils, validate, wfs

    assert not hasattr(validate, "_crs_equals")
    assert not hasattr(inspect_utils, "_crs_are_equivalent")
    assert not hasattr(wfs, "_crs_matches")


# ---------------------------------------------------------------------------
# crs_of
# ---------------------------------------------------------------------------


def _geo_block(column: str, crs, **extra) -> bytes:
    entry = {"encoding": "WKB", "geometry_types": ["Point"], **extra}
    if crs is not CRS_ABSENT:
        entry["crs"] = crs
    return json.dumps(
        {"version": "1.1.0", "primary_column": column, "columns": {column: entry}}
    ).encode()


def _table(column: str, crs, other: str | None = None) -> pa.Table:
    data = {column: pa.array([b"\x01\x01\x00\x00\x00" + b"\x00" * 16], pa.binary())}
    if other:
        data = {other: pa.array([b"\x01\x01\x00\x00\x00" + b"\x00" * 16], pa.binary()), **data}
    return pa.table(data).replace_schema_metadata({b"geo": _geo_block(column, crs)})


def test_crs_of_a_table_reads_its_primary_column_not_a_neighbour():
    five = _projjson("EPSG:5070")
    table = _table("geom", five, other="geometry")
    assert crs_of(table)["id"]["code"] == 5070


def test_crs_of_a_file_reads_the_block(tmp_path):
    path = tmp_path / "a.parquet"
    pq.write_table(_table("shape", _projjson("EPSG:5070")), path)
    assert crs_of(str(path))["id"]["code"] == 5070
    assert crs_of(path)["id"]["code"] == 5070


def test_crs_of_the_default_is_none(tmp_path):
    path = tmp_path / "a.parquet"
    pq.write_table(_table("geometry", CRS_ABSENT), path)
    assert crs_of(str(path)) is None
    assert crs_of(_table("geometry", CRS_ABSENT)) is None


def test_crs_of_a_table_and_its_file_agree(tmp_path):
    table = _table("geom", _projjson("EPSG:3857"))
    path = tmp_path / "a.parquet"
    pq.write_table(table, path)
    assert crs_of(table) == crs_of(str(path))


def test_the_two_extract_readers_are_gone():
    from geoparquet_io.core import streaming

    assert not hasattr(crs_utils, "extract_crs_from_parquet")
    assert not hasattr(crs_utils, "extract_crs_from_table")
    assert not hasattr(streaming, "extract_crs_from_table")


# ---------------------------------------------------------------------------
# the one regex, the retired alias
# ---------------------------------------------------------------------------


def test_one_authority_code_regex():
    from geoparquet_io.core import duckdb_metadata

    assert crs_utils.AUTHORITY_CODE_CRS.match("EPSG:4326")
    assert not crs_utils.AUTHORITY_CODE_CRS.match("<null>")
    assert not hasattr(duckdb_metadata, "_AUTHORITY_CODE_CRS")
    assert not hasattr(crs_utils, "_TRANSFORM_CRS_SHAPE")


def test_crs_transform_sql_expr_is_retired():
    assert not hasattr(crs_utils, "crs_transform_sql_expr")
    assert not hasattr(crs_utils, "_is_native_geo_column")


# ---------------------------------------------------------------------------
# #1014: a block that names the default on purpose beats a different logical type
# ---------------------------------------------------------------------------


def _file_with_block_and_logical_type(tmp_path, block_crs):
    import geoarrow.pyarrow as ga

    from tests.native_geo_probes import conus_wkb, projjson, write_native_geo_only
    from tests.test_defrag_g9_repros import write_with_geo

    rows = conus_wkb("ST_Transform(cell, 'EPSG:4326', 'EPSG:5070', always_xy := true)")
    src = write_native_geo_only(
        tmp_path / "both.parquet", rows, {"geometry": (2, ga.wkb().with_crs(projjson(5070)))}
    )
    table = pq.read_table(str(src))
    entry = {"encoding": "WKB", "geometry_types": ["Polygon"]}
    if block_crs is not CRS_ABSENT:
        entry["crs"] = block_crs
    block = {"version": "2.0.0", "primary_column": "geometry", "columns": {"geometry": entry}}
    write_with_geo(src, table, block, compression="zstd")
    return src


@pytest.mark.parametrize(
    "block_crs",
    [_projjson("EPSG:4326"), "EPSG:4326", _projjson("OGC:CRS84")],
    ids=["4326 projjson", "4326 string", "CRS84 projjson"],
)
def test_a_block_naming_the_default_wins_over_a_projected_logical_type(tmp_path, caplog, block_crs):
    import logging

    src = _file_with_block_and_logical_type(tmp_path, block_crs)
    crs_utils.reset_crs_disagreement_warnings()
    with caplog.at_level(logging.WARNING):
        found = crs_of(str(src))
    crs_utils.reset_crs_disagreement_warnings()

    assert found is None  # the default: the writer omits the key
    assert "5070" in caplog.text and ("4326" in caplog.text or "CRS84" in caplog.text)


def test_a_block_with_no_crs_key_still_lets_the_logical_type_speak(tmp_path):
    src = _file_with_block_and_logical_type(tmp_path, CRS_ABSENT)
    assert crs_of(str(src))["id"]["code"] == 5070


def test_a_quiet_read_does_not_warn_about_the_disagreement(tmp_path, caplog):
    import logging

    src = _file_with_block_and_logical_type(tmp_path, _projjson("EPSG:4326"))
    crs_utils.reset_crs_disagreement_warnings()
    with caplog.at_level(logging.WARNING):
        assert crs_of(str(src), quiet=True) is None
    assert "5070" not in caplog.text


def test_one_web_mercator_alias_table_for_both_extractors():
    from geoparquet_io.core import arcgis, carto

    assert crs_utils.canonical_crs_code(900913) == 3857
    assert crs_utils.canonical_crs_code(102100) == 3857
    assert crs_utils.canonical_crs_code(102113) == 3785
    assert crs_utils.canonical_crs_code(5070) == 5070
    assert not hasattr(arcgis, "WKID_TO_EPSG") and not hasattr(arcgis, "_normalize_wkid")
    assert not hasattr(carto, "_SRID_LABELS")
    # the case that moved: a Carto SRID Esri spells, and an ArcGIS WKID Google spelled
    assert carto._crs_for_srid(102100)["id"] == {"authority": "EPSG", "code": 3857}
    assert carto._crs_for_srid(102113)["id"] == {"authority": "EPSG", "code": 3785}
    assert carto._crs_for_srid(900913)["id"] == {"authority": "EPSG", "code": 3857}
    assert arcgis._projjson_from_wkid(900913)["id"] == {"authority": "EPSG", "code": 3857}
    assert arcgis._projjson_from_wkid(102100)["id"] == {"authority": "EPSG", "code": 3857}


# ---------------------------------------------------------------------------
# one table reading order (block, then the GeoArrow field); Table.crs shows it
# ---------------------------------------------------------------------------


def test_a_table_with_a_block_and_a_different_geoarrow_crs_answers_with_the_block():
    """Maintainer flag: ``Table.crs`` used to read the registered extension type first."""
    import geoarrow.pyarrow as ga

    import geoparquet_io as gpio

    wkb = b"\x01\x01\x00\x00\x00" + b"\x00" * 16
    typed = ga.wkb().with_crs("EPSG:5070")
    column = pa.ExtensionArray.from_storage(typed, pa.array([wkb], pa.binary()))
    table = pa.table({"geometry": column}).replace_schema_metadata(
        {b"geo": _geo_block("geometry", _projjson("EPSG:3857"))}
    )
    assert table.schema.field("geometry").type.crs is not None  # the carrier is really there
    assert crs_of(table)["id"]["code"] == 3857
    assert gpio.Table(table, geometry_column="geometry").crs["id"]["code"] == 3857


def test_a_stated_default_is_shown_as_stated_and_unstated_is_none():
    default = _projjson("OGC:CRS84")
    assert crs_of(_table("geometry", default)) == default
    assert crs_of(_table("geometry", CRS_ABSENT)) is None
    assert crs_of(_table("geometry", None)) is None


class _FakeReadMeta:
    """ST_Read_Meta's answer for a layer whose CRS GDAL names but does not render as PROJJSON."""

    def __init__(self, crs_info):
        self._row = (None, None, None, [{"geometry_fields": [{"crs": crs_info}]}])

    def execute(self, _sql):
        return self

    def fetchone(self):
        return self._row

    def fetchall(self):
        return [self._row]

    def complete(self):
        return None


@pytest.mark.parametrize(
    ("crs_info", "expected"),
    [
        ({"auth_name": "EPSG", "auth_code": "32610"}, ("ProjectedCRS", 32610)),
        ({"auth_name": "EPSG", "auth_code": "4269"}, ("GeographicCRS", 4269)),
    ],
)
def test_a_spatial_files_authority_code_is_resolved_by_the_owner_not_stubbed(crs_info, expected):
    """``detect_crs_from_spatial_file`` built ``{"id": ...}`` from GDAL's auth_name:auth_code
    and leaned on ``normalize_projjson_crs`` to repair it; it is a full definition now."""
    from geoparquet_io.core.crs_utils import detect_crs_from_spatial_file

    found = detect_crs_from_spatial_file("x.gpkg", _FakeReadMeta(crs_info))
    assert found["type"] == expected[0]
    assert found["id"]["code"] == expected[1]


def test_an_authority_no_database_defines_is_not_stubbed():
    """Fails closed: the layer's CRS is then not found, and the caller refuses the file."""
    from geoparquet_io.core.crs_utils import detect_crs_from_spatial_file

    found = detect_crs_from_spatial_file(
        "x.gpkg", _FakeReadMeta({"auth_name": "EPSG", "auth_code": "99999"})
    )
    assert found is None or "type" in found
