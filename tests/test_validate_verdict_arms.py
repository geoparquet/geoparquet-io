"""The verdicts ``gpio check spec`` can reach, reached -- each from a file with
exactly that defect and nothing else.

``core/validate.py`` is the oracle every other test package now leans on: WP-1's
``assert_fix_output_is_sound`` asks it whether a ``--fix`` output is still a
valid file, and the #997 CRS tests ask it whether two CRS carriers agree. A
verdict *it* never reaches in the suite is a verdict nobody has checked, and an
oracle whose own FAILED arms are unexercised can only vouch for the happy path.

Every case below is table-driven and asserts the **check id, its status and its
message** -- not "something failed". ``check spec`` reports per-check, so
asserting only ``is_valid`` would pass just as happily when the wrong check
fires for the wrong reason.

Fixtures are built in ``tmp_path`` rather than committed: each one is a
one-defect shape used by a single case, which is not what
``tests/data/generate_test_fixtures.py`` is for.

Refs: https://github.com/geoparquet/geoparquet-io/issues/1018 (WP-3)
"""

from __future__ import annotations

import ast
import json
import struct
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from geoparquet_io.core.validate import (
    CheckStatus,
    ValidationCheck,
    _check_covering_bbox_column_exists,
    _check_covering_bbox_field_types,
    _check_covering_bbox_paths,
    _check_covering_bbox_structure,
    _check_encoding_matches_data,
    _check_geometry_types_match_data,
    _execute_bounds_query,
    _interpret_bbox_result,
    _run_geoparquet_checks,
    validate_geoparquet,
)

# POINT (1 2), little-endian WKB.
WKB_POINT = bytes.fromhex("0101000000000000000000f03f0000000000000040")
BBOX_PATHS = {key: ["bbox", key] for key in ("xmin", "ymin", "xmax", "ymax")}
F64 = pa.float64()


def wkb_point(x: float, y: float) -> bytes:
    return b"\x01" + struct.pack("<I", 1) + struct.pack("<dd", x, y)


def wkb_polygon(ring: list[tuple[float, float]]) -> bytes:
    body = struct.pack("<I", 1) + struct.pack("<I", len(ring))
    for x, y in ring:
        body += struct.pack("<dd", x, y)
    return b"\x01" + struct.pack("<I", 3) + body


def base_geo(**column_overrides) -> dict:
    """A minimal, valid GeoParquet 1.1 ``geo`` block for one WKB point column."""
    column = {"encoding": "WKB", "geometry_types": ["Point"]}
    column.update(column_overrides)
    return {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": column}}


def bbox_struct(types=None) -> pa.StructArray:
    types = types or [F64] * 4
    return pa.StructArray.from_arrays(
        [pa.array([0.0, 1.0], type=t) for t in types],
        names=["xmin", "ymin", "xmax", "ymax"],
    )


def write_geoparquet(path, geo: dict, columns: dict | None = None) -> str:
    """A two-row WKB file carrying ``geo`` verbatim -- malformations included."""
    arrays = {"geometry": pa.array([WKB_POINT] * 2, pa.binary())}
    arrays.update(columns or {})
    table = pa.table(arrays).replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    pq.write_table(table, str(path))
    return str(path)


def verdicts(path, **kwargs) -> dict[str, ValidationCheck]:
    """``{check name: check}`` from the validator, with ``check spec``'s defaults."""
    return {check.name: check for check in validate_geoparquet(str(path), **kwargs).checks}


def assert_verdict(checks: dict, name: str, status: CheckStatus, fragment: str) -> None:
    assert name in checks, f"no check named {name!r}; got {sorted(checks)}"
    check = checks[name]
    assert check.status == status, (
        f"{name}: expected {status}, got {check.status} ({check.message})"
    )
    assert fragment in check.message, f"{name}: {fragment!r} not in {check.message!r}"


# =============================================================================
# Column metadata verdicts (checks 8-13), from a malformed `geo` block
# =============================================================================

#: ``(id, column-metadata override, check name, status, message fragment)``.
COLUMN_METADATA_CASES = [
    (
        "geometry_types_not_a_list",
        {"geometry_types": "Point"},
        "geometry_types_list_geometry",
        CheckStatus.FAILED,
        'must have a "geometry_types" list',
    ),
    (
        "crs_is_a_bare_string",
        {"crs": "EPSG:4326"},
        "crs_valid_geometry",
        CheckStatus.FAILED,
        "CRS must be null or valid PROJJSON object",
    ),
    (
        "bbox_is_a_short_object",
        {"bbox": {"xmin": 0}},
        "bbox_valid_geometry",
        CheckStatus.FAILED,
        "bbox must be an array",
    ),
    (
        "bbox_holds_a_string",
        {"bbox": [0, 0, "1", 1]},
        "bbox_valid_geometry",
        CheckStatus.FAILED,
        "bbox elements must be numbers",
    ),
    (
        "epoch_is_a_string",
        {"epoch": "2020.0"},
        "epoch_valid_geometry",
        CheckStatus.FAILED,
        "epoch must be a number",
    ),
]


@pytest.mark.parametrize(
    ("override", "check_name", "status", "fragment"),
    [case[1:] for case in COLUMN_METADATA_CASES],
    ids=[case[0] for case in COLUMN_METADATA_CASES],
)
def test_column_metadata_verdict(tmp_path, override, check_name, status, fragment):
    path = write_geoparquet(tmp_path / "f.parquet", base_geo(**override))
    assert_verdict(verdicts(path), check_name, status, fragment)


@pytest.mark.xfail(
    strict=True,
    raises=KeyError,
    reason="#1062: a 4-key `bbox` object crashes `gpio check spec` with KeyError",
)
def test_bbox_object_with_four_keys_is_reported_not_crashed(tmp_path):
    """``_check_bbox_valid`` already calls this bbox invalid; the data check then
    indexes it as a list anyway because ``len()`` of a 4-key dict is 4."""
    bbox = {"xmin": 0, "ymin": 0, "xmax": 1, "ymax": 1}
    path = write_geoparquet(tmp_path / "f.parquet", base_geo(bbox=bbox))
    assert_verdict(
        verdicts(path), "bbox_valid_geometry", CheckStatus.FAILED, "bbox must be an array"
    )


def test_geo_block_naming_a_column_the_schema_does_not_have(tmp_path):
    """Every schema check must say the column is missing, not read some other column."""
    geo = base_geo()
    geo["primary_column"] = "geom"
    geo["columns"] = {"geom": geo["columns"].pop("geometry")}
    checks = verdicts(write_geoparquet(tmp_path / "f.parquet", geo))

    for name in (
        "geometry_not_grouped_geom",
        "geometry_byte_array_geom",
        "geometry_not_repeated_geom",
    ):
        assert_verdict(
            checks, name, CheckStatus.FAILED, 'geometry column "geom" not found in schema'
        )


# =============================================================================
# covering.bbox verdicts (checks 1.1-2 .. 1.1-7)
# =============================================================================


def test_covering_bbox_missing_a_required_path(tmp_path):
    paths = {key: value for key, value in BBOX_PATHS.items() if key != "xmax"}
    path = write_geoparquet(
        tmp_path / "f.parquet",
        base_geo(covering={"bbox": paths}),
        {"bbox": bbox_struct()},
    )
    assert_verdict(
        verdicts(path),
        "covering_bbox_paths_geometry",
        CheckStatus.FAILED,
        "covering bbox missing required paths: ['xmax']",
    )


def test_covering_bbox_path_is_not_a_path_array(tmp_path):
    path = write_geoparquet(
        tmp_path / "f.parquet",
        base_geo(covering={"bbox": dict(BBOX_PATHS, xmin="bbox.xmin")}),
        {"bbox": bbox_struct()},
    )
    assert_verdict(
        verdicts(path),
        "covering_bbox_paths_geometry",
        CheckStatus.FAILED,
        "covering bbox xmin must be a path array [column, field]",
    )


def test_covering_bbox_without_xmin_cannot_name_its_column(tmp_path):
    """All three column-resolving checks must say so rather than guess a column."""
    paths = {key: value for key, value in BBOX_PATHS.items() if key != "xmin"}
    checks = verdicts(
        write_geoparquet(
            tmp_path / "f.parquet",
            base_geo(covering={"bbox": paths}),
            {"bbox": bbox_struct()},
        )
    )
    assert_verdict(
        checks,
        "covering_bbox_column_exists_geometry",
        CheckStatus.FAILED,
        "cannot determine bbox column name from covering",
    )
    assert_verdict(
        checks,
        "covering_bbox_structure_geometry",
        CheckStatus.FAILED,
        "cannot determine bbox column name",
    )
    assert_verdict(
        checks,
        "covering_bbox_field_types_geometry",
        CheckStatus.FAILED,
        "cannot determine bbox column name",
    )


def test_covering_bbox_fields_must_share_one_type(tmp_path):
    path = write_geoparquet(
        tmp_path / "f.parquet",
        base_geo(covering={"bbox": BBOX_PATHS}),
        {"bbox": bbox_struct([pa.float32(), F64, F64, F64])},
    )
    assert_verdict(
        verdicts(path),
        "covering_bbox_field_types_geometry",
        CheckStatus.FAILED,
        "bbox fields must all use the same type",
    )


@pytest.mark.parametrize(
    ("check", "name"),
    [
        (_check_covering_bbox_paths, "covering_bbox_paths_geometry"),
        (_check_covering_bbox_column_exists, "covering_bbox_column_exists_geometry"),
        (_check_covering_bbox_structure, "covering_bbox_structure_geometry"),
        (_check_covering_bbox_field_types, "covering_bbox_field_types_geometry"),
    ],
    ids=["paths", "column_exists", "structure", "field_types"],
)
def test_covering_bbox_checks_skip_a_column_with_no_covering(check, name):
    """``_run_geoparquet_checks`` gates on the same condition, so these SKIPPED arms
    are only reachable by calling the check directly -- but a caller that stops
    gating must still get a skip rather than a crash."""
    args = ({}, "geometry") if check is _check_covering_bbox_paths else ({}, "geometry", [])
    result = check(*args)
    assert result.name == name
    assert result.status == CheckStatus.SKIPPED
    assert result.message == "no bbox covering defined"


# =============================================================================
# Bug: a malformed `geo` value is indexed without checking its shape (issue #1062)
# =============================================================================


@pytest.mark.xfail(
    strict=True,
    raises=IndexError,
    reason="#1062: covering.bbox.xmin = [] crashes `gpio check spec` with IndexError",
)
def test_covering_bbox_empty_path_array_is_reported_not_crashed(tmp_path):
    path = write_geoparquet(
        tmp_path / "f.parquet",
        base_geo(covering={"bbox": dict(BBOX_PATHS, xmin=[])}),
        {"bbox": bbox_struct()},
    )
    assert_verdict(
        verdicts(path),
        "covering_bbox_column_exists_geometry",
        CheckStatus.FAILED,
        "cannot determine bbox column name",
    )


@pytest.mark.xfail(
    strict=True,
    reason="#1062: a string covering path is indexed character-wise, so the message "
    'names a column "b" that the file never mentions',
)
def test_covering_bbox_string_path_does_not_invent_a_column_name(tmp_path):
    path = write_geoparquet(
        tmp_path / "f.parquet",
        base_geo(covering={"bbox": dict(BBOX_PATHS, xmin="bbox.xmin")}),
        {"bbox": bbox_struct()},
    )
    message = verdicts(path)["covering_bbox_column_exists_geometry"].message
    assert '"b"' not in message, message


# =============================================================================
# File extension (check 1.1-8)
# =============================================================================


@pytest.mark.parametrize(
    ("suffix", "fragment"),
    [
        (".geoparquet", 'file extension is ".geoparquet" (recommend ".parquet")'),
        (".pq", "unusual file extension: .pq"),
    ],
    ids=["geoparquet_suffix", "unusual_suffix"],
)
def test_file_extension_warning(tmp_path, suffix, fragment):
    path = write_geoparquet(tmp_path / f"f{suffix}", base_geo())
    assert_verdict(verdicts(path), "file_extension", CheckStatus.WARNING, fragment)


# =============================================================================
# --geoparquet-version matching
# =============================================================================

PGO_FIXTURE = Path("tests/data/fields_pgo_crs84_zstd.parquet")


def test_target_version_match_passes(tmp_path):
    path = write_geoparquet(tmp_path / "f.parquet", base_geo())
    assert_verdict(
        verdicts(path, target_version="1.1"),
        "version_match",
        CheckStatus.PASSED,
        "file version matches requested 1.1",
    )


def test_target_version_exact_string_falls_back_to_equality(tmp_path):
    """ "1.1.0" is not one of the CLI's version buckets; the exact-match fallback
    is what makes it match a 1.1.0 file."""
    path = write_geoparquet(tmp_path / "f.parquet", base_geo())
    assert_verdict(
        verdicts(path, target_version="1.1.0"),
        "version_match",
        CheckStatus.PASSED,
        "file version matches requested 1.1.0",
    )


def test_parquet_geo_only_file_against_a_geoparquet_version():
    assert_verdict(
        verdicts(PGO_FIXTURE, target_version="1.0"),
        "version_match",
        CheckStatus.FAILED,
        "does not implement GeoParquet 1.0 metadata",
    )


def test_parquet_geo_only_file_matches_the_parquet_geo_only_target():
    checks = verdicts(PGO_FIXTURE, target_version="parquet-geo-only")
    assert "version_match" not in checks  # the target is not a version comparison
    assert_verdict(
        checks,
        "native_geo_type_present_geometry",
        CheckStatus.PASSED,
        "uses Parquet GEOMETRY logical type",
    )


@pytest.mark.parametrize(
    ("file_type", "status"),
    [("parquet_geo_only", CheckStatus.PASSED), ("geoparquet_v1", CheckStatus.FAILED)],
    ids=["match", "mismatch"],
)
def test_parquet_geo_only_target_is_matched_by_file_type(file_type, status):
    """``validate_geoparquet`` short-circuits this target before the version check
    runs, so the arm deciding it is only reachable by calling the check."""
    from geoparquet_io.core.validate import _check_version_matches

    check = _check_version_matches(
        "parquet-geo-only" if file_type == "parquet_geo_only" else "1.1.0",
        "parquet-geo-only",
        {"file_type": file_type},
    )
    assert check.name == "version_match"
    assert check.status == status


def test_geoparquet_file_has_no_native_geo_types(tmp_path):
    """``--geoparquet-version parquet-geo-only`` on a plain WKB file: the whole
    check run is one FAILED verdict naming what is missing."""
    path = write_geoparquet(tmp_path / "f.parquet", base_geo())
    checks = verdicts(path, target_version="parquet-geo-only")
    assert list(checks) == ["native_geo_type_present"]
    assert_verdict(
        checks,
        "native_geo_type_present",
        CheckStatus.FAILED,
        "no columns with Parquet GEOMETRY/GEOGRAPHY logical type found",
    )


# =============================================================================
# Parquet native geo types (GeoParquet 2.0 and parquet-geo-only)
# =============================================================================


def native_geo_file(path, wkb: list[bytes], *, crs=None, spherical=False, statistics=True) -> str:
    """A Parquet file with a native GEOMETRY/GEOGRAPHY logical type and no ``geo`` key.

    pyarrow is the only writer that produces this on purpose; DuckDB always adds
    the ``geo`` block.
    """
    import geoarrow.pyarrow as ga

    geo_type = ga.wkb()
    if spherical:
        geo_type = geo_type.with_edge_type("spherical")
    if crs is not None:
        geo_type = geo_type.with_crs(crs)
    column = pa.ExtensionArray.from_storage(geo_type, pa.array(wkb, pa.binary()))
    pq.write_table(
        pa.table({"id": pa.array(range(len(wkb))), "geometry": column}),
        str(path),
        write_statistics=statistics,
    )
    return str(path)


def test_geography_coordinates_outside_valid_bounds(tmp_path):
    path = native_geo_file(
        tmp_path / "geog.parquet", [wkb_point(1, 2), wkb_point(200.0, 95.0)], spherical=True
    )
    check = verdicts(path)["geography_coordinate_bounds_geometry"]
    assert check.status == CheckStatus.FAILED
    assert check.message == "GEOGRAPHY coordinates exceed valid bounds"
    assert "max_x=200.0 > 180" in check.details
    assert "max_y=95.0 > 90" in check.details


def test_native_geo_statistics_absent(tmp_path):
    """A native-geo file written with statistics off: the check must say the
    *statistics* are missing, not that the column is."""
    path = native_geo_file(tmp_path / "pgo.parquet", [wkb_point(1, 2)] * 2, statistics=False)
    assert_verdict(
        verdicts(path),
        "native_geo_stats_geometry",
        CheckStatus.WARNING,
        'geometry column "geometry" missing geospatial statistics',
    )
    assert_verdict(
        verdicts(path),
        "native_geo_stats_contains_data_geometry",
        CheckStatus.SKIPPED,
        "no geospatial statistics to validate against",
    )


#: ``(id, crs written into the Parquet logical type, message fragment)``.
PGO_CRS_CASES = [
    ("srid_reference", "srid:3857", "CRS format may not be widely recognized"),
    ("unresolvable_string", "WGS 84", "CRS format may not be widely recognized"),
    (
        "json_that_is_not_projjson",
        json.dumps({"not": "projjson"}),
        "CRS format may not be widely recognized by geospatial tools",
    ),
]


@pytest.mark.parametrize(
    ("crs", "fragment"),
    [case[1:] for case in PGO_CRS_CASES],
    ids=[case[0] for case in PGO_CRS_CASES],
)
def test_parquet_geo_only_crs_warning(tmp_path, crs, fragment):
    path = native_geo_file(tmp_path / "pgo.parquet", [wkb_point(1, 2)] * 2, crs=crs)
    assert_verdict(verdicts(path), "parquet_geo_only_crs_geometry", CheckStatus.WARNING, fragment)


def test_v2_geo_block_naming_a_column_the_schema_does_not_have(tmp_path):
    """Every 2.0 check that resolves the column against the schema must report
    the column missing -- silence here is how a mislabelled 2.0 file passes.

    The source carries EPSG:5070 rather than CRS84 so that ``v2_crs_in_parquet_type``
    gets past its "default CRS, nothing to inline" arm and has to resolve the
    column too.
    """
    source = Path("tests/data/fields_gpq2_5070_brotli.parquet")
    table = pq.read_table(source)
    geo = json.loads(table.schema.metadata[b"geo"])
    geo["primary_column"] = "geom"
    geo["columns"] = {"geom": next(iter(geo["columns"].values()))}
    path = tmp_path / "v2.parquet"
    pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(geo).encode()}), path)

    checks = verdicts(path)
    assert_verdict(
        checks,
        "native_geo_type_present_geom",
        CheckStatus.FAILED,
        'column "geom" not found in schema',
    )
    assert_verdict(
        checks, "v2_native_types_geom", CheckStatus.FAILED, 'column "geom" not found in schema'
    )
    assert_verdict(checks, "native_crs_format_geom", CheckStatus.SKIPPED, 'column "geom" not found')
    assert_verdict(
        checks, "geography_edges_valid_geom", CheckStatus.SKIPPED, 'column "geom" not found'
    )
    assert_verdict(
        checks,
        "native_geo_stats_geom",
        CheckStatus.WARNING,
        'geometry column "geom" not found in parquet metadata',
    )
    assert_verdict(
        checks,
        "geography_coordinate_bounds_geom",
        CheckStatus.SKIPPED,
        "not a GEOGRAPHY type, coordinate bounds check not applicable",
    )
    assert_verdict(
        checks, "v2_crs_in_parquet_type_geom", CheckStatus.FAILED, 'column "geom" not found'
    )


# =============================================================================
# Data-validation verdicts
# =============================================================================


def test_orientation_cannot_be_judged_on_a_zero_area_ring(tmp_path):
    """A collinear ring has no winding; claiming either verdict would be a guess."""
    geo = base_geo(geometry_types=["Polygon"], orientation="counterclockwise")
    ring = [(0.0, 0.0), (1.0, 1.0), (2.0, 2.0), (0.0, 0.0)]
    table = pa.table({"geometry": pa.array([wkb_polygon(ring)], pa.binary())})
    path = tmp_path / "f.parquet"
    pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(geo).encode()}), path)
    assert_verdict(
        verdicts(path),
        "orientation_matches_data_geometry",
        CheckStatus.SKIPPED,
        "no polygons whose ring orientation can be determined",
    )


class TestChecksAgainstAnUntypedColumn:
    """DuckDB materialises a declared WKB column as GEOMETRY, so these checks'
    ``ST_GeomFromWKB`` fallbacks only run on a file with no ``geo`` block --
    which is exactly what they exist for."""

    @staticmethod
    def _plain_wkb_file(path, values):
        pq.write_table(pa.table({"geometry": pa.array(values, pa.binary())}), str(path))
        return str(path)

    def test_encoding_matches_data_parses_wkb_itself(self, tmp_path):
        con = get_duckdb_connection(load_spatial=True)
        try:
            path = self._plain_wkb_file(tmp_path / "f.parquet", [WKB_POINT] * 2)
            check = _check_encoding_matches_data(path, "geometry", "WKB", con, 10)
        finally:
            con.close()
        assert check.status == CheckStatus.PASSED
        assert check.message == 'all geometry values match "WKB" encoding (2 checked)'

    def test_encoding_matches_data_reports_unparsable_bytes(self, tmp_path):
        """The verdict is FAILED, but the message is DuckDB's, not the check's own
        "N of M geometries do not match" -- ``ST_GeomFromWKB`` raises instead of
        returning NULL, so the counting arm is unreachable (#1064). Asserted as it
        is, so the day that arm starts working this test says so."""
        con = get_duckdb_connection(load_spatial=True)
        try:
            path = self._plain_wkb_file(tmp_path / "f.parquet", [WKB_POINT, b"\x00\x01\x02"])
            check = _check_encoding_matches_data(path, "geometry", "WKB", con, 10)
        finally:
            con.close()
        assert check.status == CheckStatus.FAILED
        assert check.message.startswith("failed to validate encoding:")

    def test_geometry_types_match_data_parses_wkb_itself(self, tmp_path):
        con = get_duckdb_connection(load_spatial=True)
        try:
            path = self._plain_wkb_file(tmp_path / "f.parquet", [WKB_POINT] * 2)
            check = _check_geometry_types_match_data(path, "geometry", ["Point"], con, 10, "WKB")
        finally:
            con.close()
        assert check.status == CheckStatus.PASSED
        assert "all geometry types match" in check.message

    def test_bounds_query_parses_wkb_itself(self, tmp_path):
        con = get_duckdb_connection(load_spatial=True)
        try:
            path = self._plain_wkb_file(tmp_path / "f.parquet", [WKB_POINT] * 2)
            bounds = _execute_bounds_query(con, path, "geometry", "LIMIT 10")
        finally:
            con.close()
        assert bounds == (1.0, 1.0, 2.0, 2.0)


def test_bbox_contains_data_with_no_result_row_is_skipped():
    """The query returning no row at all is not evidence the bbox holds."""
    check = _interpret_bbox_result(None, "geometry")
    assert check.name == "bbox_contains_data_geometry"
    assert check.status == CheckStatus.SKIPPED
    assert check.message == "no data to validate"


def test_geo_metadata_that_is_not_an_object_fails_the_run():
    """``validate_geoparquet`` rejects this earlier, but the check runner is public
    enough to be called directly and must not iterate a list as a dict."""
    checks = _run_geoparquet_checks(
        "unused.parquet", {b"geo": b"[]"}, [], [], {"file_type": "geoparquet_v1"}, None, 0, False
    )
    assert [check.name for check in checks] == ["geo_key_exists", "geo_metadata_parse"]
    assert checks[-1].status == CheckStatus.FAILED
    assert checks[-1].message == "failed to parse 'geo' metadata as a JSON object"


# =============================================================================
# The ratchet: a new FAILED arm must not arrive untested and unnoticed
# =============================================================================

#: Functions in ``validate.py`` that can emit a FAILED verdict which this file
#: does not exercise, each with why. Shrinking this set is the point; growing it
#: needs a reason. A *new* FAILED-emitting function appears in neither set and
#: fails ``test_every_failed_arm_is_accounted_for`` until it is placed.
FAILED_ARMS_EXERCISED_ELSEWHERE = {
    # Exercised by the wider suite (tests/test_validate_*.py, tests/e2e).
    "_check_geo_key_exists",
    "_check_metadata_is_json",
    "_check_version_present",
    "_check_version_known",
    "_check_version_features",
    "_check_primary_column_present",
    "_check_columns_present",
    "_check_primary_column_in_columns",
    "_check_encoding_valid",
    "_check_orientation_valid",
    "_check_edges_valid",
    "_check_geometry_not_grouped",
    "_check_geoarrow_layout",
    "_check_geometry_byte_array",
    "_geoarrow_layout_error",
    "_compare_geometry_types",
    "_check_orientation_matches_data",
    "_interpret_bbox_result",
    "_check_bbox_contains_data",
    "_check_covering_is_object",
    "_bbox_column_missing",
    "_check_native_columns_in_metadata",
    "_check_native_geo_stats_contains_data",
    "_check_geometry_types_match_stats",
    "_check_v2_crs_consistency",
    "_check_v2_edges_consistency",
    "_check_coordinates_valid_for_crs",
    "validate_geoparquet",
    "_run_parquet_geo_only_checks",
    # Reachable only through a defect this suite cannot manufacture:
    # `_check_native_geo_types_match` FAILS on geo_types declared in the
    # Parquet GeospatialStatistics that the data contradicts, and pyarrow --
    # the only writer that can produce a hand-built native-geo file -- does
    # not write those statistics at all.
    "_check_native_geo_types_match",
    # FAILS only on a GEOGRAPHY logical type whose `algorithm` property is
    # absent or unparsable; geoarrow.pyarrow cannot write one.
    "_check_geography_edges_valid",
}

FAILED_ARMS_EXERCISED_HERE = {
    "_check_geometry_types_list",
    "_check_crs_valid",
    "_check_bbox_valid",
    "_check_epoch_valid",
    "_check_geometry_not_repeated",
    "_check_encoding_matches_data",
    "_check_geometry_types_match_data",
    "_check_covering_bbox_paths",
    "_check_covering_bbox_column_exists",
    "_check_covering_bbox_structure",
    "_check_covering_bbox_field_types",
    "_check_native_geo_type_present",
    "_check_geography_coordinate_bounds",
    "_check_v2_uses_native_types",
    "_check_v2_crs_in_parquet_type",
    "_check_version_matches",
    "_run_geoparquet_checks",
}


def _mentions_failed(node: ast.AST) -> bool:
    return any(
        isinstance(child, ast.Attribute)
        and child.attr == "FAILED"
        and isinstance(child.value, ast.Name)
        and child.value.id == "CheckStatus"
        for child in ast.walk(node)
    )


def _functions_emitting_failed(source: str) -> set[str]:
    """Every top-level function that builds a FAILED ``ValidationCheck``.

    Matches the ``ValidationCheck(...)`` constructor and the ``_result(...)``
    closures that wrap it, so the status-symbol lookup tables in the formatting
    section -- which name ``CheckStatus.FAILED`` but report nothing -- stay out.
    """
    emitting = set()
    for node in ast.parse(source).body:
        if not isinstance(node, ast.FunctionDef):
            continue
        for child in ast.walk(node):
            if (
                isinstance(child, ast.Call)
                and isinstance(child.func, ast.Name)
                and child.func.id in ("ValidationCheck", "_result")
                and _mentions_failed(child)
            ):
                emitting.add(node.name)
                break
    return emitting


def test_every_failed_arm_is_accounted_for():
    import geoparquet_io.core.validate as validate_module

    source = Path(validate_module.__file__).read_text(encoding="utf-8")
    emitting = _functions_emitting_failed(source)
    accounted = FAILED_ARMS_EXERCISED_HERE | FAILED_ARMS_EXERCISED_ELSEWHERE

    unaccounted = emitting - accounted
    assert not unaccounted, (
        "these validate.py functions can report FAILED but no entry claims them; "
        "add a case to this file or list them in FAILED_ARMS_EXERCISED_ELSEWHERE "
        f"with a reason: {sorted(unaccounted)}"
    )
    stale = accounted - emitting
    assert not stale, f"these entries name functions that no longer report FAILED: {sorted(stale)}"
