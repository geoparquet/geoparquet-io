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
from tests.native_geo_probes import write_native_geo_only

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
        {"bbox": [0, 0, "2", 3]},  # covers POINT (1 2): the string is the only defect
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
    (
        "orientation_is_not_a_known_value",
        {"orientation": "sideways"},
        "orientation_valid_geometry",
        CheckStatus.FAILED,
        "orientation must be one of ['counterclockwise']",
    ),
]

#: Table ids whose one defect makes DuckDB refuse to *read* the file, so the
#: three data checks also FAIL, each quoting DuckDB's "Invalid Input Error"
#: rather than a verdict of their own (#1080).
CASCADING_CASES = {"geometry_types_not_a_list", "crs_is_a_bare_string"}


@pytest.mark.parametrize(
    ("override", "check_name", "status", "fragment"),
    [case[1:] for case in COLUMN_METADATA_CASES],
    ids=[case[0] for case in COLUMN_METADATA_CASES],
)
def test_column_metadata_verdict(tmp_path, override, check_name, status, fragment):
    path = write_geoparquet(tmp_path / "f.parquet", base_geo(**override))
    assert_verdict(verdicts(path), check_name, status, fragment)


def test_the_base_fixture_is_clean(tmp_path):
    """Every case above adds exactly one defect to this; a FAILED here would make
    the table vacuous."""
    checks = verdicts(write_geoparquet(tmp_path / "f.parquet", base_geo()))
    failed = [check.name for check in checks.values() if check.status == CheckStatus.FAILED]
    assert failed == []


@pytest.mark.parametrize(
    ("case_id", "override", "check_name"),
    [
        pytest.param(
            case[0],
            case[1],
            case[2],
            id=case[0],
            marks=pytest.mark.xfail(
                strict=True,
                reason="#1080: DuckDB refuses to read the file, so encoding_matches_data, "
                "geometry_types_match_data and coordinates_valid_for_crs FAIL too",
            )
            if case[0] in CASCADING_CASES
            else (),
        )
        for case in COLUMN_METADATA_CASES
    ],
)
def test_one_defect_is_one_failed_verdict(tmp_path, case_id, override, check_name):
    checks = verdicts(write_geoparquet(tmp_path / "f.parquet", base_geo(**override)))
    failed = sorted(check.name for check in checks.values() if check.status == CheckStatus.FAILED)
    assert failed == [check_name]


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


def test_a_2_0_block_over_a_plain_wkb_column(tmp_path):
    """The substantive arm of both native-type checks: the column exists, and is
    plain binary. The missing-column test below reaches only their lookup arm."""
    geo = base_geo()
    geo["version"] = "2.0.0"
    checks = verdicts(write_geoparquet(tmp_path / "f.parquet", geo))
    assert_verdict(
        checks,
        "native_geo_type_present_geometry",
        CheckStatus.FAILED,
        'column "geometry" does not have GEOMETRY/GEOGRAPHY logical type',
    )
    assert_verdict(
        checks,
        "v2_native_types_geometry",
        CheckStatus.FAILED,
        "GeoParquet 2.0 requires native Parquet GEOMETRY/GEOGRAPHY type",
    )


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
    """A native GEOMETRY/GEOGRAPHY file with no ``geo`` key (see ``write_native_geo_only``)."""
    import geoarrow.pyarrow as ga

    geo_type = ga.wkb()
    if spherical:
        geo_type = geo_type.with_edge_type("spherical")
    if crs is not None:
        geo_type = geo_type.with_crs(crs)
    rows = [(i, "g", value) for i, value in enumerate(wkb)]
    return str(
        write_native_geo_only(
            path,
            rows,
            {"geometry": (2, geo_type)},
            compression="snappy",
            write_statistics=statistics,
        )
    )


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
    checks = verdicts(path)
    assert_verdict(
        checks,
        "native_geo_stats_geometry",
        CheckStatus.WARNING,
        'geometry column "geometry" missing geospatial statistics',
    )
    assert_verdict(
        checks,
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
        "unused.parquet",
        kv_metadata={b"geo": b"[]"},
        geo_meta=[],
        schema_info=[],
        file_type_info={"file_type": "geoparquet_v1"},
        con=None,
        sample_size=0,
        validate_data=False,
    )
    assert [check.name for check in checks] == ["geo_key_exists", "geo_metadata_parse"]
    assert checks[-1].status == CheckStatus.FAILED
    assert checks[-1].message == "failed to parse 'geo' metadata as a JSON object"


# =============================================================================
# The ratchet: a new FAILED arm must not arrive untested and unnoticed
# =============================================================================

#: Every ``validate.py`` function that can report FAILED, and where that arm is
#: exercised: ``HERE`` (this file), or a reason it is not. Shrinking the reasons
#: is the point; a *new* FAILED-reporting function is in neither and fails
#: ``test_every_failed_arm_is_accounted_for`` until it is placed. Every key must
#: resolve to a function in the module.
HERE = "this file"
ELSEWHERE = "tests/test_validate_*.py, tests/e2e"
FAILED_ARMS = {
    "_check_geometry_types_list": HERE,
    "_check_crs_valid": HERE,
    "_check_bbox_valid": HERE,
    "_check_epoch_valid": HERE,
    "_check_orientation_valid": HERE,
    "_check_geometry_not_grouped": HERE,
    "_check_geometry_byte_array": HERE,
    "_check_geometry_not_repeated": HERE,
    "_check_encoding_matches_data": HERE,
    "_check_geometry_types_match_data": HERE,
    "_check_covering_bbox_paths": HERE,
    "_covering_bbox_column": HERE,
    "_check_covering_bbox_structure": HERE,
    "_check_covering_bbox_field_types": HERE,
    "_check_native_geo_type_present": HERE,
    "_check_geography_coordinate_bounds": HERE,
    "_check_v2_uses_native_types": HERE,
    "_check_v2_crs_in_parquet_type": HERE,
    "_check_version_matches": HERE,
    "_run_geoparquet_checks": HERE,
    "_check_geo_key_exists": ELSEWHERE,
    "_check_metadata_is_json": ELSEWHERE,
    "_check_version_present": ELSEWHERE,
    "_check_version_known": ELSEWHERE,
    "_check_version_features": ELSEWHERE,
    "_check_primary_column_present": ELSEWHERE,
    "_check_columns_present": ELSEWHERE,
    "_check_primary_column_in_columns": ELSEWHERE,
    "_check_encoding_valid": ELSEWHERE,
    "_check_edges_valid": ELSEWHERE,
    "_check_geoarrow_layout": ELSEWHERE,
    "_geoarrow_layout_error": ELSEWHERE,
    "_compare_geometry_types": ELSEWHERE,
    "_check_orientation_matches_data": ELSEWHERE,
    "_interpret_bbox_result": ELSEWHERE,
    "_check_bbox_contains_data": "tests/test_validate_bbox_dimensions.py",
    "_bbox_column_missing": ELSEWHERE,
    "_check_native_columns_in_metadata": ELSEWHERE,
    "_check_native_geo_stats_contains_data": ELSEWHERE,
    "_check_geometry_types_match_stats": ELSEWHERE,
    "_check_v2_crs_consistency": ELSEWHERE,
    "_check_v2_edges_consistency": ELSEWHERE,
    "_check_coordinates_valid_for_crs": ELSEWHERE,
    "validate_geoparquet": ELSEWHERE,
    "_run_parquet_geo_only_checks": ELSEWHERE,
    "_check_covering_is_object": HERE,
    "_check_native_geo_types_match": (
        "FAILS when the data holds a type the Parquet GeospatialStatistics do not "
        "declare. pyarrow computes those statistics from the data, so no writer in "
        "the suite can produce the contradiction"
    ),
    "_check_geography_edges_valid": (
        "FAILS only on a GEOGRAPHY logical type whose `algorithm` is absent or "
        "unparsable; geoarrow.pyarrow cannot write one"
    ),
}

#: Functions that name ``CheckStatus.FAILED`` without reporting a verdict: the
#: symbol and colour lookup tables in the formatting section.
FORMATTING_ONLY = {"_get_check_color", "_get_check_symbol"}


def _functions_mentioning_failed(source: str) -> set[str]:
    """Every top-level function whose body names ``CheckStatus.FAILED``, however it
    builds the verdict (constructor, ``_result`` closure, ``status = ...``, a helper)."""
    return {
        node.name
        for node in ast.parse(source).body
        if isinstance(node, ast.FunctionDef)
        and any(
            isinstance(child, ast.Attribute)
            and child.attr == "FAILED"
            and isinstance(child.value, ast.Name)
            and child.value.id == "CheckStatus"
            for child in ast.walk(node)
        )
    }


def test_every_failed_arm_is_accounted_for():
    import geoparquet_io.core.validate as validate_module

    source = Path(validate_module.__file__).read_text(encoding="utf-8")
    emitting = _functions_mentioning_failed(source) - FORMATTING_ONLY

    unaccounted = emitting - set(FAILED_ARMS)
    assert not unaccounted, (
        "these validate.py functions can report FAILED but no entry claims them; "
        "add a case to this file or list them in FAILED_ARMS with a reason: "
        f"{sorted(unaccounted)}"
    )
    stale = set(FAILED_ARMS) - emitting
    assert not stale, f"these entries name functions that no longer report FAILED: {sorted(stale)}"
    for name in FAILED_ARMS:
        assert callable(getattr(validate_module, name)), name


def test_every_here_entry_is_asserted_in_this_file():
    """A ``HERE`` claim is checked against this file's own source: the function's
    check name must appear in an assertion, so a deleted case cannot leave a
    stale claim behind."""
    source = Path(__file__).read_text(encoding="utf-8")
    for name, where in FAILED_ARMS.items():
        if where is not HERE:
            continue
        check_name = name.removeprefix("_check_").removeprefix("_run_")
        assert check_name in source or name in source, f"{name} is claimed HERE but not asserted"


# =============================================================================
# A malformed `geo` value gets a verdict that names it, never a traceback (#1062)
#
# The defect was one shape: a value whose *type* one check had judged wrong
# was indexed by a later check as if it had the right type. Each sweep below
# holds one representative per branch of the guard that now sits in front of
# that read.
# =============================================================================

COVERING_BBOX_CHECKS = (
    "covering_bbox_paths_geometry",
    "covering_bbox_column_exists_geometry",
    "covering_bbox_structure_geometry",
    "covering_bbox_field_types_geometry",
)


@pytest.mark.parametrize(
    "covering",
    [
        pytest.param("a bbox here", id="string"),
        pytest.param(["bbox"], id="list"),
        pytest.param(5, id="int"),
    ],
)
def test_covering_that_is_not_an_object(tmp_path, covering):
    """One verdict names the wrong type; the four bbox checks do not run at all.

    A string satisfies ``"bbox" in covering`` and ``covering["bbox"]`` both."""
    checks = verdicts(
        write_geoparquet(
            tmp_path / "f.parquet", base_geo(covering=covering), {"bbox": bbox_struct()}
        )
    )
    assert_verdict(
        checks,
        "covering_is_object_geometry",
        CheckStatus.FAILED,
        f"covering must be an object (found: {covering!r})",
    )
    assert not [name for name in checks if name.startswith("covering_bbox_")], sorted(checks)


@pytest.mark.parametrize(
    "bbox_covering",
    [
        pytest.param("bbox", id="string"),
        pytest.param(["xmin", "ymin"], id="list"),
        pytest.param(None, id="null"),
    ],
)
def test_covering_bbox_that_is_not_an_object(tmp_path, bbox_covering):
    """All four checks reject the value and quote it, none reads keys off it."""
    checks = verdicts(
        write_geoparquet(
            tmp_path / "f.parquet",
            base_geo(covering={"bbox": bbox_covering}),
            {"bbox": bbox_struct()},
        )
    )
    for name in COVERING_BBOX_CHECKS:
        assert_verdict(
            checks,
            name,
            CheckStatus.FAILED,
            f"covering bbox must be an object of [column, field] paths (found: {bbox_covering!r})",
        )


@pytest.mark.parametrize(
    "path_value",
    [
        pytest.param([], id="empty_list"),
        pytest.param("bbox.xmin", id="string"),
        pytest.param(5, id="int"),
        pytest.param({"column": "bbox", "field": "xmin"}, id="object"),
        pytest.param([None, None], id="list_of_nulls"),
    ],
)
def test_covering_bbox_path_that_is_not_a_path_array(tmp_path, path_value):
    """Every verdict quotes the offending value; none invents a column name
    (``"bbox.xmin"[0]`` is ``"b"``)."""
    checks = verdicts(
        write_geoparquet(
            tmp_path / "f.parquet",
            base_geo(covering={"bbox": dict(BBOX_PATHS, xmin=path_value)}),
            {"bbox": bbox_struct()},
        )
    )
    assert_verdict(
        checks,
        "covering_bbox_paths_geometry",
        CheckStatus.FAILED,
        f"covering bbox xmin must be a path array [column, field] (found: {path_value!r})",
    )
    for name in COVERING_BBOX_CHECKS[1:]:
        assert_verdict(
            checks,
            name,
            CheckStatus.FAILED,
            "cannot determine bbox column name from covering: its xmin must be a path array "
            f"[column, field] (found: {path_value!r})",
        )


@pytest.mark.parametrize(
    ("bbox", "fragment"),
    [
        pytest.param(
            {"xmin": 0, "ymin": 0, "xmax": 1, "ymax": 1},
            "bbox must be an array",
            id="four_key_object",
        ),
        pytest.param("1234", "bbox must be an array", id="four_character_string"),
        pytest.param(5, "bbox must be an array", id="int"),
        pytest.param(["0", "0", "2", "3"], "bbox elements must be numbers", id="numeric_strings"),
        pytest.param([True, False, True, True], "bbox elements must be numbers", id="bools"),
        pytest.param([0, 0, float("inf"), 3], "bbox elements must be numbers", id="infinity"),
        pytest.param(
            [0, 0, 10**400, 3], "bbox elements must be numbers", id="int_too_large_for_a_double"
        ),
    ],
)
def test_bbox_that_is_not_four_finite_numbers(tmp_path, bbox, fragment):
    """The metadata check FAILs and the data check skips; nothing is coerced into SQL.

    ``"1234"`` and ``["0", "0", "2", "3"]`` are the quiet half of the bug: they
    never crashed, they were read as numbers and the data was judged against them.
    """
    checks = verdicts(write_geoparquet(tmp_path / "f.parquet", base_geo(bbox=bbox)))
    assert_verdict(checks, "bbox_valid_geometry", CheckStatus.FAILED, fragment)
    assert_verdict(checks, "bbox_contains_data_geometry", CheckStatus.SKIPPED, "is not valid")


def test_a_huge_value_is_quoted_briefly(tmp_path):
    """The report quotes what it found, bounded: a 10 MB string is not a 40 MB report."""
    checks = verdicts(
        write_geoparquet(
            tmp_path / "f.parquet",
            base_geo(covering={"bbox": dict(BBOX_PATHS, xmin="A" * 10_000_000)}),
            {"bbox": bbox_struct()},
        )
    )
    assert len(checks["covering_bbox_paths_geometry"].message) < 300


def test_a_malformed_covering_does_not_crash_inspect_meta(tmp_path):
    """The same reads live in the ``inspect meta`` printer (#1062)."""
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    for covering in ("bbox", {"bbox": "bbox"}, {"bbox": dict(BBOX_PATHS, xmin="bbox.xmin")}):
        path = write_geoparquet(
            tmp_path / "f.parquet", base_geo(covering=covering), {"bbox": bbox_struct()}
        )
        result = CliRunner().invoke(cli, ["inspect", "meta", path])
        assert result.exit_code == 0, result.output
        assert "Column: b\n" not in result.output


def test_a_covering_path_naming_a_missing_column_is_reported_by_name(tmp_path):
    """The one place a covering-named column reaches a message: bounded and printable."""
    name = "\x1b[32mgreen\x1b[0m" + "x" * 500
    checks = verdicts(
        write_geoparquet(
            tmp_path / "f.parquet",
            base_geo(covering={"bbox": {k: [name, k] for k in ("xmin", "ymin", "xmax", "ymax")}}),
        )
    )
    message = checks["covering_bbox_column_exists_geometry"].message
    assert "\x1b" not in message and len(message) < 400
