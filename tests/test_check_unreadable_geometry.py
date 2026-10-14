"""``gpio check`` on geometry it cannot read, or values that hold nothing (#624, #1082).

A front-door command must never end in a raw traceback and must never pass a
value it did not read. These files are the shapes the issues report:

* a ``geoarrow.point`` struct column with no ``geo`` key (R ``sf`` + geoarrow);
* a ``geo.primary_column`` that names a column the file does not have;
* a ``geo`` value that is not JSON;
* a zero-length WKB value, which is not WKB at all;
* a NULL geometry, which the spec allows and no check may trip over.
"""

from __future__ import annotations

import json
import struct

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.check_spatial_order import check_spatial_order
from geoparquet_io.core.duckdb_utils import disable_geoparquet_conversion
from geoparquet_io.core.file_type import detect_geoparquet_file_type
from geoparquet_io.core.geometry_detection import (
    count_geometry_presence,
    resolve_geometry_read,
)
from geoparquet_io.core.validate import CheckStatus, validate_geoparquet


def _wkb_point(x: float, y: float) -> bytes:
    return struct.pack("<BIdd", 1, 1, x, y)


_GEO_V11 = {
    "version": "1.1.0",
    "primary_column": "geometry",
    "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
}


def _write_wkb_file(path, geoms, geo) -> str:
    table = pa.table({"id": pa.array(range(len(geoms))), "geometry": pa.array(geoms, pa.binary())})
    if geo is not None:
        raw = geo if isinstance(geo, bytes) else json.dumps(geo).encode()
        table = table.replace_schema_metadata({b"geo": raw})
    pq.write_table(table, path)
    return str(path)


@pytest.fixture
def geoarrow_point_file(tmp_path) -> str:
    """A geoarrow.point struct column and no ``geo`` key, as R sf + geoarrow writes."""
    coords = pa.StructArray.from_arrays(
        [pa.array([0.0, 1.0, 2.0, 3.0]), pa.array([0.0, 1.0, 2.0, 3.0])], names=["x", "y"]
    )
    field = pa.field(
        "geom",
        coords.type,
        metadata={
            b"ARROW:extension:name": b"geoarrow.point",
            b"ARROW:extension:metadata": b"{}",
        },
    )
    schema = pa.schema([pa.field("id", pa.int64()), field])
    table = pa.Table.from_arrays([pa.array([1, 2, 3, 4]), coords], schema=schema)
    path = tmp_path / "geoarrow_point.parquet"
    pq.write_table(table, path)
    return str(path)


@pytest.fixture
def missing_column_file(tmp_path) -> str:
    geo = json.loads(json.dumps(_GEO_V11))
    geo["primary_column"] = "nope"
    points = [_wkb_point(i, i) for i in range(3)]
    return _write_wkb_file(tmp_path / "missing_col.parquet", points, geo)


@pytest.fixture
def non_json_geo_file(tmp_path) -> str:
    points = [_wkb_point(i, i) for i in range(3)]
    return _write_wkb_file(tmp_path / "bad_json.parquet", points, b"not json{")


@pytest.fixture
def empty_wkb_file(tmp_path) -> str:
    geoms = [_wkb_point(0, 0), b"", _wkb_point(1, 1)]
    return _write_wkb_file(tmp_path / "empty_wkb.parquet", geoms, _GEO_V11)


@pytest.fixture
def null_geometry_file(tmp_path) -> str:
    geoms = [_wkb_point(0, 0), None, _wkb_point(1, 1)]
    return _write_wkb_file(tmp_path / "null_geom.parquet", geoms, _GEO_V11)


def _invoke(*args):
    return CliRunner().invoke(cli, list(args))


def _assert_no_traceback(result):
    """Exited through Click, not through an escaped exception."""
    if result.exception is not None:
        assert isinstance(result.exception, SystemExit), repr(result.exception)
    assert "Traceback" not in result.output


def _checks_by_prefix(report, prefix):
    return [c for c in report.checks if c.name.startswith(prefix)]


# =============================================================================
# The shared resolver: one answer to "how is this column read?"
# =============================================================================


class TestResolveGeometryRead:
    def _resolve(self, path, column):
        con = duckdb.connect()
        con.execute("LOAD spatial")
        disable_geoparquet_conversion(con)
        try:
            return resolve_geometry_read(con, path, column)
        finally:
            con.close()

    def test_wkb_blob_reads_through_st_geomfromwkb(self, null_geometry_file):
        read = self._resolve(null_geometry_file, "geometry")
        assert read.kind == "wkb"
        assert read.readable
        # A zero-length or unparsable value maps to NULL instead of failing the decode.
        # The THEN branch reads a flat vector too. A bare ST_GeomFromWKB there is
        # evaluated over a whole dictionary and raises on its bad entry.
        assert read.expr == (
            f"(CASE WHEN {read.parseable_sql} "
            'THEN ST_GeomFromWKB(list_extract(["geometry"], 1)) END)'
        )
        assert 'octet_length("geometry") > 0' in read.parseable_sql
        # TRY() on a flat vector, so one bad row does not null its dictionary neighbours.
        assert 'TRY(ST_GeomFromWKB(list_extract(["geometry"], 1)))' in read.parseable_sql
        assert read.parseable_sql in read.present_sql

    def test_geoarrow_struct_is_readable(self, geoarrow_point_file):
        read = self._resolve(geoarrow_point_file, "geom")
        assert read.kind == "geoarrow"
        assert read.readable
        assert read.problem is None

    def test_missing_column_has_a_verdict(self, missing_column_file):
        read = self._resolve(missing_column_file, "nope")
        assert read.kind == "missing"
        assert not read.readable
        assert 'geometry column "nope" not found' in read.problem

    def test_non_geometry_column_is_unsupported(self, null_geometry_file):
        read = self._resolve(null_geometry_file, "id")
        assert read.kind == "unsupported"
        assert not read.readable
        assert "BIGINT" in read.problem

    def test_presence_counts_nulls_and_empties(self, empty_wkb_file, null_geometry_file):
        con = duckdb.connect()
        con.execute("LOAD spatial")
        disable_geoparquet_conversion(con)
        try:
            empty = count_geometry_presence(
                con, empty_wkb_file, resolve_geometry_read(con, empty_wkb_file, "geometry"), ""
            )
            null = count_geometry_presence(
                con,
                null_geometry_file,
                resolve_geometry_read(con, null_geometry_file, "geometry"),
                "",
            )
        finally:
            con.close()
        assert (empty.total, empty.nulls, empty.empties, empty.first_empty_row) == (3, 0, 1, 1)
        assert (empty.unparsable, empty.first_unparsable_row) == (0, None)
        assert (null.total, null.nulls, null.empties, null.first_empty_row) == (3, 1, 0, None)

    def test_presence_counts_unparsable_values(self, tmp_path):
        path = _write_wkb_file(
            tmp_path / "garbage.parquet",
            [_wkb_point(0, 0), b"\x00\x01\x02", b"", _wkb_point(1, 1)],
            _GEO_V11,
        )
        con = duckdb.connect()
        con.execute("LOAD spatial")
        disable_geoparquet_conversion(con)
        try:
            presence = count_geometry_presence(
                con, path, resolve_geometry_read(con, path, "geometry"), ""
            )
        finally:
            con.close()
        assert (presence.empties, presence.first_empty_row) == (1, 2)
        assert (presence.unparsable, presence.first_unparsable_row) == (1, 1)


# =============================================================================
# #624: geoarrow struct, missing column, non-JSON geo
# =============================================================================


class TestGeoarrowStructFile:
    def test_not_classified_as_native_parquet_geometry(self, geoarrow_point_file):
        info = detect_geoparquet_file_type(geoarrow_point_file)
        # The struct still counts as geometry with prunable stats (#461) ...
        assert info["has_native_geo_types"] is True
        # ... but it is not the Parquet GEOMETRY logical type (#624).
        assert info["file_type"] != "parquet_geo_only"

    def test_spatial_order_reads_the_struct(self, geoarrow_point_file):
        result = check_spatial_order(geoarrow_point_file, 10, 1000, False, return_results=True)
        assert result.get("error") is None
        assert result["ratio"] is not None

    @pytest.mark.parametrize("subcommand", ["all", "spatial"])
    def test_cli_does_not_crash(self, geoarrow_point_file, subcommand):
        result = _invoke("check", subcommand, geoarrow_point_file)
        _assert_no_traceback(result)
        assert "native Parquet GEOMETRY" not in result.output


class TestMissingPrimaryColumn:
    def test_spatial_order_reports_instead_of_raising(self, missing_column_file):
        result = check_spatial_order(missing_column_file, 10, 1000, False, return_results=True)
        assert result["passed"] is False
        assert 'geometry column "nope" not found' in result["error"]
        assert result["fix_available"] is False

    def test_check_spatial_exits_cleanly_non_zero(self, missing_column_file):
        result = _invoke("check", "spatial", missing_column_file)
        _assert_no_traceback(result)
        assert result.exit_code == 1
        assert 'geometry column "nope" not found' in result.output

    def test_check_all_still_runs_spec_validation(self, missing_column_file):
        result = _invoke("check", "all", missing_column_file)
        _assert_no_traceback(result)
        assert "Spec Validation:" in result.output
        # A check that could not run is not a pass.
        assert result.exit_code == 1


class TestNonJsonGeo:
    def test_spatial_order_reads_the_blob_as_wkb(self, non_json_geo_file):
        result = check_spatial_order(non_json_geo_file, 10, 1000, False, return_results=True)
        assert result.get("error") is None
        assert result["ratio"] is not None

    @pytest.mark.parametrize("subcommand", ["all", "spatial"])
    def test_cli_does_not_crash(self, non_json_geo_file, subcommand):
        result = _invoke("check", subcommand, non_json_geo_file)
        _assert_no_traceback(result)


# =============================================================================
# #1082: zero-length WKB and NULL geometry
# =============================================================================


class TestZeroLengthWkb:
    def test_encoding_check_fails_naming_the_row(self, empty_wkb_file):
        report = validate_geoparquet(empty_wkb_file, validate_data=True, sample_size=1000)
        (encoding,) = _checks_by_prefix(report, "encoding_matches_data_")
        assert encoding.status is CheckStatus.FAILED
        assert "zero-length" in encoding.message
        assert "row 1" in encoding.message

    def test_type_and_coordinate_checks_read_the_other_rows(self, empty_wkb_file):
        report = validate_geoparquet(empty_wkb_file, validate_data=True, sample_size=1000)
        (types,) = _checks_by_prefix(report, "geometry_types_match_data_")
        (coords,) = _checks_by_prefix(report, "coordinates_valid_for_crs_")
        assert types.status is CheckStatus.PASSED, types.message
        assert coords.status is CheckStatus.PASSED, coords.message
        assert "Invalid Input Error" not in types.message + coords.message

    def test_spatial_order_skips_the_empty_value(self, empty_wkb_file):
        result = check_spatial_order(empty_wkb_file, 10, 1000, False, return_results=True)
        assert result.get("error") is None

    def test_check_all_runs_every_check(self, empty_wkb_file):
        result = _invoke("check", "all", empty_wkb_file)
        _assert_no_traceback(result)
        assert "Spatial Order Analysis:" in result.output
        assert "Spec Validation:" in result.output
        assert "Invalid Input Error" not in result.output


class TestNullGeometry:
    def test_validation_passes_the_data_checks(self, null_geometry_file):
        report = validate_geoparquet(null_geometry_file, validate_data=True, sample_size=1000)
        data_checks = [c for c in report.checks if c.category == "data_validation"]
        assert data_checks
        assert all(c.status is not CheckStatus.FAILED for c in data_checks), [
            (c.name, c.message) for c in data_checks
        ]
        (encoding,) = _checks_by_prefix(report, "encoding_matches_data_")
        assert "(2 checked" in encoding.message

    def test_check_all_passes(self, null_geometry_file):
        result = _invoke("check", "all", null_geometry_file)
        _assert_no_traceback(result)
        assert result.exit_code == 0


# =============================================================================
# check all keeps going past a check that errors
# =============================================================================


def test_check_all_runs_spec_after_spatial_order_raises(null_geometry_file, monkeypatch):
    from geoparquet_io.cli.commands import check as cli_check

    def boom(*_args, **_kwargs):
        raise RuntimeError("spatial exploded")

    monkeypatch.setattr(cli_check, "check_spatial_impl", boom)
    result = _invoke("check", "all", null_geometry_file)
    _assert_no_traceback(result)
    assert "spatial exploded" in result.output
    assert "Spec Validation:" in result.output
    assert result.exit_code == 1


# =============================================================================
# A dictionary-encoded column with one bad value, at a size DuckDB vectorizes
# =============================================================================


@pytest.fixture
def dictionary_file_with_one_bad_value(tmp_path) -> str:
    """250 dictionary-encoded points (10 distinct) with garbage at row 7."""
    geoms = [_wkb_point(i % 10, i % 10) for i in range(250)]
    geoms[7] = b"\x00\x01\x02"
    table = pa.table({"id": pa.array(range(250)), "geometry": pa.array(geoms, pa.binary())})
    table = table.replace_schema_metadata({b"geo": json.dumps(_GEO_V11).encode()})
    path = tmp_path / "dict_bad.parquet"
    pq.write_table(table, path, use_dictionary=True)
    return str(path)


class TestDictionaryColumnWithBadValue:
    def test_spatial_order_reads_the_other_rows(self, dictionary_file_with_one_bad_value):
        result = check_spatial_order(
            dictionary_file_with_one_bad_value, 10, 1000, False, return_results=True
        )
        assert result.get("error") is None
        assert result["ratio"] is not None

    def test_check_spatial_gives_a_verdict(self, dictionary_file_with_one_bad_value):
        result = _invoke("check", "spatial", dictionary_file_with_one_bad_value)
        _assert_no_traceback(result)
        assert "Invalid Input Error" not in result.output
        assert "could not run" not in result.output


def test_spatial_order_lets_an_interrupt_through(null_geometry_file, monkeypatch):
    """An interrupt is a duckdb.Error subclass, and it must not become a verdict."""
    from geoparquet_io.core import check_spatial_order as spatial_mod

    def interrupted(*_args, **_kwargs):
        raise duckdb.InterruptException("INTERRUPT Error: Interrupted!")

    monkeypatch.setattr(spatial_mod, "_sample_distances", interrupted)
    with pytest.raises(duckdb.InterruptException):
        check_spatial_order(null_geometry_file, 10, 1000, False, return_results=True)


@pytest.mark.parametrize("subcommand", ["spatial", "all"])
def test_unreadable_geometry_reason_is_printed_once(missing_column_file, subcommand):
    result = _invoke("check", subcommand, missing_column_file)
    _assert_no_traceback(result)
    assert result.exit_code == 1
    assert result.output.count('geometry column "nope" not found') == 1, result.output


def test_check_spatial_counts_the_unreadable_file_as_failed(missing_column_file, tmp_path):
    """Two files, so the multi-file summary shows how the error file was counted."""
    import shutil

    second = tmp_path / "second.parquet"
    shutil.copy(missing_column_file, second)
    result = _invoke("check", "spatial", str(tmp_path / "*.parquet"), "--all-files")
    _assert_no_traceback(result)
    assert result.exit_code == 1
    assert "2 failed" in result.output, result.output
    assert result.output.count('geometry column "nope" not found') == 2, result.output
