"""Empty and unparsable WKB values are counted, not fatal (#1082, #1064).

``gpio check spec`` must judge the bytes a file stores. A zero-length value or a
value ``ST_GeomFromWKB`` cannot parse used to either pass ``encoding_matches_data``
(DuckDB's GeoParquet reader decodes lazily, so a ``COUNT(*)`` never touched it) or
abort the scan with DuckDB's raw error, and then fail every later data check with
the same error, or, for an internal error, with "database has been invalidated".

Most files here are written with pyarrow's default writer, which
dictionary-encodes the column. That is the shape on which ``TRY()`` nulls a whole
vector when one row fails, so the counts below are the regression guard.
Carried over from #1249.
"""

from __future__ import annotations

import json
import struct
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core import duckdb_utils, validate
from geoparquet_io.core.validate import CheckStatus, validate_geoparquet

IDECOR = Path(__file__).parent / "data" / "idecor_carlospaz_invalid_wkb.parquet"
GARBAGE = b"\x00\x01\x02"
NULL_HINT = ". Write NULL for a missing geometry, or an empty geometry such as POINT EMPTY"


def wkb_point(x: float, y: float) -> bytes:
    return b"\x01" + struct.pack("<I", 1) + struct.pack("<dd", x, y)


def write_wkb_file(path: Path, values: list, geometry_types=None, use_dictionary=True) -> str:
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": geometry_types or []}},
    }
    table = pa.Table.from_pydict({"geometry": pa.array(values, pa.binary())})
    table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    pq.write_table(table, str(path), use_dictionary=use_dictionary)
    return str(path)


def verdicts(path: str, **kwargs) -> dict:
    return {check.name: check for check in validate_geoparquet(path, **kwargs).checks}


def failed(checks: dict) -> list[str]:
    return sorted(name for name, check in checks.items() if check.status == CheckStatus.FAILED)


def test_empty_wkb_is_its_own_failed_verdict(tmp_path):
    path = write_wkb_file(
        tmp_path / "f.parquet", [wkb_point(1, 2), b"", wkb_point(3, 4)], ["Point"]
    )
    checks = verdicts(path)

    assert checks["encoding_matches_data_geometry"].message == (
        '1 of 3 geometries in column "geometry" are empty (zero-length) WKB, '
        "first at row 1 (counting from 0)" + NULL_HINT
    )
    # The empty value is reported once. The other data checks judge the two
    # values that parse, instead of repeating DuckDB's error.
    assert failed(checks) == ["encoding_matches_data_geometry"]
    assert checks["geometry_types_match_data_geometry"].message == (
        'all geometry types match declared "geometry_types" (2 checked)'
    )
    assert checks["coordinates_valid_for_crs_geometry"].status == CheckStatus.PASSED


@pytest.mark.parametrize("use_dictionary", [True, False], ids=["dictionary", "plain"])
def test_unparsable_wkb_is_counted(tmp_path, use_dictionary):
    path = write_wkb_file(
        tmp_path / "f.parquet",
        [wkb_point(1, 2), GARBAGE, wkb_point(3, 4)],
        ["Point"],
        use_dictionary=use_dictionary,
    )
    checks = verdicts(path)

    assert checks["encoding_matches_data_geometry"].message == (
        '1 of 3 geometries do not match "WKB" encoding, first at row 1 (counting from 0)'
    )
    assert failed(checks) == ["encoding_matches_data_geometry"]
    assert checks["geometry_types_match_data_geometry"].message == (
        'all geometry types match declared "geometry_types" (2 checked)'
    )


def test_valid_unparsable_and_empty_in_one_column(tmp_path):
    path = write_wkb_file(tmp_path / "f.parquet", [wkb_point(1, 2), GARBAGE, b""], ["Point"])
    checks = verdicts(path)

    encoding = checks["encoding_matches_data_geometry"]
    assert encoding.status == CheckStatus.FAILED
    assert encoding.message == (
        '1 of 3 geometries in column "geometry" are empty (zero-length) WKB, '
        "first at row 2 (counting from 0). "
        '1 of 3 geometries do not match "WKB" encoding, first at row 1 (counting from 0)'
        + NULL_HINT
    )
    assert failed(checks) == ["encoding_matches_data_geometry"]
    for name in ("geometry_types_match_data_geometry", "coordinates_valid_for_crs_geometry"):
        assert checks[name].status == CheckStatus.PASSED, checks[name].message


def test_null_geometries_are_allowed(tmp_path):
    path = write_wkb_file(tmp_path / "f.parquet", [wkb_point(1, 2), None], ["Point"])
    checks = verdicts(path)

    assert checks["encoding_matches_data_geometry"].message == (
        'all geometry values match "WKB" encoding (1 checked)'
    )
    assert failed(checks) == []


def test_only_null_geometries_have_nothing_to_validate(tmp_path):
    path = write_wkb_file(tmp_path / "f.parquet", [None, None], ["Point"])
    encoding = verdicts(path)["encoding_matches_data_geometry"]

    assert encoding.status == CheckStatus.SKIPPED
    assert encoding.message == "no data to validate"


def _idecor_values() -> list:
    return pq.read_table(IDECOR, columns=["geometry"]).column("geometry").to_pylist()


def test_real_idecor_file_parses_every_value(tmp_path):
    """64 MultiPolygons, some invalid as geometries but all well-formed WKB. The
    #737/#747 file, run through TRY() on its real data layout."""
    path = write_wkb_file(tmp_path / "f.parquet", _idecor_values(), ["MultiPolygon"])
    checks = verdicts(path, sample_size=0)

    assert checks["encoding_matches_data_geometry"].message == (
        'all geometry values match "WKB" encoding (64 checked)'
    )
    assert checks["geometry_types_match_data_geometry"].message == (
        'all geometry types match declared "geometry_types" (64 checked)'
    )


def test_real_idecor_file_with_bad_values_mixed_in(tmp_path):
    values = _idecor_values()
    values[10:10] = [GARBAGE]
    values[40:40] = [b""]
    path = write_wkb_file(tmp_path / "f.parquet", values, ["MultiPolygon"])
    checks = verdicts(path, sample_size=0)

    assert checks["encoding_matches_data_geometry"].message == (
        '1 of 66 geometries in column "geometry" are empty (zero-length) WKB, '
        "first at row 40 (counting from 0). "
        '1 of 66 geometries do not match "WKB" encoding, first at row 10 (counting from 0)'
        + NULL_HINT
    )
    assert checks["geometry_types_match_data_geometry"].message == (
        'all geometry types match declared "geometry_types" (64 checked)'
    )


class _PoisonableConnection:
    """A real connection that a query marked ``/*poison*/`` invalidates, the way a
    DuckDB InternalException does. That query raises it, and every later one raises
    the "database has been invalidated" error DuckDB gives afterwards."""

    def __init__(self, inner):
        self._inner = inner
        self.poisoned = False

    def execute(self, query, *args):
        if self.poisoned:
            raise duckdb.FatalException(
                "FATAL Error: Failed: database has been invalidated because of a previous fatal error"
            )
        if "/*poison*/" in query:
            self.poisoned = True
            raise duckdb.InternalException("INTERNAL Error: Buffer overflow")
        return self._inner.execute(query, *args)

    def close(self):
        self._inner.close()


def test_a_check_that_invalidates_the_connection_does_not_poison_the_rest(tmp_path, monkeypatch):
    real_factory = duckdb_utils.get_duckdb_connection
    opened = []

    def factory(*args, **kwargs):
        con = _PoisonableConnection(real_factory(*args, **kwargs))
        opened.append(con)
        return con

    def poisoning_types_check(parquet_file, geom_col, declared_types, con, *args, **kwargs):
        try:
            con.execute("SELECT 1 /*poison*/")
        except duckdb.Error as e:
            return validate.ValidationCheck(
                name=f"geometry_types_match_data_{geom_col}",
                status=CheckStatus.FAILED,
                message=f"failed to validate geometry types: {e}",
                category="data_validation",
            )
        raise AssertionError("the poison query must raise")

    monkeypatch.setattr(duckdb_utils, "get_duckdb_connection", factory)
    monkeypatch.setattr(validate, "_check_geometry_types_match_data", poisoning_types_check)

    path = write_wkb_file(tmp_path / "f.parquet", [wkb_point(1, 2)] * 2, ["Point"])
    checks = verdicts(path)

    assert "Buffer overflow" in checks["geometry_types_match_data_geometry"].message
    # The check after the poisoned one runs on a fresh connection and judges data.
    coordinates = checks["coordinates_valid_for_crs_geometry"]
    assert coordinates.status == CheckStatus.PASSED, coordinates.message
    assert failed(checks) == ["geometry_types_match_data_geometry"]
    assert len(opened) == 2


def _dictionary_file_with_one_bad_value(tmp_path) -> str:
    """250 dictionary-encoded points (10 distinct) with garbage at row 7.

    DuckDB 1.5.6 evaluates a bare ``ST_GeomFromWKB`` in a CASE branch over the
    whole dictionary once a file has about 100 rows, so a guard that only
    protects the WHEN still raises.
    """
    values = [wkb_point(i % 10, i % 10) for i in range(250)]
    values[7] = GARBAGE
    return write_wkb_file(tmp_path / "dict.parquet", values, ["Point"], use_dictionary=True)


def test_dictionary_column_with_a_bad_value_is_counted_on_a_full_scan(tmp_path):
    path = _dictionary_file_with_one_bad_value(tmp_path)
    checks = verdicts(path, sample_size=0)

    assert checks["encoding_matches_data_geometry"].message == (
        '1 of 250 geometries do not match "WKB" encoding, first at row 7 (counting from 0)'
    )
    assert failed(checks) == ["encoding_matches_data_geometry"]
    assert checks["geometry_types_match_data_geometry"].message == (
        'all geometry types match declared "geometry_types" (249 checked)'
    )
    assert not any("Invalid Input Error" in c.message for c in checks.values())


def test_geoarrow_struct_declared_wkb_fails_the_encoding_check(tmp_path):
    """A coordinate struct is not WKB, whatever the ``geo`` block declares."""
    coords = pa.StructArray.from_arrays(
        [pa.array([0.0, 1.0, 2.0]), pa.array([0.0, 1.0, 2.0])], names=["x", "y"]
    )
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
    }
    table = pa.table({"geometry": coords}).replace_schema_metadata(
        {b"geo": json.dumps(geo).encode()}
    )
    path = tmp_path / "struct_as_wkb.parquet"
    pq.write_table(table, str(path))

    encoding = verdicts(str(path))["encoding_matches_data_geometry"]
    assert encoding.status == CheckStatus.FAILED
    assert encoding.message == (
        'column "geometry" is declared "WKB" but holds a GeoArrow coordinate struct '
        "(STRUCT(x DOUBLE, y DOUBLE)), not WKB bytes or a GEOMETRY type"
    )


def test_a_failed_reopen_does_not_hide_the_statement_error(tmp_path, monkeypatch):
    """The statement's own error is what the check reports, even if the reopen fails."""
    path = write_wkb_file(tmp_path / "f.parquet", [wkb_point(1, 2)], ["Point"])
    opens = []
    real_open = validate._open_validation_connection

    def open_once_then_fail(parquet_file):
        opens.append(parquet_file)
        if len(opens) > 1:
            raise RuntimeError("could not reopen")
        return _PoisonableConnection(real_open(parquet_file))

    monkeypatch.setattr(validate, "_open_validation_connection", open_once_then_fail)
    con = validate._ReopeningConnection(path)
    try:
        with pytest.raises(duckdb.InternalException, match="Buffer overflow"):
            con.execute("SELECT 1 /*poison*/")
    finally:
        con.close()
    assert len(opens) == 2
