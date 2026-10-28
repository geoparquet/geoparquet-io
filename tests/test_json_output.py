"""Every ``--json`` report gpio prints must be valid JSON for any column type (#1226).

``gpio inspect stats --json`` died with ``Object of type date is not JSON
serializable`` on a DATE, TIMESTAMP or TIME column, because the min/max a
DuckDB query hands back are Python ``date``/``datetime``/``time`` objects and
the report was serialized with a bare ``json.dumps``. A DECIMAL or BLOB column
failed the same way, and a NaN or infinite DOUBLE was printed as the bare
``NaN``/``Infinity`` tokens, which no strict JSON parser accepts. The reports
now share one encoder, ``core.json_output``.
"""

from __future__ import annotations

import datetime as dt
import json
import math
from decimal import Decimal

import numpy as np
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
from geoparquet_io.core.json_output import dumps_json, to_jsonable


def _strict_loads(text: str):
    """Parse JSON the way a strict consumer (jq, JavaScript) would: no NaN/Infinity."""

    def _reject(token):
        raise ValueError(f"non-standard JSON token {token}")

    return json.loads(text, parse_constant=_reject)


@pytest.fixture(scope="module")
def typed_parquet(tmp_path_factory):
    """A GeoParquet 2.0 file with one column of every awkward type."""
    path = tmp_path_factory.mktemp("json_output") / "typed.parquet"
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        con.execute(
            f"""
            COPY (
                SELECT
                    ST_Point(i, i) AS geom,
                    DATE '2024-01-02' + i::INTEGER AS d,
                    TIMESTAMP '2024-01-02 03:04:05' + to_seconds(i) AS ts,
                    TIME '01:02:03' AS t,
                    TIMESTAMPTZ '2024-01-02 03:04:05+00' AS tstz,
                    (i + 0.25)::DECIMAL(10, 2) AS dec,
                    CASE WHEN i = 1 THEN 'NaN'::DOUBLE ELSE i::DOUBLE END AS f_nan,
                    CASE WHEN i = 1 THEN 'inf'::DOUBLE ELSE i::DOUBLE END AS f_inf,
                    '\\xAA\\xBB'::BLOB AS blob_col,
                    INTERVAL 3 DAY AS iv
                FROM range(3) t(i)
            ) TO {sql_path(str(path))} (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')
            """
        )
    finally:
        con.close()
    return str(path)


class TestToJsonable:
    """The shared conversion, one type at a time."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (dt.date(2024, 1, 2), "2024-01-02"),
            (dt.datetime(2024, 1, 2, 3, 4, 5), "2024-01-02T03:04:05"),
            (
                dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc),
                "2024-01-02T03:04:05+00:00",
            ),
            (dt.time(1, 2, 3), "01:02:03"),
            (Decimal("12.50"), "12.50"),
            (b"\xaa\xbb", "aabb"),
            (bytearray(b"\x01"), "01"),
            (memoryview(b"\x02"), "02"),
            (float("nan"), None),
            (float("inf"), None),
            (float("-inf"), None),
            (Decimal("NaN"), None),
            (np.int64(7), 7),
            (np.float32(1.5), 1.5),
            (np.float64("nan"), None),
            (np.bool_(True), True),
            (1.5, 1.5),
            ("x", "x"),
            (None, None),
        ],
    )
    def test_scalar(self, value, expected):
        assert to_jsonable(value) == expected

    def test_timedelta_is_iso_8601_duration(self):
        assert to_jsonable(dt.timedelta(days=3, seconds=4)) == "P3DT4S"
        assert to_jsonable(dt.timedelta(0)) == "P0D"

    def test_nested_containers_are_walked(self):
        value = {"a": [dt.date(2024, 1, 2), (float("nan"), {"b": Decimal("1")})], 3: {1, 2}}
        assert to_jsonable(value) == {"a": ["2024-01-02", [None, {"b": "1"}]], "3": [1, 2]}

    def test_numpy_array_becomes_a_list(self):
        assert to_jsonable(np.array([1.0, np.nan])) == [1.0, None]

    def test_dumps_is_strict_and_passes_options_through(self):
        text = dumps_json({"x": float("nan"), "d": dt.date(2024, 1, 2)}, indent=2)
        assert _strict_loads(text) == {"x": None, "d": "2024-01-02"}
        assert "\n  " in text


class TestInspectStatsJson:
    """`gpio inspect stats --json` on every awkward column type (#1226)."""

    def test_cli_emits_strict_json(self, typed_parquet):
        result = CliRunner().invoke(cli, ["inspect", "stats", typed_parquet, "--json"])

        assert result.exit_code == 0, result.output
        stats = _strict_loads(result.output)["statistics"]
        assert stats["d"]["min"] == "2024-01-02"
        assert stats["d"]["max"] == "2024-01-04"
        assert stats["ts"]["min"] == "2024-01-02T03:04:05"
        assert stats["t"]["min"] == "01:02:03"
        assert stats["dec"]["min"] == "0.25"
        assert stats["dec"]["max"] == "2.25"
        # DuckDB orders NaN above every number, so it is the max; JSON has no NaN.
        assert stats["f_nan"]["max"] is None
        assert stats["f_inf"]["max"] is None
        assert stats["blob_col"]["min"] == "aabb"
        assert stats["iv"]["min"] == "P3D"

    def test_head_json_preview_is_strict(self, typed_parquet):
        result = CliRunner().invoke(cli, ["inspect", "head", typed_parquet, "3", "--json"])

        assert result.exit_code == 0, result.output
        preview = _strict_loads(result.output)["preview"]
        assert preview[1]["f_nan"] is None
        assert preview[1]["f_inf"] is None
        assert preview[0]["d"] == "2024-01-02"

    def test_python_api_stats_serialize_through_the_shared_encoder(self, typed_parquet):
        """`Table.stats()` keeps native Python values; the shared encoder makes them JSON."""
        stats = gpio.read(typed_parquet).stats()

        assert stats["d"]["min"] == dt.date(2024, 1, 2)
        assert isinstance(stats["ts"]["min"], dt.datetime)
        assert isinstance(stats["t"]["min"], dt.time)
        assert isinstance(stats["dec"]["min"], Decimal)
        assert math.isnan(stats["f_nan"]["max"])

        decoded = _strict_loads(dumps_json(stats))
        assert decoded["d"]["min"] == "2024-01-02"
        assert decoded["ts"]["min"] == "2024-01-02T03:04:05"
        assert decoded["t"]["min"] == "01:02:03"
        assert decoded["dec"]["min"] == "0.25"
        assert decoded["f_nan"]["max"] is None

    def test_core_formatter_matches_cli(self, typed_parquet):
        from geoparquet_io.core.inspect import format_stats_output, inspect_stats

        text = format_stats_output(inspect_stats(typed_parquet), json_output=True)
        assert _strict_loads(text)["statistics"]["d"]["min"] == "2024-01-02"


def test_check_spec_json_is_strict(typed_parquet):
    """`gpio check spec --json` still parses strictly after moving to the shared encoder.

    A regression guard for the switch, not a #1226 reproduction. The check
    report carries no raw column values, so it parsed on main too.
    """
    result = CliRunner().invoke(cli, ["check", "spec", typed_parquet, "--json"])

    assert result.exit_code in (0, 1), result.output
    assert _strict_loads(result.output)["file_path"] == typed_parquet


def test_meta_json_outputs_are_strict(typed_parquet):
    """Every `gpio inspect meta --json` mode still parses strictly after the switch.

    A regression guard, not a #1226 reproduction. The footer statistics these
    modes print are already strings (an infinite max reads ``"None"``), so no
    NaN or date reaches the encoder and every mode parsed on main too.
    """
    for flags in ([], ["--parquet"], ["--parquet-geo"], ["--geo"], ["--geo-stats"]):
        result = CliRunner().invoke(cli, ["inspect", "meta", typed_parquet, "--json", *flags])
        assert result.exit_code == 0, (flags, result.output)
        _strict_loads(result.output)


class TestScalarFallbacks:
    def test_numpy_scalar_and_array_become_python(self):
        assert to_jsonable(np.int64(7)) == 7
        assert to_jsonable(np.float64("nan")) is None
        assert to_jsonable(np.array([1.5, np.inf])) == [1.5, None]

    def test_numpy_object_without_tolist_uses_item(self):
        from geoparquet_io.core.json_output import _numpy_to_python

        class _Scalar:
            __module__ = "numpy.fake"

            def item(self):
                return 3

        assert _numpy_to_python(_Scalar()) == (True, 3)

    def test_unknown_object_falls_back_to_str(self):
        class _Thing:
            def __str__(self):
                return "thing"

        assert to_jsonable(_Thing()) == "thing"


def test_row_group_geo_stats_json_without_stats_is_valid(tmp_path, capsys):
    """A file with no native geo stats and no bbox column still prints JSON."""
    from geoparquet_io.core.metadata_utils import format_row_group_geo_stats

    path = tmp_path / "plain.parquet"
    con = get_duckdb_connection(load_spatial=False)
    try:
        con.execute(f"COPY (SELECT 1 AS id) TO {sql_path(str(path))} (FORMAT PARQUET)")
    finally:
        con.close()
    format_row_group_geo_stats(str(path), json_output=True)
    assert _strict_loads(capsys.readouterr().out) == {
        "row_group_geo_stats": [],
        "message": "No geo stats found",
    }


class TestNumpyAndPandasEdgeCases:
    """Scalars whose ``tolist()`` does not land on a plain Python type."""

    def test_longdouble_does_not_recurse(self):
        assert to_jsonable(np.longdouble(1.5)) == 1.5
        assert to_jsonable(np.longdouble("inf")) is None

    def test_clongdouble_does_not_recurse(self):
        assert to_jsonable(np.clongdouble(1 + 2j)) == str(np.clongdouble(1 + 2j))

    def test_longdouble_array_does_not_recurse(self):
        assert to_jsonable(np.array([1.5, np.nan], dtype=np.longdouble)) == [1.5, None]

    def test_pandas_nat_and_na_are_null(self):
        pd = pytest.importorskip("pandas")

        assert to_jsonable(pd.NaT) is None
        assert to_jsonable(pd.NA) is None
        assert to_jsonable({"a": [pd.NaT, pd.NA]}) == {"a": [None, None]}

    def test_pandas_timestamp_stays_iso(self):
        pd = pytest.importorskip("pandas")

        assert to_jsonable(pd.Timestamp("2024-01-02 03:04:05")) == "2024-01-02T03:04:05"

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (np.datetime64("2024-01-02T03:04:05.123456789", "ns"), "2024-01-02T03:04:05.123456789"),
            (np.datetime64("2024-01-02T03:04:05", "s"), "2024-01-02T03:04:05"),
            (np.datetime64("2024-01-02", "D"), "2024-01-02"),
            (np.datetime64("NaT", "ns"), None),
        ],
    )
    def test_datetime64_is_iso_8601(self, value, expected):
        assert to_jsonable(value) == expected

    def test_zero_dimensional_array_becomes_its_scalar(self):
        assert to_jsonable(np.array(1.5)) == 1.5
        assert to_jsonable(np.array(np.datetime64("2024-01-02", "D"))) == "2024-01-02"

    def test_datetime64_ns_array_is_iso_8601(self):
        values = np.array(["2024-01-02T03:04:05.000000001"], dtype="datetime64[ns]")
        assert to_jsonable(values) == ["2024-01-02T03:04:05.000000001"]


@pytest.fixture(scope="module")
def nested_parquet(tmp_path_factory):
    """A GeoParquet 2.0 file with struct, list, timestamp, tstz and BLOB columns."""
    path = tmp_path_factory.mktemp("json_preview") / "nested.parquet"
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        con.execute(
            f"""
            COPY (
                SELECT
                    ST_Point(i, i) AS geom,
                    {{'a': i, 'b': 'x'}} AS s,
                    [i, i + 1] AS l,
                    TIMESTAMP '2024-01-02 03:04:05' AS ts,
                    TIMESTAMPTZ '2024-01-02 03:04:05+00' AS tstz,
                    '\\xAA\\xBB'::BLOB AS blob_col
                FROM range(3) t(i)
            ) TO {sql_path(str(path))} (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')
            """
        )
    finally:
        con.close()
    return str(path)


class TestPreviewJsonFormat:
    """`inspect head/tail --json` cells are real JSON values, not Python reprs."""

    @pytest.mark.parametrize(("command", "row", "index"), [("head", 0, 0), ("tail", 0, 2)])
    def test_nested_and_typed_cells(self, nested_parquet, command, row, index):
        result = CliRunner().invoke(cli, ["inspect", command, nested_parquet, "1", "--json"])

        assert result.exit_code == 0, result.output
        cell = _strict_loads(result.output)["preview"][row]
        assert cell["s"] == {"a": index, "b": "x"}
        assert cell["l"] == [index, index + 1]
        assert cell["ts"] == "2024-01-02T03:04:05"
        assert cell["blob_col"] == "aabb"

    def test_tstz_preview_is_the_same_instant(self, nested_parquet):
        result = CliRunner().invoke(cli, ["inspect", "head", nested_parquet, "1", "--json"])

        assert result.exit_code == 0, result.output
        text = _strict_loads(result.output)["preview"][0]["tstz"]
        assert "T" in text
        parsed = dt.datetime.fromisoformat(text)
        assert parsed.tzinfo is not None
        assert parsed == dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc)
