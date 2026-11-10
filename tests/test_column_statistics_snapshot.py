"""``Table.stats()`` and ``gpio inspect stats`` share one stats function (#1087).

Both outputs are pinned on a file with mixed types, a secondary geometry
column and, optionally, a column whose stats query fails, so moving them onto
the shared core function changes no value, key or key order.
"""

from __future__ import annotations

import datetime
import json
import struct

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli
from geoparquet_io.core import inspect_utils


def _point(x: float, y: float) -> bytes:
    return struct.pack("<BIdd", 1, 1, x, y)


@pytest.fixture
def stats_file(tmp_path):
    table = pa.table(
        {
            "id": pa.array([1, 2, None], pa.int64()),
            "name": ["b", None, "a"],
            "flag": [True, False, None],
            "ts": pa.array(
                [
                    datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
                    None,
                    datetime.datetime(2024, 6, 30, 12, tzinfo=datetime.UTC),
                ],
                pa.timestamp("us", tz="UTC"),
            ),
            "geometry": pa.array([_point(0, 0), _point(1, 1), None], pa.binary()),
            "broken": pa.array([3, None, 4], pa.int32()),
            "footprint": pa.array([_point(5, 5), None, _point(6, 6)], pa.binary()),
        }
    )
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {"encoding": "WKB", "geometry_types": ["Point"]},
            "footprint": {"encoding": "WKB", "geometry_types": ["Point"]},
        },
    }
    table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    path = tmp_path / "stats.parquet"
    pq.write_table(table, path)
    return str(path)


@pytest.fixture(params=[False, True], ids=["all_succeed", "one_fails"])
def broken(request, monkeypatch):
    """When set, every stats query that touches column ``broken`` fails."""
    if request.param:
        real = inspect_utils.fetch_row

        def failing(con, query, params=None, **kwargs):
            if '"broken"' in query:
                raise duckdb.BinderException("simulated stats failure")
            return real(con, query, params, **kwargs)

        monkeypatch.setattr(inspect_utils, "fetch_row", failing)
    return request.param


UNKNOWN = {"nulls": None, "min": None, "max": None, "unique": None}
TS_MIN = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
TS_MAX = datetime.datetime(2024, 6, 30, 12, tzinfo=datetime.UTC)
P55 = "0101000000" + "0000000000001440" * 2
P66 = "0101000000" + "0000000000001840" * 2


def _broken_stats(broken: bool) -> dict:
    return UNKNOWN if broken else {"nulls": 1, "min": 3, "max": 4, "unique": 2}


def _plain(value):
    return value.hex() if isinstance(value, bytes) else value


def test_table_stats_are_pinned(stats_file, broken):
    stats = gpio.read(stats_file).stats()

    plain = [(col, {k: _plain(v) for k, v in s.items()}) for col, s in stats.items()]
    assert plain == [
        ("id", {"nulls": 1, "min": 1, "max": 2, "unique": 2}),
        ("name", {"nulls": 1, "min": "a", "max": "b", "unique": 2}),
        ("flag", {"nulls": 1, "min": False, "max": True, "unique": 2}),
        ("ts", {"nulls": 1, "min": TS_MIN, "max": TS_MAX, "unique": 2}),
        ("broken", _broken_stats(broken)),
        # A secondary geometry column is summarised as the bytes it holds.
        ("footprint", {"nulls": 1, "min": P55, "max": P66, "unique": 2}),
        # The primary geometry column comes last and counts nulls only.
        ("geometry", {**UNKNOWN, "nulls": 1}),
    ]


def test_cli_stats_are_pinned(stats_file, broken):
    result = CliRunner().invoke(cli, ["inspect", "stats", stats_file, "--json"])
    assert result.exit_code == 0, result.output

    stats = json.loads(result.output)["statistics"]
    # Aware datetimes compare equal across the session time zone.
    for key in ("min", "max"):
        stats["ts"][key] = datetime.datetime.fromisoformat(stats["ts"][key])
    assert list(stats.items()) == [
        ("id", {"nulls": 1, "min": 1, "max": 2, "unique": 2}),
        ("name", {"nulls": 1, "min": "a", "max": "b", "unique": 2}),
        ("flag", {"nulls": 1, "min": False, "max": True, "unique": 2}),
        ("ts", {"nulls": 1, "min": TS_MIN, "max": TS_MAX, "unique": 2}),
        # The CLI keeps file order: the primary geometry column stays in place.
        ("geometry", {**UNKNOWN, "nulls": 1}),
        ("broken", _broken_stats(broken)),
        ("footprint", {"nulls": 1, "min": P55, "max": P66, "unique": 2}),
    ]
