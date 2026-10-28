"""A 2.0 write names the primary the input declared, not DuckDB's first column.

DuckDB's ``GEOPARQUET_VERSION 'V2'`` writer generates its own ``geo`` block, and
that block's ``primary_column`` is the FIRST geometry column in the schema. When
a secondary geometry column comes before the primary (``centroid`` before
``geometry``), every 2.0 write that let DuckDB generate the block relabelled the
secondary as the primary -- ``convert`` from a 1.x input, and ``convert``,
``extract``, ``sort column`` and ``sort hilbert`` from a 2.0 input. Readers then
use the centroid as the dataset's geometry.

The funnel already knows the right answer (the ``geometry_column`` it was handed)
and already inspects the footer after every V2 COPY (the #589 repair), so the
same post-write step corrects ``primary_column`` with a footer patch.
"""

import json

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.derive_geo_from_file import derive_v2_geo_metadata
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path

_ROWS = 5

_SELECT = f"""
    SELECT i AS id,
      ST_Point(i + 0.5, i + 0.5) AS centroid,
      ST_MakeEnvelope(i, i, i + 1, i + 1) AS geometry
    FROM range({_ROWS}) t(i)
"""


def _geo_entries(path) -> list[dict]:
    """Every ``geo`` entry in the footer, repeats included."""
    con = get_duckdb_connection(load_spatial=False)
    try:
        rows = con.execute(
            f"SELECT key, value FROM parquet_kv_metadata({sql_path(str(path))})"
        ).fetchall()
    finally:
        con.close()
    return [json.loads(bytes(v)) for k, v in rows if bytes(k) == b"geo"]


def _geo(path) -> dict:
    entries = _geo_entries(path)
    assert len(entries) == 1, entries
    return entries[0]


def _declared_block(version: str, encoding: str) -> dict:
    return {
        "version": version,
        "primary_column": "geometry",
        "columns": {
            "geometry": {"encoding": encoding, "geometry_types": ["Polygon"]},
            "centroid": {"encoding": encoding, "geometry_types": ["Point"]},
        },
    }


def _copy(path, select: str, duckdb_version: str, geo: dict) -> None:
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(f"""
            COPY ({select}) TO {sql_path(str(path))} (
              FORMAT PARQUET, GEOPARQUET_VERSION '{duckdb_version}',
              KV_METADATA {{geo: '{json.dumps(geo)}'}}
            )
        """)
    finally:
        con.close()


@pytest.fixture
def v1_input(tmp_path):
    """1.1 WKB input, ``centroid`` before the declared primary ``geometry``."""
    path = tmp_path / "in11.parquet"
    select = (
        "SELECT id, ST_AsWKB(centroid) AS centroid, ST_AsWKB(geometry) AS geometry "
        f"FROM ({_SELECT})"
    )
    _copy(path, select, "NONE", _declared_block("1.1.0", "WKB"))
    assert _geo(path)["primary_column"] == "geometry"
    return path


@pytest.fixture
def v2_input(tmp_path):
    """Native 2.0 input, ``centroid`` before the declared primary ``geometry``."""
    path = tmp_path / "in20.parquet"
    _copy(path, _SELECT, "NONE", _declared_block("2.0.0", "WKB"))
    with pq.ParquetFile(str(path)) as pf:
        assert str(pf.schema.column(1).logical_type).startswith("Geometry")
    assert _geo(path)["primary_column"] == "geometry"
    return path


def _run(*args) -> None:
    result = CliRunner().invoke(cli, [str(a) for a in args])
    assert result.exit_code == 0, result.output


_COMMANDS = {
    "convert": lambda src, dst: ("convert", "geoparquet", src, dst),
    "extract": lambda src, dst: ("extract", "geoparquet", src, dst),
    "sort-column": lambda src, dst: ("sort", "column", src, dst, "id"),
    "sort-hilbert": lambda src, dst: ("sort", "hilbert", src, dst),
}


@pytest.mark.parametrize("command", sorted(_COMMANDS))
@pytest.mark.parametrize("source", ["v1_input", "v2_input"])
def test_a_2_0_write_keeps_the_declared_primary(command, source, request, tmp_path):
    src = request.getfixturevalue(source)
    out = tmp_path / "out.parquet"
    _run(*_COMMANDS[command](src, out), "--geoparquet-version", "2.0")

    geo = _geo(out)
    assert geo["primary_column"] == "geometry"
    # The rest of DuckDB's block survives the correction.
    assert set(geo["columns"]) == {"geometry", "centroid"}
    assert geo["columns"]["geometry"]["geometry_types"] == ["Polygon"]
    assert geo["columns"]["centroid"]["geometry_types"] == ["Point"]


def test_the_table_api_keeps_the_declared_primary(v2_input, tmp_path):
    import geoparquet_io as gpio

    out = tmp_path / "api.parquet"
    gpio.read(str(v2_input)).write(str(out), geoparquet_version="2.0")
    assert _geo(out)["primary_column"] == "geometry"


class TestDeriveV2GeoMetadata:
    """The repair's read half: what, if anything, to patch into the footer."""

    @staticmethod
    def _duckdb_block(tmp_path):
        """A file carrying DuckDB's own V2 block, which names ``centroid``."""
        path = tmp_path / "duckdb.parquet"
        con = get_duckdb_connection(load_spatial=True)
        try:
            con.execute(
                f"COPY ({_SELECT}) TO {sql_path(str(path))} "
                "(FORMAT PARQUET, GEOPARQUET_VERSION 'V2')"
            )
        finally:
            con.close()
        # The defect this file exists for: DuckDB picks the first column.
        assert _geo(path)["primary_column"] == "centroid"
        return path

    def test_a_block_naming_another_primary_is_corrected(self, tmp_path):
        path = self._duckdb_block(tmp_path)
        written = _geo(path)

        repaired = derive_v2_geo_metadata(str(path), primary_column="geometry")

        assert repaired == {**written, "primary_column": "geometry"}

    def test_a_block_naming_the_right_primary_needs_nothing(self, tmp_path):
        path = self._duckdb_block(tmp_path)
        assert derive_v2_geo_metadata(str(path), primary_column="centroid") is None

    @pytest.mark.parametrize("primary", [None, "id", "missing"])
    def test_a_primary_the_block_does_not_describe_is_not_forced_in(self, tmp_path, primary):
        path = self._duckdb_block(tmp_path)
        assert derive_v2_geo_metadata(str(path), primary_column=primary) is None


@pytest.mark.parametrize(
    "raw",
    [
        b"not json",
        b"\xff\xfe",
        b"[]",
        json.dumps({"primary_column": "centroid", "columns": ["geometry"]}).encode(),
    ],
    ids=["not-json", "not-utf8", "not-an-object", "columns-not-an-object"],
)
def test_an_unreadable_block_is_left_alone(raw):
    from geoparquet_io.core.derive_geo_from_file import _with_primary_column

    assert _with_primary_column(raw, "geometry") is None


@pytest.mark.parametrize("verbose", [False, True], ids=["quiet", "verbose"])
def test_the_correction_is_a_footer_patch(tmp_path, v2_input, verbose):
    """The relabel goes in by ``patch_footer_kv``: no page below the footer moves."""
    from unittest.mock import patch

    from geoparquet_io.core import write_funnels

    out = tmp_path / "out.parquet"
    with patch.object(
        write_funnels, "patch_footer_kv", wraps=write_funnels.patch_footer_kv
    ) as patched:
        _run(
            "extract",
            "geoparquet",
            v2_input,
            out,
            "--geoparquet-version",
            "2.0",
            *(["--verbose"] if verbose else []),
        )

    (call,) = patched.call_args_list
    assert json.loads(call.args[1]["geo"])["primary_column"] == "geometry"
    assert _geo(out)["primary_column"] == "geometry"
