"""One row-group answer, for every write path and both front ends.

The facade (``core/parquet_writer.py``) owns how many rows a row group gets.
Before it, each write path inherited whatever its writer happened to default to
and gpio shipped three different answers for the same request:

* ``gpio sort --row-group-size 50000`` wrote 49,152 (#967) while
  ``Table.write(row_group_rows=50000)`` wrote 51,200 (#971);
* ``gpio convert geoparquet`` wrote DuckDB's 122,880 while its docstring claimed
  100,000 and nothing set either (#981);
* the Arrow-side writers fell through to ``ParquetWriteSettings``' own 100,000.

Two of those three are outside the 10,000-50,000 band ``gpio check
optimization`` scores, so gpio failed files gpio had just written (#972).

The assertions here are about the *file*, read back from its footer, not about
what a function was handed -- which is the half
``tests/test_cli_api_call_parity_scaffold.py`` cannot see, because its two
patch points sit on opposite sides of the facade call.
"""

from __future__ import annotations

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
from geoparquet_io.core.parquet_writer import (
    DEFAULT_ROW_GROUP_ROWS,
    SPATIAL_BAND_TOP_ROWS,
    WRITER_VECTOR_ROWS,
)

#: Enough rows that the default splits the output into several groups, so a
#: wrong default shows up as a wrong *size* rather than as one short group.
_ROWS = DEFAULT_ROW_GROUP_ROWS * 2 + 1_000


@pytest.fixture(scope="module")
def many_rows(tmp_path_factory):
    """A geometry file large enough to need more than two row groups."""
    path = tmp_path_factory.mktemp("facade_rg") / "in.parquet"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            f"""
            COPY (
                SELECT i AS id, ST_Point(i % 360 - 180, i % 170 - 85) AS geometry
                FROM range({_ROWS}) t(i)
            ) TO {sql_path(str(path))} (FORMAT PARQUET)
            """
        )
    finally:
        con.close()
    return path


def _row_group_sizes(path) -> list[int]:
    metadata = pq.ParquetFile(str(path)).metadata
    return [metadata.row_group(i).num_rows for i in range(metadata.num_row_groups)]


def _full_groups(path) -> list[int]:
    """Every row group but the last, which is only as big as the leftovers."""
    return _row_group_sizes(path)[:-1]


def _run_cli(*args) -> None:
    result = CliRunner().invoke(cli, [str(a) for a in args])
    assert result.exit_code == 0, result.output


# ---------------------------------------------------------------------------
# The default: one number, not three
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "command",
    [
        pytest.param(["convert", "geoparquet"], id="convert-geoparquet"),
        pytest.param(["sort", "hilbert"], id="sort-hilbert"),
        pytest.param(["extract", "geoparquet"], id="extract-geoparquet"),
    ],
)
def test_cli_writes_the_default_row_group_size(command, many_rows, tmp_path):
    """No --row-group-size means DEFAULT_ROW_GROUP_ROWS, whatever the command.

    ``sort hilbert`` already did (#967). ``convert geoparquet`` wrote DuckDB's
    122,880 (#981) and ``extract geoparquet`` the same, because neither passed a
    row count and the writer picked.
    """
    out = tmp_path / "out.parquet"

    _run_cli(*command, many_rows, out)

    assert _full_groups(out) == [DEFAULT_ROW_GROUP_ROWS] * 2


def test_api_write_matches_the_cli_default(many_rows, tmp_path):
    """``Table.write()`` lands on the same default as the CLI (#971)."""
    out = tmp_path / "api.parquet"

    gpio.read(str(many_rows)).write(out)

    assert _full_groups(out) == [DEFAULT_ROW_GROUP_ROWS] * 2


def test_cli_and_api_write_the_same_layout_for_the_same_request(many_rows, tmp_path):
    """The parity the call-site harness cannot measure: the files agree.

    Its two patch points sit on opposite sides of the facade call, so it sees
    ``None`` on one side and the resolved number on the other. What a user gets
    is the file, and the files are identical in layout.
    """
    from_cli = tmp_path / "cli.parquet"
    from_api = tmp_path / "api.parquet"

    _run_cli("convert", "geoparquet", many_rows, from_cli)
    gpio.read(str(many_rows)).write(from_api)

    assert _row_group_sizes(from_cli) == _row_group_sizes(from_api)


# ---------------------------------------------------------------------------
# An explicit request: the number asked for is the number that lands
# ---------------------------------------------------------------------------


def test_api_aligns_an_explicit_request_like_the_cli(many_rows, tmp_path):
    """#971 head-on: ``row_group_rows=50000`` wrote 51,200 through the API.

    The band's literal top is not a size a row group can have -- the writer
    rounds a request up to a whole 2,048-row vector -- so 50,000 landed at
    51,200, outside the band ``gpio check optimization`` scores, while
    ``gpio sort --row-group-size 50000`` wrote 49,152 for the same number.
    """
    from_cli = tmp_path / "cli.parquet"
    from_api = tmp_path / "api.parquet"

    _run_cli("sort", "hilbert", many_rows, from_cli, "--row-group-size", str(SPATIAL_BAND_TOP_ROWS))
    gpio.read(str(many_rows)).write(from_api, row_group_rows=SPATIAL_BAND_TOP_ROWS)

    assert _full_groups(from_api) == [DEFAULT_ROW_GROUP_ROWS] * 2
    assert _row_group_sizes(from_api) == _row_group_sizes(from_cli)


def test_an_explicit_whole_vector_request_is_honoured_exactly(many_rows, tmp_path):
    """Alignment is not a clamp: a request the writer can express is written."""
    requested = WRITER_VECTOR_ROWS * 5  # 10,240 -- inside the band, whole vectors
    out = tmp_path / "out.parquet"

    gpio.read(str(many_rows)).write(out, row_group_rows=requested)

    assert set(_full_groups(out)) == {requested}


def test_a_megabyte_target_still_sizes_by_bytes(many_rows, tmp_path):
    """``row_group_size_mb`` is not overridden by the row-count default.

    The facade returns ``None`` for a byte target so the byte-based estimator
    stays in charge; forcing the default here would silently ignore the option
    the caller actually passed.
    """
    out = tmp_path / "out.parquet"

    gpio.read(str(many_rows)).write(out, row_group_size_mb=1)

    assert set(_full_groups(out)) != {DEFAULT_ROW_GROUP_ROWS}


# ---------------------------------------------------------------------------
# The reason the number is what it is
# ---------------------------------------------------------------------------


def test_the_default_is_inside_the_band_check_scores(many_rows, tmp_path):
    """gpio must not write a file its own optimization check then fails (#972).

    This is the whole point of the constant: ``check optimization`` scores row
    groups against the 10,000-50,000 band, and before the facade the default
    write landed at 122,880 and was told to re-partition.
    """
    from geoparquet_io.core.check_parquet_structure import SPATIAL_ROW_COUNT_RANGE

    out = tmp_path / "out.parquet"
    _run_cli("convert", "geoparquet", many_rows, out)

    low, high = SPATIAL_ROW_COUNT_RANGE
    assert all(low <= size <= high for size in _full_groups(out))
