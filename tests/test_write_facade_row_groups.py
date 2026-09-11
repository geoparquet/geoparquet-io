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

import pyarrow as pa
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


# ---------------------------------------------------------------------------
# `--help` has to state the rule the facade actually applies
# ---------------------------------------------------------------------------
#
# The facade moved the effective default onto every write path, and the help
# text was left describing the rule it replaced: "if neither is given the
# writer's own default applies (122,880 rows for DuckDB-backed writes)". Both
# halves were false the moment this landed -- gpio snaps *down* to 49,152 and
# does not fall through to the writer -- while `docs/guide/convert.md`, edited
# in the same commit, correctly said 49,152. A tool contradicting its own new
# docs in one commit is #967's failure shape, and the repo had shipped it twice.
#
# `tests/data/cli_surface.json` structurally cannot catch this: it records
# Click's *declared* default, which is still `None` (a real Click default would
# collide with `--row-group-size-mb` and raise the mutually-exclusive error).
# Only the help string names the effective default, so only the help string can
# be checked.


def _leaf_commands(group, path=()):
    """Every leaf command in the CLI tree, as ``("convert geoparquet", cmd)``."""
    for name, command in sorted(getattr(group, "commands", {}).items()):
        if hasattr(command, "commands"):
            yield from _leaf_commands(command, (*path, name))
        else:
            yield " ".join((*path, name)), command


def _row_group_size_option(command):
    return next((p for p in command.params if "--row-group-size" in p.opts), None)


def _row_group_help_text(command_path: str) -> str:
    return _row_group_size_option(dict(_leaf_commands(cli))[command_path]).help or ""


#: Every command that offers ``--row-group-size``. Discovered rather than
#: listed, so a new write command is covered the day it is registered.
ROW_GROUP_COMMANDS = sorted(
    path for path, command in _leaf_commands(cli) if _row_group_size_option(command)
)


def test_the_help_sweep_is_not_vacuous():
    """A walk that found nothing would make every test below pass silently."""
    assert len(ROW_GROUP_COMMANDS) > 20, ROW_GROUP_COMMANDS
    # The four sort subcommands already named a default; the interesting half is
    # everything else, which did not.
    assert len([c for c in ROW_GROUP_COMMANDS if not c.startswith("sort ")]) > 15


@pytest.mark.parametrize("command", ROW_GROUP_COMMANDS)
def test_help_names_the_default_that_actually_lands(command):
    """One default, so one number in every ``--row-group-size`` help string."""
    help_text = _row_group_help_text(command)

    assert f"default: {DEFAULT_ROW_GROUP_ROWS:,}" in help_text, help_text


@pytest.mark.parametrize("command", ROW_GROUP_COMMANDS)
def test_help_does_not_advertise_the_rule_the_facade_deleted(command):
    """No help string may still hand the choice to the writer."""
    help_text = _row_group_help_text(command)

    assert "122,880" not in help_text, help_text
    assert "writer's own default" not in help_text, help_text


# ---------------------------------------------------------------------------
# The facade is shared, so its errors have to name the caller's own parameter
# ---------------------------------------------------------------------------


def test_the_api_rejection_names_the_python_argument_not_the_cli_flag():
    """``Table.write(row_group_rows=0)`` must not blame ``--row-group-size``.

    The facade is one function serving two front ends. A Python caller never
    typed a flag, and telling them "Invalid parameter '--row-group-size'" sends
    them looking for something that is not in their code.
    """
    from geoparquet_io.core.exceptions import InvalidParameterError

    with pytest.raises(InvalidParameterError) as raised:
        gpio.Table(pa.table({"id": [1]})).write("unused.parquet", row_group_rows=0)

    assert "row_group_rows" in str(raised.value)
    assert "--row-group-size" not in str(raised.value)


def test_the_cli_rejection_still_names_the_flag(many_rows, tmp_path):
    """The other half: the CLI error must keep naming what the user typed."""
    result = CliRunner().invoke(
        cli,
        ["sort", "hilbert", str(many_rows), str(tmp_path / "o.parquet"), "--row-group-size", "0"],
    )

    assert result.exit_code != 0
    assert "--row-group-size" in result.output


@pytest.mark.parametrize("command", ["convert geoparquet", "extract geoparquet", "add bbox"])
def test_the_rendered_help_block_carries_the_number(command):
    """Parsed from the option, but the user reads the rendered block."""
    result = CliRunner().invoke(cli, [*command.split(), "--help"])

    assert result.exit_code == 0, result.output
    assert f"{DEFAULT_ROW_GROUP_ROWS:,}" in " ".join(result.output.split()), result.output
