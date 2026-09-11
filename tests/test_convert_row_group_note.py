"""A DuckDB-backed write says what it will do with a sub-vector request (#986).

The write facade snaps any request above one 2,048-row vector and announces the
change (``resolve_row_group_rows``, #961/#990). It deliberately passes a request
of 2,048 rows *or fewer* through untouched, because ``pq.write_table`` honours
such a request exactly and snapping it would override the only writer that could
have obeyed.

That passthrough left a hole. DuckDB's ``COPY`` rounds a sub-vector request up
to 2,048 anyway, so ``gpio convert --row-group-size 249`` wrote 2,048-row groups
and said nothing: the user read 249 in the command and 2,048 in the footer.
``note_duckdb_copy_rounding`` closes it, at the funnels that know the write
reaches ``COPY``, with the same sentence the sort commands print.

The per-strategy divergence the tests below pin is measured, not assumed: on a
10,000-row input a request of 249 produces 2,048-row groups under ``duckdb-kv``
and 249-row groups under ``in-memory``, ``streaming`` and ``disk-rewrite``.
"""

from __future__ import annotations

import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli
from geoparquet_io.core.parquet_writer import (
    WRITER_VECTOR_ROWS,
    note_duckdb_copy_rounding,
    resolve_row_group_rows,
)
from tests.test_sort_row_group_default import _row_group_rows, _write_points

ROWS = 10_000

# The exact sentence, spelled out once so a change to the wording has to be
# deliberate and so the two producers of it cannot drift apart.
NOTE_249 = (
    "Writing 2,048 rows per row group rather than the requested 249: the Parquet "
    "writer emits row groups in whole 2,048-row vectors, so 249 is not a size a "
    "row group can have."
)


@pytest.fixture(scope="module")
def points_file(tmp_path_factory):
    return _write_points(tmp_path_factory.mktemp("convert_rg") / "points.parquet", n=ROWS)


@pytest.fixture
def said(monkeypatch):
    """Capture what the facade prints, without a CLI runner in the way."""
    from geoparquet_io.core import parquet_writer

    lines: list[str] = []
    monkeypatch.setattr(parquet_writer, "info", lines.append)
    return lines


@pytest.mark.parametrize("requested", [1, 249, 1_000, 2_047])
def test_a_sub_vector_request_is_named(said, requested):
    note_duckdb_copy_rounding(requested)
    assert said == [
        f"Writing 2,048 rows per row group rather than the requested {requested:,}: "
        "the Parquet writer emits row groups in whole 2,048-row vectors, so "
        f"{requested:,} is not a size a row group can have."
    ]


@pytest.mark.parametrize("requested", [None, WRITER_VECTOR_ROWS, 4_096, 49_152])
def test_a_whole_vector_or_no_request_says_nothing(said, requested):
    note_duckdb_copy_rounding(requested)
    assert said == []


@pytest.mark.parametrize("requested", [3_000, 9_000, 50_000, 100_000])
def test_resolve_speaks_once_and_the_funnel_stays_quiet(said, requested):
    """Above one vector the facade already snapped and announced; no second note."""
    resolved = resolve_row_group_rows(requested, None)
    assert len(said) == 1
    note_duckdb_copy_rounding(resolved)
    assert len(said) == 1, "the funnel repeated a note resolve had already printed"


def test_convert_names_the_size_duckdb_will_write(points_file, tmp_path):
    output = tmp_path / "out.parquet"
    result = CliRunner().invoke(
        cli,
        ["convert", "geoparquet", str(points_file), str(output), "--skip-hilbert"]
        + ["--row-group-size", "249"],
    )
    assert result.exit_code == 0, result.output
    assert NOTE_249 in result.output
    assert _row_group_rows(output)[0] == WRITER_VECTOR_ROWS


def test_convert_says_nothing_for_a_whole_vector(points_file, tmp_path):
    output = tmp_path / "out.parquet"
    result = CliRunner().invoke(
        cli,
        ["convert", "geoparquet", str(points_file), str(output), "--skip-hilbert"]
        + ["--row-group-size", "4096"],
    )
    assert result.exit_code == 0, result.output
    assert "rather than the requested" not in result.output
    assert _row_group_rows(output)[0] == 4_096


def test_convert_says_nothing_when_no_size_is_requested(points_file, tmp_path):
    output = tmp_path / "out.parquet"
    result = CliRunner().invoke(
        cli, ["convert", "geoparquet", str(points_file), str(output), "--skip-hilbert"]
    )
    assert result.exit_code == 0, result.output
    assert "rather than the requested" not in result.output


def test_the_python_api_says_it_too(points_file, tmp_path, said):
    """``Table.write`` defaults to duckdb-kv, so it rounds and must say so (#971)."""
    output = tmp_path / "api.parquet"
    gpio.read(str(points_file)).write(str(output), row_group_rows=249)
    assert said == [NOTE_249]
    assert _row_group_rows(output)[0] == WRITER_VECTOR_ROWS


@pytest.mark.parametrize("strategy", ["in-memory", "streaming", "disk-rewrite"])
def test_a_strategy_that_honours_the_request_is_not_told_otherwise(
    points_file, tmp_path, said, strategy
):
    """Only duckdb-kv rounds; claiming 2,048 for the others would be a lie."""
    output = tmp_path / f"{strategy}.parquet"
    gpio.read(str(points_file)).write(str(output), row_group_rows=249, write_strategy=strategy)
    assert said == []
    assert _row_group_rows(output)[0] == 249
