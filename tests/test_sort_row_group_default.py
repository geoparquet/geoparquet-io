"""``gpio sort`` must default to the row-group size it advertises (#775).

Before this suite, a bare ``gpio sort hilbert`` passed ``ROW_GROUP_SIZE`` to
DuckDB as *nothing at all*, so DuckDB's own 122,880-row default applied while
``--help`` advertised 100,000 and the guide recommended 10,000-50,000 rows per
group for the spatial queries sorting exists to serve. Four numbers, none of
them agreeing.

The sort commands now resolve their own default -- ``DEFAULT_SORT_ROW_GROUP_ROWS``
-- and hand it down explicitly, so the advertised default *is* the effective
default on every write path.

"Effective" has a second half, added for #961. DuckDB's Parquet writer emits row
groups in whole vectors and rounds a ``ROW_GROUP_SIZE`` request *up* to a
multiple of 2,048, so a 50,000-row request used to land on 51,200-row groups --
outside the 10,000-50,000 band ``gpio check`` advises, which scored a freshly
sorted file ``[fail]``. gpio now snaps a row-group request to the nearest whole
vector before handing it to the writer, so the number it asks for is the number
that lands, and the assertions below are exact equalities rather than
round-ups.
"""

from __future__ import annotations

import io
import json
import logging
import math
import random
import re
import struct
import sys
from unittest import mock

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.check_parquet_structure import SPATIAL_ROW_COUNT_RANGE
from geoparquet_io.core.hilbert_order import hilbert_order
from geoparquet_io.core.parquet_writer import (
    DEFAULT_SORT_ROW_GROUP_ROWS,
    WRITER_VECTOR_ROWS,
    align_to_writer_vector,
    resolve_sort_row_group_rows,
)
from geoparquet_io.core.str_order import DEFAULT_STR_TILE_SIZE

# Enough rows that the old 122,880-row default and the new one are unambiguously
# different layouts (3 groups vs 6), while staying fast to build and sort.
ROW_COUNT = 250_000

# DuckDB's Parquet writer rounds a ROW_GROUP_SIZE up to a multiple of its
# vector-chunk size, so an exact row count is not what lands on disk unless the
# request is already a whole number of vectors.
WRITER_CHUNK_ROWS = 2048


def _round_up_to_chunk(rows: int) -> int:
    return math.ceil(rows / WRITER_CHUNK_ROWS) * WRITER_CHUNK_ROWS


def _pseudo_quadkey(value: int, digits: int = 13) -> str:
    """A quadkey-shaped string, so ``sort quadkey`` need not compute a real one.

    ``gpio sort quadkey`` auto-adds the column when it is missing, which costs
    far more than the write this suite is measuring. The column only has to
    exist and sort; its cells are never interpreted here.
    """
    out = []
    for _ in range(digits):
        out.append(str(value % 4))
        value //= 4
    return "".join(reversed(out))


def _write_points(path, n=ROW_COUNT, seed=775):
    """Write a small GeoParquet file of random WKB points as one row group."""
    rng = random.Random(seed)
    geometry = [
        struct.pack("<BIdd", 1, 1, rng.uniform(-180, 180), rng.uniform(-85, 85)) for _ in range(n)
    ]
    table = pa.table(
        {
            "id": pa.array(range(n), pa.int64()),
            "name": pa.array([f"f{i % 997}" for i in range(n)], pa.string()),
            "quadkey": pa.array([_pseudo_quadkey(i) for i in range(n)], pa.string()),
            "geometry": pa.array(geometry, pa.binary()),
        }
    )
    metadata = {
        b"geo": json.dumps(
            {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
            }
        ).encode("utf-8")
    }
    pq.write_table(table.replace_schema_metadata(metadata), str(path))
    return str(path)


def _row_group_rows(path) -> list[int]:
    parquet_file = pq.ParquetFile(str(path))
    return [parquet_file.metadata.row_group(i).num_rows for i in range(parquet_file.num_row_groups)]


@pytest.fixture(scope="module")
def points_file(tmp_path_factory):
    return _write_points(tmp_path_factory.mktemp("sort_rg") / "points.parquet")


# Every ``gpio sort`` subcommand, with the extra arguments it needs beyond
# input and output.
SORT_COMMANDS = {
    "hilbert": [],
    "str": [],
    "quadkey": [],
    "column": ["id"],
}


def _help_default_rows(command: str) -> int:
    """The row count ``--row-group-size --help`` advertises as its default."""
    result = CliRunner().invoke(cli, ["sort", command, "--help"])
    assert result.exit_code == 0, result.output
    # Collapse the wrapped help block so the option text is one line.
    flat = " ".join(result.output.split())
    match = re.search(r"--row-group-size INTEGER\s+(.*?)--row-group-size-mb", flat)
    assert match, f"could not locate --row-group-size help in:\n{result.output}"
    number = re.search(r"default: ([\d,]+)", match.group(1))
    assert number, f"--row-group-size help states no default: {match.group(1)!r}"
    return int(number.group(1).replace(",", ""))


@pytest.mark.parametrize("command", sorted(SORT_COMMANDS))
def test_help_advertises_the_sort_default(command):
    """Every sort subcommand's help names the default the sort commands use."""
    assert _help_default_rows(command) == DEFAULT_SORT_ROW_GROUP_ROWS


@pytest.mark.parametrize("command", sorted(SORT_COMMANDS))
def test_default_row_groups_match_the_advertised_default(command, points_file, tmp_path):
    """A bare ``gpio sort <cmd>`` writes groups of the size ``--help`` promises.

    This is the #775 regression: the advertised default was inert, so DuckDB's
    122,880-row default applied instead.
    """
    output = tmp_path / f"{command}.parquet"
    result = CliRunner().invoke(
        cli, ["sort", command, points_file, str(output), *SORT_COMMANDS[command]]
    )
    assert result.exit_code == 0, result.output

    advertised = _help_default_rows(command)
    ceiling = _round_up_to_chunk(advertised)
    groups = _row_group_rows(output)

    assert sum(groups) == ROW_COUNT
    # Every full group is the advertised size (rounded up to a writer chunk);
    # only the trailing remainder group may be smaller.
    assert max(groups) <= ceiling, f"{command}: groups {groups} exceed {ceiling}"
    assert max(groups) > advertised * 0.9, f"{command}: groups {groups} far below {advertised}"
    assert len(groups) == math.ceil(ROW_COUNT / ceiling), f"{command}: groups {groups}"


def test_default_is_inside_the_recommended_spatial_band(points_file, tmp_path):
    """The sort default must sit inside the 10,000-50,000 band gpio recommends.

    ``gpio check`` prints that band as advice; a default outside it means the
    tool contradicts itself the moment a user runs ``sort`` then ``check``.
    This is the #961 regression: the band has to hold for the rows that land on
    disk, not merely for the number gpio asked the writer for.
    """
    spatial_low, spatial_high = SPATIAL_ROW_COUNT_RANGE
    assert spatial_low <= DEFAULT_SORT_ROW_GROUP_ROWS <= spatial_high

    output = tmp_path / "band.parquet"
    result = CliRunner().invoke(cli, ["sort", "hilbert", points_file, str(output)])
    assert result.exit_code == 0, result.output
    assert spatial_low <= max(_row_group_rows(output)) <= spatial_high


def test_explicit_row_group_size_still_wins(points_file, tmp_path):
    """The new default must not shadow an explicit ``--row-group-size``."""
    output = tmp_path / "explicit.parquet"
    result = CliRunner().invoke(
        cli,
        ["sort", "hilbert", points_file, str(output), "--row-group-size", "20000"],
    )
    assert result.exit_code == 0, result.output
    assert max(_row_group_rows(output)) <= _round_up_to_chunk(20_000)


def test_row_group_size_mb_is_not_overridden_by_the_default(points_file, tmp_path):
    """``--row-group-size-mb`` must still size groups, not collide with the default.

    The default is resolved only when *neither* sizing option is given, so an
    MB target must not raise the mutually-exclusive usage error nor be silently
    replaced by a row count.
    """
    output = tmp_path / "mb.parquet"
    result = CliRunner().invoke(
        cli,
        ["sort", "hilbert", points_file, str(output), "--row-group-size-mb", "1MB"],
    )
    assert result.exit_code == 0, result.output
    # A 1MB target on this data is far smaller than 50,000 rows, so the MB path
    # is demonstrably the one that sized the groups.
    assert max(_row_group_rows(output)) < DEFAULT_SORT_ROW_GROUP_ROWS


def test_streaming_path_receives_the_resolved_default(points_file, tmp_path, monkeypatch):
    """The streaming branch gets the resolved default, not a bare ``None``.

    ``hilbert_order`` forks into a streaming path whenever input is stdin or
    output is stdout, and that path writes through a different code path than
    the file-based one. The default is resolved *before* the fork precisely so
    both sides get it; a resolver placed in the CLI, or after the branch, would
    leave ``gpio sort hilbert - out.parquet`` on DuckDB's 122,880-row default.
    """
    with pq.ParquetFile(points_file) as source:
        table = source.read()

    ipc_buffer = io.BytesIO()
    writer = ipc.RecordBatchStreamWriter(ipc_buffer, table.schema)
    writer.write_table(table)
    writer.close()
    ipc_buffer.seek(0)

    mock_stdin = mock.MagicMock()
    mock_stdin.isatty.return_value = False
    mock_stdin.buffer = ipc_buffer
    monkeypatch.setattr(sys, "stdin", mock_stdin)

    output = tmp_path / "streamed.parquet"
    hilbert_order("-", str(output))

    rows = _row_group_rows(output)
    assert sum(rows) == table.num_rows
    assert max(rows) == _round_up_to_chunk(DEFAULT_SORT_ROW_GROUP_ROWS)


def test_str_tile_size_tracks_the_sort_default():
    """``sort str``'s in-memory tile size is the sort default, not a stray constant.

    ``DEFAULT_STR_TILE_SIZE`` is the public default of ``ops.sort_str()`` and
    ``Table.sort_str()``, and it also picks the strip count the CLI builds. If
    it drifts from ``DEFAULT_SORT_ROW_GROUP_ROWS``, ``gpio sort str`` lays out
    different strips depending on whether ``--row-group-size-mb`` was passed,
    and the documented Python default stops matching the CLI's.
    """
    assert DEFAULT_STR_TILE_SIZE == DEFAULT_SORT_ROW_GROUP_ROWS


class TestWriterVectorAlignment:
    """A row-group request must survive the writer intact (#961).

    ``gpio sort`` asked for 50,000 rows per group -- the top of the
    10,000-50,000 band ``gpio check`` advises -- and DuckDB wrote 51,200,
    because it rounds a ``ROW_GROUP_SIZE`` request *up* to a whole 2,048-row
    vector. ``gpio check optimization`` then scored the file gpio had just
    written ``[fail]`` on its row-group factor and told the user to
    re-partition it. gpio now snaps the request to the nearest whole vector
    itself, so what it asks for is what lands.
    """

    def test_the_writer_rounds_a_request_up_to_a_whole_vector(self, tmp_path):
        """The premise of #961, measured rather than assumed.

        The rounding is a *ceiling*, not a nearest: 3,000 rows becomes 4,096,
        not the nearer 2,048. That direction is what pushes a 50,000-row
        request out of the top of the band, and it is why gpio's own alignment
        has to happen before the request reaches the writer.
        """
        import duckdb

        connection = duckdb.connect()
        try:
            connection.execute("CREATE TABLE t AS SELECT i AS id FROM range(60000) tbl(i)")
            measured = {}
            for request in (3_000, 50_000, DEFAULT_SORT_ROW_GROUP_ROWS):
                out = tmp_path / f"vector_{request}.parquet"
                connection.execute(f"COPY t TO '{out}' (FORMAT PARQUET, ROW_GROUP_SIZE {request})")
                measured[request] = max(_row_group_rows(out))
        finally:
            connection.close()

        assert measured[3_000] == 4_096, measured
        assert measured[50_000] == 51_200, measured
        # The default is a whole number of vectors, so the writer leaves it be.
        assert measured[DEFAULT_SORT_ROW_GROUP_ROWS] == DEFAULT_SORT_ROW_GROUP_ROWS, measured

    def test_the_default_is_a_whole_number_of_writer_vectors(self):
        """The default is the largest whole vector at or below the band's top."""
        spatial_high = SPATIAL_ROW_COUNT_RANGE[1]
        assert DEFAULT_SORT_ROW_GROUP_ROWS % WRITER_VECTOR_ROWS == 0
        assert DEFAULT_SORT_ROW_GROUP_ROWS <= spatial_high
        assert DEFAULT_SORT_ROW_GROUP_ROWS + WRITER_VECTOR_ROWS > spatial_high

    @pytest.mark.parametrize(
        ("requested", "expected"),
        [
            (1, 2_048),  # below one vector: the writer's own minimum
            (2_048, 2_048),
            (3_000, 2_048),  # nearest, where the writer would round up to 4,096
            (10_000, 10_240),  # the band floor rounds up, so it stays in band
            (20_000, 20_480),
            (49_152, 49_152),
            (50_000, 49_152),  # the band top rounds down, so it stays in band
            (51_200, 51_200),
            (100_000, 100_352),
        ],
    )
    def test_alignment_snaps_to_the_nearest_vector(self, requested, expected):
        assert align_to_writer_vector(requested) == expected

    def test_every_request_inside_the_spatial_band_stays_inside_it(self):
        """Nearest, not floor -- and this is the test that pins the difference.

        Rounding every request *down* would fix the top of the band and break
        the bottom: a user who types the band's own floor, ``--row-group-size
        10000``, would get 8,192-row groups and the same ``[fail]`` one end
        further along. Snapping to the nearest vector is monotonic and maps
        both endpoints inside the band, so every value between them lands
        inside it too.
        """
        spatial_low, spatial_high = SPATIAL_ROW_COUNT_RANGE
        assert align_to_writer_vector(spatial_low) >= spatial_low
        assert align_to_writer_vector(spatial_high) <= spatial_high
        for requested in range(spatial_low, spatial_high + 1, 137):
            aligned = align_to_writer_vector(requested)
            assert spatial_low <= aligned <= spatial_high, requested

    def test_an_adjusted_explicit_request_is_announced(self, caplog):
        """Changing what the user typed must not be silent."""
        with caplog.at_level(logging.INFO, logger="geoparquet_io"):
            resolved = resolve_sort_row_group_rows(50_000, None)

        assert resolved == 49_152
        assert "50,000" in caplog.text
        assert "49,152" in caplog.text

    def test_an_already_aligned_request_is_left_alone_and_silent(self, caplog):
        """No note when there is nothing to report."""
        with caplog.at_level(logging.INFO, logger="geoparquet_io"):
            resolved = resolve_sort_row_group_rows(20_480, None)

        assert resolved == 20_480
        assert caplog.text == ""

    def test_the_default_needs_no_adjustment_note(self, caplog):
        with caplog.at_level(logging.INFO, logger="geoparquet_io"):
            resolved = resolve_sort_row_group_rows(None, None)

        assert resolved == DEFAULT_SORT_ROW_GROUP_ROWS
        assert caplog.text == ""

    def test_sorted_output_lands_on_exactly_the_advertised_default(self, points_file, tmp_path):
        """No rounding left for the writer to do, so equality is exact."""
        output = tmp_path / "exact.parquet"
        result = CliRunner().invoke(cli, ["sort", "hilbert", points_file, str(output)])
        assert result.exit_code == 0, result.output
        assert max(_row_group_rows(output)) == DEFAULT_SORT_ROW_GROUP_ROWS

    def test_sorted_output_passes_the_optimization_row_group_factor(self, points_file, tmp_path):
        """The #961 repro: ``gpio check optimization`` must not fail gpio's own output."""
        from geoparquet_io.core.check_optimization import _check_row_group_size

        output = tmp_path / "scored.parquet"
        result = CliRunner().invoke(cli, ["sort", "hilbert", points_file, str(output)])
        assert result.exit_code == 0, result.output

        factor = _check_row_group_size(str(output))
        assert factor["passed"] is True, factor["detail"]

    @pytest.mark.parametrize("requested", [10_000, 25_000, 50_000])
    def test_an_explicit_in_band_request_is_written_in_band(self, requested, points_file, tmp_path):
        """The band has to hold for a value the user typed, not just for the default."""
        spatial_low, spatial_high = SPATIAL_ROW_COUNT_RANGE
        output = tmp_path / f"explicit_{requested}.parquet"
        result = CliRunner().invoke(
            cli,
            ["sort", "hilbert", points_file, str(output), "--row-group-size", str(requested)],
        )
        assert result.exit_code == 0, result.output
        assert spatial_low <= max(_row_group_rows(output)) <= spatial_high
