"""``core/sizing`` is the one size parser and formatter (#1367 concept 9, approved change 5).

``KB``/``MB``/``GB``/``TB`` are powers of 1000 and ``KiB``/``MiB``/``GiB``/``TiB``
powers of 1024 everywhere -- the contract DuckDB's own ``memory_limit`` uses --
and a bare number means MB. Before this, ``--row-group-size-mb 1GB`` and
``--min-size 1GB`` meant 1,073,741,824 bytes while ``--write-memory 1GB`` meant
1,000,000,000, and a bare ``--write-memory 512`` was refused.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.decorators import parse_row_group_options
from geoparquet_io.cli.main import cli
from geoparquet_io.core import memory_limits
from geoparquet_io.core.duckdb_utils import fetch_one
from geoparquet_io.core.parquet_writer import (
    align_to_writer_vector,
    resolve_row_group_rows_for_table,
)
from geoparquet_io.core.sizing import (
    bytes_to_mb,
    format_memory_limit,
    format_size,
    mb_to_bytes,
    normalize_size_string,
    parse_size_string,
)

REPO = Path(__file__).resolve().parents[1]
PACKAGE = REPO / "geoparquet_io"
DATA = REPO / "tests" / "data"

# --- the parse table ------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("100", 100 * 10**6),
        ("1", 10**6),
        ("1.5", 1_500_000),
        ("100B", 100),
        ("100 B", 100),
        ("0 bytes", 0),
        ("1KB", 1000),
        ("1K", 1000),
        ("1MB", 10**6),
        ("256MB", 256 * 10**6),
        ("1GB", 10**9),
        ("4.5GB", 4_500_000_000),
        ("1TB", 10**12),
        ("1KiB", 1024),
        ("1MiB", 1024**2),
        ("1GiB", 1024**3),
        ("1.5GiB", 3 * 1024**3 // 2),
        ("1TiB", 1024**4),
        ("16383.9 PiB", int(16383.9 * 1024**5)),
        ("1gb", 10**9),
        ("1gib", 1024**3),
        ("  2 GB  ", 2 * 10**9),
        ("2\tGB", 2 * 10**9),
    ],
)
def test_parse_table(text, expected):
    assert parse_size_string(text) == expected


@pytest.mark.parametrize("text", [None, "", "   "])
def test_empty_is_none(text):
    assert parse_size_string(text) is None


@pytest.mark.parametrize(
    "text", ["invalid", "GB", "1XB", "-1GB", "1GB; DROP TABLE x", "1e9", "1,000"]
)
def test_invalid_raises_with_the_offending_text(text):
    with pytest.raises(ValueError, match="Invalid size format") as excinfo:
        parse_size_string(text)
    assert repr(text) in str(excinfo.value)


def test_mb_conversions_are_decimal_and_inverse():
    assert bytes_to_mb(10**9) == 1000.0
    assert mb_to_bytes(1000) == 10**9
    assert mb_to_bytes(bytes_to_mb(123_456_789)) == 123_456_789


# --- formatters ------------------------------------------------------------------


@pytest.mark.parametrize(
    ("n", "expected"),
    [
        (512, "512.00 B"),
        (999, "999.00 B"),
        (1000, "1.00 KB"),
        (10**6, "1.00 MB"),
        (10**9, "1.00 GB"),
        (10**12, "1.00 TB"),
        (1024**3, "1.07 GB"),
    ],
)
def test_format_size_is_decimal(n, expected):
    """KB/MB/GB on display are the powers of 1000 the parser reads them as."""
    assert format_size(n) == expected


@pytest.mark.parametrize("n", [1000, 123_456, 10**9, 7 * 10**9 + 3, 2 * 10**12])
def test_format_size_reads_back_through_the_parser(n):
    """What the owner prints, the owner parses back to (within the two decimals shown)."""
    back = parse_size_string(format_size(n))
    assert abs(back - n) <= 0.005 * n, (n, format_size(n), back)


@pytest.mark.parametrize(
    "n",
    [128 * 1024**2, 700 * 1024**2, 1024**3, 5 * 1024**3, int(7.5 * 1024**3), 60 * 1024**3],
)
def test_format_memory_limit_round_trips(n):
    """The rendering means what DuckDB will read: within the one-decimal step."""
    rendered = format_memory_limit(n)
    back = parse_size_string(rendered)
    assert abs(back - n) <= 0.05 * 1024**3 + 1024**2, (n, rendered, back)


def test_format_memory_limit_never_mixes_units():
    """5 GiB used to render "5.0GB", which DuckDB reads as 5e9: 7% under."""
    assert format_memory_limit(5 * 1024**3) == "5.0GiB"
    assert format_memory_limit(10**6) == "128MiB"  # floor


# --- the shared numbers the CLI options produce -----------------------------------


class _OneBytePerRow:
    """A table-shaped stand-in whose rows weigh one byte each, so rows == target bytes."""

    num_rows = 10**13

    def get_total_buffer_size(self):
        return self.num_rows


def _writer_target_bytes(row_group_size_mb: float) -> set[int]:
    """The writer vector the facade turns ``row_group_size_mb`` into.

    The one MB -> rows step (``parquet_writer``) divides the target by the
    table's bytes per row and snaps to a whole writer vector; at one byte per
    row the row count it returns is the target in bytes, snapped. The second
    resolver ``write_strategies/row_group_sizing`` carried is gone (#1030).
    """
    table = _OneBytePerRow()
    return {resolve_row_group_rows_for_table(table, None, row_group_size_mb)}


@pytest.mark.parametrize(
    ("text", "target"),
    [("1MB", 10**6), ("1", 10**6), ("128MB", 128 * 10**6), ("1GB", 10**9), ("1GiB", 1024**3)],
)
def test_row_group_size_mb_reaches_the_writers_in_decimal_bytes(text, target):
    """``--row-group-size-mb 1MB`` is a 1,000,000-byte target, snapped to a writer vector."""
    assert _writer_target_bytes(parse_row_group_options(None, text)) == {
        align_to_writer_vector(target)
    }


def test_row_group_size_mb_and_min_size_agree_on_1gb():
    """``--row-group-size-mb 1GB`` targets the bytes ``--min-size 1GB`` selects on."""
    targets = _writer_target_bytes(parse_row_group_options(None, "1GB"))
    assert parse_size_string("1GB") == 10**9
    assert targets == {align_to_writer_vector(10**9)}


# --- --min-size reports the size it was given ----------------------------------


@pytest.fixture
def one_and_a_half_mb_dir(tmp_path):
    """A directory holding one 1,500,000-byte ``.parquet`` (1.43 MiB, 1.5 MB)."""
    directory = tmp_path / "d"
    directory.mkdir()
    (directory / "big.parquet").write_bytes(b"\0" * 1_500_000)
    return directory


def _partition_h3(directory, *args):
    return CliRunner().invoke(cli, ["partition", "h3", str(directory), "--resolution", "4", *args])


def test_min_size_preview_reports_decimal_mb(one_and_a_half_mb_dir):
    result = _partition_h3(one_and_a_half_mb_dir, "--min-size", "1MB", "--preview")
    assert result.exit_code == 0, result.output
    assert "big.parquet (1.5MB)" in result.output, result.output


def test_min_size_run_reports_decimal_mb(one_and_a_half_mb_dir):
    """The per-file line of a real run (the file is not parquet, so it then fails)."""
    result = _partition_h3(one_and_a_half_mb_dir, "--min-size", "1MB", "--in-place", "--force")
    assert "Processing: big.parquet (1.5MB)" in result.output, result.output


def test_min_size_no_match_reports_the_threshold_given(one_and_a_half_mb_dir):
    """``--min-size 100MB`` used to say "exceeding 95.4MB"."""
    result = _partition_h3(one_and_a_half_mb_dir, "--min-size", "100MB", "--in-place")
    assert result.exit_code == 0, result.output
    assert "No files found exceeding 100.0MB" in result.output, result.output


def _limits_seen_by_writes(monkeypatch) -> list[str]:
    """The memory_limit DuckDB reports while each scoped write runs."""
    seen: list[str] = []
    original = memory_limits.restore_duckdb_settings

    def recording(con, saved, verbose=False):
        seen.append(fetch_one(con.execute("SELECT current_setting('memory_limit')"))[0])
        return original(con, saved, verbose)

    monkeypatch.setattr(memory_limits, "restore_duckdb_settings", recording)
    return seen


@pytest.mark.parametrize("value", ["1GB", "1000", "1000MB", "1 gb"])
def test_write_memory_1gb_is_a_billion_bytes(monkeypatch, tmp_path, value):
    """``--write-memory`` 1GB (or a bare 1000, which is MB) reaches DuckDB as 1e9 bytes."""
    seen = _limits_seen_by_writes(monkeypatch)
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(
        cli,
        [
            "convert",
            "geoparquet",
            str(DATA / "places_test.parquet"),
            str(out),
            "--write-memory",
            value,
        ],
    )
    assert result.exit_code == 0, result.output
    # 1,000,000,000 bytes is what DuckDB displays as 953.6 MiB; 1 GiB is "1.0 GiB".
    assert seen and set(seen) == {"953.6 MiB"}


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("4.5 gb", "4.5GB"),
        ("1gib", "1GiB"),
        ("512", "512MB"),
        (" 2 TiB ", "2TiB"),
        ("100b", "100B"),
    ],
)
def test_normalize_size_string(text, expected):
    normalized = normalize_size_string(text)
    assert normalized == expected
    assert parse_size_string(normalized) == parse_size_string(text)


def test_requested_64_mb_row_group_is_graded_optimal():
    from geoparquet_io.core.check_parquet_structure import assess_row_group_size

    group_bytes = mb_to_bytes(64)
    assert assess_row_group_size(group_bytes, group_bytes * 2)[0] == "optimal"
