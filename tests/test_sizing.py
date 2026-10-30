"""``core/sizing`` is the one size parser and formatter (#1367 concept 9, approved change 5).

``KB``/``MB``/``GB``/``TB`` are powers of 1000 and ``KiB``/``MiB``/``GiB``/``TiB``
powers of 1024 everywhere -- the contract DuckDB's own ``memory_limit`` uses --
and a bare number means MB. Before this, ``--row-group-size-mb 1GB`` and
``--min-size 1GB`` meant 1,073,741,824 bytes while ``--write-memory 1GB`` meant
1,000,000,000, and a bare ``--write-memory 512`` was refused.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.decorators import parse_row_group_options
from geoparquet_io.cli.main import cli
from geoparquet_io.core import memory_limits
from geoparquet_io.core.duckdb_utils import fetch_one
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
        (1024, "1.00 KB"),
        (1024**2, "1.00 MB"),
        (1024**3, "1.00 GB"),
        (1024**4, "1.00 TB"),
    ],
)
def test_format_size_display_is_unchanged(n, expected):
    """Display formatting keeps its bytes (recorded for the lead; see PROGRESS)."""
    assert format_size(n) == expected


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


def test_row_group_size_mb_and_min_size_agree_on_1gb():
    """``--row-group-size-mb 1GB`` and ``--min-size 1GB`` are the same number of bytes."""
    row_group_mb = parse_row_group_options(None, "1GB")
    assert mb_to_bytes(row_group_mb) == parse_size_string("1GB") == 10**9


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


# --- guard: one parser, one formatter, one SET ----------------------------------

SIZING = PACKAGE / "core" / "sizing.py"
SET_OWNERS = {PACKAGE / "core" / "duckdb_utils.py", PACKAGE / "core" / "memory_limits.py"}


def _size_function_defs(tree: ast.AST) -> list[int]:
    return [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name.lstrip("_").startswith(("parse_size", "format_size", "format_memory_limit"))
    ]


def _set_memory_limit_literals(tree: ast.AST) -> list[int]:
    """String literals that issue ``SET memory_limit`` (docstrings that mention it do not)."""
    docstrings = {
        id(node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant)
    }
    return [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
        and "set memory_limit" in node.value.lower()
    ]


def _violations(root: Path = PACKAGE) -> list[str]:
    found = []
    for path in sorted(root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        rel = path.relative_to(root)
        if path.resolve() != SIZING.resolve():
            found += [f"{rel}:{n} size parser/formatter" for n in _size_function_defs(tree)]
        if path.resolve() not in {p.resolve() for p in SET_OWNERS}:
            found += [f"{rel}:{n} SET memory_limit" for n in _set_memory_limit_literals(tree)]
    return found


def test_one_parser_one_formatter_one_set():
    found = _violations()
    assert not found, (
        "size parsing/formatting belongs in core/sizing.py and SET memory_limit in "
        "duckdb_utils/memory_limits:\n  " + "\n  ".join(found)
    )


def test_guard_catches_both_patterns(tmp_path):
    (tmp_path / "rogue.py").write_text(
        '"""Docs may say SET memory_limit."""\n'
        "def _parse_size(t):\n    return 1\n\n"
        "def f(con, v):\n    con.execute(f\"SET memory_limit = '{v}'\")\n"
    )
    assert _violations(tmp_path) == [
        "rogue.py:2 size parser/formatter",
        "rogue.py:6 SET memory_limit",
    ]


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
