"""One codec list and one level policy: ``compression.resolve_codec`` (#1367 T2a).

Every write path used to carry its own codec map and its own idea of what an
unset level means: 15 in the Arrow funnel, 9 in ``gpio process``, DuckDB's own
3 on every plain ``COPY`` (#1335), and pyarrow's default on the unpatched
rewrites. ``none`` became ``UNCOMPRESSED`` on one path and ``NONE`` on another;
``LZ4_RAW`` was accepted by one rewriter and refused by every validator.

These tests pin the owner's table, run ``none``/``uncompressed``/``LZ4_RAW``
through every CLI entry that takes ``--compression``, and walk the package for
a codec map or list outside the owner.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.compression import (
    ALLOWED_CODECS,
    PROCESS_ZSTD_LEVEL,
    ResolvedCodec,
    codec_from_footer,
    resolve_codec,
)
from geoparquet_io.core.exceptions import InvalidParameterError

REPO = Path(__file__).resolve().parents[1]
PACKAGE = REPO / "geoparquet_io"
DATA = Path(__file__).parent / "data"
PLACES = DATA / "places_test.parquet"
BUILDINGS = DATA / "buildings_test.parquet"


# ---------------------------------------------------------------------------
# The resolve table
# ---------------------------------------------------------------------------

#: (name, level, context) -> (canonical, level, COPY clauses, pyarrow kwargs)
RESOLVE_TABLE = [
    (
        "ZSTD",
        None,
        "write",
        "ZSTD",
        15,
        ["COMPRESSION ZSTD", "COMPRESSION_LEVEL 15"],
        {"compression": "zstd", "compression_level": 15},
    ),
    (
        "zstd",
        None,
        "process",
        "ZSTD",
        PROCESS_ZSTD_LEVEL,
        ["COMPRESSION ZSTD", "COMPRESSION_LEVEL 9"],
        {"compression": "zstd", "compression_level": 9},
    ),
    (
        "ZSTD",
        3,
        "write",
        "ZSTD",
        3,
        ["COMPRESSION ZSTD", "COMPRESSION_LEVEL 3"],
        {"compression": "zstd", "compression_level": 3},
    ),
    (
        "ZSTD",
        22,
        "process",
        "ZSTD",
        22,
        ["COMPRESSION ZSTD", "COMPRESSION_LEVEL 22"],
        {"compression": "zstd", "compression_level": 22},
    ),
    (
        None,
        None,
        "write",
        "ZSTD",
        15,
        ["COMPRESSION ZSTD", "COMPRESSION_LEVEL 15"],
        {"compression": "zstd", "compression_level": 15},
    ),
    # DuckDB takes COMPRESSION_LEVEL for ZSTD only: a GZIP/BROTLI level is a
    # binder error there, so the COPY clause leaves it out.
    (
        "gzip",
        None,
        "write",
        "GZIP",
        6,
        ["COMPRESSION GZIP"],
        {"compression": "gzip", "compression_level": 6},
    ),
    (
        "GZIP",
        None,
        "process",
        "GZIP",
        6,
        ["COMPRESSION GZIP"],
        {"compression": "gzip", "compression_level": 6},
    ),
    (
        "GZIP",
        9,
        "write",
        "GZIP",
        9,
        ["COMPRESSION GZIP"],
        {"compression": "gzip", "compression_level": 9},
    ),
    (
        "BROTLI",
        None,
        "write",
        "BROTLI",
        11,
        ["COMPRESSION BROTLI"],
        {"compression": "brotli", "compression_level": 11},
    ),
    (
        "BROTLI",
        11,
        "write",
        "BROTLI",
        11,
        ["COMPRESSION BROTLI"],
        {"compression": "brotli", "compression_level": 11},
    ),
    ("LZ4", None, "write", "LZ4", None, ["COMPRESSION LZ4"], {"compression": "lz4"}),
    ("lz4", 9, "write", "LZ4", None, ["COMPRESSION LZ4"], {"compression": "lz4"}),
    ("SNAPPY", 15, "process", "SNAPPY", None, ["COMPRESSION SNAPPY"], {"compression": "snappy"}),
    (
        "UNCOMPRESSED",
        None,
        "write",
        "UNCOMPRESSED",
        None,
        ["COMPRESSION UNCOMPRESSED"],
        {"compression": None},
    ),
    # Approved change 4: `none` and `uncompressed` are one codec.
    (
        "none",
        None,
        "write",
        "UNCOMPRESSED",
        None,
        ["COMPRESSION UNCOMPRESSED"],
        {"compression": None},
    ),
    (
        "NONE",
        5,
        "process",
        "UNCOMPRESSED",
        None,
        ["COMPRESSION UNCOMPRESSED"],
        {"compression": None},
    ),
    (
        "uncompressed",
        None,
        "process",
        "UNCOMPRESSED",
        None,
        ["COMPRESSION UNCOMPRESSED"],
        {"compression": None},
    ),
]


@pytest.mark.parametrize(
    ("name", "level", "context", "canonical", "resolved", "copy", "kwargs"), RESOLVE_TABLE
)
def test_resolve_table(name, level, context, canonical, resolved, copy, kwargs):
    codec = resolve_codec(name, level, context=context)
    assert isinstance(codec, ResolvedCodec)
    assert codec.name == canonical
    assert codec.level == resolved
    assert codec.duckdb_copy_options() == copy
    assert codec.pyarrow_kwargs() == kwargs


#: (name, level, context, the parameter the error names)
RESOLVE_ERRORS = [
    ("LZ4_RAW", None, "write", "compression"),
    ("lz4_raw", None, "process", "compression"),
    ("LZO", None, "write", "compression"),
    ("", 3, "write", "compression"),
    ("ZSTD", 0, "write", "compression_level"),
    ("ZSTD", 23, "write", "compression_level"),
    ("GZIP", 10, "write", "compression_level"),
    ("BROTLI", 12, "process", "compression_level"),
    ("ZSTD", True, "write", "compression_level"),
    ("ZSTD", 3.5, "write", "compression_level"),
    ("ZSTD", "15", "write", "compression_level"),
    ("ZSTD", "1; DROP TABLE t", "write", "compression_level"),
]


@pytest.mark.parametrize(("name", "level", "context", "param"), RESOLVE_ERRORS)
def test_resolve_errors(name, level, context, param):
    with pytest.raises(InvalidParameterError) as excinfo:
        resolve_codec(name, level, context=context)
    assert excinfo.value.param_name == param


def test_unknown_context_is_refused():
    with pytest.raises(ValueError, match="context"):
        resolve_codec("ZSTD", None, context="sort")  # type: ignore[arg-type]


def test_resolved_codec_is_frozen():
    codec = resolve_codec("ZSTD", None, context="write")
    with pytest.raises(AttributeError):
        codec.level = 3  # type: ignore[misc]


def test_allowed_codecs_are_canonical_and_exclude_lz4_raw():
    assert ALLOWED_CODECS == ("ZSTD", "GZIP", "BROTLI", "LZ4", "SNAPPY", "UNCOMPRESSED")
    assert "LZ4_RAW" not in ALLOWED_CODECS


# The constructor is a gate too: every clause is interpolated into SQL.
CONSTRUCTOR_ERRORS = [
    ("ZSTD", "1; DROP TABLE t", "compression_level"),
    ("ZSTD", "15", "compression_level"),
    ("ZSTD", 15.0, "compression_level"),
    ("ZSTD", 3.5, "compression_level"),
    ("ZSTD", True, "compression_level"),
    ("ZSTD", None, "compression_level"),
    ("ZSTD", 23, "compression_level"),
    ("GZIP", 12, "compression_level"),
    ("SNAPPY", 3, "compression_level"),
    ("LZ4_RAW", None, "compression"),
    ("zstd", 15, "compression"),
    ("NONE", None, "compression"),
    ("ZSTD; DROP TABLE t", None, "compression"),
]


@pytest.mark.parametrize(("name", "level", "param"), CONSTRUCTOR_ERRORS)
def test_resolved_codec_cannot_hold_a_bad_value(name, level, param):
    with pytest.raises(InvalidParameterError) as excinfo:
        ResolvedCodec(name, level)
    assert excinfo.value.param_name == param


@pytest.mark.parametrize("bad", ["1; DROP TABLE t", 3.5])
def test_no_injected_level_reaches_a_copy_clause(bad):
    """String and float levels die in the owner, on the factory and the constructor."""
    with pytest.raises(InvalidParameterError):
        resolve_codec("ZSTD", bad, context="write").duckdb_copy_options()
    with pytest.raises(InvalidParameterError):
        ResolvedCodec("ZSTD", bad).duckdb_copy_options()


@pytest.mark.parametrize(
    ("footer", "level", "name", "resolved"),
    [
        ("ZSTD", None, "ZSTD", 15),
        ("zstd", 3, "ZSTD", 3),
        ("LZ4_RAW", None, "LZ4", None),
        ("LZ4", None, "LZ4", None),
        ("UNCOMPRESSED", None, "UNCOMPRESSED", None),
        ("GZIP", None, "GZIP", 6),
        ("BROTLI", None, "BROTLI", 11),
        ("SNAPPY", 9, "SNAPPY", None),
        ("LZO", None, "ZSTD", 15),
        ("", None, "ZSTD", 15),
        (None, None, "ZSTD", 15),
    ],
)
def test_codec_from_footer(footer, level, name, resolved):
    codec = codec_from_footer(footer, level)
    assert (codec.name, codec.level) == (name, resolved)


def test_pyarrow_kwargs_give_uncompressed_as_pyarrows_none():
    assert resolve_codec("none", None, context="write").pyarrow_kwargs() == {"compression": None}


# ---------------------------------------------------------------------------
# GZIP and BROTLI on a DuckDB COPY: the level DuckDB writes, stated once
# ---------------------------------------------------------------------------
#
# DuckDB 1.5.6 and 2.0.0 refuse COMPRESSION_LEVEL for every codec but ZSTD
# (a binder error), and write GZIP at miniz's MZ_DEFAULT_LEVEL (6) and BROTLI
# at BROTLI_DEFAULT_QUALITY (11) -- extension/parquet/column_writer.cpp and
# third_party/miniz/miniz_wrapper.hpp at both tags. Parquet records no level and
# miniz and zlib emit different deflate bytes at the same level, so no file
# comparison can read the level back; the owner states it instead.


@pytest.mark.parametrize("codec", ["GZIP", "BROTLI"])
def test_duckdb_copy_refuses_a_level_for_gzip_and_brotli(tmp_path, codec):
    import duckdb

    out = tmp_path / "x.parquet"
    with pytest.raises(duckdb.BinderException, match="only supported for the ZSTD"):
        duckdb.sql(
            f"COPY (SELECT 1 AS a) TO '{out}' (FORMAT PARQUET, COMPRESSION {codec}, "
            "COMPRESSION_LEVEL 6)"
        )


@pytest.mark.parametrize(
    ("codec", "level", "copy_level"),
    [
        ("ZSTD", None, 15),
        ("ZSTD", 3, 3),
        ("GZIP", None, 6),
        ("GZIP", 9, 6),
        ("BROTLI", None, 11),
        ("BROTLI", 4, 11),
        ("LZ4", None, None),
        ("SNAPPY", None, None),
        ("UNCOMPRESSED", None, None),
    ],
)
def test_copy_level_is_the_level_a_duckdb_copy_writes(codec, level, copy_level):
    assert resolve_codec(codec, level, context="write").copy_level == copy_level


@pytest.mark.parametrize("context", ["write", "process"])
@pytest.mark.parametrize(("name", "level"), [("GZIP", 6), ("BROTLI", 11)])
def test_an_unset_level_is_the_same_on_copy_and_pyarrow(context, name, level):
    """A COPY and a pyarrow write of GZIP (6) or BROTLI (11) use one level."""
    codec = resolve_codec(name, None, context=context)
    assert codec.pyarrow_kwargs()["compression_level"] == codec.copy_level == level


@pytest.mark.parametrize(("codec", "level", "written"), [("GZIP", 9, 6), ("BROTLI", 4, 11)])
def test_a_copy_that_cannot_write_the_level_says_which_it_writes(caplog, codec, level, written):
    import logging

    from geoparquet_io.core import compression

    compression._WARNED_COPY_LEVELS.clear()
    resolved = resolve_codec(codec, level, context="write")
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        assert resolved.duckdb_copy_options() == [f"COMPRESSION {codec}"]
    assert f"DuckDB writes {codec} at level {written}" in caplog.text
    assert f"not {resolved.level}" in caplog.text


@pytest.mark.parametrize(
    ("codec", "level"),
    [("GZIP", None), ("GZIP", 6), ("BROTLI", None), ("BROTLI", 11), ("ZSTD", None)],
)
def test_a_copy_at_the_level_asked_for_is_quiet(caplog, codec, level):
    import logging

    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        resolve_codec(codec, level, context="write").duckdb_copy_options()
    assert caplog.text == ""


def test_level_help_is_the_owner_level_per_codec(monkeypatch):
    """``--compression-level`` help asks the owner, not COMPRESSION_RANGES."""
    from geoparquet_io.cli import decorators as dec

    asked = []
    real = dec.resolve_codec

    def spy(codec, level, *, context, verbose=False):
        asked.append((codec, level, context))
        return real(codec, level, context=context, verbose=verbose)

    monkeypatch.setattr(dec, "resolve_codec", spy)
    text = dec._compression_level_help("process")
    assert ("GZIP", None, "process") in asked and ("ZSTD", None, "process") in asked
    assert "ZSTD: 1-22 (default: 9)" in text and "GZIP: 1-9 (default: 6)" in text


# ---------------------------------------------------------------------------
# Every write path that takes a codec name goes through the owner
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("strategy", ["duckdb-kv", "in-memory", "streaming", "disk-rewrite"])
@pytest.mark.parametrize("spelling", ["none", "uncompressed", "NONE"])
def test_table_write_none_is_uncompressed_on_every_strategy(tmp_path, strategy, spelling):
    import geoparquet_io as gpio

    out = tmp_path / "out.parquet"
    gpio.read(str(PLACES)).write(str(out), compression=spelling, write_strategy=strategy)
    assert _codecs(out) == {"UNCOMPRESSED"}


@pytest.mark.parametrize("strategy", ["duckdb-kv", "in-memory", "streaming", "disk-rewrite"])
def test_table_write_lz4_raw_is_refused_on_every_strategy(tmp_path, strategy):
    import geoparquet_io as gpio

    out = tmp_path / "out.parquet"
    with pytest.raises(InvalidParameterError, match="LZ4_RAW"):
        gpio.read(str(PLACES)).write(str(out), compression="LZ4_RAW", write_strategy=strategy)
    assert not out.exists()


@pytest.mark.parametrize("strategy", ["duckdb-kv", "disk-rewrite"])
def test_copy_strategies_reject_an_injected_level_with_the_owner_error(tmp_path, strategy):
    import geoparquet_io as gpio

    out = tmp_path / "out.parquet"
    with pytest.raises(InvalidParameterError) as excinfo:
        gpio.read(str(PLACES)).write(
            str(out), compression_level="1; DROP TABLE t", write_strategy=strategy
        )
    assert excinfo.value.param_name == "compression_level"


# ---------------------------------------------------------------------------
# The same outcome through every CLI entry that takes --compression
# ---------------------------------------------------------------------------


def _codecs(path: Path) -> set[str]:
    md = pq.ParquetFile(str(path)).metadata
    return {
        md.row_group(r).column(c).compression
        for r in range(md.num_row_groups)
        for c in range(md.num_columns)
    }


def _aggregate_output(tmp_path: Path) -> Path:
    """An h3 aggregate output for ``process overview`` to read."""
    out = tmp_path / "cells.parquet"
    result = CliRunner().invoke(
        cli, ["process", "aggregate", "h3", str(PLACES), str(out), "--resolution", "6"]
    )
    assert result.exit_code == 0, result.output
    return out


def _cli_write(tmp_path: Path, command: str, codec_args: list[str]):
    """Run ``command`` with ``codec_args``; return (result, the output file)."""
    out = tmp_path / f"{command.replace(' ', '_')}.parquet"
    args = {
        "convert geoparquet": ["convert", "geoparquet", str(PLACES), str(out)],
        "sort hilbert": ["sort", "hilbert", str(PLACES), str(out)],
        "extract": ["extract", str(PLACES), str(out)],
        # places_test already has a bbox column, which `add bbox` copies through
        # untouched when no output setting asks for a rewrite.
        "add bbox": ["add", "bbox", str(BUILDINGS), str(out)],
        "process aggregate": [
            "process",
            "aggregate",
            "h3",
            str(PLACES),
            str(out),
            "--resolution",
            "6",
        ],
    }.get(command)
    if command == "process overview":
        src = _aggregate_output(tmp_path)
        out_dir = tmp_path / "overviews"
        args = ["process", "overview", str(src), "--levels", "4", "--output-dir", str(out_dir)]
        result = CliRunner().invoke(cli, args + codec_args)
        outputs = sorted(out_dir.glob("*.parquet")) if out_dir.exists() else []
        return result, (outputs[0] if outputs else out_dir / "missing.parquet")
    result = CliRunner().invoke(cli, args + codec_args)
    return result, out


CLI_COMMANDS = [
    "convert geoparquet",
    "sort hilbert",
    "extract",
    "add bbox",
    "process aggregate",
    "process overview",
]


@pytest.mark.parametrize("command", CLI_COMMANDS)
@pytest.mark.parametrize("spelling", ["none", "uncompressed", "UNCOMPRESSED"])
def test_cli_none_and_uncompressed_write_uncompressed(tmp_path, command, spelling):
    result, out = _cli_write(tmp_path, command, ["--compression", spelling])
    assert result.exit_code == 0, result.output
    assert _codecs(out) == {"UNCOMPRESSED"}


@pytest.mark.parametrize("command", CLI_COMMANDS)
def test_cli_lz4_raw_is_a_usage_error(tmp_path, command):
    result, out = _cli_write(tmp_path, command, ["--compression", "LZ4_RAW"])
    assert result.exit_code == 2, result.output
    assert "LZ4_RAW" in result.output
    assert not out.exists()


@pytest.mark.parametrize("command", CLI_COMMANDS)
@pytest.mark.parametrize(
    ("codec_args", "message"),
    [
        (["--compression", "GZIP", "--compression-level", "12"], "GZIP"),
        (["--compression-level", "12", "--compression", "BROTLI"], None),
        (["--compression-level", "23"], None),
    ],
    ids=["gzip-12", "level-before-codec", "zstd-23"],
)
def test_cli_level_range_follows_the_codec(tmp_path, command, codec_args, message):
    """The level's range is the codec's, checked before any work runs."""
    if "BROTLI" in codec_args:
        # 12 is out of BROTLI's 1-11 even though it is inside ZSTD's 1-22.
        message = "BROTLI"
    result, out = _cli_write(tmp_path, command, codec_args)
    assert result.exit_code == 2, result.output
    if message:
        assert message in result.output
    assert not out.exists()


def test_cli_gzip_level_inside_its_range_is_accepted(tmp_path):
    result, out = _cli_write(
        tmp_path, "convert geoparquet", ["--compression", "GZIP", "--compression-level", "9"]
    )
    assert result.exit_code == 0, result.output
    assert _codecs(out) == {"GZIP"}


# ---------------------------------------------------------------------------
# #1335: an unset level on the COPY paths is the documented 15
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "command",
    ["convert geoparquet", "sort hilbert", "extract", "add bbox"],
)
def test_cli_unset_level_is_the_documented_fifteen(tmp_path, command):
    unset_dir = tmp_path / "unset"
    l15_dir = tmp_path / "l15"
    unset_dir.mkdir()
    l15_dir.mkdir()
    r1, unset = _cli_write(unset_dir, command, [])
    r2, l15 = _cli_write(l15_dir, command, ["--compression-level", "15"])
    assert r1.exit_code == 0, r1.output
    assert r2.exit_code == 0, r2.output
    assert unset.read_bytes() == l15.read_bytes()


# ---------------------------------------------------------------------------
# Guard: no codec map or codec list outside the owner
# ---------------------------------------------------------------------------

CODEC_NAMES = {"zstd", "gzip", "brotli", "lz4", "lz4_raw", "snappy", "uncompressed", "none"}
#: Names that only count as codecs beside a real one: ``"none"`` alone is a
#: common sentinel (``--out-geometry none``) that has nothing to do with codecs.
UNAMBIGUOUS = CODEC_NAMES - {"none"}
OWNER = PACKAGE / "core" / "compression.py"


def _codec_strings(nodes) -> list[str]:
    return [
        n.value.lower()
        for n in nodes
        if isinstance(n, ast.Constant)
        and isinstance(n.value, str)
        and n.value.lower() in CODEC_NAMES
    ]


def _is_codec_collection(names: list[str]) -> bool:
    return len(names) >= 2 and any(name in UNAMBIGUOUS for name in names)


def codec_collections(tree: ast.AST) -> list[int]:
    """Line numbers of dict literals keyed by, or list/tuple/set literals of, codec names."""
    hits = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Dict):
            names = _codec_strings(k for k in node.keys if k is not None)
        elif isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            names = _codec_strings(node.elts)
        else:
            continue
        if _is_codec_collection(names):
            hits.append(node.lineno)
    return hits


def test_no_codec_map_or_list_outside_the_owner():
    found = {}
    for path in sorted(PACKAGE.rglob("*.py")):
        if path == OWNER:
            continue
        lines = codec_collections(ast.parse(path.read_text(encoding="utf-8")))
        if lines:
            found[str(path.relative_to(REPO))] = lines
    assert found == {}, f"codec maps/lists outside core/compression.py: {found}"


@pytest.mark.parametrize(
    "source",
    [
        'm = {"zstd": "ZSTD", "none": "UNCOMPRESSED"}',
        'ok = ["ZSTD", "GZIP", "SNAPPY"]',
        'ok = frozenset({"ZSTD", "LZ4"})',
        'if c in ("GZIP", "ZSTD", "BROTLI"): pass',
    ],
)
def test_the_guard_catches_an_injected_copy(source):
    assert codec_collections(ast.parse(source)) != []


@pytest.mark.parametrize(
    "source",
    ['x = ["none", "geometry"]', 'mode = {"none": 1, "bbox": 2}', 'c = "ZSTD"'],
)
def test_the_guard_ignores_non_codec_collections(source):
    assert codec_collections(ast.parse(source)) == []


def test_the_owner_is_the_only_codec_list():
    assert codec_collections(ast.parse(OWNER.read_text(encoding="utf-8"))) != []


# ---------------------------------------------------------------------------
# Guard: no second default, case fold, COPY clause or pyarrow codec kwarg
# ---------------------------------------------------------------------------
#
# The collection guard above misses the shapes the T2a audit found: a codec or
# level filled in beside the call (`compression or "ZSTD"`, `compression_level
# or 15`, a parameter or field default of `"ZSTD"`/`15`), a codec name folded
# by hand (`compression.lower()`), a COPY clause spelled out (`f"COMPRESSION
# {c}"`), and pyarrow handed `compression=`/`compression_level=` directly
# instead of `**codec.pyarrow_kwargs()`.

#: pyarrow writers whose codec kwargs must come from ``pyarrow_kwargs()``.
PYARROW_WRITERS = {"write_table", "ParquetWriter", "write_to_dataset", "write_dataset"}


def _ident(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


def _names_compression(node: ast.AST) -> bool:
    return "compression" in _ident(node).lower()


def _codec_or_level_literal(node: ast.AST) -> bool:
    """A codec name, or gpio's default level, written as a literal."""
    if not isinstance(node, ast.Constant):
        return False
    if isinstance(node.value, str):
        return node.value.lower() in CODEC_NAMES
    return type(node.value) is int and node.value == 15


def _defaults(func: ast.FunctionDef | ast.AsyncFunctionDef):
    args = func.args
    positional = args.posonlyargs + args.args
    yield from zip(positional[len(positional) - len(args.defaults) :], args.defaults, strict=True)
    yield from (
        (a, d) for a, d in zip(args.kwonlyargs, args.kw_defaults, strict=True) if d is not None
    )


def _mentions_codec_or_level(node: ast.AST) -> bool:
    return any(
        any(word in _ident(sub).lower() for word in ("compression", "codec", "level"))
        for sub in ast.walk(node)
    )


def _second_default(node: ast.AST) -> bool:
    if isinstance(node, ast.IfExp):
        return _mentions_codec_or_level(node.test) and (
            _codec_or_level_literal(node.body) or _codec_or_level_literal(node.orelse)
        )
    if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or):
        return any(_names_compression(v) for v in node.values[:-1]) and _codec_or_level_literal(
            node.values[-1]
        )
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return any(
            "compression" in arg.arg and _codec_or_level_literal(default)
            for arg, default in _defaults(node)
        )
    if isinstance(node, ast.AnnAssign) and node.value is not None:
        return _names_compression(node.target) and _codec_or_level_literal(node.value)
    if isinstance(node, ast.Assign):
        return any("COMPRESSION" in _ident(t).upper() for t in node.targets) and (
            _codec_or_level_literal(node.value)
        )
    return False


def _case_fold(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in ("upper", "lower")
        and any(word in _ident(node.func.value).lower() for word in ("compression", "codec"))
    )


def _copy_clause(node: ast.AST) -> bool:
    """A string, f-string or concatenation that spells a COMPRESSION clause."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        if id(node) in _DOCSTRINGS:
            return False
        text = node.value
        return "COMPRESSION_LEVEL" in text or any(
            f"COMPRESSION {codec.upper()}" in text for codec in CODEC_NAMES
        )
    if isinstance(node, ast.JoinedStr):
        parts = [v.value for v in node.values if isinstance(v, ast.Constant)]
    elif isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        parts = [v.value for v in (node.left, node.right) if isinstance(v, ast.Constant)]
        parts = [p for p in parts if isinstance(p, str)]
    else:
        return False
    return any("COMPRESSION " in p or "COMPRESSION_LEVEL" in p for p in parts)


def _pyarrow_codec_kwarg(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Call)
        and _ident(node.func) in PYARROW_WRITERS
        and any(kw.arg in ("compression", "compression_level") for kw in node.keywords)
    )


def _codec_dict(node: ast.AST) -> bool:
    """``{"compression": "zstd"}`` -- a pyarrow kwargs dict built by hand."""
    return isinstance(node, ast.Dict) and any(
        isinstance(k, ast.Constant)
        and k.value in ("compression", "compression_level")
        and _codec_or_level_literal(v)
        for k, v in zip(node.keys, node.values, strict=True)
    )


def _codec_literal_arg(node: ast.AST) -> bool:
    """A codec name written as a literal argument to any call."""
    if not isinstance(node, ast.Call):
        return False
    values = [*node.args, *(kw.value for kw in node.keywords)]
    return any(
        isinstance(v, ast.Constant)
        and isinstance(v.value, str)
        and v.value.lower() in UNAMBIGUOUS
        # "ZSTD" or "zstd", not a "Uncompressed" table heading.
        and (v.value.isupper() or v.value.islower())
        for v in values
    )


def _level_literal_kwarg(node: ast.AST) -> bool:
    """``compression_level=<int>`` -- a level chosen at the call site."""
    return isinstance(node, ast.Call) and any(
        kw.arg == "compression_level"
        and isinstance(kw.value, ast.Constant)
        and type(kw.value.value) is int
        for kw in node.keywords
    )


SHAPES = {
    "second-default": _second_default,
    "case-fold": _case_fold,
    "copy-clause": _copy_clause,
    "pyarrow-codec-kwarg": _pyarrow_codec_kwarg,
    "codec-dict": _codec_dict,
    "codec-literal-arg": _codec_literal_arg,
    "level-literal-kwarg": _level_literal_kwarg,
}

#: Docstring nodes of the tree being scanned: prose may name a clause.
_DOCSTRINGS: set[int] = set()

#: Literal codec or level choices outside the owner that are justified, each
#: with its count and reason: internal files gpio writes for itself (a staging
#: file, a download cache, PMTiles chunks) at a codec picked for speed; the
#: benchmark baselines, fixed so runs compare across versions and tools; and a
#: case fold of a *text*-encoding codec name, which is not compression. Every
#: gpio write among them still resolves its codec through the owner.
JUSTIFIED_CALL_SITES = {
    ("geoparquet_io/core/pmtiles_chunks.py", "level-literal-kwarg"): (
        1,
        "PMTiles chunk staging: a throwaway intermediate, ZSTD 3 for speed",
    ),
    ("geoparquet_io/core/admin_datasets.py", "codec-literal-arg"): (
        2,
        "admin-boundary download caches (default and per-level Overture): ZSTD 3",
    ),
    ("geoparquet_io/core/partition/staging.py", "codec-literal-arg"): (
        1,
        "partition staging file: SNAPPY, read once and deleted",
    ),
    ("geoparquet_io/core/benchmark.py", "codec-literal-arg"): (
        3,
        "benchmark baselines: DuckDB's ZSTD 3 and the geopandas writers' own"
        " compression='zstd', fixed so runs compare across versions and tools",
    ),
    ("geoparquet_io/core/source_text.py", "case-fold"): (
        1,
        "a *text* encoding codec (latin-1, cp1252) folded for a UDF name, not compression",
    ),
}


def codec_policy_copies(tree: ast.AST) -> dict[str, list[int]]:
    """Line numbers per shape of a codec or level decided outside the owner."""
    _DOCSTRINGS.clear()
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if (
            isinstance(body, list)
            and body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
        ):
            _DOCSTRINGS.add(id(body[0].value))
    hits: dict[str, list[int]] = {}
    for node in ast.walk(tree):
        for shape, matches in SHAPES.items():
            if matches(node):
                hits.setdefault(shape, []).append(node.lineno)
    return hits


def test_no_codec_policy_outside_the_owner():
    found = {}
    for path in sorted(PACKAGE.rglob("*.py")):
        if path == OWNER:
            continue
        rel = str(path.relative_to(REPO))
        hits = codec_policy_copies(ast.parse(path.read_text(encoding="utf-8")))
        for shape, lines in hits.items():
            allowed, _reason = JUSTIFIED_CALL_SITES.get((rel, shape), (0, ""))
            if len(lines) != allowed:
                found[(rel, shape)] = lines
    assert found == {}, f"codec or level decided outside core/compression.py: {found}"


def test_every_justified_call_site_still_exists():
    """An allowlist entry whose call is gone must be deleted, not kept as slack."""
    for (rel, shape), (count, reason) in JUSTIFIED_CALL_SITES.items():
        assert reason
        hits = codec_policy_copies(ast.parse((REPO / rel).read_text(encoding="utf-8")))
        assert len(hits.get(shape, [])) == count, (rel, shape)


@pytest.mark.parametrize(
    ("shape", "source"),
    [
        ("second-default", 'c = compression or "ZSTD"'),
        ("second-default", "lvl = compression_level or 15"),
        ("second-default", 'def f(compression="ZSTD"): pass'),
        ("second-default", "def f(*, compression_level: int = 15): pass"),
        ("second-default", 'class S:\n    compression: str = "ZSTD"'),
        ("second-default", "DEFAULT_COMPRESSION_LEVEL = 15"),
        ("case-fold", "duck = compression.lower()"),
        ("case-fold", "c = self.compression.upper()"),
        ("copy-clause", 'q = f"COMPRESSION {codec}"'),
        ("copy-clause", 'q = f"(FORMAT PARQUET, COMPRESSION_LEVEL {lvl})"'),
        ("copy-clause", 'q = "COMPRESSION " + codec'),
        ("pyarrow-codec-kwarg", 'pq.write_table(t, p, compression="zstd")'),
        ("pyarrow-codec-kwarg", "pq.ParquetWriter(p, s, compression_level=lvl)"),
        # The second audit's shapes.
        ("second-default", "lvl = level if level is not None else 15"),
        ("second-default", 'c = "ZSTD" if compression is None else compression'),
        ("codec-dict", 'kw = {"compression": "zstd"}'),
        ("codec-dict", 'kw = {"compression_level": 15}'),
        ("codec-literal-arg", 'write(t, p, "zstd")'),
        ("codec-literal-arg", 'resolve_codec("GZIP", None, context="write")'),
        ("codec-literal-arg", 'opts(codec="brotli")'),
        ("copy-clause", 'q = "(FORMAT PARQUET, COMPRESSION ZSTD)"'),
        ("copy-clause", 'q = "COMPRESSION_LEVEL 15"'),
        ("case-fold", "c = codec.upper()"),
        ("case-fold", "c = self.codec_name.lower()"),
        ("level-literal-kwarg", "add_column(src, out, compression_level=9)"),
    ],
)
def test_the_policy_guard_catches_each_shape(shape, source):
    assert shape in codec_policy_copies(ast.parse(source))


@pytest.mark.parametrize(
    "source",
    [
        "pq.write_table(t, p, **codec.pyarrow_kwargs())",
        'q = f"(FORMAT PARQUET, {clauses})"',
        "def f(compression=None, compression_level=None): pass",
        "c = compression or None",
        "name = path.lower()",
        "def f(level=15): pass",
        'def f():\n    """Writes COMPRESSION ZSTD, COMPRESSION_LEVEL 15."""',
        'if out_geometry == "none": pass',
        'run(out_geometry="none")',
        "h3 = level if level is not None else 9",
        'kw = {"compression": codec.name}',
        "f(compression_level=compression_level)",
    ],
)
def test_the_policy_guard_ignores_the_owner_routes(source):
    assert codec_policy_copies(ast.parse(source)) == {}


# ---------------------------------------------------------------------------
# #1026: the geometry-less BigQuery branch skipped the owner and the remote write
# ---------------------------------------------------------------------------


def _bigquery_plain(out: str, **kwargs) -> None:
    from tests.test_extract_bigquery import _bq_result_table, _extract_to

    _extract_to(out, table=_bq_result_table(200), with_geometry=False, **kwargs)


@pytest.mark.parametrize("spelling", ["UNCOMPRESSED", "none"])
def test_1026_bigquery_plain_branch_accepts_every_codec_spelling(tmp_path, spelling):
    out = tmp_path / "plain.parquet"
    _bigquery_plain(str(out), compression=spelling)
    assert _codecs(out) == {"UNCOMPRESSED"}
    assert b"geo" not in (pq.read_schema(str(out)).metadata or {})


def test_1026_bigquery_plain_branch_refuses_lz4_raw_with_the_owner_error(tmp_path):
    with pytest.raises(InvalidParameterError, match="LZ4_RAW"):
        _bigquery_plain(str(tmp_path / "plain.parquet"), compression="LZ4_RAW")


def test_1026_bigquery_plain_branch_writes_the_documented_level(tmp_path):
    """Unset is gpio's 15, not pyarrow's 1: byte-identical to an explicit 15."""
    unset, l15 = tmp_path / "unset.parquet", tmp_path / "l15.parquet"
    _bigquery_plain(str(unset))
    _bigquery_plain(str(l15), compression_level=15)
    assert unset.read_bytes() == l15.read_bytes()


def test_1026_bigquery_plain_branch_writes_to_a_remote_url(tmp_path, monkeypatch):
    """The object store is the only thing mocked: the upload lands a real file."""
    uploaded = {}

    def fake_upload(local_path, remote_path, *args, **kwargs):
        uploaded[remote_path] = Path(local_path).read_bytes()

    # The geometry-free branch stages and uploads through the plain-table funnel.
    import geoparquet_io.core.write_funnels as funnels

    monkeypatch.setattr(funnels, "upload_if_remote", fake_upload)
    _bigquery_plain("s3://bucket/plain.parquet")
    assert list(uploaded) == ["s3://bucket/plain.parquet"]
    local = tmp_path / "landed.parquet"
    local.write_bytes(uploaded["s3://bucket/plain.parquet"])
    assert pq.read_metadata(str(local)).num_rows == 200
    assert _codecs(local) == {"ZSTD"}


# ---------------------------------------------------------------------------
# Every COPY and pyarrow site writes the owner's level (read back from the file)
# ---------------------------------------------------------------------------
#
# Parquet records no level, so each site is fingerprinted: written unset, at an
# explicit 15 and at 3, the unset bytes must equal 15's and differ from 3's,
# with the footer naming ZSTD. A site that drops its level falls to DuckDB's 3
# or pyarrow's 1 and fails here (the T2a verifier's untested sites).


def _plain_table():
    """places_test without geometry: strings that compress differently at 3 and 15."""
    return pq.read_table(str(PLACES)).drop_columns(["geometry", "bbox"])


def _strategy(name: str):
    from geoparquet_io.core.write_strategies import WriteStrategy, WriteStrategyFactory

    return WriteStrategyFactory.get_strategy(WriteStrategy(name))


def _table_plain(strategy: str):
    def write(out: Path, level):
        _strategy(strategy).write_from_table(
            _plain_table(),
            str(out),
            geometry_column=None,
            geoparquet_version=None,
            compression="ZSTD",
            compression_level=level,
            row_group_size_mb=None,
            row_group_rows=None,
            verbose=False,
        )

    return write


def _query_plain(strategy: str, geometry_column: str | None = None):
    select = "*" if geometry_column else "* EXCLUDE (geometry, bbox)"

    def write(out: Path, level):
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path

        con = get_duckdb_connection(load_httpfs=False)
        try:
            _strategy(strategy).write_from_query(
                con,
                f"SELECT {select} FROM read_parquet({sql_path(str(PLACES))})",
                str(out),
                geometry_column=geometry_column,
                original_metadata=None,
                geoparquet_version="1.1" if geometry_column else None,
                compression="ZSTD",
                compression_level=level,
                row_group_size_mb=None,
                row_group_rows=None,
                input_crs=None,
                verbose=False,
            )
        finally:
            con.close()

    return write


def _convert(version: str):
    def write(out: Path, level):
        extra = [] if level is None else ["--compression-level", str(level)]
        args = ["convert", "geoparquet", str(PLACES), str(out), "--geoparquet-version", version]
        result = CliRunner().invoke(cli, args + extra)
        assert result.exit_code == 0, result.output

    return write


def _table_write(strategy: str):
    def write(out: Path, level):
        import geoparquet_io as gpio

        gpio.read(str(PLACES)).write(str(out), compression_level=level, write_strategy=strategy)

    return write


def _geo_rewrite(out: Path, level):
    """derive_geo_from_file's pyarrow rewrite, the V2 repair's fallback."""
    from geoparquet_io.core.derive_geo_from_file import _rewrite_file_with_geo_metadata

    pq.write_table(_plain_table(), str(out), compression="snappy")
    _rewrite_file_with_geo_metadata(str(out), {"version": "2.0.0"}, "ZSTD", level, None)


LEVEL_SITES = {
    "duckdb-kv table, no geometry": _table_plain("duckdb-kv"),
    "duckdb-kv query, no geometry": _query_plain("duckdb-kv"),
    "disk-rewrite table, no geometry": _table_plain("disk-rewrite"),
    "disk-rewrite query, no geometry": _query_plain("disk-rewrite"),
    "streaming table, no geometry": _table_plain("streaming"),
    "streaming query, no geometry": _query_plain("streaming"),
    "streaming query, geometry": _query_plain("streaming", "geometry"),
    "duckdb-kv query, geometry": _query_plain("duckdb-kv", "geometry"),
    "disk-rewrite query, geometry": _query_plain("disk-rewrite", "geometry"),
    "in-memory query, geometry": _query_plain("in-memory", "geometry"),
    "in-memory table, no geometry": _table_plain("in-memory"),
    "plain COPY, 2.0": _convert("2.0"),
    "plain COPY, parquet-geo-only": _convert("parquet-geo-only"),
    "Table.write in-memory": _table_write("in-memory"),
    "Table.write disk-rewrite": _table_write("disk-rewrite"),
    "Table.write streaming": _table_write("streaming"),
    "Table.write duckdb-kv": _table_write("duckdb-kv"),
    "geo rewrite": _geo_rewrite,
}


@pytest.mark.parametrize("site", sorted(LEVEL_SITES))
def test_every_write_site_writes_the_owner_level(tmp_path, site):
    write = LEVEL_SITES[site]
    outputs = {}
    for label, level in (("unset", None), ("l15", 15), ("l3", 3)):
        out = tmp_path / f"{label}.parquet"
        write(out, level)
        outputs[label] = out
    assert "ZSTD" in _codecs(outputs["unset"])
    assert outputs["l15"].read_bytes() != outputs["l3"].read_bytes(), "15 and 3 must differ here"
    assert outputs["unset"].read_bytes() == outputs["l15"].read_bytes()


# ---------------------------------------------------------------------------
# Dry-run displays print the clauses the real write emits
# ---------------------------------------------------------------------------


def test_extract_dry_run_prints_the_owner_clauses():
    result = CliRunner().invoke(cli, ["extract", str(PLACES), "out.parquet", "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "COMPRESSION ZSTD, COMPRESSION_LEVEL 15" in result.output
    assert "Using ZSTD:15 compression" in result.output


def test_extract_dry_run_prints_the_chosen_level():
    args = ["extract", str(PLACES), "out.parquet", "--dry-run", "--compression-level", "7"]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, result.output
    assert "COMPRESSION ZSTD, COMPRESSION_LEVEL 7" in result.output


def test_admin_divisions_dry_run_prints_the_owner_clauses():
    args = [
        "add",
        "admin-divisions",
        str(PLACES),
        "out.parquet",
        "--dataset",
        "gaul",
        "--levels",
        "continent",
        "--dry-run",
        "--no-cache",
    ]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, result.output
    assert "COMPRESSION ZSTD, COMPRESSION_LEVEL 15" in result.output
    assert "Using ZSTD:15 compression" in result.output
    assert "ZSTD:None" not in result.output


def test_country_codes_dry_run_prints_the_owner_clauses(caplog):
    import logging

    from geoparquet_io.core.add.country_codes import add_country_codes

    fields = str(DATA / "fields_gpq2_crs84_zstd.parquet")
    with caplog.at_level(logging.INFO, logger="geoparquet_io"):
        add_country_codes(
            input_parquet=fields,
            countries_parquet=fields,
            output_parquet="output.parquet",
            add_bbox_flag=False,
            dry_run=True,
            verbose=False,
        )
    assert "COMPRESSION ZSTD, COMPRESSION_LEVEL 15" in caplog.text
    assert "Using ZSTD:15 compression" in caplog.text


def test_add_computed_column_dry_run_prints_the_owner_clauses(caplog):
    import logging

    from geoparquet_io.core.common import add_computed_column
    from geoparquet_io.core.parquet_writer import DEFAULT_ROW_GROUP_ROWS

    with caplog.at_level(logging.INFO, logger="geoparquet_io"):
        add_computed_column(
            str(PLACES),
            "out.parquet",
            column_name="one",
            sql_expression="1",
            dry_run=True,
            compression="GZIP",
        )
    assert f"COMPRESSION GZIP, ROW_GROUP_SIZE {DEFAULT_ROW_GROUP_ROWS});" in caplog.text
    assert "Using GZIP:6 compression" in caplog.text


@pytest.mark.parametrize(
    ("strategy", "note"),
    [
        ("native", "Using native geometry with DuckDB SPATIAL_JOIN"),
        ("bbox_prefilter", "Using bbox columns for optimized spatial join"),
        ("no_bbox", "Using full geometry intersection (no bbox optimization)"),
    ],
)
def test_one_spatial_join_dry_run_display(caplog, strategy, note):
    """admin-divisions and country-codes print their dry run through one helper."""
    import logging

    from geoparquet_io.core.duckdb_utils import print_spatial_join_dry_run

    with caplog.at_level(logging.INFO, logger="geoparquet_io"):
        print_spatial_join_dry_run(strategy, " SELECT 1 ", "o'brien.parquet", "BROTLI", None, 2_048)
    assert note in caplog.text
    assert (
        "COPY (SELECT 1)\nTO 'o''brien.parquet'\n(FORMAT PARQUET, COMPRESSION BROTLI, ROW_GROUP_SIZE 2048);"
        in (caplog.text)
    )
    assert "Using BROTLI:11 compression" in caplog.text


# ---------------------------------------------------------------------------
# One COPY dry-run body, the admin cache codec, the COPY-context description
# ---------------------------------------------------------------------------


def test_the_gaul_cache_copy_names_its_codec(tmp_path, monkeypatch):
    """The default cache COPY writes ZSTD 3, not DuckDB's unnamed SNAPPY (#1335)."""
    from geoparquet_io.core.admin_datasets import GAULAdminDataset

    dataset = GAULAdminDataset()
    monkeypatch.setattr(dataset, "get_default_source", lambda: str(PLACES))
    monkeypatch.setattr(dataset, "get_s3_config", lambda: {})
    monkeypatch.setattr(dataset, "get_read_parquet_options", lambda: {})
    cache = dataset._download_to_cache(tmp_path / "cache" / "gaul.parquet")
    assert _codecs(cache) == {"ZSTD"}

    import duckdb

    from geoparquet_io.core.duckdb_utils import sql_path

    ref = tmp_path / "ref.parquet"
    duckdb.sql(
        f"COPY (SELECT * FROM read_parquet({sql_path(str(PLACES))})) TO {sql_path(str(ref))} "
        "(FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3)"
    )
    sizes = {
        p: pq.ParquetFile(p).metadata.row_group(0).column(0).total_compressed_size
        for p in (cache, ref)
    }
    assert sizes[cache] == sizes[ref]


@pytest.mark.parametrize(
    ("codec", "level", "clause", "note"),
    [
        (None, None, "COMPRESSION ZSTD, COMPRESSION_LEVEL 15", "ZSTD:15"),
        ("GZIP", 9, "COMPRESSION GZIP", "GZIP:6"),
        ("BROTLI", None, "COMPRESSION BROTLI", "BROTLI:11"),
        ("snappy", None, "COMPRESSION SNAPPY", "SNAPPY"),
    ],
)
def test_one_copy_dry_run_body(caplog, codec, level, clause, note):
    """The one COPY display, with the level that COPY writes."""
    import logging

    from geoparquet_io.core.compression import print_copy_dry_run

    with caplog.at_level(logging.INFO, logger="geoparquet_io"):
        print_copy_dry_run(" SELECT 1 ", "o'brien.parquet", codec, level, 4_096)
    assert (
        f"COPY (SELECT 1)\nTO 'o''brien.parquet'\n(FORMAT PARQUET, {clause}, ROW_GROUP_SIZE 4096);"
        in caplog.text
    )
    assert f"Using {note} compression" in caplog.text


@pytest.mark.parametrize(
    "display",
    [
        "geoparquet_io.core.common.add_computed_column",
        "geoparquet_io.core.extract._print_dry_run_output",
        "geoparquet_io.core.duckdb_utils.print_spatial_join_dry_run",
    ],
)
def test_every_copy_dry_run_display_calls_the_one_body(display):
    import importlib
    import inspect

    module, name = display.rsplit(".", 1)
    source = inspect.getsource(getattr(importlib.import_module(module), name))
    assert "print_copy_dry_run(" in source
    assert "duckdb_copy_options" not in source and "resolve_codec" not in source


def test_copy_description_is_the_level_a_copy_writes():
    assert resolve_codec("GZIP", 9, context="write").copy_description == "GZIP:6"
    assert resolve_codec("ZSTD", 7, context="write").copy_description == "ZSTD:7"
    assert resolve_codec("LZ4", None, context="write").copy_description == "LZ4"


def test_the_copy_level_warning_is_given_once_per_codec_and_level(caplog):
    import logging

    from geoparquet_io.core import compression

    compression._WARNED_COPY_LEVELS.clear()
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        for _ in range(3):
            resolve_codec("GZIP", 8, context="write").duckdb_copy_options()
        resolve_codec("GZIP", 7, context="write").duckdb_copy_options()
    assert caplog.text.count("DuckDB writes GZIP at level 6") == 2
