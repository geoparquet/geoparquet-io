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
        6,
        ["COMPRESSION BROTLI"],
        {"compression": "brotli", "compression_level": 6},
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
        {"compression": "none"},
    ),
    # Approved change 4: `none` and `uncompressed` are one codec.
    (
        "none",
        None,
        "write",
        "UNCOMPRESSED",
        None,
        ["COMPRESSION UNCOMPRESSED"],
        {"compression": "none"},
    ),
    (
        "NONE",
        5,
        "process",
        "UNCOMPRESSED",
        None,
        ["COMPRESSION UNCOMPRESSED"],
        {"compression": "none"},
    ),
    (
        "uncompressed",
        None,
        "process",
        "UNCOMPRESSED",
        None,
        ["COMPRESSION UNCOMPRESSED"],
        {"compression": "none"},
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

    import geoparquet_io.core.extract_bigquery as bq

    monkeypatch.setattr(bq, "upload_if_remote", fake_upload, raising=False)
    _bigquery_plain("s3://bucket/plain.parquet")
    assert list(uploaded) == ["s3://bucket/plain.parquet"]
    local = tmp_path / "landed.parquet"
    local.write_bytes(uploaded["s3://bucket/plain.parquet"])
    assert pq.read_metadata(str(local)).num_rows == 200
    assert _codecs(local) == {"ZSTD"}
