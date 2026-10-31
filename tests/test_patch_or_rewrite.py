"""One patch-else-rewrite decision for every metadata-only footer write (#1367 concept 2).

``parquet_footer.patch_or_rewrite`` tries the footer patch; a footer it cannot
patch falls back to the funnel's one metadata-only rewrite
(``write_funnels.rewrite_footer_kv``), and a race -- the input changed while
it was being copied -- is never answered with a rewrite. Four callers used to
hand-write that decision, each its own way: ``add bbox-metadata`` answered a
race with a rewrite and kept the input's codec, ``check``'s covering repair
re-encoded at the facade's, and ``add stac-metadata`` refused. Two more
(``add geometry-metrics``' Vecorel key, ``reproject --assume-crs84``'s
sanitized copy) re-encoded every file without trying the patch.

The table: every footer-writing command x {patchable, unpatchable, raced}, on
one fixture carrying sidecar keys. Whatever the cell, the sidecar keys that
survive are the same on every path, and only the key the command writes
changes. A patched output keeps the input's pages (SNAPPY, 100-row groups) by
construction; a rewrite takes the facade's codec and row-group count (the lead's
policy for #1367), ``add stac-metadata`` included -- it used to refuse, and now
re-encodes like every other footer write (approved change 3's consequence); a
race propagates and writes nothing.

The four ``covering_repair`` cells are pins, not red-first tests: they passed on
the base, where ``check``'s covering repair already had this policy. They hold
it in place while the other paths are routed through the same owner.

The refusal is planted in ``parquet_footer._patch_footer_file``, the mechanism every
path reaches, rather than crafted into a file: what is under test is the
hand-off, not the refusal.
"""

from __future__ import annotations

import contextlib
import json
import os
import shutil
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core import parquet_footer
from geoparquet_io.core.check_fixes import fix_bboxless_cover
from geoparquet_io.core.parquet_footer import FooterPatchRaced, FooterPatchUnsupported
from geoparquet_io.core.parquet_writer import DEFAULT_ROW_GROUP_ROWS
from tests._defrag_fixtures import SIDECAR_KEYS, footer_kv, geo_block, points_table, write_with_geo

N_ROWS = 1_000
INPUT_GROUP_ROWS = 100
#: A third sidecar key beside the shared pair, in a namespace gpio never writes.
SIDECARS = {**SIDECAR_KEYS, b"acme:lineage": b'{"source": "survey-7"}'}
#: Keys a path is entitled to change: the one it writes, and the footer's own
#: copy of the Arrow schema (a rewrite regenerates it; a patch keeps it in step).
_WRITTEN = {b"geo", b"stac-geoparquet", b"ARROW:schema"}

COLLECTION = {
    "type": "Collection",
    "id": "defrag-t2d",
    "stac_version": "1.1.0",
    "description": "x",
    "license": "proprietary",
    "extent": {
        "spatial": {"bbox": [[-180, -90, 180, 90]]},
        "temporal": {"interval": [[None, None]]},
    },
    "links": [],
}

PATHS = ("add_bbox_metadata", "check_bbox_fix", "covering_repair", "add_stac_metadata")
#: The key each path writes, and how to tell it wrote it.
WRITES = {
    "add_bbox_metadata": b"geo",
    "check_bbox_fix": b"geo",
    "covering_repair": b"geo",
    "add_stac_metadata": b"stac-geoparquet",
}


def _source(path: Path, covering: dict | None = None) -> Path:
    column = {} if covering is None else {"covering": covering}
    return write_with_geo(
        path,
        points_table(N_ROWS, with_bbox=True),
        geo_block(**column),
        extra_kv=SIDECARS,
        compression="snappy",
        row_group_size=INPUT_GROUP_ROWS,
    )


def _invoke(*args) -> object:
    return CliRunner().invoke(cli, [str(a) for a in args], catch_exceptions=True)


def _run(name: str, tmp_path: Path):
    """Run *name* over a fresh fixture; return (source, output, result or error)."""
    covering = {"h3": {"column": "name", "resolution": 9}} if name == "covering_repair" else None
    src = _source(tmp_path / "source.parquet", covering)
    out = tmp_path / "out.parquet"
    if name == "add_bbox_metadata":
        # The command writes in place; `out` starts as a copy of the input, and
        # the raced cell's "nothing written" is then "out is still the copy".
        shutil.copy(src, out)
        return src, out, _invoke("add", "bbox-metadata", out)
    if name == "check_bbox_fix":
        return src, out, _invoke("check", "bbox", src, "--fix", "--fix-output", out)
    if name == "add_stac_metadata":
        collection = tmp_path / "collection.json"
        collection.write_text(json.dumps(COLLECTION))
        return src, out, _invoke("add", "stac-metadata", src, out, "--collection", collection)
    # `check all --fix` reaches this repair through _apply_covering_fix, after
    # fixes that would re-encode the fixture for reasons of their own; the
    # repair is called directly so the cell shows only the footer write.
    try:
        return src, out, fix_bboxless_cover(str(src), str(out))
    except Exception as exc:  # noqa: BLE001 - the raced cell asserts on it
        return src, out, exc


def _failed(result) -> BaseException | None:
    if isinstance(result, BaseException):
        return result
    if hasattr(result, "exit_code"):
        return result.exception if result.exit_code != 0 else None
    return None


def _sidecars(path: Path) -> dict[bytes, bytes]:
    return {k: v for k, v in footer_kv(path).items() if k not in _WRITTEN}


def _layout(path: Path) -> tuple[str, list[int]]:
    md = pq.read_metadata(str(path))
    return md.row_group(0).column(0).compression, [
        md.row_group(i).num_rows for i in range(md.num_row_groups)
    ]


def _plant(monkeypatch, refusal: type[FooterPatchUnsupported]) -> None:
    def refuse(*args, **kwargs):
        raise refusal("planted")

    monkeypatch.setattr(parquet_footer, "_patch_footer_file", refuse)


def _assert_wrote(name: str, src: Path, out: Path) -> None:
    key = WRITES[name]
    assert footer_kv(out).get(key) != footer_kv(src).get(key), f"{name} did not write {key!r}"


# ---------------------------------------------------------------------------
# The table
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", PATHS)
def test_a_patchable_footer_keeps_every_sidecar_key_and_the_input_pages(name, tmp_path):
    src, out, result = _run(name, tmp_path)
    assert _failed(result) is None, f"{name}: {_failed(result)!r}"

    _assert_wrote(name, src, out)
    assert _sidecars(out) == SIDECARS, sorted(footer_kv(out))
    assert _layout(out) == _layout(src), "a footer patch re-encoded the data"


@pytest.mark.parametrize("name", PATHS)
def test_an_unpatchable_footer_rewrites_through_the_funnel_keeping_the_same_keys(
    name, tmp_path, monkeypatch
):
    """Same surviving keys as the patch; the facade's ZSTD and row groups."""
    _plant(monkeypatch, FooterPatchUnsupported)
    src, out, result = _run(name, tmp_path)
    assert _failed(result) is None, f"{name}: {_failed(result)!r}"

    _assert_wrote(name, src, out)
    assert _sidecars(out) == SIDECARS, sorted(footer_kv(out))
    expected_groups = -(-N_ROWS // DEFAULT_ROW_GROUP_ROWS)
    assert _layout(out) == ("ZSTD", [N_ROWS] * expected_groups), _layout(out)
    assert pq.read_table(str(out)).num_rows == N_ROWS


@pytest.mark.parametrize("name", PATHS)
def test_a_raced_patch_is_never_answered_with_a_rewrite(name, tmp_path, monkeypatch):
    """The input changed under the copy; a rewrite would stamp another writer's rows."""
    _plant(monkeypatch, FooterPatchRaced)
    src, out, result = _run(name, tmp_path)

    error = _failed(result)
    assert error is not None, f"{name} answered a race with a rewrite"
    if isinstance(error, SystemExit):
        # The CLI's handler reports the error and exits; the race's own message
        # reaches the user, not a fallback's or a refusal's.
        assert "planted" in result.output, result.output
        assert "Falling back" not in result.output, result.output
    else:
        assert isinstance(error, FooterPatchRaced), f"{name}: {error!r}"
    if name == "add_bbox_metadata":
        assert out.read_bytes() == src.read_bytes(), f"{name} rewrote {out} after a race"
    else:
        assert not out.exists(), f"{name} wrote {out} after a race"


def test_the_surviving_keys_are_the_funnels_own(tmp_path):
    """The funnel's ordinary rewrite carries the same sidecar set the table pins."""
    src = _source(tmp_path / "source.parquet")
    out = tmp_path / "extract.parquet"
    result = _invoke("extract", src, out)
    assert result.exit_code == 0, result.output
    assert _sidecars(out) == SIDECARS


@pytest.mark.skipif(
    os.name == "nt", reason="Windows chmod does not implement POSIX permission bits"
)
def test_a_rewrite_into_an_existing_destination_keeps_its_mode(tmp_path, monkeypatch):
    """The rewrite replaces the destination in place; a 0640 file stays 0640.

    ``check``'s covering repair kept the mode (``shutil.copymode``) and
    ``add bbox-metadata``'s fallback published the funnel's 0600 staging file.
    """
    _plant(monkeypatch, FooterPatchUnsupported)
    src = _source(tmp_path / "source.parquet")
    out = tmp_path / "out.parquet"
    shutil.copy(src, out)
    out.chmod(0o640)

    result = _invoke("add", "bbox-metadata", out)

    assert result.exit_code == 0, result.output
    assert (out.stat().st_mode & 0o777) == 0o640


# ---------------------------------------------------------------------------
# The owner's own answers
# ---------------------------------------------------------------------------


def _recording_rewrite(calls: list):
    def rewrite(source, destination, updates):
        calls.append((source, destination, dict(updates)))

    return rewrite


def test_a_dropped_retype_is_named_in_the_owners_warning(tmp_path, monkeypatch, caplog):
    """A rewrite cannot retype a column; the owner says so, no caller has to."""
    _plant(monkeypatch, FooterPatchUnsupported)
    src = _source(tmp_path / "source.parquet")
    calls: list = []

    retyped = parquet_footer.patch_or_rewrite(
        str(src),
        {"geo": "{}"},
        rewrite=_recording_rewrite(calls),
        arrow_edges={"geometry": "spherical"},
    )

    assert retyped == []
    assert calls == [(str(src), str(src), {"geo": "{}"})]
    assert "Edge columns stay planar" in caplog.text, caplog.text


def test_a_remote_path_is_rewritten_without_trying_the_patch(tmp_path, monkeypatch, caplog):
    """The patcher reads a local file; a remote one goes to the rewrite, unwarned."""
    _plant(monkeypatch, FooterPatchRaced)  # reached only if the patch is tried
    calls: list = []

    parquet_footer.patch_or_rewrite(
        "s3://bucket/a.parquet", {"geo": "{}"}, rewrite=_recording_rewrite(calls)
    )

    assert calls == [("s3://bucket/a.parquet", "s3://bucket/a.parquet", {"geo": "{}"})]
    assert "Falling back" not in caplog.text


# ---------------------------------------------------------------------------
# The two callers that re-encoded every file without trying the patch
# ---------------------------------------------------------------------------


def _pages(path: Path) -> bytes:
    """Every byte below the footer: what a footer patch copies verbatim."""
    raw = path.read_bytes()
    return raw[: len(raw) - 8 - int.from_bytes(raw[-8:-4], "little")]


def test_the_vecorel_key_is_patched_onto_the_metrics_output(tmp_path, monkeypatch):
    """``add geometry-metrics`` writes once, then adds ``collection`` by patch."""
    from geoparquet_io.core.add import geometry_metrics
    from geoparquet_io.core.parquet_writer import WriteOptions

    out = tmp_path / "metrics.parquet"
    write_with_geo(out, points_table(50), geo_block(), compression="gzip")
    before = _pages(out)

    geometry_metrics._add_vecorel_metadata_to_file(
        str(out), False, WriteOptions(compression="GZIP")
    )

    assert _pages(out) == before, "the Vecorel key re-encoded the command's output"
    assert b"collection" in footer_kv(out)


def test_the_vecorel_fallback_keeps_the_commands_codec(tmp_path, monkeypatch):
    """#1335 through the owner: a GZIP output is not re-encoded as ZSTD."""
    from geoparquet_io.core.add import geometry_metrics
    from geoparquet_io.core.parquet_writer import WriteOptions

    _plant(monkeypatch, FooterPatchUnsupported)
    out = tmp_path / "metrics.parquet"
    write_with_geo(out, points_table(50), geo_block(), compression="gzip")

    geometry_metrics._add_vecorel_metadata_to_file(
        str(out), False, WriteOptions(compression="GZIP")
    )

    assert _layout(out)[0] == "GZIP", _layout(out)
    assert b"collection" in footer_kv(out)


@pytest.mark.parametrize(
    ("options", "groups"),
    [({"row_group_rows": 10}, [10] * 5), ({"compression": "GZIP"}, [50])],
    ids=["row-group-rows", "codec"],
)
def test_the_vecorel_fallback_takes_every_write_fact_of_the_command(
    tmp_path, monkeypatch, options, groups
):
    """The fallback is handed the command's WriteOptions, not just its codec:
    ``--row-group-size`` was ignored there."""
    from geoparquet_io.core.add import geometry_metrics
    from geoparquet_io.core.parquet_writer import WriteOptions

    _plant(monkeypatch, FooterPatchUnsupported)
    out = tmp_path / "metrics.parquet"
    write_with_geo(out, points_table(50), geo_block(), compression="snappy")

    geometry_metrics._add_vecorel_metadata_to_file(str(out), False, WriteOptions(**options))

    codec, landed = _layout(out)
    assert landed == groups, landed
    assert codec == options.get("compression", "ZSTD"), codec


def test_the_assume_crs84_copy_is_a_footer_patch(tmp_path):
    """``reproject --assume-crs84``'s sanitized copy keeps the input's pages."""
    from geoparquet_io.core.reproject import _sanitize_null_crs_file

    src = write_with_geo(
        tmp_path / "null_crs.parquet",
        points_table(50),
        geo_block(crs=None),
        extra_kv=dict(SIDECARS),
        # A level Parquet does not record: only a patch can keep it.
        compression="zstd",
        compression_level=19,
    )

    with contextlib.ExitStack() as scratch:
        temp_path = _sanitize_null_crs_file(str(src), scratch, verbose=False)
        assert _pages(Path(temp_path)) == _pages(src), "the sanitized copy re-encoded the data"
        assert "crs" not in json.loads(footer_kv(temp_path)[b"geo"])["columns"]["geometry"]
        assert _sidecars(Path(temp_path)) == SIDECARS
    assert not Path(temp_path).exists(), "the scratch copy outlived its scope"


def test_an_mb_target_is_measured_on_the_facades_sample_not_the_first_row_group(tmp_path):
    """The rewrite asks ``resolve_row_group_rows_for_sample`` over its leading
    rows, as every streamed write does: the answer is the same whatever row
    groups the input happened to have. It used to measure the whole first row
    group, so a file whose first group was 500 small rows got a very different
    count from the same rows in one big group."""
    import pyarrow as pa

    from geoparquet_io.core.parquet_writer import WriteOptions
    from geoparquet_io.core.write_funnels import rewrite_footer_kv

    n = 60_000
    payload = ["x" if i < 500 else "y" * 200 for i in range(n)]
    table = pa.table({"id": list(range(n)), "payload": payload})
    one_group, small_first = tmp_path / "one_group.parquet", tmp_path / "small_first.parquet"
    pq.write_table(table, str(one_group), row_group_size=n)
    pq.write_table(table, str(small_first), row_group_size=500)

    write = WriteOptions(row_group_size_mb=1)
    groups = {}
    for path in (one_group, small_first):
        rewrite_footer_kv(str(path), str(path), {"k": "v"}, write=write)
        md = pq.read_metadata(str(path))
        groups[path.name] = [md.row_group(i).num_rows for i in range(md.num_row_groups)]
        assert md.num_rows == n

    assert groups["one_group.parquet"] == groups["small_first.parquet"], groups


def test_both_streamed_writers_size_a_byte_target_through_one_helper(tmp_path):
    """The footer rewrite and the streaming simplify writer ask the facade's one
    buffering helper (``parquet_writer.sized_stream``), so the same rows and
    target get the same row-group answer on both."""
    import pyarrow as pa

    from geoparquet_io.core import parquet_writer, write_funnels
    from geoparquet_io.core.parquet_writer import WriteOptions, sized_stream
    from geoparquet_io.core.process import simplify

    assert simplify.sized_stream is parquet_writer.sized_stream
    assert write_funnels.sized_stream is parquet_writer.sized_stream

    n = 30_000
    table = pa.table({"id": list(range(n)), "payload": ["y" * 50] * n})
    src = tmp_path / "src.parquet"
    pq.write_table(table, str(src), row_group_size=1_000)
    path = tmp_path / "in.parquet"
    pq.write_table(table, str(path), row_group_size=1_000)
    write_funnels.rewrite_footer_kv(
        str(path), str(path), {"k": "v"}, write=WriteOptions(row_group_size_mb=1)
    )
    landed = pq.read_metadata(str(path)).row_group(0).num_rows

    # simplify streams tables, the rewrite record batches; fed the same rows in
    # the same chunks, the one helper answers the same.
    def read():
        return pq.ParquetFile(str(src)).iter_batches()

    _, from_batches = sized_stream(read(), None, 1, table.schema)
    _, from_tables = sized_stream(
        (pa.Table.from_batches([b]) for b in read()), None, 1, table.schema
    )
    assert landed == from_tables == from_batches, (landed, from_tables, from_batches)


def test_only_the_facade_buffers_a_stream_for_a_row_group_answer():
    """``resolve_row_group_rows_for_sample(..., final=False)`` is the buffering
    loop's question; outside ``parquet_writer`` it means a second copy of the
    loop (#1367 T2d: one lived in the footer rewrite, one in simplify)."""
    import ast

    root = Path(__file__).resolve().parents[1] / "geoparquet_io"
    owner = root / "core" / "parquet_writer.py"
    copies = []
    for path in root.rglob("*.py"):
        if path == owner:
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if (
                isinstance(node, ast.Call)
                and getattr(node.func, "id", getattr(node.func, "attr", None))
                == "resolve_row_group_rows_for_sample"
                and any(
                    kw.arg == "final"
                    and isinstance(kw.value, ast.Constant)
                    and kw.value.value is False
                    for kw in node.keywords
                )
            ):
                copies.append(f"{path.relative_to(root)}:{node.lineno}")
    assert copies == [], copies
