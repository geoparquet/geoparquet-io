"""One patch-else-rewrite decision for every metadata-only footer write (#1367 concept 2).

``parquet_footer.patch_or_rewrite`` tries the footer patch; a footer it cannot
patch falls back to the funnel's one metadata-only rewrite
(``write_funnels.rewrite_footer_kv``), and a race -- the input changed while
it was being copied -- is never answered with a rewrite. Four callers used to
hand-write that decision, each its own way: ``add bbox-metadata`` answered a
race with a rewrite and kept the input's codec, ``check``'s covering repair
re-encoded at the facade's, and ``add stac-metadata`` refused.

The table: every footer-writing command x {patchable, unpatchable, raced}, on
one fixture carrying sidecar keys. Whatever the cell, the sidecar keys that
survive are the same on every path, and only the key the command writes
changes. A patched output keeps the input's pages (SNAPPY, 100-row groups) by
construction; a rewrite takes the facade's codec and row-group count (the lead's
policy for #1367); ``add stac-metadata`` keeps its refusal, by explicit
argument; a race propagates and writes nothing.

The refusal is planted in ``parquet_footer.patch_footer``, the mechanism every
path reaches, rather than crafted into a file: what is under test is the
hand-off, not the refusal.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core import parquet_footer
from geoparquet_io.core.check_fixes import fix_bboxless_covering
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
        return src, out, fix_bboxless_covering(str(src), str(out))
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

    monkeypatch.setattr(parquet_footer, "patch_footer", refuse)


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


@pytest.mark.parametrize("name", [n for n in PATHS if n != "add_stac_metadata"])
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


def test_an_unpatchable_footer_is_refused_by_add_stac_metadata(tmp_path, monkeypatch):
    """The one caller whose policy is to stop: rewriting a file to add one key
    would cost its compression level. Passed to the owner as an argument."""
    _plant(monkeypatch, FooterPatchUnsupported)
    src, out, result = _run("add_stac_metadata", tmp_path)

    assert result.exit_code != 0
    assert "Cannot write the stac-geoparquet key" in result.output, result.output
    assert "gpio stops here instead" in result.output, result.output
    assert not out.exists()


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
        assert "Cannot write the" not in result.output, result.output
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
