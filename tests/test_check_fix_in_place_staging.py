"""An in-place ``check --fix`` never asks DuckDB to write over the file it is reading.

``fix_row_groups`` and ``fix_bbox_removal`` handed ``output_file`` straight to
``write_parquet_with_metadata``. For an in-place fix that is the input path, so
DuckDB's ``COPY`` -- finding the destination already there -- writes
``tmp_<basename>`` beside it and renames that over the original while the
original still has a reader on it. POSIX allows that; Windows does not::

    ✓ Created backup: ...\\pgo_tiny_groups.parquet.bak
    Error: IO Error: Could not move file: Access is denied.

Intermittent, because it depends on which reader the collector has released by
the time DuckDB renames -- which is why it slipped through #1009's own Windows
legs and then failed two of ``main``'s. ``fix_compression`` and
``fix_spatial_ordering`` already route an in-place rewrite through a staging
file beside the destination and ``os.replace`` it after the connection is
closed (#941, #959); these two now do the same.

The invariant is asserted directly -- the path DuckDB is asked to write is never
the path being read -- rather than the platform error, so it fails on every OS.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core import check_fixes


@pytest.fixture
def local_copy(tmp_path) -> Path:
    """A parquet-geo-only file *with* a ``bbox`` column, so both fixers have work.

    ``fix_bbox_removal`` really does ``SELECT * EXCLUDE (bbox)``; a fixture
    without the column would pass the recorder below and fail the real fixer.
    """
    src = Path("tests/data/fields_pgo_crs84_bbox_snappy.parquet")
    dst = tmp_path / "in_place.parquet"
    shutil.copy2(src, dst)
    return dst


def _recording_writer(seen: list[str]):
    """Stand in for ``write_parquet_with_metadata``: record where it was asked to write,
    and put a real Parquet file there so the move that follows has something to move."""

    def fake_write(con, query, output_file, **kwargs):
        seen.append(output_file)
        source = kwargs["input_file"]
        pq.write_table(pq.read_table(source), output_file, compression="zstd")

    return fake_write


@pytest.mark.parametrize(
    "fixer",
    [
        pytest.param(
            lambda path: check_fixes.fix_row_groups(str(path), str(path)),
            id="fix_row_groups",
        ),
        pytest.param(
            lambda path: check_fixes.fix_bbox_removal(str(path), str(path), "bbox"),
            id="fix_bbox_removal",
        ),
    ],
)
def test_an_in_place_fix_writes_beside_the_file_and_moves_it_into_place(
    fixer, local_copy, monkeypatch
):
    seen: list[str] = []
    monkeypatch.setattr(check_fixes, "write_parquet_with_metadata", _recording_writer(seen))
    before = sorted(p.name for p in local_copy.parent.iterdir())

    result = fixer(local_copy)

    assert result["success"] is True
    assert seen, "the fixer never wrote anything"
    written = Path(seen[0])
    assert written.resolve() != local_copy.resolve(), (
        "DuckDB was asked to write over the file it is reading -- on Windows that is "
        "`IO Error: Could not move file: Access is denied`"
    )
    assert written.parent == local_copy.parent, "staging must be beside the destination"
    assert not written.exists(), "the staging file must not be left behind"
    assert local_copy.exists() and pq.ParquetFile(str(local_copy)).metadata.num_rows > 0
    assert sorted(p.name for p in local_copy.parent.iterdir()) == before, (
        "an in-place fix leaves exactly the files it found"
    )


def test_the_real_fixers_work_in_place_end_to_end(local_copy):
    """No recorder: the two fixers, on a real file, leave one file and a result."""
    before = sorted(p.name for p in local_copy.parent.iterdir())

    check_fixes.fix_bbox_removal(str(local_copy), str(local_copy), "bbox")
    assert "bbox" not in pq.read_schema(str(local_copy)).names
    check_fixes.fix_row_groups(str(local_copy), str(local_copy))

    assert pq.ParquetFile(str(local_copy)).metadata.num_rows > 0
    assert sorted(p.name for p in local_copy.parent.iterdir()) == before


@pytest.mark.parametrize(
    "fixer",
    [
        pytest.param(
            lambda path: check_fixes.fix_row_groups(str(path), str(path)),
            id="fix_row_groups",
        ),
        pytest.param(
            lambda path: check_fixes.fix_bbox_removal(str(path), str(path), "bbox"),
            id="fix_bbox_removal",
        ),
    ],
)
def test_a_failed_write_leaves_neither_a_staging_file_nor_a_changed_original(
    fixer, local_copy, monkeypatch
):
    """A writer that dies after creating the staging file must not leave it.

    A dot-prefixed staging file is invisible to ``ls`` and to the ``*.parquet``
    globs that walk a partition directory, so a fix that keeps failing would
    accumulate multi-GB orphans nobody can see. The original is the good copy
    and is untouched.
    """
    before = {p.name: p.stat().st_size for p in local_copy.parent.iterdir()}

    def dying_write(con, query, output_file, **kwargs):
        Path(output_file).write_bytes(b"half a file")
        raise RuntimeError("the writer died mid-way")

    monkeypatch.setattr(check_fixes, "write_parquet_with_metadata", dying_write)

    with pytest.raises(RuntimeError, match="died"):
        fixer(local_copy)

    assert {p.name: p.stat().st_size for p in local_copy.parent.iterdir()} == before


def test_a_remote_in_place_fix_is_not_staged_locally(monkeypatch):
    """``check row-group s3://b/k.parquet --fix --overwrite`` worked on ``main``.

    The write facade stages a *remote* output itself and uploads it; there is no
    destination on this machine to rename over, so the local staging path is
    skipped and the facade is handed the remote URL as before. The first
    version of this fix routed the remote case through
    ``_move_temp_output_into_place``, whose remote branch calls
    ``remote_write_context`` with a signature it does not have.
    """
    seen: list[str] = []
    monkeypatch.setattr(check_fixes, "get_parquet_metadata", lambda *a, **k: ({}, None))
    monkeypatch.setattr(check_fixes, "setup_aws_profile_if_needed", lambda *a, **k: None)
    monkeypatch.setattr(check_fixes, "resolve_file_url", lambda path, verbose=False: path)
    monkeypatch.setattr(
        check_fixes,
        "write_parquet_with_metadata",
        lambda con, query, output_file, **kwargs: seen.append(output_file),
    )

    result = check_fixes.fix_row_groups("s3://bucket/key.parquet", "s3://bucket/key.parquet")

    assert result["success"] is True
    assert seen == ["s3://bucket/key.parquet"]


def test_a_fix_to_a_different_path_still_writes_there_directly(local_copy, tmp_path, monkeypatch):
    """Not in place: no staging, the output path is used as given."""
    seen: list[str] = []
    monkeypatch.setattr(check_fixes, "write_parquet_with_metadata", _recording_writer(seen))
    out = tmp_path / "elsewhere.parquet"

    check_fixes.fix_row_groups(str(local_copy), str(out))

    assert seen == [str(out)]
    assert out.exists()
