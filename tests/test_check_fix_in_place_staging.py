"""An in-place ``check --fix`` never asks DuckDB to write over the file it is reading.

``fix_row_groups`` and ``fix_bbox_removal`` handed ``output_file`` straight to
``write_parquet_with_metadata``. For an in-place fix that is the input path, so
DuckDB's ``COPY`` -- finding the destination already there -- writes
``<file>.tmp`` and renames it over the original while the original still has a
reader on it. POSIX allows that; Windows does not::

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
    src = Path("tests/data/fields_pgo_5070_snappy.parquet")
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


def test_a_fix_to_a_different_path_still_writes_there_directly(local_copy, tmp_path, monkeypatch):
    """Not in place: no staging, the output path is used as given."""
    seen: list[str] = []
    monkeypatch.setattr(check_fixes, "write_parquet_with_metadata", _recording_writer(seen))
    out = tmp_path / "elsewhere.parquet"

    check_fixes.fix_row_groups(str(local_copy), str(out))

    assert seen == [str(out)]
    assert out.exists()
