"""One listing rule: dotfiles and dot-directories are not data (HANDOFFS row 74).

``file_utils.get_all_parquet_files`` skipped a macOS ``._x.parquet`` sidecar
and an orphaned ``.gpio-stage-*`` directory; the sub-partition size scan and
the partition size report globbed ``**/*.parquet`` themselves and counted both.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.file_utils import get_all_parquet_files
from geoparquet_io.core.partition.common import calculate_partition_stats
from geoparquet_io.core.sub_partition import find_large_files


def _tree(root: Path) -> Path:
    pq.write_table(pa.table({"x": list(range(1000))}), root / "a.parquet")
    (root / "._a.parquet").write_bytes(b"\0" * 4096)
    stage = root / ".gpio-stage-orphan"
    stage.mkdir()
    pq.write_table(pa.table({"x": list(range(1000))}), stage / "a.parquet")
    nested = root / "k=1"
    nested.mkdir()
    pq.write_table(pa.table({"x": [1]}), nested / "b.parquet")
    return root


def test_the_owner_lists_data_files_only(tmp_path):
    root = _tree(tmp_path)
    assert get_all_parquet_files(str(root)) == [
        str(root / "a.parquet"),
        str(root / "k=1" / "b.parquet"),
    ]
    assert get_all_parquet_files(str(root), recursive=False) == [str(root / "a.parquet")]


def test_the_large_file_scan_skips_dot_entries(tmp_path):
    root = _tree(tmp_path)
    assert find_large_files(str(root), 1) == [
        str(root / "a.parquet"),
        str(root / "k=1" / "b.parquet"),
    ]
    assert find_large_files(str(root), 1, recursive=False) == [str(root / "a.parquet")]


def test_the_partition_size_report_skips_dot_entries(tmp_path):
    root = _tree(tmp_path)
    data = (root / "a.parquet").stat().st_size + (root / "k=1" / "b.parquet").stat().st_size
    total_mb, _avg = calculate_partition_stats(str(root), 2)
    assert total_mb == data / (1024 * 1024)
