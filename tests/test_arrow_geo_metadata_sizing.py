"""The row-group sizing arithmetic in ``core/arrow_geo_metadata.py``.

``_estimate_row_size`` has three answers -- pyarrow's buffer total, ``nbytes``,
or a default -- and ``_write_table_with_settings`` turns a ``--row-group-size-mb``
into a row count with it. The fallbacks were never exercised: a real
``pyarrow.Table`` always has ``get_total_buffer_size``, so the ``nbytes`` and
default branches only run for something that is not one. These pin them with
stand-ins, and pin the MB-to-rows conversion on a real table.
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.arrow_geo_metadata import _estimate_row_size, _write_table_with_settings


class _NbytesOnly:
    """A table-like object with ``nbytes`` and no ``get_total_buffer_size``."""

    num_rows = 4
    nbytes = 400


class _BufferSizeRaises:
    """``get_total_buffer_size`` blows up; ``nbytes`` answers."""

    num_rows = 2
    nbytes = 50

    def get_total_buffer_size(self):
        raise RuntimeError("no buffers")


class _NothingAnswers:
    """Neither accessor gives a usable number.

    ``nbytes`` is zero rather than raising: ``hasattr`` evaluates the attribute
    and only swallows ``AttributeError``, so a raising ``nbytes`` never reaches
    the ``try`` -- it propagates out of the ``hasattr`` guard first.
    """

    num_rows = 3
    nbytes = 0

    def get_total_buffer_size(self):
        return 0


class _Empty:
    """Zero rows: the divisor is clamped to one, not zero."""

    num_rows = 0

    def get_total_buffer_size(self):
        return 700


def test_a_real_table_is_sized_from_its_buffers():
    table = pa.table({"a": pa.array([1, 2, 3, 4], type=pa.int64())})
    assert _estimate_row_size(table) == table.get_total_buffer_size() // 4


def test_nbytes_is_the_fallback_when_there_is_no_buffer_total():
    assert _estimate_row_size(_NbytesOnly()) == 100


def test_a_failing_buffer_total_falls_through_to_nbytes():
    assert _estimate_row_size(_BufferSizeRaises()) == 25


def test_the_default_when_nothing_answers():
    assert _estimate_row_size(_NothingAnswers()) == 100


def test_zero_rows_does_not_divide_by_zero():
    assert _estimate_row_size(_Empty()) == 700


def test_row_group_size_mb_becomes_a_row_count(tmp_path):
    """One MB over ~8-byte rows is far more rows than the table has: clamp to num_rows."""
    table = pa.table({"a": pa.array(range(1000), type=pa.int64())})
    out = tmp_path / "sized.parquet"

    _write_table_with_settings(
        table,
        str(out),
        compression="ZSTD",
        compression_level=None,
        row_group_rows=None,
        row_group_size_mb=1,
        geoparquet_version="2.0",
        geometry_column="geometry",
    )

    assert pq.ParquetFile(str(out)).metadata.num_row_groups == 1


def test_a_small_row_group_size_mb_splits_the_table(tmp_path):
    """Rows of ~8 bytes at a target far below the table's size give several groups."""
    table = pa.table({"a": pa.array(range(200_000), type=pa.int64())})
    out = tmp_path / "split.parquet"

    _write_table_with_settings(
        table,
        str(out),
        compression="ZSTD",
        compression_level=None,
        row_group_rows=None,
        row_group_size_mb=1,
        geoparquet_version="2.0",
        geometry_column="geometry",
        verbose=True,
    )

    assert pq.ParquetFile(str(out)).metadata.num_row_groups > 1
