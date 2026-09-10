"""
Parquet writing utilities for GeoParquet files.

This module provides functions and classes for writing Parquet files
with GeoParquet metadata and optimal settings.
"""

from dataclasses import dataclass

from geoparquet_io.core.logging_config import info

# The unit DuckDB's Parquet writer emits row groups in.
#
# A ``ROW_GROUP_SIZE`` request is not honoured literally: the writer flushes
# whole vectors, and it rounds the request *up* to a multiple of this. Measured,
# not assumed (tests/test_sort_row_group_default.py writes files and reads the
# footers back): a request of 3,000 rows lands at 4,096 rather than the nearer
# 2,048, 50,000 lands at 51,200, and 100,000 at 100,352.
WRITER_VECTOR_ROWS = 2_048

# The default row-group size the ``gpio sort`` commands write when neither
# --row-group-size nor --row-group-size-mb is given (#775).
#
# Sorting exists to make spatial filters prune row groups, and gpio's own
# advice for that workload -- printed by ``gpio check`` and repeated in the
# guide -- is 10,000-50,000 rows per group. This is the top of that band as the
# writer can actually express it: 24 whole vectors, the largest groups the band
# allows, so bounding boxes stay tight enough to prune without multiplying
# per-group footer overhead. It is deliberately *not* the general write default:
# only the sort commands are sized for spatial pruning.
#
# It was 50,000 until #961. That is the band's literal top, but no file can have
# it: the writer rounded the request up to 51,200, so ``gpio sort`` produced
# files that ``gpio check optimization`` scored ``[fail]`` on its row-group
# factor and told the user to re-partition. 49,152 is a whole number of vectors,
# so the writer passes it through unchanged and the number gpio asks for is the
# number that lands.
DEFAULT_SORT_ROW_GROUP_ROWS = 24 * WRITER_VECTOR_ROWS  # 49,152


def align_to_writer_vector(rows: int) -> int:
    """Snap a row-group row count to the nearest whole writer vector.

    The writer will quantise the request anyway; doing it here means gpio knows
    what will land, and can say so. *Nearest* rather than down: rounding every
    request down would keep the top of gpio's 10,000-50,000 spatial band inside
    the band but push the bottom out of it, since a user typing the band's own
    floor (``--row-group-size 10000``) would get 8,192-row groups and the same
    complaint from ``gpio check`` one end further along. Nearest is monotonic
    and maps both endpoints of that band inside it (10,240 and 49,152), so
    every request between them lands inside it too.

    Ties round up, and the result is never below one vector -- the writer's own
    minimum row-group size.
    """
    vectors = max(1, (rows + WRITER_VECTOR_ROWS // 2) // WRITER_VECTOR_ROWS)
    return vectors * WRITER_VECTOR_ROWS


def resolve_sort_row_group_rows(
    row_group_rows: int | None,
    row_group_size_mb: float | None,
) -> int | None:
    """Apply the sort commands' row-group default, aligned to the writer's vector.

    An explicit row count wins, and an explicit ``--row-group-size-mb`` target
    is left alone (it sizes groups by bytes, and forcing a row count here would
    override the option the user actually passed). Only when neither is given
    does the sort default apply -- previously that case fell through as ``None``
    and the writer's own default (DuckDB's 122,880 rows) silently applied.

    An explicit row count is snapped to the nearest whole writer vector, because
    the writer would otherwise round it up itself and land somewhere the caller
    did not ask for (#961). The adjustment is announced rather than made
    silently: a request that is already a whole number of vectors passes through
    without a word, and one that is not gets a one-line note naming both the
    value asked for and the value that will be written.
    """
    if row_group_rows is None and row_group_size_mb is None:
        return DEFAULT_SORT_ROW_GROUP_ROWS
    if row_group_rows is None:
        return None

    aligned = align_to_writer_vector(row_group_rows)
    if aligned != row_group_rows:
        info(
            f"Writing {aligned:,} rows per row group rather than the requested "
            f"{row_group_rows:,}: the Parquet writer emits row groups in whole "
            f"{WRITER_VECTOR_ROWS:,}-row vectors, so {row_group_rows:,} is not a "
            "size a row group can have."
        )
    return aligned


@dataclass
class ParquetWriteSettings:
    """
    Central configuration for Parquet write best practices.
    Single source of truth for compression, row groups, and other settings.
    """

    compression: str = "ZSTD"
    compression_level: int = 15
    row_group_rows: int | None = None
    row_group_size_mb: int | None = None

    # Best practice constants
    DEFAULT_COMPRESSION = "ZSTD"
    DEFAULT_COMPRESSION_LEVEL = 15
    DEFAULT_ROW_GROUP_ROWS = 100_000
    DEFAULT_PARQUET_VERSION = "2.6"

    def get_pyarrow_kwargs(self, calculated_row_group_size: int | None = None) -> dict:
        """Get kwargs dict for PyArrow write_table()."""
        pa_compression = self.compression if self.compression != "UNCOMPRESSED" else None
        pa_compression_level = (
            self.compression_level if self.compression in ["GZIP", "ZSTD", "BROTLI"] else None
        )

        row_group_size = (
            calculated_row_group_size or self.row_group_rows or self.DEFAULT_ROW_GROUP_ROWS
        )

        kwargs = {
            "row_group_size": row_group_size,
            "compression": pa_compression,
            "write_statistics": True,
            "use_dictionary": True,
            "version": self.DEFAULT_PARQUET_VERSION,
        }

        if pa_compression_level is not None:
            kwargs["compression_level"] = pa_compression_level

        return kwargs
