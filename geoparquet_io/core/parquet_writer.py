"""
Parquet writing utilities for GeoParquet files.

This module provides functions and classes for writing Parquet files
with GeoParquet metadata and optimal settings.
"""

from dataclasses import dataclass

# The default row-group size the ``gpio sort`` commands write when neither
# --row-group-size nor --row-group-size-mb is given (#775).
#
# Sorting exists to make spatial filters prune row groups, and gpio's own
# advice for that workload -- printed by ``gpio check`` and repeated in the
# guide -- is 10,000-50,000 rows per group. This asks for the top of that band:
# the largest groups the band allows, so bounding boxes stay tight enough to
# prune without multiplying per-group footer overhead. It is deliberately *not*
# the general write default: only the sort commands are sized for spatial
# pruning.
#
# Note this is the value REQUESTED, not the one written. DuckDB rounds a
# row-group request up to a multiple of its 2,048-row vector size, so a sorted
# file actually carries 51,200 rows per group -- outside the very band this
# default aims at, which is why ``gpio check optimization`` can fail a file
# ``gpio sort`` just wrote (#961).
DEFAULT_SORT_ROW_GROUP_ROWS = 50_000


def resolve_sort_row_group_rows(
    row_group_rows: int | None,
    row_group_size_mb: float | None,
) -> int | None:
    """Apply the sort commands' row-group default when no sizing was requested.

    An explicit row count wins, and an explicit ``--row-group-size-mb`` target
    is left alone (it sizes groups by bytes, and forcing a row count here would
    override the option the user actually passed). Only when neither is given
    does the sort default apply -- previously that case fell through as ``None``
    and the writer's own default (DuckDB's 122,880 rows) silently applied.
    """
    if row_group_rows is None and row_group_size_mb is None:
        return DEFAULT_SORT_ROW_GROUP_ROWS
    return row_group_rows


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
