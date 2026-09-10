"""
Parquet writing utilities for GeoParquet files.

This module provides functions and classes for writing Parquet files
with GeoParquet metadata and optimal settings.
"""

import math
from dataclasses import dataclass

from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.logging_config import info

# The unit DuckDB's Parquet writer emits row groups in.
#
# A ``ROW_GROUP_SIZE`` request is not honoured literally: the writer flushes
# whole vectors, and it rounds the request *up* to a multiple of this -- a
# strict ceiling, never a nearest. Measured, not assumed
# (tests/test_sort_row_group_default.py writes files and reads the footers
# back): a request of 3,000 rows lands at 4,096 rather than the nearer 2,048,
# 9,000 lands at 10,240 rather than the nearer 8,192, 50,000 lands at 51,200,
# and 100,000 at 100,352. The same suite asserts this equals DuckDB's own
# published ``duckdb.__standard_vector_size__``.
WRITER_VECTOR_ROWS = 2_048

# The top of the spatial row-count band gpio's own advice quotes: the
# 10,000-50,000 rows per group that ``gpio check`` prints and that
# ``check_optimization`` scores.
#
# This is a copy of ``check_parquet_structure.SPATIAL_ROW_COUNT_RANGE[1]``, and
# a deliberate one: that module imports this one for
# DEFAULT_SORT_ROW_GROUP_ROWS, so importing the band back the other way is an
# import cycle (``lint-imports`` would refuse it).
# ``TestWriterVectorAlignment.test_the_local_band_top_matches_the_band_check_uses``
# asserts the two agree, so the copy cannot drift.
SPATIAL_BAND_TOP_ROWS = 50_000


def align_to_writer_vector(rows: int, band_top: int = SPATIAL_BAND_TOP_ROWS) -> int:
    """Snap a row-group row count to a whole writer vector, as the writer would.

    The writer quantises the request anyway; doing it here means gpio knows
    what will land, and can say so. The rule is therefore the writer's own --
    round *up* to a whole vector -- with a single exception, so that gpio's own
    advice does not contradict itself:

    * A request whose ceiling stays at or below ``band_top`` gets that ceiling,
      which is exactly the file the writer would have produced unaided.
    * A request that is itself inside the band but whose ceiling would leave it
      (anything from 49,153 to 50,000) snaps *down* to 49,152 instead -- the
      largest whole vector the band allows. This is #961: ``gpio sort`` asked
      for the band's top of 50,000, the writer wrote 51,200, and
      ``gpio check optimization`` then failed the file gpio had just written.

    Rounding to the *nearest* vector was tried and reverted. Nearest differs
    from ceiling wherever the fractional part is under a half, and in that
    window it rounds down: ``--row-group-size 9000`` became 8,192, below the
    band's 10,000-row floor, which is the same defect moved to the other end of
    the band.

    The residual behaviour, stated out loud: 49,153-50,000 is the only input
    range where gpio writes *fewer* rows than were asked for, and above the
    band gpio rounds up just like the writer, so ``--row-group-size 100000``
    yields 100,352 rather than being dragged back into the band. The result is
    monotonic and never below one vector -- the writer's own minimum row-group
    size.
    """
    ceiling = max(1, math.ceil(rows / WRITER_VECTOR_ROWS)) * WRITER_VECTOR_ROWS
    if rows <= band_top < ceiling:
        return max(WRITER_VECTOR_ROWS, ceiling - WRITER_VECTOR_ROWS)
    return ceiling


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
# number that lands. It is derived rather than typed, so the default is by
# construction whatever the alignment rule makes of the band's top.
DEFAULT_SORT_ROW_GROUP_ROWS = align_to_writer_vector(SPATIAL_BAND_TOP_ROWS)  # 49,152


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

    An explicit row count is snapped to a whole writer vector, because the
    writer would otherwise round it up itself and land somewhere the caller did
    not ask for (#961). The adjustment is announced rather than made silently:
    a request that is already a whole number of vectors passes through without
    a word, and one that is not gets a one-line note naming both the value
    asked for and the value that will be written.

    A row count below 1 is rejected, not clamped. Every sort subcommand routes
    through here, so this is the one place that can make the four of them agree
    about it: ``sort str`` raised on ``--row-group-size -5`` while the other
    three quietly wrote 2,048-row groups.
    """
    if row_group_rows is not None and row_group_rows < 1:
        raise InvalidParameterError("--row-group-size", "must be at least 1")
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
