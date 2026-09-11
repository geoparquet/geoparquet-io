"""The write facade: the decisions every gpio write shares, owned in one place.

gpio has many write *paths* -- a DuckDB ``COPY`` behind
``common.write_parquet_with_metadata``, four Arrow/DuckDB strategies behind
``Table.write()``, a partition staging rewrite -- and for a long time each one
decided the same handful of facts about its output independently. They
disagreed, and the disagreements shipped one bug at a time: ``gpio sort``
writing 50,000-row groups that landed at 51,200 (#961), ``check --fix`` writing
100,000 (#974), ``Table.write(row_group_rows=50000)`` writing 51,200 while the
identical CLI request wrote 49,152 (#971), ``convert geoparquet`` documenting
100k row groups while nothing set any (#981), and every auto-version path but
``convert`` downgrading a native-geo-only input to 1.1 WKB (#600).

This module is the single owner of those shared decisions. It owns exactly
three, because each one is a fact about the *output file* that no individual
write path is entitled to answer for itself:

1. **How many rows go in a row group** -- :func:`resolve_row_group_rows`, and
   the constants it derives the default from.
2. **Which GeoParquet version the output declares** when the caller did not ask
   for one -- :func:`resolve_output_geoparquet_version`.
3. **Whether a ``geo`` key appears in the output's file-level metadata at all**
   -- :func:`apply_output_kv_metadata`.

It deliberately does **not** own the rest of the write. Compression validation
(``common.validate_compression_settings``), the contents of the ``geo`` block
(``write_strategies.base.build_geo_metadata``), CRS resolution
(``crs_utils.apply_output_crs``), bbox coverings and geometry-type computation
all already have a single owner each, and pulling them in here would turn a
facade into a rewrite. The line is: this module decides what the paths were
*disagreeing* about; the existing owners keep what they already own.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.logging_config import info

if TYPE_CHECKING:
    import pyarrow as pa

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
# DEFAULT_ROW_GROUP_ROWS, so importing the band back the other way is an
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


# The row-group size every gpio write uses when neither --row-group-size nor
# --row-group-size-mb is given (#775, #971, #981).
#
# gpio's own advice -- printed by ``gpio check``, scored by
# ``check_optimization``, repeated in the guide -- is 10,000-50,000 rows per
# group for spatial workloads. This is the top of that band as the writer can
# actually express it: 24 whole vectors, the largest groups the band allows, so
# bounding boxes stay tight enough to prune without multiplying per-group footer
# overhead.
#
# It was 50,000 until #961. That is the band's literal top, but no file can have
# it: the writer rounded the request up to 51,200, so ``gpio sort`` produced
# files that ``gpio check optimization`` scored ``[fail]`` on its row-group
# factor and told the user to re-partition. 49,152 is a whole number of vectors,
# so the writer passes it through unchanged and the number gpio asks for is the
# number that lands. It is derived rather than typed, so the default is by
# construction whatever the alignment rule makes of the band's top.
#
# Until this module became the facade it was named DEFAULT_SORT_ROW_GROUP_ROWS
# and applied to ``gpio sort`` alone, on the reasoning that only sorted files are
# read by spatial predicates. Measured, that reasoning did not survive: every
# other command fell through to whatever its writer happened to default to --
# DuckDB's 122,880 on the COPY paths (#981), pyarrow's 100,000 on the Arrow ones
# -- so gpio shipped three different answers and ``gpio check optimization``
# failed files gpio had just written (#972). One number, one owner.
DEFAULT_ROW_GROUP_ROWS = align_to_writer_vector(SPATIAL_BAND_TOP_ROWS)  # 49,152


def resolve_row_group_rows(
    row_group_rows: int | None,
    row_group_size_mb: float | None,
    param_name: str = "--row-group-size",
) -> int | None:
    """Decide how many rows a row group gets. The facade's first decision.

    An explicit row count wins, and an explicit ``--row-group-size-mb`` target
    is left alone (it sizes groups by bytes, and forcing a row count here would
    override the option the user actually passed). Only when neither is given
    does :data:`DEFAULT_ROW_GROUP_ROWS` apply -- previously that case fell
    through as ``None`` and whichever writer the path happened to reach picked
    for itself.

    An explicit row count is snapped to a whole writer vector, because the
    writer would otherwise round it up itself and land somewhere the caller did
    not ask for (#961). The adjustment is announced rather than made silently:
    a request that is already a whole number of vectors passes through without
    a word, and one that is not gets a one-line note naming both the value
    asked for and the value that will be written. Calling this twice on one
    write is therefore harmless and silent -- the sort commands resolve early so
    they can reject a bad value before doing any work, and the write funnel
    resolves again for the paths that do not.

    A request of one whole vector or less is passed through untouched, because
    below that threshold alignment can only do harm. Measured on both writers
    with a 120,000-row input: DuckDB's ``COPY`` quantises a sub-vector request
    up to 2,048 whatever gpio does (1, 10, 100, 1,000 and 2,047 all produce
    2,048-row groups), so rounding first changes nothing there -- while
    ``pq.write_table`` honours the request exactly (10 gives 12,000 groups of
    10). Rounding a sub-vector request would therefore be gpio changing the
    output on the only writer that could have obeyed it, which is the inverse of
    what the alignment is for. ``align_to_writer_vector`` itself keeps its
    ceiling-everywhere rule: it answers "what would the DuckDB writer do", and
    that answer is still 2,048.

    A row count below 1 is rejected, not clamped: ``sort str`` raised on
    ``--row-group-size -5`` while the other three quietly wrote 2,048-row groups.
    ``param_name`` is what that rejection blames. One function now serves both
    front ends, so the default names the CLI flag and ``Table.write`` passes its
    own keyword instead -- a Python caller never typed ``--row-group-size`` and
    should not be sent looking for it in their code.
    """
    if row_group_rows is not None and row_group_rows < 1:
        raise InvalidParameterError(param_name, "must be at least 1")
    if row_group_rows is None and row_group_size_mb is None:
        return DEFAULT_ROW_GROUP_ROWS
    if row_group_rows is None:
        return None
    if row_group_rows <= WRITER_VECTOR_ROWS:
        return row_group_rows

    aligned = align_to_writer_vector(row_group_rows)
    if aligned != row_group_rows:
        info(
            f"Writing {aligned:,} rows per row group rather than the requested "
            f"{row_group_rows:,}: the Parquet writer emits row groups in whole "
            f"{WRITER_VECTOR_ROWS:,}-row vectors, so {row_group_rows:,} is not a "
            "size a row group can have."
        )
    return aligned


def resolve_output_geoparquet_version(
    requested: str | None,
    *,
    input_file: str | None = None,
    original_metadata: dict | None = None,
    verbose: bool = False,
) -> str | None:
    """Decide which GeoParquet version the output declares. The second decision.

    An explicit request always wins. Otherwise this is auto mode, which the
    shared ``--geoparquet-version`` help documents as "preserve the input's
    version" -- and for a *native-geo-only* input (a Parquet GEOMETRY logical
    type and no ``geo`` key) preserving it means writing native 2.0, which is
    what ``convert`` and ``reproject`` already did via
    ``resolve_geoparquet_version_from_file``.

    Every other entry point resolved auto mode from the carried KV metadata
    alone, and a native-geo-only input has no ``geo`` key for that to read: it
    returned ``None``, the write defaulted to 1.1, and the geometry column was
    rewritten as plain WKB with its native logical type stripped (#600). The
    file is the better witness than its ``geo`` key precisely because the
    interesting case is the one with no ``geo`` key, so the file is consulted
    first and the metadata is the fallback for callers that have no path to
    offer (an in-memory query, a remote input that cannot be inspected).

    ``input_file`` must be the file whose *rows* the write will read, or a file
    that is lossless with respect to it. Handing this the user's input while
    reading the data from a scratch rewrite that dropped the native type and
    the CRS produces the worst of both: a native 2.0 output declaring the
    default CRS over projected data, which reads as an assertion rather than an
    omission and which ``gpio check spec`` then blesses. ``gpio sort quadkey``
    does exactly that today, and it is why the index-adding ``gpio partition``
    drivers were left resolving from their scratch file rather than "fixed" the
    same way. See ``partition/staging.py`` and the strict xfails in
    ``tests/test_write_facade_version_owner.py``.

    Returns ``None`` when nothing can be detected, leaving the caller's own
    default in charge.
    """
    if requested is not None:
        return requested

    from geoparquet_io.core.common import (
        _resolve_auto_version,
        resolve_geoparquet_version_from_file,
    )
    from geoparquet_io.core.streaming import extract_version_from_metadata

    if input_file:
        detected = resolve_geoparquet_version_from_file(input_file, verbose)
        if detected:
            return detected

    return _resolve_auto_version(extract_version_from_metadata(original_metadata))


def apply_output_kv_metadata(
    table: pa.Table,
    geoparquet_version: str | None,
    extra_kv_metadata: dict[str, str] | None = None,
) -> pa.Table:
    """Decide the output's file-level KV metadata. The third decision.

    Two rules, and the version decides both:

    * ``parquet-geo-only`` means "carry no GeoParquet metadata", so the ``geo``
      key is dropped. Every other key is not GeoParquet metadata and survives.
    * ``extra_kv_metadata`` -- the input's preserved sidecar payloads (fiboa,
      vecorel, STAC), plus anything the caller minted -- is merged in, caller
      last.

    The three Arrow strategies each carried their own copy of the merge half and
    none of them had the drop half: ``write_from_table`` guards its metadata
    work on the geometry column being present, so a table whose geometry had
    been projected away was written with ``table.schema`` verbatim -- carrying a
    ``geo`` key naming a ``primary_column`` the output does not have, and
    declaring a GeoParquet version for a file explicitly asked to declare none
    (#773). The same hole let a parquet-geo-only write *with* geometry keep the
    input's carried block on the streaming path.

    Applied once, at the top of each strategy's ``write_from_table``, before any
    early return can skip it.
    """
    metadata = dict(table.schema.metadata or {})

    if geoparquet_version == "parquet-geo-only":
        metadata.pop(b"geo", None)
        metadata.pop("geo", None)

    for key, value in (extra_kv_metadata or {}).items():
        bkey = key.encode("utf-8") if isinstance(key, str) else key
        bval = value.encode("utf-8") if isinstance(value, str) else value
        metadata[bkey] = bval

    return table.replace_schema_metadata(metadata)


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
    DEFAULT_PARQUET_VERSION = "2.6"

    def get_pyarrow_kwargs(self, calculated_row_group_size: int | None = None) -> dict:
        """Get kwargs dict for PyArrow write_table()."""
        pa_compression = self.compression if self.compression != "UNCOMPRESSED" else None
        pa_compression_level = (
            self.compression_level if self.compression in ["GZIP", "ZSTD", "BROTLI"] else None
        )

        # Last resort, not a second policy: a caller that reached here without
        # going through resolve_row_group_rows still lands on the one number
        # rather than the 100,000 this used to carry -- which was neither
        # DuckDB's 122,880 nor the band gpio's own check scores (#981).
        row_group_size = calculated_row_group_size or self.row_group_rows or DEFAULT_ROW_GROUP_ROWS

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
