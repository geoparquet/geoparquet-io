"""Validating and normalizing the compression settings a write was asked for.

One codec whitelist and one level range per codec, shared by every write path.
This is deliberately *not* part of the write facade in ``core/parquet_writer.py``:
that module owns the decisions the write paths were disagreeing about, and
compression validation already had a single owner. Moving it here changes its
address, not its owner.
"""

from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.logging_config import warn

#: One ``(min, max, default)`` per codec that takes a level. An unset level
#: resolves from here, so this is where the global default is written down.
COMPRESSION_RANGES = {
    "GZIP": (1, 9, 6),
    "ZSTD": (1, 22, 15),
    "BROTLI": (1, 11, 6),
}

#: What the ``gpio process`` commands resolve an unset *ZSTD* level to, in
#: place of the global 15.
#:
#: Measured on their own output tables (#1299, benchmarks on #1195): level 9
#: writes 2.8-8.2x faster than 15 for at most 4.9% more bytes, and on a
#: 217k-row polygonize table it is both faster *and* smaller -- at 15 ZSTD's
#: long-range matching fights the dictionary encoding on a column of short WKB
#: blobs. Below 9 the size cost stops being noise: level 3 is 15-20% larger on
#: geometry-heavy tables and level 1 is 34-35% larger, the wrong trade for
#: files people host and pay egress on.
#:
#: These commands compute a whole table and then write it, so the write is a
#: large share of their wall clock -- 67-90% on the raster two. Every other
#: command keeps 15.
PROCESS_ZSTD_LEVEL = 9


def resolve_process_compression_level(compression, compression_level):
    """The level a ``gpio process`` write should use, given what the caller asked for.

    Returns ``compression_level`` untouched whenever the caller set one, so an
    explicit ``--compression-level`` always wins. Only an *unset* level moves,
    and only for ZSTD: GZIP and BROTLI have their own curves and keep the
    defaults in :data:`COMPRESSION_RANGES`, because nothing has measured them.

    Call this at the entry of a process command's core, not deeper. The
    resolved number has to be the one that reaches the write funnel, which
    would otherwise fill in the global 15 on the way past, and the streaming
    writer, which would otherwise inherit pyarrow's own default of 1.
    """
    if compression_level is not None:
        return compression_level
    if (compression or "").upper() != "ZSTD":
        return compression_level
    return PROCESS_ZSTD_LEVEL


def validate_compression_settings(compression, compression_level, verbose=False):
    """
    Validate and normalize compression settings.

    Args:
        compression: Compression type string
        compression_level: Compression level (can be None for defaults)
        verbose: Whether to print verbose output

    Returns:
        tuple: (normalized_compression, validated_level, compression_desc)
    """
    compression = compression.upper()
    valid_compressions = ["ZSTD", "GZIP", "BROTLI", "LZ4", "SNAPPY", "UNCOMPRESSED"]

    if compression not in valid_compressions:
        raise InvalidParameterError(
            "compression",
            f"Invalid compression '{compression}'. Must be one of: {', '.join(valid_compressions)}",
        )

    if compression in COMPRESSION_RANGES:
        min_level, max_level, default_level = COMPRESSION_RANGES[compression]

        # Use default if not specified
        if compression_level is None:
            compression_level = default_level

        if compression_level < min_level or compression_level > max_level:
            raise InvalidParameterError(
                "compression_level",
                f"{compression} compression level must be between {min_level} and {max_level}, got {compression_level}",
            )
        compression_desc = f"{compression}:{compression_level}"
    elif compression in ["LZ4", "SNAPPY"]:
        # Only mention a level the caller actually chose: the global ZSTD
        # default arrives here from callers that pass it unconditionally.
        if compression_level and compression_level != COMPRESSION_RANGES["ZSTD"][2] and verbose:
            warn(
                f"Note: {compression} does not support compression levels. Ignoring level {compression_level}."
            )
        compression_level = None  # These formats don't use compression levels
        compression_desc = compression
    else:
        compression_level = None  # UNCOMPRESSED doesn't use levels
        compression_desc = compression

    return compression, compression_level, compression_desc
