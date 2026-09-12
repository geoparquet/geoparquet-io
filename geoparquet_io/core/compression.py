"""Validating and normalizing the compression settings a write was asked for.

One codec whitelist and one level range per codec, shared by every write path.
This is deliberately *not* part of the write facade in ``core/parquet_writer.py``:
that module owns the decisions the write paths were disagreeing about, and
compression validation already had a single owner. Moving it here changes its
address, not its owner.
"""

from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.logging_config import warn


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

    # Handle compression level based on format
    compression_ranges = {
        "GZIP": (1, 9, 6),  # min, max, default
        "ZSTD": (1, 22, 15),  # min, max, default
        "BROTLI": (1, 11, 6),  # min, max, default
    }

    if compression in compression_ranges:
        min_level, max_level, default_level = compression_ranges[compression]

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
        if compression_level and compression_level != 15 and verbose:  # Not default
            warn(
                f"Note: {compression} does not support compression levels. Ignoring level {compression_level}."
            )
        compression_level = None  # These formats don't use compression levels
        compression_desc = compression
    else:
        compression_level = None  # UNCOMPRESSED doesn't use levels
        compression_desc = compression

    return compression, compression_level, compression_desc
