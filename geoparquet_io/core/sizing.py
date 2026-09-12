"""Byte sizes as humans write them: parsing an option value, rendering a total.

``parse_size_string`` reads the ``256MB``/``1GB``/``128`` spellings the CLI
accepts; ``format_size`` renders a byte count back for display. They are the
same fact in opposite directions, and neither knows anything about GeoParquet,
which is why they no longer live in ``core/common.py``.
"""

import re


def parse_size_string(size_str):
    """
    Parse a human-readable size string into bytes.

    Args:
        size_str: String like '256MB', '1GB', '128' (assumed MB if no unit)

    Returns:
        int: Size in bytes
    """
    if not size_str:
        return None

    # Handle plain numbers (assume MB)
    try:
        return int(size_str) * 1024 * 1024
    except ValueError:
        pass

    # Parse with units
    size_str = size_str.strip().upper()
    match = re.match(r"^(\d+(?:\.\d+)?)\s*([KMGT]?B?)$", size_str)
    if not match:
        raise ValueError(f"Invalid size format: {size_str}")

    value = float(match.group(1))
    unit = match.group(2)

    # Convert to bytes
    multipliers = {
        "B": 1,
        "KB": 1024,
        "MB": 1024 * 1024,
        "GB": 1024 * 1024 * 1024,
        "TB": 1024 * 1024 * 1024 * 1024,
        "K": 1024,
        "M": 1024 * 1024,
        "G": 1024 * 1024 * 1024,
        "T": 1024 * 1024 * 1024 * 1024,
    }

    multiplier = multipliers.get(unit, 1024 * 1024)  # Default to MB
    return int(value * multiplier)


def format_size(size_bytes):
    """Convert bytes to human readable string."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"
