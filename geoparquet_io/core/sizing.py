"""Byte sizes as humans write them: the one parser and the one formatter (#1367).

``parse_size_string`` reads every size gpio accepts or DuckDB reports
(``--write-memory``, ``--row-group-size-mb``, ``--min-size``, DuckDB's own
``current_setting('memory_limit')``). The units are DuckDB's: ``KB``/``MB``/
``GB``/``TB``/``PB`` are powers of 1000, ``KiB``/``MiB``/``GiB``/``TiB``/``PiB``
powers of 1024, a bare number is MB, and case and surrounding whitespace do not
matter. ``format_memory_limit`` renders a byte count as a ``SET memory_limit``
value that means what it says; ``format_size`` renders a total for display.
"""

from __future__ import annotations

import re

#: Bytes in one MB: a bare number, ``--row-group-size-mb`` and every "MB" gpio
#: reads is this many bytes (approved change 5 of #1367).
BYTES_PER_MB = 10**6

#: Binary buffer thresholds use MiB explicitly, never MB.
BYTES_PER_MIB = 2**20

_UNITS = {
    "": BYTES_PER_MB,
    "B": 1,
    "BYTES": 1,
    **{f"{p}{suffix}": 1000 ** (i + 1) for i, p in enumerate("KMGTP") for suffix in ("", "B")},
    **{f"{p}IB": 1024 ** (i + 1) for i, p in enumerate("KMGTP")},
}
_SIZE_RE = re.compile(r"(\d+(?:\.\d+)?)\s*([A-Za-z]*)")


def _split_size(size_str: str) -> tuple[str, str]:
    """``(number, UPPER-CASED unit)`` of a non-blank size string; ``ValueError`` if it is not one."""
    match = _SIZE_RE.fullmatch(str(size_str).strip())
    unit = match.group(2).upper() if match else None
    if match is None or unit not in _UNITS:
        raise ValueError(
            f"Invalid size format: {size_str!r} (expected e.g. '512MB', '2GB', '1GiB'; "
            "KB/MB/GB are powers of 1000, KiB/MiB/GiB of 1024, a bare number is MB)"
        )
    return match.group(1), unit


def parse_size_string(size_str: str | None) -> int | None:
    """Bytes in a size string such as ``'256MB'``, ``'1GiB'``, ``'4.5 gb'`` or ``'128'``.

    Returns ``None`` for ``None`` or a blank string, so an unset option stays
    unset. Anything else that is not a size raises ``ValueError`` naming it.
    """
    if size_str is None or not str(size_str).strip():
        return None
    number, unit = _split_size(size_str)
    return int(float(number) * _UNITS[unit])


def normalize_size_string(size_str: str) -> str:
    """The canonical spelling of a size: ``'4.5 gb'`` -> ``'4.5GB'``, ``'1gib'`` -> ``'1GiB'``, ``'512'`` -> ``'512MB'``.

    The same grammar as :func:`parse_size_string` (``ValueError`` otherwise),
    and the same bytes: the canonical form parses back to the identical count.
    """
    number, unit = _split_size(size_str)
    if not unit:
        unit = "MB"
    elif unit.endswith("IB"):
        unit = f"{unit[0]}iB"
    return f"{number}{unit}"


def bytes_to_mb(size_bytes: float) -> float:
    """``size_bytes`` in MB (10**6 bytes)."""
    return size_bytes / BYTES_PER_MB


def mb_to_bytes(size_mb: float) -> int:
    """Bytes in ``size_mb`` MB (10**6 bytes each)."""
    return int(round(size_mb * BYTES_PER_MB))


# A size as current_setting() displays it: "2.7 GiB", "953.6 MiB".
_DISPLAYED_SIZE_RE = re.compile(r"^(\d+)\.(\d+) ([KMGT]iB)$")


def nudged_size(displayed: str) -> str | None:
    """A size that DuckDB displays back as ``displayed``; None if it is not a size.

    ``current_setting`` truncates a size to one decimal, so writing the display
    string back lands a step low ("2.7 GiB" reads back "2.6 GiB") and a loop of
    save/restore drifts down. Half a displayed step up reads back unchanged.
    """
    match = _DISPLAYED_SIZE_RE.match(displayed)
    if not match:
        return None
    whole, frac, unit = match.groups()
    return f"{whole}.{frac}5{unit}"


def displayed_size_max(displayed: str) -> str | None:
    """The largest size DuckDB would display as ``displayed``; None if it is not a size.

    The same truncation seen from the other side: "7.4 GiB" is any size from
    7.4 GiB up to 7.5 GiB, so a *comparison* against a number gpio itself set has
    to use the top of that range. Against the bottom, the 8.0GB limit gpio puts
    on a connection reads back as 7.4 GiB = 7.95e9 bytes and looks like a
    stricter limit somebody else chose. :func:`nudged_size` takes the middle of
    the range instead, because a restore has to read back unchanged rather than
    err high.
    """
    match = _DISPLAYED_SIZE_RE.match(displayed)
    if not match:
        return None
    whole, frac, unit = match.groups()
    step = 10 ** -len(frac)
    return f"{float(f'{whole}.{frac}') + step:.{len(frac)}f}{unit}"


#: The smallest memory limit gpio sets: below it DuckDB cannot hold a scan's buffers.
_MIN_MEMORY_LIMIT = 128 * 1024**2


def format_memory_limit(limit_bytes: int) -> str:
    """``limit_bytes`` as a DuckDB ``memory_limit`` value, in binary units with binary suffixes.

    One decimal of GiB at or above 1 GiB, else whole MiB, floored at 128 MiB.
    (This used to divide by 1024**3 and write "GB", which DuckDB reads as
    10**9: every limit landed 7% under the number computed for it.)
    """
    limit_bytes = max(int(limit_bytes), _MIN_MEMORY_LIMIT)
    if limit_bytes >= 1024**3:
        return f"{limit_bytes / 1024**3:.1f}GiB"
    return f"{limit_bytes // 1024**2}MiB"


def format_size(size_bytes):
    """Bytes as a human-readable string for display, in 1000-based steps.

    ``KB``/``MB``/``GB``/``TB`` mean here what :func:`parse_size_string` reads
    them as, so a size gpio prints parses back to (about) the same bytes.
    """
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1000:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1000
    return f"{size_bytes:.2f} TB"
