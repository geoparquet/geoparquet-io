"""Process-wide deprecation warnings, attributed to the caller of gpio."""

import warnings
from collections.abc import Mapping
from typing import Any

_warned: set[tuple[str, str]] = set()


def _caller_stacklevel() -> int:
    """Count from the warning emitter through gpio to its external caller."""
    import sys

    level = 1
    frame: Any = sys._getframe(1)
    while frame is not None:
        name = frame.f_globals.get("__name__", "")
        if name != "geoparquet_io" and not name.startswith("geoparquet_io."):
            return level
        level += 1
        frame = frame.f_back
    return level


def warn_deprecated(replacements: Mapping[str, tuple[str, str]]) -> None:
    """Warn once per spelling/replacement, in field order, outside gpio frames.

    Mapping keys preserve field order when an entry point renames a keyword;
    values are the deprecated spelling and its replacement instruction.
    A ledger is required even under a caller's ``always`` warning filter.
    """
    stacklevel = _caller_stacklevel()
    for field in sorted(replacements):
        old, replacement = replacements[field]
        key = old, replacement
        if key in _warned:
            continue
        _warned.add(key)
        warnings.warn(
            f"{old} is deprecated and will be removed in the next minor release; {replacement}",
            DeprecationWarning,
            stacklevel=stacklevel,
        )
