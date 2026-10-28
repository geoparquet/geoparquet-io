"""The one JSON encoder for gpio's machine-readable reports.

Every ``--json`` report (``inspect``, ``inspect meta``, ``check spec``) carries
values that came out of a DuckDB query or a Parquet footer, and those are not
all JSON types: a DATE min is a ``datetime.date``, a DECIMAL a ``Decimal``, a
BLOB ``bytes``, an INTERVAL a ``timedelta``, and a DOUBLE can be NaN or
infinite. A bare ``json.dumps`` raised on the first kind (#1226) and printed
the bare tokens ``NaN``/``Infinity`` for the last, which is not JSON at all.

``to_jsonable`` turns a value into plain JSON types, and ``dumps_json`` is
``json.dumps`` over that result with ``allow_nan=False``, so a report either
serializes to strict JSON or fails loudly here rather than downstream.

The conversions:

* ``date``/``datetime``/``time`` become ISO 8601 strings (``isoformat()``).
* ``timedelta`` becomes an ISO 8601 duration (``P3DT4S``).
* ``Decimal`` becomes its exact decimal string, so no precision is lost to a
  float; a non-finite one becomes ``null``.
* ``bytes``-like values become lowercase hex.
* numpy scalars and arrays become their Python equivalents. A ``datetime64``
  becomes an ISO 8601 string at its own precision (nanoseconds included), and
  a scalar with no plain Python type (``longdouble``, ``clongdouble``) becomes
  a ``float`` or its string.
* pandas ``NaT`` and ``NA`` become ``null``.
* NaN and infinite floats become ``null``.
* sets and tuples become lists, and mapping keys become strings.
* Anything else falls back to ``str(value)``, so a report never dies on a
  type nobody listed.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import sys
from decimal import Decimal
from typing import Any


def _iso_duration(value: dt.timedelta) -> str:
    """An ISO 8601 duration for a ``timedelta`` (``P3DT4.5S``, ``-P1D``)."""
    sign = "-" if value < dt.timedelta(0) else ""
    value = abs(value)
    days = value.days
    seconds = value.seconds + value.microseconds / 1_000_000
    text = f"{sign}P{days}D" if days else f"{sign}P"
    if seconds:
        secs = f"{seconds:f}".rstrip("0").rstrip(".")
        text += f"T{secs}S"
    return text if text not in ("P", "-P") else "P0D"


def _float_or_none(value: float) -> float | None:
    return value if math.isfinite(value) else None


def _is_pandas_missing(value: Any) -> bool:
    """True for pandas ``NaT`` or ``NA``, without importing pandas ourselves.

    A pandas value can only reach us if pandas is already imported, so an
    absent module means the value is not one of them.
    """
    pd = sys.modules.get("pandas")
    if pd is None:
        return False
    return value is pd.NaT or value is pd.NA


def _numpy_scalar_to_python(value: Any) -> Any:
    """A numpy scalar as a value ``to_jsonable`` can finish converting."""
    np = sys.modules["numpy"]

    if isinstance(value, np.datetime64):
        return None if np.isnat(value) else str(np.datetime_as_string(value))
    converted = value.tolist() if hasattr(value, "tolist") else value.item()
    if not type(converted).__module__.startswith("numpy"):
        return converted
    # longdouble/clongdouble hand back themselves, so recursing never ends.
    return float(value) if isinstance(value, np.floating) else str(value)


def _numpy_to_python(value: Any) -> tuple[bool, Any]:
    """``(True, python_value)`` for a numpy scalar or array, else ``(False, None)``."""
    module = type(value).__module__
    if not module.startswith("numpy"):
        return False, None
    # A numpy value exists only once numpy is loaded, so take it from there
    # rather than importing a package gpio depends on only transitively.
    np = sys.modules["numpy"]

    if isinstance(value, np.ndarray):
        # Walk the elements so each scalar gets the scalar rules above.
        return True, value[()] if value.ndim == 0 else list(value)
    return True, _numpy_scalar_to_python(value)


def _scalar_to_jsonable(value: Any) -> Any:
    """Convert one non-container value."""
    if isinstance(value, float):
        return _float_or_none(value)
    if _is_pandas_missing(value):
        return None
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, dt.timedelta):
        return _iso_duration(value)
    if isinstance(value, Decimal):
        return str(value) if value.is_finite() else None
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    is_numpy, converted = _numpy_to_python(value)
    if is_numpy:
        return to_jsonable(converted)
    return str(value)


def to_jsonable(value: Any) -> Any:
    """Return ``value`` rebuilt from plain JSON types only.

    Containers are walked recursively. See the module docstring for the
    conversion of each type.
    """
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, dict):
        return {str(k) if not isinstance(k, str) else k: to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [to_jsonable(v) for v in value]
    return _scalar_to_jsonable(value)


def dumps_json(value: Any, **kwargs: Any) -> str:
    """``json.dumps`` for a report: every value made JSON-safe, NaN refused.

    Keyword arguments (``indent``, ``sort_keys``, ...) pass through to
    ``json.dumps``.
    """
    return json.dumps(to_jsonable(value), allow_nan=False, **kwargs)
