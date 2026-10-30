"""The one parser for a bounding box a user asks for (#1367, concept 15).

``--bbox`` on ``gpio extract`` (and its arcgis, bigquery, wfs and carto
siblings), ``gpio convert``'s filtered writers, and ``bbox=`` on the Python API
all answer the same question -- which four numbers did the caller mean -- so
they share one answer here. ``bbox_structure`` is the other bbox module and a
different question: what bbox column or covering a *file* carries.

A bbox is ``"xmin,ymin,xmax,ymax"`` text or a sequence of four numbers. It is
refused when it does not have four numeric values, when a value is NaN or
infinite (NaN compares False, so it slipped past the corner check and reached
DuckDB as the SQL token ``nan``), or when its corners are swapped
(``xmin > xmax`` or ``ymin > ymax``). Every refusal is an
:class:`~geoparquet_io.core.exceptions.InvalidParameterError` naming the
caller's parameter.
"""

from __future__ import annotations

import math
from collections.abc import Sequence

from geoparquet_io.core.exceptions import InvalidParameterError

BBox = tuple[float, float, float, float]


def _values(bbox: str | Sequence[float], param: str) -> list[float]:
    if isinstance(bbox, str):
        try:
            return [float(part.strip()) for part in bbox.split(",")]
        except ValueError as e:
            raise InvalidParameterError(
                param, f"expected numeric values: xmin,ymin,xmax,ymax. Error: {e}"
            ) from e
    try:
        return [float(value) for value in bbox]
    except (TypeError, ValueError) as e:
        raise InvalidParameterError(
            param, f"expected 4 numeric values (xmin,ymin,xmax,ymax), got {bbox!r}"
        ) from e


def parse_bbox(bbox: str | Sequence[float], param: str = "bbox") -> BBox:
    """``bbox`` as ``(xmin, ymin, xmax, ymax)`` floats, or the error saying why not.

    Args:
        bbox: ``"xmin,ymin,xmax,ymax"`` text or a sequence of four numbers.
        param: The parameter name the error reports (``bbox`` by default).

    Raises:
        InvalidParameterError: Not four numbers, a non-finite value, or swapped corners.
    """
    parts = _values(bbox, param)
    if len(parts) != 4:
        raise InvalidParameterError(
            param, f"expected 4 values (xmin,ymin,xmax,ymax), got {len(parts)}"
        )
    if not all(math.isfinite(value) for value in parts):
        raise InvalidParameterError(param, f"coordinates must be finite, got {bbox!r}")
    xmin, ymin, xmax, ymax = parts
    if xmin > xmax or ymin > ymax:
        raise InvalidParameterError(
            param,
            f"coordinates appear to be reversed. "
            f"xmin ({xmin}) must be <= xmax ({xmax}), and ymin ({ymin}) must be <= ymax ({ymax}). "
            "Expected order: xmin,ymin,xmax,ymax.",
        )
    return (xmin, ymin, xmax, ymax)
