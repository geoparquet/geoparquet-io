"""The one parser for a bounding box a user asks for (#1367, concept 15).

``--bbox`` on ``gpio extract`` (and its arcgis, bigquery, wfs and carto
siblings), ``gpio convert``'s filtered writers, and ``bbox=`` on the Python API
all answer the same question -- which four numbers did the caller mean -- so
they share one answer here. ``bbox_structure`` is the other bbox module and a
different question: what bbox column or covering a *file* carries.

A bbox is ``"xmin,ymin,xmax,ymax"`` text or a sequence of four numbers. It is
refused when it does not have four numeric values, when a value is NaN or
infinite (NaN compares False, so it slipped past the corner check and reached
DuckDB as the SQL token ``nan``), or when ``ymin > ymax``. Every refusal is an
:class:`~geoparquet_io.core.exceptions.InvalidParameterError` naming the
caller's parameter.

``xmin > xmax`` is not a refusal: it is the antimeridian wrap (RFC 7946 5.2),
the reading ``validate`` and ``check_bbox_values`` give a declared bbox.
:attr:`BBox.crosses_antimeridian` is the one predicate for it. A consumer that
builds a filter asks :meth:`BBox.pieces` for the one or two non-wrapping boxes
and ORs them (:func:`any_piece`); a service whose request has no wrapped form
calls :func:`refuse_wrap`, which raises :class:`AntimeridianBBoxError`.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Sequence
from typing import NamedTuple

from geoparquet_io.core.exceptions import InvalidParameterError


class BBox(NamedTuple):
    """A parsed ``(xmin, ymin, xmax, ymax)``; ``xmin > xmax`` wraps the antimeridian."""

    xmin: float
    ymin: float
    xmax: float
    ymax: float

    @property
    def crosses_antimeridian(self) -> bool:
        """Whether the X range wraps the antimeridian (``xmin > xmax``)."""
        return self.xmin > self.xmax

    def pieces(self) -> tuple[BBox, ...]:
        """The box itself, or its two lobes ``[xmin, 180]`` and ``[-180, xmax]`` when it wraps."""
        if not self.crosses_antimeridian:
            return (self,)
        return (
            BBox(self.xmin, self.ymin, 180.0, self.ymax),
            BBox(-180.0, self.ymin, self.xmax, self.ymax),
        )

    def polygon_wkt(self) -> str:
        """The (non-wrapping) box as a closed WKT polygon."""
        xmin, ymin, xmax, ymax = self
        return (
            f"POLYGON(({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))"
        )


class AntimeridianBBoxError(InvalidParameterError):
    """A wrapped bbox (xmin > xmax) went to a service whose request cannot carry one."""

    def __init__(self, service: str, bbox: BBox, param: str = "bbox") -> None:
        self.service = service
        super().__init__(
            param,
            f"{tuple(bbox)} crosses the antimeridian (xmin > xmax), and {service} requests "
            "have no form for a wrapped bbox. Run two extracts, one either side of 180.",
        )


def any_piece(bbox: BBox, condition: Callable[[BBox], str]) -> str:
    """One SQL condition for ``bbox``: ``condition`` of each piece, ORed when it wraps."""
    parts = [condition(piece) for piece in bbox.pieces()]
    return parts[0] if len(parts) == 1 else "(" + " OR ".join(f"({p})" for p in parts) + ")"


def refuse_wrap(bbox: BBox, service: str, param: str = "bbox") -> BBox:
    """``bbox`` unchanged, or :class:`AntimeridianBBoxError` when it wraps."""
    if bbox.crosses_antimeridian:
        raise AntimeridianBBoxError(service, bbox, param)
    return bbox


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
        InvalidParameterError: Not four numbers, a non-finite value, or ``ymin > ymax``.
            ``xmin > xmax`` is accepted as the antimeridian wrap.
    """
    parts = _values(bbox, param)
    if len(parts) != 4:
        raise InvalidParameterError(
            param, f"expected 4 values (xmin,ymin,xmax,ymax), got {len(parts)}"
        )
    if not all(math.isfinite(value) for value in parts):
        raise InvalidParameterError(param, f"coordinates must be finite, got {bbox!r}")
    xmin, ymin, xmax, ymax = parts
    if ymin > ymax:
        raise InvalidParameterError(
            param,
            f"coordinates appear to be reversed. ymin ({ymin}) must be <= ymax ({ymax}) "
            "(only X may wrap, across the antimeridian). Expected order: xmin,ymin,xmax,ymax.",
        )
    if xmin > xmax and not (-180.0 <= xmax and xmin <= 180.0):
        # Only a longitude wraps: x = 500000 with xmin > xmax is swapped, not a wrap.
        raise InvalidParameterError(
            param,
            f"coordinates appear to be reversed. xmin ({xmin}) > xmax ({xmax}) is read as an "
            "antimeridian wrap only for longitudes in [-180, 180]. "
            "Expected order: xmin,ymin,xmax,ymax.",
        )
    return BBox(xmin, ymin, xmax, ymax)


def parse_optional_bbox(bbox: str | Sequence[float] | None, param: str = "bbox") -> BBox | None:
    """:func:`parse_bbox`, with ``None`` (no bbox asked for) passed through."""
    return None if bbox is None else parse_bbox(bbox, param)
