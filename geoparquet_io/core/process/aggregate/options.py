"""The options every grid aggregation takes, as one object.

``process aggregate a5|h3`` and the Python entry points behind them
(``ops.aggregate_a5``, ``Table.aggregate_h3``, ``aggregate_by_a5`` ...) all took
the same ten keyword arguments and forwarded them by name to the next layer, so
one new option was a ten-file edit and every layer's forwarding block was a copy
of the others'. :class:`AggregateOptions` is what they pass instead: built once
where the user's values arrive, handed whole to the grid engine, which reads it.

It carries what describes *what* to aggregate and how to key it. Where the
result goes (compression, version, verbosity) is the write options' business,
and the resolution choice (``--resolution`` / ``--auto``) is per scheme.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields
from typing import Any

__all__ = ["BUCKET_POINT_GEOMETRY", "AggregateOptions"]

# --bucket-point mode keyword for the default: key each feature by its geometry.
BUCKET_POINT_GEOMETRY = "geometry"


@dataclass(frozen=True)
class AggregateOptions:
    """The aggregation a caller asks for, independent of the grid or the output.

    Attributes:
        metric: Numeric rollups, e.g. ``"sum:area_ha,avg:yield"``
        metric_nodata: NoData sentinel(s) mapped to NULL in metric columns
        breakdown: Categorical column to pivot into per-value count columns
        breakdown_limit: Max breakdown values before the remainder rolls into
            an ``other`` column
        breakdown_metric: What each breakdown column holds: None/``count``, or
            ``sum:col`` / ``min:col`` / ``max:col``
        out_geometry: ``polygon``, ``centroid``, ``both`` or ``none``
        where: DuckDB WHERE clause filtering input rows
        bucket_point: Keying point: ``geometry``, ``bbox`` or a point column name
        bbox_column: Bbox covering column for ``bucket_point="bbox"``
        memory_limit: DuckDB memory limit for the aggregation (e.g. ``"8GB"``)
    """

    metric: str | None = None
    metric_nodata: str | None = None
    breakdown: str | None = None
    breakdown_limit: int = 20
    breakdown_metric: str | None = None
    out_geometry: str = "polygon"
    where: str | None = None
    bucket_point: str = BUCKET_POINT_GEOMETRY
    bbox_column: str | None = None
    memory_limit: str | None = None

    @classmethod
    def pick(cls, values: Mapping[str, Any]) -> AggregateOptions:
        """The options named in *values* (``pick(locals())`` in a function that takes them).

        Other keys are ignored and absent fields keep their defaults, so the ten
        names are spelled out once, here, and not in every function that
        receives them as keywords.
        """
        return cls(**{f.name: values[f.name] for f in fields(cls) if f.name in values})
