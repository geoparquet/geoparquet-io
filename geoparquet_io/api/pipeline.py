"""
Pipeline composition utilities for GeoParquet operations.

Provides a pipe() helper for functional composition of operations:

    from geoparquet_io.api import pipe, ops

    transform = pipe(
        lambda t: ops.add_bbox(t),
        lambda t: ops.add_quadkey(t, resolution=12),
        lambda t: ops.sort_hilbert(t),
    )

    result = transform(input_table)
"""

from __future__ import annotations

from collections.abc import Callable
from functools import reduce
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    import pyarrow as pa

    from geoparquet_io.api.table import Table

T = TypeVar("T", "pa.Table", "Table")


def pipe(*operations: Callable[[T], T]) -> Callable[[T], T]:
    """
    Compose multiple table operations into a single pipeline.

    Each operation should accept and return the same type (Table or pa.Table).
    Operations are applied left-to-right.

    Args:
        *operations: Functions that transform a table

    Returns:
        A function that applies all operations in sequence

    Example:
        >>> from geoparquet_io.api import pipe, ops
        >>>
        >>> # Create a reusable transformation pipeline
        >>> preprocess = pipe(
        ...     lambda t: ops.add_bbox(t),
        ...     lambda t: ops.add_quadkey(t, resolution=12),
        ...     lambda t: ops.sort_hilbert(t),
        ... )
        >>>
        >>> # Apply to any table
        >>> result = preprocess(input_table)

    Example with fluent Table:
        >>> from geoparquet_io.api import pipe, read
        >>>
        >>> preprocess = pipe(
        ...     lambda t: t.add_bbox(),
        ...     lambda t: t.add_quadkey(resolution=12),
        ...     lambda t: t.sort_hilbert(),
        ... )
        >>>
        >>> result = preprocess(read('input.parquet'))
        >>> result.write('output.parquet')
    """
    if not operations:
        return lambda x: x

    def apply_pipeline(table: T) -> T:
        return reduce(lambda t, op: op(t), operations, table)

    return apply_pipeline
