from geoparquet_io.api import (
    # Lazy API
    LazyTable,
    # Eager API
    Table,
    convert,
    convert_lazy,
    extract_arcgis,
    from_arrow,
    from_relation,
    from_table,
    ops,
    pipe,
    read,
    read_bigquery,
    read_lazy,
    read_partition,
)
from geoparquet_io.api.check import CheckResult
from geoparquet_io.api.stac import generate_stac, validate_stac
from geoparquet_io.cli.main import cli

__all__ = [
    "cli",
    # Eager API
    "read",
    "read_partition",
    "read_bigquery",
    "convert",
    "extract_arcgis",
    "Table",
    "pipe",
    "ops",
    # Lazy API
    "LazyTable",
    "read_lazy",
    "convert_lazy",
    "from_table",
    "from_relation",
    "from_arrow",
    # Utilities
    "CheckResult",
    "generate_stac",
    "validate_stac",
]
