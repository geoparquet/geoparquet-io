"""geoparquet-io: GeoParquet I/O with a CLI, a Python API, and an embeddable core.

Every public name is resolved lazily (PEP 562, #1307): ``import geoparquet_io``
executes nothing beyond this file and ``geoparquet_io.core``'s tiny
``__init__``, so a host application — a QGIS plugin vendoring the embeddable
core modules, say — can import the package without pulling in click, rich,
DuckDB or the AWS stack as side effects. The names themselves are unchanged:
``gpio.read(...)`` or ``from geoparquet_io import cli`` import what they need
on first access, exactly as the old eager ``__init__`` did on every import.

``geoparquet_io.cli`` is the CLI package. The Click group lives at
``geoparquet_io.cli.main.cli``; the console entry point uses that module too.
"""

from typing import TYPE_CHECKING

# The one eager import the lazy root keeps: ``geoparquet_io.core`` installs the
# geoarrow srid-compat shim on import (#1276), and the shim must be in place
# before any pyarrow read in the process — ``pq.read_table`` segfaults on a
# ``srid:N`` CRS without it. The cost is stdlib plus geoarrow-types where it is
# installed; in a vendoring host without geoarrow the install is a no-op.
# tests/test_geoarrow_srid_compat.py proves ``import geoparquet_io`` is enough,
# and tests/test_embeddable_imports.py that it still pulls no click or duckdb.
import geoparquet_io.core  # noqa: F401

# The lazy ``__getattr__`` below is invisible to a type checker, which resolves
# every attribute it serves to ``Any`` — so without this block the py.typed
# marker would promise types that ``gpio.read(...)`` never delivers. These
# imports run only under a type checker (``TYPE_CHECKING`` is False at run
# time), so the package stays as lazy as #1307 made it. They list the same names
# as ``_LAZY_EXPORTS``, which tests/test_mypy.py checks.
if TYPE_CHECKING:
    from geoparquet_io.api import (
        InvalidParameterError,
        Table,
        convert,
        extract_arcgis,
        from_bigquery,
        list_layers,
        ops,
        pipe,
        read,
        read_bigquery,
        read_partition,
    )
    from geoparquet_io.api.check import (
        CheckResult,
        check_all,
        check_bbox,
        check_bloom_filters,
        check_compression,
        check_optimization,
        check_row_groups,
        check_spatial,
        check_spatial_pushdown,
        validate,
    )
    from geoparquet_io.api.stac import generate_stac, validate_stac
    from geoparquet_io.core.check_options import CheckOptions
    from geoparquet_io.core.exceptions import GeoParquetError
    from geoparquet_io.core.parquet_writer import WriteOptions
    from geoparquet_io.core.remote import RemoteOptions

#: name -> "module:attribute" it is imported from, on first access.
_LAZY_EXPORTS = {
    "InvalidParameterError": "geoparquet_io.api:InvalidParameterError",
    "Table": "geoparquet_io.api:Table",
    "convert": "geoparquet_io.api:convert",
    "extract_arcgis": "geoparquet_io.api:extract_arcgis",
    "from_bigquery": "geoparquet_io.api:from_bigquery",
    "list_layers": "geoparquet_io.api:list_layers",
    "ops": "geoparquet_io.api:ops",
    "pipe": "geoparquet_io.api:pipe",
    "read": "geoparquet_io.api:read",
    "read_bigquery": "geoparquet_io.api:read_bigquery",
    "read_partition": "geoparquet_io.api:read_partition",
    "CheckOptions": "geoparquet_io.core.check_options:CheckOptions",
    "CheckResult": "geoparquet_io.api.check:CheckResult",
    "check_all": "geoparquet_io.api.check:check_all",
    "check_bbox": "geoparquet_io.api.check:check_bbox",
    "check_bloom_filters": "geoparquet_io.api.check:check_bloom_filters",
    "check_compression": "geoparquet_io.api.check:check_compression",
    "check_optimization": "geoparquet_io.api.check:check_optimization",
    "check_row_groups": "geoparquet_io.api.check:check_row_groups",
    "check_spatial": "geoparquet_io.api.check:check_spatial",
    "check_spatial_pushdown": "geoparquet_io.api.check:check_spatial_pushdown",
    "validate": "geoparquet_io.api.check:validate",
    "generate_stac": "geoparquet_io.api.stac:generate_stac",
    "validate_stac": "geoparquet_io.api.stac:validate_stac",
    "GeoParquetError": "geoparquet_io.core.exceptions:GeoParquetError",
    "RemoteOptions": "geoparquet_io.core.remote:RemoteOptions",
    "WriteOptions": "geoparquet_io.core.parquet_writer:WriteOptions",
}

__all__ = [
    "cli",
    "read",
    "read_partition",
    "from_bigquery",
    # The deprecated spelling of `from_bigquery` (#1436), importable until the
    # next minor release.
    "read_bigquery",
    "convert",
    "extract_arcgis",
    "list_layers",
    "Table",
    "RemoteOptions",
    "WriteOptions",
    "pipe",
    "ops",
    "CheckOptions",
    "CheckResult",
    "check_all",
    "check_bbox",
    "check_bloom_filters",
    "check_compression",
    "check_optimization",
    "check_row_groups",
    "check_spatial",
    "check_spatial_pushdown",
    "validate",
    "generate_stac",
    "validate_stac",
    "GeoParquetError",
    "InvalidParameterError",
]


def __getattr__(name: str):
    from importlib import import_module

    if name in {"api", "cli"}:
        return import_module(f"{__name__}.{name}")
    try:
        target = _LAZY_EXPORTS[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    module_name, attribute = target.split(":")
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value  # cache: the hook runs once per name
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
