"""geoparquet-io: GeoParquet I/O with a CLI, a Python API, and an embeddable core.

Every public name is resolved lazily (PEP 562, #1307): ``import geoparquet_io``
executes nothing beyond this file and ``geoparquet_io.core``'s tiny
``__init__``, so a host application — a QGIS plugin vendoring the embeddable
core modules, say — can import the package without pulling in click, rich,
DuckDB or the AWS stack as side effects. The names themselves are unchanged:
``gpio.read(...)`` or ``from geoparquet_io import cli`` import what they need
on first access, exactly as the old eager ``__init__`` did on every import.

``geoparquet_io.cli`` keeps its historical quirk: accessed as an attribute it
is the Click *Group*, not the ``geoparquet_io.cli`` package (the eager
``from geoparquet_io.cli.main import cli`` used to shadow the subpackage the
same way). tests/test_patch_target_shadowing.py guards the consequences for
``mock.patch`` targets.
"""

# The one eager import the lazy root keeps: ``geoparquet_io.core`` installs the
# geoarrow srid-compat shim on import (#1276), and the shim must be in place
# before any pyarrow read in the process — ``pq.read_table`` segfaults on a
# ``srid:N`` CRS without it. The cost is stdlib plus geoarrow-types where it is
# installed; in a vendoring host without geoarrow the install is a no-op.
# tests/test_geoarrow_srid_compat.py proves ``import geoparquet_io`` is enough,
# and tests/test_embeddable_imports.py that it still pulls no click or duckdb.
import geoparquet_io.core  # noqa: F401

#: name -> "module:attribute" it is imported from, on first access.
_LAZY_EXPORTS = {
    "InvalidParameterError": "geoparquet_io.api:InvalidParameterError",
    "Table": "geoparquet_io.api:Table",
    "convert": "geoparquet_io.api:convert",
    "extract_arcgis": "geoparquet_io.api:extract_arcgis",
    "list_layers": "geoparquet_io.api:list_layers",
    "ops": "geoparquet_io.api:ops",
    "pipe": "geoparquet_io.api:pipe",
    "read": "geoparquet_io.api:read",
    "read_bigquery": "geoparquet_io.api:read_bigquery",
    "read_partition": "geoparquet_io.api:read_partition",
    "CheckResult": "geoparquet_io.api.check:CheckResult",
    "generate_stac": "geoparquet_io.api.stac:generate_stac",
    "validate_stac": "geoparquet_io.api.stac:validate_stac",
    "cli": "geoparquet_io.cli.main:cli",
    "GeoParquetError": "geoparquet_io.core.exceptions:GeoParquetError",
}

__all__ = [
    "cli",
    "read",
    "read_partition",
    "read_bigquery",
    "convert",
    "extract_arcgis",
    "list_layers",
    "Table",
    "pipe",
    "ops",
    "CheckResult",
    "generate_stac",
    "validate_stac",
    "GeoParquetError",
    "InvalidParameterError",
]


#: Subpackages reachable as attributes, as the eager imports left them. ``cli``
#: is absent on purpose: it resolves to the Click Group above, as it always has.
_LAZY_SUBPACKAGES = frozenset({"api", "core"})


def __getattr__(name: str):
    from importlib import import_module

    if name in _LAZY_SUBPACKAGES:
        # import_module sets the attribute on this package itself.
        return import_module(f"{__name__}.{name}")
    try:
        target = _LAZY_EXPORTS[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    module_name, attribute = target.split(":")
    value = getattr(import_module(module_name), attribute)
    # Cache it. For "cli" this also rebinds the attribute the import machinery
    # just set to the subpackage, restoring the eager-era shadowing.
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
