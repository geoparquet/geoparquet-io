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
same way). A plain PEP 562 ``__getattr__`` cannot keep that promise in every
import order -- importing any ``geoparquet_io.cli.*`` module makes the import
machinery bind the subpackage as this module's ``cli`` attribute, and the
hook never fires for a name that exists -- so ``cli`` is a property on the
module's own class below, which outranks the instance ``__dict__`` whatever
was imported first. tests/test_patch_target_shadowing.py guards the
consequences for ``mock.patch`` targets.
"""

import sys as _sys
import types as _types

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


def __getattr__(name: str):
    from importlib import import_module

    if name == "api":
        # import_module sets the attribute on this package itself. ``core`` is
        # imported eagerly above and ``cli`` is the property below, so this is
        # the one subpackage reached lazily.
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


class _RootModule(_types.ModuleType):
    """The package module's own class, so ``cli`` can be a data descriptor.

    A property outranks the instance ``__dict__``, which is what keeps
    ``geoparquet_io.cli`` the Click Group even after the import machinery has
    bound the ``cli`` subpackage as an attribute (it does so whenever any
    ``geoparquet_io.cli.*`` module is imported, in any process, and a module
    ``__getattr__`` never fires for a name that exists).
    """

    @property
    def cli(self):
        from geoparquet_io.cli.main import cli as click_group

        return click_group

    @cli.setter
    def cli(self, value) -> None:
        # The import machinery assigns the ``cli`` subpackage here when a
        # ``geoparquet_io.cli.*`` module is imported. The property keeps
        # answering with the Group, so the binding is deliberately dropped;
        # without a setter the assignment would raise and the import machinery
        # would downgrade that to an ImportWarning.
        return


_sys.modules[__name__].__class__ = _RootModule
