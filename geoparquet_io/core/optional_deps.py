"""Lazy import shims for the optional feature dependencies (ADR-0007).

The ``simplify`` and ``raster`` extras back ``gpio process simplify`` /
``polygonize`` / ``contour``. Each ``require_*`` function imports the package
on first need and raises :class:`OptionalDependencyError` with the exact
install commands when it is missing, so ``import geoparquet_io`` never touches
these packages and every other command works without them.

Docs: docs/guide/process-simplify.md, docs/guide/process-raster.md
"""

from __future__ import annotations

import importlib
import sys
from types import ModuleType

from geoparquet_io.core.exceptions import OptionalDependencyError


def _require(name: str, purpose: str, extra: str, note: str = "") -> ModuleType:
    try:
        return importlib.import_module(name)
    except ImportError as e:
        raise OptionalDependencyError(
            f"{name} is required for {purpose}. "
            f"Install with: pip install 'geoparquet-io[{extra}]' — "
            f"or: uv tool install geoparquet-io --with {name}.{note}"
        ) from e


def require_coarsen() -> ModuleType:
    """Return the coarsen module, or explain how to install it."""
    return _require("coarsen", "'gpio process simplify'", "simplify")


def require_contourrs() -> ModuleType:
    """Return the contourrs module, or explain how to install it."""
    note = ""
    if sys.version_info < (3, 12):
        # The [raster] extra's environment marker silently skips contourrs
        # here: it publishes no wheels for this Python.
        note = (
            " Note: contourrs requires Python >= 3.12 (it ships no wheels "
            "for this Python version)."
        )
    return _require(
        "contourrs",
        "'gpio process polygonize' and 'gpio process contour'",
        "raster",
        note,
    )


def require_rasterio() -> ModuleType:
    """Return the rasterio module, or explain how to install it."""
    return _require(
        "rasterio",
        "reading raster input in 'gpio process polygonize' and "
        "'gpio process contour'",
        "raster",
    )
