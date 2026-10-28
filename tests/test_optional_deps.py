"""Tests for the optional-dependency import shims (core/optional_deps.py).

These run on every CI leg regardless of which optional packages are
installed: absence is simulated by poisoning ``sys.modules`` and presence by
planting a fake module, so the shims' behavior is pinned even on legs where
contourrs cannot install (Python < 3.12).
"""

import sys
import types

import pytest

from geoparquet_io.core.exceptions import GeoParquetError, OptionalDependencyError
from geoparquet_io.core.optional_deps import (
    require_coarsen,
    require_contourrs,
    require_rasterio,
)


def _fake_module(name):
    return types.ModuleType(name)


class TestErrorType:
    def test_is_geoparquet_error(self):
        assert issubclass(OptionalDependencyError, GeoParquetError)


class TestPresent:
    """When the package imports, the shim returns the module."""

    def test_coarsen(self, monkeypatch):
        fake = _fake_module("coarsen")
        monkeypatch.setitem(sys.modules, "coarsen", fake)
        assert require_coarsen() is fake

    def test_contourrs(self, monkeypatch):
        fake = _fake_module("contourrs")
        monkeypatch.setitem(sys.modules, "contourrs", fake)
        assert require_contourrs() is fake

    def test_rasterio(self, monkeypatch):
        fake = _fake_module("rasterio")
        monkeypatch.setitem(sys.modules, "rasterio", fake)
        assert require_rasterio() is fake


class TestMissing:
    """When the package is absent, the shim raises with install instructions."""

    def test_coarsen_message(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "coarsen", None)
        with pytest.raises(OptionalDependencyError) as exc_info:
            require_coarsen()
        msg = str(exc_info.value)
        assert "coarsen is required" in msg
        assert "gpio process simplify" in msg
        assert "pip install 'geoparquet-io[simplify]'" in msg
        assert "uv tool install geoparquet-io --with coarsen" in msg

    def test_contourrs_message(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "contourrs", None)
        with pytest.raises(OptionalDependencyError) as exc_info:
            require_contourrs()
        msg = str(exc_info.value)
        assert "contourrs is required" in msg
        assert "gpio process polygonize" in msg
        assert "gpio process contour" in msg
        assert "pip install 'geoparquet-io[raster]'" in msg
        assert "uv tool install geoparquet-io --with contourrs" in msg

    def test_contourrs_python_floor_note_on_old_python(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "contourrs", None)
        monkeypatch.setattr(sys, "version_info", (3, 11, 9, "final", 0))
        with pytest.raises(OptionalDependencyError) as exc_info:
            require_contourrs()
        assert "requires Python >= 3.12" in str(exc_info.value)

    def test_contourrs_no_floor_note_on_new_python(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "contourrs", None)
        monkeypatch.setattr(sys, "version_info", (3, 12, 0, "final", 0))
        with pytest.raises(OptionalDependencyError) as exc_info:
            require_contourrs()
        assert "requires Python >= 3.12" not in str(exc_info.value)

    def test_rasterio_message(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "rasterio", None)
        with pytest.raises(OptionalDependencyError) as exc_info:
            require_rasterio()
        msg = str(exc_info.value)
        assert "rasterio is required" in msg
        assert "pip install 'geoparquet-io[raster]'" in msg
        assert "uv tool install geoparquet-io --with rasterio" in msg

    def test_chains_the_import_error(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "coarsen", None)
        with pytest.raises(OptionalDependencyError) as exc_info:
            require_coarsen()
        assert isinstance(exc_info.value.__cause__, ImportError)
