"""The in-tree plugins import against the current core surface (#1367).

A plugin lives outside ``geoparquet_io/``, so a name deleted from the package
(``handle_geoparquet_errors`` was one) is not caught by the package's own tests.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

PLUGINS = Path(__file__).resolve().parent.parent / "plugins"


@pytest.mark.parametrize("plugin", sorted(p.name for p in PLUGINS.iterdir() if p.is_dir()))
def test_the_plugin_cli_module_imports(plugin, monkeypatch):
    package = plugin.replace("-", "_")
    root = PLUGINS / plugin
    if not (root / package / "cli.py").exists():
        pytest.skip(f"{plugin} has no cli module")
    monkeypatch.syspath_prepend(str(root))
    for name in [m for m in sys.modules if m == package or m.startswith(package + ".")]:
        monkeypatch.delitem(sys.modules, name)
    module = importlib.import_module(f"{package}.cli")
    assert module is not None
