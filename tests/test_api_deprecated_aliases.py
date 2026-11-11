"""Deprecated API name aliases (#1436, decision 4).

Two twins were spelled differently on their two API legs: ``ops.read_bigquery``
against ``Table.from_bigquery``, and ``ops.create_overviews`` against
``Table.overview``. Each collapsed to one canonical name, and the old spelling
stays for one minor release as an alias built by
``api._deprecation.deprecated_alias`` -- the per-keyword deprecation mechanism
``remote_options_from_keywords`` and ``check_options_from_keywords`` already
use, raised from a keyword to a whole name.

What is pinned here: the alias forwards the call to the same core function the
canonical name reaches, it warns once per process naming the replacement and the
horizon, and the canonical name warns about nothing.
"""

from __future__ import annotations

import inspect
import warnings
from contextlib import contextmanager
from unittest.mock import patch

import pyarrow as pa
import pytest

import geoparquet_io as gpio
from geoparquet_io.api import _deprecation, ops
from geoparquet_io.api.table import Table
from geoparquet_io.core import extract_bigquery as core_bigquery
from geoparquet_io.core.process import overview as core_overview
from tests import parity_registry as reg

_CELLS = pa.table({"h3_cell": ["8928308280fffff"], "count": [3]})


@contextmanager
def fresh_deprecations():
    """Record warnings with an empty "warned once" ledger, then put it back.

    The warning fires once per process, so a test that asserts on it has to
    start from a clean ledger -- and a test that asserts on its *absence* has
    to, too, or it passes only because an earlier test warned first. The same
    contract tests/test_remote_options.py works under.
    """
    saved = set(_deprecation._warned_aliases)
    _deprecation._warned_aliases.clear()
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            yield caught
    finally:
        _deprecation._warned_aliases.clear()
        _deprecation._warned_aliases.update(saved)


def deprecations(caught):
    """The ``DeprecationWarning`` messages among *caught*, sorted."""
    return sorted(str(w.message) for w in caught if w.category is DeprecationWarning)


def _call_bigquery(name: str):
    """Call ``ops.<name>`` with the BigQuery core patched out; returns the core mock."""
    with patch.object(core_bigquery, "extract_bigquery", return_value=_CELLS) as core:
        getattr(ops, name)("p.d.t", limit=5)
    return core


def _call_overview(name: str):
    """Call ``Table.<name>`` with the rollup core patched out; returns the core mock."""
    table = Table(_CELLS)
    with patch.object(core_overview, "rollup_table", return_value=_CELLS) as core:
        getattr(table, name)(4, scheme="h3")
    return core


#: Per collapsed twin: the old dotted name, the alias, the canonical callable,
#: and a one-call driver that reaches core under a given attribute name.
ALIASES = [
    pytest.param(
        "ops.read_bigquery", ops.read_bigquery, ops.from_bigquery, _call_bigquery, id="bigquery"
    ),
    pytest.param(
        "Table.overview", Table.overview, Table.create_overviews, _call_overview, id="overview"
    ),
]


# ---------------------------------------------------------------------------
# The mechanism
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("old", "alias", "canonical", "call"), ALIASES)
def test_the_alias_is_marked_with_its_canonical_name(old, alias, canonical, call):
    """The marker is what the parity census reads, so it is part of the contract."""
    target = _deprecation.alias_target(alias)
    assert target == reg.DEPRECATED_ALIASES[old][0]
    assert target.rsplit(".", 1)[-1] == canonical.__name__


@pytest.mark.parametrize(("old", "alias", "canonical", "call"), ALIASES)
def test_the_alias_keeps_the_canonical_signature(old, alias, canonical, call):
    """``functools.wraps`` keeps ``__wrapped__``, so callers and docs tooling agree."""
    assert inspect.signature(alias) == inspect.signature(canonical)
    assert alias.__name__ == old.rsplit(".", 1)[-1]
    assert f"Deprecated alias of ``{reg.DEPRECATED_ALIASES[old][0]}``" in alias.__doc__


@pytest.mark.parametrize(("old", "alias", "canonical", "call"), ALIASES)
def test_the_alias_warns_once_per_process(old, alias, canonical, call):
    name = old.rsplit(".", 1)[-1]
    with fresh_deprecations() as caught:
        for _ in range(3):
            call(name)
        messages = deprecations(caught)
    assert len(messages) == 1, messages
    assert messages[0] == (
        f"{old}() is deprecated and will be removed in the next minor release; "
        f"call {reg.DEPRECATED_ALIASES[old][0]}() instead"
    )


@pytest.mark.parametrize(("old", "alias", "canonical", "call"), ALIASES)
def test_the_canonical_name_warns_about_nothing(old, alias, canonical, call):
    with fresh_deprecations() as caught:
        call(canonical.__name__)
        assert deprecations(caught) == []


# ---------------------------------------------------------------------------
# Forwarding: the alias is the same operation, not a stub
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("old", "alias", "canonical", "call"), ALIASES)
def test_the_alias_hands_core_the_call_the_canonical_name_does(old, alias, canonical, call):
    with fresh_deprecations():
        via_alias = call(old.rsplit(".", 1)[-1])
        via_canonical = call(canonical.__name__)
    assert via_alias.call_args == via_canonical.call_args
    assert via_alias.call_count == 1


def test_the_root_keeps_both_bigquery_spellings():
    """`gpio.from_bigquery` is the canonical root export; the old name still imports."""
    assert "from_bigquery" in gpio.__all__
    assert "read_bigquery" in gpio.__all__
    assert gpio.from_bigquery is ops.from_bigquery
    assert gpio.read_bigquery is ops.read_bigquery
