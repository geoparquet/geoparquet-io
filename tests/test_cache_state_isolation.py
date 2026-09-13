"""Every test must start from a cold ``geoparquet_io`` cache.

The companion to ``tests/test_logging_state_isolation.py``. That one pins the
*logger* half of #1016's test-order leakage; this one pins the cache half,
which is worse: a logger decides how a message is rendered, a warn-once cache
decides whether the message exists.

``tests/conftest.py`` clears the caches named in its ``PACKAGE_CACHES`` before
each test. Two things are proven here: that the tuple is complete (a walk of
the package finds nothing it does not name), and that the clear beats a
higher-scoped fixture's poisoning, which is the ordering that broke #1016's
first attempt.
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil
from collections.abc import Iterator

import pytest

import geoparquet_io
from geoparquet_io.core import crs_utils, geo_metadata
from geoparquet_io.core.file_type import detect_geoparquet_file_type
from geoparquet_io.core.write_strategies import WriteStrategy, WriteStrategyFactory
from tests.conftest import PACKAGE_CACHES, TEST_DATA_DIR, clear_package_caches

# =============================================================================
# The tuple is complete
# =============================================================================


def _cache_qualnames(module) -> Iterator[str]:
    """Yield the qualified name of every ``cache_clear``-bearing callable in ``module``.

    ``__module__`` filtering keeps a re-export (``core/common.py`` re-exports a
    good deal of the split-out modules) from yielding one cache twice. Class
    members go through :func:`inspect.getattr_static` and are unwrapped from
    their ``classmethod``: a plain ``getattr`` would run any ``property`` on the
    way past, and the factory's cache is a ``classmethod`` around an ``lru_cache``.
    """
    for attr, obj in vars(module).items():
        if getattr(obj, "__module__", None) != module.__name__:
            continue
        if inspect.isclass(obj):
            for member_name in vars(obj):
                member = inspect.getattr_static(obj, member_name)
                member = getattr(member, "__func__", member)
                if callable(member) and hasattr(member, "cache_clear"):
                    yield f"{module.__name__}.{obj.__qualname__}.{member_name}"
        elif callable(obj) and hasattr(obj, "cache_clear"):
            yield f"{module.__name__}.{getattr(obj, '__qualname__', attr)}"


def _walk_package_caches() -> set[str]:
    """Every cached callable in ``geoparquet_io``, found by importing the package."""
    modules = [geoparquet_io]
    for info in pkgutil.walk_packages(
        geoparquet_io.__path__, prefix="geoparquet_io.", onerror=lambda name: None
    ):
        try:
            modules.append(importlib.import_module(info.name))
        except ImportError:  # an optional dependency's module holds no live cache
            continue
    return {name for module in modules for name in _cache_qualnames(module)}


def _pinned_qualnames() -> set[str]:
    return {f"{cache.__module__}.{cache.__qualname__}" for cache in PACKAGE_CACHES}


def test_every_cache_in_the_package_is_in_the_tuple_conftest_clears():
    found, pinned = _walk_package_caches(), _pinned_qualnames()
    assert found == pinned, (
        f"tests/conftest.py PACKAGE_CACHES is out of date.\n"
        f"  in the package but not cleared per test: {sorted(found - pinned) or '-'}\n"
        f"  cleared per test but gone from the package: {sorted(pinned - found) or '-'}\n"
        "Add (or remove) the entry there; per-test clearing is correct for any cache "
        "that recomputes from its arguments."
    )


def test_the_walk_sees_the_classmethod_cache_and_the_re_export_once():
    """The two shapes a naive ``vars(module)`` walk gets wrong.

    ``WriteStrategyFactory.get_strategy`` is ``classmethod(lru_cache(...))``, a
    class member. ``detect_geoparquet_file_type`` is re-exported by
    ``core/common.py`` and must be counted once, under its defining module.
    """
    from geoparquet_io.core import common

    found = _walk_package_caches()
    assert "geoparquet_io.core.write_strategies.WriteStrategyFactory.get_strategy" in found
    assert common.detect_geoparquet_file_type is detect_geoparquet_file_type
    assert [n for n in found if n.endswith(".detect_geoparquet_file_type")] == [
        "geoparquet_io.core.file_type.detect_geoparquet_file_type"
    ]


# =============================================================================
# The clear beats a higher-scoped fixture
# =============================================================================

POISON_SOURCE = "tests/data/poisoned-by-a-higher-scoped-fixture.parquet"


def _warm_every_cache() -> None:
    """Burn the exact entries the test below depends on being absent.

    Warmed through the public callers rather than by poking the caches
    directly, so the keys are the real ones; a warm entry under some *other*
    key would leave the test passing for the wrong reason.
    """
    geo_metadata.carried_column_name(123, source=POISON_SOURCE)
    crs_utils.warn_null_crs_once(POISON_SOURCE)
    crs_utils._emit_crs_disagreement_warning(POISON_SOURCE, "EPSG:4326", "EPSG:3857")
    crs_utils._projjson_from_authority("EPSG", "4326")
    detect_geoparquet_file_type(str(TEST_DATA_DIR / "buildings_test.parquet"))
    WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_MEMORY)


def _entries(cache) -> int:
    info = getattr(cache, "cache_info", None)
    if info is not None:
        return info().currsize
    from geoparquet_io.core import file_type  # the hand-rolled dict cache

    return len(file_type._file_type_cache)


@pytest.fixture(scope="class")
def _poisoned_worker():
    """Pre-warm every cache from a scope wider than ``function``.

    That width is the whole point: every fixture wider than ``function`` is
    set up before any function-scoped one, so the poison is already in place
    by the time the autouse guard in ``tests/conftest.py`` gets to look.
    ``test_add.py`` poisons the *logger* from a module-scoped fixture in
    exactly this shape.

    Class-scoped, and the class holds exactly one test: a second test in the
    same class would run against a cache the first one's guard had already
    emptied, and would prove nothing.
    """
    _warm_every_cache()
    yield
    clear_package_caches()


@pytest.mark.usefixtures("_poisoned_worker")
class TestAPoisonedWorkerStillWarns:
    def test_every_cache_is_cold_and_all_three_warnings_fire_again(self, caplog):
        """#883's malformed-key warning and both CRS warnings, on a file already hit.

        Each goes through its public caller with the same key the poisoning
        used. Without the guard every one of these logs nothing at all and the
        user is never told the file is broken.
        """
        for cache in PACKAGE_CACHES:
            assert _entries(cache) == 0, f"{cache.__qualname__} was warm at test start"

        with caplog.at_level("WARNING", logger="geoparquet_io"):
            assert geo_metadata.carried_column_name(123, source=POISON_SOURCE) is None
            crs_utils.warn_null_crs_once(POISON_SOURCE)
            crs_utils._emit_crs_disagreement_warning(POISON_SOURCE, "EPSG:4326", "EPSG:3857")

        assert "Ignoring malformed 'geo' metadata" in caplog.text
        assert POISON_SOURCE in caplog.text
        assert "explicit null CRS" in caplog.text
        assert "the geo metadata says the CRS is" in caplog.text
