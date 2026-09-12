"""Every test must start from a cold ``geoparquet_io`` cache.

The companion to ``tests/test_logging_state_isolation.py``. That one pins the
*logger* half of #1016's test-order leakage; this one pins the ``lru_cache``
half, which is worse: a logger decides how a message is rendered, a warn-once
cache decides whether the message exists.

``geo_metadata._emit_malformed_geo_warning``,
``crs_utils._emit_null_crs_warning`` and
``crs_utils._emit_crs_disagreement_warning`` are keyed on the input, so two
tests that hand the same fixture to the same code path share one cache entry.
Whichever runs second sees no warning at all, and an assertion of the form "the
user is told about this file" becomes a function of the xdist schedule. All
three ship a hand-written ``reset_*`` helper documented as "Intended for
tests" -- the hazard was known; nothing enforced the reset, and only three test
files ever called one, always inline.

``tests/conftest.py`` now clears every cache in the package before each test.
Discovery is by walking the package, so a cache added tomorrow is covered
without anybody remembering to add it here; this file pins the resulting set by
name so an *unexpected* new cache is noticed rather than silently swept in.
"""

from __future__ import annotations

import time

import pytest

from geoparquet_io.core import crs_utils, geo_metadata
from geoparquet_io.core.file_type import detect_geoparquet_file_type
from geoparquet_io.core.write_strategies import WriteStrategy, WriteStrategyFactory
from tests.conftest import (
    TEST_DATA_DIR,
    clear_package_caches,
    discover_package_caches,
)

# Every cached callable in the package, as of this commit. Sorted, and written
# out in full rather than counted: the point is to notice a *new* cache, and a
# count alone cannot say which one arrived.
#
# The first three are warn-once dedup caches -- the ones that silently decide an
# assertion. The last three are performance caches, keyed on a path or an enum,
# so a test that writes a new file at a path an earlier test used would
# otherwise read the earlier test's answer.
EXPECTED_CACHES = {
    "geoparquet_io.core.crs_utils._emit_crs_disagreement_warning",
    "geoparquet_io.core.crs_utils._emit_null_crs_warning",
    "geoparquet_io.core.crs_utils._projjson_from_authority",
    "geoparquet_io.core.file_type.detect_geoparquet_file_type",
    "geoparquet_io.core.geo_metadata._emit_malformed_geo_warning",
    "geoparquet_io.core.write_strategies.WriteStrategyFactory.get_strategy",
}

WARN_ONCE_CACHES = {
    "geoparquet_io.core.crs_utils._emit_crs_disagreement_warning",
    "geoparquet_io.core.crs_utils._emit_null_crs_warning",
    "geoparquet_io.core.geo_metadata._emit_malformed_geo_warning",
}


# =============================================================================
# Discovery
# =============================================================================


class TestDiscoveryFindsTheCachesByWalkingThePackage:
    def test_the_discovered_set_is_exactly_what_is_pinned_here(self):
        """A new cache is isolated automatically -- and noticed here.

        Update ``EXPECTED_CACHES`` when you add one, having first checked that
        clearing it per test is correct (it is, for any cache that recomputes
        from its arguments) and cheap.
        """
        assert set(discover_package_caches()) == EXPECTED_CACHES

    def test_every_discovered_object_can_actually_be_cleared(self):
        for name, cache in discover_package_caches().items():
            assert callable(getattr(cache, "cache_clear", None)), name

    def test_a_re_export_is_not_counted_twice(self):
        """``core/common.py`` re-exports much of the post-3.4 split.

        Discovery keys on ``__module__``, so ``detect_geoparquet_file_type``
        appears once, under the module that defines it.
        """
        from geoparquet_io.core import common

        assert common.detect_geoparquet_file_type is detect_geoparquet_file_type
        names = [n for n in discover_package_caches() if n.endswith(".detect_geoparquet_file_type")]
        assert names == ["geoparquet_io.core.file_type.detect_geoparquet_file_type"]

    def test_the_classmethod_cache_is_found_despite_its_wrapper(self):
        """``WriteStrategyFactory.get_strategy`` is ``classmethod(lru_cache(...))``.

        A walk over ``vars(module)`` alone misses it -- it is a class member,
        and the ``classmethod`` has to be unwrapped before the ``lru_cache``
        wrapper underneath is visible.
        """
        assert (
            "geoparquet_io.core.write_strategies.WriteStrategyFactory.get_strategy"
            in discover_package_caches()
        )


# =============================================================================
# The guard itself
# =============================================================================


class TestTheGuardEmptiesEveryCache:
    def test_a_warm_warn_once_cache_is_empty_after_a_clear(self):
        geo_metadata._emit_malformed_geo_warning("not an object", "some/file.parquet")
        crs_utils._emit_null_crs_warning("some/file.parquet")
        crs_utils._emit_crs_disagreement_warning("some/file.parquet", "EPSG:4326", "EPSG:3857")
        assert geo_metadata._emit_malformed_geo_warning.cache_info().currsize == 1

        clear_package_caches()

        for name in WARN_ONCE_CACHES:
            assert discover_package_caches()[name].cache_info().currsize == 0, name

    def test_the_strategy_factory_cache_is_emptied_too(self):
        WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_MEMORY)
        assert WriteStrategyFactory.get_strategy.cache_info().currsize == 1

        clear_package_caches()

        assert WriteStrategyFactory.get_strategy.cache_info().currsize == 0

    def test_the_hand_rolled_file_type_cache_is_emptied_too(self):
        """``detect_geoparquet_file_type`` is a dict with ``cache_clear`` bolted on.

        Keyed on the path, so a test writing a *different* file to a path an
        earlier test used reads the earlier test's verdict back.
        """
        from geoparquet_io.core import file_type

        detect_geoparquet_file_type(str(TEST_DATA_DIR / "buildings_test.parquet"))
        assert file_type._file_type_cache

        clear_package_caches()

        assert not file_type._file_type_cache

    def test_a_pyproj_lookup_cache_is_emptied_too(self):
        crs_utils._projjson_from_authority("EPSG", "4326")
        assert crs_utils._projjson_from_authority.cache_info().currsize == 1

        clear_package_caches()

        assert crs_utils._projjson_from_authority.cache_info().currsize == 0


# =============================================================================
# The leak, in the shape that hides a warning
# =============================================================================

POISON_SOURCE = "tests/data/poisoned-by-a-higher-scoped-fixture.parquet"


def _warm_every_warn_once_cache() -> None:
    """Burn the exact entries the tests below depend on being absent.

    Warmed through the public callers rather than by poking the caches
    directly, so the keys are the real ones -- a warm entry under some *other*
    key would leave every test below passing for the wrong reason.
    """
    geo_metadata.carried_column_name(123, source=POISON_SOURCE)
    crs_utils.warn_null_crs_once(POISON_SOURCE)
    crs_utils._emit_crs_disagreement_warning(POISON_SOURCE, "EPSG:4326", "EPSG:3857")


@pytest.fixture(scope="class")
def _poisoned_worker():
    """Pre-warm the warn-once caches from a scope wider than ``function``.

    That width is the whole point, and it is the ordering that broke #1016's
    first attempt: every fixture wider than ``function`` is set up before any
    function-scoped one, so the poison is already in place by the time the
    autouse guard in ``tests/conftest.py`` gets to look. ``test_add.py``
    poisons the *logger* from a module-scoped fixture in exactly this shape.

    Class-scoped rather than module-scoped for two reasons. A sibling test
    would otherwise wash the poison away -- ``TestTheGuardEmptiesEveryCache``
    calls ``clear_package_caches()`` itself, and a shuffled run decides which
    of them goes first. And a class-scoped fixture is set up once per class, so
    each class below holds exactly one test: a second test in the same class
    would run against a cache the *first* one's guard had already emptied, and
    would prove nothing.
    """
    _warm_every_warn_once_cache()
    yield
    clear_package_caches()


@pytest.mark.usefixtures("_poisoned_worker")
class TestAPoisonedWorkerStillWarns:
    """The real warnings, re-emitted for a file an earlier caller already hit."""

    def test_all_three_warn_once_warnings_survive_the_poisoning(self, caplog):
        """#883's malformed-key warning, and both CRS warnings, on one file.

        Each goes through its public caller with the same key the poisoning
        used. Without the guard every one of these three logs nothing at all
        and the user is never told the file is broken.
        """
        with caplog.at_level("WARNING", logger="geoparquet_io"):
            assert geo_metadata.carried_column_name(123, source=POISON_SOURCE) is None
            crs_utils.warn_null_crs_once(POISON_SOURCE)
            crs_utils._emit_crs_disagreement_warning(POISON_SOURCE, "EPSG:4326", "EPSG:3857")

        assert "Ignoring malformed 'geo' metadata" in caplog.text
        assert POISON_SOURCE in caplog.text
        assert "explicit null CRS" in caplog.text
        assert "the geo metadata says the CRS is" in caplog.text


@pytest.mark.usefixtures("_poisoned_worker")
class TestAPoisonedWorkerStartsCold:
    def test_the_caches_were_empty_when_this_test_started(self):
        """The invariant itself, read straight off the caches."""
        for name in WARN_ONCE_CACHES:
            assert discover_package_caches()[name].cache_info().currsize == 0, name


# =============================================================================
# Cost
# =============================================================================


def test_clearing_every_cache_costs_almost_nothing_per_test():
    """The guard runs once per test, ~7,800 times a suite, so per-test is the budget.

    Measured at 0.22 microseconds on a 12-core macOS laptop -- six
    ``cache_clear`` calls over an already-built dict -- which is 1.7 ms across
    the whole fast suite. The one-time discovery walk costs 66 ms per worker
    process on top, most of the package having been imported by the test
    modules already.

    The bound below is three orders of magnitude above the measurement so a
    loaded CI box cannot make it flap. It is here to catch a future guard that
    starts doing real work per test, not to benchmark this one.
    """
    discover_package_caches()  # discovery is one-time; do not time the import walk

    start = time.perf_counter()
    for _ in range(200):
        clear_package_caches()
    per_call_ms = (time.perf_counter() - start) * 1000 / 200

    assert per_call_ms < 1.0, f"{per_call_ms:.4f} ms per test is not negligible"
