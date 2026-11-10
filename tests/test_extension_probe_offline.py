"""The extension-availability probe must skip, not fail, when offline (#1347).

``tests/conftest._community_extension_available`` drives the skip helpers
(``skip_if_grid_unavailable`` and friends). a5 on DuckDB 2.0 installs from the
Query.Farm repository over the network, so with no network the probe's INSTALL
fails with DuckDB's download error -- not the unpublished 404. That is still
"this environment cannot run the extension", so the probe reports unavailable
(the test skips) instead of erroring the test. Any other failure -- a corrupt
extension, a permission error -- must still raise and fail loudly.
"""

import os

import duckdb
import pytest

from geoparquet_io.core import duckdb_utils
from geoparquet_io.core.exceptions import ExtensionUnavailableError
from tests.conftest import _community_extension_available, _extension_download_failed

_OFFLINE_DETAIL = (
    'IO Error: Failed to download extension "a5" at URL '
    '"https://duckdb-extensions.query.farm/v2.0.0/osx_arm64/a5.duckdb_extension.gz" '
    "(ERROR Could not establish connection)"
)

# DuckDB 2.0's *cold* offline shape: with no persisted query_farm registration,
# the CREATE EXTENSION REPOSITORY statement itself fails fetching the repo
# metadata, before any extension download starts -- so this text does NOT say
# "Failed to download extension". Observed empirically on DuckDB 2.0.0.
_COLD_OFFLINE_DETAIL = (
    "installing 'a5' from the Query.Farm repository "
    "(https://duckdb-extensions.query.farm) failed: IO Error: "
    "Failed to read the metadata file of the extension repository "
    '"https://duckdb-extensions.query.farm": Could not resolve hostname '
    '"duckdb-extensions.query.farm"'
)


def _probe(monkeypatch, detail: str | None) -> bool:
    """Run the uncached probe against a require() that raises with ``detail``."""

    def fake_require(name, feature=None):
        raise ExtensionUnavailableError(name, "2.0.0", detail)

    monkeypatch.setattr(duckdb_utils, "require_community_extension", fake_require)
    # __wrapped__ bypasses the functools.cache so the fake is actually consulted
    # and no fake result is left cached for other tests.
    return _community_extension_available.__wrapped__("a5")


def test_download_failure_counts_as_unavailable(monkeypatch):
    assert _probe(monkeypatch, _OFFLINE_DETAIL) is False


def test_cold_start_metadata_failure_counts_as_unavailable(monkeypatch):
    """Offline with nothing cached, 2.0 fails before any download starts.

    The repository-metadata fetch error carries no "failed to download
    extension" phrase, so matching only that phrase made the probe raise in
    exactly the offline case this file exists to cover.
    """
    assert _probe(monkeypatch, _COLD_OFFLINE_DETAIL) is False


def test_other_failures_still_raise(monkeypatch):
    with pytest.raises(ExtensionUnavailableError):
        _probe(monkeypatch, "IO Error: Permission denied while writing extension file")


def test_download_failure_detector_walks_the_cause_chain():
    exc = ExtensionUnavailableError("a5", "2.0.0")
    exc.__cause__ = RuntimeError(_OFFLINE_DETAIL)
    assert _extension_download_failed(exc)
    assert not _extension_download_failed(ValueError("unrelated"))


def test_download_failure_detector_knows_the_repository_metadata_shape():
    assert _extension_download_failed(RuntimeError(_COLD_OFFLINE_DETAIL))


def test_a5_is_available_in_ci():
    """CI must FAIL, not skip, when a5 is expected but unavailable (#1347).

    The skip helpers are the right behavior for a developer's offline laptop,
    but in CI they would turn a Query.Farm outage or a registry regression
    into a green build that silently ran no a5 tests. This canary asserts the
    probe's answer instead of honoring it, so that failure mode turns CI red.
    """
    if not os.environ.get("GITHUB_ACTIONS"):
        pytest.skip("a5 availability canary is only meaningful in CI")
    assert _community_extension_available("a5"), (
        f"a5 must be installable in CI for DuckDB {duckdb.__version__}: the "
        "probe failing here means the community registry (DuckDB 1.5) or the "
        "Query.Farm repository (DuckDB 2.0+) did not deliver a loadable a5"
    )
