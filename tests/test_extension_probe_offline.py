"""The extension-availability probe must skip, not fail, when offline (#1347).

``tests/conftest._community_extension_available`` drives the skip helpers
(``skip_if_grid_unavailable`` and friends). a5 on DuckDB 2.0 installs from the
Query.Farm repository over the network, so with no network the probe's INSTALL
fails with DuckDB's download error -- not the unpublished 404. That is still
"this environment cannot run the extension", so the probe reports unavailable
(the test skips) instead of erroring the test. Any other failure -- a corrupt
extension, a permission error -- must still raise and fail loudly.
"""

import pytest

from geoparquet_io.core import duckdb_utils
from geoparquet_io.core.exceptions import ExtensionUnavailableError
from tests.conftest import _community_extension_available, _extension_download_failed

_OFFLINE_DETAIL = (
    'IO Error: Failed to download extension "a5" at URL '
    '"https://duckdb-extensions.query.farm/v2.0.0/osx_arm64/a5.duckdb_extension.gz" '
    "(ERROR Could not establish connection)"
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


def test_other_failures_still_raise(monkeypatch):
    with pytest.raises(ExtensionUnavailableError):
        _probe(monkeypatch, "IO Error: Permission denied while writing extension file")


def test_download_failure_detector_walks_the_cause_chain():
    exc = ExtensionUnavailableError("a5", "2.0.0")
    exc.__cause__ = RuntimeError(_OFFLINE_DETAIL)
    assert _extension_download_failed(exc)
    assert not _extension_download_failed(ValueError("unrelated"))
