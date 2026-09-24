"""Tests for Overture Maps latest release fetching."""

from unittest.mock import patch

import pytest

import geoparquet_io.core.overture as overture_mod
from geoparquet_io.core.overture import get_latest_overture_release, get_overture_divisions_url


@pytest.fixture(autouse=True)
def _clear_overture_cache():
    """Clear the module-level release cache between tests."""
    overture_mod._cached_release = None
    yield
    overture_mod._cached_release = None


class TestGetLatestOvertureRelease:
    """Test fetching the latest Overture Maps release."""

    def test_returns_version_string(self):
        with patch("geoparquet_io.core.overture._fetch_latest_release") as mock:
            mock.return_value = "2026-05-20.0"
            version = get_latest_overture_release()
            assert version == "2026-05-20.0"

    def test_falls_back_on_failure(self):
        from geoparquet_io.core.overture import OVERTURE_FALLBACK_RELEASE

        with patch("geoparquet_io.core.overture._fetch_latest_release") as mock:
            mock.side_effect = Exception("network error")
            version = get_latest_overture_release()
            assert version == OVERTURE_FALLBACK_RELEASE

    def test_caches_fallback_to_avoid_repeated_timeouts(self):
        with patch("geoparquet_io.core.overture._fetch_latest_release") as mock:
            mock.side_effect = Exception("network error")
            get_latest_overture_release()
            get_latest_overture_release()
            assert mock.call_count == 1


class TestGetOvertureDivisionsUrl:
    """Test building the Overture divisions URL."""

    def test_url_contains_release(self):
        url = get_overture_divisions_url(release="2026-05-20.0")
        assert "2026-05-20.0" in url
        assert "theme=divisions" in url
        assert "type=division_area" in url

    def test_url_uses_latest_by_default(self):
        with patch("geoparquet_io.core.overture._fetch_latest_release") as mock:
            mock.return_value = "2099-01-01.0"
            url = get_overture_divisions_url()
            assert "2099-01-01.0" in url


class TestFetchLatestRelease:
    """The latest release comes from Overture's STAC catalog.

    Overture froze ``releases.json`` at 2026-07-22.0 and deleted that release
    from S3, so reading it resolves a path that no longer exists.
    """

    def test_reads_latest_from_stac_catalog(self):
        import io
        import json

        body = json.dumps({"type": "Catalog", "latest": "2026-08-19.0"}).encode()
        with patch("urllib.request.urlopen") as mock_open:
            mock_open.return_value.__enter__.return_value = io.BytesIO(body)
            assert overture_mod._fetch_latest_release() == "2026-08-19.0"
        requested = mock_open.call_args.args[0]
        assert requested == "https://stac.overturemaps.org/catalog.json"
