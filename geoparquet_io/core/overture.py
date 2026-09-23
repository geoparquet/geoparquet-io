"""Overture Maps release utilities."""

from __future__ import annotations

from geoparquet_io.core.logging_config import debug, warn

# Overture's STAC root names the latest release. Its older releases.json is frozen
# at a release since deleted from S3, so it must not be read.
OVERTURE_RELEASES_URL = "https://stac.overturemaps.org/catalog.json"
OVERTURE_FALLBACK_RELEASE = "2026-08-19.0"
OVERTURE_S3_TEMPLATE = (
    "s3://overturemaps-us-west-2/release/{release}/theme=divisions/type=division_area/*"
)

_cached_release: str | None = None


def _fetch_latest_release() -> str:
    """Fetch the latest Overture Maps release version from the API."""
    import json
    import urllib.request

    with urllib.request.urlopen(OVERTURE_RELEASES_URL, timeout=5) as resp:  # nosec B310
        data = json.loads(resp.read())
    return str(data["latest"])


def get_latest_overture_release(verbose: bool = False) -> str:
    """Get the latest Overture Maps release, with caching and fallback."""
    global _cached_release
    if _cached_release is not None:
        return _cached_release

    try:
        release = _fetch_latest_release()
        _cached_release = release
        if verbose:
            debug(f"Using Overture Maps release: {release}")
        return release
    except Exception as e:
        warn(
            f"Could not fetch latest Overture release ({e}), "
            f"using fallback: {OVERTURE_FALLBACK_RELEASE}"
        )
        _cached_release = OVERTURE_FALLBACK_RELEASE
        return OVERTURE_FALLBACK_RELEASE


def get_overture_divisions_url(release: str | None = None, verbose: bool = False) -> str:
    """Get the S3 URL for Overture Maps division boundaries."""
    if release is None:
        release = get_latest_overture_release(verbose=verbose)
    return OVERTURE_S3_TEMPLATE.format(release=release)
