"""Lazy TOML loading for maintenance scripts and their tests."""

from pathlib import Path


def load_toml(path: Path) -> dict:
    """Read TOML lazily so dependency checks can run in an empty venv."""
    try:
        import tomllib
    except ModuleNotFoundError:  # Python 3.10
        import tomli as tomllib  # type: ignore[no-redef]

    with path.open("rb") as fh:
        return tomllib.load(fh)
