"""Shared plumbing for the pre-commit hook self-tests.

Three test modules now run a hook's own script out of ``.pre-commit-config.yaml``
against a synthetic ``geoparquet_io/core/`` tree, and each had grown its own copy
of the same four pieces: find the hook entry, pull its script out, decide whether
``bash`` can run it, and build the throwaway tree. #970 asked for the extraction
while adding the third copy.

The pattern these modules share, and why it is worth the plumbing: a guard that
silently exempts more than it names reads as protection while providing none
(#946, #970). The only way to know a guard still bites is to run its real script
-- not a re-implementation of it -- over source that should fail.

Not a ``conftest.py``: these are plain helpers, and importing them by name says
where they come from. The leading underscore keeps pytest from collecting it.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).parent.parent


def hook_entry(hook_id: str) -> dict:
    """The named hook's own YAML entry from ``.pre-commit-config.yaml``."""
    config = yaml.safe_load((REPO_ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8"))
    for repo in config["repos"]:
        for hook in repo.get("hooks", []):
            if hook["id"] == hook_id:
                return hook
    raise AssertionError(f"{hook_id} hook not found in .pre-commit-config.yaml")


def hook_script(hook_id: str) -> str:
    """The named hook's script, exactly as pre-commit invokes it."""
    return str(hook_entry(hook_id)["args"][-1])


def bash_can_run_scripts() -> bool:
    """Whether ``bash`` on this platform can actually execute a script.

    ``shutil.which("bash")`` is not enough on Windows runners: there ``bash``
    resolves to WSL's ``bash.exe``, which exits non-zero with an "install a
    distribution" notice when no WSL distro is present -- which would fail the
    tests that expect a clean tree and pass every rejection test for entirely
    the wrong reason.
    """
    try:
        probe = subprocess.run(["bash", "-c", "exit 0"], capture_output=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return False
    return probe.returncode == 0


needs_bash = pytest.mark.skipif(
    not bash_can_run_scripts(),
    reason="pre-commit hook scripts are POSIX shell; no usable bash on this platform",
)


def run_hook_script(script: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    """Run a hook script with *cwd* as the repo root it scans."""
    return subprocess.run(
        ["bash", "-c", script],
        cwd=cwd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def core_tree(tmp_path: Path, files: dict[str, str]) -> Path:
    """Build an isolated ``geoparquet_io/core/`` tree and return its repo root.

    Keys are paths relative to ``geoparquet_io/core/``, so a key may name a
    subdirectory (``"partition/common.py"``) to test that an exemption is
    scoped to one path rather than to any file with that basename.

    The tree is synthetic rather than the repo's own because the suite runs
    under pytest-xdist: a test that planted hostile source in the real tree
    would fail every other worker.
    """
    core_dir = tmp_path / "geoparquet_io" / "core"
    core_dir.mkdir(parents=True)
    for name, content in files.items():
        target = core_dir / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    return tmp_path
