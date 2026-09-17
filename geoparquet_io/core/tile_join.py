"""tile-join: merging PMTiles archives.

Two callers need this and neither can import the other: ``pmtiles_pyramid``
merges per-band archives, and ``pmtiles`` merges the per-chunk archives of a
chunked ``pmtiles create`` (#1116). ``pmtiles_pyramid`` already imports
``pmtiles``, so the shared helpers live here rather than in either.
"""

from __future__ import annotations

import shutil
import subprocess

from geoparquet_io.core.logging_config import debug


class TileJoinNotFoundError(Exception):
    """Raised when tile-join is not found in PATH."""

    def __init__(self):
        super().__init__(
            "tile-join not found in PATH.\n\n"
            "tile-join ships with tippecanoe; gpio needs it to merge the\n"
            "per-level or per-chunk archives. Install tippecanoe:\n"
            "  macOS:  brew install tippecanoe\n"
            "  Ubuntu: sudo apt install tippecanoe\n"
            "  Source: https://github.com/felt/tippecanoe#installation"
        )


def _check_tile_join() -> bool:
    """Check if tile-join is available in PATH."""
    return shutil.which("tile-join") is not None


def _build_tile_join_command(
    output_path: str,
    band_files: list[str],
    *,
    name: str,
    attribution: str | None = None,
    force: bool = False,
) -> list[str]:
    """argv for merging archives (never joined through a shell).

    No scratch control here on purpose: tile-join accepts neither ``-t`` nor
    ``--temporary-directory`` and exits 101 on either (#1115). Callers steer
    tippecanoe's scratch and their own intermediates; tile-join's spill stays
    where upstream puts it.
    """
    cmd = ["tile-join", "-o", output_path, "-pk"]
    if force:
        cmd.append("--force")
    cmd.append(f"--name={name}")
    if attribution:
        cmd.append(f"--attribution={attribution}")
    cmd.extend(band_files)
    return cmd


def _run_tile_join(cmd: list[str], verbose: bool) -> None:
    if verbose:
        debug(f"Running: {' '.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        stderr = proc.stderr.strip()
        raise RuntimeError(
            f"tile-join failed with exit code {proc.returncode}"
            + (f"\nstderr:\n{stderr}" if stderr else "")
        )
    if verbose and proc.stderr.strip():
        debug(proc.stderr.strip())
