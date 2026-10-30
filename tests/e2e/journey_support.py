"""Shared plumbing for the end-to-end journey suite (issue #667, deliverable 4).

Everything here drives the INSTALLED ``gpio`` binary through ``subprocess`` --
never the Click test runner -- so the journeys cover the real entry point,
argument parsing, stdout streaming and process exit codes. It mirrors the
pattern already used by ``tests/test_pipe_integration.py`` and
``tests/test_pmtiles.py``.

Oracle note (this is the load-bearing part of the suite): ``gpio check all``
exits 0 even when spec validation FAILS -- a file with three failed spec checks
still returns 0 and only prints ``✗ 3 failed, 25 passed``. Asserting on the exit
code alone would therefore be vacuous, so :func:`assert_check_all` parses the
report AND cross-checks with the in-process validator.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.validate import validate_geoparquet

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "tests" / "data"

# Existing fixtures used as journey inputs.
# TODO(#667): migrate to tests/data/canonical/ once the canonical-dataset
# deliverable lands; the triple there replaces these ad-hoc picks.
PLACES = DATA_DIR / "places_test.parquet"  # 766 points, West Africa (BF/BJ/GH/TG)
BUILDINGS_GEOJSON = DATA_DIR / "buildings_test.geojson"  # 42 polygons
# Labelled EPSG:5070 but the data actually sits in Europe (~18E, 47N) -- see
# journey 5; never assert a CONUS bbox on it.
FIELDS_5070 = DATA_DIR / "fields_pgo_5070_snappy.parquet"

PLACES_ROWS = 766
BUILDINGS_ROWS = 42
FIELDS_ROWS = 100

GPIO_BIN = shutil.which("gpio")
TIPPECANOE_BIN = shutil.which("tippecanoe")
TILE_JOIN_BIN = shutil.which("tile-join")

requires_gpio = pytest.mark.skipif(
    GPIO_BIN is None,
    reason="gpio binary not on PATH (run under `uv run`, or `uv sync --all-extras`)",
)
# Mirrors tests/test_pmtiles.py: tippecanoe has no native Windows build, so this
# also auto-skips the Windows CI leg.
requires_tippecanoe = pytest.mark.skipif(
    TIPPECANOE_BIN is None or TILE_JOIN_BIN is None,
    reason="tippecanoe/tile-join not installed",
)

# Runtime budgets from the issue #667 journey table, in seconds. They are NOT
# asserted as wall-clock (CI runners vary too much for that to be anything but
# flaky); they are enforced as per-command subprocess timeouts with generous
# slack, so a hung pipeline fails the test instead of hanging the job. Actual
# durations surface through pytest's --durations flags, which CI already passes.
#
# Measured against budget on a 2026 laptop (macOS, warm caches), for anyone
# retuning these: 1 ~1.6s, 2 ~1.3s, 3 ~2.1s, 4 ~1.0s, 5 ~1.6s, 6 ~1.8s,
# 7 ~1.4s, 8 ~1.6s warm (the first run also downloads ~34MB of boundaries),
# 9 ~69s (remote server; the issue table's 30s was optimistic, so the entry
# below says 70), 10 ~3.6s (far under its 120s budget -- it could be promoted
# to the fast lane if the slow job ever needs trimming).
BUDGETS = {
    1: 5,
    2: 8,
    3: 6,
    4: 4,
    5: 4,
    6: 20,
    7: 4,
    8: 60,
    # 9 is 70, not the issue table's 30: measured ~69s against the live host.
    # The budget has to describe reality, since it is what sizes the timeout.
    9: 70,
    10: 120,
}

_TIMEOUT_SLACK = 10
# Cap: 10x journey 10's budget would let a wedged command sit for 20 minutes,
# which on a hung CI runner is indistinguishable from a hang. Journey 10's real
# cost is ~4s and journey 9's ~69s, so 180s leaves ample headroom.
_TIMEOUT_CAP = 180


def _timeout(journey: int) -> int:
    return min(_TIMEOUT_CAP, max(60, BUDGETS[journey] * _TIMEOUT_SLACK))


def run_gpio(args: list[str | Path], journey: int, expect_success: bool = True):
    """Run the installed ``gpio`` binary with ``args`` (no shell)."""
    cmd = [GPIO_BIN, *[str(a) for a in args]]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        # gpio's reports contain ✓/✗/📁/❌ and the fixtures carry non-ASCII place
        # names. Without an explicit encoding, Python decodes child output with
        # the locale codec -- cp1252 on the Windows CI leg -- and a
        # UnicodeDecodeError there would fail the blocking fast job. errors are
        # replaced rather than raised: the oracle reads the report's structure,
        # not its glyphs.
        encoding="utf-8",
        errors="replace",
        timeout=_timeout(journey),
    )
    if expect_success:
        assert result.returncode == 0, (
            f"`{' '.join(str(a) for a in cmd)}` failed ({result.returncode}):\n"
            f"{result.stdout}\n{result.stderr}"
        )
        assert "Traceback (most recent call last)" not in result.stderr, (
            f"`{' '.join(str(a) for a in cmd)}` printed a traceback:\n{result.stderr}"
        )
    return result


def run_pipeline(stages: list[str], journey: int, expect_success: bool = True):
    """Run a real shell pipeline of ``gpio`` invocations (journeys 3 and 4).

    No ``pipefail``: the returncode is the last stage's (and cmd.exe has no
    equivalent anyway), so an early stage dying quietly is caught by the
    callers' content assertions on the output, not by the exit code.
    """
    pipeline = " | ".join(stages)
    result = subprocess.run(
        pipeline,
        shell=True,
        capture_output=True,
        text=True,
        encoding="utf-8",  # see run_gpio: never decode child output with the locale codec
        errors="replace",
        timeout=_timeout(journey),
    )
    if expect_success:
        assert result.returncode == 0, (
            f"pipeline failed ({result.returncode}): {pipeline}\n{result.stdout}\n{result.stderr}"
        )
        assert "Traceback (most recent call last)" not in result.stderr, (
            f"pipeline printed a traceback: {pipeline}\n{result.stderr}"
        )
    return result


def q(path: Path | str) -> str:
    """Quote a path for a shell pipeline (works under sh and cmd.exe)."""
    return f'"{path}"'


# Glyph-free on purpose: with errors="replace" (and on a console that degrades
# UTF-8), the ✓/✗ prefixes can arrive as U+FFFD, and an oracle that required them
# would silently stop matching -- turning the "no spec failures" assertion into a
# no-op across the eight journeys it backs. The counts are what carry meaning.
_SPEC_FAILED = re.compile(r"(\d+)\s+failed")
_SPEC_PASSED = re.compile(r"(\d+)\s+checks passed")
# The real summary line may carry a warnings segment between passed and failed
# ("Summary: 2 passed, 1 warnings, 3 failed"); tolerate it so the failed count
# is still captured when warnings are present.
# "N passed", "N not judged" (a spatial-order verdict withheld below eight row
# groups: not a pass, not a failure), or both, then the warning and failed counts.
_SUMMARY = re.compile(
    r"Summary:\s+(?=\d)(?:(\d+)\s+passed)?(?:,?\s*(\d+)\s+not judged)?"
    r"(?:,\s*\d+\s+warnings?)?(?:,\s*(\d+)\s+failed)?"
)


def assert_check_all(target: Path, journey: int, *, all_files: bool = False) -> str:
    """Run ``gpio check all`` on a file or partition directory as the oracle.

    Returns the captured stdout so callers can make journey-specific
    assertions on it (version banner, compression, spatial order, ...).
    """
    args: list[str | Path] = ["check", "all", target]
    if all_files:
        args.append("--all-files")
    result = run_gpio(args, journey)
    report = result.stdout + result.stderr

    # Any non-zero "N failed" anywhere in the report is a failure, whether it
    # came from spec validation or from the per-file partition summary. A zero
    # count is tolerated so a future "0 failed" line cannot fail a clean run.
    failures = [int(count) for count in _SPEC_FAILED.findall(report) if int(count) > 0]
    assert not failures, f"`gpio check all {target}` reports failures:\n{report}"

    if all_files:
        summary = _SUMMARY.search(report)
        assert summary is not None, f"no partition summary in:\n{report}"
        assert summary.group(3) is None, f"partition files failed the check:\n{report}"
    else:
        # Deliberately warning-intolerant: the CLI prints the plain "N checks
        # passed" line only when there are zero warnings, so a journey output
        # that starts warning fails here. That is the point - canonical journey
        # outputs must be warning-clean, and a new warning deserves a look.
        assert _SPEC_PASSED.search(report), (
            f"no clean spec-validation pass line in the report for {target} - "
            f"either validation failed or it passed with warnings, and journey "
            f"outputs must be warning-clean:\n{report}"
        )
        # Cross-check the CLI verdict against the in-process validator.
        validation = validate_geoparquet(str(target))
        assert validation.is_valid, (
            f"validate_geoparquet flagged {target}: "
            f"{[c.message for c in validation.checks if c.status.name == 'FAILED']}"
        )
    return report


def spatial_skip_rate(path: Path, journey: int) -> float:
    """Return the estimated row-group skip rate (%) from ``gpio check spatial``."""
    result = run_gpio(["check", "spatial", "--verbose", path], journey)
    match = re.search(r"Estimated skip rate:\s+([\d.]+)%", result.stdout + result.stderr)
    assert match is not None, f"no skip-rate line in:\n{result.stdout}\n{result.stderr}"
    return float(match.group(1))


def rows(path: Path) -> int:
    return pq.read_table(path).num_rows


def columns(path: Path) -> list[str]:
    return pq.read_table(path).column_names


def leaf_files(directory: Path) -> list[Path]:
    return sorted(directory.rglob("*.parquet"))


def total_rows(directory: Path) -> int:
    return sum(rows(leaf) for leaf in leaf_files(directory))


def bbox_extent(path: Path, bbox_column: str = "bbox") -> tuple[float, float, float, float]:
    """Compute the overall extent from a gpio-written bbox struct column."""
    import pyarrow.compute as pc

    table = pq.read_table(path, columns=[bbox_column])
    struct = table.column(bbox_column).combine_chunks()
    return (
        pc.min(struct.field("xmin")).as_py(),
        pc.min(struct.field("ymin")).as_py(),
        pc.max(struct.field("xmax")).as_py(),
        pc.max(struct.field("ymax")).as_py(),
    )
