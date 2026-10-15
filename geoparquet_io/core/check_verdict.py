"""How bad a check result is, and what that does to the exit code (#1241).

Every ``gpio check`` subcommand answers the same two questions through this
module, so the answer cannot drift per command again:

* **Severity.** A result is a :class:`Verdict`: ``PASS``, ``WARNING`` (advisory
  -- a better codec, a row-group size, an upgrade that is available) or
  ``FAILURE`` (a spec violation, a file that is not GeoParquet at all, metadata
  a reader would mis-read or cannot open). A check states its severity in a
  ``"severity"`` key of its results dict; one that does not say, and did not
  pass, is a ``WARNING`` -- the conservative reading, so a check has to opt in
  to failing a build.
* **Exit code.** :func:`exit_code`: a failure exits 1, a warning exits 0 unless
  ``--strict`` asked for warnings to exit 1 as well.

The glyphs are settled here too, so stdout reads the same across subcommands:
``✓`` passed, ``⚠`` warning, ``✗`` failure.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from enum import IntEnum

PASS_GLYPH = "✓"
WARNING_GLYPH = "⚠"
FAILURE_GLYPH = "✗"

#: The results-dict key a check states its severity in.
SEVERITY_KEY = "severity"


class Verdict(IntEnum):
    """A check's outcome, ordered so the worst of several is their ``max``."""

    PASS = 0
    WARNING = 1
    FAILURE = 2

    @property
    def label(self) -> str:
        """The value a results dict carries under :data:`SEVERITY_KEY`."""
        return self.name.lower()


_BY_LABEL = {verdict.label: verdict for verdict in Verdict}


def severity(passed: bool, *, failure: bool = False) -> str:
    """The :data:`SEVERITY_KEY` value for a check that did or did not pass.

    ``failure`` says whether *not* passing is a failure; when it is False the
    miss is a warning.
    """
    if passed:
        return Verdict.PASS.label
    return (Verdict.FAILURE if failure else Verdict.WARNING).label


def result_verdict(result: object) -> Verdict:
    """The verdict of one check's results dict.

    An explicit :data:`SEVERITY_KEY` wins. Without one, a result that passed is
    ``PASS`` and one that did not is ``WARNING``. Anything that is not a dict
    (a check that returned nothing) is ``PASS``.
    """
    if not isinstance(result, Mapping):
        return Verdict.PASS
    label = result.get(SEVERITY_KEY)
    if label is not None:
        try:
            return _BY_LABEL[label]
        except KeyError:
            raise ValueError(
                f"unknown check severity {label!r}; expected one of {sorted(_BY_LABEL)}"
            ) from None
    return Verdict.PASS if result.get("passed", True) else Verdict.WARNING


def worst(verdicts: Iterable[Verdict]) -> Verdict:
    """The most severe of ``verdicts``; ``PASS`` when there are none."""
    return max(verdicts, default=Verdict.PASS)


def results_verdict(results: Mapping) -> Verdict:
    """The worst verdict among a mapping of named results (``check all``'s shape)."""
    return worst(result_verdict(r) for r in results.values() if isinstance(r, Mapping))


def validation_verdict(*, failed: int, warnings: int) -> Verdict:
    """The verdict of a ``check spec`` run from its failed and warning counts."""
    if failed:
        return Verdict.FAILURE
    return Verdict.WARNING if warnings else Verdict.PASS


def exit_code(verdict: Verdict, *, strict: bool) -> int:
    """1 for a failure, and for a warning under ``--strict``; 0 otherwise."""
    if verdict is Verdict.FAILURE or (strict and verdict is Verdict.WARNING):
        return 1
    return 0
