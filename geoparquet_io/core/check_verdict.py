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
* **Per issue.** One result can carry findings of different severities (a bbox
  value check with both advice and defects, #1239): it names the exceptions
  in :data:`ISSUE_SEVERITY_KEY`, ``{issue text: label}``. An issue it does not
  name takes the result's own severity, and the result is as bad as its worst
  issue. :func:`labelled_issues` is how the CLI and ``CheckResult`` read them.
* **Could not run.** A check that could not run is a failure, stated by the
  result itself: :func:`could_not_run` builds it, carrying the reason under
  ``error`` and the line naming it under :data:`COULD_NOT_RUN_KEY`. Nothing
  downstream re-derives it.
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

#: ``{issue text: severity label}`` for issues whose severity is not the result's own.
ISSUE_SEVERITY_KEY = "issue_severity"

#: The reason a check could not run (see :func:`could_not_run`).
ERROR_KEY = "error"

#: The lines naming each check that could not run, in a result or a combination of them.
COULD_NOT_RUN_KEY = "could_not_run"

#: The failure every ``check`` subcommand reports for a file with no geo at all.
NOT_GEOPARQUET = "Not a GeoParquet file: no geo metadata and no native Parquet geo types"


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


def _label_verdict(label: str) -> Verdict:
    try:
        return _BY_LABEL[label]
    except KeyError:
        raise ValueError(
            f"unknown check severity {label!r}; expected one of {sorted(_BY_LABEL)}"
        ) from None


def _stated_verdict(result: Mapping) -> Verdict:
    """The severity a result states for itself, before its per-issue exceptions."""
    label = result.get(SEVERITY_KEY)
    if label is not None:
        return _label_verdict(label)
    return Verdict.PASS if result.get("passed", True) else Verdict.WARNING


def result_verdict(result: object) -> Verdict:
    """The verdict of one check's results dict.

    An explicit :data:`SEVERITY_KEY` wins. Without one, a result that passed is
    ``PASS`` and one that did not is ``WARNING``. A result is never better than
    an issue it labels in :data:`ISSUE_SEVERITY_KEY`. Anything that is not a
    dict (a check that returned nothing) is ``PASS``.
    """
    if not isinstance(result, Mapping):
        return Verdict.PASS
    levels = result.get(ISSUE_SEVERITY_KEY) or {}
    return worst([_stated_verdict(result), *map(_label_verdict, levels.values())])


def labelled_issues(result: object) -> list[tuple[Verdict, str]]:
    """Each of ``result``'s issues with its own verdict, in order.

    An issue named in :data:`ISSUE_SEVERITY_KEY` takes that label; the rest
    take the result's stated severity. A ``PASS`` label is an issue a passing
    check mentioned anyway -- advice the caller may still show.
    """
    if not isinstance(result, Mapping):
        return []
    own = _stated_verdict(result)
    levels = result.get(ISSUE_SEVERITY_KEY) or {}
    return [
        (_label_verdict(levels[issue]) if issue in levels else own, issue)
        for issue in result.get("issues", [])
    ]


def combine_results(results: Iterable[object]) -> dict:
    """Several checks' results as one, each issue keeping its own check's verdict.

    The combination is as bad as the worst of them; ``check all`` and
    ``check bbox`` report through it. A text two checks both report keeps the
    worse label.
    """
    verdicts: list[Verdict] = []
    levels: dict[str, Verdict] = {}
    could_not: list[str] = []
    for result in results:
        if not isinstance(result, Mapping):
            continue
        verdicts.append(result_verdict(result))
        for level, issue in labelled_issues(result):
            levels[issue] = max(level, levels.get(issue, Verdict.PASS))
        could_not.extend(result.get(COULD_NOT_RUN_KEY) or [])
    verdict = worst(verdicts)
    return {
        "passed": verdict is Verdict.PASS,
        SEVERITY_KEY: verdict.label,
        "issues": list(levels),
        ISSUE_SEVERITY_KEY: {issue: level.label for issue, level in levels.items()},
        COULD_NOT_RUN_KEY: could_not,
    }


def could_not_run(check: str, reason: str, **fields) -> dict:
    """The result of a ``check`` that could not run: a failure, never a pass (#624).

    Not fixable either -- ``--fix`` cannot vouch for a check that never ran.
    ``fields`` are the check's own keys (``ratio``, ...) for callers that read them.
    """
    line = f"{check} could not run, {reason}"
    return {
        "passed": False,
        SEVERITY_KEY: Verdict.FAILURE.label,
        ERROR_KEY: reason,
        COULD_NOT_RUN_KEY: [line],
        "issues": [line],
        "recommendations": [],
        "fix_available": False,
        **fields,
    }


def is_could_not_run(result: object) -> bool:
    """Whether ``result`` is a check that could not run (see :func:`could_not_run`)."""
    return isinstance(result, Mapping) and bool(result.get(COULD_NOT_RUN_KEY))


def worst(verdicts: Iterable[Verdict]) -> Verdict:
    """The most severe of ``verdicts``; ``PASS`` when there are none."""
    return max(verdicts, default=Verdict.PASS)


def validation_verdict(*, failed: int, warnings: int) -> Verdict:
    """The verdict of a ``check spec`` run from its failed and warning counts."""
    if failed:
        return Verdict.FAILURE
    return Verdict.WARNING if warnings else Verdict.PASS


def spec_result(spec: object) -> dict:
    """A ``check spec`` run as a results dict, so ``check all`` can combine it.

    ``spec`` is a :class:`~geoparquet_io.core.validate.ValidationResult` (read
    by its counts), or already a results dict -- a validation that could not
    run comes back as :func:`could_not_run`'s.
    """
    if isinstance(spec, Mapping):
        return dict(spec)
    failed, warnings = spec.failed_count, spec.warning_count  # type: ignore[attr-defined]
    verdict = validation_verdict(failed=failed, warnings=warnings)
    issues = []
    if failed:
        issues.append(f"Spec validation: {failed} checks failed")
    elif warnings:
        issues.append(f"Spec validation: {warnings} warnings")
    return {"passed": verdict is Verdict.PASS, SEVERITY_KEY: verdict.label, "issues": issues}


def all_checks_result(structure: Mapping, spatial: object, spec: object) -> dict:
    """One file's ``check all`` outcome: structure, spatial order and spec, the worst wins."""
    return combine_results([*structure.values(), spatial, spec_result(spec)])


def not_geoparquet_result(file_path: str) -> dict | None:
    """The failure for a file with neither a ``geo`` key nor a native geo type, else None.

    Every ``check`` subcommand asks this first, so a non-GeoParquet file fails
    the same way everywhere. A file this cannot read is not judged here (None):
    the check itself then reports why it could not run, with the reader's own
    error, which is more use than a guess made here.
    """
    from geoparquet_io.core.duckdb_utils import is_interrupt
    from geoparquet_io.core.file_type import detect_geoparquet_file_type

    try:
        file_type = detect_geoparquet_file_type(file_path)["file_type"]
    except Exception as e:
        if is_interrupt(e):
            raise
        return None
    if file_type != "unknown":
        return None
    return {"passed": False, SEVERITY_KEY: Verdict.FAILURE.label, "issues": [NOT_GEOPARQUET]}


def exit_code(verdict: Verdict, *, strict: bool) -> int:
    """1 for a failure, and for a warning under ``--strict``; 0 otherwise."""
    if verdict is Verdict.FAILURE or (strict and verdict is Verdict.WARNING):
        return 1
    return 0
