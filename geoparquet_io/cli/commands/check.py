"""``gpio check`` - check GeoParquet files for best practices.

Registered on the root ``gpio`` group by ``cli/main.py`` via
``cli.add_command(check)``. This module must not import ``cli.main``: the
dependency runs one way, ``main`` -> ``commands`` -> ``_shared``/``decorators``.

``MultiFileCheckRunner`` below is the group's own progress/summary helper. It is
used by exactly this group, so it lives here rather than in ``cli/_shared.py``.

Exit codes are one contract across the group (#1241): a failure exits 1, a
warning exits 0 unless ``--strict``, and an input that matches no files exits 1
(#1091). A check that could not run is a failure (#624). Severity -- per result
and per issue, including "could not run" -- is stated by the core checks and
read through :mod:`geoparquet_io.core.check_verdict`; the runner keeps each
file's verdict and :meth:`MultiFileCheckRunner.finish` settles the exit code,
so no subcommand decides it on its own.
"""

import os
import traceback
from collections.abc import Callable
from functools import partial
from pathlib import Path
from typing import TypeVar

import click
from click.core import ParameterSource

from geoparquet_io.cli._shared import _activate_s3, create_default_group, init_group_context
from geoparquet_io.cli.decorators import (
    GlobAwareCommand,
    check_partition_options,
    enable_verbose_logging,
    handle_geoparquet_errors,
    overwrite_option,
    strict_option,
    verbose_option,
)
from geoparquet_io.cli.fix_helpers import NoBackupConfirmation, handle_fix_common
from geoparquet_io.core.check_bbox_values import DEFAULT_BBOX_SAMPLE_ROWS
from geoparquet_io.core.check_parquet_structure import CheckProfile
from geoparquet_io.core.check_parquet_structure import check_all as check_structure_impl
from geoparquet_io.core.check_spatial_order import SPATIAL_ORDER_CHECK
from geoparquet_io.core.check_spatial_order import check_spatial_order as check_spatial_impl
from geoparquet_io.core.check_verdict import (
    COULD_NOT_RUN_KEY,
    FAILURE_GLYPH,
    PASS_GLYPH,
    SEVERITY_KEY,
    WARNING_GLYPH,
    Verdict,
    all_checks_result,
    could_not_run,
    exit_code,
    is_could_not_run,
    labelled_issues,
    not_geoparquet_result,
    result_verdict,
    validation_verdict,
    worst,
)
from geoparquet_io.core.duckdb_utils import is_interrupt
from geoparquet_io.core.file_utils import is_same_file_path
from geoparquet_io.core.geo_metadata import BBOX_REWRITE_HINT
from geoparquet_io.core.logging_config import configure_verbose

# CheckDefaultGroup: defaults to 'all' when no subcommand is provided
CheckDefaultGroup = create_default_group(
    "all",
    "Custom Group that invokes a default command when no subcommand is provided.",
)


@click.group(cls=CheckDefaultGroup)
@click.pass_context
def check(ctx):
    """Check GeoParquet files for best practices.

    By default, runs all checks (compression, bbox, row groups, and spatial order).
    Use subcommands for specific checks.

    When run without a subcommand, all checks are performed. Options like --fix
    can be used directly without specifying 'all'.

    Exit codes, the same for every subcommand: 0 when nothing failed (warnings
    allowed), 1 when a check failed or could not run, or when the input matched
    no files. Pass --strict to exit 1 on warnings too.
    """
    init_group_context(ctx)


T = TypeVar("T")

_CHECK_GLYPHS = {
    Verdict.PASS: (PASS_GLYPH, "green"),
    Verdict.WARNING: (WARNING_GLYPH, "yellow"),
    Verdict.FAILURE: (FAILURE_GLYPH, "red"),
}


def exit_for_verdict(verdict: Verdict, *, strict: bool) -> None:
    """Exit with the code :func:`~geoparquet_io.core.check_verdict.exit_code` gives.

    A failure has already been reported by the check that found it; a warning
    that exits 1 only because of ``--strict`` says so on stderr, since nothing
    on stdout looks like a reason to stop.
    """
    code = exit_code(verdict, strict=strict)
    if not code:
        return
    if verdict is Verdict.WARNING:
        click.echo("Warnings found; exiting 1 because --strict is set.", err=True)
    raise click.exceptions.Exit(code)


def quiet_recheck(check_func: Callable[..., dict], *args, **kwargs) -> Callable[[str], dict]:
    """``check_func`` re-run silently on a fixed file, to judge what ``--fix`` left."""
    return lambda path: check_func(path, *args, return_results=True, quiet=True, **kwargs)


class _VerifyCapture:
    """The two checks ``check all --fix`` re-runs on its output, keeping their results.

    ``verify_fixes`` already re-reads the fixed file; judging it from those
    results plus the spec spares a third read of the same bytes.
    """

    def __init__(self) -> None:
        self.structure: dict = {}
        self.spatial: object = {}

    def check_structure(self, *args, **kwargs):
        self.structure = check_structure_impl(*args, **kwargs)
        return self.structure

    def check_spatial(self, *args, **kwargs):
        self.spatial = check_spatial_impl(*args, **kwargs)
        return self.spatial

    def recheck(self, output_path: str) -> dict:
        from geoparquet_io.core.validate import validate_geoparquet

        spec = validate_geoparquet(output_path, validate_data=True, sample_size=1000, verbose=False)
        return all_checks_result(self.structure, self.spatial, spec)


class MultiFileCheckRunner:
    """Helper for running checks on multiple files with progress tracking and summary."""

    def __init__(
        self,
        files: list[str],
        verbose: bool = False,
        max_issues_shown: int = 3,
        *,
        fix_output: str | None = None,
        overwrite: bool = False,
        confirmation: NoBackupConfirmation | None = None,
        strict: bool = False,
        matched: int | None = None,
    ):
        self.files = files
        #: How many files the input matched; more than ``files`` when sampled.
        self.matched = matched if matched is not None else len(files)
        #: ``--strict``: a warning exits 1 too.
        self.strict = strict
        #: Each file's verdict -- after its fix, when one was applied.
        self.verdicts: dict[str, Verdict] = {}
        #: The re-check of each fixed file, behind its post-fix verdict.
        self.rechecks: dict[str, dict] = {}
        self.verbose = verbose
        self.max_issues_shown = max_issues_shown
        self.fix_output = fix_output
        self.overwrite = overwrite
        #: The run's one ``--no-backup`` prompt, shared by every file it fixes.
        self.confirmation = confirmation or NoBackupConfirmation(False)
        self.passed = 0
        self.warnings = 0
        self.failed = 0
        self.issues: list[tuple[str, str, str]] = []  # (file, level, message)
        self.fixed: list[tuple[str, str | None]] = []  # (output path, backup path)
        self.fix_failures: list[tuple[str, str]] = []  # (file, error)
        self.check_errors: list[tuple[str, str]] = []  # (file, why a check could not run)
        self._interrupted = False
        self.current_index = 0
        self.is_multi_file = len(files) > 1

    def _update_progress(self):
        """Print progress line (overwrites previous line in non-verbose mode)."""
        if not self.is_multi_file or self.verbose:
            return
        total = len(self.files)
        msg = f"Checking files... {self.current_index}/{total} ({self.passed} passed)"
        if self.warnings:
            msg += f", {self.warnings} warnings"
        if self.failed:
            msg += f", {self.failed} failed"
        click.echo(f"\r{msg}", nl=False)

    def _record_issue(self, file_path: str, verdict: Verdict, message: str, *, always=False):
        """Record an issue and print it if under the limit, labelled by its own verdict.

        ``always`` prints it whatever the limit and ``--verbose``: a check that
        could not run has no detailed output of its own to say so.
        """
        level = "error" if verdict is Verdict.FAILURE else "warning"
        self.issues.append((file_path, level, message))
        if always or (not self.verbose and len(self.issues) <= self.max_issues_shown):
            # Clear progress line and print issue
            click.echo("\r" + " " * 80 + "\r", nl=False)
            glyph, color = _CHECK_GLYPHS[max(verdict, Verdict.WARNING)]
            filename = Path(file_path).name
            click.echo(click.style(f"  {glyph} {level.upper()}: {filename} - {message}", fg=color))

    def start_file(self, file_path: str):
        """Called before checking each file."""
        self.current_index += 1
        self._update_progress()
        if self.verbose and self.is_multi_file:
            click.echo(click.style(f"\n{'=' * 60}", fg="bright_black"))
            click.echo(
                click.style(f"File {self.current_index}/{len(self.files)}: {file_path}", fg="cyan")
            )
            click.echo(click.style(f"{'=' * 60}", fg="bright_black"))

    def record_result(self, file_path: str, result: dict):
        """Record the result of checking a file; its verdict settles the exit code.

        The verdict is the result's own (:func:`result_verdict`): a check that
        could not run says so itself. Each issue is printed at its own level.
        """
        verdict = result_verdict(result)
        self.verdicts[file_path] = verdict
        could_not = result.get(COULD_NOT_RUN_KEY) or []
        if could_not:
            self.check_errors.append((file_path, "; ".join(could_not)))
        if verdict is Verdict.PASS:
            self.passed += 1
        else:
            if verdict is Verdict.FAILURE:
                self.failed += 1
            else:
                self.warnings += 1
            for level, issue in labelled_issues(result):
                self._record_issue(file_path, level, issue, always=issue in could_not)
        self._update_progress()

    def record_failure(self, file_path: str, message: str) -> None:
        """Record a file that failed before its check could run."""
        self.record_result(
            file_path, {"passed": False, SEVERITY_KEY: Verdict.FAILURE.label, "issues": [message]}
        )
        if self.verbose:
            click.echo(click.style(f"{FAILURE_GLYPH} {message}", fg="red"))

    def recheck(self, file_path: str, output_path: str, check: Callable[[str], dict]) -> None:
        """Judge a fixed file by what ``--fix`` wrote, replacing its earlier verdict.

        A check that could not run on the input stays a failure (#624): the fix
        cannot vouch for a check that never ran. A re-check that cannot run is
        a failure too.
        """
        result = self._attempt(partial(check, output_path), "Re-check", gate=True)
        for line in (result.get(COULD_NOT_RUN_KEY) or []) if is_could_not_run(result) else []:
            self._echo_could_not_run(file_path, line)
        verdict = result_verdict(result)
        self.verdicts[file_path] = Verdict.FAILURE if self.errored(file_path) else verdict
        self.rechecks[file_path] = result

    def echo_fixed(
        self, file_path: str, applied: tuple[str, str | None], success: str, action: str
    ) -> None:
        """What ``--fix`` wrote, judged by its re-check -- never "success" for a file that fails.

        A fix can repair one thing and leave another: ``check bbox --fix`` adds
        a missing covering but cannot recompute wrong values. Then it says what
        it did, that the file still fails, each failure, and the repair to run.
        """
        output_path, backup_path = applied
        still_fails = self.verdicts.get(file_path) is Verdict.FAILURE
        if still_fails:
            click.echo(
                click.style(f"\n{WARNING_GLYPH} {action}, but the file still fails:", fg="yellow")
            )
            result = self.rechecks.get(file_path, {})
            for level, issue in labelled_issues(result):
                if level is Verdict.FAILURE:
                    click.echo(click.style(f"  {FAILURE_GLYPH} {issue}", fg="red"))
            recommendations = result.get("recommendations") or []
            if recommendations:
                click.echo(f"  {recommendations[0]}")
        else:
            click.echo(click.style(f"\n{PASS_GLYPH} {success}", fg="green"))
        click.echo(f"{'Written to' if still_fails else 'Optimized file'}: {output_path}")
        if backup_path:
            click.echo(f"Backup: {backup_path}")

    def finish(self) -> None:
        """Summarize the run and exit with its code: the one place a check run ends."""
        self.print_summary()
        self._note_sampling()
        self.raise_if_any_fix_failed()
        exit_for_verdict(worst(self.verdicts.values()), strict=self.strict)

    def _note_sampling(self) -> None:
        """Say so when the exit code speaks for fewer files than the input matched."""
        if self.matched <= len(self.files):
            return
        click.echo(
            f"Note: the verdict covers {len(self.files)} of {self.matched} files; "
            "pass --all-files to check every file.",
            err=True,
        )

    def record_fix(self, output_path: str, backup_path: str | None):
        """Record a file this run rewrote, for the end-of-run summary."""
        self.fixed.append((output_path, backup_path))

    def _guard(self, file_path: str, call: Callable[[], T], stage: str) -> T | None:
        """Run one per-file fix step; in a multi-file run a failure is recorded, not fatal.

        A single-file run keeps its old shape -- the exception propagates with
        its message. Click's own exceptions always propagate: a declined
        prompt or a bad parameter is a verdict on the invocation, not on this
        file. So does an interrupt: DuckDB raises Ctrl-C as an
        ``InterruptException`` or a plain ``RuntimeError``, and treating that as "this file failed, next"
        would keep rewriting files after the user asked it to stop.
        """
        try:
            return call()
        except (click.Abort, click.ClickException, click.exceptions.Exit):
            raise
        except KeyboardInterrupt:
            self._on_interrupt()
            raise
        except Exception as e:
            self._raise_if_interrupt(e)
            if not self.is_multi_file:
                raise
            self.fix_failures.append((file_path, str(e)))
            click.echo("\r" + " " * 80 + "\r", nl=False)
            click.echo(
                click.style(
                    f"  {FAILURE_GLYPH} {stage} failed: {Path(file_path).name} - {e}", fg="red"
                )
            )
            return None

    def run_check(
        self, file_path: str, check_call: Callable[[], dict], stage: str = "Check"
    ) -> dict | None:
        """The check stage for one file, or None when it gave no result to act on.

        A file that is not GeoParquet at all fails here, before any check or
        fix runs on it: every ``check`` subcommand answers that the same way.
        So does a check that could not run -- one that raised, in a single-file
        run as in a multi-file one, or one whose result says so -- recorded as
        the failure it is rather than a traceback.
        """
        refused = not_geoparquet_result(file_path)
        if refused is not None:
            self.record_failure(file_path, refused["issues"][0])
            return None
        result = self._attempt(check_call, stage, gate=True)
        if is_could_not_run(result):
            self.record_result(file_path, result)
            return None
        return result

    def run_stage(self, file_path: str, call: Callable[[], T], stage: str) -> T | dict:
        """One check of several on a file: its result, or why it could not run.

        ``check all`` is a report, so one check that dies must not take the
        others with it (#1082). A check that raises comes back as
        :func:`~geoparquet_io.core.check_verdict.could_not_run`'s failure, and
        is printed with the file's other issues.
        """
        return self._attempt(call, stage)

    def _attempt(self, call: Callable[[], T], stage: str, *, gate: bool = False) -> T | dict:
        """``call()``, or a could-not-run failure naming ``stage`` when it raises.

        Click exceptions and interrupts propagate, as in :meth:`_guard`. So,
        for a ``gate`` stage in a single-file run, does an error the CLI's
        boundary renders itself -- gpio's own errors and the DuckDB failures an
        input file causes get their ``Error:`` line there, and ``--verbose``
        its traceback (#983). Anything else is this check not running: never a
        traceback, and under ``--verbose`` the traceback is still printed.
        """
        try:
            return call()
        except (click.Abort, click.ClickException, click.exceptions.Exit):
            raise
        except KeyboardInterrupt:
            self._on_interrupt()
            raise
        except Exception as e:
            self._raise_if_interrupt(e)
            if gate and not self.is_multi_file and _boundary_renders(e):
                raise
            if self.verbose:
                click.echo(traceback.format_exc(), err=True)
            return could_not_run(stage, str(e))

    def _raise_if_interrupt(self, error: Exception) -> None:
        """Turn ``error`` into ``KeyboardInterrupt`` when it is Ctrl-C.

        DuckDB reports a SIGINT as ``InterruptException`` or as a plain
        ``RuntimeError("Query interrupted")``. Either one means stop, never
        "this check failed, carry on and --fix the file". Raised as
        ``KeyboardInterrupt``, Click ends the run with "Aborted!" instead of a
        traceback.
        """
        if is_interrupt(error):
            self._on_interrupt()
            raise KeyboardInterrupt(str(error)) from error

    def _on_interrupt(self) -> None:
        """Print the fix summary once, however many guards the interrupt crosses."""
        if not self._interrupted:
            self._interrupted = True
            self._print_fix_summary()

    def _echo_could_not_run(self, file_path: str, line: str) -> None:
        click.echo("\r" + " " * 80 + "\r", nl=False)
        click.echo(click.style(f"  {FAILURE_GLYPH} {Path(file_path).name} - {line}", fg="red"))

    def errored(self, file_path: str) -> bool:
        """Whether a check could not run on ``file_path``: the one rule ``--fix`` cannot lift."""
        return any(path == file_path for path, _ in self.check_errors)

    def no_fix(self, file_path: str, result: dict, already_fine: str, *, show: bool) -> None:
        """``--fix`` had nothing to apply: say why, and still write ``--fix-output``.

        "No fix needed" only for a file that passed; a warning or failure that
        no fix covers says so, with the check's own hint. A pipeline that names
        ``--fix-output`` expects that file, so a file with nothing to repair is
        copied there -- unless it failed, which no copy makes usable.
        """
        verdict = self.verdicts.get(file_path, result_verdict(result))
        if show:
            _echo_no_fix(verdict, result, already_fine)
        if verdict is not Verdict.FAILURE:
            self.copy_unfixed(file_path)

    def copy_unfixed(self, file_path: str) -> None:
        """Copy a file ``--fix`` did not need to rewrite to its ``--fix-output``."""
        output = fix_output_for(self.fix_output, file_path)
        if not output or is_same_file_path(output, file_path):
            return
        copied = self._guard(
            file_path, partial(_copy_to_fix_output, file_path, output, self.overwrite), "Copy"
        )
        if copied is not None and not (self.is_multi_file and not self.verbose):
            click.echo(f"Nothing to fix; copied to {output}")

    def apply_fix(self, file_path: str, fix_call: Callable[[], tuple[str, str | None] | None]):
        """Run one file's fix, recording it -- or its failure -- and carrying on (#1041).

        Returns ``(output_path, backup_path)`` for a file that was rewritten,
        None when it needed nothing or could not be fixed; the exit code is
        settled at the end by :meth:`raise_if_any_fix_failed`.
        """
        outcome = self._guard(file_path, fix_call, "Fix")
        if outcome is None:
            return None
        self.record_fix(*outcome)
        return outcome

    def fix_file(self, file_path: str, fix_func, *, verbose: bool, recheck: Callable[[str], dict]):
        """Fix one file with this run's ``--fix-output``/``--no-backup``/``--overwrite``.

        ``recheck`` re-runs the check on the written file, so a repaired
        failure exits 0 and a repair that left something behind does not.
        """
        outcome = self.apply_fix(
            file_path,
            partial(
                handle_fix_common,
                file_path,
                fix_output_for(self.fix_output, file_path),
                fix_func,
                verbose,
                self.overwrite,
                None,
                confirmation=self.confirmation,
                quiet=self.is_multi_file and not self.verbose,
            ),
        )
        if outcome is not None:
            self.recheck(file_path, outcome[0], recheck)
        return outcome

    def raise_if_any_fix_failed(self) -> None:
        """Exit non-zero when any matched file went unrepaired."""
        if not self.fix_failures:
            return
        plural = "" if len(self.fix_failures) == 1 else "s"
        click.echo(
            click.style(f"Failed to fix {len(self.fix_failures)} file{plural}:", fg="red"),
        )
        for file_path, message in self.fix_failures:
            click.echo(f"  - {file_path}: {message}")
        raise click.exceptions.Exit(1)

    def _print_fix_summary(self):
        """List what ``--fix`` rewrote.

        In multi-file mode the per-file "Optimized file:" lines are suppressed,
        so a run that rewrote every file in a partition in place used to end on
        a summary that mentioned only the checks. Naming the files is the
        minimum a mass in-place rewrite owes the user.

        Every one of them, not the first ``max_issues_shown``: an issue list is
        a sample of a diagnosis the user can re-run for, while this list is the
        only record of which of their files were overwritten (#1041).
        """
        if not self.fixed:
            return

        plural = "" if len(self.fixed) == 1 else "s"
        click.echo(click.style(f"Fixed {len(self.fixed)} file{plural}:", fg="green"))
        for output_path, backup_path in self.fixed:
            suffix = f" (backup: {backup_path})" if backup_path else ""
            click.echo(f"  - {output_path}{suffix}")

    def print_summary(self):
        """Print final summary after all files are checked."""
        if not self.is_multi_file:
            return

        # Clear progress line
        if not self.verbose:
            click.echo("\r" + " " * 80 + "\r", nl=False)

        # Show remaining issues hint
        extra_issues = len(self.issues) - self.max_issues_shown
        if extra_issues > 0 and not self.verbose:
            click.echo(
                click.style(
                    f"  ... and {extra_issues} more issues (use --verbose to see all)", fg="cyan"
                )
            )

        total = len(self.files)
        summary_parts = []
        if self.passed:
            summary_parts.append(click.style(f"{self.passed} passed", fg="green"))
        if self.warnings:
            summary_parts.append(click.style(f"{self.warnings} warnings", fg="yellow"))
        if self.failed:
            summary_parts.append(click.style(f"{self.failed} failed", fg="red"))

        summary = ", ".join(summary_parts) if summary_parts else "0 checked"
        click.echo(f"Summary: {summary} ({total} files checked)")
        self._print_fix_summary()


def _boundary_renders(error: Exception) -> bool:
    """Whether the root group turns ``error`` into an ``Error:`` line of its own."""
    from geoparquet_io.cli.exception_handler import cli_error_for
    from geoparquet_io.core.exceptions import GeoParquetError

    return isinstance(error, GeoParquetError) or cli_error_for(error) is not None


def _echo_no_fix(verdict: Verdict, result: dict, already_fine: str) -> None:
    """The line ``--fix`` prints when it has nothing to apply to a file."""
    if verdict is Verdict.PASS:
        click.echo(click.style(f"\n{PASS_GLYPH} No fix needed - {already_fine}", fg="green"))
        return
    glyph, color = _CHECK_GLYPHS[verdict]
    problem = result.get("covering_problem")
    if problem:
        hint = f" - {problem}. {BBOX_REWRITE_HINT}"
    else:
        recommendations = result.get("recommendations") or []
        hint = f". {recommendations[0]}" if recommendations else ""
    click.echo(click.style(f"\n{glyph} No automatic fix available{hint}", fg=color))


def _copy_to_fix_output(file_path: str, output: str, overwrite: bool) -> str:
    from geoparquet_io.core.file_utils import copy_file, handle_output_overwrite

    handle_output_overwrite(output, overwrite, file_path)
    copy_file(file_path, output)
    return output


def refuse_shared_fix_output(files_to_check: list[str], fix: bool, fix_output: str | None) -> None:
    """Refuse a ``--fix-output`` that cannot hold one output per input.

    A single file path for several inputs, or a directory in which two inputs
    would land on one name (``k=1/part-0.parquet`` and ``k=2/part-0.parquet``),
    writes every repair to the same place in turn and keeps the last. Both are
    a :class:`click.UsageError` (exit 2) before the first file is checked, so
    nothing has been rewritten when it is refused.
    """
    if not (fix and fix_output) or len(files_to_check) <= 1:
        return
    if not Path(fix_output).is_dir():
        raise click.UsageError(
            f"When fixing multiple files ({len(files_to_check)} files), "
            f"--fix-output must be an existing directory, not a file path.\n"
            f"Either:\n"
            f"  1. Specify a directory: --fix-output /path/to/output_dir/\n"
            f"  2. Omit --fix-output to fix files in-place (with .bak backups)"
        )

    # Two inputs landing on one name is also how a directory that *contains*
    # an input gets that input overwritten: `sub/a.parquet` -> `dir/a.parquet`.
    seen: dict[str, str] = {}
    for source in files_to_check:
        output = fix_output_for(fix_output, source)
        assert output is not None
        key = os.path.realpath(output)
        if key in seen:
            raise click.UsageError(
                f"--fix-output {fix_output} cannot hold every output: {seen[key]} and "
                f"{source} would both be written to {output}. Inputs are written to "
                "their own name inside the directory, so names must be unique -- "
                "fix each subdirectory separately, or fix in place."
            )
        seen[key] = source


def refuse_no_parquet_match(path: str) -> None:
    """Exit 1 for a local glob or directory that holds no parquet files (#1091).

    For the subcommands that take one file (``check spec``): the runner ones
    refuse an empty match in :func:`begin_check_run`. A plain path is left to
    the check, which reports a file it cannot read.
    """
    from geoparquet_io.core.partition.reader import select_files_to_check
    from geoparquet_io.core.remote import is_remote_url

    if is_remote_url(path) or not (any(c in path for c in "*?[") or Path(path).is_dir()):
        return
    try:
        files, _, _ = select_files_to_check(path, check_all=True)
    except FileNotFoundError:
        files = []
    if not files:
        raise click.ClickException(f"No parquet files found in {path}")


def fix_output_for(fix_output: str | None, file_path: str) -> str | None:
    """This file's share of ``--fix-output``: its own name inside a directory."""
    if fix_output and Path(fix_output).is_dir():
        return str(Path(fix_output) / Path(file_path).name)
    return fix_output


def begin_check_run(
    parquet_file: str,
    *,
    check_all_files: bool,
    check_sample: int | None,
    verbose: bool,
    fix: bool,
    fix_output: str | None,
    no_backup: bool,
    overwrite: bool = False,
    strict: bool = False,
) -> MultiFileCheckRunner:
    """Resolve the input to files and build the runner every ``check`` command shares.

    An input that matched no parquet files -- a glob that resolves to nothing,
    an empty directory -- is an error (exit 1), with or without ``--fix``: a
    script fixing ``"data/*.parquet"`` against a mistyped path must not be told
    it succeeded (#1091). A ``--fix-output`` that cannot take one output per
    input is refused here, before any file is checked.
    """
    from geoparquet_io.core.partition.reader import select_files_to_check

    files_to_check, notice, matched = select_files_to_check(
        parquet_file,
        check_all=check_all_files,
        check_sample=check_sample,
        verbose=verbose,
        fix=fix,
    )

    if not files_to_check:
        nothing_fixed = "; nothing was fixed" if fix else ""
        raise click.ClickException(f"No parquet files found in {parquet_file}{nothing_fixed}")

    if notice:
        click.echo(click.style(f"📁 {notice}", fg="cyan"))

    refuse_shared_fix_output(files_to_check, fix, fix_output)

    return MultiFileCheckRunner(
        files_to_check,
        verbose=verbose,
        fix_output=fix_output,
        overwrite=overwrite,
        confirmation=NoBackupConfirmation(no_backup, len(files_to_check)),
        strict=strict,
        matched=matched,
    )


def _echo_spec_result(spec_result, spec_details: bool) -> None:
    """Print the spec validation block of ``check all``."""
    if is_could_not_run(spec_result):
        click.echo(click.style(f"  {FAILURE_GLYPH} Spec validation could not run", fg="red"))
        return
    if spec_details:
        from geoparquet_io.core.validate import format_terminal_output

        format_terminal_output(spec_result)
        return
    if spec_result.failed_count > 0:
        click.echo(
            click.style(
                f"  {FAILURE_GLYPH} {spec_result.failed_count} failed, "
                f"{spec_result.passed_count} passed",
                fg="red",
            )
        )
    elif spec_result.warning_count > 0:
        click.echo(
            click.style(
                f"  {WARNING_GLYPH} {spec_result.passed_count} passed, "
                f"{spec_result.warning_count} warnings",
                fg="yellow",
            )
        )
    else:
        click.echo(
            click.style(f"  {PASS_GLYPH} {spec_result.passed_count} checks passed", fg="green")
        )


@check.command(name="all", cls=GlobAwareCommand)
@click.argument("parquet_file")
@click.option(
    "--verbose",
    is_flag=True,
    help="Print detailed diagnostics",
    callback=enable_verbose_logging,
    is_eager=True,
)
@click.option("--fix", is_flag=True, help="Fix detected issues")
@click.option(
    "--fix-output",
    type=click.Path(),
    help="Output path for fixed file (default: overwrites with .bak backup)",
)
@click.option(
    "--no-backup",
    is_flag=True,
    help="Skip .bak backup when fixing",
)
@overwrite_option
@click.option(
    "--random-sample-size",
    default=100,
    show_default=True,
    help="Sample size for spatial order check",
)
@click.option(
    "--limit-rows",
    default=500000,
    show_default=True,
    help="Max rows for spatial order check",
)
@click.option(
    "--spec-details",
    is_flag=True,
    help="Show full spec validation results instead of summary",
)
@click.option(
    "--profile",
    type=click.Choice([c.value for c in CheckProfile], case_sensitive=False),
    required=False,
    default=None,
    help="Check best practices for specific use case",
)
@click.option(
    "--pmtiles",
    is_flag=True,
    help="Generate PMTiles after fixing (requires tippecanoe)",
)
@strict_option
@check_partition_options
@click.pass_context
def check_all(
    ctx,
    parquet_file,
    verbose,
    fix,
    fix_output,
    no_backup,
    overwrite,
    random_sample_size,
    limit_rows,
    spec_details,
    check_all_files,
    check_sample,
    profile,
    pmtiles,
    strict,
):
    """Check compression, bbox, row groups, spatial order, and spec compliance.

    Exits 1 if any check fails, or on a warning with --strict.
    """
    from geoparquet_io.core.remote import is_remote_url, show_remote_read_message

    configure_verbose(verbose)
    with _activate_s3(ctx):
        runner = begin_check_run(
            parquet_file,
            check_all_files=check_all_files,
            check_sample=check_sample,
            verbose=verbose,
            fix=fix,
            fix_output=fix_output,
            no_backup=no_backup,
            overwrite=overwrite,
            strict=strict,
        )

        # Check pmtiles requires --fix
        if pmtiles and not fix:
            raise click.UsageError("--pmtiles requires --fix to generate PMTiles from fixed files")

        # Check tippecanoe availability once before processing files
        if pmtiles:
            from geoparquet_io.core.pmtiles import _check_tippecanoe

            if not _check_tippecanoe():
                raise click.ClickException(
                    "--pmtiles requires tippecanoe.\n\n"
                    "Install tippecanoe:\n"
                    "  macOS:  brew install tippecanoe\n"
                    "  Ubuntu: sudo apt install tippecanoe"
                )

        # Process each file
        for file_path in runner.files:
            runner.start_file(file_path)

            # Show single progress message for remote files (only in verbose mode for multi-file)
            if runner.verbose or not runner.is_multi_file:
                show_remote_read_message(file_path, verbose=False)
                if is_remote_url(file_path):
                    click.echo()  # Add blank line after remote message

            # Run all checks and collect results
            # In non-verbose multi-file mode, suppress detailed output
            show_output = runner.verbose or not runner.is_multi_file
            quiet = not show_output

            def run_all_checks(file_path=file_path, show_output=show_output, quiet=quiet):
                from geoparquet_io.core.validate import validate_geoparquet

                # The structure check is the gate. A file it cannot read fails
                # as a whole (run_check), and is not spatially checked or fixed.
                structure = check_structure_impl(
                    file_path,
                    verbose and show_output,
                    return_results=True,
                    quiet=quiet,
                    profile=profile,
                )
                if show_output:
                    click.echo("\nSpatial Order Analysis:")
                spatial = runner.run_stage(
                    file_path,
                    partial(
                        check_spatial_impl,
                        file_path,
                        random_sample_size,
                        limit_rows,
                        verbose and show_output,
                        return_results=True,
                        quiet=quiet,
                    ),
                    SPATIAL_ORDER_CHECK,
                )
                spec = runner.run_stage(
                    file_path,
                    partial(
                        validate_geoparquet,
                        file_path,
                        validate_data=True,
                        sample_size=1000,
                        verbose=False,
                    ),
                    "Spec validation",
                )
                return {"structure": structure, "spatial": spatial, "spec": spec}

            # A file that is not GeoParquet fails in run_check, before the
            # structure checks (which assume geo metadata) or --pmtiles run.
            checked = runner.run_check(file_path, run_all_checks)
            if checked is None:
                continue
            structure_results = checked["structure"]
            spatial_result = checked["spatial"]
            spec_result = checked["spec"]

            if show_output:
                _echo_spatial_order(spatial_result)

            # Display spec validation results
            if show_output:
                click.echo("\nSpec Validation:")
                _echo_spec_result(spec_result, spec_details)

            runner.record_result(
                file_path, all_checks_result(structure_results, spatial_result, spec_result)
            )

            # If --fix flag is set, apply fixes
            if fix:
                from geoparquet_io.cli.fix_helpers import apply_check_all_fixes

                per_file_output = fix_output_for(fix_output, file_path)
                verified = _VerifyCapture()

                all_results = {**structure_results, "spatial": spatial_result}
                applied = runner.apply_fix(
                    file_path,
                    partial(
                        apply_check_all_fixes,
                        file_path=file_path,
                        all_results=all_results,
                        fix_output=per_file_output,
                        overwrite=overwrite,
                        verbose=verbose,
                        profile=None,
                        check_structure_impl=verified.check_structure,
                        check_spatial_impl=verified.check_spatial,
                        random_sample_size=random_sample_size,
                        limit_rows=limit_rows,
                        confirmation=runner.confirmation,
                        quiet=not show_output,
                    ),
                )
                if applied is None:
                    if not any(path == file_path for path, _ in runner.fix_failures):
                        runner.no_fix(file_path, {}, "file is already optimal!", show=False)
                    continue
                runner.recheck(file_path, applied[0], verified.recheck)

            # Generate PMTiles if requested (non-geo files already skipped at loop start)
            if pmtiles:
                from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

                # Generate PMTiles
                # Use fixed output path if available, otherwise original
                source_file = per_file_output if fix and per_file_output else file_path
                # Handle S3 URIs: Path().with_suffix() mangles s3:// schemes
                if source_file.startswith("s3://"):
                    output_pmtiles = source_file.rsplit(".", 1)[0] + ".pmtiles"
                else:
                    output_pmtiles = str(Path(source_file).with_suffix(".pmtiles"))

                try:
                    create_pmtiles_from_geoparquet(
                        input_path=source_file,
                        output_path=output_pmtiles,
                        verbose=verbose,
                    )
                    click.echo(click.style(f"{PASS_GLYPH} Generated {output_pmtiles}", fg="green"))
                except Exception as e:
                    click.echo(
                        click.style(
                            f"{FAILURE_GLYPH} PMTiles failed for {file_path}: {e}", fg="red"
                        )
                    )

        # Print summary for multi-file checks, and exit with the run's code
        runner.finish()


def _spatial_with_pushdown(file_path, random_sample_size, limit_rows, verbose, *, quiet):
    """``check spatial``'s verdict: spatial order, with pushdown readiness folded in."""
    from geoparquet_io.core.check_spatial_order import with_pushdown

    spatial = check_spatial_impl(
        file_path, random_sample_size, limit_rows, verbose, return_results=True, quiet=quiet
    )
    return with_pushdown(file_path, spatial, verbose=verbose)


def _echo_spatial_order(result: dict) -> None:
    ratio = result.get("ratio")
    if ratio is None:
        return
    if result.get("passed", ratio < 0.5):
        click.echo(click.style(f"{PASS_GLYPH} Data appears to be spatially ordered", fg="green"))
    else:
        click.echo(
            click.style(
                f"{WARNING_GLYPH}  Data may not be optimally spatially ordered\n"
                "Consider running 'gpio sort hilbert' to improve spatial locality",
                fg="yellow",
            )
        )


def _echo_pushdown(pushdown: dict | None) -> None:
    """The pushdown block of ``check spatial``, marked by the verdict it counts as."""
    if pushdown is None:
        return
    click.echo("\nSpatial Filter Pushdown Readiness:")
    warning = result_verdict(pushdown) is Verdict.WARNING
    if not pushdown.get("has_geo_bbox"):
        if warning:
            message = (
                f"  {WARNING_GLYPH}  No geo_bbox column found. "
                "Add bbox with 'gpio add bbox' for pushdown support."
            )
            click.echo(click.style(message, fg="yellow"))
        else:
            click.echo(
                "  ℹ️  No bbox column; native geo types carry per-row-group statistics, "
                "so a bbox column is not needed for pushdown"
            )
        return
    click.echo(f"  Row groups: {pushdown['num_row_groups']}")
    click.echo(f"  Estimated skip rate: {pushdown['estimated_skip_rate'] * 100:.0f}%")
    click.echo(f"  Avg bbox area ratio: {pushdown['avg_bbox_area_ratio']:.2f}")
    if warning:
        click.echo(click.style(f"  {WARNING_GLYPH}  Low pushdown efficiency", fg="yellow"))
    elif pushdown.get("passed"):
        click.echo(click.style(f"  {PASS_GLYPH} Good pushdown readiness", fg="green"))
    else:
        click.echo("  ℹ️  A single row group: nothing for a spatial filter to skip")


@check.command(name="spatial", cls=GlobAwareCommand)
@click.argument("parquet_file")
@click.option(
    "--random-sample-size",
    default=100,
    show_default=True,
    help="Sample size for spatial order check",
)
@click.option(
    "--limit-rows",
    default=500000,
    show_default=True,
    help="Max rows for spatial order check",
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Print detailed diagnostics",
    callback=enable_verbose_logging,
    is_eager=True,
)
@click.option("--fix", is_flag=True, help="Fix with Hilbert ordering")
@click.option(
    "--fix-output",
    type=click.Path(),
    help="Output path (default: overwrites with .bak backup)",
)
@click.option(
    "--no-backup",
    is_flag=True,
    help="Skip .bak backup when fixing",
)
@overwrite_option
@strict_option
@check_partition_options
@click.pass_context
def check_spatial(
    ctx,
    parquet_file,
    random_sample_size,
    limit_rows,
    verbose,
    fix,
    fix_output,
    no_backup,
    overwrite,
    check_all_files,
    check_sample,
    strict,
):
    """Check spatial ordering."""
    from geoparquet_io.core.check_fixes import fix_spatial_ordering

    configure_verbose(verbose)

    with _activate_s3(ctx):
        runner = begin_check_run(
            parquet_file,
            check_all_files=check_all_files,
            check_sample=check_sample,
            verbose=verbose,
            fix=fix,
            fix_output=fix_output,
            no_backup=no_backup,
            overwrite=overwrite,
            strict=strict,
        )

        for file_path in runner.files:
            runner.start_file(file_path)

            show_output = runner.verbose or not runner.is_multi_file
            quiet = not show_output
            result = runner.run_check(
                file_path,
                partial(
                    _spatial_with_pushdown,
                    file_path,
                    random_sample_size,
                    limit_rows,
                    verbose and show_output,
                    quiet=quiet,
                ),
                SPATIAL_ORDER_CHECK,
            )
            if result is None:
                continue

            if show_output:
                _echo_spatial_order(result)
                _echo_pushdown(result.get("pushdown"))

            # Record result for summary
            runner.record_result(file_path, result)

            if fix:
                if not result.get("fix_available", False):
                    runner.no_fix(file_path, result, "already spatially ordered!", show=show_output)
                    continue

                if show_output:
                    click.echo("\nApplying Hilbert spatial ordering...")
                applied = runner.fix_file(
                    file_path,
                    fix_spatial_ordering,
                    verbose=verbose,
                    recheck=lambda path: _spatial_with_pushdown(
                        path, random_sample_size, limit_rows, False, quiet=True
                    ),
                )
                if applied is not None and show_output:
                    runner.echo_fixed(
                        file_path,
                        applied,
                        "Spatial ordering applied successfully!",
                        "Applied Hilbert spatial ordering",
                    )

        # Print summary for multi-file checks, and exit with the run's code
        runner.finish()


@check.command(name="compression", cls=GlobAwareCommand)
@click.argument("parquet_file")
@click.option(
    "--verbose",
    is_flag=True,
    help="Print detailed diagnostics",
    callback=enable_verbose_logging,
    is_eager=True,
)
@click.option("--fix", is_flag=True, help="Recompress geometry with ZSTD")
@click.option(
    "--fix-output",
    type=click.Path(),
    help="Output path (default: overwrites with .bak backup)",
)
@click.option(
    "--no-backup",
    is_flag=True,
    help="Skip .bak backup when fixing",
)
@overwrite_option
@strict_option
@check_partition_options
@click.pass_context
def check_compression_cmd(
    ctx,
    parquet_file,
    verbose,
    fix,
    fix_output,
    no_backup,
    overwrite,
    check_all_files,
    check_sample,
    strict,
):
    """Check geometry column compression."""
    from geoparquet_io.core.check_fixes import fix_compression
    from geoparquet_io.core.check_parquet_structure import check_compression

    configure_verbose(verbose)

    with _activate_s3(ctx):
        runner = begin_check_run(
            parquet_file,
            check_all_files=check_all_files,
            check_sample=check_sample,
            verbose=verbose,
            fix=fix,
            fix_output=fix_output,
            no_backup=no_backup,
            overwrite=overwrite,
            strict=strict,
        )

        for file_path in runner.files:
            runner.start_file(file_path)

            show_output = runner.verbose or not runner.is_multi_file
            quiet = not show_output
            result = runner.run_check(
                file_path,
                partial(
                    check_compression,
                    file_path,
                    verbose and show_output,
                    return_results=True,
                    quiet=quiet,
                ),
            )
            if result is None:
                continue

            # Record result for summary
            runner.record_result(file_path, result)

            if fix:
                if not result.get("fix_available", False):
                    runner.no_fix(file_path, result, "already using ZSTD!", show=show_output)
                    continue

                if show_output:
                    click.echo("\nRe-compressing with ZSTD...")
                applied = runner.fix_file(
                    file_path,
                    fix_compression,
                    verbose=verbose,
                    recheck=quiet_recheck(check_compression, False),
                )
                if applied is not None and show_output:
                    runner.echo_fixed(
                        file_path,
                        applied,
                        "Compression optimized successfully!",
                        "Re-compressed with ZSTD",
                    )

        # Print summary for multi-file checks, and exit with the run's code
        runner.finish()


def _bbox_fix_action(needs_column: bool, needs_metadata: bool) -> str:
    """What ``check bbox --fix`` did to a 1.x file, for the line that reports it."""
    if needs_column:
        return "Added the bbox column and its covering metadata"
    if needs_metadata:
        return "Added the bbox covering metadata"
    return "Rewrote the bbox column"


@check.command(name="bbox", cls=GlobAwareCommand)
@click.argument("parquet_file")
@click.option(
    "--verbose",
    is_flag=True,
    help="Print detailed diagnostics",
    callback=enable_verbose_logging,
    is_eager=True,
)
@click.option("--fix", is_flag=True, help="Fix bbox (add for v1.x, remove for v2/parquet-geo)")
@click.option(
    "--fix-output",
    type=click.Path(),
    help="Output path (default: overwrites with .bak backup)",
)
@click.option(
    "--no-backup",
    is_flag=True,
    help="Skip .bak backup when fixing",
)
@click.option(
    "--sample-rows",
    type=click.IntRange(min=0),
    default=DEFAULT_BBOX_SAMPLE_ROWS,
    show_default=True,
    help="Rows to compare bbox values against geometry (0 to skip that comparison)",
)
@click.option(
    "--all-rows",
    is_flag=True,
    help="Compare every row's bbox against its geometry, not a sample",
)
@overwrite_option
@strict_option
@check_partition_options
@click.pass_context
def check_bbox_cmd(
    ctx,
    parquet_file,
    verbose,
    fix,
    fix_output,
    no_backup,
    sample_rows,
    all_rows,
    overwrite,
    check_all_files,
    check_sample,
    strict,
):
    """Check bbox column and metadata (version-aware).

    For GeoParquet 1.x: bbox column is recommended for spatial filtering.
    For GeoParquet 2.0/parquet-geo-only: bbox column is NOT recommended
    (native Parquet geo types provide row group statistics).

    Also compares each row's bbox against its geometry's real envelope. A bbox
    that does not cover its geometry is a failure -- readers prune on the
    covering, so those rows go missing from spatial query results with no error
    anywhere -- as are a NULL or NaN bbox and a geometry that is not valid WKB.
    A bbox larger than its geometry is a warning: readers over-fetch, but the
    answer stays right.

    The comparison decodes a seeded random sample (--sample-rows) or every row
    (--all-rows); either way the geometry and bbox columns are read in full.
    --fix repairs the column and metadata, not the values, so its output is
    compared again and a wrong bbox still fails the run.

    Exits 1 if any check fails, or on a warning with --strict.
    """
    from geoparquet_io.core.check_fixes import fix_bbox_all, fix_bbox_removal
    from geoparquet_io.core.check_parquet_structure import check_bbox

    configure_verbose(verbose)
    if all_rows and ctx.get_parameter_source("sample_rows") is not ParameterSource.DEFAULT:
        raise click.UsageError(
            "--all-rows compares every row; it cannot be combined with --sample-rows"
        )
    bbox_recheck = quiet_recheck(check_bbox, False, verify_rows=sample_rows, all_rows=all_rows)

    with _activate_s3(ctx):
        runner = begin_check_run(
            parquet_file,
            check_all_files=check_all_files,
            check_sample=check_sample,
            verbose=verbose,
            fix=fix,
            fix_output=fix_output,
            no_backup=no_backup,
            overwrite=overwrite,
            strict=strict,
        )

        for file_path in runner.files:
            runner.start_file(file_path)

            show_output = runner.verbose or not runner.is_multi_file
            quiet = not show_output
            result = runner.run_check(
                file_path,
                partial(
                    check_bbox,
                    file_path,
                    verbose and show_output,
                    return_results=True,
                    quiet=quiet,
                    verify_rows=sample_rows,
                    all_rows=all_rows,
                ),
            )
            if result is None:
                continue

            # Record result for summary
            runner.record_result(file_path, result)

            if fix:
                if not result.get("fix_available", False):
                    # Not "optimal" unless it passed: `--fix` declines a covering
                    # whose repair rewrites the column, not the metadata (#1035).
                    runner.no_fix(file_path, result, "bbox is optimal!", show=show_output)
                    continue

                # Check if this is a removal (v2/parquet-geo-only) or addition (v1.x)
                if result.get("needs_bbox_removal", False):
                    # V2 or parquet-geo-only: remove bbox column
                    bbox_column_name = result.get("bbox_column_name")

                    def bbox_fix_func(
                        input_path, output_path, verbose_flag, profile_name, _col=bbox_column_name
                    ):
                        return fix_bbox_removal(
                            input_path, output_path, _col, verbose_flag, profile_name
                        )

                    applied = runner.fix_file(
                        file_path, bbox_fix_func, verbose=verbose, recheck=bbox_recheck
                    )
                    if applied is not None and show_output:
                        runner.echo_fixed(
                            file_path,
                            applied,
                            "Bbox column removed successfully!",
                            "Removed the bbox column",
                        )
                else:
                    # V1.x: add bbox column/metadata (existing logic)
                    needs_column = result.get("needs_bbox_column", False)
                    needs_metadata = result.get("needs_bbox_metadata", False)

                    def bbox_fix_func(
                        input_path,
                        output_path,
                        verbose_flag,
                        profile_name,
                        _needs_col=needs_column,
                        _needs_meta=needs_metadata,
                    ):
                        return fix_bbox_all(
                            input_path,
                            output_path,
                            _needs_col,
                            _needs_meta,
                            verbose_flag,
                            profile_name,
                        )

                    applied = runner.fix_file(
                        file_path, bbox_fix_func, verbose=verbose, recheck=bbox_recheck
                    )
                    if applied is not None and show_output:
                        runner.echo_fixed(
                            file_path,
                            applied,
                            "Bbox optimized successfully!",
                            _bbox_fix_action(needs_column, needs_metadata),
                        )

        # Print summary for multi-file checks, and exit with the run's code
        runner.finish()


@check.command(name="row-group", cls=GlobAwareCommand)
@click.argument("parquet_file")
@click.option(
    "--verbose",
    is_flag=True,
    help="Print detailed diagnostics",
    callback=enable_verbose_logging,
    is_eager=True,
)
@click.option("--fix", is_flag=True, help="Optimize row group size")
@click.option(
    "--fix-output",
    type=click.Path(),
    help="Output path (default: overwrites with .bak backup)",
)
@click.option(
    "--no-backup",
    is_flag=True,
    help="Skip .bak backup when fixing",
)
@click.option(
    "--profile",
    type=click.Choice([c.value for c in CheckProfile], case_sensitive=False),
    required=False,
    default=None,
    help="Check best practices for specific use case",
)
@overwrite_option
@strict_option
@check_partition_options
@click.pass_context
def check_row_group_cmd(
    ctx,
    parquet_file,
    verbose,
    fix,
    fix_output,
    no_backup,
    overwrite,
    check_all_files,
    check_sample,
    profile,
    strict,
):
    """Check row group size."""
    from geoparquet_io.core.check_fixes import fix_row_groups
    from geoparquet_io.core.check_parquet_structure import check_row_groups

    configure_verbose(verbose)

    with _activate_s3(ctx):
        runner = begin_check_run(
            parquet_file,
            check_all_files=check_all_files,
            check_sample=check_sample,
            verbose=verbose,
            fix=fix,
            fix_output=fix_output,
            no_backup=no_backup,
            overwrite=overwrite,
            strict=strict,
        )

        for file_path in runner.files:
            runner.start_file(file_path)

            show_output = runner.verbose or not runner.is_multi_file
            quiet = not show_output
            result = runner.run_check(
                file_path,
                partial(
                    check_row_groups,
                    file_path,
                    verbose and show_output,
                    return_results=True,
                    quiet=quiet,
                    profile=profile,
                ),
            )
            if result is None:
                continue

            # Record result for summary
            runner.record_result(file_path, result)

            if fix:
                if not result.get("fix_available", False):
                    runner.no_fix(file_path, result, "row groups are optimal!", show=show_output)
                    continue

                if show_output:
                    click.echo("\nOptimizing row groups...")
                applied = runner.fix_file(
                    file_path,
                    fix_row_groups,
                    verbose=verbose,
                    recheck=quiet_recheck(check_row_groups, False, profile=profile),
                )
                if applied is not None and show_output:
                    runner.echo_fixed(
                        file_path,
                        applied,
                        "Row groups optimized successfully!",
                        "Rewrote the row groups",
                    )

        # Print summary for multi-file checks, and exit with the run's code
        runner.finish()


@check.command(name="stac")
@handle_geoparquet_errors
@click.argument("stac_file")
@verbose_option
@strict_option
@click.pass_context
def check_stac_cmd(ctx, stac_file, verbose, strict):
    """
    Validate STAC Item or Collection JSON.

    Exits 1 when validation fails, or on a warning with --strict.

    Checks:

      • STAC spec compliance

      • Required fields

      • Asset href resolution (local files)

      • Best practices

    Example:

      \b
      gpio check stac output.json
    """
    with _activate_s3(ctx):
        from geoparquet_io.core.remote import setup_aws_profile_if_needed, validate_profile_for_urls
        from geoparquet_io.core.stac_check import check_stac

        # Validate profile is only used with S3
        validate_profile_for_urls(None, stac_file)

        # Setup AWS profile if needed
        setup_aws_profile_if_needed(None, stac_file)

        exit_for_verdict(check_stac(stac_file, verbose), strict=strict)


@check.command(name="spec", cls=GlobAwareCommand)
@click.argument("parquet_file")
@click.option(
    "--geoparquet-version",
    type=click.Choice(["1.0", "1.1", "2.0", "parquet-geo-only"]),
    default=None,
    help="Validate against specific GeoParquet version (default: auto-detect)",
)
@click.option(
    "--json",
    "json_output",
    is_flag=True,
    help="Output as JSON for machine parsing",
)
@click.option(
    "--skip-data-validation",
    is_flag=True,
    help="Skip validation of actual data values against metadata claims",
)
@click.option(
    "--sample-size",
    type=click.IntRange(0, None),
    default=1000,
    show_default=True,
    help="Number of rows to sample for data validation (0 = all rows)",
)
@verbose_option
@strict_option
@click.pass_context
def check_spec(
    ctx,
    parquet_file,
    geoparquet_version,
    json_output,
    skip_data_validation,
    sample_size,
    verbose,
    strict,
):
    """
    Validate a GeoParquet file against specification requirements.

    Checks file structure, metadata, and optionally data consistency against
    the GeoParquet specification. Automatically detects the file version unless
    --geoparquet-version is specified.

    Supports GeoParquet 1.0, 1.1, 2.0, and Parquet native geo types.

    \b
    Exit codes:
      0 - No check failed (warnings allowed)
      1 - One or more checks failed, or any warning with --strict

    \b
    Examples:
      # Basic validation (auto-detect version)
      gpio check spec data.parquet

      \b
      # Validate against specific version
      gpio check spec data.parquet --geoparquet-version 1.1

      \b
      # JSON output for CI/CD
      gpio check spec data.parquet --json

      \b
      # Skip data validation for faster check
      gpio check spec data.parquet --skip-data-validation
    """
    from geoparquet_io.core.remote import (
        setup_aws_profile_if_needed,
        validate_profile_for_urls,
    )
    from geoparquet_io.core.validate import (
        format_json_output as format_json,
    )
    from geoparquet_io.core.validate import (
        format_terminal_output as format_terminal,
    )
    from geoparquet_io.core.validate import (
        validate_geoparquet,
    )

    configure_verbose(verbose)

    with _activate_s3(ctx):
        # Validate profile is only used with S3
        validate_profile_for_urls(None, parquet_file)
        setup_aws_profile_if_needed(None, parquet_file)
        refuse_no_parquet_match(parquet_file)

        result = validate_geoparquet(
            parquet_file,
            target_version=geoparquet_version,
            validate_data=not skip_data_validation,
            sample_size=sample_size,
            verbose=verbose,
        )

        if json_output:
            click.echo(format_json(result))
        else:
            format_terminal(result)

        verdict = validation_verdict(failed=result.failed_count, warnings=result.warning_count)
        exit_for_verdict(verdict, strict=strict)


@check.command(name="optimization", cls=GlobAwareCommand)
@click.argument("parquet_file")
@click.option(
    "--verbose",
    is_flag=True,
    help="Print detailed diagnostics",
    callback=enable_verbose_logging,
    is_eager=True,
)
@strict_option
@check_partition_options
@click.pass_context
def check_optimization_cmd(
    ctx,
    parquet_file,
    verbose,
    check_all_files,
    check_sample,
    strict,
):
    """Check combined spatial query optimization.

    Evaluates five factors that affect spatial query performance:

    \b
    1. Native geo types (v2.0 or parquet-geo-only)
    2. Per-row-group geo bbox statistics
    3. Spatial sorting (Hilbert or similar)
    4. Row group size (10k-50k rows optimal)
    5. ZSTD compression on geometry column

    \b
    Scoring:
      5/5  Fully optimized for spatial queries
      3-4  Partially optimized, improvements possible
      0-2  Not optimized for spatial queries

    Every factor is advice: a score below 5 is a warning (exit 0, or 1 with
    --strict). A file that is not GeoParquet at all fails (exit 1).

    \b
    Examples:
      gpio check optimization data.parquet
      gpio check optimization data.parquet --verbose
    """
    from geoparquet_io.core.check_optimization import check_optimization

    configure_verbose(verbose)

    with _activate_s3(ctx):
        runner = begin_check_run(
            parquet_file,
            check_all_files=check_all_files,
            check_sample=check_sample,
            verbose=verbose,
            fix=False,
            fix_output=None,
            no_backup=False,
            strict=strict,
        )

        for file_path in runner.files:
            runner.start_file(file_path)

            show_output = runner.verbose or not runner.is_multi_file
            quiet = not show_output

            result = runner.run_check(
                file_path,
                partial(
                    check_optimization,
                    file_path,
                    verbose=verbose and show_output,
                    return_results=True,
                    quiet=quiet,
                ),
            )
            if result is None:
                continue

            runner.record_result(file_path, result)

        runner.finish()
