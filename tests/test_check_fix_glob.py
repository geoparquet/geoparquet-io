"""``gpio check <cmd> "*.parquet" --fix`` acts on every file the glob matched (#1041).

Every test here drives the real CLI over a directory of genuinely defective
files and judges *each* output with
:func:`tests.fix_output_oracle.assert_fix_output_is_sound`, so a fix that runs
on N files is held to exactly the standard a fix on one file is held to. The
fixture is ``austria_bbox_covering.parquet``: 30 rows in EPSG:31287, so a
rewrite that drops the CRS cannot hide behind CRS84 being the default (#993).
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from unittest import mock

import duckdb
import pytest
from click.testing import CliRunner, Result

from geoparquet_io.cli.commands import check as check_commands
from geoparquet_io.cli.main import cli
from geoparquet_io.core import check_fixes as core_check_fixes
from tests.fix_output_oracle import assert_fix_output_is_sound
from tests.test_check_fix_output_is_valid import (
    make_snappy,
    make_tiny_row_groups,
    make_unsorted,
    make_without_bbox,
)

#: The three names from the issue report.
NAMES = ("a", "b", "d")

AUSTRIA_ROWS = 30
AUSTRIA_CRS = {"authority": "EPSG", "code": 31287}


def _make_snappy_unsorted(source: Path, target: Path) -> Path:
    """Every defect ``check all`` repairs at once."""
    return make_unsorted(source, target, compression="SNAPPY")


def _make_without_bbox(source: Path, target: Path) -> Path:
    return make_without_bbox(source, target, "geometry_bbox")


@dataclass(frozen=True)
class FixCase:
    """One ``check`` subcommand that takes ``--fix``, and a defect it repairs."""

    command: str
    make_broken: Callable[[Path, Path], Path]
    extra: list[str] = field(default_factory=list)


#: Every ``check`` subcommand whose ``--fix`` accepts a glob.
FIX_CASES = [
    FixCase("row-group", make_tiny_row_groups),
    FixCase("compression", make_snappy),
    FixCase("bbox", _make_without_bbox),
    FixCase("spatial", make_unsorted, ["--random-sample-size", "20"]),
    FixCase("all", _make_snappy_unsorted, ["--random-sample-size", "20"]),
]
CASE_IDS = [case.command for case in FIX_CASES]
COMPRESSION, ALL = FIX_CASES[1], FIX_CASES[4]


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def broken_files(
    case: FixCase, source: str, directory: Path, names: Sequence[str] = NAMES
) -> list[Path]:
    """One defective copy per name, in *directory*."""
    directory.mkdir(parents=True, exist_ok=True)
    return [case.make_broken(Path(source), directory / f"{name}.parquet") for name in names]


def invoke(*args: object, stdin: str | None = None) -> Result:
    return CliRunner().invoke(cli, [str(a) for a in args], input=stdin)


def assert_austria_output_is_sound(path: Path) -> None:
    """The oracle, with the facts every repair of this fixture has to preserve."""
    assert_fix_output_is_sound(
        path,
        expected_rows=AUSTRIA_ROWS,
        expected_crs=AUSTRIA_CRS,
        expects_covering=True,
        expected_version_prefix="1.1",
    )


def failing_on(fix_function: str, name: str, error: BaseException):
    """The real fix, raising *error* for the file called *name*."""
    real = getattr(core_check_fixes, fix_function)

    def flaky(parquet_file, *args, **kwargs):
        if Path(parquet_file).name == name:
            raise error
        return real(parquet_file, *args, **kwargs)

    return mock.patch.object(core_check_fixes, fix_function, flaky)


class TestGlobFixesEveryMatch:
    """The bug itself: N matched, N fixed."""

    @pytest.mark.parametrize("case", FIX_CASES, ids=CASE_IDS)
    def test_every_matched_file_is_rewritten_and_sound(
        self, case, austria_bbox_covering_file, tmp_path
    ):
        files = broken_files(case, austria_bbox_covering_file, tmp_path / "data")
        before = {path: digest(path) for path in files}

        result = invoke(
            "check", case.command, tmp_path / "data" / "*.parquet", "--fix", *case.extra
        )

        assert result.exit_code == 0, result.output
        for path in files:
            assert digest(path) != before[path], f"{path.name} was not rewritten:\n{result.output}"
            assert Path(f"{path}.bak").exists(), f"{path.name} got no backup"
            assert path.name in result.output, f"{path.name} is not named in the report"
            assert_austria_output_is_sound(path)

    def test_an_explicit_sample_is_still_honoured(self, austria_bbox_covering_file, tmp_path):
        """``--sample-files N`` is the user naming a subset; ``--fix`` does not widen it."""
        files = broken_files(COMPRESSION, austria_bbox_covering_file, tmp_path / "data")
        before = {path: digest(path) for path in files}

        result = invoke(
            "check", "compression", tmp_path / "data" / "*.parquet", "--fix", "--sample-files", 1
        )

        assert result.exit_code == 0, result.output
        rewritten = [path.name for path in files if digest(path) != before[path]]
        assert rewritten == ["a.parquet"], result.output

    def test_a_directory_walk_skips_dotfiles(self, austria_bbox_covering_file, tmp_path):
        """macOS ``._x.parquet`` sidecars and an orphaned staging file are not data."""
        files = broken_files(COMPRESSION, austria_bbox_covering_file, tmp_path / "data")
        (tmp_path / "data" / "._a.parquet").write_bytes(b"AppleDouble")
        (tmp_path / "data" / ".gpio-fix-orphan.parquet").write_bytes(b"half a file")

        result = invoke("check", "compression", tmp_path / "data", "--fix")

        assert result.exit_code == 0, result.output
        assert "Fixing all 3 files" in result.output
        for path in files:
            assert_austria_output_is_sound(path)


class TestFixOutputMustHoldOneOutputPerInput:
    """``--fix-output`` is refused, before any file is checked, when it cannot."""

    def _assert_refused(self, result: Result, files: list[Path], before: dict[Path, str]) -> None:
        assert result.exit_code == 2, result.output
        for path in files:
            assert digest(path) == before[path], f"{path.name} was touched"
            assert not Path(f"{path}.bak").exists()

    def test_a_single_path_for_many_inputs_is_a_usage_error(
        self, austria_bbox_covering_file, tmp_path
    ):
        files = broken_files(COMPRESSION, austria_bbox_covering_file, tmp_path / "data")
        before = {path: digest(path) for path in files}
        shared = tmp_path / "fixed.parquet"

        result = invoke(
            "check", "compression", tmp_path / "data" / "*.parquet", "--fix", "--fix-output", shared
        )

        assert "--fix-output must be an existing directory, not a file path" in result.output
        assert "When fixing multiple files (3 files)" in result.output
        assert not shared.exists()
        self._assert_refused(result, files, before)

    @pytest.mark.parametrize("case", [COMPRESSION, ALL], ids=["compression", "all"])
    def test_a_directory_receives_one_output_per_input(
        self, case, austria_bbox_covering_file, tmp_path
    ):
        files = broken_files(case, austria_bbox_covering_file, tmp_path / "data")
        before = {path: digest(path) for path in files}
        out_dir = tmp_path / "out"
        out_dir.mkdir()

        result = invoke(
            "check",
            case.command,
            tmp_path / "data" / "*.parquet",
            "--fix",
            "--fix-output",
            out_dir,
            *case.extra,
        )

        assert result.exit_code == 0, result.output
        assert sorted(p.name for p in out_dir.iterdir()) == sorted(p.name for p in files)
        for path in files:
            # --fix-output leaves the inputs alone and writes no .bak.
            assert digest(path) == before[path], f"{path.name} was rewritten in place"
            assert not Path(f"{path}.bak").exists()
            assert_austria_output_is_sound(out_dir / path.name)

    def test_two_inputs_with_one_basename_cannot_share_a_directory(
        self, austria_bbox_covering_file, tmp_path
    ):
        """A hive layout: ``k=1/part-0.parquet`` and ``k=2/part-0.parquet``.

        Written to their own name inside the directory, the second repair would
        land on the first -- the very "Fixed 2 files: out/part-0.parquet twice"
        the file-path refusal exists for, through the directory door.
        """
        files = [
            COMPRESSION.make_broken(
                Path(austria_bbox_covering_file), tmp_path / "data" / f"k={k}" / "part-0.parquet"
            )
            for k in (1, 2)
            if (tmp_path / "data" / f"k={k}").mkdir(parents=True) is None
        ]
        before = {path: digest(path) for path in files}
        out_dir = tmp_path / "out"
        out_dir.mkdir()

        result = invoke("check", "compression", tmp_path / "data", "--fix", "--fix-output", out_dir)

        assert "would both be written to" in result.output, result.output
        assert list(out_dir.iterdir()) == []
        self._assert_refused(result, files, before)

    def test_a_directory_holding_an_input_is_refused(self, austria_bbox_covering_file, tmp_path):
        """``--fix-output data/`` for ``data/`` itself.

        The repair of ``sub/a.parquet`` would land on ``data/a.parquet`` -- an
        input, overwritten as a "different" file with no backup and no prompt.
        It is the same refusal: two inputs, one output name.
        """
        data = tmp_path / "data"
        (data / "sub").mkdir(parents=True)
        top = COMPRESSION.make_broken(Path(austria_bbox_covering_file), data / "a.parquet")
        nested = COMPRESSION.make_broken(
            Path(austria_bbox_covering_file), data / "sub" / "a.parquet"
        )
        before = {top: digest(top), nested: digest(nested)}

        result = invoke("check", "compression", data, "--fix", "--fix-output", data)

        assert "would both be written to" in result.output, result.output
        self._assert_refused(result, [top, nested], before)

    def test_a_single_match_still_takes_a_plain_output_path(
        self, austria_bbox_covering_file, tmp_path
    ):
        """The refusal is about *many* inputs; one file keeps the old spelling."""
        (broken,) = broken_files(
            COMPRESSION, austria_bbox_covering_file, tmp_path / "data", names=["a"]
        )
        before = digest(broken)
        fixed = tmp_path / "fixed.parquet"

        result = invoke(
            "check", "compression", tmp_path / "data" / "*.parquet", "--fix", "--fix-output", fixed
        )

        assert result.exit_code == 0, result.output
        assert digest(broken) == before, "--fix-output must leave the input alone"
        assert_austria_output_is_sound(fixed)


class TestNoBackupOverAGlob:
    """``--no-backup`` asks once for the run, not once per matched file."""

    @pytest.mark.parametrize("case", [COMPRESSION, ALL], ids=["compression", "all"])
    def test_one_confirmation_covers_every_file(self, case, austria_bbox_covering_file, tmp_path):
        files = broken_files(case, austria_bbox_covering_file, tmp_path / "data")
        before = {path: digest(path) for path in files}

        result = invoke(
            "check",
            case.command,
            tmp_path / "data" / "*.parquet",
            "--fix",
            "--no-backup",
            *case.extra,
            stdin="y\n",
        )

        assert result.exit_code == 0, result.output
        assert result.output.count("without backup. Continue?") == 1, result.output
        assert "up to 3 original files under" in result.output
        for path in files:
            assert digest(path) != before[path], f"{path.name} was not rewritten"
            assert not Path(f"{path}.bak").exists(), "--no-backup still wrote a .bak"
            assert_austria_output_is_sound(path)

    def test_writing_elsewhere_is_never_confirmed(self, austria_bbox_covering_file, tmp_path):
        """``--no-backup`` with ``--fix-output`` has nothing to overwrite, so it asks nothing."""
        files = broken_files(COMPRESSION, austria_bbox_covering_file, tmp_path / "data")
        out_dir = tmp_path / "out"
        out_dir.mkdir()

        result = invoke(
            "check",
            "compression",
            tmp_path / "data" / "*.parquet",
            "--fix",
            "--no-backup",
            "--fix-output",
            out_dir,
        )

        assert result.exit_code == 0, result.output
        assert "without backup. Continue?" not in result.output, result.output
        for path in files:
            assert_austria_output_is_sound(out_dir / path.name)

    def test_declining_the_confirmation_rewrites_nothing(
        self, austria_bbox_covering_file, tmp_path
    ):
        files = broken_files(COMPRESSION, austria_bbox_covering_file, tmp_path / "data")
        before = {path: digest(path) for path in files}

        result = invoke(
            "check",
            "compression",
            tmp_path / "data" / "*.parquet",
            "--fix",
            "--no-backup",
            stdin="n\n",
        )

        assert result.exit_code == 1, result.output
        for path in files:
            assert digest(path) == before[path], f"{path.name} was rewritten after an abort"


class TestOneFailureAmongMany:
    """A file that cannot be checked or fixed must not silence the rest, or exit 0.

    Two paths: the four single-check commands share ``runner.fix_file``, and
    ``check all`` has its own (``apply_check_all_fixes``, which restores the
    backup itself).
    """

    @pytest.mark.parametrize(
        ("case", "fix_function"),
        [(COMPRESSION, "fix_compression"), (ALL, "apply_all_fixes")],
        ids=["compression", "all"],
    )
    def test_a_failed_fix_is_reported_and_the_run_continues(
        self, case, fix_function, austria_bbox_covering_file, tmp_path
    ):
        files = broken_files(case, austria_bbox_covering_file, tmp_path / "data")
        before = {path: digest(path) for path in files}

        with failing_on(fix_function, "b.parquet", RuntimeError("synthetic fix failure")):
            result = invoke(
                "check", case.command, tmp_path / "data" / "*.parquet", "--fix", *case.extra
            )

        assert result.exit_code == 1, result.output
        assert (
            result.output.count("synthetic fix failure") == 2
        )  # once as it happens, once in the summary
        assert "Failed to fix 1 file:" in result.output
        rewritten = {path.name for path in files if digest(path) != before[path]}
        assert rewritten == {"a.parquet", "d.parquet"}, result.output
        for path in files:
            if path.name == "b.parquet":
                # Untouched, and no orphan backup beside it: the fix never landed.
                assert not Path(f"{path}.bak").exists(), "a failed fix left its .bak behind"
            else:
                assert Path(f"{path}.bak").exists()
                assert_austria_output_is_sound(path)

    @pytest.mark.parametrize("case", FIX_CASES, ids=CASE_IDS)
    def test_a_file_that_cannot_be_checked_is_reported_and_the_run_continues(
        self, case, austria_bbox_covering_file, tmp_path
    ):
        """The failure a real directory hits first: a file DuckDB cannot open.

        It fails in the *check* stage, before any fix; the run still names
        what it did rewrite, and exits 1.
        """
        files = broken_files(case, austria_bbox_covering_file, tmp_path / "data")
        before = {path: digest(path) for path in files}
        files[1].write_bytes(b"not a parquet file")
        before[files[1]] = digest(files[1])

        result = invoke(
            "check", case.command, tmp_path / "data" / "*.parquet", "--fix", *case.extra
        )

        assert result.exit_code == 1, result.output
        assert "Check failed: b.parquet" in result.output
        assert "Failed to fix 1 file:" in result.output
        assert "Fixed 2 files:" in result.output
        assert digest(files[1]) == before[files[1]]
        for path in (files[0], files[2]):
            assert_austria_output_is_sound(path)

    def test_an_interrupt_stops_the_run_and_still_lists_what_was_rewritten(
        self, austria_bbox_covering_file, tmp_path
    ):
        """Ctrl-C reaches gpio as DuckDB's ``InterruptException`` -- a plain ``Exception``.

        Treating it as "this file failed, next" would keep rewriting files after
        the user asked it to stop.
        """
        files = broken_files(COMPRESSION, austria_bbox_covering_file, tmp_path / "data")
        before = {path: digest(path) for path in files}

        with failing_on(
            "fix_compression", "b.parquet", duckdb.InterruptException("Query interrupted")
        ):
            result = invoke("check", "compression", tmp_path / "data" / "*.parquet", "--fix")

        assert result.exit_code != 0
        rewritten = {path.name for path in files if digest(path) != before[path]}
        assert rewritten == {"a.parquet"}, "the run went on past the interrupt"
        assert "Fixed 1 file:" in result.output
        assert not Path(f"{files[1]}.bak").exists(), "the interrupted fix left its .bak behind"

    def test_a_single_file_failure_keeps_its_exception(self, austria_bbox_covering_file, tmp_path):
        """One file is not a run: the error propagates as it always did."""
        (broken,) = broken_files(COMPRESSION, austria_bbox_covering_file, tmp_path / "data", ["a"])

        with failing_on("fix_compression", "a.parquet", RuntimeError("synthetic fix failure")):
            result = invoke("check", "compression", broken, "--fix")

        assert result.exit_code != 0
        assert "Failed to fix 1 file" not in result.output
        assert isinstance(result.exception, RuntimeError)


class TestFilesThatNeedNothing:
    """A glob that matches sound files as well as broken ones."""

    def test_only_the_files_that_needed_a_fix_are_reported(
        self, austria_bbox_covering_file, tmp_path
    ):
        data = tmp_path / "data"
        data.mkdir()
        source = Path(austria_bbox_covering_file)
        broken = _make_snappy_unsorted(source, data / "a.parquet")
        # `b` is a copy of the pristine fixture: nothing for `check all` to do.
        healthy = data / "b.parquet"
        healthy.write_bytes(source.read_bytes())
        untouched = digest(healthy)

        result = invoke("check", "all", data / "*.parquet", "--fix", "--random-sample-size", "20")

        assert result.exit_code == 0, result.output
        assert "Fixed 1 file:" in result.output
        assert digest(healthy) == untouched, "a sound file must not be rewritten"
        assert not Path(f"{healthy}.bak").exists()
        assert_austria_output_is_sound(broken)

    def test_a_file_that_still_fails_after_its_fix_gets_one_line(
        self, austria_bbox_covering_file, tmp_path
    ):
        """``check all`` re-checks each output; in a multi-file run a leftover is one line."""
        files = broken_files(ALL, austria_bbox_covering_file, tmp_path / "data")

        def never_sorted(*args, **kwargs):
            return {
                "passed": False,
                "ratio": 1.0,
                "issues": ["still unsorted"],
                "fix_available": True,
            }

        with mock.patch.object(check_commands, "check_spatial_impl", never_sorted):
            result = invoke("check", "all", tmp_path / "data" / "*.parquet", "--fix", *ALL.extra)

        assert result.exit_code == 0, result.output
        assert result.output.count("issues remain after fixes (Spatial Ordering)") == len(files)
        assert "Some issues remain after fixes:" not in result.output

    def test_a_multi_file_run_prints_one_line_per_file_not_a_banner(
        self, austria_bbox_covering_file, tmp_path
    ):
        """Twenty files is twenty lines, not twenty "Re-validating after fixes" blocks."""
        files = broken_files(ALL, austria_bbox_covering_file, tmp_path / "data")

        result = invoke("check", "all", tmp_path / "data" / "*.parquet", "--fix", *ALL.extra)

        assert result.exit_code == 0, result.output
        assert "Re-validating after fixes" not in result.output
        assert "Applying fixes..." not in result.output
        assert result.output.count("Created backup") == 0
        assert "Fixed 3 files:" in result.output
        for path in files:
            assert_austria_output_is_sound(path)
