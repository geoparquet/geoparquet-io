"""``gpio check <cmd> "*.parquet" --fix`` acts on every file the glob matched.

#1041: with ``a.parquet``, ``b.parquet`` and ``d.parquet`` all carrying the same
defect, ``check row-group "*.parquet" --fix`` rewrote ``a`` and left the other
two broken, reporting one success and exiting 0. The narrowing happened in
``get_files_to_check``, which samples a multi-file input down to its first file
unless ``--all-files`` is passed -- a sensible default for a read-only look at a
10,000-file partition, and the wrong one for a repair: the user is told "3 total"
and then handed a run that fixed one of them.

Every test here drives the real CLI over a directory of genuinely defective
files and judges *each* output with
:func:`tests.fix_output_oracle.assert_fix_output_is_sound`. The defect injectors
are the ones ``test_check_fix_output_is_valid`` already uses, so a fix that runs
on N files is held to exactly the standard a fix on one file is held to.

The fixture is ``austria_bbox_covering.parquet`` -- 30 rows in EPSG:31287, so a
rewrite that drops the CRS cannot hide behind CRS84 being the default (#993).
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from unittest import mock

import pytest
from click.testing import CliRunner

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
NAMES = ["a", "b", "d"]

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


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def broken_files(case: FixCase, source: str, directory: Path, names=NAMES) -> list[Path]:
    """One defective copy per name, in *directory*."""
    directory.mkdir(parents=True, exist_ok=True)
    return [case.make_broken(Path(source), directory / f"{name}.parquet") for name in names]


def invoke(*args: object, stdin: str | None = None):
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

    def test_the_summary_names_every_file_it_rewrote(self, austria_bbox_covering_file, tmp_path):
        """Not the first three and "... and 2 more": a mass rewrite names them all."""
        names = ["a", "b", "c", "d", "e"]
        files = broken_files(
            FIX_CASES[1], austria_bbox_covering_file, tmp_path / "data", names=names
        )

        result = invoke("check", "compression", tmp_path / "data" / "*.parquet", "--fix")

        assert result.exit_code == 0, result.output
        assert "Fixed 5 files:" in result.output
        summary = result.output.rsplit("Fixed 5 files:", 1)[1]
        assert "more" not in summary, summary
        for path in files:
            assert f"  - {path}" in summary, f"{path} missing from the summary"

    def test_an_explicit_sample_is_still_honoured(self, austria_bbox_covering_file, tmp_path):
        """``--sample-files N`` is the user naming a subset; ``--fix`` does not widen it."""
        files = broken_files(FIX_CASES[1], austria_bbox_covering_file, tmp_path / "data")
        before = {path: digest(path) for path in files}

        result = invoke(
            "check", "compression", tmp_path / "data" / "*.parquet", "--fix", "--sample-files", 1
        )

        assert result.exit_code == 0, result.output
        rewritten = [path.name for path in files if digest(path) != before[path]]
        assert rewritten == ["a.parquet"], result.output


class TestSharedFixOutputIsRefused:
    """One ``--fix-output`` path cannot receive N inputs."""

    @pytest.mark.parametrize("case", FIX_CASES, ids=CASE_IDS)
    def test_a_single_path_for_many_inputs_is_a_usage_error(
        self, case, austria_bbox_covering_file, tmp_path
    ):
        files = broken_files(case, austria_bbox_covering_file, tmp_path / "data")
        before = {path: digest(path) for path in files}
        shared = tmp_path / "fixed.parquet"

        result = invoke(
            "check",
            case.command,
            tmp_path / "data" / "*.parquet",
            "--fix",
            "--fix-output",
            shared,
            *case.extra,
        )

        assert result.exit_code == 2, result.output
        assert "--fix-output must be a directory, not a file path" in result.output
        assert "When fixing multiple files (3 files)" in result.output
        # Refused up front: no input was touched and nothing was written.
        assert not shared.exists()
        for path in files:
            assert digest(path) == before[path]
            assert not Path(f"{path}.bak").exists()

    @pytest.mark.parametrize("case", FIX_CASES, ids=CASE_IDS)
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

    def test_a_single_match_still_takes_a_plain_output_path(
        self, austria_bbox_covering_file, tmp_path
    ):
        """The refusal is about *many* inputs; one file keeps the old spelling."""
        (broken,) = broken_files(
            FIX_CASES[1], austria_bbox_covering_file, tmp_path / "data", names=["a"]
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

    def test_one_confirmation_covers_every_file(self, austria_bbox_covering_file, tmp_path):
        files = broken_files(FIX_CASES[1], austria_bbox_covering_file, tmp_path / "data")
        before = {path: digest(path) for path in files}

        result = invoke(
            "check",
            "compression",
            tmp_path / "data" / "*.parquet",
            "--fix",
            "--no-backup",
            stdin="y\n",
        )

        assert result.exit_code == 0, result.output
        assert result.output.count("without backup. Continue?") == 1, result.output
        for path in files:
            assert digest(path) != before[path], f"{path.name} was not rewritten"
            assert not Path(f"{path}.bak").exists(), "--no-backup still wrote a .bak"
            assert_austria_output_is_sound(path)

    def test_writing_elsewhere_is_never_confirmed(self, austria_bbox_covering_file, tmp_path):
        """``--no-backup`` with ``--fix-output`` has nothing to overwrite, so it asks nothing."""
        files = broken_files(FIX_CASES[1], austria_bbox_covering_file, tmp_path / "data")
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
        files = broken_files(FIX_CASES[1], austria_bbox_covering_file, tmp_path / "data")
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


#: Every ``--fix`` branch, with the core function it calls and a fixture that
#: makes it run. ``check bbox`` has two: 1.x adds a bbox column, a native-geo
#: file has its undeclared one removed, and they are separate call sites.
FAILURE_CASES = [
    ("row-group", "fix_row_groups", "austria_bbox_covering_file", make_tiny_row_groups, []),
    ("compression", "fix_compression", "austria_bbox_covering_file", make_snappy, []),
    (
        "spatial",
        "fix_spatial_ordering",
        "austria_bbox_covering_file",
        make_unsorted,
        ["--random-sample-size", "20"],
    ),
    ("bbox", "fix_bbox_all", "austria_bbox_covering_file", _make_without_bbox, []),
    ("bbox", "fix_bbox_removal", "fields_geom_type_only_file", make_snappy, []),
    (
        "all",
        "apply_all_fixes",
        "austria_bbox_covering_file",
        _make_snappy_unsorted,
        ["--random-sample-size", "20"],
    ),
]


class TestOneFailureAmongMany:
    """A fix that raises on one file must not silence the rest, or exit 0."""

    @pytest.mark.parametrize(
        "command,fix_function,fixture,make_broken,extra",
        FAILURE_CASES,
        ids=[f"{command}-{fn}" for command, fn, *_ in FAILURE_CASES],
    )
    def test_the_run_continues_and_the_exit_code_reflects_the_failure(
        self, command, fix_function, fixture, make_broken, extra, tmp_path, request
    ):
        source = Path(str(request.getfixturevalue(fixture)))
        data = tmp_path / "data"
        data.mkdir()
        files = [make_broken(source, data / f"{name}.parquet") for name in NAMES]
        before = {path: digest(path) for path in files}
        real = getattr(core_check_fixes, fix_function)

        def flaky(parquet_file, *args, **kwargs):
            if Path(parquet_file).name == "b.parquet":
                raise RuntimeError("synthetic fix failure")
            return real(parquet_file, *args, **kwargs)

        with mock.patch.object(core_check_fixes, fix_function, flaky):
            result = invoke("check", command, data / "*.parquet", "--fix", *extra)

        assert result.exit_code == 1, result.output
        assert "synthetic fix failure" in result.output
        assert "Failed to fix 1 file:" in result.output
        rewritten = {path.name for path in files if digest(path) != before[path]}
        assert rewritten == {"a.parquet", "d.parquet"}, result.output

    def test_the_files_that_did_get_fixed_are_still_sound(
        self, austria_bbox_covering_file, tmp_path
    ):
        """A partial run is not an excuse for a half-written file."""
        files = broken_files(FIX_CASES[0], austria_bbox_covering_file, tmp_path / "data")
        real = core_check_fixes.fix_row_groups

        def flaky(parquet_file, *args, **kwargs):
            if Path(parquet_file).name == "b.parquet":
                raise RuntimeError("synthetic fix failure")
            return real(parquet_file, *args, **kwargs)

        with mock.patch.object(core_check_fixes, "fix_row_groups", flaky):
            result = invoke("check", "row-group", tmp_path / "data" / "*.parquet", "--fix")

        assert result.exit_code == 1, result.output
        for path in files:
            if path.name != "b.parquet":
                assert_austria_output_is_sound(path)


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
        assert "No fixes needed" in result.output
        assert "Fixed 1 file:" in result.output
        assert digest(healthy) == untouched, "a sound file must not be rewritten"
        assert not Path(f"{healthy}.bak").exists()
        assert_austria_output_is_sound(broken)
