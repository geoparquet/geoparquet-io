"""``gpio check ... --fix --fix-output OTHER`` must leave the input byte-identical.

``--fix-output`` is the option a user reaches for precisely *because* they do not
want their file touched. On the branch where ``check bbox --fix`` had only a
``covering`` key to add, ``fix_bbox_all`` reached ``shutil.move(current_file,
output_file)`` with ``current_file`` still the user's own path, so the input was
renamed away -- and, because ``handle_fix_common`` only makes a ``.bak`` for an
in-place fix, there was no backup either (#1036).

The invariant these tests pin:

    The user's input is never moved, unlinked or written over unless the
    operation is explicitly in place, and an in-place operation takes the
    backup first.

So the assertion is a SHA-256 of the input taken before the run and compared
after it -- existence alone cannot tell a file that was left alone from one that
was rewritten in place under a different name. It is checked for **every**
``check`` subcommand that accepts ``--fix-output``, because the same "move
whatever ``current_file`` happens to be" shape lived in
``_apply_compression_fix``'s no-rewrite-needed branch, which ``check all --fix``
reaches. And "in place" is decided by :func:`is_same_file_path`, not by string
equality: an aliased ``--fix-output`` (``./in.parquet``, or ``IN.parquet`` on a
case-insensitive filesystem) is an in-place fix and gets the backup.

Every case runs a fix that actually *does* something -- a fix that declines to
run cannot destroy anything -- so each one asserts the command did not print
"No fix needed", that the output holds the input's rows, and that the fix left
the output with no ``check spec`` failure the input did not already have.

Refs: https://github.com/geoparquet/geoparquet-io/issues/1036
"""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core import check_fixes
from geoparquet_io.core.check_fixes import _place_result, fix_bbox_all
from geoparquet_io.core.exceptions import GeoParquetError
from tests.native_geo_probes import spec_problems

# ---------------------------------------------------------------------------
# Inputs: one per defect a `--fix` repairs, each built so the fix really runs.
# ---------------------------------------------------------------------------


def build_bbox_covering_missing(places_v11_file: str | Path, tmp_path: Path) -> Path:
    """1.1, a ``bbox`` column, no ``covering``: the exact shape of #1036.

    The one branch of ``fix_bbox_all`` that needed no rewrite, so
    ``current_file`` was never redirected away from the user's path.
    """
    target = tmp_path / "input.parquet"
    shutil.copy2(places_v11_file, target)
    return target


def build_bbox_column_missing(places_test_file: str | Path, tmp_path: Path) -> Path:
    """No bbox column at all: ``check bbox --fix`` adds one."""
    target = tmp_path / "input.parquet"
    with pq.ParquetFile(str(places_test_file)) as reader:
        table = reader.read().drop(["bbox"])
    pq.write_table(table, str(target))
    return target


def build_bbox_undeclared(fields_geom_type_only_file: str | Path, tmp_path: Path) -> Path:
    """Native-geo-only carrying a bbox column: ``check bbox --fix`` removes it."""
    target = tmp_path / "input.parquet"
    shutil.copy2(fields_geom_type_only_file, target)
    return target


def build_snappy(places_test_file: str | Path, tmp_path: Path) -> Path:
    """SNAPPY: ``check compression --fix`` re-compresses it."""
    target = tmp_path / "input.parquet"
    with pq.ParquetFile(str(places_test_file)) as reader:
        table = reader.read()
    pq.write_table(table, str(target), compression="snappy")
    return target


def build_tiny_row_groups(places_test_file: str | Path, tmp_path: Path) -> Path:
    """Five-row row groups: ``check row-group --fix`` merges them."""
    target = tmp_path / "input.parquet"
    with pq.ParquetFile(str(places_test_file)) as reader:
        table = reader.read()
    pq.write_table(table, str(target), row_group_size=5, compression="zstd")
    return target


def build_unsorted(unsorted_test_file: str | Path, tmp_path: Path) -> Path:
    """Rows in id order across 15 row groups: ``check spatial --fix`` sorts them."""
    target = tmp_path / "input.parquet"
    shutil.copy2(unsorted_test_file, target)
    return target


#: ``(subcommand, builder, fixture the builder needs)``. One entry per
#: ``--fix-output``-accepting subcommand, and one per *branch* of ``check bbox``,
#: because the branches differ in whether a rewrite ever redirects the path that
#: is eventually placed at the output.
CASES = [
    pytest.param("bbox", build_bbox_covering_missing, "places_v11_file", id="bbox-covering-only"),
    pytest.param("bbox", build_bbox_column_missing, "places_test_file", id="bbox-add-column"),
    pytest.param(
        "bbox", build_bbox_undeclared, "fields_geom_type_only_file", id="bbox-remove-column"
    ),
    pytest.param("compression", build_snappy, "places_test_file", id="compression"),
    pytest.param("row-group", build_tiny_row_groups, "places_test_file", id="row-group"),
    pytest.param("spatial", build_unsorted, "unsorted_test_file", id="spatial"),
    pytest.param("all", build_bbox_covering_missing, "places_v11_file", id="all-covering-only"),
    pytest.param("all", build_tiny_row_groups, "places_test_file", id="all-tiny-row-groups"),
]


#: Spec failures a fix is known to *introduce*, by case id. The only one today
#: is #1035: ``check spatial --fix`` on the 1.0 ``unsorted`` fixture upgrades it
#: to 1.1 and derives a covering over its ``xmin, xmax, ymin, ymax`` struct,
#: which the spec orders differently. ``places_v11_file``'s own failure (#1037)
#: is on the input already, so it needs no entry here.
KNOWN_INTRODUCED_SPEC_FAILURES = {
    "spatial": {"covering_bbox_structure_geometry"},
}


def _spec_failure_names(path: Path) -> set[str]:
    return {problem.split(":", 1)[0] for problem in spec_problems(path)}


def _dot_alias(path: Path) -> str:
    """``dir/./name``: a second spelling of one path.

    Built as a string on purpose -- ``pathlib`` collapses a ``.`` segment, so
    ``tmp_path / "." / name`` is not an alias at all, it is the same string.
    """
    return f"{path.parent}{os.sep}.{os.sep}{path.name}"


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _rows(path: Path) -> int:
    with pq.ParquetFile(str(path)) as reader:
        return reader.metadata.num_rows


def _run(*args: str, input: str | None = None):
    return CliRunner().invoke(cli, ["check", *args], input=input)


def _assert_nothing_left_behind(tmp_path: Path) -> None:
    assert list(tmp_path.glob(".gpio-fix-*")) == [], "a staging file was left behind"
    assert list(tmp_path.glob("*.tmp")) == [], "a staging file was left behind"


@pytest.mark.parametrize(("subcommand", "builder", "fixture_name"), CASES)
def test_a_fix_to_another_path_leaves_the_input_byte_identical(
    subcommand, builder, fixture_name, request, tmp_path
):
    """Repair *elsewhere*, touch nothing here: no rewrite, no ``.bak``, no scratch."""
    source = builder(request.getfixturevalue(fixture_name), tmp_path)
    before = _digest(source)
    rows_in = _rows(source)
    problems_in = _spec_failure_names(source)
    output = tmp_path / "output.parquet"

    result = _run(subcommand, str(source), "--fix", "--fix-output", str(output))
    assert result.exit_code == 0, result.output

    # Non-vacuity: a fix that declined to run cannot destroy anything.
    assert "No fix needed" not in result.output, result.output
    assert "No fixes needed" not in result.output, result.output

    assert source.exists(), (
        f"`check {subcommand} --fix --fix-output` wrote elsewhere and destroyed the input"
    )
    assert _digest(source) == before, (
        f"`check {subcommand} --fix --fix-output` rewrote the input it was told not to touch"
    )
    assert output.exists(), f"`check {subcommand} --fix --fix-output` produced no output"
    assert _rows(output) == rows_in
    allowed = KNOWN_INTRODUCED_SPEC_FAILURES.get(request.node.callspec.id, set())
    assert _spec_failure_names(output) - problems_in <= allowed, (
        f"the fix introduced spec failures: {sorted(_spec_failure_names(output) - problems_in)}"
    )

    # Nothing was at risk, so nothing was backed up; nothing was left behind.
    assert not source.with_name(source.name + ".bak").exists()
    _assert_nothing_left_behind(tmp_path)


# ---------------------------------------------------------------------------
# A second spelling of the input's own path is an in-place fix, and takes the
# backup first. `is_same_file_path` decides that; raw string equality is what
# let `check all` and a case-only alias write over an input with no `.bak`.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("subcommand", ["bbox", "all"])
def test_an_aliased_fix_output_is_an_in_place_fix_with_a_backup(
    subcommand, places_v11_file, tmp_path
):
    source = build_bbox_covering_missing(places_v11_file, tmp_path)
    before = _digest(source)
    problems_in = _spec_failure_names(source)
    alias = _dot_alias(source)

    result = _run(subcommand, str(source), "--fix", "--fix-output", str(alias))
    assert result.exit_code == 0, result.output

    backup = source.with_name(source.name + ".bak")
    assert backup.exists(), "an aliased --fix-output was treated as another file: no backup"
    assert _digest(backup) == before
    assert _digest(source) != before, "the in-place fix left the file untouched"
    assert _spec_failure_names(source) <= problems_in
    _assert_nothing_left_behind(tmp_path)


def test_check_all_with_an_aliased_fix_output_and_no_backup_still_asks(places_v11_file, tmp_path):
    """``--no-backup`` on an in-place fix prompts; an alias must not skip the prompt."""
    source = build_bbox_covering_missing(places_v11_file, tmp_path)
    before = _digest(source)
    alias = _dot_alias(source)

    result = _run(
        "all", str(source), "--fix", "--fix-output", str(alias), "--no-backup", input="n\n"
    )

    assert result.exit_code != 0
    assert "without backup" in result.output
    assert _digest(source) == before


def test_a_fix_output_differing_only_in_case_is_an_in_place_fix(places_v11_file, tmp_path):
    """``--fix-output IN.parquet`` for ``in.parquet`` on macOS or Windows.

    ``Path.resolve()`` does not fold case on macOS, so the resolve-based
    comparison saw two files: no backup, no prompt, and then the staged
    rewrite landed on the input's inode anyway.
    """
    source = build_bbox_covering_missing(places_v11_file, tmp_path)
    alias = tmp_path / "INPUT.parquet"
    if not alias.exists():
        pytest.skip("case-sensitive filesystem")
    before = _digest(source)
    problems_in = _spec_failure_names(source)

    result = _run("bbox", str(source), "--fix", "--fix-output", str(alias))
    assert result.exit_code == 0, result.output

    backup = source.with_name(source.name + ".bak")
    assert backup.exists(), "a case-only alias was treated as another file: no backup"
    assert _digest(backup) == before
    assert _digest(source) != before
    assert _spec_failure_names(source) <= problems_in


# ---------------------------------------------------------------------------
# The rule at the level it is written down. `check all --fix` cannot currently
# reach `_place_result` with the user's own file -- every check that sets
# `fix_available` today routes step 1, 2 or 3 through a scratch file first --
# but `apply_all_fixes` takes its check results from the caller, and the next
# check to grow a `fix_available` key gets there for free.
# ---------------------------------------------------------------------------


class TestPlaceResult:
    """The one place a finished fix is put at the output path."""

    def test_a_scratch_file_is_moved(self, tmp_path):
        """gpio's own scratch file has no second referent, so it is consumed."""
        scratch = tmp_path / "scratch.parquet"
        scratch.write_bytes(b"a rewrite")
        output = tmp_path / "output.parquet"
        output.write_bytes(b"a stale output to be replaced")

        _place_result(str(scratch), str(output), owned=True)

        assert not scratch.exists()
        assert output.read_bytes() == b"a rewrite"
        _assert_nothing_left_behind(tmp_path)

    def test_an_aliased_output_is_left_alone(self, tmp_path, monkeypatch):
        """``--fix-output ./out.parquet`` for ``out.parquet`` is not a second file.

        Comparing the raw strings would see two paths and try to place the file
        on top of itself -- ``shutil.copy2`` raises ``SameFileError`` for that
        (#959).
        """
        output = tmp_path / "output.parquet"
        output.write_bytes(b"already in place")
        alias = _dot_alias(output)

        def never(*args, **kwargs):
            raise AssertionError(f"a file was placed on top of itself: {args}")

        monkeypatch.setattr(check_fixes.shutil, "move", never)
        monkeypatch.setattr(check_fixes.shutil, "copy2", never)
        monkeypatch.setattr(check_fixes.os, "replace", never)

        _place_result(str(output), str(alias), owned=False)

        assert output.read_bytes() == b"already in place"

    def test_a_failed_placement_leaves_an_existing_output_intact(self, tmp_path, monkeypatch):
        """The swap is ``_staged_output``'s: a move that dies mid-way touches nothing."""
        scratch = tmp_path / "scratch.parquet"
        scratch.write_bytes(b"a rewrite")
        output = tmp_path / "output.parquet"
        output.write_bytes(b"the previous output")

        def die_half_way(src, dst, *args, **kwargs):
            Path(dst).write_bytes(b"half")
            raise OSError("disk went away")

        monkeypatch.setattr(check_fixes.shutil, "move", die_half_way)

        with pytest.raises(OSError, match="disk went away"):
            _place_result(str(scratch), str(output), owned=True)

        assert output.read_bytes() == b"the previous output"
        _assert_nothing_left_behind(tmp_path)

    def test_the_users_input_is_copied_even_over_an_existing_output(self, tmp_path):
        """The copy goes through the same staging, so the swap stays atomic."""
        source = tmp_path / "input.parquet"
        source.write_bytes(b"the original bytes")
        output = tmp_path / "output.parquet"
        output.write_bytes(b"a stale output to be replaced")

        _place_result(str(source), str(output), owned=False)

        assert source.read_bytes() == b"the original bytes"
        assert output.read_bytes() == b"the original bytes"
        _assert_nothing_left_behind(tmp_path)


class TestFixBboxAllStaging:
    """``fix_bbox_all``'s in-place rewrite, which is the only branch that stages."""

    def test_an_in_place_column_fix_leaves_no_staging_file(self, places_test_file, tmp_path):
        target = build_bbox_column_missing(places_test_file, tmp_path)
        before = _digest(target)

        fix_bbox_all(str(target), str(target), needs_column=True, needs_metadata=False)

        assert _digest(target) != before, "the in-place fix left the file untouched"
        assert "bbox" in pq.read_schema(str(target)).names
        _assert_nothing_left_behind(tmp_path)

    def test_a_failed_in_place_column_fix_keeps_the_original(
        self, places_test_file, tmp_path, monkeypatch
    ):
        """The #959 rule: a rewrite that never landed takes its scratch with it.

        The original is untouched because it was only ever read -- the swap is
        an ``os.replace`` that never happened.
        """
        target = build_bbox_column_missing(places_test_file, tmp_path)
        before = _digest(target)

        def explode(input_parquet, output_parquet, **kwargs):
            Path(output_parquet).write_bytes(b"half a parquet file")
            raise RuntimeError("disk went away")

        monkeypatch.setattr(check_fixes, "add_bbox_column", explode)

        with pytest.raises(RuntimeError, match="disk went away"):
            check_fixes.fix_bbox_all(
                str(target), str(target), needs_column=True, needs_metadata=False
            )

        assert _digest(target) == before
        _assert_nothing_left_behind(tmp_path)

    def test_nothing_to_do_writes_nothing(self, places_test_file, tmp_path):
        """Both flags false is unreachable from the CLI, and must stay inert.

        ``fix_bbox_all`` is a public core function, and the one thing it must
        not do with nothing to do is relocate the input -- or report a fix.
        """
        source = tmp_path / "input.parquet"
        shutil.copy2(places_test_file, source)
        before = _digest(source)
        output = tmp_path / "output.parquet"

        result = fix_bbox_all(str(source), str(output), needs_column=False, needs_metadata=False)

        assert result == {"fix_applied": None, "success": True}
        assert _digest(source) == before
        assert not output.exists()


# ---------------------------------------------------------------------------
# The chain's scratch files: created in the system temp dir, always removed.
# ---------------------------------------------------------------------------


class TestCheckAllScratchFiles:
    @pytest.fixture
    def scratch(self, tmp_path, monkeypatch) -> Path:
        """Point ``tempfile`` at a directory this test can inspect."""
        scratch = tmp_path / "scratch"
        scratch.mkdir()
        monkeypatch.setattr(tempfile, "tempdir", str(scratch))
        return scratch

    def test_a_verbose_run_names_each_step_and_leaves_no_scratch(
        self, unsorted_test_file, tmp_path, scratch
    ):
        source = tmp_path / "input.parquet"
        with pq.ParquetFile(str(unsorted_test_file)) as reader:
            table = reader.read()
        pq.write_table(table, str(source), compression="snappy", row_group_size=10)
        output = tmp_path / "output.parquet"

        result = _run(
            "all",
            str(source),
            "--fix",
            "--fix-output",
            str(output),
            "--verbose",
            "--random-sample-size",
            "20",
        )
        assert result.exit_code == 0, result.output

        assert "[3/4] Applying Hilbert spatial ordering" in result.output
        assert "[4/4] Optimizing compression and row groups" in result.output
        assert output.exists()
        assert list(scratch.iterdir()) == [], "a scratch file was left behind"

    def test_a_failing_step_removes_the_scratch_and_keeps_the_input(
        self, places_test_file, tmp_path, scratch, monkeypatch
    ):
        source = tmp_path / "input.parquet"
        shutil.copy2(places_test_file, source)
        before = _digest(source)
        output = tmp_path / "output.parquet"

        def sort_into(src, dst, *args, **kwargs):
            shutil.copy2(src, dst)

        def explode(*args, **kwargs):
            raise RuntimeError("disk went away")

        monkeypatch.setattr(check_fixes, "fix_spatial_ordering", sort_into)
        monkeypatch.setattr(check_fixes, "fix_compression", explode)
        results = {"spatial": {"fix_available": True}, "compression": {"fix_available": True}}

        with pytest.raises(GeoParquetError, match="disk went away"):
            check_fixes.apply_all_fixes(str(source), str(output), results)

        assert _digest(source) == before
        assert not output.exists()
        assert list(scratch.iterdir()) == [], "the failed chain left its scratch behind"

    def test_a_scratch_that_will_not_delete_does_not_fail_the_fix(self, tmp_path, monkeypatch):
        stubborn = tmp_path / "scratch.parquet"
        stubborn.write_bytes(b"x")

        def refuse(path):
            raise OSError("busy")

        monkeypatch.setattr(check_fixes.os, "remove", refuse)

        check_fixes._cleanup_temp_files([str(stubborn)], output_file=None)  # no raise
