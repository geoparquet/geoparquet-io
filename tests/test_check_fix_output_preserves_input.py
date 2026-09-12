"""``gpio check ... --fix --fix-output OTHER`` must leave the input byte-identical.

``--fix-output`` is the option a user reaches for precisely *because* they do not
want their file touched. gpio took that as licence to consume it: on the branch
where ``check bbox --fix`` had only a ``covering`` key to add, ``fix_bbox_all``
reached ``shutil.move(current_file, output_file)`` with ``current_file`` still
the user's own path -- so the input was renamed away and, because
``handle_fix_common`` only makes a ``.bak`` for an in-place fix, there was no
backup either. The file was simply gone (#1036).

The invariant these tests pin is stronger than "the input still exists":

    The user's input is never moved, unlinked or written over unless the
    operation is explicitly in place, and an in-place operation takes the
    backup first.

So the assertion is a SHA-256 of the input taken before the run and compared
after it -- existence alone cannot tell a file that was left alone from one that
was rewritten in place under a different name. It is checked for **every**
``check`` subcommand that accepts ``--fix-output``, not only ``bbox``: the same
"move whatever ``current_file`` happens to be" shape lives in
``_apply_compression_fix``'s no-rewrite-needed branch, which ``check all --fix``
reaches, and a shape like this is worth a net rather than a patch.

Every case runs a fix that actually *does* something -- a fix that declines to
run cannot destroy anything, and would pass this file vacuously -- so each one
asserts the command did not print "No fix needed" and that the output is a
readable Parquet file with the input's rows.

#1043 pins the same defect from the other side, as
``test_check_fix_output_is_valid.py::TestKnownDefectsInTheFixes::
test_a_fix_to_another_path_leaves_the_input_where_it_was`` under
``xfail(strict=True)``. That marker has to go when either lands second --
the assertion under it is correct and passes now; only the expectation of
failure is stale.

Refs: https://github.com/geoparquet/geoparquet-io/issues/1036
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli

# ---------------------------------------------------------------------------
# Inputs: one per defect a `--fix` repairs, each built so the fix really runs.
# ---------------------------------------------------------------------------


def _write_v11(source: Path, target: Path, *, drop_bbox: bool) -> Path:
    """A real GeoParquet 1.1 file with no ``covering`` key, optionally no bbox.

    Written through DuckDB's ``KV_METADATA`` rather than pyarrow for the reason
    ``conftest.places_v11_file`` gives: ``write_table`` either drops the whole
    schema-metadata block or adds an ``ARROW:schema`` key that ``add
    bbox-metadata`` cannot survive.
    """
    from geoparquet_io.core.common import get_duckdb_connection

    geo = json.loads(pq.read_metadata(str(source)).metadata[b"geo"].decode("utf-8"))
    geo["version"] = "1.1.0"
    geo["columns"][geo["primary_column"]].pop("covering", None)
    geo_json = json.dumps(geo).replace("'", "''")

    projection = "* EXCLUDE (bbox)" if drop_bbox else "*"
    con = get_duckdb_connection(load_spatial=False)
    try:
        con.execute(
            f"COPY (SELECT {projection} FROM '{source.as_posix()}') "
            f"TO '{target.as_posix()}' "
            f"(FORMAT PARQUET, GEOPARQUET_VERSION 'NONE', KV_METADATA {{geo: '{geo_json}'}})"
        )
    finally:
        con.close()
    return target


def build_bbox_covering_missing(places_test_file, tmp_path) -> Path:
    """1.1, a ``bbox`` column, no ``covering``: the exact shape of #1036.

    Every 1.1 file written before coverings were common looks like this, and it
    is the one branch of ``fix_bbox_all`` that needed no rewrite -- so
    ``current_file`` was never redirected away from the user's path before the
    move.
    """
    return _write_v11(Path(places_test_file), tmp_path / "input.parquet", drop_bbox=False)


def build_bbox_column_missing(places_test_file, tmp_path) -> Path:
    """1.1 with no bbox column at all: ``check bbox --fix`` adds one."""
    return _write_v11(Path(places_test_file), tmp_path / "input.parquet", drop_bbox=True)


def build_bbox_undeclared(fields_geom_type_only_file, tmp_path) -> Path:
    """Native-geo-only carrying a bbox column: ``check bbox --fix`` removes it."""
    target = tmp_path / "input.parquet"
    shutil.copy2(fields_geom_type_only_file, target)
    return target


def build_snappy(places_test_file, tmp_path) -> Path:
    """SNAPPY: ``check compression --fix`` re-compresses it."""
    target = tmp_path / "input.parquet"
    with pq.ParquetFile(str(places_test_file)) as reader:
        table = reader.read()
    pq.write_table(table, str(target), compression="snappy")
    return target


def build_tiny_row_groups(places_test_file, tmp_path) -> Path:
    """Five-row row groups: ``check row-group --fix`` merges them."""
    target = tmp_path / "input.parquet"
    with pq.ParquetFile(str(places_test_file)) as reader:
        table = reader.read()
    pq.write_table(table, str(target), row_group_size=5, compression="zstd")
    return target


def build_unsorted(unsorted_test_file, tmp_path) -> Path:
    """Rows in id order across 15 row groups: ``check spatial --fix`` sorts them."""
    target = tmp_path / "input.parquet"
    shutil.copy2(unsorted_test_file, target)
    return target


#: ``(subcommand, builder, fixture the builder needs)``. One entry per
#: ``--fix-output``-accepting subcommand, and one per *branch* of ``check bbox``,
#: because the branches differ in whether a rewrite ever redirects the path that
#: is eventually moved.
CASES = [
    pytest.param("bbox", build_bbox_covering_missing, "places_test_file", id="bbox-covering-only"),
    pytest.param("bbox", build_bbox_column_missing, "places_test_file", id="bbox-add-column"),
    pytest.param(
        "bbox", build_bbox_undeclared, "fields_geom_type_only_file", id="bbox-remove-column"
    ),
    pytest.param("compression", build_snappy, "places_test_file", id="compression"),
    pytest.param("row-group", build_tiny_row_groups, "places_test_file", id="row-group"),
    pytest.param("spatial", build_unsorted, "unsorted_test_file", id="spatial"),
    pytest.param("all", build_bbox_covering_missing, "places_test_file", id="all-covering-only"),
    pytest.param("all", build_tiny_row_groups, "places_test_file", id="all-tiny-row-groups"),
]


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _rows(path: Path) -> int:
    with pq.ParquetFile(str(path)) as reader:
        return reader.metadata.num_rows


@pytest.mark.parametrize(("subcommand", "builder", "fixture_name"), CASES)
def test_a_fix_to_another_path_leaves_the_input_byte_identical(
    subcommand, builder, fixture_name, request, tmp_path
):
    """The whole point of ``--fix-output``: repair *elsewhere*, touch nothing here."""
    source = builder(request.getfixturevalue(fixture_name), tmp_path)
    before = _digest(source)
    rows_in = _rows(source)
    output = tmp_path / "output.parquet"

    result = CliRunner().invoke(
        cli, ["check", subcommand, str(source), "--fix", "--fix-output", str(output)]
    )
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


@pytest.mark.parametrize(("subcommand", "builder", "fixture_name"), CASES)
def test_a_fix_to_another_path_leaves_no_backup_of_an_untouched_input(
    subcommand, builder, fixture_name, request, tmp_path
):
    """No ``.bak``, because nothing was at risk -- and no staging file either.

    The corollary of the invariant. ``handle_fix_common`` declines to back up a
    file it is not going to write, which is correct *given* the input is only
    read; it was the pairing of that decision with a fix that moved the input
    anyway that made #1036 unrecoverable. If a fix ever starts needing a backup
    here, this is the test that says so.
    """
    source = builder(request.getfixturevalue(fixture_name), tmp_path)
    output = tmp_path / "output.parquet"

    result = CliRunner().invoke(
        cli, ["check", subcommand, str(source), "--fix", "--fix-output", str(output)]
    )
    assert result.exit_code == 0, result.output

    assert not source.with_name(source.name + ".bak").exists()
    assert list(tmp_path.glob(".gpio-fix-*")) == [], "a staging file was left behind"
    assert list(tmp_path.glob("*.tmp")) == [], "a staging file was left behind"


# ---------------------------------------------------------------------------
# The rule itself, at the level it is written down. `check all --fix` cannot
# currently reach `_place_result` with the user's own file -- every check that
# sets `fix_available` today routes step 1, 2 or 3 through a scratch file first
# -- but `apply_all_fixes` takes its check results from the caller, and the next
# check to grow a `fix_available` key gets there for free. The branch is in the
# code, so it is pinned here rather than left to a future reader to rediscover.
# ---------------------------------------------------------------------------


class TestPlaceResult:
    """The one place a finished fix is put at the output path."""

    def test_the_users_input_is_copied_not_moved(self, tmp_path):
        from geoparquet_io.core.check_fixes import _place_result

        source = tmp_path / "input.parquet"
        source.write_bytes(b"the original bytes")
        output = tmp_path / "output.parquet"

        _place_result(str(source), str(output), str(source))

        assert source.read_bytes() == b"the original bytes"
        assert output.read_bytes() == b"the original bytes"

    def test_a_scratch_file_is_moved(self, tmp_path):
        """gpio's own scratch file has no second referent, so it is consumed."""
        from geoparquet_io.core.check_fixes import _place_result

        scratch = tmp_path / "scratch.parquet"
        scratch.write_bytes(b"a rewrite")
        output = tmp_path / "output.parquet"
        output.write_bytes(b"a stale output to be replaced")

        _place_result(str(scratch), str(output), str(tmp_path / "input.parquet"))

        assert not scratch.exists()
        assert output.read_bytes() == b"a rewrite"
        assert list(tmp_path.glob(".gpio-fix-*")) == [], "a staging file was left behind"

    def test_an_aliased_output_is_left_alone(self, tmp_path):
        """``--fix-output ./out.parquet`` for ``out.parquet`` is not a second file.

        Comparing the raw strings would see two paths and try to place the file
        on top of itself -- ``shutil.copy2`` raises ``SameFileError`` for that
        (#959).
        """
        from geoparquet_io.core.check_fixes import _place_result

        output = tmp_path / "output.parquet"
        output.write_bytes(b"already in place")
        alias = tmp_path / "." / "output.parquet"

        _place_result(str(output), str(alias), str(tmp_path / "input.parquet"))

        assert output.read_bytes() == b"already in place"

    def test_replacing_an_existing_output_with_the_input_is_still_a_copy(self, tmp_path):
        """The copy goes through the same staging, so the swap stays atomic."""
        from geoparquet_io.core.check_fixes import _place_result

        source = tmp_path / "input.parquet"
        source.write_bytes(b"the original bytes")
        output = tmp_path / "output.parquet"
        output.write_bytes(b"a stale output to be replaced")

        _place_result(str(source), str(output), str(source))

        assert source.read_bytes() == b"the original bytes"
        assert output.read_bytes() == b"the original bytes"
        assert list(tmp_path.glob(".gpio-fix-*")) == [], "a staging file was left behind"


class TestFixBboxAllStaging:
    """``fix_bbox_all``'s in-place rewrite, which is the only branch that stages."""

    def test_an_in_place_column_fix_leaves_no_staging_file(self, places_test_file, tmp_path):
        from geoparquet_io.core.check_fixes import fix_bbox_all

        target = tmp_path / "in_place.parquet"
        table = pq.read_table(str(places_test_file)).drop(["bbox"])
        pq.write_table(table, str(target))
        before = _digest(target)

        fix_bbox_all(str(target), str(target), needs_column=True, needs_metadata=False)

        assert _digest(target) != before, "the in-place fix left the file untouched"
        assert "bbox" in pq.read_schema(str(target)).names
        assert list(tmp_path.glob(".gpio-fix-*")) == [], "a staging file was left behind"

    def test_a_failed_in_place_column_fix_keeps_the_original(
        self, places_test_file, tmp_path, monkeypatch
    ):
        """The #959 rule: a rewrite that never landed takes its scratch with it.

        The original is untouched because it was only ever read -- the swap is
        an ``os.replace`` that never happened.
        """
        from geoparquet_io.core import check_fixes

        target = tmp_path / "in_place.parquet"
        table = pq.read_table(str(places_test_file)).drop(["bbox"])
        pq.write_table(table, str(target))
        before = _digest(target)

        def explode(input_parquet, output_parquet, **kwargs):
            Path(output_parquet).write_bytes(b"half a parquet file")
            raise RuntimeError("disk went away")

        monkeypatch.setattr(check_fixes, "add_bbox_column", explode)

        with pytest.raises(RuntimeError, match="disk went away"):
            fix_bbox_all = check_fixes.fix_bbox_all
            fix_bbox_all(str(target), str(target), needs_column=True, needs_metadata=False)

        assert _digest(target) == before
        assert list(tmp_path.glob(".gpio-fix-*")) == [], "a staging file was left behind"

    def test_nothing_to_do_writes_nothing(self, places_test_file, tmp_path):
        """Both flags false is unreachable from the CLI, and must stay inert.

        ``check bbox --fix`` only calls this when ``needs_bbox_column`` or
        ``needs_bbox_metadata`` is set, but ``fix_bbox_all`` is a public core
        function -- and the one thing it must not do with nothing to do is
        relocate the input.
        """
        from geoparquet_io.core.check_fixes import fix_bbox_all

        source = tmp_path / "input.parquet"
        shutil.copy2(places_test_file, source)
        before = _digest(source)
        output = tmp_path / "output.parquet"

        result = fix_bbox_all(str(source), str(output), needs_column=False, needs_metadata=False)

        assert result["success"] is True
        assert _digest(source) == before
        assert not output.exists()
