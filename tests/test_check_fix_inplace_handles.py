"""``gpio check --fix`` must not rename a file anything still has open.

An in-place ``--fix`` used to hand DuckDB its own input as the ``COPY`` target.
DuckDB writes ``tmp_<name>`` beside an existing destination and then *moves* that
onto it -- so the file being moved over was the file the same statement was
reading, and the two events are ordered by nothing but the scheduler. POSIX does
not care: a rename over an open file succeeds and the reader keeps its inode.
Windows refuses, and the whole command dies with::

    Error: IO Error: Could not move file: Access is denied.

which is why ``test_geo_native_file_loses_its_bbox_column`` failed on
``windows-latest, 3.11`` in some runs and passed in others, on the same commit.
#1009 widened the window rather than opening it: it added an unconditional
``get_parquet_metadata()`` read and an ``input_file=`` witness to those rewrites,
both of which open the destination with PyArrow immediately before the ``COPY``.

The tests below are the invariant, not the error -- there is no Windows runner
here, and a test that waits for "Access is denied" would never fail on this
machine. Both are checked on every platform:

1. no ``COPY`` a fix issues writes to the file it is reading, and
2. the swap that puts a rewrite over the user's file is gpio's own
   ``os.replace()``, called when nothing in this process holds that path open --
   no live ``pyarrow.parquet.ParquetFile``, no unclosed DuckDB connection that
   has named it.

Refs: https://github.com/geoparquet/geoparquet-io/issues/1032
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core import write_funnels
from geoparquet_io.core.exceptions import RemoteAccessError
from tests.fix_output_oracle import CRS84, NO_GEO_BLOCK, assert_fix_output_is_sound

# ---------------------------------------------------------------------------
# Inputs: one per fix, each with exactly the defect its `--fix` repairs, so the
# rewrite actually runs. A fix that no-ops proves nothing about its rename.
# ---------------------------------------------------------------------------


@pytest.fixture
def undeclared_bbox_file(fields_geom_type_only_file, tmp_path):
    """Native-geo-only with a bbox column: ``check bbox --fix`` removes it."""
    target = tmp_path / "pgo_with_bbox.parquet"
    shutil.copy(fields_geom_type_only_file, target)
    return target


@pytest.fixture
def tiny_row_group_file(places_test_file, tmp_path):
    """Five-row row groups: ``check row-group --fix`` merges them."""
    target = tmp_path / "tiny_groups.parquet"
    with pq.ParquetFile(places_test_file) as reader:
        table = reader.read()
    pq.write_table(table, target, row_group_size=5, compression="zstd")
    return target


@pytest.fixture
def snappy_file(places_test_file, tmp_path):
    """SNAPPY: ``check compression --fix`` re-compresses it."""
    target = tmp_path / "snappy.parquet"
    with pq.ParquetFile(places_test_file) as reader:
        table = reader.read()
    pq.write_table(table, target, compression="snappy")
    return target


@pytest.fixture
def no_bbox_file(places_test_file, tmp_path):
    """No bbox column: ``check bbox --fix`` adds one through ``add_computed_column``."""
    target = tmp_path / "no_bbox.parquet"
    with pq.ParquetFile(places_test_file) as reader:
        table = reader.read().drop(["bbox"])
    pq.write_table(table, target)
    return target


IN_PLACE_FIXES = [
    pytest.param("bbox", "undeclared_bbox_file", id="bbox"),
    pytest.param("bbox", "no_bbox_file", id="bbox-add-column"),
    pytest.param("row-group", "tiny_row_group_file", id="row-group"),
    pytest.param("compression", "snappy_file", id="compression"),
]

#: What each of those inputs must look like once its fix has run (keyed by the
#: fixture, since one subcommand can have several), for the
#: shared WP-1 oracle (#1018). "The bytes changed" is all the repair assertion
#: below could see on its own, and an in-place fix has no backup to fall back on
#: under ``--no-backup``: if the staged rewrite it renames into place is not a
#: valid GeoParquet file, the user's file is simply gone.
IN_PLACE_FIX_OUTPUTS = {
    # A native-geo-only input loses its bbox column and stays native-geo-only:
    # no `geo` key invented, the CRS still only in the Parquet logical type.
    "undeclared_bbox_file": {
        "expected_rows": 100,
        "expects_covering": False,
        "expected_version_prefix": NO_GEO_BLOCK,
    },
    # places without its bbox column gets one back, declared, as 1.1.
    "no_bbox_file": {
        "expected_rows": 766,
        "expects_covering": True,
        "expected_version_prefix": "1.1",
    },
    # places is 1.0 in; the write facade repairs it as 1.1 and declares the
    # bbox column it kept.
    "tiny_row_group_file": {
        "expected_rows": 766,
        "expects_covering": True,
        "expected_version_prefix": "1.1",
    },
    "snappy_file": {
        "expected_rows": 766,
        "expects_covering": True,
        "expected_version_prefix": "1.1",
    },
}


def _run_fix(subcommand: str, target) -> None:
    result = CliRunner().invoke(cli, ["check", subcommand, str(target), "--fix"])
    assert result.exit_code == 0, result.output


@pytest.mark.parametrize(("subcommand", "fixture_name"), IN_PLACE_FIXES)
def test_an_in_place_fix_never_copies_over_the_file_it_is_reading(
    subcommand, fixture_name, request, fix_probe
):
    """DuckDB's ``COPY`` must never be pointed at its own source.

    This is the defect itself: ``COPY (SELECT ... FROM 'f.parquet') TO
    'f.parquet'`` makes DuckDB move ``tmp_f.parquet`` onto the file its own scan
    has open. gpio stages the rewrite beside the output instead and does the
    swap itself, once everything is closed.
    """
    target = request.getfixturevalue(fixture_name)

    _run_fix(subcommand, target)

    assert fix_probe.copy_destinations, "no COPY ran -- the fix did not do any work"
    assert fix_probe.copies_onto(target) == [], (
        f"`check {subcommand} --fix` asked DuckDB to write over the file it reads"
    )


@pytest.mark.parametrize(("subcommand", "fixture_name"), IN_PLACE_FIXES)
def test_an_in_place_fix_swaps_its_rewrite_in_with_nothing_holding_the_file_open(
    subcommand, fixture_name, request, fix_probe
):
    """gpio owns the rename, and nothing holds the destination when it happens."""
    target = request.getfixturevalue(fixture_name)

    _run_fix(subcommand, target)

    swaps = fix_probe.swaps_onto(target)
    assert swaps, f"`check {subcommand} --fix` never renamed its rewrite over the user's file"
    for swap in swaps:
        assert swap.holders == [], (
            f"`check {subcommand} --fix` renamed over {swap.destination} while "
            f"{swap.holders} still held it open"
        )
        assert swap.source_holders == [], (
            f"`check {subcommand} --fix` renamed {swap.source} while "
            f"{swap.source_holders} still held it open"
        )


@pytest.mark.parametrize(("subcommand", "fixture_name"), IN_PLACE_FIXES)
def test_an_in_place_fix_still_repairs_the_file_and_leaves_a_backup(
    subcommand, fixture_name, request
):
    """Non-vacuity: the staged rewrite is the file the user is left with."""
    target = request.getfixturevalue(fixture_name)
    before = target.read_bytes()

    _run_fix(subcommand, target)

    assert target.read_bytes() != before, "the fix left the file untouched"
    backup = target.with_name(target.name + ".bak")
    assert backup.exists() and backup.read_bytes() == before
    assert list(target.parent.glob(".gpio-stage-*")) == [], "a staging file was left behind"
    # ...and what was renamed into place is a file gpio itself accepts (#1018).
    assert_fix_output_is_sound(target, expected_crs=CRS84, **IN_PLACE_FIX_OUTPUTS[fixture_name])


# ---------------------------------------------------------------------------
# What a failed rewrite leaves behind. Staging only helps if the file the user
# already has survives the staging step going wrong.
# ---------------------------------------------------------------------------


def test_a_failed_rewrite_keeps_the_original_and_removes_the_staging_file(tmp_path, monkeypatch):
    """The #959 rule, one function on: never discard a rewrite that landed, and
    never leave one that did not.

    The original is still the only copy of the data at this point -- under
    ``--fix --no-backup`` there is no ``.bak`` -- so a staged write that raises
    must leave it untouched and take its own scratch file with it.
    """
    target = tmp_path / "unchanged.parquet"
    target.write_bytes(b"the original bytes")

    def explode(**kwargs):
        # Half a file, the way a write that runs out of disk leaves one.
        Path(kwargs["output_file"]).write_bytes(b"half a parquet file")
        raise duckdb.IOException("disk went away")

    monkeypatch.setattr(write_funnels, "write_parquet_with_metadata", explode)

    with pytest.raises(duckdb.IOException):
        write_funnels.rewrite_file_through_funnel(
            str(target), str(target), "SELECT 1", verbose=False, profile=None
        )

    assert target.read_bytes() == b"the original bytes"
    assert list(tmp_path.glob(".gpio-stage-*")) == [], "a staging file was left behind"


def test_a_failed_move_keeps_the_original_and_discards_the_rewrite(tmp_path, monkeypatch):
    """The other half of #959: the swap itself fails (ENOSPC, EXDEV, a quota).

    The original was never unlinked, so it is intact and is the good copy. The
    completed rewrite is discarded rather than left as a dot-prefixed orphan --
    the user re-runs the fix; they do not go looking for a hidden file.
    """
    target = tmp_path / "precious.parquet"
    target.write_bytes(b"the original bytes")

    def write_something(**kwargs):
        Path(kwargs["output_file"]).write_bytes(b"a complete rewrite")

    def no_space(*_args, **_kwargs):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(write_funnels, "write_parquet_with_metadata", write_something)
    monkeypatch.setattr(write_funnels, "get_duckdb_connection", lambda **kwargs: duckdb.connect())
    monkeypatch.setattr(os, "replace", no_space)

    with pytest.raises(OSError, match="No space left on device"):
        write_funnels.rewrite_file_through_funnel(
            str(target), str(target), "SELECT 1", verbose=False, profile=None
        )

    assert target.read_bytes() == b"the original bytes"
    assert list(tmp_path.glob(".gpio-stage-*")) == [], "a staging file was left behind"


def test_a_remote_in_place_fix_stages_before_the_facade(monkeypatch):
    """The facade refuses equal paths; the rewrite owner stages and uploads remote output."""
    seen: list[str] = []
    uploads = []
    monkeypatch.setattr(write_funnels, "upload_if_remote", lambda *a, **kw: uploads.append(a))
    monkeypatch.setattr(
        write_funnels,
        "write_parquet_with_metadata",
        lambda con, query, output_file, **kwargs: seen.append(output_file),
    )
    monkeypatch.setattr(write_funnels, "get_duckdb_connection", lambda **kwargs: duckdb.connect())

    write_funnels.rewrite_file_through_funnel(
        "s3://bucket/key.parquet",
        "s3://bucket/key.parquet",
        "SELECT 1",
        verbose=False,
        profile=None,
    )

    assert len(seen) == 1 and not seen[0].startswith("s3://")
    assert uploads and uploads[0][1] == "s3://bucket/key.parquet"
    assert not Path(seen[0]).exists()


def test_a_remote_rewrite_that_cannot_read_its_input_says_so(monkeypatch):
    """A remote input keeps its credential/endpoint hint through the staging move."""

    def explode(**kwargs):
        raise duckdb.IOException("HTTP 403 https://bucket/file.parquet?X-Amz-Signature=secret")

    monkeypatch.setattr(write_funnels, "write_parquet_with_metadata", explode)
    monkeypatch.setattr(write_funnels, "get_duckdb_connection", lambda **kwargs: duckdb.connect())

    with pytest.raises(RemoteAccessError) as error:
        write_funnels.rewrite_file_through_funnel(
            "s3://bucket/in.parquet",
            "s3://bucket/out.parquet",
            "SELECT 1",
            verbose=False,
            profile=None,
        )
    assert "secret" not in str(error.value)
    assert "X-Amz-Signature" not in str(error.value)


def test_facade_refuses_aliased_input_before_touching_it(tmp_path):
    from geoparquet_io.core.exceptions import GeoParquetError

    target = tmp_path / "original.parquet"
    target.write_bytes(b"precious")
    con = duckdb.connect()
    try:
        with pytest.raises(GeoParquetError, match="refuses in-place"):
            write_funnels.write_parquet_with_metadata(
                con, "SELECT 1", f"{target.parent}/./{target.name}", input_file=str(target)
            )
    finally:
        con.close()
    assert target.read_bytes() == b"precious"
    with pytest.raises(GeoParquetError, match="refuses in-place"):
        write_funnels.write_parquet_with_metadata(
            None, "SELECT 1", "s3://bucket/input.parquet", input_file="s3://bucket/input.parquet"
        )


def test_shared_probe_tracks_context_manager_close(fix_probe, tmp_path):
    target = tmp_path / "ctx.parquet"
    with duckdb.connect() as con:
        con.execute("SELECT ?", [str(target)])
        con.execute(f"SELECT '{target}'").fetchall()
    target.write_bytes(b"source")
    other = tmp_path / "moved.parquet"
    os.replace(target, other)
    assert fix_probe.renames[-1].source_holders == []


def test_other_in_place_commands_release_handles(places_test_file, tmp_path, fix_probe):
    """Use the same probe for all three staging clients requested by #1040."""
    for command in ("bbox-metadata", "geometry-metrics", "reproject"):
        target = tmp_path / f"{command}.parquet"
        import json

        table = pq.read_table(places_test_file)
        metadata = dict(table.schema.metadata)
        block = json.loads(metadata[b"geo"])
        block["version"] = "1.1.0"
        metadata[b"geo"] = json.dumps(block).encode()
        pq.write_table(table.replace_schema_metadata(metadata), target)
        if command == "reproject":
            argv = [
                "convert",
                "reproject",
                str(target),
                str(target),
                "--dst-crs",
                "EPSG:3857",
                "--overwrite",
            ]
        elif command == "geometry-metrics":
            argv = ["add", command, str(target), str(target), "--overwrite"]
        else:
            argv = ["add", command, str(target)]
        before = target.read_bytes()
        result = CliRunner().invoke(cli, argv)
        if command == "geometry-metrics":
            # This command already refuses in-place writes, even with --overwrite.
            assert result.exit_code == 1
            assert "Cannot overwrite input file" in result.output
            assert target.read_bytes() == before
            assert not fix_probe.copies_onto(target)
            assert not fix_probe.swaps_onto(target)
            continue
        assert result.exit_code == 0, (result.output, result.exception)
        assert not fix_probe.copies_onto(target)
        swaps = fix_probe.swaps_onto(target)
        assert swaps, (command, result.output, fix_probe.renames)
        assert all(not r.holders and not r.source_holders for r in swaps), swaps


def test_verbose_fix_reports_final_destination(places_test_file, tmp_path, caplog):
    source = tmp_path / "input.parquet"
    output = tmp_path / "fixed.parquet"
    shutil.copy(places_test_file, source)
    with caplog.at_level("DEBUG", logger="geoparquet_io"):
        write_funnels.rewrite_file_through_funnel(
            str(source), str(output), f"SELECT * FROM '{source}'", verbose=True, profile=None
        )
    successes = [r.message for r in caplog.records if "Wrote " in r.message]
    assert successes and any(str(output) in text for text in successes)
    assert all(".gpio-stage-" not in text for text in successes)
