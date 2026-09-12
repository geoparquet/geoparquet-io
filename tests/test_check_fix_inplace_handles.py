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
import re
import shutil
from dataclasses import dataclass, field
from itertools import count
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core import check_fixes
from geoparquet_io.core.exceptions import RemoteAccessError
from geoparquet_io.core.file_utils import is_same_file_path
from tests.fix_output_oracle import CRS84, assert_fix_output_is_sound

# `COPY (...) TO '<path>' (...)`, with the SQL-literal doubling undone by the
# caller. DuckDB is the only writer that moves a file gpio did not name.
_COPY_TO = re.compile(r"\bCOPY\b.*?\bTO\b\s+'((?:[^']|'')*)'", re.IGNORECASE | re.DOTALL)


@dataclass
class _Rename:
    """One ``os.replace()``, and what still held either of its paths open.

    Windows refuses ``MoveFileEx`` when *either* the source or the destination
    has an open handle, so both are recorded.
    """

    source: str
    destination: str
    holders: list[str] = field(default_factory=list)
    source_holders: list[str] = field(default_factory=list)


@dataclass
class _Probe:
    copy_destinations: list[str] = field(default_factory=list)
    renames: list[_Rename] = field(default_factory=list)

    def swaps_onto(self, path) -> list[_Rename]:
        return [r for r in self.renames if is_same_file_path(r.destination, str(path))]

    def copies_onto(self, path) -> list[str]:
        return [d for d in self.copy_destinations if is_same_file_path(d, str(path))]


@pytest.fixture
def fix_probe(monkeypatch):
    """Watch every DuckDB ``COPY``, every ``os.replace()``, and every open handle.

    Handles are tracked at the Python level rather than through the OS, and by
    *discipline* rather than by liveness: a ``ParquetFile`` counts as a holder
    from construction until its ``close()`` (which ``with`` calls), and a DuckDB
    connection is remembered against every path its SQL names until it is
    closed. Liveness would not do -- on CPython a reader bound to a local is
    collected at function return whether or not anyone closed it, so a probe
    keyed on liveness read the un-``with``ed reads in ``get_parquet_metadata``
    and the pyarrow fast paths as already gone, and passed with them reverted.
    That is what "open on Windows" means here, and unlike an ``/proc``-style
    descriptor scan it reads the same on macOS, Linux and Windows.
    """
    probe = _Probe()
    unclosed_readers: dict[int, str] = {}
    connection_paths: dict[int, set[str]] = {}
    ids = count()

    def holders_of(path: str) -> list[str]:
        held = [
            f"pyarrow.ParquetFile({seen}) never closed"
            for seen in unclosed_readers.values()
            if is_same_file_path(seen, path)
        ]
        held += [
            f"open duckdb connection that read {path}"
            for paths in connection_paths.values()
            if any(is_same_file_path(seen, path) for seen in paths)
        ]
        return held

    original_reader_init = pq.ParquetFile.__init__
    original_reader_close = pq.ParquetFile.close

    def reader_init(self, source, *args, **kwargs):
        original_reader_init(self, source, *args, **kwargs)
        if isinstance(source, (str, os.PathLike)):
            self._probe_key = next(ids)
            unclosed_readers[self._probe_key] = str(source)

    def reader_close(self, *args, **kwargs):
        unclosed_readers.pop(getattr(self, "_probe_key", None), None)
        return original_reader_close(self, *args, **kwargs)

    original_execute = duckdb.DuckDBPyConnection.execute
    original_close = duckdb.DuckDBPyConnection.close

    def execute(self, query, *args, **kwargs):
        if isinstance(query, str):
            destination = _COPY_TO.search(query)
            if destination:
                probe.copy_destinations.append(destination.group(1).replace("''", "'"))
            connection_paths.setdefault(id(self), set()).update(
                literal.replace("''", "'") for literal in re.findall(r"'((?:[^']|'')*)'", query)
            )
        return original_execute(self, query, *args, **kwargs)

    def close(self):
        connection_paths.pop(id(self), None)
        return original_close(self)

    original_replace = os.replace

    def replace(src, dst, **kwargs):
        probe.renames.append(
            _Rename(str(src), str(dst), holders_of(str(dst)), holders_of(str(src)))
        )
        return original_replace(src, dst, **kwargs)

    monkeypatch.setattr(pq.ParquetFile, "__init__", reader_init)
    monkeypatch.setattr(pq.ParquetFile, "close", reader_close)
    monkeypatch.setattr(duckdb.DuckDBPyConnection, "execute", execute)
    monkeypatch.setattr(duckdb.DuckDBPyConnection, "close", close)
    monkeypatch.setattr(os, "replace", replace)
    return probe


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


IN_PLACE_FIXES = [
    pytest.param("bbox", "undeclared_bbox_file", id="bbox"),
    pytest.param("row-group", "tiny_row_group_file", id="row-group"),
    pytest.param("compression", "snappy_file", id="compression"),
]

#: What each of those three inputs must look like once its fix has run, for the
#: shared WP-1 oracle (#1018). "The bytes changed" is all the repair assertion
#: below could see on its own, and an in-place fix has no backup to fall back on
#: under ``--no-backup``: if the staged rewrite it renames into place is not a
#: valid GeoParquet file, the user's file is simply gone.
IN_PLACE_FIX_OUTPUTS = {
    # A native-geo-only input loses its bbox column and stays native-geo-only:
    # no `geo` key invented, the CRS still only in the Parquet logical type.
    "bbox": {"expected_rows": 100, "expects_covering": False, "expected_version_prefix": None},
    # places is 1.0 in; the write facade repairs it as 1.1 and declares the
    # bbox column it kept.
    "row-group": {
        "expected_rows": 766,
        "expects_covering": True,
        "expected_version_prefix": "1.1",
    },
    "compression": {
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
    assert list(target.parent.glob(".gpio-fix-*")) == [], "a staging file was left behind"
    # ...and what was renamed into place is a file gpio itself accepts (#1018).
    assert_fix_output_is_sound(target, expected_crs=CRS84, **IN_PLACE_FIX_OUTPUTS[subcommand])


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

    monkeypatch.setattr(check_fixes, "write_parquet_with_metadata", explode)

    with pytest.raises(duckdb.IOException):
        check_fixes._rewrite_through_staging(
            str(target), str(target), "SELECT 1", verbose=False, profile=None
        )

    assert target.read_bytes() == b"the original bytes"
    assert list(tmp_path.glob(".gpio-fix-*")) == [], "a staging file was left behind"


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

    monkeypatch.setattr(check_fixes, "write_parquet_with_metadata", write_something)
    monkeypatch.setattr(check_fixes, "get_duckdb_connection", lambda **kwargs: duckdb.connect())
    monkeypatch.setattr(os, "replace", no_space)

    with pytest.raises(OSError, match="No space left on device"):
        check_fixes._rewrite_through_staging(
            str(target), str(target), "SELECT 1", verbose=False, profile=None
        )

    assert target.read_bytes() == b"the original bytes"
    assert list(tmp_path.glob(".gpio-fix-*")) == [], "a staging file was left behind"


def test_a_remote_in_place_fix_is_handed_to_the_facade_unstaged(monkeypatch):
    """``check row-group s3://b/k.parquet --fix --overwrite``: nothing to rename over here.

    The facade stages a remote output locally and uploads it. Staging it a
    second time on this machine and then trying to ``os.replace`` onto a URL is
    how the first version of this fix's sibling (#1029) turned a working command
    into a ``TypeError``.
    """
    seen: list[str] = []
    monkeypatch.setattr(
        check_fixes,
        "write_parquet_with_metadata",
        lambda con, query, output_file, **kwargs: seen.append(output_file),
    )
    monkeypatch.setattr(check_fixes, "get_duckdb_connection", lambda **kwargs: duckdb.connect())

    check_fixes._rewrite_through_staging(
        "s3://bucket/key.parquet",
        "s3://bucket/key.parquet",
        "SELECT 1",
        verbose=False,
        profile=None,
    )

    assert seen == ["s3://bucket/key.parquet"]


def test_a_remote_rewrite_that_cannot_read_its_input_says_so(monkeypatch):
    """A remote input keeps its credential/endpoint hint through the staging move."""

    def explode(**kwargs):
        raise duckdb.IOException("HTTP 403")

    monkeypatch.setattr(check_fixes, "write_parquet_with_metadata", explode)
    monkeypatch.setattr(check_fixes, "get_duckdb_connection", lambda **kwargs: duckdb.connect())

    with pytest.raises(RemoteAccessError):
        check_fixes._rewrite_through_staging(
            "s3://bucket/in.parquet",
            "s3://bucket/out.parquet",
            "SELECT 1",
            verbose=False,
            profile=None,
        )
