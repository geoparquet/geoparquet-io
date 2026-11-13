"""Shared in-place rewrite handle and COPY probe (#1040)."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from itertools import count

import duckdb
import pyarrow.parquet as pq
import pytest

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
        from geoparquet_io.core.file_utils import is_same_file_path

        return [r for r in self.renames if is_same_file_path(r.destination, str(path))]

    def copies_onto(self, path) -> list[str]:
        from geoparquet_io.core.file_utils import is_same_file_path

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
    from geoparquet_io.core.file_utils import is_same_file_path

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
    original_exit = duckdb.DuckDBPyConnection.__exit__
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

    def exit_connection(self, *args):
        try:
            return original_exit(self, *args)
        finally:
            connection_paths.pop(id(self), None)

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
    monkeypatch.setattr(duckdb.DuckDBPyConnection, "__exit__", exit_connection)
    monkeypatch.setattr(os, "replace", replace)
    return probe
