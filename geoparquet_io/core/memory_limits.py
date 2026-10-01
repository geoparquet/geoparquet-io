"""DuckDB memory limits: the process's memory ceiling and the default drawn from it.

Every gpio write that runs through DuckDB -- the plain-COPY fast path, the
duckdb-kv strategy, partition staging -- takes its default ``memory_limit``
from here, so there is one rule: a fixed share of the *ceiling*, the most
memory this process may ever hold (its cgroup cap, or physical RAM when that
is lower). Not a share of what is free at the moment: a cgroup's usage counts
its page cache, so after a job has read a large input "free" sits near zero
and the limit collapsed to the 128MB floor (#1153).

The same rule bounds work that is not a write. :func:`scoped_write_memory_limit`
wraps one statement on a connection the caller owns, which suits a COPY; the
aggregate and overview queries instead end in ``.arrow().read_all()``, with no
statement to wrap, so :func:`open_bounded_connection` applies the rule when the
connection is *opened* (#1179). Both draw the number from the same place.
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterator
from contextlib import contextmanager

import duckdb
import psutil

from geoparquet_io.core.duckdb_utils import (
    _current_setting,
    get_duckdb_connection,
    restore_duckdb_settings,
)
from geoparquet_io.core.logging_config import debug

# DuckDB's memory_limit is a SET value, which cannot be parameterised, so the
# value has to be interpolated into SQL. Only accept a plain size literal: a
# decimal number with an optional decimal (KB/MB/GB/TB) or binary (KiB/…) unit.
_MEMORY_LIMIT_RE = re.compile(r"^\d+(\.\d+)?\s*(K|M|G|T)?i?B$", re.IGNORECASE)


def validate_memory_limit(value: str) -> str:
    """Validate/normalize a DuckDB memory limit before interpolating it into SQL.

    ``memory_limit`` originates from ``--write-memory`` (or from a library
    caller's config) and ends up inside ``SET memory_limit = '…'``. DuckDB's
    ``execute`` runs multi-statement strings, so an unvalidated value can close
    the string literal and append arbitrary SQL. Reject anything that is not a
    plain size.

    Args:
        value: Candidate memory limit, e.g. "512MB", "2GB", "4.5 GB", "1GiB"

    Returns:
        The normalized value (whitespace removed, unit upper-cased).

    Raises:
        ValueError: If the value is not a plain size literal.
    """
    text = str(value).strip()
    if not _MEMORY_LIMIT_RE.match(text):
        raise ValueError(
            f"Invalid memory_limit {value!r}; expected a size like "
            f"'512MB', '2GB', '4.5GB', or '1GiB'."
        )
    return text.upper().replace(" ", "")


_SIZE_UNITS = {
    "B": 1,
    "BYTES": 1,
    "KB": 1000,
    "MB": 1000**2,
    "GB": 1000**3,
    "TB": 1000**4,
    "KIB": 1024,
    "MIB": 1024**2,
    "GIB": 1024**3,
    "TIB": 1024**4,
}
_SIZE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([A-Za-z]+)\s*$")


def parse_size(text: str) -> int | None:
    """Bytes in a size as DuckDB reads or reports it ("2GB", "14.3 GiB", "0 bytes").

    DuckDB, like ``--write-memory``, reads KB/MB/GB as powers of 1000 and
    KiB/MiB/GiB as powers of 1024. ``None`` for anything else.
    """
    match = _SIZE_RE.match(str(text))
    if not match or match.group(2).upper() not in _SIZE_UNITS:
        return None
    return int(float(match.group(1)) * _SIZE_UNITS[match.group(2).upper()])


#: Where the process's own cgroup is named, and where the hierarchy is mounted.
#: Module attributes so tests can point them at a fake tree.
_PROC_SELF_CGROUP = "/proc/self/cgroup"
_CGROUP_ROOT = "/sys/fs/cgroup"

# A cgroup v1 limit at or above this is the kernel's "no limit" sentinel.
_V1_UNLIMITED = 2**60


def _read_cgroup_int(path: str) -> int | None:
    """An integer cgroup file, or None when it is absent or not a number ("max" in v2)."""
    try:
        with open(path) as f:
            text = f.read().strip()
    except OSError:
        return None
    return int(text) if text.isdigit() else None


def _cgroup_limit_files(proc_self_cgroup: str, cgroup_root: str) -> list[str]:
    """Every cgroup limit file that can cap this process.

    A batch scheduler does not give the job a cgroup namespace: Slurm puts it at
    ``/slurm/uid_N/job_N/step_batch`` (v1) or ``.../job_N/step_batch/...`` (v2)
    and caps the *job* directory, so the root of the hierarchy -- the only place
    gpio used to look -- says "no limit" (#1153). Walk from the process's own
    cgroup up to the root; a container that does have a namespace shows ``/``
    and reduces to the root check that was here before.
    """
    v2_path, v1_path = "/", "/"
    try:
        with open(proc_self_cgroup) as f:
            for line in f:
                parts = line.strip().split(":", 2)
                if len(parts) != 3:
                    continue
                if parts[0] == "0" and parts[1] == "":
                    v2_path = parts[2]
                elif "memory" in parts[1].split(","):
                    v1_path = parts[2]
    except OSError:
        pass

    def ancestors(path: str) -> list[str]:
        segments = [s for s in path.split("/") if s]
        return ["/".join(segments[:i]) for i in range(len(segments), -1, -1)]

    files = [os.path.join(cgroup_root, rel, "memory.max") for rel in ancestors(v2_path)]
    files += [
        os.path.join(cgroup_root, "memory", rel, "memory.limit_in_bytes")
        for rel in ancestors(v1_path)
    ]
    return files


def _cgroup_limit(proc_self_cgroup: str, cgroup_root: str) -> int | None:
    """The tightest cgroup memory cap on this process, or None when nothing caps it."""
    limits = [
        limit
        for path in _cgroup_limit_files(proc_self_cgroup, cgroup_root)
        if (limit := _read_cgroup_int(path)) is not None and limit < _V1_UNLIMITED
    ]
    return min(limits, default=None)


def memory_ceiling() -> int | None:
    """Bytes this process may use in total: its cgroup cap or physical RAM, the lower."""
    candidates = [_cgroup_limit(_PROC_SELF_CGROUP, _CGROUP_ROOT), psutil.virtual_memory().total]
    return min((v for v in candidates if v), default=None)


def _format_memory_limit(limit_bytes: int) -> str:
    # Divides by 1024**3 but writes "GB", which DuckDB reads as 10**9: the
    # ~7% undershoot is extra headroom, kept rather than churn every limit.
    limit_gb = limit_bytes / (1024**3)
    if limit_gb >= 1:
        return f"{limit_gb:.1f}GB"
    limit_mb = limit_bytes / (1024**2)
    return f"{max(128, int(limit_mb))}MB"  # Minimum 128MB


#: Share of the memory ceiling a write gives DuckDB by default.
#:
#: DuckDB's own default is 80%, but its limit covers only its buffer manager:
#: the Parquet writer, compression and spatial functions allocate beside it.
#: Measured on Hilbert-sorted 2.0 converts (duckdb 1.5.5, 12 threads), peak
#: RSS ran 30-70% past the limit -- so 80% reaches a cgroup cap before DuckDB
#: spills or raises, and the kernel kills the process instead (#1153).
DEFAULT_MEMORY_FRACTION = 0.5


def default_memory_limit() -> str | None:
    """DuckDB memory limit for a write that names none; None if the ceiling is unknown."""
    ceiling = memory_ceiling()
    if ceiling is None:
        return None
    return _format_memory_limit(int(ceiling * DEFAULT_MEMORY_FRACTION))


def get_default_memory_limit() -> str:
    """``default_memory_limit``, or a conservative 2GB when the ceiling is unknown."""
    return default_memory_limit() or "2GB"


#: Memory each DuckDB thread needs for a large sort to spill rather than fail.
#: Measured on a 4M-polygon Hilbert COPY: ~250MB per thread spilled reliably,
#: under ~170MB raised OutOfMemoryException with a temp_directory set.
_BYTES_PER_THREAD = 512 * 1024**2


def _connection_limit(memory_limit: str | None) -> tuple[str | None, int | None]:
    """The ``memory_limit`` and thread cap a freshly opened connection should carry.

    ``_resolve_limit``'s rule, minus the part that reads the session: a new
    connection has no caller-chosen limit to leave alone, only DuckDB's own
    default of ~80% of host RAM, which is the very thing being replaced. An
    explicit value is used as given; otherwise the ceiling-based default
    applies, and ``(None, None)`` leaves DuckDB alone when no ceiling is known.

    Threads are capped so each keeps ``_BYTES_PER_THREAD`` -- a small limit
    spread over many threads makes DuckDB raise instead of spill -- and only
    when that is fewer threads than DuckDB would otherwise take.
    """
    limit = validate_memory_limit(memory_limit) if memory_limit else default_memory_limit()
    if limit is None:
        return None, None
    limit_bytes = parse_size(limit)
    if not limit_bytes:  # pragma: no cover - validate_memory_limit accepts only sizes
        return limit, None
    threads = max(1, limit_bytes // _BYTES_PER_THREAD)
    default_threads = os.cpu_count() or threads
    return limit, (threads if threads < default_threads else None)


def open_bounded_connection(
    *,
    memory_limit: str | None = None,
    load_spatial: bool = True,
    load_httpfs: bool | None = None,
    verbose: bool = False,
    preserve_insertion_order: bool = False,
) -> duckdb.DuckDBPyConnection:
    """A DuckDB connection for one analysis query, opened inside a memory limit.

    For work the funnel cannot wrap: `gpio process aggregate` and `gpio process
    overview` materialize their result client-side with ``.arrow().read_all()``,
    so there is no COPY for :func:`scoped_write_memory_limit` to scope and the
    cap has to be part of the connection (#1179). Left uncapped, DuckDB sized
    itself for the host and a 42 GB aggregate peaked at 115 GiB RSS.

    ``preserve_insertion_order`` defaults off: every caller today takes its
    output order from a GROUP BY, so buffering the scan to keep the input's
    order buys nothing and costs the whole scan's width in memory. It is a
    parameter rather than a constant because the setting is about ordering, not
    memory, and a future caller that does need the input's order would
    otherwise be silently reordered by a function whose name promises only a
    memory bound.
    """
    limit, threads = _connection_limit(memory_limit)
    con: duckdb.DuckDBPyConnection = get_duckdb_connection(
        load_spatial=load_spatial,
        load_httpfs=load_httpfs,
        threads=threads,
        memory_limit=limit,
    )
    con.execute(f"SET preserve_insertion_order = {str(preserve_insertion_order).lower()}")
    if verbose and limit:
        debug(f"DuckDB memory limit: {limit}")
    if verbose and threads:
        debug(f"DuckDB threads: {threads} (for the memory limit)")
    return con


def _resolve_limit(con: duckdb.DuckDBPyConnection, memory_limit: str | None) -> str | None:
    """The limit to SET for one write, or None to leave the session's in place.

    An explicit ``memory_limit`` always wins. Otherwise the default applies,
    unless the caller already set a stricter limit on their own connection.
    """
    if memory_limit:
        return validate_memory_limit(memory_limit)
    default = default_memory_limit()
    if default is None:
        return None
    session = parse_size(str(_current_setting(con, "memory_limit")))
    default_bytes = parse_size(default)
    if session is not None and default_bytes is not None and session < default_bytes:
        return None
    return validate_memory_limit(default)


@contextmanager
def scoped_write_memory_limit(
    con: duckdb.DuckDBPyConnection,
    memory_limit: str | None,
    verbose: bool,
    pinned: dict[str, int | bool] | None = None,
) -> Iterator[None]:
    """Bound one DuckDB write by a memory limit, then restore the session's settings.

    ``memory_limit`` is the user's ``--write-memory``; without one the default
    leaves headroom under the ceiling (``default_memory_limit``). Threads are
    capped so each keeps ``_BYTES_PER_THREAD``: a small limit spread over many
    threads makes DuckDB raise instead of spill. Row order is left alone, so a
    write keeps every thread and its ordering until the limit actually binds.

    ``pinned`` holds settings a particular write needs for its duration (the
    single-pass partition COPY pins ``threads=1`` for one file per partition);
    they are set after the limit, replace the thread cap when they pin
    ``threads``, and are restored with everything else.
    """
    pinned = dict(pinned or {})
    keys = dict.fromkeys(("memory_limit", "threads", *pinned))
    saved = {key: _current_setting(con, key) for key in keys}
    try:
        effective = _resolve_limit(con, memory_limit)
        if effective is not None:
            con.execute(f"SET memory_limit = '{effective}'")
            if verbose:
                debug(f"DuckDB memory limit: {effective}")
        limit_bytes = parse_size(str(_current_setting(con, "memory_limit")))
        if limit_bytes and "threads" not in pinned:
            threads = max(1, limit_bytes // _BYTES_PER_THREAD)
            if threads < int(str(saved["threads"])):
                con.execute(f"SET threads = {threads}")
                if verbose:
                    debug(f"DuckDB threads: {threads} (for the memory limit)")
        for key, value in pinned.items():
            rendered = str(value).lower() if isinstance(value, bool) else int(value)
            con.execute(f"SET {key} = {rendered}")
        yield
    finally:
        restore_duckdb_settings(con, saved, verbose)
