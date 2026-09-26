"""DuckDB memory limits for a write: the process's memory ceiling and the default drawn from it.

Every gpio write that runs through DuckDB -- the plain-COPY fast path, the
duckdb-kv strategy, partition staging -- takes its default ``memory_limit``
from here, so there is one rule: a fixed share of the *ceiling*, the most
memory this process may ever hold (its cgroup cap, or physical RAM when that
is lower). Not a share of what is free at the moment: a cgroup's usage counts
its page cache, so after a job has read a large input "free" sits near zero
and the limit collapsed to the 128MB floor (#1153).
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterator
from contextlib import contextmanager

import duckdb
import psutil

from geoparquet_io.core.duckdb_utils import _current_setting, restore_duckdb_settings
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
    con: duckdb.DuckDBPyConnection, memory_limit: str | None, verbose: bool
) -> Iterator[None]:
    """Bound one DuckDB write by a memory limit, then restore the session's settings.

    ``memory_limit`` is the user's ``--write-memory``; without one the default
    leaves headroom under the ceiling (``default_memory_limit``). Threads are
    capped so each keeps ``_BYTES_PER_THREAD``: a small limit spread over many
    threads makes DuckDB raise instead of spill. Row order is left alone, so a
    write keeps every thread and its ordering until the limit actually binds.
    """
    saved = {key: _current_setting(con, key) for key in ("memory_limit", "threads")}
    try:
        effective = _resolve_limit(con, memory_limit)
        if effective is not None:
            con.execute(f"SET memory_limit = '{effective}'")
            if verbose:
                debug(f"DuckDB memory limit: {effective}")
        limit_bytes = parse_size(str(_current_setting(con, "memory_limit")))
        if limit_bytes:
            threads = max(1, limit_bytes // _BYTES_PER_THREAD)
            if threads < int(str(saved["threads"])):
                con.execute(f"SET threads = {threads}")
                if verbose:
                    debug(f"DuckDB threads: {threads} (for the memory limit)")
        yield
    finally:
        restore_duckdb_settings(con, saved, verbose)
