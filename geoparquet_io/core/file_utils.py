"""
File path utilities for GeoParquet files.
"""

import contextlib
import fnmatch
import functools
import glob as glob_module
import os
import re
import shutil
import stat
import tempfile
import time
import uuid
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path, PurePath
from typing import BinaryIO
from urllib.parse import urlsplit

from geoparquet_io.core.exceptions import (
    FileNotFoundGeoParquetError,
    GeoParquetError,
    InvalidParameterError,
)
from geoparquet_io.core.logging_config import debug
from geoparquet_io.core.remote import (
    canonical_store_url,
    is_account_first_azure_url,
    is_azure_url,
    is_http_url,
    is_remote_url,
    needs_httpfs,
    protocol_label,
)

_STAGE_PREFIX = ".gpio-stage-"


def _has_glob_characters(text: str) -> bool:
    """Whether *text* holds a character a glob reads as magic: ``*``, ``?``, ``[``."""
    return any(c in text for c in ("*", "?", "["))


def has_glob_pattern(path: str) -> bool:
    """Whether *path* is a glob pattern rather than the name of one file.

    A local path that exists as a regular file names that file, whatever
    characters it holds: ``[`` is legal in file names and common in names
    decoded from Shift_JIS ZIPs, and such a file is not a dataset (#1243).
    Remote URLs are classified by their characters alone; gpio does not stat
    an object store to ask.
    """
    if not _has_glob_characters(path):
        return False
    return is_remote_url(path) or not os.path.isfile(path)


def http_url_path(path: str) -> str:
    """The part of *path* that names the object: an http(s) URL's path, any other *path* whole.

    An http URL's authority (an IPv6 ``[::1]``), query and fragment are neither a
    glob nor a hive key. For an object store DuckDB reads ``?`` as a glob wildcard,
    so its URL is scanned as written, as is a malformed http URL (``http://[bad/x``).
    """
    if not is_http_url(path):
        return path
    try:
        return urlsplit(path).path
    except ValueError:
        return path


def is_partition_path(path: str) -> bool:
    if not is_remote_url(path):
        return has_glob_pattern(path) or os.path.isdir(path)
    # A scheme or bucket segment never holds '=', so every segment can be scanned.
    name = http_url_path(path)
    return _has_glob_characters(name) or any(
        "=" in part and not part.endswith(".parquet") for part in name.split("/")
    )


def resolve_partition_path(path: str, hive_partitioning: bool | None = None) -> tuple[str, dict]:
    options = {}
    resolved = path

    if not is_remote_url(path) and os.path.isdir(path):
        try:
            items = os.listdir(path)
            subdirs = [d for d in items if os.path.isdir(os.path.join(path, d))]
            has_parquet_files = any(
                f.endswith(".parquet") for f in items if not os.path.isdir(os.path.join(path, f))
            )
            has_hive_subdirs = any("=" in d for d in subdirs)

            if has_hive_subdirs:
                resolved = os.path.join(path, "**", "*.parquet")
                options["hive_partitioning"] = True
            elif subdirs and not has_parquet_files:
                resolved = os.path.join(path, "**", "*.parquet")
            elif has_parquet_files:
                resolved = os.path.join(path, "*.parquet")
            else:
                resolved = os.path.join(path, "**", "*.parquet")
        except OSError:
            resolved = os.path.join(path, "**", "*.parquet")

    if hive_partitioning is None:
        path_parts = resolved.replace("\\", "/").split("/")
        dir_parts = [p for p in path_parts[:-1] if p and p not in ("**", "*")]
        has_hive_dirs = any("=" in part for part in dir_parts)
        if has_hive_dirs:
            options["hive_partitioning"] = True
    elif hive_partitioning is not None:
        options["hive_partitioning"] = hive_partitioning

    return resolved, options


def _remote_glob_matches(pattern: str) -> list[str]:
    """Every object a remote glob matches, as DuckDB's ``glob()`` lists it.

    The one remote listing (#1367 T2e): the first-file read, the all-files read,
    a partition's info and the GeoJSON footer sweep all come here. Sorted, and
    dot-named objects skipped as a local glob skips them. Without it a "first
    file" read of ``s3://b/part-*.parquet`` handed DuckDB the whole pattern,
    which read every matched footer and merged row groups across files (#1332).
    """
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection_for_s3

    con = get_duckdb_connection_for_s3(pattern, load_spatial=False)
    try:
        rows = con.execute("SELECT file FROM glob(?) ORDER BY file", [pattern]).fetchall()
    finally:
        con.close()
    return [row[0] for row in rows if not row[0].rsplit("/", 1)[-1].startswith(".")]


def glob_matches(pattern: str) -> list[str]:
    """Every path a glob *pattern* matches, sorted, whatever its suffix.

    Local patterns go to :mod:`glob` (``**`` recursive); an object-store
    pattern is listed once by :func:`_remote_glob_matches`. An HTTP(S) URL
    cannot be listed and comes back as given.
    """
    if is_remote_url(pattern):
        return _remote_glob_matches(pattern) if needs_httpfs(pattern) else [pattern]
    return sorted(glob_module.glob(pattern, recursive=True))


def _parquet_glob_matches(pattern: str) -> list[str]:
    return [m for m in glob_matches(pattern) if m.endswith(".parquet")]


def get_first_parquet_file(partition_path: str) -> str | None:
    """The one file a metadata read of *partition_path* should open.

    A directory or a glob resolves to its first ``.parquet`` file, local or
    in an object store (``s3``, ``gcs``, ``azure``). An HTTP(S) URL cannot be
    listed and is returned as given, as is any other single path.
    """
    if is_remote_url(partition_path):
        if has_glob_pattern(partition_path) and needs_httpfs(partition_path):
            return next(iter(_parquet_glob_matches(partition_path)), None)
        return partition_path

    if os.path.isdir(partition_path):
        return next(iter(get_all_parquet_files(partition_path)), None)

    if has_glob_pattern(partition_path):
        return next(iter(_parquet_glob_matches(partition_path)), None)

    return partition_path


def get_all_parquet_files(partition_path: str, *, recursive: bool = True) -> list[str]:
    """Every ``.parquet`` file under a directory or matched by a glob, sorted.

    The one listing rule (#1367 HANDOFFS row 74): dotfiles and dot-directories
    are not data. An object-store glob is listed (:func:`glob_matches`); any
    other remote URL cannot be listed and is returned as given.
    ``recursive=False`` lists a directory's own files only.
    """
    if is_remote_url(partition_path):
        if has_glob_pattern(partition_path) and needs_httpfs(partition_path):
            return _parquet_glob_matches(partition_path)
        return [partition_path]

    if os.path.isdir(partition_path):
        # Dotfiles and dot-directories are skipped, as the glob branch below
        # already does: macOS AppleDouble `._x.parquet` sidecars and an
        # orphaned `.gpio-stage-*` staging directory are not data.
        parquet_files = []
        for root, dirs, files in os.walk(partition_path):
            dirs[:] = [d for d in dirs if not d.startswith(".")] if recursive else []
            for f in files:
                if f.endswith(".parquet") and not f.startswith("."):
                    parquet_files.append(os.path.join(root, f))
        return sorted(parquet_files)

    if has_glob_pattern(partition_path):
        return _parquet_glob_matches(partition_path)

    return [partition_path] if os.path.exists(partition_path) else []


def validate_output_path(output_path, verbose=False):
    if is_remote_url(output_path):
        return

    output_dir = os.path.dirname(output_path) or "."
    if not os.path.exists(output_dir):
        raise FileNotFoundGeoParquetError(output_dir, "output directory")
    if not os.access(output_dir, os.W_OK):
        raise GeoParquetError(f"No write permission for: {output_dir}")


def validate_parquet_extension(output_file: str, any_extension: bool = False) -> None:
    if output_file is None or output_file == "-":
        return

    if any_extension:
        return

    if is_remote_url(output_file):
        path_part = output_file.split("://", 1)[1]
        filename = path_part.split("/")[-1] if "/" in path_part else path_part
    else:
        filename = os.path.basename(output_file)

    _, ext = os.path.splitext(filename)
    if ext.lower() != ".parquet":
        raise InvalidParameterError(
            "output_file",
            f"'{output_file}' does not have .parquet extension. "
            f"Use --any-extension to allow non-standard extensions.",
        )


def _match_path_segments(pattern: tuple[str, ...], path: tuple[str, ...]) -> bool:
    """True when ``path``'s segments satisfy glob ``pattern``'s segments.

    Segment-aware on purpose: :func:`fnmatch.fnmatch` on whole paths lets a
    single ``*`` swallow separators, which would call ``parts/sub/out.parquet``
    a match for ``parts/*.parquet`` -- a pattern DuckDB does not read
    recursively. Only ``**`` spans segments, and it spans zero or more.
    """
    if not pattern:
        return not path
    head, rest = pattern[0], pattern[1:]
    if head == "**":
        return any(_match_path_segments(rest, path[i:]) for i in range(len(path) + 1))
    if not path or not fnmatch.fnmatchcase(path[0], head):
        return False
    return _match_path_segments(rest, path[1:])


def _absolute_glob_segments(pattern: str) -> tuple[str, ...]:
    """Absolute segments of a glob, its magic-free prefix symlink-resolved.

    Only the leading directories can be resolved: ``Path.resolve()`` on a
    pattern would treat ``*`` as a literal directory name, and the answer has
    to be comparable with a resolved output path (``/var`` vs ``/private/var``
    on macOS is exactly this trap).
    """
    parts = PurePath(pattern).parts
    first_magic = next((i for i, p in enumerate(parts) if _has_glob_characters(p)), len(parts))
    return (*Path(*parts[:first_magic]).resolve().parts, *parts[first_magic:])


def _output_is_read_as_input(input_path: str, output_path: str) -> bool:
    """Would a multi-file read of ``input_path`` pick ``output_path`` up?"""
    output = Path(output_path).resolve()
    if os.path.isdir(input_path):
        return Path(input_path).resolve() in output.parents
    return _match_path_segments(_absolute_glob_segments(input_path), output.parts)


def guard_output_not_read_as_input(input_path: str | None, output_path: str | None) -> None:
    """Refuse an output the *next* run would read back as part of its input.

    A directory or glob input is re-expanded on every run, so an output written
    inside it joins the dataset. The second run then reads its own first output
    back and every row it holds is counted twice -- exit 0, no warning, and the
    counts merely grow (#867). ``--overwrite`` is no defence either: the stale
    output is read before it is replaced.

    Nothing downstream can notice, because the read genuinely succeeds. So the
    write is refused up front, before the first run creates the file that
    poisons the dataset -- which is why this runs whether or not the output
    already exists.

    Single-file inputs are not this guard's business: they name exactly one
    file, and :func:`handle_output_overwrite`'s equality check already covers
    writing over it. Remote paths are skipped -- gpio does not enumerate them
    here, so there is nothing local to compare.

    Args:
        input_path: The RAW input path the user gave (file, directory or glob)
        output_path: The output path the command is about to write

    Raises:
        GeoParquetError: When the output lies inside the input directory, or
            matches the input glob.
    """

    if not input_path or not output_path or "-" in (input_path, output_path):
        return
    if is_remote_url(input_path) or is_remote_url(output_path):
        return
    if not is_partition_path(input_path) or not _output_is_read_as_input(input_path, output_path):
        return

    where = "inside the input directory" if os.path.isdir(input_path) else "matched by the input"
    raise GeoParquetError(
        f"Output '{output_path}' is {where} '{input_path}'.\n\n"
        "Writing it there would add it to the dataset, so the next run would "
        "read it back as input and count every row twice.\n\n"
        "Write the output somewhere else, outside the input dataset."
    )


def is_same_file_path(first: str | None, second: str | None) -> bool:
    """Whether two paths name one file.

    ``./a.parquet``, ``a.parquet``, an absolute spelling of either and a symlink
    alias are all the same file, and every caller that decides "am I about to
    write back over my own input?" has to agree on that. Comparing the raw
    strings instead is what let an aliased ``--fix-output`` slip past a caller's
    in-place branch and then get refused by this module's ``resolve()``-based
    guard -- reproducing the very failure the in-place path exists to avoid
    (#941, #959).

    Remote URLs are compared under their store spelling (``canonical_store_url``,
    the one ``copy_file`` writes through), so ``s3a://b/k`` is ``s3://b/k``;
    there is no local filesystem to resolve them against.
    """
    if not first or not second:
        return False
    if first == second:
        return True

    if is_remote_url(first) or is_remote_url(second):
        return canonical_store_url(first) == canonical_store_url(second)
    try:
        if os.path.exists(first) and os.path.exists(second):
            # The inode is the truth once both exist: it also catches a
            # case-only alias on macOS/Windows and a hard link, which
            # ``resolve()`` reads as two files.
            return os.path.samefile(first, second)
        return Path(first).resolve() == Path(second).resolve()
    except (OSError, ValueError):
        # ValueError is a Windows path shape -- a malformed drive spec or an
        # embedded NUL. "I cannot tell" answers no: the callers use this to
        # decide whether they are about to overwrite their own input, and the
        # safe direction is to take the not-in-place branch.
        return False


def output_is_kept(output_path: str, overwrite: bool) -> bool:
    """Whether a re-run leaves an existing output in place instead of writing it.

    The resume half of the overwrite policy, for the per-partition files a
    partition run writes: an existing one is skipped, not refused, unless
    *overwrite* is set.
    """
    return not overwrite and os.path.exists(output_path)


def output_exists_error(output_path: str, flag: str = "--overwrite") -> GeoParquetError:
    """The one refusal for an existing output, naming the switch that replaces it."""
    return GeoParquetError(f"Output file already exists: {output_path}\nUse {flag} to replace it.")


def remote_in_place_error(flag: str = "--overwrite") -> GeoParquetError:
    """The refusal to rewrite a remote input in place without the switch."""
    return GeoParquetError(
        f"Cannot modify remote file without {flag} flag. "
        f"Use {flag} to confirm overwriting the remote file, "
        "or write to a different output path."
    )


def handle_output_overwrite(
    output_path: str | None,
    overwrite: bool,
    input_path: str | None = None,
    *,
    flag: str = "--overwrite",
) -> None:
    """The one overwrite policy: what a command does about an output that exists.

    Every command calls this before it writes (#1367). An output inside, or
    matched by, a directory or glob input is refused whether or not it exists
    yet (:func:`guard_output_not_read_as_input`); an existing output that is
    the input itself is refused; any other existing output is refused unless
    *overwrite* is set. A remote output cannot be looked up; the one existing
    remote output gpio knows of is the input itself, and replacing it in place
    is refused unless *overwrite* is set (:func:`remote_in_place_error`).

    Args:
        output_path: The output the command is about to write
        overwrite: The caller's overwrite switch
        input_path: The RAW input, when the command has one
        flag: How the caller spells the switch, for the refusal's hint
            (``--force`` for the tile and overview commands, ``overwrite=True``
            for the Python API)

    Existing outputs remain in place until the writer replaces them on success.

    Raises:
        GeoParquetError: When the output may not be written.
    """
    if not output_path:
        return

    # Before the existence check: on the FIRST run the offending file does not
    # exist yet, and creating it is what breaks every later run (#867).
    guard_output_not_read_as_input(input_path, output_path)

    if is_remote_url(output_path):
        if is_same_file_path(output_path, input_path) and not overwrite:
            raise remote_in_place_error(flag)
        return
    output_file = Path(output_path)

    if not output_file.exists():
        return

    if input_path and is_same_file_path(output_path, input_path):
        raise GeoParquetError(f"Cannot overwrite input file: {output_path}")

    if not overwrite:
        raise output_exists_error(output_path, flag)


#: Seconds to wait before each further attempt at an unlink Windows refused.
_UNLINK_WAITS = (0.1, 0.2, 0.3)


def unlink_released_file(path: str | os.PathLike | None) -> None:
    """Delete a scratch file, waiting for Windows to release its handle.

    The one scratch cleanup (#1367 concept 11): a missing file or ``None`` is
    nothing to do. Windows refuses to unlink a file another handle still holds,
    and a reader gpio just closed can keep it briefly. Three tries, then give
    up quietly: this is cleanup of a temporary file, never a write path.
    """
    import time

    if path is None:
        return
    for wait in _UNLINK_WAITS:
        try:
            Path(path).unlink(missing_ok=True)
            return
        except OSError:
            time.sleep(wait)


def _publish_sidecars(stage_dir: Path, staged: Path, final: Path, suffixes: Iterable[str]) -> None:
    """Move every file the writer left beside *staged*, and drop the old set's leftovers."""
    published = {part.name for part in stage_dir.iterdir()}
    for sidecar in sorted(stage_dir.iterdir()):
        if sidecar != staged and sidecar.name != _SPILL_LOCK_NAME:
            os.replace(sidecar, final.parent / sidecar.name)
    for stale in suffixes:
        old = final.parent / f"{final.stem}{stale}"
        if old.name not in published and old != final:
            old.unlink(missing_ok=True)


_SPILL_LOCK_NAME = ".gpio-owner.lock"


def _spill_lock(path: str) -> BinaryIO | None:
    """Take a nonblocking ownership lock; None means Windows (no fcntl)."""
    try:
        import fcntl
    except ImportError:
        return None
    marker = open(os.path.join(path, _SPILL_LOCK_NAME), "a+b")
    try:
        fcntl.flock(marker, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BaseException:
        marker.close()
        raise
    return marker


@contextlib.contextmanager
def _spill_base_lock(base: str | os.PathLike):
    """Serialize publishing a marker and sweeping on a shared spill volume."""
    try:
        import fcntl
    except ImportError:
        yield
        return
    # Lock the base directory itself: a sidecar lock would outlive every spill
    # and violate callers' scratch cleanup guarantees. This also works when
    # the base is read-only (scanning it still must be best effort).
    descriptor = os.open(base, os.O_RDONLY)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        os.close(descriptor)


@contextlib.contextmanager
def _staging_directory_owner(path: Path, *, wait: bool = True):
    """Wait for ownership, coordinating directory creation with collectors.

    Never wait while holding the base lock: the current owner may need it to
    publish a stage. Reopen after contention because that owner can delete the
    directory (and its marker) before releasing ownership. No-op on Windows.
    """
    while True:
        try:
            with _spill_base_lock(path.parent):
                path.mkdir(exist_ok=True)
                marker = _spill_lock(str(path))
            break
        except BlockingIOError:
            if not wait:
                raise
            time.sleep(0.05)
    try:
        yield
    finally:
        if marker is not None:
            marker.close()


def _process_is_alive(pid: int) -> bool:
    """Whether ``pid`` is still running. Answers True whenever it cannot tell.

    Only ever used to decide *not* to delete something, so the unsure answer has
    to be "alive": a false "dead" would take a running sibling's scratch space
    away mid-query, while a false "alive" merely leaves one more directory for
    the next run (or the OS tmp reaper) to collect.
    """
    try:
        import psutil

        return psutil.pid_exists(pid)
    except Exception:  # psutil is optional on vendored hosts (#1330): unsure means alive
        return True


def sweep_orphaned_dirs(
    base_dir: str | os.PathLike,
    candidates: Callable[[], Iterable[tuple[str, int | None]]],
    minimum_age: float,
    *,
    dry_run: bool = False,
) -> list[str]:
    """Shared collector: enumerate under the base lock, hold ownership through deletion.

    A None PID means no process identity is available; on Windows these
    directories have no concurrency protection.

    Probing a marker before taking the base lock can steal it from a creator
    between open() and flock(), leaving that live creator without protection.
    """
    removed = []
    try:
        with _spill_base_lock(base_dir):
            for path, pid in candidates():
                marker = None
                try:
                    directory = Path(path)
                    if directory.is_symlink() or not directory.is_dir():
                        continue
                    if minimum_age and time.time() - directory.stat().st_mtime < minimum_age:
                        continue
                    if dry_run and not (directory / _SPILL_LOCK_NAME).is_file():
                        if pid is None or not _process_is_alive(pid):
                            removed.append(path)
                        continue
                    marker = _spill_lock(path)
                    if marker is None and pid is not None and _process_is_alive(pid):
                        continue
                    if not dry_run:
                        shutil.rmtree(path)
                    removed.append(path)
                except OSError:
                    continue
                finally:
                    if marker is not None:
                        marker.close()
    except OSError:
        pass  # Missing/read-only base: cleanup must not fail the caller.
    return removed


def sweep_orphaned_staging_dirs(base_dir: Path, *, dry_run: bool = False) -> list[str]:
    """Collect stages older than a day, only while holding their ownership lock.

    Reuse spill ownership across PID namespaces; age alone cannot protect a slow
    live writer. Legacy stages without an owner identity are left untouched.
    """

    def candidates() -> Iterator[tuple[str, int]]:
        return (
            (str(stage), int(match[1]))
            for stage in base_dir.glob(f"{_STAGE_PREFIX}*")
            if (
                match := re.fullmatch(
                    rf"{re.escape(_STAGE_PREFIX)}(\d+)-[a-z0-9_]{{8}}", stage.name
                )
            )
        )

    return sweep_orphaned_dirs(base_dir, candidates, 24 * 60 * 60, dry_run=dry_run)


@contextlib.contextmanager
def atomic_write(
    destination: str,
    suffix: str = ".parquet",
    *,
    sidecar_suffixes: Iterable[str] | None = None,
    mode_from: str | None = None,
    reclaim_staging: bool = False,
) -> Iterator[str]:
    """The path a write goes to, moved over *destination* once the block succeeds.

    The one staging rule for every gpio output written where a reader may be
    looking (#1367 concept 11). The writer gets a path in a fresh
    ``.gpio-stage-*`` directory beside the destination -- the same filesystem,
    so publishing is one ``os.replace`` -- named after the destination's stem
    plus *suffix*, so a writer that derives a layer name from the file sees the
    output's. On success the staged file is moved over the destination; on any
    failure, a ``KeyboardInterrupt`` included, the staging directory is removed
    and the destination is exactly as it was (#1076). The original is never
    unlinked or truncated first, and the move happens only after the block has
    closed its writer and every reader it opened: Windows refuses to rename
    over a path a handle holds open (#1032, #959). A block that writes nothing
    publishes nothing.

    A symlinked destination is written through: the link's target is the file
    replaced, and the link stays a link (#1076). The published file takes the
    destination's mode when it replaces one, else *mode_from*'s when given (a
    footer patch's copy takes its input's), else the umask's -- never a temp
    file's 0600. Replacing an existing file strictly advances its whole-second
    modification time, so timestamp-validating readers see the new version.

    *sidecar_suffixes* publishes a set of files that share the output's stem (a
    Shapefile's ``.shp``/``.shx``/``.dbf``/``.prj``/...): every file the writer
    leaves in the staging directory lands beside the output, and an old
    ``<stem><suffix>`` for a listed suffix the new set lacks is removed, so a
    stale ``.cpg`` cannot outlive an overwrite. A multi-file publish is atomic
    per file, not as a set.

    ``reclaim_staging`` opts cache writers into age-guarded orphan collection.
    If the parent cannot be locked, the stage uses the legacy, uncollected name.

    A remote destination is staged locally and uploaded after the block closes
    its readers. The staging caller scopes credentials with ``uses_aws_profile``.
    """
    if is_remote_url(str(destination)):
        from geoparquet_io.core.remote import upload_if_remote

        if sidecar_suffixes is not None:
            raise GeoParquetError("Remote writes do not support sidecar files.")
        with scratch_file(suffix=suffix) as local:
            yield local
            if Path(local).exists():
                upload_if_remote(local, str(destination))
        return
    final = Path(os.path.realpath(destination))
    # Only cache stages opt into reclamation; ordinary writes retain their old cost.
    marker = None
    with contextlib.ExitStack() as locks:
        owned = False
        if reclaim_staging:
            try:
                locks.enter_context(_spill_base_lock(final.parent))
                owned = True
            except OSError:
                pass  # A writable/searchable directory need not be readable.
        prefix = f"{_STAGE_PREFIX}{os.getpid()}-" if owned else _STAGE_PREFIX
        stage_dir = Path(tempfile.mkdtemp(prefix=prefix, dir=final.parent))
        try:
            if owned:
                marker = _spill_lock(str(stage_dir))
        except BaseException:
            shutil.rmtree(stage_dir, ignore_errors=True)
            raise

    staged = stage_dir / f"{final.stem}{suffix}"
    try:
        yield str(staged)
        if not staged.exists():
            return  # the writer decided there was nothing to write
        try:
            previous = final.stat()
        except FileNotFoundError:
            previous = None
        if previous is not None:
            os.chmod(staged, stat.S_IMODE(previous.st_mode))
        elif mode_from is not None and not is_remote_url(str(mode_from)):
            shutil.copymode(mode_from, staged)
        if sidecar_suffixes is not None:
            _publish_sidecars(stage_dir, staged, final, sidecar_suffixes)
        os.replace(staged, final)
        if previous is not None:
            published = final.stat()
            previous_second = previous.st_mtime_ns // 1_000_000_000
            if published.st_mtime_ns // 1_000_000_000 <= previous_second:
                # Windows has no file version tag; DuckDB trusts whole-second mtime.
                os.utime(
                    final,
                    ns=(published.st_atime_ns, (previous_second + 1) * 1_000_000_000),
                )
    finally:
        shutil.rmtree(stage_dir, ignore_errors=True)
        if marker is not None:
            marker.close()


def scratch_path(suffix: str = ".parquet", *, prefix: str = "gpio_") -> str:
    """A fresh path in the temp directory for a scratch file that outlives a block.

    For a copy whose lifetime crosses a function boundary -- returned to the
    caller (stdin's spool), held by a strategy until its upload -- where
    :func:`scratch_file` cannot own it. The file is not created; whoever holds
    the path removes it with :func:`unlink_released_file`.
    """
    return os.path.join(tempfile.gettempdir(), f"{prefix}{uuid.uuid4().hex}{suffix}")


@contextlib.contextmanager
def scratch_file(
    suffix: str = ".parquet", *, prefix: str = "gpio_", directory: str | None = None
) -> Iterator[str]:
    """A path for a scratch file that is read back and never published.

    The one temp-naming rule for an intermediate copy (a pass between two
    writes, a spooled input, an in-memory table a file-based call reads). The
    path is in a fresh private directory (0700) under the temp directory, so
    the copy of the user's data is no one else's to read, and the file is not
    created -- the writer makes it. ``directory`` stages beside a final output
    when the intermediate must stay on its volume. The directory and everything in it are
    removed when the block exits, whatever the outcome.
    """
    with scratch_dir(prefix=prefix, directory=directory) as scratch:
        yield str(Path(scratch) / f"scratch{suffix}")


@contextlib.contextmanager
def scratch_dir(*, prefix: str = "gpio_", directory: str | None = None) -> Iterator[str]:
    """A private (0700) scratch directory, removed with its contents on exit."""
    scratch = Path(
        tempfile.mkdtemp(prefix=_STAGE_PREFIX if directory is not None else prefix, dir=directory)
    )
    try:
        yield str(scratch)
    finally:
        for leftover in scratch.rglob("*"):
            if leftover.is_file() or leftover.is_symlink():
                unlink_released_file(leftover)
        shutil.rmtree(scratch, ignore_errors=True)


# copy_file resolves the s3, gcs and azure families to a configured object store
# under remote.STORE_SCHEME_FOR_FAMILY, the spelling upload() reads too.
# Anything else is refused by name, rather than dying inside a filesystem
# library on a dependency gpio does not ship (#810). The abfs[s]:// and azure://
# spellings are refused rather than mis-parsed as account-first (#864).

# _setup_store_and_kwargs() folds this into the upload kwargs it returns alongside
# the store. A streamed copy does not use those kwargs, so this only satisfies the
# signature; obstore's own default is the same number.
_COPY_CHUNK_CONCURRENCY = 12


def _check_copyable_scheme(param_name: str, url: str, writing: bool) -> None:
    """Reject a scheme copy_file cannot serve, before any store or I/O is built."""
    if is_http_url(url):
        if writing:
            raise InvalidParameterError(
                param_name,
                f"'{url}' is an HTTP(S) URL, which is read-only. Write to a local "
                "path or to an s3://, gs:// or az:// URL instead.",
            )
        return

    if is_azure_url(url) and not is_account_first_azure_url(url):
        from geoparquet_io.core.upload import AZURE_URL_FORM

        raise InvalidParameterError(
            param_name,
            f"cannot copy '{url}': gpio addresses Azure Blob Storage as "
            f"{AZURE_URL_FORM}. Rewrite the URL in that form.",
        )

    if not is_remote_url(url):
        scheme = url.partition("://")[0].lower()
        raise InvalidParameterError(
            param_name,
            f"cannot copy '{scheme}://' URLs. gpio copies s3://, gs:// and az:// "
            "URLs, and reads http:// and https:// ones.",
        )


def _copy_http_source(url: str, dest_path: str, dest_is_remote: bool) -> None:
    """Stream an http(s) copy source with a plain GET of the URL exactly as given.

    The URL is requested **verbatim** -- query string included, nothing
    re-encoded -- matching the contract of :func:`resolve_file_url` for reads:
    a URL already is its percent-encoded form (#825), and a presigned URL is
    only valid with its signature attached. An object store gains nothing here;
    gpio's store configuration is S3-endpoint plumbing, so HTTP(S) bypasses it.

    The destination is not opened until the server has answered with a success
    status, so a 404 cannot leave a truncated or empty output behind. This is
    one attempt over gpio's shared HTTP client; :func:`copy_file` runs it under
    the one retry policy (``core/http_retry``), and a failed attempt's partial
    local file is removed by :func:`_copy_destination` before the next.
    """
    from geoparquet_io.core.http_retry import get_shared_http_client

    with get_shared_http_client().stream("GET", url) as response:
        response.raise_for_status()

        with _copy_destination(dest_path, dest_is_remote) as dest_handle:
            for chunk in response.iter_bytes():
                dest_handle.write(chunk)


@contextlib.contextmanager
def _copy_destination(dest_path: str, dest_is_remote: bool):
    """Yield a writable handle for a copy destination, committing only on success.

    Closing an obstore writer **commits** whatever it buffered. Doing that on a
    failure path would replace a good object at the destination key with a
    truncated one, so the writer is closed -- committed -- only when the copy
    body ran to completion. On failure the remote writer is dropped
    un-committed, and local copies publish through ``atomic_write`` only on
    success, so either destination keeps its previous version.
    """
    if not dest_is_remote:
        with atomic_write(dest_path) as staged, open(staged, "wb") as handle:
            yield handle
        return

    import obstore as obs

    store, key = resolve_object_store(dest_path)
    handle = obs.open_writer(store, key)
    yield handle
    handle.close()


def resolve_object_store(url: str) -> tuple[object, str]:
    """Resolve a remote URL to the ``(obstore store, key)`` pair gpio should use.

    S3 stores are built by :func:`geoparquet_io.core.upload._setup_store_and_kwargs`
    from the ambient S3 config, so a copy honours ``--s3-endpoint``,
    ``--s3-region``, ``--s3-no-ssl`` and ``--aws-profile`` exactly as every other
    remote write in gpio does (#810). Azure stores are built from the account and
    container in the ``az://`` URL, with credentials still read from the
    ``AZURE_STORAGE_*`` environment (#864). GCS goes through obstore's own
    ``from_url``, which needs no extra dependency. HTTP(S) never reaches this
    function: it carries a full URL, not a store plus key, and is streamed
    verbatim by :func:`_copy_http_source` instead.

    Args:
        url: Remote URL of an object-store family; an alias spelling
            (``s3a://``, ``gcs://``) is read under ``remote.STORE_SCHEME_FOR_FAMILY``
            via ``canonical_store_url``

    Returns:
        Tuple of (obstore store, key within that store)

    Raises:
        InvalidParameterError: If the scheme has no configured store
    """
    _check_copyable_scheme("path", url, writing=False)
    canonical = canonical_store_url(url)

    from geoparquet_io.core.upload import _setup_store_and_kwargs, parse_object_store_url

    bucket_url, key = parse_object_store_url(canonical)
    from geoparquet_io.core.duckdb_utils import get_active_s3_config

    s3_config = get_active_s3_config()
    store, _kwargs = _setup_store_and_kwargs(
        bucket_url,
        s3_config.get("profile"),
        chunk_concurrency=_COPY_CHUNK_CONCURRENCY,
        chunk_size=None,
        s3_endpoint=s3_config.get("s3_endpoint"),
        s3_region=s3_config.get("s3_region"),
        s3_use_ssl=s3_config.get("s3_use_ssl", True),
    )
    return store, key


def copy_file(source_path: str, dest_path: str, verbose: bool = False) -> None:
    """
    Copy a file byte-for-byte, from and to local paths or remote URLs.

    Used when a command's requested end state is already satisfied by its input,
    so the output it was asked for is a verbatim copy rather than a rewrite
    (``gpio add bbox`` on a file that already has a bbox column, #728).

    A remote s3://, s3a://, gs:// or gcs:// side is read or written through the
    object store :func:`resolve_object_store` builds, which is the store gpio is
    configured to use -- not a filesystem assembled from ambient credentials. An
    http(s):// source is streamed with a plain GET of the URL exactly as given
    (:func:`_copy_http_source`). Either way the bytes are streamed rather than
    held in memory.

    Args:
        source_path: Local path or remote URL to read
        dest_path: Local path or remote URL to write
        verbose: Whether to log debug info

    Raises:
        InvalidParameterError: If either side is a remote URL gpio cannot copy
    """
    import shutil

    if verbose:
        debug(f"Copying {source_path} to {dest_path}")

    source_is_remote = is_remote_url(source_path)
    dest_is_remote = is_remote_url(dest_path)

    if not source_is_remote and not dest_is_remote:
        with atomic_write(dest_path) as staged:
            shutil.copyfile(source_path, staged)
        return

    # Both schemes are checked before a byte moves, so an unsupported destination
    # cannot fail halfway through a read.
    if source_is_remote:
        _check_copyable_scheme("source_path", source_path, writing=False)
    if dest_is_remote:
        _check_copyable_scheme("dest_path", dest_path, writing=True)

    # An http(s) source is a full URL, not a store plus key: it is streamed with
    # a plain GET of the URL verbatim, so a presigned query string survives and
    # nothing is percent-encoded a second time (#825).
    if source_is_remote and is_http_url(source_path):
        from geoparquet_io.core.http_retry import (
            DEFAULT_CREDENTIALS_HINT,
            DEFAULT_MAX_RETRIES,
            _request_failure_types,
            final_request_error,
            run_with_retry,
        )

        try:
            run_with_retry(
                functools.partial(_copy_http_source, source_path, dest_path, dest_is_remote)
            )
        except _request_failure_types() as exc:
            error, cause = final_request_error(
                source_path,
                exc,
                max_retries=DEFAULT_MAX_RETRIES,
                credentials=DEFAULT_CREDENTIALS_HINT,
            )
            raise error from cause
        return

    import obstore as obs

    if source_is_remote:
        store, key = resolve_object_store(source_path)
        source_handle = obs.open_reader(store, key)
    else:
        source_handle = open(source_path, "rb")

    try:
        # _copy_destination commits the write (closes the writer) only if the
        # stream ran to completion; a mid-copy failure leaves the destination
        # as it was rather than committing a truncated object.
        with _copy_destination(dest_path, dest_is_remote) as dest_handle:
            shutil.copyfileobj(source_handle, dest_handle)
    finally:
        with contextlib.suppress(Exception):
            source_handle.close()


def resolve_file_url(file_path, verbose=False):
    """
    Validate a path and resolve it to the URL readers should open.

    A remote URL is passed through **verbatim**: per RFC 3986 a URL already *is*
    the percent-encoded form, so gpio takes the one the user pasted as-is rather
    than encoding it again. Encoding here used to turn a browser-copied
    ``my%20file.parquet`` into ``my%2520file.parquet`` and 404 (#825); ``%20``
    (an encoded space) and ``%2520`` (a name containing ``%20``) cannot be told
    apart from the string, so -- as with SQL escaping (#718) -- the value is
    transformed exactly once, at the boundary where its state is known. What an
    un-encoded URL (raw space, bracket) does is no longer defined by gpio but
    by the underlying reader -- today's HTTP stacks happen to encode a raw
    space themselves -- so encode it yourself rather than rely on that.

    Local paths are checked for existence. Nothing here escapes the result for
    SQL, which is what makes it the right resolver for both cases: hand the
    result to anything that opens the file directly (fsspec, pyarrow, metadata
    helpers), or to :func:`~geoparquet_io.core.duckdb_utils.sql_path`, which is
    the single point at which a path becomes a SQL literal (#802).

    Args:
        file_path: Local path or remote URL
        verbose: Whether to log debug info

    Returns:
        str: Resolved, unescaped file path/URL
    """

    if is_remote_url(file_path):
        # Azure reads are not supported: gpio never loads DuckDB's azure
        # extension, so an az:// input would die deep in the metadata probe
        # with a misleading "not a valid GeoParquet file". Name the actual
        # limitation here, before anything is opened. (az:// still works as a
        # write destination -- uploads and copies go through obstore.)
        if is_azure_url(file_path):
            raise InvalidParameterError(
                "path",
                f"cannot read '{file_path}': Azure Blob Storage is supported as a "
                "write destination only (az://<account>/<container>/<path>). "
                "Reading from Azure is not supported yet; download the file first.",
            )
        if verbose:
            debug(f"Reading from {protocol_label(file_path)}: {file_path}")
        return file_path

    if not has_glob_pattern(file_path) and not os.path.exists(file_path):
        raise FileNotFoundGeoParquetError(file_path)
    return file_path


def _get_file_cache_key(parquet_file: str) -> tuple[str, float]:
    if is_remote_url(parquet_file):
        return (parquet_file, 0)

    path = Path(parquet_file)
    if path.exists():
        return (str(path.resolve()), path.stat().st_mtime)
    return (str(path), 0)
