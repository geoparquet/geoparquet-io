"""
File path utilities for GeoParquet files.
"""

import contextlib
import fnmatch
import glob as glob_module
import os
from pathlib import Path, PurePath

from geoparquet_io.core.duckdb_utils import _escape_sql_string
from geoparquet_io.core.exceptions import (
    FileNotFoundGeoParquetError,
    GeoParquetError,
    InvalidParameterError,
)
from geoparquet_io.core.logging_config import debug


def has_glob_pattern(path: str) -> bool:
    return any(c in path for c in ("*", "?", "["))


def is_partition_path(path: str) -> bool:
    from geoparquet_io.core.remote import is_remote_url

    if has_glob_pattern(path):
        return True

    if not is_remote_url(path) and os.path.isdir(path):
        return True

    if is_remote_url(path):
        path_parts = path.split("/")
        for part in path_parts[3:]:
            if "=" in part and not part.endswith(".parquet"):
                return True

    return False


def resolve_partition_path(path: str, hive_partitioning: bool | None = None) -> tuple[str, dict]:
    from geoparquet_io.core.remote import is_remote_url

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


def get_first_parquet_file(partition_path: str) -> str | None:
    from geoparquet_io.core.remote import is_remote_url

    if is_remote_url(partition_path):
        return partition_path

    if os.path.isdir(partition_path):
        for root, _dirs, files in os.walk(partition_path):
            for f in sorted(files):
                if f.endswith(".parquet"):
                    return os.path.join(root, f)
        return None

    if has_glob_pattern(partition_path):
        matches = glob_module.glob(partition_path, recursive=True)
        parquet_matches = [m for m in sorted(matches) if m.endswith(".parquet")]
        return parquet_matches[0] if parquet_matches else None

    return partition_path


def get_all_parquet_files(partition_path: str) -> list[str]:
    from geoparquet_io.core.remote import is_remote_url

    if is_remote_url(partition_path):
        return [partition_path]

    if os.path.isdir(partition_path):
        parquet_files = []
        for root, _dirs, files in os.walk(partition_path):
            for f in files:
                if f.endswith(".parquet"):
                    parquet_files.append(os.path.join(root, f))
        return sorted(parquet_files)

    if has_glob_pattern(partition_path):
        matches = glob_module.glob(partition_path, recursive=True)
        return sorted([m for m in matches if m.endswith(".parquet")])

    return [partition_path] if os.path.exists(partition_path) else []


def validate_output_path(output_path, verbose=False):
    from geoparquet_io.core.remote import is_remote_url

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

    if "://" in output_file:
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
    first_magic = next((i for i, p in enumerate(parts) if has_glob_pattern(p)), len(parts))
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
    from geoparquet_io.core.remote import is_remote_url

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

    Remote URLs are compared verbatim: there is no local filesystem to resolve
    them against.
    """
    if not first or not second:
        return False
    if first == second:
        return True

    from geoparquet_io.core.remote import is_remote_url

    if is_remote_url(first) or is_remote_url(second):
        return False
    try:
        return Path(first).resolve() == Path(second).resolve()
    except (OSError, ValueError):
        # ValueError is a Windows path shape -- a malformed drive spec or an
        # embedded NUL. "I cannot tell" answers no: the callers use this to
        # decide whether they are about to overwrite their own input, and the
        # safe direction is to take the not-in-place branch.
        return False


def handle_output_overwrite(
    output_path: str | None, overwrite: bool, input_path: str | None = None
) -> None:
    if not output_path:
        return

    # Before the existence check: on the FIRST run the offending file does not
    # exist yet, and creating it is what breaks every later run (#867).
    guard_output_not_read_as_input(input_path, output_path)

    output_file = Path(output_path)

    if not output_file.exists():
        return

    if input_path and is_same_file_path(output_path, input_path):
        raise GeoParquetError(f"Cannot overwrite input file: {output_path}")

    if not overwrite:
        raise GeoParquetError(f"Output file already exists: {output_path}")

    output_file.unlink()


# Schemes copy_file can resolve to a configured object store. The aliases map onto
# the canonical scheme gpio's upload path speaks; every other scheme is_remote_url()
# accepts is refused by name, rather than dying inside a filesystem library on a
# dependency gpio does not ship (#810). Azure is served through the one spelling
# gpio's upload path parses -- az://<account>/<container>/<path>, built by
# upload._build_azure_store (#864). The abfs[s]:// and azure:// spellings put the
# account and container in a different order, or in a host name, so they are
# refused by name rather than mis-parsed as account-first.
_COPYABLE_STORE_SCHEMES = ("s3", "gs", "az")
_COPY_SCHEME_ALIASES = {"s3a": "s3", "gcs": "gs"}
_HTTP_SCHEMES = ("http", "https")
_UNPARSEABLE_AZURE_SCHEMES = ("abfs", "abfss", "azure")

# _setup_store_and_kwargs() folds this into the upload kwargs it returns alongside
# the store. A streamed copy does not use those kwargs, so this only satisfies the
# signature; obstore's own default is the same number.
_COPY_CHUNK_CONCURRENCY = 12


def _canonical_remote_url(url: str) -> tuple[str, str]:
    """Split a remote URL into (canonical scheme, canonical URL)."""
    scheme, separator, rest = url.partition("://")
    scheme = scheme.lower()
    canonical = _COPY_SCHEME_ALIASES.get(scheme, scheme)
    return canonical, f"{canonical}{separator}{rest}"


def _check_copyable_scheme(param_name: str, url: str, writing: bool) -> None:
    """Reject a scheme copy_file cannot serve, before any store or I/O is built."""
    scheme, _ = _canonical_remote_url(url)

    if scheme in _HTTP_SCHEMES:
        if writing:
            raise InvalidParameterError(
                param_name,
                f"'{url}' is an HTTP(S) URL, which is read-only. Write to a local "
                "path or to an s3://, gs:// or az:// URL instead.",
            )
        return

    if scheme in _UNPARSEABLE_AZURE_SCHEMES:
        from geoparquet_io.core.upload import AZURE_URL_FORM

        raise InvalidParameterError(
            param_name,
            f"cannot copy '{url}': gpio addresses Azure Blob Storage as "
            f"{AZURE_URL_FORM}. Rewrite the URL in that form.",
        )

    if scheme not in _COPYABLE_STORE_SCHEMES:
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
    status, so a 404 cannot leave a truncated or empty output behind.
    """
    import httpx

    with (
        httpx.Client(follow_redirects=True) as client,
        client.stream("GET", url) as response,
    ):
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
    un-committed, which leaves the destination exactly as it was, and a partial
    local file is unlinked. Cleanup exceptions are suppressed so they cannot
    mask the copy failure itself.
    """
    if dest_is_remote:
        import obstore as obs

        store, key = resolve_object_store(dest_path)
        handle = obs.open_writer(store, key)
    else:
        handle = open(dest_path, "wb")

    committed = False
    try:
        yield handle
        handle.close()
        committed = True
    finally:
        if not committed and not dest_is_remote:
            with contextlib.suppress(Exception):
                handle.close()
            with contextlib.suppress(OSError):
                os.unlink(dest_path)


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
        url: Remote URL for a scheme in ``_COPYABLE_STORE_SCHEMES`` (or an alias)

    Returns:
        Tuple of (obstore store, key within that store)

    Raises:
        InvalidParameterError: If the scheme has no configured store
    """
    _check_copyable_scheme("path", url, writing=False)
    _scheme, canonical = _canonical_remote_url(url)

    from geoparquet_io.core.duckdb_utils import get_active_s3_config
    from geoparquet_io.core.upload import _setup_store_and_kwargs, parse_object_store_url

    bucket_url, key = parse_object_store_url(canonical)
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

    from geoparquet_io.core.remote import is_remote_url

    if verbose:
        debug(f"Copying {source_path} to {dest_path}")

    source_is_remote = is_remote_url(source_path)
    dest_is_remote = is_remote_url(dest_path)

    if not source_is_remote and not dest_is_remote:
        shutil.copyfile(source_path, dest_path)
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
    if source_is_remote and _canonical_remote_url(source_path)[0] in _HTTP_SCHEMES:
        _copy_http_source(source_path, dest_path, dest_is_remote)
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
    from geoparquet_io.core.remote import is_remote_url

    if is_remote_url(file_path):
        # Azure reads are not supported: gpio never loads DuckDB's azure
        # extension, so an az:// input would die deep in the metadata probe
        # with a misleading "not a valid GeoParquet file". Name the actual
        # limitation here, before anything is opened. (az:// still works as a
        # write destination -- uploads and copies go through obstore.)
        scheme = file_path.split("://", 1)[0].lower()
        if scheme in ("az", *_UNPARSEABLE_AZURE_SCHEMES):
            raise InvalidParameterError(
                "path",
                f"cannot read '{file_path}': Azure Blob Storage is supported as a "
                "write destination only (az://<account>/<container>/<path>). "
                "Reading from Azure is not supported yet; download the file first.",
            )
        if verbose:
            protocol = file_path.split("://")[0].upper() if "://" in file_path else "HTTP"
            debug(f"Reading from {protocol}: {file_path}")
        return file_path

    if not has_glob_pattern(file_path) and not os.path.exists(file_path):
        raise FileNotFoundGeoParquetError(file_path)
    return file_path


def safe_file_url(file_path, verbose=False):
    """
    Prepare a file path for safe use in SQL queries.

    .. deprecated:: superseded by
       :func:`~geoparquet_io.core.duckdb_utils.sql_path`

       No gpio caller uses this any more (#802). It returns a *bare* escaped
       value that the caller must quote itself, which is the shape that made
       the path escape someone else's problem and produced #718's crashes:
       write ``FROM {sql_path(raw_path)}`` instead, or
       :func:`resolve_file_url` when the value is not going into SQL at all.
       It is kept only because the escape-exactly-once contract these tests
       pin is worth stating in one place.

    Resolves the path with :func:`resolve_file_url` -- which takes a remote URL
    as already percent-encoded (#825) -- and escapes single quotes to prevent
    SQL injection when paths are interpolated into queries. That escape is the
    only transform applied to the value.

    Escaping is **not** idempotent: the result is only ever interpolated into
    SQL, never re-escaped and never handed back to a filesystem API. Pass the
    raw path to helpers that escape their own arguments (everything in
    ``duckdb_metadata``), and use :func:`resolve_file_url` for direct reads.

    Args:
        file_path: Local path or remote URL
        verbose: Whether to log debug info

    Returns:
        str: Safe file path/URL with single quotes escaped for SQL
    """
    return _escape_sql_string(resolve_file_url(file_path, verbose))


def _get_file_cache_key(parquet_file: str) -> tuple[str, float]:
    from geoparquet_io.core.remote import is_remote_url

    if is_remote_url(parquet_file):
        return (parquet_file, 0)

    path = Path(parquet_file)
    if path.exists():
        return (str(path.resolve()), path.stat().st_mtime)
    return (str(path), 0)
