#!/usr/bin/env python3
"""
Arrow IPC streaming utilities for Unix-style piping between gpio commands.

This module provides low-level streaming primitives for reading/writing
Arrow IPC format to stdin/stdout, enabling pipelines like:

    gpio add bbox input.parquet | gpio sort hilbert - output.parquet

Arrow IPC is used because:
- Zero-copy data exchange between processes
- Preserves schema metadata (including GeoParquet geo metadata)
- Native support in PyArrow and DuckDB
- Efficient columnar format for geospatial data
"""

from __future__ import annotations

import hashlib
import json
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc

# Marker for stdin/stdout in CLI arguments
STREAM_MARKER = "-"

# Arrow's plain ``binary`` type uses 32-bit offsets, so the cumulative value
# bytes of a single array must stay below INT32_MAX (2,147,483,647). geoarrow's
# ``as_wkb`` re-encodes each Arrow chunk into a 32-bit ``binary`` array, so a
# chunk whose total WKB exceeds this ceiling overflows
# (``GeoArrowKernel<as_geoarrow>::push_batch() failed (75)``). DuckDB exports
# Arrow in ~1M-row batches with ``arrow_large_buffer_size=true`` (64-bit
# ``large_binary``), and 1M detailed polygons can easily blow past 2 GB. We
# re-chunk oversized batches under this byte budget before conversion so the
# 32-bit output stays valid while remaining maximally compatible downstream.
# The margin below 2 GB leaves headroom for per-value offset bookkeeping.
_MAX_WKB_CHUNK_BYTES = 1_500_000_000


def is_stdin(path: str | None) -> bool:
    """Check if path indicates stdin streaming."""
    return path == STREAM_MARKER


def is_stdout(path: str | None) -> bool:
    """Check if path indicates explicit stdout streaming."""
    return path == STREAM_MARKER


def should_stream_output(output_path: str | None) -> bool:
    """
    Determine if output should go to stdout.

    Returns True if:
    - output_path is "-" (explicit stdout)
    - output_path is None and stdout is a pipe (auto-detect)

    Returns False if:
    - output_path is a file path
    - output_path is None and stdout is a terminal
    """
    if output_path == STREAM_MARKER:
        return True
    if output_path is None:
        # Auto-detect: stream if stdout is piped, not a terminal
        return not sys.stdout.isatty()
    return False


def validate_stdin() -> None:
    """
    Validate stdin is available for streaming.

    Raises:
        StreamingError: If stdin is a terminal (no data piped)
    """
    if sys.stdin.isatty():
        raise StreamingError(
            "No data on stdin. Pipe from another command or use a file path.\n\n"
            "Examples:\n"
            "  gpio add bbox input.parquet | gpio sort hilbert - output.parquet\n"
            "  gpio sort hilbert input.parquet output.parquet"
        )


def validate_output(output_path: str | None) -> None:
    """
    Validate output configuration and raise/warn appropriately.

    Raises:
        StreamingError: If no output and stdout is a terminal

    Warns:
        If explicit "-" and stdout is a terminal (binary to terminal)
    """
    if output_path is None and sys.stdout.isatty():
        raise StreamingError(
            "Missing output. Pipe to another command or specify an output file.\n\n"
            "Examples:\n"
            "  gpio add bbox input.parquet output.parquet\n"
            "  gpio add bbox input.parquet | gpio sort hilbert - output.parquet"
        )
    if output_path == STREAM_MARKER and sys.stdout.isatty():
        from geoparquet_io.core.logging_config import warn

        warn("Writing binary Arrow IPC data to terminal...")


def read_arrow_stream() -> pa.Table:
    """
    Read an Arrow IPC stream from stdin.

    Returns:
        PyArrow Table with all data from the stream

    Raises:
        StreamingError: If stdin is a terminal or stream is invalid
    """
    validate_stdin()
    try:
        reader = ipc.RecordBatchStreamReader(sys.stdin.buffer)
        return reader.read_all()
    except pa.ArrowInvalid as e:
        error_msg = str(e)
        if "null or length 0" in error_msg:
            raise StreamingError(
                "No data received on stdin. This usually means the upstream command failed.\n\n"
                "Common causes:\n"
                "  - Upstream command encountered an error (check messages above)\n"
                "  - Input file doesn't exist or is invalid\n\n"
                "Example of correct piping syntax:\n"
                "  gpio extract input.parquet | gpio add bbox - | gpio sort hilbert - out.parquet\n"
                "                             ^               ^\n"
                "              (auto-streams when piped)  (read from stdin)"
            ) from e
        raise StreamingError(
            f"Invalid Arrow IPC stream on stdin. Ensure input is from a gpio command.\n\nError: {e}"
        ) from e


def write_arrow_stream(table: pa.Table) -> None:
    """
    Write a PyArrow Table as Arrow IPC stream to stdout.

    Args:
        table: PyArrow Table to write
    """
    writer = ipc.RecordBatchStreamWriter(sys.stdout.buffer, table.schema)
    writer.write_table(table)
    writer.close()


def extract_geo_metadata(table: pa.Table) -> dict | None:
    """
    Extract GeoParquet metadata from Arrow table schema.

    Args:
        table: PyArrow Table with potential geo metadata

    Returns:
        Parsed geo metadata dict, or None if not present
    """
    if table.schema.metadata and b"geo" in table.schema.metadata:
        try:
            return json.loads(table.schema.metadata[b"geo"].decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None
    return None


def apply_geo_metadata(table: pa.Table, geo_meta: dict) -> pa.Table:
    """
    Apply geo metadata to Arrow table schema.

    Args:
        table: PyArrow Table to update
        geo_meta: GeoParquet metadata dict to apply

    Returns:
        New table with updated schema metadata
    """
    metadata = dict(table.schema.metadata) if table.schema.metadata else {}
    metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")
    return table.replace_schema_metadata(metadata)


def apply_metadata_to_table(table: pa.Table, metadata: dict | None) -> pa.Table:
    """
    Apply raw metadata dict to Arrow table schema.

    Args:
        table: PyArrow Table to update
        metadata: Metadata dict (with bytes keys) to apply

    Returns:
        New table with updated schema metadata
    """
    if not metadata:
        return table
    return table.replace_schema_metadata(metadata)


def find_geometry_column_from_metadata(metadata: dict | None) -> str | None:
    """
    Find the primary geometry column name from metadata.

    A column-name reader shared with read-only callers (``Table.geometry_column``),
    so it takes the line #945 drew: it guards rather than sanitizes, and refuses
    to hand back the one thing that cannot be a column name. A carried
    ``primary_column: 123`` used to travel from here through ``extract`` into
    ``quote_identifier`` and fail with ``TypeError: argument of type 'int' is
    not iterable``, four frames from anything a user can act on (#968).

    It also no longer falls back to the literal ``"geometry"``: on a file whose
    column is called ``geom`` that default names a column that does not exist,
    which is the silently-wrong-coordinates bug #945's review identified. Every
    caller asks the file's schema when this reader says nothing, which is the
    answer a block-less file already gets.

    Args:
        metadata: Schema metadata dict (with bytes keys)

    Returns:
        Geometry column name or None if the block does not name a usable one
    """
    from geoparquet_io.core.geo_metadata import carried_column_name, decode_carried_geo

    if not metadata or b"geo" not in metadata:
        return None
    geo_meta = decode_carried_geo(metadata[b"geo"])
    if not isinstance(geo_meta, dict):
        return None
    return carried_column_name(geo_meta.get("primary_column"))


def find_geometry_column_from_table(table: pa.Table) -> str | None:
    """
    Find the geometry column name from table metadata or common names.

    Args:
        table: PyArrow Table to inspect

    Returns:
        Geometry column name or None if not found
    """
    metadata = dict(table.schema.metadata) if table.schema.metadata else {}

    # Try to find from geo metadata
    geom_col = find_geometry_column_from_metadata(metadata)
    if geom_col and geom_col in table.column_names:
        return geom_col

    # Fall back to common names
    from geoparquet_io.core.geometry_detection import STANDARD_GEOMETRY_NAMES

    for name in STANDARD_GEOMETRY_NAMES:
        if name in table.column_names:
            return name

    return None


def get_crs_from_arrow_table(table: pa.Table, geometry_column: str) -> str | None:
    """
    Get CRS from Arrow table's GeoParquet metadata.

    Args:
        table: PyArrow Table to inspect
        geometry_column: Name of the geometry column

    Returns:
        CRS string (e.g., "EPSG:4326") or None if not found
    """
    import json

    metadata = dict(table.schema.metadata) if table.schema.metadata else {}

    # Check for GeoParquet geo metadata
    geo_bytes = metadata.get(b"geo")
    if not geo_bytes:
        return None

    try:
        from geoparquet_io.core.crs_utils import crs_is_explicitly_null, warn_null_crs_once

        geo_meta = json.loads(geo_bytes.decode("utf-8"))
        columns = geo_meta.get("columns", {})
        col_meta = columns.get(geometry_column, {})

        if crs_is_explicitly_null(col_meta):
            warn_null_crs_once(
                f"table:{hashlib.sha1(geo_bytes, usedforsecurity=False).hexdigest()}"
            )

        crs = col_meta.get("crs")
        if crs:
            # Extract EPSG code from CRS object
            if isinstance(crs, dict):
                auth = crs.get("id", {})
                if auth.get("authority") == "EPSG":
                    return f"EPSG:{auth.get('code')}"
            return str(crs) if not isinstance(crs, dict) else None

        return None
    except Exception:
        return None


def read_stdin_to_temp_file(verbose: bool = False) -> str:
    """
    Read Arrow IPC stream from stdin and write to a temporary parquet file.

    This is a shared utility for commands that need file-based processing
    but want to support stdin input. The caller is responsible for cleanup.

    The stream's ``geo`` metadata is reconciled with the stream's own schema
    before the temp file is written: entries naming a column the stream does not
    carry are dropped, then any derived stat left out is computed from the rows.
    gpio's own producers now carry ``geometry_types``, but a stream can come from
    anywhere, and DuckDB refuses to open a Parquet file whose ``geo`` metadata
    declares a geometry column without it — so closing both gaps here is what
    keeps a pipe readable by the file-based command on the other end (#722).

    Args:
        verbose: Whether to print verbose output

    Returns:
        Path to the temporary parquet file. Caller must delete after use.
    """
    import os
    import tempfile
    import uuid

    # Registers the GeoArrow extension types process-wide, so a geometry column
    # arriving on the stream as `geoarrow.wkb` is written to the scratch file
    # with its Parquet GEOMETRY/GEOGRAPHY logical type -- and the CRS inside it --
    # rather than demoted to plain `binary`. That scratch file is the witness the
    # write facade then reads the output's version and CRS from (#993), so it has
    # to be lossless with respect to the stream. The registration is global, which
    # is why it happens here rather than being relied on (#1006).
    import geoarrow.pyarrow  # noqa: F401
    import pyarrow.parquet as pq

    from geoparquet_io.core.geo_metadata import (
        backfill_derived_stats,
        prune_geo_metadata_to_columns,
    )
    from geoparquet_io.core.logging_config import debug

    if verbose:
        debug("Reading Arrow IPC stream from stdin...")

    table = read_arrow_stream()

    metadata = dict(table.schema.metadata) if table.schema.metadata else None
    if metadata:
        metadata = prune_geo_metadata_to_columns(metadata, table.column_names)
        table = table.replace_schema_metadata(backfill_derived_stats(metadata, table, verbose))

    # Write to temp file with UUID for uniqueness
    temp_dir = tempfile.gettempdir()
    temp_path = os.path.join(temp_dir, f"gpio_stdin_{uuid.uuid4()}.parquet")

    pq.write_table(table, temp_path)

    if verbose:
        debug(f"Wrote {table.num_rows} rows to temporary file: {temp_path}")

    return temp_path


def _wkb_chunk_data_nbytes(arr: pa.Array) -> int:
    """Return the total value bytes (excluding offsets/validity) of a binary array."""
    if len(arr) == 0:
        return 0
    total = pc.sum(pc.binary_length(arr)).as_py()
    return int(total or 0)


def byte_limited_spans(arr: pa.Array, max_bytes: int) -> list[tuple[int, int]]:
    """
    Return ``(offset, length)`` row spans whose cumulative value bytes stay under a limit.

    Bounds on bytes rather than rows because per-row WKB size varies wildly
    (a point is ~21 bytes, a detailed field-boundary polygon can be kilobytes).
    A single value larger than ``max_bytes`` is emitted in its own span; the
    caller surfaces a clear error if even that one row overflows the 32-bit
    ceiling on conversion.

    Shared by the two places that must respect Arrow's 32-bit binary ceiling:
    IPC streaming (:func:`_split_array_under_byte_limit`) and the arrow-streaming
    Parquet writer, which narrows ``large_binary`` WKB to plain ``binary``.
    """
    lengths = pc.binary_length(arr).to_pylist()
    spans: list[tuple[int, int]] = []
    start = 0
    running = 0
    for i, length in enumerate(lengths):
        length = length or 0  # null geometries contribute no value bytes
        if i > start and running + length > max_bytes:
            spans.append((start, i - start))
            start = i
            running = 0
        running += length
    spans.append((start, len(arr) - start))
    return spans


def _split_array_under_byte_limit(arr: pa.Array, max_bytes: int) -> list[pa.Array]:
    """Split a binary array into slices whose cumulative value bytes stay under a limit."""
    return [arr.slice(offset, length) for offset, length in byte_limited_spans(arr, max_bytes)]


def _rebatch_wkb_under_byte_limit(
    geom_col: pa.ChunkedArray | pa.Array,
    max_bytes: int | None = None,
) -> pa.ChunkedArray | pa.Array:
    """
    Re-chunk a (chunked) binary array so no chunk exceeds the 32-bit offset ceiling.

    Returns the input unchanged when every chunk already fits the budget, so the
    common (small-batch) case is a no-op. Oversized chunks are split on byte
    boundaries (see :func:`_split_array_under_byte_limit`) before geoarrow
    re-encodes them into 32-bit ``binary`` WKB (issue #511).
    """
    if max_bytes is None:
        max_bytes = _MAX_WKB_CHUNK_BYTES

    chunks = geom_col.chunks if isinstance(geom_col, pa.ChunkedArray) else [geom_col]
    rebatched: list[pa.Array] = []
    needs_split = False
    for chunk in chunks:
        if _wkb_chunk_data_nbytes(chunk) > max_bytes:
            needs_split = True
            rebatched.extend(_split_array_under_byte_limit(chunk, max_bytes))
        else:
            rebatched.append(chunk)

    if not needs_split:
        return geom_col
    return pa.chunked_array(rebatched, type=geom_col.type)


def _ensure_at_least_one_chunk(
    geom_col: pa.ChunkedArray | pa.Array,
) -> pa.ChunkedArray | pa.Array:
    """Give a zero-chunk ChunkedArray a single empty chunk of its own type.

    A DuckDB result with no rows exports its columns as ChunkedArrays holding
    *zero* chunks. ``geoarrow.pyarrow.as_wkb`` converts chunk by chunk and then
    rebuilds a ChunkedArray from the results without passing a type, and Arrow
    C++ aborts the whole process on ``ChunkedArray([])`` with an omitted type
    ("cannot construct ChunkedArray from empty vector and omitted type",
    SIGABRT). That is not a Python exception, so it cannot be caught -- the
    zero-chunk case has to be avoided rather than handled (issue #804).

    One empty chunk of the same type carries the type through the conversion
    and leaves the column's length at 0.
    """
    if isinstance(geom_col, pa.ChunkedArray) and geom_col.num_chunks == 0:
        return pa.chunked_array([pa.array([], type=geom_col.type)], type=geom_col.type)
    return geom_col


def apply_geoarrow_extension_type(
    table: pa.Table,
    geometry_column: str,
    crs: dict | str | None = None,
) -> pa.Table:
    """
    Convert geometry column to geoarrow extension type.

    This enables native geometry performance in downstream operations.
    Arrow IPC preserves extension types, so geoarrow types survive piping.

    Args:
        table: PyArrow Table with geometry column
        geometry_column: Name of the geometry column
        crs: CRS as PROJJSON dict, string identifier, or None

    Returns:
        Table with geometry column converted to geoarrow extension type

    Raises:
        StreamingError: If the geometry column is WKB but cannot be encoded
            (e.g. a single geometry above the 2 GB Arrow offset ceiling, or
            malformed WKB). Surfacing this loudly avoids the misleading
            downstream "No data received on stdin" (issue #511).
    """
    import geoarrow.pyarrow as ga

    if geometry_column not in table.column_names:
        return table

    try:
        geom_col = table.column(geometry_column)

        # A zero-row result has zero chunks, which aborts the process inside
        # geoarrow's conversion. Materialize one empty, typed chunk first so an
        # empty result streams as a valid, correctly-typed empty column (#804).
        geom_col = _ensure_at_least_one_chunk(geom_col)

        # Keep each Arrow chunk under the 32-bit binary offset ceiling before
        # geoarrow re-encodes it. DuckDB exports geometry as 64-bit
        # large_binary batches that can exceed 2 GB, but ga.as_wkb produces
        # 32-bit binary — so an oversized batch overflows without this guard
        # (issue #511).
        geom_col = _rebatch_wkb_under_byte_limit(geom_col)

        # Convert to geoarrow WKB extension type
        wkb_arr = ga.as_wkb(geom_col)

        # Apply CRS if provided
        if crs:
            new_type = wkb_arr.type.with_crs(crs)
            # Use from_storage to preserve CRS (cast() resets it)
            new_chunks = []
            for chunk in wkb_arr.chunks:
                new_chunk = pa.ExtensionArray.from_storage(new_type, chunk.storage)
                new_chunks.append(new_chunk)
            wkb_arr = pa.chunked_array(new_chunks, type=new_type)

        # Replace geometry column in table
        col_index = table.schema.get_field_index(geometry_column)
        return table.set_column(col_index, geometry_column, wkb_arr)

    except (TypeError, ValueError, AttributeError):
        # The column isn't convertible WKB (e.g. already-native nested
        # geometry); pass it through unchanged rather than failing the stream.
        return table
    except Exception as e:
        # Any other failure — notably geoarrow's GeoArrowCException (a
        # RuntimeError subclass) for malformed WKB or a single value above the
        # 2 GB offset ceiling — must surface with its true cause. Returning the
        # table here would silently emit un-converted geometry, and letting the
        # raw exception escape gets masked downstream as the misleading "No
        # data received on stdin" (issue #511).
        raise StreamingError(
            f"Failed to convert geometry column '{geometry_column}' to GeoArrow "
            f"WKB for streaming: {e}\n\n"
            "If a single geometry's WKB exceeds 2 GB it cannot be encoded into "
            "Arrow's 32-bit binary layout; simplify very large geometries before "
            "streaming."
        ) from e


def extract_crs_from_table(
    table: pa.Table,
    geometry_column: str | None = None,
) -> dict | str | None:
    """
    Extract CRS from Arrow table.

    Checks in order:
    1. Geoarrow extension type CRS (``field.type.crs`` — the shape PyArrow
       produces once ``geoarrow.pyarrow`` has registered its extension types)
    2. GeoParquet geo metadata
    3. The geometry field's raw ``ARROW:extension:metadata`` — the *other*
       import state, where nothing registered the extension type and the CRS is
       still sitting on the field. Without it a GeoArrow file with no ``geo``
       block returned None and the write labelled projected data as the CRS84
       default (issue #863).

    Args:
        table: PyArrow Table to inspect
        geometry_column: Name of geometry column (auto-detect if None)

    Returns:
        CRS as PROJJSON dict, string, or None if not found
    """
    from geoparquet_io.core.crs_utils import _crs_from_geoarrow_field, geoarrow_crs_to_projjson

    # Find geometry column if not specified
    if geometry_column is None:
        geometry_column = find_geometry_column_from_table(table)

    if geometry_column and geometry_column in table.column_names:
        geom_type = table.column(geometry_column).type

        # Check for geoarrow extension type with CRS
        if hasattr(geom_type, "crs") and geom_type.crs is not None:
            resolved = geoarrow_crs_to_projjson(geom_type.crs)
            if resolved is not None:
                return resolved
            # Unreadable CRS object: fall through to the geo metadata rather
            # than returning a value that isn't a CRS (issue #816).

    # Fall back to geo metadata
    if table.schema.metadata and b"geo" in table.schema.metadata:
        try:
            from geoparquet_io.core.crs_utils import crs_is_explicitly_null, warn_null_crs_once
            from geoparquet_io.core.geo_metadata import (
                carried_geometry_column,
                sanitize_geo_metadata,
            )

            geo_bytes = table.schema.metadata[b"geo"]
            # `Table.write()` resolves the CRS through here before it builds any
            # metadata, so this reader sees a malformed carried block first and
            # has to survive it too (#771).
            geo_meta = sanitize_geo_metadata(json.loads(geo_bytes.decode("utf-8")))
            if isinstance(geo_meta, dict):
                columns = geo_meta.get("columns", {})
                # The literal `"geometry"` default named a column that does not
                # exist on a file whose column is `geom`, and reported its CRS
                # as absent (#887 review). Ask the table's schema instead — the
                # shared recovery #960 shared out (#968).
                geom_col_name = geometry_column or carried_geometry_column(
                    geo_meta, table.column_names
                )
                if geom_col_name in columns:
                    if crs_is_explicitly_null(columns[geom_col_name]):
                        warn_null_crs_once(
                            f"table:{hashlib.sha1(geo_bytes, usedforsecurity=False).hexdigest()}"
                        )
                    return columns[geom_col_name].get("crs")
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

    # Last resort: a GeoArrow field whose extension type nothing registered, so
    # the CRS is still in `ARROW:extension:metadata`. Reached only when the file
    # has no `geo` block for this column at all — a `geo` block that declares a
    # null CRS returns above, because "unknown" is an answer.
    if geometry_column:
        return _crs_from_geoarrow_field(table, geometry_column)

    return None


def extract_version_from_metadata(metadata: dict | None) -> str | None:
    """
    Extract GeoParquet version string from schema metadata.

    Upgrades 1.0 to 1.1 since 1.1 is backwards compatible and preferred.

    Args:
        metadata: Schema metadata dict (with bytes keys)

    Returns:
        Version string suitable for --geoparquet-version (e.g., "1.1", "2.0")
        or None if no version detected
    """
    if not metadata or b"geo" not in metadata:
        return None
    from geoparquet_io.core.geo_metadata import carried_version

    try:
        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        if isinstance(geo_meta, dict):
            # A truthiness guard let `2`, `2.0`, `["2.0"]` and `true` through to
            # `.split`, and the `except` below names the two decoder errors
            # specifically, so the `AttributeError` passed straight through it
            # and out of `gpio sort hilbert` as a traceback (#979).
            version = carried_version(geo_meta.get("version"))
            if version:
                parts = version.split(".")
                if len(parts) >= 2:
                    major = parts[0]
                    # Upgrade all 1.x versions to 1.1 (backwards compatible)
                    if major == "1":
                        return "1.1"
                    # Flatten any 2.x to "2.0" — the highest 2.x this writer
                    # knows — so this path agrees with its file-based twin,
                    # resolve_geoparquet_version_from_file (common.py).
                    if major == "2":
                        return "2.0"
                    return f"{major}.{parts[1]}"
        return None
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def has_geoarrow_extension_in_table(table: pa.Table) -> bool:
    """
    Check if table has geoarrow extension types (indicating native geo types).

    Args:
        table: PyArrow Table to inspect

    Returns:
        True if table has geoarrow extension type columns
    """
    from geoparquet_io.core.geoarrow_encoding import is_geoarrow_extension_field

    # Field, not type: the extension name may be carried in the field metadata
    # instead of a resolved extension type, and both shapes are the same
    # column (#792).
    return any(is_geoarrow_extension_field(field) for field in table.schema)


def detect_version_for_output(
    original_metadata: dict | None,
    table: pa.Table | None = None,
) -> str | None:
    """
    Detect the appropriate GeoParquet version for output.

    Logic:
    - If geo metadata has version 1.x -> return "1.1" (upgrade 1.0 to 1.1)
    - If geo metadata has version 2.x -> return "2.0"
    - If no geo metadata but has geoarrow types -> return "2.0" (upgrade)
    - Otherwise -> return None (will use default 1.1)

    Args:
        original_metadata: Schema metadata from input
        table: Arrow table (optional, for detecting geoarrow types)

    Returns:
        Version string or None
    """
    # First check geo metadata for explicit version
    version = extract_version_from_metadata(original_metadata)
    if version:
        return version

    # Check for parquet-geo-only (geoarrow types without geo metadata)
    if table is not None and has_geoarrow_extension_in_table(table):
        return "2.0"  # Upgrade to 2.0 with proper metadata

    # No version info - will use default (1.1)
    return None


class StreamingError(Exception):
    """Error raised during Arrow IPC streaming operations."""

    pass
