"""PMTiles generation using tippecanoe subprocess.

Orchestrates a streaming pipeline: GeoParquet → gpio commands → tippecanoe → PMTiles.
Requires tippecanoe to be installed and available in PATH.
"""

import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import IO

import pyarrow.parquet as pq

from geoparquet_io.core.column_selection import split_column_list
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.common import get_dataset_bounds
from geoparquet_io.core.duckdb_utils import quote_identifier
from geoparquet_io.core.inspect import get_primary_geometry_column
from geoparquet_io.core.logging_config import debug, success
from geoparquet_io.core.tile_join import (
    TileJoinNotFoundError,
    _build_tile_join_command,
    _check_tile_join,
    _run_tile_join,
)


class TippecanoeNotFoundError(Exception):
    """Raised when tippecanoe is not found in PATH."""

    def __init__(self):
        super().__init__(
            "tippecanoe not found in PATH.\n\n"
            "To use gpio pmtiles, install tippecanoe:\n"
            "  macOS:  brew install tippecanoe\n"
            "  Ubuntu: sudo apt install tippecanoe\n"
            "  Source: https://github.com/felt/tippecanoe#installation\n\n"
            "Alternatively, use the streaming approach:\n"
            "  gpio convert geojson data.parquet | tippecanoe -P -o output.pmtiles"
        )


def _validate_path(path: str) -> None:
    """
    Validate file path to prevent shell injection.

    Raises:
        ValueError: If path contains shell metacharacters
    """
    dangerous_chars = [";", "|", "&", "$", "`", "\n", "\r"]
    for char in dangerous_chars:
        if char in path:
            raise ValueError(
                f"Path contains dangerous character '{char}': {path}\n"
                "File paths must not contain shell metacharacters."
            )


def _get_gpio_executable() -> str:
    """Get the path to the gpio executable in the current Python environment."""
    python_bin_dir = Path(sys.executable).parent
    gpio_path = python_bin_dir / "gpio"

    if gpio_path.exists() and gpio_path.is_file():
        return str(gpio_path)

    gpio_in_path = shutil.which("gpio")
    if gpio_in_path:
        return gpio_in_path

    return "gpio"


def resolve_scratch_directory(explicit: str | None) -> str:
    """Where one pmtiles run puts its scratch: tippecanoe's ``-t`` and gpio's own.

    An explicit directory must already exist and be writable. Checked here,
    before any input is scanned, because the alternative is tippecanoe's
    ``mkstemp`` error after the streaming has started (#1115). Without one
    the package rule applies -- ``tempfile.gettempdir()``, the same call
    ``spill_directory`` and every other temp file in gpio use -- so
    ``TMPDIR``/``TEMP``/``TMP`` are honoured and a stale value falls back to
    ``/tmp`` instead of being forwarded raw to a tool that would die on it.
    tippecanoe itself ignores ``TMPDIR``, which is why the directory is
    always passed explicitly.
    """
    if not explicit:
        return tempfile.gettempdir()
    path = os.path.abspath(explicit)
    if not os.path.isdir(path):
        raise InvalidParameterError("temporary_directory", f"not a directory: {path}")
    if not os.access(path, os.W_OK):
        raise InvalidParameterError("temporary_directory", f"not writable: {path}")
    return path


def scratch_env(scratch: str) -> dict[str, str]:
    """Environment for a child gpio process so its temp files follow ``scratch``.

    ``-t`` moves tippecanoe's scratch only; the ``gpio extract | gpio convert``
    chain feeding it spills DuckDB sorts and stdin buffers wherever the
    child's ``tempfile.gettempdir()`` points. Setting all three variables
    keeps every temp file of the run on the volume the user named.
    """
    return {**os.environ, "TMPDIR": scratch, "TEMP": scratch, "TMP": scratch}


def _check_tippecanoe() -> bool:
    """Check if tippecanoe is available in PATH."""
    return shutil.which("tippecanoe") is not None


@dataclass(frozen=True)
class ChunkCell:
    """One cell of the chunk grid, with the column/row it came from."""

    ix: int
    iy: int
    minx: float
    miny: float
    maxx: float
    maxy: float
    last_x: bool
    last_y: bool


def _parse_chunks(spec: str) -> tuple[int, int]:
    """Parse a ``NxM`` chunk grid.

    ``auto`` is deliberately unsupported: choosing a grid means modelling
    tippecanoe's scratch against feature count and free space, and an
    uncalibrated guess picks a grid that still fills the disk -- the exact
    failure chunking exists to prevent (#1116).
    """
    text = (spec or "").strip()
    if text.lower() == "auto":
        raise ValueError("--chunks auto is not supported yet; give an explicit grid such as 4x3")
    parts = text.lower().split("x")
    if len(parts) != 2:
        raise ValueError(f"--chunks must look like NxM (e.g. 4x3), got {spec!r}")
    try:
        nx, ny = int(parts[0]), int(parts[1])
    except ValueError:
        raise ValueError(f"--chunks must look like NxM (e.g. 4x3), got {spec!r}") from None
    if nx < 1 or ny < 1:
        raise ValueError(f"--chunks needs positive counts, got {spec!r}")
    return nx, ny


def _chunk_cells(bounds: tuple[float, float, float, float], nx: int, ny: int) -> list[ChunkCell]:
    """Split ``bounds`` into an ``nx`` by ``ny`` grid of abutting cells."""
    minx, miny, maxx, maxy = bounds
    dx = (maxx - minx) / nx
    dy = (maxy - miny) / ny
    cells = []
    for iy in range(ny):
        for ix in range(nx):
            cells.append(
                ChunkCell(
                    ix=ix,
                    iy=iy,
                    minx=minx + ix * dx,
                    miny=miny + iy * dy,
                    maxx=minx + (ix + 1) * dx if ix < nx - 1 else maxx,
                    maxy=miny + (iy + 1) * dy if iy < ny - 1 else maxy,
                    last_x=ix == nx - 1,
                    last_y=iy == ny - 1,
                )
            )
    return cells


def _chunk_where(cell: ChunkCell, geometry_column: str, user_where: str | None) -> str:
    """A predicate assigning each feature to exactly one chunk, by centroid.

    ``--bbox`` selects features that *intersect* a box, so a polygon straddling
    a chunk edge is tiled by both neighbours and appears twice in the joined
    archive -- a visible seam under translucent fills. Assigning by centroid is
    disjoint: the interval is half-open, closed only on the grid's far edge so
    the extreme centroid still lands somewhere (#1116).
    """
    col = quote_identifier(geometry_column)
    cx = f"ST_X(ST_Centroid({col}))"
    cy = f"ST_Y(ST_Centroid({col}))"
    x_hi = "<=" if cell.last_x else "<"
    y_hi = "<=" if cell.last_y else "<"
    clauses = [
        f"{cx} >= {cell.minx!r}",
        f"{cx} {x_hi} {cell.maxx!r}",
        f"{cy} >= {cell.miny!r}",
        f"{cy} {y_hi} {cell.maxy!r}",
    ]
    if user_where:
        # Parenthesised so an OR inside the caller's clause cannot swallow the
        # chunk predicate and pull in the whole dataset.
        clauses.append(f"({user_where})")
    return " AND ".join(clauses)


def _parts_dir(output_path: str) -> str:
    """Where per-chunk archives live between runs.

    Beside the output, deterministically, so a re-run after a crash finds the
    parts it already built and skips them. A TemporaryDirectory would be
    removed on failure too, which is precisely when resume matters (#1116).
    """
    return f"{output_path}.parts"


def _part_path(output_path: str, cell: ChunkCell) -> str:
    return os.path.join(_parts_dir(output_path), f"chunk_{cell.iy}_{cell.ix}.pmtiles")


def _reject_levelled_input(input_path: str) -> None:
    """Refuse an overview GeoParquet: chunking it would multiply-count.

    In the overviews spec's ``duplicating`` mode every feature appears at every
    level, so a centroid grid would assign each copy to a chunk and the join
    would stack all levels into the same tiles. Such a file is tiled by
    ``tylertoo export-pmtiles``, which reads the levels as written (#1117).

    The column check is case-insensitive because SQL engines resolve
    identifiers that way, which is the same reason the spec forbids a
    case-colliding source column (OVERVIEWS_SPEC 4.1).
    """
    try:
        schema = pq.read_schema(input_path)
    except Exception:  # not a local parquet (remote URL, say) -- nothing to check
        return
    names = {n.lower() for n in schema.names}
    meta = schema.metadata or {}
    has_key = any(k.decode(errors="replace") == "geo:overviews" for k in meta)
    if "level" in names or has_key:
        raise ValueError(
            f"{input_path} looks like an overview GeoParquet (levelled rows). "
            "--chunks would tile every level into the same tiles. Tile it with "
            "`tylertoo export-pmtiles`, or chunk the single-level source instead."
        )


def _build_gpio_commands(
    input_path: str,
    bbox: str | None,
    where: str | None,
    include_cols: str | None,
    precision: int,
    verbose: bool,
    profile: str | None,
    src_crs: str | None,
    repair_geometry: bool = True,
) -> list[list[str]]:
    """
    Build the gpio command(s) for GeoJSON conversion.

    Returns a list of commands to be piped together.

    The chain always ends with ``gpio convert geojson``, which repairs invalid
    geometry by default — so the geometry tippecanoe receives is valid (issue
    #506). When ``repair_geometry`` is False we propagate ``--no-repair-geometry``
    to that final step so opt-out reaches the pipeline.
    """
    gpio_exe = _get_gpio_executable()

    needs_reproject = src_crs is not None
    needs_extract = bbox or where or include_cols

    if needs_reproject or needs_extract:
        commands: list[list[str]] = []

        if needs_reproject:
            assert src_crs is not None  # Type narrowing for mypy
            reproject_cmd = [
                gpio_exe,
                "convert",
                "reproject",
                input_path,
                "-",
                "--dst-crs",
                "EPSG:4326",
                "--src-crs",
                src_crs,
            ]
            if verbose:
                reproject_cmd.append("--verbose")
            if profile:
                reproject_cmd.extend(["--profile", profile])
            commands.append(reproject_cmd)
            next_input = "-"
        else:
            next_input = input_path

        if needs_extract:
            extract_cmd = [gpio_exe, "extract", "geoparquet", next_input]

            if bbox:
                extract_cmd.extend(["--bbox", bbox])
            if where:
                extract_cmd.extend(["--where", where])
            if include_cols:
                extract_cmd.extend(["--include-cols", include_cols])
            if verbose:
                extract_cmd.append("--verbose")
            if profile and not needs_reproject:
                extract_cmd.extend(["--profile", profile])

            commands.append(extract_cmd)
            next_input = "-"

        convert_cmd = [gpio_exe, "convert", "geojson", next_input, "--precision", str(precision)]

        if verbose:
            convert_cmd.append("--verbose")
        if profile:
            convert_cmd.extend(["--profile", profile])
        if not repair_geometry:
            convert_cmd.append("--no-repair-geometry")

        commands.append(convert_cmd)

        return commands

    convert_cmd = [gpio_exe, "convert", "geojson", input_path, "--precision", str(precision)]

    if verbose:
        convert_cmd.append("--verbose")
    if profile:
        convert_cmd.extend(["--profile", profile])
    if not repair_geometry:
        convert_cmd.append("--no-repair-geometry")

    return [convert_cmd]


def _add_layer_info(geojson_stream: IO[str], layer_by_column: str) -> Iterator[str]:
    """
    Read newline-delimited GeoJSON features from a stream and re-emit them
    with a `tippecanoe.layer` property injected on each feature.

    gpio outputs one bare Feature JSON object per line (not a FeatureCollection).
    Tippecanoe accepts this format directly when passed via stdin.
    """

    for line in geojson_stream:
        line = line.strip()
        if not line:
            continue

        feature = json.loads(line)
        props = feature.get("properties") or {}
        raw_value = props.get(layer_by_column)

        # fall back to _unknown if layer_by_column is not found
        if raw_value is None or str(raw_value).strip() == "":
            layer_name = "_unknown"
        else:
            layer_name = str(raw_value).strip()

        feature["tippecanoe"] = {"layer": layer_name}
        yield json.dumps(feature, separators=(",", ":")) + "\n"


def _build_tippecanoe_command(
    output_path: str,
    layer: str | None,
    min_zoom: int | None,
    max_zoom: int | None,
    verbose: bool,
    attribution: str | None = None,
    layer_by_column: str | None = None,
    simplify_only_low_zooms: bool = True,
    no_simplification_of_shared_nodes: bool = True,
    no_tile_size_limit: bool = True,
    drop_densest_as_needed: bool = True,
    maximum_tile_bytes: int | None = None,
    force: bool = False,
    temporary_directory: str | None = None,
) -> list[str]:
    """Build the tippecanoe command with production-quality settings.

    The four production-quality tippecanoe flags are individually
    toggleable; their defaults reproduce the historical behaviour. When
    ``maximum_tile_bytes`` is set it takes precedence over
    ``no_tile_size_limit`` — the two are contradictory, and an explicit
    cap is what gives ``--drop-densest-as-needed`` a limit to drop
    features against.

    ``temporary_directory`` becomes tippecanoe's ``-t``; callers resolve it
    with :func:`resolve_scratch_directory` first (tippecanoe ignores
    ``TMPDIR``, #1115).
    """
    cmd = ["tippecanoe", "-P", "-o", output_path]

    if layer_by_column:
        # Let tippecanoe read layer names from the `tippecanoe.layer` property
        # on each feature — do NOT pass -l at all.
        pass
    elif layer:
        cmd.extend(["-l", layer])
    else:
        layer_name = Path(output_path).stem
        cmd.extend(["-l", layer_name])

    if attribution is None:
        attribution = '<a href="https://geoparquet.io/" target="_blank">geoparquet-io</a>'
    cmd.append(f"--attribution={attribution}")

    if min_zoom is not None and max_zoom is not None:
        cmd.extend(["-Z", str(min_zoom), "-z", str(max_zoom)])
    elif min_zoom is not None:
        cmd.extend(["-Z", str(min_zoom), "-zg"])
    elif max_zoom is not None:
        cmd.extend(["-z", str(max_zoom)])
    else:
        cmd.append("-zg")

    if simplify_only_low_zooms:
        cmd.append("--simplify-only-low-zooms")
    if no_simplification_of_shared_nodes:
        cmd.append("--no-simplification-of-shared-nodes")

    # An explicit byte cap and "no limit" are mutually exclusive; the cap wins
    # so drop-densest has a limit to drop features against.
    if maximum_tile_bytes is not None:
        cmd.append(f"--maximum-tile-bytes={maximum_tile_bytes}")
    elif no_tile_size_limit:
        cmd.append("--no-tile-size-limit")

    if drop_densest_as_needed:
        cmd.append("--drop-densest-as-needed")

    if force:
        cmd.append("--force")

    if temporary_directory:
        cmd.extend(["-t", temporary_directory])

    if verbose:
        cmd.append("--progress-interval=1")

    return cmd


def _format_proc_error(proc: "subprocess.Popen[bytes]", stderr_bytes: bytes) -> str:
    """Build a diagnostic message for a failed pipeline process.

    Picks an informative `cmd_name` — for `python -m geoparquet_io …`
    invocations, surfaces the module + subcommand rather than the
    interpreter path.
    """
    args = proc.args if isinstance(proc.args, list) else [str(proc.args)]
    cmd_name = "command"
    if args:
        if "-m" in args:
            m_idx = args.index("-m")
            tail = args[m_idx + 1 : m_idx + 4]
            if tail:
                cmd_name = " ".join(tail)
        else:
            binary = Path(args[0]).name
            rest = next((a for a in args[1:] if not a.startswith("-")), "")
            cmd_name = f"{binary} {rest}".strip() or binary
    stderr_text = stderr_bytes.decode(errors="replace").strip()
    msg = f"{cmd_name} failed with exit code {proc.returncode}"
    if stderr_text:
        msg = f"{msg}\nstderr:\n{stderr_text}"
    return msg


def _log_pipeline(
    gpio_commands: list[list[str]],
    tippecanoe_cmd: list[str],
    layer_by_column: str | None,
) -> None:
    """Emit a debug line describing the pipeline that is about to run."""
    cmd_str = " | ".join(" ".join(cmd) for cmd in gpio_commands)
    debug(f"Running: {cmd_str} | {' '.join(tippecanoe_cmd)}")
    if layer_by_column:
        debug(f"Adding layer metadata into PMTiles from column '{layer_by_column}'")


def _spawn_gpio_chain(
    gpio_commands: list[list[str]],
    verbose: bool,
    env: dict[str, str] | None = None,
) -> list["subprocess.Popen"]:
    """Spawn the gpio commands as a connected stdin→stdout chain.

    Cleans up any already-spawned processes if a later spawn fails, so the
    caller never leaks subprocesses on a partial chain. ``env`` is handed to
    every child (see :func:`scratch_env`).
    """
    processes: list[subprocess.Popen] = []
    try:
        for i, cmd in enumerate(gpio_commands):
            stdin_source = processes[-1].stdout if processes else None

            proc = subprocess.Popen(
                cmd,
                stdin=stdin_source,
                stdout=subprocess.PIPE,
                stderr=None if verbose else subprocess.PIPE,
                env=env,
            )
            processes.append(proc)

            if i > 0 and processes[-2].stdout:
                processes[-2].stdout.close()
    except Exception:
        for proc in processes:
            if proc.poll() is None:
                proc.terminate()
        raise

    return processes


def _collect_upstream_errors(procs: list["subprocess.Popen"]) -> list[str]:
    """Drain stderr and wait for each gpio process, returning failure messages.

    Stderr must be drained before waiting to avoid a deadlock on a full pipe,
    and we drain every process so its stderr is the real diagnostic when
    tippecanoe exits non-zero on truncated input.
    """
    errors: list[str] = []
    for proc in procs:
        stderr_bytes = b""
        if proc.stderr:
            stderr_bytes = proc.stderr.read()
            proc.stderr.close()
        proc.wait()
        if proc.returncode != 0:
            errors.append(_format_proc_error(proc, stderr_bytes))
    return errors


def _pump_features(
    geojson_stream: IO[str],
    layer_by_column: str,
    tippecanoe_proc: "subprocess.Popen[str]",
) -> bool:
    """Inject layer metadata into each feature and stream it into tippecanoe.

    Returns True if at least one feature was written.
    """
    assert tippecanoe_proc.stdin is not None  # narrowed by caller
    wrote_any = False
    for line in _add_layer_info(geojson_stream, layer_by_column):
        # Detect if tippecanoe already exited (e.g., file exists, bad args, etc.)
        if tippecanoe_proc.poll() is not None:
            raise RuntimeError(f"tippecanoe failed with code {tippecanoe_proc.returncode}")

        # tippecanoe can exit between the poll() above and the write below,
        # closing the pipe out from under us. Convert that BrokenPipeError into
        # the same RuntimeError we raise above so subprocess failures surface
        # consistently — but only if the process really has exited; otherwise
        # the broken pipe is unexpected and should propagate as-is.
        try:
            tippecanoe_proc.stdin.write(line)
        except BrokenPipeError:
            if tippecanoe_proc.poll() is not None:
                raise RuntimeError(
                    f"tippecanoe failed with code {tippecanoe_proc.returncode}"
                ) from None
            raise
        wrote_any = True
    return wrote_any


def _run_with_layer_injection(
    processes: list["subprocess.Popen"],
    tippecanoe_cmd: list[str],
    layer_by_column: str,
) -> None:
    """Stream gpio output through layer injection into tippecanoe."""
    last_proc = processes[-1]

    tippecanoe_proc: subprocess.Popen[str] = subprocess.Popen(
        tippecanoe_cmd,
        stdin=subprocess.PIPE,
        stdout=None,
        stderr=None,
        text=True,  # work with strings instead of bytes
    )

    if last_proc.stdout is None:
        raise RuntimeError("last process has no stdout")
    if tippecanoe_proc.stdin is None:
        raise RuntimeError("tippecanoe has no stdin")

    geojson_stream = io.TextIOWrapper(last_proc.stdout, encoding="utf-8")

    try:
        wrote_any = _pump_features(geojson_stream, layer_by_column, tippecanoe_proc)

        # Signal EOF to tippecanoe, then drain the gpio chain.
        tippecanoe_proc.stdin.close()
        upstream_errors = _collect_upstream_errors(processes)

        if upstream_errors:
            tippecanoe_proc.wait()
            raise RuntimeError("\n\n".join(upstream_errors))

        if not wrote_any:
            tippecanoe_proc.wait()
            raise RuntimeError(
                "gpio pipeline produced no output — check input path, "
                "filters, and that the column exists in the file"
            )

        tippecanoe_proc.wait()

        if tippecanoe_proc.returncode != 0:
            raise RuntimeError(f"tippecanoe failed with exit code {tippecanoe_proc.returncode}")

    except Exception:
        # Ensure processes are cleaned up on failure
        if tippecanoe_proc.poll() is None:
            tippecanoe_proc.terminate()
        if last_proc.poll() is None:
            last_proc.terminate()
        raise


def _run_simple(
    processes: list["subprocess.Popen"],
    tippecanoe_cmd: list[str],
    verbose: bool,
) -> None:
    """Pipe gpio output straight into tippecanoe (no layer injection)."""
    tippecanoe_proc = subprocess.Popen(
        tippecanoe_cmd,
        stdin=processes[-1].stdout if processes else None,
        stdout=None if verbose else subprocess.PIPE,
        stderr=None,
    )
    processes.append(tippecanoe_proc)
    if len(processes) > 1 and processes[-2].stdout:
        processes[-2].stdout.close()

    tippecanoe_proc.communicate()

    # Drain stderr and wait for upstream procs FIRST. If an upstream gpio
    # process crashed, tippecanoe almost always exits non-zero too (read
    # truncated input), and the upstream stderr is the real diagnostic —
    # we must not short-circuit on tippecanoe's exit code before collecting it.
    upstream_gpio_errors = _collect_upstream_errors(processes[:-1])

    if tippecanoe_proc.returncode != 0:
        msg = f"tippecanoe failed with exit code {tippecanoe_proc.returncode}"
        if upstream_gpio_errors:
            msg = f"{msg}\nUpstream errors:\n" + "\n\n".join(upstream_gpio_errors)
        raise RuntimeError(msg)

    if upstream_gpio_errors:
        raise RuntimeError("\n\n".join(upstream_gpio_errors))


def _run_pipeline(
    gpio_commands: list[list[str]],
    tippecanoe_cmd: list[str],
    verbose: bool,
    layer_by_column: str | None = None,
    scratch: str | None = None,
) -> None:
    """Execute the gpio to tippecanoe pipeline.

    If layer_by_column is given, the gpio output is intercepted and each
    feature is annotated with a `tippecanoe.layer` value derived from
    that column before being forwarded to tippecanoe. ``scratch`` is the
    run's resolved temp directory; the gpio children inherit it as their
    ``TMPDIR`` so their spill lands beside tippecanoe's.
    """
    if verbose:
        _log_pipeline(gpio_commands, tippecanoe_cmd, layer_by_column)

    processes = _spawn_gpio_chain(gpio_commands, verbose, scratch_env(scratch) if scratch else None)

    try:
        if layer_by_column:
            _run_with_layer_injection(processes, tippecanoe_cmd, layer_by_column)
        else:
            _run_simple(processes, tippecanoe_cmd, verbose)
    except KeyboardInterrupt:
        for proc in processes:
            proc.terminate()
        raise
    except Exception:
        for proc in processes:
            if proc.poll() is None:
                proc.terminate()
        raise


def _count_chunk_features(input_path: str, where: str) -> int:
    """Rows a chunk's predicate selects.

    Checked before tiling rather than inferred from a tippecanoe failure: a
    chunk over ocean is normal and must be skipped, but a chunk that fails for
    any other reason must still surface. Counting separates the two (#1116).
    """
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path

    con = get_duckdb_connection()
    try:
        row = con.execute(f"SELECT count(*) FROM {sql_path(input_path)} WHERE {where}").fetchone()
        return int(row[0]) if row else 0
    finally:
        con.close()


def _create_pmtiles_chunked(
    input_path: str,
    output_path: str,
    chunks: str,
    tiling_kwargs: dict,
    *,
    where: str | None,
    attribution: str | None,
    layer: str | None,
    force: bool,
    verbose: bool,
) -> None:
    """Tile a grid of disjoint chunks and tile-join them into one archive.

    tippecanoe's scratch scales with the feature count, not the output: 168M
    polygons reached ~270GB against a ~34GB archive. Tiling a spatial subset
    at a time bounds peak scratch by the chunk (#1116). Parts persist beside
    the output so a failed run resumes instead of restarting -- these runs
    take hours, and losing the last chunk should not cost the first eleven.
    """
    nx, ny = _parse_chunks(chunks)
    _reject_levelled_input(input_path)
    if not _check_tile_join():
        raise TileJoinNotFoundError()

    geometry_column = get_primary_geometry_column(input_path) or "geometry"
    bounds = get_dataset_bounds(input_path, geometry_column=geometry_column, verbose=verbose)
    if bounds is None:
        raise RuntimeError(f"Could not determine bounds of {input_path} to chunk it")

    cells = _chunk_cells(tuple(bounds), nx, ny)
    parts_dir = _parts_dir(output_path)
    os.makedirs(parts_dir, exist_ok=True)

    parts: list[str] = []
    for n, cell in enumerate(cells, start=1):
        part = _part_path(output_path, cell)
        if os.path.exists(part):
            debug(f"chunk {n}/{len(cells)}: reusing {part}")
            parts.append(part)
            continue

        chunk_where = _chunk_where(cell, geometry_column, where)
        if _count_chunk_features(input_path, chunk_where) == 0:
            debug(f"chunk {n}/{len(cells)}: empty, skipping")
            continue

        debug(f"chunk {n}/{len(cells)}: tiling {part}")
        create_pmtiles_from_geoparquet(
            input_path,
            part,
            where=chunk_where,
            attribution=attribution,
            layer=layer or Path(output_path).stem,
            force=True,
            verbose=verbose,
            **tiling_kwargs,
        )
        parts.append(part)

    if not parts:
        raise RuntimeError(f"No chunk of {input_path} contained any features; nothing to tile")

    _run_tile_join(
        _build_tile_join_command(
            output_path,
            parts,
            name=Path(output_path).stem,
            attribution=attribution,
            force=force,
        ),
        verbose,
    )
    shutil.rmtree(parts_dir, ignore_errors=True)
    success(f"Created {output_path} from {len(parts)} chunk(s)")


def create_pmtiles_from_geoparquet(
    input_path: str,
    output_path: str,
    *,
    layer: str | None = None,
    min_zoom: int | None = None,
    max_zoom: int | None = None,
    bbox: str | None = None,
    where: str | None = None,
    include_cols: str | None = None,
    precision: int = 6,
    verbose: bool = False,
    profile: str | None = None,
    src_crs: str | None = None,
    attribution: str | None = None,
    layer_by_column: str | None = None,
    simplify_only_low_zooms: bool = True,
    no_simplification_of_shared_nodes: bool = True,
    no_tile_size_limit: bool = True,
    drop_densest_as_needed: bool = True,
    maximum_tile_bytes: int | None = None,
    force: bool = False,
    repair_geometry: bool = True,
    temporary_directory: str | None = None,
    chunks: str | None = None,
) -> None:
    """
    Create PMTiles using gpio streaming + tippecanoe subprocess.

    Orchestrates subprocesses to:
    1. Reproject if needed (gpio convert reproject)
    2. Filter/transform if needed (gpio extract)
    3. Stream GeoJSON from GeoParquet (gpio convert geojson)
    4. Generate PMTiles using tippecanoe

    Args:
        input_path: Path to input GeoParquet file
        output_path: Path for output PMTiles file
        layer: Layer name in PMTiles (defaults to output filename)
        min_zoom: Minimum zoom level (optional)
        max_zoom: Maximum zoom level (optional, auto-detected if not set)
        bbox: Bounding box filter as "minx,miny,maxx,maxy"
        where: SQL WHERE clause for filtering
        include_cols: Comma-separated list of columns to include
        precision: Coordinate decimal precision (default: 6)
        verbose: Enable verbose output
        profile: AWS profile name for S3 files
        src_crs: Source CRS for reprojection to WGS84
        attribution: Attribution HTML for the tiles
        layer_by_column: Split tiles into layers grouped by values of this column
        simplify_only_low_zooms: Pass --simplify-only-low-zooms (default: True)
        no_simplification_of_shared_nodes: Pass --no-simplification-of-shared-nodes (default: True)
        no_tile_size_limit: Pass --no-tile-size-limit, removing the tile size
            cap (default: True). Set False to respect tippecanoe's size limit so
            that drop_densest_as_needed actually drops features on dense data.
        drop_densest_as_needed: Pass --drop-densest-as-needed (default: True).
            Only drops features to bring a tile back under the size limit, so it
            has no effect while no_tile_size_limit is True.
        maximum_tile_bytes: Set an explicit per-tile byte cap via
            --maximum-tile-bytes. Takes precedence over no_tile_size_limit.
        force: Pass --force to overwrite the output file if it already exists.
        chunks: Tile an ``NxM`` grid of disjoint chunks and tile-join them,
            bounding tippecanoe's scratch by the chunk rather than the dataset
            (#1116). Features are assigned by centroid, so none is tiled twice.
        repair_geometry: Repair invalid geometry with ST_MakeValid (default: True).
            Prevents tippecanoe TopologyExceptions on self-intersecting polygons.
            Set False to pass geometry through unrepaired.
        temporary_directory: Scratch for this run -- tippecanoe's ``-t`` and
            the gpio children's temp files. Must exist; defaults to the OS
            temp directory (``TMPDIR``). Several times the input in size.

    Raises:
        InvalidParameterError: If include_cols carries a blank entry or
            temporary_directory is not a writable directory
        TippecanoeNotFoundError: If tippecanoe is not in PATH
        ValueError: If paths contain shell metacharacters or the user supplied an invalid layer_by_column
        RuntimeError: If any subprocess fails
    """
    _validate_path(input_path)
    _validate_path(output_path)
    if layer and layer_by_column:
        raise ValueError(
            "When creating pmtiles, you cannot specify both 'layer' which defines one layer name "
            "and 'layer_by_column' which defines multiple layer names based on the values of a column"
        )
    # --include-cols is forwarded verbatim into a `gpio extract` subprocess
    # argv, so a blank entry used to be rejected one process away from the user
    # (#980). Checked here, with the other usage errors and before the
    # tippecanoe probe, so a bad option is not reported as a missing binary.
    cols = split_column_list(include_cols, "--include-cols")
    scratch = resolve_scratch_directory(temporary_directory)

    if not _check_tippecanoe():
        raise TippecanoeNotFoundError()

    # If layer_by_column is set, ensure that the group by column is always included
    include_cols_with_layer_by_column: str | None
    if layer_by_column and cols:
        if layer_by_column not in cols:
            cols = [*cols, layer_by_column]
        include_cols_with_layer_by_column = ",".join(cols)
    else:
        include_cols_with_layer_by_column = include_cols

    if chunks:
        _create_pmtiles_chunked(
            input_path,
            output_path,
            chunks,
            {
                "min_zoom": min_zoom,
                "max_zoom": max_zoom,
                "include_cols": include_cols,
                "precision": precision,
                "profile": profile,
                "src_crs": src_crs,
                "layer_by_column": layer_by_column,
                "simplify_only_low_zooms": simplify_only_low_zooms,
                "no_simplification_of_shared_nodes": no_simplification_of_shared_nodes,
                "no_tile_size_limit": no_tile_size_limit,
                "drop_densest_as_needed": drop_densest_as_needed,
                "maximum_tile_bytes": maximum_tile_bytes,
                "repair_geometry": repair_geometry,
                "temporary_directory": temporary_directory,
            },
            where=where,
            attribution=attribution,
            layer=layer,
            force=force,
            verbose=verbose,
        )
        return

    gpio_commands = _build_gpio_commands(
        input_path,
        bbox,
        where,
        include_cols_with_layer_by_column,
        precision,
        verbose,
        profile,
        src_crs,
        repair_geometry=repair_geometry,
    )
    tippecanoe_cmd = _build_tippecanoe_command(
        output_path,
        layer,
        min_zoom,
        max_zoom,
        verbose,
        attribution,
        layer_by_column,
        simplify_only_low_zooms=simplify_only_low_zooms,
        no_simplification_of_shared_nodes=no_simplification_of_shared_nodes,
        no_tile_size_limit=no_tile_size_limit,
        drop_densest_as_needed=drop_densest_as_needed,
        maximum_tile_bytes=maximum_tile_bytes,
        force=force,
        temporary_directory=scratch,
    )

    _run_pipeline(gpio_commands, tippecanoe_cmd, verbose, layer_by_column, scratch)

    if verbose:
        success(f"Created {output_path}")
