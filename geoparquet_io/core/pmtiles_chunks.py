"""Chunked tiling: split a large file into a centroid grid, tile each part, join.

tippecanoe's scratch scales with the feature count, not the output: 168M
polygons reached ~270GB against a ~34GB archive (#1116). Tiling a spatial
subset at a time bounds peak scratch by the chunk.

The split is one DuckDB ``COPY ... PARTITION_BY`` pass over the input, the
same single-scan write ``gpio partition`` uses, into ``<output>.parts/`` as one
GeoParquet file per non-empty cell. Each chunk is then tiled through the
ordinary streaming path (``gpio convert geojson <chunk> | tippecanoe``) and the
per-chunk archives are merged with tile-join. That shape is what makes the
feature deliver what it promises:

* Memory stays bounded. Filtering the input with ``--where`` per chunk would
  route every chunk through ``gpio extract``, whose stream output
  materialises the whole selection in Arrow -- several GB per chunk on the
  motivating input -- so tippecanoe's scratch would have been traded for
  Python heap.
* The input is scanned once, not once per cell for a count, once for the
  repair pass and once for the stream.
* Empty cells (ocean) are simply absent, and every row lands in exactly one
  cell: the cell index is clamped to the grid, so a centroid that a float32
  bbox covering or a stale footer places just outside the bounds still tiles.
* The parts directory is the resume state. Chunk archives are tiled to a
  temporary name and renamed on tippecanoe's success, so a file with the
  final name is complete (tippecanoe writes its SQLite scratch at the output
  path from the first second, so existence alone proves nothing). A manifest
  records the input, grid, filters and tiling flags; a re-run with different
  options is refused rather than joining a stale grid into the archive.

Not composed with ``--bbox``: filter with ``gpio extract --bbox`` first, then
chunk the result. ``--max-zoom`` is required: tippecanoe's ``-zg`` would guess
a zoom per chunk and the sparse chunks would have no tiles above their own
guess while the joined header claimed the dense chunk's maximum.
"""

from __future__ import annotations

import glob
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

from geoparquet_io.core.common import get_dataset_bounds, get_parquet_metadata
from geoparquet_io.core.duckdb_utils import (
    get_duckdb_connection,
    quote_identifier,
    spill_directory,
    sql_path,
)
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.file_utils import resolve_file_url
from geoparquet_io.core.geo_metadata import is_levelled_overview, sanitized_carried_geo
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import debug, success, warn
from geoparquet_io.core.parquet_writer import resolve_output_geoparquet_version
from geoparquet_io.core.partition.staging import (
    PartitionWriteOptions,
    finalize_partition_file,
    iter_staging_partitions,
    make_partition_aliases,
    run_partitioned_copy,
)
from geoparquet_io.core.pmtiles import (
    create_pmtiles_from_geoparquet,
    resolve_scratch_directory,
)
from geoparquet_io.core.remote import is_remote_url, needs_httpfs, setup_aws_profile_if_needed
from geoparquet_io.core.tile_join import (
    TileJoinNotFoundError,
    _build_tile_join_command,
    _check_tile_join,
    _run_tile_join,
)

#: A typo (``400x300`` for ``4x3``) would otherwise queue 120,000 tippecanoe
#: runs; nothing legitimate needs more cells than this.
MAX_CHUNK_CELLS = 1024

_MANIFEST = "manifest.json"
_CHUNK_PREFIX = "chunk_"


def parse_chunks(spec: str) -> tuple[int, int]:
    """``"4x3"`` -> ``(4, 3)``. ``auto`` is not offered: choosing a grid means
    modelling tippecanoe's scratch against free space, and an uncalibrated
    guess fills the disk -- the failure chunking exists to prevent."""
    try:
        nx, ny = (int(part) for part in spec.strip().lower().split("x"))
    except ValueError:
        raise InvalidParameterError("chunks", f"expected NxM (e.g. 4x3), got {spec!r}") from None
    if nx < 1 or ny < 1:
        raise InvalidParameterError("chunks", f"needs positive counts, got {spec!r}")
    if nx * ny > MAX_CHUNK_CELLS:
        raise InvalidParameterError(
            "chunks", f"{nx}x{ny} is {nx * ny} cells; the most is {MAX_CHUNK_CELLS}"
        )
    return nx, ny


def chunk_key_sql(
    geometry_column: str, bounds: tuple[float, float, float, float], nx: int, ny: int
) -> str:
    """SQL naming each row's cell, ``chunk_<iy>_<ix>``, by geometry centroid.

    Centroid, not intersection: a polygon straddling a cell edge would be
    tiled by both neighbours and appear twice in the join -- a visible seam
    under translucent fills. The index is clamped to ``[0, n-1]`` so every
    centroid lands in some cell, including one the bounds do not quite cover.
    """
    minx, miny, maxx, maxy = bounds
    col = quote_identifier(geometry_column)

    def axis(coord: str, lo: float, hi: float, n: int) -> str:
        if n == 1 or hi <= lo:
            return "0"
        step = (hi - lo) / n
        cell = f"CAST(FLOOR(({coord} - {lo!r}) / {step!r}) AS INTEGER)"
        return f"LEAST(GREATEST({cell}, 0), {n - 1})"

    ix = axis(f"ST_X(ST_Centroid({col}))", minx, maxx, nx)
    iy = axis(f"ST_Y(ST_Centroid({col}))", miny, maxy, ny)
    return f"'{_CHUNK_PREFIX}' || CAST({iy} AS VARCHAR) || '_' || CAST({ix} AS VARCHAR)"


def _footer_bounds(geo: dict, geometry_column: str) -> tuple[float, float, float, float] | None:
    """The dataset bbox the ``geo`` footer already carries, if it is usable."""
    bbox = geo.get("columns", {}).get(geometry_column, {}).get("bbox")
    if isinstance(bbox, list) and len(bbox) == 4 and all(isinstance(v, int | float) for v in bbox):
        minx, miny, maxx, maxy = (float(v) for v in bbox)
        return (minx, miny, maxx, maxy)
    return None


def _dataset_bounds(
    input_path: str, metadata, geometry_column: str, verbose: bool
) -> tuple[float, float, float, float]:
    """The footer's bbox when it has one (free), else a scan."""
    bounds = _footer_bounds(sanitized_carried_geo(metadata), geometry_column)
    if bounds is not None:
        return bounds
    found = get_dataset_bounds(input_path, geometry_column=geometry_column, verbose=verbose)
    if found is None:
        raise RuntimeError(f"Could not determine bounds of {input_path} to chunk it")
    minx, miny, maxx, maxy = (float(v) for v in found)
    return (minx, miny, maxx, maxy)


def _manifest(
    input_path: str,
    nx: int,
    ny: int,
    *,
    where: str | None,
    include_cols: str | None,
    tiling: dict[str, Any],
) -> dict[str, Any]:
    """What the parts were built from; anything here changing invalidates them."""
    entry: dict[str, Any] = {
        "input": input_path if is_remote_url(input_path) else os.path.abspath(input_path),
        "chunks": f"{nx}x{ny}",
        "where": where,
        "include_cols": include_cols,
        "tiling": {k: v for k, v in tiling.items() if k != "temporary_directory"},
    }
    if not is_remote_url(input_path):
        st = os.stat(input_path)
        entry["input_size"] = st.st_size
        entry["input_mtime_ns"] = st.st_mtime_ns
    return entry


def _prepare_parts_dir(parts_dir: str, manifest: dict[str, Any]) -> bool:
    """Create the parts directory or check an existing one is ours.

    Returns whether the split into chunk files already completed, so a re-run
    goes straight to tiling. Refuses a directory holding parts built with
    other options: joining a 2x2 grid's leftovers into a 3x3 run duplicated
    27 of 42 features silently, exit 0.
    """
    if os.path.lexists(parts_dir):
        if os.path.islink(parts_dir) or not os.path.isdir(parts_dir):
            raise InvalidParameterError(
                "output_path", f"{parts_dir} exists and is not a directory; remove it first"
            )
        path = os.path.join(parts_dir, _MANIFEST)
        if not os.path.isfile(path):
            raise InvalidParameterError(
                "output_path", f"{parts_dir} is not a gpio parts directory; remove it first"
            )
        with open(path, encoding="utf-8") as fh:
            existing = json.load(fh)
        split_complete = bool(existing.pop("split_complete", False))
        if existing != manifest:
            raise InvalidParameterError(
                "chunks",
                f"{parts_dir} holds parts built from a different input, grid, filter or "
                "tiling options. Re-run with the options they were built with, or delete "
                "the directory to start over.",
            )
        return split_complete
    os.makedirs(parts_dir)
    _write_manifest(parts_dir, manifest, split_complete=False)
    return False


def _write_manifest(parts_dir: str, manifest: dict[str, Any], *, split_complete: bool) -> None:
    with open(os.path.join(parts_dir, _MANIFEST), "w", encoding="utf-8") as fh:
        json.dump({**manifest, "split_complete": split_complete}, fh, indent=1)


def _projection_sql(include_cols: str | None, geometry_column: str, layer_by_column: str | None):
    """Columns the chunk files carry: everything, or ``--include-cols`` plus
    the geometry and the layer column, which tiling cannot do without."""
    if not include_cols:
        return "*"
    names = [c.strip() for c in include_cols.split(",") if c.strip()]
    for required in (geometry_column, layer_by_column):
        if required and required not in names:
            names.append(required)
    return ", ".join(quote_identifier(n) for n in names)


def _split_input(
    input_path: str,
    parts_dir: str,
    *,
    geometry_column: str,
    key_sql: str,
    where: str | None,
    projection: str,
    scratch: str,
    verbose: bool,
) -> None:
    """One partitioned COPY of the input into ``<parts_dir>/chunk_<iy>_<ix>.parquet``.

    Staging goes through DuckDB's fast transient format and each cell is then
    rewritten as a proper GeoParquet file carrying the input's metadata (CRS
    included, which ``--src-crs`` tiling of the chunk depends on) -- the same
    two steps ``gpio partition`` takes, for the same reasons.
    """
    input_url = resolve_file_url(input_path, verbose)
    metadata, _ = get_parquet_metadata(input_path, verbose)
    col = quote_identifier(geometry_column)
    predicate = f"{col} IS NOT NULL AND NOT ST_IsEmpty({col})"
    if where:
        # Parenthesised so an OR in the caller's clause cannot swallow the
        # NULL guard.
        predicate = f"{predicate} AND ({where})"

    con = get_duckdb_connection(
        load_httpfs=needs_httpfs(input_path), temp_directory=spill_directory(scratch)
    )
    staging = tempfile.mkdtemp(prefix=".staging_", dir=parts_dir)
    try:
        describe = con.execute(
            f"SELECT {projection} FROM {sql_path(input_url)} LIMIT 0"
        ).description
        alias = make_partition_aliases(1, [d[0] for d in describe])[0]
        select_sql = f"SELECT {projection}, {key_sql} AS {alias} FROM {sql_path(input_url)} WHERE {predicate}"
        run_partitioned_copy(con, select_sql, [alias], staging, verbose)

        options = PartitionWriteOptions(
            geoparquet_version=resolve_output_geoparquet_version(
                None, input_file=input_path, verbose=verbose
            ),
            compression_level=3,  # read once by tippecanoe; not worth level 15
        )
        for values, partition_dir in iter_staging_partitions(staging):
            chunk_file = os.path.join(parts_dir, f"{values[0]}.parquet")
            finalize_partition_file(
                con, partition_dir, chunk_file, metadata, True, verbose, options
            )
            shutil.rmtree(partition_dir, ignore_errors=True)
    finally:
        con.close()
        shutil.rmtree(staging, ignore_errors=True)


def _tile_chunks(
    chunk_files: list[str],
    *,
    layer: str | None,
    tiling: dict[str, Any],
    verbose: bool,
) -> list[str]:
    """Tile each chunk file to its archive, reusing the complete ones.

    The archive is built under a temporary name and renamed on success, so a
    part with the final name is one tippecanoe finished. ``.pmtiles`` stays
    the suffix because tippecanoe picks the output format from it.
    """
    parts: list[str] = []
    for n, chunk in enumerate(chunk_files, start=1):
        part = chunk[: -len(".parquet")] + ".pmtiles"
        if os.path.exists(part):
            debug(f"chunk {n}/{len(chunk_files)}: reusing {part}")
            parts.append(part)
            continue
        building = chunk[: -len(".parquet")] + ".building.pmtiles"
        debug(f"chunk {n}/{len(chunk_files)}: tiling {part}")
        create_pmtiles_from_geoparquet(
            chunk, building, layer=layer, force=True, verbose=verbose, **tiling
        )
        os.replace(building, part)
        parts.append(part)
    return parts


def create_pmtiles_chunked(
    input_path: str,
    output_path: str,
    chunks: str,
    *,
    bbox: str | None,
    where: str | None,
    include_cols: str | None,
    layer: str | None,
    attribution: str | None,
    force: bool,
    verbose: bool,
    profile: str | None,
    tiling: dict[str, Any],
) -> None:
    """Tile ``input_path`` in an ``NxM`` grid of chunks and join them into ``output_path``.

    ``tiling`` holds the per-chunk ``create_pmtiles_from_geoparquet`` options
    (zooms, simplification, size cap, repair, src_crs, layer_by_column,
    precision, temporary_directory). See the module docstring for why the
    input is split on disk first and what makes a part resumable.
    """
    nx, ny = parse_chunks(chunks)
    layer_by_column = tiling.get("layer_by_column")
    if bbox:
        raise InvalidParameterError(
            "bbox",
            "cannot be combined with --chunks; filter first with "
            "`gpio extract --bbox`, then chunk the result",
        )
    if tiling.get("max_zoom") is None:
        raise InvalidParameterError(
            "max_zoom",
            "--chunks needs --max-zoom: tippecanoe would guess a zoom per chunk and the "
            "sparse chunks would have no tiles above their own guess",
        )
    if os.path.exists(output_path) and not force:
        raise InvalidParameterError(
            "output_path", f"output file already exists: {output_path}. Use --force to overwrite."
        )
    if not _check_tile_join():
        raise TileJoinNotFoundError()
    scratch = resolve_scratch_directory(tiling.get("temporary_directory"))
    tiling = {**tiling, "temporary_directory": scratch}

    setup_aws_profile_if_needed(profile, input_path)
    metadata, _ = get_parquet_metadata(input_path, verbose)
    if is_levelled_overview(metadata):
        raise InvalidParameterError(
            "input_path",
            f"{input_path} is a levelled overview GeoParquet; --chunks would tile every "
            "level into the same tiles. Tile it with `tylertoo export-pmtiles`, or chunk "
            "the single-level source instead.",
        )
    geometry_column = find_primary_geometry_column(input_path, verbose)
    bounds = _dataset_bounds(input_path, metadata, geometry_column, verbose)
    debug(f"Dataset bounds: {bounds}; chunking {nx}x{ny}")

    manifest = _manifest(input_path, nx, ny, where=where, include_cols=include_cols, tiling=tiling)
    parts_dir = f"{output_path}.parts"
    if not _prepare_parts_dir(parts_dir, manifest):
        for stale in glob.glob(os.path.join(parts_dir, f"{_CHUNK_PREFIX}*")):
            os.remove(stale)
        _split_input(
            input_path,
            parts_dir,
            geometry_column=geometry_column,
            key_sql=chunk_key_sql(geometry_column, bounds, nx, ny),
            where=where,
            projection=_projection_sql(include_cols, geometry_column, layer_by_column),
            scratch=scratch,
            verbose=verbose,
        )
        _write_manifest(parts_dir, manifest, split_complete=True)

    chunk_files = sorted(glob.glob(os.path.join(parts_dir, f"{_CHUNK_PREFIX}*.parquet")))
    if not chunk_files:
        shutil.rmtree(parts_dir, ignore_errors=True)
        raise RuntimeError(f"No chunk of {input_path} contained any features; nothing to tile")

    stem = Path(output_path).stem
    parts = _tile_chunks(
        chunk_files,
        layer=None if layer_by_column else (layer or stem),
        tiling=tiling,
        verbose=verbose,
    )

    # Joined under the parts directory and moved into place, so a failed join
    # leaves no partial archive at the output path and the next run resumes.
    joined = os.path.join(parts_dir, "joined.pmtiles")
    _run_tile_join(
        _build_tile_join_command(joined, parts, name=stem, attribution=attribution, force=True),
        verbose,
    )
    os.replace(joined, output_path)
    shutil.rmtree(parts_dir, ignore_errors=True)
    if os.path.exists(parts_dir):
        warn(f"Could not remove {parts_dir}; it is safe to delete")
    if verbose:
        success(f"Created {output_path} from {len(parts)} chunk(s)")
