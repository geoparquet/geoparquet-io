"""
Base classes and types for write strategy implementations.

This module provides the Strategy Pattern foundation for GeoParquet write operations.
Each strategy encapsulates a different approach to writing GeoParquet files with
varying memory and performance characteristics.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from abc import ABC, abstractmethod
from collections.abc import Iterable
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

from geoparquet_io.core.crs_utils import (
    PARQUET_UNKNOWN_CRS,
    entry_crs_is_unknown,
    is_default_crs,
    is_unknown_type_crs,
)
from geoparquet_io.core.duckdb_utils import (
    describe_query,
    duckdb_type_is_geometry,
    quote_identifier,
    sql_literal,
)
from geoparquet_io.core.geo_metadata import (
    GEOPARQUET_VERSIONS,
    NATIVE_GEOMETRY_VERSIONS,
    read_raw_geo,
)
from geoparquet_io.core.parquet_writer import carried_geoparquet_version

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa


def resolve_geometry_columns(
    geometry_column: str,
    geometry_info: dict | None = None,
    geo_meta: dict | None = None,
) -> set[str]:
    """Every column the write must treat as geometry, not just the primary.

    A file may declare more than one geometry column: `geo["columns"]` carries a
    `primary_column` plus secondaries, and gpio threads those through
    `geometry_info["secondary"]`. Validation applies the same per-version
    requirements to *every* column in `geo["columns"]`, so every strategy has to
    make the same per-version carrier decision for all of them (#706).

    The secondary names must be taken from `geometry_info` and not recovered from
    `geo_meta` alone: `parquet-geo-only` writes no geo metadata at all, so by
    schema-build time there is nothing left to name them by.
    """
    columns = {geometry_column}
    if geometry_info:
        columns.update(geometry_info.get("secondary") or ())
        columns.update(geometry_info.get("metadata") or {})
    if geo_meta:
        columns.update(geo_meta.get("columns") or {})
    columns.discard(None)
    return columns


def native_geometry_crs(
    geoparquet_version: str,
    geo_meta: dict | None,
    geometry_column: str,
    geometry_info: dict | None = None,
) -> dict[str, dict | None]:
    """Geometry columns needing a native Parquet GEOMETRY type, each with its CRS.

    Empty below 2.0, where geometry is plain BYTE_ARRAY WKB and the CRS lives in
    the ``geo`` block alone.

    EVERY declared geometry column, not just the primary: 2.0 validation applies
    the same requirement to each column in ``geo["columns"]``, and under
    parquet-geo-only -- which writes no ``geo`` block at all -- the logical type
    is a column's only geometry identity (#706).

    Each CRS is read back out of the metadata just built for the file, so the
    type and the block cannot disagree (``v2_crs_consistency``). A column the
    block gives no ``crs`` is the spec default, and carries none in its type. A
    column whose ``crs`` is null (unknown) is typed ``srid:0`` (#1276).

    ``geo_meta`` is therefore the *output's* block, after ``apply_output_crs``
    has resolved a requested ``input_crs`` against what the source declared --
    never the source's own. Callers that write no block (parquet-geo-only) still
    build one and key the types off it, because the alternative is what #848 was:
    the primary column typed from an ``input_crs`` that is ``None`` on every
    write except a reprojection, and so left bare beside a block declaring
    EPSG:3857.

    One definition, shared by every strategy: the streaming writer's output
    schema, the disk-rewrite writer's, and the in-memory path in
    ``arrow_geo_metadata._apply_geoparquet_metadata``.
    """
    if geoparquet_version not in NATIVE_GEOMETRY_VERSIONS:
        return {}

    column_metadata = (geo_meta or {}).get("columns") or {}
    return {
        column: type_crs_for(column_metadata.get(column))
        for column in resolve_geometry_columns(geometry_column, geometry_info, geo_meta)
    }


def type_crs_for(col_meta: dict | None) -> dict | str | None:
    """The type CRS for one column's block entry: ``srid:0`` for an unknown CRS.

    Unknown is an explicit null or an empty value (#1267). The type's unset CRS
    means OGC:CRS84, so a column the block calls unknown would read as lon/lat
    if its type carried nothing (#1276). Every write path types its geometry
    columns through this, the primary and each secondary alike.
    """
    if entry_crs_is_unknown(col_meta):
        return PARQUET_UNKNOWN_CRS
    return (col_meta or {}).get("crs")


def secondary_type_crs(geo_columns: dict | None, primary: str | None) -> dict[str, dict | str]:
    """The type CRS each secondary needs set: unknown or a named non-default CRS."""
    wanted = {}
    for name, col_meta in (geo_columns or {}).items():
        if name == primary or not isinstance(col_meta, dict):
            continue
        crs = type_crs_for(col_meta)
        if is_unknown_type_crs(crs) or (crs and not is_default_crs(crs)):
            wanted[name] = crs
    return wanted


def has_unknown_secondary(geo_columns: dict | None, primary: str | None) -> bool:
    """Whether a secondary's CRS is unknown, so it is typed ``srid:0``."""
    return any(is_unknown_type_crs(c) for c in secondary_type_crs(geo_columns, primary).values())


def wrap_query_with_secondary_crs(
    con, query: str, geo_columns: dict | None, primary: str | None, geoparquet_version: str | None
) -> str:
    """Type each secondary GEOMETRY column with its own CRS in a 2.0 or parquet-geo-only output.

    DuckDB reads a secondary's CRS from no ``geo`` entry: a GeoParquet input
    hands its secondaries over as bare GEOMETRY, which a 2.0 type then calls
    OGC:CRS84. The primary gets its CRS from ``_wrap_query_with_crs``, and this does
    the same for the rest, ``srid:0`` for an unknown one (#1277 review). Only
    columns the query already types GEOMETRY are touched, as ``ST_SetCRS``
    takes nothing else.
    """
    if geoparquet_version not in NATIVE_GEOMETRY_VERSIONS:
        return query
    wanted = secondary_type_crs(geo_columns, primary)
    if not wanted:
        return query
    native = {name for name, type_ in describe_query(con, query) if duckdb_type_is_geometry(type_)}
    replacements = []
    for name in sorted(set(wanted) & native):
        crs = wanted[name]
        literal = crs if isinstance(crs, str) else json.dumps(crs)
        ident = quote_identifier(name)
        replacements.append(f"ST_SetCRS({ident}, {sql_literal(literal)}) AS {ident}")
    if not replacements:
        return query
    return f"SELECT * REPLACE ({', '.join(replacements)}) FROM ({query})"


def geoarrow_input_encoding(
    geometry_info: dict | None, geometry_column: str, geoparquet_version: str
) -> str | None:
    """The primary's input encoding a 1.1-geoarrow write preserves, else None.

    1.1-geoarrow keeps the input's native GeoArrow encoding; every other v1.x
    version converts geometry to a WKB blob, so the composer's own rule (the
    carried encoding, or WKB) decides.
    """
    if not geometry_info or geoparquet_version != "1.1-geoarrow":
        return None
    return geometry_info.get("metadata", {}).get(geometry_column, {}).get("encoding")


class WriteStrategy(str, Enum):
    """Available write strategies for GeoParquet metadata writes."""

    ARROW_MEMORY = "in-memory"
    ARROW_STREAMING = "streaming"
    DUCKDB_KV = "duckdb-kv"
    DISK_REWRITE = "disk-rewrite"


@contextmanager
def atomic_write(
    output_path: str,
    suffix: str = ".parquet",
    *,
    sidecar_suffixes: Iterable[str] | None = None,
):
    """
    Context manager for atomic file writes with cleanup.

    Stages in a temp directory beside the output, yields a path inside it, and
    on success moves the result over ``output_path``. On any failure, including
    a ``KeyboardInterrupt``, the whole directory is removed, so a partial file
    (and anything the writer left next to it) never reaches the destination,
    and a file that already existed there is never deleted.

    The staged file is named after the output's stem plus ``suffix``, so a
    writer that derives a name from the file (a GDAL layer) sees the output's.

    ``sidecar_suffixes`` stages a set of files that share the output's stem
    (a Shapefile's ``.shp``/``.shx``/``.dbf``/``.prj``/...). Every file the
    writer leaves in the staging directory is published beside the output, and
    an old file named ``<stem><suffix>`` for a listed suffix that the new set
    lacks is removed, so a stale ``.cpg`` or ``.sbn`` cannot outlive an
    overwrite. A multi-file publish is atomic per file, not as a set: a crash
    between two moves leaves a mix of old and new files.

    Args:
        output_path: Final destination path for the file
        suffix: File suffix for the staged file (default: .parquet)
        sidecar_suffixes: Suffixes of sidecar files that belong to the output

    Yields:
        str: Path to the staged file to write to

    Raises:
        Any exception from the write operation (after cleanup)
    """
    final = Path(output_path)
    stage_dir = Path(tempfile.mkdtemp(prefix=".gpio-stage-", dir=final.parent))
    staged = stage_dir / f"{final.stem}{suffix}"
    try:
        yield str(staged)
        if sidecar_suffixes is not None:
            published = {part.name for part in stage_dir.iterdir()}
            for sidecar in sorted(stage_dir.iterdir()):
                if sidecar != staged:
                    os.replace(sidecar, final.parent / sidecar.name)
            for stale in sidecar_suffixes:
                old = final.parent / f"{final.stem}{stale}"
                if old.name not in published and old != final:
                    old.unlink(missing_ok=True)
        os.replace(staged, final)
    finally:
        shutil.rmtree(stage_dir, ignore_errors=True)


#: The :meth:`BaseWriteStrategy.write_from_table` arguments a query write takes
#: unchanged.
_TABLE_WRITE_FORWARDED = (
    "output_path",
    "geometry_column",
    "compression",
    "compression_level",
    "row_group_size_mb",
    "row_group_rows",
    "input_crs",
    "verbose",
    "custom_metadata",
    "extra_kv_metadata",
)


class BaseWriteStrategy(ABC):
    """
    Base class for write strategy implementations.

    Each strategy must implement write_from_query() and write_from_table()
    to handle both DuckDB query results and Arrow tables as input.
    """

    name: str
    description: str
    supports_streaming: bool
    supports_remote: bool

    @abstractmethod
    def write_from_query(
        self,
        con: duckdb.DuckDBPyConnection,
        query: str,
        output_path: str,
        geometry_column: str,
        original_metadata: dict | None,
        geoparquet_version: str,
        compression: str,
        compression_level: int,
        row_group_size_mb: int | None,
        row_group_rows: int | None,
        input_crs: dict | None,
        verbose: bool,
        custom_metadata: dict | None = None,
        geometry_info: dict | None = None,
        extra_kv_metadata: dict[str, str] | None = None,
    ) -> None:
        """
        Write query results to GeoParquet file.

        Args:
            con: DuckDB connection with spatial extension loaded
            query: SQL SELECT query to execute
            output_path: Path to output file
            geometry_column: Name of geometry column
            original_metadata: Metadata dict from input file
            geoparquet_version: Target GeoParquet version
            compression: Compression codec
            compression_level: Compression level
            row_group_size_mb: Target row group size in MB
            row_group_rows: Exact number of rows per row group
            input_crs: CRS dict from input file
            verbose: Enable verbose logging
            custom_metadata: Optional dict with custom metadata (e.g., H3 covering info)
            geometry_info: Multi-geometry column info from input file
            extra_kv_metadata: Additional Parquet KV metadata (e.g., Vecorel collection info)
        """
        ...

    @abstractmethod
    def write_from_table(
        self,
        table: pa.Table,
        output_path: str,
        geometry_column: str,
        geoparquet_version: str,
        compression: str,
        compression_level: int,
        row_group_size_mb: int | None,
        row_group_rows: int | None,
        verbose: bool,
        input_crs: dict | None = None,
        custom_metadata: dict | None = None,
        extra_kv_metadata: dict[str, str] | None = None,
    ) -> None:
        """
        Write Arrow table to GeoParquet file.

        Args:
            table: Arrow table to write
            output_path: Path to output file
            geometry_column: Name of geometry column
            geoparquet_version: Target GeoParquet version
            compression: Compression codec
            compression_level: Compression level
            row_group_size_mb: Target row group size in MB
            row_group_rows: Exact number of rows per row group
            verbose: Enable verbose logging
            input_crs: CRS dict to apply to geometry column
            custom_metadata: Optional dict with custom metadata (e.g., H3 covering info)
            extra_kv_metadata: Additional Parquet file-level KV metadata as
                {key: json_string}, written alongside 'geo'. Callers pass the
                input's preserved non-geo keys here so sidecar payloads
                (fiboa, vecorel, STAC) survive on every strategy (#690).
        """
        ...

    def _write_table_query(
        self, con, query: str, table_args: dict, *, geoparquet_version: str, geometry_info
    ) -> None:
        """:meth:`write_from_query` over a query on a registered table.

        ``table_args`` is the caller's :meth:`write_from_table` arguments; the
        ones a query write takes are forwarded under their own names, so each
        strategy's table entry point does not restate the list.
        """
        forwarded = {name: table_args[name] for name in _TABLE_WRITE_FORWARDED}
        self.write_from_query(
            con=con,
            query=query,
            original_metadata=None,
            geoparquet_version=geoparquet_version,
            geometry_info=geometry_info,
            **forwarded,
        )

    def _validate_output_path(self, output_path: str) -> None:
        """
        Validate output path for security concerns.

        Prevents path traversal attacks, symlink attacks, and SQL injection vectors.

        Args:
            output_path: Path to validate

        Raises:
            ValueError: If path contains security concerns
        """
        # Check for dangerous characters that could break SQL or cause issues
        if re.search(r"[;\x00]", output_path):
            raise ValueError(f"Invalid characters in output path: {output_path}")

        # Check for directory traversal in the original path (before resolution)
        normalized_input = os.path.normpath(output_path)
        if ".." in normalized_input.split(os.sep):
            raise ValueError(f"Invalid output path (directory traversal detected): {output_path}")

        # Resolve symlinks and normalize for robust path handling
        try:
            Path(output_path).resolve()
        except (OSError, ValueError) as e:
            raise ValueError(f"Invalid output path: {output_path}") from e


def needs_metadata_rewrite(
    geoparquet_version: str,
    original_metadata: dict | None,
    operation: str = "default",
) -> bool:
    """
    Determine if metadata rewrite is needed for this operation.

    Some operations can skip metadata rewriting when the output format
    already has sufficient metadata or doesn't require geo metadata.

    Args:
        geoparquet_version: Target GeoParquet version
        original_metadata: Metadata from input file
        operation: Type of operation (columns_only, sort, default)

    Returns:
        True if metadata rewrite is needed
    """

    version_config = GEOPARQUET_VERSIONS[geoparquet_version]

    if geoparquet_version == "parquet-geo-only":
        # For parquet-geo-only, we need to strip any existing geo metadata
        # Check if input has geo metadata that needs to be stripped
        return read_raw_geo(original_metadata).present

    if geoparquet_version == "2.0":
        if operation in ("columns_only", "sort"):
            return False
        # For 2.0 output, check if input has different version that needs updating
        # A carried block needs the rewrite unless it already vouches for a 2.x
        # version. An undecodable or non-object one reads as `{}` and so cannot
        # vouch (a raw json.loads here once aborted the write instead).
        if carried_geoparquet_version(original_metadata) == "2.0":
            return False
        return read_raw_geo(original_metadata).present

    return version_config["rewrite_metadata"]
