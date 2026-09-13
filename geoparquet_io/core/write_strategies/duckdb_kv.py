"""
DuckDB KV_METADATA write strategy.

This strategy uses DuckDB's native COPY TO with the KV_METADATA option to write
geo metadata directly during the streaming write. Single atomic operation with
no post-processing required.

Best for: Very large files, minimal memory usage
Memory: O(1) - nearly constant
Speed: Fast writes, no post-processing needed
Reliability: Atomic write - either succeeds completely or fails
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow.parquet as pq

from geoparquet_io.core.arrow_geo_metadata import (
    _canonicalize_wkb_columns,
    _detect_version_from_table,
    _parse_geo_metadata_quietly,
)
from geoparquet_io.core.duckdb_utils import (
    _escape_sql_string,
    build_kv_metadata_clause,
    quote_identifier,
    sql_path,
    validate_compression_level,
)
from geoparquet_io.core.geo_metadata import declare_carried_bbox_column
from geoparquet_io.core.geoarrow_encoding import arrow_extension_name
from geoparquet_io.core.logging_config import configure_verbose, debug, success
from geoparquet_io.core.write_strategies.base import (
    BaseWriteStrategy,
    build_geo_metadata,
    resolve_geometry_columns,
)
from geoparquet_io.core.write_strategies.row_group_sizing import _resolve_row_group_rows

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa

# Valid compression values whitelist (prevents injection via compression param)
VALID_COMPRESSIONS = frozenset({"ZSTD", "SNAPPY", "GZIP", "LZ4", "UNCOMPRESSED", "BROTLI"})

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


def _get_available_memory() -> int | None:
    """
    Get available memory in bytes, accounting for container limits.

    Checks cgroup v2 and v1 limits first (Docker, Kubernetes, etc.),
    then falls back to psutil for bare-metal systems.

    Returns:
        Available memory in bytes, or None if detection fails
    """
    # Check cgroup v2 memory limit (Docker, Kubernetes)
    try:
        with open("/sys/fs/cgroup/memory.max") as f:
            limit = f.read().strip()
            if limit != "max":
                cgroup_limit = int(limit)
                # Try to get current usage to calculate available
                try:
                    with open("/sys/fs/cgroup/memory.current") as f2:
                        current = int(f2.read().strip())
                        return cgroup_limit - current
                except (FileNotFoundError, ValueError):
                    # Return 80% of limit if we can't get current usage
                    return int(cgroup_limit * 0.8)
    except (FileNotFoundError, ValueError):
        pass

    # Check cgroup v1 memory limit
    try:
        with open("/sys/fs/cgroup/memory/memory.limit_in_bytes") as f:
            limit = int(f.read().strip())
            # Values near 2^63 indicate no limit
            if limit < 2**60:
                try:
                    with open("/sys/fs/cgroup/memory/memory.usage_in_bytes") as f2:
                        usage = int(f2.read().strip())
                        return limit - usage
                except (FileNotFoundError, ValueError):
                    return int(limit * 0.8)
    except (FileNotFoundError, ValueError):
        pass

    # Fall back to psutil for non-containerized environments
    try:
        import psutil

        return psutil.virtual_memory().available
    except ImportError:
        return None


def get_default_memory_limit() -> str:
    """
    Get default memory limit for DuckDB streaming (50% of available RAM).

    Container-aware: detects Docker/Kubernetes memory limits via cgroups
    before falling back to psutil for bare-metal systems.

    Returns:
        Memory limit string for DuckDB (e.g., '2GB', '512MB')
    """
    available = _get_available_memory()

    if available is None:
        return "2GB"  # Conservative fallback

    # Use 50% of available memory
    limit_bytes = int(available * 0.5)
    limit_gb = limit_bytes / (1024**3)

    if limit_gb >= 1:
        return f"{limit_gb:.1f}GB"

    limit_mb = limit_bytes / (1024**2)
    return f"{max(128, int(limit_mb))}MB"  # Minimum 128MB


def _wrap_query_with_crs(
    query: str,
    geometry_column: str,
    input_crs: dict | None,
) -> str:
    """Wrap query with ST_SetCRS() — delegates to shared helper in common.py."""
    from geoparquet_io.core.crs_utils import _wrap_query_with_crs as _common_wrap_query_with_crs

    return _common_wrap_query_with_crs(query, geometry_column, input_crs)


def _plain_wkb_for_secondary_columns(table, geometry_column: str, verbose: bool):
    """Give every NON-primary geometry column the canonical plain-WKB carrier.

    Named from the table's own carried ``geo`` key, which is the only place a
    table entry point learns about secondaries. The primary is left alone (it is
    passed as ``native_columns``): the caller converts it explicitly, and casting
    it here would both copy the whole geometry column and undo the
    field-metadata detection the query wrapper depends on (#727).

    Both carrier shapes reach here -- a resolved ``geoarrow.wkb`` extension type,
    and plain ``large_binary`` carrying only an ``ARROW:extension:name`` field
    metadata key, which PyArrow leaves unresolved but DuckDB still registers as
    GEOMETRY, making the COPY write a native Parquet GEOMETRY type that is
    illegal below GeoParquet 2.0 (#706, #727). ``_canonicalize_wkb_columns``
    handles both, and its ``_WKB_EXTENSION_NAMES`` guard leaves a *native* nested
    carrier (``geoarrow.point`` over ``struct<x, y>``) alone rather than forcing
    it through a binary cast PyArrow cannot perform.
    """

    carried_geo = _parse_geo_metadata_quietly(table.schema.metadata)
    secondaries = resolve_geometry_columns(geometry_column, None, carried_geo) - {geometry_column}
    if not secondaries:
        return table
    return _canonicalize_wkb_columns(
        table, sorted(secondaries), verbose, native_columns={geometry_column}
    )


def _build_copy_options(
    compression: str,
    row_group_rows: int | None,
    geo_meta_json: str | None = None,
    extra_kv_metadata: dict[str, str] | None = None,
    compression_level: int | None = None,
) -> list[str]:
    """Build COPY TO options list."""
    options = [
        "FORMAT PARQUET",
        f"COMPRESSION {compression}",
        "GEOPARQUET_VERSION 'NONE'",
    ]
    # DuckDB accepts COMPRESSION_LEVEL for ZSTD only ("Compression level is only
    # supported for the ZSTD compression codec"); naming any other codec with a
    # level is a binder error. Omitting it entirely silently dropped the user's
    # --compression-level and fell back to DuckDB's ZSTD default of 3 against
    # gpio's default of 15 -- ~15% larger output on every write through here.
    if compression_level is not None and compression.upper() == "ZSTD":
        options.append(f"COMPRESSION_LEVEL {validate_compression_level(compression_level)}")
    kv_pairs = dict(extra_kv_metadata or {})
    if geo_meta_json:
        # `geo` is generated by this write, so it wins over a preserved copy of
        # the input's stale one. Note the deliberate precedence change: the
        # generated block is applied last, where an explicit `geo` in
        # `extra_kv_metadata` used to overwrite it. Preservation never reaches
        # this (`extract_preserved_kv_metadata` excludes `geo`), so only a
        # caller that hand-passes `geo` sees the difference -- and that caller
        # is asking this function to describe the geometry it is writing.
        kv_pairs["geo"] = geo_meta_json
    kv_clause = build_kv_metadata_clause(kv_pairs)
    if kv_clause:
        options.append(kv_clause)
    if row_group_rows:
        options.append(f"ROW_GROUP_SIZE {row_group_rows}")
    return options


class DuckDBKVStrategy(BaseWriteStrategy):
    """
    Use DuckDB COPY TO with native KV_METADATA for geo metadata.

    This strategy streams data directly through DuckDB's COPY TO command
    with the KV_METADATA option, which embeds geo metadata directly in
    the Parquet footer during the write. No post-processing is needed.
    """

    name = "duckdb-kv"
    description = "DuckDB streaming write with native metadata support"
    supports_streaming = True
    supports_remote = True

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
        memory_limit: str | None = None,
        geometry_info: dict | None = None,
        extra_kv_metadata: dict[str, str] | None = None,
    ) -> None:
        """Write query results to GeoParquet using DuckDB COPY TO with KV_METADATA."""
        from geoparquet_io.core.remote import is_remote_url, upload_if_remote

        configure_verbose(verbose)
        self._validate_output_path(output_path)

        compression_upper = compression.upper()
        if compression_upper not in VALID_COMPRESSIONS:
            raise ValueError(
                f"Invalid compression: {compression}. Valid: {', '.join(VALID_COMPRESSIONS)}"
            )

        # DuckDB COPY TO sizes row groups by row count, so translate any MB target
        # into rows before it can take effect on this strategy (fixes #547).
        row_group_rows = _resolve_row_group_rows(
            con, query, row_group_size_mb, row_group_rows, verbose
        )

        # Handle non-geo queries: write plain Parquet without geo metadata
        if geometry_column is None:
            self._write_plain_parquet_from_query(
                con,
                query,
                output_path,
                compression_upper,
                row_group_rows,
                verbose,
                extra_kv_metadata,
            )
            return

        saved_settings = self._configure_duckdb_memory(con, memory_limit, verbose)

        is_remote = is_remote_url(output_path)
        local_path = self._get_local_path(output_path, is_remote)

        try:
            if geoparquet_version == "parquet-geo-only":
                self._write_parquet_geo_only(
                    con,
                    query,
                    local_path,
                    geometry_column,
                    compression_upper,
                    compression_level,
                    row_group_rows,
                    input_crs,
                    output_path,
                    verbose,
                    extra_kv_metadata=extra_kv_metadata,
                )
            else:
                self._write_with_geo_metadata(
                    con,
                    query,
                    local_path,
                    geometry_column,
                    geoparquet_version,
                    compression_upper,
                    compression_level,
                    row_group_rows,
                    original_metadata,
                    input_crs,
                    custom_metadata,
                    output_path,
                    verbose,
                    geometry_info,
                    extra_kv_metadata=extra_kv_metadata,
                )

            if is_remote:
                upload_if_remote(local_path, output_path, is_directory=False, verbose=verbose)

        finally:
            self._restore_duckdb_settings(con, saved_settings, verbose)
            if is_remote and Path(local_path).exists():
                Path(local_path).unlink()

    #: Session settings this strategy overrides for the duration of one write.
    _MANAGED_SETTINGS = ("threads", "preserve_insertion_order", "memory_limit")

    def _restore_duckdb_settings(
        self,
        con: duckdb.DuckDBPyConnection,
        saved: dict[str, object],
        verbose: bool,
    ) -> None:
        """Put back the session settings this strategy clamped.

        The connection belongs to the caller, not to this write. Leaving
        threads=1 and a halved memory_limit behind meant one write pinned every
        later query on that connection -- partition loops finalize N files on a
        shared connection, so the first partition throttled the whole run, and
        the Python API holds a connection across operations.
        """
        for key, value in saved.items():
            try:
                if isinstance(value, str):
                    con.execute(f"SET {key} = '{_escape_sql_string(value)}'")
                else:
                    con.execute(f"SET {key} = {value}")
                # DuckDB reports sizes as rounded display strings ("14.3 GiB"),
                # so writing one back can land a hair off and drift further on
                # every write in a partition loop. A value that will not
                # round-trip was the engine's own default, so ask for that
                # instead of an approximation of it.
                if con.execute(f"SELECT current_setting('{key}')").fetchone()[0] != value:
                    con.execute(f"RESET {key}")
            except duckdb.Error as e:  # pragma: no cover - defensive
                if verbose:
                    debug(f"Could not restore DuckDB setting {key}: {e}")

    def _configure_duckdb_memory(
        self,
        con: duckdb.DuckDBPyConnection,
        memory_limit: str | None,
        verbose: bool,
    ) -> dict[str, object]:
        """Configure DuckDB memory settings for streaming.

        Returns the prior values so the caller can restore them; see
        ``_restore_duckdb_settings``.
        """
        saved: dict[str, object] = {}
        for key in self._MANAGED_SETTINGS:
            try:
                saved[key] = con.execute(f"SELECT current_setting('{key}')").fetchone()[0]
            except duckdb.Error as e:  # pragma: no cover - defensive
                if verbose:
                    debug(f"Could not read DuckDB setting {key}: {e}")

        con.execute("SET threads = 1")  # Required for memory control (DuckDB #8270)
        # Let COPY TO parquet flush row groups to disk instead of buffering the
        # entire result to preserve order. Without this the writer holds the whole
        # output in RAM and a large COPY runs out of memory even with a memory_limit
        # + temp_directory set (the constant-memory design from #185 relied on this
        # spilling). Safe because threads=1 already makes the single pipeline emit
        # rows in order, so output ordering (e.g. sorted files) is preserved.
        con.execute("SET preserve_insertion_order = false")
        # Validate before interpolation: a SET value cannot be parameterised.
        effective_limit = validate_memory_limit(memory_limit or get_default_memory_limit())
        con.execute(f"SET memory_limit = '{effective_limit}'")
        if verbose:
            debug(f"DuckDB memory limit: {effective_limit}")
        return saved

    def _get_local_path(self, output_path: str, is_remote: bool) -> str:
        """Get local path for writing (temp file if remote)."""
        if is_remote:
            fd, local_path = tempfile.mkstemp(suffix=".parquet")
            os.close(fd)
            return local_path
        return output_path

    def _write_parquet_geo_only(
        self,
        con: duckdb.DuckDBPyConnection,
        query: str,
        local_path: str,
        geometry_column: str,
        compression: str,
        compression_level: int,
        row_group_rows: int | None,
        input_crs: dict | None,
        output_path: str,
        verbose: bool,
        extra_kv_metadata: dict[str, str] | None = None,
    ) -> None:
        """Write parquet-geo-only format (no geo metadata)."""
        if verbose:
            debug("Writing parquet-geo-only (no geo metadata)...")

        # DuckDB 1.5+: Keep native GEOMETRY type — DuckDB writes native Parquet
        # geometry encoding directly. No WKB conversion needed.
        # Apply CRS via ST_SetCRS so DuckDB writes it into the schema natively.
        final_query = _wrap_query_with_crs(query, geometry_column, input_crs)

        copy_options = _build_copy_options(
            compression,
            row_group_rows,
            extra_kv_metadata=extra_kv_metadata,
            compression_level=compression_level,
        )
        copy_query = f"COPY ({final_query}) TO {sql_path(local_path)} ({', '.join(copy_options)})"
        con.execute(copy_query)

        if verbose:
            pf = pq.ParquetFile(local_path)
            success(f"Wrote {pf.metadata.num_rows:,} rows to {output_path}")

    def _write_with_geo_metadata(
        self,
        con: duckdb.DuckDBPyConnection,
        query: str,
        local_path: str,
        geometry_column: str,
        geoparquet_version: str,
        compression: str,
        compression_level: int,
        row_group_rows: int | None,
        original_metadata: dict | None,
        input_crs: dict | None,
        custom_metadata: dict | None,
        output_path: str,
        verbose: bool,
        geometry_info: dict | None = None,
        extra_kv_metadata: dict[str, str] | None = None,
    ) -> None:
        """Write with geo metadata (v1.0, v1.1, v2.0)."""
        from geoparquet_io.core.duckdb_utils import _wrap_query_with_blob_conversion

        geo_meta = build_geo_metadata(
            geometry_column=geometry_column,
            geoparquet_version=geoparquet_version,
            original_metadata=original_metadata,
            input_crs=input_crs,
            custom_metadata=custom_metadata,
            geometry_info=geometry_info,
        )

        col_meta = geo_meta["columns"][geometry_column]
        self._compute_missing_metadata(con, query, geometry_column, col_meta, verbose)
        declare_carried_bbox_column(con, query, col_meta, verbose, geoparquet_version)

        # For v1.x: Cast to BLOB so DuckDB writes plain binary WKB. EVERY geometry
        # column, not just the primary: validation applies the same per-version
        # requirement to each column in geo["columns"], and a secondary left as
        # DuckDB's GEOMETRY type lands as a native Parquet GEOMETRY logical type
        # inside a 1.x file, which is invalid (#706).
        # For v2.0: Keep native GEOMETRY type with CRS — DuckDB writes native
        # Parquet geometry encoding and CRS directly, for every geometry column.
        if geoparquet_version in ("1.0", "1.1"):
            secondary_columns = resolve_geometry_columns(
                geometry_column, geometry_info, geo_meta
            ) - {geometry_column}
            final_query = _wrap_query_with_blob_conversion(
                query, geometry_column, con, secondary_columns=sorted(secondary_columns)
            )
        else:
            final_query = _wrap_query_with_crs(query, geometry_column, input_crs)

        copy_options = _build_copy_options(
            compression, row_group_rows, json.dumps(geo_meta), extra_kv_metadata, compression_level
        )
        copy_query = f"COPY ({final_query}) TO {sql_path(local_path)} ({', '.join(copy_options)})"

        if verbose:
            debug(f"Writing via DuckDB COPY TO with {compression} compression...")
        con.execute(copy_query)

        if verbose:
            pf = pq.ParquetFile(local_path)
            success(f"Wrote {pf.metadata.num_rows:,} rows to {output_path}")

    def _compute_missing_metadata(
        self,
        con: duckdb.DuckDBPyConnection,
        query: str,
        geometry_column: str,
        col_meta: dict,
        verbose: bool,
    ) -> None:
        """Compute whichever of bbox/geometry_types the carried metadata lacks.

        Both come out of one scan, so a caller that invalidated both (a row
        filter, a reprojection, a multi-file merge) pays for a single pass.
        """
        from geoparquet_io.core.geo_metadata import compute_geo_stats_via_sql

        need_bbox = "bbox" not in col_meta
        need_types = "geometry_types" not in col_meta
        if not (need_bbox or need_types):
            return

        if verbose:
            debug("Computing bbox/geometry types via SQL...")
        bbox, geometry_types = compute_geo_stats_via_sql(
            con,
            query,
            geometry_column,
            need_bbox=need_bbox,
            need_geometry_types=need_types,
        )
        if need_bbox and bbox:
            col_meta["bbox"] = bbox
        if need_types:
            col_meta["geometry_types"] = geometry_types

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
        """Write Arrow table to GeoParquet using DuckDB COPY TO with KV_METADATA."""
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection

        configure_verbose(verbose)
        self._validate_output_path(output_path)

        # Handle non-geo tables: write plain Parquet without geo metadata
        if geometry_column is None:
            self._write_plain_parquet_from_table(
                table, output_path, compression, row_group_rows, verbose, extra_kv_metadata
            )
            return

        # Auto-detect version from table schema metadata if not specified
        effective_version = geoparquet_version
        if effective_version is None:
            effective_version = _detect_version_from_table(table, verbose)

        # DuckDB registers a `geoarrow.wkb` column as GEOMETRY, and the COPY then
        # writes it as a native Parquet GEOMETRY logical type. That is right for
        # 2.0 and wrong inside a 1.x file. The primary column is handled by the
        # blob conversion further down, which is keyed on names this entry point
        # does not have for the secondaries -- so strip them here, named by the
        # table's own carried geo key (#706).
        if effective_version in ("1.0", "1.1"):
            table = _plain_wkb_for_secondary_columns(table, geometry_column, verbose)

        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            con.register("input_table", table)

            # Convert WKB bytes to GEOMETRY for proper spatial processing.
            # geoarrow.wkb extension columns already register as GEOMETRY in
            # DuckDB, where ST_GeomFromWKB(GEOMETRY) is a binder error. The
            # extension name has to be read from the field metadata as well as
            # the Arrow type: gpio's own `add` operations hand back a plain
            # `large_binary` column carrying `ARROW:extension:name`, which
            # DuckDB still registers as GEOMETRY (#727).
            geom_field = table.schema.field(geometry_column)
            if arrow_extension_name(geom_field) == "geoarrow.wkb":
                query = "SELECT * FROM input_table"
            else:
                query = f"""
                    SELECT * REPLACE (ST_GeomFromWKB({quote_identifier(geometry_column)}) AS {quote_identifier(geometry_column)})
                    FROM input_table
                """

            self.write_from_query(
                con=con,
                query=query,
                output_path=output_path,
                geometry_column=geometry_column,
                original_metadata=None,
                geoparquet_version=effective_version,
                compression=compression,
                compression_level=compression_level,
                row_group_size_mb=row_group_size_mb,
                row_group_rows=row_group_rows,
                input_crs=input_crs,
                verbose=verbose,
                custom_metadata=custom_metadata,
                extra_kv_metadata=extra_kv_metadata,
            )
        finally:
            con.close()

    def _write_plain_parquet_from_table(
        self,
        table: pa.Table,
        output_path: str,
        compression: str,
        row_group_rows: int | None,
        verbose: bool,
        extra_kv_metadata: dict[str, str] | None = None,
    ) -> None:
        """Write plain Parquet (no geo metadata) from an Arrow table.

        Preservation must not depend on whether the table happens to have a
        geometry column (#708). This path cannot reuse ``_build_copy_options``:
        that helper emits ``GEOPARQUET_VERSION 'NONE'``, which the spatial
        extension provides, and the connection below deliberately omits spatial.
        """
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.remote import is_remote_url, upload_if_remote

        compression_upper = compression.upper()
        if compression_upper not in VALID_COMPRESSIONS:
            raise ValueError(
                f"Invalid compression: {compression}. Valid: {', '.join(VALID_COMPRESSIONS)}"
            )

        is_remote = is_remote_url(output_path)
        local_path = self._get_local_path(output_path, is_remote)

        con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
        try:
            con.register("input_table", table)

            options = [
                "FORMAT PARQUET",
                f"COMPRESSION {compression_upper}",
            ]
            kv_clause = build_kv_metadata_clause(extra_kv_metadata)
            if kv_clause:
                options.append(kv_clause)
            if row_group_rows:
                options.append(f"ROW_GROUP_SIZE {row_group_rows}")

            copy_query = f"COPY input_table TO {sql_path(local_path)} ({', '.join(options)})"

            if verbose:
                debug(f"Writing plain Parquet with {compression_upper} compression...")
            con.execute(copy_query)

            if is_remote:
                upload_if_remote(local_path, output_path, is_directory=False, verbose=verbose)

            if verbose:
                pf = pq.ParquetFile(local_path)
                success(f"Wrote {pf.metadata.num_rows:,} rows to {output_path}")
        finally:
            con.close()
            if is_remote and Path(local_path).exists():
                Path(local_path).unlink()

    def _write_plain_parquet_from_query(
        self,
        con: duckdb.DuckDBPyConnection,
        query: str,
        output_path: str,
        compression: str,
        row_group_rows: int | None,
        verbose: bool,
        extra_kv_metadata: dict[str, str] | None = None,
    ) -> None:
        """Write plain Parquet (no geo metadata) from a query.

        Same contract as the table entry point above: a query with no geometry
        column still carries the input's sidecar keys (#708).
        """
        from geoparquet_io.core.remote import is_remote_url, upload_if_remote

        is_remote = is_remote_url(output_path)
        local_path = self._get_local_path(output_path, is_remote)

        try:
            options = [
                "FORMAT PARQUET",
                f"COMPRESSION {compression}",
            ]
            kv_clause = build_kv_metadata_clause(extra_kv_metadata)
            if kv_clause:
                options.append(kv_clause)
            if row_group_rows:
                options.append(f"ROW_GROUP_SIZE {row_group_rows}")

            copy_query = f"COPY ({query}) TO {sql_path(local_path)} ({', '.join(options)})"

            if verbose:
                debug(f"Writing plain Parquet with {compression} compression...")
            con.execute(copy_query)

            if is_remote:
                upload_if_remote(local_path, output_path, is_directory=False, verbose=verbose)

            if verbose:
                pf = pq.ParquetFile(local_path)
                success(f"Wrote {pf.metadata.num_rows:,} rows to {output_path}")
        finally:
            if is_remote and Path(local_path).exists():
                Path(local_path).unlink()
