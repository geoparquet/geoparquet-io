"""
Disk rewrite write strategy.

This strategy writes with DuckDB first (fast but no geo metadata), then reads
and rewrites the file row-group by row-group with PyArrow to add geo metadata.

Best for: Maximum compatibility, fallback when other strategies fail
Memory: O(row_group_size) - one row group at a time
Speed: Slower (reads file twice, writes twice)
"""

from __future__ import annotations

import json
import os
import tempfile
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.compression import validate_compression_settings
from geoparquet_io.core.duckdb_utils import (
    get_duckdb_connection,
    quote_identifier,
    sql_path,
)
from geoparquet_io.core.geoarrow_encoding import arrow_extension_name, native_wkb_type
from geoparquet_io.core.logging_config import configure_verbose, debug, progress, success
from geoparquet_io.core.parquet_writer import apply_output_kv_metadata
from geoparquet_io.core.write_strategies.arrow_streaming import to_geoarrow_column
from geoparquet_io.core.write_strategies.base import (
    BaseWriteStrategy,
    build_geo_metadata,
    native_geometry_crs,
)
from geoparquet_io.core.write_strategies.row_group_sizing import (
    _resolve_row_group_rows,
    _resolve_row_group_rows_for_table,
)

if TYPE_CHECKING:
    import duckdb


def _native_geometry_schema(schema: pa.Schema, native_crs: dict[str, dict | None]) -> pa.Schema:
    """Retype the given geometry columns as ``geoarrow.wkb``, leaving the rest alone.

    ``pa.schema`` starts with no metadata, so the source's is reattached: the
    caller reads the carried KV keys straight off the returned schema, and
    dropping them here would make the metadata a function of the target version.
    """
    fields = [
        pa.field(field.name, native_wkb_type(native_crs[field.name]), nullable=field.nullable)
        if field.name in native_crs
        else field
        for field in schema
    ]
    return pa.schema(fields, metadata=schema.metadata)


def _to_native_geometry(column: pa.ChunkedArray, target_type) -> pa.ChunkedArray:
    """Encode one WKB column as ``target_type``, chunk by chunk.

    ``to_geoarrow_column`` (shared with the streaming strategy) takes a single
    array; a row group read back through PyArrow -- and anything the row-group
    coarsening concatenates -- arrives as a ChunkedArray. Passing ``target_type``
    on is what keeps a zero-chunk column (an empty row group) typed, and what
    restores the CRS on chunks whose own type dropped it.
    """
    return pa.chunked_array(
        [to_geoarrow_column(chunk, target_type) for chunk in column.chunks], type=target_type
    )


def _conform_row_group(table: pa.Table, schema: pa.Schema, native_crs: dict[str, dict | None]):
    """Give one row group the output schema, converting its native geometry columns."""
    if not native_crs:
        return table.replace_schema_metadata(schema.metadata)
    columns = [
        _to_native_geometry(table.column(index), field.type)
        if field.name in native_crs
        else table.column(index)
        for index, field in enumerate(schema)
    ]
    # Built against the output schema, so the field's type -- and the CRS that
    # `WkbType.__eq__` ignores -- is what reaches the writer.
    return pa.Table.from_arrays(columns, schema=schema)


class DiskRewriteStrategy(BaseWriteStrategy):
    """
    Write with DuckDB, then read/rewrite entire file with PyArrow for metadata.

    This is the most reliable fallback strategy. It writes to disk first using
    DuckDB's fast COPY TO, then rewrites row-group by row-group to add geo
    metadata. Memory usage is bounded by one row group.
    """

    name = "disk-rewrite"
    description = "Full file rewrite (reliable, memory-efficient via row groups)"
    supports_streaming = False
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
        geometry_info: dict | None = None,
        extra_kv_metadata: dict[str, str] | None = None,
    ) -> None:
        """Write query results to GeoParquet using DuckDB COPY then PyArrow rewrite."""
        from geoparquet_io.core.common import compute_geometry_types_via_sql
        from geoparquet_io.core.duckdb_utils import _wrap_query_with_wkb_conversion
        from geoparquet_io.core.geo_metadata import compute_bbox_via_sql
        from geoparquet_io.core.remote import is_remote_url, upload_if_remote

        configure_verbose(verbose)
        self._validate_output_path(output_path)

        # DuckDB COPY TO sizes row groups by row count, so translate any MB target
        # into rows before it can take effect on this strategy (fixes #689 — both
        # sizing options were accepted and then ignored entirely).
        resolved_row_group_rows = _resolve_row_group_rows(
            con, query, row_group_size_mb, row_group_rows, verbose
        )

        # Handle non-geo queries: write plain Parquet without geo metadata
        if geometry_column is None:
            self._write_plain_parquet_from_query(
                con,
                query,
                output_path,
                compression,
                compression_level,
                verbose,
                resolved_row_group_rows,
            )
            return

        compression_map = {
            "zstd": "ZSTD",
            "gzip": "GZIP",
            "snappy": "SNAPPY",
            "lz4": "LZ4",
            "none": "UNCOMPRESSED",
            "uncompressed": "UNCOMPRESSED",
            "brotli": "BROTLI",
        }
        duckdb_compression = compression_map.get(compression.lower(), "ZSTD")

        validated_compression, validated_level, _ = validate_compression_settings(
            compression, compression_level, verbose
        )

        is_remote = is_remote_url(output_path)
        work_dir = tempfile.mkdtemp(prefix="gpio_disk_rewrite_")

        try:
            temp_path = os.path.join(work_dir, "temp_duckdb.parquet")
            final_path = os.path.join(work_dir, "final.parquet") if is_remote else output_path

            if verbose:
                debug("Computing bbox via SQL...")
            bbox = compute_bbox_via_sql(con, query, geometry_column)

            if verbose:
                debug("Computing geometry types via SQL...")
            geometry_types = compute_geometry_types_via_sql(con, query, geometry_column)

            final_query = _wrap_query_with_wkb_conversion(query, geometry_column, con)

            copy_options = ["FORMAT PARQUET", f"COMPRESSION {duckdb_compression}"]
            if resolved_row_group_rows:
                copy_options.append(f"ROW_GROUP_SIZE {resolved_row_group_rows}")
            copy_query = f"""
                COPY ({final_query})
                TO {sql_path(temp_path)}
                ({", ".join(copy_options)})
            """

            if verbose:
                debug(f"Writing via DuckDB COPY TO with {duckdb_compression} compression...")

            con.execute(copy_query)

            if verbose:
                # Closed before leaving the block: this file is `os.unlink`ed a
                # few lines below, and Windows refuses to delete a file with an
                # open handle -- `--verbose` disk-rewrite writes died there with
                # `PermissionError: [WinError 32]` while POSIX cleaned up fine.
                with pq.ParquetFile(temp_path) as pf:
                    debug(
                        f"DuckDB wrote {pf.metadata.num_rows:,} rows, "
                        f"{pf.metadata.num_row_groups} row groups"
                    )

            geo_meta = build_geo_metadata(
                geometry_column=geometry_column,
                geoparquet_version=geoparquet_version,
                original_metadata=original_metadata,
                input_crs=input_crs,
                custom_metadata=custom_metadata,
                bbox=bbox,
                geometry_types=geometry_types,
                geometry_info=geometry_info,
            )

            # 2.0 and parquet-geo-only require a native Parquet GEOMETRY logical
            # type; 1.0/1.1 require plain BYTE_ARRAY WKB and forbid it. Every
            # other strategy branches on the version here -- this one did not, so
            # its 2.0 output declared a version whose own spec its bytes violated
            # (#764). parquet-geo-only additionally writes no `geo` block: the
            # one built above is still what the native types are keyed off, but
            # it names a null version, which DuckDB refuses to open.
            native_crs = native_geometry_crs(
                geoparquet_version, geo_meta, geometry_column, geometry_info
            )
            if verbose and native_crs:
                debug(f"Writing native Parquet GEOMETRY types for {sorted(native_crs)}")

            self._rewrite_with_metadata(
                input_path=temp_path,
                output_path=final_path,
                geo_meta=None if geoparquet_version == "parquet-geo-only" else geo_meta,
                compression=validated_compression,
                compression_level=validated_level,
                verbose=verbose,
                extra_kv_metadata=extra_kv_metadata,
                row_group_rows=resolved_row_group_rows,
                native_geometry_crs=native_crs,
            )

            os.unlink(temp_path)

            if is_remote:
                upload_if_remote(final_path, output_path, is_directory=False, verbose=verbose)
                os.unlink(final_path)

        finally:
            if os.path.exists(work_dir):
                import shutil

                shutil.rmtree(work_dir, ignore_errors=True)

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
        """Write Arrow table to GeoParquet using temporary file and rewrite."""
        from geoparquet_io.core.common import _detect_version_from_table

        configure_verbose(verbose)
        self._validate_output_path(output_path)

        # The facade has the last word on the output's file-level keys, and it
        # runs before the non-geo early return below -- which is exactly the
        # return that wrote a carried `geo` key through under an explicit
        # parquet-geo-only request (#773).
        table = apply_output_kv_metadata(table, geoparquet_version, extra_kv_metadata)

        # Handle non-geo tables: write plain Parquet without geo metadata
        if geometry_column is None:
            self._write_plain_parquet_from_table(
                table,
                output_path,
                compression,
                compression_level,
                verbose,
                _resolve_row_group_rows_for_table(
                    table, row_group_size_mb, row_group_rows, verbose
                ),
            )
            return

        # Auto-detect version from table schema metadata if not specified
        effective_version = geoparquet_version
        if effective_version is None:
            effective_version = _detect_version_from_table(table, verbose)

        con = get_duckdb_connection(load_httpfs=False)
        try:
            con.register("input_table", table)

            # Convert WKB bytes to GEOMETRY for proper spatial processing.
            # A geoarrow.wkb column already registers as GEOMETRY, and
            # ST_GeomFromWKB(GEOMETRY) is a binder error -- in either shape the
            # extension name arrives in, resolved on the Arrow type or carried
            # in the field metadata (#727).
            if arrow_extension_name(table.schema.field(geometry_column)) == "geoarrow.wkb":
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
        compression_level: int | None,
        verbose: bool,
        row_group_rows: int | None = None,
    ) -> None:
        """Write plain Parquet (no geo metadata) from an Arrow table."""
        from geoparquet_io.core.remote import is_remote_url, upload_if_remote

        validated_compression, validated_level, _ = validate_compression_settings(
            compression, compression_level, verbose
        )

        pa_compression = validated_compression if validated_compression != "UNCOMPRESSED" else None
        writer_kwargs = {"compression": pa_compression}
        if validated_level is not None and pa_compression:
            writer_kwargs["compression_level"] = validated_level
        if row_group_rows:
            writer_kwargs["row_group_size"] = row_group_rows

        is_remote = is_remote_url(output_path)
        work_dir = tempfile.mkdtemp(prefix="gpio_disk_rewrite_") if is_remote else None

        try:
            local_path = os.path.join(work_dir, "output.parquet") if is_remote else output_path

            if verbose:
                debug(f"Writing plain Parquet with {validated_compression} compression...")

            pq.write_table(table, local_path, **writer_kwargs)

            if is_remote:
                upload_if_remote(local_path, output_path, is_directory=False, verbose=verbose)

            if verbose:
                success(f"Wrote {table.num_rows:,} rows to {output_path}")
        finally:
            if work_dir and os.path.exists(work_dir):
                import shutil

                shutil.rmtree(work_dir, ignore_errors=True)

    def _write_plain_parquet_from_query(
        self,
        con: duckdb.DuckDBPyConnection,
        query: str,
        output_path: str,
        compression: str,
        compression_level: int | None,  # noqa: ARG002 - DuckDB COPY doesn't support compression_level
        verbose: bool,
        row_group_rows: int | None = None,
    ) -> None:
        """Write plain Parquet (no geo metadata) from a query.

        Note: compression_level is accepted for API consistency but not used.
        DuckDB's COPY TO command doesn't support compression_level for most
        compression types. For compression_level support, use write_from_table
        which uses PyArrow directly.
        """
        from geoparquet_io.core.remote import is_remote_url, upload_if_remote

        compression_map = {
            "zstd": "ZSTD",
            "gzip": "GZIP",
            "snappy": "SNAPPY",
            "lz4": "LZ4",
            "none": "UNCOMPRESSED",
            "uncompressed": "UNCOMPRESSED",
            "brotli": "BROTLI",
        }
        duckdb_compression = compression_map.get(compression.lower(), "ZSTD")

        is_remote = is_remote_url(output_path)
        work_dir = tempfile.mkdtemp(prefix="gpio_disk_rewrite_") if is_remote else None

        try:
            local_path = os.path.join(work_dir, "output.parquet") if is_remote else output_path

            copy_options = ["FORMAT PARQUET", f"COMPRESSION {duckdb_compression}"]
            if row_group_rows:
                copy_options.append(f"ROW_GROUP_SIZE {row_group_rows}")

            copy_query = f"""
                COPY ({query})
                TO {sql_path(local_path)}
                ({", ".join(copy_options)})
            """

            if verbose:
                debug(f"Writing plain Parquet with {duckdb_compression} compression...")
            con.execute(copy_query)

            if is_remote:
                upload_if_remote(local_path, output_path, is_directory=False, verbose=verbose)

            if verbose:
                with pq.ParquetFile(local_path) as pf:
                    success(f"Wrote {pf.metadata.num_rows:,} rows to {output_path}")
        finally:
            if work_dir and os.path.exists(work_dir):
                import shutil

                shutil.rmtree(work_dir, ignore_errors=True)

    def _rewrite_with_metadata(
        self,
        input_path: str,
        output_path: str,
        geo_meta: dict | None,
        compression: str,
        compression_level: int | None,
        verbose: bool,
        extra_kv_metadata: dict[str, str] | None = None,
        row_group_rows: int | None = None,
        native_geometry_crs: dict[str, dict | None] | None = None,
    ) -> None:
        """Rewrite file with proper geo metadata, row group by row group.

        ``geo_meta`` is ``None`` for parquet-geo-only output, which carries its
        geometry typing in the Parquet schema and must declare no GeoParquet
        version at all -- writing the block built for it put ``"version": null``
        in the file, which DuckDB's reader rejects outright (#764).

        ``native_geometry_crs`` names the geometry columns that must reach the
        writer as a native Parquet GEOMETRY logical type (2.0 and
        parquet-geo-only), each with the CRS its ``geo`` entry declares. Empty at
        1.0/1.1, where the same columns stay plain BYTE_ARRAY WKB.

        ``row_group_rows`` is the already-resolved rows-per-group request. The
        DuckDB COPY that produced ``input_path`` was given the same value, but it
        does not always honour it: DuckDB leaves tiny source groups alone, so a
        400-row file of 10-row groups reaches this method still shaped 40x10.

        Writing one ``write_table`` per *source* row group starts a new row group
        each time, so the rewrite could only ever make groups smaller than the
        source's: that file with ``row_group_rows=100`` came back as 40 groups of
        10, silently ignoring the request (#697).

        Source groups are therefore accumulated until the target is reached, and
        the target is sliced off and written whole. The remainder is *carried*
        into the next group rather than flushed with it -- flushing it turns
        every over-full batch into a full group plus a runt, which is how a
        request of 25 against 10-row sources produced ``[25, 5, 25, 5, ...]``.
        """
        native_crs = native_geometry_crs or {}

        pf = pq.ParquetFile(input_path)
        try:
            self._write_rewritten_row_groups(
                pf,
                output_path,
                geo_meta,
                compression,
                compression_level,
                verbose,
                extra_kv_metadata,
                row_group_rows,
                native_crs,
            )
        finally:
            # The caller unlinks `input_path` as soon as this returns, and
            # Windows refuses to delete a file this process still has open.
            pf.close()

    def _write_rewritten_row_groups(
        self,
        pf: pq.ParquetFile,
        output_path: str,
        geo_meta: dict | None,
        compression: str,
        compression_level: int | None,
        verbose: bool,
        extra_kv_metadata: dict[str, str] | None,
        row_group_rows: int | None,
        native_crs: dict[str, dict | None],
    ) -> None:
        """Write the already-open source file out again, one row group at a time."""
        from geoparquet_io.core.common import _CARRIED_SCHEMA_METADATA_KEYS_BYTES

        schema = _native_geometry_schema(pf.schema_arrow, native_crs)

        new_meta = dict(schema.metadata or {})
        if geo_meta is not None:
            new_meta[b"geo"] = json.dumps(geo_meta).encode("utf-8")
        else:
            # parquet-geo-only: no `geo` key, and no serialized descriptor of the
            # input's schema either -- the same exclusion set the other write
            # paths use (`_strip_geo_metadata_key`).
            new_meta = {
                key: value
                for key, value in new_meta.items()
                if key not in _CARRIED_SCHEMA_METADATA_KEYS_BYTES
            }
        if extra_kv_metadata:
            for key, value in extra_kv_metadata.items():
                bkey = key.encode("utf-8") if isinstance(key, str) else key
                bval = value.encode("utf-8") if isinstance(value, str) else value
                new_meta[bkey] = bval
        new_schema = schema.with_metadata(new_meta)

        if verbose:
            progress(f"Rewriting with geo metadata ({pf.metadata.num_row_groups} row groups)...")

        pa_compression = compression if compression != "UNCOMPRESSED" else None
        writer_kwargs = {
            "compression": pa_compression,
        }
        if compression_level is not None and pa_compression:
            writer_kwargs["compression_level"] = compression_level

        num_source_groups = pf.metadata.num_row_groups

        with pq.ParquetWriter(output_path, new_schema, **writer_kwargs) as writer:
            # No sizing request: keep the source's row-group shape, one for one.
            if not row_group_rows:
                for i in range(num_source_groups):
                    writer.write_table(
                        _conform_row_group(pf.read_row_group(i), new_schema, native_crs)
                    )
                    if verbose and (i + 1) % 10 == 0:
                        debug(f"Rewrote {i + 1}/{num_source_groups} row groups...")
            else:
                # Buffer until a whole target group is available, write exactly
                # that many rows, and keep the overshoot for the next group.
                pending: list[pa.Table] = []
                pending_rows = 0
                for i in range(num_source_groups):
                    table = _conform_row_group(pf.read_row_group(i), new_schema, native_crs)
                    pending.append(table)
                    pending_rows += table.num_rows
                    while pending_rows >= row_group_rows:
                        combined = pa.concat_tables(pending)
                        writer.write_table(
                            combined.slice(0, row_group_rows), row_group_size=row_group_rows
                        )
                        remainder = combined.slice(row_group_rows)
                        pending = [remainder] if remainder.num_rows else []
                        pending_rows = remainder.num_rows

                    if verbose and (i + 1) % 10 == 0:
                        debug(f"Rewrote {i + 1}/{num_source_groups} row groups...")

                if pending:
                    writer.write_table(pa.concat_tables(pending), row_group_size=row_group_rows)

        if verbose:
            with pq.ParquetFile(output_path) as result_pf:
                success(f"Wrote {result_pf.metadata.num_rows:,} rows to {output_path}")
