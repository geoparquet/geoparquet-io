"""
Arrow streaming write strategy.

This strategy streams DuckDB query results as Arrow RecordBatches directly
to a ParquetWriter, maintaining constant memory usage regardless of file size.

Best for: Large files, memory-constrained environments
Memory: O(batch_size) - constant regardless of total size
Speed: Slightly slower due to batch overhead
"""

from __future__ import annotations

import itertools
import json
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.compression import validate_compression_settings
from geoparquet_io.core.duckdb_utils import quote_identifier
from geoparquet_io.core.geoarrow_encoding import (
    WKB_EXTENSION_NAMES,
    arrow_extension_name,
    native_wkb_type,
)
from geoparquet_io.core.logging_config import configure_verbose, debug, progress, success
from geoparquet_io.core.parquet_writer import apply_output_kv_metadata
from geoparquet_io.core.write_strategies.base import (
    BaseWriteStrategy,
    merge_secondary_geometry_metadata,
    native_geometry_crs,
)

if TYPE_CHECKING:
    import duckdb

# Default batch size for streaming (100K rows per batch)
DEFAULT_BATCH_SIZE = 100_000


def _is_wkb_carrier(field: pa.Field) -> bool:
    """True when a field holds WKB bytes, in any shape DuckDB's Arrow export emits."""
    name = arrow_extension_name(field)
    if name is not None and name not in WKB_EXTENSION_NAMES:
        return False  # native nested GeoArrow (geoarrow.point, ...) — not WKB bytes
    storage = field.type.storage_type if isinstance(field.type, pa.ExtensionType) else field.type
    return pa.types.is_binary(storage) or pa.types.is_large_binary(storage)


def canonicalize_wkb_fields(
    schema: pa.Schema,
    geometry_columns: set[str],
    native_columns: set[str],
) -> pa.Schema:
    """Rewrite WKB geometry fields to the canonical plain-``binary`` carrier.

    GeoParquet 1.x requires geometry to be a plain BYTE_ARRAY WKB column, but
    DuckDB hands the streaming writer one of three shapes for the very same
    column depending on whether anything in the process imported
    ``geoarrow.pyarrow``: ``large_binary`` (writes a LARGE_BINARY column),
    ``large_binary`` plus raw ``ARROW:extension:name`` metadata, or a resolved
    ``geoarrow.wkb`` extension type (which PyArrow writes as a *native* Parquet
    GEOMETRY logical type — illegal below GeoParquet 2.0). Normalizing here makes
    the output a function of the inputs alone (#688).

    ``native_columns`` are the geometry columns the target version writes as
    native types and which must be left alone: all geometry columns for 2.0 and
    parquet-geo-only, the primary alone for 1.1-geoarrow, none for 1.0/1.1.
    """
    fields = [
        pa.field(field.name, pa.binary(), nullable=field.nullable)
        if (
            field.name not in native_columns
            and (field.name in geometry_columns or arrow_extension_name(field))
            and _is_wkb_carrier(field)
        )
        else field
        for field in schema
    ]
    return pa.schema(fields)


def _wkb_storage(array: pa.Array) -> pa.Array:
    """The underlying binary array, whether or not geoarrow's extension type is registered."""
    return array.storage if isinstance(array, pa.ExtensionArray) else array


def _to_plain_wkb_array(array: pa.Array | pa.ChunkedArray) -> pa.Array | pa.ChunkedArray:
    """Unwrap an extension array and narrow 64-bit offsets to Arrow's plain ``binary``."""
    array = _wkb_storage(array)
    if array.type == pa.binary():
        return array
    try:
        return array.cast(pa.binary())
    except pa.ArrowInvalid as e:
        # Plain ``binary`` uses 32-bit offsets, so >2 GB of WKB in one batch
        # cannot be narrowed. Splitting (see _binary_cast_spans) handles this,
        # so reaching here means several WKB columns overflowed at once.
        raise ValueError(
            "Geometry batch is too large to write as a plain WKB column "
            f"(Arrow's 2 GB limit for 32-bit binary offsets): {e}. "
            "Write smaller row groups (--row-group-rows) or simplify very large geometries."
        ) from e


def to_geoarrow_column(column: pa.Array, target_type) -> pa.Array:
    """Encode one WKB column as the given geoarrow extension type.

    Module level, and shared with the disk-rewrite strategy, which converts the
    same columns one row group at a time (#764).

    Note that ``WkbType.__eq__`` ignores the CRS, so the equality shortcut can
    return an array whose type carries a different CRS than ``target_type``.
    Both callers build their output batch/table against an explicit schema, so
    the schema's field type -- and its CRS -- is what gets written.
    """
    import geoarrow.pyarrow as ga

    geoarrow_arr = ga.as_wkb(column)
    if geoarrow_arr.type == target_type:
        return geoarrow_arr
    # from_buffers cannot carry a slice offset, so rebuild from the storage
    # array — ga.as_wkb returns a fresh, offset-free array, but a sliced input
    # (see the >2 GB split path) must not silently take the parent's rows.
    return pa.ExtensionArray.from_storage(
        target_type, _wkb_storage(geoarrow_arr).cast(target_type.storage_type)
    )


def _coerce_column(array: pa.Array, target_type: pa.DataType) -> pa.Array:
    """Narrow one column to the canonical carrier when the schema asks for plain binary."""
    if target_type == pa.binary() and array.type != target_type:
        return _to_plain_wkb_array(array)
    return array


def _narrows_to_32_bit_binary(field_type: pa.DataType) -> bool:
    """True when writing this field means putting WKB into 32-bit binary offsets.

    Covers both output shapes: the plain ``binary`` column of 1.0/1.1, and the
    ``geoarrow.wkb`` extension of 2.0 / parquet-geo-only, whose storage is also
    32-bit ``binary``. Both hit Arrow's 2 GB ceiling identically.
    """
    storage = field_type.storage_type if isinstance(field_type, pa.ExtensionType) else field_type
    return storage == pa.binary()


def _binary_cast_spans(batch: pa.RecordBatch, schema: pa.Schema) -> list[tuple[int, int]]:
    """Row spans that keep every narrowed WKB column under Arrow's 32-bit ceiling.

    Returns a single whole-batch span in the normal case. When a column would
    overflow, the boundaries of every offending column are merged so each
    resulting slice satisfies all of them at once.
    """
    from geoparquet_io.core.streaming import _MAX_WKB_CHUNK_BYTES, byte_limited_spans

    whole = [(0, batch.num_rows)]
    boundaries = {0, batch.num_rows}
    oversized = False
    for index, field in enumerate(schema):
        column = batch.column(index)
        if not _narrows_to_32_bit_binary(field.type):
            continue
        if _wkb_storage(column).type == pa.binary():
            continue  # already 32-bit; nothing is being narrowed
        # nbytes counts offsets/validity too, so it only ever over-estimates:
        # a batch that passes this cheap gate provably fits.
        if column.nbytes <= _MAX_WKB_CHUNK_BYTES:
            continue
        oversized = True
        # Measure the storage array: pyarrow's binary kernels reject extension
        # arrays, which is what a registered geoarrow.wkb column arrives as.
        boundaries.update(
            offset for offset, _ in byte_limited_spans(_wkb_storage(column), _MAX_WKB_CHUNK_BYTES)
        )

    if not oversized:
        return whole
    ordered = sorted(boundaries)
    return [(start, end - start) for start, end in zip(ordered, ordered[1:], strict=False)]


def _has_carried_bbox(original_metadata: dict | None, geometry_column: str) -> bool:
    """True when ``original_metadata`` still carries a bbox for this column.

    Callers that filter rows or move coordinates strip it first, which is the
    signal that this write must compute a fresh one.
    """
    from geoparquet_io.core.write_strategies.base import _parse_existing_geo_metadata

    geo_meta = _parse_existing_geo_metadata(original_metadata)
    if not isinstance(geo_meta, dict):
        return False
    col_meta = (geo_meta.get("columns") or {}).get(geometry_column)
    return isinstance(col_meta, dict) and col_meta.get("bbox") is not None


def _detect_bbox_column(schema: pa.Schema) -> dict:
    """Detect bbox column from schema using common naming conventions."""
    for name in schema.names:
        if name in ["bbox", "bounds", "extent"] or name.endswith("_bbox"):
            field = schema.field(name)
            if hasattr(field.type, "names") and set(field.type.names) >= {
                "xmin",
                "ymin",
                "xmax",
                "ymax",
            }:
                return {"has_bbox_column": True, "bbox_column_name": name}
    return {"has_bbox_column": False, "bbox_column_name": None}


def _check_geometry_type(
    con: duckdb.DuckDBPyConnection,
    query: str,
    geometry_column: str,
    verbose: bool,
) -> tuple[bool, str]:
    """Check if geometry column needs WKB conversion. Returns (needs_conversion, final_sql)."""
    from geoparquet_io.core.duckdb_utils import _wrap_query_with_wkb_conversion

    type_result = con.execute(
        f"SELECT TYPEOF({quote_identifier(geometry_column)}) FROM ({query}) LIMIT 1"
    ).fetchone()
    duckdb_type = type_result[0] if type_result else None
    needs_wkb = duckdb_type == "GEOMETRY"

    if verbose:
        msg = "wrapping with WKB conversion" if needs_wkb else "no conversion needed"
        debug(f"Geometry column is {duckdb_type}, {msg}")

    final_sql = _wrap_query_with_wkb_conversion(query, geometry_column, con) if needs_wkb else query
    return needs_wkb, final_sql


class ArrowStreamingStrategy(BaseWriteStrategy):
    """
    Stream DuckDB results as RecordBatches to ParquetWriter.

    This strategy executes a SQL query and streams the results directly to
    a Parquet file, avoiding the memory spike of materializing the full
    result as an Arrow table. Memory usage is bounded by batch size.
    """

    name = "streaming"
    description = "Stream query results in batches for constant memory usage"
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
        geometry_info: dict | None = None,
        extra_kv_metadata: dict[str, str] | None = None,
    ) -> None:
        """Write query results to GeoParquet using streaming RecordBatch approach."""
        from geoparquet_io.core.geo_metadata import GEOPARQUET_VERSIONS

        configure_verbose(verbose)
        self._validate_output_path(output_path)

        # Setup compression and version config
        validated_compression, validated_level, _ = validate_compression_settings(
            compression or "ZSTD", compression_level, verbose
        )
        if validated_compression == "UNCOMPRESSED":
            validated_compression = None

        batch_size = row_group_rows or DEFAULT_BATCH_SIZE

        # Handle non-geo queries: write plain Parquet without geo metadata
        if geometry_column is None:
            self._write_plain_parquet_from_query(
                con, query, output_path, validated_compression, validated_level, batch_size, verbose
            )
            return

        version_config = GEOPARQUET_VERSIONS.get(geoparquet_version, GEOPARQUET_VERSIONS["1.1"])

        # Check geometry type and prepare SQL
        _, final_sql = _check_geometry_type(con, query, geometry_column, verbose)

        # Determine version flags
        use_native_geometry = geoparquet_version in ("2.0", "parquet-geo-only")
        should_add_geo_metadata = geoparquet_version != "parquet-geo-only"
        geoarrow_native = geoparquet_version == "1.1-geoarrow"

        # Pre-compute metadata. A carried bbox is only reusable when it survived
        # into original_metadata; if a caller invalidated it (filter, reproject,
        # multi-file merge) it must be recomputed here or the v1.x output would
        # ship with no bbox at all.
        precomputed_bbox, precomputed_geom_types = self._precompute_metadata(
            con,
            query,
            geometry_column,
            verbose,
            should_add_geo_metadata,
            need_bbox=use_native_geometry
            or not _has_carried_bbox(original_metadata, geometry_column),
        )

        # Compute the geoarrow target type once from whole-dataset geometry types.
        # All batches will be coerced to this single physical type (schema is fixed
        # from batch 1 and cannot change mid-stream).
        geoarrow_target_type = None
        geoarrow_encoding = None
        if geoarrow_native and should_add_geo_metadata:
            from geoparquet_io.core.common import compute_geometry_dimensions_via_sql
            from geoparquet_io.core.geoarrow_encoding import determine_geoarrow_target_type

            # Detect Z/M dimensionality so 3D/measured coordinates select a
            # dimension-aware native type (or fall back to WKB) rather than being
            # silently coerced down to 2D.
            dimensions = compute_geometry_dimensions_via_sql(con, query, geometry_column)
            geoarrow_target_type, geoarrow_encoding = determine_geoarrow_target_type(
                precomputed_geom_types, input_crs, dimensions=dimensions
            )
            if verbose:
                debug(f"1.1-geoarrow target encoding: {geoarrow_encoding}")

        if verbose:
            debug(f"Executing query with batch size {batch_size:,}...")

        result = con.execute(final_sql)
        reader = result.fetch_record_batch(rows_per_batch=batch_size)
        schema = reader.schema

        if geometry_column not in schema.names:
            raise ValueError(
                f"Geometry column '{geometry_column}' not found in query result. "
                f"Available columns: {schema.names}"
            )

        # Build geo metadata. Built even for parquet-geo-only, which writes no
        # `geo` block: the native Parquet GEOMETRY types below are keyed off the
        # CRS the block declares per column, so it is what the types come from
        # before it is dropped (#848). Discarded immediately afterwards.
        geo_meta = self._build_geo_metadata_for_query(
            schema,
            geometry_column,
            original_metadata,
            input_crs,
            verbose,
            version_config["metadata_version"],
            precomputed_geom_types,
            precomputed_bbox,
            geometry_info,
            geoarrow_encoding=geoarrow_encoding,
        )
        native_crs = native_geometry_crs(
            geoparquet_version, geo_meta, geometry_column, geometry_info
        )
        if not should_add_geo_metadata:
            geo_meta = None

        # Prepare writer kwargs
        writer_kwargs = {"compression": validated_compression or "NONE"}
        if validated_level is not None and validated_compression:
            writer_kwargs["compression_level"] = validated_level

        if verbose:
            progress(f"Streaming to {output_path}...")

        # Secondary geometry columns must reach the schema builder explicitly:
        # parquet-geo-only writes no geo metadata, so there is nothing else left
        # to name them by the time the schema is built.
        geometry_columns = {geometry_column}
        if geometry_info:
            geometry_columns.update(geometry_info.get("secondary") or [])

        # Stream batches to file
        self._stream_batches_to_file(
            reader,
            output_path,
            schema,
            geometry_column,
            geo_meta,
            use_native_geometry,
            native_crs,
            precomputed_geom_types,
            writer_kwargs,
            verbose,
            extra_kv_metadata=extra_kv_metadata,
            geoarrow_target_type=geoarrow_target_type,
            geometry_columns=geometry_columns,
        )

    def _precompute_metadata(
        self,
        con: duckdb.DuckDBPyConnection,
        query: str,
        geometry_column: str,
        verbose: bool,
        should_add_geo_metadata: bool,
        need_bbox: bool,
    ) -> tuple[list[float] | None, list[str]]:
        """Pre-compute bbox and geometry types in a single scan of the query."""
        from geoparquet_io.core.geo_metadata import compute_geo_stats_via_sql

        if not should_add_geo_metadata:
            return None, []

        if verbose:
            debug("Pre-computing bbox/geometry types via SQL...")
        return compute_geo_stats_via_sql(
            con, query, geometry_column, need_bbox=need_bbox, need_geometry_types=True
        )

    def _build_geo_metadata_for_query(
        self,
        schema: pa.Schema,
        geometry_column: str,
        original_metadata: dict | None,
        input_crs: dict | None,
        verbose: bool,
        metadata_version: str,
        precomputed_geom_types: list[str],
        precomputed_bbox: list[float] | None,
        geometry_info: dict | None = None,
        geoarrow_encoding: str | None = None,
    ) -> dict:
        """Build geo metadata for query results.

        Always returns a block, even for parquet-geo-only: the caller keys the
        native Parquet GEOMETRY types off it and then drops it (#848).
        """
        from geoparquet_io.core.crs_utils import apply_output_crs
        from geoparquet_io.core.geo_metadata import create_geo_metadata

        bbox_info = _detect_bbox_column(schema)
        geo_meta = create_geo_metadata(
            original_metadata=original_metadata,
            geom_col=geometry_column,
            bbox_info=bbox_info,
            custom_metadata=None,
            verbose=verbose,
            version=metadata_version,
        )

        col_meta = geo_meta["columns"][geometry_column]
        apply_output_crs(col_meta, input_crs)
        col_meta["encoding"] = "WKB"
        col_meta["geometry_types"] = precomputed_geom_types
        if precomputed_bbox is not None:
            col_meta["bbox"] = precomputed_bbox

        # Override encoding when geoarrow_target_type resolved a native encoding.
        # geoarrow_encoding is "WKB" when mixed/unconvertible — no override needed.
        if geoarrow_encoding is not None and geoarrow_encoding != "WKB":
            col_meta["encoding"] = geoarrow_encoding

        # Handle secondary geometry columns from geometry_info
        merge_secondary_geometry_metadata(geo_meta, geometry_info)

        return geo_meta

    def _stream_batches_to_file(
        self,
        reader,
        output_path: str,
        schema: pa.Schema,
        geometry_column: str,
        geo_meta: dict | None,
        use_native_geometry: bool,
        native_crs: dict[str, dict | None],
        precomputed_geom_types: list[str],
        writer_kwargs: dict,
        verbose: bool,
        extra_kv_metadata: dict[str, str] | None = None,
        geoarrow_target_type=None,
        geometry_columns: set[str] | None = None,
    ) -> None:
        """Stream batches from reader to Parquet file."""
        first_batch = None
        try:
            first_batch = next(iter(reader))
        except StopIteration:
            self._write_empty_result(
                output_path,
                schema,
                geometry_column,
                geo_meta,
                use_native_geometry,
                native_crs,
                writer_kwargs,
                verbose,
                extra_kv_metadata=extra_kv_metadata,
                geoarrow_target_type=geoarrow_target_type,
                geometry_columns=geometry_columns,
            )
            return

        schema_with_meta = self._build_streaming_schema(
            schema=schema,
            geometry_column=geometry_column,
            geo_meta=geo_meta,
            use_native_geometry=use_native_geometry,
            native_crs=native_crs,
            geom_types=precomputed_geom_types,
            verbose=verbose,
            extra_kv_metadata=extra_kv_metadata,
            geoarrow_target_type=geoarrow_target_type,
            geometry_columns=geometry_columns,
        )

        geoarrow_type = None
        if use_native_geometry:
            col_index = schema_with_meta.get_field_index(geometry_column)
            geoarrow_type = schema_with_meta.field(col_index).type

        rows_written, batches_written = self._write_all_batches(
            output_path,
            schema_with_meta,
            first_batch,
            reader,
            geometry_column,
            geoarrow_type,
            use_native_geometry,
            writer_kwargs,
            verbose,
            geoarrow_target_type=geoarrow_target_type,
        )

        if verbose:
            success(f"Wrote {rows_written:,} rows in {batches_written} row groups")

    def _write_empty_result(
        self,
        output_path: str,
        schema: pa.Schema,
        geometry_column: str,
        geo_meta: dict | None,
        use_native_geometry: bool,
        native_crs: dict[str, dict | None],
        writer_kwargs: dict,
        verbose: bool,
        extra_kv_metadata: dict[str, str] | None = None,
        geoarrow_target_type=None,
        geometry_columns: set[str] | None = None,
    ) -> None:
        """Handle writing an empty result set."""
        schema_with_meta = self._build_streaming_schema(
            schema=schema,
            geometry_column=geometry_column,
            geo_meta=geo_meta,
            use_native_geometry=use_native_geometry,
            native_crs=native_crs,
            geom_types=[],
            verbose=verbose,
            extra_kv_metadata=extra_kv_metadata,
            geoarrow_target_type=geoarrow_target_type,
            geometry_columns=geometry_columns,
        )
        with pq.ParquetWriter(output_path, schema_with_meta, **writer_kwargs):
            pass
        if verbose:
            success("Wrote 0 rows (empty result)")

    def _write_all_batches(
        self,
        output_path: str,
        schema_with_meta: pa.Schema,
        first_batch: pa.RecordBatch,
        reader,
        geometry_column: str,
        geoarrow_type,
        use_native_geometry: bool,
        writer_kwargs: dict,
        verbose: bool,
        geoarrow_target_type=None,
    ) -> tuple[int, int]:
        """Write all batches to Parquet file. Returns (rows_written, batches_written)."""
        rows_written = 0
        batches_written = 0

        with pq.ParquetWriter(output_path, schema_with_meta, **writer_kwargs) as writer:
            for batch in itertools.chain([first_batch], reader):
                rows, groups = self._write_batch(
                    writer,
                    batch,
                    schema_with_meta,
                    geometry_column,
                    geoarrow_type,
                    use_native_geometry,
                    geoarrow_target_type,
                )
                rows_written += rows
                batches_written += groups

                if verbose and batches_written % 10 == 0:
                    debug(f"Written {rows_written:,} rows in {batches_written} batches...")

        return rows_written, batches_written

    def _write_batch(
        self,
        writer: pq.ParquetWriter,
        batch: pa.RecordBatch,
        schema_with_meta: pa.Schema,
        geometry_column: str,
        geoarrow_type,
        use_native_geometry: bool,
        geoarrow_target_type,
    ) -> tuple[int, int]:
        """Convert one batch to the output schema and write it.

        Returns ``(rows_written, row_groups_written)`` — normally one row group,
        but a batch holding more than 2 GB of WKB is split so each part fits
        Arrow's 32-bit binary offsets.
        """
        rows_written = 0
        groups_written = 0
        for offset, length in _binary_cast_spans(batch, schema_with_meta):
            part = batch if length == batch.num_rows else batch.slice(offset, length)
            if use_native_geometry:
                part = self._convert_batch_to_geoarrow(
                    part, geometry_column, geoarrow_type, schema_with_meta
                )
            elif geoarrow_target_type is not None:
                part = self._convert_batch_to_native_geoarrow(
                    part, geometry_column, geoarrow_target_type, schema_with_meta
                )
            else:
                part = self._coerce_batch_to_schema(part, schema_with_meta)
            writer.write_table(pa.Table.from_batches([part], schema=schema_with_meta))
            rows_written += part.num_rows
            groups_written += 1
        return rows_written, groups_written

    def _coerce_batch_to_schema(self, batch: pa.RecordBatch, schema: pa.Schema) -> pa.RecordBatch:
        """Rebuild a batch against the canonical output schema (WKB → plain binary)."""
        columns = [
            _coerce_column(batch.column(index), field.type) for index, field in enumerate(schema)
        ]
        return pa.RecordBatch.from_arrays(columns, schema=schema)

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
        """Write Arrow table to GeoParquet using batch streaming."""
        from geoparquet_io.core.common import (
            _compute_geometry_types,
            _detect_version_from_table,
        )
        from geoparquet_io.core.crs_utils import apply_output_crs
        from geoparquet_io.core.geo_metadata import GEOPARQUET_VERSIONS, create_geo_metadata

        configure_verbose(verbose)
        self._validate_output_path(output_path)

        # The facade has the last word on the output's file-level keys, and it
        # runs before the non-geo early return below -- which is exactly the
        # return that wrote a carried `geo` key through under an explicit
        # parquet-geo-only request (#773). `_build_streaming_schema` starts from
        # this table's metadata, so the same call covers the geometry path,
        # where a parquet-geo-only write used to keep the input's block too.
        table = apply_output_kv_metadata(table, geoparquet_version, extra_kv_metadata)

        validated_compression, validated_level, _ = validate_compression_settings(
            compression or "ZSTD", compression_level, verbose
        )
        if validated_compression == "UNCOMPRESSED":
            validated_compression = None

        batch_size = row_group_rows or DEFAULT_BATCH_SIZE

        # Handle non-geo tables: write plain Parquet without geo metadata
        if geometry_column is None:
            self._write_plain_parquet_from_table(
                table, output_path, validated_compression, validated_level, batch_size, verbose
            )
            return

        # Auto-detect version from table schema metadata if not specified
        effective_version = geoparquet_version
        if effective_version is None:
            effective_version = _detect_version_from_table(table, verbose)

        version_config = GEOPARQUET_VERSIONS.get(effective_version, GEOPARQUET_VERSIONS["1.1"])
        metadata_version = version_config["metadata_version"]

        use_native_geometry = effective_version in ("2.0", "parquet-geo-only")
        should_add_geo_metadata = effective_version != "parquet-geo-only"
        geoarrow_native = effective_version == "1.1-geoarrow"

        # Compute the geoarrow target type once from the whole-table geometry types,
        # so every batch is coerced to one physical type (schema is fixed up front).
        geoarrow_target_type = None
        geoarrow_encoding = None

        geom_types: list[str] = []
        if should_add_geo_metadata:
            geom_types = _compute_geometry_types(table, geometry_column, verbose)

            if geoarrow_native:
                from geoparquet_io.core.geoarrow_encoding import (
                    detect_wkb_dimensions,
                    determine_geoarrow_target_type,
                )

                # Detect Z/M dimensionality from the actual geometry so 3D/measured
                # coordinates select a dimension-aware native type (or fall back to
                # WKB) instead of being silently coerced down to 2D.
                dimensions = detect_wkb_dimensions(table.column(geometry_column))
                geoarrow_target_type, geoarrow_encoding = determine_geoarrow_target_type(
                    geom_types, input_crs, dimensions=dimensions
                )
                if verbose:
                    debug(f"1.1-geoarrow target encoding: {geoarrow_encoding}")

        # Built for every version, parquet-geo-only included: it writes no `geo`
        # block, but the native Parquet GEOMETRY types are keyed off the CRS this
        # resolves per column, so it is built first and dropped after (#848).
        geo_meta = create_geo_metadata(
            original_metadata=None,
            geom_col=geometry_column,
            bbox_info={"has_bbox_column": False, "bbox_column_name": None},
            custom_metadata=custom_metadata,
            verbose=verbose,
            version=metadata_version,
        )
        geo_meta["columns"][geometry_column]["encoding"] = "WKB"
        geo_meta["columns"][geometry_column]["geometry_types"] = geom_types

        apply_output_crs(geo_meta["columns"][geometry_column], input_crs)

        # Override encoding when geoarrow_target_type resolved a native encoding.
        # geoarrow_encoding is "WKB" for mixed/unconvertible geometry — no override.
        if geoarrow_encoding is not None and geoarrow_encoding != "WKB":
            geo_meta["columns"][geometry_column]["encoding"] = geoarrow_encoding

        native_crs = native_geometry_crs(effective_version, geo_meta, geometry_column)
        if not should_add_geo_metadata:
            geo_meta = None

        schema_with_meta = self._build_streaming_schema(
            schema=table.schema,
            geometry_column=geometry_column,
            geo_meta=geo_meta,
            use_native_geometry=use_native_geometry,
            native_crs=native_crs,
            geom_types=geo_meta["columns"][geometry_column]["geometry_types"] if geo_meta else [],
            verbose=verbose,
            extra_kv_metadata=extra_kv_metadata,
            geoarrow_target_type=geoarrow_target_type,
        )

        writer_kwargs = {"compression": validated_compression or "NONE"}
        if validated_level is not None and validated_compression:
            writer_kwargs["compression_level"] = validated_level

        geoarrow_type = None
        if use_native_geometry:
            col_index = schema_with_meta.get_field_index(geometry_column)
            geoarrow_type = schema_with_meta.field(col_index).type

        rows_written = 0
        batches_written = 0

        with pq.ParquetWriter(output_path, schema_with_meta, **writer_kwargs) as writer:
            for batch in table.to_batches(max_chunksize=batch_size):
                rows, groups = self._write_batch(
                    writer,
                    batch,
                    schema_with_meta,
                    geometry_column,
                    geoarrow_type,
                    use_native_geometry,
                    geoarrow_target_type,
                )
                rows_written += rows
                batches_written += groups

        if verbose:
            success(f"Wrote {rows_written:,} rows in {batches_written} row groups")

    def _build_streaming_schema(
        self,
        schema: pa.Schema,
        geometry_column: str,
        geo_meta: dict | None,
        use_native_geometry: bool,
        native_crs: dict[str, dict | None],
        geom_types: list[str],
        verbose: bool,
        extra_kv_metadata: dict[str, str] | None = None,
        geoarrow_target_type=None,
        geometry_columns: set[str] | None = None,
    ) -> pa.Schema:
        """Build the output schema for streaming write.

        ``native_crs`` maps each geometry column to the CRS its ``geo`` entry
        declares, from ``native_geometry_crs``; empty when the target version
        writes plain BYTE_ARRAY WKB. It replaces the ``input_crs`` this used to
        take: a caller only supplies one when it is *changing* the CRS, so on
        every other write the primary column was typed with ``None`` while the
        block kept the source's EPSG:3857 -- the disagreement in #848.
        """
        schema_metadata = dict(schema.metadata or {})

        geo_columns = set(geometry_columns or ())
        geo_columns.add(geometry_column)
        geo_columns.update((geo_meta or {}).get("columns", {}))

        if use_native_geometry:
            # EVERY geometry column becomes native, not just the primary: 2.0
            # validation requires a native Parquet GEOMETRY type for each column in
            # geo["columns"], and under parquet-geo-only (which writes no geo
            # metadata) the logical type is a column's only geometry identity.
            # Each column's CRS comes from the geo entry built for that same
            # column, so the type and the block cannot disagree (#848).
            if verbose:
                with_crs = sorted(name for name, crs in native_crs.items() if crs)
                debug(f"Native Parquet GEOMETRY type for {sorted(geo_columns)}, CRS on {with_crs}")
            new_fields = [
                pa.field(field.name, native_wkb_type(native_crs.get(field.name)))
                if field.name in geo_columns
                else field
                for field in schema
            ]
            schema = pa.schema(new_fields)
        elif geoarrow_target_type is not None:
            # 1.1-geoarrow path: replace geometry field with the native GeoArrow extension type.
            # Only triggers when geometry is WKB (binary); already-native inputs are not
            # routed here (Task 3 keeps them on duckdb-kv).
            new_fields = [
                pa.field(geometry_column, geoarrow_target_type) if f.name == geometry_column else f
                for f in schema
            ]
            schema = pa.schema(new_fields)

        # Every geometry column NOT written natively above gets the canonical
        # plain-binary WKB carrier, so the file no longer depends on whether
        # geoarrow.pyarrow happens to be imported. For 1.0/1.1 that is all of them.
        if use_native_geometry:
            native_columns = geo_columns
        elif geoarrow_target_type is not None:
            native_columns = {geometry_column}
        else:
            native_columns = set()
        schema = canonicalize_wkb_fields(schema, geo_columns, native_columns=native_columns)

        if geo_meta is not None:
            if "geometry_types" not in geo_meta["columns"].get(geometry_column, {}):
                geo_meta["columns"][geometry_column]["geometry_types"] = geom_types
            schema_metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")

        if extra_kv_metadata:
            for key, value in extra_kv_metadata.items():
                bkey = key.encode("utf-8") if isinstance(key, str) else key
                bval = value.encode("utf-8") if isinstance(value, str) else value
                schema_metadata[bkey] = bval

        return schema.with_metadata(schema_metadata)

    def _convert_batch_to_geoarrow(
        self,
        batch: pa.RecordBatch,
        geometry_column: str,
        geoarrow_type,
        schema_with_geoarrow: pa.Schema,
    ) -> pa.RecordBatch:
        """Convert every native geometry column of a batch to its geoarrow type.

        Driven by the target schema rather than by the primary column name, so
        secondary geometry columns are converted too — 2.0 requires a native type
        on each of them, and leaving one as plain binary produces a file that
        fails validation deterministically (#688 review).
        """
        new_columns = [
            to_geoarrow_column(batch.column(i), field.type)
            if isinstance(field.type, pa.ExtensionType)
            else _coerce_column(batch.column(i), field.type)
            for i, field in enumerate(schema_with_geoarrow)
        ]

        return pa.RecordBatch.from_arrays(new_columns, schema=schema_with_geoarrow)

    def _convert_batch_to_native_geoarrow(
        self,
        batch: pa.RecordBatch,
        geometry_column: str,
        geoarrow_target_type,
        schema_with_meta: pa.Schema,
    ) -> pa.RecordBatch:
        """Convert WKB binary geometry column to native GeoArrow nested type (1.1-geoarrow path).

        Only converts when the incoming column is binary/large_binary (WKB) or a
        geoarrow.wkb extension type. If it is already a nested GeoArrow type, it is
        left untouched — though this path should not normally be reached because Task 3
        routing sends already-native inputs to the duckdb-kv strategy instead.
        """
        from geoparquet_io.core.geoarrow_encoding import wkb_array_to_geoarrow

        col_index = batch.schema.get_field_index(geometry_column)
        geom_col = batch.column(col_index)

        col_type = batch.schema.field(geometry_column).type
        is_wkb_binary = pa.types.is_binary(col_type) or pa.types.is_large_binary(col_type)
        is_wkb_extension = (
            isinstance(col_type, pa.ExtensionType)
            and getattr(col_type, "extension_name", "") == "geoarrow.wkb"
        )

        if is_wkb_binary or is_wkb_extension:
            converted = wkb_array_to_geoarrow(geom_col, geoarrow_target_type)
            cols = [
                converted
                if i == col_index
                else _coerce_column(batch.column(i), schema_with_meta.field(i).type)
                for i in range(batch.num_columns)
            ]
            return pa.RecordBatch.from_arrays(cols, schema=schema_with_meta)

        # Already native or unexpected type — leave untouched but cast schema
        return pa.RecordBatch.from_arrays(
            [
                _coerce_column(batch.column(i), schema_with_meta.field(i).type)
                for i in range(batch.num_columns)
            ],
            schema=schema_with_meta,
        )

    def _write_plain_parquet_from_table(
        self,
        table: pa.Table,
        output_path: str,
        compression: str | None,
        compression_level: int | None,
        batch_size: int,
        verbose: bool,
    ) -> None:
        """Write plain Parquet (no geo metadata) from an Arrow table using streaming."""
        writer_kwargs = {"compression": compression or "NONE"}
        if compression_level is not None and compression:
            writer_kwargs["compression_level"] = compression_level

        rows_written = 0
        batches_written = 0

        if verbose:
            debug(f"Writing plain Parquet in batches of {batch_size:,}...")

        with pq.ParquetWriter(output_path, table.schema, **writer_kwargs) as writer:
            for batch in table.to_batches(max_chunksize=batch_size):
                table_batch = pa.Table.from_batches([batch], schema=table.schema)
                writer.write_table(table_batch)
                rows_written += batch.num_rows
                batches_written += 1

        if verbose:
            success(f"Wrote {rows_written:,} rows in {batches_written} row groups")

    def _write_plain_parquet_from_query(
        self,
        con: duckdb.DuckDBPyConnection,
        query: str,
        output_path: str,
        compression: str | None,
        compression_level: int | None,
        batch_size: int,
        verbose: bool,
    ) -> None:
        """Write plain Parquet (no geo metadata) from a query using streaming."""
        if verbose:
            debug(f"Executing query with batch size {batch_size:,}...")

        result = con.execute(query)
        reader = result.fetch_record_batch(rows_per_batch=batch_size)
        schema = reader.schema

        writer_kwargs = {"compression": compression or "NONE"}
        if compression_level is not None and compression:
            writer_kwargs["compression_level"] = compression_level

        rows_written = 0
        batches_written = 0

        first_batch = None
        try:
            first_batch = next(iter(reader))
        except StopIteration:
            # Empty result set
            with pq.ParquetWriter(output_path, schema, **writer_kwargs):
                pass
            if verbose:
                success("Wrote 0 rows (empty result)")
            return

        with pq.ParquetWriter(output_path, schema, **writer_kwargs) as writer:
            # Write first batch
            table_batch = pa.Table.from_batches([first_batch], schema=schema)
            writer.write_table(table_batch)
            rows_written += first_batch.num_rows
            batches_written += 1

            # Write remaining batches
            for batch in reader:
                table_batch = pa.Table.from_batches([batch], schema=schema)
                writer.write_table(table_batch)
                rows_written += batch.num_rows
                batches_written += 1

        if verbose:
            success(f"Wrote {rows_written:,} rows in {batches_written} row groups")
