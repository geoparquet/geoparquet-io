"""
Arrow streaming write strategy.

This strategy streams DuckDB query results as Arrow RecordBatches directly
to a ParquetWriter, maintaining constant memory usage regardless of file size.

Best for: Large files, memory-constrained environments
Memory: O(batch_size) - constant regardless of total size
Speed: Slightly slower due to batch overhead
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.logging_config import configure_verbose, debug, progress, success
from geoparquet_io.core.write_strategies.base import BaseWriteStrategy

if TYPE_CHECKING:
    import duckdb

# Default batch size for streaming (100K rows per batch)
DEFAULT_BATCH_SIZE = 100_000


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
    ) -> None:
        """Write query results to GeoParquet using streaming RecordBatch approach."""
        from geoparquet_io.core.common import (
            GEOPARQUET_VERSIONS,
            _wrap_query_with_wkb_conversion,
            compute_geometry_types_via_sql,
            create_geo_metadata,
            is_default_crs,
            validate_compression_settings,
        )

        configure_verbose(verbose)
        self._validate_output_path(output_path)

        validated_compression, validated_level, _ = validate_compression_settings(
            compression or "ZSTD", compression_level, verbose
        )
        if validated_compression == "UNCOMPRESSED":
            validated_compression = None

        version_config = GEOPARQUET_VERSIONS.get(geoparquet_version, GEOPARQUET_VERSIONS["1.1"])
        metadata_version = version_config["metadata_version"]

        batch_size = row_group_rows or DEFAULT_BATCH_SIZE

        quoted_geom = geometry_column.replace('"', '""')
        type_result = con.execute(
            f'SELECT TYPEOF("{quoted_geom}") FROM ({query}) LIMIT 1'
        ).fetchone()
        duckdb_type = type_result[0] if type_result else None
        needs_wkb_conversion = duckdb_type == "GEOMETRY"

        if verbose:
            if needs_wkb_conversion:
                debug(f"Geometry column is {duckdb_type}, wrapping with WKB conversion")
            else:
                debug(f"Geometry column is {duckdb_type}, no conversion needed")

        final_sql = query
        if needs_wkb_conversion:
            final_sql = _wrap_query_with_wkb_conversion(query, geometry_column, con)

        use_native_geometry = geoparquet_version in ("2.0", "parquet-geo-only")
        should_add_geo_metadata = geoparquet_version != "parquet-geo-only"

        precomputed_bbox = None
        precomputed_geom_types = []

        if should_add_geo_metadata:
            # Pre-compute geometry types via SQL to get accurate types across all rows
            if verbose:
                debug("Pre-computing geometry types via SQL...")
            precomputed_geom_types = compute_geometry_types_via_sql(con, query, geometry_column)

            if use_native_geometry:
                precomputed_bbox = self._compute_bbox_via_duckdb(
                    con, query, geometry_column, verbose
                )

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

        geo_meta = None
        if should_add_geo_metadata:
            bbox_info = {"has_bbox_column": False, "bbox_column_name": None}

            for name in schema.names:
                if name in ["bbox", "bounds", "extent"] or name.endswith("_bbox"):
                    field = schema.field(name)
                    if hasattr(field.type, "names") and set(field.type.names) >= {
                        "xmin",
                        "ymin",
                        "xmax",
                        "ymax",
                    }:
                        bbox_info = {"has_bbox_column": True, "bbox_column_name": name}
                        break

            geo_meta = create_geo_metadata(
                original_metadata=original_metadata,
                geom_col=geometry_column,
                bbox_info=bbox_info,
                custom_metadata=None,
                verbose=verbose,
                version=metadata_version,
            )

            if input_crs and not is_default_crs(input_crs):
                geo_meta["columns"][geometry_column]["crs"] = input_crs

            geo_meta["columns"][geometry_column]["encoding"] = "WKB"
            geo_meta["columns"][geometry_column]["geometry_types"] = precomputed_geom_types

            if precomputed_bbox is not None:
                geo_meta["columns"][geometry_column]["bbox"] = precomputed_bbox

        writer_kwargs = {"compression": validated_compression or "NONE"}
        if validated_level is not None and validated_compression:
            writer_kwargs["compression_level"] = validated_level

        if verbose:
            progress(f"Streaming to {output_path}...")

        first_batch = None
        try:
            first_batch = next(iter(reader))
        except StopIteration:
            schema_with_meta = self._build_streaming_schema(
                schema=schema,
                geometry_column=geometry_column,
                geo_meta=geo_meta,
                use_native_geometry=use_native_geometry,
                input_crs=input_crs,
                geom_types=[],
                verbose=verbose,
            )
            with pq.ParquetWriter(output_path, schema_with_meta, **writer_kwargs) as writer:
                pass
            if verbose:
                success("Wrote 0 rows (empty result)")
            return

        schema_with_meta = self._build_streaming_schema(
            schema=schema,
            geometry_column=geometry_column,
            geo_meta=geo_meta,
            use_native_geometry=use_native_geometry,
            input_crs=input_crs,
            geom_types=precomputed_geom_types,
            verbose=verbose,
        )

        geoarrow_type = None
        if use_native_geometry:
            col_index = schema_with_meta.get_field_index(geometry_column)
            geoarrow_type = schema_with_meta.field(col_index).type

        rows_written = 0
        batches_written = 0

        with pq.ParquetWriter(output_path, schema_with_meta, **writer_kwargs) as writer:
            if use_native_geometry:
                first_batch = self._convert_batch_to_geoarrow(
                    first_batch, geometry_column, geoarrow_type, schema_with_meta
                )
            table_batch = pa.Table.from_batches([first_batch], schema=schema_with_meta)
            writer.write_table(table_batch)
            rows_written += first_batch.num_rows
            batches_written += 1

            for batch in reader:
                if use_native_geometry:
                    batch = self._convert_batch_to_geoarrow(
                        batch, geometry_column, geoarrow_type, schema_with_meta
                    )
                table_batch = pa.Table.from_batches([batch], schema=schema_with_meta)
                writer.write_table(table_batch)
                rows_written += batch.num_rows
                batches_written += 1

                if verbose and batches_written % 10 == 0:
                    debug(f"Written {rows_written:,} rows in {batches_written} batches...")

        if verbose:
            success(f"Wrote {rows_written:,} rows in {batches_written} row groups")

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
    ) -> None:
        """Write Arrow table to GeoParquet using batch streaming."""
        from geoparquet_io.core.common import (
            GEOPARQUET_VERSIONS,
            _compute_geometry_types,
            create_geo_metadata,
            is_default_crs,
            validate_compression_settings,
        )

        configure_verbose(verbose)
        self._validate_output_path(output_path)

        validated_compression, validated_level, _ = validate_compression_settings(
            compression or "ZSTD", compression_level, verbose
        )
        if validated_compression == "UNCOMPRESSED":
            validated_compression = None

        version_config = GEOPARQUET_VERSIONS.get(geoparquet_version, GEOPARQUET_VERSIONS["1.1"])
        metadata_version = version_config["metadata_version"]

        batch_size = row_group_rows or DEFAULT_BATCH_SIZE
        use_native_geometry = geoparquet_version in ("2.0", "parquet-geo-only")
        should_add_geo_metadata = geoparquet_version != "parquet-geo-only"

        geo_meta = None
        if should_add_geo_metadata:
            bbox_info = {"has_bbox_column": False, "bbox_column_name": None}
            geom_types = _compute_geometry_types(table, geometry_column, verbose)

            geo_meta = create_geo_metadata(
                original_metadata=None,
                geom_col=geometry_column,
                bbox_info=bbox_info,
                custom_metadata=custom_metadata,
                verbose=verbose,
                version=metadata_version,
            )
            geo_meta["columns"][geometry_column]["encoding"] = "WKB"
            geo_meta["columns"][geometry_column]["geometry_types"] = geom_types

            if input_crs and not is_default_crs(input_crs):
                geo_meta["columns"][geometry_column]["crs"] = input_crs

        schema_with_meta = self._build_streaming_schema(
            schema=table.schema,
            geometry_column=geometry_column,
            geo_meta=geo_meta,
            use_native_geometry=use_native_geometry,
            input_crs=input_crs,
            geom_types=geo_meta["columns"][geometry_column]["geometry_types"] if geo_meta else [],
            verbose=verbose,
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
                if use_native_geometry:
                    batch = self._convert_batch_to_geoarrow(
                        batch, geometry_column, geoarrow_type, schema_with_meta
                    )
                table_batch = pa.Table.from_batches([batch], schema=schema_with_meta)
                writer.write_table(table_batch)
                rows_written += batch.num_rows
                batches_written += 1

        if verbose:
            success(f"Wrote {rows_written:,} rows in {batches_written} row groups")

    def _compute_bbox_via_duckdb(
        self,
        con: duckdb.DuckDBPyConnection,
        query: str,
        geometry_column: str,
        verbose: bool,
    ) -> list[float] | None:
        """Pre-compute bbox via DuckDB aggregation for v2.0."""
        from geoparquet_io.core.common import compute_bbox_via_sql

        if verbose:
            debug("Pre-computing bbox via DuckDB aggregation...")

        return compute_bbox_via_sql(con, query, geometry_column)

    def _build_streaming_schema(
        self,
        schema: pa.Schema,
        geometry_column: str,
        geo_meta: dict | None,
        use_native_geometry: bool,
        input_crs: dict | None,
        geom_types: list[str],
        verbose: bool,
    ) -> pa.Schema:
        """Build the output schema for streaming write."""
        from geoparquet_io.core.common import is_default_crs

        schema_metadata = dict(schema.metadata or {})

        if use_native_geometry:
            import geoarrow.pyarrow as ga

            geoarrow_type = ga.wkb()

            if input_crs and not is_default_crs(input_crs):
                geoarrow_type = geoarrow_type.with_crs(input_crs)
                if verbose:
                    debug("Built geoarrow type with CRS")

            new_fields = []
            for field in schema:
                if field.name == geometry_column:
                    new_fields.append(pa.field(geometry_column, geoarrow_type))
                else:
                    new_fields.append(field)
            schema = pa.schema(new_fields)

        if geo_meta is not None:
            if "geometry_types" not in geo_meta["columns"].get(geometry_column, {}):
                geo_meta["columns"][geometry_column]["geometry_types"] = geom_types
            schema_metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")

        return schema.with_metadata(schema_metadata)

    def _convert_batch_to_geoarrow(
        self,
        batch: pa.RecordBatch,
        geometry_column: str,
        geoarrow_type,
        schema_with_geoarrow: pa.Schema,
    ) -> pa.RecordBatch:
        """Convert a record batch's geometry column to geoarrow extension type."""
        import geoarrow.pyarrow as ga

        col_index = batch.schema.get_field_index(geometry_column)
        geom_col = batch.column(col_index)
        geoarrow_arr = ga.as_wkb(geom_col)

        if geoarrow_arr.type != geoarrow_type:
            geoarrow_arr = pa.ExtensionArray.from_buffers(
                geoarrow_type,
                len(geoarrow_arr),
                geoarrow_arr.buffers(),
            )

        new_columns = []
        for i in range(batch.num_columns):
            if i == col_index:
                new_columns.append(geoarrow_arr)
            else:
                new_columns.append(batch.column(i))

        return pa.RecordBatch.from_arrays(new_columns, schema=schema_with_geoarrow)
