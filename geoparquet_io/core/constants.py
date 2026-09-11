"""
Shared constants for geoparquet-io.

This module defines constants that are shared across multiple modules to ensure
consistency and make it easy to change values in one place.
"""

# Default column name for H3 cell IDs
DEFAULT_H3_COLUMN_NAME = "h3_cell"

# Default column name for A5 cell IDs
DEFAULT_A5_COLUMN_NAME = "a5_cell"

# Default column name for quadkey cells
DEFAULT_QUADKEY_COLUMN_NAME = "quadkey"

# Default resolution (zoom level) for quadkey generation
DEFAULT_QUADKEY_RESOLUTION = 13

# Default resolution for quadkey partitioning (prefix length)
DEFAULT_QUADKEY_PARTITION_RESOLUTION = 9

# Default column name for S2 cell IDs
DEFAULT_S2_COLUMN_NAME = "s2_cell"

# Default S2 level (comparable to H3 resolution 9, ~1.2 km² cells)
DEFAULT_S2_LEVEL = 13

# Default compression level for S2 temp file generation
DEFAULT_S2_COMPRESSION_LEVEL = 15

# Default target rows per partition for S2 auto-resolution
DEFAULT_S2_TARGET_ROWS = 100000

# Vecorel schema URLs
VECOREL_CORE_SCHEMA = "https://vecorel.org/specification/v0.1.0/schema.yaml"
VECOREL_ADMIN_SCHEMA = "https://vecorel.org/administrative-division-extension/v0.1.0/schema.yaml"
VECOREL_METRICS_SCHEMA = "https://vecorel.org/geometry-metrics-extension/v0.1.0/schema.yaml"
FIBOA_CORE_SCHEMA = "https://fiboa.org/specification/v0.3.0/schema.yaml"


def build_collection_metadata(
    schema_urls: list[str],
    existing_metadata: dict | None = None,
    collection_id: str = "default",
) -> dict[str, str]:
    """Build Parquet KV metadata with Vecorel collection schemas.

    Merges the given schema URLs with any existing collection metadata
    from the input file, deduplicating entries. Always includes the
    Vecorel core schema URL.

    The output follows the Vecorel GeoParquet encoding: the ``collection``
    Parquet metadata key holds a JSON object with at least ``schemas``
    (mapping collection IDs to lists of schema URLs) and ``collection``
    (the collection identifier string).

    Args:
        schema_urls: Extension schema URLs to include.
        existing_metadata: Raw Parquet KV metadata dict from the input file.
        collection_id: Collection identifier (default: "default").

    Returns:
        Dict with a "collection" key containing JSON-encoded metadata,
        ready to pass as extra_kv_metadata to write_parquet_with_metadata().
    """
    import json

    # Always include the vecorel core schema
    all_urls = [VECOREL_CORE_SCHEMA]
    for url in schema_urls:
        if url not in all_urls:
            all_urls.append(url)

    # Start from existing collection metadata if present
    collection_obj = {}
    if existing_metadata:
        raw = existing_metadata.get("collection") or existing_metadata.get(b"collection")
        if raw:
            try:
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                collection_obj = json.loads(raw)
            except (json.JSONDecodeError, AttributeError):
                pass

    # Preserve existing collection ID if set
    if "collection" in collection_obj:
        collection_id = collection_obj["collection"]
    collection_obj["collection"] = collection_id

    # Merge schema URLs into the correct collection group
    existing_schemas = collection_obj.get("schemas", {})
    existing_urls = existing_schemas.get(collection_id, [])
    for url in all_urls:
        if url not in existing_urls:
            existing_urls.append(url)
    existing_schemas[collection_id] = existing_urls
    collection_obj["schemas"] = existing_schemas

    return {"collection": json.dumps(collection_obj)}


# Vecorel-required non-nullable columns
VECOREL_NON_NULLABLE = {"id", "geometry", "admin:country_code"}


def ensure_vecorel_columns(parquet_file: str, verbose: bool = False) -> None:
    """Ensure a Parquet file has Vecorel-required columns with correct schema.

    - Adds a row-number 'id' column if one doesn't exist.
    - Sets required columns to non-nullable in the Parquet schema.
    - Converts UTC timestamps to millisecond precision.

    Rewrites the file in place.
    """
    import os
    import tempfile

    from geoparquet_io.core.common import add_computed_column
    from geoparquet_io.core.duckdb_metadata import get_column_names

    columns = get_column_names(parquet_file)

    # Add id column if missing
    if "id" not in columns:
        fd, temp_out = tempfile.mkstemp(suffix=".parquet")
        os.close(fd)
        os.unlink(temp_out)
        try:
            add_computed_column(
                parquet_file,
                temp_out,
                column_name="id",
                sql_expression="CAST(row_number() OVER () AS VARCHAR)",
            )
            os.replace(temp_out, parquet_file)
        finally:
            if os.path.exists(temp_out):
                os.unlink(temp_out)

    # Fix schema: nullability + timestamp precision
    # Only mark columns non-nullable if they actually exist in the file.
    # RAW path: get_column_names escapes its own argument, and pre-escaping here
    # sent `out''_gm.parquet` on to pyarrow (issue #718).
    columns = get_column_names(parquet_file)
    non_nullable = [c for c in VECOREL_NON_NULLABLE if c in columns]
    _fix_vecorel_schema(parquet_file, non_nullable)


def _fix_vecorel_schema(parquet_file: str, non_nullable_columns: list[str]) -> None:
    """Fix Parquet schema for Vecorel compliance.

    - Sets specified columns to non-nullable
    - Converts timestamp columns with tz=UTC from microseconds to milliseconds
      (Vecorel spec requires TIMESTAMP_MS)

    Preserves all existing Parquet metadata, compression, and row group structure.
    Processes one row group at a time to avoid loading the entire file into memory.

    This is a pyarrow read/write of the whole file, so it has to preserve the
    things pyarrow does not carry by default. A Parquet ``GEOMETRY``/``GEOGRAPHY``
    logical type is one: plain pyarrow reads such a column as ``binary`` and
    writes it back as ``binary``, dropping the logical type **and the CRS stored
    inside it**, which for a native-geo-only file is the only place the CRS
    lives. Importing ``geoarrow.pyarrow`` registers the extension types that make
    pyarrow materialise those columns as ``geoarrow.wkb`` instead, so the read ->
    cast -> write below round-trips the type, the CRS and a GEOGRAPHY's edge type
    unchanged.

    That import is a process-wide registration, which is why it has to happen
    *here* rather than be relied on: before #993 this function's behaviour
    depended on whether something else in the process had already imported it.
    Measured on ``gpio add geometry-metrics`` over a native-geo-only EPSG:5070
    input, the same code scored ``0 failed`` under pytest (whose fixtures import
    ``geoarrow.pyarrow`` to write their files) and ``4 failed`` from a fresh CLI
    process. ``tests/test_add_preserves_native_geo.py`` runs that command in a
    subprocess for exactly that reason.
    """
    import geoarrow.pyarrow  # noqa: F401  -- registers the extension types; see above
    import pyarrow as pa
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(parquet_file)
    schema = pf.schema_arrow

    # Check nullability per-column using row-group-level statistics to avoid
    # loading the full file. Fall back to scanning only if stats are missing.
    safe_non_nullable = set()
    for col_name in non_nullable_columns:
        if col_name not in schema.names:
            continue
        col_idx = schema.get_field_index(col_name)
        has_nulls = False
        stats_available = True
        for rg_idx in range(pf.metadata.num_row_groups):
            rg_col_meta = pf.metadata.row_group(rg_idx).column(col_idx)
            if not rg_col_meta.is_stats_set or rg_col_meta.statistics is None:
                stats_available = False
                break
            if rg_col_meta.statistics.null_count > 0:
                has_nulls = True
                break
        if not stats_available:
            col = pf.read_row_groups(range(pf.metadata.num_row_groups), columns=[col_name])
            has_nulls = col.column(col_name).null_count > 0
        if not has_nulls:
            safe_non_nullable.add(col_name)

    needs_fix = False
    new_fields = []
    for field in schema:
        new_field = field
        if field.name in safe_non_nullable and field.nullable:
            new_field = new_field.with_nullable(False)
            needs_fix = True
        if (
            pa.types.is_timestamp(field.type)
            and field.type.tz is not None
            and field.type.unit != "ms"
        ):
            new_field = new_field.with_type(pa.timestamp("ms", tz=field.type.tz))
            needs_fix = True
        new_fields.append(new_field)

    if not needs_fix:
        pf.close()
        return

    new_schema = pa.schema(new_fields, metadata=schema.metadata)

    import os
    import tempfile

    # Detect compression from the first row group's first column
    rg0 = pf.metadata.row_group(0)
    original_compression = rg0.column(0).compression.lower()
    compression_map = {
        "snappy": "SNAPPY",
        "gzip": "GZIP",
        "brotli": "BROTLI",
        "lz4": "LZ4",
        "zstd": "ZSTD",
        "none": "NONE",
        "uncompressed": "NONE",
    }
    pa_compression = compression_map.get(original_compression, "ZSTD")

    fd, temp_out = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    try:
        with pq.ParquetWriter(temp_out, new_schema, compression=pa_compression) as writer:
            for rg_idx in range(pf.metadata.num_row_groups):
                rg_table = pf.read_row_group(rg_idx)
                rg_table = rg_table.cast(new_schema)
                writer.write_table(rg_table)
        # Release the read handle before replacing the source. On Windows
        # os.replace() raises PermissionError if the target is still open.
        pf.close()
        os.replace(temp_out, parquet_file)
    finally:
        if os.path.exists(temp_out):
            os.unlink(temp_out)
