"""Reconciling Arrow types across tables that should share one schema.

A paginated source (WFS, ArcGIS) infers each page's types independently, so the
same field arrives as ``int64`` on one page and ``decimal128`` on the next.
``_compute_unified_schema`` picks a type both can reach and
``_cast_table_to_schema`` moves each page onto it, handling the conversions
``pyarrow.concat_tables(promote=True)`` refuses.

Pure pyarrow: nothing here knows about geometry, GeoParquet or files.
"""


def _rebuild_array_with_type(
    chunked_array,
    new_type,
):
    """
    Rebuild a chunked array with a new extension type.

    This preserves CRS and other type metadata, unlike cast() which may reset them.
    Used when applying CRS to geoarrow geometry arrays.

    Args:
        chunked_array: PyArrow ChunkedArray to rebuild
        new_type: New PyArrow ExtensionType to apply

    Returns:
        pa.ChunkedArray: New chunked array with the new type
    """
    import pyarrow as pa

    new_chunks = []
    for chunk in chunked_array.chunks:
        new_chunk = pa.ExtensionArray.from_storage(new_type, chunk.storage)
        new_chunks.append(new_chunk)

    return pa.chunked_array(new_chunks, type=new_type)


def _type_category(arrow_type) -> str:
    """Classify a PyArrow type into a single promotion category.

    Categories are mutually exclusive, so two types in the same category are
    treated alike by the promotion rules. Returns "other" for types that have
    no special promotion handling (caller falls back to the first type).
    """
    import pyarrow as pa

    if pa.types.is_null(arrow_type):
        return "null"
    if pa.types.is_boolean(arrow_type):
        return "bool"
    if pa.types.is_integer(arrow_type):
        return "int"
    if pa.types.is_floating(arrow_type):
        return "float"
    if pa.types.is_decimal(arrow_type):
        return "decimal"
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "string"
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return "binary"
    if pa.types.is_timestamp(arrow_type):
        return "timestamp"
    return "other"


def _promote_special(type_a, type_b, cat_a: str, cat_b: str):
    """Promote null / binary / string categories. Returns None if neither applies.

    Order matters: binary is handled before string so that binary geometry
    data is never coerced to string.
    """
    import pyarrow as pa

    if cat_a == "null":
        return type_b
    if cat_b == "null":
        return type_a

    # Binary types: don't promote to string (would corrupt geometry data)
    if cat_a == "binary" or cat_b == "binary":
        if cat_a == "binary" and cat_b == "binary":
            return pa.large_binary()  # Safest binary type
        return type_a  # Can't promote binary + non-binary safely

    # String wins over numeric (safest fallback for text data)
    if cat_a == "string" or cat_b == "string":
        return pa.string()

    return None


def _promote_int_pair(type_a, type_b):
    """Promote two integer types, guarding against uint64/signed overflow."""
    import pyarrow as pa

    a_unsigned = pa.types.is_unsigned_integer(type_a)
    b_unsigned = pa.types.is_unsigned_integer(type_b)
    # uint64 mixed with signed int → float64 (avoids overflow for large uint64)
    if (a_unsigned and type_a == pa.uint64() and not b_unsigned) or (
        b_unsigned and type_b == pa.uint64() and not a_unsigned
    ):
        return pa.float64()
    return pa.int64()


def _promote_numeric_pair(type_a, type_b, cat_a: str, cat_b: str):
    """Promote bool / int / float / decimal combinations. None if not numeric."""
    import pyarrow as pa

    cats = {cat_a, cat_b}

    # bool + int → int64 (bool can safely cast to int)
    if cats == {"bool", "int"}:
        return pa.int64()

    # bool + float → float64
    if cats == {"bool", "float"}:
        return pa.float64()

    # int + int → check for unsigned overflow risk
    if cat_a == "int" and cat_b == "int":
        return _promote_int_pair(type_a, type_b)

    # Any remaining int/float/decimal mix → float64 (safest common type)
    numeric = {"int", "float", "decimal"}
    if cat_a in numeric and cat_b in numeric:
        return pa.float64()

    return None


def _promote_timestamp_pair(type_a, type_b, cat_a: str, cat_b: str):
    """Promote two timestamps to the finer unit. None if not both timestamps."""
    if cat_a != "timestamp" or cat_b != "timestamp":
        return None

    # ns > us > ms > s — return the finer-grained unit (lower number = finer)
    unit_order = {"ns": 0, "us": 1, "ms": 2, "s": 3}
    if unit_order.get(type_a.unit, 99) <= unit_order.get(type_b.unit, 99):
        return type_a
    return type_b


def _promote_numeric_type(type_a, type_b):
    """
    Compute a common type that both input types can safely cast to.

    Type promotion rules (ordered by preference):
    - Same types → return as-is
    - null + any → any
    - bool + int* → int64
    - int8/16/32 + int64 → int64
    - float32 + float64 → float64
    - int* + float* → float64
    - int* + decimal128 → float64
    - decimal128 + float* → float64
    - timestamp[*] + timestamp[*] → timestamp with finest unit
    - any numeric + string → string (but not binary + string)
    - incompatible types → first type (caller handles cast errors)

    Args:
        type_a: First PyArrow type
        type_b: Second PyArrow type

    Returns:
        PyArrow type that both can safely cast to
    """
    if type_a == type_b:
        return type_a

    cat_a = _type_category(type_a)
    cat_b = _type_category(type_b)

    for handler in (_promote_special, _promote_numeric_pair, _promote_timestamp_pair):
        result = handler(type_a, type_b, cat_a, cat_b)
        if result is not None:
            return result

    # Fallback: return first type (caller handles cast errors with context)
    return type_a


def _compute_unified_schema(schemas: list):
    """
    Compute unified schema from multiple schemas with safe type promotion.

    Used when merging paginated results where different pages may have
    different inferred types for the same field (e.g., int64 vs decimal128).

    Args:
        schemas: List of PyArrow schemas to unify

    Returns:
        Unified PyArrow schema that all input schemas can safely cast to
    """
    import pyarrow as pa

    if not schemas:
        return pa.schema([])

    if len(schemas) == 1:
        return schemas[0]

    # Build field name → list of types mapping
    field_types: dict[str, list] = {}
    field_order: list[str] = []

    for schema in schemas:
        for field in schema:
            if field.name not in field_types:
                field_types[field.name] = []
                field_order.append(field.name)
            field_types[field.name].append(field.type)

    # Compute unified type for each field
    unified_fields = []
    for name in field_order:
        types = field_types[name]
        unified_type = types[0]
        for t in types[1:]:
            unified_type = _promote_numeric_type(unified_type, t)
        unified_fields.append(pa.field(name, unified_type))

    return pa.schema(unified_fields)


def _parse_time_strings(col, target_type):
    """Parse HH:MM:SS[.fff] strings into a time32/time64 array.

    PyArrow has no string → time cast, but GDAL delivers ArcGIS TimeOnly
    values as strings (ESRIJSON attributes, and all-null GeoJSON pages are
    inferred as string), so the conversion must be explicit. Whole-second
    values take the vectorized strptime path; fractional seconds fall back
    to Python's ISO parser, which strptime cannot express.

    Raises ValueError on a value that is not a time of day.
    """
    import datetime

    import pyarrow as pa
    import pyarrow.compute as pc

    parsed = pc.strptime(col, format="%H:%M:%S", unit="ms", error_is_null=True)
    unmatched = pc.and_(pc.is_valid(col), pc.is_null(parsed))
    if pc.any(unmatched).as_py():
        values = []
        for value in col.to_pylist():
            if value is None:
                values.append(None)
                continue
            try:
                values.append(datetime.time.fromisoformat(value))
            except ValueError as e:
                raise ValueError(f"{value!r} is not a time of day: {e}") from e
        return pa.array(values, type=target_type)
    return parsed.cast(target_type)


def _cast_table_to_schema(table, target_schema, *, page_info: str | None = None):
    """
    Cast a table's columns to match target schema types.

    Handles type conversions that PyArrow's concat_tables(promote=True) cannot,
    such as int64 → float64 when another table has decimal128.

    Args:
        table: PyArrow table to cast
        target_schema: Target schema with desired types
        page_info: Optional context string for error messages (e.g., "page 3")

    Returns:
        Table with columns cast to target schema types

    Raises:
        ValueError: If a column cannot be cast to the target type
    """
    import pyarrow as pa

    from geoparquet_io.core.logging_config import warn

    if table.schema.equals(target_schema):
        return table

    new_columns = []
    for field in target_schema:
        if field.name in table.column_names:
            col = table.column(field.name)
            if col.type != field.type:
                # Warn about precision loss for decimal → float64
                if pa.types.is_decimal(col.type) and pa.types.is_floating(field.type):
                    warn(
                        f"Column '{field.name}' cast from {col.type} to {field.type} "
                        f"may lose precision for large values"
                    )
                # PyArrow cannot cast int32 → timestamp directly (it needs int64 as
                # an intermediate) and raises ArrowNotImplementedError. This happens
                # when a source (e.g. DuckDB) infers an epoch-zero/null date column as
                # int32 while the target schema declares timestamp. Upcast to int64
                # first so the timestamp cast below succeeds (issue #516).
                if (
                    pa.types.is_timestamp(field.type)
                    and pa.types.is_integer(col.type)
                    and col.type.bit_width < 64
                ):
                    col = col.cast(pa.int64())
                # Cast to target type with error context. pa.cast() raises siblings
                # ArrowInvalid (bad value) and ArrowNotImplementedError (unsupported
                # conversion); catch both so neither escapes uncontextualized.
                # ValueError also covers the explicit time-string parse, which
                # PyArrow cannot do as a cast.
                try:
                    if pa.types.is_time(field.type) and (
                        pa.types.is_string(col.type) or pa.types.is_large_string(col.type)
                    ):
                        col = _parse_time_strings(col, field.type)
                    else:
                        col = col.cast(field.type, safe=True)
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError, ValueError) as e:
                    context = f" ({page_info})" if page_info else ""
                    raise ValueError(
                        f"Failed to cast column '{field.name}' from {col.type} to "
                        f"{field.type}{context}: {e}"
                    ) from e
            new_columns.append(col)
        else:
            # Missing column - create null array
            null_array = pa.nulls(table.num_rows, type=field.type)
            new_columns.append(null_array)

    return pa.table(dict(zip([f.name for f in target_schema], new_columns, strict=True)))
