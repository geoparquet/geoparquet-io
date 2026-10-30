# Geometry Metadata Handling

This document describes how geometry metadata (especially CRS) is stored and accessed in GeoParquet files, and the complexities that arise from different library interactions.

## Background: Where CRS Lives

GeoParquet files can store CRS (Coordinate Reference System) in multiple locations:

1. **GeoParquet 'geo' metadata** - JSON blob in file-level key-value metadata
2. **Arrow extension metadata** - `ARROW:extension:metadata` on the geometry field
3. **Parquet logical type** - Native `Geometry(crs=...)` logical type (GeoParquet 2.0+)

Code that reads CRS must handle all these locations.

---

## GeoArrow-PyArrow Extension Type Behavior

### The Issue

When `geoarrow-pyarrow` is imported, it registers custom PyArrow extension types that fundamentally change how geometry metadata is exposed. The library "consumes" the Arrow extension metadata and exposes it through its own API.

**Without geoarrow-pyarrow:**
```python
field.metadata[b'ARROW:extension:metadata']  # Contains {"crs": {...}}
field.type  # Just binary or large_binary
```

**With geoarrow-pyarrow:**
```python
field.metadata[b'ARROW:extension:metadata']  # Empty or missing!
field.type  # WkbType with .crs attribute
field.type.crs.to_json_dict()  # Contains the CRS
```

### Why This Matters

- CI environments often have geoarrow-pyarrow installed
- Local development may not
- Code that only checks `field.metadata` will silently return `None` for CRS when geoarrow-pyarrow is present
- This was part of what motivated the original shift from PyArrow to DuckDB for metadata reading

### Solution Pattern

Check for CRS in both locations, with `field.type.crs` taking priority:

```python
def get_crs_from_field(field) -> dict | None:
    # Case 1: geoarrow-pyarrow is imported - CRS is in field.type.crs
    # This takes priority because geoarrow-pyarrow consumes the metadata
    if hasattr(field.type, "crs") and field.type.crs is not None:
        crs_obj = field.type.crs
        if hasattr(crs_obj, "to_json_dict"):
            return crs_obj.to_json_dict()

    # Case 2: Standard Arrow - CRS is in extension metadata
    if hasattr(field.type, "extension_metadata") and field.type.extension_metadata:
        try:
            ext_meta = json.loads(field.type.extension_metadata)
            if "crs" in ext_meta:
                return ext_meta["crs"]
        except (json.JSONDecodeError, KeyError):
            pass

    return None
```

### Files Affected

- `geoparquet_io/core/duckdb_metadata.py`:
  - `_get_pyarrow_logical_type()`
  - `_pyarrow_get_schema_info()`

---

## SRID-Format CRS Limitations

### The Issue

`geoarrow-pyarrow` cannot deserialize certain CRS formats, particularly SRID references like `srid:5070`. When encountering these, it raises:

```
ValueError: Can't create geoarrow.types.Crs from 5070
```

This happens because geoarrow-pyarrow expects PROJJSON format for CRS, not simple SRID references.

### Solution

Catch this specific error and fall back to DuckDB, which can handle SRID-format CRS:

```python
except ValueError as e:
    if "Can't create geoarrow.types.Crs" in str(e):
        return None  # Signal to fall back to DuckDB
    raise
```

---

## DuckDB vs PyArrow for Metadata Reading

### Trade-offs

| Aspect | PyArrow | DuckDB |
|--------|---------|--------|
| Speed (local files) | ~0.1ms | ~44ms |
| CRS handling | Complex (library-dependent) | Robust |
| Remote files | No auth support | Full httpfs support |
| SRID CRS | Depends on geoarrow-pyarrow | Full support |

### Current Strategy

1. **Local files**: Use PyArrow fast-path with geoarrow-pyarrow handling
2. **Remote files**: Use DuckDB (required for S3 auth, HTTP)
3. **CRS edge cases**: Fall back to DuckDB when PyArrow can't handle the format

### References

- PR #233 (inspect metadata performance)
- Issue #232 (performance regression)
- Historical issues with CRS preservation that motivated DuckDB usage
