# What is GeoParquet?

GeoParquet is a standardized way to store geospatial vector data in Apache Parquet files. It combines Parquet's columnar storage efficiency with metadata conventions for geometry columns.

## Why GeoParquet?

Traditional geospatial formats have limitations:

| Format | Limitation |
|--------|------------|
| Shapefile | 2GB size limit, separate files, limited data types |
| GeoJSON | Text-based (slow, large), no streaming |
| GeoPackage | Single-file but not cloud-optimized |

GeoParquet solves these by leveraging Parquet:

- **No size limits**: Handle billions of features
- **Fast queries**: Columnar storage, predicate pushdown
- **Cloud-native**: Partial reads from S3, GCS, Azure
- **Type-rich**: Full support for complex data types
- **Compression**: Efficient storage with ZSTD, LZ4, etc.

## Key Concepts

### Geometry Column

GeoParquet files have one or more geometry columns storing spatial data. The format supports:

- Points, LineStrings, Polygons
- Multi-geometries (MultiPoint, MultiLineString, MultiPolygon)
- GeometryCollections

Geometries are stored as Well-Known Binary (WKB) encoded data.

### GeoParquet Metadata

GeoParquet adds a `geo` key to Parquet's key-value metadata containing:

```json
{
  "version": "1.1.0",
  "primary_column": "geometry",
  "columns": {
    "geometry": {
      "encoding": "WKB",
      "geometry_types": ["Polygon", "MultiPolygon"],
      "crs": { ... },
      "covering": { ... }
    }
  }
}
```

This metadata tells tools how to interpret the geometry data.

### Coordinate Reference System (CRS)

GeoParquet uses PROJJSON to define the coordinate reference system. Most data uses WGS84 (EPSG:4326), but any CRS is supported.

### Bounding Box (Bbox) Columns

A key optimization for spatial queries. Bbox columns store precomputed bounding boxes:

```
bbox: struct<xmin: double, ymin: double, xmax: double, ymax: double>
```

This enables fast spatial filtering by checking bbox overlap before expensive geometry operations.

### Covering Metadata

The `covering` metadata in GeoParquet 1.1+ tells query engines how to use bbox columns for filtering:

```json
"covering": {
  "bbox": {
    "xmin": ["bbox", "xmin"],
    "ymin": ["bbox", "ymin"],
    "xmax": ["bbox", "xmax"],
    "ymax": ["bbox", "ymax"]
  }
}
```

### Row Groups

Parquet files are divided into row groups (chunks of rows). Spatial ordering within row groups improves query performance by keeping nearby features together.

## GeoParquet Versions

### Version 1.0.0
- Initial stable release
- Core geometry column support
- CRS and encoding metadata

### Version 1.1.0 (Current)
- Added `covering` metadata for bbox columns
- Better interoperability with query engines
- Recommended for new files

### Version 2.0 / Parquet-Geo
- Native Parquet GEOMETRY and GEOGRAPHY logical types
- Geometry stored in Parquet's native format (not just WKB)
- Still emerging specification

## When to Use GeoParquet

**Use GeoParquet when:**
- Files are larger than a few hundred MB
- You need cloud-native access patterns
- Analytics and querying are primary use cases
- You're building data pipelines
- You need efficient compression

**Consider alternatives when:**
- Interoperability with legacy GIS tools is critical (use Shapefile)
- Data is small and simplicity matters (use GeoJSON)
- Editing workflows are primary use case (use GeoPackage)

## Where gpio Fits In

geoparquet-io helps you create and optimize GeoParquet files:

| Task | gpio Command |
|------|--------------|
| Convert from other formats | `gpio convert` |
| Add bbox columns | `gpio add bbox` |
| Apply spatial ordering | `gpio sort hilbert` |
| Validate best practices | `gpio check all` |
| Partition large files | `gpio partition` |

## Learn More

- [GeoParquet Specification](https://geoparquet.org/) - Official specification
- [Best Practices Guide](best-practices.md) - Optimization techniques
- [Quick Start Tutorial](../getting-started/quickstart.md) - Get started with gpio
