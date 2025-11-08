# STAC Generation

Generate STAC (SpatioTemporal Asset Catalog) metadata for GeoParquet datasets.

## What is STAC?

STAC is a specification for describing geospatial data with standardized metadata. It enables dataset discovery and cataloging on platforms and catalogs.

## Single File → STAC Item

Generate a STAC Item JSON for a single GeoParquet file:

```bash
gpio stac roads.parquet roads.json \
  --bucket s3://source.coop/my-org/roads/
```

Creates `roads.json` with:

- Bounding box from data
- GeoParquet asset link
- PMTiles overview (if `overview.pmtiles` exists)
- Projection information (CRS, geometry types)

## Partitioned Dataset → STAC Collection

Generate Collection + Items for partitioned datasets:

```bash
gpio stac partitioned/ stac-output/ \
  --bucket s3://source.coop/my-org/roads/
```

Creates:

- `stac-output/collection.json` - Overall dataset metadata
- `stac-output/usa.json`, `can.json`, etc. - Per-partition Items

## Public URL Mapping

Convert S3 URIs to public HTTPS URLs:

```bash
gpio stac data.parquet output.json \
  --bucket s3://my-bucket/roads/ \
  --public-url https://data.example.com/roads/
```

Use `--public-url` to map S3 bucket prefixes to public HTTPS URLs for your assets.

## PMTiles Overviews

STAC automatically detects PMTiles overview files for map visualization.

**Detection rules:**

- Exactly 1 `.pmtiles` file in directory → included as asset
- 0 files → warning, continue without overview
- >1 files → error, clean up duplicates

**Create PMTiles overview** (external tool):

```bash
tippecanoe -o partitioned/overview.pmtiles roads.parquet
```

**Standard naming:** Use `overview.pmtiles` for consistency.

## Validation

Check STAC compliance:

```bash
gpio check stac output.json
```

Validates:

- STAC spec compliance
- Required fields
- Asset href resolution (local files)
- Best practices

## End-to-End Workflow

```bash
# 1. Convert to optimized GeoParquet
gpio convert roads.geojson roads.parquet

# 2. Partition by country
gpio partition admin roads.parquet partitioned/ \
  --dataset gaul --levels country

# 3. Create PMTiles overview (optional)
tippecanoe -o partitioned/overview.pmtiles roads.parquet

# 4. Generate STAC collection
gpio stac partitioned/ stac-catalog/ \
  --bucket s3://my-bucket/roads/ \
  --public-url https://data.example.com/roads/

# 5. Validate
gpio check stac stac-catalog/collection.json

# 6. Upload to S3 (external)
aws s3 sync partitioned/ s3://my-bucket/roads/
aws s3 sync stac-catalog/ s3://my-bucket/roads/
```

## Options

### Custom IDs

```bash
# Custom Item ID
gpio stac data.parquet output.json \
  --item-id my-roads \
  --bucket s3://...

# Custom Collection ID
gpio stac partitions/ output/ \
  --collection-id global-roads \
  --bucket s3://...
```

### Verbose Output

```bash
gpio stac data.parquet output.json \
  --bucket s3://... \
  --verbose
```

## Metadata Extracted

STAC Items automatically include:

- **Bounding box** - Calculated from geometry data
- **Geometry** - GeoJSON Polygon from dataset extent
- **CRS** - From GeoParquet metadata (EPSG code, PROJJSON, or WKT)
- **Geometry types** - From GeoParquet metadata
- **Datetime** - From file modification time
- **Assets** - GeoParquet file and PMTiles overview (if present)

## Best Practices

1. **Use consistent naming** - `overview.pmtiles` for PMTiles files
2. **Validate before publishing** - Run `gpio check stac` before upload
3. **Include PMTiles** - Enables interactive map visualization
4. **Use public URLs** - Map S3 URIs to HTTPS with `--public-url` for web access
5. **Custom IDs** - Use meaningful IDs for better discoverability
