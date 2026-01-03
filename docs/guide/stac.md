# STAC Generation

Generate STAC (SpatioTemporal Asset Catalog) metadata for GeoParquet datasets.

## What is STAC?

STAC is a specification for describing geospatial data with standardized metadata. It enables dataset discovery and cataloging on platforms and catalogs.

## Single File → STAC Item

Generate a STAC Item JSON for a single GeoParquet file:

```bash
gpio publish stac roads.parquet roads.json \
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
gpio publish stac partitioned/ . \
  --bucket s3://source.coop/my-org/roads/
```

Creates:

- `collection.json` - Overall dataset metadata in output directory
- `partitioned/usa.json`, `can.json`, etc. - Per-partition Items **co-located with data**

**STAC Best Practice:** Items are written alongside their parquet files, not in a separate directory. This follows the STAC principle of co-locating metadata with data for better organization and discoverability.

## Public URL Mapping

Convert S3 URIs to public HTTPS URLs:

```bash
gpio publish stac data.parquet output.json \
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

**Create PMTiles overview:**

Use [tippecanoe](https://github.com/felt/tippecanoe) to create PMTiles from your vector data.

**Standard naming:** Use `overview.pmtiles` for consistency.

## Overwriting Existing STAC Files

If the output location already contains a valid STAC Collection or Item, the command will error to prevent accidental overwrites:

```bash
# Error if output already exists
gpio publish stac data.parquet output.json --bucket s3://...

# Use --overwrite to allow overwriting
gpio publish stac data.parquet output.json --bucket s3://... --overwrite
```

**Note:** The command will error if the **input** is a pure STAC file (no parquet files). If the input directory contains both STAC files and parquet files, it will generate from the parquet files.

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

# 3. Create PMTiles overview (optional, see https://github.com/felt/tippecanoe)

# 4. Generate STAC collection
# Items written next to parquet files, collection.json in partitioned/
gpio publish stac partitioned/ partitioned/ \
  --bucket s3://my-bucket/roads/ \
  --public-url https://data.example.com/roads/

# 5. Validate
gpio check stac partitioned/collection.json

# 6. Upload to S3 (external)
# Single sync uploads both data and metadata together
aws s3 sync partitioned/ s3://my-bucket/roads/
```

**Directory structure after step 4:**
```
partitioned/
├── collection.json          # Collection metadata
├── overview.pmtiles         # Optional overview
├── usa.parquet
├── usa.json                 # STAC Item for USA
├── can.parquet
├── can.json                 # STAC Item for Canada
└── ...
```

## Options

### Custom IDs

```bash
# Custom Item ID
gpio publish stac data.parquet output.json \
  --item-id my-roads \
  --bucket s3://...

# Custom Collection ID
gpio publish stac partitions/ output/ \
  --collection-id global-roads \
  --bucket s3://...
```

### Verbose Output

```bash
gpio publish stac data.parquet output.json \
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
- **Links** - Self link, and collection link (for items in collections)

## Best Practices

1. **Co-locate metadata with data** - Items are automatically written alongside parquet files
2. **Use consistent naming** - `overview.pmtiles` for PMTiles files
3. **Validate before publishing** - Run `gpio check stac` before upload
4. **Include PMTiles** - Enables interactive map visualization
5. **Use public URLs** - Map S3 URIs to HTTPS with `--public-url` for web access
6. **Custom IDs** - Use meaningful IDs for better discoverability
7. **Single directory uploads** - With co-located metadata, upload data and STAC files together
