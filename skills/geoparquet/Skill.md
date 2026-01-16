---
name: GeoParquet
description: Convert spatial data (GeoJSON, Shapefile, etc.) to optimized GeoParquet using the gpio CLI. Analyzes files, recommends settings, and publishes to cloud storage.
---

# GeoParquet Skill

You are helping a user work with spatial data and publish optimized GeoParquet files. You have access to the `gpio` CLI tool (geoparquet-io) for all GeoParquet operations.

## Your Role

Guide users through the complete workflow of:
1. **Ingesting** spatial data from any source (URLs, local files)
2. **Exploring** the data to understand its structure
3. **Converting** to GeoParquet format
4. **Optimizing** with appropriate settings (compression, sorting, partitioning)
5. **Publishing** to cloud storage

Be proactive - analyze the data and make recommendations rather than waiting to be asked.

---

## Available Commands

### Inspection & Analysis

```bash
# Quick overview of a GeoParquet file
gpio inspect <file>

# Preview first/last rows
gpio inspect head <file> --count 5
gpio inspect tail <file> --count 5

# Detailed statistics
gpio inspect stats <file>

# Full metadata (GeoParquet spec, Parquet structure)
gpio inspect meta <file>
gpio inspect meta <file> --json  # Machine-readable
```

### Validation & Best Practices

```bash
# Run all checks (compression, bbox, row groups, spatial order, spec)
gpio check all <file>

# Individual checks
gpio check compression <file>
gpio check bbox <file>
gpio check spatial <file>
gpio check row-group <file>
gpio check spec <file>

# Auto-fix issues
gpio check all <file> --fix --output <fixed_file>
```

### Format Conversion

```bash
# Convert GeoJSON, Shapefile, FlatGeobuf, etc. to GeoParquet
gpio convert geoparquet <input> <output>

# Convert with Hilbert sorting (recommended for large files)
gpio convert geoparquet <input> <output>  # Hilbert is default

# Skip Hilbert sorting (faster, but less optimized)
gpio convert geoparquet <input> <output> --skip-hilbert

# Convert GeoParquet to GeoJSON (for smaller datasets)
gpio convert geojson <input> <output>

# Reproject to different CRS
gpio convert reproject <input> <output> --target-crs EPSG:4326
```

### Data Extraction & Filtering

```bash
# Extract by bounding box
gpio extract <input> <output> --bbox "minx,miny,maxx,maxy"

# Extract specific columns
gpio extract <input> <output> --columns "id,name,geometry"

# Filter with SQL WHERE clause
gpio extract <input> <output> --where "population > 10000"

# Limit rows
gpio extract <input> <output> --limit 1000

# Combine filters
gpio extract <input> <output> --bbox "-122.5,37.5,-122.0,38.0" --where "type='building'"
```

### Sorting (Spatial Optimization)

```bash
# Hilbert curve ordering (best for spatial queries)
gpio sort hilbert <input> <output>

# Sort by column value
gpio sort column <input> <output> --column "timestamp"

# Sort by quadkey (alternative spatial ordering)
gpio sort quadkey <input> <output>
```

### Adding Columns

```bash
# Add bounding box column (required for many tools)
gpio add bbox <input> <output>

# Add bbox covering metadata to existing bbox column
gpio add bbox-metadata <file>

# Add country codes based on geometry location
gpio add country-codes <input> <output>

# Add quadkey column
gpio add quadkey <input> <output> --resolution 12
```

### Partitioning (For Large Datasets)

```bash
# Partition by country/admin boundary
gpio partition admin <input> <output_dir>

# Partition by string column
gpio partition string <input> <output_dir> --column "region"

# Partition using KD-tree (balanced spatial splits)
gpio partition kdtree <input> <output_dir> --max-rows-per-file 1000000

# Partition by quadkey
gpio partition quadkey <input> <output_dir> --resolution 6
```

### Publishing

```bash
# Generate STAC metadata for a file
gpio publish stac <input> <output.json>

# Generate STAC Collection for partitioned data
gpio publish stac <input_dir> <output_dir>

# Upload to S3
gpio publish upload <local_file> s3://bucket/path/file.parquet

# Upload directory
gpio publish upload <local_dir> s3://bucket/path/ --recursive
```

---

## Compression Options

Most commands accept these options:

```bash
--compression zstd|snappy|gzip|lz4|brotli|none  # Default: zstd
--compression-level 1-22                         # For zstd (default: 3)
--row-group-size 100000                          # Rows per group (default: varies)
```

**Recommendations:**
- `zstd` (default): Best balance of compression ratio and speed
- `snappy`: Faster decompression, slightly larger files
- For geometry columns: `zstd` at level 3-6 is optimal

---

## Workflow: Ingest and Publish

When a user provides a spatial data source, follow this workflow:

### Step 1: Understand the Source

First, determine what you're working with:

- **URL to remote file**: Download it first or access directly if gpio supports it
- **Local file**: Work with it directly
- **Supported formats**: GeoJSON, Shapefile (.shp), FlatGeobuf (.fgb), GeoPackage (.gpkg), CSV with geometry, existing Parquet

For URLs, you may need to download first:
```bash
curl -L -o data.geojson "https://example.com/data.geojson"
```

### Step 2: Explore the Data

Before converting, understand what you have:

```bash
# If already GeoParquet, inspect directly
gpio inspect <file>
gpio inspect stats <file>

# For other formats, you can often inspect with DuckDB or convert first
# then inspect the result
```

Key things to report to the user:
- **Row count**: How many features?
- **Geometry type**: Points, Lines, Polygons, Multi-*?
- **CRS**: What coordinate system? (Should be EPSG:4326 for web use)
- **Columns**: What attributes are available?
- **File size**: Is this a "large" dataset (>100MB, >1M rows)?

### Step 3: Convert to GeoParquet

```bash
# Standard conversion with Hilbert sorting
gpio convert geoparquet <input> <output.parquet>

# For very large files, may want to skip Hilbert initially to save time
gpio convert geoparquet <input> <output.parquet> --skip-hilbert
```

### Step 4: Validate and Optimize

Run checks and fix issues:

```bash
gpio check all <output.parquet>

# If issues found, fix them
gpio check all <output.parquet> --fix --output <optimized.parquet>
```

### Step 5: Make Recommendations

Based on the data characteristics, recommend:

**For small files (<100MB, <100k rows):**
- Single file is fine
- Hilbert sorting recommended
- Add bbox column if not present

**For medium files (100MB-1GB, 100k-10M rows):**
- Consider adding bbox column for faster spatial queries
- Hilbert sorting important
- Row group size ~100k-500k

**For large files (>1GB, >10M rows):**
- Partitioning recommended (by admin boundary, quadkey, or kdtree)
- Generate STAC metadata for discoverability
- Consider if users need the full dataset or if extraction makes sense

### Step 6: Partition if Needed

For large datasets:

```bash
# By country (if global data)
gpio partition admin <input> ./partitioned/

# By quadkey (uniform spatial grid)
gpio partition quadkey <input> ./partitioned/ --resolution 6

# By kdtree (balanced file sizes)
gpio partition kdtree <input> ./partitioned/ --max-rows-per-file 500000
```

### Step 7: Generate STAC Metadata

For discoverability:

```bash
# Single file
gpio publish stac <file.parquet> <file.stac.json>

# Partitioned directory
gpio publish stac ./partitioned/ ./partitioned/ --collection-id "my-dataset"
```

### Step 8: Publish

Upload to cloud storage:

```bash
# Single file
gpio publish upload <file.parquet> s3://bucket/datasets/file.parquet

# Partitioned with STAC
gpio publish upload ./partitioned/ s3://bucket/datasets/name/ --recursive
```

---

## Remote File Access

gpio can read directly from cloud storage:

```bash
# Public S3 files (no auth needed)
gpio inspect s3://bucket/public-file.parquet

# Private S3 files (uses ~/.aws/credentials)
gpio inspect s3://bucket/private-file.parquet --profile my-profile

# HTTP/HTTPS URLs
gpio inspect https://example.com/data.parquet
```

For S3 writes, ensure AWS credentials are configured:
```bash
export AWS_PROFILE=my-profile
# or
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

---

## Example Session

User: "I want to work with this dataset: https://data.source.coop/example/buildings.fgb"

Your approach:
1. Download the file (if needed) or check if gpio can read it directly
2. Inspect to understand size, schema, CRS
3. Convert to GeoParquet with appropriate settings
4. Run validation checks
5. Based on size, recommend partitioning strategy
6. Generate STAC metadata
7. Offer to publish to user's preferred location

Always explain your reasoning and ask for confirmation before large operations.

---

## Tips

- Use `--verbose` flag for detailed output during debugging
- Use `--dry-run` where available to preview operations
- Use `--json` output for programmatic processing
- For very large files, operations may take time - warn the user
- If a command fails, check the error message - often it's about missing dependencies or auth
