# STAC Generation

The `gpio publish stac` command generates [STAC (SpatioTemporal Asset Catalog)](https://stacspec.org/) metadata from GeoParquet files, making your data discoverable and interoperable.

## Quick Start

=== "CLI"

    <!-- doctest: skip="needs cloud credentials" -->
    ```bash
    # Single file -> STAC Item
    gpio publish stac data.parquet output.json --bucket s3://my-bucket/data/

    # Partitioned directory -> STAC Collection + Items
    gpio publish stac partitions/ . --bucket s3://my-bucket/dataset/
    ```

=== "Python"

    ```python
    from geoparquet_io.api.stac import generate_stac

    # Generate STAC Item
    generate_stac(
        "data.parquet",
        "output.json",
        bucket="s3://my-bucket/data/"
    )
    ```

## Output Modes

### Single File

For a single GeoParquet file, gpio generates a STAC Item:

```bash
gpio publish stac roads.parquet roads_item.json --bucket s3://my-bucket/roads/
```

This creates a STAC Item JSON with:
- Bounding box from the geometry column
- Asset link to the parquet file
- Properties from file metadata

### Partitioned Dataset

For partitioned directories, gpio generates a STAC Collection with Items:

<!-- doctest: skip="needs cloud credentials" -->
```bash
gpio publish stac partitions/ . --bucket s3://my-bucket/dataset/
```

This creates:
- `collection.json` in the output directory
- Individual Item JSONs co-located with each parquet file
- Links between Collection and Items

## Asset URL Configuration

### S3 Bucket Prefix

The `--bucket` option sets the S3 prefix for asset hrefs:

```bash
gpio publish stac data.parquet output.json --bucket s3://source.coop/org/dataset/
```

### Public URL Mapping

For publicly accessible data, add a public URL:

```bash
gpio publish stac data.parquet output.json \
  --bucket s3://my-bucket/roads/ \
  --public-url https://data.example.com/roads/
```

This adds alternate links with public HTTPS URLs.

## PMTiles Overview Support

GPIO automatically detects PMTiles overview files and includes them as additional assets:

```bash
# If data.pmtiles exists alongside data.parquet
gpio publish stac data.parquet output.json --bucket s3://bucket/data/
```

The STAC Item will include both the parquet and pmtiles assets.

## Custom IDs

### Item ID

```bash
gpio publish stac data.parquet output.json \
  --bucket s3://bucket/data/ \
  --item-id my-custom-item-id
```

### Collection ID

<!-- doctest: skip="needs cloud credentials" -->
```bash
gpio publish stac partitions/ . \
  --bucket s3://bucket/dataset/ \
  --collection-id my-dataset-collection
```

## Overwriting Existing Files

Use `--overwrite` to replace existing STAC files:

```bash
gpio publish stac data.parquet output.json --bucket s3://bucket/data/ --overwrite
```

## Example Workflow

Complete workflow from partition to STAC:

<!-- doctest: network -->
```bash
# 1. Partition by admin boundaries
gpio partition admin roads.parquet by_country/ --levels country

# 2. Generate STAC Collection with Items
gpio publish stac by_country/ . \
  --bucket s3://source.coop/my-org/roads/ \
  --public-url https://data.source.coop/my-org/roads/ \
  --collection-id global-roads

# 3. Upload everything including STAC metadata
gpio publish upload by_country/ s3://source.coop/my-org/roads/
```

## CLI Reference

See the [CLI Reference](../cli/stac.md) for complete options.
