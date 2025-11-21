# Uploading to Cloud Storage

The `upload` command uploads GeoParquet files to cloud object storage (S3, GCS, Azure) with parallel transfers and progress tracking.

## Basic Usage

```bash
# Single file to S3
gpio upload input.parquet s3://bucket/path/output.parquet --profile my-profile

# Directory to S3
gpio upload data/ s3://bucket/dataset/ --profile my-profile
```

## Supported Destinations

Provider support via URL scheme:

- **AWS S3** - `s3://bucket/path/`
- **Google Cloud Storage** - `gs://bucket/path/`
- **Azure Blob Storage** - `az://account/container/path/`
- **HTTP stores** - `https://...`

## Authentication

### AWS S3

Use AWS profiles configured in `~/.aws/credentials`:

```bash
gpio upload data.parquet s3://bucket/file.parquet --profile my-profile
```

Profile credentials are automatically loaded from AWS CLI configuration.

### Google Cloud Storage

Uses application default credentials. Set up with:

```bash
gcloud auth application-default login
```

### Azure Blob Storage

Uses Azure CLI credentials. Set up with:

```bash
az login
```

## Options

### Pattern Filtering

Upload only specific file types:

```bash
# Only JSON files
gpio upload data/ s3://bucket/dataset/ --pattern "*.json"

# Only Parquet files
gpio upload data/ s3://bucket/dataset/ --pattern "*.parquet"
```

### Parallel Uploads

Control concurrency for directory uploads:

```bash
# Upload 8 files in parallel (default: 4)
gpio upload data/ s3://bucket/dataset/ --max-files 8
```

Trade-off: Higher parallelism = faster uploads but more bandwidth/memory usage.

### Chunk Concurrency

Control concurrent chunks within each file:

```bash
# More concurrent chunks per file (default: 12)
gpio upload large.parquet s3://bucket/file.parquet --chunk-concurrency 20
```

### Custom Chunk Size

Override default multipart upload chunk size:

```bash
# 10MB chunks instead of default 5MB
gpio upload data.parquet s3://bucket/file.parquet --chunk-size 10485760
```

### Error Handling

By default, continues uploading remaining files if one fails:

```bash
# Stop immediately on first error
gpio upload data/ s3://bucket/dataset/ --fail-fast
```

### Dry Run

Preview what would be uploaded without actually uploading:

```bash
gpio upload data/ s3://bucket/dataset/ --dry-run
```

Shows:
- Files that would be uploaded
- Total size
- Destination paths
- AWS profile (if specified)

## Directory Structure

When uploading directories, the structure is preserved:

```bash
# Input structure:
data/
  ├── region1/
  │   ├── file1.parquet
  │   └── file2.parquet
  └── region2/
      └── file3.parquet

# After upload to s3://bucket/dataset/:
s3://bucket/dataset/region1/file1.parquet
s3://bucket/dataset/region1/file2.parquet
s3://bucket/dataset/region2/file3.parquet
```


## See Also

- [convert command](convert.md) - Convert vector formats to GeoParquet
- [check command](check.md) - Validate and fix GeoParquet files
- [partition command](partition.md) - Partition GeoParquet files
