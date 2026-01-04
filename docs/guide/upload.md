# Uploading to Cloud Storage

The `upload` command uploads GeoParquet files to cloud object storage (S3, GCS, Azure) with parallel transfers and progress tracking.

## Basic Usage

```bash
# Single file to S3
gpio publish upload input.parquet s3://bucket/path/output.parquet --profile my-profile

# Directory to S3
gpio publish upload data/ s3://bucket/dataset/ --profile my-profile
```

## Supported Destinations

Provider support via URL scheme:

- **AWS S3** - `s3://bucket/path/`
- **Google Cloud Storage** - `gs://bucket/path/`
- **Azure Blob Storage** - `az://account/container/path/`
- **HTTP stores** - `https://...`

## Authentication

For authentication setup, see the [Remote Files guide](remote-files.md#authentication).

**Quick reference:**
- **AWS S3**: Use `--profile` flag or set `AWS_PROFILE` env var
- **Google Cloud Storage**: Set `GOOGLE_APPLICATION_CREDENTIALS`
- **Azure**: Set `AZURE_STORAGE_ACCOUNT_NAME` and `AZURE_STORAGE_ACCOUNT_KEY`

## Options

### Pattern Filtering

Upload only specific file types:

```bash
# Only JSON files
gpio publish upload data/ s3://bucket/dataset/ --pattern "*.json"

# Only Parquet files
gpio publish upload data/ s3://bucket/dataset/ --pattern "*.parquet"
```

### Parallel Uploads

Control concurrency for directory uploads:

```bash
# Upload 8 files in parallel (default: 4)
gpio publish upload data/ s3://bucket/dataset/ --max-files 8
```

Trade-off: Higher parallelism = faster uploads but more bandwidth/memory usage.

### Chunk Concurrency

Control concurrent chunks within each file:

```bash
# More concurrent chunks per file (default: 12)
gpio publish upload large.parquet s3://bucket/file.parquet --chunk-concurrency 20
```

### Custom Chunk Size

Override default multipart upload chunk size:

```bash
# 10MB chunks instead of default 5MB
gpio publish upload data.parquet s3://bucket/file.parquet --chunk-size 10485760
```

### Error Handling

By default, continues uploading remaining files if one fails:

```bash
# Stop immediately on first error
gpio publish upload data/ s3://bucket/dataset/ --fail-fast
```

### Dry Run

Preview what would be uploaded without actually uploading:

```bash
gpio publish upload data/ s3://bucket/dataset/ --dry-run
```

Shows:
- Files that would be uploaded
- Total size
- Destination paths
- AWS profile (if specified)

### S3-Compatible Storage

Upload to MinIO, Ceph, or other S3-compatible storage:

```bash
# MinIO without SSL
gpio publish upload data.parquet s3://bucket/file.parquet \
  --s3-endpoint minio.example.com:9000 \
  --s3-no-ssl

# Custom endpoint with specific region
gpio publish upload data/ s3://bucket/dataset/ \
  --s3-endpoint storage.example.com \
  --s3-region eu-west-1
```

Options:
- `--s3-endpoint` - Custom endpoint hostname and optional port
- `--s3-region` - Override region (defaults to us-east-1 for custom endpoints)
- `--s3-no-ssl` - Use HTTP instead of HTTPS

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
