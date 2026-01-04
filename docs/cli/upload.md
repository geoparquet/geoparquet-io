# upload

Upload GeoParquet files to cloud object storage (S3, GCS, Azure).

## Usage

```bash
gpio publish upload SOURCE DESTINATION [OPTIONS]
```

## Arguments

- `SOURCE` - Local file or directory path
- `DESTINATION` - Object store URL (s3://, gs://, az://)

## Options

```bash
--profile TEXT              AWS profile name (S3 only)
--pattern TEXT              Glob pattern for filtering files (e.g., '*.parquet')
--max-files INTEGER         Max parallel file uploads for directories [default: 4]
--chunk-concurrency INTEGER Max concurrent chunks per file [default: 12]
--chunk-size INTEGER        Chunk size in bytes for multipart uploads
--fail-fast                 Stop immediately on first error
--s3-endpoint TEXT          Custom S3-compatible endpoint (e.g., 'minio.example.com:9000')
--s3-region TEXT            S3 region (default: us-east-1 when using custom endpoint)
--s3-no-ssl                 Disable SSL for S3 endpoint (use HTTP instead of HTTPS)
--dry-run                   Preview what would be uploaded without uploading
```

## Examples

### Single File

```bash
# Upload to S3 with AWS profile
gpio publish upload buildings.parquet s3://bucket/data/buildings.parquet --profile prod

# Upload to GCS
gpio publish upload data.parquet gs://bucket/path/data.parquet

# Upload to Azure
gpio publish upload data.parquet az://account/container/path/data.parquet
```

### Directory

```bash
# Upload all files
gpio publish upload partitions/ s3://bucket/dataset/ --profile prod

# Upload only JSON files
gpio publish upload data/ s3://bucket/json-files/ --pattern "*.json" --profile prod

# Upload with higher parallelism
gpio publish upload large-dataset/ s3://bucket/data/ --max-files 16 --profile prod
```

### Preview

```bash
# See what would be uploaded
gpio publish upload data/ s3://bucket/dataset/ --dry-run
```

### S3-Compatible Storage

```bash
# MinIO with custom endpoint
gpio publish upload data.parquet s3://bucket/file.parquet \
  --s3-endpoint minio.example.com:9000 \
  --s3-no-ssl

# Custom endpoint with specific region
gpio publish upload data.parquet s3://bucket/file.parquet \
  --s3-endpoint storage.example.com \
  --s3-region eu-west-1
```

## Authentication

### AWS S3

Uses AWS profiles from `~/.aws/credentials`:

```bash
gpio publish upload data.parquet s3://bucket/file.parquet --profile my-profile
```

### Google Cloud Storage

Uses application default credentials:

```bash
gcloud auth application-default login
gpio publish upload data.parquet gs://bucket/file.parquet
```

### Azure Blob Storage

Uses Azure CLI credentials:

```bash
az login
gpio publish upload data.parquet az://account/container/file.parquet
```

## See Also

- [Upload Guide](../guide/upload.md) - Detailed guide with workflows
- [convert](convert.md) - Convert to GeoParquet
- [check](check.md) - Validate before upload
