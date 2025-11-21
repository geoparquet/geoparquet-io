# upload

Upload GeoParquet files to cloud object storage (S3, GCS, Azure).

## Usage

```bash
gpio upload SOURCE DESTINATION [OPTIONS]
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
--dry-run                   Preview what would be uploaded without uploading
```

## Examples

### Single File

```bash
# Upload to S3 with AWS profile
gpio upload buildings.parquet s3://bucket/data/buildings.parquet --profile prod

# Upload to GCS
gpio upload data.parquet gs://bucket/path/data.parquet

# Upload to Azure
gpio upload data.parquet az://account/container/path/data.parquet
```

### Directory

```bash
# Upload all files
gpio upload partitions/ s3://bucket/dataset/ --profile prod

# Upload only JSON files
gpio upload data/ s3://bucket/json-files/ --pattern "*.json" --profile prod

# Upload with higher parallelism
gpio upload large-dataset/ s3://bucket/data/ --max-files 16 --profile prod
```

### Preview

```bash
# See what would be uploaded
gpio upload data/ s3://bucket/dataset/ --dry-run
```

## Authentication

### AWS S3

Uses AWS profiles from `~/.aws/credentials`:

```bash
gpio upload data.parquet s3://bucket/file.parquet --profile my-profile
```

### Google Cloud Storage

Uses application default credentials:

```bash
gcloud auth application-default login
gpio upload data.parquet gs://bucket/file.parquet
```

### Azure Blob Storage

Uses Azure CLI credentials:

```bash
az login
gpio upload data.parquet az://account/container/file.parquet
```

## See Also

- [Upload Guide](../guide/upload.md) - Detailed guide with workflows
- [convert](convert.md) - Convert to GeoParquet
- [check](check.md) - Validate before upload
