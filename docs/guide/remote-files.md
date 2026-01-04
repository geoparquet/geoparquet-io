# Remote Files

All commands work with remote URLs (`s3://`, `gs://`, `az://`, `https://`). Use them anywhere you'd use local paths.

## How Remote Access Works

gpio uses different libraries for reads and writes:

- **Reads**: All commands read remote files via DuckDB's httpfs extension. This supports S3, GCS, Azure, and HTTPS URLs transparently.

- **Writes**: All commands write to remote destinations using obstore. When you specify a remote output path, gpio writes to a local temp file first, then uploads via obstore automatically.

The `--profile` flag is available on all commands for AWS authentication.

### gpio publish upload

For more control over uploads, use `gpio publish upload` which provides:

- Parallel multipart uploads for large files
- Custom S3-compatible endpoints (MinIO, Ceph, etc.)
- Directory uploads with pattern filtering
- Progress tracking and error handling options

For simple remote outputs, commands write directly. For batch uploads or S3-compatible storage, use `gpio publish upload`.

## Authentication

geoparquet-io uses standard cloud provider authentication. Configure your credentials once using your cloud provider's standard tools - no CLI flags needed for basic usage.

### AWS S3

Credentials are automatically discovered in this order:

1. **Environment variables**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
2. **AWS profile**: `~/.aws/credentials` via `AWS_PROFILE` env var or `--profile` flag
3. **IAM role**: EC2/ECS/EKS instance metadata (when running on AWS infrastructure)

**Examples:**

```bash
# Use default credentials (from ~/.aws/credentials [default] or IAM role)
gpio add bbox s3://bucket/input.parquet s3://bucket/output.parquet

# Use environment variables
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
gpio add bbox s3://bucket/input.parquet s3://bucket/output.parquet

# Use a named AWS profile (convenient CLI flag)
gpio add bbox s3://bucket/input.parquet s3://bucket/output.parquet --profile production

# Or set AWS_PROFILE environment variable (equivalent to --profile)
export AWS_PROFILE=production
gpio add bbox s3://bucket/input.parquet s3://bucket/output.parquet
```

**Note:** The `--profile` flag is available on all commands and sets `AWS_PROFILE` for you.

### Azure Blob Storage

Azure credentials are discovered automatically when reading files:

```bash
# Set account credentials via environment variables
export AZURE_STORAGE_ACCOUNT_NAME=myaccount
export AZURE_STORAGE_ACCOUNT_KEY=mykey

# Or use SAS token
export AZURE_STORAGE_SAS_TOKEN=mytoken

# Then use Azure URLs
gpio add bbox az://container/input.parquet az://container/output.parquet
```

**Note:** Azure support for reads is currently limited. For full Azure support, process files locally.

### Google Cloud Storage

GCS support requires HMAC keys (not service account JSON):

```bash
# Generate HMAC keys at: https://console.cloud.google.com/storage/settings
export GCS_ACCESS_KEY_ID=your_access_key
export GCS_SECRET_ACCESS_KEY=your_secret_key

gpio add bbox gs://bucket/input.parquet gs://bucket/output.parquet
```

**Note:** DuckDB's GCS support requires HMAC keys, which differs from standard GCP authentication. For writes, obstore can use service account JSON via `GOOGLE_APPLICATION_CREDENTIALS`. For reads, use HMAC keys or process files locally.

## S3-Compatible Storage

For MinIO, Ceph, or other S3-compatible storage, use `gpio publish upload` which supports custom endpoints:

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

These options are only available on `gpio publish upload` because it uses obstore for direct uploads. Other commands use DuckDB's httpfs which only supports standard AWS S3 endpoints.

## Piping to Upload

For efficient workflows, process data locally and pipe to upload. This uses Arrow IPC streaming with minimal overhead:

```bash
# Process and upload in one pipeline
gpio extract --bbox "-122.5,37.5,-122.0,38.0" input.parquet | \
  gpio add bbox - | \
  gpio sort hilbert - local_output.parquet && \
  gpio publish upload local_output.parquet s3://bucket/output.parquet --profile prod
```

Or use the Python API for zero-copy streaming:

```python
import geoparquet_io as gpio

# Process in memory, then upload
table = gpio.read('input.parquet') \
    .extract(bbox=(-122.5, 37.5, -122.0, 38.0)) \
    .add_bbox() \
    .sort_hilbert()

# Upload directly (writes temp file, uploads, cleans up)
table.upload('s3://bucket/output.parquet', profile='prod')
```

See [Command Piping](piping.md) for more streaming patterns.

## Exceptions

**STAC generation** (`gpio publish stac`) requires local files because asset paths reference local storage.

## Notes

- Remote writes use temporary local storage (~2× output file size required)
- HTTPS wildcards (`*.parquet`) not supported
- For very large files (>10 GB), consider processing locally for better performance
- S3-compatible endpoints (MinIO, Ceph) require `gpio publish upload`
