# Remote Files

All commands work with remote URLs (`s3://`, `gs://`, `az://`, `https://`). Use them anywhere you'd use local paths.

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

**Note:** The `--profile` flag is a convenience wrapper that sets `AWS_PROFILE` for you. Both approaches work identically.

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

## Exceptions

**STAC generation** (`gpio publish stac`) requires local files because asset paths reference local storage.

## Notes

- Remote writes use temporary local storage (~2× output file size required)
- HTTPS wildcards (`*.parquet`) not supported
- For very large files (>10 GB), consider processing locally for better performance
