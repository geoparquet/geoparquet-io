# Remote Files

Read GeoParquet files from cloud storage and HTTPS URLs.

## Supported Protocols

- **HTTPS**: Public and private URLs
- **S3**: `s3://bucket/path/file.parquet`
- **Azure**: `az://container/file.parquet` or `https://account.blob.core.windows.net/...`
- **GCS**: `gs://bucket/path/file.parquet`

## Basic Usage

```bash
# Inspect remote file
gpio inspect https://data.source.coop/path/file.parquet

# Check remote file
gpio check all s3://bucket/data.parquet

# Convert remote to local
gpio convert https://example.com/data.geojson local.parquet

# Process remote, upload result
gpio sort hilbert s3://bucket/input.parquet local-sorted.parquet
gpio upload local-sorted.parquet s3://bucket/output/sorted.parquet
```

## Authentication

### AWS S3

```bash
# Environment variables
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret

# Or use AWS CLI
aws configure

# Or use profile
export AWS_PROFILE=your-profile
```

### Azure

```bash
export AZURE_STORAGE_ACCOUNT_NAME=account
export AZURE_STORAGE_ACCOUNT_KEY=key

# Or SAS token
export AZURE_STORAGE_SAS_TOKEN=token
```

### Google Cloud

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

## Limitations

- Outputs write to local filesystem only
- Use `gpio upload` to transfer results to cloud storage
- STAC generation requires local files
- HTTPS wildcards (`*.parquet`) not supported

## See Also

- [upload command](upload.md)
- [convert command](convert.md)
