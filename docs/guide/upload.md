# Uploading to Cloud Storage

The `gpio publish upload` command uploads files and directories to cloud object storage, supporting S3, GCS, Azure, and HTTP destinations.

## Quick Start

=== "CLI"

    <!-- doctest: skip="needs cloud credentials" -->
    ```bash
    # Upload single file to S3
    gpio publish upload data.parquet s3://bucket/path/data.parquet

    # Upload directory (preserves structure)
    gpio publish upload output/ s3://bucket/dataset/

    # With AWS profile
    gpio publish upload data.parquet s3://bucket/data.parquet --aws-profile my-profile
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    table = gpio.read("data.parquet")
    ```

    <!-- doctest: skip="needs cloud credentials" -->
    ```python
    # Upload to S3
    table.upload("s3://bucket/path/data.parquet", profile="my-profile")
    ```

## Supported Destinations

| Destination | URL Format | Example |
|-------------|------------|---------|
| Amazon S3 | `s3://` | `s3://my-bucket/path/file.parquet` |
| Google Cloud Storage | `gs://` | `gs://my-bucket/path/file.parquet` |
| Azure Blob Storage | `az://` | `az://myaccount/mycontainer/path/file.parquet` |
| HTTP/HTTPS | `http://` or `https://` | `https://api.example.com/upload` |

### S3 credentials come from the full AWS chain

An S3 upload resolves credentials through the standard AWS credential chain, the same one gpio's reads use: environment variables, `~/.aws/credentials` (the profile named by `--aws-profile` or `AWS_PROFILE`), an `aws sso login` session, an assume-role profile (`role_arn` with `source_profile`, or a web identity token), `credential_process`, and EC2/ECS/EKS instance metadata ([#865](https://github.com/geoparquet/geoparquet-io/issues/865)). Uploads and the byte-for-byte copies other commands make now share one resolution, so a command no longer succeeds on one branch and fails Access Denied on the other.

Credentials from SSO, assume-role or `credential_process` expire. gpio resolves them once when the command starts, which covers a single upload or copy; a run longer than the credential lifetime needs to be restarted.

### Azure URLs name the account first

An Azure destination is `az://<account>/<container>/<path>` — the storage account, then the container, then the key. gpio builds the store from those two segments itself, so the account never has to be in the environment and is never mistaken for the container ([#864](https://github.com/geoparquet/geoparquet-io/issues/864)).

The credential still comes from the environment, and gpio checks for one before it uploads:

<!-- doctest: skip="needs cloud credentials" -->
```bash
# Storage account key
export AZURE_STORAGE_ACCOUNT_KEY=your_key

# ...or a SAS token
export AZURE_STORAGE_SAS_TOKEN=your_token

gpio publish upload data.parquet az://myaccount/mycontainer/data.parquet
```

`AZURE_STORAGE_ACCESS_KEY`, `AZURE_STORAGE_SAS_KEY`, the `AZURE_STORAGE_CLIENT_*` client-secret variables and `AZURE_USE_AZURE_CLI=true` are honoured too. The Azure CLI opt-in is explicit: `az login` alone is not picked up — set `AZURE_USE_AZURE_CLI=true` as well. `AZURE_STORAGE_ACCOUNT_NAME` is not needed — the account in the URL wins over it.

## Directory Uploads

When uploading directories, gpio preserves the directory structure and uploads files in parallel:

<!-- doctest: skip="needs cloud credentials" -->
```bash
# Upload all files
gpio publish upload output/ s3://bucket/dataset/

# Only parquet files
gpio publish upload output/ s3://bucket/dataset/ --pattern "*.parquet"

# Increase parallelism
gpio publish upload output/ s3://bucket/dataset/ --max-files 8
```

## AWS Configuration

### Using AWS Profiles

<!-- doctest: skip="needs cloud credentials" -->
```bash
gpio publish upload data.parquet s3://bucket/data.parquet --aws-profile source-coop
```

### S3-Compatible Endpoints

For MinIO, Wasabi, or other S3-compatible storage:

<!-- doctest: skip="needs cloud credentials" -->
```bash
gpio publish upload data.parquet s3://bucket/data.parquet \
  --s3-endpoint minio.example.com:9000 \
  --s3-region us-east-1
```

### Disable SSL

For local development or non-SSL endpoints:

<!-- doctest: skip="needs cloud credentials" -->
```bash
gpio publish upload data.parquet s3://bucket/data.parquet \
  --s3-endpoint localhost:9000 \
  --s3-no-ssl
```

## Multipart Uploads

Large files are automatically uploaded using multipart uploads:

<!-- doctest: skip="needs cloud credentials" -->
```bash
# Customize chunk settings
gpio publish upload large.parquet s3://bucket/large.parquet \
  --chunk-size 104857600 \
  --chunk-concurrency 12
```

## Error Handling

By default, directory uploads continue on errors. Use `--fail-fast` to stop on first error:

<!-- doctest: skip="needs cloud credentials" -->
```bash
gpio publish upload output/ s3://bucket/dataset/ --fail-fast
```

Either way, the summary counts only the files that actually reached the store,
and a directory upload that left anything out exits **1** — so `&&` and `set -e`
stop the script instead of carrying on over an incomplete dataset:

```
==================================================
✓ 2/10 file(s) uploaded successfully
✗ 1 file(s) failed
⊘ 7 file(s) not attempted (stopped on first error)
```

`--fail-fast` stops every file that has not started yet; uploads already in
flight are allowed to finish, and are counted by whether they arrived. The three
counts always add up to the total, so a retry knows exactly how much is missing.
Partial and total failure both exit 1 — read the counts, not the exit code, to
tell them apart.

In Python the same failure raises `RemoteAccessError`:

<!-- doctest: skip="needs cloud credentials" -->
```python
from pathlib import Path

from geoparquet_io.core.exceptions import RemoteAccessError
from geoparquet_io.core.upload import upload

try:
    upload(Path("output/"), "s3://bucket/dataset/", fail_fast=True)
except RemoteAccessError as e:
    print(f"upload incomplete: {e}")
```

## Dry Run

Preview what would be uploaded without actually uploading:

<!-- doctest: skip="needs cloud credentials" -->
```bash
gpio publish upload output/ s3://bucket/dataset/ --dry-run
```

## CLI Reference

See the [CLI Reference](../cli/upload.md) for complete options.
