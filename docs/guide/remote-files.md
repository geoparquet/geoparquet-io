# Remote Files

All commands work with remote URLs (`s3://`, `gs://`, `https://` for reads and writes; `az://` for writes). Use them anywhere you'd use local paths.

## How Remote Access Works

gpio uses different libraries for reads and writes:

- **Reads**: All commands read remote files via DuckDB's httpfs extension. This supports S3, GCS, and HTTPS URLs transparently. Azure is not readable — see below.

- **Writes**: All commands write to remote destinations using obstore. When you specify a remote output path, gpio writes to a local temp file first, then uploads via obstore automatically.

The `--aws-profile` global flag is available on all commands for AWS authentication. See also `--s3-endpoint`, `--s3-region`, and `--s3-no-ssl` for S3-compatible storage.

### URLs are taken as-is

A URL you pass to gpio is used exactly as you typed it: gpio assumes it is already percent-encoded, as a URL is by definition, and never re-encodes it. Paste the URL that works in your browser or in `curl` — `gpio inspect meta 'https://example.com/my%20file.parquet'` requests `my%20file.parquet`, not `my%2520file.parquet`. A URL containing a raw space, bracket or other character that has to be escaped is yours to encode before passing it in.

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

Credentials come from the standard AWS credential chain, so anything the AWS CLI can use, gpio can use — for reads, for writes and for copies alike ([#865](https://github.com/geoparquet/geoparquet-io/issues/865)):

1. **Environment variables**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (and `AWS_SESSION_TOKEN`)
2. **Shared credentials file**: `~/.aws/credentials`, the profile named by `AWS_PROFILE` or `--aws-profile`
3. **SSO / IAM Identity Center**: an `aws sso login` session
4. **Assume-role profiles**: `role_arn` with `source_profile`, or a web identity token
5. **`credential_process`**: an external command that prints credentials
6. **IAM role**: EC2 instance, ECS task or EKS pod identity metadata

For DuckDB reads, precedence follows the AWS CLI. Naming a profile with `--aws-profile` (or `profile=` in Python) makes that profile win over environment keys. An exported `AWS_PROFILE` does not, so when `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are also set, DuckDB reads sign with the environment keys. Uploads and copies go through obstore, where an exported `AWS_PROFILE` still wins over environment keys.

The region comes from `--s3-region` (or `s3_region=` in Python) first, then `AWS_REGION` or `AWS_DEFAULT_REGION`. After that, a profile named with `--aws-profile` gives its own `region`. An exported `AWS_PROFILE` does not, and DuckDB reads use the `region` of the `default` profile instead. If the bucket is in another region, pass `--s3-region` (or `s3_region=`) or set `AWS_REGION`.

Credentials from SSO, assume-role and `credential_process` are short-lived. gpio resolves them once at the start of a command, which is fine for a single upload or copy; a run that outlives the credential lifetime needs to be restarted.

**Known limit for assume-role profiles on DuckDB reads.** For a profile with `role_arn` and `source_profile`, the DuckDB `aws` extension signs the STS `AssumeRole` call with the default credential chain (environment keys or the `default` profile), not with the keys of `source_profile`. Uploads and copies (obstore) use `source_profile` as expected. Until this is fixed upstream, put the source keys in the `default` profile or in environment variables, or use a `credential_process` or SSO profile.

**Examples:**

=== "CLI"

    <!-- doctest: skip="needs cloud credentials" -->
    ```bash
    # Use default credentials (from ~/.aws/credentials [default] or IAM role)
    gpio add bbox s3://bucket/input.parquet s3://bucket/output.parquet

    # Use environment variables
    export AWS_ACCESS_KEY_ID=your_key
    export AWS_SECRET_ACCESS_KEY=your_secret
    gpio add bbox s3://bucket/input.parquet s3://bucket/output.parquet

    # Use a named AWS profile (convenient CLI flag)
    gpio --aws-profile production add bbox s3://bucket/input.parquet s3://bucket/output.parquet

    # Or set AWS_PROFILE environment variable (env keys, when set, win over it on reads)
    export AWS_PROFILE=production
    gpio add bbox s3://bucket/input.parquet s3://bucket/output.parquet
    ```

=== "Python"

    <!-- doctest: skip="needs cloud credentials" -->
    ```python
    import os
    import geoparquet_io as gpio

    # Use default credentials (from ~/.aws/credentials [default] or IAM role)
    gpio.read('s3://bucket/input.parquet').add_bbox().write('output.parquet')

    # Use a named AWS profile
    gpio.read('s3://bucket/input.parquet').add_bbox().upload(
        's3://bucket/output.parquet',
        profile='production'
    )

    # Or set AWS_PROFILE environment variable
    os.environ['AWS_PROFILE'] = 'production'
    gpio.read('s3://bucket/input.parquet').add_bbox().upload('s3://bucket/output.parquet')
    ```

**Note:** The `--aws-profile` flag is available on all commands. It applies to that one command only and does not change `AWS_PROFILE` in your environment.

### Azure Blob Storage

Azure is a **write destination only**: `gpio publish upload` and remote outputs can target `az://`, but no command reads from Azure — an `az://` input is refused up front with a message saying so. To process data that lives in Azure, download it first.

Credentials for writes are discovered from the environment:

```bash
# Set the account key via an environment variable
export AZURE_STORAGE_ACCOUNT_KEY=mykey

# Or use a SAS token
export AZURE_STORAGE_SAS_TOKEN=mytoken
```

<!-- doctest: skip="needs cloud credentials" -->
```bash
# Then use Azure URLs: account first, then container, then the key
gpio publish upload data.parquet az://myaccount/mycontainer/data.parquet
```

Everything gpio writes to Azure — `gpio publish upload` and the object-store copies behind commands such as `gpio add bbox` — addresses it as `az://<account>/<container>/<path>`. The account comes from the URL, so `AZURE_STORAGE_ACCOUNT_NAME` is optional there, and the container is never guessed from the wrong segment ([#864](https://github.com/geoparquet/geoparquet-io/issues/864)). The `abfs://`, `abfss://` and `azure://` spellings order those parts differently and are refused by name.

`AZURE_STORAGE_ACCESS_KEY`, `AZURE_STORAGE_SAS_KEY`, the `AZURE_STORAGE_CLIENT_*` client-secret variables and `AZURE_USE_AZURE_CLI=true` (required to use an `az login` session — `az login` alone is not picked up) are honoured too.

### Google Cloud Storage

GCS support requires HMAC keys (not service account JSON):

<!-- doctest: skip="needs cloud credentials" -->
```bash
# Generate HMAC keys at: https://console.cloud.google.com/storage/settings
export GCS_ACCESS_KEY_ID=your_access_key
export GCS_SECRET_ACCESS_KEY=your_secret_key

gpio add bbox gs://bucket/input.parquet gs://bucket/output.parquet
```

**Note:** DuckDB's GCS support requires HMAC keys, which differs from standard GCP authentication. For writes, obstore can use service account JSON via `GOOGLE_APPLICATION_CREDENTIALS`. For reads, use HMAC keys or process files locally.

## S3-Compatible Storage

All commands support S3-compatible endpoints (MinIO, Cloudflare R2, source.coop, Ceph) via global flags:

=== "CLI"

    <!-- doctest: skip="needs cloud credentials" -->
    ```bash
    # Read from source.coop
    gpio --s3-endpoint data.source.coop inspect summary s3://bucket/file.parquet

    # MinIO without SSL
    gpio --s3-endpoint minio.local:9000 --s3-no-ssl \
      extract geoparquet s3://bucket/input.parquet output.parquet

    # Upload to custom endpoint
    gpio --s3-endpoint storage.example.com --s3-region eu-west-1 \
      publish upload data.parquet s3://bucket/file.parquet
    ```

=== "Python"

    <!-- doctest: skip="needs cloud credentials" -->
    ```python
    import geoparquet_io as gpio

    # Read from source.coop
    table = gpio.read_partition(
        's3://bucket/data/',
        s3_endpoint='data.source.coop'
    )

    # Upload to MinIO
    gpio.read('data.parquet').upload(
        's3://bucket/file.parquet',
        s3_endpoint='minio.example.com:9000',
        s3_use_ssl=False
    )
    ```

### Environment Variables

Instead of flags, you can set standard AWS environment variables:

| Variable | Equivalent Flag |
|----------|----------------|
| `AWS_ENDPOINT_URL` | `--s3-endpoint` |
| `AWS_REGION` / `AWS_DEFAULT_REGION` | `--s3-region` |
| `AWS_PROFILE` | `--aws-profile` |

<!-- doctest: skip="needs cloud credentials" -->
```bash
export AWS_ENDPOINT_URL=https://data.source.coop
gpio inspect summary s3://bucket/file.parquet
```

### SSL Detection

SSL is auto-detected from the endpoint URL:

- `http://` → SSL off
- `https://` or no scheme → SSL on
- `--s3-no-ssl` overrides in either case

## Piping to Upload

For efficient workflows, process data locally and pipe to upload. This uses Arrow IPC streaming with minimal overhead:

<!-- doctest: skip="needs cloud credentials" -->
```bash
# Process and upload in one pipeline
gpio extract --bbox "-122.5,37.5,-122.0,38.0" input.parquet | \
  gpio add bbox - | \
  gpio sort hilbert - local_output.parquet && \
  gpio publish upload local_output.parquet s3://bucket/output.parquet --aws-profile prod
```

Or use the Python API for zero-copy streaming:

```python
import geoparquet_io as gpio

# Process in memory, then upload
table = gpio.read('input.parquet') \
    .extract(bbox=(-122.5, 37.5, -122.0, 38.0)) \
    .add_bbox() \
    .sort_hilbert()
```

<!-- doctest: skip="needs cloud credentials" -->
```python
# Upload directly (writes temp file, uploads, cleans up)
table.upload('s3://bucket/output.parquet', profile='prod')
```

See [Command Piping](piping.md) for more streaming patterns.

## Exceptions

**STAC generation** (`gpio publish stac`) requires local files because asset paths reference local storage.

## Notes

- Remote writes use temporary local storage (~2× output file size required)
- URLs are passed through unchanged; encode them yourself if they contain spaces or other reserved characters
- HTTPS wildcards (`*.parquet`) not supported
- For very large files (>10 GB), consider processing locally for better performance
- S3-compatible endpoints work with all commands via `--s3-endpoint`
