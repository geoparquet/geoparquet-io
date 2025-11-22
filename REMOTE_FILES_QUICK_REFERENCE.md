# Remote File Support - Quick Reference

## TL;DR

**DuckDB already supports remote files.** Just need to:
1. Add `requests` and `aiohttp` to dependencies
2. Update `safe_file_url()` to detect remote URLs
3. Remove `type=click.Path(exists=True)` from CLI arguments
4. Load `httpfs` extension for S3/Azure/GCS

## Supported Protocols

| Protocol | URL Format | Auth Required | Extension |
|----------|-----------|---------------|-----------|
| HTTPS (public) | `https://domain/path/file.parquet` | No | None |
| AWS S3 | `s3://bucket/path/file.parquet` | Yes* | httpfs |
| Azure Blob | `https://account.blob.core.windows.net/...` | No** | None |
| Azure (native) | `az://container/path/file.parquet` | Yes | httpfs |
| Google Cloud | `gs://bucket/path/file.parquet` | Yes | httpfs |

\* For private buckets only
\*\* Public blobs work via HTTPS

## Authentication Setup

### AWS S3
```bash
# Option 1: Environment variables
export AWS_ACCESS_KEY_ID=your_key_id
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1

# Option 2: AWS CLI (creates ~/.aws/credentials)
aws configure

# Option 3: Use profile
export AWS_PROFILE=your-profile
```

### Azure Blob Storage
```bash
export AZURE_STORAGE_ACCOUNT_NAME=your_account
export AZURE_STORAGE_ACCOUNT_KEY=your_key
# Or use SAS token:
export AZURE_STORAGE_SAS_TOKEN=your_token
```

### Google Cloud Storage
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

## Performance Characteristics

### ✓ What's Fast
- **Column projection**: Only fetches needed columns
- **Row group filtering**: Skips irrelevant data using statistics
- **Predicate pushdown**: WHERE clauses applied before reading
- **Spatial filtering**: With bbox columns or spatial sorting

### ⚠ What's Slower
- Full table scans without filters
- Reading many small files (network overhead)
- Operations requiring all rows (e.g., COUNT(*) without sampling)

## Code Changes Required

### 1. Add Dependencies (pyproject.toml)
```toml
dependencies = [
    # ... existing ...
    "requests>=2.28.0",
    "aiohttp>=3.8.0",
]
```

### 2. Update safe_file_url() (geoparquet_io/core/common.py)
```python
def safe_file_url(file_path, verbose=False):
    """Handle both local and remote files."""
    remote_schemes = ['http://', 'https://', 's3://', 's3a://',
                     'gs://', 'gcs://', 'az://', 'azure://']

    if any(file_path.startswith(s) for s in remote_schemes):
        if verbose:
            click.echo(f"Reading remote file: {file_path}")
        return file_path
    else:
        if not os.path.exists(file_path):
            raise click.BadParameter(f"Local file not found: {file_path}")
        return file_path
```

### 3. Update CLI Arguments (geoparquet_io/cli/main.py)

**Before:**
```python
@click.argument("parquet_file", type=click.Path(exists=True))
```

**After:**
```python
@click.argument("parquet_file")  # Validation in safe_file_url()
```

### 4. Load httpfs Extension (where S3/Azure/GCS support needed)
```python
con = duckdb.connect()
con.execute("INSTALL httpfs;")  # Auto-installs from community repo
con.execute("LOAD httpfs;")
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")
```

## Testing URLs

### Public HTTPS (works now with DuckDB)
```bash
https://data.source.coop/nlebovits/gaul-l2-admin/by_country/USA.parquet
```

### Private S3 (works with credentials)
```bash
s3://arg-fulbright-data/censo-argentino-2022/radios-2022.parquet
```

## Error Messages

Users will see clear errors from DuckDB:

| Error | Meaning |
|-------|---------|
| `403 (Forbidden)` | Auth failure - check credentials |
| `404 (Not Found)` | Invalid URL or file doesn't exist |
| `Connection timeout` | Network issue |
| `Access Denied` | Insufficient S3 permissions |

## Usage Examples (After Implementation)

```bash
# Inspect remote file
gpio inspect https://data.source.coop/path/file.parquet

# Check S3 file
gpio check s3://my-bucket/data.parquet

# Add bbox to remote file
gpio add bbox s3://bucket/input.parquet s3://bucket/output.parquet

# Sort remote file (output to local)
gpio sort hilbert https://example.com/data.parquet sorted.parquet

# Partition S3 data
gpio partition admin s3://bucket/large.parquet ./output_dir/

# Convert remote file
gpio convert https://example.com/data.geojson output.parquet
```

## Best Practices

1. **Use bbox columns** for faster spatial operations
2. **Sort spatially** to improve row group filtering
3. **Partition large datasets** by region/category
4. **Project columns** when possible (SELECT only needed fields)
5. **Cache intermediate results** locally for iterative work

## Limitations

### What Works ✅
- **Read operations**: All commands can read from remote URLs (HTTPS, S3, Azure, GCS)
- **Inspect & check**: View metadata, validate files, check spatial properties
- **Convert**: Convert remote files to local GeoParquet (including CSV, Shapefile, etc.)
- **Partition**: Partition remote files with `--preview` to analyze first
- **Add columns**: Add bbox, H3, admin divisions to remote files (output locally)
- **Sort**: Hilbert sort remote files (output locally)

### What Doesn't Work ❌
- **STAC generation**: Cannot create STAC items/collections for remote files
  - Rationale: STAC asset hrefs would reference files you may not control
  - Workaround: Download file first or use `gpio convert` to create local copy
  - Future: May support if there's demand for cataloging public datasets

- **Writing to remote locations**: All outputs write to local filesystem only
  - Rationale: Safety and simplicity - remote writes are complex and error-prone
  - Future: Could add remote write support if needed

- **Wildcards in HTTPS URLs**: `https://example.com/*.parquet` not supported
  - Reason: HTTP protocol doesn't support directory listing
  - S3 wildcards (`s3://bucket/*.parquet`) may work but untested

- **Admin divisions remote lookup**: Admin boundary files must be local
  - Low priority - users typically have these files locally

### Performance Notes ⚠️
- Network latency affects small operations more than large ones
- Very large files may timeout - try smaller files first
- DuckDB uses HTTP range requests - only fetches needed data
- Consider caching intermediate results locally for iterative work

## Files to Modify

1. `pyproject.toml` - Add requests/aiohttp
2. `geoparquet_io/core/common.py` - Update safe_file_url()
3. `geoparquet_io/cli/main.py` - Remove Path(exists=True) validation (8 locations)
4. Core modules using DuckDB - Add httpfs loading where needed

## Estimated Effort

- Dependencies: 2 minutes
- safe_file_url(): 10 minutes
- CLI updates: 20 minutes
- Testing: 30 minutes
- **Total: ~1 hour** 🎉

---

*For detailed findings, see REMOTE_FILES_FINDINGS.md*
