# Troubleshooting

Common issues and solutions when using geoparquet-io.

## Installation Issues

### DuckDB Installation Fails

**Symptom**: Error installing DuckDB on certain platforms.

**Solution**: Upgrade pip and try again:
```bash
pip install --upgrade pip
pip install duckdb
```

If on Apple Silicon (M1/M2/M3), ensure you're using a native ARM Python, not Rosetta.

### PyArrow Version Conflicts

**Symptom**: Version conflicts with other geospatial packages.

**Solution**: Use a fresh virtual environment:
```bash
python -m venv gpio-env
source gpio-env/bin/activate  # Windows: gpio-env\Scripts\activate
pip install geoparquet-io
```

## File Access Issues

### "File not found" for Remote URLs

**Symptom**: Error accessing S3, GCS, or HTTPS files.

**Solutions**:

1. Verify the URL is correct and accessible
2. Check authentication (see below)
3. For S3, ensure the bucket region is correct

### S3 Authentication Errors

**Symptom**: Access denied or credentials errors for S3 files.

**Solutions**:

```bash
# Option 1: Use AWS profile
gpio inspect s3://bucket/file.parquet --aws-profile my-profile

# Option 2: Set environment variables
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
gpio inspect s3://bucket/file.parquet

# Option 3: Use default credentials
aws configure  # Set up ~/.aws/credentials
gpio inspect s3://bucket/file.parquet
```

### Azure Blob Storage Issues

**Symptom**: Cannot read from Azure Blob Storage.

**Solution**: Set Azure credentials:
```bash
export AZURE_STORAGE_ACCOUNT_NAME=myaccount
export AZURE_STORAGE_ACCOUNT_KEY=mykey
# Or use SAS token
export AZURE_STORAGE_SAS_TOKEN=mytoken
```

### GCS Requires HMAC Keys

**Symptom**: GCS authentication fails with service account.

**Solution**: DuckDB requires HMAC keys for GCS:
```bash
# Generate HMAC keys at: https://console.cloud.google.com/storage/settings
export GCS_ACCESS_KEY_ID=your_access_key
export GCS_SECRET_ACCESS_KEY=your_secret_key
gpio inspect gs://bucket/file.parquet
```

## Windows-Specific Issues

### File Locking Errors

**Symptom**: "The process cannot access the file because it is being used by another process"

**Cause**: DuckDB keeps file handles open, preventing cleanup. Unlike Unix, Windows doesn't allow deleting files that have open handles.

**Solutions for Users**:

1. Close any other applications accessing the file
2. Use unique output filenames (avoid overwriting)
3. Run operations sequentially, not in parallel

**Solutions for Developers/Contributors**:

When writing tests that create temporary files and use DuckDB to read them:

1. **Use `safe_unlink` from conftest.py** instead of `Path.unlink()`:
   ```python
   from tests.conftest import safe_unlink

   # In your test cleanup:
   safe_unlink(temp_path)  # Retries with delays for Windows
   ```

2. **Use fixtures like `temp_output_dir`** which handle cleanup automatically:
   ```python
   def test_example(temp_output_dir):
       output_file = os.path.join(temp_output_dir, "output.parquet")
       # ... test code ...
       # Cleanup handled automatically by fixture
   ```

3. **Use `safe_rmtree` for directories**:
   ```python
   from tests.conftest import safe_rmtree
   safe_rmtree(temp_dir)  # Retries with delays for Windows
   ```

The `safe_unlink` and `safe_rmtree` functions retry file deletion up to 5 times with 0.1s delays between attempts, which gives Windows time to release file handles.

### Path Issues with Spaces

**Symptom**: Commands fail when file paths contain spaces.

**Solution**: Quote paths with spaces:
```bash
gpio inspect "C:\Users\My Name\data file.parquet"
```

## Performance Issues

### Slow Operations on Large Files

**Symptom**: Commands take a long time on large files.

**Solutions**:

1. **Skip Hilbert for conversion**: `gpio convert input.shp output.parquet --skip-hilbert`
2. **Use --limit for testing**: `gpio extract input.parquet sample.parquet --limit 1000`
3. **Process locally**: Download remote files before processing for very large files (>10GB)

### Out of Memory Errors

**Symptom**: Process killed or memory errors on large files.

**Solutions**:

1. **Use the default write strategy** - gpio automatically streams data with constant memory:
   ```bash
   gpio extract large_file.parquet output.parquet --bbox -122.5,37.5,-122.0,38.0
   ```

2. **Set explicit memory limit** - For containerized environments or tight constraints:
   ```bash
   gpio extract input.parquet output.parquet --write-memory 512MB
   ```

3. **Try streaming strategy** - Alternative if default still uses too much memory:
   ```bash
   gpio extract input.parquet output.parquet --write-strategy streaming --write-memory 256MB
   ```

4. **Process in chunks using partitioning** - For extremely large datasets:
   ```bash
   gpio partition input.parquet output/ --by-quadkey --quadkey-resolution 4
   ```

5. Increase system swap space or use a machine with more RAM

### Slow Writes in Containers

**Symptom**: Write operations are slow in Docker or Kubernetes.

**Cause**: gpio may not correctly detect container memory limits in some configurations.

**Solutions**:

1. **Set explicit memory limit** - Tell gpio exactly how much memory to use:
   ```bash
   gpio extract input.parquet output.parquet --write-memory 1GB
   ```

2. **Verify container memory limits** - Ensure your container has enough memory:
   ```bash
   docker run -m 4g my-gpio-image gpio extract input.parquet output.parquet
   ```

3. **Check cgroup version** - gpio supports both cgroup v1 and v2. Verify your container runtime is configured correctly.

### Write Strategy Selection

**Symptom**: Need to choose between different write strategies.

**When to use each strategy**:

| Scenario | Strategy | Command |
|----------|----------|---------|
| Default (any file size) | `duckdb-kv` | (no flag needed) |
| Verify output correctness | `in-memory` | `--write-strategy in-memory` |
| DuckDB has issues | `streaming` | `--write-strategy streaming` |
| Maximum compatibility | `disk-rewrite` | `--write-strategy disk-rewrite` |

**Example - Debugging output differences**:
```bash
# 1. Write with default
gpio extract input.parquet output_default.parquet --bbox 0,0,10,10

# 2. Write with in-memory to verify
gpio extract input.parquet output_verify.parquet --bbox 0,0,10,10 --write-strategy in-memory

# 3. Compare
gpio inspect output_default.parquet --stats
gpio inspect output_verify.parquet --stats
```

See the [Write Strategies Guide](guide/write-strategies.md) for detailed information

## GeoParquet Issues

### "No geometry column found"

**Symptom**: Error about missing geometry column.

**Solutions**:

1. Verify file is actually GeoParquet: `gpio inspect file.parquet`
2. Check if geometry column has a different name
3. Specify geometry column explicitly if supported

### CRS Warning: Coordinates Look Wrong

**Symptom**: Warning about coordinate ranges not matching CRS.

**Cause**: Data might be in a projected CRS but metadata says WGS84 (or vice versa).

**Solutions**:

1. Check actual coordinate ranges: `gpio inspect file.parquet --stats`
2. Convert with correct CRS: `gpio convert data.csv output.parquet --crs EPSG:3857`

### Bbox Column Exists But No Covering Metadata

**Symptom**: `gpio check bbox` warns about missing covering metadata.

**Solution**: Add just the metadata (doesn't rewrite data):
```bash
gpio add bbox-metadata myfile.parquet
```

## Command-Specific Issues

### Extract WHERE Clause Errors

**Symptom**: SQL syntax errors with special column names.

**Solution**: Quote column names with special characters:
```bash
# Columns with colons, dashes, dots need double quotes in SQL
gpio extract data.parquet output.parquet --where '"crop:name" = '\''wheat'\'''

# Use --dry-run to preview the SQL
gpio extract data.parquet output.parquet --where "status = 'active'" --dry-run
```

### Partition Preview Shows No Output

**Symptom**: `gpio partition --preview` shows no partitions.

**Cause**: Column has no data or all null values.

**Solution**: Check column values first:
```bash
gpio inspect file.parquet --stats
```

### Convert Fails on CSV with WKT

**Symptom**: Error parsing WKT geometry from CSV.

**Solutions**:

1. Check WKT syntax is valid
2. Use `--skip-invalid` to skip bad rows:
```bash
gpio convert data.csv output.parquet --skip-invalid
```

### CSV "Maximum line size exceeded" Error

**Symptom**: `Maximum line size of X bytes exceeded` when converting CSV with large WKT geometries.

**Cause**: WKT geometries (complex polygons, coastlines) can exceed line size limits.

**Solutions**:

1. **Use `--csv-max-line-size`** to increase the limit:
```bash
gpio convert data.csv output.parquet --csv-max-line-size 100000000  # 100MB
```

2. **Set environment variable** for persistent override:
```bash
export GPIO_CSV_MAX_LINE_SIZE=100000000
gpio convert data.csv output.parquet
```

Default is 50MB. Increase if your WKT geometries are larger.

**Note**: Large values increase memory usage during CSV parsing. On memory-constrained systems, avoid setting excessively high limits.

### A Command Succeeded but the Process Exited 134

**Symptom**: the command prints its success line, the output file is correct, and the shell still reports a failure: `terminate called without an active exception`, exit status 134 (or `-6` from a subprocess).

**Cause**: not gpio's work. After the command returned, the Python interpreter unloaded DuckDB, its spatial extension, Arrow, GEOS and PROJ, and that teardown aborted. It was seen on Linux under CPU contention, about one run in twenty.

Since 1.6, `gpio` ends its process itself, on the status the command chose, after running the atexit handlers and flushing its output, and skips that teardown. A run whose output has been written is a success.

If you are debugging the teardown itself, `GPIO_INTERPRETER_EXIT=1` makes `gpio` leave through the interpreter as before:

```bash
GPIO_INTERPRETER_EXIT=1 gpio add geometry-metrics in.parquet out.parquet
```

`CliRunner`, `cli(standalone_mode=False)` and the Python API are unaffected; only the `gpio` console script exits this way.

## Getting Help

### Debug Information

Use `--verbose` for detailed output:
```bash
gpio convert input.shp output.parquet --verbose
```

Use `--dry-run` to preview SQL without executing:
```bash
gpio extract data.parquet output.parquet --where "x > 1" --dry-run
```

### Reporting Issues

When reporting issues, include:

1. Command you ran
2. Error message
3. Output of `gpio --version`
4. Python version: `python --version`
5. Operating system

File issues at: [GitHub Issues](https://github.com/geoparquet/geoparquet-io/issues)
