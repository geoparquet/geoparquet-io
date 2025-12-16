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
gpio inspect s3://bucket/file.parquet --profile my-profile

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

**Cause**: DuckDB keeps file handles open, preventing cleanup.

**Solutions**:

1. Close any other applications accessing the file
2. Use unique output filenames (avoid overwriting)
3. Run operations sequentially, not in parallel

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

1. Process in chunks using partitioning
2. Increase system swap space
3. Use a machine with more RAM

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

File issues at: [GitHub Issues](https://github.com/cholmes/geoparquet-io/issues)
