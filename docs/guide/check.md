# Checking Best Practices

The `check` commands validate GeoParquet files against [best practices](https://github.com/opengeospatial/geoparquet/pull/254/files).

## Run All Checks

```bash
gpio check all myfile.parquet
```

Runs all validation checks:

- Spatial ordering
- Compression settings
- Bbox structure and metadata
- Row group optimization

## Individual Checks

### Spatial Ordering

```bash
gpio check spatial myfile.parquet
```

Checks if data is spatially ordered using random sampling. Spatially ordered data improves:

- Query performance
- Compression ratios
- Cloud access patterns

### Compression

```bash
gpio check compression myfile.parquet
```

Validates geometry column compression settings.

### Bbox Structure

```bash
gpio check bbox myfile.parquet
```

Verifies:

- Bbox column structure
- GeoParquet metadata version
- Bbox covering metadata

### Row Groups

```bash
gpio check row-group myfile.parquet
```

Checks row group size optimization for cloud-native access.

### STAC Validation

```bash
gpio check stac output.json
```

Validates STAC Item or Collection JSON:

- STAC spec compliance
- Required fields
- Asset href resolution (local files)
- Best practices

## Options

```bash
# Verbose output with details
gpio check all myfile.parquet --verbose

# Custom sampling for spatial check
gpio check spatial myfile.parquet --random-sample-size 200 --limit-rows 1000000
```

## See Also

- [CLI Reference: check](../cli/check.md)
- [add command](add.md) - Add spatial indices
- [sort command](sort.md)
- [stac command](stac.md) - Generate STAC metadata
