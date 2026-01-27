# Performance Benchmarks

gpio includes a benchmark suite for measuring performance and detecting regressions across versions.

## Quick Start

Run benchmarks comparing current version against a previous release:

```bash
# Run benchmarks on current version
python scripts/version_benchmark.py --version-label "current" -o results_current.json

# Compare against previous results
python scripts/version_benchmark.py --compare results_baseline.json results_current.json
```

## Benchmark Operations

The suite tests these operations covering most gpio capabilities:

### Extract Operations

| Operation | Description |
|-----------|-------------|
| `inspect` | Read and display file metadata |
| `extract-limit` | Extract first 100 rows |
| `extract-columns` | Extract specific columns (includes geometry) |
| `extract-bbox` | Spatial bounding box filtering |

### Add Column Operations

| Operation | Description |
|-----------|-------------|
| `add-bbox` | Add bounding box column |
| `add-quadkey` | Add quadkey column (resolution 12) |
| `add-h3` | Add H3 cell ID column (resolution 8) |

### Sort Operations

| Operation | Description |
|-----------|-------------|
| `sort-hilbert` | Sort by Hilbert curve for spatial locality |
| `sort-quadkey` | Sort by quadkey spatial index |

### Transform Operations

| Operation | Description |
|-----------|-------------|
| `reproject` | Reproject to Web Mercator (EPSG:3857) |

### Partition Operations

| Operation | Description |
|-----------|-------------|
| `partition-quadkey` | Partition by quadkey (resolution 4) |
| `partition-h3` | Partition by H3 cells (resolution 4) |

### Convert/Export Operations

| Operation | Description |
|-----------|-------------|
| `convert-geojson` | Convert to GeoJSON format |
| `convert-flatgeobuf` | Convert to FlatGeobuf format |
| `convert-geopackage` | Convert to GeoPackage format |

### Import Operations

| Operation | Description |
|-----------|-------------|
| `import-geojson` | Import from GeoJSON to GeoParquet |
| `import-geopackage` | Import from GeoPackage to GeoParquet |

Note: Import operations only run on tiny/small file sizes as source format files are only available in those sizes.

### Chain Operations (Multi-step Workflows)

| Operation | Description |
|-----------|-------------|
| `chain-extract-bbox-sort` | Extract columns → Add bbox → Hilbert sort |
| `chain-filter-sort` | Bbox filter → Hilbert sort |

### Operation Presets

| Preset | Operations |
|--------|------------|
| `quick` | inspect, extract-limit, add-bbox |
| `standard` | inspect, extract-limit, extract-columns, add-bbox, sort-hilbert |
| `full` | All 19 operations including imports and chains |

## Test Data

Benchmark files are hosted on source.coop with different size tiers:

| Tier | Rows | Geometry | CRS | Source |
|------|------|----------|-----|--------|
| tiny | 1,000 | Polygon | EPSG:4326 | Overture Buildings (Singapore) |
| small | 10,000 | Polygon | EPSG:4326 | Overture Buildings (Singapore) |
| medium | 100,000 | Polygon | EPSG:4326 | Overture Buildings (Singapore) |
| large | 809,000 | Polygon | EPSG:3794 | fiboa field boundaries (Slovenia) |
| points-tiny | 1,000 | Point | EPSG:3857 | Building centroids (Web Mercator) |
| points-small | 10,000 | Point | EPSG:3857 | Building centroids (Web Mercator) |

The points files provide variation in geometry type and CRS for regression testing.

### File Presets

| Preset | Files |
|--------|-------|
| `quick` | tiny, small |
| `standard` | small, medium |
| `full` | tiny, small, medium, large, points-tiny, points-small |

Files are automatically downloaded and cached locally in `/tmp/gpio-benchmark-cache/`.

## Running Benchmarks Locally

### Version Comparison Script

The `scripts/version_benchmark.py` script works with any gpio version:

```bash
# Run full benchmarks (all files, all operations)
python scripts/version_benchmark.py --version-label "v0.9.0" -o results.json

# Run quick benchmarks (smaller file set, fewer operations)
python scripts/version_benchmark.py --version-label "v0.9.0" -o results.json --files quick --ops quick

# Run benchmarks with more iterations for accuracy
python scripts/version_benchmark.py --version-label "main" -o results.json -n 5

# Run specific operations on specific files
python scripts/version_benchmark.py --version-label "test" --files small,medium --ops add-bbox,sort-hilbert

# Compare two result files
python scripts/version_benchmark.py --compare results_baseline.json results_current.json

# Skip local caching (test remote file performance)
python scripts/version_benchmark.py --version-label "remote-test" --no-cache
```

### Sample Output

```
======================================================================
Comparison: v0.9.0 vs main
======================================================================

Operation                 File     v0.9.0       main         Delta
----------------------------------------------------------------------
inspect                   tiny     0.468s       0.440s       -5.8% faster
extract-limit             tiny     0.543s       0.540s       -0.5% faster
add-bbox                  large    0.378s       0.408s       +8.1% slower
sort-hilbert              large    27.366s      26.946s      -1.5% faster
```

### CLI Benchmark Commands

gpio also includes built-in benchmark commands:

```bash
# Run benchmark suite on specific files
gpio benchmark suite --files path/to/file.parquet --operations core

# Run quick benchmark (single operation, timing only)
gpio benchmark run inspect path/to/file.parquet
```

## GitHub Actions Workflows

### PR Benchmarks (Opt-in)

Benchmarks run on PRs only when the `benchmark` label is added:

1. Add the `benchmark` label to your PR
2. The workflow runs automatically
3. Results are posted as a comment on the PR

### Manual Benchmark Run

Run benchmarks manually from the Actions tab:

1. Go to **Actions** → **Benchmark Suite**
2. Click **Run workflow**
3. Configure options:
   - **iterations**: Number of runs per operation (default: 3)
   - **files**: File preset or comma-separated list (default: full)
   - **ops**: Operation preset or comma-separated list (default: full)
   - **compare_version**: Optional version to compare against (e.g., `v0.9.0`)
4. View results in the workflow summary

### Release Benchmarks

When a release is created, benchmarks automatically:

1. Run on the new release version
2. Compare against the previous release tag
3. Detect regressions (>25% slower)
4. Append results to the release notes

**Results include:**
- Comparison table showing performance delta
- Warning for any significant regressions
- Detailed benchmark data in collapsible section

### Where Results Are Published

| Trigger | Results Location |
|---------|------------------|
| PR with `benchmark` label | Comment on PR |
| Manual workflow run | Workflow summary + artifacts |
| Release | Appended to release notes |

All runs also upload JSON artifacts for historical tracking.

## Interpreting Results

### Regression Thresholds

| Severity | Threshold | Action |
|----------|-----------|--------|
| Normal variance | ±10% | No action needed |
| Warning | +10-25% | Investigate cause |
| Regression | >+25% | Flagged in release notes |

### Expected Variance

- **Small files (<10K rows)**: High variance (±20%) due to startup overhead
- **Large files (>100K rows)**: Low variance (±5%), most reliable for comparison
- **CI environment**: May differ from local; compare CI-to-CI results

### Known Performance Characteristics

| Operation | Notes |
|-----------|-------|
| `inspect` | Slower since v0.6.0 due to geometry type detection |
| `add-bbox` | 75x faster since v0.6.0 for large files |
| `extract` with geometry | Slow due to WKB serialization; use `--exclude-cols geometry` if not needed |
| `sort-hilbert` | Scales linearly with row count |

## Pre-Release Checklist

Before releasing a new version:

1. **Run benchmarks locally** against the previous release:
   ```bash
   # Install previous version
   git checkout v0.9.0 && pip install -e .
   python scripts/version_benchmark.py --version-label "v0.9.0" -o baseline.json -n 5

   # Install new version
   git checkout main && pip install -e .
   python scripts/version_benchmark.py --version-label "new" -o current.json -n 5

   # Compare
   python scripts/version_benchmark.py --compare baseline.json current.json
   ```

2. **Check for regressions** (>25% slower on large files)

3. **Document known changes** in release notes if performance differs intentionally

4. **Create release** - the release-benchmark workflow will automatically verify and append results
