# Performance Benchmarks

gpio includes a benchmark suite for measuring performance and detecting regressions across versions.

## Quick Start

Run benchmarks comparing current version against a previous release:

<!-- doctest: skip="runs a script that ships outside the guide" -->
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

<!-- doctest: skip="runs a script that ships outside the guide" -->
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

# Analyze trends across multiple baselines (oldest to newest)
python scripts/version_benchmark.py --trend results_v0.7.0.json results_v0.8.0.json results_v0.9.0.json

# Customize degradation threshold for trend detection (default: 0.05 = 5%)
python scripts/version_benchmark.py --trend baseline1.json baseline2.json baseline3.json --trend-threshold 0.10

# Skip local caching (test remote file performance)
python scripts/version_benchmark.py --version-label "remote-test" --no-cache
```

### Managing Historical Baselines

Use the `scripts/manage_baselines.py` tool to work with baselines stored in GitHub artifacts:

<!-- doctest: skip="runs a script that ships outside the guide" -->
```bash
# List available baselines
uv run python scripts/manage_baselines.py list

# Download specific baseline versions
uv run python scripts/manage_baselines.py download v0.9.0 v0.8.0

# Compare specific baselines (downloads if needed)
uv run python scripts/manage_baselines.py compare v0.8.0 v0.9.0

# Analyze trends across multiple versions
uv run python scripts/manage_baselines.py trends v0.7.0 v0.8.0 v0.9.0

# Use custom degradation threshold
uv run python scripts/manage_baselines.py trends v0.7.0 v0.8.0 v0.9.0 --threshold 0.10
```

**Authentication:**
- Requires GitHub token: set `GITHUB_TOKEN` or authenticate with `gh auth login`
- Auto-detects repository from git remote
- Downloads baselines to `baselines/` directory by default

### Sample Output

**Point-in-time comparison:**

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

**Trend analysis across releases:**

```
======================================================================
Trend Analysis Across Releases
======================================================================
Versions: v0.7.0 → v0.8.0 → v0.9.0
Baselines: 3
Operations tracked: 42

Overall Statistics:
  Average change: +1.23%
  Max regression: +12.5%
  Max improvement: -8.3%

⚠️  Gradual Degradation Detected (2 operations):
----------------------------------------------------------------------
  • extract-limit (small): 7.2% avg degradation over last 2 releases
  • partition-quadkey (medium): 6.1% avg degradation over last 2 releases

🚀 Consistent Improvements (3 operations):
----------------------------------------------------------------------
  • add-bbox (large): 8.5% avg improvement over last 2 releases
  • sort-hilbert (small): 5.9% avg improvement over last 2 releases
  • inspect (tiny): 5.2% avg improvement over last 2 releases
```

### CLI Benchmark Commands

gpio also includes built-in benchmark commands:

<!-- doctest: skip="needs path/to/file.parquet, which the harness does not seed" -->
```bash
# Run benchmark suite on specific files
gpio benchmark suite --files path/to/file.parquet --operations core

# Compare converter performance on a single file
gpio benchmark compare path/to/file.parquet
```

## Profiling Integration

When benchmarks identify performance regressions, profiling helps diagnose which code paths are responsible.

### Enabling Profiling

Add the `--profile` flag to enable cProfile integration:

=== "CLI"
    <!-- doctest: skip="needs path/to/file.parquet, which the harness does not seed" -->
    ```bash
    # Run benchmarks with profiling enabled
    gpio benchmark suite \
      --files path/to/file.parquet \
      --operations core \
      --profile \
      --profile-dir ./profiles

    # Profile specific operations
    gpio benchmark suite \
      --files large.parquet \
      --operations add-bbox,sort-hilbert \
      --profile
    ```

=== "Python"
    <!-- doctest: skip="names a benchmark operation ('extract') the suite does not define" -->
    ```python
    from geoparquet_io.core.benchmark_suite import run_benchmark_suite
    from pathlib import Path

    # Run benchmarks with profiling enabled
    result = run_benchmark_suite(
        input_files=[Path('path/to/file.parquet')],
        operations=['add-bbox', 'extract', 'inspect'],
        iterations=3,
        profile=True,
        profile_dir=Path('./profiles'),
        verbose=True
    )

    # Profile files are saved in ./profiles/
    print(f"Generated {len(result.results)} benchmark results")
    ```

This generates `.prof` files in the specified directory (default: `./profiles/`).

### Analyzing Profile Data

**View profile interactively:**

=== "CLI"
    ```bash
    uv run python -m pstats profiles/add-bbox_large_1.prof
    # Then use commands like:
    # - stats 20  (show top 20 functions)
    # - sort cumtime  (sort by cumulative time)
    # - callers duckdb  (show callers of duckdb functions)
    ```

=== "Python"
    <!-- doctest: skip="passes a str where the benchmark API expects a Path" -->
    ```python
    from geoparquet_io.benchmarks.profile_report import format_profile_stats

    # Show top 20 slowest functions
    summary = format_profile_stats('profiles/add-bbox_large_1.prof', top_n=20)
    print(summary)
    ```

**Sample profile output:**
```
Profile: add-bbox_large_1.prof
================================================================================

Top 20 functions by cumulative time:

   ncalls  tottime  percall  cumtime  percall filename:lineno(function)
        1    0.002    0.002   12.456   12.456 geoparquet_io/core/add_column.py:45(add_bbox_column)
        1    0.001    0.001   11.234   11.234 duckdb.py:123(execute)
      100    5.678    0.057    9.876    0.099 duckdb.py:234(_fetch_arrow)
    10000    2.345    0.000    3.456    0.000 pyarrow.lib:456(cast)
```

### DuckDB Query Profiles

cProfile shows where the Python time goes, but most of a conversion's work
happens inside DuckDB queries. Set the `GPIO_DUCKDB_PROFILE` environment
variable to a directory to turn on DuckDB's own profiler for every connection
gpio opens — any command, no flags or code changes:

=== "CLI"
    ```bash
    GPIO_DUCKDB_PROFILE=profiles gpio convert geoparquet input.parquet output.parquet
    ls profiles
    # gpio-profile-20261015T110948123456-p99396-3fa2c1-c1-q0017.json
    # gpio-profile-20261015T110948123456-p99396-3fa2c1-c1-q0021.json
    ```

=== "Python"
    ```python
    import os

    os.environ["GPIO_DUCKDB_PROFILE"] = "profiles"

    import geoparquet_io as gpio

    gpio.convert("input.parquet").write("output.parquet")
    ```

The value is a **directory path**, not an on/off switch. It is resolved to an
absolute path when each connection opens, so a later change of working
directory does not move it, and it is created if missing. A value that looks
like a flag (`1`, `true`, `yes`, `on`, `0`, `false`, `no`, `off`) is only used
when a directory of that name already exists; otherwise gpio warns and runs
without profiling rather than creating a `./true/` directory. Any other
unusable path also logs a warning and the command runs on without profiling.

Each profile is its own JSON file, named
`gpio-profile-<time>-p<pid>-<token>-c<connection>-q<statement>.json`. The
microsecond time, process ID and random token keep names distinct across
processes (containers often all run as PID 1), and gpio never picks a name
that is already on disk, so one query's file never overwrites another's. The
`query_name` key in each file holds the SQL text and the operator tree below
it carries per-operator timings, so you can see which query inside a command
is slow.

**Which statements get a profile.** DuckDB decides, and it writes a profile
only when a statement's result is *fully consumed*: a `COPY ... TO` write, a
`fetchall()`, an Arrow table read to the end. Those are the reads and writes
that move a command's data, so they are the ones you usually want. Several
kinds of statement leave no file, which is why the statement numbers have
gaps:

- `SET`, `CREATE` and other statements DuckDB does not profile;
- small lookups gpio reads with a single `fetchone()` (a row count, a bounding
  box, a metadata probe), whose result is never exhausted;
- a result abandoned when the next statement starts.

A `gpio sort hilbert` run, for example, writes a single profile — for the
`COPY` that writes its output — out of more than a dozen statements. To
profile a statement that leaves no file, take its SQL (`--show-sql` prints it
on the commands that offer it) and run it yourself under `EXPLAIN ANALYZE` in
the DuckDB CLI. Setting DuckDB's `enable_profiling` by hand does not help: it
follows the same fully-consumed rule.

Two further caveats. For code that uses a gpio connection directly, a DuckDB
relation from `con.sql()` runs when it is fetched, not when it is built, so if
another query ran in between, the relation's profile replaces that query's
file. And a profile directory deleted or
made unwritable between statements is re-created, or profiling is switched
off for that connection with a single warning — but one removed (or a disk
that fills) *while* a statement runs fails that statement, because DuckDB
writes the profile as the statement finishes.

### Profiling Overhead

- Profiling adds ~5-15% overhead to benchmark timing
- Profile files are typically 50-500KB each
- Disabled by default to keep benchmarks fast

### CI Integration

The benchmark workflow automatically suggests profiling when regressions are detected:

```
⚠️ Performance regression detected (+25% slower on sort-hilbert)

💡 To diagnose, run locally with profiling:
   gpio benchmark suite --files large.parquet --operations sort-hilbert --profile
```

Profile artifacts are uploaded with 30-day retention when profiling is enabled.

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
4. Fetch historical baselines from up to 5 previous releases
5. Analyze performance trends across releases
6. Append results to the release notes

**Results include:**
- Point-in-time comparison table showing performance delta
- Performance trends across multiple releases
- Warning for significant regressions (>25% in single release)
- Warning for gradual degradation (>5% per release for 2+ consecutive releases)
- Detailed benchmark data in collapsible section

**Baseline Storage:**
- Baselines are stored as GitHub Actions artifacts
- Retention: 400 days (covers ~5-10 releases)
- Artifact naming: `release-benchmark-{version}`
- Contains: benchmark results JSON, comparison text, trend analysis

### Where Results Are Published

| Trigger | Results Location |
|---------|------------------|
| PR with `benchmark` label | Comment on PR |
| Manual workflow run | Workflow summary + artifacts |
| Release | Appended to release notes |

All runs also upload JSON artifacts for historical tracking.

## Interpreting Results

### Regression Thresholds

**Point-in-time (single release comparison):**

| Severity | Threshold | Action |
|----------|-----------|--------|
| Normal variance | ±10% | No action needed |
| Warning | +10-25% | Investigate cause |
| Regression | >+25% | Flagged in release notes |

**Trend analysis (across multiple releases):**

| Pattern | Threshold | Action |
|---------|-----------|--------|
| Gradual degradation | >5% per release for 2+ consecutive releases | Warning flagged |
| Consistent improvement | >5% per release for 2+ consecutive releases | Highlighted |
| Single spike | One-time regression/improvement | Ignored (not a trend) |

Trend analysis helps detect gradual performance drift that might be missed when comparing only adjacent releases.

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
   <!-- doctest: skip="runs a script that ships outside the guide" -->
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
