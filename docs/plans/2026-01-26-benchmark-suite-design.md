# Benchmark Suite Design

**Date:** 2026-01-26
**Status:** Draft

## Overview

A comprehensive performance testing system for gpio that measures speed and memory usage across a range of file sizes and operations. Designed to catch regressions before releases and validate that the library handles files larger than available memory.

## Goals

1. **Regression detection** — catch performance regressions before releases
2. **Memory efficiency validation** — prove streaming works with files 4x larger than memory
3. **Historical tracking** — compare performance across versions
4. **Flexible execution** — run locally, in CI, or on-demand via PR

## Test Files

All files are real-world datasets hosted on source.coop:

| Name | Size | Purpose |
|------|------|---------|
| `tiny.parquet` | ~80KB | Quick sanity check |
| `small.parquet` | ~1MB | Fast iteration baseline |
| `medium.parquet` | ~50MB | Moderate workload |
| `large.parquet` | ~500MB | Stress test |
| `xlarge.parquet` | ~2GB | Memory efficiency validation |

**Location:** `s3://source.coop/geoparquet-io/benchmarks/`

## Operations

### Core Set (default)

| Operation | What it tests |
|-----------|---------------|
| `read` | Load parquet into memory |
| `write` | Write table back to parquet |
| `convert-geojson` | GeoJSON → GeoParquet |
| `convert-gpkg` | GeoPackage → GeoParquet |
| `extract-bbox` | Spatial filtering |
| `extract-columns` | Column selection |
| `reproject` | CRS transformation (4326→3857) |
| `sort-hilbert` | Hilbert curve ordering |
| `add-bbox` | Compute bbox column |
| `partition-quadkey` | Partition by quadkey |

### Full Suite (optional)

Adds:
- `convert-shapefile`, `convert-fgb`
- `sort-quadkey`
- `add-h3`, `add-quadkey`, `add-country`
- `partition-h3`, `partition-country`

## Test Matrix

Two memory configurations, all files on both:

| Memory Limit | Files | Purpose |
|--------------|-------|---------|
| 512MB | tiny, small, medium, large, xlarge | Memory-constrained streaming |
| 4GB | tiny, small, medium, large, xlarge | Normal operation headroom |

The 512MB + xlarge (2GB file) combination validates that gpio can process files 4x larger than available memory.

## Regression Thresholds

| Level | Time | Memory |
|-------|------|--------|
| Warning | >10% slower | >20% more memory |
| Failure | >25% slower | >50% more memory |

Thresholds are configurable via CLI flags.

## CLI Interface

### Command Group

```
gpio benchmark
├── suite      Run the full benchmark suite
├── compare    Compare converters on a single file (existing)
└── report     Generate/view reports from stored results
```

### `gpio benchmark suite`

```
gpio benchmark suite [OPTIONS]

Options:
  --operations [core|full]    Operation set to run (default: core)
  --files [tiny|small|medium|large|xlarge|all]
                              File sizes to test (default: all)
  --memory-limit TEXT         Memory limit for Docker, e.g. "512m" (local only)
  --profile [standard|comprehensive]
                              Output detail level (default: standard)
  --compare PATH              Compare against baseline JSON file
  --output PATH               Write results to JSON file
  --iterations INTEGER        Runs per operation (default: 3)
  --threshold-time FLOAT      Regression threshold for time (default: 0.10)
  --threshold-memory FLOAT    Regression threshold for memory (default: 0.20)
  -v, --verbose               Show detailed progress
```

### `gpio benchmark report`

```
gpio benchmark report [OPTIONS] [RESULT_FILES]...

Options:
  --format [table|json|markdown]  Output format (default: table)
  --compare PATH                  Compare two result files
  --trend                         Show trend across multiple versions
```

## Result Format

### JSON Structure

```json
{
  "version": "0.5.0",
  "timestamp": "2026-01-26T14:30:00Z",
  "environment": {
    "os": "Linux",
    "cpu": "AMD EPYC 7763 / 4 cores",
    "ram": "7 GB",
    "python": "3.11.5",
    "duckdb": "1.1.0"
  },
  "results": [
    {
      "operation": "sort-hilbert",
      "file": "medium.parquet",
      "memory_limit_mb": 512,
      "time_seconds": 4.23,
      "peak_memory_mb": 387,
      "output_size_mb": 48.2,
      "success": true
    }
  ]
}
```

### Comparison Output

```
## Benchmark Results vs main

| Operation | File | Time | Δ | Memory | Δ | Status |
|-----------|------|------|---|--------|---|--------|
| sort-hilbert | medium | 4.23s | +8% | 387MB | +3% | ✓ |
| reproject | large | 12.1s | +32% | 1.8GB | +12% | ⚠️ REGRESSION |
```

## GitHub Actions Workflow

### Triggers

- `workflow_dispatch` — manual trigger with inputs
- `pull_request` with label `benchmark` — runs comparison against main
- Weekly schedule (optional) — track trends over time

### Job Matrix

```yaml
jobs:
  benchmark:
    strategy:
      matrix:
        include:
          - name: "constrained-512mb"
            memory: "512m"
          - name: "normal-4gb"
            memory: "4g"
```

### Steps

1. Checkout code
2. Pull test files from source.coop (cached)
3. Build Docker image with gpio installed
4. Run `docker run --memory={limit} gpio benchmark suite`
5. Upload results as artifact

### Aggregation Job

- Waits for all matrix jobs
- Merges results into single JSON
- On main: commits to `benchmarks/results/`
- On PR: posts comparison comment

## Comprehensive Profiling Mode

Enabled with `--profile comprehensive`:

### Additional Metrics

- Per-phase timing (read, transform, write separated)
- Memory over time (sampled every 100ms)
- Peak RSS vs. heap allocation
- DuckDB query plans (for SQL-based operations)
- Output file statistics (row groups, compression ratio)

### Additional Outputs

- `*_memory_timeline.json` — memory samples over time
- `*_profile.json` — cProfile output as JSON
- Flame graph SVG (using py-spy if available)

## File Structure

```
geoparquet_io/
├── core/
│   ├── benchmark.py              # existing (compare logic)
│   └── benchmark_suite.py        # NEW: suite runner
├── cli/
│   └── main.py                   # benchmark command group

benchmarks/
├── results/                      # Git-tracked results
│   ├── v0.5.0.json
│   ├── v0.5.1.json
│   └── latest.json              # Symlink to most recent
├── config.py                    # Threshold defaults, operation definitions
└── Dockerfile                   # Image for memory-constrained runs

.github/workflows/
└── benchmark.yml                # CI workflow

tests/
└── test_benchmark_suite.py      # Unit tests
```

## Dependencies

- `psutil` — already present
- `tracemalloc` — stdlib, already used
- `py-spy` — optional, for flame graphs in comprehensive mode

No new required dependencies.

## Runtime Estimate

~35-45 minutes for full suite (2 memory tiers × 5 files × 10 operations)

## Open Questions

1. Which specific real-world datasets to use for each file size?
2. Should we cache test files in GitHub Actions between runs?
3. Naming convention for result files (version-based vs. date-based)?
