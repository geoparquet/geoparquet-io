# ADR-0005: Test Fixture Strategy

## Status

Accepted

## Context

geoparquet-io operates on GeoParquet files, which are binary Parquet files containing geometry columns with specific metadata. Tests need reliable, predictable test data, but real-world GeoParquet files are often too large to commit to the repository and may contain sensitive or licensed data.

The test suite needs to:

1. Run quickly in CI (target: under 60 seconds for non-slow tests).
2. Cover valid and invalid file variants.
3. Be deterministic and reproducible.
4. Avoid large binary files bloating the repository.

## Decision

Test data is organized into tiers based on size and usage:

1. **Small fixtures in `tests/data/`** (committed to git, each file under 1MB): Pre-built Parquet, GeoPackage, GeoJSON, Shapefile, and CSV files that cover common scenarios. These include files with different CRS projections (EPSG:4326, EPSG:5070, EPSG:6933), compression codecs (Snappy, Zstd, Brotli), and geometry types. Both valid and deliberately malformed files are included for validation testing.

2. **Generated fixtures via helper scripts**: `tests/data/create_csv_fixtures.py` and `tests/data/generate_test_fixtures.py` generate test data programmatically when the fixture needs to represent specific edge cases or when binary files would be too large.

3. **Network-dependent fixtures** (marked `@pytest.mark.network`): Tests that fetch real-world data from remote sources. These are skipped in fast CI runs and used for integration testing.

4. **Slow test fixtures** (marked `@pytest.mark.slow`): Tests using larger datasets or operations that take more than 5 seconds (format conversions, reprojection). Skipped by default with `-m "not slow"`.

Test markers control which tiers run:

```bash
# Fast development cycle (default)
uv run pytest -n auto -m "not slow and not network"

# Full suite including slow tests
uv run pytest -n auto -m "not network"

# Everything including network tests
uv run pytest -n auto
```

## Consequences

### Positive
- Fast test runs (under 60 seconds for non-slow, non-network tests) enable rapid development iteration.
- Committed fixtures ensure tests are reproducible across environments without external dependencies.
- Multiple file format variants (different CRS, codecs, geometry types) catch format-specific bugs.
- Test markers allow developers to choose the appropriate test scope for their workflow.

### Negative
- Committed binary fixtures (Parquet, GeoPackage, Shapefile) increase repository size, though the 1MB limit per file keeps this manageable.
- Fixture files may drift from real-world data patterns, missing edge cases present in production files.
- Maintaining both valid and invalid fixture variants requires discipline to keep them synchronized with format changes.

### Neutral
- The `tests/data/` directory currently contains ~30 fixture files totaling approximately 1.5MB.
- Some test modules create temporary fixtures in pytest's `tmp_path` for tests that modify data, ensuring test isolation.
- Coverage requirement is 85% minimum (enforced via `--cov-fail-under=85`; a trailing ratchet two points under the measured full-fast-suite number, which is the combined line+branch figure since `branch = true`), with a higher target for new code.

## Alternatives Considered

### Generate all data on-the-fly
Creating test data programmatically in each test function or conftest.py. Rejected because generating valid GeoParquet files with spatial metadata is slow, non-trivial, and adds significant test setup complexity. Pre-built fixtures are faster and ensure exact binary reproducibility.

### Use Git LFS for large fixtures
Storing large real-world files in Git LFS for comprehensive testing. Rejected as the primary strategy because it adds Git LFS as a development dependency and slows clone times. However, this remains an option for future integration test suites.

### Property-based testing (Hypothesis)
Generating random test data with Hypothesis for fuzz testing. Not rejected but deferred -- property-based testing is useful for finding edge cases in geometry handling but is slow and non-deterministic, making it unsuitable as the primary testing strategy.

## References

- `tests/data/` -- Committed test fixtures
- `tests/data/create_csv_fixtures.py` -- CSV fixture generator
- `tests/data/generate_test_fixtures.py` -- Parquet fixture generator
- `tests/conftest.py` -- Shared pytest fixtures and configuration
- `pyproject.toml` -- pytest markers configuration (`slow`, `network`)
