# GeoParquet Tools Test Suite

This directory contains the comprehensive test suite for geoparquet-tools.

## Overview

The test suite covers all major commands and functionality:
- **Check commands**: Testing validation and inspection of GeoParquet files
- **Sort commands**: Testing Hilbert curve spatial ordering
- **Add commands**: Testing adding bbox columns and admin divisions
- **Format commands**: Testing bbox metadata formatting
- **Partition commands**: Testing partitioning by string columns and admin boundaries

## Test Files

- `test_check.py` - Tests for check commands (all, spatial, compression, bbox, row-group)
- `test_sort.py` - Tests for sort commands (hilbert)
- `test_add.py` - Tests for add commands (bbox, admin-divisions)
- `test_format.py` - Tests for format commands (bbox-metadata)
- `test_partition.py` - Tests for partition commands (string, admin)
- `conftest.py` - Shared fixtures and configuration

## Test Data

The `data/` directory contains test GeoParquet files:
- `places_test.parquet` - 766 rows with place data including bbox column
- `buildings_test.parquet` - 42 rows with building geometries

## Running Tests

### Run all tests
```bash
pytest
```

### Run with verbose output
```bash
pytest -v
```

### Run with coverage
```bash
pytest --cov=geoparquet_tools --cov-report=term-missing
```

### Run specific test file
```bash
pytest tests/test_check.py
```

### Run specific test
```bash
pytest tests/test_check.py::TestCheckCommands::test_check_all_places
```

## Coverage

Current test coverage is approximately 71%, covering:
- CLI command interfaces
- Core functionality for all major operations
- Error handling for invalid inputs
- Output file verification
- Data preservation checks

## Continuous Integration

The test suite is automatically run on every commit via GitHub Actions (`.github/workflows/tests.yml`).

The CI runs tests on:
- Multiple operating systems (Ubuntu, macOS, Windows)
- Multiple Python versions (3.9, 3.10, 3.11, 3.12)

## Adding New Tests

When adding new functionality:
1. Add tests to the appropriate test file
2. Use the fixtures from `conftest.py` for test data and temporary directories
3. Follow the existing test patterns for consistency
4. Ensure tests clean up after themselves (fixtures handle cleanup automatically)

## Test Fixtures

Common fixtures available in all tests:
- `test_data_dir` - Path to test data directory
- `places_test_file` - Path to places test parquet file
- `buildings_test_file` - Path to buildings test parquet file
- `temp_output_dir` - Temporary directory for test outputs (auto-cleaned)
- `temp_output_file` - Temporary output file path (auto-cleaned)
