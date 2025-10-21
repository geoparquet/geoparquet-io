# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
- Created generic `add_computed_column` helper in `common.py` to minimize boilerplate for adding computed columns
- Added H3 hexagonal cell ID support via DuckDB H3 extension with `gpio add h3` and `gpio partition h3` commands. H3 columns are excluded from partition output by default (use `--keep-h3-column` to retain), except for Hive partitioning where they're kept by default.
- Enhanced metadata system to support custom covering metadata for multiple spatial indices (bbox + H3) in GeoParquet 1.1 spec
- Added intelligent partition strategy analysis that automatically validates partition plans before execution with configurable thresholds (errors for pathological cases like >10K partitions or <100 avg rows, warnings for moderate issues). New flags: `--force` to override errors, `--skip-analysis` for performance, and enhanced `--preview` for dry-run with actionable recommendations.
- Added KD-tree partitioning support with large dataset protection ([#30](https://github.com/cholmes/geoparquet-io/pull/30))

## [0.1.0] - 2025-10-19

### Added

#### Package & CLI
- Renamed package from `geoparquet-tools` to `geoparquet-io` for clearer purpose
- New CLI command: `gpio` (GeoParquet I/O) for all operations
- Legacy `gt` command maintained as alias for backwards compatibility
- Version flag: `gpio --version` displays current version
- Comprehensive help text for all commands with usage examples

#### Development Tools
- Migrated to `uv` package manager for faster, more reproducible builds
- Added `ruff` for linting and code formatting with comprehensive ruleset
- Setup pre-commit hooks for automated code quality checks
- Added custom pytest markers (`slow`, `network`) for better test organization
- Created `CONTRIBUTING.md` with detailed development guidelines
- Created `CHANGELOG.md` for tracking changes

#### CI/CD
- GitHub Actions workflow for automated testing
- Lint job using ruff for code quality enforcement
- Test matrix covering Python 3.9-3.13 on Linux, macOS, and Windows
- Code coverage reporting with pytest-cov
- Optimized CI with uv caching for faster runs

#### Core Features
- **Spatial Sorting**: Hilbert curve ordering for optimal spatial locality
- **Bbox Operations**: Add bbox columns and metadata for query performance
- **Country Codes**: Spatial join with admin boundaries to add ISO codes
- **Partitioning**: Split files by string columns or admin divisions
  - Support for Hive-style partitioning
  - Preview mode to inspect partitions before creating
  - Character prefix partitioning
- **Checking**: Validate GeoParquet files against best practices
  - Compression settings
  - Spatial ordering
  - Bbox structure and metadata
  - Row group optimization

#### Output Options
- Configurable compression (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
- Compression level control for supported formats
- Flexible row group sizing (by count or size)
- Automatic metadata preservation and enhancement
- GeoParquet 1.1 format support with bbox covering metadata

### Changed

- Updated README.md with `gpio` command examples throughout
- Improved CLI help messages and command documentation
- All commands now reference `gpio` instead of `gt` in user-facing messages
- Organized code into clear `core/` and `cli/` modules
- Centralized common utilities in `core/common.py`
- Standardized compression and metadata handling across all commands

### Fixed

- Proper handling of Hive-partitioned files in metadata operations
- Consistent bbox metadata format across all output operations
- Improved error messages and validation
- Fixed linting issues across codebase (exception handling, imports, etc.)

### Infrastructure

- Added `.pre-commit-config.yaml` for automated checks
- Added `pyproject.toml` configuration for all tools
- Generated `uv.lock` for reproducible installs
- Added `.ruff_cache` to `.gitignore`
- Updated `.github/workflows/tests.yml` with lint and test jobs

## [0.0.1] - 2024-10-10 (Previous - geoparquet-tools)

Initial release as `geoparquet-tools` with basic functionality.

[Unreleased]: https://github.com/cholmes/geoparquet-io/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/cholmes/geoparquet-io/releases/tag/v0.1.0
[0.0.1]: https://github.com/cholmes/geoparquet-tools/releases/tag/v0.0.1
