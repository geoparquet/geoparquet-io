# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
- Add support for partitioning by multiple admin levels from GAUL and Overture Maps ([#38](https://github.com/cholmes/geoparquet-io/pull/38))

## [0.2.0] - 2025-10-24

### Added
- MkDocs documentation site with GitHub Pages deployment ([#35](https://github.com/cholmes/geoparquet-io/pull/35))
  - Comprehensive user guide and CLI reference
  - API documentation
  - Real-world examples
  - Published at https://cholmes.github.io/geoparquet-io/

### Changed
- Consolidated 177 lines of duplicated CLI option definitions into reusable decorators ([#36](https://github.com/cholmes/geoparquet-io/pull/36))

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
- **H3 Support**: H3 hexagonal cell ID support via DuckDB H3 extension ([#23](https://github.com/cholmes/geoparquet-io/pull/23))
  - `gpio add h3` and `gpio partition h3` commands
  - H3 columns excluded from partition output by default (configurable)
  - Enhanced metadata system for custom covering metadata (bbox + H3) in GeoParquet 1.1 spec
- **KD-tree Partitioning**: Balanced spatial partitioning support ([#30](https://github.com/cholmes/geoparquet-io/pull/30))
  - `gpio add kdtree` and `gpio partition kdtree` commands
  - Auto-select partitions targeting ~120k rows using approximate mode
  - Exact computation mode available for deterministic results
- **Inspect Command**: Fast metadata inspection ([#31](https://github.com/cholmes/geoparquet-io/pull/31))
  - Optional data preview with `--head`/`--tail` flags
  - Column statistics with `--stats` flag
  - JSON output support with `--json` flag
- **Country Codes**: Spatial join with admin boundaries to add ISO codes
- **Partitioning**: Split files by string columns or admin divisions
  - Support for Hive-style partitioning
  - Preview mode to inspect partitions before creating
  - Character prefix partitioning
  - Intelligent partition strategy analysis with configurable thresholds
  - `--force` to override warnings, `--skip-analysis` for performance
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
  - Created generic `add_computed_column` helper to minimize boilerplate
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

[Unreleased]: https://github.com/cholmes/geoparquet-io/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/cholmes/geoparquet-io/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/cholmes/geoparquet-io/releases/tag/v0.1.0
[0.0.1]: https://github.com/cholmes/geoparquet-tools/releases/tag/v0.0.1
