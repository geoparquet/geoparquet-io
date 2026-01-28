# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `--show-sql` option now available on all DuckDB-based commands (add, partition, sort, extract arcgis)
  - Shows exact SQL queries as they execute for debugging and transparency
  - Complements existing `--verbose` flag

- `--verbose` option added to missing commands:
  - `gpio inspect` subcommands (summary, head, tail, stats, meta)
  - `gpio publish upload`

- Progress reporting added to data-processing commands:
  - `gpio add h3` - Shows "Adding H3 column..." during processing
  - `gpio add quadkey` - Shows "Adding quadkey column..." during processing
  - `gpio sort column` - Shows "Sorting by..." during processing

### Changed

- **BREAKING**: Renamed `--profile` to `--aws-profile` for clarity
  - Only affects AWS S3 operations (convert, extract, upload commands)
  - More accurately describes the parameter (sets AWS_PROFILE environment variable)
  - Local operations (add, partition, sort, check, inspect) no longer have this flag

- **BREAKING**: Removed `--profile` flag from local commands
  - Affects: add, partition, sort, check, inspect, publish stac
  - These commands don't need AWS profiles (local file operations only)
  - Follows Arrow-based pipeline pattern: extract/convert → transform locally → upload

### Removed

- **BREAKING**: Removed `gpio inspect legacy` command with flag-based interface
  - Use subcommands instead:
    - `gpio inspect head <file> [count]` (replaces `--head`)
    - `gpio inspect tail <file> [count]` (replaces `--tail`)
    - `gpio inspect stats <file>` (replaces `--stats`)
    - `gpio inspect meta <file>` (replaces `--meta`)
  - Removed 186 lines of Grade E complexity code
  - This command was hidden and deprecated - subcommands are the stable API

### Internal

- Simplified codebase by removing deprecated legacy inspect interface
