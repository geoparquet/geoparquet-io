# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
