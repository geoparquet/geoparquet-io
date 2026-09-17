# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) documenting significant technical decisions in geoparquet-io.

## What is an ADR?

An ADR captures an architecturally significant decision along with its context and consequences. ADRs are:

- **Immutable once accepted**: We don't edit accepted ADRs. If a decision changes, we create a new ADR that supersedes the old one.
- **Numbered sequentially**: ADR-0001, ADR-0002, etc.
- **Short and focused**: Each ADR covers one decision.

## Creating a New ADR

1. Copy `template.md` to `NNNN-short-title.md` (next number in sequence)
2. Fill in all sections
3. Set status to "Proposed"
4. Open a PR for review
5. After merge, update status to "Accepted"

## ADR Index

| ADR | Title | Status |
|-----|-------|--------|
| [0001](0001-cli-core-separation.md) | CLI/Core Separation | Accepted |
| [0002](0002-duckdb-as-processing-engine.md) | DuckDB as Processing Engine | Accepted |
| [0003](0003-logging-over-click-echo.md) | Logging Over click.echo in Core | Accepted |
| [0004](0004-python-api-mirrors-cli.md) | Python API Mirrors CLI | Accepted |
| [0005](0005-test-fixture-strategy.md) | Test Fixture Strategy | Accepted |
| [0006](0006-coverage-floor-is-a-trailing-ratchet.md) | The Coverage Floor Is a Trailing Ratchet on the Combined Figure | Accepted |

## References

- [ADR GitHub Organization](https://adr.github.io/)
- [Michael Nygard's original article](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
