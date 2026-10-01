# ADR-0007: Rust Array Libraries for Geometry Transforms That DuckDB Cannot Do

## Status

Accepted

## Context

`gpio process simplify`, `gpio process polygonize` and `gpio process contour` are
backed by two external Rust libraries — [coarsen](https://github.com/isaaccorley/coarsen)
(drop-in replacements for `shapely.simplify` / `shapely.coverage_simplify`,
byte-identical to GEOS) and [contourrs](https://github.com/isaaccorley/contourrs)
(raster polygonization and contour bands, Arrow/WKB output).

ADR-0002 makes DuckDB the processing engine, but DuckDB spatial has no
coverage-preserving simplification and no raster support at all. Both libraries
operate on in-memory arrays (shapely geometry arrays; 2D numpy rasters), not SQL
relations, so these commands run the geometry operation outside DuckDB while all
I/O still flows through gpio's Arrow machinery and the write funnels
(`write_geoparquet_table`), which own row groups, output version and the `geo` block.

Install constraints shaped the dependency decision:

- contourrs publishes wheels only for CPython 3.12–3.14; gpio supports ≥3.10, so a
  hard dependency would break installs on 3.10/3.11.
- coarsen is LGPL-2.1-or-later (gpio is Apache-2.0) and is a young project.
- The raster commands additionally need rasterio to read GeoTIFFs — a large wheel
  most gpio users don't need.

## Decision

Ship the three commands built in (not as `gpio.plugins` plugins — precedent:
pmtiles was promoted from plugin to built-in), with the libraries as **optional
dependencies**:

- Two extras in `pyproject.toml`: `simplify` (coarsen) and `raster` (contourrs with a
  `python_version >= '3.12'` marker, plus rasterio).
- Lazy function-level imports behind `require_coarsen()` / `require_contourrs()` /
  `require_rasterio()` in `core/optional_deps.py`, raising `OptionalDependencyError`
  with install instructions (the owslib pattern). `import geoparquet_io` never touches
  the optional packages.
- The libraries stay in their own upstream repos; gpio pins minimum versions only and
  carries cross-validation tests (GEOS/GDAL reference comparisons, roundtrips) that
  double as an upstream regression signal. Tests skip when the optional package is
  absent and run in CI legs where it installs.
- coarsen is consumed as an unmodified, dynamically imported dependency — never
  vendor code from it (LGPL-2.1).

## Consequences

### Positive
- `pip install geoparquet-io` stays lean and keeps working on Python 3.10/3.11 and
  platforms the Rust wheels don't cover.
- Users get GEOS-quality simplification at Rust speed and gpio's first raster→vector
  path with one extra: `pip install 'geoparquet-io[simplify]'` / `'geoparquet-io[raster]'`.
- Upstream projects evolve independently; gpio's invariant tests catch regressions
  before users do.

### Negative
- A second processing idiom beside DuckDB SQL: these paths materialize geometry
  arrays in memory (coverage simplification needs the whole column at once).
- The diff-cover CI leg runs Python 3.11, where contourrs cannot install, so the
  contourrs-touching code needs a dependency-free unit-test layer to stay covered.
- `pip install 'geoparquet-io[raster]'` on 3.10/3.11 silently skips contourrs (the
  marker); the runtime error message must explain the 3.12+ floor.

### Neutral
- First optional *feature* extras in gpio (previous extras were developer-facing).
- First raster input surface; contained to `core/process/raster/`.

## Alternatives Considered

### Hard dependencies
Smallest install story for users who have them, but contourrs would break
`pip install geoparquet-io` on Python 3.10/3.11 outright, and LGPL coarsen would ship
to every user. Rejected.

### Separate plugin packages (`gpio-coarsen`, `gpio-contourrs`)
Maximum decoupling via the existing `gpio.plugins` entry-point system, but a worse
install story (`--with` juggling), no CI coverage for plugins today, and the point of
the integration is first-class commands with gpio-grade tests. Rejected.

### Reimplement in DuckDB SQL
No coverage-simplify or raster support exists in DuckDB spatial; reimplementing GEOS
simplification or marching squares in SQL is out of scope and would lose the
byte-identical-to-GEOS property coarsen provides. Rejected.

## References

- ADR-0002: DuckDB as the processing engine (the rule these commands are the
  documented exception to)
- ADR-0004: Python API mirrors CLI (applies to the three new commands)
- `geoparquet_io/core/optional_deps.py`, `geoparquet_io/core/process/simplify.py`,
  `geoparquet_io/core/process/raster/`
- coarsen: https://github.com/isaaccorley/coarsen (LGPL-2.1-or-later)
- contourrs: https://github.com/isaaccorley/contourrs (Apache-2.0)
