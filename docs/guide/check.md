# Checking Best Practices

The `check` commands validate GeoParquet files against [best practices](https://github.com/opengeospatial/geoparquet/pull/254/files).

## Run All Checks

=== "CLI"

    ```bash
    gpio check all myfile.parquet
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    table = gpio.read('myfile.parquet')
    result = table.check()

    if result.passed():
        print("All checks passed!")
    else:
        for failure in result.failures():
            print(f"Failed: {failure}")

    # Get full results as dictionary
    details = result.to_dict()
    ```

Runs all validation checks:

- Spatial ordering
- Compression settings
- Bbox structure and metadata
- Row group optimization
- Bloom filter detection
- GeoParquet v2.0 upgrade recommendation (for v1.1 files)
- Spec validation

## Individual Checks

### Spatial Ordering

=== "CLI"

    ```bash
    gpio check spatial myfile.parquet
    ```

=== "Python"

    <!-- doctest: prelude="import geoparquet_io as gpio; table = gpio.read('myfile.parquet')" -->
    ```python
    result = table.check_spatial()
    print(f"Spatially ordered: {result.passed()}")
    ```

Checks if data is spatially ordered. Spatially ordered data improves:

- Query performance (10-100x faster for spatial queries)
- Compression ratios
- Cloud access patterns

**Method Selection:**

- **GeoParquet 2.0+ files** (with bbox column): Uses fast bbox-stats method by analyzing row group metadata (~10-100x faster)
- **GeoParquet 1.x files** (no bbox column): Falls back to sampling method which analyzes actual geometry data

!!! tip "For faster spatial order checks"
    Add a bbox column to your file with `gpio add bbox` to enable the fast bbox-stats method.

**How it works:**

- **Bbox-stats method**: Checks if consecutive row groups have overlapping bounding boxes. Non-overlapping row groups indicate good spatial ordering. Passes if < 30% of row group pairs overlap.
- **Sampling method**: Compares average distance between consecutive features vs random feature pairs. Lower ratio indicates better spatial clustering. Passes if ratio < 0.5.

### Compression

=== "CLI"

    ```bash
    gpio check compression myfile.parquet
    ```

=== "Python"

    <!-- doctest: prelude="import geoparquet_io as gpio; table = gpio.read('myfile.parquet')" -->
    ```python
    result = table.check_compression()
    print(f"Compression optimal: {result.passed()}")
    ```

Validates geometry column compression settings.

### Bbox Structure

=== "CLI"

    ```bash
    gpio check bbox myfile.parquet
    ```

=== "Python"

    <!-- doctest: prelude="import geoparquet_io as gpio; table = gpio.read('myfile.parquet')" -->
    ```python
    result = table.check_bbox()
    if not result.passed():
        # Add bbox if missing
        table = table.add_bbox().add_bbox_metadata()
    ```

Verifies:

- Bbox column structure
- GeoParquet metadata version
- Bbox covering metadata

A bbox column is optional in GeoParquet 2.0, but it must be declared, and the
declaration must be true:

- A bbox column that no `covering` entry points at costs file size and cannot be
  used by any reader, so `check bbox` fails the file and suggests
  `gpio add bbox-metadata` (or `--fix` to drop the column). `--fix` only ever
  removes an *undeclared* column — a covering the file legitimately declares is
  never deleted.
- A `covering` that names a column the file does not contain is treated as
  undeclared rather than accepted. Such a covering makes readers prune away rows
  that genuinely match, so it is worse than none at all.

`check spec` runs its four `covering` checks — paths well-formed, column
present, struct shape, field types — at GeoParquet 1.1 **and** 2.0. They were
previously gated to 1.1 only, so an identical broken covering failed at 1.1 and
passed at 2.0.

### Row Groups

=== "CLI"

    ```bash
    gpio check row-group myfile.parquet
    ```

=== "Python"

    <!-- doctest: prelude="import geoparquet_io as gpio; table = gpio.read('myfile.parquet')" -->
    ```python
    result = table.check_row_groups()
    for rec in result.recommendations():
        print(rec)
    ```

Checks row group size optimization for cloud-native access.

The check reports two numbers, and they answer different questions:

- **The verdict is 10,000-200,000 rows per group.** This is what passes or fails, and it is deliberately wide: it has to accept the layouts mainstream writers produce at their own defaults — DuckDB writes 122,880 rows per group, gpio's general write default lands at 100,352, and `gpio sort` writes 49,152. A pass/fail band that excluded all of those would be reporting on the ecosystem rather than on your file. Outside it the check reports `suboptimal`, and below 2,000 or above 1,000,000 rows, `poor`.
- **The advice is 10,000-50,000 rows per group**, for spatial queries. This is the band with measurements behind it, so a file can sit inside the verdict band and still be worth re-sorting.

!!! tip "Spatial filter pushdown and row group sizing"
    For GeoParquet 2.0 or parquet-geo-only files with Hilbert sorting, row groups of 10,000-50,000 rows create tighter bounding boxes that enable more row group skipping during spatial queries. Re-sorting nine published catalog files from 100,000 to 50,000 rows per group cut the share of the file a query window covering 10% of each dimension has to read from 43-100% down to 10-28% ([#775](https://github.com/geoparquet/geoparquet-io/issues/775)).

    Neither end of that band is a size a row group can actually have. The Parquet writer emits row groups in whole 2,048-row vectors and rounds a request *up* to a multiple of that, so a request for the band's top of 50,000 lands at 51,200 — just outside it. That used to be exactly what `gpio sort` asked for, so `gpio check optimization`, which scores this band as one of its five factors, marked a freshly sorted file `[fail]` and advised re-partitioning it ([#961](https://github.com/geoparquet/geoparquet-io/issues/961)). The band did not move; `gpio sort` now snaps its request to a whole vector itself, which maps the band's endpoints to 10,240 and 49,152, and writes 49,152 rows per group by default. It snaps up, as the writer does, except where that would push a request out of the top of the band — 50,000 becomes 49,152 rather than 51,200. Other write paths do not snap — if you pass `--row-group-size 50000` to `gpio convert`, the writer still rounds it up to 51,200.

    `--fix` writes those same 49,152-row groups. It is a repair, so the file it leaves behind has to pass the checks gpio runs next, and this is the narrower of the two bands. Until [#972](https://github.com/geoparquet/geoparquet-io/issues/972) it asked for 100,000 rows per group — landing at 100,352, inside the general 10,000-200,000 band that `gpio check row-group` passes a file on but outside this one — so `gpio check optimization` scored a freshly fixed file `[fail]` on its row-group factor and advised re-partitioning it.

### Optimization Check

=== "CLI"

    ```bash
    gpio check optimization myfile.parquet
    ```

=== "Python"

    <!-- doctest: prelude="import geoparquet_io as gpio; table = gpio.read('myfile.parquet')" -->
    ```python
    result = table.check_optimization()
    print(f"Score: {result.to_dict()['score']}/5")
    ```

Evaluates five factors affecting spatial query performance and returns a score from 0 to 5:

1. **Native Geo Types** - Uses native Parquet geo types (GeoParquet 2.0 or parquet-geo-only)
2. **Geo Bbox Stats** - Per-row-group geo bbox statistics present
3. **Spatial Sorting** - Data is spatially sorted (Hilbert or similar)
4. **Row Group Size** - Appropriate for file size (10k-50k rows for spatial pushdown)
5. **Compression** - ZSTD compression on geometry column

**Scoring levels:**

- `fully_optimized` (5/5) - All checks pass
- `partially_optimized` (3-4/5) - Some improvements possible
- `not_optimized` (0-2/5) - Significant improvements needed

### Spatial Filter Pushdown Readiness

The `gpio check spatial` command also reports spatial filter pushdown readiness when bbox data is available:

=== "CLI"

    ```bash
    gpio check spatial myfile.parquet
    ```

=== "Python"

    <!-- doctest: prelude="import geoparquet_io as gpio; table = gpio.read('myfile.parquet')" -->
    ```python
    result = table.check_spatial_pushdown()
    details = result.to_dict()
    print(f"Skip rate: {details['estimated_skip_rate']}")
    ```

Shows:

- **Row group count** and bbox coverage
- **Estimated skip rate** - percentage of row groups that can be skipped for representative spatial queries
- **Avg bbox area ratio** - how tight the row group bounding boxes are

!!! note "Requires bbox data"
    Pushdown readiness requires GeoParquet 2.0 native geo stats or a bbox column. For v1.1 files, add a bbox column with `gpio add bbox`.

### Bloom Filters

Bloom filter detection is included in `gpio check all` and `gpio inspect meta`:

=== "CLI"

    ```bash
    # Included automatically in check all
    gpio check all myfile.parquet
    ```

=== "Python"

    <!-- doctest: prelude="import geoparquet_io as gpio; table = gpio.read('myfile.parquet')" -->
    ```python
    result = table.check_bloom_filters()
    details = result.to_dict()
    ```

Reports which columns have bloom filters, coverage percentages, and total bloom filter bytes. DuckDB 1.5+ automatically writes bloom filters for low-cardinality columns.

### Spec Validation

=== "CLI"

    ```bash
    # Auto-detect version
    gpio check spec data.parquet

    # Validate against specific version
    gpio check spec data.parquet --geoparquet-version 1.1

    # JSON output for CI/CD
    gpio check spec data.parquet --json
    ```

=== "Python"

    <!-- doctest: prelude="import geoparquet_io as gpio; table = gpio.read('myfile.parquet')" -->
    ```python
    result = table.validate()
    if result.passed():
        print("Valid GeoParquet!")
    ```

Validates file structure and metadata against the GeoParquet specification:

- Supports GeoParquet 1.0, 1.1, 2.0, and Parquet native geo types
- Auto-detects version unless `--geoparquet-version` is specified
- Optional data validation against metadata claims

**Metadata checks include:**

- **Version sanity** — an unknown `geo` metadata version (e.g. `99.0.0`) fails
  validation, even in auto mode. Version/feature mismatches also fail: a file
  declaring version 1.0 while using GeoParquet 2.0 native Parquet geo types, or
  the `covering` key that 1.1 introduced, is flagged as inconsistent.
- **CRS structure** — PROJJSON `crs` objects must carry the required `type`
  member, and it must be a known PROJJSON CRS type.
- **A null CRS is unknown, not the default** — omitting the `crs` key means
  OGC:CRS84, but an explicit `"crs": null` says the CRS is *unknown*, and the
  two are checked as the different claims they are. A null `crs` always warns.
  In a GeoParquet 2.0 file it also has to be declared on both carriers: pairing
  `"crs": null` with a Parquet geo type that names no CRS fails
  `v2_crs_consistency`, because the Parquet type's own default is OGC:CRS84.
  The spec's pairing for an unknown CRS is `"crs": null` in the geo metadata
  *and* the string `srid:0` in the Parquet logical type; written that way, the
  file passes. If the coordinates really are lon/lat WGS84, drop the null
  instead: `gpio convert reproject in.parquet out.parquet --assume-crs84`
  writes the default (see [convert](../cli/convert.md)).
- **Every CRS form the Parquet geo type may use** — the `GEOMETRY`/`GEOGRAPHY`
  logical type can name its CRS as inline PROJJSON, `srid:<id>`,
  `projjson:<key>` or the compact `<authority>:<code>` (e.g. `EPSG:32633`), and
  `v2_crs_consistency` compares the real CRS for the resolvable forms (inline
  PROJJSON, `srid:<id>` as EPSG:<id>, and the compact form). A `projjson:<key>`
  reference names a metadata key this check cannot look up, so it is compared
  conservatively: it reports a mismatch rather than guessing. A type whose CRS
  gpio cannot read would otherwise look like one that names no CRS, which the
  Parquet spec defines as OGC:CRS84 — a claim the file never made. The Parquet
  `crs` property is free-form, so an unrecognized value (`4326`, `WGS 84`, raw
  WKT, an authority code PROJ does not carry) is treated the same way: it is
  carried through verbatim, `native_crs_format` warns with the literal, and
  `v2_crs_consistency` compares it as-is, so it fails closed and names the value
  it could not read rather than reading it as the default.
- **Datum-aware epoch validation** — a coordinate `epoch` on a datum ensemble
  (e.g. EPSG:4326, or the OGC:CRS84 default when `crs` is omitted) fails; on a
  specific static frame (e.g. GDA2020) it warns; on a dynamic frame (e.g. ITRF)
  it passes. With an explicit `"crs": null` the datum cannot be verified, so an
  epoch produces a warning.
- **Malformed metadata** — a `geo` key containing invalid JSON or a non-object
  value fails cleanly in auto mode instead of crashing.
- **Dimension-aware geometry types** — `geometry_types` entries carry Z/M
  suffixes (`"Point Z"`, `"LineString ZM"`), and validation matches declared
  suffixes against the actual coordinate dimensions in both directions
  (declared-but-absent and present-but-undeclared).
- **GeoArrow encodings** — the single-geometry type encodings (`point`,
  `linestring`, `polygon`, `multipoint`, `multilinestring`, `multipolygon`)
  are valid for GeoParquet 1.1, so files written with
  `--geoparquet-version 1.1-geoarrow` validate. The checks are layout-aware:
  the `BYTE_ARRAY` requirement applies only to `WKB` columns, native columns
  are checked for the DOUBLE coordinate group the spec requires, and the data
  scans read GeoArrow coordinates directly. GeoParquet 1.0 and 2.0 are
  WKB-only per their spec text, so a file claiming a GeoArrow encoding under
  either version fails.
- **Spherical edges on a projected CRS** — a column declaring `edges` other
  than `planar` while its CRS is projected **warns**
  (`edges_spherical_on_projected_crs_<column>`). Great-circle edges are only
  meaningful on an ellipsoid; in projected space the edge between two vertices
  is a straight line, so readers will draw it that way. Files with this
  combination exist in the wild, so it is a warning, not a failure — densify
  the geometries and declare planar edges, or keep the data in a geographic
  CRS.
- **Native geospatial statistics** — for files using the Parquet `GEOMETRY` or
  `GEOGRAPHY` logical types, `native_geo_stats_*` reports the bounds a file
  declares and `native_geo_stats_contains_data_*` checks its geometries against
  them. Both use the whole file's statistics — the union over every row group,
  not the first one's — so a correctly written multi-row-group file passes
  (#721).

**Exit codes:**

- `0` - All checks passed
- `1` - One or more checks failed
- `2` - Warnings only (all required checks passed)

!!! note "Coordinate/CRS mismatch heuristic is now a warning"
    The heuristic that flags geographic-looking coordinates (values within
    ±180/±90) under a projected CRS reports a **WARNING** instead of a failure,
    so `gpio check spec` exits with code `2` instead of `1` for affected files.
    Update CI pipelines that gate only on exit code `1`. The deterministic
    CRS-consistency check for GeoParquet 2.0 native geo statistics still fails
    on a real mismatch.

### STAC Validation

=== "CLI"

    <!-- doctest: skip="needs output.json, which the harness does not seed" -->
    ```bash
    gpio check stac output.json
    ```

=== "Python"

    ```python
    from geoparquet_io import validate_stac

    result = validate_stac('output.json')
    if result.passed():
        print("Valid STAC!")
    ```

The argument is the STAC Item or Collection JSON — the file `gpio publish stac`
writes — not the GeoParquet data file it describes.

Validates STAC Item or Collection JSON:

- STAC spec compliance
- Required fields
- Asset href resolution (local files)
- Best practices

## Options

=== "CLI"

    ```bash
    # Verbose output with details
    gpio check all myfile.parquet --verbose

    # Custom sampling for spatial check
    gpio check spatial myfile.parquet --random-sample-size 200 --limit-rows 1000000
    ```

=== "Python"

    <!-- doctest: prelude="import geoparquet_io as gpio; table = gpio.read('myfile.parquet')" -->
    ```python
    # Custom sampling for spatial check
    result = table.check_spatial(sample_size=200, limit_rows=1000000)
    ```

## Checking Partitioned Data

When checking a directory containing partitioned data, you can control how many files are checked:

<!-- doctest: skip="needs partitions/, which the harness does not seed" -->
```bash
# By default, checks only the first file
gpio check all partitions/
# Output: Checking first file (of 4 total). Use --all-files or --sample-files N for more.

# Check all files in the partition
gpio check all partitions/ --all-files

# Check a sample of files (first N files)
gpio check all partitions/ --sample-files 3
```

!!! warning "--fix rewrites every file it checks"
    `--fix` applies to whichever files the check ran on. With `--all-files` or
    `--sample-files N` on a directory that means **every file it checked is
    rewritten in place**, not just the first one:

    <!-- doctest: skip="needs partitions/, which the harness does not seed" -->
    ```bash
    # Rewrites all 4 files in the partition, in place
    gpio check spatial partitions/ --fix --all-files
    ```

    Each rewritten file gets a `.bak` alongside it (unless you pass
    `--no-backup`), and the run ends with a `Fixed N files:` list naming what it
    wrote. There is no confirmation prompt, so check the file count in the
    `📁 Checking ...` notice before adding `--fix`.

    To write the fixed files somewhere else instead, `gpio check all` takes a
    directory as `--fix-output` (it refuses a single file path when more than
    one file is being fixed). Alternatively, consolidate the partition with
    `gpio extract` first, fix that, then re-partition.

## See Also

- [CLI Reference: check](../cli/check.md)
- [add command](add.md) - Add spatial indices
- [sort command](sort.md)
