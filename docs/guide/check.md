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

## Exit Codes

Every `check` subcommand — `all`, `bbox`, `compression`, `optimization`,
`row-group`, `spatial`, `spec` and `stac` — follows one contract, so any of them
can gate a CI build:

| Outcome | Exit code | With `--strict` |
|---------|-----------|-----------------|
| Every check passed | `0` | `0` |
| Warnings only (advice) | `0` | `1` |
| A check failed | `1` | `1` |
| A check could not run | `1` | `1` |
| The input matched no files | `1` | `1` |

A **failure** is a defect: a spec violation, a file that is not GeoParquet at
all (no `geo` metadata and no native Parquet geo types), or metadata a reader
would mis-read or cannot open — a `covering` the spec rejects, or one with no
`bbox` member. A **warning** is advice: a non-ZSTD codec, a row-group size
outside the recommended band, data that is not spatially sorted, a missing bbox
column on a 1.x file, low spatial-filter pushdown readiness in `check spatial`,
an optimization score below 5/5, or "GeoParquet 2.0 is available". `check all`
exits 1 if any of its checks failed, including spec validation. Each issue is
printed at its own check's level, so `check all` on a file that fails the spec
and uses SNAPPY prints the spec failure as `✗ ERROR` and the codec as
`⚠ WARNING`.

Over a directory or glob, the run exits 1 if any of the files it checked
failed. Without `--all-files` (or `--sample-files N`) a check reads only the
first file, so its verdict covers that file alone, and the run says so on
stderr: pass `--all-files` to gate on every file. `--fix` never samples — it
checks and repairs every matched file.

A check that cannot run counts as a failure. It prints a line naming the check
and the reason, the remaining checks still run, and the run then exits 1 with or
without `--strict`. A check that could not read the data is never reported as a
pass, even after `--fix` (#624).

Pass `--strict` to make warnings exit 1 as well; it prints a line on stderr
saying that is why.

On stdout, `✓` marks a pass, `⚠` a warning and `✗` a failure, in every
subcommand; `ℹ️` marks information that does not count either way.

=== "CLI"

    ```bash
    # Fails the build only on a defect
    gpio check all myfile.parquet

    # Fails the build on any finding, advice included
    gpio check compression myfile.parquet --strict || echo "not ZSTD"
    ```

=== "Python"

    <!-- doctest: prelude="import geoparquet_io as gpio; table = gpio.read('myfile.parquet')" -->
    ```python
    # table.check() runs the structure checks only; `gpio check all` adds
    # spatial order and spec validation, so a CI gate combines all three.
    results = [table.check(), table.check_spatial(), table.validate()]
    verdicts = {r.verdict() for r in results}  # "pass", "warning" or "failure"

    failed = "failure" in verdicts  # what `gpio check all` exits 1 on
    strict_failed = verdicts != {"pass"}  # the --strict equivalent
    for result in results:
        print(*result.failures(), *result.warnings(), sep="\n")
    ```

    `failures()` and `warnings()` bucket each issue by the same severity
    `verdict()` reads.

`--fix` keeps the contract: a file is judged by what the fix wrote, so a
failure that `--fix` repairs exits 0, while a fix that fails, or that `--fix`
declines because it cannot repair the problem, exits 1 (and says
`✗ No automatic fix available`). With `--fix-output`, a file that needed no
repair is copied there, so a pipeline always gets the file it named; a failing
file that `--fix` cannot repair is not.

An input that resolves to no parquet files — a glob that matches nothing, or an
empty directory — exits 1 with `No parquet files found` on stderr, with or
without `--fix`, so a mistyped path cannot pass for a successful run. That holds
for every subcommand that reads parquet files, `check spec` included; `check
stac` takes one JSON file, and a missing one is an error (exit 1) too.

!!! note "Changed after 1.6.0"
    Six of the eight subcommands used to print a failure and exit 0, a glob that
    matched nothing exited 0, and `check spec` exited `2` for warnings only.
    Failures now exit 1, warnings exit 0 (1 with `--strict`), and `check spec`
    no longer uses exit code `2` ([#1241](https://github.com/geoparquet/geoparquet-io/issues/1241),
    [#1091](https://github.com/geoparquet/geoparquet-io/issues/1091)).

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

The sampling method reads WKB, native Parquet GEOMETRY and GeoArrow coordinate
struct columns. NULL, zero-length and unparsable values are skipped, and the
spec validation reports them. When the geometry column is missing or has a type
that holds no geometry, the check fails with the reason and `gpio check spatial`
exits with status 1 (#624).

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
- That each row's bbox really covers that row's geometry

A bbox column is optional in GeoParquet 2.0, but it must be declared, and the
declaration must be true:

- A bbox column that no `covering` entry points at costs file size and cannot be
  used by any reader, so `check bbox` warns about it and suggests
  `gpio add bbox-metadata` (or `--fix` to drop the column). `--fix` only ever
  removes an *undeclared* column — a covering the file legitimately declares is
  never deleted.
- A `covering` that names a column the file does not contain is treated as
  undeclared rather than accepted. Such a covering makes readers prune away rows
  that genuinely match, so it is worse than none at all.

`check spec` runs its `covering` checks — the `bbox` member present, paths
well-formed, column present, struct shape, field types — at GeoParquet 1.1
**and** 2.0. The last four were previously gated to 1.1 only, so an identical
broken covering failed at 1.1 and passed at 2.0.

#### Does the bbox cover the geometry?

The checks above all read the footer. They say the column is there and that the
`covering` points at it — not that its numbers are right. A bbox column is a
*copy* of information the geometry already holds, so any operation that rewrites
geometry without rewriting the copy leaves a file that passes every metadata
check while describing geometry it no longer has. Simplifying one partition of a
published catalogue with Douglas-Peucker at 0.0002° moved a bbox extreme on
135,712 of its 435,138 rows; `gpio check spec` still reported 29 passed, 0
failed.

So `check bbox` compares the numbers too, and the two ways to be wrong are not
the same finding:

- **A bbox smaller than its geometry fails the check.** The whole point of a
  covering is that a reader can prune row groups and pages without decoding
  geometry, so an undersized box makes that pruning drop rows that genuinely
  intersect the query window. Nothing raises; the reader just gets less data
  than the file holds and cannot tell.
- **A bbox larger than its geometry is a warning.** The reader over-fetches and
  re-filters on the real geometry, so the answer stays right. It costs time, not
  correctness.
- **A NULL bbox beside a non-NULL geometry fails**, for the same reason an
  undersized one does: every comparison against NULL is NULL, so a
  covering-based filter drops the row. A bbox struct that is present but has a
  NULL in any of its four fields is a NULL bbox, and a NaN in one fails the same
  way — no filter can match it.
- **A geometry value that is not valid WKB fails** — zero-length or unparsable
  bytes. No reader can decode it, and its bbox cannot be checked. It is counted
  on its own; the rows around it are still compared (including the other rows
  of a dictionary-encoded page, which DuckDB would otherwise read as NULL).

A row with no geometry — NULL or empty — has no envelope to compare against and
is counted and skipped. If it still carries a bbox, that is a warning: there is
nothing for the bbox to describe.

Each finding is reported at its own level, so a file with both an undersized
and an oversized bbox prints one failure and one warning. A failure exits 1 like
any other check failure; a file with only warnings exits 0 unless `--strict` is
set. See [exit codes](#exit-codes) for the shared contract.

**Antimeridian.** On a geographic CRS (including the default, OGC:CRS84), a bbox
with `xmin > xmax` crosses the antimeridian (RFC 7946, section 5.2): longitudes in
`[xmin, 180]` or `[-180, xmax]` are inside it, judged per vertex. This is how
`check spec` reads the file-level `bbox`, so the two agree; the output says how
many rows were read that way. On a projected CRS, `xmin > xmax` is simply wrong
and fails as undersized.

**Spherical edges.** The comparison uses the planar envelope of a geometry's
vertices. For a column whose `edges` are `spherical`, a geodesic edge can bulge
past that envelope, so the output names the edges and the check is a check on
the vertices, not on the curve.

The report names the worst offending rows by their position in the file, which
is the part you can act on:

```
Bbox vs geometry:
   5,000 of 435,138 rows checked (a random sample; --all-rows checks every row)
✗ 1,547 rows where the bbox does not cover its geometry
   Readers prune on the covering, so these rows are missing from spatial query results with no error anywhere
   Worst: row 229584 (short by 0.0113), row 81070 (short by 0.00981), ...
   Regenerate the column: gpio add bbox <input> <output> --force
```

`--fix` cannot repair this. It adds a missing column or metadata, or drops an
unusable column; numbers that are wrong have to be recomputed from the
geometry, which is a rewrite. When `--fix` does apply a metadata repair, it
compares the values again on what it wrote, so a wrong bbox still fails the run:

```bash
gpio add bbox --force input.parquet output.parquet
```

**Cost and sampling.** Comparing means decoding geometry, which the other checks
never do, so the default is a random sample of 5,000 rows drawn across the whole
file, large enough that a corruption affecting even 1% of rows is seen with
overwhelming probability. It is a random sample rather than the first N rows on
purpose: the realistic corruption is an operation that rewrote part of a file,
and a head-of-file sample would miss a rewritten tail.

The sample fixes the *decode* cost, not the I/O. Drawing rows from across the
file means reading the geometry and bbox columns of every row group, so a
sampled check on a remote file still transfers both columns in full — budget for
that on a large object-store file. The sample is seeded, so the same file gets
the same verdict every run, but a passing sample is evidence, not a guarantee: a
corruption confined to a handful of rows can fall between the sampled ones.
`--all-rows` compares every row; `--sample-rows N` sets the sample size, and
`--sample-rows 0` skips the comparison (and says so) and checks structure and
metadata only.

=== "CLI"

    ```bash
    # Default: 5,000 rows sampled across the file
    gpio check bbox myfile.parquet

    # Exhaustive
    gpio check bbox myfile.parquet --all-rows

    # Wider sample, or none at all
    gpio check bbox myfile.parquet --sample-rows 50000
    gpio check bbox myfile.parquet --sample-rows 0
    ```

=== "Python"

    <!-- doctest: prelude="import geoparquet_io as gpio; table = gpio.read('myfile.parquet')" -->
    ```python
    result = table.check_bbox(all_rows=True)
    if result.verdict() == "failure":
        for failure in result.failures():
            print(failure)
    for warning in result.warnings():  # an oversized bbox, say
        print(warning)

    # Wider sample, and the per-row report behind the verdict
    report = table.check_bbox(sample_rows=50000).to_dict()['bbox_values']
    print(report['undersized_rows'], report['undersized_examples'])
    ```

**Tolerance.** Exact equality is the wrong test — a bbox that has been through a
reprojection or a float32 column is not bit-identical to the envelope recomputed
from the geometry. The slack is four ULP of the bbox column's *own* storage
type, scaled to the magnitude of the coordinate, with a 1e-9 absolute floor for
coordinates near zero. A float64 covering is therefore held to about 9e-16
relative, and a float32 one to about 5e-7: the GeoParquet spec allows a float32
covering and asks writers to round outward, and writers that round to nearest
instead put a bound up to half an ULP inside the geometry. That slack is an
artefact of the column's storage, not a wrong bbox. The comparison is not
symmetric — it is an undersized box beyond the tolerance that fails.

`check all` does **not** run this comparison. Every other question it asks is
answered from the footer, so it stays a metadata-only pass; reading geometry
belongs to the command you ran for it.

#### A covering with no `bbox` member

The spec defines one covering encoding: "The keys of the 'covering' object MUST
be a supported encoding. Currently the only supported encoding is 'bbox'". gpio
records its own spatial-index entries (`h3`, `s2`, `a5`, `quadkey`, `kdtree`)
*beside* a `bbox` member, which stays valid — but a covering carrying only those
entries, with no `bbox` member at all, is a file no reader can use:
`geopandas.read_parquet` indexes `covering["bbox"]` unguarded and raises
`KeyError: 'bbox'`.

`gpio add h3/s2/a5/quadkey/kdtree` and `gpio partition <index> --keep-*-column`
wrote that shape over a bbox-less input up to gpio 1.6. They no longer do, but
the files are published, so both halves of `check` now say so:

- `check spec` **fails** such a file at 1.1+ (`covering_has_bbox_<column>`). It
  is a failure, not a warning, because the file does not open. `check bbox` and
  `check all` fail it too, at any version.
- `check all --fix` drops the covering, before any other repair, and says it
  did. The repair patches the footer only — compression codec and level,
  encodings, bloom filters, row-group boundaries and row order are all kept —
  and a covering that *does* carry a `bbox` member is never touched, index
  entries and all. At GeoParquet 1.x the same run then adds a bbox column and a
  legal covering, as it always has.

So `gpio check spec published.parquet` reports
`covering_has_bbox_geometry: FAILED` and exits 1, and
`gpio check all published.parquet --fix` repairs the file in place, keeping a
`.bak`.

#### Bbox struct field order

GeoParquet 1.1 fixes the *order* of a bbox column's struct fields — `xmin`,
`ymin`, `xmax`, `ymax` (or `xmin`, `ymin`, `zmin`, `xmax`, `ymax`, `zmax`) — not
merely which names appear. Overture writes `xmin, xmax, ymin, ymax`, which is a
perfectly good Parquet column and, at GeoParquet 1.0, a perfectly valid file:
1.0 has no `covering` key, so nothing points at the column.

No gpio write declares a `covering` over such a column, and every field must be
FLOAT or DOUBLE. A rewrite (`check compression/row-group/spatial --fix`,
`convert geoparquet`, `sort hilbert`, `extract geoparquet`, `Table.write()`)
carries the column through untouched and leaves it undeclared, with one warning
per run saying why; `gpio add bbox-metadata`, whose only job is to write that
key, refuses outright. Rewriting the column in the spec's order is a data change
and stays explicit:

```bash
gpio add bbox --force input.parquet output.parquet
```

An input that already *declares* a covering over such a column is written
without it, with a warning that says so: gpio does not write a file its own
`check spec` rejects, even when the input asked it to. A covering the spec
allows is carried through unchanged.

`check bbox` reports the field order (or the field types) as the issue for such
a file, rather than reporting a missing covering it cannot legally add, and
`--fix` says there is nothing it can repair.

!!! note "Changed after 1.5.0"
    A rewrite of a file with an `xmin, xmax, ymin, ymax` bbox column at 1.1 used
    to declare a covering over it, which `gpio check spec` then failed. It now
    leaves the column undeclared, and drops a covering the input declared over
    such a column. `gpio add bbox --force` gets the covering back.

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

- **The verdict is 10,000-200,000 rows per group.** This is what passes or fails, and it is deliberately wide: it has to accept the layouts mainstream writers produce at their own defaults — DuckDB writes 122,880 rows per group, and gpio writes 49,152. A pass/fail band that excluded both of those would be reporting on the ecosystem rather than on your file. Outside it the check reports `suboptimal`, and below 2,000 or above 1,000,000 rows, `poor`. Not every default fits: pyarrow's is `min(num_rows, 1048576)`, so a 300,000-row table becomes one 300,000-row group (`suboptimal`) and anything past a million rows starts producing 1,048,576-row groups (`poor`). The band accepts the defaults that are usable over a network, not all of them.
- **The advice is 10,000-50,000 rows per group**, for spatial queries. This is the band with measurements behind it, so a file can sit inside the verdict band and still be worth re-sorting.

!!! tip "Spatial filter pushdown and row group sizing"
    For GeoParquet 2.0 or parquet-geo-only files with Hilbert sorting, row groups of 10,000-50,000 rows create tighter bounding boxes that enable more row group skipping during spatial queries. Re-sorting nine published catalog files from 100,000 to 50,000 rows per group cut the share of the file a query window covering 10% of each dimension has to read from 43-100% down to 10-28% ([#775](https://github.com/geoparquet/geoparquet-io/issues/775)).

    Neither end of that band is a size a row group can actually have. The Parquet writer emits row groups in whole 2,048-row vectors and rounds a request *up* to a multiple of that, so a request for the band's top of 50,000 lands at 51,200 — just outside it. That used to be exactly what `gpio sort` asked for, so `gpio check optimization`, which scores this band as one of its five factors, marked a freshly sorted file `[fail]` and advised re-partitioning it ([#961](https://github.com/geoparquet/geoparquet-io/issues/961)). The band did not move; `gpio sort` now snaps its request to a whole vector itself, which maps the band's endpoints to 10,240 and 49,152, and writes 49,152 rows per group by default. It snaps up, as the writer does, except where that would push a request out of the top of the band — 50,000 becomes 49,152 rather than 51,200. Every gpio write path snaps, not just `gpio sort`: `gpio convert --row-group-size 50000` and `Table.write(row_group_rows=50000)` both write 49,152 too ([#981](https://github.com/geoparquet/geoparquet-io/issues/981), [#971](https://github.com/geoparquet/geoparquet-io/issues/971)).

    `--fix` writes those same 49,152-row groups. It is a repair, so the file it leaves behind has to pass the checks gpio runs next, and this is the narrower of the two bands. Until [#972](https://github.com/geoparquet/geoparquet-io/issues/972) it asked for 100,000 rows per group — landing at 100,352, inside the general 10,000-200,000 band that `gpio check row-group` passes a file on but outside this one — so `gpio check optimization` scored a freshly fixed file `[fail]` on its row-group factor and advised re-partitioning it.

    **`--fix` follows this band, not the verdict band.** A file inside 10,000-200,000 but outside 10,000-50,000 still passes `gpio check row-group` — it is laid out the way mainstream writers lay files out — and `--fix` will still offer to rewrite it, because 49,152 rows per group is what it writes. Offering the fix off the verdict band instead meant `gpio check row-group --fix` printed "No fix needed" for a 100,352-row-group file while `gpio check optimization` on the same file printed `[fail] Row Group Size`; it also made the result depend on which subcommand you ran, since `check spatial --fix`, `check compression --fix` and `check bbox --fix` each rewrote the file for their own reasons and fixed the row groups in passing. The report says which band it is talking about, so a green "optimal for general use" line is followed by a note that `--fix` has something to do.

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

It counts toward the verdict where it is advice: low pushdown efficiency on a
file with several row groups, or a 1.x file with no bbox column, is a `⚠`
warning (exit 1 with `--strict`). A file with native geo types needs no bbox
column for pushdown, and a single row group has nothing to skip; both are shown
as `ℹ️` information.

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
- `--json` prints strict JSON that `jq` and JavaScript parse as is. Dates and
  times become ISO 8601 strings, decimals become exact strings, binary becomes
  hex, and a NaN or infinite value becomes `null`. `gpio inspect stats`, `head`,
  `tail` and `meta` use the same encoder for their `--json` output
  ([#1226](https://github.com/geoparquet/geoparquet-io/issues/1226)).

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
- **A `v2_crs_consistency` failure does not survive a rewrite** — a gpio
  command that rewrites the file as GeoParquet (`add`, `sort`, `partition`,
  `convert geoparquet`, `check --fix`) answers "which CRS?" for the primary
  column from the `geo` block first, falling back to the Parquet logical type.
  When the two disagree, the `geo` block wins and its answer is written into
  *both* places, so the output passes this check over coordinates that were
  never transformed. gpio warns when it does this, naming both CRSs and which
  one it used, but the warning is the only trace: fix whichever half is wrong
  on the **input**, before the rewrite.
- **Datum-aware epoch validation** — a coordinate `epoch` on a datum ensemble
  (e.g. EPSG:4326, or the OGC:CRS84 default when `crs` is omitted) fails; on a
  specific static frame (e.g. GDA2020) it warns; on a dynamic frame (e.g. ITRF)
  it passes. With an explicit `"crs": null` the datum cannot be verified, so an
  epoch produces a warning.
- **One `geo` key** — a footer that carries the `geo` key more than once fails
  `geo_key_unique`. Readers pick different entries, so the file means different
  things to different tools. DuckDB writes this shape when a `geo` entry is
  supplied beside `GEOPARQUET_VERSION` `V1` or `V2` (#1229).
- **Malformed metadata** — a `geo` key containing invalid JSON or a non-object
  value fails cleanly in auto mode instead of crashing. The same holds for a
  value *inside* a valid `geo` object whose type is wrong: a `covering` that is
  not an object, a `covering.bbox` that is not an object of `[column, field]`
  paths, a `bbox` that is not an array of finite numbers. Each gets a FAILED
  check quoting the value it found (bounded, so a huge value cannot flood the
  report), and the rest of the report is still printed (#1062).
- **Empty and unparsable WKB.** `encoding_matches_data_<column>` reads the
  bytes the file stores and counts the values that are not WKB. A zero-length
  value is reported as `N of M geometries in column "geometry" are empty
  (zero-length) WKB`, and bytes that do not parse as `K of M geometries do not
  match "WKB" encoding`, each with the row of the first one. Both fail, and a
  NULL geometry is allowed. The other data checks judge the values that do
  parse, so one bad value is reported once, not as a DuckDB error under every
  later check (#1064, #1082).
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

**Exit codes:** `0` when no check failed, `1` when one did; warnings exit `0`,
or `1` with `--strict` (see [Exit Codes](#exit-codes)). `check spec` used to
exit `2` for warnings only; it no longer does.

!!! note "Coordinate/CRS mismatch heuristic is a warning"
    The heuristic that flags geographic-looking coordinates (values within
    ±180/±90) under a projected CRS reports a **WARNING** instead of a failure,
    so `gpio check spec` exits `0` for affected files (`1` with `--strict`).
    The deterministic CRS-consistency check for GeoParquet 2.0 native geo
    statistics still fails on a real mismatch.

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

    # Custom sampling for the bbox-vs-geometry comparison
    gpio check bbox myfile.parquet --sample-rows 50000
    gpio check bbox myfile.parquet --all-rows
    ```

=== "Python"

    <!-- doctest: prelude="import geoparquet_io as gpio; table = gpio.read('myfile.parquet')" -->
    ```python
    # Custom sampling for spatial check
    result = table.check_spatial(sample_size=200, limit_rows=1000000)

    # Custom sampling for the bbox-vs-geometry comparison
    result = table.check_bbox(sample_rows=50000)
    result = table.check_bbox(all_rows=True)
    ```

## Checking Partitioned Data

When checking a directory containing partitioned data, you can control how many files are checked:

<!-- doctest: setup="gpio partition quadkey input.parquet partitions/ --resolution 6 --partition-resolution 2" -->
```bash
# By default, checks only the first file
gpio check all partitions/
# Output: Checking first file (of N total). Use --all-files or --sample-files N for more.

# Check all files in the partition
gpio check all partitions/ --all-files

# Check a sample of files (first N files)
gpio check all partitions/ --sample-files 3
```

!!! warning "--fix rewrites every file the input matched"
    Sampling is a *checking* default. `--fix` never samples: it applies to
    **every file the input resolved to**, not the first one, so a glob or a
    directory rewrites all of them in place:

    <!-- doctest: setup="gpio partition quadkey input.parquet partitions/ --resolution 6 --partition-resolution 2" -->
    ```bash
    # Checks the first file only
    gpio check spatial partitions/

    # Fixes every file in the partition, in place
    gpio check spatial partitions/ --fix
    ```

    A quoted glob is fixed the same way — gpio expands it itself, so the
    pattern has to reach it unexpanded:

    <!-- doctest: setup="gpio partition quadkey input.parquet partitions/ --resolution 6 --partition-resolution 2", skip-on="win32: Git Bash's MSYS runtime expands the quoted glob before gpio sees it" -->
    ```bash
    gpio check spatial "partitions/*.parquet" --fix
    ```

    `--sample-files N` still wins, because that is you naming the subset.
    Before 1.6, `--fix` on a directory or glob rewrote only the first file;
    pass `--sample-files 1` to keep that:

    <!-- doctest: setup="gpio partition quadkey input.parquet partitions/ --resolution 6 --partition-resolution 2" -->
    ```bash
    # Fixes the first file and no other
    gpio check spatial partitions/ --fix --sample-files 1
    ```

    Each rewritten file gets a `.bak` alongside it (unless you pass
    `--no-backup`, which asks for confirmation once for the whole run), and the
    run ends with a `Fixed N files:` list naming every one of them. There is no
    confirmation prompt otherwise: run the command without `--fix` first and
    read the file count in the `📁 Checking first file (of N total)` notice.

    A file that cannot be checked or fixed does not stop the others: the run
    reports it, fixes the rest, and exits non-zero. Ctrl-C stops the run and
    still lists what was rewritten before it.

    To write the fixed files somewhere else instead, pass an existing
    **directory** as `--fix-output` and each input is written to its own name
    inside it. A single file path for more than one input is refused (exit 2)
    rather than overwritten once per file, and so is a directory in which two
    inputs would share a name (`k=1/part-0.parquet` and `k=2/part-0.parquet`):
    fix each subdirectory separately, or fix in place.

`--fix-output` leaves the input untouched and writes no `.bak`; a file already at
the output path is replaced without a prompt. A second spelling of the input's own
path (`./in.parquet`, a symlink, a different letter case on macOS or Windows) counts
as in place: the file is backed up and rewritten like a plain `--fix`.

## See Also

- [CLI Reference: check](../cli/check.md)
- [add command](add.md) - Add spatial indices
- [sort command](sort.md)
