# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## v1.5.0 (2026-09-08)

What is newly possible in this release:

- **S2 works again.** The `geography` DuckDB extension is published once more, so `gpio add s2` and `gpio partition s2` run instead of stopping with an explanation. This is what raises the DuckDB floor to 1.5.5. ([#893](https://github.com/geoparquet/geoparquet-io/pull/893))
- **`gpio extract wfs` reads GML.** It asks each service for the format that service advertises and parses whatever comes back, which makes the many WFS 1.x deployments serving only GML usable for the first time. ([#835](https://github.com/geoparquet/geoparquet-io/pull/835))
- **`gpio sort column` and `sort quadkey` take a directory** of GeoParquet files, with `--allow-schema-diff` for directories whose files disagree — and they now refuse to re-read their own output as input, a loop that silently doubled row counts on every run. ([#852](https://github.com/geoparquet/geoparquet-io/pull/852), [#873](https://github.com/geoparquet/geoparquet-io/pull/873))
- **`az://account/container` uploads and copies work.** They were being handed to a container-first URL parser and could not build a store at all. ([#871](https://github.com/geoparquet/geoparquet-io/pull/871))
- **S3 credentials resolve through the full AWS chain**, so `aws sso login`, assume-role and `credential_process` profiles authenticate the same way gpio's DuckDB reads already did. ([#874](https://github.com/geoparquet/geoparquet-io/pull/874))
- **`gpio check spec` gained four GeoParquet 2.0 conformance checks** — `geometry_types` uniqueness, native geometry columns missing from `columns`, `geometry_types` against the Parquet geospatial statistics, and orientation checked against the actual ring winding rather than merely being declared. The existing bbox and covering checks were corrected in the same pass: 6- and 8-element bboxes are read at the right offsets, antimeridian extents are no longer flagged, and the `covering.bbox` path, field-order and field-type rules are enforced. Thanks **@jatorre**. ([#879](https://github.com/geoparquet/geoparquet-io/pull/879), [#882](https://github.com/geoparquet/geoparquet-io/pull/882), [#876](https://github.com/geoparquet/geoparquet-io/pull/876), [#877](https://github.com/geoparquet/geoparquet-io/pull/877))
- **Python API parity with the CLI on partitioning.** Every partition method takes `auto`, the kdtree methods take `auto` and `target_rows`, `ops.partition_by_*` twins exist for every subcommand, and directory sub-partitioning is reachable from Python. ([#800](https://github.com/geoparquet/geoparquet-io/pull/800), [#808](https://github.com/geoparquet/geoparquet-io/pull/808), [#853](https://github.com/geoparquet/geoparquet-io/pull/853), [#855](https://github.com/geoparquet/geoparquet-io/pull/855))
- **`gpio partition a5` sub-partitions** with `--min-size` and `--in-place`, matching h3, s2 and quadkey. ([#790](https://github.com/geoparquet/geoparquet-io/pull/790))

**The fixes are overwhelmingly about not lying to you regarding the CRS.** Seven separate paths could write, or bless, a file whose stated coordinate reference system was wrong — projected coordinates labelled OGC:CRS84, which nothing downstream can detect. M and ZM geometry at 2.0 dropped the CRS entirely (#888); the in-memory and streaming write strategies wrote a bare GEOMETRY type beside a `geo` block that declared one (#869); a GeoArrow file with no `geo` block lost its CRS depending on whether `geoarrow.pyarrow` happened to be imported (#872, #846); a free-form `crs` string on the Parquet type was read as the CRS84 default rather than failing closed (#870, #851); and reprojecting geography-edged data left `edges: spherical` on projected output (#884). Alongside those: apostrophes in paths no longer crash `convert`, `add` and `admin-divisions`; zero-row inputs no longer traceback through `partition` and `check`; and `check stac` explains that it wants the STAC JSON rather than reporting a codec error.

**Six changes are marked breaking, and this is a minor release rather than a 2.0 on purpose.** Each of them is narrow: five are behaviour catching up with what the documentation already said, and none asks you to redesign anything. A major bump signals "your code needs rework", which would misdescribe this release — and would spend 2.0.0 on a bug-fix batch when the Python API redesign that deserves it has not happened yet.

- `partition_by_admin` returns the same dict as every other partition method instead of an int ([#841](https://github.com/geoparquet/geoparquet-io/pull/841)). Its annotation always said dict; the int was the defect. **If you were using the return value as a count**, read `result["file_count"]`.
- The partition and kdtree API methods raise if you name neither a resolution nor `auto` ([#800](https://github.com/geoparquet/geoparquet-io/pull/800), [#855](https://github.com/geoparquet/geoparquet-io/pull/855)), where the API used to silently pick values the CLI refused to assume — `iterations` defaulted to a hardcoded 9. **Pass `auto=True` or an explicit resolution.**
- `resolve_file_url()` no longer percent-encodes `http(s)` URLs; one is passed to the reader exactly as you typed it ([#845](https://github.com/geoparquet/geoparquet-io/pull/845)). This only affects you **if you were relying on gpio encoding spaces for you** — encode them yourself.
- `gpio sort` defaults to 50,000 rows per row group ([#797](https://github.com/geoparquet/geoparquet-io/pull/797)), the top of the band gpio's own guidance recommends. No API changes, but **sorted output differs byte-for-byte from 1.4.0** for identical input; pass `--row-group-size` to pin the old value.
- The DuckDB floor moves to 1.5.5 ([#893](https://github.com/geoparquet/geoparquet-io/pull/893)), which is what makes S2 work again. **Pinned below that, you will need to upgrade DuckDB.**

Three people made their first contribution. **@maxmalynowsky** mapped the ArcGIS 10.9 field-type cohort — BigInteger, DateOnly and TimeOnly — which had been falling back to strings, and the fix turned up a crash in the time-parsing path that is fixed here too (#786). **@bertt** found that a WFS service URL already carrying a query parameter, such as an apikey, had that parameter dropped when gpio built the GetFeature URL, giving an HTTP 403 that looked like an auth problem rather than a URL-building bug (#836). **@be-student** fixed `gpio partition quadkey` in directory mode, where only `--resolution` was forwarded to each per-file run so every file failed unless `--auto` was passed (#900). Thank you all three — these are exactly the reports and fixes that come from using the tool on real services and real data, and we would be glad to see you back.

### Breaking

- feat(api)!: accept auto resolution on the partition methods by [@cholmes](https://github.com/cholmes) in [#800](https://github.com/geoparquet/geoparquet-io/pull/800)
- fix(sort)!: default to 50,000 rows per row group by [@cholmes](https://github.com/cholmes) in [#797](https://github.com/geoparquet/geoparquet-io/pull/797)
- fix(api)!: return the same dict from partition_by_admin as the other partition methods by [@cholmes](https://github.com/cholmes) in [#841](https://github.com/geoparquet/geoparquet-io/pull/841)
- fix(remote)!: take an http(s) URL as already percent-encoded instead of re-encoding it by [@cholmes](https://github.com/cholmes) in [#845](https://github.com/geoparquet/geoparquet-io/pull/845)
- feat(api)!: accept auto mode on the kdtree methods instead of pinning iterations=9 by [@cholmes](https://github.com/cholmes) in [#855](https://github.com/geoparquet/geoparquet-io/pull/855)
- feat(s2)!: bring S2 back by requiring duckdb>=1.5.5, where the geography extension is published by [@cholmes](https://github.com/cholmes) in [#893](https://github.com/geoparquet/geoparquet-io/pull/893)

### Added

- feat(partition): sub-partition a5 with --min-size and --in-place by [@cholmes](https://github.com/cholmes) in [#790](https://github.com/geoparquet/geoparquet-io/pull/790)
- feat(api): add ops.partition_by_* twins for every partition subcommand by [@cholmes](https://github.com/cholmes) in [#808](https://github.com/geoparquet/geoparquet-io/pull/808)
- feat(api): expose directory sub-partitioning (min_size, in_place, preview) by [@cholmes](https://github.com/cholmes) in [#853](https://github.com/geoparquet/geoparquet-io/pull/853)
- feat(sort): guard against re-reading the output as input, and accept --allow-schema-diff on directory input by [@cholmes](https://github.com/cholmes) in [#873](https://github.com/geoparquet/geoparquet-io/pull/873)
- feat(remote): resolve S3 credentials through the full AWS chain, so SSO and assume-role work by [@cholmes](https://github.com/cholmes) in [#874](https://github.com/geoparquet/geoparquet-io/pull/874)
- feat(validate): geometry_types uniqueness, undeclared native columns, geometry_types vs geospatial statistics by [@jatorre](https://github.com/jatorre) in [#879](https://github.com/geoparquet/geoparquet-io/pull/879)
- feat(validate): implement the orientation-vs-data check (exterior CCW, holes CW) by [@jatorre](https://github.com/jatorre) in [#882](https://github.com/geoparquet/geoparquet-io/pull/882)

### Changed

- refactor: delete the dead metadata/writer duplicates by [@cholmes](https://github.com/cholmes) in [#829](https://github.com/geoparquet/geoparquet-io/pull/829)
- refactor: rehome the test-only schema parser and delete its dead siblings by [@cholmes](https://github.com/cholmes) in [#834](https://github.com/geoparquet/geoparquet-io/pull/834)

### Fixed

- fix(ci): accept metadata 2.5 when publishing to PyPI by [@cholmes](https://github.com/cholmes) in [#783](https://github.com/geoparquet/geoparquet-io/pull/783)
- fix(deps): upgrade vulnerable dependencies (automated) by [@nlebovits](https://github.com/nlebovits) in [#787](https://github.com/geoparquet/geoparquet-io/pull/787)
- fix(arcgis): map BigInteger, DateOnly, and TimeOnly field types by [@maxmalynowsky](https://github.com/maxmalynowsky) in [#786](https://github.com/geoparquet/geoparquet-io/pull/786)
- fix(convert): stop copying an invalid PROJJSON CRS into the output by [@cholmes](https://github.com/cholmes) in [#794](https://github.com/geoparquet/geoparquet-io/pull/794)
- fix(write): detect geoarrow.wkb from field metadata, not just the Arrow type by [@cholmes](https://github.com/cholmes) in [#791](https://github.com/geoparquet/geoparquet-io/pull/791)
- fix(extract): carry geometry_types into the piped Arrow stream by [@cholmes](https://github.com/cholmes) in [#801](https://github.com/geoparquet/geoparquet-io/pull/801)
- fix(metadata): keep epoch, orientation and a derived covering on the no-rewrite fast path by [@cholmes](https://github.com/cholmes) in [#793](https://github.com/geoparquet/geoparquet-io/pull/793)
- fix(api): validate extract columns and keep an attribute table writable by [@cholmes](https://github.com/cholmes) in [#788](https://github.com/geoparquet/geoparquet-io/pull/788)
- fix(validate): treat OGC:CRS84 and EPSG:4326 as the same CRS, and an absent crs as CRS84 by [@cholmes](https://github.com/cholmes) in [#796](https://github.com/geoparquet/geoparquet-io/pull/796)
- fix(stream): write a valid empty stream instead of aborting on a zero-row result by [@cholmes](https://github.com/cholmes) in [#806](https://github.com/geoparquet/geoparquet-io/pull/806)
- fix(geoarrow): detect the extension name from field metadata everywhere, not just in the writers by [@cholmes](https://github.com/cholmes) in [#807](https://github.com/geoparquet/geoparquet-io/pull/807)
- fix(sql): stop apostrophes in paths crashing convert, add and admin-divisions by [@cholmes](https://github.com/cholmes) in [#803](https://github.com/geoparquet/geoparquet-io/pull/803)
- fix(add): write OUTPUT_FILE when the input already has a bbox column by [@cholmes](https://github.com/cholmes) in [#798](https://github.com/geoparquet/geoparquet-io/pull/798)
- fix(admin): attribute Overture dependency territories instead of dropping them to ZZ by [@cholmes](https://github.com/cholmes) in [#820](https://github.com/geoparquet/geoparquet-io/pull/820)
- fix(sql): show the real path, not the SQL-escaped one, in logs and errors by [@cholmes](https://github.com/cholmes) in [#809](https://github.com/geoparquet/geoparquet-io/pull/809)
- fix(wfs): request the format the server advertises and parse GML responses by [@cholmes](https://github.com/cholmes) in [#835](https://github.com/geoparquet/geoparquet-io/pull/835)
- fix(streaming): read a geoarrow CRS object as PROJJSON instead of stringifying it by [@cholmes](https://github.com/cholmes) in [#846](https://github.com/geoparquet/geoparquet-io/pull/846)
- fix(partition,check): handle a zero-row input cleanly instead of crashing by [@cholmes](https://github.com/cholmes) in [#850](https://github.com/geoparquet/geoparquet-io/pull/850)
- fix(write): give disk-rewrite 2.0 output a native GEOMETRY logical type by [@cholmes](https://github.com/cholmes) in [#847](https://github.com/geoparquet/geoparquet-io/pull/847)
- fix(sort): say that an explicit default CRS is normalized, not preserved by [@cholmes](https://github.com/cholmes) in [#842](https://github.com/geoparquet/geoparquet-io/pull/842)
- fix(validate): recognize the authority:code CRS form in the Parquet GEOMETRY type by [@cholmes](https://github.com/cholmes) in [#851](https://github.com/geoparquet/geoparquet-io/pull/851)
- fix(extract): recompute geometry_types and bbox after geometry repair by [@cholmes](https://github.com/cholmes) in [#843](https://github.com/geoparquet/geoparquet-io/pull/843)
- fix(check): explain that check stac wants the STAC JSON, not the Parquet file by [@cholmes](https://github.com/cholmes) in [#857](https://github.com/geoparquet/geoparquet-io/pull/857)
- fix(add): copy a remote input through gpio's configured object store, not bare fsspec by [@cholmes](https://github.com/cholmes) in [#849](https://github.com/geoparquet/geoparquet-io/pull/849)
- fix(sort): accept a directory of GeoParquet files as input by [@cholmes](https://github.com/cholmes) in [#852](https://github.com/geoparquet/geoparquet-io/pull/852)
- fix(crs): keep a GeoArrow file's CRS when it has no geo block by [@cholmes](https://github.com/cholmes) in [#872](https://github.com/geoparquet/geoparquet-io/pull/872)
- fix(remote): build Azure stores from az://account/container so uploads and copies work by [@cholmes](https://github.com/cholmes) in [#871](https://github.com/geoparquet/geoparquet-io/pull/871)
- fix(validate): fail closed on a free-form Parquet GEOMETRY crs instead of reading it as CRS84 by [@cholmes](https://github.com/cholmes) in [#870](https://github.com/geoparquet/geoparquet-io/pull/870)
- fix(extract): say what --write-memory does on extract bigquery by [@cholmes](https://github.com/cholmes) in [#878](https://github.com/geoparquet/geoparquet-io/pull/878)
- fix(convert): report when an input's explicit default CRS is normalized away by [@cholmes](https://github.com/cholmes) in [#881](https://github.com/geoparquet/geoparquet-io/pull/881)
- fix(wfs): reword the GML-path error, sanitize advertised formats, refuse XML with a DTD by [@cholmes](https://github.com/cholmes) in [#861](https://github.com/geoparquet/geoparquet-io/pull/861)
- fix(write): don't crash on a malformed 'geo' block carried from the input by [@cholmes](https://github.com/cholmes) in [#883](https://github.com/geoparquet/geoparquet-io/pull/883)
- fix(write): keep the declared CRS on in-memory and streaming 2.0 geometry types by [@cholmes](https://github.com/cholmes) in [#869](https://github.com/geoparquet/geoparquet-io/pull/869)
- fix(convert): keep the source CRS in the geo block for M and ZM geometry at 2.0 by [@cholmes](https://github.com/cholmes) in [#888](https://github.com/geoparquet/geoparquet-io/pull/888)
- fix(reproject): drop a spherical edges declaration when the destination CRS is projected by [@cholmes](https://github.com/cholmes) in [#884](https://github.com/geoparquet/geoparquet-io/pull/884)
- fix(validate): enforce the covering.bbox path, field-order and field-type rules by [@jatorre](https://github.com/jatorre) in [#877](https://github.com/geoparquet/geoparquet-io/pull/877)
- fix(wfs): keep existing query params (e.g. apikey) when building WFS GetFeature URLs by [@bertt](https://github.com/bertt) in [#836](https://github.com/geoparquet/geoparquet-io/pull/836)
- fix(partition): support explicit quadkey resolutions in directory mode by [@be-student](https://github.com/be-student) in [#900](https://github.com/geoparquet/geoparquet-io/pull/900)
- fix(partition): forward --use-centroid and refuse a lone --resolution up front in quadkey directory mode by [@cholmes](https://github.com/cholmes) in [#902](https://github.com/geoparquet/geoparquet-io/pull/902)
- fix(validate): read X/Y from 6- and 8-element bboxes; handle antimeridian extents by [@jatorre](https://github.com/jatorre) in [#876](https://github.com/geoparquet/geoparquet-io/pull/876)
- fix(geojson): write UTF-8 to stdout from the Python API too by [@cholmes](https://github.com/cholmes) in [#907](https://github.com/geoparquet/geoparquet-io/pull/907)

### Documentation

- docs(inspect): use the key compression_stats actually returns by [@cholmes](https://github.com/cholmes) in [#789](https://github.com/geoparquet/geoparquet-io/pull/789)
- docs: S2 is available again -- the geography extension is published for DuckDB 1.5.5 by [@cholmes](https://github.com/cholmes) in [#860](https://github.com/geoparquet/geoparquet-io/pull/860)
- docs(release): record what the v1.4.0 release actually did by [@cholmes](https://github.com/cholmes) in [#784](https://github.com/geoparquet/geoparquet-io/pull/784)

### New Contributors

- @maxmalynowsky made their first contribution in https://github.com/geoparquet/geoparquet-io/pull/786
- @bertt made their first contribution in https://github.com/geoparquet/geoparquet-io/pull/836
- @be-student made their first contribution in https://github.com/geoparquet/geoparquet-io/pull/900

_16 internal changes and 2 dependency updates are not listed here; the full changelog has them._

**Full Changelog**: https://github.com/geoparquet/geoparquet-io/compare/v1.4.0...v1.5.0

## v1.4.0 (2026-08-30)

This release adds a pipeline for visualizing large data at low zoom.
`gpio process aggregate` rolls features into A5, H3 or admin buckets with `count`,
`--metric` rollups and `--breakdown` pivots; `gpio process overview` derives coarser
levels from that output; and `gpio pmtiles pyramid` bakes the whole stack into one
zoom-banded archive. `gpio sort str` adds Sort-Tile-Recursive ordering beside Hilbert,
`gpio convert --geoparquet-version 1.1-geoarrow` produces native GeoArrow from any
input, and WFS extraction now auto-tiles large layers instead of silently truncating
them at a capped server.

Most of the work went into correctness. A write-contract characterization suite, a
CLI-to-Python-API parity harness, a CLI-surface snapshot, spatial-index golden values,
ten end-to-end journeys, and every fenced example in `docs/guide/*.md` running as a
test together surfaced a large share of the fixes below: stale bbox metadata after a
reproject or a filtered extract, `--row-group-size` ignored by the disk-rewrite write
strategy, secondary geometry columns written with the wrong physical carrier, sidecar
key/value metadata dropped from tables with no geometry column, and unquoted column
identifiers at several raw-SQL sites.

Two changes break existing behavior. Python API defaults that had silently diverged
from their CLI twins are now aligned, and a parity test fails on any new mismatch
([#661](https://github.com/geoparquet/geoparquet-io/issues/661)). The dependency floor
rises to `duckdb>=1.5.2`, which drops a segfault in geometry repair that could kill a
long extraction with no output and no error. The cost is that `gpio add s2` and
`gpio partition s2` are unavailable in this release: the `geography` community
extension is not published for that DuckDB, so both stop with an explanation before
reading input, and both start working again with no gpio change once the extension is
republished upstream. `gpio add a5` is the closest substitute
([#778](https://github.com/geoparquet/geoparquet-io/issues/778)), and it had to be
made reliable to earn that: the a5 community extension POSTs load-time telemetry from
a detached thread that races process exit, which killed roughly one `gpio add a5`
invocation in thirteen with a segfault after the output file was already written. gpio
now opts out of that telemetry before it loads any community extension
([#779](https://github.com/geoparquet/geoparquet-io/issues/779)).

Three people contributed to gpio for the first time in this release, and we are
grateful to all of them. [@cayetanobv](https://github.com/cayetanobv) sent ten pull
requests, and between them they made gpio survive the geometry the real world
actually contains: curved WKB is linearized on read and streamed rather than
materialized, empty geometries sort last instead of failing `ST_Hilbert`, CSV rows
with NULL geometry are kept, repair no longer segfaults on NULL geometry rows,
conversion names the types it cannot handle, `convert geojson` reprojects before it
repairs, and `Table.convert()` stops losing the source CRS on the way to
`Table.write()` — most of them bugs nobody else had hit yet.
[@oakhill87](https://github.com/oakhill87) sent three: Sort-Tile-Recursive ordering,
the second spatial sort gpio offers; the catch that `sort hilbert` dropped
`--write-memory` before it reached the write engine; and the `geoparquet_version`
documentation on `Table.write` and `Table.upload`.
[@Sanjays2402](https://github.com/Sanjays2402) sent three as well, making two silent
failures speak up — DuckDB extension install errors, and the misleading "Batch size 0
too large" on non-paged ArcGIS requests — and adding regression coverage for
empty-geometry bbox exclusion. If any of this sounds like work you would enjoy, the
issue tracker is open and the guides now run as tests, so a fix is easy to prove.

### Breaking

- fix(api)!: align CLI and Python API defaults (admin dataset, hive, WFS page size) by [@cholmes](https://github.com/cholmes) in [#661](https://github.com/geoparquet/geoparquet-io/pull/661)
- fix(deps)!: require duckdb>=1.5.2 and withdraw S2 until the geography extension returns by [@cholmes](https://github.com/cholmes) in [#778](https://github.com/geoparquet/geoparquet-io/pull/778)

### Added

- feat(pmtiles): expose tippecanoe tile-size/drop-densest flags ([#492](https://github.com/geoparquet/geoparquet-io/issues/492)) by [@nlebovits](https://github.com/nlebovits) in [#493](https://github.com/geoparquet/geoparquet-io/pull/493)
- feat(wfs): auto-tile large datasets and optimize extraction performance by [@nlebovits](https://github.com/nlebovits) in [#502](https://github.com/geoparquet/geoparquet-io/pull/502)
- feat: default geometry repair with ST_MakeValid (issue [#506](https://github.com/geoparquet/geoparquet-io/issues/506)) by [@nlebovits](https://github.com/nlebovits) in [#507](https://github.com/geoparquet/geoparquet-io/pull/507)
- feat(carto): extract non-geo (tabular) tables to plain Parquet (issue [#508](https://github.com/geoparquet/geoparquet-io/issues/508)) by [@nlebovits](https://github.com/nlebovits) in [#509](https://github.com/geoparquet/geoparquet-io/pull/509)
- feat(arcgis): make the HTTP timeout configurable for large polygon layers by [@nlebovits](https://github.com/nlebovits) in [#533](https://github.com/geoparquet/geoparquet-io/pull/533)
- feat(inspect): fit head/tail previews to terminal width by [@cholmes](https://github.com/cholmes) in [#520](https://github.com/geoparquet/geoparquet-io/pull/520)
- feat(geoarrow): produce native GeoArrow encoding for 1.1-geoarrow from any input by [@cholmes](https://github.com/cholmes) in [#519](https://github.com/geoparquet/geoparquet-io/pull/519)
- feat(process): add aggregate (a5, h3, admin) for low-zoom visualization by [@cholmes](https://github.com/cholmes) in [#529](https://github.com/geoparquet/geoparquet-io/pull/529)
- feat(ci): harden CI and code-trust machinery — deterministic gates, green main, bots open PRs by [@nlebovits](https://github.com/nlebovits) in [#552](https://github.com/geoparquet/geoparquet-io/pull/552)
- feat(validate): add corpus-surfaced missing checks ([#586](https://github.com/geoparquet/geoparquet-io/issues/586)) by [@cholmes](https://github.com/cholmes) in [#597](https://github.com/geoparquet/geoparquet-io/pull/597)
- feat(process): add --where row filter to process aggregate ([#568](https://github.com/geoparquet/geoparquet-io/issues/568)) by [@cholmes](https://github.com/cholmes) in [#612](https://github.com/geoparquet/geoparquet-io/pull/612)
- feat(process): add --metric-nodata sentinel handling to process aggregate ([#566](https://github.com/geoparquet/geoparquet-io/issues/566)) by [@cholmes](https://github.com/cholmes) in [#613](https://github.com/geoparquet/geoparquet-io/pull/613)
- feat(process): add --bucket-point bbox keying to process aggregate ([#567](https://github.com/geoparquet/geoparquet-io/issues/567)) by [@cholmes](https://github.com/cholmes) in [#614](https://github.com/geoparquet/geoparquet-io/pull/614)
- feat: process overview + pmtiles pyramid ([#570](https://github.com/geoparquet/geoparquet-io/issues/570)) by [@cholmes](https://github.com/cholmes) in [#615](https://github.com/geoparquet/geoparquet-io/pull/615)
- feat(convert): linearize curved WKB geometries on read instead of failing by [@cayetanobv](https://github.com/cayetanobv) in [#647](https://github.com/geoparquet/geoparquet-io/pull/647)
- feat(sort): add Sort-Tile-Recursive ordering by [@oakhill87](https://github.com/oakhill87) in [#766](https://github.com/geoparquet/geoparquet-io/pull/766)

### Changed

- refactor(common): decompose _promote_numeric_type (xenon F→A) by [@nlebovits](https://github.com/nlebovits) in [#494](https://github.com/geoparquet/geoparquet-io/pull/494)
- perf(add): lean on DuckDB SPATIAL_JOIN for admin-divisions/country-codes; fix misleading native-geometry message; re-evaluate bbox pre-filter (supersedes [#462](https://github.com/geoparquet/geoparquet-io/issues/462)) by [@nlebovits](https://github.com/nlebovits) in [#540](https://github.com/geoparquet/geoparquet-io/pull/540)
- perf(convert): stream the linearized curved-geometry read by [@cayetanobv](https://github.com/cayetanobv) in [#650](https://github.com/geoparquet/geoparquet-io/pull/650)

### Fixed

- fix(wfs): add sortBy parameter for stable pagination on PK-less layers by [@nlebovits](https://github.com/nlebovits) in [#489](https://github.com/geoparquet/geoparquet-io/pull/489)
- fix(wfs): trust server-declared CRS from GeoJSON response ([#499](https://github.com/geoparquet/geoparquet-io/issues/499)) by [@nlebovits](https://github.com/nlebovits) in [#500](https://github.com/geoparquet/geoparquet-io/pull/500)
- fix(wfs): auto-tiling fails with --workers > 1 by [@nlebovits](https://github.com/nlebovits) in [#504](https://github.com/geoparquet/geoparquet-io/pull/504)
- fix(arcgis): catch the int32 to timestamp cast error during extraction by [@nlebovits](https://github.com/nlebovits) in [#535](https://github.com/geoparquet/geoparquet-io/pull/535)
- fix(partition): make --auto resolution extent-aware ([#524](https://github.com/geoparquet/geoparquet-io/issues/524)) by [@cholmes](https://github.com/cholmes) in [#526](https://github.com/geoparquet/geoparquet-io/pull/526)
- fix(arcgis): catch duckdb.IOException so the batch-size fallback ladder fires by [@nlebovits](https://github.com/nlebovits) in [#536](https://github.com/geoparquet/geoparquet-io/pull/536)
- fix(spatial): make admin joins and grid keying CRS-aware for non-CRS84 input by [@nlebovits](https://github.com/nlebovits) in [#537](https://github.com/geoparquet/geoparquet-io/pull/537)
- fix(pmtiles): stop the geoarrow push_batch crash on large GeoParquet by [@nlebovits](https://github.com/nlebovits) in [#541](https://github.com/geoparquet/geoparquet-io/pull/541)
- fix(spatial): make add/partition CRS-aware for non-CRS84 input ([#525](https://github.com/geoparquet/geoparquet-io/issues/525)) by [@cholmes](https://github.com/cholmes) in [#530](https://github.com/geoparquet/geoparquet-io/pull/530)
- fix(geometry): summarize geo metadata in verbose output instead of full dump by [@cholmes](https://github.com/cholmes) in [#542](https://github.com/geoparquet/geoparquet-io/pull/542)
- fix(partition): stop leaking the internal __gpio_part alias column into output by [@nlebovits](https://github.com/nlebovits) in [#539](https://github.com/geoparquet/geoparquet-io/pull/539)
- fix(convert): honor --row-group-size-mb on the duckdb-kv write path ([#547](https://github.com/geoparquet/geoparquet-io/issues/547)) by [@nlebovits](https://github.com/nlebovits) in [#549](https://github.com/geoparquet/geoparquet-io/pull/549)
- fix(mutmut): keep test suite CWD-stable so mutation stats phase passes by [@nlebovits](https://github.com/nlebovits) in [#565](https://github.com/geoparquet/geoparquet-io/pull/565)
- fix(ci): rename Codecov 'file' input to 'files' for v7 action by [@nlebovits](https://github.com/nlebovits) in [#575](https://github.com/geoparquet/geoparquet-io/pull/575)
- fix(ci): drop self-approval from Dependabot auto-merge by [@nlebovits](https://github.com/nlebovits) in [#577](https://github.com/geoparquet/geoparquet-io/pull/577)
- fix(ci): handle missing duckdb 'geography' extension for duckdb 1.5.4 by [@nlebovits](https://github.com/nlebovits) in [#598](https://github.com/geoparquet/geoparquet-io/pull/598)
- fix: adversarial review findings for the corpus audit stack ([#591](https://github.com/geoparquet/geoparquet-io/issues/591)–[#597](https://github.com/geoparquet/geoparquet-io/issues/597)) by [@cholmes](https://github.com/cholmes) in [#602](https://github.com/geoparquet/geoparquet-io/pull/602)
- fix(validate): corpus-surfaced validation bugs ([#581](https://github.com/geoparquet/geoparquet-io/issues/581), [#582](https://github.com/geoparquet/geoparquet-io/issues/582), [#584](https://github.com/geoparquet/geoparquet-io/issues/584), [#585](https://github.com/geoparquet/geoparquet-io/issues/585)) by [@cholmes](https://github.com/cholmes) in [#592](https://github.com/geoparquet/geoparquet-io/pull/592)
- fix(validate): dimension-aware Z/M geometry_types handling ([#583](https://github.com/geoparquet/geoparquet-io/issues/583)) by [@cholmes](https://github.com/cholmes) in [#593](https://github.com/geoparquet/geoparquet-io/pull/593)
- fix(convert): preserve input GeoParquet version in auto mode ([#587](https://github.com/geoparquet/geoparquet-io/issues/587)) by [@cholmes](https://github.com/cholmes) in [#594](https://github.com/geoparquet/geoparquet-io/pull/594)
- fix(write): guarantee geo metadata on 2.0 writes of M/ZM data ([#589](https://github.com/geoparquet/geoparquet-io/issues/589)) by [@cholmes](https://github.com/cholmes) in [#595](https://github.com/geoparquet/geoparquet-io/pull/595)
- fix(convert): preserve non-planar edges when rewriting geography data ([#588](https://github.com/geoparquet/geoparquet-io/issues/588)) by [@cholmes](https://github.com/cholmes) in [#596](https://github.com/geoparquet/geoparquet-io/pull/596)
- fix(arcgis): stop reporting 'Batch size 0 too large' on non-paged requests by [@Sanjays2402](https://github.com/Sanjays2402) in [#606](https://github.com/geoparquet/geoparquet-io/pull/606)
- fix(deps): upgrade vulnerable dependencies (automated) by [@nlebovits](https://github.com/nlebovits) in [#623](https://github.com/geoparquet/geoparquet-io/pull/623)
- fix(process): clear errors for missing --metric/--breakdown columns by [@cholmes](https://github.com/cholmes) in [#617](https://github.com/geoparquet/geoparquet-io/pull/617)
- fix(sort): forward --write-memory from sort hilbert to the write engine by [@oakhill87](https://github.com/oakhill87) in [#627](https://github.com/geoparquet/geoparquet-io/pull/627)
- fix(geometry): stop the SIGSEGV in repair on tables with NULL geometry rows by [@cayetanobv](https://github.com/cayetanobv) in [#645](https://github.com/geoparquet/geoparquet-io/pull/645)
- fix(convert): name the offending types when non-linear WKB fails conversion by [@cayetanobv](https://github.com/cayetanobv) in [#646](https://github.com/geoparquet/geoparquet-io/pull/646)
- fix(api): keep the source CRS between Table.convert() and Table.write() by [@cayetanobv](https://github.com/cayetanobv) in [#644](https://github.com/geoparquet/geoparquet-io/pull/644)
- fix(convert): order empty geometries last instead of failing on ST_Hilbert by [@cayetanobv](https://github.com/cayetanobv) in [#651](https://github.com/geoparquet/geoparquet-io/pull/651)
- fix(convert): reproject before repairing geometry in convert geojson by [@cayetanobv](https://github.com/cayetanobv) in [#653](https://github.com/geoparquet/geoparquet-io/pull/653)
- fix(convert): keep CSV rows whose geometry is NULL by [@cayetanobv](https://github.com/cayetanobv) in [#656](https://github.com/geoparquet/geoparquet-io/pull/656)
- fix(duckdb): route bare duckdb.connect() sites through the connection factory by [@cholmes](https://github.com/cholmes) in [#659](https://github.com/geoparquet/geoparquet-io/pull/659)
- fix(core): make library writes free of global state leaks by [@cholmes](https://github.com/cholmes) in [#660](https://github.com/geoparquet/geoparquet-io/pull/660)
- fix(add,sort): forward --write-memory to the write engine by [@cholmes](https://github.com/cholmes) in [#663](https://github.com/geoparquet/geoparquet-io/pull/663)
- fix(metadata): drop stale geo bbox after reproject and filtered extract by [@cholmes](https://github.com/cholmes) in [#658](https://github.com/geoparquet/geoparquet-io/pull/658)
- fix(sql): quote geometry-column identifiers at raw SQL interpolation by [@cholmes](https://github.com/cholmes) in [#662](https://github.com/geoparquet/geoparquet-io/pull/662)
- fix(extract): validate --where on bigquery and carto extractors by [@cholmes](https://github.com/cholmes) in [#657](https://github.com/geoparquet/geoparquet-io/pull/657)
- fix(tests): complete the WFS mock so six tests stop hitting the network by [@cholmes](https://github.com/cholmes) in [#676](https://github.com/geoparquet/geoparquet-io/pull/676)
- fix(add): quote quadkey column identifiers in the file-based path by [@cholmes](https://github.com/cholmes) in [#695](https://github.com/geoparquet/geoparquet-io/pull/695)
- fix(validate): accept the GeoArrow encodings GeoParquet 1.1 permits by [@cholmes](https://github.com/cholmes) in [#715](https://github.com/geoparquet/geoparquet-io/pull/715)
- fix(wfs): surface failed feature-count probes instead of swallowing them by [@cholmes](https://github.com/cholmes) in [#696](https://github.com/geoparquet/geoparquet-io/pull/696)
- fix(write): honor row-group sizing in the disk-rewrite strategy by [@cholmes](https://github.com/cholmes) in [#698](https://github.com/geoparquet/geoparquet-io/pull/698)
- fix(convert): omit the 1.1-only covering key from GeoParquet 1.0 output by [@cholmes](https://github.com/cholmes) in [#714](https://github.com/geoparquet/geoparquet-io/pull/714)
- fix(common): honor explicit parquet-geo-only in write_geoparquet_table by [@cholmes](https://github.com/cholmes) in [#702](https://github.com/geoparquet/geoparquet-io/pull/702)
- fix(write): make arrow-streaming output independent of geoarrow import state by [@cholmes](https://github.com/cholmes) in [#707](https://github.com/geoparquet/geoparquet-io/pull/707)
- fix(metadata): preserve input non-geo KV metadata in convert and Table.write by [@cholmes](https://github.com/cholmes) in [#710](https://github.com/geoparquet/geoparquet-io/pull/710)
- fix(duckdb): surface extension install errors when the extension fails to load by [@Sanjays2402](https://github.com/Sanjays2402) in [#605](https://github.com/geoparquet/geoparquet-io/pull/605)
- fix(metadata): keep the bbox covering on GeoParquet 2.0 output by [@cholmes](https://github.com/cholmes) in [#744](https://github.com/geoparquet/geoparquet-io/pull/744)
- fix(geojson): stop --write-bbox and --id-field truncating every Feature by [@cholmes](https://github.com/cholmes) in [#745](https://github.com/geoparquet/geoparquet-io/pull/745)
- fix(geojson): read stdin into a named GeoJSON output file by [@cholmes](https://github.com/cholmes) in [#746](https://github.com/geoparquet/geoparquet-io/pull/746)
- fix(add): build the country-codes summary from the columns actually written ([#672](https://github.com/geoparquet/geoparquet-io/issues/672)) by [@cholmes](https://github.com/cholmes) in [#750](https://github.com/geoparquet/geoparquet-io/pull/750)
- fix(write): let the disk-rewrite metadata pass merge row groups, not only split ([#697](https://github.com/geoparquet/geoparquet-io/issues/697)) by [@cholmes](https://github.com/cholmes) in [#757](https://github.com/geoparquet/geoparquet-io/pull/757)
- fix(convert): explain that csv/fgb/gpkg/shp cannot read stdin ([#749](https://github.com/geoparquet/geoparquet-io/issues/749)) by [@cholmes](https://github.com/cholmes) in [#752](https://github.com/geoparquet/geoparquet-io/pull/752)
- fix(metadata): honor parquet-geo-only when the geometry column is absent ([#701](https://github.com/geoparquet/geoparquet-io/issues/701)) by [@cholmes](https://github.com/cholmes) in [#753](https://github.com/geoparquet/geoparquet-io/pull/753)
- fix(add): keep bbox-metadata metadata-only and refuse plain Parquet ([#712](https://github.com/geoparquet/geoparquet-io/issues/712), [#713](https://github.com/geoparquet/geoparquet-io/issues/713)) by [@cholmes](https://github.com/cholmes) in [#754](https://github.com/geoparquet/geoparquet-io/pull/754)
- fix(write): give secondary geometry columns the target version's carrier ([#706](https://github.com/geoparquet/geoparquet-io/issues/706)) by [@cholmes](https://github.com/cholmes) in [#765](https://github.com/geoparquet/geoparquet-io/pull/765)
- fix(metadata): carry sidecar KV keys without geometry and without a rewrite ([#708](https://github.com/geoparquet/geoparquet-io/issues/708), [#709](https://github.com/geoparquet/geoparquet-io/issues/709)) by [@cholmes](https://github.com/cholmes) in [#756](https://github.com/geoparquet/geoparquet-io/pull/756)
- fix(metadata): judge geometries against the whole file's geo statistics ([#721](https://github.com/geoparquet/geoparquet-io/issues/721)) by [@cholmes](https://github.com/cholmes) in [#770](https://github.com/geoparquet/geoparquet-io/pull/770)
- fix(duckdb): opt out of a5 extension telemetry that segfaults at exit ([#779](https://github.com/geoparquet/geoparquet-io/issues/779)) by [@cholmes](https://github.com/cholmes) in [#781](https://github.com/geoparquet/geoparquet-io/pull/781)

### Documentation

- docs(api): document geoparquet_version on Table.write/upload; drop write_memory by [@oakhill87](https://github.com/oakhill87) in [#510](https://github.com/geoparquet/geoparquet-io/pull/510)
- docs(spatial-join): correct pre-filter rationale to memory-safety ([#545](https://github.com/geoparquet/geoparquet-io/issues/545) Fix A) by [@nlebovits](https://github.com/nlebovits) in [#551](https://github.com/geoparquet/geoparquet-io/pull/551)
- docs(deps): record why the duckdb pin also holds the TRY() workaround by [@cayetanobv](https://github.com/cayetanobv) in [#654](https://github.com/geoparquet/geoparquet-io/pull/654)
- docs: correct guide examples that use nonexistent CLI flags by [@cholmes](https://github.com/cholmes) in [#735](https://github.com/geoparquet/geoparquet-io/pull/735)
- docs(changelog): compress unreleased entries and link every issue reference by [@cholmes](https://github.com/cholmes) in [#743](https://github.com/geoparquet/geoparquet-io/pull/743)
- docs(write-strategies): say what --write-memory does on extract bigquery ([#673](https://github.com/geoparquet/geoparquet-io/issues/673)) by [@cholmes](https://github.com/cholmes) in [#763](https://github.com/geoparquet/geoparquet-io/pull/763)

### New Contributors

- @oakhill87 made their first contribution in https://github.com/geoparquet/geoparquet-io/pull/510
- @Sanjays2402 made their first contribution in https://github.com/geoparquet/geoparquet-io/pull/606
- @cayetanobv made their first contribution in https://github.com/geoparquet/geoparquet-io/pull/645

_21 internal changes and 24 dependency updates are not listed here; the full changelog has them._

**Full Changelog**: https://github.com/geoparquet/geoparquet-io/compare/v1.3.0...v1.4.0

## v1.3.0 (2026-06-11)

### Feat

- **extract arcgis**: add --max-allowable-offset for server-side generalization ([#484](https://github.com/geoparquet/geoparquet-io/issues/484))
- **api**: add output_crs to from_arcgis and extract_arcgis
- **extract arcgis**: add --output-crs CLI option
- **arcgis**: thread output_crs through convert_arcgis_to_geoparquet
- **arcgis**: tag native CRS from returned SR with mismatch warning
- **arcgis**: thread output_crs through streaming and capture returned SR
- **arcgis**: add EsriJSON page-to-table converter via ST_Read
- **arcgis**: request EsriJSON+outSR in fetch_features_page when output_crs set
- **arcgis**: add CRS parsing helpers for output-crs
- **wfs**: add typed exception subclasses for downstream consumers ([#481](https://github.com/geoparquet/geoparquet-io/issues/481))
- add fiboa plugin (gpio fiboa) ([#451](https://github.com/geoparquet/geoparquet-io/issues/451))
- add Vecorel specification support ([#450](https://github.com/geoparquet/geoparquet-io/issues/450))
- fetch latest Overture Maps release dynamically ([#455](https://github.com/geoparquet/geoparquet-io/issues/455))
- **convert**: add --geoparquet-version 1.1-geoarrow output format ([#436](https://github.com/geoparquet/geoparquet-io/issues/436))
- **extract**: add Carto SQL API extractor ([#449](https://github.com/geoparquet/geoparquet-io/issues/449))

### Fix

- **test**: xfail GeoPackage sequential conversion test on Windows/macOS ([#486](https://github.com/geoparquet/geoparquet-io/issues/486))
- **arcgis**: anchor maxAllowableOffset units by setting outSR
- **arcgis**: unset CRS for unresolvable WKID, resolve native WKT to EPSG ([#482](https://github.com/geoparquet/geoparquet-io/issues/482))
- **partition**: single-pass COPY PARTITION_BY instead of per-value re-scan ([#478](https://github.com/geoparquet/geoparquet-io/issues/478)) ([#480](https://github.com/geoparquet/geoparquet-io/issues/480))
- **check**: assert dict result to satisfy mypy in optimization check
- **arcgis**: read layer SR from extent/sourceSpatialReference fallbacks
- **check**: scale locality threshold by row-group count, report uncomputed metrics as None
- **arcgis**: normalize WKIDs, validate output-crs upfront, dedupe page converters
- **deps**: update mutmut config for 3.6.0 breaking changes
- **deps**: pin duckdb<1.5.2 for geography extension compatibility
- **check**: use spatial locality metrics instead of row-count heuristic ([#456](https://github.com/geoparquet/geoparquet-io/issues/456))
- **add**: filter Overture admin caches to land — stop ~2.6x row multiplication ([#474](https://github.com/geoparquet/geoparquet-io/issues/474))
- **wfs**: handle uint64 overflow in type promotion
- **wfs**: harden schema unification with edge case handling ([#476](https://github.com/geoparquet/geoparquet-io/issues/476))
- **wfs**: handle type mismatches across paginated pages ([#476](https://github.com/geoparquet/geoparquet-io/issues/476))
- **crs**: distinguish null CRS (unknown) from omitted CRS (default) ([#471](https://github.com/geoparquet/geoparquet-io/issues/471))
- **ci**: repair recurring slow-tests failures on main ([#472](https://github.com/geoparquet/geoparquet-io/issues/472))
- **add**: admin-divisions OOM fix — restore plain spatial join, retire in-join dedup ([#461](https://github.com/geoparquet/geoparquet-io/issues/461))
- **add**: restore bbox pre-filter in spatial join ON clause ([#460](https://github.com/geoparquet/geoparquet-io/issues/460))
- **add**: remove bbox pre-filter from spatial join ON clauses ([#457](https://github.com/geoparquet/geoparquet-io/issues/457))
- **ci**: separate blocking security checks from proactive CVE alerts ([#454](https://github.com/geoparquet/geoparquet-io/issues/454))
- **carto**: address code review findings
- **carto**: address security and robustness issues in Carto extractor

## v1.2.0 (2026-06-02)

### Feat

- **s3**: wire all remaining commands with _activate_s3()
- **api**: add S3 endpoint kwargs to read_partition()
- **cli**: wire global S3 config into all commands via ambient config scope
- **cli**: add global --s3-endpoint, --s3-region, --s3-no-ssl, --aws-profile flags
- **s3**: add ambient S3 config scope and explicit kwargs to get_duckdb_connection()
- **s3**: add resolve_s3_config() with env var fallbacks and SSL detection
- **wfs**: add --auto-tile to bypass server startIndex limits

### Fix

- **sort**: use quote_identifier for SQL identifiers and fix cleanup call
- **sort**: address adversarial review findings for PR [#446](https://github.com/geoparquet/geoparquet-io/issues/446)
- **sort**: handle empty/null geometries in Hilbert ordering ([#446](https://github.com/geoparquet/geoparquet-io/issues/446))
- **test**: skip DuckDB tests that crash xdist workers
- **wfs**: handle empty/null properties in GeoJSON features ([#445](https://github.com/geoparquet/geoparquet-io/issues/445))
- **write**: handle remote URLs and clean up dead code in no-geometry path ([#444](https://github.com/geoparquet/geoparquet-io/issues/444))
- **write**: handle geometry_column=None in all write strategies ([#440](https://github.com/geoparquet/geoparquet-io/issues/440))
- **add**: address CodeRabbit review comments
- **add**: preserve ALL metadata in bbox-metadata operation ([#439](https://github.com/geoparquet/geoparquet-io/issues/439))
- **add**: preserve bloom filters and GEOMETRY type in bbox-metadata ([#433](https://github.com/geoparquet/geoparquet-io/issues/433))
- **convert**: support GeoArrow native-encoded GeoParquet as input ([#435](https://github.com/geoparquet/geoparquet-io/issues/435))
- **validate**: guard against None logical_type in schema lookups ([#438](https://github.com/geoparquet/geoparquet-io/issues/438))
- **arcgis**: use CRS84 for metadata when extracting via f=geojson ([#427](https://github.com/geoparquet/geoparquet-io/issues/427))
- **cli**: reconfigure stdout/stderr to UTF-8 at CLI entry ([#432](https://github.com/geoparquet/geoparquet-io/issues/432))
- **tests**: mark flaky WFS test as xfail
- **s3**: connection leak in admin_datasets + add CLI docs
- **deps**: update vulnerable dependencies
- **s3**: address adversarial review findings
- **deps**: upgrade pip to 26.1 for CVE-2026-6357
- **wfs**: add retry logic for transient network errors
- **wfs**: address adversarial review findings ([#431](https://github.com/geoparquet/geoparquet-io/issues/431))
- **wfs**: adaptive pagination and reliability fixes for auto-tile
- **wfs**: detect server startIndex limits and improve error messages
- **wfs**: auto-paginate large datasets and fix parallel worker crash
- **pmtiles**: address adversarial review of [#422](https://github.com/geoparquet/geoparquet-io/issues/422)
- **pmtiles**: handle BrokenPipe and surface upstream stderr ([#421](https://github.com/geoparquet/geoparquet-io/issues/421))

### Refactor

- **wfs**: extract property probe and query builder helpers
- **geoarrow**: move coord-expr helpers to duckdb_utils, remove circular import
- **geo_metadata**: deduplicate _get_query_column_type by importing from duckdb_utils
- **admin**: use s3_config_scope() instead of configure_s3(con)
- **cli**: hide per-command S3 and profile flags (now global)
- **wfs**: unify fetch to always use httpx + local parsing ([#425](https://github.com/geoparquet/geoparquet-io/issues/425), [#426](https://github.com/geoparquet/geoparquet-io/issues/426), [#429](https://github.com/geoparquet/geoparquet-io/issues/429))

## v1.1.1 (2026-04-29)

### Fix

- **windows**: release file handles before replace in bbox_metadata ([#420](https://github.com/geoparquet/geoparquet-io/issues/420))

## v1.1.0 (2026-04-29)

### Feat

- integrate gpio-pmtiles into core ([#417](https://github.com/geoparquet/geoparquet-io/issues/417))
- **api**: expose axis_order and strict_crs in Python API
- **wfs**: add WFS 2.0 support, axis order fix, and CRS validation ([#312](https://github.com/geoparquet/geoparquet-io/issues/312), [#397](https://github.com/geoparquet/geoparquet-io/issues/397), [#398](https://github.com/geoparquet/geoparquet-io/issues/398))

### Fix

- **wfs**: address adversarial review findings for --output-crs
- **wfs**: honor --output-crs by reprojecting when server returns different CRS ([#407](https://github.com/geoparquet/geoparquet-io/issues/407))
- skip non-GeoParquet files early in check --pmtiles ([#408](https://github.com/geoparquet/geoparquet-io/issues/408))
- address PR [#417](https://github.com/geoparquet/geoparquet-io/issues/417) adversarial review findings
- PMTiles pipeline bugs found in adversarial review
- extend bbox fixes to streaming code paths ([#415](https://github.com/geoparquet/geoparquet-io/issues/415))
- GeoParquet 2.0 bbox handling for reproject, check, and add commands ([#409](https://github.com/geoparquet/geoparquet-io/issues/409), [#410](https://github.com/geoparquet/geoparquet-io/issues/410), [#412](https://github.com/geoparquet/geoparquet-io/issues/412))
- **tests**: use tuple comparison for pip version check
- **tests**: resolve CI failures for pip-audit CVE and wfs import ([#414](https://github.com/geoparquet/geoparquet-io/issues/414))
- **inspect**: scope fixtures to module, add markers, fix multi-geometry stats
- **inspect**: default --geo-stats to 1 row group with "... and N more" hint
- **inspect**: adapt geo_bbox precision to fit large projected coordinates
- **inspect**: use 2 decimal places for geo_bbox display and remove truncation
- **inspect**: show geo_bbox stats in row group tables and --geo-stats ([#413](https://github.com/geoparquet/geoparquet-io/issues/413))
- **wfs**: add version guard for srsName and improve test coverage ([#406](https://github.com/geoparquet/geoparquet-io/issues/406))
- **wfs**: include srsName parameter without bbox filter ([#406](https://github.com/geoparquet/geoparquet-io/issues/406))
- **test**: correct import path for list_layers module
- **convert**: apply gc.collect() on all platforms for multi-layer reads ([#403](https://github.com/geoparquet/geoparquet-io/issues/403))
- **wfs**: move type inference after concat, fix resource leak ([#400](https://github.com/geoparquet/geoparquet-io/issues/400))
- **wfs**: infer column types from string values ([#402](https://github.com/geoparquet/geoparquet-io/issues/402))
- **wfs**: address review findings
- **deps**: bump lxml>=6.1.0 for CVE-2026-41066 ([#395](https://github.com/geoparquet/geoparquet-io/issues/395))
- **deps**: remove pyarrow version cap ([#394](https://github.com/geoparquet/geoparquet-io/issues/394))
- **ci**: rename release.yml back to publish.yml for PyPI trusted publisher

## v1.0.1 (2026-04-20)

### Fix

- **arcgis**: address code review feedback for adaptive batch
- **arcgis**: add adaptive batch size and --batch-size flag ([#382](https://github.com/geoparquet/geoparquet-io/issues/382))
- **reproject**: use PROJJSON for ST_Transform when CRS lacks authority id ([#383](https://github.com/geoparquet/geoparquet-io/issues/383))
- **inspect**: eliminate false-positive CRS mismatch warnings for GeoParquet 2.0 ([#384](https://github.com/geoparquet/geoparquet-io/issues/384))
- **deps**: bump pytest to 9.0.3 for CVE-2025-71176 ([#389](https://github.com/geoparquet/geoparquet-io/issues/389))

### Refactor

- **release**: adopt portolan-cli release workflow ([#390](https://github.com/geoparquet/geoparquet-io/issues/390))

## v1.0.0 (2026-04-13)

### BREAKING CHANGE

- `geography_as_geometry` parameter no longer accepted.
BREAKING CHANGE: `gt` CLI alias removed, use `gpio` instead.

### Feat

- **core**: add framework-agnostic exception classes and CLI handler
- **api**: add partition_by_a5() and fix documentation drift ([#361](https://github.com/geoparquet/geoparquet-io/issues/361))
- **convert**: support multiple geometry columns in GeoParquet files ([#357](https://github.com/geoparquet/geoparquet-io/issues/357))
- **skills**: Package skill with gpio for all LLM users
- **docs**: Add menard documentation anti-drift system ([#332](https://github.com/geoparquet/geoparquet-io/issues/332))

### Fix

- **release**: use PyPI menard instead of git dependency ([#381](https://github.com/geoparquet/geoparquet-io/issues/381))
- **test**: align slow test assertions with core exception types
- **test**: handle Windows path separators in test_file_utils
- **security**: address adversarial review findings for v1.0
- **imports**: update disk_rewrite.py to import compute_bbox_via_sql from geo_metadata
- **cli**: unify exception handling across all CLI commands
- **exceptions**: unify GeoParquetError and update tests for new exception types
- **docs**: update mkdocstrings paths for reorganized modules
- **security**: sanitize URLs in logs to prevent credential leakage
- **security**: escape single quotes in SQL to prevent injection
- import-linter config and broken test monkeypatch
- **imports**: update all imports for partition/ and add/ module reorganization
- Remove inconsistent force/skip_analysis params and fix test marker
- **tests**: Relax performance threshold for macOS CI runners
- **tests**: Mark all transportforcairo WFS integration tests as xfail
- **tests**: Narrow WFS xfail to only catch WFSError exceptions
- **tests**: mark unreliable transportforcairo WFS tests as xfail
- update remaining tests to use dynamic geometry column detection
- geometry column detection and SQL identifier quoting
- **convert**: preserve original geometry column names and fix multi-geometry for all write strategies ([#357](https://github.com/geoparquet/geoparquet-io/issues/357))
- **core**: address CodeRabbit findings and centralize geometry detection
- **bigquery**: Address PR review findings
- **arcgis**: handle schema mismatch between paginated batches ([#355](https://github.com/geoparquet/geoparquet-io/issues/355))
- **benchmark**: use shared DuckDB connection helper and add test markers ([#353](https://github.com/geoparquet/geoparquet-io/issues/353))
- address CodeRabbit review feedback
- support native geo stats and honor row_groups limit
- **test**: update assertion to avoid conflict with v1.1 warning
- **docs**: Fix README badges, mkdocs build, and add changelog sync
- **docs**: Fix stale documentation and menard links
- **bigquery**: Add Python API parity and fix edges semantic bug
- **deps**: resolve security audit failures
- **deps**: resolve security audit failures
- **wfs**: switch integration tests from offline USGS to Cairo WFS
- address review issues in mutmut PR
- **test**: improve commitizen test robustness and coverage

### Refactor

- **core**: remove duplicate functions from common.py
- **core/partition**: replace click exceptions with core exceptions
- **core/add**: replace click exceptions with core exceptions
- **core**: replace click exceptions with core exceptions
- **core**: remove duplicated code from common.py, add re-exports
- **core**: extract modules from common.py monolith ([#364](https://github.com/geoparquet/geoparquet-io/issues/364))
- **core**: extract remote, geometry_detection, file_utils from common.py ([#363](https://github.com/geoparquet/geoparquet-io/issues/363))
- **docs**: Compress contributing.md and add auto-sync
- **docs**: Compress CLAUDE.md and add deterministic enforcement
- **scripts**: Consolidate doc generators and pre-commit hooks
- **wfs**: remove ~900 lines of dead code, fix SQL injection, add parallel pagination

## v1.0.0b2 (2026-03-06)

### Feat

- allow specifying --profile web for web specific parquet structure checks ([#252](https://github.com/geoparquet/geoparquet-io/issues/252))

### Fix

- Address code review issues for FileGDB CRS detection
- Add FileGDB CRS detection workaround ([#250](https://github.com/geoparquet/geoparquet-io/issues/250))

## v1.0-beta (2026-02-10)

### Feat

- add directory input support with --min-size to partition s2 and quadkey commands
- add directory input support with --min-size to partition h3 command
- add sub_partition_directory function for batch sub-partitioning
- add find_large_files function for directory scanning
- add --min-size and --in-place options to partition_options decorator

### Fix

- add min_size and in_place params to all partition function signatures
- Fix CRS export for GDAL formats — projected CRS now roundtrips through FlatGeobuf and GeoPackage (fixes [#189](https://github.com/geoparquet/geoparquet-io/issues/189), [#190](https://github.com/geoparquet/geoparquet-io/issues/190))
- Fix crash on non-numeric CRS codes like IGNF:LAMB93 ([#193](https://github.com/geoparquet/geoparquet-io/issues/193))
- Fix inspect metadata performance regression ([#232](https://github.com/geoparquet/geoparquet-io/issues/232))
- Improved error messages for common user mistakes — invalid Parquet files now show helpful hints ([#140](https://github.com/geoparquet/geoparquet-io/issues/140))

## v0.9.0 (2026-01-17)

## v0.8.0 (2026-01-04)

## v0.7.0 (2025-12-28)

## v0.6.1 (2025-12-11)

## v0.6.0 (2025-12-06)

### Feat

- enhance inspect command with geometry types and WKT preview ([#93](https://github.com/geoparquet/geoparquet-io/issues/93))

## v0.5.1 (2025-12-04)

## v0.5.0 (2025-12-02)

### Feat

- **io**: add remote-to-remote support with consolidated write infrastructure ([#74](https://github.com/geoparquet/geoparquet-io/issues/74))
- **auth**: add automatic AWS credential discovery for S3
- **io**: add remote-to-remote operations and automatic AWS auth
- **remote**: handle edge cases for remote file operations                                                                                                                             │ │                                                                                                                                                                                                         │ │   Edge case improvements:                                                                                                                                                                               │ │   - fix(convert): support remote parquet files via read_parquet() instead of ST_Read()                                                                                                                  │ │   - fix(convert): validate only local files, allow remote URLs to pass through                                                                                                                          │ │   - feat(stac): block remote files with clear error message and TODO for future                                                                                                                         │ │   - feat(common): add progress indicator for remote operations (shows protocol)                                                                                                                         │ │   - feat(common): add get_remote_error_hint() for better error messages                                                                                                                                 │ │   - docs: update limitations section with what works vs doesn't work                                                                                                                                    │ │                                                                                                                                                                                                         │ │   Closes edge cases for remote reads before tests/docs phase.
- **upload**: add upload command to remote buckets using obstore - Upload command with obstore, supporting parallelism and progress tracking - Single file and directory uploads - Support for s3, GCS, Azure, HTTP - Pattern filtering - Dry run mode ([#68](https://github.com/geoparquet/geoparquet-io/issues/68))
- **check**: add --fix flag to automatically correct issues detected by check command - add --fix, --fix-output, and --no-backup flags to all check commands - refactor check functions to return structured results enabling fixes via new core/check_fixes.py module. ([#63](https://github.com/geoparquet/geoparquet-io/issues/63))
- **cli**: add benchmark command for conversion performance testing ([#65](https://github.com/geoparquet/geoparquet-io/issues/65))

### Fix

- **check**: remove format command group - remove format command group; consolidate under check - move add-bbox-metadata to add command group - simplify add-bbox command - update + reorg tests

## v0.4.0 (2025-11-17)

### Feat

- **cli**: add ability to pass custom basename for partition output file, e.g., fields_NL.parquet instead of NL.parquet
- **stac**: add STAC Item and Collection generation ([#57](https://github.com/geoparquet/geoparquet-io/issues/57))
- **cli**: Add convert command for optimized GeoParquet conversion ([#56](https://github.com/geoparquet/geoparquet-io/issues/56))

### Fix

- **tests**: correct issue with failing test on windows

## v0.3.0 (2025-11-06)

## v0.2.0 (2025-10-24)

### Refactor

- **cli**: consolidate repetitive option decorators ([#36](https://github.com/geoparquet/geoparquet-io/issues/36))

## v0.1.0 (2025-10-24)

### Feat

- **cli**: add inspect command for fast file examination ([#31](https://github.com/geoparquet/geoparquet-io/issues/31))
- **partition**: add KD-tree spatial partitioning ([#30](https://github.com/geoparquet/geoparquet-io/issues/30))
- Add intelligent partition analysis with recommendations and H3 column exclusion
- **partition**: add H3 partitioning with auto-column creation
- **add**: add H3 support with computed column abstraction ([#23](https://github.com/geoparquet/geoparquet-io/issues/23))

### Fix

- Add Windows compatibility for hive partition tests
- Resolve Windows file locking issues in tests
- **tests**: Ensure DuckDB connections are closed before file cleanup
- Update test_partition_format.py import to use geoparquet_io
