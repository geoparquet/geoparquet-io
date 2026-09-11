# Converting Between Formats

The `convert` command transforms between GeoParquet and other vector formats with automatic format detection and optimization.

!!! note "CLI vs Python Behavior"
    The CLI `gpio convert` applies Hilbert sorting by default for optimal spatial queries.
    The Python `gpio.convert()` does NOT sort by default - chain `.sort_hilbert()` explicitly if needed.

## Basic Usage

=== "CLI"

    <!-- doctest: needs-ogr, setup="ogr2ogr -f 'ESRI Shapefile' input.shp places.geojson" -->
    ```bash
    gpio convert input.shp output.parquet
    ```

    Automatically applies:

    - ZSTD compression (level 15)
    - Bbox column with proper metadata
    - Hilbert spatial ordering
    - GeoParquet metadata (version auto-detected — see
      [GeoParquet Version](#geoparquet-version); 1.1 for non-GeoParquet inputs)

    Row groups are left to the Parquet writer's own default (122,880 rows for
    DuckDB-backed writes). `convert` does not apply the 49,152-row default
    `gpio sort` uses — pass `--row-group-size` if you want it. It also does not
    snap the value to the writer's 2,048-row vector the way the sort commands
    do, so the writer rounds whatever you pass *up* to a multiple of 2,048.

=== "Python"

    <!-- doctest: needs-ogr, setup="ogr2ogr -f 'ESRI Shapefile' input.shp places.geojson" -->
    ```python
    import geoparquet_io as gpio

    # Convert with Hilbert sorting (recommended)
    gpio.convert('input.shp').sort_hilbert().write('output.parquet')

    # Or without sorting (faster but less optimal for spatial queries)
    gpio.convert('input.shp').write('output.parquet')
    ```

## Supported Formats

### Input Formats (to GeoParquet)

Auto-detected by file extension:

- **Shapefile** (.shp)
- **GeoJSON** (.geojson, .json)
- **GeoPackage** (.gpkg)
- **FlatGeobuf** (.fgb)
- **File Geodatabase** (.gdb)
- **CSV/TSV** (.csv, .tsv, .txt) - See [CSV/TSV Support](#csvtsv-support) below

Any format supported by DuckDB's spatial extension (50+ formats) can be read.

### Output Formats (from GeoParquet)

Auto-detected from output file extension:

- **GeoParquet** (.parquet) - Optimized cloud-native format
- **GeoPackage** (.gpkg) - SQLite-based OGC standard
- **FlatGeobuf** (.fgb) - Cloud-native streaming format
- **CSV** (.csv) - Tabular with WKT geometry
- **Shapefile** (.shp) - Legacy ESRI format
- **GeoJSON** (.geojson, .json) - Web-friendly JSON format

## Converting FROM GeoParquet

Convert GeoParquet to other formats with automatic format detection:

=== "CLI Auto-Detection"

    ```bash
    # Auto-detects format from extension
    gpio convert data.parquet output.gpkg      # → GeoPackage
    gpio convert data.parquet output.fgb       # → FlatGeobuf
    gpio convert data.parquet output.csv       # → CSV with WKT
    gpio convert data.parquet output.shp       # → Shapefile
    gpio convert data.parquet output.geojson   # → GeoJSON
    ```

=== "CLI Explicit Format"

    ```bash
    # Use explicit subcommand
    gpio convert geopackage data.parquet output.gpkg
    gpio convert flatgeobuf data.parquet output.fgb
    gpio convert csv data.parquet output.csv
    gpio convert shapefile data.parquet output.shp
    gpio convert geojson data.parquet output.geojson
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Load and convert
    table = gpio.read('data.parquet')

    # Auto-detects from extension
    table.write('output.gpkg')      # → GeoPackage
    table.write('output.fgb')       # → FlatGeobuf
    table.write('output.csv')       # → CSV with WKT
    table.write('output.shp')       # → Shapefile
    table.write('output.geojson')   # → GeoJSON

    # Or use explicit format
    table.write('output.dat', format='csv')
    ```

### Format-Specific Options

**GeoPackage:**

```bash
# Custom layer name
gpio convert data.parquet output.gpkg --layer-name buildings

# Overwrite existing
gpio convert data.parquet output.gpkg --overwrite
```

**Shapefile:**

```bash
# Custom encoding (default: UTF-8)
gpio convert data.parquet output.shp --encoding ISO-8859-1

# Overwrite existing
gpio convert data.parquet output.shp --overwrite
```

!!! warning "Shapefile Limitations"
    - Column names truncated to 10 characters
    - File size limit of 2GB
    - Limited data type support
    - Creates multiple files (.shp, .shx, .dbf, .prj)
    - Consider using GeoPackage or FlatGeobuf instead

!!! info "Remote Shapefile Storage"
    When writing shapefiles to remote storage (S3, GCS, Azure), all sidecar files (.shp, .shx, .dbf, .prj, etc.) are automatically packaged into a single `.shp.zip` archive before upload. This ensures atomic uploads and avoids incomplete multi-file uploads.

    ```bash
    # Local: Creates output.shp, output.shx, output.dbf, etc.
    gpio convert data.parquet output.shp
    ```

    <!-- doctest: skip="needs cloud credentials" -->
    ```bash
    # Remote: Uploads output.shp.zip containing all files
    gpio convert data.parquet s3://bucket/output.shp
    # → Creates s3://bucket/output.shp.zip
    ```

**CSV:**

<!-- doctest: menu -->
```bash
# Include WKT geometry (default)
gpio convert data.parquet output.csv

# Exclude geometry
gpio convert data.parquet output.csv --no-wkt

# Exclude bbox column
gpio convert data.parquet output.csv --no-bbox
```

**GeoJSON:**

<!-- doctest: menu -->
```bash
# Custom precision (default: 7)
gpio convert data.parquet output.geojson --precision 5

# Pretty-print JSON
gpio convert data.parquet output.geojson --pretty
```

```bash
# Include bbox for each feature
gpio convert data.parquet output.geojson --write-bbox
```

<!-- doctest: skip="uses an attribute column the sample dataset does not carry" -->
```bash
# Use specific field as feature ID
gpio convert data.parquet with-ids.geojson --id-field osm_id
```

### Cloud Output Support

All formats support cloud destinations via upload:

=== "CLI"

    ```bash
    # Write local then upload
    gpio convert data.parquet local.gpkg
    ```

    <!-- doctest: skip="needs cloud credentials" -->
    ```bash
    gpio publish upload local.gpkg s3://bucket/output.gpkg
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Write locally first
    table = gpio.read('data.parquet')
    table.write('local.gpkg')
    ```

    <!-- doctest: skip="needs cloud credentials" -->
    ```python
    # Upload to cloud
    gpio.upload('local.gpkg', 's3://bucket/output.gpkg')
    ```

## Multi-Layer Formats

GeoPackage and FileGDB files can contain multiple layers. By default, the first layer is read. Use `--layer` to select a specific layer.

=== "CLI"

    <!-- doctest: skip="needs a multi-layer GeoPackage; gpio convert geopackage writes one layer" -->
    ```bash
    # Read specific layer from GeoPackage
    gpio convert geoparquet multilayer.gpkg buildings.parquet --layer buildings

    # Read specific layer from FileGDB
    gpio convert geoparquet data.gdb roads.parquet --layer roads

    # Without --layer, reads the first/default layer
    gpio convert geoparquet data.gpkg output.parquet
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio
    ```

    <!-- doctest: skip="needs a multi-layer GeoPackage; gpio convert geopackage writes one layer" -->
    ```python
    # Read specific layer
    gpio.convert('multilayer.gpkg', layer='buildings').write('buildings.parquet')
    gpio.convert('multilayer.gpkg', layer='roads').write('roads.parquet')

    # Read first layer (default)
    gpio.convert('multilayer.gpkg').write('output.parquet')
    ```

!!! warning "Invalid Layer Names"
    Due to an upstream bug in DuckDB's spatial extension, specifying a non-existent layer name may cause a crash instead of raising an error. Ensure layer names are valid before conversion. You can inspect available layers using tools like `ogrinfo`:

    <!-- doctest: skip="needs a multi-layer GeoPackage and the GDAL command line" -->
    ```bash
    ogrinfo multilayer.gpkg
    ```

## GeoParquet-to-GeoParquet Conversion

When converting between GeoParquet files, special handling is applied to preserve GeoParquet-specific features.

### Multiple Geometry Columns

GeoParquet files can have multiple geometry columns (e.g., `geometry` for point locations and `boundary` for polygon boundaries). When converting, all geometry columns are preserved with their:

- Original column names
- CRS (coordinate reference system)
- Encoding (WKB)
- Geometry types

=== "CLI"

    <!-- doctest: skip="needs input_multi_geom.parquet, which the harness does not seed" -->
    ```bash
    # Both geometry columns preserved
    gpio convert input_multi_geom.parquet output.parquet
    ```

=== "Python"

    <!-- doctest: skip="needs input_multi_geom.parquet, which the harness does not seed" -->
    ```python
    import geoparquet_io as gpio

    # Both geometry columns preserved automatically
    gpio.convert('input_multi_geom.parquet').write('output.parquet')

    # With Hilbert sorting (uses primary geometry column)
    gpio.convert('input_multi_geom.parquet').sort_hilbert().write('output.parquet')
    ```

!!! note "Bbox and Hilbert Ordering"
    Bbox computation and Hilbert spatial ordering use the **primary geometry column only**. Secondary geometry columns are preserved but do not influence spatial indexing.

!!! note "When the `covering` metadata is written"
    A `covering` entry tells readers that a bbox column's values bound the
    geometry, so engines prune on it. `gpio convert` declares one only when it
    can stand behind the claim:

    - it computed the bbox column itself, from the geometry, during this convert
    - the input's own metadata already declared a covering for the column being
      preserved
    - the output carries a conventional `bbox` struct column (the shape every
      GeoParquet 1.0 writer emitted, before `covering` existed) — this is what
      makes a 1.0 → 1.1 upgrade declare it

    A preserved column with any other name — `bounds`, `parcel_extent`,
    `tile_bounds` — is left undeclared, because its name is not evidence that
    its values bound the geometry, and a wrong covering makes readers skip rows
    that genuinely match. Declare such a column deliberately with
    [`gpio add bbox-metadata`](add.md); `gpio check bbox` will point this out.

!!! note "When the input's CRS is not valid PROJJSON"
    GeoParquet requires a column's `crs` to be a PROJJSON object, which means it
    must carry a `type` member such as `"GeographicCRS"` or `"ProjectedCRS"`.
    `gpio convert` writes the input's CRS into the output, so it checks that CRS
    before writing rather than passing a defect on:

    - a CRS missing only `type`, but carrying an `id` such as
      `{"authority": "EPSG", "code": 3857}`, is **repaired** — the identifier
      names the CRS unambiguously, so gpio rebuilds the full PROJJSON from it
      and says so
    - anything else — no usable identifier, an identifier no CRS database
      knows, or a `type` that is not a PROJJSON CRS type — **fails the
      conversion** with an error naming the file and the CRS

    The result is either a valid file or a clear error, never a file that
    `gpio check spec` would reject. Fix such a CRS in the source data, or
    re-export it from a tool that writes valid PROJJSON.

!!! note "An explicit default CRS is normalized, not preserved"
    GeoParquet writes its default CRS (OGC:CRS84, equivalently EPSG:4326) by
    *omitting* the `crs` key, so an input that spells that default out comes back
    without the key. The coordinates and their meaning are unchanged, but a
    byte-for-byte diff of the `geo` metadata will show the key gone. `convert`
    rebuilds the `geo` block from the converted data, so this applies at every
    `--geoparquet-version`. Run with `--verbose` to see a note when it happens.

### Custom Geometry Column Names

GeoParquet files can use non-standard geometry column names (e.g., `the_geom`, `my_geometry`). These names are preserved during conversion:

=== "CLI"

    ```bash
    # Input has primary_column: "the_geom"
    # Output preserves "the_geom" (not renamed to "geometry")
    gpio convert input.parquet output.parquet
    ```

=== "Python"

    <!-- doctest: skip="names 'the_geom', a geometry column the sample data does not use" -->
    ```python
    import geoparquet_io as gpio

    # Input has primary_column: "the_geom"
    # Output preserves "the_geom" (not renamed to "geometry")
    gpio.convert('input.parquet').write('output.parquet')

    # Custom geometry column names are automatically detected
    # For files without GeoParquet metadata, specify the column:
    gpio.convert('input.parquet', geometry_column='the_geom').write('output.parquet')
    ```

### Edges Metadata Preservation

Non-planar edges metadata (`"edges": "spherical"`, e.g. from BigQuery
GEOGRAPHY extracts) survives every rewrite: `convert`, `extract`, `sort`,
`convert reproject`, and `partition` all carry it through to the output —
including remote (S3/GCS/Azure) outputs.

The one exception is reprojecting into a **projected** CRS. `gpio` reprojects
vertices, not edges: it does not densify along great circles first, so the
output's edges are straight lines in the destination CRS and the declaration is
dropped (with a warning) rather than carried onto data it no longer describes —
`planar`, the spec default, is what the output actually is. Densify before
reprojecting if great-circle edges must be preserved. Reprojecting between two
geographic CRSs (a datum shift, e.g. `EPSG:4326` → `EPSG:4269`) keeps the
declaration unchanged. `gpio check spec` warns about files that carry
`edges: spherical` on a projected CRS.

=== "CLI"

    ```bash
    # Input with edges: "spherical" keeps it in the output
    gpio convert geography.parquet output.parquet
    gpio sort hilbert geography.parquet sorted.parquet
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # edges metadata is preserved through the pipeline
    gpio.read('geography.parquet').sort_hilbert().write('output.parquet')
    ```

When a GeoParquet 2.0 input declares an ellipsoidal edges algorithm
(`vincenty`, `karney`, `andoyer`, `thomas`) and the output is 1.x, the
algorithm is mapped to `"spherical"` — the only non-planar value 1.x supports
— with a warning. 2.0 outputs keep the algorithm verbatim.

### Z and M Dimensions

Geometries with Z (elevation) and/or M (measure) values are preserved, and the
written `geometry_types` metadata carries the spec's dimension suffixes
(`"Point Z"`, `"LineString ZM"`). `gpio check spec` validates these suffixes
against the actual coordinate dimensions in both directions.

## Remote Files

Read from cloud storage or HTTPS:

<!-- doctest: skip="needs cloud credentials" -->
```bash
# Convert remote file
gpio convert https://example.com/data.geojson local.parquet

# Convert from S3
gpio convert s3://bucket/input.parquet local-optimized.parquet

# Convert remote to local format
gpio convert s3://bucket/data.parquet local.gpkg
```

A URL is used exactly as you paste it — gpio takes it as already percent-encoded and never re-encodes it. See [Remote Files Guide](remote-files.md) for authentication setup.

## Options

### Geometry Repair

Invalid geometry (self-intersections, unclosed rings — common in government and
municipal data) is repaired automatically with `ST_MakeValid` and a warning
reports how many features were fixed.

Repair is **on by default** across the pipeline (`convert`, `extract wfs`,
`extract arcgis`, `extract bigquery`, `extract carto`, `extract geoparquet`,
`convert geojson`, and `pmtiles create`). It prevents downstream failures such
as tippecanoe `TopologyException` crashes during PMTiles generation.

To preserve invalid geometry exactly (e.g. for round-tripping), opt out — gpio
still counts and warns about the invalid features it left untouched.

=== "CLI"

    <!-- doctest: skip="needs a GeoPackage containing invalid geometries to repair" -->
    ```bash
    # Default: repairs and warns ("Repaired 3 invalid geometries")
    gpio convert cordoba.gpkg output.parquet

    # Opt out: preserves invalid geometry, still warns
    # ("Left unrepaired 3 invalid geometries (--no-repair-geometry)")
    gpio convert cordoba.gpkg output.parquet --no-repair-geometry
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio
    ```

    <!-- doctest: skip="needs a GeoPackage containing invalid geometries to repair" -->
    ```python
    # Default: repairs invalid geometry
    gpio.convert("cordoba.gpkg").write("output.parquet")

    # Opt out: preserve invalid geometry exactly
    gpio.convert("cordoba.gpkg", repair_geometry=False).write("output.parquet")
    ```

Only invalid geometry is touched: valid geometry is byte-identical, NULL is
preserved, and bounding boxes stay correct (`ST_MakeValid` never expands an
envelope).

### Curved Geometries

GeoParquet cannot represent curved geometry types (CircularString,
CompoundCurve, CurvePolygon, MultiCurve, MultiSurface — common in GeoPackage
and FileGDB exports from CAD or ArcGIS), so by default gpio strokes arcs into
line segments on read, the same operation as `ogr2ogr -nlt CONVERT_TO_LINEAR`
or PostGIS `ST_CurveToLine`. A warning reports how many features were
linearized.

Note this **alters the geometry**: arcs become chains of straight segments,
sampled every 4° of arc by default (GDAL's default; ~0.08% area error on a
full circle). Earlier gpio versions failed on curved input — opt out with
`--no-linearize-curves` to keep that strict behavior.

=== "CLI"

    <!-- doctest: skip="needs a GeoPackage containing curved geometries to linearise" -->
    ```bash
    # Default: linearizes and warns ("Linearized 66 curved geometries ...")
    gpio convert curved.gpkg output.parquet

    # Denser sampling: 1 degree per segment
    gpio convert curved.gpkg output.parquet --max-angle-deg 1

    # Opt out: curved input fails with an actionable error
    gpio convert curved.gpkg output.parquet --no-linearize-curves
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio
    ```

    <!-- doctest: skip="needs a GeoPackage containing curved geometries to linearise" -->
    ```python
    # Default: linearizes curved geometries
    gpio.convert("curved.gpkg").write("output.parquet")

    # Denser sampling: 1 degree per segment
    gpio.convert("curved.gpkg", max_angle_deg=1.0).write("output.parquet")

    # Opt out: raise on curved input instead
    gpio.convert("curved.gpkg", linearize_curves=False)
    ```

Linear geometries pass through untouched, NULL is preserved, and empty curves
become their empty linear counterpart. The surface family (PolyhedralSurface,
TIN, Triangle) is not linearized and still raises an error.

Curved input is spotted either by a header scan of local GeoPackages or on the
first pass that parses geometry. `--skip-hilbert` removes that pass, so for a
curved source which is not a local `.gpkg` (FileGDB, a GeoPackage on S3) the
write is where the curves first show up. gpio then linearizes the source and
writes again. That costs one extra read of the source, and only when it holds
curves. `--no-linearize-curves` keeps the error instead.

### Skip Hilbert Ordering

For faster conversion when spatial ordering isn't critical:

<!-- doctest: setup="gpio convert geopackage input.parquet large.gpkg" -->
```bash
gpio convert large.gpkg output.parquet --skip-hilbert
```

Trade-off: Faster conversion but less optimal for spatial queries.

### Custom Compression

Control compression type and level:

<!-- doctest: needs-ogr, setup="ogr2ogr -f 'ESRI Shapefile' input.shp places.geojson" -->
```bash
# GZIP compression
gpio convert input.shp output.parquet --compression GZIP --compression-level 6

# Uncompressed (not recommended)
gpio convert input.geojson output.parquet --compression UNCOMPRESSED
```

Available compression types:
- `ZSTD` (default, level 15) - Best compression + speed balance
- `GZIP` (level 1-9) - Wide compatibility
- `BROTLI` (level 1-11) - High compression
- `LZ4` - Fastest decompression
- `SNAPPY` - Fast compression
- `UNCOMPRESSED` - No compression

### GeoParquet Version

Control the GeoParquet encoding version written to output.

**Auto-detection (default):** when `--geoparquet-version` is not specified, the
output version is resolved from the input:

- **GeoParquet 2.0 input** → stays 2.0 (no silent downgrade to 1.1)
- **Bare native geo types** (Parquet `GEOMETRY`/`GEOGRAPHY` columns without
  `geo` metadata) → upgraded to 2.0
- **GeoParquet 1.x input** → written as 1.1
- **Non-GeoParquet input** (Shapefile, GeoJSON, ...) → written as 1.1

Auto-detection applies to both `gpio convert` and `gpio convert reproject`. An
explicit `--geoparquet-version` always wins. The Python API resolves the
version the same way, so `gpio.read('native.parquet').write('out.parquet')`
writes true 2.0 native output just like the CLI.

The "bare native geo types" rule holds for an in-memory Arrow table too: a table
with no `geo` metadata whose geometry field declares a GeoArrow extension type —
whether PyArrow resolved that type or it arrives as an `ARROW:extension:name`
field-metadata key — is the same shape as a native-geo file, and auto mode
writes it as 2.0. A `geo` block that declares a version still wins over both.

=== "CLI"

    ```bash
    # Auto mode: a 2.0 input stays 2.0
    gpio convert input_v2.parquet output.parquet

    # Auto mode: reproject preserves the input version too
    gpio convert reproject input_v2.parquet output.parquet --dst-crs EPSG:3857

    # Explicit version always wins
    gpio convert input_v2.parquet output.parquet --geoparquet-version 1.1
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Auto mode: a 2.0 or native-geo input stays/becomes 2.0
    gpio.read('input_v2.parquet').write('output.parquet')

    # Explicit version always wins
    gpio.read('input_v2.parquet').write(
        'output.parquet', geoparquet_version='1.1'
    )
    ```

**Explicit versions:**

=== "CLI"

    <!-- doctest: needs-ogr, setup="ogr2ogr -f 'ESRI Shapefile' input.shp places.geojson" -->
    ```bash
    # GeoParquet 1.1 with native GeoArrow nested-coordinate encoding
    # (no bbox column; incompatible mixed-geometry columns fall back to WKB)
    gpio convert input.geojson output.parquet --geoparquet-version 1.1-geoarrow

    # GeoParquet 1.0 with WKB encoding
    gpio convert input.shp output.parquet --geoparquet-version 1.0

    # GeoParquet 1.1 with WKB encoding (default)
    gpio convert input.shp output.parquet --geoparquet-version 1.1
    ```

=== "Python"

    <!-- doctest: needs-ogr, setup="ogr2ogr -f 'ESRI Shapefile' input.shp places.geojson" -->
    ```python
    import geoparquet_io as gpio

    # GeoParquet 1.1 with native GeoArrow nested-coordinate encoding
    # Converts geometry from any input (GeoJSON, Shapefile, GeoPackage, CSV, WKB GeoParquet)
    # to native GeoArrow types. No bbox column is added.
    # Columns mixing incompatible geometry types fall back to WKB.
    gpio.convert('input.geojson').write(
        'output.parquet', geoparquet_version='1.1-geoarrow'
    )

    # GeoParquet 1.0 with WKB encoding
    gpio.convert('input.shp').write(
        'output.parquet', geoparquet_version='1.0'
    )
    ```

Available versions:
- `1.0` — GeoParquet 1.0 with WKB encoding
- `1.1` — GeoParquet 1.1 with WKB encoding (default for 1.x and non-GeoParquet inputs)
- `1.1-geoarrow` — GeoParquet 1.1 with native GeoArrow (nested-coordinate) encoding; no bbox
  column; compatible geometry type mixes are promoted (e.g. Polygon + MultiPolygon →
  MultiPolygon); incompatible mixes fall back to WKB
- `2.0` — GeoParquet 2.0 with native Parquet geo types
- `parquet-geo-only` — Native Parquet geo types without GeoParquet metadata

### Verbose Output

Track progress and see detailed information:

<!-- doctest: setup="gpio convert geopackage input.parquet input.gpkg" -->
```bash
gpio convert input.gpkg output.parquet --verbose
```

Shows:
- Geometry column detection
- Dataset bounds calculation
- Bbox column creation
- Hilbert ordering progress
- File size and validation

## Examples

### Basic Shapefile Conversion

=== "CLI"

    <!-- doctest: needs-ogr, setup="ogr2ogr -f 'ESRI Shapefile' buildings.shp places.geojson" -->
    ```bash
    gpio convert buildings.shp buildings.parquet
    ```

    Output:
    ```
    Converting buildings.shp...
    Done in 2.3s
    Output: buildings.parquet (4.2 MB)
    ✓ Output passes GeoParquet validation
    ```

=== "Python"

    <!-- doctest: needs-ogr, setup="ogr2ogr -f 'ESRI Shapefile' buildings.shp places.geojson" -->
    ```python
    import geoparquet_io as gpio

    gpio.convert('buildings.shp').sort_hilbert().write('buildings.parquet')
    ```

### Large Dataset Without Hilbert

=== "CLI"

    <!-- doctest: setup="gpio convert geopackage input.parquet large_dataset.gpkg" -->
    ```bash
    gpio convert large_dataset.gpkg output.parquet --skip-hilbert
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio
    ```

    <!-- doctest: setup="gpio convert geopackage input.parquet large_dataset.gpkg", prelude="import geoparquet_io as gpio" -->
    ```python
    # Python doesn't sort by default, so just skip sort_hilbert()
    gpio.convert('large_dataset.gpkg').write('output.parquet')
    ```

Skips Hilbert ordering for faster processing on large files.

### Custom Compression Settings

```bash
gpio convert roads.geojson roads.parquet \
  --compression ZSTD \
  --compression-level 22 \
  --verbose
```

Maximum ZSTD compression with progress tracking.

### Convert and Inspect

<!-- doctest: needs-ogr, setup="ogr2ogr -f 'ESRI Shapefile' input.shp places.geojson" -->
```bash
# Convert
gpio convert input.shp output.parquet

# Verify
gpio inspect output.parquet

# Validate
gpio check all output.parquet
```

## CSV/TSV Support

Auto-detects geometry columns. WKT columns (wkt, geometry, geom) checked first, then lat/lon pairs (lat/lon, latitude/longitude).

```bash
# Auto-detect WKT or lat/lon
gpio convert points.csv points.parquet
```

<!-- doctest: skip="needs a CSV with a 'geom_wkt' column" -->
```bash
# Explicit columns
gpio convert data.csv out.parquet --wkt-column geom_wkt

gpio convert data.csv out.parquet --lat-column lat --lon-column lng

# Custom delimiter
gpio convert data.txt out.parquet --delimiter "|"
```

### CRS and Validation

Default: WGS84 (EPSG:4326). Override with `--crs` for WKT data:

<!-- doctest: skip="needs projected.csv, which the harness does not seed" -->
```bash
gpio convert projected.csv out.parquet --crs EPSG:3857
```

Validates lat/lon ranges (-90 to 90, -180 to 180). Warns on large coordinates suggesting projected CRS.

### Invalid Geometries

Fails on invalid WKT by default. Skip the unparsable rows with
`--skip-invalid`:

<!-- doctest: skip="needs messy.csv, which the harness does not seed" -->
```bash
gpio convert messy.csv out.parquet --skip-invalid
```

A row whose geometry column is *empty* is not invalid — it is a row without
geometry. Those rows are kept either way, with NULL geometry, so their
attributes survive the conversion; they sort after the ordered rows, as they do
for every other input format.

Skips invalid rows, disables Hilbert ordering. Mixed geometry types supported.

### Delimiters

Auto-detects comma and tab. Override with `--delimiter` for semicolon, pipe, or any single character.

<!-- doctest: skip="needs a CSV with the quirks the example describes" -->
```bash
gpio convert data.csv out.parquet --delimiter ";"
```

## Performance

The convert command uses DuckDB's spatial extension - the fastest option for GeoParquet conversion, especially for large files.

**Benchmarks on representative datasets:**

| Dataset | Size | Features | DuckDB | PyOGRIO | ogr2ogr | Fiona |
|---------|------|----------|--------|---------|---------|-------|
| GAUL L2 Shapefile | 739 MB | 45k | **4.6s** | 5.9s | 4.1s | 187s |
| Argentina Roads | 1.1 GB | 3.5M | **30s** | 66s | 117s | 349s |

DuckDB also uses significantly less memory than alternatives (near-zero vs 600MB-2GB for GeoPandas).

To run your own benchmarks:

```bash
gpio benchmark compare input.geojson --iterations 3
```

See [`gpio benchmark`](../cli/benchmark.md) for details.

## See Also

- [CLI Reference: convert](../cli/convert.md)
- [benchmark command](../cli/benchmark.md) - Compare conversion performance
- [add command](add.md) - Add indices to existing GeoParquet
- [sort command](sort.md) - Sort existing GeoParquet spatially
- [check command](check.md) - Validate and fix GeoParquet files
