# GeoJSON Conversion

gpio can convert GeoParquet files to GeoJSON format, with a focus on streaming output for vector tile generation workflows.

## Overview

The `gpio convert geojson` command supports two modes:

1. **Streaming Mode** (default): Outputs newline-delimited GeoJSON (GeoJSONSeq) to stdout, designed for piping to tools like tippecanoe
2. **File Mode**: Writes a standard GeoJSON FeatureCollection to a file

## Streaming to tippecanoe for PMTiles

The primary use case is generating PMTiles or MBTiles from GeoParquet data by piping to [tippecanoe](https://github.com/felt/tippecanoe):

<!-- doctest: needs-tippecanoe -->
```bash
# Basic PMTiles generation
gpio convert geojson buildings.parquet | tippecanoe -P -o buildings.pmtiles

# With layer name
gpio convert geojson roads.parquet | tippecanoe -P -l roads -o roads.pmtiles

# Generate MBTiles instead
gpio convert geojson data.parquet | tippecanoe -P -o tiles.mbtiles
```

### Why Streaming Works with tippecanoe

The streaming output includes RFC 8142 record separators by default. These special characters (`\x1e`) enable tippecanoe's **parallel mode** (`-P` flag), which significantly speeds up tile generation by allowing tippecanoe to process features in parallel.

<!-- doctest: needs-tippecanoe -->
```bash
# The -P flag tells tippecanoe to read in parallel mode
gpio convert geojson data.parquet | tippecanoe -P -o output.pmtiles
```

If you're piping to a tool that doesn't support RFC 8142, disable the separators:

<!-- doctest: skip="pipes into a placeholder tool that stands in for the reader's own" -->
```bash
gpio convert geojson data.parquet --no-rs | some-other-tool
```

### Using gpio pmtiles (Built-in)

For a simpler PMTiles workflow, use the built-in `gpio pmtiles` command. It provides integrated PMTiles generation with production-quality defaults and built-in CRS handling.

!!! note "Requires tippecanoe"
    Install tippecanoe first: `brew install tippecanoe` (macOS) or `sudo apt install tippecanoe` (Ubuntu)

=== "CLI"

    <!-- doctest: needs-tippecanoe, menu -->
    ```bash
    # Basic usage
    gpio pmtiles create buildings.parquet buildings.pmtiles

    # With CRS override (for incorrect metadata)
    gpio pmtiles create data.parquet tiles.pmtiles --src-crs EPSG:3857

    # Add layer metadata to the output PMTiles based on the values of column 'owner'
    gpio pmtiles create --layer-by-column owner data.parquet tiles.pmtiles
    ```

    <!-- doctest: skip="filters on 'population', a column the sample data does not have" -->
    ```bash
    # With filtering (no manual piping needed)
    gpio pmtiles create data.parquet tiles.pmtiles \
      --bbox "-122.5,37.5,-122.0,38.0" \
      --where "population > 10000" \
      --include-cols name,type,height
    ```

=== "Python"

    <!-- doctest: skip="filters on 'population', a column the sample data does not have" -->
    ```python
    from geoparquet_io.api import ops

    # Basic usage
    ops.create_pmtiles(
        input_path="buildings.parquet",
        output_path="buildings.pmtiles"
    )

    # With filtering (no manual piping needed)
    ops.create_pmtiles(
        input_path="data.parquet",
        output_path="tiles.pmtiles",
        bbox="-122.5,37.5,-122.0,38.0",
        where="population > 10000",
        include_cols="name,type,height"
    )

    # With CRS override (for incorrect metadata)
    ops.create_pmtiles(
        input_path="data.parquet",
        output_path="tiles.pmtiles",
        src_crs="EPSG:3857"
    )

    # Add layer metadata to the output PMTiles based on the values of column 'owner'
    ops.create_pmtiles(
        input_path="data.parquet",
        output_path="tiles.pmtiles",
        layer_by_column="owner"
    )
    ```

The command handles the entire pipeline internally (reprojection → filtering → conversion → tippecanoe) with optimal settings.

#### Tuning tile generation (dense data)

By default `gpio pmtiles create` runs tippecanoe with `--no-tile-size-limit` and `--drop-densest-as-needed`. This is a max-fidelity setting: nothing is dropped, so tiles can grow without bound. `--drop-densest-as-needed` only drops features to bring a tile back **under the size limit**, so while the limit is off it never fires.

For national- or global-overview maps over dense data, re-enable the size limit so feature dropping actually happens. Each flag is individually toggleable; the defaults reproduce the historical behavior.

!!! warning "`--no-tile-size-limit` defeats `--drop-densest-as-needed`"
    These two ship on by default and interact: with no size limit, there is nothing for drop-densest to drop against. Pass `--tile-size-limit` (or set an explicit `--maximum-tile-bytes`) to make dropping take effect.

=== "CLI"

    <!-- doctest: needs-tippecanoe, menu -->
    ```bash
    # Re-enable tippecanoe's size limit so drop-densest actually drops
    gpio pmtiles create dense.parquet tiles.pmtiles \
      --tile-size-limit \
      --max-zoom 14

    # Or set an explicit per-tile byte cap (takes precedence over
    # --no-tile-size-limit)
    gpio pmtiles create dense.parquet tiles.pmtiles \
      --maximum-tile-bytes 500000

    # Disable individual production-quality flags
    gpio pmtiles create data.parquet tiles.pmtiles \
      --no-simplify-only-low-zooms \
      --no-drop-densest-as-needed

    # Overwrite an existing output file
    gpio pmtiles create data.parquet tiles.pmtiles --force
    ```

=== "Python"

    <!-- doctest: skip="the lines are alternatives that write the same output file" -->
    ```python
    from geoparquet_io.api import ops

    # Re-enable tippecanoe's size limit so drop-densest actually drops
    ops.create_pmtiles(
        input_path="dense.parquet",
        output_path="tiles.pmtiles",
        no_tile_size_limit=False,
        max_zoom=14,
    )

    # Or set an explicit per-tile byte cap (takes precedence over
    # no_tile_size_limit)
    ops.create_pmtiles(
        input_path="dense.parquet",
        output_path="tiles.pmtiles",
        maximum_tile_bytes=500000,
    )
    ```

#### Scratch space (`--temporary-directory`)

tippecanoe sorts the whole input on disk before it writes a tile, and that
scratch is **several times the input, not the output**. Tiling 168M polygons at
`-Z 12 -z 14` (~12 GB of GeoParquet in, ~34 GB of PMTiles out) accumulated
**~270 GB** of scratch. Budget for it.

By default it goes to the OS temp directory (`$TMPDIR`, or `/tmp` when that is
unset) — on macOS the boot volume, which is usually not where your data lives.
Point the run at an existing directory with room; tippecanoe's scratch and the
temp files of the `gpio` processes feeding it all follow:

=== "CLI"

    <!-- doctest: needs-tippecanoe, setup="mkdir -p scratch", setup="gpio process aggregate h3 input.parquet cells.parquet --resolution 5" -->
    ```bash
    gpio pmtiles create buildings.parquet out.pmtiles --temporary-directory scratch

    # Same flag on pyramid, where it also parents the intermediate band archives
    gpio pmtiles pyramid cells.parquet pyramid.pmtiles -t scratch
    ```

=== "Python"

    <!-- doctest: needs-tippecanoe, setup="mkdir -p scratch", setup="gpio process aggregate h3 input.parquet cells.parquet --resolution 5" -->
    ```python
    from geoparquet_io.api import ops

    ops.create_pmtiles('buildings.parquet', 'out.pmtiles', temporary_directory='scratch')
    ops.create_pmtiles_pyramid(
        'cells.parquet', 'pyramid.pmtiles', temporary_directory='scratch'
    )
    ```

Without the flag, `TMPDIR` is the knob — the same one every other gpio command
follows, described under [Where Spilled Data Goes](write-strategies.md#where-spilled-data-goes).
tippecanoe does not read that variable itself; gpio passes the resolved
directory to it.

!!! warning "A full scratch volume is hard to diagnose"

    tippecanoe creates these files with `mkstemp` + `unlink`, so they have no
    directory entry: they are invisible to `ls` and `du`, and `rm` cannot
    reclaim them. From outside, the disk simply fills with nothing visible
    consuming it, and only killing the process releases the space.

    `tile-join`, used by `gpio pmtiles pyramid`, accepts no equivalent flag
    upstream, so its own spill stays wherever it puts it.

## PMTiles Pyramids

A fine-grained aggregate (say A5 resolution 10 over billions of buildings) renders beautifully zoomed in but overflows tile limits at low zooms, forcing tippecanoe to drop the densest cells — exactly the hotspots you care about. `gpio pmtiles pyramid` fixes this by building a **banded archive**: coarser aggregate levels serve low zooms, finer levels take over as you zoom in, and (optionally) the raw features appear at the highest zooms.

Levels come from [`gpio process overview`](process-overview.md) — existing `_r*` siblings are reused, missing ones are built automatically — and each level is pinned to the zoom band where its worst tile fits `--max-tile-kb` (default 500 KB). One tippecanoe run per band, merged with `tile-join` (ships with tippecanoe), with the bands recorded in the archive metadata under `gpio:pyramid`.

=== "CLI"

    <!-- doctest: skip="needs cells.parquet, which the harness does not seed" -->
    ```bash
    # Auto bands from the tile budget (builds temp overviews if missing)
    gpio pmtiles pyramid cells.parquet cells.pmtiles

    # Explicit overview levels and a capped base band
    gpio pmtiles pyramid cells.parquet out.pmtiles --levels 5 --max-zoom 10

    # Country cells -> region cells -> raw polygons in one archive
    gpio pmtiles pyramid by_region.parquet out.pmtiles \
        --include-features --features-source buildings.parquet --max-zoom 8
    ```

=== "Python"

    <!-- doctest: needs-tippecanoe, setup="gpio process aggregate h3 input.parquet cells.parquet --resolution 5" -->
    ```python
    from geoparquet_io.api import ops

    ops.create_pmtiles_pyramid('cells.parquet', 'cells.pmtiles')

    ops.create_pmtiles_pyramid(
        'cells.parquet',
        'pyramid.pmtiles',
        include_features=True,
        features_source='buildings.parquet',
        max_zoom=8,
    )
    ```

### Layer Modes

`--layer-mode` controls how bands map to vector tile layers, which drives how you style them in MapLibre:

| Mode | Layers | MapLibre styling |
|------|--------|------------------|
| `single` | one layer named after the output file | One `source-layer` for everything. Bands never overlap zooms, so a single style layer renders seamlessly across the whole zoom range. |
| `grouped` *(default)* | `aggregate` + `features` | Two style layers: one for `source-layer: "aggregate"` (all cell levels share the schema, so one `fill-color` ramp on `count` just works), one for `source-layer: "features"` (raw attributes differ). |
| `per-level` | `r5`, `r10`, … (or `country`, `region`) + `features` | One style layer per `source-layer` — style each resolution independently, or toggle levels client-side for comparison. |

For example, with the default `grouped` mode:

```js
map.addLayer({
  id: 'cells',
  type: 'fill',
  source: 'pyramid',            // your pmtiles source
  'source-layer': 'aggregate',  // every aggregate band lands here
  paint: {
    'fill-color': ['interpolate', ['linear'], ['get', 'count'],
                   1, '#ffffcc', 10000, '#800026'],
  },
});
```

Because each zoom is served by exactly one band, no zoom-range filtering is needed in the style. The `gpio:pyramid` metadata key lists every band (`level`, `layer`, `minzoom`, `maxzoom`) if a client wants to introspect the archive.

## Common Workflows

### Filter Before Converting

Use `gpio extract` to filter data before conversion to reduce output size:

<!-- doctest: needs-tippecanoe -->
```bash
# Limit rows for testing
gpio extract data.parquet --limit 1000 | \
  gpio convert geojson - | \
  tippecanoe -P -o sample.pmtiles
```

<!-- doctest: skip="filters on 'population', a column the sample data does not have" -->
```bash
# Filter by bounding box
gpio extract data.parquet --bbox "-122.5,37.5,-122,38" | \
  gpio convert geojson - | \
  tippecanoe -P -o sf.pmtiles

# Filter by column values
gpio extract data.parquet --where "population > 10000" | \
  gpio convert geojson - | \
  tippecanoe -P -o cities.pmtiles
```

```bash
# End the pipeline in a GeoJSON file instead of a pipe
gpio extract data.parquet --limit 1000 | \
  gpio convert geojson - sample.geojson
```

Reading from stdin with `-` works in both modes: pipe onward without an output
file to get newline-delimited GeoJSON, or name an output file to get a
`FeatureCollection` written there.

### Select Specific Columns

Reduce output size by selecting only needed columns:

<!-- doctest: needs-tippecanoe -->
```bash
gpio extract data.parquet --include-cols name,type,population | \
  gpio convert geojson - | \
  tippecanoe -P -o output.pmtiles
```

### Transform Before Converting

Apply spatial operations before conversion:

<!-- doctest: needs-tippecanoe, menu -->
```bash
# Add bbox and sort, then convert
gpio add bbox data.parquet | \
  gpio sort hilbert - | \
  gpio convert geojson - | \
  tippecanoe -P -o output.pmtiles

# Reproject before converting
gpio convert reproject data.parquet - --dst-crs EPSG:4326 | \
  gpio convert geojson - | \
  tippecanoe -P -o output.pmtiles
```

**Tip:** The built-in `gpio pmtiles` command handles reprojection automatically:

=== "CLI"

    <!-- doctest: needs-tippecanoe -->
    ```bash
    # Automatically reproject from EPSG:3857 to WGS84
    gpio pmtiles create data.parquet tiles.pmtiles --src-crs EPSG:3857
    ```

=== "Python"

    <!-- doctest: needs-tippecanoe -->
    ```python
    from geoparquet_io.api import ops

    # Automatically reproject from EPSG:3857 to WGS84
    ops.create_pmtiles(
        input_path="data.parquet",
        output_path="tiles.pmtiles",
        src_crs="EPSG:3857"
    )
    ```

## Writing to File

To write a standard GeoJSON FeatureCollection, specify an output file:

=== "CLI"

    ```bash
    # Write to GeoJSON file
    gpio convert geojson data.parquet output.geojson
    ```

    ```bash
    # With options
    gpio convert geojson data.parquet output.geojson --precision 5 --write-bbox
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Write to GeoJSON file
    gpio.read('data.parquet').to_geojson('output.geojson')

    # With options
    gpio.read('data.parquet').to_geojson(
        'output.geojson',
        precision=5,
        write_bbox=True
    )

    # Get as string (no file output)
    geojson_str = gpio.read('data.parquet').to_geojson()
    ```

File output is generated directly from query results as RFC 7946-compliant GeoJSON; no GDAL is involved.

## Options Reference

| Option | Default | Description |
|--------|---------|-------------|
| `--no-rs` | false | Disable RFC 8142 record separators (streaming only) |
| `--precision N` | 7 | Coordinate decimal precision (RFC 7946 recommends 7) |
| `--write-bbox` | false | Include bbox property for each feature |
| `--id-field COLUMN` | none | Use this column as the GeoJSON feature `id` |
| `--description TEXT` | none | Add a description to the FeatureCollection |
| `--feature-collection` | false | Output a FeatureCollection instead of GeoJSONSeq (streaming only) |
| `--pretty` | false | Pretty-print the JSON output with indentation |
| `--keep-crs` | false | Keep the original CRS instead of reprojecting to WGS84 |
| `--no-repair-geometry` | false | Preserve invalid geometry instead of repairing it with `ST_MakeValid` |
| `--verbose` | false | Show debug output |
| `--aws-profile NAME` | none | AWS profile for S3 files |

### Coordinate Precision

The `--precision` option controls decimal places for coordinates. Lower precision reduces output size but decreases accuracy:

| Precision | Accuracy | Use Case |
|-----------|----------|----------|
| 7 (default) | ~1cm | High accuracy, RFC 7946 default |
| 6 | ~10cm | Most mapping applications |
| 5 | ~1m | City-level visualization |
| 4 | ~10m | Regional maps |

<!-- doctest: needs-tippecanoe -->
```bash
# Reduce precision for smaller output
gpio convert geojson data.parquet --precision 5 | tippecanoe -P -o output.pmtiles
```

### Feature ID Field

Use `--id-field` to specify which column should become the GeoJSON feature `id`:

<!-- doctest: skip="uses attribute columns the sample dataset does not carry" -->
```bash
gpio convert geojson buildings.parquet --id-field osm_id | tippecanoe -P -o output.pmtiles
```

This is useful for feature state in map rendering or for joining data.

### Bounding Box

Include per-feature bounding boxes with `--write-bbox`:

```bash
gpio convert geojson data.parquet output.geojson --write-bbox
```

### Description

Add a description to the FeatureCollection:

```bash
gpio convert geojson data.parquet output.geojson --description "My dataset"
```

### Pretty Print

For human-readable output with indentation:

```bash
gpio convert geojson data.parquet output.geojson --pretty
```

### FeatureCollection Mode (Streaming)

By default, streaming outputs newline-delimited GeoJSONSeq. To output a complete FeatureCollection instead:

```bash
gpio convert geojson data.parquet --feature-collection > output.geojson
```

### No GDAL Layer Creation Options

`gpio convert geojson` does not use GDAL, so there are no layer creation
options to pass: the writer generates GeoJSON directly from query results. The
settings that matter for GeoJSON output are available as dedicated flags
(`--precision`, `--write-bbox`, `--id-field`, `--feature-collection`,
`--pretty`). If you need a GDAL driver option that has no dedicated flag, run
`ogr2ogr` on the output file.

## Performance Tips

1. **Filter first**: Use `gpio extract` to reduce row count before conversion
2. **Select columns**: Only include columns needed for visualization
3. **Lower precision**: Use `--precision 5` or `--precision 6` for smaller output
4. **Pipeline processing**: Chain commands to avoid intermediate files

### Large File Example

<!-- doctest: skip="pipes into tippecanoe and filters on a column the sample data lacks" -->
```bash
# Efficient pipeline for large files
gpio extract large.parquet \
  --bbox "-122.5,37.5,-122,38" \
  --include-cols name,type,height | \
  gpio convert geojson - --precision 5 | \
  tippecanoe -P -z14 -o sf_buildings.pmtiles
```

## Remote Files

Read from S3, GCS, or Azure:

<!-- doctest: skip="needs cloud credentials" -->
```bash
# From S3 with profile
gpio convert geojson s3://bucket/data.parquet --aws-profile my-aws | tippecanoe -P -o output.pmtiles

# From public URL
gpio convert geojson https://example.com/data.parquet | tippecanoe -P -o output.pmtiles
```

## See Also

- [tippecanoe documentation](https://github.com/felt/tippecanoe)
- [PMTiles specification](https://github.com/protomaps/PMTiles)
- [Command Piping](piping.md) - More on gpio piping
- [Extract Guide](extract.md) - Filtering before conversion
- [Convert Guide](convert.md) - Other conversion options
