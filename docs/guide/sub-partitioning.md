# Sub-Partitioning Large Files

After partitioning by administrative boundaries or string columns, some partitions may still be too large for efficient querying. Use `--min-size` — or `ops.sub_partition_by_<index>()` from Python — to automatically sub-partition oversized files.

## Quick Start

First, partition by country — the boundaries dataset is downloaded on first use,
so this step needs the network ([Partitioning Files](partition.md) covers it in
full):

<!-- doctest: network -->
```bash
gpio partition admin input.parquet by_country/ --dataset gaul --levels country
```

Then sub-partition whatever came out too large:

=== "CLI"

    <!-- doctest: setup="gpio partition quadkey input.parquet by_country/ --resolution 6 --partition-resolution 2" -->
    ```bash
    gpio partition h3 by_country/ --min-size 100MB --resolution 7 --in-place
    ```

=== "Python"

    <!-- doctest: setup="gpio partition quadkey input.parquet by_country/ --resolution 6 --partition-resolution 2" -->
    ```python
    from geoparquet_io.api import ops

    result = ops.sub_partition_by_h3(
        'by_country/',
        min_size='100MB',
        resolution=7,
        in_place=True,
    )
    print(f"sub-partitioned {result['processed']} file(s)")
    ```

This finds all parquet files over 100MB in `by_country/` and partitions them by H3 cells, replacing the original files with sub-partition directories.

## How It Works

When you pass a directory to a partition command with `--min-size`:

1. Scans the directory recursively for `.parquet` files
2. Filters to files exceeding the size threshold
3. Partitions each large file into a sibling subdirectory
4. With `--in-place`, removes the original file after success

An original is only removed once the sub-partitions hold every row it had. Rows
with a NULL or empty geometry get a NULL index cell and are dropped by
partitioning, so those files are reported as errors and left alone.

## Result Structure

```
by_country/
├── country=USA/
│   └── USA_h3/           ← Sub-partitioned (was >100MB)
│       ├── 872a1008fffffff.parquet
│       └── ...
├── country=Vatican/
│   └── Vatican.parquet   ← Unchanged (under threshold)
└── country=Monaco/
    └── Monaco.parquet    ← Unchanged (under threshold)
```

## Options

| Option | Python argument | Description |
|--------|-----------------|-------------|
| `--min-size` | `min_size=` | Size threshold (e.g., '100MB', '1GB', or a byte count from Python). Required for directory input. |
| `--in-place` | `in_place=` | Delete original files after successful sub-partitioning |
| `--preview` | `preview=` | List the files that would be processed, then stop |
| `--resolution` / `--level` | `resolution=` / `level=` | Spatial index resolution (or use `--auto`) |
| `--auto` | `auto=` | Auto-calculate optimal resolution |
| `--partition-resolution` | `partition_resolution=` | Quadkey partition prefix length, no greater than `resolution` |

`OUTPUT_FOLDER` and the index column name options (`--h3-name`, `--a5-name`,
`--s2-name`, `--quadkey-column`) apply to single-file runs only: in directory
mode each file gets its own sibling `<file>_<index>/` directory and the default
column name, so passing them is an error rather than a silent no-op. The Python
twins refuse their `output_dir=` and `column_name=` arguments for the same
reason, with the same explanation.

Sub-partitioning is accepted by `gpio partition h3`, `gpio partition a5`,
`gpio partition quadkey` and `gpio partition s2`. All four run, including when
they are reached through `--min-size`.

## Examples

=== "H3"

    <!-- doctest: setup="gpio partition quadkey input.parquet by_country/ --resolution 6 --partition-resolution 2" -->
    ```bash
    gpio partition h3 by_country/ --min-size 100MB --resolution 7 --in-place
    ```

    <!-- doctest: setup="gpio partition quadkey input.parquet by_country/ --resolution 6 --partition-resolution 2" -->
    ```python
    from geoparquet_io.api import ops

    ops.sub_partition_by_h3('by_country/', min_size='100MB', resolution=7, in_place=True)
    ```

=== "A5"

    A tiny threshold is used here so the example actually splits the small sample
    directory; on real data use `100MB` as in the H3 tab.

    <!-- doctest: setup="gpio partition quadkey input.parquet by_country/ --resolution 6 --partition-resolution 2" -->
    ```bash
    gpio partition a5 by_country/ --min-size 1B --resolution 4 --in-place
    ```

    <!-- doctest: setup="gpio partition quadkey input.parquet by_country/ --resolution 6 --partition-resolution 2" -->
    ```python
    from geoparquet_io.api import ops

    result = ops.sub_partition_by_a5(
        'by_country/',
        min_size='1B',
        resolution=4,
        in_place=True,
    )
    print(f"{result['processed']} file(s) sub-partitioned, {len(result['errors'])} failed")
    ```

=== "S2"

    As in the A5 tab, a tiny threshold makes the example split the small sample
    directory; on real data use `100MB` as in the H3 tab.

    <!-- doctest: setup="gpio partition quadkey input.parquet by_country/ --resolution 6 --partition-resolution 2" -->
    ```bash
    gpio partition s2 by_country/ --min-size 1B --level 4 --in-place
    ```

    <!-- doctest: setup="gpio partition quadkey input.parquet by_country/ --resolution 6 --partition-resolution 2" -->
    ```python
    from geoparquet_io.api import ops

    result = ops.sub_partition_by_s2(
        'by_country/',
        min_size='1B',
        level=4,
        in_place=True,
    )
    print(f"{result['processed']} file(s) sub-partitioned, {len(result['errors'])} failed")
    ```

=== "Quadkey"

    Pass both a column resolution and a partition resolution, just as in single-file
    mode. The partition resolution must be between zero and the column resolution
    (at most 23). Alternatively, use `--auto` / `auto=True` to calculate both.

    <!-- doctest: setup="gpio partition quadkey input.parquet by_country/ --resolution 6 --partition-resolution 2" -->
    ```bash
    gpio partition quadkey by_country/ --min-size 1B --resolution 13 --partition-resolution 6 --in-place
    ```

    <!-- doctest: setup="gpio partition quadkey input.parquet by_country/ --resolution 6 --partition-resolution 2" -->
    ```python
    from geoparquet_io.api import ops

    ops.sub_partition_by_quadkey(
        'by_country/', min_size='1B', resolution=13, partition_resolution=6, in_place=True
    )
    ```

## From Python

Sub-partitioning walks a directory of files on disk, so it is not a `Table`
method — a `Table` is one table already in memory. It is a function over a path,
one per index: `ops.sub_partition_by_h3`, `ops.sub_partition_by_a5`,
`ops.sub_partition_by_quadkey` and `ops.sub_partition_by_s2`, each mirroring the
`gpio partition <index> <dir>/ --min-size` command of the same name.

Each returns a dict describing the run: `processed`, `skipped`, `errors`, the
`candidates` the threshold selected, and `preview`.

<!-- doctest: setup="gpio partition quadkey input.parquet by_country/ --resolution 6 --partition-resolution 2" -->
```python
from geoparquet_io.api import ops
from geoparquet_io.core.exceptions import PartitionError

try:
    result = ops.sub_partition_by_a5(
        'by_country/',
        min_size='1B',       # a byte count works too: min_size=1
        resolution=4,
        in_place=True,
    )
except PartitionError as exc:
    # Raised when any file failed; exc.result holds the whole run, including
    # the files that succeeded.
    result = exc.result
    print(f"{len(result['errors'])} file(s) kept their original")

for candidate in result['candidates']:
    print(f"{candidate['path']} -> {candidate['output_dir']}/")
```

An `InvalidParameterError` is raised before anything is written for a path that
is not a directory, an unparsable `min_size`, a missing resolution, or an
argument directory mode cannot honour.

## Preview Mode

`--preview` (`preview=True`) lists the files that exceed the threshold and the
directory each one would be written to, then stops. Nothing is partitioned and
nothing is deleted, even when `--in-place` is also passed.

=== "CLI"

    <!-- doctest: setup="gpio partition quadkey input.parquet by_country/ --resolution 6 --partition-resolution 2" -->
    ```bash
    # See which files would be sub-partitioned, without touching them
    gpio partition h3 by_country/ --min-size 1B --resolution 7 --preview
    ```

=== "Python"

    <!-- doctest: setup="gpio partition quadkey input.parquet by_country/ --resolution 6 --partition-resolution 2" -->
    ```python
    from geoparquet_io.api import ops

    plan = ops.sub_partition_by_h3(
        'by_country/',
        min_size='1B',
        resolution=7,
        preview=True,
    )
    for candidate in plan['candidates']:
        size_mb = candidate['size_bytes'] / (1024 * 1024)
        print(f"{candidate['path']} ({size_mb:.1f}MB) -> {candidate['output_dir']}/")
    ```

Without `--preview` the files are partitioned for real; the originals are kept
unless you pass `--in-place`.

## Size Threshold Examples

| Threshold | Use Case |
|-----------|----------|
| `50MB` | Aggressive splitting for web delivery |
| `100MB` | Balanced (recommended default) |
| `250MB` | Light splitting for local analysis |
| `1GB` | Only split very large files |

## See Also

- [Partitioning Files](partition.md) - All partition command options
- [Command Piping](piping.md) - Chaining commands
