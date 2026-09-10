# Write Strategies for Large Files

gpio handles larger-than-memory GeoParquet files efficiently through a pluggable write strategy system. The default strategy streams data directly to disk with constant memory usage, allowing you to process files of any size.

## The Default: DuckDB Streaming

gpio uses the `duckdb-kv` strategy by default. This strategy:

- Uses **O(1) constant memory** regardless of file size
- Streams data through DuckDB's native COPY TO command
- Embeds GeoParquet metadata directly in the Parquet footer
- Handles files of any size without running out of memory
- Is **container-aware**, automatically detecting Docker/Kubernetes memory limits

For most users, the default just works. No configuration is needed:

=== "CLI"

    ```bash
    # Process a 50GB file on a machine with 4GB RAM
    gpio extract huge_dataset.parquet filtered.parquet --bbox -122.5,37.5,-122.0,38.0
    ```

    <!-- doctest: skip="needs large_file.shp, which the harness does not seed" -->
    ```bash
    # Convert a massive shapefile to GeoParquet
    gpio convert large_file.shp output.parquet
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Process large files with the fluent API
    gpio.read('huge_dataset.parquet') \
        .extract(bbox=(-122.5, 37.5, -122.0, 38.0)) \
        .write('filtered.parquet')
    ```

## Strategy Comparison

| Strategy | Memory Usage | Speed | Best For |
|----------|--------------|-------|----------|
| `duckdb-kv` | O(1) constant | Fastest | **Default for all use cases** |
| `streaming` | O(batch) constant | Moderate | Alternative when duckdb-kv has issues |
| `disk-rewrite` | O(rowgroup) | Slowest | Maximum compatibility fallback |
| `in-memory` | O(n) proportional | Fast | Legacy/verification mode |

### Strategy Details

**duckdb-kv** (Default)

The recommended strategy for all production workloads. Uses DuckDB's native COPY TO with KV_METADATA option for a single atomic write operation with no post-processing.

**streaming**

Uses PyArrow's streaming writer to process data in batches. Good alternative if you encounter issues with the DuckDB strategy. Memory usage is proportional to batch size, not file size.

**disk-rewrite**

Writes data with DuckDB, then rewrites row-group by row-group using PyArrow to add proper GeoParquet metadata. Uses more memory than streaming but provides maximum compatibility.

**in-memory**

Loads the entire dataset into memory before writing. Only use this for small files or when you need to verify that another strategy is producing correct output.

## Selecting a Strategy

Override the default strategy when needed:

=== "CLI"

    <!-- doctest: menu -->
    ```bash
    # Use streaming strategy
    gpio extract input.parquet output.parquet --write-strategy streaming

    # Use in-memory for verification
    gpio extract input.parquet output.parquet --write-strategy in-memory
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Use streaming strategy
    gpio.read('input.parquet').write('output.parquet', write_strategy='streaming')

    # Use in-memory for verification
    gpio.read('input.parquet').write('output.parquet', write_strategy='in-memory')
    ```

## When to Use Alternative Strategies

### Decision Flowchart

1. **Start with the default** (`duckdb-kv`) - it handles any file size efficiently
2. **Output seems wrong?** Try `in-memory` to verify correct behavior
3. **`in-memory` works but `duckdb-kv` doesn't?** Report a bug, use `streaming` as workaround
4. **Need maximum compatibility?** Try `disk-rewrite`

### Specific Scenarios

| Scenario | Recommended Strategy |
|----------|---------------------|
| Large file, limited memory | `duckdb-kv` (default) |
| Debugging output differences | `in-memory` to verify |
| DuckDB issues with specific data | `streaming` |
| Older tools can't read output | `disk-rewrite` |

## Memory Configuration

### Where Spilled Data Goes

A memory limit only helps if DuckDB has somewhere to put the data that does not
fit. gpio gives every DuckDB connection its own scratch directory under the
system temp directory — `$TMPDIR` on macOS and Linux, `%TEMP%` on Windows — and
DuckDB removes it again when the query finishes. A run that never exceeds its
memory limit writes nothing there.

This matters in three situations:

- **A read-only working directory.** DuckDB's own default spill location is the
  relative path `.tmp`, so out of the box a large sort fails with
  `IO Error: Failed to create directory ".tmp"` even when the input and output
  volumes are perfectly writable. gpio never spills into the working directory.
- **A small root volume.** A sort of a very large file can spill more bytes than
  the output itself. If `/tmp` is too small, point `TMPDIR` at a volume that is
  not:

    <!-- doctest: skip="names /mnt/scratch and huge.parquet, neither of which the harness seeds" -->
    ```bash
    TMPDIR=/mnt/scratch gpio sort hilbert huge.parquet sorted.parquet
    ```

- **A RAM-backed `/tmp`.** systemd mounts `/tmp` as a tmpfs sized at half of RAM
  by default on Fedora, Arch and openSUSE, and so do Kubernetes'
  `emptyDir: {medium: Memory}` and `docker run --tmpfs /tmp`. Spilling onto a
  tmpfs spends the very memory the spill was meant to save, so on those systems
  `TMPDIR` should name real storage — `df -h /tmp` and `findmnt /tmp` say which
  you have.

!!! warning "\"Out of Memory Error\" can mean out of *disk*"
    DuckDB caps its own spill at `max_temp_directory_size` (90% of the spill
    volume by default), so a small or RAM-backed temp volume fails cleanly
    rather than filling the disk. The message it raises, though, says
    `Out of Memory Error: failed to offload data block of size 256.0 KiB` and
    suggests only memory-side remedies. gpio appends a line naming `TMPDIR`
    whenever DuckDB's error mentions `max_temp_directory_size`, because that is
    the knob that actually fixes it.

!!! warning "Do not point two runs at one spill directory"
    DuckDB names its spill files after the block size alone
    (`duckdb_temp_storage_S192K-0.tmp`), with nothing in the name identifying
    the connection or the process. Two DuckDB connections sharing one temp
    directory therefore overwrite each other's blocks, and the loser fails with
    `IO Error: Could not read enough bytes from file`. `TMPDIR` is safe because
    gpio still gives each connection a private subdirectory beneath it; a fixed
    `temp_directory` passed straight to DuckDB is not.

### Cleaning Up After an Interrupted Run

DuckDB removes its scratch directory when the query finishes — including when it
fails. A run **killed** mid-spill (Ctrl-C, `SIGTERM`, the OOM killer) is the
exception: the process is gone before that cleanup can run, and a directory
holding gigabytes stays on disk.

gpio names those directories `gpio-spill-<pid>-<random>` and sweeps them at the
start of every run: any leftover whose owning process has exited is removed
before a new one is created, so interrupted runs do not accumulate. A directory
belonging to a process that is still running is never touched. (Pids are the
test, so give each container its own `TMPDIR` rather than bind-mounting one
spill volume into several — across pid namespaces the test cannot tell a live
run from a dead one.)

The one place an OS temp reaper would never help is the admin cache, because the
admin-boundary commands spill onto the cache volume rather than into `/tmp`.
`--clear-cache` prices and deletes those leftovers too:

<!-- doctest: skip="deletes the user's real admin cache" -->
```bash
gpio add admin-divisions in.parquet out.parquet --clear-cache
```

```
Cache directory: /home/you/.geoparquet-io/cache/admin
Files to delete: 1
Total size: 3.00 MB
Leftover spill directories: 1 (250.00 MB)
```

### Automatic Detection

gpio automatically detects available memory and configures DuckDB to use 50% of it. This detection is container-aware:

- **cgroup v2** (modern Docker, Kubernetes): Reads `/sys/fs/cgroup/memory.max`
- **cgroup v1** (older Docker): Reads `/sys/fs/cgroup/memory/memory.limit_in_bytes`
- **Bare metal**: Falls back to psutil for system memory detection

### Explicit Memory Limits

Override auto-detection when needed:

=== "CLI"

    <!-- doctest: menu -->
    ```bash
    # Limit DuckDB to 2GB for streaming writes
    gpio extract input.parquet output.parquet --write-memory 2GB

    # Smaller limit for restricted environments
    gpio extract input.parquet output.parquet --write-memory 512MB
    ```

    !!! warning "On file-writing commands, `--write-memory` requires `duckdb-kv`"
        On commands that write through a DuckDB write engine, only `duckdb-kv`
        (the default) has one to configure.
        Combining `--write-memory` with an explicitly chosen `--write-strategy`
        of `streaming`, `in-memory`, or `disk-rewrite` is rejected as a
        parameter error. `--geoparquet-version 1.1-geoarrow` internally reroutes
        WKB input to the arrow-streaming strategy; there gpio warns and ignores
        `--write-memory` rather than failing. Streaming Arrow IPC to stdout
        (`-`) also has no write engine to configure, so the limit is ignored
        with a warning — `--write-memory` only applies to file outputs.

    !!! note "On `gpio extract bigquery`, `--write-memory` governs the scan"
        That command writes through PyArrow and has no `--write-strategy` flag,
        so there is no DuckDB write engine to configure. The value is validated
        and applied as `memory_limit` on the DuckDB connection running the
        BigQuery scan — it is honored, not dropped, but it governs the read side
        only.

        It is **not** a cap on the command's peak memory. The whole result set
        is materialized as an Arrow table before the write, and that copy is
        allocated by PyArrow, outside DuckDB's `memory_limit` accounting — so
        the scan spills but the Arrow table it produces does not. Size a
        BigQuery extract against the result set, not against `--write-memory`.

        The flag keeps its name here — it is the same knob, spelled the same way
        — but `gpio extract bigquery --help` describes it accurately: "Memory
        limit for the DuckDB scan of the BigQuery result."

=== "Python"

    The fluent API auto-detects DuckDB's memory limit (respecting container
    cgroup limits). Explicit override is currently CLI-only via
    `--write-memory`; from Python you select the write strategy:

    ```python
    import geoparquet_io as gpio

    # Streaming strategy for constant memory usage
    gpio.read('input.parquet').write('output.parquet', write_strategy='streaming')
    ```

### Memory Sizing Guidelines

| Environment | Recommended `--write-memory` |
|-------------|------------------------------|
| Laptop (8GB RAM) | `2GB` - `4GB` |
| Workstation (32GB RAM) | `8GB` - `16GB` (default auto-detects) |
| Docker container | Auto-detected from cgroup limits |
| Kubernetes pod | Auto-detected from cgroup limits |
| AWS Lambda (1GB) | `384MB` |
| Cloud Run (2GB) | `768MB` |

!!! tip "Container Environments"
    gpio automatically respects container memory limits. If you're running in Docker or Kubernetes with memory limits set, you typically don't need to specify `--write-memory` manually.

## Container Environments

### Docker

gpio detects Docker memory limits automatically via cgroups. No extra configuration needed:

<!-- doctest: skip="requires Docker" -->
```bash
# Docker automatically limits memory, gpio respects it
docker run -m 2g my-gpio-image gpio extract input.parquet output.parquet
```

If you need explicit control:

<!-- doctest: skip="requires Docker" -->
```bash
docker run -m 2g my-gpio-image gpio extract input.parquet output.parquet --write-memory 1GB
```

### Kubernetes

Memory limits from pod specifications are detected via cgroups:

```yaml
resources:
  limits:
    memory: "4Gi"
```

gpio will automatically use approximately 2GB (50% of the limit) for DuckDB operations.

### Serverless (AWS Lambda, Cloud Run)

For serverless environments with tight memory constraints:

=== "CLI"

    <!-- doctest: menu -->
    ```bash
    # AWS Lambda with 1GB memory
    gpio extract input.parquet output.parquet --write-memory 384MB

    # Cloud Run with 2GB memory
    gpio extract input.parquet output.parquet --write-memory 768MB
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    def handler(event, context):
        # Memory limit is auto-detected from the Lambda environment
        gpio.read('s3://bucket/input.parquet') \
            .extract(bbox=event['bbox']) \
            .write('/tmp/output.parquet')
    ```

## Examples

### Process a Large Dataset

=== "CLI"

    <!-- doctest: skip="needs large_dataset.parquet, which the harness does not seed" -->
    ```bash
    # 100GB dataset on a 16GB machine - just works
    gpio extract large_dataset.parquet filtered.parquet \
        --bbox -122.5,37.5,-122.0,38.0 \
        --where "population > 1000"
    ```

=== "Python"

    <!-- doctest: skip="needs large_dataset.parquet, which the harness does not seed" -->
    ```python
    import geoparquet_io as gpio

    # Large file processing with the fluent API
    gpio.read('large_dataset.parquet') \
        .extract(
            bbox=(-122.5, 37.5, -122.0, 38.0),
            where="population > 1000"
        ) \
        .write('filtered.parquet')
    ```

### Troubleshoot Output Issues

If you suspect the default strategy is producing incorrect output:

=== "CLI"

    <!-- doctest: setup="gpio extract input.parquet output.parquet" -->
    ```bash
    # 1. Write with in-memory strategy (loads full dataset)
    gpio extract input.parquet test_inmemory.parquet --write-strategy in-memory

    # 2. Compare with default strategy output
    gpio inspect stats test_inmemory.parquet
    gpio inspect stats output.parquet
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Verify with in-memory strategy
    table = gpio.read('input.parquet')

    # Write with default
    table.write('output_default.parquet')

    # Write with in-memory for comparison
    table.write('output_inmemory.parquet', write_strategy='in-memory')

    # Compare
    default = gpio.read('output_default.parquet')
    inmemory = gpio.read('output_inmemory.parquet')
    print(f"Default rows: {default.num_rows}, In-memory rows: {inmemory.num_rows}")
    ```

### Batch Processing in Constrained Environment

=== "CLI"

    <!-- doctest: skip="reads a directory of partitioned files the reader produces" -->
    ```bash
    # Process multiple files with limited memory
    for f in data/*.parquet; do
        gpio extract "$f" "output/$(basename $f)" \
            --write-memory 512MB \
            --bbox -122.5,37.5,-122.0,38.0
    done
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio
    from pathlib import Path

    # Batch processing (memory limit auto-detected per environment)
    for input_file in Path('data').glob('*.parquet'):
        gpio.read(input_file) \
            .extract(bbox=(-122.5, 37.5, -122.0, 38.0)) \
            .write(f'output/{input_file.name}', write_strategy='streaming')
    ```

## See Also

- [Extracting Data](extract.md) - Full extract command documentation
- [Best Practices](../concepts/best-practices.md) - GeoParquet optimization tips
- [Troubleshooting](../troubleshooting.md) - Common issues and solutions
