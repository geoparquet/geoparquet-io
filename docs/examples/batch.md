# Batch Processing Examples

Process multiple GeoParquet files efficiently.

## Sequential Processing

```python
from pathlib import Path
from geoparquet_io.core.add_bbox_column import add_bbox_column

def process_directory(input_dir, output_dir):
    """Process all parquet files in a directory."""
    import os
    os.makedirs(output_dir, exist_ok=True)

    for input_file in Path(input_dir).glob("*.parquet"):
        output_file = Path(output_dir) / input_file.name

        add_bbox_column(
            input_parquet=str(input_file),
            output_parquet=str(output_file),
            bbox_name="bbox",
            verbose=False,
            compression="ZSTD",
            compression_level=15
        )

        print(f"✓ Processed {input_file.name}")

# Usage
process_directory("input/", "output/")
```

## Parallel Processing

For processing many files on multi-core machines:

```python
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from geoparquet_io.core.hilbert_order import hilbert_order

def process_file(args):
    """Process a single file."""
    input_file, output_dir = args
    output_file = Path(output_dir) / input_file.name

    try:
        hilbert_order(
            input_parquet=str(input_file),
            output_parquet=str(output_file),
            geometry_column="geometry",
            add_bbox=True,
            verbose=False
        )
        return (True, input_file.name, None)
    except Exception as e:
        return (False, input_file.name, str(e))

def parallel_process(input_dir, output_dir):
    """Process files in parallel."""
    import os
    os.makedirs(output_dir, exist_ok=True)

    files = list(Path(input_dir).glob("*.parquet"))
    args_list = [(f, output_dir) for f in files]

    max_workers = os.cpu_count() or 4

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_file, args): args[0]
                  for args in args_list}

        for future in as_completed(futures):
            success, filename, error = future.result()
            if success:
                print(f"✓ {filename}")
            else:
                print(f"❌ {filename}: {error}")

# Usage
parallel_process("input/", "output/")
```

## Progress Tracking

Add progress bars with tqdm:

```python
from pathlib import Path
from tqdm import tqdm
from geoparquet_io.core.add_bbox_column import add_bbox_column

def process_with_progress(input_dir, output_dir):
    """Process files with progress bar."""
    import os
    os.makedirs(output_dir, exist_ok=True)

    files = list(Path(input_dir).glob("*.parquet"))

    for input_file in tqdm(files, desc="Processing files"):
        output_file = Path(output_dir) / input_file.name

        add_bbox_column(
            input_parquet=str(input_file),
            output_parquet=str(output_file),
            verbose=False
        )

# Usage
process_with_progress("input/", "output/")
```

## Complete Example Script

See the [examples/batch_processing.py](https://github.com/cholmes/geoparquet-io/blob/main/examples/batch_processing.py) file in the repository for a complete working example with error handling and file size reporting.

## See Also

- [Basic Usage Examples](basic.md)
- [API Reference](../api/overview.md)
