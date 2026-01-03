# Batch Processing Examples

Process multiple GeoParquet files efficiently using the Python API.

## Sequential Processing

```python
from pathlib import Path
import geoparquet_io as gpio

def process_directory(input_dir: str, output_dir: str):
    """Process all parquet files in a directory."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    for input_file in Path(input_dir).glob("*.parquet"):
        output_file = Path(output_dir) / input_file.name

        gpio.read(input_file) \
            .add_bbox() \
            .sort_hilbert() \
            .write(output_file)

        print(f"Processed {input_file.name}")

# Usage
process_directory("input/", "output/")
```

## Parallel Processing

For processing many files on multi-core machines:

```python
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
import geoparquet_io as gpio

def process_file(args: tuple) -> tuple:
    """Process a single file."""
    input_file, output_dir = args
    output_file = Path(output_dir) / input_file.name

    try:
        gpio.read(input_file) \
            .add_bbox() \
            .sort_hilbert() \
            .write(output_file)
        return (True, input_file.name, None)
    except Exception as e:
        return (False, input_file.name, str(e))

def parallel_process(input_dir: str, output_dir: str, max_workers: int = None):
    """Process files in parallel."""
    import os
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    files = list(Path(input_dir).glob("*.parquet"))
    args_list = [(f, output_dir) for f in files]

    if max_workers is None:
        max_workers = os.cpu_count() or 4

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_file, args): args[0]
                   for args in args_list}

        for future in as_completed(futures):
            success, filename, error = future.result()
            if success:
                print(f"Done: {filename}")
            else:
                print(f"Failed: {filename}: {error}")

# Usage
parallel_process("input/", "output/")
```

## Progress Tracking with tqdm

Add progress bars for better visibility:

```python
from pathlib import Path
from tqdm import tqdm
import geoparquet_io as gpio

def process_with_progress(input_dir: str, output_dir: str):
    """Process files with progress bar."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    files = list(Path(input_dir).glob("*.parquet"))

    for input_file in tqdm(files, desc="Processing files"):
        output_file = Path(output_dir) / input_file.name

        gpio.read(input_file) \
            .add_bbox() \
            .sort_hilbert() \
            .write(output_file)

# Usage
process_with_progress("input/", "output/")
```

## Batch Conversion from Other Formats

Convert multiple Shapefiles or GeoPackages:

```python
from pathlib import Path
import geoparquet_io as gpio

def convert_directory(input_dir: str, output_dir: str, pattern: str = "*.shp"):
    """Convert vector files to optimized GeoParquet."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    for input_file in Path(input_dir).glob(pattern):
        output_file = Path(output_dir) / f"{input_file.stem}.parquet"

        gpio.convert(input_file) \
            .add_bbox() \
            .sort_hilbert() \
            .write(output_file)

        print(f"Converted {input_file.name} -> {output_file.name}")

# Convert Shapefiles
convert_directory("shapefiles/", "parquet/", "*.shp")

# Convert GeoPackages
convert_directory("geopackages/", "parquet/", "*.gpkg")
```

## Batch Upload to Cloud Storage

Upload processed files to S3:

```python
from pathlib import Path
import geoparquet_io as gpio

def upload_directory(input_dir: str, s3_bucket: str, profile: str = None):
    """Upload all parquet files to S3."""
    for input_file in Path(input_dir).glob("*.parquet"):
        destination = f"s3://{s3_bucket}/{input_file.name}"

        gpio.read(input_file) \
            .upload(destination, profile=profile)

        print(f"Uploaded {input_file.name}")

# Usage
upload_directory("output/", "my-bucket/data/", profile="my-aws")
```

## Processing with Different Operations per File

Apply different transformations based on file characteristics:

```python
from pathlib import Path
import geoparquet_io as gpio

def smart_process(input_dir: str, output_dir: str):
    """Apply different processing based on file size/content."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    for input_file in Path(input_dir).glob("*.parquet"):
        table = gpio.read(input_file)
        output_file = Path(output_dir) / input_file.name

        # Always add bbox and sort
        result = table.add_bbox().sort_hilbert()

        # Add H3 for larger files (useful for partitioning later)
        if table.num_rows > 100000:
            result = result.add_h3(resolution=9)

        result.write(output_file)
        print(f"Processed {input_file.name} ({table.num_rows:,} rows)")

# Usage
smart_process("input/", "output/")
```

## Error Handling and Logging

Robust batch processing with error handling:

```python
from pathlib import Path
import logging
import geoparquet_io as gpio

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def robust_process(input_dir: str, output_dir: str):
    """Process files with comprehensive error handling."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    files = list(Path(input_dir).glob("*.parquet"))
    success_count = 0
    error_count = 0
    errors = []

    for input_file in files:
        output_file = Path(output_dir) / input_file.name

        try:
            gpio.read(input_file) \
                .add_bbox() \
                .sort_hilbert() \
                .write(output_file)

            success_count += 1
            logger.info(f"Processed: {input_file.name}")

        except Exception as e:
            error_count += 1
            errors.append((input_file.name, str(e)))
            logger.error(f"Failed: {input_file.name} - {e}")

    # Summary
    logger.info(f"Completed: {success_count} success, {error_count} failed")

    if errors:
        logger.warning("Failed files:")
        for filename, error in errors:
            logger.warning(f"  {filename}: {error}")

    return success_count, errors

# Usage
success, errors = robust_process("input/", "output/")
```

## Advanced: Using Core API for Batch Processing

For file-based operations without loading into memory:

```python
from pathlib import Path
from geoparquet_io.core.add_bbox_column import add_bbox_column
from geoparquet_io.core.hilbert_order import hilbert_order

def batch_process_core_api(input_dir: str, output_dir: str):
    """Process using core API (file-based operations)."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    for input_file in Path(input_dir).glob("*.parquet"):
        temp_file = Path(output_dir) / f"temp_{input_file.name}"
        output_file = Path(output_dir) / input_file.name

        # Add bbox
        add_bbox_column(
            input_parquet=str(input_file),
            output_parquet=str(temp_file),
            bbox_name="bbox",
            verbose=False
        )

        # Sort by Hilbert curve
        hilbert_order(
            input_parquet=str(temp_file),
            output_parquet=str(output_file),
            verbose=False
        )

        # Clean up temp file
        temp_file.unlink()

        print(f"Processed {input_file.name}")

# Usage
batch_process_core_api("input/", "output/")
```

## See Also

- [Basic Usage Examples](basic.md) - Single file operations
- [Workflow Examples](workflows.md) - Complete end-to-end workflows
- [Python API Reference](../api/python-api.md) - Full API documentation
