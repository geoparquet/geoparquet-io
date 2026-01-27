#!/usr/bin/env python3
"""
Version comparison benchmark for gpio.

Runs common operations across different gpio versions and compares performance.
Works with any gpio version (doesn't require the benchmark suite).

Usage:
    python scripts/version_benchmark.py --version-label "v0.9.0" --output results_v0.9.0.json
    python scripts/version_benchmark.py --version-label "main" --output results_main.json
    python scripts/version_benchmark.py --compare results_v0.9.0.json results_main.json
"""

import argparse
import gc
import json
import os
import subprocess
import tempfile
import time
import urllib.request
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

# Import base URL and benchmark files from config to avoid duplication
from geoparquet_io.benchmarks.config import BENCHMARK_DATA_URL, BENCHMARK_FILES

# Test files from source.coop (ordered by size)
# Built from BENCHMARK_FILES plus additional test files
TEST_FILES = {
    "tiny": BENCHMARK_FILES["tiny"]["buildings"],
    "small": BENCHMARK_FILES["small"]["buildings"],
    "medium": BENCHMARK_FILES["medium"]["buildings"],
    "large": BENCHMARK_FILES["large"]["fields"],
    # Points in alternate projection (EPSG:3857) for geometry type and CRS variation
    "points-tiny": f"{BENCHMARK_DATA_URL}/points_tiny_3857.parquet",
    "points-small": f"{BENCHMARK_DATA_URL}/points_small_3857.parquet",
}

# Source format files for import/convert benchmarks (tiny and small only)
# NOTE: flatgeobuf and shapefile are excluded because gpio's export to those formats
# doesn't preserve CRS metadata for EPSG:4326 files (see issues #189 and #190)
SOURCE_FORMAT_FILES = {
    "geojson": {
        "tiny": f"{BENCHMARK_DATA_URL}/geojson/buildings_tiny.geojson",
        "small": f"{BENCHMARK_DATA_URL}/geojson/buildings_small.geojson",
    },
    "geopackage": {
        "tiny": f"{BENCHMARK_DATA_URL}/geopackage/buildings_tiny.gpkg",
        "small": f"{BENCHMARK_DATA_URL}/geopackage/buildings_small.gpkg",
    },
}

# File size presets
FILE_PRESETS = {
    "quick": ["tiny", "small"],
    "standard": ["small", "medium"],
    "full": ["tiny", "small", "medium", "large", "points-tiny", "points-small"],
}

# Local cache directory
CACHE_DIR = Path("/tmp/gpio-benchmark-cache")

# Operations to benchmark (CLI commands)
# All available operations
ALL_OPERATIONS = {
    # Inspect
    "inspect": {
        "cmd": ["gpio", "inspect", "{input}"],
        "description": "Inspect file metadata",
    },
    # Extract operations
    "extract-limit": {
        "cmd": ["gpio", "extract", "{input}", "{output}", "--limit", "100"],
        "description": "Extract first 100 rows",
    },
    "extract-columns": {
        "cmd": ["gpio", "extract", "{input}", "{output}", "--include-cols", "id,geometry"],
        "description": "Extract specific columns",
    },
    "extract-bbox": {
        "cmd": ["gpio", "extract", "{input}", "{output}", "--bbox", "-180,-45,0,45"],
        "description": "Spatial bbox filtering",
    },
    # Add column operations
    "add-bbox": {
        "cmd": ["gpio", "add", "bbox", "{input}", "{output}", "--force", "--bbox-name", "bounds"],
        "description": "Add bbox column",
    },
    "add-quadkey": {
        "cmd": [
            "gpio",
            "add",
            "quadkey",
            "{input}",
            "{output}",
            "--resolution",
            "12",
            "--quadkey-name",
            "quadkey_bench",
        ],
        "description": "Add quadkey column",
    },
    "add-h3": {
        "cmd": ["gpio", "add", "h3", "{input}", "{output}", "--resolution", "8"],
        "description": "Add H3 column",
    },
    # Sort operations
    "sort-hilbert": {
        "cmd": ["gpio", "sort", "hilbert", "{input}", "{output}"],
        "description": "Sort by Hilbert curve",
    },
    "sort-quadkey": {
        "cmd": ["gpio", "sort", "quadkey", "{input}", "{output}", "--resolution", "12"],
        "description": "Sort by quadkey",
    },
    # Reproject
    "reproject": {
        "cmd": ["gpio", "convert", "reproject", "{input}", "{output}", "-d", "EPSG:3857"],
        "description": "Reproject to Web Mercator",
    },
    # Partition operations
    "partition-quadkey": {
        "cmd": [
            "gpio",
            "partition",
            "quadkey",
            "{input}",
            "{output_dir}",
            "--resolution",
            "12",
            "--partition-resolution",
            "4",
        ],
        "description": "Partition by quadkey",
    },
    "partition-h3": {
        "cmd": ["gpio", "partition", "h3", "{input}", "{output_dir}", "--resolution", "4"],
        "description": "Partition by H3",
    },
    # Convert/export operations
    "convert-geojson": {
        "cmd": ["gpio", "convert", "geojson", "{input}", "{output_geojson}"],
        "description": "Convert to GeoJSON",
    },
    "convert-flatgeobuf": {
        "cmd": ["gpio", "convert", "flatgeobuf", "{input}", "{output_fgb}"],
        "description": "Convert to FlatGeobuf",
    },
    "convert-geopackage": {
        "cmd": ["gpio", "convert", "geopackage", "{input}", "{output_gpkg}"],
        "description": "Convert to GeoPackage",
    },
}

# Import operations (convert TO GeoParquet from other formats)
# These use special source format files, only available for tiny/small
# Uses {input} placeholder - the source file is passed as input_file
# NOTE: import-flatgeobuf and import-shapefile are excluded because gpio's export
# to those formats doesn't preserve CRS metadata for EPSG:4326 (see issues #189 and #190)
IMPORT_OPERATIONS = {
    "import-geojson": {
        "cmd": ["gpio", "convert", "{input}", "{output}"],
        "description": "Import from GeoJSON",
        "source_format": "geojson",
    },
    "import-geopackage": {
        "cmd": ["gpio", "convert", "{input}", "{output}"],
        "description": "Import from GeoPackage",
        "source_format": "geopackage",
    },
}

# Chain operations (multi-step workflows)
CHAIN_OPERATIONS = {
    "chain-extract-bbox-sort": {
        "cmd": None,  # Handled specially
        "description": "Extract → Add bbox → Hilbert sort",
        "steps": [
            ["gpio", "extract", "{input}", "{step1}", "--include-cols", "id,geometry"],
            ["gpio", "add", "bbox", "{step1}", "{step2}", "--force", "--bbox-name", "bounds"],
            ["gpio", "sort", "hilbert", "{step2}", "{output}"],
        ],
    },
    "chain-filter-sort": {
        "cmd": None,
        "description": "Bbox filter → Hilbert sort",
        "steps": [
            # Bbox covering most Singapore test data (tiny/small/medium)
            # Note: This won't work correctly for the large file (Slovenia, EPSG:3794)
            ["gpio", "extract", "{input}", "{step1}", "--bbox", "103.7,1.4,104.0,1.47"],
            ["gpio", "sort", "hilbert", "{step1}", "{output}"],
        ],
    },
}

# Operation presets
OPERATION_PRESETS = {
    "quick": ["inspect", "extract-limit", "add-bbox"],
    "standard": ["inspect", "extract-limit", "extract-columns", "add-bbox", "sort-hilbert"],
    "full": [
        # Core operations
        "inspect",
        "extract-limit",
        "extract-columns",
        "extract-bbox",
        # Add column operations
        "add-bbox",
        "add-quadkey",
        "add-h3",
        # Sort operations
        "sort-hilbert",
        "sort-quadkey",
        # Reproject
        "reproject",
        # Partition operations
        "partition-quadkey",
        "partition-h3",
        # Export/convert operations
        "convert-geojson",
        "convert-flatgeobuf",
        "convert-geopackage",
        # Import operations (only run on tiny/small)
        "import-geojson",
        "import-geopackage",
        # Chain operations
        "chain-extract-bbox-sort",
        "chain-filter-sort",
    ],
}

# Default operations (for backward compatibility)
OPERATIONS = [{"name": name, **ALL_OPERATIONS[name]} for name in OPERATION_PRESETS["standard"]]


# Trusted domains for benchmark file downloads
TRUSTED_DOMAINS = {"data.source.coop", "source.coop"}


def _validate_url(url: str) -> None:
    """Validate URL is from a trusted domain."""
    parsed = urlparse(url)
    if parsed.netloc not in TRUSTED_DOMAINS:
        raise ValueError(f"URL domain '{parsed.netloc}' not in trusted domains: {TRUSTED_DOMAINS}")


def download_file(url: str, dest: Path) -> bool:
    """Download a file from URL to destination.

    Only downloads from trusted domains (source.coop).
    """
    try:
        _validate_url(url)
        # Use basename to get just the filename, preventing path traversal
        filename = os.path.basename(urlparse(url).path)
        print(f"  Downloading {filename}...", end=" ", flush=True)
        urllib.request.urlretrieve(url, dest)
        size_mb = dest.stat().st_size / (1024 * 1024)
        print(f"done ({size_mb:.2f} MB)")
        return True
    except ValueError as e:
        print(f"validation error: {e}")
        return False
    except Exception as e:
        print(f"failed: {e}")
        return False


def get_cached_file(url: str) -> Path:
    """Get local cached path for a URL, downloading if needed.

    Uses os.path.basename to prevent path traversal attacks.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # Use basename to extract just the filename, preventing path traversal
    # e.g., "../../../etc/passwd" becomes "passwd"
    filename = os.path.basename(urlparse(url).path)
    if not filename:
        raise ValueError(f"Could not extract filename from URL: {url}")
    cached_path = CACHE_DIR / filename

    # Download if not cached
    if not cached_path.exists():
        if not download_file(url, cached_path):
            raise RuntimeError(f"Failed to download {url}")

    return cached_path


def ensure_files_cached(
    file_sizes: list[str] | None = None, include_source_formats: bool = False
) -> tuple[dict[str, Path], dict[str, dict[str, Path]]]:
    """Download test files to local cache.

    Args:
        file_sizes: List of file sizes to cache. If None, caches all files.
        include_source_formats: Whether to also cache source format files for imports.

    Returns:
        Tuple of (parquet_files, source_format_files)
    """
    print("\nEnsuring test files are cached locally...")
    local_files = {}
    sizes_to_cache = file_sizes if file_sizes else list(TEST_FILES.keys())
    for size_name in sizes_to_cache:
        if size_name in TEST_FILES:
            local_files[size_name] = get_cached_file(TEST_FILES[size_name])

    # Cache source format files for import operations
    source_files: dict[str, dict[str, Path]] = {}
    if include_source_formats:
        print("  Caching source format files for import tests...")
        for fmt, sizes in SOURCE_FORMAT_FILES.items():
            source_files[fmt] = {}
            for size_name in sizes_to_cache:
                if size_name in sizes:
                    source_files[fmt][size_name] = get_cached_file(sizes[size_name])

    print()
    return local_files, source_files


def _substitute_cmd(cmd: list[str], substitutions: dict[str, str]) -> list[str]:
    """Substitute placeholders in command."""
    final_cmd = []
    for arg in cmd:
        if arg.startswith("{") and arg.endswith("}"):
            key = arg[1:-1]
            if key in substitutions:
                final_cmd.append(substitutions[key])
            else:
                final_cmd.append(arg)
        else:
            final_cmd.append(arg)
    return final_cmd


def run_operation(cmd: list[str], input_file: str, output_dir: Path) -> dict:
    """Run a single operation and measure time."""
    output_file = output_dir / "output.parquet"
    partition_dir = output_dir / "partitioned"
    output_geojson = output_dir / "output.geojson"
    output_fgb = output_dir / "output.fgb"
    output_gpkg = output_dir / "output.gpkg"

    substitutions = {
        "input": input_file,
        "output": str(output_file),
        "output_dir": str(partition_dir),
        "output_geojson": str(output_geojson),
        "output_fgb": str(output_fgb),
        "output_gpkg": str(output_gpkg),
    }
    final_cmd = _substitute_cmd(cmd, substitutions)

    # Force garbage collection before timing
    gc.collect()

    start_time = time.perf_counter()
    try:
        result = subprocess.run(
            final_cmd,
            capture_output=True,
            text=True,
            timeout=300,
        )
        end_time = time.perf_counter()

        success = result.returncode == 0
        error = result.stderr if not success else None

    except subprocess.TimeoutExpired:
        end_time = time.perf_counter()
        success = False
        error = "Timeout"
    except Exception as e:
        end_time = time.perf_counter()
        success = False
        error = str(e)

    elapsed = end_time - start_time

    # Clean up output files
    for output_path in [output_file, output_geojson, output_fgb, output_gpkg]:
        if output_path.exists():
            output_path.unlink()
    if partition_dir.exists():
        import shutil

        shutil.rmtree(partition_dir, ignore_errors=True)

    return {
        "time_seconds": elapsed,
        "success": success,
        "error": error,
    }


def run_chain_operation(steps: list[list[str]], input_file: str, output_dir: Path) -> dict:
    """Run a chain of operations and measure total time."""
    # Create step files
    step_files = {
        "input": input_file,
        "step1": str(output_dir / "step1.parquet"),
        "step2": str(output_dir / "step2.parquet"),
        "step3": str(output_dir / "step3.parquet"),
        "output": str(output_dir / "output.parquet"),
    }

    # Force garbage collection before timing
    gc.collect()

    start_time = time.perf_counter()
    try:
        for step_cmd in steps:
            final_cmd = _substitute_cmd(step_cmd, step_files)
            result = subprocess.run(
                final_cmd,
                capture_output=True,
                text=True,
                timeout=300,
            )
            if result.returncode != 0:
                end_time = time.perf_counter()
                return {
                    "time_seconds": end_time - start_time,
                    "success": False,
                    "error": result.stderr,
                }

        end_time = time.perf_counter()
        success = True
        error = None

    except subprocess.TimeoutExpired:
        end_time = time.perf_counter()
        success = False
        error = "Timeout"
    except Exception as e:
        end_time = time.perf_counter()
        success = False
        error = str(e)

    elapsed = end_time - start_time

    # Clean up step files
    for key, path in step_files.items():
        if key != "input":
            p = Path(path)
            if p.exists():
                p.unlink()

    return {
        "time_seconds": elapsed,
        "success": success,
        "error": error,
    }


def run_benchmarks(
    version_label: str,
    iterations: int = 3,
    use_cache: bool = True,
    file_sizes: list[str] | None = None,
    ops: list[str] | None = None,
) -> dict:
    """Run all benchmarks and return results.

    Args:
        version_label: Label for this version (e.g., 'v0.9.0', 'main')
        iterations: Number of iterations per operation
        use_cache: Whether to cache files locally
        file_sizes: List of file sizes to test. If None, uses all available.
        ops: List of operation names to run. If None, uses standard preset.
    """
    # Determine which files to run
    sizes_to_run = file_sizes if file_sizes else list(TEST_FILES.keys())

    # Determine which operations to run
    ops_to_run = ops if ops else OPERATION_PRESETS["standard"]

    results = {
        "version": version_label,
        "timestamp": datetime.now().isoformat(),
        "iterations": iterations,
        "file_sizes": sizes_to_run,
        "operations": ops_to_run,
        "benchmarks": [],
    }

    # Get gpio version
    try:
        version_result = subprocess.run(["gpio", "--version"], capture_output=True, text=True)
        results["gpio_version"] = version_result.stdout.strip()
    except Exception:
        results["gpio_version"] = "unknown"

    # Check if we need source format files for import operations
    has_import_ops = any(op in IMPORT_OPERATIONS for op in ops_to_run)

    # Get local files if caching enabled
    if use_cache:
        local_files, source_files = ensure_files_cached(sizes_to_run, has_import_ops)
    else:
        local_files = None
        source_files = {}

    print(f"\n{'=' * 60}")
    print(f"Benchmarking: {version_label}")
    print(f"GPIO Version: {results['gpio_version']}")
    print(f"Iterations: {iterations}")
    print(f"File sizes: {', '.join(sizes_to_run)}")
    print(f"Operations: {', '.join(ops_to_run)}")
    print(f"Using local cache: {use_cache}")
    print(f"{'=' * 60}\n")

    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)

        for size_name in sizes_to_run:
            input_url = TEST_FILES[size_name]
            # Use cached local file or remote URL
            if local_files:
                input_path = str(local_files[size_name])
            else:
                input_path = input_url

            print(f"\n--- File: {size_name} ({input_url.split('/')[-1]}) ---")

            for op_name in ops_to_run:
                # Get operation details
                if op_name in ALL_OPERATIONS:
                    op = ALL_OPERATIONS[op_name]
                    is_chain = False
                    is_import = False
                elif op_name in IMPORT_OPERATIONS:
                    op = IMPORT_OPERATIONS[op_name]
                    is_chain = False
                    is_import = True
                    # Skip import ops for sizes that don't have source files
                    source_fmt = op["source_format"]
                    if source_fmt not in source_files or size_name not in source_files.get(
                        source_fmt, {}
                    ):
                        print(f"  {op_name}: SKIPPED (no {source_fmt} file for {size_name})")
                        continue
                elif op_name in CHAIN_OPERATIONS:
                    op = CHAIN_OPERATIONS[op_name]
                    is_chain = True
                    is_import = False
                else:
                    print(f"  {op_name}: SKIPPED (unknown operation)")
                    continue

                print(f"  {op_name}: ", end="", flush=True)

                times = []
                errors = []

                for _i in range(iterations):
                    if is_chain:
                        result = run_chain_operation(op["steps"], input_path, output_dir)
                    elif is_import:
                        # Get the source format file for import operations
                        source_fmt = op["source_format"]
                        source_input = str(source_files[source_fmt][size_name])
                        result = run_operation(op["cmd"], source_input, output_dir)
                    else:
                        result = run_operation(op["cmd"], input_path, output_dir)

                    if result["success"]:
                        times.append(result["time_seconds"])
                        print(".", end="", flush=True)
                    else:
                        errors.append(result["error"])
                        print("x", end="", flush=True)

                if times:
                    avg_time = sum(times) / len(times)
                    min_time = min(times)
                    max_time = max(times)
                    print(f" {avg_time:.3f}s (min={min_time:.3f}, max={max_time:.3f})")
                else:
                    print(f" FAILED: {errors[0] if errors else 'unknown'}")
                    avg_time = None
                    min_time = None
                    max_time = None

                results["benchmarks"].append(
                    {
                        "file_size": size_name,
                        "operation": op_name,
                        "description": op["description"],
                        "avg_time": avg_time,
                        "min_time": min_time,
                        "max_time": max_time,
                        "success_count": len(times),
                        "fail_count": len(errors),
                        "errors": errors if errors else None,
                    }
                )

    return results


def compare_results(file1: str, file2: str):
    """Compare two benchmark result files."""
    with open(file1) as f:
        results1 = json.load(f)
    with open(file2) as f:
        results2 = json.load(f)

    print(f"\n{'=' * 70}")
    print(f"Comparison: {results1['version']} vs {results2['version']}")
    print(f"{'=' * 70}\n")

    # Build lookup for results2
    lookup2 = {}
    for b in results2["benchmarks"]:
        key = (b["file_size"], b["operation"])
        lookup2[key] = b

    print(
        f"{'Operation':<25} {'File':<8} {results1['version']:<12} {results2['version']:<12} {'Delta':<12}"
    )
    print("-" * 70)

    for b1 in results1["benchmarks"]:
        key = (b1["file_size"], b1["operation"])
        b2 = lookup2.get(key)

        op_name = b1["operation"]
        file_size = b1["file_size"]

        if b1["avg_time"] is None:
            time1_str = "FAILED"
        else:
            time1_str = f"{b1['avg_time']:.3f}s"

        if b2 is None or b2["avg_time"] is None:
            time2_str = "FAILED" if b2 else "N/A"
            delta_str = "N/A"
        else:
            time2_str = f"{b2['avg_time']:.3f}s"

            if b1["avg_time"] is not None and b1["avg_time"] > 0:
                delta = (b2["avg_time"] - b1["avg_time"]) / b1["avg_time"] * 100
                if delta > 0:
                    delta_str = f"+{delta:.1f}% slower"
                elif delta < 0:
                    delta_str = f"{delta:.1f}% faster"
                else:
                    delta_str = "same"
            else:
                delta_str = "N/A"

        print(f"{op_name:<25} {file_size:<8} {time1_str:<12} {time2_str:<12} {delta_str:<12}")

    print()


def main():
    parser = argparse.ArgumentParser(description="Version comparison benchmark")
    parser.add_argument(
        "--version-label",
        help="Label for this version (e.g., 'v0.9.0', 'main')",
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Output JSON file for results",
    )
    parser.add_argument(
        "--iterations",
        "-n",
        type=int,
        default=3,
        help="Number of iterations per operation (default: 3)",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Don't cache files locally, use remote URLs directly",
    )
    parser.add_argument(
        "--files",
        "-f",
        default="full",
        help=(
            "File sizes to benchmark. Use preset names (quick, standard, full) "
            "or comma-separated sizes (tiny,small,medium,large). Default: full"
        ),
    )
    parser.add_argument(
        "--ops",
        default="full",
        help=(
            "Operations to benchmark. Use preset names (quick, standard, full) "
            "or comma-separated operation names. Default: full"
        ),
    )
    parser.add_argument(
        "--compare",
        nargs=2,
        metavar=("FILE1", "FILE2"),
        help="Compare two result files",
    )

    args = parser.parse_args()

    # Parse file sizes
    if args.files in FILE_PRESETS:
        file_sizes = FILE_PRESETS[args.files]
    else:
        file_sizes = [s.strip() for s in args.files.split(",")]
        # Validate file sizes
        invalid = [s for s in file_sizes if s not in TEST_FILES]
        if invalid:
            parser.error(f"Invalid file sizes: {invalid}. Valid: {list(TEST_FILES.keys())}")

    # Parse operations
    all_ops = (
        set(ALL_OPERATIONS.keys()) | set(CHAIN_OPERATIONS.keys()) | set(IMPORT_OPERATIONS.keys())
    )
    if args.ops in OPERATION_PRESETS:
        ops_list = OPERATION_PRESETS[args.ops]
    else:
        ops_list = [s.strip() for s in args.ops.split(",")]
        # Validate operations
        invalid = [s for s in ops_list if s not in all_ops]
        if invalid:
            parser.error(f"Invalid operations: {invalid}. Valid: {sorted(all_ops)}")

    if args.compare:
        compare_results(args.compare[0], args.compare[1])
    elif args.version_label:
        results = run_benchmarks(
            args.version_label,
            args.iterations,
            use_cache=not args.no_cache,
            file_sizes=file_sizes,
            ops=ops_list,
        )

        if args.output:
            with open(args.output, "w") as f:
                json.dump(results, f, indent=2)
            print(f"\nResults saved to {args.output}")
        else:
            print(json.dumps(results, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
