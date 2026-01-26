#!/usr/bin/env python
"""
Performance benchmark for LazyTable vs eager Table API.
"""

import gc
import os
import tempfile
import time
from pathlib import Path

# Test files
TEST_FILES = {
    "small": "/Users/cholmes/geodata/parquet-test-data/osmextract-nl2.parquet",  # 61KB, 679 rows
    "medium": "/Users/cholmes/geodata/parquet-test-data/fields-1_1.parquet",  # 623KB, 2073 rows
    "large": "/Users/cholmes/Downloads/nl-15.parquet",  # 32MB, 175K rows
}

# For very large file tests (optional)
LARGE_FILE = "/Users/cholmes/geodata/parquet-test-data/japan.parquet"  # 4.5GB


def time_operation(func, name, warmup=False):
    """Time an operation and return duration."""
    gc.collect()
    start = time.perf_counter()
    try:
        result = func()
        duration = time.perf_counter() - start
        if not warmup:
            print(f"  {name}: {duration:.3f}s")
        return duration, result
    except Exception as e:
        if not warmup:
            print(f"  {name}: ERROR - {e}")
        return None, None


def benchmark_eager_api(input_file, output_file):
    """Benchmark the eager Table API with chained operations."""
    import geoparquet_io as gpio

    def run():
        table = gpio.read(input_file)
        table = table.add_bbox()
        table = table.extract(limit=10000)  # Limit to avoid memory issues on large files
        table = table.sort_hilbert()
        table.write(output_file)
        return True

    return time_operation(run, "Eager (read→add_bbox→extract→sort_hilbert→write)")


def benchmark_lazy_api(input_file, output_file):
    """Benchmark the lazy LazyTable API with chained operations."""
    import geoparquet_io as gpio

    def run():
        gpio.read_lazy(input_file).add_bbox().extract(limit=10000).sort_hilbert().write(output_file)
        return True

    return time_operation(run, "Lazy  (read→add_bbox→extract→sort_hilbert→write)")


def benchmark_eager_simple(input_file, output_file):
    """Benchmark eager API with simpler operations."""
    import geoparquet_io as gpio

    def run():
        table = gpio.read(input_file)
        table = table.add_bbox()
        table.write(output_file)
        return True

    return time_operation(run, "Eager (read→add_bbox→write)")


def benchmark_lazy_simple(input_file, output_file):
    """Benchmark lazy API with simpler operations."""
    import geoparquet_io as gpio

    def run():
        gpio.read_lazy(input_file).add_bbox().write(output_file)
        return True

    return time_operation(run, "Lazy  (read→add_bbox→write)")


def run_benchmarks(test_files=None, include_large=False):
    """Run all benchmarks."""
    if test_files is None:
        test_files = TEST_FILES

    print("=" * 70)
    print("PERFORMANCE BENCHMARK: LazyTable vs Eager Table API")
    print("=" * 70)

    results = {}

    for size, input_file in test_files.items():
        if not Path(input_file).exists():
            print(f"\nSkipping {size}: file not found")
            continue

        print(f"\n{'=' * 70}")
        print(f"Test file: {size} ({Path(input_file).name})")
        print(f"Size: {Path(input_file).stat().st_size / (1024 * 1024):.1f} MB")
        print("=" * 70)

        # Create temp output file
        output_file = tempfile.mktemp(suffix=".parquet")

        try:
            # Simple operations (add_bbox only)
            print("\n--- Simple chain (read → add_bbox → write) ---")
            eager_simple, _ = benchmark_eager_simple(input_file, output_file)
            if Path(output_file).exists():
                os.unlink(output_file)

            lazy_simple, _ = benchmark_lazy_simple(input_file, output_file)
            if Path(output_file).exists():
                os.unlink(output_file)

            if eager_simple and lazy_simple:
                speedup = eager_simple / lazy_simple
                print(
                    f"  Speedup: {speedup:.2f}x {'(lazy faster)' if speedup > 1 else '(eager faster)'}"
                )

            # Complex operations (add_bbox + extract + sort_hilbert)
            print("\n--- Complex chain (read → add_bbox → extract → sort_hilbert → write) ---")
            eager_complex, _ = benchmark_eager_api(input_file, output_file)
            if Path(output_file).exists():
                os.unlink(output_file)

            lazy_complex, _ = benchmark_lazy_api(input_file, output_file)
            if Path(output_file).exists():
                os.unlink(output_file)

            if eager_complex and lazy_complex:
                speedup = eager_complex / lazy_complex
                print(
                    f"  Speedup: {speedup:.2f}x {'(lazy faster)' if speedup > 1 else '(eager faster)'}"
                )

            results[size] = {
                "eager_simple": eager_simple,
                "lazy_simple": lazy_simple,
                "eager_complex": eager_complex,
                "lazy_complex": lazy_complex,
            }

        finally:
            if Path(output_file).exists():
                os.unlink(output_file)

    # Large file test (japan.parquet)
    if include_large and Path(LARGE_FILE).exists():
        print(f"\n{'=' * 70}")
        print(f"LARGE FILE TEST: {Path(LARGE_FILE).name}")
        print(f"Size: {Path(LARGE_FILE).stat().st_size / (1024 * 1024 * 1024):.1f} GB")
        print("=" * 70)
        print("Note: This may take a while...")

        output_file = tempfile.mktemp(suffix=".parquet")
        try:
            # Only test lazy API on large file (eager would OOM)
            print("\n--- Lazy API only (eager would run out of memory) ---")

            import geoparquet_io as gpio

            def large_lazy():
                gpio.read_lazy(LARGE_FILE).add_bbox().extract(limit=100000).sort_hilbert().write(
                    output_file
                )
                return True

            time_operation(large_lazy, "Lazy (100K row limit)")

        finally:
            if Path(output_file).exists():
                os.unlink(output_file)

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    for size, data in results.items():
        print(f"\n{size}:")
        if data.get("eager_simple") and data.get("lazy_simple"):
            speedup = data["eager_simple"] / data["lazy_simple"]
            print(
                f"  Simple:  Eager {data['eager_simple']:.3f}s, Lazy {data['lazy_simple']:.3f}s, Speedup: {speedup:.2f}x"
            )
        if data.get("eager_complex") and data.get("lazy_complex"):
            speedup = data["eager_complex"] / data["lazy_complex"]
            print(
                f"  Complex: Eager {data['eager_complex']:.3f}s, Lazy {data['lazy_complex']:.3f}s, Speedup: {speedup:.2f}x"
            )

    return results


if __name__ == "__main__":
    import sys

    include_large = "--large" in sys.argv
    run_benchmarks(include_large=include_large)
