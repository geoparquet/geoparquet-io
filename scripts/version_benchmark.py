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
import subprocess
import tempfile
import time
import urllib.request
from datetime import datetime
from pathlib import Path

# Test files from source.coop
TEST_FILES = {
    "tiny": "https://data.source.coop/cholmes/gpio-test/benchmark/buildings_tiny.parquet",
    "small": "https://data.source.coop/cholmes/gpio-test/benchmark/buildings_small.parquet",
}

# Local cache directory
CACHE_DIR = Path("/tmp/gpio-benchmark-cache")

# Operations to benchmark (CLI commands)
OPERATIONS = [
    {
        "name": "inspect",
        "cmd": ["gpio", "inspect", "{input}"],
        "description": "Inspect file metadata",
    },
    {
        "name": "extract-limit",
        "cmd": ["gpio", "extract", "{input}", "{output}", "--limit", "100"],
        "description": "Extract first 100 rows",
    },
    {
        "name": "extract-columns",
        "cmd": ["gpio", "extract", "{input}", "{output}", "--include-cols", "id,geometry"],
        "description": "Extract specific columns",
    },
    {
        "name": "add-bbox",
        "cmd": ["gpio", "add", "bbox", "{input}", "{output}", "--force", "--bbox-name", "bounds"],
        "description": "Add bbox column (as bounds)",
    },
    {
        "name": "sort-hilbert",
        "cmd": ["gpio", "sort", "hilbert", "{input}", "{output}"],
        "description": "Sort by Hilbert curve",
    },
]


def download_file(url: str, dest: Path) -> bool:
    """Download a file from URL to destination."""
    try:
        print(f"  Downloading {url.split('/')[-1]}...", end=" ", flush=True)
        urllib.request.urlretrieve(url, dest)
        size_mb = dest.stat().st_size / (1024 * 1024)
        print(f"done ({size_mb:.2f} MB)")
        return True
    except Exception as e:
        print(f"failed: {e}")
        return False


def get_cached_file(url: str) -> Path:
    """Get local cached path for a URL, downloading if needed."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # Use filename from URL
    filename = url.split("/")[-1]
    cached_path = CACHE_DIR / filename

    # Download if not cached
    if not cached_path.exists():
        if not download_file(url, cached_path):
            raise RuntimeError(f"Failed to download {url}")

    return cached_path


def ensure_files_cached() -> dict[str, Path]:
    """Download all test files to local cache."""
    print("\nEnsuring test files are cached locally...")
    local_files = {}
    for size_name, url in TEST_FILES.items():
        local_files[size_name] = get_cached_file(url)
    print()
    return local_files


def run_operation(cmd: list[str], input_file: str, output_dir: Path) -> dict:
    """Run a single operation and measure time."""
    output_file = output_dir / "output.parquet"

    # Substitute placeholders
    final_cmd = []
    for arg in cmd:
        if arg == "{input}":
            final_cmd.append(input_file)
        elif arg == "{output}":
            final_cmd.append(str(output_file))
        else:
            final_cmd.append(arg)

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

    # Clean up output file
    if output_file.exists():
        output_file.unlink()

    return {
        "time_seconds": elapsed,
        "success": success,
        "error": error,
    }


def run_benchmarks(version_label: str, iterations: int = 3, use_cache: bool = True) -> dict:
    """Run all benchmarks and return results."""
    results = {
        "version": version_label,
        "timestamp": datetime.now().isoformat(),
        "iterations": iterations,
        "benchmarks": [],
    }

    # Get gpio version
    try:
        version_result = subprocess.run(["gpio", "--version"], capture_output=True, text=True)
        results["gpio_version"] = version_result.stdout.strip()
    except Exception:
        results["gpio_version"] = "unknown"

    # Get local files if caching enabled
    if use_cache:
        local_files = ensure_files_cached()
    else:
        local_files = None

    print(f"\n{'=' * 60}")
    print(f"Benchmarking: {version_label}")
    print(f"GPIO Version: {results['gpio_version']}")
    print(f"Iterations: {iterations}")
    print(f"Using local cache: {use_cache}")
    print(f"{'=' * 60}\n")

    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)

        for size_name, input_url in TEST_FILES.items():
            # Use cached local file or remote URL
            if local_files:
                input_path = str(local_files[size_name])
            else:
                input_path = input_url

            print(f"\n--- File: {size_name} ({input_url.split('/')[-1]}) ---")

            for op in OPERATIONS:
                op_name = op["name"]
                print(f"  {op_name}: ", end="", flush=True)

                times = []
                errors = []

                for _i in range(iterations):
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

            if b1["avg_time"] is not None:
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
        "--compare",
        nargs=2,
        metavar=("FILE1", "FILE2"),
        help="Compare two result files",
    )

    args = parser.parse_args()

    if args.compare:
        compare_results(args.compare[0], args.compare[1])
    elif args.version_label:
        results = run_benchmarks(
            args.version_label,
            args.iterations,
            use_cache=not args.no_cache,
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
