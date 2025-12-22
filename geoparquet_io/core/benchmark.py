"""
Benchmark utilities for comparing GeoParquet conversion methods.

Tests available converters and reports performance metrics.
"""

from __future__ import annotations

import json
import platform
import shutil
import subprocess
import sys
import tempfile
import time
import tracemalloc
from pathlib import Path
from statistics import mean, stdev
from typing import Any

import click
import duckdb
import psutil

from geoparquet_io.core.logging_config import progress

# Converter registry with detection functions
CONVERTERS = {
    "duckdb": {
        "name": "DuckDB",
        "check": lambda: True,  # Always available (core dep)
        "install": None,
    },
    "geopandas_fiona": {
        "name": "GeoPandas (Fiona)",
        "check": lambda: _check_geopandas_fiona(),
        "install": "pip install geopandas fiona",
    },
    "geopandas_pyogrio": {
        "name": "GeoPandas (PyOGRIO)",
        "check": lambda: _check_geopandas_pyogrio(),
        "install": "pip install geopandas pyogrio",
    },
    "gdal_ogr2ogr": {
        "name": "GDAL ogr2ogr",
        "check": lambda: _check_ogr2ogr(),
        "install": "Install GDAL (e.g., apt install gdal-bin or brew install gdal)",
    },
}


def _check_geopandas_fiona() -> bool:
    """Check if geopandas with fiona engine is available."""
    try:
        import fiona  # noqa: F401
        import geopandas  # noqa: F401

        return True
    except ImportError:
        return False


def _check_geopandas_pyogrio() -> bool:
    """Check if geopandas with pyogrio engine is available."""
    try:
        import geopandas  # noqa: F401
        import pyogrio  # noqa: F401

        return True
    except ImportError:
        return False


def _check_ogr2ogr() -> bool:
    """Check if ogr2ogr CLI is available."""
    return shutil.which("ogr2ogr") is not None


def detect_available_converters() -> tuple[list[str], list[str]]:
    """
    Detect which converters are available.

    Returns:
        Tuple of (available_converters, missing_converters)
    """
    available = []
    missing = []

    for name, info in CONVERTERS.items():
        if info["check"]():
            available.append(name)
        else:
            missing.append(name)

    return available, missing


def get_file_info(filepath: Path) -> dict[str, Any]:
    """Get basic info about input file using DuckDB."""
    # Check file exists first
    if not filepath.exists():
        return {
            "name": filepath.name,
            "format": filepath.suffix,
            "error": f"File not found: {filepath}",
        }

    try:
        conn = duckdb.connect()
        conn.execute("INSTALL spatial; LOAD spatial;")

        # Get feature count and basic info
        result = conn.execute(f"""
            SELECT COUNT(*) as cnt
            FROM ST_Read('{filepath}')
        """).fetchone()

        feature_count = result[0] if result else 0

        # Get schema to find geometry column
        schema = conn.execute(f"""
            SELECT * FROM ST_Read('{filepath}') LIMIT 0
        """).description

        # Find geometry column (common names)
        geom_col = None
        for col_info in schema:
            col_name = col_info[0].lower()
            if col_name in ["geometry", "geom", "wkb_geometry", "shape"]:
                geom_col = col_info[0]
                break

        # Try to get geometry type from first row
        geom_type = "unknown"
        if geom_col:
            geom_result = conn.execute(f"""
                SELECT ST_GeometryType({geom_col}) as geom_type
                FROM ST_Read('{filepath}')
                LIMIT 1
            """).fetchone()
            geom_type = geom_result[0] if geom_result else "unknown"

        conn.close()

        return {
            "name": filepath.name,
            "format": filepath.suffix,
            "size_mb": round(filepath.stat().st_size / (1024 * 1024), 2),
            "feature_count": feature_count,
            "geometry_type": geom_type,
        }
    except Exception as e:
        return {
            "name": filepath.name,
            "format": filepath.suffix,
            "size_mb": round(filepath.stat().st_size / (1024 * 1024), 2),
            "error": str(e),
        }


def _get_cpu_info_linux() -> str:
    """Get CPU info on Linux systems."""
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    cpu_name = line.split(":", 1)[1].strip()
                    cpu_count = psutil.cpu_count(logical=True)
                    return f"{cpu_name} / {cpu_count} cores"
        cpu_count = psutil.cpu_count(logical=True)
        return f"Unknown CPU / {cpu_count} cores"
    except OSError:
        cpu_count = psutil.cpu_count(logical=True)
        return f"Unknown CPU / {cpu_count} cores"


def _get_cpu_info() -> str:
    """Get CPU information string."""
    try:
        if platform.system() == "Linux":
            return _get_cpu_info_linux()
        cpu_info = platform.processor()
        cpu_count = psutil.cpu_count(logical=True)
        if cpu_info:
            return f"{cpu_info} / {cpu_count} cores"
        return f"Unknown CPU / {cpu_count} cores"
    except Exception:
        return "Unknown"


def _get_optional_versions(env: dict[str, Any]) -> None:
    """Add optional dependency versions to environment dict."""
    try:
        import geopandas

        env["geopandas_version"] = geopandas.__version__
    except ImportError:
        pass

    try:
        import pyogrio

        env["pyogrio_version"] = pyogrio.__version__
    except ImportError:
        pass

    try:
        import fiona

        env["fiona_version"] = fiona.__version__
    except ImportError:
        pass


def get_environment_info() -> dict[str, Any]:
    """Get environment information for benchmark results."""
    env = {
        "os": platform.system(),
        "python_version": platform.python_version(),
        "duckdb_version": duckdb.__version__,
        "cpu": _get_cpu_info(),
    }

    try:
        ram_gb = psutil.virtual_memory().total / (1024**3)
        env["ram"] = f"{int(ram_gb)} GB"
    except Exception:
        env["ram"] = "Unknown"

    _get_optional_versions(env)

    return env


def benchmark_duckdb(input_path: Path, output_path: Path) -> tuple[float, float]:
    """
    Benchmark DuckDB conversion.

    Returns:
        Tuple of (elapsed_time_seconds, peak_memory_mb)
    """
    tracemalloc.start()

    start = time.perf_counter()

    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute(f"""
        COPY (SELECT * FROM ST_Read('{input_path}'))
        TO '{output_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
    """)
    conn.close()

    elapsed = time.perf_counter() - start
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    peak_memory_mb = peak / (1024 * 1024)

    return elapsed, peak_memory_mb


def benchmark_geopandas_fiona(input_path: Path, output_path: Path) -> tuple[float, float]:
    """
    Benchmark Geopandas with Fiona engine.

    Returns:
        Tuple of (elapsed_time_seconds, peak_memory_mb)
    """
    import geopandas as gpd

    tracemalloc.start()

    start = time.perf_counter()

    gdf = gpd.read_file(input_path, engine="fiona")
    gdf.to_parquet(output_path, compression="zstd")

    elapsed = time.perf_counter() - start
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    peak_memory_mb = peak / (1024 * 1024)

    return elapsed, peak_memory_mb


def benchmark_geopandas_pyogrio(input_path: Path, output_path: Path) -> tuple[float, float]:
    """
    Benchmark Geopandas with Pyogrio engine.

    Returns:
        Tuple of (elapsed_time_seconds, peak_memory_mb)
    """
    import geopandas as gpd

    tracemalloc.start()

    start = time.perf_counter()

    gdf = gpd.read_file(input_path, engine="pyogrio")
    gdf.to_parquet(output_path, compression="zstd")

    elapsed = time.perf_counter() - start
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    peak_memory_mb = peak / (1024 * 1024)

    return elapsed, peak_memory_mb


def benchmark_gdal(input_path: Path, output_path: Path) -> tuple[float, float]:
    """
    Benchmark GDAL ogr2ogr CLI tool.

    Returns:
        Tuple of (elapsed_time_seconds, peak_memory_mb)
    """
    start = time.perf_counter()

    process = subprocess.Popen(
        [
            "ogr2ogr",
            "-f",
            "Parquet",
            "-lco",
            "COMPRESSION=ZSTD",
            "-lco",
            "ROW_GROUP_SIZE=100000",
            str(output_path),
            str(input_path),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Track memory while it runs
    peak_memory = 0
    try:
        ps_process = psutil.Process(process.pid)

        while process.poll() is None:
            try:
                mem = ps_process.memory_info().rss / (1024 * 1024)
                peak_memory = max(peak_memory, mem)
            except psutil.NoSuchProcess:
                break
            time.sleep(0.05)

    except Exception:
        peak_memory = 0

    stdout, stderr = process.communicate()
    elapsed = time.perf_counter() - start

    if process.returncode != 0:
        raise RuntimeError(f"ogr2ogr failed: {stderr.decode()}")

    return elapsed, peak_memory


# Map converter names to benchmark functions
BENCHMARK_FUNCTIONS = {
    "duckdb": benchmark_duckdb,
    "geopandas_fiona": benchmark_geopandas_fiona,
    "geopandas_pyogrio": benchmark_geopandas_pyogrio,
    "gdal_ogr2ogr": benchmark_gdal,
}


def run_single_benchmark(
    converter_name: str,
    input_path: Path,
    output_dir: Path,
    iteration: int,
) -> dict[str, Any]:
    """Run a single benchmark iteration."""
    output_path = output_dir / f"{input_path.stem}_{converter_name}_iter{iteration}.parquet"

    benchmark_func = BENCHMARK_FUNCTIONS[converter_name]

    try:
        elapsed, memory = benchmark_func(input_path, output_path)

        return {
            "converter": converter_name,
            "input_file": input_path.name,
            "iteration": iteration,
            "elapsed_seconds": round(elapsed, 3),
            "memory_mb": round(memory, 2),
            "output_file": output_path.name,
            "output_size_mb": round(output_path.stat().st_size / (1024 * 1024), 2),
            "success": True,
        }
    except Exception as e:
        return {
            "converter": converter_name,
            "input_file": input_path.name,
            "iteration": iteration,
            "error": str(e),
            "success": False,
        }


def calculate_statistics(results: list[dict], converters: list[str]) -> dict[str, Any]:
    """Calculate mean and std dev for each converter."""
    stats = {}

    for converter in converters:
        converter_results = [r for r in results if r["converter"] == converter and r["success"]]

        if len(converter_results) >= 2:
            times = [r["elapsed_seconds"] for r in converter_results]
            memories = [r["memory_mb"] for r in converter_results]

            stats[converter] = {
                "mean_time": round(mean(times), 3),
                "std_time": round(stdev(times), 3),
                "mean_memory": round(mean(memories), 2),
                "std_memory": round(stdev(memories), 2),
                "iterations": len(converter_results),
            }
        elif len(converter_results) == 1:
            stats[converter] = {
                "mean_time": round(converter_results[0]["elapsed_seconds"], 3),
                "std_time": 0,
                "mean_memory": round(converter_results[0]["memory_mb"], 2),
                "std_memory": 0,
                "iterations": 1,
            }

    return stats


def format_table_output(
    stats: dict[str, Any],
    file_info: dict[str, Any],
    converters: list[str],
) -> str:
    """Format results as a human-readable table."""
    lines = []

    lines.append("=" * 70)
    lines.append("BENCHMARK RESULTS")
    lines.append("=" * 70)

    # File info
    lines.append(f"\nFile: {file_info.get('name', 'unknown')}")
    lines.append(f"  Format: {file_info.get('format', 'unknown')}")
    if "feature_count" in file_info:
        lines.append(f"  Features: {file_info['feature_count']:,}")
    lines.append(f"  Size: {file_info.get('size_mb', 0):.2f} MB")
    if "geometry_type" in file_info:
        lines.append(f"  Geometry: {file_info['geometry_type']}")
    lines.append("")

    # Results table
    lines.append(f"{'Converter':<25} {'Time (s)':<18} {'Memory (MB)':<18}")
    lines.append("-" * 61)

    fastest_converter = None
    fastest_time = float("inf")

    for converter in converters:
        if converter in stats:
            s = stats[converter]
            time_str = f"{s['mean_time']:.3f} +/- {s['std_time']:.3f}"
            memory_str = f"{s['mean_memory']:.1f} +/- {s['std_memory']:.1f}"
            lines.append(f"{CONVERTERS[converter]['name']:<25} {time_str:<18} {memory_str:<18}")

            if s["mean_time"] < fastest_time:
                fastest_time = s["mean_time"]
                fastest_converter = converter

    if fastest_converter:
        lines.append(f"\nFastest: {CONVERTERS[fastest_converter]['name']} ({fastest_time:.3f}s)")

    return "\n".join(lines)


def format_json_output(
    stats: dict[str, Any],
    file_info: dict[str, Any],
    environment: dict[str, Any],
    raw_results: list[dict],
    config: dict[str, Any],
) -> str:
    """Format results as JSON."""
    output = {
        "environment": environment,
        "file_info": file_info,
        "statistics": stats,
        "raw_results": raw_results,
        "config": config,
    }
    return json.dumps(output, indent=2)


def _check_invalid_converters(requested: list[str]) -> None:
    """Raise error if any requested converters are unknown."""
    invalid = [c for c in requested if c not in CONVERTERS]
    if invalid:
        raise click.ClickException(
            f"Unknown converters: {', '.join(invalid)}. Available: {', '.join(CONVERTERS.keys())}"
        )


def _check_unavailable_converters(requested: list[str], available: list[str]) -> None:
    """Raise error if any requested converters are not installed."""
    unavailable = [c for c in requested if c not in available]
    if unavailable:
        msgs = [f"  {c}: {CONVERTERS[c]['install']}" for c in unavailable]
        raise click.ClickException("Requested converters not available:\n" + "\n".join(msgs))


def _validate_converters(converters: list[str] | None, available: list[str]) -> list[str]:
    """Validate and filter requested converters."""
    if not converters:
        return available

    requested = [c.strip() for c in converters]
    _check_invalid_converters(requested)
    _check_unavailable_converters(requested, available)

    return [c for c in requested if c in available]


def _print_setup_info(
    input_path: Path,
    run_converters: list[str],
    iterations: int,
    warmup: bool,
    missing: list[str],
) -> None:
    """Print benchmark setup information."""
    progress(f"Benchmarking: {input_path.name}")
    progress(f"Converters: {', '.join(run_converters)}")
    progress(f"Iterations: {iterations}")
    if warmup:
        progress("Warmup: enabled")

    if missing:
        progress("\nNot installed (optional):")
        for m in missing:
            progress(f"  {m}: {CONVERTERS[m]['install']}")

    progress("\nTo install all benchmark dependencies:")
    progress("  uv pip install geoparquet-io[benchmark]")
    progress("  # or: pip install geoparquet-io[benchmark]")
    progress("")


def _run_warmup(run_converters: list[str], input_path: Path, output_dir: Path) -> None:
    """Run warmup iterations for all converters."""
    progress("Running warmup...")
    for converter in run_converters:
        output_path = output_dir / f"warmup_{converter}.parquet"
        try:
            BENCHMARK_FUNCTIONS[converter](input_path, output_path)
            if output_path.exists():
                output_path.unlink()
        except Exception:
            pass


def _run_all_benchmarks(
    run_converters: list[str],
    input_path: Path,
    output_dir: Path,
    iterations: int,
    quiet: bool,
) -> list[dict]:
    """Run all benchmark iterations and return results."""
    all_results = []

    for converter in run_converters:
        if not quiet:
            sys.stdout.write(f"  {CONVERTERS[converter]['name']}...")
            sys.stdout.flush()

        for iteration in range(1, iterations + 1):
            result = run_single_benchmark(converter, input_path, output_dir, iteration)
            all_results.append(result)

            if not quiet:
                sys.stdout.write(" ." if result["success"] else " X")
                sys.stdout.flush()

        if not quiet:
            sys.stdout.write("\n")
            sys.stdout.flush()

    return all_results


def _setup_output_dir(keep_output: str | None) -> tuple[Path, bool]:
    """Setup output directory, return (path, should_cleanup)."""
    if keep_output:
        output_dir = Path(keep_output)
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir, False
    return Path(tempfile.mkdtemp(prefix="gpq_benchmark_")), True


def _format_and_display_results(
    stats: dict,
    file_info: dict,
    environment: dict,
    all_results: list,
    config: dict,
    run_converters: list[str],
    output_format: str,
    output_json: str | None,
    quiet: bool,
) -> None:
    """Format results and display/save them."""
    if output_format == "json":
        output = format_json_output(stats, file_info, environment, all_results, config)
    else:
        output = format_table_output(stats, file_info, run_converters)

    if not quiet:
        progress(output)

    if output_json:
        json_output = format_json_output(stats, file_info, environment, all_results, config)
        Path(output_json).write_text(json_output)
        if not quiet:
            progress(f"\nResults saved to: {output_json}")


def run_benchmark(
    input_file: str,
    iterations: int = 3,
    converters: list[str] | None = None,
    output_json: str | None = None,
    keep_output: str | None = None,
    warmup: bool = True,
    output_format: str = "table",
    quiet: bool = False,
) -> dict[str, Any]:
    """
    Run benchmark on input file with specified converters.

    Returns:
        Dictionary with benchmark results
    """
    input_path = Path(input_file)

    if not input_path.exists():
        raise click.ClickException(f"Input file not found: {input_file}")

    available, missing = detect_available_converters()
    run_converters = _validate_converters(converters, available)

    if not run_converters:
        raise click.ClickException("No converters available to run")

    if not quiet:
        _print_setup_info(input_path, run_converters, iterations, warmup, missing)

    file_info = get_file_info(input_path)
    output_dir, cleanup_output = _setup_output_dir(keep_output)

    try:
        if warmup and not quiet:
            _run_warmup(run_converters, input_path, output_dir)

        all_results = _run_all_benchmarks(run_converters, input_path, output_dir, iterations, quiet)

        stats = calculate_statistics(all_results, run_converters)
        environment = get_environment_info()
        config = {"iterations": iterations, "converters": run_converters, "warmup": warmup}

        _format_and_display_results(
            stats,
            file_info,
            environment,
            all_results,
            config,
            run_converters,
            output_format,
            output_json,
            quiet,
        )

        return {
            "statistics": stats,
            "file_info": file_info,
            "environment": environment,
            "raw_results": all_results,
            "config": config,
        }

    finally:
        if cleanup_output and output_dir.exists():
            shutil.rmtree(output_dir)
