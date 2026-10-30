"""
Benchmark utilities for comparing GeoParquet conversion methods.

Tests available converters and reports performance metrics.
Includes EXPLAIN ANALYZE support for DuckDB query plan analysis.
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

import duckdb
import psutil

from geoparquet_io.core.compression import resolve_codec
from geoparquet_io.core.duckdb_utils import (
    duckdb_capabilities,
    fetch_one,
    get_duckdb_connection,
    query_schema,
    quote_identifier,
    sql_path,
)
from geoparquet_io.core.exceptions import FileNotFoundGeoParquetError, GeoParquetError
from geoparquet_io.core.geometry_detection import STANDARD_GEOMETRY_NAMES
from geoparquet_io.core.logging_config import progress
from geoparquet_io.core.remote import needs_httpfs

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
        # Context-managed so the handle is closed on the error paths too; a
        # leaked DuckDB handle breaks file cleanup on Windows.
        with get_duckdb_connection(load_httpfs=False) as conn:
            # Get feature count and basic info
            result = fetch_one(
                conn.execute(f"""
                SELECT COUNT(*) as cnt
                FROM ST_Read({sql_path(filepath)})
            """)
            )

            feature_count = result[0] if result else 0

            # Get schema to find geometry column
            names = query_schema(conn, f"SELECT * FROM ST_Read({sql_path(filepath)})").names

            # Find geometry column (common names)
            geom_col = None
            for name in names:
                if name.lower() in [n.lower() for n in STANDARD_GEOMETRY_NAMES]:
                    geom_col = name
                    break

            # Try to get geometry type from first row
            geom_type = "unknown"
            if geom_col:
                geom_result = fetch_one(
                    conn.execute(f"""
                    SELECT ST_GeometryType({quote_identifier(geom_col)}) as geom_type
                    FROM ST_Read({sql_path(filepath)})
                    LIMIT 1
                """)
                )
                geom_type = geom_result[0] if geom_result else "unknown"

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
        "duckdb_version": duckdb_capabilities().version,
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
    # Connect before the timer starts. Factory setup (extension install/load,
    # session settings, ambient S3 config) is fixed overhead that the GeoPandas
    # and pyogrio arms never pay, so timing it here would bias the comparison
    # against DuckDB. Only the COPY is measured.
    conn = get_duckdb_connection(load_httpfs=False)

    tracemalloc.start()
    start = time.perf_counter()
    try:
        # DuckDB's fast ZSTD 3, named through the owner rather than by default.
        codec_options = ", ".join(resolve_codec("ZSTD", 3, context="write").duckdb_copy_options())
        conn.execute(f"""
            COPY (SELECT * FROM ST_Read({sql_path(input_path)}))
            TO {sql_path(output_path)}
            (FORMAT PARQUET, {codec_options}, ROW_GROUP_SIZE 100000)
        """)
        elapsed = time.perf_counter() - start
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        conn.close()

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
        raise GeoParquetError(
            f"Unknown converters: {', '.join(invalid)}. Available: {', '.join(CONVERTERS.keys())}"
        )


def _check_unavailable_converters(requested: list[str], available: list[str]) -> None:
    """Raise error if any requested converters are not installed."""
    unavailable = [c for c in requested if c not in available]
    if unavailable:
        msgs = [f"  {c}: {CONVERTERS[c]['install']}" for c in unavailable]
        raise GeoParquetError("Requested converters not available:\n" + "\n".join(msgs))


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
        raise FileNotFoundGeoParquetError(input_file)

    available, missing = detect_available_converters()
    run_converters = _validate_converters(converters, available)

    if not run_converters:
        raise GeoParquetError("No converters available to run")

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


def _normalize_extra_info(extra_info: dict | str) -> str:
    """Convert extra_info to a string regardless of input type."""
    if isinstance(extra_info, dict):
        parts = []
        for key, value in extra_info.items():
            if isinstance(value, list):
                parts.append(f"{key}: {', '.join(str(v) for v in value)}")
            else:
                parts.append(f"{key}: {value}")
        return "\n".join(parts)
    return str(extra_info) if extra_info else ""


def _collect_operators(node: dict, depth: int = 0) -> list[dict]:
    """Recursively collect operators from a query plan tree."""
    # DuckDB 1.x profiling keys, the simplified format, then DuckDB 2.0's keys
    # ("type", and "intermediate_rows" for the rows an operator emits).
    name = node.get("operator_name") or node.get("name") or node.get("type", "UNKNOWN")
    timing = node.get("operator_timing") or node.get("timing", 0.0)
    cardinality = (
        node.get("operator_cardinality")
        or node.get("cardinality")
        or node.get("intermediate_rows", 0)
    )
    raw_extra = node.get("extra_info", "")

    operator = {
        "name": name,
        "timing": timing,
        "cardinality": cardinality,
        "extra_info": _normalize_extra_info(raw_extra),
        "depth": depth,
    }
    # DuckDB 2.0 reports row groups as fields of the scan, not in extra_info.
    if "row_groups_scanned" in node and "total_row_groups_to_scan" in node:
        operator["row_groups"] = (node["row_groups_scanned"], node["total_row_groups_to_scan"])
    operators = [operator]

    for child in node.get("children", []):
        operators.extend(_collect_operators(child, depth + 1))

    return operators


def _detect_filter_pushdown(operators: list[dict]) -> bool:
    """Check if any operator indicates filter pushdown."""
    for op in operators:
        extra = op.get("extra_info", "")
        if isinstance(extra, dict):
            extra = str(extra)
        if "filter" in extra.lower():
            return True
        if op["name"] == "FILTER":
            return True
    return False


def _detect_row_group_skip(operators: list[dict]) -> bool:
    """Check if any operator shows row group skipping."""
    for op in operators:
        if "row_groups" in op:
            scanned, total = op["row_groups"]
            if scanned < total:
                return True
            continue
        extra = op.get("extra_info", "")
        if isinstance(extra, dict):
            extra = str(extra)
        if "Row Groups" in extra or "Row Group" in extra:
            parts = extra.split("Row Group")
            for part in parts[1:]:
                # Look for patterns like "1/3" meaning 1 of 3 scanned
                stripped = part.lstrip("s:").strip()
                if "/" in stripped:
                    nums = stripped.split("/")[0:2]
                    try:
                        scanned = int(nums[0].strip())
                        total = int(nums[1].strip().split()[0])
                        if scanned < total:
                            return True
                    except (ValueError, IndexError):
                        pass
    return False


def parse_query_plan(plan: dict) -> dict:
    """
    Parse a DuckDB EXPLAIN ANALYZE JSON plan into a structured result.

    Args:
        plan: The JSON query plan from DuckDB profiling output.

    Returns:
        Dictionary with operators list, filter/skip detection, and total time.
    """
    # DuckDB 2.0 nests the tree under "operator", a list of root operators.
    roots = plan["operator"] if isinstance(plan.get("operator"), list) else [plan]
    operators = [op for root in roots for op in _collect_operators(root)]
    total_time = sum(op["timing"] for op in operators)

    return {
        "operators": operators,
        "has_filter_pushdown": _detect_filter_pushdown(operators),
        "row_groups_skipped": _detect_row_group_skip(operators),
        "total_time": total_time,
    }


def _format_explain_table(parsed: dict) -> str:
    """Format explain results as a human-readable table."""
    lines = []
    lines.append("=" * 70)
    lines.append("QUERY PLAN ANALYSIS")
    lines.append("=" * 70)
    lines.append("")

    # Operator table
    lines.append(f"{'Operator':<35} {'Time (s)':<12} {'Rows':<12}")
    lines.append("-" * 59)

    for op in parsed["operators"]:
        indent = "  " * op["depth"]
        name = f"{indent}{op['name']}"
        lines.append(f"{name:<35} {op['timing']:<12.6f} {op['cardinality']:<12}")

        # Show extra info if present (indented below)
        if op["extra_info"]:
            for info_line in op["extra_info"].split("\n"):
                info_line = info_line.strip()
                if info_line:
                    lines.append(f"  {indent}  {info_line}")

    lines.append("")
    lines.append(f"Total time: {parsed['total_time']:.6f}s")
    lines.append("")

    # Summary
    lines.append("Observations:")
    pushdown_status = "detected" if parsed["has_filter_pushdown"] else "not detected"
    lines.append(f"  Filter pushdown: {pushdown_status}")
    skip_status = "detected" if parsed["row_groups_skipped"] else "not detected"
    lines.append(f"  Row group pruning: {skip_status}")

    return "\n".join(lines)


def format_explain_output(parsed: dict, output_format: str = "table") -> str:
    """
    Format explain results for display.

    Args:
        parsed: Parsed query plan from parse_query_plan().
        output_format: Either "table" or "json".

    Returns:
        Formatted string.
    """
    if output_format == "json":
        return json.dumps(parsed, indent=2)
    return _format_explain_table(parsed)


def _get_explain_connection(file_path: str) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection configured for EXPLAIN ANALYZE.

    Uses the shared get_duckdb_connection helper to ensure consistent
    handling of local files and remote URLs (S3, HTTP, etc.).
    """
    conn = get_duckdb_connection(
        load_spatial=True,
        load_httpfs=needs_httpfs(file_path),
    )
    conn.execute("SET enable_profiling = 'json';")
    conn.execute("SET profiling_output = '';")
    return conn


def _build_explain_query(file_path: str, query: str | None) -> str:
    """Build the EXPLAIN ANALYZE query string."""
    if query:
        # ``{file}`` is the whole sql_path literal, quotes included. The quoted
        # spelling ``'{file}'`` the docs used to show is the same literal, so
        # it is replaced first, as one unit.
        literal = sql_path(file_path)
        sql = query.replace("'{file}'", literal).replace("{file}", literal)
    else:
        sql = f"SELECT * FROM read_parquet({sql_path(file_path)})"
    return f"EXPLAIN ANALYZE {sql}"


def explain_analyze(
    file_path: str,
    query: str | None = None,
) -> dict:
    """
    Run EXPLAIN ANALYZE on a DuckDB query against a Parquet file.

    Shows per-operator timing, estimated vs actual cardinality,
    and detects filter pushdown and row group pruning.

    Args:
        file_path: Path to the input Parquet file (local path or URL).
        query: Optional SQL query. ``{file}`` is replaced by the file path as a
               quoted, escaped SQL string literal, so write
               ``read_parquet({file})``; ``'{file}'`` means the same. Defaults
               to SELECT * FROM read_parquet({file}).

    Returns:
        Parsed query plan dictionary with operators, timing, and analysis.

    Raises:
        ValueError: If the file does not exist or query fails.
    """
    conn = _get_explain_connection(file_path)

    try:
        explain_query = _build_explain_query(file_path, query)
        result = conn.execute(explain_query)
        rows = result.fetchall()

        # DuckDB EXPLAIN ANALYZE returns rows with explain_key and explain_value
        # The JSON profile is in the second column of the last row
        raw_plan = ""
        plan_json = None

        for row in rows:
            # Try to find JSON content in the row
            for col in row:
                if isinstance(col, str):
                    raw_plan = col
                    try:
                        plan_json = json.loads(col)
                    except (json.JSONDecodeError, ValueError):
                        pass

        if plan_json:
            parsed = parse_query_plan(plan_json)
        else:
            # Fall back to text-based parsing if JSON is not available
            parsed = {
                "operators": [
                    {
                        "name": "QUERY_RESULT",
                        "timing": 0.0,
                        "cardinality": 0,
                        "extra_info": raw_plan,
                        "depth": 0,
                    }
                ],
                "has_filter_pushdown": "filter" in raw_plan.lower(),
                "row_groups_skipped": False,
                "total_time": 0.0,
            }

        parsed["raw_plan"] = raw_plan
        return parsed

    except duckdb.IOException as e:
        # Convert DuckDB file errors to ValueError for cleaner API
        raise ValueError(f"File not found or inaccessible: {file_path}") from e
    except duckdb.Error as e:
        # Convert other DuckDB errors to ValueError
        raise ValueError(f"Query failed: {e}") from e
    finally:
        conn.close()
