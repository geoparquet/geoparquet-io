"""``gpio benchmark`` - measure GeoParquet read performance.

Registered on the root ``gpio`` group by ``cli/main.py`` via
``cli.add_command(benchmark)``. This module must not import ``cli.main``: the
dependency runs one way, ``main`` -> ``commands`` -> ``_shared``/``decorators``.
"""

import click

from geoparquet_io.cli.decorators import handle_geoparquet_errors, verbose_option
from geoparquet_io.core.logging_config import configure_verbose


# Benchmark commands group
@click.group()
@click.pass_context
def benchmark(ctx):
    """Benchmark GeoParquet performance.

    Commands for measuring and comparing performance of GeoParquet operations.

    \b
    Subcommands:
      suite    Run comprehensive benchmark suite
      compare  Compare converter performance on a single file
      explain  Show DuckDB query plan analysis (EXPLAIN ANALYZE)
      report   View and compare benchmark results
    """
    ctx.ensure_object(dict)


@benchmark.command("compare")
@handle_geoparquet_errors
@click.argument("input_file", type=click.Path(exists=True))
@click.option(
    "--iterations",
    "-n",
    default=3,
    type=int,
    help="Number of iterations per converter (default: 3)",
)
@click.option(
    "--converters",
    "-c",
    help="Comma-separated list of converters to run (default: all available)",
)
@click.option(
    "--output-json",
    "-o",
    type=click.Path(),
    help="Save results to JSON file",
)
@click.option(
    "--keep-output",
    type=click.Path(),
    help="Directory to save converted files (default: temp dir, cleaned up)",
)
@click.option(
    "--warmup/--no-warmup",
    default=True,
    help="Run warmup iteration before timing (default: enabled)",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format (default: table)",
)
@click.option(
    "--quiet",
    "-q",
    is_flag=True,
    help="Suppress progress output, show only results",
)
@verbose_option
def benchmark_compare(
    input_file,
    iterations,
    converters,
    output_json,
    keep_output,
    warmup,
    output_format,
    quiet,
    verbose,
):
    """
    Compare converter performance on a single file.

    Tests different conversion methods (DuckDB, GeoPandas, GDAL) on an input
    geospatial file and reports time and memory usage.

    \b
    Available converters:
      - duckdb: DuckDB spatial extension (always available)
      - geopandas_fiona: GeoPandas with Fiona engine
      - geopandas_pyogrio: GeoPandas with PyOGRIO engine
      - gdal_ogr2ogr: GDAL ogr2ogr CLI

    \b
    Example:
        gpio benchmark compare input.geojson --iterations 5
    """
    configure_verbose(verbose)
    from geoparquet_io.core.benchmark import run_benchmark

    # Parse converters string to list
    converter_list = None
    if converters:
        converter_list = [c.strip() for c in converters.split(",")]

    run_benchmark(
        input_file=input_file,
        iterations=iterations,
        converters=converter_list,
        output_json=output_json,
        keep_output=keep_output,
        warmup=warmup,
        output_format=output_format,
        quiet=quiet,
    )


@benchmark.command("suite")
@handle_geoparquet_errors
@click.option(
    "--operations",
    type=click.Choice(["core", "full"]),
    default="core",
    help="Operation set to run (default: core)",
)
@click.option(
    "--files",
    multiple=True,
    help="Files to test (paths or size names: tiny, small, medium, large, xlarge)",
)
@click.option(
    "--iterations",
    "-n",
    default=3,
    help="Runs per operation (default: 3)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    help="Write results to JSON file",
)
@click.option(
    "--profile",
    is_flag=True,
    help="Enable cProfile profiling to diagnose performance bottlenecks",
)
@click.option(
    "--profile-dir",
    type=click.Path(),
    default="./profiles",
    help="Directory for profile output files (default: ./profiles)",
)
@verbose_option
def benchmark_suite(
    operations,
    files,
    iterations,
    output,
    profile,
    profile_dir,
    verbose,
):
    """
    Run comprehensive benchmark suite.

    Tests gpio operations across multiple file sizes with timing and memory tracking.

    \b
    Example:
        gpio benchmark suite --operations core --output results.json
        gpio benchmark suite --files input.parquet --output results.json
        gpio benchmark suite --profile --profile-dir ./my-profiles
    """
    from pathlib import Path

    configure_verbose(verbose)
    from geoparquet_io.benchmarks.config import CORE_OPERATIONS, FULL_OPERATIONS
    from geoparquet_io.core.benchmark_suite import run_benchmark_suite
    from geoparquet_io.core.logging_config import info, progress, success

    # Determine operations
    ops = CORE_OPERATIONS if operations == "core" else FULL_OPERATIONS

    # Resolve files
    if not files:
        raise click.ClickException("No files specified. Use --files with paths.")

    input_files = []
    for f in files:
        path = Path(f)
        if path.exists():
            input_files.append(path)
        else:
            raise click.ClickException(f"File not found: {f}")

    # Setup profiling if requested
    profile_path = None
    if profile:
        profile_path = Path(profile_dir)
        profile_path.mkdir(parents=True, exist_ok=True)
        info(f"Profiling enabled - output directory: {profile_path}")

    progress(
        f"Running benchmark suite: {len(ops)} operations, "
        f"{len(input_files)} files, {iterations} iterations"
    )

    result = run_benchmark_suite(
        input_files=input_files,
        operations=ops,
        iterations=iterations,
        verbose=verbose,
        profile=profile,
        profile_dir=profile_path,
    )

    # Display summary
    success_count = sum(1 for r in result.results if r.success)
    total_count = len(result.results)
    progress(f"\nCompleted: {success_count}/{total_count} benchmarks")

    # Show profile summary if profiling was enabled
    if profile:
        profile_files = [Path(p) for r in result.results if (p := r.details.get("profile_path"))]

        if profile_files:
            info(f"\nGenerated {len(profile_files)} profile files in {profile_path}")
            info("\nTo view profile details, use:")
            info(f"  uv run python -m pstats {profile_files[0]}")
            info("\nOr generate a summary:")
            info("  from geoparquet_io.benchmarks.profile_report import format_profile_stats")
            info(f"  print(format_profile_stats('{profile_files[0]}'))")

    # Save if requested
    if output:
        Path(output).write_text(result.to_json())
        success(f"Results saved to {output}")


@benchmark.command("explain")
@handle_geoparquet_errors
@click.argument("input_file", type=click.Path(exists=True))
@click.option(
    "--query",
    "-q",
    type=str,
    default=None,
    help="Custom SQL query. Use {file} as placeholder for file path.",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format (default: table)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default=None,
    help="Save results to JSON file",
)
@verbose_option
def benchmark_explain(
    input_file,
    query,
    output_format,
    output,
    verbose,
):
    """
    Show DuckDB query plan analysis (EXPLAIN ANALYZE).

    Runs EXPLAIN ANALYZE on a DuckDB query against a GeoParquet file to reveal
    per-operator timing, cardinality, filter pushdown, and row group pruning.

    \b
    Example:
        gpio benchmark explain input.parquet
        gpio benchmark explain input.parquet --format json
        gpio benchmark explain input.parquet --query "SELECT * FROM read_parquet('{file}') WHERE id > 10"
        gpio benchmark explain input.parquet --output plan.json
    """
    configure_verbose(verbose)
    from geoparquet_io.core.benchmark import (
        explain_analyze,
        format_explain_output,
    )
    from geoparquet_io.core.logging_config import progress

    result = explain_analyze(
        file_path=input_file,
        query=query,
    )

    formatted = format_explain_output(result, output_format=output_format)
    progress(formatted)

    if output:
        from pathlib import Path

        json_output = format_explain_output(result, output_format="json")
        Path(output).write_text(json_output)
        progress(f"\nResults saved to: {output}")


@benchmark.command("report")
@handle_geoparquet_errors
@click.argument("result_files", nargs=-1, type=click.Path(exists=True))
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format (default: table)",
)
@verbose_option
def benchmark_report(
    result_files,
    output_format,
    verbose,
):
    """
    View and compare benchmark results.

    \b
    Example:
        gpio benchmark report results.json
        gpio benchmark report results/*.json
    """
    import json

    configure_verbose(verbose)
    from geoparquet_io.core.benchmark_report import format_table
    from geoparquet_io.core.benchmark_suite import BenchmarkResult
    from geoparquet_io.core.logging_config import progress

    if not result_files:
        raise click.ClickException("No result files provided")

    # Load results
    all_results = []
    for rf in result_files:
        with open(rf) as f:
            data = json.load(f)
            for r in data.get("results", []):
                all_results.append(
                    BenchmarkResult(
                        operation=r["operation"],
                        file=r["file"],
                        time_seconds=r["time_seconds"],
                        peak_rss_memory_mb=r["peak_rss_memory_mb"],
                        success=r["success"],
                        error=r.get("error"),
                        details=r.get("details", {}),
                    )
                )

    if output_format == "json":
        click.echo(json.dumps([r.__dict__ for r in all_results], indent=2, default=str))
    else:
        progress(format_table(all_results))
