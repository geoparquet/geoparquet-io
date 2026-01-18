"""Core PMTiles generation logic using tippecanoe subprocess."""

import shutil
import subprocess
import sys
from pathlib import Path


class TippecanoeNotFoundError(Exception):
    """Raised when tippecanoe is not found in PATH."""

    def __init__(self):
        super().__init__(
            "tippecanoe not found in PATH.\n\n"
            "To use gpio pmtiles, install tippecanoe:\n"
            "  macOS:  brew install tippecanoe\n"
            "  Ubuntu: sudo apt install tippecanoe\n"
            "  Source: https://github.com/felt/tippecanoe#installation\n\n"
            "Alternatively, use the streaming approach:\n"
            "  gpio convert geojson data.parquet | tippecanoe -P -o output.pmtiles"
        )


def _validate_path(path: str) -> None:
    """
    Validate file path to prevent shell injection.

    Raises:
        ValueError: If path contains shell metacharacters
    """
    # Check for shell metacharacters that could be dangerous
    dangerous_chars = [";", "|", "&", "$", "`", "\n", "\r"]
    for char in dangerous_chars:
        if char in path:
            raise ValueError(
                f"Path contains dangerous character '{char}': {path}\n"
                "File paths must not contain shell metacharacters."
            )


def _get_gpio_executable() -> str:
    """Get the path to the gpio executable in the current Python environment."""
    # Get the directory where the current Python interpreter is located
    python_bin_dir = Path(sys.executable).parent

    # Look for gpio in the same directory as the Python executable
    gpio_path = python_bin_dir / "gpio"

    if gpio_path.exists() and gpio_path.is_file():
        return str(gpio_path)

    # Fallback to searching PATH
    gpio_in_path = shutil.which("gpio")
    if gpio_in_path:
        return gpio_in_path

    # Last resort: just return 'gpio' and hope for the best
    return "gpio"


def _check_tippecanoe() -> bool:
    """Check if tippecanoe is available in PATH."""
    return shutil.which("tippecanoe") is not None


def _build_gpio_commands(
    input_path: str,
    bbox: str | None,
    where: str | None,
    include_cols: str | None,
    precision: int,
    verbose: bool,
    profile: str | None,
    src_crs: str | None,
) -> list[list[str]]:
    """
    Build the gpio command(s) for GeoJSON conversion.

    Returns a list of commands to be piped together.
    If filters or reprojection needed, returns [reproject/extract_cmd, convert_cmd].
    Otherwise, returns [convert_cmd].
    """
    gpio_exe = _get_gpio_executable()

    # Check if we need reprojection or filtering
    needs_reproject = src_crs is not None
    needs_extract = bbox or where or include_cols

    if needs_reproject or needs_extract:
        # Start with input - may need reprojection first, then extract
        commands = []

        # If we need to reproject, do that first
        if needs_reproject:
            reproject_cmd = [
                gpio_exe,
                "convert",
                "reproject",
                input_path,
                "-",  # Output to stdout
                "--dst-crs",
                "EPSG:4326",
                "--src-crs",
                src_crs,
            ]
            if verbose:
                reproject_cmd.append("--verbose")
            if profile:
                reproject_cmd.extend(["--profile", profile])
            commands.append(reproject_cmd)

            # Next command reads from stdin
            next_input = "-"
        else:
            next_input = input_path

        # If we have filters, apply extract
        if needs_extract:
            extract_cmd = [gpio_exe, "extract", next_input]

            if bbox:
                extract_cmd.extend(["--bbox", bbox])
            if where:
                extract_cmd.extend(["--where", where])
            if include_cols:
                extract_cmd.extend(["--include-cols", include_cols])
            if verbose:
                extract_cmd.append("--verbose")
            if profile and not needs_reproject:  # Only if not already added
                extract_cmd.extend(["--profile", profile])

            commands.append(extract_cmd)
            next_input = "-"

        # Final step: convert to GeoJSON
        convert_cmd = [gpio_exe, "convert", "geojson", next_input, "--precision", str(precision)]
        commands.append(convert_cmd)

        return commands

    # No filtering or reprojection - just convert directly
    convert_cmd = [gpio_exe, "convert", "geojson", input_path, "--precision", str(precision)]

    if verbose:
        convert_cmd.append("--verbose")
    if profile:
        convert_cmd.extend(["--profile", profile])

    return [convert_cmd]


def _build_tippecanoe_command(
    output_path: str,
    layer: str | None,
    min_zoom: int | None,
    max_zoom: int | None,
    verbose: bool,
) -> list[str]:
    """Build the tippecanoe command."""
    cmd = ["tippecanoe", "-P", "-o", output_path]

    # Add layer name
    if layer:
        cmd.extend(["-l", layer])
    else:
        # Default layer name from output filename
        layer_name = Path(output_path).stem
        cmd.extend(["-l", layer_name])

    # Add zoom levels
    if min_zoom is not None and max_zoom is not None:
        cmd.extend(["-Z", str(min_zoom), "-z", str(max_zoom)])
    elif max_zoom is not None:
        cmd.extend(["-z", str(max_zoom)])
    else:
        # Use tippecanoe's auto zoom detection
        cmd.append("-zg")

    # Recommended defaults for PMTiles
    cmd.append("--drop-densest-as-needed")

    if verbose:
        cmd.append("--progress-interval=1")

    return cmd


def _run_pipeline(
    gpio_commands: list[list[str]],
    tippecanoe_cmd: list[str],
    verbose: bool,
) -> None:
    """
    Execute the pipeline of commands.

    Args:
        gpio_commands: List of gpio commands to pipe together
        tippecanoe_cmd: Tippecanoe command to run at the end
        verbose: Whether to show command output
    """
    if verbose:
        # Show the commands being executed
        if len(gpio_commands) == 1:
            cmd_str = " ".join(gpio_commands[0])
            print(f"Running: {cmd_str} | {' '.join(tippecanoe_cmd)}", file=sys.stderr)
        else:
            cmd_str = " | ".join(" ".join(cmd) for cmd in gpio_commands)
            print(f"Running: {cmd_str} | {' '.join(tippecanoe_cmd)}", file=sys.stderr)

    processes = []

    try:
        # Create all gpio processes in the pipeline
        for i, cmd in enumerate(gpio_commands):
            stdin_source = processes[-1].stdout if processes else None

            proc = subprocess.Popen(
                cmd,
                stdin=stdin_source,
                stdout=subprocess.PIPE,
                stderr=None if verbose else subprocess.PIPE,
            )
            processes.append(proc)

            # Close the previous process's stdout to allow SIGPIPE
            if i > 0 and processes[-2].stdout:
                processes[-2].stdout.close()

        # Final step: tippecanoe reads from the last gpio process
        tippecanoe_proc = subprocess.Popen(
            tippecanoe_cmd,
            stdin=processes[-1].stdout if processes else None,
            stdout=None if verbose else subprocess.PIPE,
            stderr=None,  # tippecanoe writes progress to stderr
        )
        processes.append(tippecanoe_proc)

        # Close last gpio process's stdout
        if len(processes) > 1 and processes[-2].stdout:
            processes[-2].stdout.close()

        # Wait for the final process to complete
        tippecanoe_proc.communicate()

        # Check for errors in the pipeline
        if tippecanoe_proc.returncode != 0:
            raise RuntimeError(f"tippecanoe failed with exit code {tippecanoe_proc.returncode}")

        # Check earlier processes in the pipeline
        for proc in processes[:-1]:  # Skip tippecanoe (already checked)
            proc.wait()
            if proc.returncode != 0:
                cmd_name = proc.args[0] if hasattr(proc, "args") else "command"
                raise RuntimeError(f"{cmd_name} failed with exit code {proc.returncode}")

    except KeyboardInterrupt:
        # Clean up processes on interrupt
        for proc in processes:
            proc.terminate()
        raise
    except Exception:
        # Clean up on any error
        for proc in processes:
            if proc.poll() is None:
                proc.terminate()
        raise


def create_pmtiles_from_geoparquet(
    input_path: str,
    output_path: str,
    *,
    layer: str | None = None,
    min_zoom: int | None = None,
    max_zoom: int | None = None,
    bbox: str | None = None,
    where: str | None = None,
    include_cols: str | None = None,
    precision: int = 6,
    verbose: bool = False,
    profile: str | None = None,
    src_crs: str | None = None,
) -> None:
    """
    Create PMTiles using gpio streaming + tippecanoe subprocess.

    This function orchestrates subprocesses to:
    1. Reproject if needed (gpio convert reproject)
    2. Filter/transform if needed (gpio extract)
    3. Stream GeoJSON from GeoParquet (gpio convert geojson)
    4. Generate PMTiles using tippecanoe

    Args:
        input_path: Path to input GeoParquet file
        output_path: Path for output PMTiles file
        layer: Layer name in PMTiles (defaults to output filename)
        min_zoom: Minimum zoom level (optional)
        max_zoom: Maximum zoom level (optional, auto-detected if not set)
        bbox: Bounding box filter as "minx,miny,maxx,maxy"
        where: SQL WHERE clause for filtering
        include_cols: Comma-separated list of columns to include
        precision: Coordinate decimal precision (default: 6)
        verbose: Enable verbose output
        profile: AWS profile name for S3 files
        src_crs: Source CRS for reprojection to WGS84 (if metadata is wrong)

    Raises:
        TippecanoeNotFoundError: If tippecanoe is not in PATH
        ValueError: If paths contain shell metacharacters
        RuntimeError: If any subprocess fails
    """
    # Validate paths to prevent shell injection
    _validate_path(input_path)
    _validate_path(output_path)

    # Check tippecanoe availability
    if not _check_tippecanoe():
        raise TippecanoeNotFoundError()

    # Build commands
    gpio_commands = _build_gpio_commands(
        input_path, bbox, where, include_cols, precision, verbose, profile, src_crs
    )

    tippecanoe_cmd = _build_tippecanoe_command(output_path, layer, min_zoom, max_zoom, verbose)

    # Run the pipeline
    _run_pipeline(gpio_commands, tippecanoe_cmd, verbose)

    if verbose:
        print(f"Successfully created {output_path}", file=sys.stderr)
