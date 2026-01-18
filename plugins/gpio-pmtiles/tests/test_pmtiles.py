"""Tests for gpio-pmtiles plugin."""

import shutil
import subprocess

import pytest


def has_tippecanoe():
    """Check if tippecanoe is available."""
    return shutil.which("tippecanoe") is not None


def has_gpio():
    """Check if gpio is available."""
    return shutil.which("gpio") is not None


@pytest.mark.skipif(not has_gpio(), reason="gpio not installed")
def test_plugin_loaded():
    """Test that the pmtiles plugin is loaded."""
    result = subprocess.run(
        ["gpio", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "pmtiles" in result.stdout


@pytest.mark.skipif(not has_gpio(), reason="gpio not installed")
def test_pmtiles_help():
    """Test that pmtiles help works."""
    result = subprocess.run(
        ["gpio", "pmtiles", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "PMTiles generation commands" in result.stdout


@pytest.mark.skipif(not has_gpio(), reason="gpio not installed")
def test_create_help():
    """Test that pmtiles create help works."""
    result = subprocess.run(
        ["gpio", "pmtiles", "create", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "Create PMTiles from GeoParquet file" in result.stdout
    assert "--layer" in result.stdout
    assert "--bbox" in result.stdout


@pytest.mark.skipif(not has_tippecanoe(), reason="tippecanoe not installed")
def test_tippecanoe_not_found_error():
    """Test error message when tippecanoe is not found."""
    from gpio_pmtiles.core import TippecanoeNotFoundError

    error = TippecanoeNotFoundError()
    error_msg = str(error)

    assert "tippecanoe not found" in error_msg
    assert "brew install tippecanoe" in error_msg
    assert "sudo apt install tippecanoe" in error_msg


def test_gpio_executable_detection():
    """Test that gpio executable is correctly detected."""
    from gpio_pmtiles.core import _get_gpio_executable

    gpio_exe = _get_gpio_executable()
    assert gpio_exe is not None
    assert isinstance(gpio_exe, str)
    assert len(gpio_exe) > 0


def test_build_gpio_commands_simple():
    """Test building simple gpio convert command."""
    from gpio_pmtiles.core import _build_gpio_commands

    commands = _build_gpio_commands(
        input_path="input.parquet",
        bbox=None,
        where=None,
        include_cols=None,
        precision=6,
        verbose=False,
        profile=None,
    )

    assert len(commands) == 1
    assert "convert" in commands[0]
    assert "geojson" in commands[0]
    assert "input.parquet" in commands[0]
    assert "--precision" in commands[0]
    assert "6" in commands[0]


def test_build_gpio_commands_with_filters():
    """Test building gpio commands with filters."""
    from gpio_pmtiles.core import _build_gpio_commands

    commands = _build_gpio_commands(
        input_path="input.parquet",
        bbox="-122,37,-121,38",
        where="population > 1000",
        include_cols="name,type",
        precision=5,
        verbose=True,
        profile="my-profile",
    )

    assert len(commands) == 2

    # Extract command
    extract_cmd = commands[0]
    assert "extract" in extract_cmd
    assert "input.parquet" in extract_cmd
    assert "--bbox" in extract_cmd
    assert "-122,37,-121,38" in extract_cmd
    assert "--where" in extract_cmd
    assert "population > 1000" in extract_cmd
    assert "--include-cols" in extract_cmd
    assert "name,type" in extract_cmd
    assert "--verbose" in extract_cmd
    assert "--profile" in extract_cmd
    assert "my-profile" in extract_cmd

    # Convert command
    convert_cmd = commands[1]
    assert "convert" in convert_cmd
    assert "geojson" in convert_cmd
    assert "-" in convert_cmd  # Reading from stdin
    assert "--precision" in convert_cmd
    assert "5" in convert_cmd


def test_build_tippecanoe_command_basic():
    """Test building basic tippecanoe command."""
    from gpio_pmtiles.core import _build_tippecanoe_command

    cmd = _build_tippecanoe_command(
        output_path="output.pmtiles",
        layer="test_layer",
        min_zoom=None,
        max_zoom=None,
        verbose=False,
    )

    assert "tippecanoe" in cmd
    assert "-P" in cmd  # Parallel mode
    assert "-o" in cmd
    assert "output.pmtiles" in cmd
    assert "-l" in cmd
    assert "test_layer" in cmd
    assert "-zg" in cmd  # Auto zoom detection
    assert "--drop-densest-as-needed" in cmd


def test_build_tippecanoe_command_with_zoom():
    """Test building tippecanoe command with explicit zoom levels."""
    from gpio_pmtiles.core import _build_tippecanoe_command

    cmd = _build_tippecanoe_command(
        output_path="output.pmtiles",
        layer="test_layer",
        min_zoom=0,
        max_zoom=14,
        verbose=True,
    )

    assert "-Z" in cmd
    assert "0" in cmd
    assert "-z" in cmd
    assert "14" in cmd
    assert "-zg" not in cmd  # No auto detection when explicit
    assert "--progress-interval=1" in cmd  # Verbose mode
