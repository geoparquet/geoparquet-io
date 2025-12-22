"""Tests for cli/decorators.py module."""

import click
from click.testing import CliRunner

from geoparquet_io.cli.decorators import (
    bbox_option,
    dry_run_option,
    geoparquet_version_option,
    prefix_option,
    profile_option,
    verbose_option,
)


class TestBboxOption:
    """Tests for bbox_option decorator (covers line 126)."""

    def test_bbox_option_adds_flag(self):
        """Test that bbox_option adds --add-bbox flag to command."""

        @click.command()
        @bbox_option
        def test_cmd(add_bbox):
            if add_bbox:
                click.echo("bbox enabled")
            else:
                click.echo("bbox disabled")

        runner = CliRunner()

        # Test with flag
        result = runner.invoke(test_cmd, ["--add-bbox"])
        assert result.exit_code == 0
        assert "bbox enabled" in result.output

        # Test without flag
        result = runner.invoke(test_cmd, [])
        assert result.exit_code == 0
        assert "bbox disabled" in result.output


class TestVerboseOption:
    """Tests for verbose_option decorator."""

    def test_verbose_option_adds_flag(self):
        """Test that verbose_option adds -v/--verbose flag."""

        @click.command()
        @verbose_option
        def test_cmd(verbose):
            if verbose:
                click.echo("verbose")
            else:
                click.echo("quiet")

        runner = CliRunner()

        result = runner.invoke(test_cmd, ["--verbose"])
        assert result.exit_code == 0
        assert "verbose" in result.output

        result = runner.invoke(test_cmd, ["-v"])
        assert result.exit_code == 0
        assert "verbose" in result.output

        result = runner.invoke(test_cmd, [])
        assert result.exit_code == 0
        assert "quiet" in result.output


class TestDryRunOption:
    """Tests for dry_run_option decorator."""

    def test_dry_run_option_adds_flag(self):
        """Test that dry_run_option adds --dry-run flag."""

        @click.command()
        @dry_run_option
        def test_cmd(dry_run):
            if dry_run:
                click.echo("dry run mode")
            else:
                click.echo("real mode")

        runner = CliRunner()

        result = runner.invoke(test_cmd, ["--dry-run"])
        assert result.exit_code == 0
        assert "dry run mode" in result.output


class TestProfileOption:
    """Tests for profile_option decorator."""

    def test_profile_option_adds_option(self):
        """Test that profile_option adds --profile option."""

        @click.command()
        @profile_option
        def test_cmd(profile):
            if profile:
                click.echo(f"profile: {profile}")
            else:
                click.echo("no profile")

        runner = CliRunner()

        result = runner.invoke(test_cmd, ["--profile", "my-profile"])
        assert result.exit_code == 0
        assert "profile: my-profile" in result.output


class TestPrefixOption:
    """Tests for prefix_option decorator."""

    def test_prefix_option_adds_option(self):
        """Test that prefix_option adds --prefix option."""

        @click.command()
        @prefix_option
        def test_cmd(prefix):
            if prefix:
                click.echo(f"prefix: {prefix}")
            else:
                click.echo("no prefix")

        runner = CliRunner()

        result = runner.invoke(test_cmd, ["--prefix", "fields"])
        assert result.exit_code == 0
        assert "prefix: fields" in result.output


class TestGeoparquetVersionOption:
    """Tests for geoparquet_version_option decorator."""

    def test_geoparquet_version_option_choices(self):
        """Test that geoparquet_version_option accepts valid choices."""

        @click.command()
        @geoparquet_version_option
        def test_cmd(geoparquet_version):
            if geoparquet_version:
                click.echo(f"version: {geoparquet_version}")
            else:
                click.echo("default version")

        runner = CliRunner()

        for version in ["1.0", "1.1", "2.0", "parquet-geo-only"]:
            result = runner.invoke(test_cmd, ["--geoparquet-version", version])
            assert result.exit_code == 0
            assert f"version: {version}" in result.output
