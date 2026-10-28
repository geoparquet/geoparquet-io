"""Tests for PMTiles generation module."""

import shutil
import sys
from pathlib import Path

import pytest


def has_tippecanoe():
    """Check if tippecanoe is available."""
    return shutil.which("tippecanoe") is not None


def has_gpio():
    """Check if gpio is available."""
    return shutil.which("gpio") is not None


# Skip integration tests on Windows (tippecanoe has no native Windows build)
skip_windows = pytest.mark.skipif(
    sys.platform == "win32",
    reason="tippecanoe not available on Windows",
)


class TestTippecanoeNotFoundError:
    """Tests for TippecanoeNotFoundError exception."""

    def test_error_message_content(self):
        """Test error message contains installation instructions."""
        from geoparquet_io.core.pmtiles import TippecanoeNotFoundError

        error = TippecanoeNotFoundError()
        error_msg = str(error)

        assert "tippecanoe not found" in error_msg
        assert "brew install tippecanoe" in error_msg
        assert "sudo apt install tippecanoe" in error_msg


class TestGpioExecutableDetection:
    """Tests for gpio executable detection."""

    def test_returns_string(self):
        """Test that gpio executable detection returns a string."""
        from geoparquet_io.core.pmtiles import _get_gpio_executable

        gpio_exe = _get_gpio_executable()
        assert gpio_exe is not None
        assert isinstance(gpio_exe, str)
        assert len(gpio_exe) > 0


class TestBuildGpioCommands:
    """Tests for gpio command building."""

    def test_simple_command(self):
        """Test building simple gpio convert command."""
        from geoparquet_io.core.pmtiles import _build_gpio_commands

        commands = _build_gpio_commands(
            input_path="input.parquet",
            bbox=None,
            where=None,
            include_cols=None,
            precision=6,
            verbose=False,
            profile=None,
            src_crs=None,
        )

        assert len(commands) == 1
        assert "convert" in commands[0]
        assert "geojson" in commands[0]
        assert "input.parquet" in commands[0]
        assert "--precision" in commands[0]
        assert "6" in commands[0]

    def test_with_filters(self):
        """Test building gpio commands with filters."""
        from geoparquet_io.core.pmtiles import _build_gpio_commands

        commands = _build_gpio_commands(
            input_path="input.parquet",
            bbox="-122,37,-121,38",
            where="population > 1000",
            include_cols="name,type",
            precision=5,
            verbose=True,
            profile="my-profile",
            src_crs=None,
        )

        assert len(commands) == 2

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

        convert_cmd = commands[1]
        assert "convert" in convert_cmd
        assert "geojson" in convert_cmd
        assert "-" in convert_cmd

    def test_with_reprojection(self):
        """Test building gpio commands with CRS reprojection."""
        from geoparquet_io.core.pmtiles import _build_gpio_commands

        commands = _build_gpio_commands(
            input_path="input.parquet",
            bbox=None,
            where=None,
            include_cols=None,
            precision=6,
            verbose=True,
            profile="my-profile",
            src_crs="EPSG:3857",
        )

        assert len(commands) == 2

        reproject_cmd = commands[0]
        assert "convert" in reproject_cmd
        assert "reproject" in reproject_cmd
        assert "input.parquet" in reproject_cmd
        assert "--dst-crs" in reproject_cmd
        assert "EPSG:4326" in reproject_cmd
        assert "--src-crs" in reproject_cmd
        assert "EPSG:3857" in reproject_cmd

        convert_cmd = commands[1]
        assert "convert" in convert_cmd
        assert "geojson" in convert_cmd
        assert "-" in convert_cmd

    def test_with_reprojection_and_filters(self):
        """Test building gpio commands with both reprojection and filters."""
        from geoparquet_io.core.pmtiles import _build_gpio_commands

        commands = _build_gpio_commands(
            input_path="input.parquet",
            bbox="-122,37,-121,38",
            where="type = 'building'",
            include_cols="name,height",
            precision=6,
            verbose=False,
            profile=None,
            src_crs="EPSG:3857",
        )

        assert len(commands) == 3

        reproject_cmd = commands[0]
        assert "convert" in reproject_cmd
        assert "reproject" in reproject_cmd
        assert "input.parquet" in reproject_cmd
        assert "--src-crs" in reproject_cmd
        assert "EPSG:3857" in reproject_cmd

        extract_cmd = commands[1]
        assert "extract" in extract_cmd
        assert "geoparquet" in extract_cmd
        assert "-" in extract_cmd
        assert "--bbox" in extract_cmd
        assert "--where" in extract_cmd
        assert "--include-cols" in extract_cmd

        convert_cmd = commands[2]
        assert "convert" in convert_cmd
        assert "geojson" in convert_cmd
        assert "-" in convert_cmd


class TestBuildTippecanoeCommand:
    """Tests for tippecanoe command building."""

    def test_basic_command(self):
        """Test building basic tippecanoe command."""
        from geoparquet_io.core.pmtiles import _build_tippecanoe_command

        cmd = _build_tippecanoe_command(
            output_path="output.pmtiles",
            layer="test_layer",
            min_zoom=None,
            max_zoom=None,
            verbose=False,
            attribution=None,
        )

        assert "tippecanoe" in cmd
        assert "-P" in cmd
        assert "-o" in cmd
        assert "output.pmtiles" in cmd
        assert "-l" in cmd
        assert "test_layer" in cmd
        assert "-zg" in cmd
        assert "--drop-densest-as-needed" in cmd

    @pytest.mark.parametrize(
        ("explicit", "expected"),
        [("/data/scratch", "/data/scratch"), (None, None)],
        ids=["explicit", "none"],
    )
    def test_temporary_directory_becomes_dash_t(self, explicit, expected):
        """#1115: the resolved scratch directory is tippecanoe's ``-t``."""
        from geoparquet_io.core.pmtiles import _build_tippecanoe_command

        cmd = _build_tippecanoe_command(
            output_path="output.pmtiles",
            layer="test_layer",
            min_zoom=None,
            max_zoom=None,
            verbose=False,
            temporary_directory=explicit,
        )

        assert ("-t" in cmd) is (expected is not None)
        if expected:
            assert cmd[cmd.index("-t") + 1] == expected

    def test_with_zoom_levels(self):
        """Test building tippecanoe command with explicit zoom levels."""
        from geoparquet_io.core.pmtiles import _build_tippecanoe_command

        cmd = _build_tippecanoe_command(
            output_path="output.pmtiles",
            layer="test_layer",
            min_zoom=0,
            max_zoom=14,
            verbose=True,
            attribution=None,
        )

        assert "-Z" in cmd
        assert "0" in cmd
        assert "-z" in cmd
        assert "14" in cmd
        assert "-zg" not in cmd
        assert "--progress-interval=1" in cmd

    def test_default_attribution(self):
        """Test that default attribution is included."""
        from geoparquet_io.core.pmtiles import _build_tippecanoe_command

        cmd = _build_tippecanoe_command(
            output_path="output.pmtiles",
            layer="test_layer",
            min_zoom=None,
            max_zoom=None,
            verbose=False,
            attribution=None,
        )

        assert any("--attribution=" in arg for arg in cmd)
        assert any("geoparquet.io" in arg for arg in cmd)

    def test_custom_attribution(self):
        """Test building tippecanoe command with custom attribution."""
        from geoparquet_io.core.pmtiles import _build_tippecanoe_command

        custom_attr = '<a href="https://example.com/">&copy; Example</a>'
        cmd = _build_tippecanoe_command(
            output_path="output.pmtiles",
            layer="test_layer",
            min_zoom=None,
            max_zoom=11,
            verbose=False,
            attribution=custom_attr,
        )

        assert any("--attribution=" in arg for arg in cmd)
        assert any("example.com" in arg for arg in cmd)

    def test_production_quality_flags(self):
        """Test that production-quality flags are included."""
        from geoparquet_io.core.pmtiles import _build_tippecanoe_command

        cmd = _build_tippecanoe_command(
            output_path="output.pmtiles",
            layer="test_layer",
            min_zoom=None,
            max_zoom=11,
            verbose=False,
            attribution=None,
        )

        assert "-P" in cmd
        assert "--simplify-only-low-zooms" in cmd
        assert "--no-simplification-of-shared-nodes" in cmd
        assert "--no-tile-size-limit" in cmd
        assert "--drop-densest-as-needed" in cmd

    def test_simplify_only_low_zooms_toggle_off(self):
        """--simplify-only-low-zooms is omitted when disabled."""
        from geoparquet_io.core.pmtiles import _build_tippecanoe_command

        cmd = _build_tippecanoe_command(
            output_path="output.pmtiles",
            layer="test_layer",
            min_zoom=None,
            max_zoom=None,
            verbose=False,
            attribution=None,
            simplify_only_low_zooms=False,
        )

        assert "--simplify-only-low-zooms" not in cmd

    def test_no_simplification_of_shared_nodes_toggle_off(self):
        """--no-simplification-of-shared-nodes is omitted when disabled."""
        from geoparquet_io.core.pmtiles import _build_tippecanoe_command

        cmd = _build_tippecanoe_command(
            output_path="output.pmtiles",
            layer="test_layer",
            min_zoom=None,
            max_zoom=None,
            verbose=False,
            attribution=None,
            no_simplification_of_shared_nodes=False,
        )

        assert "--no-simplification-of-shared-nodes" not in cmd

    def test_tile_size_limit_toggle_off(self):
        """--no-tile-size-limit is omitted when the size limit is re-enabled."""
        from geoparquet_io.core.pmtiles import _build_tippecanoe_command

        cmd = _build_tippecanoe_command(
            output_path="output.pmtiles",
            layer="test_layer",
            min_zoom=None,
            max_zoom=None,
            verbose=False,
            attribution=None,
            no_tile_size_limit=False,
        )

        assert "--no-tile-size-limit" not in cmd

    def test_drop_densest_toggle_off(self):
        """--drop-densest-as-needed is omitted when disabled."""
        from geoparquet_io.core.pmtiles import _build_tippecanoe_command

        cmd = _build_tippecanoe_command(
            output_path="output.pmtiles",
            layer="test_layer",
            min_zoom=None,
            max_zoom=None,
            verbose=False,
            attribution=None,
            drop_densest_as_needed=False,
        )

        assert "--drop-densest-as-needed" not in cmd

    def test_maximum_tile_bytes_sets_cap_and_suppresses_no_limit(self):
        """--maximum-tile-bytes takes precedence over --no-tile-size-limit.

        The two are contradictory: passing an explicit cap while also
        disabling the limit would defeat the cap. The byte cap wins so
        that --drop-densest-as-needed has a limit to drop features against.
        """
        from geoparquet_io.core.pmtiles import _build_tippecanoe_command

        cmd = _build_tippecanoe_command(
            output_path="output.pmtiles",
            layer="test_layer",
            min_zoom=None,
            max_zoom=None,
            verbose=False,
            attribution=None,
            maximum_tile_bytes=500000,
        )

        assert "--maximum-tile-bytes=500000" in cmd
        assert "--no-tile-size-limit" not in cmd

    def test_force_omitted_by_default(self):
        """--force is not passed unless requested."""
        from geoparquet_io.core.pmtiles import _build_tippecanoe_command

        cmd = _build_tippecanoe_command(
            output_path="output.pmtiles",
            layer="test_layer",
            min_zoom=None,
            max_zoom=None,
            verbose=False,
            attribution=None,
        )

        assert "--force" not in cmd

    def test_force_passed_when_enabled(self):
        """--force is passed to tippecanoe when force=True."""
        from geoparquet_io.core.pmtiles import _build_tippecanoe_command

        cmd = _build_tippecanoe_command(
            output_path="output.pmtiles",
            layer="test_layer",
            min_zoom=None,
            max_zoom=None,
            verbose=False,
            attribution=None,
            force=True,
        )

        assert "--force" in cmd

    def test_max_zoom_only(self):
        """Test that -z is used for max zoom without min zoom."""
        from geoparquet_io.core.pmtiles import _build_tippecanoe_command

        cmd = _build_tippecanoe_command(
            output_path="output.pmtiles",
            layer="test_layer",
            min_zoom=None,
            max_zoom=11,
            verbose=False,
            attribution=None,
        )

        assert "-z" in cmd
        assert "11" in cmd
        assert cmd.count("-Z") == 0

    def test_min_zoom_only(self):
        """Test that -Z and -zg are used for min zoom without max zoom."""
        from geoparquet_io.core.pmtiles import _build_tippecanoe_command

        cmd = _build_tippecanoe_command(
            output_path="output.pmtiles",
            layer="test_layer",
            min_zoom=5,
            max_zoom=None,
            verbose=False,
            attribution=None,
        )

        assert "-Z" in cmd
        assert "5" in cmd
        assert "-zg" in cmd
        assert cmd.count("-z") == 0  # lowercase -z should not be present


class TestPathValidation:
    """Tests for path validation security."""

    def test_valid_paths(self):
        """Test path validation with valid paths."""
        from geoparquet_io.core.pmtiles import _validate_path

        _validate_path("/path/to/file.parquet")
        _validate_path("relative/path.parquet")
        _validate_path("file_with_underscores.parquet")
        _validate_path("file-with-dashes.parquet")
        _validate_path("file.with.dots.parquet")
        _validate_path("/path with spaces/file.parquet")

    def test_rejects_shell_injection(self):
        """Test that path validation rejects shell metacharacters."""
        from geoparquet_io.core.pmtiles import _validate_path

        # Build dangerous paths without raw shell metacharacters in source
        backtick = chr(96)
        newline = chr(10)
        carriage_return = chr(13)
        dangerous_paths = [
            "file.parquet; rm -rf /",
            "file.parquet | cat",
            "file.parquet && echo pwned",
            "file.parquet" + "$" + "malicious",
            "file.parquet" + backtick + "whoami" + backtick,
            "file.parquet" + newline + "rm -rf /",
            "file.parquet" + carriage_return + "rm -rf /",
        ]

        for path in dangerous_paths:
            with pytest.raises(ValueError, match="dangerous character"):
                _validate_path(path)

    def test_create_pmtiles_rejects_dangerous_input_path(self):
        """Test that create_pmtiles rejects input paths with shell metacharacters."""
        from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

        with pytest.raises(ValueError, match="dangerous character"):
            create_pmtiles_from_geoparquet(
                input_path="input.parquet; rm -rf /",
                output_path="output.pmtiles",
            )

    def test_create_pmtiles_rejects_dangerous_output_path(self):
        """Test that create_pmtiles rejects output paths with shell metacharacters."""
        from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

        with pytest.raises(ValueError, match="dangerous character"):
            create_pmtiles_from_geoparquet(
                input_path="input.parquet",
                output_path="output.pmtiles | cat",
            )


class TestPMTilesIntegration:
    """Integration tests for PMTiles creation (require tippecanoe)."""

    @skip_windows
    @pytest.mark.skipif(not has_gpio(), reason="gpio not installed")
    @pytest.mark.skipif(not has_tippecanoe(), reason="tippecanoe not installed")
    @pytest.mark.slow
    def test_basic_creation(self, tmp_path):
        """Test basic PMTiles creation from test data."""
        test_data_dir = Path(__file__).parent / "data"
        input_file = test_data_dir / "places_test.parquet"

        if not input_file.exists():
            pytest.skip(f"Test file not found: {input_file}")

        output_file = tmp_path / "output.pmtiles"

        from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

        create_pmtiles_from_geoparquet(
            input_path=str(input_file),
            output_path=str(output_file),
            layer="places",
            verbose=True,
        )

        assert output_file.exists()
        assert output_file.stat().st_size > 0

    @skip_windows
    @pytest.mark.skipif(not has_gpio(), reason="gpio not installed")
    @pytest.mark.skipif(not has_tippecanoe(), reason="tippecanoe not installed")
    @pytest.mark.slow
    def test_with_filters(self, tmp_path):
        """Test PMTiles creation with filtering options."""
        test_data_dir = Path(__file__).parent / "data"
        input_file = test_data_dir / "places_test.parquet"

        if not input_file.exists():
            pytest.skip(f"Test file not found: {input_file}")

        output_file = tmp_path / "filtered.pmtiles"

        from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

        create_pmtiles_from_geoparquet(
            input_path=str(input_file),
            output_path=str(output_file),
            layer="filtered_places",
            bbox="-180,-90,180,90",
            precision=5,
            verbose=True,
        )

        assert output_file.exists()
        assert output_file.stat().st_size > 0

    @skip_windows
    @pytest.mark.skipif(not has_gpio(), reason="gpio not installed")
    @pytest.mark.skipif(not has_tippecanoe(), reason="tippecanoe not installed")
    @pytest.mark.slow
    def test_with_zoom_levels(self, tmp_path):
        """Test PMTiles creation with explicit zoom levels."""
        test_data_dir = Path(__file__).parent / "data"
        input_file = test_data_dir / "places_test.parquet"

        if not input_file.exists():
            pytest.skip(f"Test file not found: {input_file}")

        output_file = tmp_path / "zoomed.pmtiles"

        from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

        create_pmtiles_from_geoparquet(
            input_path=str(input_file),
            output_path=str(output_file),
            layer="zoomed_places",
            min_zoom=0,
            max_zoom=10,
            verbose=True,
        )

        assert output_file.exists()
        assert output_file.stat().st_size > 0

    @skip_windows
    @pytest.mark.skipif(not has_gpio(), reason="gpio not installed")
    @pytest.mark.skipif(not has_tippecanoe(), reason="tippecanoe not installed")
    @pytest.mark.slow
    def test_layer_by_column(self, tmp_path):
        """Test PMTiles creation with multiple layers based on a column name"""
        test_data_dir = Path(__file__).parent / "data"
        input_file = test_data_dir / "places_test.parquet"

        if not input_file.exists():
            pytest.skip(f"Test file not found: {input_file}")

        output_file = tmp_path / "layer_by_column.pmtiles"

        from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

        create_pmtiles_from_geoparquet(
            input_path=str(input_file),
            output_path=str(output_file),
            min_zoom=0,
            max_zoom=10,
            verbose=True,
            layer_by_column="address",
        )

        assert output_file.exists()
        assert output_file.stat().st_size > 0

        with pytest.raises(ValueError):
            # ensure that both a layer by column
            # and a single layer name cannot be specified
            # since these are mutually exclusive
            create_pmtiles_from_geoparquet(
                input_path=str(input_file),
                output_path=str(output_file),
                verbose=True,
                layer_by_column="address",
                layer="DUMMY",
            )


class TestScratchDirectory:
    """#1115: one resolution rule for where a pmtiles run's scratch goes."""

    def test_explicit_directory_is_made_absolute(self, tmp_path):
        """tippecanoe warns on a relative ``-t``; the resolved path never is."""
        import os

        from geoparquet_io.core.pmtiles import resolve_scratch_directory

        try:
            relative = os.path.relpath(tmp_path)
        except ValueError:  # Windows CI: tmp_path on another drive than the cwd
            pytest.skip("no relative path to tmp_path from the cwd")
        assert not os.path.isabs(relative)
        assert resolve_scratch_directory(relative) == os.path.abspath(tmp_path)

    def test_missing_directory_is_rejected_before_any_work(self, tmp_path):
        from geoparquet_io.core.exceptions import InvalidParameterError
        from geoparquet_io.core.pmtiles import resolve_scratch_directory

        with pytest.raises(InvalidParameterError, match="not a directory"):
            resolve_scratch_directory(str(tmp_path / "nope"))

    def test_unwritable_directory_is_rejected(self, tmp_path, monkeypatch):
        import os

        from geoparquet_io.core.exceptions import InvalidParameterError
        from geoparquet_io.core.pmtiles import resolve_scratch_directory

        monkeypatch.setattr(os, "access", lambda path, mode: False)
        with pytest.raises(InvalidParameterError, match="not writable"):
            resolve_scratch_directory(str(tmp_path))

    @pytest.mark.parametrize("explicit", [None, ""], ids=["none", "empty"])
    def test_unset_follows_the_package_temp_rule(self, explicit, tmp_path, monkeypatch):
        """A stale TMPDIR falls back like every other gpio temp file does,
        instead of being forwarded raw to tippecanoe (which would die on it)."""
        import tempfile

        from geoparquet_io.core.pmtiles import resolve_scratch_directory

        monkeypatch.setenv("TMPDIR", str(tmp_path / "gone"))
        monkeypatch.setattr(tempfile, "tempdir", None)
        try:
            resolved = resolve_scratch_directory(explicit)
            assert resolved == tempfile.gettempdir()
            assert resolved != str(tmp_path / "gone")
        finally:
            monkeypatch.setattr(tempfile, "tempdir", None)

    def test_gpio_children_inherit_the_scratch_as_tmpdir(self, tmp_path):
        """``-t`` only moves tippecanoe; the extract|convert chain spills where
        its own TMPDIR points, so the run hands it the same directory."""
        from unittest.mock import MagicMock, patch

        from geoparquet_io.core.pmtiles import _run_pipeline

        proc = MagicMock()
        proc.stdout = MagicMock()
        with (
            patch("geoparquet_io.core.pmtiles.subprocess.Popen", return_value=proc) as popen,
            patch("geoparquet_io.core.pmtiles._run_simple"),
        ):
            _run_pipeline([["gpio", "extract"]], ["tippecanoe"], False, None, str(tmp_path))

        env = popen.call_args.kwargs["env"]
        assert env["TMPDIR"] == env["TEMP"] == env["TMP"] == str(tmp_path)

    def test_create_threads_the_resolved_directory_to_tippecanoe_and_children(self, tmp_path):
        from unittest.mock import patch

        from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

        with (
            patch("geoparquet_io.core.pmtiles._check_tippecanoe", return_value=True),
            patch("geoparquet_io.core.pmtiles._run_pipeline") as run,
        ):
            create_pmtiles_from_geoparquet(
                "in.parquet", "out.pmtiles", temporary_directory=str(tmp_path)
            )

        _gpio_cmds, tippecanoe_cmd, _verbose, _layer, scratch = run.call_args.args
        assert tippecanoe_cmd[tippecanoe_cmd.index("-t") + 1] == str(tmp_path)
        assert scratch == str(tmp_path)

    def test_create_rejects_a_missing_directory_before_probing_tippecanoe(self, tmp_path):
        from unittest.mock import patch

        from geoparquet_io.core.exceptions import InvalidParameterError
        from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

        with (
            patch("geoparquet_io.core.pmtiles._check_tippecanoe", return_value=False) as probe,
            pytest.raises(InvalidParameterError, match="not a directory"),
        ):
            create_pmtiles_from_geoparquet(
                "in.parquet", "out.pmtiles", temporary_directory=str(tmp_path / "nope")
            )
        probe.assert_not_called()


class TestPMTilesCreateCLIFlags:
    """The pmtiles create CLI exposes the tippecanoe production flags."""

    def _invoke(self, args):
        from unittest.mock import patch

        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        with patch("geoparquet_io.core.pmtiles.create_pmtiles_from_geoparquet") as mock_create:
            runner = CliRunner()
            result = runner.invoke(
                cli,
                ["pmtiles", "create", "in.parquet", "out.pmtiles", *args],
            )
            return result, mock_create

    def test_defaults_thread_through(self):
        result, mock_create = self._invoke([])

        assert result.exit_code == 0, result.output
        kwargs = mock_create.call_args.kwargs
        assert kwargs["simplify_only_low_zooms"] is True
        assert kwargs["no_simplification_of_shared_nodes"] is True
        assert kwargs["no_tile_size_limit"] is True
        assert kwargs["drop_densest_as_needed"] is True
        assert kwargs["maximum_tile_bytes"] is None
        assert kwargs["force"] is False

    def test_toggles_off_thread_through(self):
        result, mock_create = self._invoke(
            [
                "--no-simplify-only-low-zooms",
                "--simplification-of-shared-nodes",
                "--tile-size-limit",
                "--no-drop-densest-as-needed",
            ]
        )

        assert result.exit_code == 0, result.output
        kwargs = mock_create.call_args.kwargs
        assert kwargs["simplify_only_low_zooms"] is False
        assert kwargs["no_simplification_of_shared_nodes"] is False
        assert kwargs["no_tile_size_limit"] is False
        assert kwargs["drop_densest_as_needed"] is False

    def test_maximum_tile_bytes_thread_through(self):
        result, mock_create = self._invoke(["--maximum-tile-bytes", "500000"])

        assert result.exit_code == 0, result.output
        assert mock_create.call_args.kwargs["maximum_tile_bytes"] == 500000

    def test_force_thread_through(self):
        result, mock_create = self._invoke(["--force"])

        assert result.exit_code == 0, result.output
        assert mock_create.call_args.kwargs["force"] is True

    def test_temporary_directory_thread_through(self, tmp_path):
        import os

        result, mock_create = self._invoke(["--temporary-directory", str(tmp_path)])

        assert result.exit_code == 0, result.output
        assert mock_create.call_args.kwargs["temporary_directory"] == os.path.realpath(tmp_path)

    def test_temporary_directory_must_exist(self, tmp_path):
        result, mock_create = self._invoke(["-t", str(tmp_path / "nope")])

        assert result.exit_code == 2
        assert "does not exist" in result.output
        mock_create.assert_not_called()

    def test_force_short_flag_thread_through(self):
        result, mock_create = self._invoke(["-f"])

        assert result.exit_code == 0, result.output
        assert mock_create.call_args.kwargs["force"] is True


class TestRunPipelineErrorSurfacing:
    """Pipeline errors must surface upstream stderr.

    Regression for issue #421: a failing upstream gpio process had its stderr
    captured to PIPE, drained, then discarded — the raised RuntimeError only
    contained the exit code, leaving users debugging blind.
    """

    @pytest.mark.skipif(sys.platform == "win32", reason="needs POSIX shell")
    def test_pipeline_error_includes_upstream_stderr(self):
        from geoparquet_io.core.pmtiles import _run_pipeline

        sentinel = "GPIO_UPSTREAM_BOOM_a3f9"
        with pytest.raises(RuntimeError) as exc_info:
            _run_pipeline(
                gpio_commands=[
                    ["sh", "-c", f"echo {sentinel} >&2; exit 7"],
                ],
                tippecanoe_cmd=["cat"],
                verbose=False,
            )
        msg = str(exc_info.value)
        assert "exit code 7" in msg
        assert sentinel in msg, f"upstream stderr '{sentinel}' not surfaced in error: {msg!r}"

    @pytest.mark.skipif(sys.platform == "win32", reason="needs POSIX shell")
    def test_tippecanoe_failure_still_surfaces_upstream_stderr(self):
        """When tippecanoe also exits non-zero, upstream stderr must still surface.

        Real-world failure mode: upstream gpio crashes, tippecanoe sees a
        truncated stream and also exits non-zero. The previous implementation
        short-circuited on tippecanoe's exit code, hiding the real cause.
        """
        from geoparquet_io.core.pmtiles import _run_pipeline

        sentinel = "GPIO_UPSTREAM_TIPPECANOE_DUAL_b7c4"
        with pytest.raises(RuntimeError) as exc_info:
            _run_pipeline(
                gpio_commands=[
                    ["sh", "-c", f"echo {sentinel} >&2; exit 5"],
                ],
                tippecanoe_cmd=["sh", "-c", "exit 9"],
                verbose=False,
            )
        msg = str(exc_info.value)
        assert "tippecanoe failed" in msg
        assert "exit code 9" in msg
        assert sentinel in msg, (
            f"upstream stderr '{sentinel}' lost behind tippecanoe error: {msg!r}"
        )
        assert "exit code 5" in msg
