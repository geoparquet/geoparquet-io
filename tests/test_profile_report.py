"""Tests for profile report formatting."""

import cProfile
import tempfile
from pathlib import Path


class TestProfileReportFormatting:
    """Tests for profile stats formatting."""


class TestProfileDataSaving:
    """Tests for saving profile data."""

    def test_save_profile_data(self):
        """Test save_profile_data creates file."""
        from geoparquet_io.benchmarks.profile_report import save_profile_data

        # Create a profiler with some data
        profiler = cProfile.Profile()
        profiler.enable()
        sum(range(100))
        profiler.disable()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.prof"
            save_profile_data(profiler, output_path)

            assert output_path.exists()
            assert output_path.stat().st_size > 0

    def test_save_profile_data_creates_parent_dirs(self):
        """Test that save_profile_data creates parent directories."""
        from geoparquet_io.benchmarks.profile_report import save_profile_data

        profiler = cProfile.Profile()
        profiler.enable()
        sum(range(100))
        profiler.disable()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "subdir" / "another" / "test.prof"
            save_profile_data(profiler, output_path)

            assert output_path.exists()
            assert output_path.parent.exists()
