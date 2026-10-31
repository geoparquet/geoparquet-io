"""Profile report formatting utilities for benchmark profiling."""

from __future__ import annotations

import cProfile
from pathlib import Path


def save_profile_data(profiler: cProfile.Profile, output_path: Path) -> None:
    """
    Save cProfile profiler data to file.

    Creates parent directories if they don't exist.

    Args:
        profiler: cProfile.Profile object with collected data
        output_path: Path where .prof file should be written
    """
    # Ensure parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Save profile stats
    profiler.dump_stats(output_path)
