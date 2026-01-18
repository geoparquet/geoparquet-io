"""Pytest configuration for gpio-pmtiles tests."""

import sys
from pathlib import Path

# Add parent directory to path so we can import gpio_pmtiles
plugin_dir = Path(__file__).parent.parent
sys.path.insert(0, str(plugin_dir))
