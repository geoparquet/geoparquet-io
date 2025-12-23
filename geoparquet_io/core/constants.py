"""
Shared constants for geoparquet-io.

This module defines constants that are shared across multiple modules to ensure
consistency and make it easy to change values in one place.
"""

# Default column name for H3 cell IDs
DEFAULT_H3_COLUMN_NAME = "h3_cell"

# Default column name for quadkey cells
DEFAULT_QUADKEY_COLUMN_NAME = "quadkey"

# Default resolution (zoom level) for quadkey generation
DEFAULT_QUADKEY_RESOLUTION = 13

# Default resolution for quadkey partitioning (prefix length)
DEFAULT_QUADKEY_PARTITION_RESOLUTION = 9
