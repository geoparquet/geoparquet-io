"""Lightweight defaults shared by WFS clients and CLI options."""

from typing import Final

# Single source of truth for the default page size used when paginating WFS
# requests. Every entry point references it -- the CLI option
# (`gpio extract wfs --page-size`), the core functions (`wfs_to_table`,
# `convert_wfs_to_geoparquet`, `convert_wfs_layers_to_directory`,
# `fetch_all_features_duckdb`, `_fetch_with_spatial_tiles`) and the Python API
# wrappers (`ops.from_wfs`, `ops.from_wfs_layers`, `Table.from_wfs`) -- so their
# defaults cannot drift apart again.
#
# Note: the CLI option is typed `click.IntRange(1000, 500000)`, so any new
# value for this constant must fall inside that range.
DEFAULT_WFS_PAGE_SIZE: Final[int] = 100_000
