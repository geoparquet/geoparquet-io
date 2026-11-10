"""
Layer enumeration for multi-layer spatial formats.

Provides utilities for listing layers in GeoPackage and FileGDB files.
Single-layer formats (GeoJSON, Shapefile, etc.) return None.

Semantic Behavior:
    - Multi-layer file with 2+ layers: Returns list of layer names
    - Multi-layer file with 1 layer: Returns None (degenerate case)
    - Multi-layer file with 0 layers: Returns None (empty file)
    - Single-layer format (GeoJSON, Shapefile, Parquet): Returns None
    - Unsupported/unknown format: Returns None

Example:
    from geoparquet_io.core.layers import list_layers

    # Multi-layer GeoPackage
    layers = list_layers('multilayer.gpkg')
    # ['buildings', 'roads', 'parcels']

    # FileGDB
    layers = list_layers('data.gdb')
    # ['points', 'lines', 'polygons']

    # Single-layer format
    layers = list_layers('single.geojson')
    # None
"""

from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path
from urllib.parse import quote as url_quote

from geoparquet_io.core.duckdb_utils import fetch_one, sql_path
from geoparquet_io.core.logging_config import debug
from geoparquet_io.core.remote import is_remote_url

logger = logging.getLogger(__name__)


def _is_geopackage(path: str) -> bool:
    """Check if file is a GeoPackage based on extension."""
    normalized = os.path.normpath(path).lower()
    return normalized.endswith(".gpkg")


def _is_filegdb(path: str) -> bool:
    """Check if path is a FileGDB directory based on extension."""
    normalized = os.path.normpath(path).lower()
    return normalized.endswith(".gdb")


def _list_geopackage_layers(path: str) -> list[str] | None:
    """
    List layers in a GeoPackage file by querying gpkg_contents.

    Uses sqlite3 in read-only mode. Only returns feature layers (not tiles)
    since "layers" in the context of gpio means vector layers.

    Args:
        path: Path to the GeoPackage file

    Returns:
        List of layer names if 2+ layers, None otherwise

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file isn't a valid GeoPackage or has other issues
    """
    # Normalize path for consistent handling
    path = os.path.normpath(path)
    debug(f"Listing layers in GeoPackage: {path}")

    # Use URI format with proper encoding for paths with special characters
    # sqlite3 URI format: file:path?mode=ro
    # Path must be URL-encoded for special characters
    encoded_path = url_quote(path, safe="/:\\")
    uri = f"file:{encoded_path}?mode=ro"

    try:
        con = sqlite3.connect(uri, uri=True)
    except sqlite3.OperationalError as e:
        # File doesn't exist or can't be opened
        if "unable to open" in str(e).lower():
            raise FileNotFoundError(f"GeoPackage file not found: {path}") from e
        raise ValueError(f"Cannot open GeoPackage: {path}. Error: {e}") from e

    try:
        # Query only feature layers (data_type='features')
        # Tiles are not considered "layers" in gpio's context
        cursor = con.execute(
            """
            SELECT table_name FROM gpkg_contents
            WHERE data_type = 'features'
            ORDER BY table_name
            """
        )
        layers = [row[0] for row in cursor.fetchall()]
        debug(f"Found {len(layers)} feature layer(s) in GeoPackage")

    except sqlite3.DatabaseError as e:
        error_msg = str(e).lower()
        if "no such table" in error_msg:
            raise ValueError(f"Invalid GeoPackage (missing gpkg_contents table): {path}") from e
        raise ValueError(f"Invalid GeoPackage file: {path}. Error: {e}") from e

    finally:
        con.close()

    # Return None for 0 or 1 layers (consistent API contract)
    if len(layers) <= 1:
        debug(f"GeoPackage has {len(layers)} layer(s), returning None")
        return None

    return layers


def _list_filegdb_layers(path: str) -> list[str] | None:
    """
    List layers in a FileGDB directory using DuckDB's OpenFileGDB driver.

    First attempts to read the GDB_Items catalog table (a00000004.gdbtable),
    falling back to iterating .gdbtable files if that fails.

    Args:
        path: Path to the FileGDB directory (.gdb)

    Returns:
        List of layer names if 2+ layers, None otherwise

    Raises:
        FileNotFoundError: If directory doesn't exist
        RuntimeError: If DuckDB can't read the FileGDB
    """
    # Normalize path
    path = os.path.normpath(path)
    debug(f"Listing layers in FileGDB: {path}")

    if not os.path.isdir(path):
        raise FileNotFoundError(f"FileGDB directory not found: {path}")

    from geoparquet_io.core.duckdb_utils import get_duckdb_connection

    con = get_duckdb_connection(load_spatial=True)
    try:
        layers = _list_filegdb_via_catalog(path, con)
    except (FileNotFoundError, ValueError, RuntimeError) as e:
        debug(f"Catalog approach failed ({e}), trying fallback method")
        try:
            layers = _list_filegdb_layers_fallback(path, con)
        except RuntimeError:
            raise  # Re-raise if fallback also fails
    finally:
        con.close()

    # Return None for 0 or 1 layers
    if len(layers) <= 1:
        debug(f"FileGDB has {len(layers)} layer(s), returning None")
        return None

    return layers


def _list_filegdb_via_catalog(path: str, con) -> list[str]:
    """
    List FileGDB layers by querying the GDB_Items catalog table.

    The a00000004.gdbtable file contains the catalog of all items in the GDB.
    This is faster than iterating through all .gdbtable files.

    Args:
        path: Path to the FileGDB directory
        con: DuckDB connection with spatial extension loaded

    Returns:
        List of layer names (may be empty)

    Raises:
        FileNotFoundError: If catalog table doesn't exist
        RuntimeError: If query fails
    """
    # The catalog table is at a fixed location in FileGDB structure
    # This is part of the ESRI FileGDB specification
    gdb_items_path = os.path.join(path, "a00000004.gdbtable")

    if not os.path.exists(gdb_items_path):
        raise FileNotFoundError(f"FileGDB catalog table not found: {gdb_items_path}")

    try:
        result = con.execute(
            f"""
            SELECT DISTINCT Name
            FROM ST_Read({sql_path(gdb_items_path)})
            WHERE Name IS NOT NULL
              AND Name NOT LIKE 'GDB_%'
              AND PhysicalName IS NOT NULL
            ORDER BY Name
            """
        ).fetchall()

        layers = [row[0] for row in result]
        debug(f"Found {len(layers)} layer(s) via catalog table")
        return layers

    except Exception as e:
        raise RuntimeError(f"Failed to query FileGDB catalog: {gdb_items_path}. Error: {e}") from e


def _list_filegdb_layers_fallback(path: str, con) -> list[str]:
    """
    Fallback method: enumerate layers by trying ST_Read_Meta on each .gdbtable file.

    This is slower but more robust when the catalog table approach fails
    (e.g., for older or non-standard FileGDB structures).

    Args:
        path: Path to the FileGDB directory
        con: DuckDB connection with spatial extension loaded

    Returns:
        List of layer names (may be empty)

    Raises:
        RuntimeError: If no layers can be read
    """
    debug(f"Using fallback method to enumerate FileGDB layers: {path}")

    gdbtable_files = sorted(
        [f for f in os.listdir(path) if f.endswith(".gdbtable")],
        reverse=True,  # Higher-numbered files are typically user tables
    )

    if not gdbtable_files:
        raise RuntimeError(f"No .gdbtable files found in FileGDB: {path}")

    layers = []
    errors = []

    for table_file in gdbtable_files:
        table_path = os.path.join(path, table_file)
        try:
            result = fetch_one(con.execute(f"SELECT * FROM ST_Read_Meta({sql_path(table_path)})"))

            if result and result[0]:  # layer_name is first column
                layer_name = result[0]
                # Skip internal GDB tables
                if not layer_name.startswith("GDB_"):
                    layers.append(layer_name)
                    debug(f"Found layer: {layer_name}")

        except Exception as e:
            # Track errors but continue - some .gdbtable files are not spatial
            errors.append(f"{table_file}: {e}")
            continue

    if not layers and errors:
        # All .gdbtable reads failed - this indicates corruption, not empty GDB
        error_summary = "; ".join(errors[:3])
        if len(errors) > 3:
            error_summary += f" (and {len(errors) - 3} more)"
        raise RuntimeError(
            f"Failed to read any layers from FileGDB: {path}. Errors: {error_summary}"
        )

    return sorted(set(layers))


def list_layers(path: str | os.PathLike) -> list[str] | None:
    """
    List layers in a multi-layer spatial file.

    Supports GeoPackage (.gpkg) and FileGDB (.gdb) formats.

    Return Value Semantics:
        - Returns list of layer names when file has 2+ layers
        - Returns None when:
            - File has 0 or 1 layers (degenerate multi-layer)
            - Format is inherently single-layer (GeoJSON, Shapefile, Parquet)
            - Format is unrecognized

    This function only works with local files. Remote URLs (S3, HTTP, etc.)
    are not supported because layer enumeration requires random file access.

    Args:
        path: Path to the spatial file or directory (local only)

    Returns:
        List of layer names for files with 2+ layers, None otherwise

    Raises:
        FileNotFoundError: If file/directory doesn't exist
        ValueError: If path is empty or a remote URL
        RuntimeError: If FileGDB cannot be read

    Example:
        >>> from geoparquet_io import list_layers
        >>> list_layers('multilayer.gpkg')
        ['buildings', 'roads', 'parcels']
        >>> list_layers('single.geojson')
        None
        >>> list_layers('single_layer.gpkg')
        None
    """
    if not path:
        raise ValueError("Path cannot be empty")

    # Convert to string (but don't normalize yet - URLs break with normpath)
    path_str = str(path)

    # Remote URLs are not supported - layer enumeration requires random access
    # Check BEFORE normalizing since normpath mangles URL schemes (s3:// -> s3:/)
    if is_remote_url(path_str):
        raise ValueError(
            f"Remote URLs are not supported for layer enumeration: {path_str}. "
            "Download the file locally first."
        )

    # Now safe to normalize for local paths
    path_str = os.path.normpath(path_str)

    debug(f"list_layers called with path: {path_str}")

    # GeoPackage: sqlite-based, use direct query
    if _is_geopackage(path_str):
        return _list_geopackage_layers(path_str)

    # FileGDB: directory-based, use DuckDB
    if _is_filegdb(path_str):
        return _list_filegdb_layers(path_str)

    # For other formats, check existence then return None
    path_obj = Path(path_str)
    if not path_obj.exists():
        raise FileNotFoundError(f"File not found: {path_str}")

    debug(f"Unrecognized or single-layer format: {path_str}")
    # Single-layer formats: GeoJSON, Shapefile, Parquet, FlatGeobuf, etc.
    return None
