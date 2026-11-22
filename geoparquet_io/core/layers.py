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
    """List real layer names without assuming a fixed FileGDB catalog ID."""
    path = os.path.normpath(path)
    if not os.path.isdir(path):
        raise FileNotFoundError(f"FileGDB directory not found: {path}")

    from geoparquet_io.core.duckdb_utils import get_duckdb_connection

    con = get_duckdb_connection(load_spatial=True)
    try:
        metadata = _spatial_layer_metadata(con, path)
        layers = sorted({layer["name"] for layer in metadata[1]}) if metadata else []
    except Exception as error:
        raise RuntimeError(f"Failed to enumerate FileGDB layers: {path}. Error: {error}") from error
    finally:
        con.close()
    return layers if len(layers) > 1 else None


def _filegdb_raster_names(con, tables: dict[str, str]) -> set[str]:
    """Raster datasets and their backing tables are not vector layers in GDAL."""
    if "GDB_Items" in tables:
        query = (
            f"SELECT items.Name FROM ST_Read({sql_path(tables['GDB_Items'])}) items "
            f"JOIN ST_Read({sql_path(tables['GDB_ItemTypes'])}) types ON items.Type = types.UUID "
            "WHERE types.Name = 'Raster Dataset'"
        )
    elif "GDB_FeatureClasses" in tables and "GDB_ObjectClasses" in tables:
        query = (
            f"SELECT objects.Name FROM ST_Read({sql_path(tables['GDB_FeatureClasses'])}) features "
            f"JOIN ST_Read({sql_path(tables['GDB_ObjectClasses'])}) objects "
            "ON features.ObjectClassID = objects.ID WHERE features.FeatureType = 14"
        )
    else:
        raise RuntimeError("FileGDB has no recognized dataset catalog")
    rasters = {row[0].casefold() for row in con.execute(query).fetchall()}
    return {
        prefix + name
        for name in rasters
        for prefix in ("", "fras_ras_", "fras_aux_", "fras_bnd_", "fras_blk_")
    }


def _filegdb_layer_metadata(con, path: str) -> tuple[str, list[dict]]:
    """Read only tables registered in the FileGDB's system catalog.

    A loose physical table is not necessarily a selectable dataset layer.
    GDB_SystemCatalog maps its row IDs to the physical hexadecimal file IDs;
    unlike GDB_Items, its location is fixed by the FileGDB format.
    """
    catalog = os.path.join(path, "a00000001.gdbtable")
    catalog_meta = _spatial_layer_metadata(con, catalog)
    if not catalog_meta or catalog_meta[0] != "OpenFileGDB":
        raise RuntimeError(f"Cannot read FileGDB system catalog: {path}")
    entries = con.execute(
        f"SELECT ID, Name, FileFormat FROM ST_Read({sql_path(catalog)})"
    ).fetchall()
    tables = {
        name: os.path.join(path, f"a{table_id:08x}.gdbtable")
        for table_id, name, file_format in entries
        if file_format == 0 and name
    }
    rasters = _filegdb_raster_names(con, tables)
    registered = {
        name: filename
        for name, filename in tables.items()
        if not name.casefold().startswith(("gdb_", "vat_")) and name.casefold() not in rasters
    }
    if not registered:
        return "OpenFileGDB", []
    sources = ", ".join(sql_path(filename) for filename in registered.values())
    rows = con.execute(
        f"SELECT driver_short_name, layers FROM ST_Read_Meta([{sources}])"
    ).fetchall()
    names = {name.casefold() for name in registered}
    layers = [
        layer
        for driver, entries in rows
        for layer in entries
        if driver == "OpenFileGDB" and layer["name"].casefold() in names
    ]
    if not layers:
        raise RuntimeError(f"Cannot read registered FileGDB layers: {path}")
    return "OpenFileGDB", layers


def _spatial_layer_metadata(con, path: str) -> tuple[str, list[dict]] | None:
    """GDAL's driver and actual layer records, including single layers.

    DuckDB's file glob skips FileGDB directories, so those use their catalog.
    The nested ``layers`` records name layers; the first column of
    ``ST_Read_Meta`` is only the input file path.
    """
    if _is_filegdb(path):
        return _filegdb_layer_metadata(con, path)
    row = fetch_one(
        con.execute(f"SELECT driver_short_name, layers FROM ST_Read_Meta({sql_path(path)})")
    )
    return (row[0], row[1]) if row else None


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
