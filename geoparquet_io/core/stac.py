#!/usr/bin/env python3
"""
STAC Item and Collection generation for GeoParquet files.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import click
import pystac

from geoparquet_io.core.common import (
    find_primary_geometry_column,
    get_dataset_bounds,
    get_parquet_metadata,
    is_remote_url,
    parse_geo_metadata,
)
from geoparquet_io.core.logging_config import debug, warn


def _detect_stac_file(file_path: Path) -> str | None:
    """Detect if a file is a STAC Item or Collection."""
    if file_path.suffix.lower() != ".json":
        return None

    try:
        with open(file_path) as f:
            data = json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return None

    stac_type = data.get("type")
    if stac_type == "Feature":
        return "Item"
    elif stac_type == "Collection":
        return "Collection"
    return None


def _detect_stac_directory(dir_path: Path) -> str | None:
    """Detect if a directory is a pure STAC Collection (no parquet files)."""
    collection_file = dir_path / "collection.json"
    if not collection_file.exists():
        return None

    try:
        with open(collection_file) as f:
            data = json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return None

    if data.get("type") != "Collection":
        return None

    # Check if directory also contains parquet files
    # If it does, it's a mixed directory (STAC + parquet) - allow it
    parquet_files = list(dir_path.glob("*.parquet")) + list(dir_path.glob("*/*.parquet"))
    if parquet_files:
        # Mixed directory - has both STAC and parquet, allow generation from parquet
        return None

    # Pure STAC directory - no parquet files
    return "Collection"


def detect_stac(input_path: str) -> str | None:
    """
    Detect if input is already a STAC Item or Collection.

    For directories, only returns STAC type if it contains STAC files
    but NO parquet files (i.e., it's a pure STAC directory, not a mixed directory).

    Args:
        input_path: Path to file or directory

    Returns:
        "Item" if STAC Item, "Collection" if STAC Collection, None if not STAC or mixed
    """
    path = Path(input_path)

    if path.is_file():
        return _detect_stac_file(path)
    elif path.is_dir():
        return _detect_stac_directory(path)

    return None


def detect_pmtiles(base_path: str, verbose: bool = False) -> str | None:
    """
    Detect PMTiles overview file in directory.

    Rules:
    - Exactly 1 .pmtiles file → return path
    - 0 files → return None (warn in caller)
    - >1 files → raise error

    Args:
        base_path: Directory to search (single file dir or partition root)
        verbose: Print verbose output

    Returns:
        Path to PMTiles file or None

    Raises:
        click.ClickException: If multiple PMTiles files found
    """
    # For single file, check same directory
    if os.path.isfile(base_path):
        search_dir = os.path.dirname(base_path)
    else:
        # For directory, search root
        search_dir = base_path

    # Find all .pmtiles files
    pmtiles_files = list(Path(search_dir).glob("*.pmtiles"))

    if len(pmtiles_files) == 0:
        # No PMTiles found - caller will warn
        return None
    elif len(pmtiles_files) == 1:
        # Exactly one - perfect!
        if verbose:
            debug(f"Found PMTiles overview: {pmtiles_files[0].name}")
        return str(pmtiles_files[0])
    else:
        # Multiple found - error
        files_list = "\n  - ".join([f.name for f in pmtiles_files])
        raise click.ClickException(
            f"Multiple PMTiles files found in {search_dir}:\n  - {files_list}\n\n"
            "Keep only one PMTiles overview file.\n"
            "Recommended: Use 'overview.pmtiles' as the standard name."
        )


def generate_stac_geometry(parquet_file: str, verbose: bool = False) -> dict:
    """
    Generate GeoJSON geometry for STAC Item from dataset bounds.

    Uses existing get_dataset_bounds() function.

    Args:
        parquet_file: Path to parquet file
        verbose: Print verbose output

    Returns:
        GeoJSON Polygon dict representing dataset extent
    """
    bounds = get_dataset_bounds(parquet_file, verbose=verbose)
    if not bounds:
        raise click.ClickException("Could not calculate dataset bounds")

    xmin, ymin, xmax, ymax = bounds

    # Convert bbox to GeoJSON Polygon (closed ring)
    return {
        "type": "Polygon",
        "coordinates": [[[xmin, ymin], [xmax, ymin], [xmax, ymax], [xmin, ymax], [xmin, ymin]]],
    }


def generate_item_id(parquet_file: str, partition_key: str | None = None) -> str:
    """
    Generate unique STAC Item ID from filename or partition key.

    Args:
        parquet_file: Path to parquet file
        partition_key: Optional partition identifier (e.g., "usa", "can")

    Returns:
        STAC Item ID string
    """
    if partition_key:
        # Use partition key directly
        return partition_key
    else:
        # Derive from filename without extension
        return Path(parquet_file).stem


def construct_asset_href(filename: str, bucket_prefix: str, public_url: str | None = None) -> str:
    """
    Construct asset href from bucket prefix and optional public URL.

    Args:
        filename: Base filename (e.g., "usa.parquet")
        bucket_prefix: S3 or local path prefix
        public_url: Optional public HTTPS URL mapping

    Returns:
        Full asset href (S3 URI or HTTPS URL)
    """
    # Remove trailing slash from bucket prefix
    bucket_prefix = bucket_prefix.rstrip("/")

    if public_url:
        # User provided explicit public URL mapping
        public_url = public_url.rstrip("/")
        return f"{public_url}/{filename}"

    # Default: use bucket prefix as-is (S3 URI or local path)
    return f"{bucket_prefix}/{filename}"


def _add_projection_properties(item: pystac.Item, geo_meta: dict | None, parquet_file: str) -> None:
    """Add projection info to STAC Item properties if available."""
    if not geo_meta:
        return

    geom_col = find_primary_geometry_column(parquet_file, verbose=False)
    if "columns" not in geo_meta or geom_col not in geo_meta["columns"]:
        return

    col_info = geo_meta["columns"][geom_col]

    # Add CRS if available
    if "crs" in col_info:
        crs = col_info["crs"]
        # Try to extract EPSG code
        if isinstance(crs, dict):
            if "id" in crs and "code" in crs["id"]:
                item.properties["proj:epsg"] = crs["id"]["code"]
            # Store full CRS as PROJJSON
            item.properties["proj:projjson"] = crs
        elif isinstance(crs, str):
            # WKT string
            item.properties["proj:wkt2"] = crs

    # Add geometry types if available
    if "geometry_types" in col_info:
        item.properties["geoparquet:geometry_types"] = col_info["geometry_types"]


def _add_pmtiles_asset(
    item: pystac.Item,
    parquet_file: str,
    bucket_prefix: str,
    public_url: str | None,
    verbose: bool,
    show_warning: bool = True,
) -> None:
    """Add PMTiles asset to STAC Item if present."""
    pmtiles_path = detect_pmtiles(parquet_file, verbose=False)
    if pmtiles_path:
        pmtiles_filename = Path(pmtiles_path).name
        item.add_asset(
            key="overview",
            asset=pystac.Asset(
                href=construct_asset_href(pmtiles_filename, bucket_prefix, public_url),
                media_type="application/vnd.pmtiles",
                roles=["visual", "overview"],
                title="PMTiles overview for interactive mapping",
            ),
        )
        if verbose:
            debug(f"Added PMTiles asset: {pmtiles_filename}")
    elif show_warning:
        warn(
            "⚠️  No PMTiles overview found. Consider creating one for map visualization.\n"
            "   See: https://github.com/felt/tippecanoe"
        )


def _add_self_link(
    item: pystac.Item, item_id: str, bucket_prefix: str, public_url: str | None
) -> None:
    """Add self link to STAC Item."""
    item.add_link(
        pystac.Link(
            rel=pystac.RelType.SELF,
            target=construct_asset_href(f"{item_id}.json", bucket_prefix, public_url),
            media_type=pystac.MediaType.JSON,
        )
    )


def _add_collection_link(item: pystac.Item, bucket_prefix: str, public_url: str | None) -> None:
    """Add collection/parent link to STAC Item."""
    item.add_link(
        pystac.Link(
            rel=pystac.RelType.COLLECTION,
            target=construct_asset_href("collection.json", bucket_prefix, public_url),
            media_type=pystac.MediaType.JSON,
        )
    )


def get_file_datetime(parquet_file: str) -> datetime:
    """
    Get datetime for STAC Item from file modification time.

    Args:
        parquet_file: Path to parquet file

    Returns:
        UTC datetime object
    """
    if os.path.exists(parquet_file):
        mtime = os.path.getmtime(parquet_file)
        return datetime.fromtimestamp(mtime, tz=timezone.utc)
    else:
        # Fallback to current time for remote files
        return datetime.now(timezone.utc)


def generate_stac_item(
    parquet_file: str,
    bucket_prefix: str,
    public_url: str | None = None,
    item_id: str | None = None,
    verbose: bool = False,
) -> dict:
    """
    Generate STAC Item for a single GeoParquet file.

    Uses existing metadata extraction:
    - get_dataset_bounds() for bbox
    - parse_geo_metadata() for CRS/geometry info
    - file mtime or current time for datetime

    Args:
        parquet_file: Path to input parquet file
        bucket_prefix: S3 bucket prefix (e.g., "s3://bucket/path/")
        public_url: Optional public URL mapping
        item_id: Optional custom item ID (auto-generated if None)
        verbose: Print verbose output

    Returns:
        STAC Item as dict (pystac.Item.to_dict())
    """
    # TODO: Consider supporting remote files in the future if there's demand.
    # This would require careful consideration of:
    # - Asset hrefs pointing to files user may not control
    # - Mixed local/remote semantics in STAC catalogs
    # - Use cases: cataloging public datasets vs. self-owned data
    # For now, blocking to avoid confusing semantics and edge cases.
    if is_remote_url(parquet_file):
        raise click.ClickException(
            "STAC generation requires local parquet files.\n"
            "Remote files cannot be cataloged because STAC asset hrefs would reference "
            "files you may not control.\n"
            "Download the file first or use 'gpio convert' to create a local copy."
        )

    if verbose:
        debug(f"Generating STAC Item for {parquet_file}")

    # Check for PMTiles immediately (before processing)
    pmtiles_path = detect_pmtiles(parquet_file, verbose=False)
    if not pmtiles_path:
        warn(
            "⚠️  No PMTiles overview found. Consider creating one for map visualization.\n"
            "   See: https://github.com/felt/tippecanoe"
        )

    # Get metadata
    metadata, _ = get_parquet_metadata(parquet_file, verbose=False)
    geo_meta = parse_geo_metadata(metadata, verbose=False)

    # Get bounds and geometry
    bounds = get_dataset_bounds(parquet_file, verbose=False)
    if not bounds:
        raise click.ClickException(f"Could not calculate bounds for {parquet_file}")

    geometry = generate_stac_geometry(parquet_file, verbose=False)

    # Get datetime
    datetime_obj = get_file_datetime(parquet_file)

    # Generate item ID
    final_item_id = item_id or generate_item_id(parquet_file)

    # Create pystac Item
    item = pystac.Item(
        id=final_item_id,
        geometry=geometry,
        bbox=list(bounds),
        datetime=datetime_obj,
        properties={},
    )

    # Add GeoParquet asset
    parquet_filename = Path(parquet_file).name
    item.add_asset(
        key="data",
        asset=pystac.Asset(
            href=construct_asset_href(parquet_filename, bucket_prefix, public_url),
            media_type="application/vnd.apache.parquet",
            roles=["data"],
            title="GeoParquet dataset",
        ),
    )

    # Add projection info to properties if available
    _add_projection_properties(item, geo_meta, parquet_file)

    # Add PMTiles asset if present (warning already shown above)
    _add_pmtiles_asset(item, parquet_file, bucket_prefix, public_url, verbose, show_warning=False)

    # Add self link
    _add_self_link(item, final_item_id, bucket_prefix, public_url)

    return item.to_dict()


def _generate_stac_item_no_warning(
    parquet_file: str,
    bucket_prefix: str,
    public_url: str | None = None,
    item_id: str | None = None,
    add_collection_link: bool = False,
) -> dict:
    """
    Generate STAC Item without PMTiles warning (used for collection items).

    Internal function to avoid duplicate warnings when generating collections.

    Args:
        parquet_file: Path to parquet file
        bucket_prefix: S3 bucket prefix
        public_url: Optional public URL mapping
        item_id: Optional custom item ID
        add_collection_link: Whether to add collection/parent link

    Returns:
        STAC Item as dict
    """
    # Get metadata
    metadata, _ = get_parquet_metadata(parquet_file, verbose=False)
    geo_meta = parse_geo_metadata(metadata, verbose=False)

    # Get bounds and geometry
    bounds = get_dataset_bounds(parquet_file, verbose=False)
    if not bounds:
        raise click.ClickException(f"Could not calculate bounds for {parquet_file}")

    geometry = generate_stac_geometry(parquet_file, verbose=False)
    datetime_obj = get_file_datetime(parquet_file)
    final_item_id = item_id or generate_item_id(parquet_file)

    # Create pystac Item
    item = pystac.Item(
        id=final_item_id,
        geometry=geometry,
        bbox=list(bounds),
        datetime=datetime_obj,
        properties={},
    )

    # Add GeoParquet asset
    parquet_filename = Path(parquet_file).name
    item.add_asset(
        key="data",
        asset=pystac.Asset(
            href=construct_asset_href(parquet_filename, bucket_prefix, public_url),
            media_type="application/vnd.apache.parquet",
            roles=["data"],
            title="GeoParquet dataset",
        ),
    )

    # Add projection info and PMTiles (without warning)
    _add_projection_properties(item, geo_meta, parquet_file)
    _add_pmtiles_asset(
        item, parquet_file, bucket_prefix, public_url, verbose=False, show_warning=False
    )
    _add_self_link(item, final_item_id, bucket_prefix, public_url)

    # Add collection link if part of collection
    if add_collection_link:
        _add_collection_link(item, bucket_prefix, public_url)

    return item.to_dict()


def generate_stac_collection(
    partition_dir: str,
    bucket_prefix: str,
    public_url: str | None = None,
    collection_id: str | None = None,
    verbose: bool = False,
) -> tuple[dict, list[dict]]:
    """
    Generate STAC Collection and Items for partitioned dataset.

    Args:
        partition_dir: Directory containing partition files
        bucket_prefix: S3 bucket prefix
        public_url: Optional public URL mapping
        collection_id: Optional custom collection ID
        verbose: Print verbose output

    Returns:
        Tuple of (collection_dict, list_of_item_dicts)
    """
    # Collections require local directory structure
    # Remote directories don't make sense for STAC generation
    if is_remote_url(partition_dir):
        raise click.ClickException(
            "STAC collection generation requires a local directory.\n"
            "Remote directories cannot be cataloged.\n"
            "Download the files first or partition a local dataset."
        )

    if verbose:
        debug(f"Generating STAC Collection for {partition_dir}")

    # Check for PMTiles immediately (before processing)
    pmtiles_path = detect_pmtiles(partition_dir, verbose=False)
    if not pmtiles_path:
        warn(
            "⚠️  No PMTiles overview found. Consider creating one for map visualization.\n"
            "   See: https://github.com/felt/tippecanoe"
        )

    # Find all .parquet files in partition_dir
    all_files = _find_partition_files(partition_dir)
    if not all_files:
        raise click.ClickException(f"No parquet files found in {partition_dir}")

    if verbose:
        debug(f"Found {len(all_files)} partition files")

    # Generate items for each partition
    items, overall_bounds = _generate_collection_items(all_files, bucket_prefix, public_url)

    # Generate collection ID
    final_collection_id = collection_id or Path(partition_dir).name

    # Create collection
    collection = pystac.Collection(
        id=final_collection_id,
        description=f"GeoParquet dataset: {final_collection_id}",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent(bboxes=[overall_bounds]),
            temporal=pystac.TemporalExtent(intervals=[[None, None]]),
        ),
    )

    # Add links to all items
    for item_dict in items:
        collection.add_link(
            pystac.Link(
                rel=pystac.RelType.ITEM,
                target=construct_asset_href(f"{item_dict['id']}.json", bucket_prefix, public_url),
                media_type=pystac.MediaType.JSON,
            )
        )

    # Add self link
    collection.add_link(
        pystac.Link(
            rel=pystac.RelType.SELF,
            target=construct_asset_href("collection.json", bucket_prefix, public_url),
            media_type=pystac.MediaType.JSON,
        )
    )

    # Add PMTiles overview if in root directory
    _add_collection_pmtiles(collection, partition_dir, bucket_prefix, public_url, verbose)

    if verbose:
        debug(f"Generated collection with {len(items)} items")

    return collection.to_dict(), items


def _find_partition_files(partition_dir: str) -> list[Path]:
    """Find all parquet files in partition directory."""
    partition_files = list(Path(partition_dir).glob("*.parquet"))
    hive_partitions = list(Path(partition_dir).glob("*/*.parquet"))
    return partition_files + hive_partitions


def _generate_collection_items(
    all_files: list[Path], bucket_prefix: str, public_url: str | None
) -> tuple[list[dict], list[float]]:
    """Generate STAC items for all partition files and calculate overall bounds."""
    items = []
    overall_bounds: list[float] | None = None

    for parquet_file in all_files:
        partition_key = parquet_file.stem
        # Generate item without PMTiles warning (will show once for collection)
        # Add collection link since these are part of a collection
        item_dict = _generate_stac_item_no_warning(
            str(parquet_file),
            bucket_prefix,
            public_url,
            item_id=partition_key,
            add_collection_link=True,
        )
        items.append(item_dict)

        # Update overall bounds
        item_bbox = item_dict["bbox"]
        if overall_bounds is None:
            overall_bounds = list(item_bbox)
        else:
            overall_bounds[0] = min(overall_bounds[0], item_bbox[0])  # xmin
            overall_bounds[1] = min(overall_bounds[1], item_bbox[1])  # ymin
            overall_bounds[2] = max(overall_bounds[2], item_bbox[2])  # xmax
            overall_bounds[3] = max(overall_bounds[3], item_bbox[3])  # ymax

    # overall_bounds is guaranteed to be set after first iteration
    assert overall_bounds is not None
    return items, overall_bounds


def _add_collection_pmtiles(
    collection: pystac.Collection,
    partition_dir: str,
    bucket_prefix: str,
    public_url: str | None,
    verbose: bool,
) -> None:
    """Add PMTiles asset to STAC Collection if present."""
    pmtiles_path = detect_pmtiles(partition_dir, verbose=False)
    if pmtiles_path:
        pmtiles_filename = Path(pmtiles_path).name
        collection.add_asset(
            key="overview",
            asset=pystac.Asset(
                href=construct_asset_href(pmtiles_filename, bucket_prefix, public_url),
                media_type="application/vnd.pmtiles",
                roles=["visual", "overview"],
                title="PMTiles overview for interactive mapping",
            ),
        )
        if verbose:
            debug(f"Added PMTiles asset to collection: {pmtiles_filename}")


def write_stac_json(stac_dict: dict, output_path: str, verbose: bool = False):
    """
    Write STAC dict to JSON file with proper formatting.

    Args:
        stac_dict: STAC Item or Collection dict
        output_path: Output JSON file path
        verbose: Print verbose output
    """
    with open(output_path, "w") as f:
        json.dump(stac_dict, f, indent=2)

    if verbose:
        debug(f"Wrote STAC JSON to {output_path}")
