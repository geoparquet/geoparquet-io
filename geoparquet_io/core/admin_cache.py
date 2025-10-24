#!/usr/bin/env python3

"""
Caching for remote admin boundary datasets.

Provides local caching of remote datasets to improve performance and reduce
network usage for repeated operations.
"""

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Optional

import click


def get_cache_dir() -> Path:
    """
    Get the cache directory for admin datasets.

    Uses XDG_CACHE_HOME if set, otherwise ~/.cache/geoparquet-io/admin-datasets/

    Returns:
        Path to cache directory
    """
    if "XDG_CACHE_HOME" in os.environ:
        cache_root = Path(os.environ["XDG_CACHE_HOME"])
    else:
        cache_root = Path.home() / ".cache"

    cache_dir = cache_root / "geoparquet-io" / "admin-datasets"
    cache_dir.mkdir(parents=True, exist_ok=True)

    return cache_dir


def get_cache_key(dataset_url: str) -> str:
    """
    Generate a cache key for a dataset URL.

    Args:
        dataset_url: URL of the remote dataset

    Returns:
        Cache key (hash of URL)
    """
    # Use SHA256 hash of URL as cache key
    url_hash = hashlib.sha256(dataset_url.encode()).hexdigest()[:16]
    return url_hash


def get_cached_file(dataset_url: str, dataset_name: str) -> Optional[str]:
    """
    Get cached file path if it exists.

    Args:
        dataset_url: URL of the remote dataset
        dataset_name: Human-readable dataset name

    Returns:
        Path to cached file, or None if not cached
    """
    cache_dir = get_cache_dir()
    cache_key = get_cache_key(dataset_url)
    cache_file = cache_dir / f"{dataset_name}_{cache_key}.parquet"

    if cache_file.exists():
        return str(cache_file)

    return None


def get_cache_metadata(dataset_url: str, dataset_name: str) -> Optional[dict]:
    """
    Get metadata about cached dataset.

    Args:
        dataset_url: URL of the remote dataset
        dataset_name: Human-readable dataset name

    Returns:
        Metadata dict, or None if not cached
    """
    cache_file = get_cached_file(dataset_url, dataset_name)
    if not cache_file:
        return None

    cache_dir = get_cache_dir()
    cache_key = get_cache_key(dataset_url)
    metadata_file = cache_dir / f"{dataset_name}_{cache_key}.json"

    if metadata_file.exists():
        with open(metadata_file) as f:
            return json.load(f)

    # If no metadata but file exists, create basic metadata
    stat = os.stat(cache_file)
    return {
        "url": dataset_url,
        "dataset_name": dataset_name,
        "cached_at": stat.st_mtime,
        "size_bytes": stat.st_size,
    }


def save_cache_metadata(dataset_url: str, dataset_name: str, metadata: dict):
    """
    Save metadata about cached dataset.

    Args:
        dataset_url: URL of the remote dataset
        dataset_name: Human-readable dataset name
        metadata: Metadata to save
    """
    cache_dir = get_cache_dir()
    cache_key = get_cache_key(dataset_url)
    metadata_file = cache_dir / f"{dataset_name}_{cache_key}.json"

    with open(metadata_file, "w") as f:
        json.dump(metadata, f, indent=2)


def cache_dataset(dataset_url: str, dataset_name: str, verbose: bool = False) -> str:
    """
    Download and cache a remote dataset.

    Args:
        dataset_url: URL of the remote dataset
        dataset_name: Human-readable dataset name
        verbose: Enable verbose output

    Returns:
        Path to cached file
    """
    import duckdb

    cache_dir = get_cache_dir()
    cache_key = get_cache_key(dataset_url)
    cache_file = cache_dir / f"{dataset_name}_{cache_key}.parquet"

    if cache_file.exists():
        if verbose:
            click.echo(f"Using cached dataset: {cache_file}")
        return str(cache_file)

    if verbose:
        click.echo(f"Downloading {dataset_name} dataset...")
        click.echo(f"  From: {dataset_url}")
        click.echo(f"  To: {cache_file}")

    # Download and cache using DuckDB
    con = duckdb.connect()
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    # Configure for source.coop S3 if needed
    if "source.coop" in dataset_url or "s3://" in dataset_url:
        con.execute("SET s3_endpoint='data.source.coop';")
        con.execute("SET s3_url_style='path';")
        con.execute("SET s3_use_ssl=true;")

    try:
        # Copy remote dataset to local cache
        start_time = time.time()

        # For wildcard patterns, we need to read and write
        if "*" in dataset_url:
            copy_query = f"""
                COPY (SELECT * FROM '{dataset_url}')
                TO '{cache_file}'
                (FORMAT PARQUET, COMPRESSION 'ZSTD', COMPRESSION_LEVEL 15)
            """
        else:
            copy_query = f"""
                COPY (SELECT * FROM '{dataset_url}')
                TO '{cache_file}'
                (FORMAT PARQUET, COMPRESSION 'ZSTD', COMPRESSION_LEVEL 15)
            """

        con.execute(copy_query)

        elapsed = time.time() - start_time

        # Get file size
        size_bytes = os.path.getsize(cache_file)
        size_mb = size_bytes / (1024 * 1024)

        # Save metadata
        metadata = {
            "url": dataset_url,
            "dataset_name": dataset_name,
            "cached_at": time.time(),
            "size_bytes": size_bytes,
            "download_time_seconds": elapsed,
        }
        save_cache_metadata(dataset_url, dataset_name, metadata)

        if verbose:
            click.echo(f"  ✓ Downloaded {size_mb:.1f} MB in {elapsed:.1f}s")

    except Exception as e:
        # Clean up partial download
        if cache_file.exists():
            cache_file.unlink()
        raise click.ClickException(f"Failed to download dataset: {e}") from e
    finally:
        con.close()

    return str(cache_file)


def clear_cache(dataset_name: Optional[str] = None, verbose: bool = False) -> int:
    """
    Clear cached admin datasets.

    Args:
        dataset_name: Specific dataset to clear, or None to clear all
        verbose: Enable verbose output

    Returns:
        Number of files removed
    """
    cache_dir = get_cache_dir()

    if not cache_dir.exists():
        return 0

    removed_count = 0

    if dataset_name:
        # Remove specific dataset caches
        pattern = f"{dataset_name}_*.parquet"
        for cache_file in cache_dir.glob(pattern):
            if verbose:
                click.echo(f"Removing: {cache_file.name}")
            cache_file.unlink()
            removed_count += 1

            # Also remove metadata
            metadata_file = cache_file.with_suffix(".json")
            if metadata_file.exists():
                metadata_file.unlink()
                removed_count += 1
    else:
        # Remove all caches
        for cache_file in cache_dir.glob("*.parquet"):
            if verbose:
                click.echo(f"Removing: {cache_file.name}")
            cache_file.unlink()
            removed_count += 1

        for metadata_file in cache_dir.glob("*.json"):
            if verbose:
                click.echo(f"Removing: {metadata_file.name}")
            metadata_file.unlink()
            removed_count += 1

    return removed_count


def list_cached_datasets(verbose: bool = False) -> list:
    """
    List all cached datasets.

    Args:
        verbose: Enable verbose output

    Returns:
        List of cache info dicts
    """
    cache_dir = get_cache_dir()

    if not cache_dir.exists():
        return []

    cached = []

    for cache_file in cache_dir.glob("*.parquet"):
        # Try to load metadata
        metadata_file = cache_file.with_suffix(".json")
        if metadata_file.exists():
            with open(metadata_file) as f:
                metadata = json.load(f)
        else:
            # Generate basic info from file
            stat = os.stat(cache_file)
            metadata = {
                "dataset_name": cache_file.stem.rsplit("_", 1)[0],
                "cached_at": stat.st_mtime,
                "size_bytes": stat.st_size,
            }

        metadata["file_path"] = str(cache_file)
        cached.append(metadata)

    return cached
