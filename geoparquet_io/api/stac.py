"""
STAC standalone functions for generating and validating STAC metadata.

These functions work with files on disk rather than in-memory tables,
as STAC generation requires file-level metadata and asset references.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from geoparquet_io.api.check import CheckResult


def generate_stac(
    input_path: str | Path,
    output_path: str | Path | None = None,
    *,
    bucket: str,
    item_id: str | None = None,
    collection_id: str | None = None,
    public_url: str | None = None,
    overwrite: bool = False,
    verbose: bool = False,
) -> Path:
    """
    Generate STAC Item or Collection from GeoParquet file(s).

    For a single file, generates a STAC Item. For a directory of partitioned
    files, generates a STAC Collection with Items for each partition.

    Args:
        input_path: Path to GeoParquet file or directory
        output_path: Output path for STAC JSON (defaults to input path + .json)
        bucket: S3 bucket prefix (e.g., "s3://bucket/path/")
        item_id: Custom item ID (auto-generated if None)
        collection_id: Custom collection ID (for directories)
        public_url: Optional public URL mapping
        overwrite: Overwrite existing STAC files
        verbose: Print verbose output

    Returns:
        Path to generated STAC JSON file

    Example:
        >>> from geoparquet_io import generate_stac
        >>> stac_path = generate_stac(
        ...     'data.parquet',
        ...     bucket='s3://my-bucket/data/',
        ...     item_id='my-dataset'
        ... )
        >>> print(f"Generated STAC: {stac_path}")
    """
    import json

    from geoparquet_io.core.stac import generate_stac_collection, generate_stac_item

    input_path = Path(input_path)

    # Determine output path
    if output_path is None:
        if input_path.is_file():
            output_path = input_path.with_suffix(".json")
        else:
            output_path = input_path / "collection.json"
    else:
        output_path = Path(output_path)

    # Check for existing file
    if output_path.exists() and not overwrite:
        raise ValueError(
            f"STAC file already exists: {output_path}\nUse overwrite=True to replace it."
        )

    # Generate STAC
    if input_path.is_file():
        # Single file -> STAC Item
        stac_dict = generate_stac_item(
            parquet_file=str(input_path),
            bucket_prefix=bucket,
            public_url=public_url,
            item_id=item_id,
            verbose=verbose,
        )

        # Write output
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(stac_dict, f, indent=2, default=str)
    else:
        # Directory -> STAC Collection with Items
        collection_dict, items_list = generate_stac_collection(
            partition_dir=str(input_path),
            bucket_prefix=bucket,
            public_url=public_url,
            collection_id=collection_id,
            verbose=verbose,
        )

        # Write collection
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(collection_dict, f, indent=2, default=str)

        # Write each item as a separate JSON file
        items_dir = output_path.parent / "items"
        items_dir.mkdir(parents=True, exist_ok=True)
        for item in items_list:
            item_id_str = item.get("id", "unknown")
            item_path = items_dir / f"{item_id_str}.json"
            with open(item_path, "w") as f:
                json.dump(item, f, indent=2, default=str)

    return output_path


def validate_stac(stac_path: str | Path, verbose: bool = False) -> CheckResult:
    """
    Validate a STAC Item or Collection.

    Checks:
    - Valid JSON structure
    - Required STAC fields present
    - Asset references valid
    - Geometry/bbox consistency

    Args:
        stac_path: Path to STAC JSON file
        verbose: Print verbose output

    Returns:
        CheckResult with validation results

    Example:
        >>> from geoparquet_io import validate_stac
        >>> result = validate_stac('collection.json')
        >>> if result.passed():
        ...     print("Valid STAC!")
        >>> else:
        ...     for failure in result.failures():
        ...         print(failure)
    """
    from geoparquet_io.api.check import CheckResult
    from geoparquet_io.core.stac_check import validate_stac_file

    results = validate_stac_file(str(stac_path), verbose=verbose)

    # Translate "valid" key to "passed" for CheckResult compatibility
    results["passed"] = results.pop("valid", True)

    return CheckResult(results, check_type="stac")
