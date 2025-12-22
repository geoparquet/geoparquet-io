#!/usr/bin/env python3
"""
STAC validation utilities.
"""

import json
from pathlib import Path

import click
import pystac.validation

from geoparquet_io.core.logging_config import debug, error, progress, success, warn


def _load_stac_json(stac_path: str) -> dict:
    """Load STAC JSON file."""
    try:
        with open(stac_path) as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}") from e
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {stac_path}") from None


def _validate_stac_spec(stac_dict: dict, results: dict, verbose: bool) -> None:
    """Validate STAC spec compliance via pystac.validation."""
    stac_type = stac_dict.get("type", "Unknown")
    stac_version = stac_dict.get("stac_version", "Unknown")
    results["info"]["stac_type"] = stac_type
    results["info"]["stac_version"] = stac_version

    try:
        pystac.validation.validate_dict(stac_dict)
        if verbose:
            success(f"✓ Valid STAC {stac_type} (version {stac_version})")
    except ImportError as e:
        results["warnings"].append(
            f"STAC validation unavailable: {e}. Install jsonschema for full validation."
        )
    except Exception as e:
        error_msg = str(e)
        if "jsonschema" in error_msg.lower():
            results["warnings"].append(
                f"STAC validation unavailable: {error_msg}. Install jsonschema for full validation."
            )
        else:
            results["valid"] = False
            results["errors"].append(f"STAC validation failed: {error_msg}")


def _check_required_fields(stac_dict: dict, results: dict) -> None:
    """Check required fields for Items and Collections."""
    stac_type = stac_dict.get("type")
    if stac_type == "Feature":
        required_fields = ["id", "geometry", "bbox", "properties", "assets", "links"]
        for field in required_fields:
            if field not in stac_dict:
                results["errors"].append(f"Missing required field: {field}")
                results["valid"] = False

        if "properties" in stac_dict and "datetime" not in stac_dict["properties"]:
            results["warnings"].append("Missing 'datetime' in properties")
    elif stac_type == "Collection":
        required_fields = ["id", "description", "extent", "links"]
        for field in required_fields:
            if field not in stac_dict:
                results["errors"].append(f"Missing required field: {field}")
                results["valid"] = False


def _validate_assets(stac_dict: dict, stac_dir: Path, results: dict) -> None:
    """Validate assets: href resolution and best practices."""
    if "assets" not in stac_dict:
        return

    for asset_key, asset in stac_dict["assets"].items():
        href = asset.get("href", "")
        if not href.startswith(("http://", "https://", "s3://")):
            asset_path = stac_dir / href
            if not asset_path.exists():
                results["warnings"].append(
                    f"Asset '{asset_key}' href does not resolve locally: {href}"
                )

        if "type" not in asset:
            results["warnings"].append(f"Asset '{asset_key}' missing media type")

    has_data_asset = any("data" in asset.get("roles", []) for asset in stac_dict["assets"].values())
    if not has_data_asset:
        results["warnings"].append("No asset with 'data' role found")

    results["info"]["asset_count"] = len(stac_dict["assets"])


def _validate_links(stac_dict: dict, results: dict) -> None:
    """Validate links: check for self link."""
    if "links" not in stac_dict:
        return

    has_self_link = any(link.get("rel") == "self" for link in stac_dict["links"])
    if not has_self_link:
        results["warnings"].append("No 'self' link found (recommended)")

    results["info"]["link_count"] = len(stac_dict["links"])


def _validate_geometry_bbox(stac_dict: dict, results: dict) -> None:
    """Validate geometry/bbox consistency for Items."""
    if stac_dict.get("type") != "Feature":
        return

    geometry = stac_dict.get("geometry")
    bbox = stac_dict.get("bbox")
    if not (geometry and bbox):
        return

    if len(bbox) not in [4, 6]:
        results["errors"].append(f"Invalid bbox length: {len(bbox)} (expected 4 or 6)")
        results["valid"] = False
        return

    if len(bbox) == 4:
        xmin, ymin, xmax, ymax = bbox
        if xmin > xmax or ymin > ymax:
            results["errors"].append(f"Invalid bbox ordering: [{xmin}, {ymin}, {xmax}, {ymax}]")
            results["valid"] = False


def validate_stac_file(stac_path: str, verbose: bool = False) -> dict:
    """
    Validate STAC JSON file for compliance and best practices.

    Checks:
    - STAC spec compliance (via pystac.validation)
    - Required fields present
    - Asset hrefs resolve (if local)
    - Best practices

    Args:
        stac_path: Path to STAC JSON file
        verbose: Print verbose output

    Returns:
        Dict with validation results:
        {
            "valid": bool,
            "errors": list[str],
            "warnings": list[str],
            "info": dict
        }
    """
    results = {"valid": True, "errors": [], "warnings": [], "info": {}}

    # Load STAC JSON
    try:
        stac_dict = _load_stac_json(stac_path)
    except (ValueError, FileNotFoundError) as e:
        results["valid"] = False
        results["errors"].append(str(e))
        return results

    # Run validation checks
    _validate_stac_spec(stac_dict, results, verbose)
    _check_required_fields(stac_dict, results)
    _validate_assets(stac_dict, Path(stac_path).parent, results)
    _validate_links(stac_dict, results)
    _validate_geometry_bbox(stac_dict, results)

    return results


def _print_validation_results(results: dict, verbose: bool) -> None:
    """Print validation results to console."""
    if results["valid"]:
        success("✓ STAC validation passed")
    else:
        error("✗ STAC validation failed")

    if results["errors"]:
        progress("\nErrors:")
        for error_msg in results["errors"]:
            error(f"  ✗ {error_msg}")

    if results["warnings"]:
        progress("\nWarnings:")
        for warning in results["warnings"]:
            warn(f"  ⚠️  {warning}")

    if verbose and results["info"]:
        progress("\nInfo:")
        for key, value in results["info"].items():
            debug(f"  {key}: {value}")


def _should_raise_error(results: dict) -> bool:
    """Determine if validation should raise an error."""
    if not results["valid"] and len(results["errors"]) > 0:
        non_jsonschema_errors = [e for e in results["errors"] if "jsonschema" not in e.lower()]
        return bool(non_jsonschema_errors)
    return False


def check_stac(stac_path: str, verbose: bool = False):
    """
    CLI-friendly STAC validation function.

    Validates STAC file and prints results.

    Args:
        stac_path: Path to STAC JSON file
        verbose: Print verbose output
    """
    if verbose:
        debug(f"Validating STAC file: {stac_path}\n")

    results = validate_stac_file(stac_path, verbose)
    _print_validation_results(results, verbose)

    if _should_raise_error(results):
        raise click.ClickException("STAC validation failed")
