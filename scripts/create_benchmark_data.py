#!/usr/bin/env python3
"""
Create benchmark data subsets from public GeoParquet sources.

This script creates standardized benchmark files at various sizes for
performance testing. Files are saved locally and can be uploaded to source.coop.

Usage:
    python scripts/create_benchmark_data.py [--output-dir ./benchmark-data]
"""

import argparse
import subprocess
from pathlib import Path

# Source data URLs
SOURCES = {
    "buildings": {
        "url": "https://data.source.coop/cholmes/overture/geoparquet-country-quad-2/SG.parquet",
        "description": "Overture Singapore Buildings (polygons)",
        "total_rows": 115_000,
    },
    "places": {
        "url": "https://data.source.coop/cholmes/overture/places-geoparquet-country/SG.parquet",
        "description": "Overture Singapore Places (points)",
        "total_rows": 50_000,  # Approximate
    },
    "fields_medium": {
        "url": "https://data.source.coop/fiboa/data/si/si-2024.parquet",
        "description": "Slovenia fiboa Field Boundaries (polygons)",
        "total_rows": 809_000,
    },
    "fields_large": {
        "url": "https://data.source.coop/fiboa/japan/japan.parquet",
        "description": "Japan fiboa Field Boundaries (polygons)",
        "total_rows": 29_400_000,
    },
}

# Target file sizes
SIZES = {
    "tiny": 1_000,
    "small": 10_000,
    "medium": 100_000,
    "large": 1_000_000,
    "xlarge": 10_000_000,
}


def run_gpio_extract(input_url: str, output_path: Path, limit: int) -> bool:
    """Run gpio extract to create a subset."""
    cmd = [
        "gpio",
        "extract",
        input_url,
        str(output_path),
        "--limit",
        str(limit),
    ]

    print(f"  Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode != 0:
            print(f"  Error: {result.stderr}")
            return False
        return True
    except subprocess.TimeoutExpired:
        print("  Error: Command timed out")
        return False
    except Exception as e:
        print(f"  Error: {e}")
        return False


def inspect_file(path: Path) -> dict:
    """Get file info using gpio inspect."""
    cmd = ["gpio", "inspect", str(path)]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        return {"success": True, "output": result.stdout}
    except Exception as e:
        return {"success": False, "error": str(e)}


def create_benchmark_files(output_dir: Path):
    """Create all benchmark files."""
    output_dir.mkdir(parents=True, exist_ok=True)

    created_files = []

    # Buildings - tiny, small, medium
    print("\n=== Creating building benchmark files ===")
    buildings_url = SOURCES["buildings"]["url"]

    for size_name in ["tiny", "small", "medium"]:
        limit = SIZES[size_name]
        output_path = output_dir / f"buildings_{size_name}.parquet"
        print(f"\nCreating {output_path.name} ({limit:,} rows)...")

        if run_gpio_extract(buildings_url, output_path, limit):
            created_files.append(output_path)
            print(f"  Created: {output_path}")

    # Places (points) - tiny, small
    print("\n=== Creating places benchmark files (points) ===")
    places_url = SOURCES["places"]["url"]

    for size_name in ["tiny", "small"]:
        limit = SIZES[size_name]
        output_path = output_dir / f"places_{size_name}.parquet"
        print(f"\nCreating {output_path.name} ({limit:,} rows)...")

        if run_gpio_extract(places_url, output_path, limit):
            created_files.append(output_path)
            print(f"  Created: {output_path}")

    # Slovenia fields - for large
    print("\n=== Creating field boundary benchmark files ===")
    slovenia_url = SOURCES["fields_medium"]["url"]

    # Full Slovenia dataset (~800K) as "large"
    output_path = output_dir / "fields_large.parquet"
    print(f"\nCreating {output_path.name} (full Slovenia, ~800K rows)...")
    if run_gpio_extract(slovenia_url, output_path, 1_000_000):  # Will get all ~800K
        created_files.append(output_path)
        print(f"  Created: {output_path}")

    # Japan fields - for xlarge (subset to 10M)
    japan_url = SOURCES["fields_large"]["url"]
    output_path = output_dir / "fields_xlarge.parquet"
    print(f"\nCreating {output_path.name} (10M rows from Japan)...")
    if run_gpio_extract(japan_url, output_path, 10_000_000):
        created_files.append(output_path)
        print(f"  Created: {output_path}")

    # Summary
    print("\n=== Summary ===")
    print(f"Created {len(created_files)} benchmark files in {output_dir}:\n")

    total_size = 0
    for f in created_files:
        if f.exists():
            size_mb = f.stat().st_size / (1024 * 1024)
            total_size += size_mb
            print(f"  {f.name}: {size_mb:.2f} MB")

    print(f"\nTotal size: {total_size:.2f} MB")

    # Print upload command
    print("\n=== To upload to source.coop ===")
    print("First, get credentials from https://source.coop/cholmes/gpio-test")
    print("Then run:")
    print(
        f"  aws s3 sync {output_dir} s3://us-west-2.opendata.source.coop/cholmes/gpio-test/benchmark/"
    )


def main():
    parser = argparse.ArgumentParser(description="Create benchmark data subsets")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./benchmark-data"),
        help="Output directory for benchmark files",
    )
    args = parser.parse_args()

    print("Creating benchmark data files...")
    print(f"Output directory: {args.output_dir}")

    create_benchmark_files(args.output_dir)


if __name__ == "__main__":
    main()
