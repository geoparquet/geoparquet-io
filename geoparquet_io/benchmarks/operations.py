"""Operation definitions for benchmark suite."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, TypedDict

import pyarrow.parquet as pq


# Type-safe operation definition
class OperationInfo(TypedDict):
    """Type-safe definition for benchmark operations."""

    name: str
    description: str
    run: Callable[[Path, Path], dict[str, Any]]


def _run_read(input_path: Path, _output_dir: Path) -> dict[str, Any]:
    """Benchmark read operation."""
    table = pq.read_table(input_path)
    return {"rows": table.num_rows, "columns": table.num_columns}


def _run_write(input_path: Path, output_dir: Path) -> dict[str, Any]:
    """Benchmark write operation."""
    table = pq.read_table(input_path)
    output_path = output_dir / "output.parquet"
    # Use ZSTD level 15 for optimal compression (per PyArrow best practices)
    pq.write_table(table, output_path, compression="zstd", compression_level=15)
    return {"output_size_mb": output_path.stat().st_size / (1024 * 1024)}


def _run_convert_geojson(input_path: Path, output_dir: Path) -> dict[str, Any]:
    """Benchmark GeoJSON conversion."""
    from geoparquet_io.core.convert import convert_to_geoparquet

    output_path = output_dir / "output.parquet"
    geojson_path = input_path.with_suffix(".geojson")
    if geojson_path.exists():
        convert_to_geoparquet(str(geojson_path), str(output_path))
        return {"output_size_mb": output_path.stat().st_size / (1024 * 1024)}
    return {"skipped": True, "reason": "No GeoJSON version available"}


def _run_convert_gpkg(input_path: Path, output_dir: Path) -> dict[str, Any]:
    """Benchmark GeoPackage conversion."""
    from geoparquet_io.core.convert import convert_to_geoparquet

    output_path = output_dir / "output.parquet"
    gpkg_path = input_path.with_suffix(".gpkg")
    if gpkg_path.exists():
        convert_to_geoparquet(str(gpkg_path), str(output_path))
        return {"output_size_mb": output_path.stat().st_size / (1024 * 1024)}
    return {"skipped": True, "reason": "No GeoPackage version available"}


def _run_extract_bbox(input_path: Path, output_dir: Path) -> dict[str, Any]:
    """Benchmark bbox extraction."""
    from geoparquet_io.core.extract import extract

    output_path = output_dir / "output.parquet"
    # Use a bbox that covers ~50% of typical data
    extract(
        str(input_path),
        str(output_path),
        bbox="-180,-45,0,45",
    )
    result_table = pq.read_table(output_path)
    return {"output_rows": result_table.num_rows}


def _run_extract_columns(input_path: Path, output_dir: Path) -> dict[str, Any]:
    """Benchmark column extraction."""
    from geoparquet_io.core.extract import extract

    output_path = output_dir / "output.parquet"
    schema = pq.read_schema(input_path)
    columns = [schema.field(i).name for i in range(min(3, len(schema)))]
    extract(
        str(input_path),
        str(output_path),
        include_cols=",".join(columns),
    )
    return {"columns_selected": len(columns)}


def _run_reproject(input_path: Path, output_dir: Path) -> dict[str, Any]:
    """Benchmark reprojection."""
    from geoparquet_io.core.reproject import reproject

    output_path = output_dir / "output.parquet"
    reproject(str(input_path), str(output_path), target_crs="EPSG:3857")
    return {"target_crs": "EPSG:3857"}


def _run_sort_hilbert(input_path: Path, output_dir: Path) -> dict[str, Any]:
    """Benchmark Hilbert sorting."""
    from geoparquet_io.core.hilbert_order import hilbert_order

    output_path = output_dir / "output.parquet"
    hilbert_order(str(input_path), str(output_path))
    return {"sorted": True}


def _run_add_bbox(input_path: Path, output_dir: Path) -> dict[str, Any]:
    """Benchmark adding bbox column."""
    from geoparquet_io.core.add_bbox_column import add_bbox_column

    output_path = output_dir / "output.parquet"
    add_bbox_column(str(input_path), str(output_path), force=True)
    return {"bbox_added": True}


def _run_partition_quadkey(input_path: Path, output_dir: Path) -> dict[str, Any]:
    """Benchmark quadkey partitioning."""
    from geoparquet_io.core.partition_by_quadkey import partition_by_quadkey

    output_path = output_dir / "partitioned"
    partition_by_quadkey(str(input_path), str(output_path), partition_resolution=4)
    output_files = list(Path(output_path).glob("**/*.parquet"))
    return {"partitions": len(output_files)}


# Registry mapping operation names to handlers (TypedDict for type safety)
OPERATION_REGISTRY: dict[str, OperationInfo] = {
    "read": {
        "name": "Read",
        "description": "Load parquet into memory",
        "run": _run_read,
    },
    "write": {
        "name": "Write",
        "description": "Write table back to parquet",
        "run": _run_write,
    },
    "convert-geojson": {
        "name": "Convert GeoJSON",
        "description": "GeoJSON to GeoParquet",
        "run": _run_convert_geojson,
    },
    "convert-gpkg": {
        "name": "Convert GeoPackage",
        "description": "GeoPackage to GeoParquet",
        "run": _run_convert_gpkg,
    },
    "extract-bbox": {
        "name": "Extract BBox",
        "description": "Spatial filtering",
        "run": _run_extract_bbox,
    },
    "extract-columns": {
        "name": "Extract Columns",
        "description": "Column selection",
        "run": _run_extract_columns,
    },
    "reproject": {
        "name": "Reproject",
        "description": "CRS transformation (4326->3857)",
        "run": _run_reproject,
    },
    "sort-hilbert": {
        "name": "Sort Hilbert",
        "description": "Hilbert curve ordering",
        "run": _run_sort_hilbert,
    },
    "add-bbox": {
        "name": "Add BBox",
        "description": "Compute bbox column",
        "run": _run_add_bbox,
    },
    "partition-quadkey": {
        "name": "Partition Quadkey",
        "description": "Partition by quadkey",
        "run": _run_partition_quadkey,
    },
}


def get_operation(name: str) -> OperationInfo:
    """Get operation by name.

    Args:
        name: Operation name (e.g., "read", "write", "sort-hilbert")

    Returns:
        OperationInfo with name, description, and run callable

    Raises:
        KeyError: If operation name is not registered
    """
    if name not in OPERATION_REGISTRY:
        raise KeyError(f"Unknown operation: {name}")
    return OPERATION_REGISTRY[name]
