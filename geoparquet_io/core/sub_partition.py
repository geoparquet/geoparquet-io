"""Sub-partition functionality for processing directories of parquet files."""

from __future__ import annotations

import importlib
import os
from pathlib import Path

from geoparquet_io.core.duckdb_metadata import get_row_count
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.index_ops import (
    get_index_spec,
    preflight_index,
    validate_partition_arguments,
)
from geoparquet_io.core.logging_config import (
    configure_verbose,
    debug,
    info,
    progress,
    success,
    warn,
)
from geoparquet_io.core.sizing import bytes_to_mb


def offending_single_file_only_options(
    partition_type: str,
    column_name: str | None = None,
    output_folder: str | None = None,
    *,
    column_label: str | None = None,
    output_label: str = "OUTPUT_FOLDER",
) -> list[str]:
    """Name the arguments a directory sub-partition run cannot honour.

    Args:
        partition_type: Type of partition ("a5", "h3", "s2", "quadkey")
        column_name: Index column name the caller asked for, if any
        output_folder: Single output directory the caller asked for, if any
        column_label: How to spell the column argument back at the caller
            (defaults to the CLI option name for ``partition_type``)
        output_label: How to spell the output argument back at the caller

    Returns:
        The labels that were supplied and cannot be honoured, in the order they
        should be reported. Empty when there is nothing to refuse.
    """
    spec = get_index_spec(partition_type)
    option, default = spec.column_option, spec.column_name
    label = column_label or option

    offending = []
    if label and column_name is not None and column_name != default:
        offending.append(label)
    if output_folder:
        offending.append(output_label)
    return offending


def single_file_only_option_message(partition_type: str, offending: list[str]) -> str:
    """Explain why ``offending`` cannot be honoured in directory mode."""
    verb = "does" if len(offending) == 1 else "do"
    return (
        f"{' and '.join(offending)} {verb} not apply to directory input with a size "
        f"threshold.\n\n"
        f"Each file over the threshold is partitioned into a sibling <file>_{partition_type}/\n"
        "directory, using the default index column name. Run on a single file if you\n"
        "need to control those."
    )


def plan_sub_partition(
    directory: str,
    partition_type: str,
    min_size_bytes: int,
    recursive: bool = True,
) -> list[dict]:
    """List the files a sub-partition run would process, and where each would go.

    The read-only half of :func:`sub_partition_directory`, backing ``--preview``
    and its API twin: nothing is partitioned and nothing is removed.

    Args:
        directory: Directory containing parquet files
        partition_type: Type of partition ("a5", "h3", "s2", "quadkey")
        min_size_bytes: Minimum file size to process
        recursive: Search subdirectories (default: True)

    Returns:
        One dict per candidate -- ``path``, ``size_bytes`` and the ``output_dir``
        it would be partitioned into -- largest file first.
    """
    candidates = []
    for file_path in find_large_files(directory, min_size_bytes, recursive=recursive):
        path = Path(file_path)
        candidates.append(
            {
                "path": file_path,
                "size_bytes": path.stat().st_size,
                "output_dir": str(path.parent / f"{path.stem}_{partition_type}"),
            }
        )
    return candidates


def _assert_no_rows_lost(source_file: str, output_dir: str) -> None:
    """Refuse to delete an original whose rows did not all reach the output.

    Partitioning drops rows whose partition value is NULL, and a NULL or empty
    geometry yields a NULL index cell -- so "some output parquet exists" was
    never proof the data survived. Compare row counts before ``--in-place``
    removes the only copy.

    Raises:
        RuntimeError: If the output holds no files, or fewer rows than the source.
    """

    output_files = sorted(Path(output_dir).glob("**/*.parquet"))
    if not output_files:
        raise RuntimeError(
            f"Sub-partition created no output files, keeping original: {source_file}"
        )

    source_rows = get_row_count(source_file)
    output_rows = sum(get_row_count(str(f)) for f in output_files)

    if output_rows != source_rows:
        raise RuntimeError(
            f"Sub-partition wrote {output_rows} row(s) from a {source_rows}-row source, "
            f"keeping original: {source_file}. Rows with a NULL or empty geometry get a "
            f"NULL index cell and are dropped by partitioning; the sub-partitions are in "
            f"{output_dir}"
        )


def quadkey_resolution_error(
    resolution: int | None,
    partition_resolution: int | None,
    *,
    auto: bool,
    labels: tuple[str, str] = ("--resolution", "--partition-resolution"),
    auto_label: str = "--auto",
) -> str | None:
    """Say what a quadkey run is missing, or None when it has what it needs.

    Quadkey is the one index that takes two numbers: the cell column is built at
    ``resolution`` and the directories are split on the first
    ``partition_resolution`` characters of it. Directory mode used to forward
    only the first, so every file tripped the single-file gate (#854); the check
    now runs once, before the loop, through either front door.

    Args:
        resolution: Resolution the cell column would be built at
        partition_resolution: Prefix length the partitions would be split on
        auto: Whether both are to be calculated from the data instead
        labels: How to spell the two arguments back at the caller
        auto_label: How to spell the auto argument back at the caller

    Returns:
        The message to report, or None when the combination is usable.
    """
    try:
        validate_partition_arguments(
            get_index_spec("quadkey"), resolution, partition_resolution, auto
        )
    except InvalidParameterError as exc:
        message = str(exc)
        return (
            message.replace("--partition-resolution", labels[1])
            .replace("--resolution", labels[0])
            .replace("--auto", auto_label)
        )
    return None


def find_large_files(
    directory: str,
    min_size_bytes: int,
    recursive: bool = True,
) -> list[str]:
    """
    Find parquet files in a directory that exceed the size threshold.

    Args:
        directory: Directory to search
        min_size_bytes: Minimum file size in bytes
        recursive: Search subdirectories (default: True)

    Returns:
        List of file paths exceeding the threshold, sorted by size descending
    """
    large_files = []
    dir_path = Path(directory)

    pattern = "**/*.parquet" if recursive else "*.parquet"

    for parquet_file in dir_path.glob(pattern):
        if parquet_file.is_file():
            size = parquet_file.stat().st_size
            if size >= min_size_bytes:
                large_files.append((str(parquet_file), size))

    # Sort by size descending (largest first)
    large_files.sort(key=lambda x: x[1], reverse=True)

    return [f[0] for f in large_files]


def sub_partition_directory(
    directory: str,
    partition_type: str,
    min_size_bytes: int,
    resolution: int | None = None,
    level: int | None = None,
    partition_resolution: int | None = None,
    use_centroid: bool = False,
    in_place: bool = False,
    hive: bool = False,
    overwrite: bool = False,
    verbose: bool = False,
    force: bool = False,
    skip_analysis: bool = True,
    compression: str | None = None,
    compression_level: int | None = None,
    auto: bool = False,
    target_rows: int = 100000,
    max_partitions: int = 10000,
) -> dict:
    """
    Sub-partition large files in a directory.

    Finds all parquet files exceeding min_size_bytes and partitions them
    using the specified spatial index type.

    Args:
        directory: Directory containing parquet files
        partition_type: Type of partition ("a5", "h3", "s2", "quadkey")
        min_size_bytes: Minimum file size to process
        resolution: Resolution for A5/H3/quadkey (0-15 for H3, 0-30 for A5)
        level: Level for S2 (alias for resolution)
        partition_resolution: Quadkey only -- prefix length the partitions are
            split on (0-23). Required alongside ``resolution`` unless ``auto``
        use_centroid: Quadkey only -- use the geometry centroid when adding the
            quadkey column
        in_place: If True, delete original after successful sub-partition
        hive: Use Hive-style partitioning
        overwrite: Overwrite existing output directories
        verbose: Print verbose output
        force: Force operation even with warnings
        skip_analysis: Skip partition analysis (default True for batch)
        compression: Compression codec
        compression_level: Compression level
        auto: Auto-calculate resolution
        target_rows: Target rows per partition for auto mode
        max_partitions: Max partitions for auto mode

    Returns:
        dict with keys: processed, skipped, errors
    """
    configure_verbose(verbose)
    spec = get_index_spec(partition_type)
    module = importlib.import_module(f"geoparquet_io.core.partition.by_{spec.name}")
    partition = getattr(module, f"partition_by_{spec.name}")
    res_value = resolution if resolution is not None else level
    validate_partition_arguments(spec, res_value, partition_resolution, auto)
    preflight_index(spec, "partition", directory=True)

    large_files = find_large_files(directory, min_size_bytes)

    if not large_files:
        info(f"No files found exceeding {bytes_to_mb(min_size_bytes):.1f}MB in {directory}")
        return {"processed": 0, "skipped": 0, "errors": []}

    progress(f"Found {len(large_files)} file(s) exceeding threshold")

    processed = 0
    skipped = 0
    errors = []

    for file_path in large_files:
        file_size_mb = bytes_to_mb(os.path.getsize(file_path))
        file_name = os.path.basename(file_path)
        file_stem = Path(file_path).stem
        file_dir = os.path.dirname(file_path)

        # Output directory is sibling of input file
        output_dir = os.path.join(file_dir, f"{file_stem}_{partition_type}")

        progress(f"Processing: {file_name} ({file_size_mb:.1f}MB)")

        try:
            # Build kwargs for partition function
            kwargs = {
                "input_parquet": file_path,
                "output_folder": output_dir,
                "hive": hive,
                "overwrite": overwrite,
                "verbose": verbose,
                "force": force,
                "skip_analysis": skip_analysis,
                "compression": compression,
                "compression_level": compression_level,
                "auto": auto,
                "target_rows": target_rows,
                "max_partitions": max_partitions,
            }

            kwargs[spec.unit] = res_value
            if spec.prefix_partition:
                kwargs.update(partition_resolution=partition_resolution, use_centroid=use_centroid)
            partition(**kwargs)

            if in_place:
                # Validate output before deleting original
                _assert_no_rows_lost(file_path, output_dir)
                os.remove(file_path)
                debug(f"Removed original: {file_path}")

            processed += 1
            success(f"  Created: {output_dir}/")

        except Exception as e:
            warn(f"  ERROR: {e}")
            errors.append({"file": file_path, "error": str(e)})

    return {"processed": processed, "skipped": skipped, "errors": errors}
