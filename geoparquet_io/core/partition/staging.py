#!/usr/bin/env python3

"""Shared single-pass partitioning primitives (issue #478).

Both ``partition_by_column`` (``common.py``) and
``partition_by_admin_hierarchical`` (``admin_hierarchical.py``) drive the same
two-phase write:

1. One streaming DuckDB ``COPY ... PARTITION_BY`` routes every input row into a
   staging directory (``run_partitioned_copy``) — the input is read exactly once.
2. Each (small) staging partition is rewritten into its final file with correct
   per-partition metadata (``finalize_partition_file``).

These helpers are the public seam between the two partition drivers; keeping them
here (rather than reaching into another module's underscore-private names) makes
the shared contract explicit and lets ``import-linter`` reason about it.
"""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from urllib.parse import unquote

from geoparquet_io.core.common import write_parquet_with_metadata
from geoparquet_io.core.duckdb_utils import sql_path
from geoparquet_io.core.exceptions import PartitionError
from geoparquet_io.core.logging_config import debug
from geoparquet_io.core.write_strategies.duckdb_kv import (
    get_default_memory_limit,
)
from geoparquet_io.core.write_strategies.duckdb_kv import (
    validate_memory_limit as _validate_memory_limit,
)

# Internal alias used to drive the single-pass PARTITION_BY split. DuckDB drops
# the PARTITION_BY column from the written files, so using a dedicated alias lets
# callers keep or drop the *original* column independently.
PARTITION_ALIAS = "__gpio_part"


@dataclass(frozen=True)
class PartitionWriteOptions:
    """Per-partition write settings forwarded to ``write_parquet_with_metadata``.

    Replaces an opaque ``**write_kwargs`` passthrough so both partition drivers
    forward exactly the same, type-checked set of options to every partition.
    """

    geoparquet_version: str | None = None
    compression: str = "ZSTD"
    compression_level: int = 15
    row_group_size_mb: int | None = None
    row_group_rows: int | None = None
    memory_limit: str | None = None
    profile: str | None = None
    extra_kv_metadata: dict[str, str] | None = None

    def as_write_kwargs(self) -> dict:
        """Return the kwargs accepted by ``write_parquet_with_metadata``."""
        return {
            "geoparquet_version": self.geoparquet_version,
            "compression": self.compression,
            "compression_level": self.compression_level,
            "row_group_size_mb": self.row_group_size_mb,
            "row_group_rows": self.row_group_rows,
            "memory_limit": self.memory_limit,
            "profile": self.profile,
            "extra_kv_metadata": self.extra_kv_metadata,
        }


def make_partition_aliases(count: int, existing_columns) -> list[str]:
    """Return ``count`` PARTITION_BY alias names guaranteed not to collide.

    The aliases must not clash with any real input column (a column literally
    named ``__gpio_part`` would otherwise produce an ambiguous ``SELECT *, ...``)
    nor with each other. For a single level the base name is used when free.
    """
    existing = set(existing_columns)
    aliases: list[str] = []
    for i in range(count):
        candidate = PARTITION_ALIAS if count == 1 else f"{PARTITION_ALIAS}{i}"
        suffix = 0
        while candidate in existing or candidate in aliases:
            suffix += 1
            candidate = f"{PARTITION_ALIAS}_{suffix}_{i}"
        aliases.append(candidate)
    return aliases


def check_output_collision(seen_outputs: dict, output_filename: str, partition_value) -> None:
    """Guard against two distinct partition values mapping to one output file.

    Filename sanitization is lossy (e.g. ``"a b"`` and ``"a_b"`` both →
    ``a_b.parquet``), so distinct staging partitions can resolve to the same
    final path. Writing both would silently drop or overwrite rows, so raise
    instead and tell the user which values clashed (issue #480). Records the
    mapping so the next collision is detected.
    """
    prior = seen_outputs.get(output_filename)
    if prior is not None and str(prior) != str(partition_value):
        raise PartitionError(
            f"Partition values {prior!r} and {partition_value!r} both map to the same "
            f"output file {os.path.basename(output_filename)!r} after filename "
            f"sanitization. Rename or pre-process these values to avoid silent data loss."
        )
    seen_outputs[output_filename] = partition_value


def create_staging_dir(output_dir: str) -> str:
    """Create a unique staging directory as a *sibling* of ``output_dir``.

    Sibling (not child) so the staging tree can never be swept into a recursive
    upload of ``output_dir`` even if cleanup fails; unique name so two partition
    jobs targeting the same output folder don't clobber each other's staging.
    Same parent volume as the output keeps the finalize rewrite on one device.
    """
    parent = os.path.dirname(os.path.abspath(output_dir))
    os.makedirs(parent, exist_ok=True)
    return tempfile.mkdtemp(prefix=".gpio_partition_staging_", dir=parent)


def run_partitioned_copy(con, select_sql, partition_cols, staging_dir, verbose, memory_limit=None):
    """Run a single DuckDB ``COPY ... PARTITION_BY`` into ``staging_dir``.

    This reads the input exactly once and routes every row to its partition
    writer (issue #478). Staging files use native geometry + SNAPPY (fast,
    transient); the final files are rewritten with the requested settings and
    correct per-partition metadata by the caller.
    """
    con.execute("SET threads = 1")  # one file per partition + bounded memory
    con.execute("SET preserve_insertion_order = false")
    effective_limit = _validate_memory_limit(memory_limit or get_default_memory_limit())
    con.execute(f"SET memory_limit = '{effective_limit}'")

    part_cols = ", ".join(partition_cols)
    copy_sql = (
        f"COPY ({select_sql}) TO {sql_path(staging_dir)} "
        f"(FORMAT PARQUET, PARTITION_BY ({part_cols}), "
        f"COMPRESSION SNAPPY, GEOPARQUET_VERSION 'NONE', OVERWRITE_OR_IGNORE)"
    )
    if verbose:
        debug("Single-pass partition COPY (one scan of input):")
        debug(copy_sql)
    con.execute(copy_sql)


def iter_staging_partitions(staging_dir):
    """Yield ``(values, partition_dir)`` for each leaf partition in staging.

    ``values`` is the list of decoded partition values (one per PARTITION_BY
    level), recovered from DuckDB's Hive-encoded ``alias=value`` directory
    names. Supports nested (multi-level) partitioning.
    """

    def _walk(current_dir, prefix):
        entries = sorted(
            d
            for d in os.listdir(current_dir)
            if "=" in d and os.path.isdir(os.path.join(current_dir, d))
        )
        for entry in entries:
            _, _, enc_value = entry.partition("=")
            value = unquote(enc_value)
            child = os.path.join(current_dir, entry)
            sub = [
                d for d in os.listdir(child) if "=" in d and os.path.isdir(os.path.join(child, d))
            ]
            if sub:
                yield from _walk(child, [*prefix, value])
            else:
                yield [*prefix, value], child

    yield from _walk(staging_dir, [])


def finalize_partition_file(
    con,
    partition_dir,
    output_filename,
    metadata,
    overwrite,
    verbose,
    options: PartitionWriteOptions,
):
    """Rewrite one staging partition into its final file with correct metadata.

    Reads the (small) staging partition via a glob so a partition that DuckDB
    split across files still collapses into one output file. Recomputes the
    tight per-partition ``bbox``/``geometry_types`` by invalidating the
    inherited ones — ``metadata`` describes the whole input, so on a partition
    holding a fraction of its rows they are at best far too wide and, when the
    input was itself a glob, only the first file's. Returns ``True`` when a file
    was written, ``False`` when skipped (exists and not overwrite).

    The invalidation is left to ``write_parquet_with_metadata`` rather than done
    here: only it knows which column it will recompute, and a secondary geometry
    column that loses ``geometry_types`` without one lands in the output missing
    a key GeoParquet 1.1 requires (#934).
    """
    if os.path.exists(output_filename) and not overwrite:
        if verbose:
            debug(f"Output file {output_filename} already exists, skipping...")
        return False

    # hive_partitioning=false: the staging path is ``__gpio_part=value/`` (the
    # PARTITION_BY alias), and DuckDB would otherwise auto-detect that and re-add
    # the dropped alias as a data column, leaking ``__gpio_part`` into every
    # output file.
    staging_glob = os.path.join(partition_dir, "*.parquet")
    query = f"SELECT * FROM read_parquet({sql_path(staging_glob)}, hive_partitioning=false)"
    write_parquet_with_metadata(
        con,
        query,
        output_filename,
        original_metadata=metadata,
        invalidate_derived_stats=True,
        verbose=False,
        **options.as_write_kwargs(),
    )
    if verbose:
        debug(f"Wrote {output_filename}")
    return True
