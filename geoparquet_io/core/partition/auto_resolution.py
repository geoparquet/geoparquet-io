#!/usr/bin/env python3

"""
Auto-resolution calculation for spatial partitioning.

This module provides utilities to automatically calculate optimal spatial index
resolutions based on target partition sizes and constraints.
"""

from __future__ import annotations

import math

from geoparquet_io.core.crs_utils import source_crs_string, transform_geom_sql
from geoparquet_io.core.duckdb_utils import (
    get_duckdb_connection,
    load_community_extension,
    quote_identifier,
    sql_path,
    validate_where_clause,
    where_sql_fragment,
)
from geoparquet_io.core.file_utils import resolve_file_url
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import debug, info, warn
from geoparquet_io.core.remote import needs_httpfs, setup_aws_profile_if_needed

# Tuning for the extent-aware probe (issue #524). Sampling a multiple of the
# target partition count keeps cells near the target resolution well-populated,
# so the sampled distinct-cell count is a good estimate of the true non-empty
# count. The floor stabilizes coarse counts; the ceiling bounds probe cost.
_PROBE_OVERSAMPLE = 100
_PROBE_MIN_SAMPLE = 50_000
_PROBE_MAX_SAMPLE = 500_000

# Community extension each index's cell function lives in, loaded before the
# probe runs. Loaded through load_community_extension() rather than raw INSTALL
# / LOAD so this path shares its unavailable-extension error and its opt-out
# from the a5 extension's crash-prone load-time telemetry (#779).
_INDEX_EXTENSIONS = {
    "a5": "a5",
    "h3": "h3",
    "s2": "geography",
    "quadkey": None,
}


def _count_query(url: str, where: str | None) -> str:
    """``COUNT(*)`` over ``url``, with any WHERE clause nested in a subquery.

    The nesting is a safety boundary, not cosmetics: interpolated into a
    top-level statement the clause sits at the end of the string, where a
    separator could start a second statement (DuckDB's ``execute()`` runs
    multi-statement strings). Inside the subquery it cannot reach the top level.
    :func:`validate_where_clause` rejects separators as well (gpio #612).
    """
    if not where:
        return f"SELECT COUNT(*) FROM {sql_path(url)}"
    return (
        f"SELECT COUNT(*) FROM (SELECT 1 FROM {sql_path(url)}"
        f"{where_sql_fragment(where)}) AS __filtered"
    )


def _get_total_row_count(
    input_parquet: str,
    verbose: bool = False,
    profile: str | None = None,
    where: str | None = None,
) -> int:
    """
    Get total row count from parquet file.

    Args:
        input_parquet: Input file path
        verbose: Print debug messages
        profile: AWS profile name for S3 authentication (optional)
        where: Optional WHERE clause; the count then reflects only matching rows
            so auto-resolution sizes the grid on the filtered data (#568)

    Returns:
        Total number of rows
    """

    input_url = resolve_file_url(input_parquet, verbose)

    # Create connection inside try block to ensure cleanup on any error
    con = None
    try:
        con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_parquet))

        # Setup S3 authentication if profile specified
        if profile:
            setup_aws_profile_if_needed(profile, input_parquet)

        result = con.execute(_count_query(input_url, where)).fetchone()
        return result[0] if result else 0
    finally:
        if con is not None:
            con.close()


def _calculate_h3_resolution(
    total_rows: int,
    target_rows_per_partition: int,
    max_partitions: int = 10000,
    min_resolution: int = 0,
    max_resolution: int = 15,
    verbose: bool = False,
) -> int:
    """
    Calculate optimal H3 resolution for target partition size.

    H3 indexing system:
    - Resolution 0: ~122 cells globally (average cell area ~4.3M km²)
    - Each resolution level subdivides cells by ~7x (hexagonal subdivision)
    - Resolution 15: ~569 trillion cells globally (average cell area ~0.9 m²)

    Strategy:
    - Calculate target partition count from total_rows / target_rows
    - Use geometric progression to estimate resolution
    - Clamp to reasonable bounds

    Args:
        total_rows: Total number of rows in dataset
        target_rows_per_partition: Desired rows per partition
        max_partitions: Maximum allowed partitions
        min_resolution: Minimum H3 resolution (default 0)
        max_resolution: Maximum H3 resolution (default 15)
        verbose: Print debug messages

    Returns:
        Optimal H3 resolution (0-15)
    """
    if total_rows == 0:
        return min_resolution

    # Calculate target partition count
    target_partitions = total_rows / target_rows_per_partition

    # Respect max_partitions constraint
    if target_partitions > max_partitions:
        if verbose:
            warn(
                f"Target partition count ({target_partitions:.0f}) exceeds max ({max_partitions}), "
                f"adjusting target to {max_partitions}"
            )
        target_partitions = max_partitions

    # H3 has approximately 122 base cells at resolution 0
    # Each resolution level multiplies by ~7 (hexagonal subdivision)
    # Formula: cells(res) ≈ 122 × 7^res
    #
    # Solving for res: target_partitions = 122 × 7^res
    # res = log(target_partitions / 122) / log(7)

    if target_partitions < 122:
        # Very few partitions needed, use resolution 0
        estimated_resolution = 0
    else:
        estimated_resolution = math.log(target_partitions / 122) / math.log(7)
        estimated_resolution = round(estimated_resolution)

    # Clamp to valid range
    resolution = max(min_resolution, min(estimated_resolution, max_resolution))

    if verbose:
        estimated_partitions = 122 * (7**resolution)
        if estimated_partitions > 0:
            estimated_rows_per_partition = total_rows / estimated_partitions
            info(
                f"H3 auto-resolution: {resolution} "
                f"(~{estimated_partitions:.0f} partitions, ~{estimated_rows_per_partition:.0f} rows/partition)"
            )
        else:
            info(f"H3 auto-resolution: {resolution}")

    return resolution


def _calculate_quadkey_resolution(
    total_rows: int,
    target_rows_per_partition: int,
    max_partitions: int = 10000,
    min_resolution: int = 0,
    max_resolution: int = 23,
    verbose: bool = False,
) -> int:
    """
    Calculate optimal quadkey zoom level for target partition size.

    Quadkey (Bing Maps) indexing system:
    - Zoom level 0: 1 tile covering entire world
    - Each zoom level quadruples the number of tiles (2x2 subdivision)
    - Zoom level 23: ~70 trillion tiles globally

    Strategy:
    - Calculate target partition count from total_rows / target_rows
    - Use geometric progression to estimate zoom level
    - Clamp to reasonable bounds

    Args:
        total_rows: Total number of rows in dataset
        target_rows_per_partition: Desired rows per partition
        max_partitions: Maximum allowed partitions
        min_resolution: Minimum quadkey zoom level (default 0)
        max_resolution: Maximum quadkey zoom level (default 23)
        verbose: Print debug messages

    Returns:
        Optimal quadkey zoom level (0-23)
    """
    if total_rows == 0:
        return min_resolution

    # Calculate target partition count
    target_partitions = total_rows / target_rows_per_partition

    # Respect max_partitions constraint
    if target_partitions > max_partitions:
        if verbose:
            warn(
                f"Target partition count ({target_partitions:.0f}) exceeds max ({max_partitions}), "
                f"adjusting target to {max_partitions}"
            )
        target_partitions = max_partitions

    # Quadkey has 4^zoom tiles at each zoom level
    # Formula: tiles(zoom) = 4^zoom
    #
    # Solving for zoom: target_partitions = 4^zoom
    # zoom = log(target_partitions) / log(4) = log2(target_partitions) / 2

    if target_partitions <= 1:
        estimated_resolution = 0
    else:
        estimated_resolution = math.log2(target_partitions) / 2
        estimated_resolution = round(estimated_resolution)

    # Clamp to valid range
    resolution = max(min_resolution, min(estimated_resolution, max_resolution))

    if verbose:
        estimated_partitions = 4**resolution
        if estimated_partitions > 0:
            estimated_rows_per_partition = total_rows / estimated_partitions
            info(
                f"Quadkey auto-resolution: {resolution} "
                f"(~{estimated_partitions:.0f} partitions, ~{estimated_rows_per_partition:.0f} rows/partition)"
            )
        else:
            info(f"Quadkey auto-resolution: {resolution}")

    return resolution


def _calculate_a5_resolution(
    total_rows: int,
    target_rows_per_partition: int,
    max_partitions: int = 10000,
    min_resolution: int = 0,
    max_resolution: int = 30,
    verbose: bool = False,
    index_name: str = "A5",
) -> int:
    """
    Calculate optimal A5 (or S2) resolution for target partition size.

    A5/S2 indexing system:
    - Resolution 0: 6 base cells (one per cube face)
    - Each resolution level quadruples the number of cells (2x2 subdivision)
    - Resolution 30: maximum resolution (~1cm cells)

    Strategy:
    - Calculate target partition count from total_rows / target_rows
    - Use geometric progression to estimate resolution
    - Clamp to reasonable bounds

    Args:
        total_rows: Total number of rows in dataset
        target_rows_per_partition: Desired rows per partition
        max_partitions: Maximum allowed partitions
        min_resolution: Minimum A5/S2 resolution (default 0)
        max_resolution: Maximum A5/S2 resolution (default 30)
        verbose: Print debug messages
        index_name: Name of index for logging (default 'A5')

    Returns:
        Optimal A5/S2 resolution (0-30)
    """
    if total_rows == 0:
        return min_resolution

    # Calculate target partition count
    target_partitions = total_rows / target_rows_per_partition

    # Respect max_partitions constraint
    if target_partitions > max_partitions:
        if verbose:
            warn(
                f"Target partition count ({target_partitions:.0f}) exceeds max ({max_partitions}), "
                f"adjusting target to {max_partitions}"
            )
        target_partitions = max_partitions

    # A5/S2 has 6 base cells at resolution 0
    # Each resolution level multiplies by 4 (quadtree subdivision)
    # Formula: cells(res) = 6 × 4^res
    #
    # Solving for res: target_partitions = 6 × 4^res
    # res = log(target_partitions / 6) / log(4) = log2(target_partitions / 6) / 2

    if target_partitions < 6:
        # Very few partitions needed, use resolution 0
        estimated_resolution = 0
    else:
        estimated_resolution = math.log2(target_partitions / 6) / 2
        estimated_resolution = round(estimated_resolution)

    # Clamp to valid range
    resolution = max(min_resolution, min(estimated_resolution, max_resolution))

    if verbose:
        estimated_partitions = 6 * (4**resolution)
        if estimated_partitions > 0:
            estimated_rows_per_partition = total_rows / estimated_partitions
            info(
                f"{index_name} auto-resolution: {resolution} "
                f"(~{estimated_partitions:.0f} partitions, ~{estimated_rows_per_partition:.0f} rows/partition)"
            )
        else:
            info(f"{index_name} auto-resolution: {resolution}")

    return resolution


def _cell_expr(index_type: str, lon: str, lat: str, resolution: int) -> str:
    """SQL expression assigning a cell at ``resolution`` (mirrors add/ modules)."""
    if index_type == "a5":
        return f"a5_lonlat_to_cell({lon}, {lat}, {resolution})"
    if index_type == "s2":
        return f"s2_cell_token(s2_cell_parent(s2_cellfromlonlat({lon}, {lat}), {resolution}))"
    if index_type == "h3":
        return f"h3_latlng_to_cell_string({lat}, {lon}, {resolution})"
    if index_type == "quadkey":
        return f"lat_lon_to_quadkey({lat}, {lon}, {resolution})"
    raise ValueError(f"Unsupported spatial index type: {index_type}")


def _register_quadkey_udf(con) -> None:
    """Register the mercantile-based quadkey UDF used by the add-quadkey path."""
    import mercantile

    def lat_lon_to_quadkey(lat: float, lon: float, level: int) -> str:
        return mercantile.quadkey(mercantile.tile(lon, lat, level))

    con.create_function(
        "lat_lon_to_quadkey",
        lat_lon_to_quadkey,
        ["DOUBLE", "DOUBLE", "INTEGER"],
        "VARCHAR",
    )


def _geom_sql(con, url: str, geom_col: str) -> str:
    """Return a GEOMETRY-typed SQL expression for ``geom_col`` in ``url``.

    DuckDB reads a GeoParquet geometry column as GEOMETRY; WKB-blob columns come
    back as BLOB and must be decoded. Raises if the column is absent.
    """
    qcol = quote_identifier(geom_col)
    desc = con.execute(f"DESCRIBE SELECT {qcol} FROM {sql_path(url)}").fetchall()
    col_type = (desc[0][1] if desc else "").upper()
    if "GEOMETRY" in col_type:
        return qcol
    return f"ST_GeomFromWKB({qcol})"


def _probe_distinct_cell_counts(
    con,
    url: str,
    index_type: str,
    geom_sql: str,
    sample_clause: str,
    resolutions: list[int],
    where: str | None = None,
) -> list[int]:
    """Count distinct non-empty cells per resolution over a bounded sample.

    ``sample_clause`` is reservoir sampling (``USING SAMPLE n ROWS``) or empty
    when the whole file fits the budget. Reservoir is deliberate: it draws a
    uniform random sample regardless of row order. Block/percentage sampling
    (``TABLESAMPLE``) would be cheaper but, on spatially-sorted data, would only
    see a contiguous slice of the extent and badly underestimate the non-empty
    cell count -- the exact failure mode this probe exists to prevent (#524).

    The centroid mirrors the cell assignment in the ``add/`` modules
    (``ST_X/ST_Y(ST_Centroid(geom))``), so probe counts track real partitioning.
    It is computed once per sampled row and reused for lon and lat.

    ``where`` restricts the probe to the rows the aggregation will see. The
    filter is nested *beneath* the sample: a WHERE sitting next to ``USING
    SAMPLE`` is planned as a FILTER *above* RESERVOIR_SAMPLE, so a selective
    clause would leave only a fraction of the sample budget and the probe would
    over-resolve (gpio #612). Nesting also keeps the clause away from the top
    level of the statement, where it could otherwise chain a second one.
    """
    centroid = f"ST_Centroid({geom_sql})"
    filtered = f"(SELECT {centroid} AS c FROM {sql_path(url)}{where_sql_fragment(where)})"
    sample_cte = f"SELECT ST_X(c) AS lon, ST_Y(c) AS lat FROM {filtered}{sample_clause}"
    if index_type == "quadkey":
        # A level-r quadkey is the length-r prefix of a finer one, so compute the
        # finest quadkey once per row and take substrings (avoids a UDF call per
        # resolution per row).
        max_res = resolutions[-1]
        cols = ", ".join(
            f"COUNT(DISTINCT substr(qk, 1, {r})) AS c{i}" for i, r in enumerate(resolutions)
        )
        query = (
            f"WITH sample AS ({sample_cte}), "
            f"keyed AS (SELECT lat_lon_to_quadkey(lat, lon, {max_res}) AS qk "
            f"FROM sample WHERE lon IS NOT NULL AND lat IS NOT NULL) "
            f"SELECT {cols} FROM keyed"
        )
    else:
        cols = ", ".join(
            f"COUNT(DISTINCT {_cell_expr(index_type, 'lon', 'lat', r)}) AS c{i}"
            for i, r in enumerate(resolutions)
        )
        query = (
            f"WITH sample AS ({sample_cte}) SELECT {cols} FROM sample "
            f"WHERE lon IS NOT NULL AND lat IS NOT NULL"
        )
    return list(con.execute(query).fetchone())


def _select_closest_resolution(
    resolutions: list[int], counts: list[int], target_partitions: float
) -> int:
    """Resolution whose distinct cell count is closest to the target.

    Counts are monotonic non-decreasing in resolution; the strict comparison
    keeps the coarsest resolution among ties (avoids over-resolving on plateaus).
    """
    best_res = resolutions[0]
    best_diff: float | None = None
    for res, count in zip(resolutions, counts, strict=False):
        diff = abs(count - target_partitions)
        if best_diff is None or diff < best_diff:
            best_diff = diff
            best_res = res
    return best_res


def _probe_extent_resolution(
    input_parquet: str,
    spatial_index_type: str,
    target_partitions: float,
    min_resolution: int,
    max_resolution: int,
    total_rows: int,
    verbose: bool = False,
    profile: str | None = None,
    where: str | None = None,
) -> int | None:
    """Pick the resolution whose non-empty cell count best matches the target.

    Probes distinct cell counts over a bounded sample of the actual data so the
    chosen resolution reflects the data's real extent rather than global uniform
    coverage (issue #524). Returns None if the data can't be probed (e.g. no
    geometry column, sampling error), so the caller can fall back to the global
    estimate.
    """

    url = resolve_file_url(input_parquet, verbose)
    resolutions = list(range(min_resolution, max_resolution + 1))
    con = None
    try:
        con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_parquet))
        # Ensure ST_Transform (CRS-aware probing) emits lon/lat order.
        con.execute("SET geometry_always_xy = true;")
        if profile:
            setup_aws_profile_if_needed(profile, input_parquet)
        index_extension = _INDEX_EXTENSIONS[spatial_index_type]
        if index_extension:
            load_community_extension(con, index_extension)
        if spatial_index_type == "quadkey":
            _register_quadkey_udf(con)

        geom_col = find_primary_geometry_column(input_parquet, verbose)
        # Reproject a known non-CRS84 input to lon/lat before centroiding, so the
        # probe keys the same coordinates the add/ modules will (#530). Without
        # this the probe feeds raw projected metres to the lon/lat cell functions
        # and picks a resolution from garbage cell counts.
        source_crs = source_crs_string(input_parquet, verbose)
        geom_sql = transform_geom_sql(_geom_sql(con, url, geom_col), source_crs)

        sample_size = min(
            total_rows,
            _PROBE_MAX_SAMPLE,
            max(int(target_partitions * _PROBE_OVERSAMPLE), _PROBE_MIN_SAMPLE),
        )
        # Skip the reservoir sample (a full scan) when the file already fits the
        # budget; otherwise draw a uniform sample of sample_size rows.
        sample_clause = "" if sample_size >= total_rows else f" USING SAMPLE {sample_size} ROWS"
        counts = _probe_distinct_cell_counts(
            con, url, spatial_index_type, geom_sql, sample_clause, resolutions, where=where
        )
    except Exception as e:
        # Unconditional: falling back to the global formula silently produces a
        # wrong-but-plausible resolution, and a user who never passes --verbose
        # has no way to tell that from a correct one.
        warn(f"Extent-aware probe unavailable ({e}); using global estimate")
        return None
    finally:
        if con is not None:
            con.close()

    # All-zero counts mean the sample held no usable geometries (NULL/invalid).
    # Treat that like a probe failure so the global estimate is used instead of
    # silently selecting the coarsest resolution.
    if not any(counts):
        warn("Extent-aware probe found no non-empty cells; using global estimate")
        return None

    resolution = _select_closest_resolution(resolutions, counts, target_partitions)
    if verbose:
        cell_count = counts[resolutions.index(resolution)]
        info(
            f"{spatial_index_type} extent-aware resolution: {resolution} "
            f"(~{cell_count} non-empty cells, target ~{target_partitions:.0f} partitions)"
        )
    return resolution


def calculate_auto_resolution(
    input_parquet: str,
    spatial_index_type: str,
    target_rows_per_partition: int,
    max_partitions: int = 10000,
    min_resolution: int | None = None,
    max_resolution: int | None = None,
    verbose: bool = False,
    profile: str | None = None,
    where: str | None = None,
) -> int:
    """
    Calculate optimal spatial index resolution for target partition size.

    This is the main entry point for auto-resolution calculation. It determines
    total row count, then calls the appropriate calculator for the spatial index type.

    Args:
        input_parquet: Input file path
        spatial_index_type: Type of spatial index ('h3', 'quadkey', 'a5', 's2')
        target_rows_per_partition: Desired rows per partition
        max_partitions: Maximum allowed partitions (default 10000)
        min_resolution: Minimum resolution (None = use index default)
        max_resolution: Maximum resolution (None = use index default)
        verbose: Print debug messages
        profile: AWS profile name for S3 authentication (optional)
        where: Optional WHERE clause; sizing then reflects only matching rows (#568)

    Returns:
        Optimal resolution for the specified spatial index

    Raises:
        ValueError: If spatial_index_type is not supported or parameters are invalid

    Examples:
        >>> # Calculate H3 resolution for ~100K rows per partition
        >>> resolution = calculate_auto_resolution(
        ...     'input.parquet',
        ...     'h3',
        ...     target_rows_per_partition=100000
        ... )
        >>> print(f"Use H3 resolution {resolution}")

        >>> # Calculate quadkey zoom level with custom bounds
        >>> zoom = calculate_auto_resolution(
        ...     'input.parquet',
        ...     'quadkey',
        ...     target_rows_per_partition=50000,
        ...     min_resolution=5,
        ...     max_resolution=12
        ... )
    """
    # Validate parameters
    if target_rows_per_partition <= 0:
        raise ValueError(
            f"target_rows_per_partition must be a positive integer, got {target_rows_per_partition}"
        )

    if max_partitions <= 0:
        raise ValueError(f"max_partitions must be a positive integer, got {max_partitions}")

    # Shared partition entry point: callers may pass a clause straight through,
    # so validate here rather than trusting each of them to have done it (#612).
    if where:
        validate_where_clause(where)

    if verbose:
        debug(f"Calculating auto-resolution for {spatial_index_type}...")

    # Get total row count
    total_rows = _get_total_row_count(input_parquet, verbose, profile, where=where)

    if verbose:
        debug(f"Total rows: {total_rows:,}")

    if total_rows == 0:
        # Name the filter when there is one: the file may be full of rows and
        # only the clause empty-handed, which "no rows" alone would misattribute.
        if where:
            raise ValueError(
                f'No rows match the --where filter "{where}", so there is nothing to size '
                "the resolution on. Check the clause, or pass an explicit resolution."
            )
        raise ValueError("Input file has no rows")

    # Calculate resolution based on spatial index type
    if spatial_index_type == "h3":
        # H3 resolution range: 0-15
        default_min = 0
        default_max = 15
        calc_func = _calculate_h3_resolution
        index_name = None  # Use default in function

    elif spatial_index_type == "quadkey":
        # Quadkey zoom level range: 0-23
        default_min = 0
        default_max = 23
        calc_func = _calculate_quadkey_resolution
        index_name = None  # Use default in function

    elif spatial_index_type == "a5":
        # A5 resolution range: 0-30
        default_min = 0
        default_max = 30
        calc_func = _calculate_a5_resolution
        index_name = "A5"

    elif spatial_index_type == "s2":
        # S2 level range: 0-30 (same as A5)
        default_min = 0
        default_max = 30
        calc_func = _calculate_a5_resolution
        index_name = "S2"

    else:
        raise ValueError(
            f"Unsupported spatial index type: {spatial_index_type}. Supported types: h3, quadkey, a5, s2"
        )

    # Use defaults if not specified
    if min_resolution is None:
        min_resolution = default_min
    if max_resolution is None:
        max_resolution = default_max

    # Extent-aware probe (issue #524): size the resolution to the data's actual
    # extent instead of assuming uniform global coverage. Falls back to the
    # global-formula estimate below if the data can't be probed.
    target_partitions = min(total_rows / target_rows_per_partition, max_partitions)
    probed = _probe_extent_resolution(
        input_parquet=input_parquet,
        spatial_index_type=spatial_index_type,
        target_partitions=target_partitions,
        min_resolution=min_resolution,
        max_resolution=max_resolution,
        total_rows=total_rows,
        verbose=verbose,
        profile=profile,
        where=where,
    )
    if probed is not None:
        return probed

    # Fallback: global-formula estimate
    kwargs = {
        "total_rows": total_rows,
        "target_rows_per_partition": target_rows_per_partition,
        "max_partitions": max_partitions,
        "min_resolution": min_resolution,
        "max_resolution": max_resolution,
        "verbose": verbose,
    }
    # For A5/S2, pass index_name parameter
    if index_name is not None:
        kwargs["index_name"] = index_name

    return calc_func(**kwargs)
