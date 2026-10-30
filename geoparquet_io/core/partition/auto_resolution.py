#!/usr/bin/env python3

"""
Auto-resolution calculation for spatial partitioning.

This module provides utilities to automatically calculate optimal spatial index
resolutions based on target partition sizes and constraints.
"""

from __future__ import annotations

from geoparquet_io.core.crs_utils import source_crs_string, transform_geom_sql
from geoparquet_io.core.duckdb_utils import (
    fetch_one,
    get_duckdb_connection,
    quote_identifier,
    sql_path,
    uses_aws_profile,
    validate_where_clause,
    where_sql_fragment,
)
from geoparquet_io.core.edges import representative_point_sql
from geoparquet_io.core.file_utils import resolve_file_url
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.index_ops import calculate_index_resolution, get_index_spec, load_index
from geoparquet_io.core.logging_config import debug, info, warn
from geoparquet_io.core.remote import needs_httpfs
from geoparquet_io.core.write_funnels import collect_nonplanar_edges

# Tuning for the extent-aware probe (issue #524). Sampling a multiple of the
# target partition count keeps cells near the target resolution well-populated,
# so the sampled distinct-cell count is a good estimate of the true non-empty
# count. The floor stabilizes coarse counts; the ceiling bounds probe cost.
_PROBE_OVERSAMPLE = 100
_PROBE_MIN_SAMPLE = 50_000
_PROBE_MAX_SAMPLE = 500_000


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


@uses_aws_profile
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

        result = fetch_one(con.execute(_count_query(input_url, where)))
        return result[0] if result else 0
    finally:
        if con is not None:
            con.close()


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
    edges: str | None = None,
) -> list[int]:
    """Count distinct non-empty cells per resolution over a bounded sample.

    ``sample_clause`` is reservoir sampling (``USING SAMPLE n ROWS``) or empty
    when the whole file fits the budget. Reservoir is deliberate: it draws a
    uniform random sample regardless of row order. Block/percentage sampling
    (``TABLESAMPLE``) would be cheaper but, on spatially-sorted data, would only
    see a contiguous slice of the extent and badly underestimate the non-empty
    cell count -- the exact failure mode this probe exists to prevent (#524).

    The point mirrors the cell assignment in the ``add/`` modules
    (``edges.representative_point_sql``: the centroid for planar ``edges``, a
    point on the sphere otherwise, #1293), so probe counts track real
    partitioning. It is computed once per sampled row and reused for lon and lat.

    ``where`` restricts the probe to the rows the aggregation will see. The
    filter is nested *beneath* the sample: a WHERE sitting next to ``USING
    SAMPLE`` is planned as a FILTER *above* RESERVOIR_SAMPLE, so a selective
    clause would leave only a fraction of the sample budget and the probe would
    over-resolve (gpio #612). Nesting also keeps the clause away from the top
    level of the statement, where it could otherwise chain a second one.
    """
    centroid = representative_point_sql(geom_sql, edges)
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
            f"COUNT(DISTINCT {get_index_spec(index_type).expression('lon', 'lat', r)}) AS c{i}"
            for i, r in enumerate(resolutions)
        )
        query = (
            f"WITH sample AS ({sample_cte}) SELECT {cols} FROM sample "
            f"WHERE lon IS NOT NULL AND lat IS NOT NULL"
        )
    return list(fetch_one(con.execute(query)))


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


@uses_aws_profile
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
        load_index(con, get_index_spec(spatial_index_type))

        geom_col = find_primary_geometry_column(input_parquet, verbose)
        # Reproject a known non-CRS84 input to lon/lat before centroiding, so the
        # probe keys the same coordinates the add/ modules will (#530). Without
        # this the probe feeds raw projected metres to the lon/lat cell functions
        # and picks a resolution from garbage cell counts.
        source_crs = source_crs_string(input_parquet, verbose, geom_col)
        geom_sql = transform_geom_sql(_geom_sql(con, url, geom_col), source_crs)

        sample_size = min(
            total_rows,
            _PROBE_MAX_SAMPLE,
            max(int(target_partitions * _PROBE_OVERSAMPLE), _PROBE_MIN_SAMPLE),
        )
        # Skip the reservoir sample (a full scan) when the file already fits the
        # budget; otherwise draw a uniform sample of sample_size rows.
        sample_clause = "" if sample_size >= total_rows else f" USING SAMPLE {sample_size} ROWS"
        edges = collect_nonplanar_edges(input_parquet).get(geom_col)
        counts = _probe_distinct_cell_counts(
            con, url, spatial_index_type, geom_sql, sample_clause, resolutions, where, edges
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

    spec = get_index_spec(spatial_index_type)

    def probe(target, minimum, maximum):
        return _probe_extent_resolution(
            input_parquet=input_parquet,
            spatial_index_type=spatial_index_type,
            target_partitions=target,
            min_resolution=minimum,
            max_resolution=maximum,
            total_rows=total_rows,
            verbose=verbose,
            profile=profile,
            where=where,
        )

    return calculate_index_resolution(
        spec,
        total_rows,
        target_rows_per_partition,
        max_partitions,
        min_resolution,
        max_resolution,
        verbose,
        probe=probe,
    )
