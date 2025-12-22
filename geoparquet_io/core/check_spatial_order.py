#!/usr/bin/env python3


from geoparquet_io.core.common import (
    find_primary_geometry_column,
    get_duckdb_connection,
    needs_httpfs,
    safe_file_url,
)
from geoparquet_io.core.logging_config import debug, progress


def check_spatial_order(
    parquet_file, random_sample_size, limit_rows, verbose, return_results=False
):
    """Check if a GeoParquet file is spatially ordered.

    Args:
        parquet_file: Path to parquet file
        random_sample_size: Number of rows in each random sample
        limit_rows: Max number of rows to analyze
        verbose: Print additional information
        return_results: If True, return structured results dict

    Returns:
        ratio (float) if return_results=False, or dict if return_results=True containing:
            - passed: bool
            - ratio: float
            - consecutive_avg: float
            - random_avg: float
            - issues: list of issue descriptions
            - recommendations: list of recommendations
    """
    safe_url = safe_file_url(parquet_file, verbose)

    # Get geometry column name
    geometry_column = find_primary_geometry_column(parquet_file, verbose)
    if verbose:
        debug(f"Using geometry column: {geometry_column}")

    # Create DuckDB connection with httpfs if needed
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(parquet_file))

    # First get total rows
    total_rows = con.execute(f"SELECT COUNT(*) FROM '{safe_url}'").fetchone()[0]
    if verbose:
        debug(f"Total rows in file: {total_rows:,}")

    # Limit rows if needed
    if total_rows > limit_rows:
        if verbose:
            debug(f"Limiting analysis to first {limit_rows:,} rows")
        row_limit = f"LIMIT {limit_rows}"
    else:
        row_limit = ""

    # Get consecutive pairs
    consecutive_query = f"""
    WITH numbered AS (
        SELECT
            ROW_NUMBER() OVER () as id,
            {geometry_column} as geom
        FROM '{safe_url}'
        {row_limit}
    )
    SELECT
        AVG(ST_Distance(a.geom, b.geom)) as avg_dist
    FROM numbered a
    JOIN numbered b ON b.id = a.id + 1;
    """

    if verbose:
        progress("Calculating average distance between consecutive features...")

    consecutive_result = con.execute(consecutive_query).fetchone()
    consecutive_avg = consecutive_result[0] if consecutive_result else None

    if verbose:
        debug(f"Average distance between consecutive features: {consecutive_avg}")

    # Get random pairs
    random_query = f"""
    WITH sample AS (
        SELECT
            {geometry_column} as geom
        FROM '{safe_url}'
        {row_limit}
    ),
    random_pairs AS (
        SELECT
            a.geom as geom1,
            b.geom as geom2
        FROM
            (SELECT geom FROM sample ORDER BY random() LIMIT {random_sample_size}) a,
            (SELECT geom FROM sample ORDER BY random() LIMIT {random_sample_size}) b
        WHERE a.geom != b.geom
    )
    SELECT AVG(ST_Distance(geom1, geom2)) as avg_dist
    FROM random_pairs;
    """

    if verbose:
        progress(f"Calculating average distance between {random_sample_size} random pairs...")

    random_result = con.execute(random_query).fetchone()
    random_avg = random_result[0] if random_result else None

    if verbose:
        debug(f"Average distance between random features: {random_avg}")

    # Calculate ratio
    ratio = consecutive_avg / random_avg if consecutive_avg and random_avg else None

    if not verbose:  # Only print results if not being called from check_all
        progress("\nResults:")
        debug(f"Average distance between consecutive features: {consecutive_avg}")
        debug(f"Average distance between random features: {random_avg}")
        progress(f"Ratio (consecutive / random): {ratio}")

        if ratio is not None and ratio < 0.5:
            progress("=> Data seems strongly spatially clustered.")
        elif ratio is not None:
            progress("=> Data might not be strongly clustered (or is partially clustered).")

    if return_results:
        passed = ratio is not None and ratio < 0.5
        issues = []
        recommendations = []

        if ratio is not None and ratio >= 0.5:
            issues.append(f"Poor spatial ordering (ratio: {ratio:.2f})")
            recommendations.append("Apply Hilbert spatial ordering for better query performance")

        return {
            "passed": passed,
            "ratio": ratio,
            "consecutive_avg": consecutive_avg,
            "random_avg": random_avg,
            "issues": issues,
            "recommendations": recommendations,
            "fix_available": not passed,
        }

    return ratio


if __name__ == "__main__":
    check_spatial_order()
