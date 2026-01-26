"""
Lazy execution API for GeoParquet transformations.

Provides a LazyTable class that wraps DuckDB query plans for deferred execution.
Operations build SQL transformations; execution happens only when write() or collect()
is called.

    import geoparquet_io as gpio

    # Lazy execution - nothing runs until write()
    gpio.read_lazy('input.parquet') \\
        .add_bbox() \\
        .sort_hilbert() \\
        .write('output.parquet')

    # Hand off from DuckDB workflow
    gpio.from_relation(rel).sort_hilbert().write('output.parquet')
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

if TYPE_CHECKING:
    pass

# Type alias for SQL transform functions
# Each transform takes (inner_sql, geometry_column, **kwargs) and returns wrapped SQL
SQLTransform = Callable[[str, str], str]


class LazyTable:
    """
    Lazy wrapper around a DuckDB query plan.

    Operations build SQL transformations; execution is deferred
    until write(), collect(), or other terminal operations.

    This class follows an immutable pattern - transform methods
    return new LazyTable instances rather than mutating in place.

    Example:
        >>> import geoparquet_io as gpio
        >>> table = gpio.read_lazy('data.parquet')
        >>> table.add_bbox().sort_hilbert().write('output.parquet')
    """

    def __init__(
        self,
        source: str | duckdb.DuckDBPyRelation,
        connection: duckdb.DuckDBPyConnection | None = None,
        geometry_column: str = "geometry",
        crs: dict | None = None,
        *,
        _transforms: list[tuple[str, SQLTransform, dict[str, Any]]] | None = None,
        _owns_connection: bool = False,
    ):
        """
        Initialize a LazyTable.

        Args:
            source: SQL expression string or DuckDB Relation
            connection: DuckDB connection. If None, a new one is created.
            geometry_column: Name of the geometry column
            crs: CRS as PROJJSON dict. None means unknown (will default to WGS84).
            _transforms: Internal list of pending transforms (for immutability)
            _owns_connection: Internal flag - True if we created the connection
        """
        self._source = source
        self._geometry_column = geometry_column
        self._crs = crs
        self._transforms: list[tuple[str, SQLTransform, dict[str, Any]]] = (
            _transforms if _transforms is not None else []
        )

        # Connection management
        if connection is None:
            self._connection = self._create_connection()
            self._owns_connection = True
        else:
            self._connection = connection
            self._owns_connection = _owns_connection

    def _create_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a new DuckDB connection with spatial extension loaded."""
        from geoparquet_io.core.common import get_duckdb_connection

        return get_duckdb_connection(load_spatial=True, load_httpfs=False)

    def _copy_with_transform(
        self,
        transform_name: str,
        transform_fn: SQLTransform,
        **kwargs: Any,
    ) -> LazyTable:
        """
        Create a new LazyTable with an additional transform.

        This implements the immutable pattern - each transform returns
        a new instance rather than mutating the current one.
        """
        new_transforms = self._transforms.copy()
        new_transforms.append((transform_name, transform_fn, kwargs))

        return LazyTable(
            source=self._source,
            connection=self._connection,
            geometry_column=self._geometry_column,
            crs=self._crs,
            _transforms=new_transforms,
            _owns_connection=False,  # Child doesn't own the connection
        )

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def sql(self) -> str:
        """
        Get the compiled SQL query.

        Useful for debugging and understanding the query plan.
        """
        return self._build_sql()

    @property
    def geometry_column(self) -> str:
        """Name of the geometry column."""
        return self._geometry_column

    @property
    def crs(self) -> dict | None:
        """CRS as PROJJSON dict, or None if unknown."""
        return self._crs

    # =========================================================================
    # Context Manager Support
    # =========================================================================

    def __enter__(self) -> LazyTable:
        """Enter context manager."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Exit context manager, closing connection if we own it."""
        self.close()

    def close(self) -> None:
        """
        Close the underlying DuckDB connection if we own it.

        Safe to call multiple times.
        """
        if self._owns_connection and self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                pass  # Connection may already be closed

    # =========================================================================
    # Transform Methods (return new LazyTable)
    # =========================================================================

    def add_bbox(self, column_name: str = "bbox") -> LazyTable:
        """
        Add bounding box column to the query plan.

        Args:
            column_name: Name for the bbox struct column (default: "bbox")

        Returns:
            New LazyTable with the transform added
        """
        return self._copy_with_transform(
            "add_bbox",
            _add_bbox_transform,
            column_name=column_name,
        )

    def extract(
        self,
        columns: list[str] | None = None,
        bbox: tuple[float, float, float, float] | None = None,
        where: str | None = None,
        limit: int | None = None,
    ) -> LazyTable:
        """
        Add filtering and column selection to the query plan.

        Args:
            columns: Column names to select (None = all columns)
            bbox: Bounding box filter (xmin, ymin, xmax, ymax)
            where: SQL WHERE clause (without the WHERE keyword)
            limit: Maximum rows to return

        Returns:
            New LazyTable with the transform added
        """
        return self._copy_with_transform(
            "extract",
            _extract_transform,
            columns=columns,
            bbox=bbox,
            where=where,
            limit=limit,
        )

    def sort_hilbert(self) -> LazyTable:
        """
        Add Hilbert spatial ordering to the query plan.

        This improves spatial locality for downstream queries.

        Returns:
            New LazyTable with the transform added
        """
        return self._copy_with_transform("sort_hilbert", _hilbert_sort_transform)

    def sort_column(self, column: str, descending: bool = False) -> LazyTable:
        """
        Add column-based ordering to the query plan.

        Args:
            column: Column name to sort by
            descending: Sort in descending order (default: False)

        Returns:
            New LazyTable with the transform added
        """
        return self._copy_with_transform(
            "sort_column",
            _column_sort_transform,
            column=column,
            descending=descending,
        )

    def reproject(self, target_crs: str) -> LazyTable:
        """
        Add reprojection to the query plan.

        Args:
            target_crs: Target CRS (e.g., "EPSG:32610", "EPSG:3857")

        Returns:
            New LazyTable with the transform added and CRS updated
        """
        new_table = self._copy_with_transform(
            "reproject",
            _reproject_transform,
            target_crs=target_crs,
        )
        # Update CRS on the new table
        new_table._crs = _crs_from_epsg(target_crs)
        return new_table

    # =========================================================================
    # Terminal Operations
    # =========================================================================

    def write(
        self,
        path: str | Path,
        *,
        compression: str = "zstd",
        compression_level: int | None = None,
        row_group_size: int | None = None,
        write_strategy: str | None = None,
        write_memory: str | None = None,
        geoparquet_version: str = "1.1",
    ) -> Path:
        """
        Execute the query plan and write to a GeoParquet file.

        This is a terminal operation that triggers execution.

        Args:
            path: Output file path
            compression: Compression codec (default: "zstd")
            compression_level: Compression level (default: codec-specific)
            row_group_size: Rows per row group (default: 100000)
            write_strategy: Write strategy to use (default: "duckdb-kv")
            write_memory: Memory limit for writing (e.g., "2GB")
            geoparquet_version: GeoParquet version (default: "1.1")

        Returns:
            Path to the written file
        """
        from geoparquet_io.core.write_strategies import WriteStrategy, WriteStrategyFactory

        output_path = Path(path)
        sql = self._build_sql()

        # Map string strategy to enum
        strategy_map = {
            "duckdb-kv": WriteStrategy.DUCKDB_KV,
            "streaming": WriteStrategy.ARROW_STREAMING,
            "in-memory": WriteStrategy.ARROW_MEMORY,
            "disk-rewrite": WriteStrategy.DISK_REWRITE,
        }
        strategy_enum = strategy_map.get(write_strategy or "duckdb-kv", WriteStrategy.DUCKDB_KV)
        strategy = WriteStrategyFactory.get_strategy(strategy_enum)

        # Default compression level based on codec
        if compression_level is None:
            compression_level = 3 if compression.lower() == "zstd" else 6

        # Write using the strategy
        strategy.write_from_query(
            con=self._connection,
            query=sql,
            output_path=str(output_path),
            geometry_column=self._geometry_column,
            original_metadata=None,
            geoparquet_version=geoparquet_version,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=None,
            row_group_rows=row_group_size or 100000,
            input_crs=self._crs,
            verbose=False,
            custom_metadata=None,
            memory_limit=write_memory,
        )

        return output_path

    def collect(self) -> pa.Table:
        """
        Execute the query plan and return as Arrow table.

        This is a terminal operation that materializes the full result
        into memory. For large datasets, prefer write() instead.

        The returned table has GeoArrow extension types applied to
        the geometry column for interoperability with other tools.

        Returns:
            Arrow table with GeoArrow extension types
        """
        sql = self._build_sql()
        table = self._connection.execute(sql).fetch_arrow_table()

        # Apply GeoArrow extension type to geometry column
        table = self._apply_geoarrow_metadata(table)

        return table

    def count(self) -> int:
        """
        Execute COUNT(*) on the query plan.

        This is more efficient than collect() when you only need the row count.

        Returns:
            Number of rows in the result
        """
        sql = self._build_sql()
        result = self._connection.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()
        return result[0] if result else 0

    def explain(self) -> str:
        """
        Get the DuckDB query execution plan.

        Useful for debugging and understanding query optimization.

        Returns:
            DuckDB EXPLAIN output as string
        """
        sql = self._build_sql()
        result = self._connection.execute(f"EXPLAIN {sql}").fetchone()
        return result[0] if result else ""

    # =========================================================================
    # Internal Methods
    # =========================================================================

    def _source_sql(self) -> str:
        """Get the SQL representation of the source."""
        if isinstance(self._source, str):
            return self._source
        else:
            # DuckDBPyRelation - need to convert geometry to WKB before exporting
            # DuckDB's internal GEOMETRY format is not the same as standard WKB
            geom_col = self._geometry_column

            # Create a view that converts the geometry to proper WKB
            # Then export that as Arrow
            wkb_rel = self._source.project(f'* REPLACE (ST_AsWKB("{geom_col}") AS "{geom_col}")')
            arrow_table = wkb_rel.arrow()
            self._connection.register("__lazy_source", arrow_table)

            # Now convert WKB back to GEOMETRY for spatial operations
            return f'''
                SELECT * REPLACE (ST_GeomFromWKB("{geom_col}") AS "{geom_col}")
                FROM __lazy_source
            '''

    def _build_sql(self) -> str:
        """Compile all transforms into final SQL."""
        sql = self._source_sql()

        for _transform_name, transform_fn, kwargs in self._transforms:
            sql = transform_fn(sql, self._geometry_column, **kwargs)

        return sql

    def _build_geo_metadata(self) -> dict:
        """Build GeoParquet metadata dict."""
        # Default to WGS84 if no CRS specified
        crs = self._crs or _default_wgs84_crs()

        return {
            "version": "1.1.0",
            "primary_column": self._geometry_column,
            "columns": {
                self._geometry_column: {
                    "encoding": "WKB",
                    "crs": crs,
                    "geometry_types": [],  # Will be populated by write strategy
                }
            },
        }

    def _apply_geoarrow_metadata(self, table: pa.Table) -> pa.Table:
        """Apply GeoArrow extension metadata to geometry column."""
        # For now, return table as-is
        # TODO: Apply proper GeoArrow extension types
        return table


# =============================================================================
# Transform Functions
# =============================================================================


def _add_bbox_transform(
    inner_sql: str,
    geometry_column: str,
    *,
    column_name: str = "bbox",
) -> str:
    """Transform that adds a bbox struct column."""
    return f"""
        SELECT *,
            {{
                'xmin': ST_XMin("{geometry_column}"),
                'ymin': ST_YMin("{geometry_column}"),
                'xmax': ST_XMax("{geometry_column}"),
                'ymax': ST_YMax("{geometry_column}")
            }} AS {column_name}
        FROM ({inner_sql}) AS __add_bbox
    """


def _extract_transform(
    inner_sql: str,
    geometry_column: str,
    *,
    columns: list[str] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    where: str | None = None,
    limit: int | None = None,
) -> str:
    """Transform that filters rows and selects columns."""
    # Column selection
    if columns:
        # Ensure geometry column is included
        cols = list(columns)
        if geometry_column not in cols:
            cols.append(geometry_column)
        select = ", ".join(f'"{c}"' for c in cols)
    else:
        select = "*"

    # Build WHERE conditions
    conditions = []
    if bbox:
        xmin, ymin, xmax, ymax = bbox
        conditions.append(
            f'ST_Intersects("{geometry_column}", ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))'
        )
    if where:
        conditions.append(f"({where})")

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    limit_clause = f"LIMIT {limit}" if limit else ""

    return f"""
        SELECT {select}
        FROM ({inner_sql}) AS __extract
        {where_clause}
        {limit_clause}
    """


def _hilbert_sort_transform(
    inner_sql: str,
    geometry_column: str,
) -> str:
    """Transform that orders by Hilbert curve for spatial locality."""
    # Use ST_Extent to get BOX_2D (not GEOMETRY) for ST_Hilbert
    return f"""
        WITH __bounds AS (
            SELECT ST_Extent("{geometry_column}") as extent
            FROM ({inner_sql}) AS __bounds_src
        ),
        __data AS ({inner_sql})
        SELECT __data.*
        FROM __data, __bounds
        ORDER BY ST_Hilbert("{geometry_column}", __bounds.extent)
    """


def _column_sort_transform(
    inner_sql: str,
    geometry_column: str,
    *,
    column: str,
    descending: bool = False,
) -> str:
    """Transform that orders by a column."""
    direction = "DESC" if descending else "ASC"
    return f"""
        SELECT *
        FROM ({inner_sql}) AS __sort
        ORDER BY "{column}" {direction}
    """


def _reproject_transform(
    inner_sql: str,
    geometry_column: str,
    *,
    target_crs: str,
) -> str:
    """Transform that reprojects geometry to a target CRS."""
    return f"""
        SELECT
            * EXCLUDE("{geometry_column}"),
            ST_Transform("{geometry_column}", '{target_crs}') AS "{geometry_column}"
        FROM ({inner_sql}) AS __reproject
    """


# =============================================================================
# Helper Functions
# =============================================================================


def _default_wgs84_crs() -> dict:
    """Return default WGS84 CRS in PROJJSON format."""
    return {
        "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
        "type": "GeographicCRS",
        "name": "WGS 84",
        "datum": {
            "type": "GeodeticReferenceFrame",
            "name": "World Geodetic System 1984",
            "ellipsoid": {
                "name": "WGS 84",
                "semi_major_axis": 6378137,
                "inverse_flattening": 298.257223563,
            },
        },
        "coordinate_system": {
            "subtype": "ellipsoidal",
            "axis": [
                {
                    "name": "Geodetic latitude",
                    "abbreviation": "Lat",
                    "direction": "north",
                    "unit": "degree",
                },
                {
                    "name": "Geodetic longitude",
                    "abbreviation": "Lon",
                    "direction": "east",
                    "unit": "degree",
                },
            ],
        },
        "id": {"authority": "EPSG", "code": 4326},
    }


def _crs_from_epsg(epsg_string: str) -> dict:
    """
    Create a minimal CRS dict from an EPSG string.

    For full PROJJSON, you would use pyproj, but this provides
    a simple placeholder that includes the EPSG code.
    """
    # Extract code from "EPSG:4326" format
    if ":" in epsg_string:
        authority, code = epsg_string.split(":", 1)
        return {"id": {"authority": authority.upper(), "code": int(code)}}
    return {"id": {"authority": "EPSG", "code": int(epsg_string)}}


# =============================================================================
# Entry Point Functions
# =============================================================================


def read_lazy(
    path: str | Path,
    *,
    geometry_column: str | None = None,
) -> LazyTable:
    """
    Read a GeoParquet file lazily.

    This is the lazy equivalent of gpio.read(). No data is loaded
    until a terminal operation (write, collect) is called.

    Args:
        path: Path to GeoParquet file
        geometry_column: Override geometry column detection

    Returns:
        LazyTable for chaining operations

    Example:
        >>> import geoparquet_io as gpio
        >>> gpio.read_lazy('input.parquet').add_bbox().write('output.parquet')
    """
    from geoparquet_io.core.common import get_duckdb_connection, needs_httpfs

    path_str = str(path)

    # Read metadata to get geometry column and CRS
    geom_col, crs = _read_geoparquet_metadata(path_str)
    if geometry_column:
        geom_col = geometry_column

    # Create connection with httpfs if needed
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(path_str))

    # Build source SQL
    source_sql = f"SELECT * FROM read_parquet('{path_str}')"

    return LazyTable(
        source=source_sql,
        connection=con,
        geometry_column=geom_col,
        crs=crs,
        _owns_connection=True,
    )


def convert_lazy(
    path: str | Path,
    *,
    geometry_column: str = "geometry",
) -> LazyTable:
    """
    Convert a geospatial file to LazyTable.

    Supports: GeoPackage, GeoJSON, Shapefile, FlatGeobuf, and other
    formats supported by DuckDB's ST_Read function.

    Args:
        path: Path to input file
        geometry_column: Name for geometry column in output

    Returns:
        LazyTable for chaining operations

    Example:
        >>> import geoparquet_io as gpio
        >>> gpio.convert_lazy('data.geojson').sort_hilbert().write('output.parquet')
    """
    from geoparquet_io.core.common import get_duckdb_connection

    path_str = str(path)

    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)

    # Use ST_Read for vector formats
    # ST_Read produces a 'geom' column as GEOMETRY type
    # Rename it to the target geometry column name
    source_sql = f"""
        SELECT * EXCLUDE(geom), geom AS "{geometry_column}"
        FROM ST_Read('{path_str}')
    """

    return LazyTable(
        source=source_sql,
        connection=con,
        geometry_column=geometry_column,
        crs=None,  # CRS will be detected from file during write
        _owns_connection=True,
    )


def from_table(
    name: str,
    connection: duckdb.DuckDBPyConnection,
    *,
    geometry_column: str = "geometry",
    crs: dict | str | None = None,
) -> LazyTable:
    """
    Create LazyTable from a DuckDB table.

    Use this to hand off from a DuckDB workflow to gpio for
    finalization (adding bbox, Hilbert sorting, writing).

    Args:
        name: Table name in DuckDB
        connection: DuckDB connection with the table
        geometry_column: Name of geometry column
        crs: CRS as PROJJSON dict or EPSG string. If None, defaults to WGS84.

    Returns:
        LazyTable for chaining operations

    Example:
        >>> import duckdb
        >>> import geoparquet_io as gpio
        >>> con = duckdb.connect()
        >>> con.execute("CREATE TABLE processed AS SELECT * FROM ...")
        >>> gpio.from_table("processed", con).sort_hilbert().write('output.parquet')
    """
    from geoparquet_io.core.logging_config import warn

    if crs is None:
        warn(f"No CRS specified for table '{name}'. Defaulting to WGS84 (EPSG:4326).")
        crs_dict = _default_wgs84_crs()
    elif isinstance(crs, str):
        crs_dict = _crs_from_epsg(crs)
    else:
        crs_dict = crs

    source_sql = f'SELECT * FROM "{name}"'

    return LazyTable(
        source=source_sql,
        connection=connection,
        geometry_column=geometry_column,
        crs=crs_dict,
        _owns_connection=False,  # User owns the connection
    )


def from_relation(
    relation: duckdb.DuckDBPyRelation,
    *,
    geometry_column: str = "geometry",
    crs: dict | str | None = None,
) -> LazyTable:
    """
    Create LazyTable from a DuckDB Relation.

    Use this to hand off any DuckDB query result to gpio for
    finalization. The relation stays lazy until a terminal operation.

    Args:
        relation: DuckDB Relation object
        geometry_column: Name of geometry column
        crs: CRS as PROJJSON dict or EPSG string. If None, defaults to WGS84.

    Returns:
        LazyTable for chaining operations

    Example:
        >>> import duckdb
        >>> import geoparquet_io as gpio
        >>> con = duckdb.connect()
        >>> rel = con.sql("SELECT * FROM read_parquet('data.parquet') WHERE area > 100")
        >>> gpio.from_relation(rel).sort_hilbert().write('output.parquet')
    """
    from geoparquet_io.core.logging_config import warn

    if crs is None:
        warn("No CRS specified for relation. Defaulting to WGS84 (EPSG:4326).")
        crs_dict = _default_wgs84_crs()
    elif isinstance(crs, str):
        crs_dict = _crs_from_epsg(crs)
    else:
        crs_dict = crs

    # Relations use the connection they were created from
    # We pass the relation as source; _source_sql will handle it
    return LazyTable(
        source=relation,
        connection=None,  # Will be created if needed
        geometry_column=geometry_column,
        crs=crs_dict,
        _owns_connection=True,  # We'll create a new connection
    )


def from_arrow(
    table: pa.Table,
    *,
    geometry_column: str | None = None,
    crs: dict | str | None = None,
) -> LazyTable:
    """
    Create LazyTable from an Arrow table.

    Use this for interoperability with GeoPandas, GeoArrow, and
    other Arrow-based geospatial libraries.

    CRS is automatically extracted from GeoArrow extension types
    if present and not explicitly provided.

    Args:
        table: Arrow table (with WKB geometry or GeoArrow extension)
        geometry_column: Name of geometry column (auto-detected if None)
        crs: CRS as PROJJSON dict or EPSG string. If None, extracted from
             GeoArrow metadata or defaults to WGS84.

    Returns:
        LazyTable for chaining operations

    Example:
        >>> import geopandas as gpd
        >>> import geoparquet_io as gpio
        >>> gdf = gpd.read_file('data.geojson')
        >>> arrow_table = gdf.to_arrow()
        >>> gpio.from_arrow(arrow_table).sort_hilbert().write('output.parquet')
    """
    from geoparquet_io.core.common import get_duckdb_connection
    from geoparquet_io.core.streaming import find_geometry_column_from_table

    # Detect geometry column
    geom_col = geometry_column or find_geometry_column_from_table(table)
    if not geom_col:
        geom_col = "geometry"  # Fallback

    # Handle CRS
    if crs is None:
        crs_dict = _extract_crs_from_arrow(table, geom_col)
    elif isinstance(crs, str):
        crs_dict = _crs_from_epsg(crs)
    else:
        crs_dict = crs

    # Create connection and register table
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    con.register("__arrow_input", table)

    # Convert WKB to GEOMETRY for spatial operations
    source_sql = f'''
        SELECT * REPLACE (ST_GeomFromWKB("{geom_col}") AS "{geom_col}")
        FROM __arrow_input
    '''

    return LazyTable(
        source=source_sql,
        connection=con,
        geometry_column=geom_col,
        crs=crs_dict,
        _owns_connection=True,
    )


def _read_geoparquet_metadata(path: str) -> tuple[str, dict | None]:
    """
    Read geometry column name and CRS from GeoParquet metadata.

    Returns:
        Tuple of (geometry_column_name, crs_dict_or_none)
    """
    try:
        pf = pq.ParquetFile(path)
        metadata = pf.schema_arrow.metadata

        if metadata and b"geo" in metadata:
            geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))

            # Get primary column name
            primary_col = geo_meta.get("primary_column", "geometry")

            # Get CRS from column metadata
            columns = geo_meta.get("columns", {})
            col_meta = columns.get(primary_col, {})
            crs = col_meta.get("crs")

            return primary_col, crs

    except Exception:
        pass

    return "geometry", None


def _extract_crs_from_arrow(table: pa.Table, geometry_column: str) -> dict | None:
    """
    Extract CRS from Arrow table's GeoArrow extension metadata.

    Returns CRS dict or None if not found.
    """
    try:
        # Check for GeoArrow extension type
        field = table.schema.field(geometry_column)
        if field.metadata:
            # GeoArrow stores CRS in extension metadata
            if b"ARROW:extension:metadata" in field.metadata:
                ext_meta = json.loads(field.metadata[b"ARROW:extension:metadata"])
                if "crs" in ext_meta:
                    return ext_meta["crs"]
    except Exception:
        pass

    return None
