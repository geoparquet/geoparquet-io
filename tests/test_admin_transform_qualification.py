"""Guard against ambiguous admin-transform column refs (todo 015).

The Overture region transform must qualify its column references with the admin
alias `b.`. Otherwise, when the input file already has a `region` column, the
two-sided join (`a.*` forwards the user's columns) makes the bare reference
ambiguous and DuckDB raises `BinderException: Ambiguous reference ... "region"`.
"""

from geoparquet_io.core.add.admin_divisions import _build_admin_select_clause
from geoparquet_io.core.admin_datasets import AdminDatasetFactory, build_admin_join_query
from geoparquet_io.core.duckdb_utils import get_duckdb_connection


def test_overture_region_transform_is_qualified():
    """The Overture region transform references `b."region"`, not a bare name."""
    dataset = AdminDatasetFactory.create("overture")
    transform = dataset.get_column_transform("region")
    assert transform is not None
    assert 'b."region"' in transform
    # No bare `region` reference remains once the qualified ones are removed.
    assert "region" not in transform.replace('b."region"', "")


def test_admin_divisions_handles_input_with_region_column():
    """A join succeeds when the input already has a `region` column.

    Without `b.` qualification this raises BinderException (ambiguous "region").
    """
    dataset = AdminDatasetFactory.create("overture")
    partition_columns = dataset.get_partition_columns(["region"])
    select_clause = _build_admin_select_clause(dataset, ["region"], partition_columns)

    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        # Input already carries `region` (and `country`) columns.
        con.execute(
            """
            CREATE OR REPLACE TEMP TABLE _gpio_test_input AS
            SELECT ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))') AS geometry,
                   'pre-existing' AS region, 'XX' AS country, 1 AS fid
            """
        )
        # Admin side exposes its own `region` partition column.
        con.execute(
            """
            CREATE OR REPLACE TEMP TABLE _admin AS
            SELECT ST_GeomFromText('POLYGON((-1 -1, 3 -1, 3 3, -1 3, -1 -1))') AS geometry,
                   'US-CA' AS region
            """
        )

        query = build_admin_join_query(
            input_relation="_gpio_test_input",
            admin_table_ref="_admin",
            admin_select_clause=select_clause,
            input_bbox_col=None,
            admin_bbox_col=None,
            input_geom_col="geometry",
            admin_geom_col="geometry",
            boundary_columns=["region"],
        )
        con.execute(f"CREATE OR REPLACE TEMP TABLE _res AS {query}")
        out_col = dataset.get_output_column_name("region")
        rows = con.execute(f'SELECT fid, "{out_col}" FROM _res ORDER BY fid').fetchall()
    finally:
        con.close()

    # One row; the admin region 'US-CA' is stripped to 'CA' by the transform.
    assert len(rows) == 1
    assert rows[0][1] == "CA"
