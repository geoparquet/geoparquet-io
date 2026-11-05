"""File facts live for one validation; data checks still run for every column."""

import json
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.validate import validate_geoparquet
from scripts.profile_command import CallSpies


def _metadata(columns):
    return {
        b"geo": json.dumps(
            {"version": "1.1.0", "primary_column": next(iter(columns)), "columns": columns}
        ).encode()
    }


def test_wide_validation_bounds_file_work(tmp_path):
    import geoparquet_io.core.parquet_schema as schema_owner
    from geoparquet_io.core import duckdb_utils, validate

    visits = 0
    original = schema_owner.schema_subtree_end

    def subtree(*args):
        nonlocal visits
        visits += 1
        return original(*args)

    for width in (4, 24):
        path = tmp_path / f"wide-{width}.parquet"
        columns = {
            f"geom_{i}": {"encoding": "WKB", "geometry_types": ["Point"]} for i in range(width)
        }
        # A real WKB point; no data check may be elided to achieve the bounds.
        point = bytes.fromhex("0101000000000000000000f03f0000000000000040")
        table = pa.table({name: [point, None] for name in columns})
        pq.write_table(table.replace_schema_metadata(_metadata(columns)), path)
        visits = 0
        with (
            patch.object(schema_owner, "schema_subtree_end", subtree),
            patch.object(
                validate, "describe_query", wraps=duckdb_utils.describe_query, create=True
            ) as descriptions,
            CallSpies() as spies,
        ):
            result = validate_geoparquet(str(path))
        assert result.is_valid, [
            (check.name, check.message) for check in result.checks if check.status.value == "failed"
        ]
        for name in columns:
            for kind in (
                "encoding_matches_data",
                "geometry_types_match_data",
                "coordinates_valid_for_crs",
            ):
                check = next(check for check in result.checks if check.name == f"{kind}_{name}")
                assert check.status == validate.CheckStatus.PASSED, check.message
        assert spies.counts["footer_reads"] <= 8
        assert spies.counts["execute"] <= 5 * width + 12
        assert visits <= 8 * width
        assert descriptions.call_count == 1

    # The shared scan must preserve the independent checks' sample and verdicts,
    # including bounds failures that must not prevent a successful type check.
    import shapely

    for values in (
        [None, "POINT EMPTY", "POINT (3 4)", "POINT (1000 1000)"],
        [None, None],
        ["POINT (Infinity 2)", "POINT (3 4)"],
        [b"", b"bad WKB", "POINT (3 4)"],
    ):
        path = tmp_path / "samples.parquet"
        wkbs = [
            shapely.to_wkb(shapely.from_wkt(value)) if isinstance(value, str) else value
            for value in values
        ]
        table = pa.table({"geometry": pa.array(wkbs, pa.binary())})
        metadata = _metadata({"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}})
        pq.write_table(table.replace_schema_metadata(metadata), path)
        for sample_size in (1, 2, 0):
            shared = validate_geoparquet(str(path), sample_size=sample_size)
            with patch.object(
                validate._ReopeningConnection, "sampled_geometry_facts", return_value=None
            ):
                independent = validate_geoparquet(str(path), sample_size=sample_size)
            assert shared == independent

    # The cached file facts retain the schema and CRS owners' semantics.
    from geoparquet_io.core.crs_utils import source_crs_is_unknown
    from geoparquet_io.core.parquet_schema import root_schema_index

    listings = [
        [],
        [{"name": "", "type": "BYTE_ARRAY", "num_children": 0}],
        [
            {"file_name": "x", "type": None, "num_children": 3},
            {"name": "meta", "type": None, "num_children": 1},
            {"name": "geometry", "type": "INT32", "num_children": 9},
            {"name": "geometry", "type": "BYTE_ARRAY", "num_children": 0},
        ],
        [
            {"name": "broken", "type": None, "num_children": 20},
            {"name": "geometry", "type": "BYTE_ARRAY", "num_children": 0},
        ],
        [
            {"name": "geometry", "type": "BYTE_ARRAY", "num_children": 99},
            {"name": "geometry", "type": "INT32", "num_children": 0},
        ],
    ]
    for listing in listings:
        indexed = validate._ValidationSchema(listing, {})
        for name in ("meta", "broken", "geometry", "missing", ""):
            assert validate._schema_index(indexed, name) == root_schema_index(listing, name)

    from geoparquet_io.core.duckdb_metadata import get_kv_metadata, get_schema_info

    for i, crs in enumerate(
        ("absent", None, {}, "", 0, "srid:0", {"id": {"authority": "EPSG", "code": 4326}})
    ):
        entry = {"encoding": "WKB", "geometry_types": []}
        if crs != "absent":
            entry["crs"] = crs
        path = tmp_path / f"crs-{i}.parquet"
        table = pa.table({"geometry": [None]}, schema=pa.schema([("geometry", pa.binary())]))
        pq.write_table(table.replace_schema_metadata(_metadata({"geometry": entry})), path)
        schema = validate._ValidationSchema(get_schema_info(str(path)), get_kv_metadata(str(path)))
        assert schema.crs_is_unknown(str(path), "geometry") == source_crs_is_unknown(
            str(path), "geometry"
        )
        # A new operation must see replacement metadata at the same path.
        changed = _metadata({"geometry": {"crs": None}})
        pq.write_table(table.replace_schema_metadata(changed), path)
        fresh = validate._ValidationSchema(get_schema_info(str(path)), get_kv_metadata(str(path)))
        assert fresh.crs_is_unknown(str(path), "geometry")

    # Native/geography CRS uses exactly the same type parser and entry precedence.
    for family in ("Geometry", "Geography"):
        for type_crs in ("null", "srid:0", "EPSG:3857"):
            listing = [
                {
                    "name": "geometry",
                    "type": "BYTE_ARRAY",
                    "logical_type": f"{family}Type(crs={type_crs})",
                }
            ]
            for metadata in ({}, _metadata({"geometry": {"crs": None}})):
                with (
                    patch(
                        "geoparquet_io.core.duckdb_metadata.get_schema_info", return_value=listing
                    ),
                    patch(
                        "geoparquet_io.core.duckdb_metadata.get_kv_metadata", return_value=metadata
                    ),
                ):
                    indexed = validate._ValidationSchema(listing, metadata)
                    assert indexed.crs_is_unknown(str(path), "geometry") == source_crs_is_unknown(
                        str(path), "geometry"
                    )

    # Descriptions depend on the connection's conversion setting and expire on reopen.
    from geoparquet_io.core.geometry_detection import resolve_geometry_read

    con = validate._ReopeningConnection(str(path))
    try:
        with patch.object(
            validate, "describe_query", wraps=validate.describe_query
        ) as descriptions:
            for column in ("geometry", "GEOMETRY", "missing"):
                assert con.geometry_read(str(path), column) == resolve_geometry_read(
                    con, str(path), column
                )
            assert descriptions.call_count == 1
            con._sampled_facts[(str(path), "geometry", "")] = (None, 0)
            con._reopen()
            assert not con._sampled_facts
            assert con.geometry_read(str(path), "geometry") == resolve_geometry_read(
                con, str(path), "geometry"
            )
            assert descriptions.call_count == 2
        for interrupt in (KeyboardInterrupt(), RuntimeError("Query interrupted")):
            with patch.object(validate, "geometry_stats_of", side_effect=interrupt):
                with pytest.raises(KeyboardInterrupt):
                    con.sampled_geometry_facts(str(path), "geometry", "", "SELECT 1")
    finally:
        con.close()
