"""The malformed-input and verbose arms of the #1172 covering gates.

Each gate reads a ``geo`` block exactly as a file may hold it, so the guard
arms answer rather than crash (#947); and each decision explains itself under
``verbose``. The happy paths are pinned end to end by
``tests/test_covering_write_path_parity.py``; these are the arms a CLI run
only reaches with a hostile footer, kept covered so the fast lane exercises
every branch the gates carry.
"""

import json
import logging

import pyarrow as pa
import pytest

from geoparquet_io.core.geo_metadata import (
    bbox_column_to_declare,
    build_bbox_covering,
    strip_absent_covering,
    strip_illegal_bbox_covering,
)
from geoparquet_io.core.write_funnels import _fast_path_geo_decision

BBOX_STRUCT = pa.struct(
    [("xmin", pa.float64()), ("ymin", pa.float64()), ("xmax", pa.float64()), ("ymax", pa.float64())]
)
OVERTURE_STRUCT = pa.struct(
    [("xmin", pa.float64()), ("xmax", pa.float64()), ("ymin", pa.float64()), ("ymax", pa.float64())]
)


class TestStripIllegalBboxCoveringGuards:
    @pytest.mark.parametrize("columns", ["not-a-dict", ["geometry"], 42, None])
    def test_a_columns_value_that_is_not_an_object_is_left_alone(self, columns):
        geo = {"version": "1.1.0", "columns": columns}
        schema = pa.schema([("geometry", pa.binary())])

        assert strip_illegal_bbox_covering(geo, schema) is geo

    def test_an_index_entry_beside_the_dropped_member_survives(self):
        """Only the ``bbox`` member goes; what remains is #954's question."""
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "covering": {
                        "bbox": build_bbox_covering("bad_box"),
                        "h3": {"column": "h3", "resolution": 9},
                    },
                }
            },
        }
        schema = pa.schema([("geometry", pa.binary()), ("bad_box", OVERTURE_STRUCT)])

        stripped = strip_illegal_bbox_covering(geo, schema)

        covering = stripped["columns"]["geometry"]["covering"]
        assert "bbox" not in covering
        assert covering["h3"] == {"column": "h3", "resolution": 9}
        # Never mutates its input.
        assert "bbox" in geo["columns"]["geometry"]["covering"]


class TestStripAbsentCoveringGuards:
    @pytest.mark.parametrize("columns", ["not-a-dict", ["geometry"], 42, None])
    def test_a_columns_value_that_is_not_an_object_is_left_alone(self, columns):
        geo = {"version": "1.1.0", "columns": columns}

        assert strip_absent_covering(geo, ["geometry"]) is geo

    def test_only_the_absent_entry_goes(self):
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "covering": {
                        "bbox": build_bbox_covering("gone"),
                        "quadkey": {"column": "quadkey"},
                    },
                }
            },
        }

        stripped = strip_absent_covering(geo, ["geometry", "quadkey"])

        covering = stripped["columns"]["geometry"]["covering"]
        assert "bbox" not in covering
        assert covering["quadkey"] == {"column": "quadkey"}
        assert "bbox" in geo["columns"]["geometry"]["covering"]


class TestDeclareRefusesADuplicatedName:
    def test_a_declared_column_the_schema_holds_twice_is_not_declared(self, caplog):
        """Parquet allows two fields of one name; neither is *the* bbox column."""
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {"encoding": "WKB", "covering": {"bbox": build_bbox_covering("mybox")}}
            },
        }
        schema = pa.schema(
            [("geometry", pa.binary()), ("mybox", BBOX_STRUCT), ("mybox", pa.int64())]
        )

        with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
            assert bbox_column_to_declare(schema, geo, verbose=True) is None

        assert "more than once" in caplog.text


class TestTheForcedRewriteSaysWhy:
    def test_the_verbose_arm_names_the_invalidated_stats(self, caplog):
        """A block worth keeping whose derived stats were stripped: (None, True)."""
        block = {
            "version": "2.0.0",
            "primary_column": "geometry",
            # A covering says more than DuckDB generates; geometry_types/bbox
            # are missing, which is what a stats-invalidating filter leaves.
            "columns": {
                "geometry": {"encoding": "WKB", "covering": {"bbox": build_bbox_covering("bbox")}}
            },
        }
        original = {"geo": json.dumps(block)}

        with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
            carried, needs_rewrite = _fast_path_geo_decision(
                original, "geometry", "2.0", verbose=True
            )

        assert carried is None
        assert needs_rewrite is True
        assert "Taking the metadata rewrite" in caplog.text
