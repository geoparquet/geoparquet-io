"""One collision, one behaviour: a column named ``bbox`` that is not a bbox (#1176).

``gpio convert`` picks a free name and declares the covering over it (#1079,
#1168). The other two paths disagreed: ``add_bbox_table`` -- behind
``Table.add_bbox()`` and ``gpio extract arcgis/wfs/carto`` -- silently *dropped*
the user's column, and ``gpio add bbox`` wrote the computed struct under a name
DuckDB renamed while the covering still pointed at the string column, i.e. an
invalid file. A case-variant (``BBOX``) collided too, invisibly: Parquet column
names are case-sensitive, DuckDB's binder is not.

All of them now take convert's decision.
"""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.add.bbox import add_bbox_column, add_bbox_table
from geoparquet_io.core.bbox_structure import check_bbox_structure
from geoparquet_io.core.validate import validate_geoparquet

BBOX_FIELDS = {"xmin", "ymin", "xmax", "ymax"}


def _covering_column(metadata_or_path):
    """The column a geo block's ``covering.bbox`` points at (None when absent)."""
    if isinstance(metadata_or_path, dict):
        raw = metadata_or_path.get(b"geo")
    else:
        raw = pq.read_schema(str(metadata_or_path)).metadata[b"geo"]
    if not raw:
        return None
    geo = json.loads(raw)
    covering = geo["columns"][geo["primary_column"]].get("covering")
    return covering["bbox"]["xmin"][0] if covering else None


def _is_string(field):
    """A string column, whichever width DuckDB handed back."""
    return pa.types.is_string(field.type) or pa.types.is_large_string(field.type)


def _is_bbox_struct(field):
    return pa.types.is_struct(field.type) and BBOX_FIELDS <= {f.name for f in field.type}


def _at_1_1(table):
    """The same table declaring GeoParquet 1.1, which is where ``covering`` lives."""
    metadata = dict(table.schema.metadata or {})
    geo = json.loads(metadata[b"geo"])
    geo["version"] = "1.1.0"
    metadata[b"geo"] = json.dumps(geo).encode("utf-8")
    return table.replace_schema_metadata(metadata)


@pytest.fixture
def table_with_string_bbox(buildings_test_file):
    """A 1.1 GeoParquet table whose ``bbox`` column is a plain string."""
    table = _at_1_1(pq.read_table(buildings_test_file))
    labels = pa.array([f"tile-{i}" for i in range(table.num_rows)], type=pa.string())
    return table.append_column("bbox", labels)


@pytest.fixture
def file_with_string_bbox(table_with_string_bbox, tmp_path):
    path = tmp_path / "string_bbox.parquet"
    pq.write_table(table_with_string_bbox, str(path))
    return str(path)


@pytest.fixture
def file_with_upper_bbox(buildings_test_file, tmp_path):
    """The same collision spelled ``BBOX``: Parquet keeps the two apart, DuckDB does not."""
    table = _at_1_1(pq.read_table(buildings_test_file))
    labels = pa.array([f"tile-{i}" for i in range(table.num_rows)], type=pa.string())
    table = table.append_column("BBOX", labels)
    path = tmp_path / "upper_bbox.parquet"
    pq.write_table(table, str(path))
    return str(path)


#: The user's ``bbox`` struct, spelled the way the spec does not.
UPPER_CHILD_NAMES = ("XMIN", "YMIN", "XMAX", "YMAX")


@pytest.fixture
def table_with_upper_child_bbox(buildings_test_file):
    """A ``bbox`` struct whose children are ``XMIN/YMIN/XMAX/YMAX``.

    A column spelled that way is not one gpio may read as a bbox: the file-based
    detector matches child names as spelled, and no 1.1 ``covering`` may point at
    it (``bbox_covering_problem``). So it is the user's own data, and the
    computed struct has to move aside -- it must not be removed and replaced.
    """
    table = _at_1_1(pq.read_table(buildings_test_file))
    corners = [
        pa.array([float(i) for i in range(table.num_rows)], type=pa.float64())
        for _ in UPPER_CHILD_NAMES
    ]
    struct = pa.StructArray.from_arrays(corners, names=list(UPPER_CHILD_NAMES))
    return table.append_column("bbox", struct)


@pytest.fixture
def file_with_upper_child_bbox(table_with_upper_child_bbox, tmp_path):
    path = tmp_path / "upper_child_bbox.parquet"
    pq.write_table(table_with_upper_child_bbox, str(path))
    return str(path)


def _assert_upper_child_bbox_survived(schema):
    """The user's ``bbox`` struct is still there, children spelled as they were."""
    field = schema.field("bbox")
    assert pa.types.is_struct(field.type), f"'bbox' is {field.type}, not the user's struct"
    assert tuple(child.name for child in field.type) == UPPER_CHILD_NAMES, (
        f"'bbox' children were rewritten to {[c.name for c in field.type]}"
    )


class TestAddBboxTable:
    """The Arrow path behind ``Table.add_bbox()`` and the extract backends."""

    def test_keeps_the_users_column(self, table_with_string_bbox):
        result = add_bbox_table(table_with_string_bbox, geometry_column="geometry")

        assert _is_string(result.schema.field("bbox"))
        assert result.column("bbox").to_pylist() == (
            table_with_string_bbox.column("bbox").to_pylist()
        )

    def test_computes_under_a_free_name(self, table_with_string_bbox):
        result = add_bbox_table(table_with_string_bbox, geometry_column="geometry")

        assert _is_bbox_struct(result.schema.field("bbox_1"))
        assert _covering_column(dict(result.schema.metadata)) == "bbox_1"

    def test_case_variant_collides_too(self, buildings_test_file):
        table = pq.read_table(buildings_test_file)
        labels = pa.array(["x"] * table.num_rows, type=pa.string())
        table = table.append_column("BBOX", labels)

        result = add_bbox_table(table, geometry_column="geometry")

        assert result.column("BBOX").to_pylist() == ["x"] * table.num_rows
        assert _is_bbox_struct(result.schema.field("bbox_1"))
        assert _covering_column(dict(result.schema.metadata)) == "bbox_1"

    def test_an_existing_bbox_struct_is_still_recomputed_in_place(self, buildings_test_file):
        """Not a collision: a real bbox column is replaced, as it always was."""
        table = add_bbox_table(pq.read_table(buildings_test_file), geometry_column="geometry")

        result = add_bbox_table(table, geometry_column="geometry")

        assert result.schema.names.count("bbox") == 1
        assert "bbox_1" not in result.schema.names
        assert _is_bbox_struct(result.schema.field("bbox"))

    def test_no_collision_keeps_the_requested_name(self, buildings_test_file):
        result = add_bbox_table(pq.read_table(buildings_test_file), geometry_column="geometry")

        assert _is_bbox_struct(result.schema.field("bbox"))
        assert _covering_column(dict(result.schema.metadata)) == "bbox"


class TestAddBboxFileBased:
    """``gpio add bbox`` over a file."""

    def test_writes_a_valid_file(self, file_with_string_bbox, tmp_path):
        output = tmp_path / "out.parquet"

        add_bbox_column(file_with_string_bbox, str(output))

        schema = pq.read_schema(str(output))
        assert _is_string(schema.field("bbox"))
        assert _is_bbox_struct(schema.field("bbox_1"))
        assert _covering_column(output) == "bbox_1"

        info = check_bbox_structure(str(output), verbose=False)
        assert info["bbox_column_name"] == "bbox_1"
        assert info["has_bbox_metadata"]
        assert info["covering_problem"] is None

        result = validate_geoparquet(str(output))
        failed = [check.message for check in result.checks if check.status.value == "failed"]
        assert result.is_valid, f"output failed spec validation: {failed}"

    def test_case_variant_writes_a_valid_file(self, file_with_upper_bbox, tmp_path):
        output = tmp_path / "out.parquet"

        add_bbox_column(file_with_upper_bbox, str(output))

        schema = pq.read_schema(str(output))
        assert _is_string(schema.field("BBOX"))
        assert _covering_column(output) == "bbox_1"
        assert _is_bbox_struct(schema.field("bbox_1"))

        result = validate_geoparquet(str(output))
        failed = [check.message for check in result.checks if check.status.value == "failed"]
        assert result.is_valid, f"output failed spec validation: {failed}"

    def test_collision_is_announced(self, file_with_string_bbox, tmp_path, caplog):
        import logging

        output = tmp_path / "out.parquet"
        with caplog.at_level(logging.WARNING):
            add_bbox_column(file_with_string_bbox, str(output))

        assert "column named 'bbox'" in caplog.text
        assert "bbox_1" in caplog.text

    def test_an_existing_bbox_column_is_still_a_pass_through(self, buildings_test_file, tmp_path):
        """Not a collision: an input that already has a usable bbox is copied (#728)."""
        with_bbox = tmp_path / "with_bbox.parquet"
        add_bbox_column(buildings_test_file, str(with_bbox))
        output = tmp_path / "out.parquet"

        add_bbox_column(str(with_bbox), str(output))

        names = pq.read_schema(str(output)).names
        assert names.count("bbox") == 1
        assert "bbox_1" not in names


class TestAddBboxStreaming:
    """The streaming path: same decision, or a pipeline step contradicts the file one."""

    def test_writes_the_covering_over_the_computed_column(self, file_with_string_bbox, tmp_path):
        from geoparquet_io.core.add.bbox import _add_bbox_streaming

        output = tmp_path / "streamed.parquet"

        # The internal entry point the stdin->file path uses.
        _add_bbox_streaming(
            input_path=file_with_string_bbox,
            output_path=str(output),
            bbox_column_name="bbox",
            verbose=False,
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=None,
            profile=None,
            force=False,
            geoparquet_version="1.1",
            memory_limit=None,
        )

        schema = pq.read_schema(str(output))
        assert _is_string(schema.field("bbox"))
        assert _is_bbox_struct(schema.field("bbox_1"))
        assert _covering_column(output) == "bbox_1"

    def test_force_still_replaces_a_real_bbox_struct(self, buildings_test_file, tmp_path):
        from geoparquet_io.core.add.bbox import _add_bbox_streaming

        with_bbox = tmp_path / "with_bbox.parquet"
        add_bbox_column(buildings_test_file, str(with_bbox), geoparquet_version="1.1")
        output = tmp_path / "streamed.parquet"

        _add_bbox_streaming(
            input_path=str(with_bbox),
            output_path=str(output),
            bbox_column_name="bbox",
            verbose=False,
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=None,
            profile=None,
            force=True,
            geoparquet_version="1.1",
            memory_limit=None,
        )

        names = pq.read_schema(str(output)).names
        assert names.count("bbox") == 1
        assert "bbox_1" not in names
        assert _covering_column(output) == "bbox"


class TestThreePathsAgree:
    def test_same_input_same_bbox_column(self, file_with_string_bbox, tmp_path):
        from_file = tmp_path / "from_file.parquet"
        add_bbox_column(file_with_string_bbox, str(from_file))

        from_table = add_bbox_table(
            pq.read_table(file_with_string_bbox), geometry_column="geometry"
        )
        from_convert = tmp_path / "from_convert.parquet"
        from geoparquet_io.core.convert import convert_to_geoparquet

        convert_to_geoparquet(file_with_string_bbox, str(from_convert), geoparquet_version="1.1")

        assert _covering_column(from_file) == "bbox_1"
        assert _covering_column(dict(from_table.schema.metadata)) == "bbox_1"
        assert _covering_column(from_convert) == "bbox_1"

    def test_a_struct_with_case_variant_children_is_user_data(
        self, file_with_upper_child_bbox, table_with_upper_child_bbox, tmp_path
    ):
        """``XMIN/YMIN/XMAX/YMAX`` children: no covering may point there, so the
        column is the user's and the computed struct moves aside on all three
        paths. The Arrow path used to *destroy* it (#1176)."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        from_file = tmp_path / "from_file.parquet"
        add_bbox_column(file_with_upper_child_bbox, str(from_file))
        from_convert = tmp_path / "from_convert.parquet"
        convert_to_geoparquet(
            file_with_upper_child_bbox, str(from_convert), geoparquet_version="1.1"
        )
        from_table = add_bbox_table(table_with_upper_child_bbox, geometry_column="geometry")

        for schema in (
            pq.read_schema(str(from_file)),
            pq.read_schema(str(from_convert)),
            from_table.schema,
        ):
            _assert_upper_child_bbox_survived(schema)
            assert _is_bbox_struct(schema.field("bbox_1"))

        assert _covering_column(from_file) == "bbox_1"
        assert _covering_column(from_convert) == "bbox_1"
        assert _covering_column(dict(from_table.schema.metadata)) == "bbox_1"

    def test_streaming_computes_rather_than_passing_through(
        self, file_with_upper_child_bbox, tmp_path
    ):
        """The streaming detector substring-tested an upper-cased type string, so
        the same struct read as "already has a bbox" and nothing was computed."""
        from geoparquet_io.core.add.bbox import _add_bbox_streaming

        output = tmp_path / "streamed.parquet"
        _add_bbox_streaming(
            input_path=file_with_upper_child_bbox,
            output_path=str(output),
            bbox_column_name="bbox",
            verbose=False,
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=None,
            profile=None,
            force=False,
            geoparquet_version="1.1",
            memory_limit=None,
        )

        schema = pq.read_schema(str(output))
        _assert_upper_child_bbox_survived(schema)
        assert _is_bbox_struct(schema.field("bbox_1"))
        assert _covering_column(output) == "bbox_1"
