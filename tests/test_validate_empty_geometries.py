from geoparquet_io.core.validate import (
    CheckStatus,
    _build_bbox_query,
    _check_native_geo_stats_contains_data,
)


class _Result:
    def __init__(self, row=None, rows=None):
        self._row = row
        self._rows = rows

    def fetchone(self):
        return self._row

    def fetchall(self):
        return self._rows


# One row group, so the file-wide union the check reads is this chunk's bounds.
_DECLARED_STATS_ROWS = [
    (0, {"xmin": 0, "ymin": 0, "xmax": 1, "ymax": 1, "zmin": None, "zmax": None}, ["point"]),
]


class _RecordingConnection:
    def __init__(self, containment_row=(3, 3)):
        self.queries = []
        self._containment_row = containment_row

    def execute(self, query):
        self.queries.append(query)
        if "parquet_metadata" in query:
            return _Result(rows=_DECLARED_STATS_ROWS)
        return _Result(row=self._containment_row)


def test_bbox_query_excludes_empty_geometries():
    query = _build_bbox_query("input.parquet", "geometry", "BLOB", (0, 0, 1, 1), "")

    assert "NOT ST_IsEmpty((CASE WHEN" in query
    assert 'THEN ST_GeomFromWKB(list_extract(["geometry"], 1)) END)' in query


def test_native_geo_stats_query_excludes_empty_geometries(tmp_path):
    connection = _RecordingConnection(containment_row=(3, 3))
    parquet_file = tmp_path / "input.parquet"
    parquet_file.touch()

    result = _check_native_geo_stats_contains_data(
        str(parquet_file), "geometry", connection, sample_size=0
    )

    assert result.status is CheckStatus.PASSED
    containment_query = connection.queries[-1]
    assert 'NOT ST_IsEmpty("geometry")' in containment_query


def test_native_geo_stats_all_rows_empty_is_skipped(tmp_path):
    """When every row is NULL or EMPTY there is nothing to vouch for: SKIPPED."""
    connection = _RecordingConnection(containment_row=(0, 0))
    parquet_file = tmp_path / "input.parquet"
    parquet_file.touch()

    result = _check_native_geo_stats_contains_data(
        str(parquet_file), "geometry", connection, sample_size=0
    )

    assert result.status is CheckStatus.SKIPPED
    assert "no non-empty geometries" in result.message
