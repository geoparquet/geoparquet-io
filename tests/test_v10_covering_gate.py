"""GeoParquet 1.0 output must not carry the 1.1-only ``covering`` key (gpio #686).

``covering`` is a column-metadata field introduced in GeoParquet 1.1 — it does not
appear anywhere in the v1.0.0 specification. The bbox *column* itself is an ordinary
Parquet column and stays legal at 1.0; only the metadata key is version-gated.

Before the fix, ``convert geoparquet --geoparquet-version 1.0`` wrote a file declaring
version 1.0.0 with a ``covering`` entry, which gpio's own validator rejects, while the
command still exited 0.
"""

import json

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from geoparquet_io.core.geo_metadata import create_geo_metadata
from geoparquet_io.core.validate import validate_geoparquet
from geoparquet_io.core.write_strategies.base import build_geo_metadata
from tests.conftest import get_geo_metadata, get_geoparquet_version


@pytest.fixture
def points_input(tmp_path):
    """A small native-geometry parquet input with no bbox column."""
    path = tmp_path / "src.parquet"
    con = get_duckdb_connection(load_spatial=True)
    con.execute(f"""
        COPY (
          SELECT * FROM (VALUES
            (1, ST_GeomFromText('POINT (30 10)')),
            (2, ST_GeomFromText('POINT (40 40)'))
          ) t(id, geometry)
        ) TO '{path.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V1')
    """)
    con.close()
    return path


def _covering(parquet_file) -> dict | None:
    geo = get_geo_metadata(str(parquet_file))
    primary = geo["primary_column"]
    return geo["columns"][primary].get("covering")


@pytest.fixture
def v10_with_bbox_column(points_input, tmp_path):
    """A valid 1.0 file: bbox column present, no covering key."""
    path = tmp_path / "v10_bbox.parquet"
    convert_to_geoparquet(str(points_input), str(path), skip_hilbert=True, geoparquet_version="1.0")
    assert _covering(path) is None
    assert validate_geoparquet(str(path)).is_valid
    return path


@pytest.fixture
def v11_with_bbox_column(tmp_path):
    """A 1.1 file with a bbox column but no covering — what `add bbox-metadata` is for.

    Written straight through DuckDB rather than by rewriting a converted file with
    pyarrow, so the geo block under test is spelled out here instead of depending
    on what pyarrow happens to attach. (The original reason was a defect — pyarrow
    adds an ``ARROW:schema`` KV key and `add bbox-metadata` emitted unquoted key
    names, so a key containing ':' broke the rewrite. That is fixed: the command
    now shares ``build_kv_metadata_clause()``, and
    ``test_preserves_kv_key_containing_colon`` in ``tests/test_add.py`` covers it.)
    """
    path = tmp_path / "v11_bbox.parquet"
    geo = json.dumps(
        {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": ["Point"],
                    "bbox": [30.0, 10.0, 40.0, 40.0],
                }
            },
        }
    )
    con = get_duckdb_connection(load_spatial=True)
    con.execute(f"""
        COPY (
          SELECT
            id,
            ST_AsWKB(geom)::BLOB AS geometry,
            {{
              'xmin': ST_XMin(geom), 'ymin': ST_YMin(geom),
              'xmax': ST_XMax(geom), 'ymax': ST_YMax(geom)
            }} AS bbox
          FROM (VALUES
            (1, ST_GeomFromText('POINT (30 10)')),
            (2, ST_GeomFromText('POINT (40 40)'))
          ) t(id, geom)
        ) TO '{path.as_posix()}'
        (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE', KV_METADATA {{geo: '{geo}'}})
    """)
    con.close()

    assert get_geoparquet_version(str(path)) == "1.1.0"
    assert _covering(path) is None
    return path


def test_convert_v10_omits_covering_but_keeps_bbox_column(points_input, tmp_path):
    """1.0 output: no covering key, bbox column still written, validator clean."""
    out = tmp_path / "out_10.parquet"
    convert_to_geoparquet(str(points_input), str(out), skip_hilbert=True, geoparquet_version="1.0")

    assert get_geoparquet_version(str(out)) == "1.0.0"
    assert _covering(out) is None, "covering is 1.1-only and must not appear in 1.0 output"

    # The bbox column itself stays — it is a plain column, legal at any version.
    assert "bbox" in pq.ParquetFile(str(out)).schema_arrow.names

    result = validate_geoparquet(str(out))
    failures = [c.message for c in result.checks if c.status.value == "failed"]
    assert result.is_valid, f"1.0 output failed validation: {failures}"


def test_check_bbox_does_not_demand_covering_on_v10(points_input, tmp_path):
    """`check bbox` must not ask a 1.0 file for a key that version cannot carry."""
    from geoparquet_io.core.check_parquet_structure import check_metadata_and_bbox

    out = tmp_path / "check_10.parquet"
    convert_to_geoparquet(str(points_input), str(out), skip_hilbert=True, geoparquet_version="1.0")

    results = check_metadata_and_bbox(str(out), return_results=True, quiet=True)
    assert results["has_bbox_column"]
    assert not results["needs_bbox_metadata"]
    assert not any("covering" in issue for issue in results["issues"])
    # The actionable advice for 1.0 stays the version upgrade, which is already reported.
    assert any("outdated" in issue for issue in results["issues"])


def test_convert_v11_still_writes_covering(points_input, tmp_path):
    """1.1 output keeps the covering key pointing at the bbox column."""
    out = tmp_path / "out_11.parquet"
    convert_to_geoparquet(str(points_input), str(out), skip_hilbert=True, geoparquet_version="1.1")

    assert get_geoparquet_version(str(out)) == "1.1.0"
    covering = _covering(out)
    assert covering is not None and covering["bbox"]["xmin"] == ["bbox", "xmin"]
    assert validate_geoparquet(str(out)).is_valid


def test_convert_v20_output_valid(points_input, tmp_path):
    """2.0 output stays valid (native geo types, no bbox column)."""
    out = tmp_path / "out_20.parquet"
    convert_to_geoparquet(str(points_input), str(out), skip_hilbert=True, geoparquet_version="2.0")
    assert get_geoparquet_version(str(out)) == "2.0.0"
    assert validate_geoparquet(str(out)).is_valid


def test_cli_convert_v10_exits_zero_and_passes_check_spec(points_input, tmp_path):
    """CLI parity for the issue's repro: exit 0 and a file its own checker accepts."""
    out = tmp_path / "cli_10.parquet"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "convert",
            "geoparquet",
            str(points_input),
            str(out),
            "--geoparquet-version",
            "1.0",
            "--skip-hilbert",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "did not pass GeoParquet metadata validation" not in result.output
    assert _covering(out) is None

    check = runner.invoke(cli, ["check", "spec", str(out)])
    assert check.exit_code == 0, check.output
    assert "covering" not in check.output


def test_v10_downgrade_drops_covering_carried_from_source(points_input, tmp_path):
    """A 1.1 file with covering, re-converted to 1.0, must lose the covering key."""
    v11 = tmp_path / "v11.parquet"
    convert_to_geoparquet(str(points_input), str(v11), skip_hilbert=True, geoparquet_version="1.1")
    assert _covering(v11) is not None

    out = tmp_path / "downgraded.parquet"
    convert_to_geoparquet(str(v11), str(out), skip_hilbert=True, geoparquet_version="1.0")
    assert get_geoparquet_version(str(out)) == "1.0.0"
    assert _covering(out) is None
    assert validate_geoparquet(str(out)).is_valid


def test_build_geo_metadata_gates_covering_by_version():
    """The shared assembly point drops covering for 1.0 and keeps it for 1.1/2.0."""
    source = {
        "geo": json.dumps(
            {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {
                    "geometry": {
                        "encoding": "WKB",
                        "covering": {
                            "bbox": {
                                "xmin": ["bbox", "xmin"],
                                "ymin": ["bbox", "ymin"],
                                "xmax": ["bbox", "xmax"],
                                "ymax": ["bbox", "ymax"],
                            }
                        },
                    }
                },
            }
        )
    }

    v10 = build_geo_metadata("geometry", "1.0", original_metadata=source)
    assert "covering" not in v10["columns"]["geometry"]

    for version in ("1.1", "2.0"):
        kept = build_geo_metadata("geometry", version, original_metadata=source)
        assert "covering" in kept["columns"]["geometry"], version


def test_create_geo_metadata_gates_covering_by_version():
    """The Arrow-table assembly point applies the same gate."""
    bbox_info = {"has_bbox_column": True, "bbox_column_name": "bbox"}

    v10 = create_geo_metadata(None, "geometry", bbox_info, version="1.0.0")
    assert "covering" not in v10["columns"]["geometry"]

    v11 = create_geo_metadata(None, "geometry", bbox_info, version="1.1.0")
    assert v11["columns"]["geometry"]["covering"]["bbox"]["xmin"] == ["bbox", "xmin"]

    # Verbose mode reports the drop rather than silently changing the output.
    verbose_v10 = create_geo_metadata(None, "geometry", bbox_info, None, True, version="1.0.0")
    assert "covering" not in verbose_v10["columns"]["geometry"]


def test_create_geo_metadata_drops_custom_covering_for_v10():
    """Spatial-index coverings (h3/s2/...) are also 1.1-only."""
    custom = {"covering": {"h3": {"column": "h3", "resolution": 8}}}

    v10 = create_geo_metadata(None, "geometry", None, custom, version="1.0.0")
    assert "covering" not in v10["columns"]["geometry"]

    v11 = create_geo_metadata(None, "geometry", None, custom, version="1.1.0")
    assert "h3" in v11["columns"]["geometry"]["covering"]


# ---------------------------------------------------------------------------
# Review round 1: the explicit "add the covering" entry points.
#
# These differ from the write paths above. There, covering is an *implicit*
# side effect of writing a bbox column, so silently omitting it at 1.0 is the
# right call. Here the user has explicitly asked for the covering key, and at
# 1.0 that request cannot be honored — so these raise rather than quietly
# doing nothing while reporting success.
# ---------------------------------------------------------------------------


def test_add_bbox_metadata_rejects_v10_file(v10_with_bbox_column):
    """`add bbox-metadata` on a 1.0 file errors instead of writing an invalid key."""
    from geoparquet_io.core.add.bbox_metadata import add_bbox_metadata
    from geoparquet_io.core.exceptions import GeoParquetError

    with pytest.raises(GeoParquetError, match="1.1"):
        add_bbox_metadata(str(v10_with_bbox_column))

    # The file must be left exactly as valid as it was.
    assert _covering(v10_with_bbox_column) is None
    assert validate_geoparquet(str(v10_with_bbox_column)).is_valid


def test_add_bbox_metadata_cli_rejects_v10_file(v10_with_bbox_column):
    """CLI surfaces the conflict as a clean error, not a silent success."""
    runner = CliRunner()
    result = runner.invoke(cli, ["add", "bbox-metadata", str(v10_with_bbox_column)])

    assert result.exit_code != 0
    assert "1.1" in result.output
    assert _covering(v10_with_bbox_column) is None
    assert validate_geoparquet(str(v10_with_bbox_column)).is_valid


def test_add_bbox_metadata_still_works_on_v11_file(v11_with_bbox_column):
    """The 1.1 path is untouched: the covering key is written as before.

    Overall validity is asserted too, now that #712 is fixed -- the rewrite used
    to re-materialize a v1.x WKB column as a native Parquet GEOMETRY type while
    leaving the version at 1.1.0, which the validator rejects.
    """
    from geoparquet_io.core.add.bbox_metadata import add_bbox_metadata

    add_bbox_metadata(str(v11_with_bbox_column))

    covering = _covering(v11_with_bbox_column)
    assert covering is not None and covering["bbox"]["xmin"] == ["bbox", "xmin"]
    assert validate_geoparquet(str(v11_with_bbox_column)).is_valid


def test_api_add_bbox_metadata_rejects_v10_table(v10_with_bbox_column):
    """Table.add_bbox_metadata() applies the same gate as the CLI path."""
    from geoparquet_io.api import read

    table = read(str(v10_with_bbox_column))
    with pytest.raises(ValueError, match="1.1"):
        table.add_bbox_metadata()


def test_api_add_bbox_metadata_still_works_on_v11_table(v11_with_bbox_column):
    """The 1.1 Python API path is untouched."""
    from geoparquet_io.api import read

    table = read(str(v11_with_bbox_column))
    with_meta = table.add_bbox_metadata()

    geo_meta = with_meta.metadata().get("geo_metadata", {})
    columns = geo_meta.get("columns", {})
    assert columns[with_meta.geometry_column]["covering"]["bbox"]["xmin"] == ["bbox", "xmin"]


def test_build_geo_metadata_does_not_mutate_caller_metadata():
    """The 1.0 gate must not strip covering out of the caller's own dict.

    Partition loops reuse one ``original_metadata`` dict across many writes
    (see the aliasing note at common.py:2362); popping through the shallow copy
    in ``_initialize_geo_metadata`` would strip the shared dict permanently and
    silently cost later 1.1 writes their covering.
    """
    covering = {
        "bbox": {
            "xmin": ["bbox", "xmin"],
            "ymin": ["bbox", "ymin"],
            "xmax": ["bbox", "xmax"],
            "ymax": ["bbox", "ymax"],
        }
    }
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "covering": covering}},
    }
    source = {"geo": geo}

    built = build_geo_metadata("geometry", "1.0", original_metadata=source)
    assert "covering" not in built["columns"]["geometry"]
    assert "covering" in geo["columns"]["geometry"], "caller's metadata dict was mutated"

    # A later write from the same shared dict must still get its covering.
    again = build_geo_metadata("geometry", "1.1", original_metadata=source)
    assert again["columns"]["geometry"]["covering"] == covering


def test_check_bbox_flags_v10_file_that_carries_covering(v10_with_bbox_column):
    """A pre-existing 1.0+covering file is reported as a problem, not affirmed."""
    from geoparquet_io.core.check_parquet_structure import check_metadata_and_bbox

    # Hand-build the invalid combination this branch no longer produces.
    table = pq.read_table(str(v10_with_bbox_column))
    meta = dict(table.schema.metadata)
    geo = json.loads(meta[b"geo"].decode())
    geo["columns"][geo["primary_column"]]["covering"] = {
        "bbox": {
            "xmin": ["bbox", "xmin"],
            "ymin": ["bbox", "ymin"],
            "xmax": ["bbox", "xmax"],
            "ymax": ["bbox", "ymax"],
        }
    }
    meta[b"geo"] = json.dumps(geo).encode()
    pq.write_table(table.replace_schema_metadata(meta), str(v10_with_bbox_column))
    assert not validate_geoparquet(str(v10_with_bbox_column)).is_valid

    results = check_metadata_and_bbox(str(v10_with_bbox_column), return_results=True, quiet=True)
    assert not results["passed"]
    assert any("covering" in issue for issue in results["issues"]), (
        f"check bbox affirmed an invalid 1.0+covering file: {results['issues']}"
    )

    # The printed report must not show a ✓ for this file either.
    check_metadata_and_bbox(str(v10_with_bbox_column), return_results=True, quiet=False)


def test_add_bbox_on_v10_file_does_not_suggest_a_command_that_refuses(v10_with_bbox_column):
    """`add bbox` must not send a 1.0 user to `add bbox-metadata`, which now refuses.

    The file already has a bbox column but no covering, so `add bbox` bails with
    advice. Pointing at `add bbox-metadata` would be a dead end for a 1.0 file.
    """
    runner = CliRunner()
    out = v10_with_bbox_column.parent / "add_bbox_out.parquet"
    result = runner.invoke(cli, ["add", "bbox", str(v10_with_bbox_column), str(out)])

    assert "1.1" in result.output, result.output
    assert "add bbox-metadata" not in result.output, (
        f"1.0 user sent to a command that will refuse: {result.output}"
    )


def test_add_bbox_on_v11_file_still_suggests_bbox_metadata(v11_with_bbox_column):
    """The 1.1 advice is unchanged — that command does work there."""
    runner = CliRunner()
    out = v11_with_bbox_column.parent / "add_bbox_out.parquet"
    result = runner.invoke(cli, ["add", "bbox", str(v11_with_bbox_column), str(out)])

    assert "add bbox-metadata" in result.output, result.output


def test_strip_unsupported_covering_tolerates_malformed_columns():
    """Malformed third-party geo metadata must not crash the gate."""
    from geoparquet_io.core.geo_metadata import strip_unsupported_covering

    malformed = {"version": "1.0.0", "columns": "not-a-dict"}
    assert strip_unsupported_covering(malformed, "1.0") is malformed
