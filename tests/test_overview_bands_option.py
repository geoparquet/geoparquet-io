"""`gpio process overview --bands`: build the levels an explicit plan names.

`gpio pmtiles pyramid` already takes a band plan (#1096). Overview took only
`--levels`, so driving both from one ladder meant hand-translating the plan --
dropping the zooms, dropping the base level -- which is exactly the step that
goes wrong silently. The same string now works on both.
"""

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.process.overview.detect import AggregateInfo
from geoparquet_io.core.process.overview.run import create_overviews, levels_from_bands


def _grid_info(base_level: int = 8) -> AggregateInfo:
    return AggregateInfo(
        scheme="h3",
        cell_column="h3_cell",
        base_level=base_level,
        rollup_columns=(),
        out_geometry="polygon",
    )


def _admin_info() -> AggregateInfo:
    return AggregateInfo(
        scheme="admin",
        cell_column="admin_code",
        base_level="region",
        rollup_columns=(),
        out_geometry="polygon",
    )


# --- the band plan -> levels step ------------------------------------------


def test_a_band_plan_names_its_overview_levels():
    # The pyramid ladder from gpio#1103: the base (8) is the input, not an
    # overview, so it drops out.
    levels = levels_from_bands("2:0,4:5,5:7,6:9,7:10,8:11", _grid_info(8))
    assert levels == [2, 4, 5, 6, 7]


def test_the_base_level_is_dropped_wherever_it_sits():
    assert levels_from_bands("4:0,8:6", _grid_info(8)) == [4]


def test_a_plan_naming_only_the_base_names_no_overviews():
    assert levels_from_bands("8:0", _grid_info(8)) == []


def test_a_plan_naming_a_level_finer_than_the_base_is_rejected():
    # 9 is finer than a base of 8; there is nothing to roll up from.
    with pytest.raises(InvalidParameterError):
        levels_from_bands("4:0,9:6", _grid_info(8))


def test_an_admin_plan_names_country():
    assert levels_from_bands("country:0,region:5", _admin_info()) == ["country"]


def test_a_reversed_plan_still_gets_the_reversed_hint():
    with pytest.raises(InvalidParameterError, match="reversed"):
        levels_from_bands("8:11,7:10,6:9,5:7,4:5,2:0", _grid_info(8))


# --- wiring ----------------------------------------------------------------


def test_bands_and_levels_together_are_rejected(tmp_path):
    # The guard must fire before the input is opened, so a missing file is fine.
    src = tmp_path / "by_region.parquet"
    with pytest.raises(InvalidParameterError, match="bands"):
        create_overviews(str(src), levels="country", bands="country:0,region:5")


def test_cli_exposes_bands_on_overview():
    result = CliRunner().invoke(cli, ["process", "overview", "--help"])
    assert result.exit_code == 0
    assert "--bands" in result.output


def test_ops_create_overviews_accepts_bands():
    import inspect

    from geoparquet_io.api import ops

    assert "bands" in inspect.signature(ops.create_overviews).parameters
