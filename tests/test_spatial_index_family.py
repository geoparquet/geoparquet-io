"""Consolidated per-index spatial test family (issue #666, item 1).

One parametrized module replacing the near-verbatim per-index files
(``test_add_{a5,h3,s2,quadkey}.py``, ``test_partition_{a5,s2,quadkey}.py``,
``test_partition_{a5,h3,quadkey}_auto.py`` and the per-index calc classes of
``test_partition_auto_resolution.py``). Behavior common to every index is
asserted once, parametrized over an ``IndexSpec`` table; behavior unique to a
single index (S2's dry-run and token-type checks, quadkey's #680 quoting
regression suite, quadkey's structural partition differences) is kept as
non-parametrized tests at the bottom.

The ``IndexSpec`` table is deliberately shaped to become the adapter for the
planned index registry: per index it carries the table/file/partition entry
points, the column name, the resolution parameter names and valid/invalid
ranges, the CLI command, and the pytest marks.

Marks are attached per spec entry and per family section, mirroring what the
per-index files declared:

- ``test_add_a5.py``, ``test_partition_a5.py`` and ``test_partition_s2.py``
  carried a module-level ``pytest.mark.network`` (the a5/geography DuckDB
  community extensions are installed over the network), so every a5 add/
  partition param and every s2 partition param is network-marked here. Do not
  loosen these: the fast suite must not silently start requiring extensions.
- ``test_add_s2.py`` carried no module mark; its core-level tests rely on
  conftest's unpublished-extension hook to skip when 'geography' 404s (#737),
  and its CLI tests call ``skip_if_geography_unavailable()`` explicitly
  because Click converts the ExtensionUnavailableError into a non-zero exit
  before the hook can see it. That contract is preserved via
  ``cli_needs_geography``.
- ``test_partition_{a5,h3,quadkey}_auto.py`` and the calc classes of
  ``test_partition_auto_resolution.py`` carried *no* marks, a5 included, so
  ``auto_marks`` is empty for every spec. That is deliberate: the a5 auto
  tests run in the fast suite today and this module must not change which
  tests that suite selects.

Old -> new mapping (every replaced test has a home here):

===============================================  ==========================
old                                              new
===============================================  ==========================
Test{AddA5,AddH3,AddS2}Table                     ``test_add_table_*``
Test{AddA5,AddH3,AddS2}File                      ``test_add_file_*``
Test{AddA5,AddH3,AddS2}Streaming                 ``test_add_streaming_*``
Test{AddA5,AddH3,AddS2}CLI                       ``test_add_cli_*``
TestAddQuadkeyCommand::...help                   ``test_add_cli_help[quadkey]``
TestAddQuadkeyCommand::...invalid_resolution     ``TestQuadkeyCLIValidation``
TestAddS2Table::...values_are_strings            ``TestAddS2Specific``
TestAddS2File::...dry_run                        ``TestAddS2Specific``
TestPartitionBy{A5,S2}                           ``test_partition_*``
TestPartition{A5,S2}CLI                          ``test_partition_cli_*``
TestPartitionQuadkeyCommand                      ``test_partition_cli_help``
                                                 + ``TestQuadkeyCLIValidation``
Test{A5,H3,Quadkey}AutoResolutionIntegration     ``test_partition_auto_*``
Test{H3,Quadkey,A5,S2}ResolutionCalculation      ``test_calc_*``
TestLatLonToQuadkey                              kept verbatim
TestGetCrsDisplayName                            kept verbatim
TestQuadkeyColumnNameQuoting                     kept verbatim (#680)
TestValidateResolutions                          kept verbatim
TestCalculatePartitionStats                      kept verbatim
TestPartitionByQuadkeyFunction                   kept verbatim (#490)
TestPartitionByQuadkeyStreaming                  ``TestPartitionByQuadkey
                                                 Function::test_stdin_*``
===============================================  ==========================
"""

import io
import json
import sys
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from functools import partial
from pathlib import Path
from typing import Any
from unittest import mock

import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.add.a5 import add_a5_column, add_a5_table
from geoparquet_io.core.add.h3 import add_h3_column, add_h3_table
from geoparquet_io.core.add.quadkey import (
    add_quadkey_column,
    add_quadkey_table,
)
from geoparquet_io.core.add.s2 import add_s2_column, add_s2_table
from geoparquet_io.core.crs_utils import get_crs_display_name
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.index_ops import (
    calculate_index_resolution,
    get_index_spec,
    lat_lon_to_quadkey,
    validate_partition_resolutions,
)
from geoparquet_io.core.partition.auto_resolution import (
    calculate_auto_resolution,
)
from geoparquet_io.core.partition.by_a5 import partition_by_a5
from geoparquet_io.core.partition.by_h3 import partition_by_h3
from geoparquet_io.core.partition.by_quadkey import (
    partition_by_quadkey,
)
from geoparquet_io.core.partition.by_s2 import partition_by_s2
from geoparquet_io.core.partition.common import calculate_partition_stats
from tests.conftest import skip_if_geography_unavailable, skip_if_grid_unavailable

# Row count of tests/data/places_test.parquet, pinned by the original
# per-index file tests.
PLACES_ROWS = 766


def _no_extra_manual_kwargs(resolution: int) -> dict[str, Any]:
    """Default for ``IndexSpec.auto_manual_extra``: the manual leg needs nothing.

    A module-level function rather than an inline lambda so the dataclass
    default reads as a normal callable; the frozen dataclass's ``__init__``
    always writes it into the instance dict, so it is never bound as a method.
    """
    return {}


@dataclass(frozen=True)
class CalcExpect:
    """Expected ranges for the pure-math auto-resolution calculators.

    Tolerance bands are inherited verbatim from the per-index calc classes:
    the algorithm rounds, spatial data is rarely uniform, and neighbouring
    resolutions are equally valid partitionings — so ranges, not exact values.
    """

    small: tuple  # (lo, hi) for 10K rows / 1K target
    medium: tuple  # 1M rows / 100K target
    large: tuple  # 100M rows / 100K target; hi=None means only a floor
    very_large: tuple  # 1B rows / 10K target
    max_partitions_cap: int  # 1M rows / 10 target / max_partitions=1000
    bounds_max: int  # clamp ceiling asserted (1B rows, target 1)
    bounds_min: tuple  # (min_resolution, max_resolution) with 100 rows / 100


@dataclass(frozen=True)
class IndexSpec:
    """One spatial index's entry points, parameters, CLI shape and marks."""

    name: str
    default_column: str
    # add: table / file / streaming entry points
    add_table: Callable | None
    table_res_kw: str  # e.g. "resolution" (a5/h3) or "level" (s2)
    table_col_kw: str  # e.g. "a5_column_name"
    add_file: Callable | None
    file_res_kw: str  # e.g. "a5_resolution", "s2_level"
    file_col_kw: str
    default_resolution: int  # the resolution the originals exercised
    valid_resolutions: tuple  # each is round-tripped through add_table
    invalid_resolutions: tuple  # each must raise ValueError from add_table
    invalid_resolution_match: str
    # CLI
    cli_res_flag: str  # "--resolution" or "--level"
    cli_needs_geography: bool = False
    # partition (manual resolution)
    partition: Callable | None = None
    partition_res_kw: str = "resolution"
    partition_col_kw: str = ""
    partition_resolution: int = 10
    # No usable defaults: ranges differ per index (a5/s2 0-30, h3 0-15,
    # quadkey 0-23), so a spec that joins PARTITION_FAMILY without setting
    # these fails the spec-sanity test instead of silently testing nothing.
    partition_invalid_resolution: int | None = None
    partition_invalid_match: str = ""
    hive_prefix: str = ""
    # partition --auto
    auto_hive_prefix: str = ""
    auto_conflict_resolution: int = 0
    auto_conflict_match: str = "(?i)cannot specify both"
    auto_first_kwargs: dict = field(default_factory=dict)
    # Extra kwargs for the manual leg of the auto-vs-manual comparison.
    auto_manual_extra: Callable[[int], dict] = _no_extra_manual_kwargs
    # pure-math calculator
    calc: Callable | None = None
    calc_kwargs: dict = field(default_factory=dict)
    calc_expect: CalcExpect | None = None
    # pytest marks per family section (mirrors the old per-file pytestmark)
    add_marks: tuple = ()
    partition_marks: tuple = ()
    auto_marks: tuple = ()

    def add_table_kwargs(self, resolution: int, column: str | None = None) -> dict[str, Any]:
        kwargs: dict[str, Any] = {self.table_res_kw: resolution}
        if column is not None:
            kwargs[self.table_col_kw] = column
        return kwargs

    def add_file_kwargs(self, resolution: int, column: str | None = None) -> dict[str, Any]:
        kwargs: dict[str, Any] = {self.file_res_kw: resolution}
        if column is not None:
            kwargs[self.file_col_kw] = column
        return kwargs

    def partition_kwargs(self, column: str | None = None, **extra: Any) -> dict[str, Any]:
        kwargs: dict[str, Any] = {self.partition_res_kw: self.partition_resolution}
        if column is not None:
            kwargs[self.partition_col_kw] = column
        kwargs.update(extra)
        return kwargs


A5 = IndexSpec(
    name="a5",
    default_column="a5_cell",
    add_table=add_a5_table,
    table_res_kw="resolution",
    table_col_kw="a5_column_name",
    add_file=add_a5_column,
    file_res_kw="a5_resolution",
    file_col_kw="a5_column_name",
    default_resolution=15,
    valid_resolutions=(5, 15, 25),
    invalid_resolutions=(-1, 31),
    invalid_resolution_match="resolution must be between",
    cli_res_flag="--resolution",
    partition=partition_by_a5,
    partition_res_kw="resolution",
    partition_col_kw="a5_column_name",
    partition_invalid_resolution=31,
    partition_invalid_match="must be between 0 and 30",
    hive_prefix="a5_cell=",
    auto_hive_prefix="a5_cell=",
    auto_conflict_resolution=15,
    calc=partial(calculate_index_resolution, get_index_spec("a5")),
    calc_expect=CalcExpect(
        # A5/S2: power-of-4 progression with 6 base cells.
        small=(0, 2),
        medium=(0, 2),  # 6 * 4^res = 10 -> res ~ 0.36
        large=(3, 5),  # 6 * 4^res = 1000 -> res ~ 3.7
        very_large=(5, 9),  # 6 * 4^res = 100K -> res ~ 7.4
        max_partitions_cap=5,
        bounds_max=15,
        bounds_min=(5, 30),
    ),
    # The a5 DuckDB community extension is installed over the network.
    add_marks=(pytest.mark.network,),
    partition_marks=(pytest.mark.network,),
)

H3 = IndexSpec(
    name="h3",
    default_column="h3_cell",
    add_table=add_h3_table,
    table_res_kw="resolution",
    table_col_kw="h3_column_name",
    add_file=add_h3_column,
    file_res_kw="h3_resolution",
    file_col_kw="h3_column_name",
    default_resolution=9,
    valid_resolutions=(5, 9, 12),
    invalid_resolutions=(-1, 16),
    invalid_resolution_match="resolution must be between",
    cli_res_flag="--resolution",
    partition=partition_by_h3,
    partition_col_kw="h3_column_name",
    auto_hive_prefix="h3_cell=",
    auto_conflict_resolution=5,
    calc=partial(calculate_index_resolution, get_index_spec("h3")),
    calc_expect=CalcExpect(
        # H3: ~122 cells at res 0, ~7x more per level.
        small=(0, 2),
        medium=(0, 3),  # ~122 to ~850 cells
        large=(1, None),  # 122 * 7^n = 1000 -> n ~ 1.1
        very_large=(2, 5),  # 122 * 7^n = 100K -> n ~ 3.5
        max_partitions_cap=3,
        bounds_max=5,
        bounds_min=(3, 15),
    ),
)

S2 = IndexSpec(
    name="s2",
    default_column="s2_cell",
    add_table=add_s2_table,
    table_res_kw="level",
    table_col_kw="s2_column_name",
    add_file=add_s2_column,
    file_res_kw="s2_level",
    file_col_kw="s2_column_name",
    default_resolution=13,
    valid_resolutions=(8, 13, 18),
    invalid_resolutions=(-1, 31),
    invalid_resolution_match="level must be between",
    cli_res_flag="--level",
    # S2 needs the 'geography' community extension, unpublished for gpio's
    # DuckDB floor today (#737). Core-level tests run for real and are turned
    # into skips by conftest's unpublished-extension hook; CLI tests must ask
    # first because Click swallows the exception into a non-zero exit.
    cli_needs_geography=True,
    partition=partition_by_s2,
    partition_res_kw="level",
    partition_col_kw="s2_column_name",
    partition_invalid_resolution=31,
    partition_invalid_match="must be between 0 and 30",
    hive_prefix="s2_cell=",
    calc=partial(calculate_index_resolution, get_index_spec("s2")),
    calc_expect=CalcExpect(
        small=(0, 2),
        medium=(0, 2),
        large=(3, 5),
        very_large=(5, 9),
        max_partitions_cap=5,
        bounds_max=15,
        bounds_min=(5, 30),
    ),
    partition_marks=(pytest.mark.network,),
)

QUADKEY = IndexSpec(
    name="quadkey",
    default_column="quadkey",
    add_table=add_quadkey_table,
    table_res_kw="resolution",
    table_col_kw="quadkey_column_name",
    add_file=add_quadkey_column,
    file_res_kw="resolution",
    file_col_kw="quadkey_column_name",
    default_resolution=13,
    valid_resolutions=(),  # quadkey's behavior is pinned by its golden suite
    invalid_resolutions=(),
    invalid_resolution_match="",
    cli_res_flag="--resolution",
    partition=partition_by_quadkey,
    partition_col_kw="quadkey_column_name",
    auto_hive_prefix="quadkey_prefix=",
    auto_conflict_resolution=10,
    auto_conflict_match="(?i)cannot specify --resolution or --partition-resolution",
    auto_first_kwargs={"partition_resolution": None, "use_centroid": False},
    auto_manual_extra=lambda res: {"partition_resolution": res},
    calc=partial(calculate_index_resolution, get_index_spec("quadkey")),
    calc_expect=CalcExpect(
        # Quadkey: 4^zoom tiles.
        small=(0, 3),
        medium=(1, 3),  # 4^zoom = 10 -> zoom ~ 1.7
        large=(4, 6),  # 4^zoom = 1000 -> zoom ~ 5
        very_large=(7, 11),  # 4^zoom = 1M -> zoom = 10
        max_partitions_cap=6,
        bounds_max=8,
        bounds_min=(5, 23),
    ),
)


def _params(specs, marks_attr):
    """Build pytest params carrying each spec's marks for one family section."""
    return [pytest.param(s, id=s.name, marks=list(getattr(s, marks_attr))) for s in specs]


# quadkey's add path is pinned by its own golden-value quoting suite below,
# and its partition path differs structurally (resolution + partition
# resolution), so the parametrized families cover the parallel indexes only.
ADD_FAMILY = _params((A5, H3, S2), "add_marks")
ADD_CLI = _params((A5, H3, S2, QUADKEY), "add_marks")
# One case per out-of-range value, so the low and high ends fail independently
# (the originals had them as separate ``..._invalid_resolution_{low,high}``).
ADD_INVALID_RESOLUTION = [
    pytest.param(spec, resolution, id=f"{spec.name}-{resolution}", marks=list(spec.add_marks))
    for spec in (A5, H3, S2)
    for resolution in spec.invalid_resolutions
]
PARTITION_FAMILY = _params((A5, S2), "partition_marks")
PARTITION_CLI = _params((A5, S2, QUADKEY), "partition_marks")
AUTO_FAMILY = _params((A5, H3, QUADKEY), "auto_marks")
CALC_FAMILY = _params((H3, QUADKEY, A5, S2), "auto_marks")


@pytest.fixture
def sample_table(places_test_file):
    """The places test data as an Arrow table."""
    return pq.read_table(places_test_file)


@pytest.fixture
def sample_file(test_data_dir):
    """Return path to the sample file."""
    return str(test_data_dir / "sample.parquet")


def _table_to_stdin(table, monkeypatch):
    """Mock stdin to stream `table` as Arrow IPC."""
    ipc_buffer = io.BytesIO()
    writer = ipc.RecordBatchStreamWriter(ipc_buffer, table.schema)
    writer.write_table(table)
    writer.close()
    ipc_buffer.seek(0)

    mock_stdin = mock.MagicMock()
    mock_stdin.isatty.return_value = False
    mock_stdin.buffer = ipc_buffer
    monkeypatch.setattr(sys, "stdin", mock_stdin)


# ---------------------------------------------------------------------------
# add <index>: table-level
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("spec", ADD_FAMILY)
def test_add_table_basic(spec, sample_table):
    """Basic cell-column addition on an in-memory table."""
    result = spec.add_table(sample_table, **spec.add_table_kwargs(spec.default_resolution))
    assert spec.default_column in result.column_names
    assert result.num_rows == sample_table.num_rows


@pytest.mark.parametrize("spec", ADD_FAMILY)
def test_add_table_custom_column_name(spec, sample_table):
    """A custom column name lands in the output schema."""
    column = f"my_{spec.name}"
    result = spec.add_table(
        sample_table, **spec.add_table_kwargs(spec.default_resolution, column=column)
    )
    assert column in result.column_names
    assert result.num_rows == sample_table.num_rows


def _specs(params):
    """The IndexSpec carried by each pytest param in a family list."""
    return [param.values[0] for param in params]


def _check_add_family_specs(params, family="ADD_FAMILY"):
    """Every add-family member must carry non-empty resolution knobs.

    ``invalid_resolution_match`` included: ``pytest.raises(..., match="")``
    matches any message, so a blank one silently downgrades
    ``test_add_table_invalid_resolution`` to a bare "some ValueError" check.
    """
    for spec in _specs(params):
        assert spec.valid_resolutions, f"{spec.name}: empty valid_resolutions in {family}"
        assert spec.invalid_resolutions, f"{spec.name}: empty invalid_resolutions in {family}"
        assert spec.invalid_resolution_match, (
            f"{spec.name}: empty invalid_resolution_match in {family}"
        )


def _check_partition_family_specs(params, family="PARTITION_FAMILY"):
    """Every partition-family member must carry its invalid pair and Hive prefix.

    ``hive_prefix`` included: ``test_partition_hive_style`` asserts
    ``spec.hive_prefix in d.name``, and ``"" in name`` is always True.
    """
    for spec in _specs(params):
        assert spec.partition_invalid_resolution is not None, (
            f"{spec.name}: partition_invalid_resolution unset in {family}"
        )
        assert spec.partition_invalid_match, (
            f"{spec.name}: partition_invalid_match unset in {family}"
        )
        assert spec.hive_prefix, f"{spec.name}: empty hive_prefix in {family}"


def _check_auto_family_specs(params, family="AUTO_FAMILY"):
    """Every auto-family member must carry its Hive directory prefix.

    ``test_partition_auto_with_hive`` asserts ``name.startswith(prefix)``,
    which is always True for the ``""`` default.
    """
    for spec in _specs(params):
        assert spec.auto_hive_prefix, f"{spec.name}: empty auto_hive_prefix in {family}"


def test_family_specs_are_not_vacuous():
    """A spec that joins a family with empty knobs must fail loudly, not pass.

    Every other spec-table mistake surfaces as a failing parametrized case;
    an empty resolutions tuple (quadkey's spec carries them today, outside
    the families), an unset partition-invalid pair, a blank ``match`` pattern
    or a blank Hive prefix would instead collect zero cases, or collect cases
    whose assertion is a tautology, and silently green.
    """
    _check_add_family_specs(ADD_FAMILY)
    _check_partition_family_specs(PARTITION_FAMILY)
    _check_auto_family_specs(AUTO_FAMILY)


@pytest.mark.parametrize(
    ("check", "blank_field", "blank_value"),
    [
        (_check_add_family_specs, "valid_resolutions", ()),
        (_check_add_family_specs, "invalid_resolutions", ()),
        (_check_add_family_specs, "invalid_resolution_match", ""),
        (_check_partition_family_specs, "partition_invalid_resolution", None),
        (_check_partition_family_specs, "partition_invalid_match", ""),
        (_check_partition_family_specs, "hive_prefix", ""),
        (_check_auto_family_specs, "auto_hive_prefix", ""),
    ],
    ids=lambda value: getattr(value, "__name__", None) if callable(value) else None,
)
def test_vacuity_checks_reject_a_blank_field(check, blank_field, blank_value):
    """Each guarded field, blanked on an otherwise-complete spec, is rejected.

    Without this the guard itself could regress to a no-op unnoticed: it only
    ever runs against the four real specs, all of which already pass.
    """
    vacuous = replace(A5, name="future", **{blank_field: blank_value})
    with pytest.raises(AssertionError, match=blank_field):
        check([pytest.param(vacuous, id=vacuous.name)])


def test_family_membership_is_pinned():
    """Dropping a spec from a family list must fail, not quietly shrink coverage.

    Removing a member is otherwise invisible: the remaining params still pass,
    so the suite stays green while covering one index less.
    """
    assert {spec.name for spec in _specs(ADD_FAMILY)} == {"a5", "h3", "s2"}
    assert {spec.name for spec in _specs(ADD_CLI)} == {"a5", "h3", "s2", "quadkey"}
    assert {spec.name for spec in _specs(PARTITION_FAMILY)} == {"a5", "s2"}
    assert {spec.name for spec in _specs(PARTITION_CLI)} == {"a5", "s2", "quadkey"}
    assert {spec.name for spec in _specs(AUTO_FAMILY)} == {"a5", "h3", "quadkey"}
    assert {spec.name for spec in _specs(CALC_FAMILY)} == {"a5", "h3", "s2", "quadkey"}
    # One case per (spec, out-of-range value); pinned as pairs so dropping
    # either a spec or one end of a range is loud.
    assert {(param.values[0].name, param.values[1]) for param in ADD_INVALID_RESOLUTION} == {
        ("a5", -1),
        ("a5", 31),
        ("h3", -1),
        ("h3", 16),
        ("s2", -1),
        ("s2", 31),
    }


@pytest.mark.parametrize("spec", ADD_FAMILY)
def test_add_table_different_resolutions(spec, sample_table):
    """Every representative resolution level produces the cell column."""
    for resolution in spec.valid_resolutions:
        result = spec.add_table(sample_table, **spec.add_table_kwargs(resolution))
        assert spec.default_column in result.column_names
        assert result.num_rows == sample_table.num_rows


@pytest.mark.parametrize(("spec", "resolution"), ADD_INVALID_RESOLUTION)
def test_add_table_invalid_resolution(spec, resolution, sample_table):
    """An out-of-range resolution raises ValueError naming the parameter."""
    with pytest.raises(ValueError, match=spec.invalid_resolution_match):
        spec.add_table(sample_table, **spec.add_table_kwargs(resolution))


@pytest.mark.parametrize("spec", ADD_FAMILY)
def test_add_table_metadata_preserved(spec, sample_table):
    """GeoParquet metadata survives the column addition."""
    result = spec.add_table(sample_table, **spec.add_table_kwargs(spec.default_resolution))
    if sample_table.schema.metadata and b"geo" in sample_table.schema.metadata:
        assert b"geo" in result.schema.metadata


# ---------------------------------------------------------------------------
# add <index>: file-level
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("spec", ADD_FAMILY)
def test_add_file_basic(spec, places_test_file, tmp_path):
    """Basic file-to-file cell-column addition."""
    output_file = str(tmp_path / "out.parquet")
    spec.add_file(places_test_file, output_file, **spec.add_file_kwargs(spec.default_resolution))
    assert Path(output_file).exists()
    result = pq.read_table(output_file)
    assert spec.default_column in result.column_names
    assert result.num_rows == PLACES_ROWS


@pytest.mark.parametrize("spec", ADD_FAMILY)
def test_add_file_custom_name(spec, places_test_file, tmp_path):
    """A custom column name reaches the written file."""
    output_file = str(tmp_path / "out.parquet")
    column = f"custom_{spec.name}"
    spec.add_file(
        places_test_file,
        output_file,
        **spec.add_file_kwargs(spec.default_resolution, column=column),
    )
    assert Path(output_file).exists()
    result = pq.read_table(output_file)
    assert column in result.column_names


# ---------------------------------------------------------------------------
# add <index>: streaming
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("spec", ADD_FAMILY)
def test_add_streaming_stdin_to_file(spec, sample_table, tmp_path, monkeypatch):
    """Reading Arrow IPC from mocked stdin writes a complete file."""
    _table_to_stdin(sample_table, monkeypatch)

    output_file = str(tmp_path / "out.parquet")
    spec.add_file("-", output_file, **spec.add_file_kwargs(spec.default_resolution))

    assert Path(output_file).exists()
    result = pq.read_table(output_file)
    assert spec.default_column in result.column_names
    assert result.num_rows == sample_table.num_rows


@pytest.mark.parametrize("spec", ADD_FAMILY)
def test_add_streaming_file_to_stdout(spec, places_test_file, monkeypatch):
    """Writing to mocked stdout produces a readable Arrow IPC stream."""
    output_buffer = io.BytesIO()
    mock_stdout = mock.MagicMock()
    mock_stdout.buffer = output_buffer
    mock_stdout.isatty.return_value = False
    monkeypatch.setattr(sys, "stdout", mock_stdout)

    spec.add_file(places_test_file, "-", **spec.add_file_kwargs(spec.default_resolution))

    output_buffer.seek(0)
    reader = ipc.RecordBatchStreamReader(output_buffer)
    result = reader.read_all()
    assert result.num_rows > 0
    assert spec.default_column in result.column_names


# ---------------------------------------------------------------------------
# add <index>: CLI
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("spec", ADD_CLI)
def test_add_cli_help(spec):
    """Every add subcommand has help naming the index."""
    runner = CliRunner()
    result = runner.invoke(cli, ["add", spec.name, "--help"])
    assert result.exit_code == 0
    assert spec.name in result.output.lower()


@pytest.mark.parametrize("spec", ADD_FAMILY)
def test_add_cli_basic(spec, places_test_file, tmp_path):
    """Basic CLI invocation writes the cell column."""
    if spec.cli_needs_geography:
        skip_if_geography_unavailable()
    skip_if_grid_unavailable(spec.name)

    output_file = str(tmp_path / "out.parquet")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "add",
            spec.name,
            places_test_file,
            output_file,
            spec.cli_res_flag,
            str(spec.default_resolution),
        ],
    )
    assert result.exit_code == 0
    assert Path(output_file).exists()
    loaded = pq.read_table(output_file)
    assert spec.default_column in loaded.column_names


# ---------------------------------------------------------------------------
# partition <index>: manual resolution (a5/s2 parallel family)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("spec", PARTITION_FAMILY)
def test_partition_basic(spec, places_test_file, tmp_path):
    """Basic partitioning creates partition files."""
    output_folder = str(tmp_path / "parts")
    spec.partition(
        places_test_file, output_folder, **spec.partition_kwargs(), verbose=False, force=True
    )
    assert Path(output_folder).exists()
    parquet_files = list(Path(output_folder).glob("*.parquet"))
    assert len(parquet_files) > 0


@pytest.mark.parametrize("spec", PARTITION_FAMILY)
def test_partition_with_existing_column(spec, places_test_file, tmp_path):
    """Partitioning works when the cell column already exists in the input."""
    temp_file = str(tmp_path / f"with_{spec.name}.parquet")
    spec.add_file(
        places_test_file,
        temp_file,
        **spec.add_file_kwargs(spec.partition_resolution),
        verbose=False,
    )

    output_folder = str(tmp_path / "parts")
    spec.partition(temp_file, output_folder, **spec.partition_kwargs(), verbose=False, force=True)
    assert Path(output_folder).exists()
    parquet_files = list(Path(output_folder).glob("*.parquet"))
    assert len(parquet_files) > 0


@pytest.mark.parametrize("spec", PARTITION_FAMILY)
def test_partition_custom_column_name(spec, places_test_file, tmp_path):
    """Partitioning honors a custom cell-column name."""
    output_folder = str(tmp_path / "parts")
    spec.partition(
        places_test_file,
        output_folder,
        **spec.partition_kwargs(column=f"custom_{spec.name}"),
        verbose=False,
        force=True,
    )
    assert Path(output_folder).exists()
    parquet_files = list(Path(output_folder).glob("*.parquet"))
    assert len(parquet_files) > 0


@pytest.mark.parametrize("spec", PARTITION_FAMILY)
def test_partition_hive_style(spec, places_test_file, tmp_path):
    """Hive-style partitioning creates <column>=<value> directories."""
    output_folder = str(tmp_path / "parts")
    spec.partition(
        places_test_file,
        output_folder,
        **spec.partition_kwargs(),
        hive=True,
        verbose=False,
        force=True,
    )
    assert Path(output_folder).exists()
    subdirs = [d for d in Path(output_folder).iterdir() if d.is_dir()]
    assert len(subdirs) > 0
    assert any(spec.hive_prefix in d.name for d in subdirs)


@pytest.mark.parametrize("spec", PARTITION_FAMILY)
def test_partition_preview(spec, places_test_file, tmp_path):
    """Preview mode creates no files."""
    output_folder = str(tmp_path / "parts")
    spec.partition(
        places_test_file, output_folder, **spec.partition_kwargs(), preview=True, verbose=False
    )
    assert not Path(output_folder).exists()


@pytest.mark.parametrize("spec", PARTITION_FAMILY)
def test_partition_invalid_resolution(spec, places_test_file, tmp_path):
    """An out-of-range resolution raises InvalidParameterError."""
    output_folder = str(tmp_path / "parts")
    with pytest.raises(InvalidParameterError, match=spec.partition_invalid_match):
        spec.partition(
            places_test_file,
            output_folder,
            **{spec.partition_res_kw: spec.partition_invalid_resolution},
        )


# ---------------------------------------------------------------------------
# partition <index>: CLI
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("spec", PARTITION_CLI)
def test_partition_cli_help(spec):
    """Every partition subcommand has help naming the index."""
    runner = CliRunner()
    result = runner.invoke(cli, ["partition", spec.name, "--help"])
    assert result.exit_code == 0
    assert spec.name in result.output.lower()


@pytest.mark.parametrize("spec", PARTITION_FAMILY)
def test_partition_cli_basic(spec, places_test_file, tmp_path):
    """Basic CLI invocation creates partition files."""
    if spec.cli_needs_geography:
        skip_if_geography_unavailable()
    skip_if_grid_unavailable(spec.name)

    output_folder = str(tmp_path / "parts")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "partition",
            spec.name,
            places_test_file,
            output_folder,
            spec.cli_res_flag,
            str(spec.partition_resolution),
            "--force",
        ],
    )
    assert result.exit_code == 0
    assert Path(output_folder).exists()
    parquet_files = list(Path(output_folder).glob("*.parquet"))
    assert len(parquet_files) > 0


@pytest.mark.parametrize("spec", PARTITION_FAMILY)
def test_partition_cli_preview(spec, places_test_file, tmp_path):
    """CLI preview mode creates no files."""
    if spec.cli_needs_geography:
        skip_if_geography_unavailable()
    skip_if_grid_unavailable(spec.name)

    output_folder = str(tmp_path / "parts")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "partition",
            spec.name,
            places_test_file,
            output_folder,
            spec.cli_res_flag,
            str(spec.partition_resolution),
            "--preview",
        ],
    )
    assert result.exit_code == 0
    assert not Path(output_folder).exists()


# ---------------------------------------------------------------------------
# partition <index> --auto: integration (a5/h3/quadkey parallel family)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("spec", AUTO_FAMILY)
def test_partition_auto_resolution(spec, fields_5070_file, tmp_path):
    """Auto-resolution partitioning creates valid, non-empty partitions."""
    output_dir = tmp_path / "auto_output"

    spec.partition(
        input_parquet=fields_5070_file,
        output_folder=str(output_dir),
        **{spec.partition_col_kw: spec.default_column},
        resolution=None,
        auto=True,
        target_rows=50,  # Small target for 100-row test file
        max_partitions=100,
        force=True,  # Override tiny partition warnings for test
        verbose=True,
        **spec.auto_first_kwargs,
    )

    assert output_dir.exists()
    parquet_files = list(output_dir.glob("*.parquet"))
    assert len(parquet_files) > 0, "Should create at least one partition"

    for file in parquet_files:
        table = pq.read_table(file)
        assert table.num_rows > 0, f"Partition {file} should have rows"


@pytest.mark.parametrize("spec", AUTO_FAMILY)
def test_partition_auto_vs_manual(spec, fields_5070_file, tmp_path):
    """Auto-resolution matches a manual run at the resolution it calculates."""
    auto_dir = tmp_path / "auto"
    manual_dir = tmp_path / "manual"

    spec.partition(
        input_parquet=fields_5070_file,
        output_folder=str(auto_dir),
        auto=True,
        target_rows=50,
        force=True,
        verbose=False,
    )

    calculated_res = calculate_auto_resolution(
        input_parquet=fields_5070_file,
        spatial_index_type=spec.name,
        target_rows_per_partition=50,
        max_partitions=100,
        verbose=False,
    )

    spec.partition(
        input_parquet=fields_5070_file,
        output_folder=str(manual_dir),
        resolution=calculated_res,
        auto=False,
        force=True,
        verbose=False,
        **spec.auto_manual_extra(calculated_res),
    )

    auto_files = list(auto_dir.glob("*.parquet"))
    manual_files = list(manual_dir.glob("*.parquet"))
    assert len(auto_files) == len(manual_files), "Auto and manual should create same partitions"


@pytest.mark.parametrize("spec", AUTO_FAMILY)
def test_partition_auto_with_constraints(spec, fields_5070_file, tmp_path):
    """Auto-resolution respects the max_partitions constraint (loosely)."""
    output_dir = tmp_path / "constrained"

    spec.partition(
        input_parquet=fields_5070_file,
        output_folder=str(output_dir),
        auto=True,
        target_rows=1,  # Would normally create many partitions
        max_partitions=10,  # But limit to 10
        force=True,
        verbose=False,
    )

    # Low resolutions are coarse (A5: 6/24/96 cells, H3: ~122/~854,
    # quadkey: 1/4/16/64 tiles), so the bound is loose by design.
    parquet_files = list(output_dir.glob("*.parquet"))
    assert len(parquet_files) <= 200, "Should respect max_partitions constraint loosely"


@pytest.mark.parametrize("spec", AUTO_FAMILY)
def test_partition_auto_with_hive(spec, fields_5070_file, tmp_path):
    """Auto-resolution with Hive-style output uses the index's directory prefix."""
    output_dir = tmp_path / "hive"

    spec.partition(
        input_parquet=fields_5070_file,
        output_folder=str(output_dir),
        auto=True,
        target_rows=50,
        hive=True,
        force=True,
        verbose=False,
    )

    subdirs = [d for d in output_dir.iterdir() if d.is_dir()]
    assert len(subdirs) > 0, "Should create Hive-style subdirectories"
    for subdir in subdirs:
        assert subdir.name.startswith(spec.auto_hive_prefix), (
            f"Subdir should be Hive-style: {subdir.name}"
        )


@pytest.mark.parametrize("spec", AUTO_FAMILY)
def test_partition_auto_error_when_both_auto_and_resolution(spec, fields_5070_file, tmp_path):
    """Specifying both --auto and --resolution raises an error."""
    output_dir = tmp_path / "error"

    with pytest.raises(Exception, match=spec.auto_conflict_match):
        spec.partition(
            input_parquet=fields_5070_file,
            output_folder=str(output_dir),
            resolution=spec.auto_conflict_resolution,  # Manual resolution
            auto=True,  # And auto - should conflict
            verbose=False,
        )


@pytest.mark.parametrize("spec", AUTO_FAMILY)
def test_partition_auto_preview(spec, fields_5070_file):
    """Preview mode with auto-resolution runs without an output folder."""
    spec.partition(
        input_parquet=fields_5070_file,
        output_folder=None,  # Not required for preview
        auto=True,
        target_rows=50,
        preview=True,
        verbose=False,
    )
    # If we get here without error, preview worked


# ---------------------------------------------------------------------------
# auto-resolution: pure-math calculators (a5/h3/quadkey/s2)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("spec", CALC_FAMILY)
def test_calc_small_dataset(spec):
    """Small dataset (10K rows / 1K target) uses a low resolution."""
    resolution = spec.calc(
        total_rows=10000, target_rows_per_partition=1000, verbose=False, **spec.calc_kwargs
    )
    lo, hi = spec.calc_expect.small
    assert lo <= resolution <= hi


@pytest.mark.parametrize("spec", CALC_FAMILY)
def test_calc_medium_dataset(spec):
    """Medium dataset (1M rows / 100K target) uses a medium resolution."""
    resolution = spec.calc(
        total_rows=1000000, target_rows_per_partition=100000, verbose=False, **spec.calc_kwargs
    )
    lo, hi = spec.calc_expect.medium
    assert lo <= resolution <= hi


@pytest.mark.parametrize("spec", CALC_FAMILY)
def test_calc_large_dataset(spec):
    """Large dataset (100M rows / 100K target) uses a higher resolution."""
    resolution = spec.calc(
        total_rows=100000000, target_rows_per_partition=100000, verbose=False, **spec.calc_kwargs
    )
    lo, hi = spec.calc_expect.large
    assert resolution >= lo
    if hi is not None:
        assert resolution <= hi


@pytest.mark.parametrize("spec", CALC_FAMILY)
def test_calc_very_large_dataset(spec):
    """Very large dataset (1B rows / 10K target for A5/S2/H3, 1K for quadkey)."""
    # Quadkey's original used a 1K target (aiming at ~1M partitions); the
    # others used 10K (~100K partitions). Preserve each file's arithmetic.
    target = 1000 if spec.name == "quadkey" else 10000
    resolution = spec.calc(
        total_rows=1000000000, target_rows_per_partition=target, verbose=False, **spec.calc_kwargs
    )
    lo, hi = spec.calc_expect.very_large
    assert lo <= resolution <= hi


@pytest.mark.parametrize("spec", CALC_FAMILY)
def test_calc_respects_max_partitions(spec):
    """The calculator caps the resolution to honor max_partitions."""
    resolution = spec.calc(
        total_rows=1000000,
        target_rows_per_partition=10,  # Would create 100K partitions
        max_partitions=1000,  # But limit to 1K
        verbose=False,
        **spec.calc_kwargs,
    )
    assert resolution <= spec.calc_expect.max_partitions_cap


@pytest.mark.parametrize("spec", CALC_FAMILY)
def test_calc_respects_bounds(spec):
    """min_resolution / max_resolution clamp the result at both ends."""
    resolution = spec.calc(
        total_rows=1000000000,
        target_rows_per_partition=1,
        min_resolution=0,
        max_resolution=spec.calc_expect.bounds_max,
        verbose=False,
        **spec.calc_kwargs,
    )
    assert resolution <= spec.calc_expect.bounds_max

    min_res, max_res = spec.calc_expect.bounds_min
    resolution = spec.calc(
        total_rows=100,
        target_rows_per_partition=100,
        min_resolution=min_res,
        max_resolution=max_res,
        **spec.calc_kwargs,
    )
    assert resolution >= min_res


@pytest.mark.parametrize("spec", CALC_FAMILY)
def test_calc_zero_rows(spec):
    """Zero rows returns the minimum resolution."""
    resolution = spec.calc(
        total_rows=0, target_rows_per_partition=100, verbose=False, **spec.calc_kwargs
    )
    assert resolution == 0


# ===========================================================================
# Index-specific tests (unique behavior kept unparametrized)
# ===========================================================================


class TestAddS2Specific:
    """S2-only behavior from test_add_s2.py."""

    def test_add_s2_values_are_strings(self, sample_table):
        """Test that S2 cell values are stored as strings (tokens)."""
        result = add_s2_table(sample_table, level=13)
        s2_col = result.column("s2_cell")
        # S2 tokens are hex strings
        assert s2_col.type == "string" or str(s2_col.type) in ["string", "large_string"]

    def test_add_s2_dry_run(self, places_test_file, tmp_path):
        """Dry-run mode does not create the output file."""
        output_file = str(tmp_path / "out.parquet")
        add_s2_column(places_test_file, output_file, s2_level=13, dry_run=True)
        assert not Path(output_file).exists()


class TestLatLonToQuadkey:
    """Tests for lat_lon_to_quadkey function."""

    def test_known_location(self):
        """Test quadkey generation for a known location."""
        # San Francisco area at zoom level 10
        quadkey = lat_lon_to_quadkey(37.7749, -122.4194, 10)
        assert isinstance(quadkey, str)
        assert len(quadkey) == 10

    def test_equator_prime_meridian(self):
        """Test quadkey at equator/prime meridian."""
        quadkey = lat_lon_to_quadkey(0.0, 0.0, 5)
        assert isinstance(quadkey, str)
        assert len(quadkey) == 5

    def test_different_resolutions(self):
        """Test that higher resolution produces longer quadkeys."""
        lat, lon = 40.7128, -74.0060  # New York
        qk_low = lat_lon_to_quadkey(lat, lon, 5)
        qk_high = lat_lon_to_quadkey(lat, lon, 15)
        assert len(qk_low) == 5
        assert len(qk_high) == 15
        # Higher resolution should start with the lower resolution key
        assert qk_high.startswith(qk_low)


class TestGetCrsDisplayName:
    """Tests for get_crs_display_name function (shared from crs_utils)."""

    def test_none_crs(self):
        """None is an explicit ``crs: null`` — an unknown CRS, not the default."""
        assert get_crs_display_name(None) == "null (CRS unknown)"

    def test_string_crs(self):
        """Test with string CRS."""
        assert get_crs_display_name("EPSG:4326") == "EPSG:4326"

    def test_dict_with_name_and_code(self):
        """Test dict with name and code."""
        crs_dict = {"name": "WGS 84", "id": {"authority": "EPSG", "code": 4326}}
        result = get_crs_display_name(crs_dict)
        assert "WGS 84" in result
        assert "4326" in result

    def test_dict_with_only_code(self):
        """Test dict with only code."""
        crs_dict = {"id": {"authority": "EPSG", "code": 4326}}
        assert get_crs_display_name(crs_dict) == "EPSG:4326"

    def test_empty_dict(self):
        """Test with empty dict."""
        assert get_crs_display_name({}) == "PROJJSON object"


class TestQuadkeyCLIValidation:
    """Quadkey CLI rejects out-of-range resolutions (no other index pins this)."""

    def test_add_quadkey_invalid_resolution_via_cli(self, sample_file, tmp_path):
        """Test with invalid resolution via CLI."""
        output_file = str(tmp_path / "out.parquet")
        runner = CliRunner()
        result = runner.invoke(
            cli, ["add", "quadkey", sample_file, output_file, "--resolution", "25"]
        )
        # Should fail - resolution out of range
        assert result.exit_code != 0

    def test_partition_quadkey_invalid_resolution(self, sample_file, tmp_path):
        """Test with invalid resolution."""
        output_folder = str(tmp_path / "parts")
        runner = CliRunner()
        result = runner.invoke(
            cli, ["partition", "quadkey", sample_file, output_folder, "--resolution", "30"]
        )
        assert result.exit_code != 0


# Column names that are legal in Parquet but are not bare SQL identifiers.
# 'weird name' broke the file-based path with a parser error; 'has"quote'
# broke it with an unterminated-quoted-identifier error (issue #680).
HOSTILE_QUADKEY_NAMES = ["weird name", 'has"quote', "quad-key", "SELECT"]


def _expected_bbox_quadkeys(table, resolution):
    """Golden quadkeys computed in Python from the bbox struct midpoints."""
    return [
        lat_lon_to_quadkey(
            (b["ymin"] + b["ymax"]) / 2.0,
            (b["xmin"] + b["xmax"]) / 2.0,
            resolution,
        )
        for b in table["bbox"].to_pylist()
    ]


class TestQuadkeyColumnNameQuoting:
    """Regression tests for issue #680: non-identifier --quadkey-name values."""

    def test_bbox_path_golden_values_with_default_name(self, places_test_file, tmp_path):
        """Pin the golden quadkey values the bbox fast path must produce."""
        out = tmp_path / "default.parquet"
        add_quadkey_column(places_test_file, str(out), resolution=13)
        result = pq.read_table(str(out))
        source = pq.read_table(places_test_file)

        assert result["quadkey"].to_pylist() == _expected_bbox_quadkeys(source, 13)
        # Row 0 is POINT (-0.9247532486915588 9.85634708404541); the literal below
        # was derived from the Slippy-tile formula independently of this module.
        assert result["quadkey"][0].as_py() == "0333311123230"

    @pytest.mark.parametrize("name", HOSTILE_QUADKEY_NAMES)
    def test_file_based_bbox_path_hostile_name(self, places_test_file, tmp_path, name):
        """File-based bbox path: hostile names keep the correct quadkey values."""
        out = tmp_path / "hostile.parquet"
        add_quadkey_column(places_test_file, str(out), quadkey_column_name=name, resolution=13)

        result = pq.read_table(str(out))
        assert name in result.column_names
        source = pq.read_table(places_test_file)
        assert result[name].to_pylist() == _expected_bbox_quadkeys(source, 13)
        assert result.num_rows == source.num_rows

    @pytest.mark.parametrize("name", ["weird name", 'has"quote'])
    def test_file_based_centroid_path_hostile_name(self, places_test_file, tmp_path, name):
        """File-based centroid path: hostile names keep the correct quadkey values."""
        default_out = tmp_path / "centroid_default.parquet"
        hostile_out = tmp_path / "centroid_hostile.parquet"
        add_quadkey_column(places_test_file, str(default_out), resolution=13, use_centroid=True)
        add_quadkey_column(
            places_test_file,
            str(hostile_out),
            quadkey_column_name=name,
            resolution=13,
            use_centroid=True,
        )

        expected = pq.read_table(str(default_out))["quadkey"].to_pylist()
        actual = pq.read_table(str(hostile_out))[name].to_pylist()
        # Points: centroid keying must agree with bbox-midpoint keying.
        assert expected == _expected_bbox_quadkeys(pq.read_table(places_test_file), 13)
        assert actual == expected

    @pytest.mark.parametrize("name", ["weird name", 'has"quote'])
    def test_cli_hostile_name_end_to_end(self, places_test_file, tmp_path, name):
        """CLI e2e: --quadkey-name accepts hostile names and writes correct values."""
        out = tmp_path / "cli.parquet"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "add",
                "quadkey",
                places_test_file,
                str(out),
                "--resolution",
                "13",
                "--quadkey-name",
                name,
            ],
        )
        assert result.exit_code == 0, result.output

        table = pq.read_table(str(out))
        assert name in table.column_names
        assert table[name].to_pylist() == _expected_bbox_quadkeys(
            pq.read_table(places_test_file), 13
        )

    @pytest.mark.parametrize("name", ["weird name", 'has"quote'])
    def test_geo_metadata_primary_column_survives_hostile_name(
        self, places_test_file, tmp_path, name
    ):
        """Writing a hostile-named quadkey column must not corrupt geo metadata.

        The quadkey `covering` entry must reach the file under a hostile name too,
        alongside the bbox covering derived from the input's bbox column (#694).
        `tests/test_spatial_index_covering.py` pins that coexistence for all four
        index commands; here it guards the quoting path specifically.
        """
        out = tmp_path / "meta.parquet"
        add_quadkey_column(places_test_file, str(out), quadkey_column_name=name, resolution=13)

        meta = pq.ParquetFile(str(out)).metadata.metadata or {}
        geo = json.loads(meta[b"geo"].decode())
        assert geo["primary_column"] == "geometry"
        assert "geometry" in geo["columns"]

        covering = geo["columns"]["geometry"].get("covering", {})
        assert covering.get("quadkey") == {"column": name, "resolution": 13}, covering
        assert covering.get("bbox", {}).get("xmin") == ["bbox", "xmin"], covering

    @pytest.mark.parametrize("name", ["weird name", 'has"quote'])
    def test_table_path_hostile_name(self, places_test_file, name):
        """Sibling table path already quotes — pin it against regressions."""
        source = pq.read_table(places_test_file)
        result = add_quadkey_table(source, quadkey_column_name=name, resolution=13)

        assert name in result.column_names
        assert result[name].to_pylist() == _expected_bbox_quadkeys(source, 13)

    @pytest.mark.parametrize("name", ["weird name", 'has"quote'])
    def test_streaming_path_hostile_name(self, places_test_file, tmp_path, monkeypatch, name):
        """Sibling streaming path already quotes — pin it against regressions."""
        source = pq.read_table(places_test_file)
        _table_to_stdin(source, monkeypatch)

        out = tmp_path / "streamed.parquet"
        add_quadkey_column("-", str(out), quadkey_column_name=name, resolution=13)

        result = pq.read_table(str(out))
        assert name in result.column_names
        assert result[name].to_pylist() == _expected_bbox_quadkeys(source, 13)


class TestValidateResolutions:
    """Tests for quadkey's validate_partition_resolutions function."""

    def test_valid_resolutions(self):
        """Test with valid resolutions."""
        # Should not raise
        validate_partition_resolutions(13, 9, get_index_spec("quadkey"))
        validate_partition_resolutions(23, 23, get_index_spec("quadkey"))
        validate_partition_resolutions(0, 0, get_index_spec("quadkey"))

    def test_resolution_out_of_range(self):
        """Test with resolution out of range."""
        with pytest.raises(InvalidParameterError):
            validate_partition_resolutions(25, 9, get_index_spec("quadkey"))

    def test_partition_resolution_out_of_range(self):
        """Test with partition resolution out of range."""
        with pytest.raises(InvalidParameterError):
            validate_partition_resolutions(13, 25, get_index_spec("quadkey"))

    def test_partition_resolution_exceeds_resolution(self):
        """Test with partition resolution exceeding column resolution."""
        with pytest.raises(InvalidParameterError):
            validate_partition_resolutions(5, 10, get_index_spec("quadkey"))


class TestCalculatePartitionStats:
    """Tests for calculate_partition_stats function."""

    def test_empty_folder(self, tmp_path):
        """Test with empty folder."""
        total_mb, avg_mb = calculate_partition_stats(str(tmp_path), 0)
        assert total_mb == 0
        assert avg_mb == 0

    def test_with_parquet_files(self, tmp_path):
        """Test with parquet files in folder."""
        # Create some dummy parquet files
        for i in range(3):
            f = tmp_path / f"file_{i}.parquet"
            f.write_bytes(b"x" * 1024)  # 1KB each

        total_mb, avg_mb = calculate_partition_stats(str(tmp_path), 3)
        assert total_mb > 0
        assert avg_mb > 0


class TestPartitionByQuadkeyFunction:
    """Quadkey's two-resolution partition path (structurally unlike a5/s2)."""

    def test_partition_basic(self, places_test_file, tmp_path):
        """Test basic partitioning."""
        output_folder = str(tmp_path / "parts")
        partition_by_quadkey(
            places_test_file,
            output_folder,
            resolution=10,
            partition_resolution=5,
            skip_analysis=True,
        )
        # Check partitions were created
        output_path = Path(output_folder)
        assert output_path.exists()
        parquet_files = list(output_path.glob("*.parquet"))
        assert len(parquet_files) > 0
        # Regression #490: the shared finalize must not leak the internal
        # __gpio_part alias into cell-id partition outputs.
        for f in parquet_files:
            names = pq.ParquetFile(f).schema_arrow.names
            assert not any(n.startswith("__gpio_part") for n in names), names

    def test_partition_hive_style(self, places_test_file, tmp_path):
        """Test Hive-style partitioning."""
        output_folder = str(tmp_path / "parts")
        partition_by_quadkey(
            places_test_file,
            output_folder,
            resolution=10,
            partition_resolution=3,
            hive=True,
            skip_analysis=True,
        )
        # Check partitions were created in subdirectories
        output_path = Path(output_folder)
        assert output_path.exists()
        # Hive style creates directories like quadkey=abc/
        subdirs = [d for d in output_path.iterdir() if d.is_dir()]
        assert len(subdirs) > 0

    def test_stdin_to_partition(self, sample_table, tmp_path, monkeypatch):
        """Test partitioning from stdin."""
        _table_to_stdin(sample_table, monkeypatch)

        output_folder = str(tmp_path / "parts")
        partition_by_quadkey(
            "-",
            output_folder,
            resolution=10,
            partition_resolution=5,
            skip_analysis=True,
        )

        # Verify partitions were created
        output_path = Path(output_folder)
        assert output_path.exists()
        parquet_files = list(output_path.glob("*.parquet"))
        assert len(parquet_files) > 0
