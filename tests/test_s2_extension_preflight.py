"""S2 commands check for the 'geography' extension before doing any work (#737).

The `geography` community extension is published for every DuckDB gpio supports
-- that is why the floor is duckdb>=1.5.6 -- so S2 works out of the box. The
guard still matters: a community extension is downloaded on first use, so an
offline machine, a proxy or a firewall can still leave it unloadable. When that
happens every S2 entry point must fail immediately, with an actionable message,
rather than reading the input and failing deep in the pipeline. These tests mock
the extension load to fail, so they exercise the guard either way.
"""

from pathlib import Path
from unittest import mock

import pyarrow as pa
import pytest

from geoparquet_io.core.add import s2 as add_s2_module
from geoparquet_io.core.exceptions import ExtensionUnavailableError
from geoparquet_io.core.partition import by_s2 as partition_s2_module

MISSING_INPUT = "/nonexistent/definitely-not-here-737.parquet"


def _unavailable(name, feature=None):
    raise ExtensionUnavailableError(
        name, "1.5.5", "Failed to download extension (HTTP 404)", feature=feature
    )


class TestAddS2Preflight:
    """`gpio add s2` refuses up front when 'geography' is unavailable."""

    def test_add_s2_column_checks_before_touching_the_input(self, tmp_path):
        """The extension error wins over the missing input: the check runs first."""
        output = str(tmp_path / "out.parquet")

        with mock.patch.object(add_s2_module, "require_community_extension", _unavailable):
            with pytest.raises(ExtensionUnavailableError) as exc_info:
                add_s2_module.add_s2_column(MISSING_INPUT, output)

        assert "gpio add s2" in str(exc_info.value)
        assert not Path(output).exists()

    def test_add_s2_table_checks_before_registering(self):
        """The table API fails on the preflight, not inside DuckDB."""
        table = pa.table({"geometry": pa.array([], type=pa.binary())})

        with mock.patch.object(add_s2_module, "require_community_extension", _unavailable):
            with pytest.raises(ExtensionUnavailableError) as exc_info:
                add_s2_module.add_s2_table(table)

        assert "gpio add s2" in str(exc_info.value)

    def test_add_s2_preflight_names_geography_and_a_way_forward(self, tmp_path):
        """The message identifies the extension and how to get S2 working today."""
        with mock.patch.object(add_s2_module, "require_community_extension", _unavailable):
            with pytest.raises(ExtensionUnavailableError) as exc_info:
                add_s2_module.add_s2_column(MISSING_INPUT, str(tmp_path / "out.parquet"))

        message = str(exc_info.value)
        assert "geography" in message
        # The way forward is a5, not a downgrade: pyproject forbids duckdb 1.5.1.
        assert "a5" in message
        assert "duckdb==1.5.1" not in message

    def test_invalid_level_still_reported_before_the_extension_check(self, tmp_path):
        """Argument validation stays first: a bad level is a user error, not a build gap."""
        from geoparquet_io.core.exceptions import InvalidParameterError

        with mock.patch.object(add_s2_module, "require_community_extension", _unavailable):
            with pytest.raises(InvalidParameterError):
                add_s2_module.add_s2_column(
                    MISSING_INPUT, str(tmp_path / "out.parquet"), s2_level=99
                )


class TestPartitionS2Preflight:
    """`gpio partition s2` refuses up front when 'geography' is unavailable."""

    def test_partition_by_s2_checks_before_touching_the_input(self, tmp_path):
        """The extension error wins over the missing input: the check runs first."""
        output_folder = str(tmp_path / "parts")

        with mock.patch.object(partition_s2_module, "require_community_extension", _unavailable):
            with pytest.raises(ExtensionUnavailableError) as exc_info:
                partition_s2_module.partition_by_s2(MISSING_INPUT, output_folder, level=10)

        assert "gpio partition s2" in str(exc_info.value)
        assert not Path(output_folder).exists()

    def test_partition_by_s2_validates_arguments_first(self, tmp_path):
        """--auto with --level is a user error and must be reported as one."""
        from geoparquet_io.core.exceptions import InvalidParameterError

        with mock.patch.object(partition_s2_module, "require_community_extension", _unavailable):
            with pytest.raises(InvalidParameterError):
                partition_s2_module.partition_by_s2(
                    MISSING_INPUT, str(tmp_path / "parts"), level=10, auto=True
                )
