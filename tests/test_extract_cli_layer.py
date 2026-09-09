"""Offline tests for the ``gpio extract`` CLI layer.

These cover the marshalling that happens *above* the core converters - option
parsing, bbox strings, the multi-layer/single-layer split, and the translation
of core errors into Click errors. Everything below that line (the actual
network converters) is patched out, so nothing here touches the network.

Patching follows the repo convention: ``importlib.import_module`` plus
``patch.object`` on the module object, never a dotted string target.
"""

import importlib
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli

arcgis_module = importlib.import_module("geoparquet_io.core.arcgis")
bigquery_module = importlib.import_module("geoparquet_io.core.extract_bigquery")
carto_module = importlib.import_module("geoparquet_io.core.carto")
streaming_module = importlib.import_module("geoparquet_io.core.streaming")
wfs_module = importlib.import_module("geoparquet_io.core.wfs")


@pytest.fixture
def runner():
    return CliRunner()


class TestExtractGeoparquetCliLayer:
    """``gpio extract geoparquet`` error translation."""

    def test_streaming_error_becomes_click_exception(self, runner, places_test_file, tmp_path):
        """A StreamingError from validate_output is reported without a traceback."""
        output = tmp_path / "out.parquet"
        with patch.object(
            streaming_module,
            "validate_output",
            side_effect=streaming_module.StreamingError("Missing output. Pipe to another command."),
        ):
            result = runner.invoke(cli, ["extract", "geoparquet", places_test_file, str(output)])

        assert result.exit_code != 0
        assert "Missing output. Pipe to another command." in result.output
        assert "Traceback" not in result.output
        assert not output.exists()


class TestExtractArcgisCliLayer:
    """``gpio extract arcgis`` bbox parsing."""

    SERVICE_URL = "https://example.com/arcgis/rest/services/x/FeatureServer/0"

    def test_bbox_string_reaches_core_as_float_tuple(self, runner, tmp_path):
        output = tmp_path / "out.parquet"
        with patch.object(arcgis_module, "convert_arcgis_to_geoparquet") as mock_convert:
            result = runner.invoke(
                cli,
                [
                    "extract",
                    "arcgis",
                    self.SERVICE_URL,
                    str(output),
                    "--bbox",
                    " -122.5, 37.5 ,-122.0,38.0 ",
                ],
            )

        assert result.exit_code == 0, result.output
        kwargs = mock_convert.call_args.kwargs
        assert kwargs["bbox"] == (-122.5, 37.5, -122.0, 38.0)
        assert kwargs["service_url"] == self.SERVICE_URL
        assert kwargs["output_file"] == str(output)

    def test_no_bbox_passes_none(self, runner, tmp_path):
        output = tmp_path / "out.parquet"
        with patch.object(arcgis_module, "convert_arcgis_to_geoparquet") as mock_convert:
            result = runner.invoke(cli, ["extract", "arcgis", self.SERVICE_URL, str(output)])

        assert result.exit_code == 0, result.output
        assert mock_convert.call_args.kwargs["bbox"] is None

    def test_bbox_with_wrong_value_count_is_rejected(self, runner, tmp_path):
        output = tmp_path / "out.parquet"
        with patch.object(arcgis_module, "convert_arcgis_to_geoparquet") as mock_convert:
            result = runner.invoke(
                cli,
                [
                    "extract",
                    "arcgis",
                    self.SERVICE_URL,
                    str(output),
                    "--bbox",
                    "-122.5,37.5,-122.0",
                ],
            )

        assert result.exit_code != 0
        assert "Invalid bbox format" in result.output
        assert "xmin,ymin,xmax,ymax" in result.output
        mock_convert.assert_not_called()

    def test_non_numeric_bbox_is_rejected(self, runner, tmp_path):
        output = tmp_path / "out.parquet"
        with patch.object(arcgis_module, "convert_arcgis_to_geoparquet") as mock_convert:
            result = runner.invoke(
                cli,
                ["extract", "arcgis", self.SERVICE_URL, str(output), "--bbox", "west,south,e,n"],
            )

        assert result.exit_code != 0
        assert "Invalid bbox format" in result.output
        mock_convert.assert_not_called()


class TestExtractBigqueryCliLayer:
    """``gpio extract bigquery`` option marshalling."""

    TABLE_ID = "myproject.geodata.buildings"

    def test_options_reach_the_extractor(self, runner, tmp_path):
        output = tmp_path / "out.parquet"
        with patch.object(bigquery_module, "extract_bigquery") as mock_extract:
            result = runner.invoke(
                cli,
                [
                    "extract",
                    "bigquery",
                    self.TABLE_ID,
                    str(output),
                    "--where",
                    "area > 1000",
                    "--limit",
                    "25",
                    "--compression",
                    "zstd",
                    "--row-group-size-mb",
                    "128MB",
                    "--geography-column",
                    "geom",
                    "--geometry-format",
                    "geojson",
                ],
            )

        assert result.exit_code == 0, result.output
        kwargs = mock_extract.call_args.kwargs
        assert kwargs["table_id"] == self.TABLE_ID
        assert kwargs["output_parquet"] == str(output)
        assert kwargs["where"] == "area > 1000"
        assert kwargs["limit"] == 25
        # The CLI upper-cases the codec before handing it to core.
        assert kwargs["compression"] == "ZSTD"
        assert kwargs["row_group_size_mb"] == 128
        assert kwargs["row_group_rows"] is None
        assert kwargs["geography_column"] == "geom"
        assert kwargs["geometry_format"] == "geojson"

    def test_row_group_rows_passed_through_without_mb(self, runner, tmp_path):
        output = tmp_path / "out.parquet"
        with patch.object(bigquery_module, "extract_bigquery") as mock_extract:
            result = runner.invoke(
                cli,
                ["extract", "bigquery", self.TABLE_ID, str(output), "--row-group-size", "50000"],
            )

        assert result.exit_code == 0, result.output
        kwargs = mock_extract.call_args.kwargs
        assert kwargs["row_group_rows"] == 50000
        assert kwargs["row_group_size_mb"] is None

    def test_non_parquet_output_is_rejected(self, runner, tmp_path):
        output = tmp_path / "out.csv"
        with patch.object(bigquery_module, "extract_bigquery") as mock_extract:
            result = runner.invoke(cli, ["extract", "bigquery", self.TABLE_ID, str(output)])

        assert result.exit_code != 0
        assert ".parquet extension" in result.output
        mock_extract.assert_not_called()

    def test_any_extension_allows_other_suffixes(self, runner, tmp_path):
        output = tmp_path / "out.pq"
        with patch.object(bigquery_module, "extract_bigquery") as mock_extract:
            result = runner.invoke(
                cli, ["extract", "bigquery", self.TABLE_ID, str(output), "--any-extension"]
            )

        assert result.exit_code == 0, result.output
        assert mock_extract.call_args.kwargs["output_parquet"] == str(output)

    def test_streaming_error_becomes_click_exception(self, runner, tmp_path):
        output = tmp_path / "out.parquet"
        with (
            patch.object(
                streaming_module,
                "validate_output",
                side_effect=streaming_module.StreamingError("Missing output."),
            ),
            patch.object(bigquery_module, "extract_bigquery") as mock_extract,
        ):
            result = runner.invoke(cli, ["extract", "bigquery", self.TABLE_ID, str(output)])

        assert result.exit_code != 0
        assert "Missing output." in result.output
        assert "Traceback" not in result.output
        mock_extract.assert_not_called()


class TestWfsDeprecatedVersionFlag:
    """The hidden ``--version`` alias warns and still takes effect."""

    def test_deprecated_flag_warns_and_overrides_wfs_version(self, runner):
        with patch.object(wfs_module, "list_available_layers", return_value=[]) as mock_list:
            result = runner.invoke(
                cli, ["extract", "wfs", "https://geo.example.com/wfs", "--version", "2.0.0"]
            )

        assert result.exit_code == 0, result.output
        assert "Warning: --version is deprecated, use --wfs-version instead" in result.output
        assert mock_list.call_args.kwargs["version"] == "2.0.0"

    def test_no_warning_without_the_deprecated_flag(self, runner):
        with patch.object(wfs_module, "list_available_layers", return_value=[]) as mock_list:
            result = runner.invoke(cli, ["extract", "wfs", "https://geo.example.com/wfs"])

        assert result.exit_code == 0, result.output
        assert "deprecated" not in result.output
        assert mock_list.call_args.kwargs["version"] == "1.1.0"


class TestExtractWfsListLayers:
    """``gpio extract wfs URL`` with no TYPENAME lists layers."""

    URL = "https://geo.example.com/wfs"

    def test_layers_are_printed_with_title_and_truncated_abstract(self, runner):
        long_abstract = "A" * 150
        layers = [
            {"typename": "roads", "title": "Road network", "abstract": long_abstract},
            {"typename": "cities", "title": "cities", "abstract": ""},
            {},
        ]
        with patch.object(wfs_module, "list_available_layers", return_value=layers):
            result = runner.invoke(cli, ["extract", "wfs", self.URL])

        assert result.exit_code == 0, result.output
        assert "Available layers in WFS service (3 found):" in result.output
        assert "roads" in result.output
        assert "Title: Road network" in result.output
        # A title equal to the typename is not repeated.
        assert "Title: cities" not in result.output
        # Long abstracts are truncated to 97 chars plus an ellipsis.
        assert f"Description: {'A' * 97}..." in result.output
        assert "A" * 98 not in result.output
        # A layer with no typename key falls back to "unknown".
        assert "unknown" in result.output

    def test_auto_version_is_negotiated_before_listing(self, runner):
        with (
            patch.object(
                wfs_module, "negotiate_wfs_version", return_value=("2.0.0", None)
            ) as mock_negotiate,
            patch.object(wfs_module, "list_available_layers", return_value=[]) as mock_list,
        ):
            result = runner.invoke(cli, ["extract", "wfs", self.URL, "--wfs-version", "auto"])

        assert result.exit_code == 0, result.output
        mock_negotiate.assert_called_once_with(self.URL)
        assert mock_list.call_args.kwargs["version"] == "2.0.0"

    def test_empty_layer_list_reports_none_found(self, runner):
        with patch.object(wfs_module, "list_available_layers", return_value=[]):
            result = runner.invoke(cli, ["extract", "wfs", self.URL])

        assert result.exit_code == 0, result.output
        assert "No layers found in WFS service." in result.output

    def test_wfs_error_while_listing_becomes_click_exception(self, runner):
        with patch.object(
            wfs_module,
            "list_available_layers",
            side_effect=wfs_module.WFSError("Service returned 500"),
        ):
            result = runner.invoke(cli, ["extract", "wfs", self.URL])

        assert result.exit_code != 0
        assert "Service returned 500" in result.output
        assert "Traceback" not in result.output


class TestExtractWfsArgumentValidation:
    """``gpio extract wfs`` argument checks that never reach the network."""

    URL = "https://geo.example.com/wfs"

    def test_typename_without_output_file_is_rejected(self, runner):
        result = runner.invoke(cli, ["extract", "wfs", self.URL, "roads"])

        assert result.exit_code != 0
        assert "OUTPUT_FILE is required when TYPENAME is specified." in result.output
        assert f"gpio extract wfs {self.URL} roads OUTPUT_FILE" in result.output

    def test_blank_typename_is_rejected(self, runner, tmp_path):
        result = runner.invoke(
            cli, ["extract", "wfs", self.URL, " , ", str(tmp_path / "o.parquet")]
        )

        assert result.exit_code != 0
        assert "No valid typename(s) provided" in result.output

    def test_multiple_layers_with_a_file_output_is_rejected(self, runner, tmp_path):
        output = tmp_path / "out.parquet"
        result = runner.invoke(cli, ["extract", "wfs", self.URL, "roads,buildings", str(output)])

        assert result.exit_code != 0
        assert "Multiple layers specified (2)" in result.output
        assert "provide a directory path" in result.output

    def test_single_layer_output_must_be_parquet(self, runner, tmp_path):
        result = runner.invoke(
            cli, ["extract", "wfs", self.URL, "roads", str(tmp_path / "out.csv")]
        )

        assert result.exit_code != 0
        assert ".parquet extension" in result.output

    def test_invalid_bbox_is_rejected(self, runner, tmp_path):
        with patch.object(wfs_module, "convert_wfs_to_geoparquet") as mock_convert:
            result = runner.invoke(
                cli,
                [
                    "extract",
                    "wfs",
                    self.URL,
                    "roads",
                    str(tmp_path / "out.parquet"),
                    "--bbox",
                    "-122.5,37.5,-122.0",
                ],
            )

        assert result.exit_code != 0
        assert "Invalid bbox format" in result.output
        assert "xmin,ymin,xmax,ymax" in result.output
        mock_convert.assert_not_called()


class TestExtractWfsConverterDispatch:
    """Which converter the CLI picks, and with which arguments."""

    URL = "https://geo.example.com/wfs"

    def test_single_layer_options_reach_the_converter(self, runner, tmp_path):
        output = tmp_path / "roads.parquet"
        with patch.object(wfs_module, "convert_wfs_to_geoparquet") as mock_convert:
            result = runner.invoke(
                cli,
                [
                    "extract",
                    "wfs",
                    self.URL,
                    " roads ",
                    str(output),
                    "--bbox",
                    "-122.5,37.5,-122.0,38.0",
                    "--limit",
                    "10",
                    "--workers",
                    "3",
                    "--compression",
                    "zstd",
                    "--sort-by",
                    "gid",
                ],
            )

        assert result.exit_code == 0, result.output
        kwargs = mock_convert.call_args.kwargs
        assert kwargs["service_url"] == self.URL
        # Whitespace around a typename is stripped.
        assert kwargs["typename"] == "roads"
        assert kwargs["output_file"] == str(output)
        assert kwargs["bbox"] == (-122.5, 37.5, -122.0, 38.0)
        assert kwargs["limit"] == 10
        assert kwargs["max_workers"] == 3
        assert kwargs["compression"] == "ZSTD"
        assert kwargs["sort_by"] == "gid"
        assert kwargs["version"] == "1.1.0"

    def test_multi_layer_dispatches_to_the_directory_converter(self, runner, tmp_path):
        output_dir = tmp_path / "layers"
        with (
            patch.object(wfs_module, "convert_wfs_layers_to_directory") as mock_dir,
            patch.object(wfs_module, "convert_wfs_to_geoparquet") as mock_single,
        ):
            result = runner.invoke(
                cli,
                [
                    "extract",
                    "wfs",
                    self.URL,
                    "roads, buildings",
                    str(output_dir),
                    "--parallel-layers",
                    "2",
                ],
            )

        assert result.exit_code == 0, result.output
        mock_single.assert_not_called()
        kwargs = mock_dir.call_args.kwargs
        assert kwargs["typenames"] == ["roads", "buildings"]
        assert kwargs["output_dir"] == str(output_dir)
        assert kwargs["parallel_layers"] == 2

    def test_converter_wfs_error_becomes_click_exception(self, runner, tmp_path):
        with patch.object(
            wfs_module,
            "convert_wfs_to_geoparquet",
            side_effect=wfs_module.WFSError("Layer 'roads' not found"),
        ):
            result = runner.invoke(
                cli, ["extract", "wfs", self.URL, "roads", str(tmp_path / "out.parquet")]
            )

        assert result.exit_code != 0
        assert "Layer 'roads' not found" in result.output
        assert "Traceback" not in result.output


class TestExtractCartoCliLayer:
    """``gpio extract carto`` option marshalling and error translation."""

    URL = "https://phl.carto.com/api/v2/sql"
    TABLE = "opa_properties_public"

    def test_options_reach_the_converter(self, runner, tmp_path):
        output = tmp_path / "out.parquet"
        with patch.object(carto_module, "convert_carto_to_geoparquet") as mock_convert:
            result = runner.invoke(
                cli,
                [
                    "extract",
                    "carto",
                    self.URL,
                    self.TABLE,
                    str(output),
                    "--where",
                    "market_value > 100",
                    "--bbox",
                    "-75.2, 39.9,-75.1,40.0",
                    "--limit",
                    "5",
                    "--timeout",
                    "60",
                    "--compression",
                    "zstd",
                    "--row-group-size-mb",
                    "64MB",
                    "--no-geometry",
                ],
            )

        assert result.exit_code == 0, result.output
        kwargs = mock_convert.call_args.kwargs
        assert kwargs["url"] == self.URL
        assert kwargs["table_name"] == self.TABLE
        assert kwargs["output_file"] == str(output)
        assert kwargs["where"] == "market_value > 100"
        assert kwargs["bbox"] == (-75.2, 39.9, -75.1, 40.0)
        assert kwargs["limit"] == 5
        # --timeout is an int option but core takes seconds as a float.
        assert kwargs["timeout"] == 60.0
        assert isinstance(kwargs["timeout"], float)
        assert kwargs["compression"] == "ZSTD"
        assert kwargs["row_group_size_mb"] == 64
        assert kwargs["geometry"] is False

    def test_bbox_defaults_to_none(self, runner, tmp_path):
        output = tmp_path / "out.parquet"
        with patch.object(carto_module, "convert_carto_to_geoparquet") as mock_convert:
            result = runner.invoke(cli, ["extract", "carto", self.URL, self.TABLE, str(output)])

        assert result.exit_code == 0, result.output
        kwargs = mock_convert.call_args.kwargs
        assert kwargs["bbox"] is None
        # Auto-detect is the default: neither --geometry nor --no-geometry.
        assert kwargs["geometry"] is None

    def test_invalid_bbox_is_rejected(self, runner, tmp_path):
        output = tmp_path / "out.parquet"
        with patch.object(carto_module, "convert_carto_to_geoparquet") as mock_convert:
            result = runner.invoke(
                cli,
                ["extract", "carto", self.URL, self.TABLE, str(output), "--bbox", "-75.2,39.9"],
            )

        assert result.exit_code != 0
        assert "Invalid bbox format" in result.output
        assert "xmin,ymin,xmax,ymax" in result.output
        mock_convert.assert_not_called()

    def test_non_parquet_output_is_rejected(self, runner, tmp_path):
        output = tmp_path / "out.csv"
        with patch.object(carto_module, "convert_carto_to_geoparquet") as mock_convert:
            result = runner.invoke(cli, ["extract", "carto", self.URL, self.TABLE, str(output)])

        assert result.exit_code != 0
        assert ".parquet extension" in result.output
        mock_convert.assert_not_called()

    def test_carto_error_becomes_click_exception(self, runner, tmp_path):
        output = tmp_path / "out.parquet"
        with patch.object(
            carto_module,
            "convert_carto_to_geoparquet",
            side_effect=carto_module.CartoError("relation does not exist"),
        ):
            result = runner.invoke(cli, ["extract", "carto", self.URL, self.TABLE, str(output)])

        assert result.exit_code != 0
        assert "relation does not exist" in result.output
        assert "Traceback" not in result.output
