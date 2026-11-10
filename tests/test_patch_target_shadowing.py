"""The CLI package must support dotted patch targets without shadowing."""


def test_cli_package_supports_dotted_patch_targets():
    import importlib
    from unittest.mock import patch

    module = importlib.import_module("geoparquet_io.cli.commands.add")
    with patch("geoparquet_io.cli.commands.add.click") as replacement:
        assert module.click is replacement
