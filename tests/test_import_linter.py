"""Tests for import-linter architecture contracts.

Tests are split into two categories:
- Unit tests that verify configuration via the Python API (fast, contribute to coverage)
- Integration tests that run lint-imports as a subprocess (slow, marked accordingly)
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
from importlinter import api as importlinter_api


class TestImportLinterConfiguration:
    """Unit tests: verify import-linter contracts are correctly configured."""

    @pytest.fixture()
    def config(self):
        """Load import-linter configuration from pyproject.toml."""
        return importlinter_api.read_configuration()

    def test_root_package_is_geoparquet_io(self, config):
        """Verify root_package is set to geoparquet_io."""
        session_options = config["session_options"]
        assert session_options["root_packages"] == ["geoparquet_io"]

    def test_include_external_packages_enabled(self, config):
        """External packages must be included to catch click imports."""
        session_options = config["session_options"]
        assert session_options.get("include_external_packages") == "True"

    def test_has_five_contracts(self, config):
        """Exactly five architecture contracts should be defined."""
        contracts_options = config["contracts_options"]
        assert len(contracts_options) == 5, (
            f"Expected 5 contracts, found {len(contracts_options)}: "
            f"{[c.get('name', c.get('id', '?')) for c in contracts_options]}"
        )

    def test_core_no_click_contract_configured(self, config):
        """Verify core-no-click contract is correctly configured."""
        contracts = {c["id"]: c for c in config["contracts_options"]}
        assert "core-no-click" in contracts, "Missing core-no-click contract"

        contract = contracts["core-no-click"]
        assert contract["type"] == "forbidden"
        assert "geoparquet_io.core" in contract["source_modules"]
        assert "click" in contract["forbidden_modules"]

    def test_api_no_cli_contract_configured(self, config):
        """Verify api-no-cli contract is correctly configured."""
        contracts = {c["id"]: c for c in config["contracts_options"]}
        assert "api-no-cli" in contracts, "Missing api-no-cli contract"

        contract = contracts["api-no-cli"]
        assert contract["type"] == "forbidden"
        assert "geoparquet_io.api" in contract["source_modules"]
        assert "geoparquet_io.cli" in contract["forbidden_modules"]

    def test_commands_no_cli_main_contract_configured(self, config):
        """The command-group layer must not import back into cli.main."""
        contracts = {c["id"]: c for c in config["contracts_options"]}
        assert "commands-no-cli-main" in contracts, "Missing commands-no-cli-main contract"

        contract = contracts["commands-no-cli-main"]
        assert contract["type"] == "forbidden"
        assert "geoparquet_io.cli.commands" in contract["source_modules"]
        assert "geoparquet_io.cli.main" in contract["forbidden_modules"]

    def test_commands_independent_contract_covers_every_group(self, config):
        """Every group module must be listed, or a pair could import each other unchecked."""
        contracts = {c["id"]: c for c in config["contracts_options"]}
        assert "commands-independent" in contracts, "Missing commands-independent contract"

        contract = contracts["commands-independent"]
        assert contract["type"] == "independence"

        commands_dir = Path(__file__).resolve().parents[1] / "geoparquet_io" / "cli" / "commands"
        on_disk = {
            f"geoparquet_io.cli.commands.{path.stem}"
            for path in commands_dir.glob("*.py")
            if path.stem != "__init__"
        }
        assert set(contract["modules"]) == on_disk, (
            "commands-independent must list exactly the group modules on disk; "
            f"missing: {sorted(on_disk - set(contract['modules']))}, "
            f"stale: {sorted(set(contract['modules']) - on_disk)}"
        )

    def test_core_no_click_ignore_imports_minimal(self, config):
        """Click has been removed from core - only transitive ignores should remain."""
        contracts = {c["id"]: c for c in config["contracts_options"]}
        contract = contracts["core-no-click"]
        ignore_imports = contract.get("ignore_imports", [])
        # After PR #364, Click was removed from core entirely.
        # Only transitive imports (e.g., benchmark_suite -> package root) may remain.
        # No direct core.* -> click ignores should be needed anymore.
        assert not any("-> click" in imp for imp in ignore_imports), (
            f"Direct click ignores should no longer be needed, found: {ignore_imports}"
        )

    def test_common_split_layers_contract_configured(self, config):
        """The split's layering (#1083): common on top, the owners in order below.

        The type-code tables' owner, ``geo_metadata``, must sit below both of
        its consumers, and the two geo-block builders must be independent.
        """
        contracts = {c["id"]: c for c in config["contracts_options"]}
        assert "core-common-split-layers" in contracts, "Missing core-common-split-layers"

        contract = contracts["core-common-split-layers"]
        assert contract["type"] == "layers"
        layers = contract["layers"]
        assert layers[0] == "geoparquet_io.core.common"
        assert layers[1] == "geoparquet_io.core.write_funnels"

        def index_of(module: str) -> int:
            for i, layer in enumerate(layers):
                if module in {m.strip() for m in layer.replace(":", "|").split("|")}:
                    return i
            raise AssertionError(f"{module} is not in the layers contract")

        builders = index_of("geoparquet_io.core.arrow_geo_metadata")
        assert builders == index_of("geoparquet_io.core.derive_geo_from_file")
        assert "|" in layers[builders], "the two geo-block builders must be independent"
        assert index_of("geoparquet_io.core.geo_metadata") > builders
        assert index_of("geoparquet_io.core.duckdb_utils") > index_of(
            "geoparquet_io.core.geo_metadata"
        )

    def test_common_split_layers_ignores_only_residue_back_edges(self, config):
        """Every ignored edge is a deferred import *into* common.py, and a stale
        one fails the build: the list is meant to shrink as stage C empties
        common.py, never to grow.
        """
        contracts = {c["id"]: c for c in config["contracts_options"]}
        contract = contracts["core-common-split-layers"]
        assert contract["unmatched_ignore_imports_alerting"] == "error"
        for edge in contract["ignore_imports"]:
            importer, imported = (side.strip() for side in edge.split("->"))
            assert imported == "geoparquet_io.core.common", (
                f"{edge}: only back-edges into common.py may be ignored; a back-edge "
                "between two owner modules is a layering bug, not a residue"
            )
            assert importer != "geoparquet_io.core.common"

    def test_api_no_cli_has_no_ignore_imports(self, config):
        """api-no-cli should have no ignored violations (clean contract)."""
        contracts = {c["id"]: c for c in config["contracts_options"]}
        contract = contracts["api-no-cli"]
        ignore_imports = contract.get("ignore_imports", [])
        assert len(ignore_imports) == 0, (
            f"api-no-cli should not need any ignore_imports, found: {ignore_imports}"
        )


@pytest.mark.slow
class TestImportLinterIntegration:
    """Integration tests: run lint-imports subprocess to verify contracts pass."""

    _SUBPROCESS_TIMEOUT = 120  # seconds

    def test_all_contracts_pass(self):
        """Verify all import-linter contracts pass (runs lint-imports)."""
        result = subprocess.run(
            ["uv", "run", "lint-imports"],
            capture_output=True,
            text=True,
            timeout=self._SUBPROCESS_TIMEOUT,
        )
        assert result.returncode == 0, f"Import contracts failed:\n{result.stdout}\n{result.stderr}"
        assert "KEPT" in result.stdout
        # The summary line always contains "0 broken" on success;
        # a real failure says "N broken" where N > 0 and also "BROKEN" on individual lines.
        assert "BROKEN" not in result.stdout
