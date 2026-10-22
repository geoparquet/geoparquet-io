"""Tests for doc_sync.py - CLAUDE.md and skill section generation."""

import importlib
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]

# Repo tooling checks: these exercise the doc_sync script that the CI lint job
# already runs via pre-commit, several of them through subprocesses, so they
# live in the meta lane instead of the fast suite.
pytestmark = pytest.mark.meta

# Project root for import resolution
PROJECT_ROOT = Path(__file__).parent.parent
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "doc_sync.py"


def _load_module():
    """Load the generation module using importlib to avoid stale cache issues."""
    spec = importlib.util.spec_from_file_location("doc_sync", str(SCRIPT_PATH))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestScriptExists:
    """Verify script infrastructure."""

    def test_script_exists(self):
        """Verify generation script exists."""
        assert SCRIPT_PATH.exists(), f"Expected script at {SCRIPT_PATH}"

    def test_script_runs_without_update(self):
        """Verify script runs in default (diff) mode without error."""
        result = subprocess.run(
            [sys.executable, str(SCRIPT_PATH)],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
        )
        # Default mode should always return 0 (never 1)
        assert result.returncode == 0, (
            f"Script error (rc={result.returncode}):\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )

    def test_script_check_mode(self):
        """Verify --check flag works."""
        result = subprocess.run(
            [sys.executable, str(SCRIPT_PATH), "--check"],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
        )
        # Exit 0 if current, 1 if outdated
        assert result.returncode in [0, 1], (
            f"Script error (rc={result.returncode}):\n{result.stderr}"
        )

    def test_script_claude_only_mode(self):
        """Verify --claude flag works."""
        result = subprocess.run(
            [sys.executable, str(SCRIPT_PATH), "--claude"],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
        )
        assert result.returncode == 0, f"Script error (rc={result.returncode}):\n{result.stderr}"
        assert "CLAUDE.md" in result.stdout

    def test_script_skill_only_mode(self):
        """Verify --skill flag works."""
        result = subprocess.run(
            [sys.executable, str(SCRIPT_PATH), "--skill"],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
        )
        assert result.returncode == 0, f"Script error (rc={result.returncode}):\n{result.stderr}"
        assert "skill" in result.stdout.lower()

    def test_update_and_check_mutually_exclusive(self):
        """Verify --update and --check cannot be used together."""
        result = subprocess.run(
            [sys.executable, str(SCRIPT_PATH), "--update", "--check"],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
        )
        assert result.returncode != 0
        assert "not allowed" in result.stderr.lower() or "error" in result.stderr.lower()


class TestCliCommandGeneration:
    """Unit tests for CLI command generation."""

    @pytest.fixture(autouse=True)
    def _import_module(self):
        """Import the generation module for unit tests."""
        self.mod = _load_module()

    def test_get_cli_commands_returns_dict(self):
        """Test CLI introspection returns a dict of commands."""
        commands = self.mod.get_cli_commands()
        assert isinstance(commands, dict)
        assert len(commands) > 0

    def test_get_cli_commands_known_groups(self):
        """Test that known command groups are present."""
        commands = self.mod.get_cli_commands()
        expected = ["add", "convert", "inspect", "extract", "partition", "sort", "check", "publish"]
        for group in expected:
            assert group in commands, f"Expected command group '{group}' not found"

    def test_get_cli_commands_has_subcommands(self):
        """Test that groups have subcommands."""
        commands = self.mod.get_cli_commands()
        # 'add' group should have subcommands
        assert len(commands["add"]["subcommands"]) > 0
        assert "bbox" in commands["add"]["subcommands"]

    def test_get_cli_commands_has_help_text(self):
        """Test that commands have help text."""
        commands = self.mod.get_cli_commands()
        for name, info in commands.items():
            assert "help" in info, f"Command '{name}' missing help key"

    def test_generate_cli_section_format(self):
        """Test CLI section has correct markdown format."""
        section = self.mod.generate_cli_section()
        assert "<!-- BEGIN GENERATED: cli-commands -->" in section
        assert "<!-- END GENERATED: cli-commands -->" in section
        assert "| Command Group |" in section
        assert "| Subcommands |" in section

    def test_generate_cli_section_contains_commands(self):
        """Test CLI section contains actual command names."""
        section = self.mod.generate_cli_section()
        assert "`gpio add`" in section
        assert "`gpio convert`" in section
        assert "`gpio inspect`" in section


class TestMarkerGeneration:
    """Unit tests for test marker generation."""

    @pytest.fixture(autouse=True)
    def _import_module(self):
        """Import the generation module for unit tests."""
        self.mod = _load_module()

    def test_get_test_markers_returns_list(self):
        """Test marker parsing returns a list."""
        markers = self.mod.get_test_markers(PROJECT_ROOT / "pyproject.toml")
        assert isinstance(markers, list)
        assert len(markers) > 0

    def test_get_test_markers_known_markers(self):
        """Test that known markers are present."""
        markers = self.mod.get_test_markers(PROJECT_ROOT / "pyproject.toml")
        marker_names = [m["name"] for m in markers]
        assert "slow" in marker_names
        assert "network" in marker_names

    def test_get_test_markers_have_descriptions(self):
        """Test that markers have descriptions."""
        markers = self.mod.get_test_markers(PROJECT_ROOT / "pyproject.toml")
        for marker in markers:
            assert "name" in marker
            assert "description" in marker
            assert len(marker["description"]) > 0

    def test_generate_markers_section_format(self):
        """Test markers section has correct markdown format."""
        section = self.mod.generate_markers_section(PROJECT_ROOT / "pyproject.toml")
        assert "<!-- BEGIN GENERATED: test-markers -->" in section
        assert "<!-- END GENERATED: test-markers -->" in section
        assert "| Marker |" in section
        assert "@pytest.mark." in section


class TestSectionUpdate:
    """Unit tests for section replacement logic."""

    @pytest.fixture(autouse=True)
    def _import_module(self):
        """Import the generation module for unit tests."""
        self.mod = _load_module()

    def test_update_existing_section(self):
        """Test replacing an existing generated section."""
        content = """# Header
<!-- BEGIN GENERATED: test -->
old content
<!-- END GENERATED: test -->
# Footer"""

        new_section = """<!-- BEGIN GENERATED: test -->
new content
<!-- END GENERATED: test -->"""

        result = self.mod.update_section(content, "test", new_section)
        assert "new content" in result
        assert "old content" not in result
        assert "# Header" in result
        assert "# Footer" in result

    def test_update_preserves_surrounding_content(self):
        """Test that content before and after is preserved."""
        content = """Line 1
Line 2
<!-- BEGIN GENERATED: foo -->
old stuff
<!-- END GENERATED: foo -->
Line 3
Line 4"""

        new_section = """<!-- BEGIN GENERATED: foo -->
new stuff
<!-- END GENERATED: foo -->"""

        result = self.mod.update_section(content, "foo", new_section)
        assert "Line 1" in result
        assert "Line 2" in result
        assert "Line 3" in result
        assert "Line 4" in result
        assert "new stuff" in result

    def test_update_nonexistent_section_returns_unchanged(self):
        """Test that updating a missing section returns content unchanged."""
        content = "# No generated sections here"
        new_section = """<!-- BEGIN GENERATED: missing -->
stuff
<!-- END GENERATED: missing -->"""

        result = self.mod.update_section(content, "missing", new_section)
        assert result == content


class TestUpdateMode:
    """Tests for --update mode on CLAUDE.md."""

    @pytest.fixture(autouse=True)
    def _import_module(self):
        self.mod = _load_module()

    def test_claude_md_has_generated_markers(self):
        """Test that CLAUDE.md contains the generated section markers."""
        claude_md = PROJECT_ROOT / "CLAUDE.md"
        content = claude_md.read_text(encoding="utf-8")
        for section_name in ["cli-commands", "test-markers"]:
            assert f"<!-- BEGIN GENERATED: {section_name} -->" in content, (
                f"CLAUDE.md missing BEGIN marker for '{section_name}'"
            )
            assert f"<!-- END GENERATED: {section_name} -->" in content, (
                f"CLAUDE.md missing END marker for '{section_name}'"
            )

    def test_update_produces_valid_markdown(self):
        """Test that updated CLAUDE.md still has valid structure."""
        claude_md = PROJECT_ROOT / "CLAUDE.md"
        content = claude_md.read_text(encoding="utf-8")

        # Generate all sections and apply
        sections = {
            "cli-commands": self.mod.generate_cli_section(),
            "test-markers": self.mod.generate_markers_section(PROJECT_ROOT / "pyproject.toml"),
        }

        updated = content
        for name, section_content in sections.items():
            updated = self.mod.update_section(updated, name, section_content)

        # Should still have the main title
        assert "# Claude Code Instructions for geoparquet-io" in updated
        # Should have both generated sections
        for name in sections:
            assert f"<!-- BEGIN GENERATED: {name} -->" in updated
            assert f"<!-- END GENERATED: {name} -->" in updated

    def test_update_is_idempotent(self):
        """Test that running update twice produces identical output."""
        claude_md = PROJECT_ROOT / "CLAUDE.md"
        content = claude_md.read_text(encoding="utf-8")

        sections = {
            "cli-commands": self.mod.generate_cli_section(),
            "test-markers": self.mod.generate_markers_section(PROJECT_ROOT / "pyproject.toml"),
        }

        # First pass
        first_pass = content
        for name, section_content in sections.items():
            first_pass = self.mod.update_section(first_pass, name, section_content)

        # Second pass (regenerate sections and apply again)
        sections2 = {
            "cli-commands": self.mod.generate_cli_section(),
            "test-markers": self.mod.generate_markers_section(PROJECT_ROOT / "pyproject.toml"),
        }

        second_pass = first_pass
        for name, section_content in sections2.items():
            second_pass = self.mod.update_section(second_pass, name, section_content)

        assert first_pass == second_pass, "Update is not idempotent"


class TestSkillGeneration:
    """Tests for skill section generation."""

    @pytest.fixture(autouse=True)
    def _import_module(self):
        self.mod = _load_module()

    def test_skill_file_exists(self):
        """Test that skill file exists."""
        skill_path = PROJECT_ROOT / "geoparquet_io" / "skills" / "geoparquet.md"
        assert skill_path.exists(), f"Expected skill file at {skill_path}"

    def test_skill_has_generated_sections(self):
        """Test that skill file has generated section markers."""
        skill_path = PROJECT_ROOT / "geoparquet_io" / "skills" / "geoparquet.md"
        content = skill_path.read_text(encoding="utf-8")
        for section_name in [
            "skill-commands",
            "compression-options",
            "inspect-commands",
            "check-commands",
        ]:
            assert f"<!-- BEGIN GENERATED: {section_name} -->" in content, (
                f"Skill missing BEGIN marker for '{section_name}'"
            )
            assert f"<!-- END GENERATED: {section_name} -->" in content, (
                f"Skill missing END marker for '{section_name}'"
            )

    def test_generate_skill_commands_table(self):
        """Test skill commands table generation."""
        section = self.mod.generate_skill_commands_table()
        assert "<!-- BEGIN GENERATED: skill-commands -->" in section
        assert "<!-- END GENERATED: skill-commands -->" in section
        assert "| Command |" in section

    def test_generate_compression_options(self):
        """Test compression options generation."""
        section = self.mod.generate_compression_options()
        assert "<!-- BEGIN GENERATED: compression-options -->" in section
        assert "<!-- END GENERATED: compression-options -->" in section
        assert "zstd" in section

    def test_generate_inspection_commands(self):
        """Test inspection commands generation."""
        section = self.mod.generate_inspection_commands()
        assert "<!-- BEGIN GENERATED: inspect-commands -->" in section
        assert "<!-- END GENERATED: inspect-commands -->" in section
        assert "gpio inspect" in section

    def test_generate_check_commands(self):
        """Test check commands generation."""
        section = self.mod.generate_check_commands()
        assert "<!-- BEGIN GENERATED: check-commands -->" in section
        assert "<!-- END GENERATED: check-commands -->" in section
        assert "gpio check" in section


class TestChangelogSingleSource:
    """docs/CHANGELOG.md includes the root file instead of holding a copy."""

    def test_docs_changelog_is_include_stub(self):
        """The docs page is only a snippets include of the root CHANGELOG.md."""
        stub = (PROJECT_ROOT / "docs" / "CHANGELOG.md").read_text(encoding="utf-8")
        lines = [line for line in stub.splitlines() if line.strip()]
        assert lines == ['--8<-- "CHANGELOG.md"'], stub
        assert "## v" not in stub

    def test_snippets_resolve_repo_root_before_docs(self):
        """Root must come first in base_path, or the stub would include itself."""
        config = tomllib.loads((PROJECT_ROOT / "zensical.toml").read_text(encoding="utf-8"))
        snippets = config["project"]["markdown_extensions"]["pymdownx.snippets"]
        base_path = snippets["base_path"]
        assert isinstance(base_path, list)
        assert base_path.index(".") < base_path.index("docs")

    def test_doc_sync_no_longer_copies_changelog(self):
        """The copy step is gone, so nothing regenerates docs/CHANGELOG.md."""
        mod = _load_module()
        assert not hasattr(mod, "sync_changelog")
