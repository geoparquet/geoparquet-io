"""Tests for commitizen configuration and conventional commit validation."""

import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

# Repo tooling checks: these spawn `uv run cz` subprocesses, the fast suite's
# main source of contention flakes, so they run in the meta lane instead.
# Unlike most of the lane they are NOT redundant with the lint job: the
# commitizen hook is `stages: [commit-msg]`, which `pre-commit run --all-files`
# never invokes, so the meta lane is the only place this is checked.
pytestmark = pytest.mark.meta

# Root of the repository - resolve once, use everywhere.
# This avoids dependence on the working directory at test time.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_PYPROJECT = _REPO_ROOT / "pyproject.toml"


def _load_pyproject() -> dict:
    """Load and parse pyproject.toml from the repository root."""
    with open(_PYPROJECT, "rb") as f:
        return tomllib.load(f)


def _cz_check(message: str) -> subprocess.CompletedProcess:
    """Run ``cz check --message`` and return the CompletedProcess."""
    return subprocess.run(
        [sys.executable, "-m", "commitizen.cli", "check", "--message", message],
        capture_output=True,
        text=True,
        timeout=30,
        cwd=_REPO_ROOT,
    )


# ---------------------------------------------------------------------------
# Configuration tests
# ---------------------------------------------------------------------------


class TestCommitizenConfig:
    """Test commitizen is properly configured in pyproject.toml."""

    def test_commitizen_installed(self):
        """Verify the commitizen package is installed and importable."""
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "from importlib.metadata import version; print(version('commitizen'))",
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
        assert result.returncode == 0, f"commitizen not installed: {result.stderr}"
        assert result.stdout.strip(), "commitizen version should be non-empty"

    def test_commitizen_config_exists(self):
        """Verify [tool.commitizen] section exists with correct adapter."""
        config = _load_pyproject()
        assert "commitizen" in config.get("tool", {}), (
            "[tool.commitizen] section missing from pyproject.toml"
        )
        cz_config = config["tool"]["commitizen"]
        assert cz_config.get("name") == "cz_conventional_commits"

    def test_commitizen_config_has_required_fields(self):
        """Verify all required commitizen config fields are present and correct."""
        cz_config = _load_pyproject()["tool"]["commitizen"]
        assert cz_config.get("tag_format") == "v$version"
        assert cz_config.get("changelog_file") == "CHANGELOG.md"
        # False on purpose: scripts/release_notes.py writes the changelog from
        # merged pull requests, and cz regenerating it from commit subjects
        # would overwrite that reviewed section with a second, unreviewed one.
        assert cz_config.get("update_changelog_on_bump") is False
        assert cz_config.get("major_version_zero") is True
        assert "version" in cz_config
        assert "version_files" in cz_config

    def test_version_matches_pyproject(self):
        """Verify commitizen version matches [project] version."""
        config = _load_pyproject()
        project_version = config["project"]["version"]
        cz_version = config["tool"]["commitizen"]["version"]
        assert project_version == cz_version, (
            f"Version mismatch: [project].version={project_version}, "
            f"[tool.commitizen].version={cz_version}"
        )

    def test_commitizen_in_dev_dependencies(self):
        """Verify commitizen is listed in [project.optional-dependencies] dev."""
        config = _load_pyproject()
        dev_deps = config.get("project", {}).get("optional-dependencies", {}).get("dev", [])
        assert any("commitizen" in dep for dep in dev_deps), (
            "commitizen not found in [project.optional-dependencies] dev"
        )


# ---------------------------------------------------------------------------
# Commit message validation -- valid messages
# ---------------------------------------------------------------------------


class TestValidCommitMessages:
    """Verify commitizen accepts well-formed conventional commit messages."""

    @pytest.mark.parametrize(
        "message",
        [
            "feat(cli): add new command",
            "fix: resolve null pointer issue",
            "docs: update Python API examples",
            "style: format code with ruff",
            "refactor(core): extract geometry helpers",
            "perf(api): speed up parquet reads",
            "test(api): add coverage for partition module",
            "chore(ci): add security scanning",
            "build(deps): bump pyarrow to v18",
            "ci: add GitHub Actions workflow",
            "revert: undo accidental breaking change",
        ],
        ids=[
            "feat-with-scope",
            "fix-no-scope",
            "docs",
            "style",
            "refactor-with-scope",
            "perf-with-scope",
            "test-with-scope",
            "chore-with-scope",
            "build-with-scope",
            "ci-no-scope",
            "revert",
        ],
    )
    def test_valid_message_accepted(self, message):
        """Each conventional commit type should be accepted by cz check."""
        result = _cz_check(message)
        assert result.returncode == 0, (
            f"cz check rejected valid message: {message!r}\nstderr: {result.stderr}"
        )

    @pytest.mark.parametrize(
        "message",
        [
            "feat!: remove deprecated API",
            "feat(api)!: rename convert method",
        ],
        ids=["breaking-no-scope", "breaking-with-scope"],
    )
    def test_breaking_change_accepted(self, message):
        """Breaking-change markers (!) should be accepted."""
        result = _cz_check(message)
        assert result.returncode == 0, (
            f"cz check rejected breaking change: {message!r}\nstderr: {result.stderr}"
        )

    def test_multiline_message_accepted(self):
        """A multiline commit message with body should be accepted."""
        message = "feat(cli): add new command\n\nThis adds a new command to the CLI."
        result = _cz_check(message)
        assert result.returncode == 0, (
            f"cz check rejected multiline message\nstderr: {result.stderr}"
        )


# ---------------------------------------------------------------------------
# Commit message validation -- invalid messages
# ---------------------------------------------------------------------------


class TestInvalidCommitMessages:
    """Verify commitizen rejects non-conventional commit messages."""

    @pytest.mark.parametrize(
        "message",
        [
            "Updated some stuff",
            "Add new feature",
            "fixed the bug",
            "WIP",
            "oops",
            "",
        ],
        ids=[
            "past-tense-no-type",
            "imperative-no-type",
            "lowercase-past-no-type",
            "wip",
            "single-word",
            "empty-message",
        ],
    )
    def test_invalid_message_rejected(self, message):
        """Non-conventional messages should be rejected by cz check."""
        result = _cz_check(message)
        assert result.returncode != 0, f"cz check incorrectly accepted invalid message: {message!r}"


# ---------------------------------------------------------------------------
# Pre-commit hook configuration
# ---------------------------------------------------------------------------


class TestPreCommitConfig:
    """Verify the pre-commit configuration is correct."""

    def test_commitizen_hook_in_pre_commit_config(self):
        """Verify commitizen hook is present in .pre-commit-config.yaml."""
        pre_commit_cfg = _REPO_ROOT / ".pre-commit-config.yaml"
        assert pre_commit_cfg.exists(), ".pre-commit-config.yaml not found"

        import yaml  # safe_load is available via pyyaml, a commitizen dep

        with open(pre_commit_cfg, encoding="utf-8") as f:
            config = yaml.safe_load(f)

        repos = config.get("repos", [])
        commitizen_repos = [
            r for r in repos if isinstance(r.get("repo"), str) and "commitizen" in r["repo"]
        ]
        assert commitizen_repos, "No commitizen repo found in .pre-commit-config.yaml"
        hooks = commitizen_repos[0].get("hooks", [])
        hook_ids = [h["id"] for h in hooks]
        assert "commitizen" in hook_ids
        # The hook should run at the commit-msg stage
        cz_hook = next(h for h in hooks if h["id"] == "commitizen")
        assert "commit-msg" in cz_hook.get("stages", [])

    def test_commitizen_hook_runs_at_commit_msg_stage(self):
        """Verify commitizen hook runs at commit-msg stage (replaces legacy check-commit-msg)."""
        pre_commit_cfg = _REPO_ROOT / ".pre-commit-config.yaml"

        import yaml

        with open(pre_commit_cfg, encoding="utf-8") as f:
            config = yaml.safe_load(f)

        repos = config.get("repos", [])
        commitizen_repos = [
            r for r in repos if isinstance(r.get("repo"), str) and "commitizen" in r["repo"]
        ]
        assert commitizen_repos, "No commitizen repo found in .pre-commit-config.yaml"

        hooks = commitizen_repos[0].get("hooks", [])
        cz_hook = next((h for h in hooks if h["id"] == "commitizen"), None)
        assert cz_hook is not None, "commitizen hook not found"
        assert "commit-msg" in cz_hook.get("stages", []), (
            "commitizen hook should run at commit-msg stage"
        )


# ---------------------------------------------------------------------------
# Changelog dry-run
# ---------------------------------------------------------------------------


class TestChangelogGeneration:
    """Test changelog generation capability."""

    def test_changelog_dry_run(self):
        """Test changelog generation in dry-run mode does not crash from misconfiguration."""
        result = subprocess.run(
            [sys.executable, "-m", "commitizen.cli", "changelog", "--dry-run"],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=_REPO_ROOT,
        )
        # If the command succeeds, great.  If it fails, it may be because
        # there are no conventional commits yet -- that is acceptable.
        # What is NOT acceptable is a crash from bad configuration.
        if result.returncode != 0:
            stderr_lower = result.stderr.lower()
            # These indicate real configuration problems:
            config_errors = [
                "no commitizen configuration",
                "configuration is not",
                "toml",
                "name.*not found",
            ]
            for pattern in config_errors:
                assert pattern not in stderr_lower, (
                    f"Changelog dry-run failed with configuration error: {result.stderr}"
                )
