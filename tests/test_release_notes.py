"""Tests for release_notes.py - changelog section generation from merged PRs."""

import importlib.util
import sys
from pathlib import Path

import pytest

# Repo tooling checks: this script runs once per release, by hand, and its only
# network call is shelled out to `gh`. It belongs in the meta lane, beside the
# other script tests, not in the fast suite.
pytestmark = pytest.mark.meta

PROJECT_ROOT = Path(__file__).parent.parent
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "release_notes.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("release_notes", str(SCRIPT_PATH))
    mod = importlib.util.module_from_spec(spec)
    # Registered before exec: @dataclass resolves annotations through
    # sys.modules, and raises AttributeError if the module is not there yet.
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


rn = _load_module()

SAMPLE = """## What's Changed
* feat(sort): add Sort-Tile-Recursive ordering by @oakhill87 in https://github.com/geoparquet/geoparquet-io/pull/766
* fix(api)!: align CLI and Python API defaults by @cholmes in https://github.com/geoparquet/geoparquet-io/pull/661
* fix(metadata): judge geometries against the whole file (#721) by @cholmes in https://github.com/geoparquet/geoparquet-io/pull/770
* perf(add): lean on DuckDB SPATIAL_JOIN by @nlebovits in https://github.com/geoparquet/geoparquet-io/pull/540
* docs(api): document geoparquet_version by @oakhill87 in https://github.com/geoparquet/geoparquet-io/pull/510
* test(e2e): ten user journeys by @nlebovits in https://github.com/geoparquet/geoparquet-io/pull/723
* ci: bump actions/checkout by @dependabot[bot] in https://github.com/geoparquet/geoparquet-io/pull/513
* build(deps): bump pyarrow from 24.0.0 to 25.0.1 by @dependabot[bot] in https://github.com/geoparquet/geoparquet-io/pull/769
* Time to relax duckdb pin? by @nlebovits in https://github.com/geoparquet/geoparquet-io/pull/534

## New Contributors
* @oakhill87 made their first contribution in https://github.com/geoparquet/geoparquet-io/pull/510

**Full Changelog**: https://github.com/geoparquet/geoparquet-io/compare/v1.3.0...v1.4.0
"""


class TestParse:
    def test_reads_every_entry(self):
        notes = rn.parse(SAMPLE)
        assert len(notes.entries) == 9

    def test_splits_title_author_and_number(self):
        entry = rn.parse(SAMPLE).entries[0]
        assert entry.title == "feat(sort): add Sort-Tile-Recursive ordering"
        assert entry.author == "oakhill87"
        assert entry.number == 766

    def test_keeps_new_contributors(self):
        notes = rn.parse(SAMPLE)
        assert notes.new_contributors == [
            "* @oakhill87 made their first contribution in "
            "https://github.com/geoparquet/geoparquet-io/pull/510"
        ]

    def test_keeps_full_changelog_link(self):
        assert "compare/v1.3.0...v1.4.0" in rn.parse(SAMPLE).full_changelog

    def test_new_contributors_are_not_entries(self):
        numbers = [e.number for e in rn.parse(SAMPLE).entries]
        assert numbers.count(510) == 1


class TestClassify:
    @pytest.mark.parametrize(
        ("title", "author", "section"),
        [
            ("feat(sort): add STR ordering", "cholmes", "Added"),
            ("fix(api)!: align defaults", "cholmes", "Breaking"),
            ("feat!: drop python 3.9", "cholmes", "Breaking"),
            ("fix(metadata): judge geometries", "cholmes", "Fixed"),
            ("perf(add): lean on SPATIAL_JOIN", "cholmes", "Changed"),
            ("refactor(common): decompose helper", "cholmes", "Changed"),
            ("docs(api): document write", "cholmes", "Documentation"),
            ("test(e2e): ten journeys", "cholmes", "Internal"),
            ("ci: bump checkout", "dependabot[bot]", "Dependencies"),
            ("chore: guardrails", "cholmes", "Internal"),
            ("build(deps): bump pyarrow", "dependabot[bot]", "Dependencies"),
            ("build(deps-dev): bump pytest", "dependabot[bot]", "Dependencies"),
            ("build: retune the wheel", "cholmes", "Internal"),
            ("Time to relax duckdb pin?", "nlebovits", "Uncategorized"),
        ],
    )
    def test_section_for(self, title, author, section):
        assert rn.classify(title, author) == section

    def test_a_bot_ci_bump_is_a_dependency_not_internal(self):
        # ci: bumps from dependabot are action version updates, not repo chores.
        assert rn.classify("ci: bump actions/checkout", "dependabot[bot]") == "Dependencies"
        assert rn.classify("ci: harden the gates", "nlebovits") == "Internal"


class TestRender:
    def test_groups_entries_under_keep_a_changelog_headings(self):
        out = rn.render(rn.parse(SAMPLE), version="1.4.0", date="2026-08-30")
        assert "## v1.4.0 (2026-08-30)" in out
        for heading in ("### Breaking", "### Added", "### Changed", "### Fixed"):
            assert heading in out

    def test_sections_follow_the_fixed_order(self):
        out = rn.render(rn.parse(SAMPLE), version="1.4.0", date="2026-08-30")
        order = [s for s in rn.SECTIONS if f"### {s}" in out]
        positions = [out.index(f"### {s}") for s in order]
        assert positions == sorted(positions)

    def test_entry_links_author_and_pull_request(self):
        out = rn.render(rn.parse(SAMPLE), version="1.4.0", date="2026-08-30")
        assert (
            "- feat(sort): add Sort-Tile-Recursive ordering by "
            "[@oakhill87](https://github.com/oakhill87) in "
            "[#766](https://github.com/geoparquet/geoparquet-io/pull/766)"
        ) in out

    def test_linkifies_issue_references_inside_a_title(self):
        out = rn.render(rn.parse(SAMPLE), version="1.4.0", date="2026-08-30")
        assert "([#721](https://github.com/geoparquet/geoparquet-io/issues/721))" in out

    def test_carries_new_contributors_and_full_changelog(self):
        out = rn.render(rn.parse(SAMPLE), version="1.4.0", date="2026-08-30")
        assert "### New Contributors" in out
        assert "made their first contribution" in out
        assert "**Full Changelog**:" in out

    def test_summary_placeholder_prompts_for_the_highlights(self):
        out = rn.render(rn.parse(SAMPLE), version="1.4.0", date="2026-08-30")
        assert rn.SUMMARY_PLACEHOLDER in out
        assert out.index(rn.SUMMARY_PLACEHOLDER) < out.index("### Breaking")

    def test_a_given_summary_replaces_the_placeholder(self):
        out = rn.render(
            rn.parse(SAMPLE),
            version="1.4.0",
            date="2026-08-30",
            summary="Two paragraphs of highlights.",
        )
        assert "Two paragraphs of highlights." in out
        assert rn.SUMMARY_PLACEHOLDER not in out

    def test_empty_sections_are_omitted(self):
        out = rn.render(rn.parse(SAMPLE), version="1.4.0", date="2026-08-30")
        assert "### Removed" not in out


class TestSplice:
    def test_replaces_the_unreleased_heading(self):
        changelog = "# Changelog\n\nintro\n\n## Unreleased\n\nold prose\n\n## v1.3.0 (2026-06-11)\n\nolder\n"
        out = rn.splice(changelog, "## v1.4.0 (2026-08-30)\n\nnew section\n")
        assert "## Unreleased" not in out
        assert "old prose" not in out
        assert "## v1.4.0 (2026-08-30)" in out
        assert "## v1.3.0 (2026-06-11)" in out
        assert out.index("## v1.4.0") < out.index("## v1.3.0")

    def test_keeps_the_file_header(self):
        changelog = (
            "# Changelog\n\nintro\n\n## Unreleased\n\nold\n\n## v1.3.0 (2026-06-11)\n\nolder\n"
        )
        out = rn.splice(changelog, "## v1.4.0 (2026-08-30)\n\nnew\n")
        assert out.startswith("# Changelog\n\nintro\n")

    def test_inserts_above_the_newest_release_when_there_is_no_unreleased(self):
        changelog = "# Changelog\n\nintro\n\n## v1.3.0 (2026-06-11)\n\nolder\n"
        out = rn.splice(changelog, "## v1.4.0 (2026-08-30)\n\nnew\n")
        assert out.index("## v1.4.0") < out.index("## v1.3.0")

    def test_refuses_to_write_a_version_twice(self):
        changelog = "# Changelog\n\n## v1.4.0 (2026-08-30)\n\nalready here\n"
        with pytest.raises(rn.ReleaseNotesError, match="v1.4.0"):
            rn.splice(changelog, "## v1.4.0 (2026-08-30)\n\nnew\n")


class TestOverrides:
    def test_rewrites_a_title_by_pull_request_number(self):
        notes = rn.apply_overrides(rn.parse(SAMPLE), {"534": "chore(deps): relax the duckdb pin"})
        entry = next(e for e in notes.entries if e.number == 534)
        assert entry.title == "chore(deps): relax the duckdb pin"

    def test_a_rewritten_title_decides_the_section(self):
        notes = rn.apply_overrides(rn.parse(SAMPLE), {"534": "perf(convert): stream the read"})
        out = rn.render(notes, version="1.4.0", date="2026-08-30")
        assert "### Uncategorized" not in out
        assert "stream the read" in out.split("### Changed")[1]

    def test_keys_starting_with_underscore_are_comments(self):
        notes = rn.apply_overrides(rn.parse(SAMPLE), {"_note": "not a pull request"})
        assert len(notes.entries) == 9

    def test_an_override_for_an_absent_pull_request_is_reported_and_skipped(self, capsys):
        """Out-of-range overrides are a note, not a failure.

        The file is keyed by pull-request number across the whole project, so
        after the first generated release most of its entries belong to
        releases that already shipped. Raising on them made release one work
        and every release after it fail. The number still has to reach stderr:
        an override that matches nothing rewrites nothing, and a mistyped
        number is otherwise silent.
        """
        notes = rn.apply_overrides(rn.parse(SAMPLE), {"9999": "fix: nothing"})

        assert "9999" in capsys.readouterr().err
        assert len(notes.entries) == 9
        assert all(e.title != "fix: nothing" for e in notes.entries)

    def test_an_in_range_override_still_applies_alongside_an_absent_one(self, capsys):
        """One stale entry must not stop the entries that do match."""
        notes = rn.apply_overrides(
            rn.parse(SAMPLE),
            {"9999": "fix: nothing", "534": "chore(deps): relax the duckdb pin"},
        )

        assert "9999" in capsys.readouterr().err
        entry = next(e for e in notes.entries if e.number == 534)
        assert entry.title == "chore(deps): relax the duckdb pin"

    def test_the_shipped_overrides_file_parses(self):
        overrides = rn.load_overrides(rn.OVERRIDES_PATH)
        assert all(k.startswith("_") or k.isdigit() for k in overrides)


class TestContributorBrief:
    def test_lists_every_pull_request_a_new_contributor_wrote(self):
        brief = rn.contributor_brief(rn.parse(SAMPLE))
        assert brief["oakhill87"] == [
            (510, "docs(api): document geoparquet_version"),
            (766, "feat(sort): add Sort-Tile-Recursive ordering"),
        ]

    def test_covers_only_new_contributors(self):
        assert list(rn.contributor_brief(rn.parse(SAMPLE))) == ["oakhill87"]


class TestOmittedSections:
    def test_internal_and_dependency_work_is_not_listed(self):
        out = rn.render(rn.parse(SAMPLE), version="1.4.0", date="2026-08-30")
        assert "### Internal" not in out
        assert "### Dependencies" not in out
        assert "ten user journeys" not in out
        assert "bump pyarrow" not in out

    def test_a_note_counts_what_was_left_out(self):
        out = rn.render(rn.parse(SAMPLE), version="1.4.0", date="2026-08-30")
        assert "1 internal change and 2 dependency updates are not listed" in out

    def test_the_note_is_omitted_when_there_is_nothing_to_leave_out(self):
        notes = rn.Notes(
            entries=[rn.Entry(title="fix(x): y", author="a", number=1)],
            full_changelog="**Full Changelog**: https://example.com/compare",
        )
        out = rn.render(notes, version="1.4.0", date="2026-08-30")
        assert "are not listed" not in out

    def test_the_note_sits_with_the_full_changelog_link(self):
        out = rn.render(rn.parse(SAMPLE), version="1.4.0", date="2026-08-30")
        assert out.index("are not listed") > out.index("### New Contributors")

    def test_uncategorized_is_still_shown_so_triage_cannot_be_skipped(self):
        out = rn.render(rn.parse(SAMPLE), version="1.4.0", date="2026-08-30")
        assert "### Uncategorized" in out
