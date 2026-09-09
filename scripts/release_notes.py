#!/usr/bin/env python3
"""Build a CHANGELOG.md release section from the merged pull requests.

GitHub's own release-note generator already knows every pull request in a tag
range, who wrote it, and who contributed for the first time. This script asks
for that list, then groups it into Keep a Changelog sections by the
conventional-commit type each pull request title carries, so a release reads
the same way every time and nobody hand-writes 130 bullets.

    uv run python scripts/release_notes.py 1.4.0              # print the section
    uv run python scripts/release_notes.py 1.4.0 --write      # splice into CHANGELOG.md
    uv run python scripts/release_notes.py 1.4.0 --contributors   # who is new, and why

Not every contributor writes a conventional-commit title, and the pull request
keeps whatever it merged with. `release_title_overrides.json` rewrites a title
for the changelog only, which is what gives the entry a section.

`--write` replaces the `## Unreleased` heading and everything under it, so the
generated section is the only thing describing the release. It refuses to write
a version the changelog already carries.

The summary paragraphs are not generated. The section is written with a
placeholder where they belong, and the release skill (`.claude/skills/release/`)
fills it in and stops for human review before anything is tagged.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import date as date_cls
from pathlib import Path

REPO = "geoparquet/geoparquet-io"
CHANGELOG = Path(__file__).resolve().parent.parent / "CHANGELOG.md"
OVERRIDES_PATH = Path(__file__).resolve().parent / "release_title_overrides.json"

# Section order in the rendered output. The four a reader cares about come
# first; housekeeping sits below them, so scanning can stop at Documentation.
SECTIONS = (
    "Breaking",
    "Added",
    "Changed",
    "Fixed",
    "Documentation",
    "Internal",
    "Dependencies",
    "Uncategorized",
)

# Classified, counted, but not listed. Bot bumps and repo chores are not what a
# changelog is for, and GitHub's release page and the compare link below still
# carry every one of them.
OMITTED_SECTIONS = ("Internal", "Dependencies")

# Conventional-commit type -> section. Types absent here land in Internal;
# a title with no recognizable type lands in Uncategorized, which the review
# step exists to empty.
TYPE_SECTIONS = {
    "feat": "Added",
    "fix": "Fixed",
    "perf": "Changed",
    "refactor": "Changed",
    "revert": "Changed",
    "docs": "Documentation",
}

SUMMARY_PLACEHOLDER = "<!-- TODO: 2-4 paragraphs on the highlights of this release. -->"

NEW_CONTRIBUTOR_RE = re.compile(r"^[*-] @(?P<author>[\w.\[\]-]+) made their first")

# "* <title> by @<author> in <url>", the shape GitHub generates.
ENTRY_RE = re.compile(
    r"^\* (?P<title>.+?) by @(?P<author>[\w.\[\]-]+) in "
    r"https://github\.com/[\w.-]+/[\w.-]+/pull/(?P<number>\d+)\s*$"
)
# "feat(scope)!: subject" — the trailing "!" is what marks a breaking change.
PREFIX_RE = re.compile(r"^(?P<type>[a-z]+)(?:\((?P<scope>[^)]*)\))?(?P<bang>!)?:")
ISSUE_RE = re.compile(r"(?<![\w/])#(\d+)\b")
VERSION_HEADING_RE = re.compile(r"^## ", re.MULTILINE)


class ReleaseNotesError(RuntimeError):
    """The changelog or the generated notes are not in a shape we can use."""


@dataclass(frozen=True)
class Entry:
    title: str
    author: str
    number: int

    @property
    def url(self) -> str:
        return f"https://github.com/{REPO}/pull/{self.number}"


@dataclass
class Notes:
    entries: list[Entry] = field(default_factory=list)
    new_contributors: list[str] = field(default_factory=list)
    full_changelog: str = ""


def parse(body: str) -> Notes:
    """Split GitHub's generated notes into entries, contributors and the link."""
    notes = Notes()
    section = ""

    for line in body.splitlines():
        if line.startswith("## "):
            section = line[3:].strip()
            continue
        if line.startswith("**Full Changelog**"):
            notes.full_changelog = line.strip()
            continue
        if not line.startswith("* "):
            continue
        if section == "New Contributors":
            notes.new_contributors.append(line.strip())
            continue
        match = ENTRY_RE.match(line)
        if match:
            notes.entries.append(
                Entry(
                    title=match["title"].strip(),
                    author=match["author"],
                    number=int(match["number"]),
                )
            )

    return notes


def load_overrides(path: Path) -> dict[str, str]:
    """Read the checked-in title rewrites. A missing file means none."""
    if not path.is_file():
        return {}
    return json.loads(path.read_text())


def apply_overrides(notes: Notes, overrides: dict[str, str]) -> Notes:
    """Replace entry titles from the overrides map, keyed by pull-request number.

    Contributors do not all write conventional-commit titles, and the pull
    request keeps whatever it was merged with. The changelog does not have to:
    a rewrite here gives the entry a type, which is what puts it in a section.

    The file is keyed by pull-request number across the whole project, so most
    of its entries belong to releases that have already shipped. Those are
    reported and skipped rather than treated as an error: raising on them made
    the first release work and every release after it fail, since the file only
    ever grows. A number here that is *not* in any release is still worth
    seeing — a typo'd number silently rewrites nothing — so it goes to stderr.
    """
    wanted = {int(k): v for k, v in overrides.items() if not k.startswith("_")}
    present = {entry.number for entry in notes.entries}
    out_of_range = sorted(wanted.keys() - present)
    if out_of_range:
        print(
            f"note: {len(out_of_range)} override(s) are not in this range and were "
            "skipped (earlier releases, or a mistyped number): "
            + ", ".join(str(n) for n in out_of_range),
            file=sys.stderr,
        )

    notes.entries = [
        Entry(title=wanted.get(e.number, e.title), author=e.author, number=e.number)
        for e in notes.entries
    ]
    return notes


def contributor_brief(notes: Notes) -> dict[str, list[tuple[int, str]]]:
    """Every pull request each first-time contributor wrote in this range.

    The summary has to thank them for what they actually did, and GitHub's
    "New Contributors" list names only the first one.
    """
    brief: dict[str, list[tuple[int, str]]] = {}
    for line in notes.new_contributors:
        match = NEW_CONTRIBUTOR_RE.match(line)
        if match:
            brief[match["author"]] = []
    for entry in notes.entries:
        if entry.author in brief:
            brief[entry.author].append((entry.number, entry.title))
    return {author: sorted(prs) for author, prs in brief.items()}


def classify(title: str, author: str) -> str:
    """Return the section an entry belongs in, from its title and its author."""
    match = PREFIX_RE.match(title)
    if match is None:
        return "Uncategorized"
    if match["bang"]:
        return "Breaking"

    kind, scope = match["type"], (match["scope"] or "")
    # A bot's bump is a dependency update whichever type it wears: dependabot
    # opens them as build(deps) here and as plain "ci:" for action versions.
    if author.endswith("[bot]") and (kind == "build" or kind == "ci"):
        return "Dependencies"
    if kind == "build" and scope.startswith("deps"):
        return "Dependencies"
    return TYPE_SECTIONS.get(kind, "Internal")


def _linkify_issues(title: str) -> str:
    """Turn bare #123 references in a title into links, as the changelog does."""
    return ISSUE_RE.sub(
        lambda m: f"[#{m.group(1)}](https://github.com/{REPO}/issues/{m.group(1)})",
        title,
    )


def _render_entry(entry: Entry) -> str:
    return (
        f"- {_linkify_issues(entry.title)} "
        f"by [@{entry.author}](https://github.com/{entry.author}) "
        f"in [#{entry.number}]({entry.url})"
    )


def _omission_note(buckets: dict[str, list[Entry]]) -> str:
    """Say how much housekeeping was left out, so the count is not a surprise."""
    parts = []
    for name, noun in (("Internal", "internal change"), ("Dependencies", "dependency update")):
        count = len(buckets[name])
        if count:
            parts.append(f"{count} {noun}{'' if count == 1 else 's'}")
    if not parts:
        return ""
    return f"_{' and '.join(parts)} are not listed here; the full changelog has them._"


def render(notes: Notes, version: str, date: str, summary: str | None = None) -> str:
    """Render one changelog section: heading, summary, grouped entries, footer."""
    buckets: dict[str, list[Entry]] = {name: [] for name in SECTIONS}
    for entry in notes.entries:
        buckets[classify(entry.title, entry.author)].append(entry)

    lines = [f"## v{version} ({date})", "", summary or SUMMARY_PLACEHOLDER, ""]

    for name in SECTIONS:
        if not buckets[name] or name in OMITTED_SECTIONS:
            continue
        lines.append(f"### {name}")
        lines.append("")
        lines.extend(_render_entry(entry) for entry in buckets[name])
        lines.append("")

    if notes.new_contributors:
        lines.append("### New Contributors")
        lines.append("")
        lines.extend(line.replace("* ", "- ", 1) for line in notes.new_contributors)
        lines.append("")

    note = _omission_note(buckets)
    if note:
        lines.append(note)
        lines.append("")

    if notes.full_changelog:
        lines.append(notes.full_changelog)
        lines.append("")

    return "\n".join(lines)


def splice(changelog: str, section: str) -> str:
    """Put the section above the newest release, dropping any Unreleased block."""
    heading = section.splitlines()[0]
    version = heading.split()[1]
    if re.search(rf"^## {re.escape(version)}\b", changelog, re.MULTILINE):
        raise ReleaseNotesError(
            f"CHANGELOG.md already has a {version} section; remove it before rewriting."
        )

    headings = [m.start() for m in VERSION_HEADING_RE.finditer(changelog)]
    if not headings:
        raise ReleaseNotesError("CHANGELOG.md has no '## ' headings to insert above.")

    head = changelog[: headings[0]]
    rest = changelog[headings[0] :]

    # An Unreleased block is superseded by this section, so it goes.
    if rest.startswith("## Unreleased"):
        rest = rest[headings[1] - headings[0] :] if len(headings) > 1 else ""

    return f"{head}{section}\n{rest}" if rest else f"{head}{section}"


def generate(version: str, previous: str | None) -> Notes:
    """Ask GitHub for the notes between the previous tag and this version."""
    args = [
        "gh",
        "api",
        "-X",
        "POST",
        f"repos/{REPO}/releases/generate-notes",
        "-f",
        f"tag_name=v{version}",
        "-f",
        "target_commitish=main",
    ]
    if previous:
        args += ["-f", f"previous_tag_name={previous}"]

    result = subprocess.run(args, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise ReleaseNotesError(f"gh api failed: {result.stderr.strip()}")
    return parse(json.loads(result.stdout)["body"])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version", help="version being released, without the v (1.4.0)")
    parser.add_argument("--previous", help="previous tag (default: GitHub's own guess)")
    parser.add_argument("--date", default=date_cls.today().isoformat())
    parser.add_argument("--write", action="store_true", help="splice into CHANGELOG.md in place")
    parser.add_argument(
        "--contributors",
        action="store_true",
        help="list each first-time contributor and every PR they wrote",
    )
    parser.add_argument("--overrides", type=Path, default=OVERRIDES_PATH)
    args = parser.parse_args(argv)

    try:
        notes = apply_overrides(
            generate(args.version, args.previous), load_overrides(args.overrides)
        )
        if args.contributors:
            for author, prs in contributor_brief(notes).items():
                print(f"@{author}")
                for number, title in prs:
                    print(f"  #{number} {title}")
            return 0

        section = render(notes, args.version, args.date)
        if not args.write:
            print(section, end="")
            return 0
        CHANGELOG.write_text(splice(CHANGELOG.read_text(), section))
    except ReleaseNotesError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(f"Wrote the v{args.version} section to {CHANGELOG}.")
    print("Now replace the summary placeholder and triage any Uncategorized entries.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
