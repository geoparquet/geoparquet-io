#!/usr/bin/env python3
"""Fail when a built page's table of contents holds an unclosed tag (#1329).

Zensical removes any ``id=`` text it finds in a heading's table-of-contents
entry, from ``id=`` up to the next ``>``. The heading
``add_quadkey(column_name='quadkey', resolution=13, use_centroid=False)``
became ``<code>add_quadkey(..., use_centro>`` in the TOC. The ``</code>`` went
with the cut, and the browser carried the open ``<code>`` into every later
element. The Python API page then rendered all its text in monospace, with the
TOC drawn over the content.

The check runs in two places:

* ``main`` reads a built site and reports every TOC entry whose tags do not
  balance. The docs workflow runs it after ``zensical build``, so any trigger
  fails the pull request, not only ``id=``.
* ``headings_with_trigger`` reads the Markdown sources for the one trigger we
  know, so a test catches it before a site build.

To keep ``id=`` in a heading, write the ``d`` as ``&#100;`` inside raw
``<code>`` tags. The anchor and the rendered text stay the same.

Usage::

    uv run python scripts/check_toc_html.py site
"""

from __future__ import annotations

import argparse
import re
import sys
from html.parser import HTMLParser
from pathlib import Path

# The text inside each TOC link, as Zensical's partials/toc-item.html writes it.
_TOC_ENTRY = re.compile(r'<span class="md-typeset">\s*(.*?)\s*</span>', re.DOTALL)
_VOID = frozenset("area base br col embed hr img input link meta source track wbr".split())
_FENCE = re.compile(r"^\s*(```|~~~)")
_HEADING = re.compile(r"^#{1,6}\s")


class _TagBalance(HTMLParser):
    """Track open elements; ``balanced`` is false on any unmatched tag."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.stack: list[str] = []
        self.mismatch = False

    def handle_starttag(self, tag, attrs):
        if tag not in _VOID:
            self.stack.append(tag)

    def handle_endtag(self, tag):
        if self.stack and self.stack[-1] == tag:
            self.stack.pop()
        else:
            self.mismatch = True

    @property
    def balanced(self) -> bool:
        return not self.stack and not self.mismatch


def _is_balanced(fragment: str) -> bool:
    parser = _TagBalance()
    parser.feed(fragment)
    parser.close()
    return parser.balanced


def unbalanced_entries(html: str) -> list[str]:
    """Return each TOC entry in ``html`` whose tags do not balance."""
    return [m for m in _TOC_ENTRY.findall(html) if not _is_balanced(m)]


def headings_with_trigger(docs_root: Path) -> list[str]:
    """Return ``path:line`` for each Markdown heading that contains ``id=``."""
    hits = []
    for page in sorted(docs_root.rglob("*.md")):
        in_fence = False
        lines = page.read_text(encoding="utf-8").splitlines()
        for number, line in enumerate(lines, start=1):
            if _FENCE.match(line):
                in_fence = not in_fence
            elif not in_fence and _HEADING.match(line) and "id=" in line:
                hits.append(f"{page.relative_to(docs_root)}:{number}")
    return hits


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("site", type=Path, help="the built site directory")
    site = parser.parse_args(argv).site

    pages = sorted(site.rglob("*.html"))
    if not pages:
        print(f"No HTML pages under {site}; build the site first.")
        return 1

    failures = 0
    for page in pages:
        for entry in unbalanced_entries(page.read_text(encoding="utf-8")):
            failures += 1
            print(f"{page.relative_to(site).as_posix()}: unclosed tag in TOC: {entry}")
    if failures:
        print(f"{failures} broken TOC entries. See scripts/check_toc_html.py.")
        return 1
    print(f"TOC entries balanced in {len(pages)} pages.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
