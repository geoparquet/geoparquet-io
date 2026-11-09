"""Tests for scripts/check_toc_html.py, the built-site TOC guard (#1329).

Zensical strips any ``id=`` text it finds in a heading's table-of-contents
entry, from ``id=`` up to the next ``>``. A heading such as
``add_quadkey(..., use_centroid=False)`` lost its closing ``</code>``, and the
browser carried that open ``<code>`` into every later element of the page.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts import check_toc_html

pytestmark = pytest.mark.meta


def _page(*entries: str) -> str:
    items = "".join(
        f'<li class="md-nav__item"><a href="#x" class="md-nav__link">'
        f'<span class="md-ellipsis"><span class="md-typeset">\n'
        f"        {entry}\n      </span></span></a></li>"
        for entry in entries
    )
    return f'<html><body><nav class="md-nav"><ul>{items}</ul></nav></body></html>'


def test_balanced_entries_pass():
    html = _page("<code>add_bbox(column_name='bbox')</code>", "Installation")
    assert check_toc_html.unbalanced_entries(html) == []


def test_the_1329_truncation_is_reported():
    html = _page("<code>add_quadkey(column_name='quadkey', resolution=13, use_centro>")
    assert check_toc_html.unbalanced_entries(html) == [
        "<code>add_quadkey(column_name='quadkey', resolution=13, use_centro>"
    ]


def test_several_code_spans_in_one_entry_pass():
    html = _page("<code>head(n=10)</code> / <code>tail(n=10)</code>")
    assert check_toc_html.unbalanced_entries(html) == []


def test_main_scans_every_page_and_fails_on_one_bad_entry(tmp_path: Path, capsys):
    (tmp_path / "good").mkdir()
    (tmp_path / "good" / "index.html").write_text(_page("<code>f()</code>"))
    (tmp_path / "bad").mkdir()
    (tmp_path / "bad" / "index.html").write_text(_page("<code>f(use_centro>"))

    assert check_toc_html.main([str(tmp_path)]) == 1
    out = capsys.readouterr().out
    assert "bad/index.html" in out
    assert "good/index.html" not in out


def test_main_passes_a_clean_site(tmp_path: Path):
    (tmp_path / "index.html").write_text(_page("<code>f()</code>"))
    assert check_toc_html.main([str(tmp_path)]) == 0


def test_main_refuses_a_site_with_no_pages(tmp_path: Path):
    """A wrong path must not pass as a clean site."""
    assert check_toc_html.main([str(tmp_path)]) == 1


def test_no_docs_heading_carries_the_trigger():
    """Catch the trigger at the source too, before a site build."""
    docs = Path(__file__).resolve().parent.parent / "docs"
    assert check_toc_html.headings_with_trigger(docs) == []
