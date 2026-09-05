"""Unit tests for the docs-as-tests harness itself.

The harness is test infrastructure, which makes it exactly the kind of code that
can quietly stop working — a parser that silently matches nothing turns 400
examples into 0 tests and the suite still goes green. These tests pin the
behaviour that non-vacuity depends on: indented fences are found, directives are
read, unknown directives are loud, and the seeder produces the placeholder names
the guides use.
"""

from __future__ import annotations

import pytest

from tests.docs_examples.collector import (
    BLOCK_TIMEOUT_SECONDS,
    NETWORK_BLOCK_TIMEOUT_SECONDS,
    DocExampleFailure,
    block_timeout,
    check_menu_is_splittable,
    menu_refusal_reason,
    split_statements,
)
from tests.docs_examples.parser import (
    DIRECTIVE_KEYWORDS,
    Directives,
    DirectiveSyntaxError,
    iter_blocks,
    iter_fences,
    parse_directives,
    strip_prompts,
)
from tests.docs_examples.seeder import SEED_FILES, missing_canonical_files, seed_workdir

TABBED_PAGE = """# Title

=== "CLI"

    ```bash
    gpio inspect meta input.parquet
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio
    ```

Prose in between.

```text
not executable
```
"""


def _write(tmp_path, name: str, text: str):
    page = tmp_path / "guide" / name
    page.parent.mkdir(exist_ok=True)
    page.write_text(text)
    return page


def test_finds_indented_fences_inside_tabbed_blocks(tmp_path):
    """The reason for a custom collector: off-the-shelf plugins miss these."""
    blocks = list(iter_blocks(_write(tmp_path, "t.md", TABBED_PAGE), tmp_path))
    assert [b.lang for b in blocks] == ["bash", "python"]
    # Indentation is stripped, otherwise Python blocks would be an IndentationError.
    assert blocks[0].source.rstrip("\n") == "gpio inspect meta input.parquet"
    assert blocks[1].source.rstrip("\n") == "import geoparquet_io as gpio"


def test_inert_fences_are_seen_but_not_executable(tmp_path):
    fences = list(iter_fences(_write(tmp_path, "t.md", TABBED_PAGE), tmp_path))
    assert [f.lang for f in fences] == ["bash", "python", "text"]
    assert [f.executable for f in fences] == [True, True, False]


def test_test_id_carries_the_doc_line(tmp_path):
    """Ids like guide/t.md:L5[bash] are how a failure maps back to a doc."""
    blocks = list(iter_blocks(_write(tmp_path, "t.md", TABBED_PAGE), tmp_path))
    assert blocks[0].test_id == "guide/t.md:L5[bash]"
    assert TABBED_PAGE.split("\n")[blocks[0].line - 1].strip() == "```bash"


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("skip", Directives(skip=True)),
        ('skip="needs creds"', Directives(skip=True, skip_reason="needs creds")),
        ("network", Directives(network=True)),
        ("slow", Directives(slow=True)),
        ("fast", Directives(fast=True)),
        ("needs-tippecanoe", Directives(needs_tippecanoe=True)),
        ("menu", Directives(menu=True)),
        ('setup="gpio add bbox a b"', Directives(setup=("gpio add bbox a b",))),
        (
            'network, setup="gpio add bbox a b"',
            Directives(network=True, setup=("gpio add bbox a b",)),
        ),
    ],
)
def test_parse_directives(text, expected):
    assert parse_directives(text) == expected


def test_multiple_setup_commands_are_kept_in_order():
    parsed = parse_directives('setup="one", setup="two"')
    assert parsed.setup == ("one", "two")


@pytest.mark.parametrize(
    "text",
    [
        "skipz",  # typo
        "network=true",  # takes no value
        "setup",  # requires a value
        "SKIP",  # keywords are lowercase
    ],
)
def test_bad_directives_raise(text):
    """Loud beats silent: a mistyped directive must not leave a block unguarded."""
    with pytest.raises(DirectiveSyntaxError):
        parse_directives(text)


def test_directive_attaches_to_the_fence_below_it(tmp_path):
    page = _write(
        tmp_path,
        "t.md",
        '<!-- doctest: skip="first only" -->\n```bash\na\n```\n\n```bash\nb\n```\n',
    )
    first, second = iter_blocks(page, tmp_path)
    assert first.directives.skip and first.directives.skip_reason == "first only"
    assert not second.directives.present
    assert second.directives.runs


def test_directive_is_read_through_blank_lines_and_indentation(tmp_path):
    page = _write(
        tmp_path,
        "t.md",
        '=== "CLI"\n\n    <!-- doctest: network -->\n\n    ```bash\n    x\n    ```\n',
    )
    (block,) = iter_blocks(page, tmp_path)
    assert block.directives.network


def test_prose_between_comment_and_fence_detaches_the_directive(tmp_path):
    """A directive has to visibly belong to the block it governs."""
    page = _write(tmp_path, "t.md", "<!-- doctest: skip -->\n\nSome prose.\n\n```bash\nx\n```\n")
    (block,) = iter_blocks(page, tmp_path)
    assert not block.directives.present


def test_every_documented_keyword_is_parseable():
    """DIRECTIVE_KEYWORDS is the vocabulary the meta-test error messages promise."""
    for keyword, arity in DIRECTIVE_KEYWORDS.items():
        text = f'{keyword}="v"' if arity in ("required", "optional") else keyword
        assert parse_directives(text).present


MENU_BLOCK = """# Custom column name
gpio add bbox input.parquet output.parquet --bbox-name bounds

# Force replace existing bbox
gpio add bbox input.parquet output.parquet --force

gpio sort hilbert a b \\
    --compression ZSTD
"""


def test_split_statements_keeps_comments_with_their_command():
    statements = split_statements(MENU_BLOCK)
    assert len(statements) == 3
    assert statements[0].startswith("# Custom column name\ngpio add bbox")
    assert statements[1].startswith("# Force replace existing bbox\ngpio add bbox")
    # A backslash continuation stays with the command it continues.
    assert statements[2] == "gpio sort hilbert a b \\\n    --compression ZSTD"


def test_split_statements_drops_a_comment_that_follows_the_last_command():
    """A trailing comment is a note about the output, not a command to run."""
    assert split_statements("gpio a b\n\n# trailing note\n") == ["gpio a b"]


@pytest.mark.parametrize(
    "source",
    [
        "for f in *.parquet; do gpio add bbox $f out/$f; done",
        "count=$(gpio inspect meta a.parquet)",
        "gpio inspect meta `ls *.parquet`",
        "cat <<EOF\nhi\nEOF",
    ],
)
def test_menu_refuses_anything_that_is_really_a_script(source):
    """Better to refuse than to mis-split a script into 'alternatives'."""
    assert check_menu_is_splittable(source) is not None


def test_menu_accepts_a_plain_list_of_commands():
    assert check_menu_is_splittable(MENU_BLOCK) is None


def test_menu_plus_prelude_is_refused():
    """Combined, the prelude would be split off and run as its own 'alternative'."""
    refusal = menu_refusal_reason("bash", MENU_BLOCK, parse_directives('menu, prelude="x = 1"'))
    assert refusal is not None
    assert "prelude" in refusal


def test_menu_on_a_python_block_is_refused():
    refusal = menu_refusal_reason("python", "gpio.read('a')", parse_directives("menu"))
    assert refusal == "the menu directive only applies to bash blocks"


def test_menu_on_a_plain_bash_list_is_allowed():
    assert menu_refusal_reason("bash", MENU_BLOCK, parse_directives("menu")) is None


def test_menu_refusal_is_none_without_the_directive():
    assert menu_refusal_reason("bash", "for f in *; do :; done", parse_directives("slow")) is None


def _block(tmp_path, directive: str):
    """One bash block carrying ``directive``, parsed the way the collector sees it."""
    comment = f"<!-- doctest: {directive} -->\n" if directive else ""
    page = _write(
        tmp_path, "t.md", f"{comment}```bash\ngpio partition admin input.parquet out/\n```\n"
    )
    (block,) = iter_blocks(page, tmp_path)
    return block


def test_network_blocks_get_the_longer_timeout(tmp_path):
    """120 s cannot cover a 724 MB boundary download; the network lane gets 600 s."""
    assert block_timeout(_block(tmp_path, "network")) == NETWORK_BLOCK_TIMEOUT_SECONDS
    assert NETWORK_BLOCK_TIMEOUT_SECONDS > BLOCK_TIMEOUT_SECONDS


def test_ordinary_blocks_keep_the_short_timeout(tmp_path):
    """A hanging local example must still fail fast — the bump is network-only."""
    assert block_timeout(_block(tmp_path, "")) == BLOCK_TIMEOUT_SECONDS
    assert block_timeout(_block(tmp_path, "slow")) == BLOCK_TIMEOUT_SECONDS


def test_a_network_failure_reads_as_a_download_not_a_doc_error(tmp_path):
    """The closing advice is the part a reader acts on, so it has to be right."""
    block = _block(tmp_path, "network")
    message = str(DocExampleFailure(block, "block", "gpio ...", -1, "", "timed out after 600s"))
    assert "needs the internet" in message
    assert 'doctest: skip="needs credentials"' not in message


def test_a_local_failure_still_points_at_the_doc(tmp_path):
    block = _block(tmp_path, "")
    message = str(DocExampleFailure(block, "block", "gpio ...", 1, "", "boom"))
    assert 'doctest: skip="needs credentials"' in message
    assert "needs the internet" not in message


def test_end_line_points_at_the_closing_fence(tmp_path):
    """The fence-parity meta-test is only as good as this number."""
    page = _write(tmp_path, "t.md", "intro\n\n```bash\na\nb\n```\n\ntail\n")
    (block,) = iter_blocks(page, tmp_path)
    lines = page.read_text().split("\n")
    assert lines[block.line - 1].strip() == "```bash"
    assert lines[block.end_line - 1].strip() == "```"


def test_end_line_is_right_for_an_indented_fence(tmp_path):
    page = _write(tmp_path, "t.md", '=== "CLI"\n\n    ```bash\n    a\n    ```\n\nafter\n')
    (block,) = iter_blocks(page, tmp_path)
    lines = page.read_text().split("\n")
    assert lines[block.end_line - 1].strip() == "```"
    # Every marker in the page is accounted for; nothing is stray.
    markers = {i for i, ln in enumerate(lines, 1) if ln.strip().startswith("```")}
    assert markers == {block.line, block.end_line}


def test_a_stray_fence_marker_is_detectable(tmp_path):
    """A stray marker leaves a ``` that pairs with nothing — what the meta-test finds."""
    page = _write(tmp_path, "t.md", "```bash\na\n```\n\nprose\n```\n\n```bash\nb\n```\n")
    lines = page.read_text().split("\n")
    markers = {i for i, ln in enumerate(lines, 1) if ln.strip().startswith("```")}
    paired = set()
    for block in iter_fences(page, tmp_path):
        paired.update({block.line, block.end_line})
    assert markers - paired, "the stray marker should not pair with any block"


def test_prelude_is_parsed_and_marks_the_block_as_annotated():
    parsed = parse_directives("prelude=\"table = gpio.read('x.parquet')\"")
    assert parsed.prelude == "table = gpio.read('x.parquet')"
    assert parsed.present and parsed.runs


def test_strip_prompts():
    assert strip_prompts("$ gpio inspect meta x\n$ echo hi") == "gpio inspect meta x\necho hi"
    # A block with no prompts is returned untouched, dollar signs and all.
    assert strip_prompts("echo $HOME") == "echo $HOME"


def test_seed_workdir_creates_the_placeholder_names(tmp_path):
    work = seed_workdir(tmp_path / "work")
    for placeholder in SEED_FILES:
        assert (work / placeholder).stat().st_size > 0, placeholder
    assert (work / "output_dir").is_dir()


def test_seeded_files_are_independent_copies(tmp_path):
    """Blocks are hermetic: mangling input.parquet cannot poison data.parquet."""
    work = seed_workdir(tmp_path / "work")
    (work / "input.parquet").write_bytes(b"corrupt")
    assert (work / "data.parquet").stat().st_size > 1000


def test_every_seed_source_exists():
    assert missing_canonical_files() == []
