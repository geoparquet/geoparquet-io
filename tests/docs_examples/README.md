# Running the guide's examples as tests

Every fenced `bash` and `python` block in `docs/guide/*.md` is collected as a
pytest item and executed. If you edit a guide, your example runs in CI.

```bash
uv run pytest docs/guide -n 4 -m "not network"   # the whole docs lane
uv run pytest "docs/guide/sort.md"               # one page
uv run pytest "docs/guide/sort.md::guide/sort.md:L14[bash]"   # one block
```

**Keep the `-m "not network"`.** Dropping it adds 29 blocks that download
hundreds of megabytes of administrative boundaries from live services. Those
belong to the network CI lane; run without the filter and you get a dozen or so
timeouts whose count drifts with the state of your boundary cache, which looks
like a flaky docs lane and is not.

Each block runs in its own throwaway directory, seeded from
`tests/data/canonical/` under the placeholder names the guides already use —
`input.parquet`, `data.parquet`, `buildings.parquet`, `places.geojson`,
`data.csv`, an empty `output_dir/`, and a few aliases. See `seeder.py` for the
full list. That is why most examples run verbatim, with no doc edits.

## When your example cannot run

Say so in the doc with an HTML comment on the line **above** the fence. It is
invisible in the rendered page, and only blank lines may sit between it and the
fence.

```markdown
<!-- doctest: skip="needs cloud credentials" -->
```bash
gpio add bbox s3://bucket/in.parquet s3://bucket/out.parquet
```
```

| Directive | Effect |
|---|---|
| `skip="reason"` | Not executed. **The reason is required** and must be true of the whole fence. |
| `network` | Runs only in the network CI lane (`-m network`), with a 600 s block timeout instead of the usual 120 s — a boundary download cannot finish in two minutes. Required on any fence that needs the internet; the meta-test checks the known ones. |
| `slow` / `fast` | Force the block out of / into the fast suite, whatever page it is on. |
| `needs-tippecanoe` | Skipped when `tippecanoe` is not installed. Use it for anything piping to tippecanoe or calling `gpio pmtiles`. |
| `needs-ogr` | Skipped when `ogr2ogr` is not installed. |
| `setup="shell command"` | Runs before the block, in the same directory. Repeatable. Use it to build a precondition rather than skipping. |
| `prelude="python source"` | Prepended to a Python block. For tabs that continue a session an earlier tab started. |
| `menu` | The lines are *alternatives*, not a script: each runs from a fresh directory. Use it when several lines write the same output file. |
| `demonstrates-error` | The fence deliberately shows a command failing. Exempts it from the "no working commands inside a skipped fence" check. |

Combine with commas: `<!-- doctest: network, setup="gpio add bbox in.parquet out.parquet" -->`

## Rules the meta-test enforces

`tests/test_docs_examples_meta.py` fails the build when:

- a fence uses a language the harness cannot run (` ```sh `, ` ```py ` — use
  ` ```bash ` / ` ```python `);
- a ` ``` ` marker does not pair with any block (a stray marker silently
  swallows the next real example);
- a directive is misspelled or malformed;
- a block is skipped without a reason;
- a command-shaped snippet hides in a prose fence with no directive;
- a fence runs a command that cannot work without the internet (`gpio add
  admin-divisions`, `gpio partition admin`, `gpio process aggregate admin`,
  `--dataset gaul`, a `source.coop` URL, …) without `network` or `skip`;
- the number of opted-out blocks drifts past the agreed ceiling.

`tests/test_docs_examples_coverage.py` (slow lane) adds the expensive one: it
runs each statement of every skipped, locally-runnable bash fence in a seeded
directory and fails if any exits 0. A skip is not allowed to take working
commands down with it — split the fence, or mark it `demonstrates-error` when
the failure is the point.

## Writing a good skip reason

The reason has to be true of **every** line in the fence, and specific enough
that the next reader can tell whether it still applies.

- Good: `skip="filters on 'population', a column the sample data does not have"`
- Good: `skip="needs cells.parquet, which the harness does not seed"`
- Bad: `skip="needs cloud credentials"` on a fence whose first two lines are
  purely local — split the fence instead, so the local half runs.

Prefer, in order: **make it run** (often a one-line `setup=`), **split the
fence** so the runnable part is not held back, then **skip with a precise
reason**.

## What a green run proves

That every documented command still parses, runs, and does not crash. Blocks
are judged on exit status only — nothing inspects what they wrote — so an
example that succeeds while doing nothing still passes. See the module
docstring in `collector.py`.
