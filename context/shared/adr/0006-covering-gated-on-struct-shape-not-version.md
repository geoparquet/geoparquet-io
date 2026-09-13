# ADR-0006: A `covering` Is Gated on the Bbox Struct's Shape, Not on the Output Version

## Status

Accepted

## Context

GeoParquet 1.1 fixes the *order* of a bbox covering column's struct fields:
`xmin, ymin, xmax, ymax`, or `xmin, ymin, zmin, xmax, ymax, zmax`. It also
requires every field to be FLOAT or DOUBLE. A file whose `covering` points at a
struct in any other order or type fails `gpio check spec` on
`covering_bbox_structure`.

Overture writes `xmin, xmax, ymin, ymax`. At GeoParquet 1.0 that is a valid
file: 1.0 has no `covering` key, so nothing points at the column. Until #1070,
every gpio write that *invented* a covering for a carried bbox column tested the
struct with set membership, so order was discarded and the Overture column
qualified. Rewriting such a file at 1.1 (`check compression/row-group/spatial
--fix`, `convert geoparquet`, `sort hilbert`, `Table.write()`, the streaming
extract path) declared a covering gpio's own validator then rejected.

Three write paths each had their own probe: the DuckDB metadata rewrite in
`core/geo_metadata.py`, the Arrow writer in `core/arrow_geo_metadata.py`, and
the streaming writer in `core/write_strategies/arrow_streaming.py`. Two were
fixed in the first pass; the third was found in review. One rule, three
spellings, drifting independently, was the defect.

Issue #1035 also offered a different repair: have the three `check --fix`
rewrites keep the input's version (1.0 stays 1.0) so no covering is ever
declared. That was measured and rejected, see below.

## Decision

One function, `bbox_column_to_declare(schema, geo_meta)` in
`core/geo_metadata.py`, decides whether a write may declare a covering over a
bbox column. Every path that invents a covering calls it. The predicate is
`bbox_covering_problem`, which returns `None` when the struct is legal and a
sentence naming the problem otherwise (field order, non-float fields, not a
struct at all).

Three rules follow from the gate:

1. **Never invent an illegal covering.** A carried bbox column whose struct the
   spec forbids is written through unchanged and left undeclared, with one
   warning per process saying why and pointing at `gpio add bbox --force`,
   which rewrites the column in the spec's order and declares it.
2. **Drop a declared illegal covering.** An input that already declares a
   covering over an illegal struct is written without that `covering` entry,
   with a warning that says it was dropped. gpio does not write a file its own
   validator rejects, even when the input asked it to. Legal declared
   coverings are carried unchanged (#738).
3. **`check bbox` reports the real problem.** Such a file is reported as
   "cannot declare a covering" or "covering declared over an illegal struct",
   not as "missing metadata covering", and `--fix` says there is nothing it
   can repair. `gpio add bbox-metadata` refuses outright, since its only job
   is to write the key.

`core/validate.py` derives its field list from the same
`BBOX_COVERING_FIELD_ORDERS` constant, so the writers and the validator cannot
disagree again.

The output version is left alone. Auto mode still writes 1.x inputs at 1.1.

## Consequences

### Positive
- Every gpio write produces a file `gpio check spec` accepts, whatever the
  input's bbox column looks like.
- One predicate, one warning, one hint. A fourth write path gets the rule by
  calling the gate, not by copying a probe.
- The version question stays a separate decision, owned by
  `resolve_output_geoparquet_version` in the write facade.

### Negative
- A rewrite of an Overture-shaped 1.0 file at 1.1 carries the bbox column but
  does not declare it. Readers that use `covering` for pruning gain nothing
  from that column until the user runs `gpio add bbox --force`. Before #1070
  they got a declared covering, but in a file the spec rejects.
- An input that declared an illegal covering loses it on rewrite. This is a
  behaviour change for anyone who was carrying such files through gpio and
  tolerating the validator failure.

### Neutral
- The gate warns once per process per column and problem, so a partition
  writing hundreds of files says it once.
- `check all --fix` still preserves the input's version, as it did before.

## Alternatives Considered

### Keep the input's version on `check --fix` (option (a) in #1035)

Rejected. `1.x → 1.1` in auto mode is documented policy shared by every write
path, and the three fixes pass `input_file` to the resolver on purpose (#1009).
Reversing it for three commands would move the issue's own complaint, one tool
with two version policies, rather than remove it.

It also does not close the defect. A clean 1.1 input with an Overture-order
bbox column and no covering is valid, because `covering_bbox_structure` is
skipped when nothing declares a covering. Version preservation cannot help it:
it is already at the version it will be written at. Measured on `origin/main`
before #1070, that clean 1.1 file was turned invalid by all three rewrite
fixes, by `check all --fix`, by `convert geoparquet --geoparquet-version 1.1`,
by `sort hilbert` and by `gpio.read(f).write(out)`.

### Reorder the struct fields during the rewrite

Rejected. Changing a column's shape is a data change, and the rewrite fixes
promise not to make one. `gpio add bbox --force` exists for exactly this and
stays explicit.

### Carry a declared illegal covering through as the input asked

Rejected in review of #1070. The two write facades disagreed on this before
the review (one carried it, one dropped it). Carrying it means gpio writes a
file its own `check spec` fails on, and the user finds out one command later.
Dropping it with a warning keeps every gpio output valid and tells the user
what happened and how to get the covering back.

## References

- Issue #1035, PR #1070
- `geoparquet_io/core/geo_metadata.py`: `bbox_column_to_declare`,
  `bbox_covering_problem`, `declare_carried_bbox_column`,
  `BBOX_COVERING_FIELD_ORDERS`
- `docs/guide/check.md`, "Bbox struct field order"
- GeoParquet 1.1 specification, `covering.bbox`
- ADR-0004 (the Python API takes the same path, so `Table.write()` is gated too)
