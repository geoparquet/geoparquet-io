# ADR-0009: Aggregate Output Column Names Are the Rollup Contract

## Status

Accepted

## Context

`gpio process aggregate` writes one row per cell with `count`, the `--metric`
columns (`sum_<col>`, `avg_<col>`, `min_<col>`, `max_<col>`) and, with
`--breakdown`, one pivot column per category. `gpio process overview` and
`gpio pmtiles pyramid` then roll those rows up to coarser cells. The rollup
does not know how a column was produced; `overview/detect.py` recognises the
roll-up behaviour by the column name's prefix (`sum_` is summed, `min_`/`max_`
take the extremum, `avg_` is count-weighted, `count_` is summed) and drops
every column it does not recognise, with a warning.

#1100 added `--breakdown-metric`, which makes a pivot carry `SUM`/`MIN`/`MAX`
of a column instead of a row count. The pivot's name decides whether it
survives an overview, so the name had to be settled before the SQL.

## Decision

The output column name is the contract between the aggregate and every
consumer that rolls it up. A pivot is named `<func>_<col>_<value>`, its
remainder bucket `<func>_<col>_other`, so it lands on a prefix `detect.py`
already understands; a plain count pivot keeps its historical `count_<value>`.

Three consequences are accepted with it:

1. **The grammar is not invertible.** Both `<col>` and `<value>` may contain
   underscores, so a name cannot be split back into its parts. A consumer may
   read the *func* off the head of the name and nothing else.
2. **Collisions are resolved by suffix, never by merging.** A pivot that would
   land on a `--metric` output name, on the remainder bucket, or on another
   category's sanitised name gets `_2`, `_3`, ... and a warning names the
   clash. Two different numbers never share one column.
3. **`avg` is refused as a breakdown metric.** A per-category mean can only be
   rolled up weighted by the *category's* count, and a metric pivot replaces
   the `count_<value>` columns rather than sitting beside them. Until the
   output carries that weight in a form the rollup can pair with the pivot,
   `avg` would be silently wrong at every overview level, so it is an error.

## Consequences

### Positive
- No new machinery in `overview`: every consumer that rolled up `sum_*`
  columns rolls up `sum_<col>_<value>` unchanged.
- Files written by older gpio versions, and by other tools that follow the
  prefixes, keep working.

### Negative
- The recogniser and the namer live in two modules (`aggregate/common.py`,
  `overview/detect.py`) bound only by tests. A change to one without the other
  drops columns silently.
- The next features that need more than the head prefix (`avg` with a carried
  weight, a second breakdown column) cannot be expressed in the name.

### Neutral
- A per-file manifest (`gpio:aggregate` in the Parquet key-value metadata,
  naming each column's func, source and category) would let the rollup stop
  parsing names and unlock `avg`. It would be an *override* with the name
  grammar as the fallback, since a manifest goes stale when a column is
  dropped by `gpio extract` and is absent from files gpio did not write.
  Tracked as #1110.

## Alternatives Considered

### A distinct prefix for pivots (`bd_<col>_<value>`)
Rejected: `detect.py` drops it, so the pivot would survive the base band and
vanish from every overview, exactly where wide time selections hurt most.

### Emit the manifest now
Deferred: the name grammar is enough for `sum`/`min`/`max`, and a manifest
without a fallback would break on every file that lacks one.

### Coalesce an empty `sum` pivot to 0
Rejected in review of #1101: `--metric sum:<col>` reports NULL for a cell
whose values are all NULL, and the same file would have said 0 for the pivot
and NULL for the metric under the same condition. Both read NULL; clients
that need a number coalesce.

## References

- Issue #1100, PR #1101; follow-up #1110
- `geoparquet_io/core/process/aggregate/common.py`: `breakdown_prefix`,
  `build_breakdown_column_names`, `build_breakdown_pivot`
- `geoparquet_io/core/process/overview/detect.py`: `_classify_columns`
- `docs/guide/process-aggregate.md`, `docs/guide/process-overview.md`
