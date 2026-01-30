# Issue #191 Implementation Status

## ✅ Completed

### Phase 1: Extract Commands
- `gpio extract geoparquet` - DONE
- `gpio extract arcgis` - DONE
- `gpio extract bigquery` - DONE

### Phase 2: Add Commands (Partial)
- `gpio add admin-divisions` - CLI DONE (core function needs update)
- `gpio add bbox` - CLI DONE (core function needs update)
- `gpio add h3` - CLI DONE (core function needs update)
- `gpio add kdtree` - CLI PARTIAL (needs implementation call update + core function)

## ⏳ In Progress

### Remaining Add Commands
- `gpio add quadkey` - Need to add @overwrite_option, parameter, and core update
- `gpio add bbox-metadata` - Special case (no output file)

### Phase 3: Sort Commands
- `gpio sort hilbert` - Need to add @overwrite_option, parameter, and core update
- `gpio sort column` - Need to add @overwrite_option, parameter, and core update
- `gpio sort quadkey` - Need to add @overwrite_option, parameter, and core update

## 📝 Core Functions Needing Overwrite Check

Each needs this pattern added:
```python
if output_file and not overwrite:
    from pathlib import Path
    import click
    if Path(output_file).exists():
        raise click.ClickException(
            f"Output file already exists: {output_file}\nUse --overwrite to replace it."
        )
```

Files to update:
- `core/add_admin_divisions_multi.py`
- `core/add_bbox_column.py`
- `core/add_h3_column.py`
- `core/add_kdtree_column.py`
- `core/add_quadkey_column.py`
- `core/hilbert_order.py`
- `core/sort_by_column.py`
- `core/sort_quadkey.py`

## 📚 Documentation
- Update CLAUDE.md to mention using @overwrite_option for new commands
- Update contributing docs if needed

## Current PR
- #202 (Draft) - https://github.com/geoparquet/geoparquet-io/pull/202
