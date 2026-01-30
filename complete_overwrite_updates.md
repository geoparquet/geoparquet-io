# Remaining Overwrite Updates Checklist

## Commands Completed ✅
- Extract: geoparquet, arcgis, bigquery
- Add: admin-divisions, bbox, h3

## Commands Still Needed

### Add Commands
- [ ] add kdtree (needs overwrite param in function)
- [ ] add quadkey
- [ ] add bbox-metadata (different - no output file)

### Sort Commands
- [ ] sort hilbert
- [ ] sort column
- [ ] sort quadkey

## Core Functions to Update

Each core function needs:
```python
def function_name(..., overwrite: bool = False):
    # Check if output file exists
    if output_parquet and not overwrite:
        from pathlib import Path
        import click
        if Path(output_parquet).exists():
            raise click.ClickException(
                f"Output file already exists: {output_parquet}\nUse --overwrite to replace it."
            )
```

### Core files needing updates:
- add_admin_divisions_multi.py
- add_bbox_column.py
- add_h3_column.py
- add_kdtree_column.py
- add_quadkey_column.py
- hilbert_order.py
- sort_by_column.py
- sort_quadkey.py
