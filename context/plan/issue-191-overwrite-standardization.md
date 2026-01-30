# Implementation Plan: Standardize --overwrite behavior across commands

## Issue Summary
Standardize the `--overwrite` flag behavior across all data-processing commands for consistency.
Issue #191 with additional commands identified by Chris.

## Commands Needing --overwrite Option

### From Original Issue:
**Add commands (6):**
1. `gpio add admin-divisions`
2. `gpio add bbox`
3. `gpio add bbox-metadata`
4. `gpio add h3`
5. `gpio add kdtree`
6. `gpio add quadkey`

**Sort commands (3):**
7. `gpio sort hilbert`
8. `gpio sort column`
9. `gpio sort quadkey`

### Additional from Chris's Comment:
**Extract commands (3):**
10. `gpio extract bigquery`
11. `gpio extract arcgis`
12. `gpio extract geoparquet`

## Commands Already Having --overwrite
Based on code analysis (lines with @overwrite_option):
- Line 414: check_all
- Line 714: check_compression_cmd
- Line 798: check_bbox_cmd
- Line 928: check_row_group_cmd
- Line 1285: convert_to_geoparquet_cmd (convert geoparquet)
- Line 1480: convert_geopackage
- Line 1656: convert_to_csv

## Implementation Steps

### Step 1: Write tests for --overwrite behavior
For each command needing --overwrite:
1. Test default behavior (fails if output exists)
2. Test --overwrite=true behavior (overwrites existing)
3. Test --overwrite=false behavior (fails if output exists)
4. Test atomic rename pattern (temp file usage)

### Step 2: Add @overwrite_option to CLI commands
For each command in the list:
1. Import overwrite_option if not already imported
2. Add @overwrite_option decorator before the function
3. Add overwrite parameter to function signature
4. Pass overwrite to core function

### Step 3: Update core functions
For each core function corresponding to CLI commands:
1. Add overwrite parameter (default=False)
2. Check if output exists and overwrite=False -> raise error
3. Use temp file + atomic rename pattern for safe overwriting
4. Follow existing pattern from convert/reproject

### Step 4: Update documentation
1. Update CLAUDE.md to mention always using overwrite_option for new commands
2. Update command help text to mention --overwrite
3. Update relevant guide docs

## Implementation Order

### Phase 1: Extract commands (highest user impact)
- extract_geoparquet
- extract_arcgis
- extract_bigquery_cmd

### Phase 2: Add commands (6 commands)
- add_country_codes (admin-divisions)
- add_bbox
- add_bbox_metadata_cmd
- add_h3
- add_kdtree
- add_quadkey

### Phase 3: Sort commands (3 commands)
- hilbert_order (sort hilbert)
- sort_column
- sort_quadkey

### Phase 4: Documentation & Cleanup
- Update CLAUDE.md
- Update contributing docs
- Final testing

## Testing Strategy

Each command needs:
1. Unit test in tests/test_<command>.py for overwrite logic
2. Integration test verifying temp file usage
3. Test that existing files are preserved when overwrite=False
4. Test that existing files are replaced when overwrite=True

## Notes

- Use existing overwrite pattern from convert/reproject commands
- Temp file pattern ensures atomic operations (no partial files on failure)
- This is a breaking change but acceptable for v1.0-beta
- check --fix commands keep --no-backup flag (different semantics from --overwrite)

## Commit Strategy

One commit per phase:
1. "Add --overwrite to extract commands"
2. "Add --overwrite to add commands"
3. "Add --overwrite to sort commands"
4. "Update documentation for --overwrite standardization"
