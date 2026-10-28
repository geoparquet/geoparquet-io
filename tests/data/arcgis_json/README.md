# ArcGIS JSON pages

Recorded ArcGIS Feature Service responses for the ArcGIS conversion tests. Each
file is an unmodified response body; do not edit them in place. Tests construct
variants (such as a different top-level member order) instead of storing them.

The shoreline page is gzip-compressed (`gzip -9 -n`). Its hash is of the
uncompressed response body.

| File | Format | Features | Source layer | Captured | SHA-256 (uncompressed) |
|------|--------|---------:|--------------|----------|---------|
| `shoreline.esrijson.json.gz` | EsriJSON (`f=json`) | 1 polyline (`OBJECTID` 11; 119 parts, 8,597 positions) | [ASIS_Shoreline_2005_2020/FeatureServer/1](https://services1.arcgis.com/fBc8EJBxQRMcHlei/ArcGIS/rest/services/ASIS_Shoreline_2005_2020/FeatureServer/1) | 2026-10-15 03:34:05 UTC | `3c5adade08109e09059e30786fe241fcd1929b0d3926b5ab374f83aaedb939b5` |

The shoreline response was freshly captured on **2026-10-15 at
03:34:05.803871 UTC** with this GET request:

```text
https://services1.arcgis.com/fBc8EJBxQRMcHlei/ArcGIS/rest/services/ASIS_Shoreline_2005_2020/FeatureServer/1/query?f=json&objectIds=11&outFields=%2A&returnGeometry=true&outSR=4326
```

Parameters: `f=json`, `objectIds=11`, `outFields=*`, `returnGeometry=true`,
`outSR=4326`. The uncompressed body is 318,044 bytes. It exactly matches the
previous fixture's SHA-256, so the existing compressed fixture is retained.
This timestamp records the verified fresh capture, not the original capture
whose date was unknown. Tests run offline against the stored response.
