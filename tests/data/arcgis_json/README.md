# ArcGIS JSON pages

Recorded ArcGIS Feature Service responses for the ArcGIS conversion tests. Each
file is an unmodified response body; do not edit them in place. Tests construct
variants (such as a different top-level member order) instead of storing them.

The shoreline page is gzip-compressed (`gzip -9 -n`). Its hash is of the
uncompressed response body.

| File | Format | Features | Source layer | Captured | SHA-256 (uncompressed) |
|------|--------|---------:|--------------|----------|---------|
| `shoreline.esrijson.json.gz` | EsriJSON (`f=json`) | 1 polyline (`OBJECTID` 11; 119 parts, 8,597 positions) | [ASIS_Shoreline_2005_2020/FeatureServer/1](https://services1.arcgis.com/fBc8EJBxQRMcHlei/ArcGIS/rest/services/ASIS_Shoreline_2005_2020/FeatureServer/1) | not recorded | `3c5adade08109e09059e30786fe241fcd1929b0d3926b5ab374f83aaedb939b5` |
