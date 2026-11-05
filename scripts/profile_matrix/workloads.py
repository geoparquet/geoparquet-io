"""Deterministic local profiling inputs."""

from __future__ import annotations

# Rows and file counts are fixed so repeated revisions use the same input bytes.
WORKLOADS = {
    "W1": {"small": 256, "full": 1_000_000},
    "W2": {"small": 256, "full": 1_000_000},
    "W2-skew": {"small": 256, "full": 1_000_000},
    "W3": {"small": 8, "full": 1_000},
    "W4": {"small": 256, "full": 1_000_000},
    "W5": {"small": 16, "full": 2},
    "W6": {"small": 16, "full": 1_000},
}


def _batch(start, count, name, rng, geometry_columns=1):
    import struct

    import pyarrow as pa

    polygons = name.startswith("W2")
    geometry = []
    for _ in range(count):
        x = rng.uniform(-120, 120)
        y = rng.uniform(-60, 60)
        if name == "W2-skew" and rng.random() < 0.95:
            x, y = rng.uniform(0, 0.01), rng.uniform(0, 0.01)
        if polygons:
            ring = [(x, y), (x + 0.001, y), (x + 0.001, y + 0.001), (x, y + 0.001), (x, y)]
            geometry.append(
                struct.pack("<BIII", 1, 3, 1, 5) + b"".join(struct.pack("<dd", *p) for p in ring)
            )
        else:
            geometry.append(struct.pack("<BIdd", 1, 1, x, y))
    columns = {
        "id": pa.array(range(start, start + count), type=pa.int64()),
        "category": pa.array(
            [f"category-{i % 32}" for i in range(start, start + count)], type=pa.string()
        ),
        "value": pa.array([rng.random() * 100 for _ in range(count)], type=pa.float64()),
        "geometry": pa.array(geometry, type=pa.binary()),
    }
    geometry_names = ["geometry"]
    for i in range(1, geometry_columns):
        column = f"geometry_{i:04}"
        columns[column] = columns["geometry"]
        geometry_names.append(column)
    import json

    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            column: {"encoding": "WKB", "geometry_types": ["Polygon" if polygons else "Point"]}
            for column in geometry_names
        },
    }
    return pa.table(columns).replace_schema_metadata(
        {b"geo": json.dumps(geo, sort_keys=True).encode()}
    )


def generate_workload(name, directory, size="small", seed=1368):
    """Write a deterministic seeded fixture; W3 returns a directory of tiny files.

    W4's Parquet and W6's GeoJSON are served by :func:`serve_workload`.
    Generation belongs outside the measured command's process.
    """
    import json
    import random
    from pathlib import Path

    import pyarrow.parquet as pq

    count = WORKLOADS[name][size]
    geometry_columns = (500 if size == "full" else 8) if name == "W5" else 1
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    if name == "W6":
        path = directory / f"{name}-{size}-{seed}-{count}.geojson"
        if path.exists():
            return path
        features = [
            {
                "type": "Feature",
                "id": f"points.{i}",
                "geometry": {
                    "type": "Point",
                    "coordinates": [rng.uniform(-120, 120), rng.uniform(-60, 60)],
                },
                "properties": {"id": i, "value": i % 32},
            }
            for i in range(count)
        ]
        temporary = path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps({"type": "FeatureCollection", "features": features}, sort_keys=True)
        )
        temporary.replace(path)
        return path
    if name == "W3":
        path = directory / f"{name}-{size}-{seed}-{count}"
        path.mkdir(exist_ok=True)
        complete = path / ".complete"
        if complete.exists():
            return path
        for i in range(count):
            pq.write_table(_batch(i * 100, 100, "W1", rng), path / f"part-{i:05}.parquet")
        complete.touch()
        return path
    width = f"-{geometry_columns}geo" if name == "W5" else ""
    path = directory / f"{name}-{size}-{seed}-{count}{width}.parquet"
    if path.exists():
        return path
    temporary = path.with_suffix(".tmp")
    with pq.ParquetWriter(
        temporary, _batch(0, 0, name, random.Random(seed), geometry_columns).schema
    ) as writer:
        for start in range(0, count, 16_384):
            writer.write_table(
                _batch(start, min(16_384, count - start), name, rng, geometry_columns)
            )
    temporary.replace(path)
    return path


def _wfs_response(path, query, endpoint):
    import json

    request = query.get("request", ["getcapabilities"])[0].lower()
    if request == "getcapabilities":
        # WFS 1.1 is enough for the tiny deterministic transport workload.
        body = f"""<WFS_Capabilities version="1.1.0" xmlns="http://www.opengis.net/wfs" xmlns:ows="http://www.opengis.net/ows" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:gml="http://www.opengis.net/gml" xmlns:bench="urn:bench"><ows:ServiceIdentification><ows:Title>Local benchmark</ows:Title><ows:ServiceType>WFS</ows:ServiceType><ows:ServiceTypeVersion>1.1.0</ows:ServiceTypeVersion></ows:ServiceIdentification><ows:OperationsMetadata>{"".join(f'<ows:Operation name="{op}"><ows:DCP><ows:HTTP><ows:Get xlink:href="{endpoint}"/></ows:HTTP></ows:DCP><ows:Parameter name="outputFormat"><ows:Value>application/json</ows:Value></ows:Parameter></ows:Operation>' for op in ["GetCapabilities", "DescribeFeatureType", "GetFeature"])}</ows:OperationsMetadata><FeatureTypeList><FeatureType><Name>bench:points</Name><Title>Points</Title><DefaultSRS>urn:ogc:def:crs:EPSG::4326</DefaultSRS><ows:WGS84BoundingBox><ows:LowerCorner>-120 -60</ows:LowerCorner><ows:UpperCorner>120 60</ows:UpperCorner></ows:WGS84BoundingBox></FeatureType></FeatureTypeList></WFS_Capabilities>"""
        return "text/xml", body.encode()
    if request == "describefeaturetype":
        return (
            "text/xml",
            b'<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:gml="http://www.opengis.net/gml" targetNamespace="urn:bench"><xsd:complexType name="pointsType"><xsd:complexContent><xsd:extension base="gml:AbstractFeatureType"><xsd:sequence><xsd:element name="geometry" type="gml:PointPropertyType"/><xsd:element name="id" type="xsd:long"/><xsd:element name="value" type="xsd:long"/></xsd:sequence></xsd:extension></xsd:complexContent></xsd:complexType></xsd:schema>',
        )
    document = json.loads(path.read_text())
    total = len(document["features"])
    if query.get("resulttype", [""])[0].lower() == "hits":
        return (
            "text/xml",
            f'<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs" numberOfFeatures="{total}" numberMatched="{total}"/>'.encode(),
        )
    start = int(query.get("startindex", [0])[0])
    count = int(query.get("count", query.get("maxfeatures", [total]))[0])
    document["features"] = document["features"][start : start + count]
    document.update(
        numberMatched=total, numberReturned=len(document["features"]), totalFeatures=total
    )
    return "application/json", json.dumps(document).encode()


def serve_workload(path):
    """Context manager for localhost HTTP ranges and a paginated tiny WFS."""
    from contextlib import contextmanager
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from pathlib import Path
    from threading import Thread
    from urllib.parse import parse_qs, urlsplit

    path = Path(path)

    def respond(self, head=False):
        if path.suffix == ".geojson":
            query = {
                key.lower(): value for key, value in parse_qs(urlsplit(self.path).query).items()
            }
            content_type, body = _wfs_response(
                path, query, f"http://127.0.0.1:{self.server.server_port}/wfs"
            )
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if not head:
                self.wfile.write(body)
            return
        size = path.stat().st_size
        start, end = 0, size - 1
        byte_range = self.headers.get("Range")
        if byte_range:
            bounds = byte_range.removeprefix("bytes=").split("-", 1)
            start = int(bounds[0] or 0)
            end = min(int(bounds[1] or end), end)
            if start > end:
                self.send_error(416)
                return
        self.send_response(206 if byte_range else 200)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(end - start + 1))
        if byte_range:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.end_headers()
        if not head:
            with path.open("rb") as source:
                source.seek(start)
                remaining = end - start + 1
                while remaining:
                    chunk = source.read(min(remaining, 1024 * 1024))
                    self.wfile.write(chunk)
                    remaining -= len(chunk)

    @contextmanager
    def running():
        handler = type(
            "WorkloadHandler",
            (BaseHTTPRequestHandler,),
            {
                "do_GET": respond,
                "do_HEAD": lambda self: respond(self, head=True),
                "log_message": lambda *_args: None,
            },
        )
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}/{path.name}"
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    return running()
