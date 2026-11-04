"""The issue #1368 command matrix and its deterministic invocation adapters."""

from __future__ import annotations

# Each entry is a command/workload pair; no network data is needed.
COMMANDS = [
    {
        "id": "cli-import",
        "capture_stdout": True,
        "workload": "W1",
        "argv": ["--help"],
        "tickets": [921],
    },
    {
        "id": "sort-str",
        "workload": "W2",
        "argv": ["sort", "str", "{input}", "{output}"],
        "tickets": [777],
    },
    {
        "id": "sort-str-skew",
        "workload": "W2-skew",
        "argv": ["sort", "str", "{input}", "{output}"],
        "tickets": [777],
    },
    {
        "id": "sort-hilbert",
        "workload": "W2",
        "argv": ["sort", "hilbert", "{input}", "{output}"],
        "tickets": [],
    },
    {
        "id": "sort-column",
        "workload": "W1",
        "argv": ["sort", "column", "{input}", "{output}", "value"],
        "tickets": [1342],
    },
    {
        "id": "convert-geoparquet",
        "workload": "W2",
        "argv": ["convert", "geoparquet", "{input}", "{output}"],
        "tickets": [],
    },
    {
        "id": "convert-geojson",
        "workload": "W2",
        "argv": ["convert", "geojson", "{input}", "{geojson}"],
        "tickets": [1322],
    },
    {
        "id": "convert-flatgeobuf",
        "workload": "W2",
        "argv": ["convert", "flatgeobuf", "{input}", "{fgb}"],
        "tickets": [1322],
    },
    {
        "id": "convert-geopackage",
        "workload": "W2",
        "argv": ["convert", "geopackage", "{input}", "{gpkg}"],
        "tickets": [1322],
    },
    {
        "id": "reproject",
        "workload": "W2",
        "argv": ["convert", "reproject", "{input}", "{output}", "-d", "EPSG:3857"],
        "tickets": [1342],
    },
    {
        "id": "add-bbox",
        "workload": "W1",
        "argv": ["add", "bbox", "{input}", "{output}"],
        "tickets": [],
    },
    {
        "id": "add-h3",
        "workload": "W1",
        "argv": ["add", "h3", "{input}", "{output}", "--resolution", "5"],
        "tickets": [1342],
    },
    {
        "id": "add-kdtree",
        "workload": "W1",
        "argv": ["add", "kdtree", "{input}", "{output}", "--partitions", "8", "--exact"],
        "tickets": [1342],
    },
    {
        "id": "partition-string",
        "workload": "W1",
        "argv": [
            "partition",
            "string",
            "{input}",
            "{directory}",
            "--column",
            "category",
            "--skip-analysis",
        ],
        "tickets": [],
    },
    {
        "id": "partition-quadkey",
        "workload": "W1",
        "argv": [
            "partition",
            "quadkey",
            "{input}",
            "{directory}",
            "--resolution",
            "4",
            "--partition-resolution",
            "2",
            "--skip-analysis",
        ],
        "tickets": [],
    },
    {
        "id": "check-spec",
        "capture_stdout": True,
        "workload": "W5",
        "argv": ["check", "spec", "{input}"],
        "tickets": [1081],
    },
    {
        "id": "check-all",
        "capture_stdout": True,
        "workload": "W2",
        "argv": ["check", "all", "{input}"],
        "tickets": [],
    },
    {
        "id": "check-fix",
        "workload": "W2",
        "argv": ["check", "all", "{input}", "--fix", "--fix-output", "{output}"],
        "tickets": [1057],
    },
    {
        "id": "inspect-summary",
        "capture_stdout": True,
        "workload": "W1",
        "argv": ["inspect", "summary", "{input}"],
        "tickets": [],
    },
    {
        "id": "inspect-stats",
        "capture_stdout": True,
        "workload": "W5",
        "argv": ["inspect", "stats", "{input}"],
        "tickets": [],
    },
    {
        "id": "extract-columns",
        "workload": "W5",
        "argv": ["extract", "geoparquet", "{input}", "{output}", "--include-cols", "id,geometry"],
        "tickets": [1342],
    },
    {
        "id": "extract-bbox",
        "workload": "W2",
        "argv": ["extract", "geoparquet", "{input}", "{output}", "--bbox=-60,-30,60,30"],
        "tickets": [],
    },
    {
        "id": "extract-remote",
        "workload": "W4",
        "argv": ["extract", "geoparquet", "{url}", "{output}", "--limit", "10000"],
        "tickets": [1348, 1351, 1350, 113],
    },
    {
        "id": "extract-many",
        "workload": "W3",
        "argv": ["extract", "geoparquet", "{glob}", "{output}"],
        "tickets": [1013, 1058, 1343, 1353],
    },
    {"id": "extract-stdout", "workload": "W2", "kind": "stream", "tickets": [1126]},
    {"id": "convert-stdin", "workload": "W2", "kind": "stdin", "prepare": True, "tickets": [1126]},
    {
        "id": "aggregate-breakdown",
        "workload": "W1",
        "argv": [
            "process",
            "aggregate",
            "h3",
            "{input}",
            "{output}",
            "--resolution",
            "5",
            "--breakdown",
            "category",
            "--breakdown-limit",
            "32",
        ],
        "tickets": [1111],
    },
    {"id": "overview", "workload": "W1", "kind": "overview", "tickets": [1123]},
    {"id": "aggregate-admin", "workload": "W2", "kind": "admin", "tickets": [1190, 550, 543]},
    {
        "id": "extract-wfs",
        "workload": "W6",
        "argv": [
            "extract",
            "wfs",
            "{url}",
            "bench:points",
            "{output}",
            "--skip-hilbert",
            "--skip-bbox",
        ],
        "tickets": [1344],
    },
    {
        "id": "add-admin",
        "workload": "W2",
        "kind": "admin-add",
        "prepare": True,
        "tickets": [543, 550],
    },
    {
        "id": "partition-admin",
        "workload": "W2",
        "kind": "admin-partition",
        "prepare": True,
        "tickets": [543, 550],
    },
    {
        "id": "aggregate-admin-native",
        "workload": "W2",
        "kind": "admin-native",
        "prepare": True,
        "tickets": [550],
    },
    {
        "id": "aggregate-admin-reprojected",
        "workload": "W2",
        "kind": "admin-reprojected",
        "prepare": True,
        "tickets": [550],
    },
    {
        "id": "inspect-remote",
        "workload": "W4",
        "prepare": True,
        "prepared_input": True,
        "capture_stdout": True,
        "argv": ["inspect", "summary", "{url}"],
        "tickets": [1348, 1351],
    },
    {
        "id": "convert-remote",
        "workload": "W4",
        "prepare": True,
        "prepared_input": True,
        "argv": ["convert", "geoparquet", "{url}", "{output}"],
        "tickets": [1348, 1351],
    },
    {
        "id": "extract-remote-bbox",
        "workload": "W4",
        "prepare": True,
        "prepared_input": True,
        "argv": ["extract", "geoparquet", "{url}", "{output}", "--bbox=-60,-30,60,30"],
        "tickets": [1348, 1351],
    },
    {
        "id": "benchmark-explain",
        "workload": "W4",
        "prepare": True,
        "prepared_input": True,
        "capture_stdout": True,
        "argv": [
            "benchmark",
            "explain",
            "{input}",
            "--format",
            "json",
            "--query",
            "SELECT count(*) FROM read_parquet({file}) WHERE bbox.xmin >= -60 AND bbox.xmax <= 60 AND bbox.ymin >= -30 AND bbox.ymax <= 30",
        ],
        "tickets": [1348, 1351],
    },
] + [
    {"id": f"api-{kind}", "workload": "W2", "kind": f"api-{kind}", "tickets": [1342]}
    for kind in ["convert", "write", "h3", "kdtree", "sort-column", "reproject", "extract"]
]


def _cli(argv):
    from geoparquet_io.cli.main import cli

    return cli.main(args=argv, prog_name="gpio", standalone_mode=False)


def invoke(spec, input_path, output, url, *, prepare=False, prepared=None):
    """Invoke the actual CLI/API; setup artifacts are excluded from measurements."""
    from pathlib import Path

    kind = spec.get("kind", "cli")
    if prepare:
        if kind == "stdin":
            from contextlib import redirect_stdout

            stream = output.parent / "input.arrow"
            with stream.open("w") as stdout, redirect_stdout(stdout):
                _cli(["extract", "geoparquet", str(input_path), "-"])
            return stream
        if spec.get("prepared_input"):
            path = output.parent / "bbox.parquet"
            unsorted = output.parent / "bbox-unsorted.parquet"
            _cli(["add", "bbox", str(input_path), str(unsorted), "--overwrite"])
            _cli(["sort", "hilbert", str(unsorted), str(path), "--overwrite"])
            return path
        if kind.startswith("admin-"):
            _admin_fixture(output.parent / "admin.parquet")
            if kind == "admin-native":
                _cli(
                    [
                        "convert",
                        "geoparquet",
                        str(input_path),
                        str(output.parent / "input.parquet"),
                        "--geoparquet-version",
                        "2.0",
                        "--overwrite",
                    ]
                )
            elif kind == "admin-reprojected":
                _cli(
                    [
                        "convert",
                        "reproject",
                        str(input_path),
                        str(output.parent / "input.parquet"),
                        "-d",
                        "EPSG:3857",
                        "--overwrite",
                    ]
                )
            return output.parent
        if kind == "overview":
            base = output.parent / "aggregate.parquet"
            _cli(["process", "aggregate", "h3", str(input_path), str(base), "--resolution", "6"])
            return base
        return None
    if kind == "overview":
        return _cli(
            [
                "process",
                "overview",
                str(prepared),
                "--levels",
                "2,3,4,5",
                "--output-dir",
                str(output),
            ]
        )
    if kind.startswith("api-"):
        from geoparquet_io.api.table import convert, read

        operation = kind.removeprefix("api-")
        table = convert(input_path) if operation == "convert" else read(input_path)
        if operation == "h3":
            table = table.add_h3(resolution=5)
        elif operation == "kdtree":
            table = table.add_kdtree(iterations=3)
        elif operation == "sort-column":
            table = table.sort_column("value")
        elif operation == "reproject":
            table = table.reproject("EPSG:3857")
        elif operation == "extract":
            table = table.extract(bbox=(-60, -30, 60, 30))
        return table.write(output / "result.parquet")
    if kind in {"stream", "stdin"}:
        import sys
        from contextlib import redirect_stdout
        from unittest.mock import patch

        if kind == "stream":
            with (output / "stream.arrow").open("w") as stdout, redirect_stdout(stdout):
                return _cli(["extract", "geoparquet", str(input_path), "-"])
        with Path(prepared).open("r") as stdin, patch.object(sys, "stdin", stdin):
            return _cli(["convert", "geojson", "-", str(output / "result.geojson")])
    if kind == "admin":
        return _admin(input_path, output)
    if kind.startswith("admin-"):
        fixture = Path(prepared)
        prepared_input = fixture / "input.parquet"
        return _admin(
            prepared_input if prepared_input.exists() else input_path,
            output,
            boundary=fixture / "admin.parquet",
            operation=kind.removeprefix("admin-"),
        )
    values = {
        "input": str(input_path),
        "output": str(output / "result.parquet"),
        "directory": str(output / "parts"),
        "geojson": str(output / "result.geojson"),
        "fgb": str(output / "result.fgb"),
        "gpkg": str(output / "result.gpkg"),
        "glob": str(Path(input_path) / "*.parquet"),
        "url": url,
    }
    argv = [arg if "{file}" in arg else arg.format(**values) for arg in spec["argv"]]
    if spec.get("capture_stdout"):
        from contextlib import redirect_stdout

        with (output / "stdout.txt").open("w") as stdout, redirect_stdout(stdout):
            return _cli(argv)
    return _cli(argv)


def _admin_fixture(boundary):
    """Write a small synthetic Overture boundary pair for offline spatial joins."""
    import json
    import struct

    import pyarrow as pa
    import pyarrow.parquet as pq

    def polygon(xmin, xmax):
        ring = [(xmin, -90), (xmax, -90), (xmax, 90), (xmin, 90), (xmin, -90)]
        return struct.pack("<BIII", 1, 3, 1, 5) + b"".join(struct.pack("<dd", *p) for p in ring)

    table = pa.table(
        {
            "country": ["west", "east"],
            "region": ["west", "east"],
            "subtype": ["country", "country"],
            "bbox": [
                {"xmin": -180.0, "xmax": 10.0, "ymin": -90.0, "ymax": 90.0},
                {"xmin": -10.0, "xmax": 180.0, "ymin": -90.0, "ymax": 90.0},
            ],
            "geometry": [polygon(-180, 10), polygon(-10, 180)],
        }
    ).replace_schema_metadata(
        {
            b"geo": json.dumps(
                {
                    "version": "1.1.0",
                    "primary_column": "geometry",
                    "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Polygon"]}},
                }
            ).encode()
        }
    )
    pq.write_table(table, boundary)


def _admin(input_path, output, *, boundary=None, operation="aggregate"):
    """Keep the real admin command paths, substituting only local source data."""
    from unittest.mock import patch

    from geoparquet_io.core.admin_datasets import AdminDatasetFactory
    from geoparquet_io.core.process.aggregate.by_admin import aggregate_by_admin

    if boundary is None:
        boundary = output.parent / "admin.parquet"
        _admin_fixture(boundary)
    factory = AdminDatasetFactory.create

    def local(dataset_name, source_path=None, verbose=False):
        return factory(dataset_name, str(boundary), verbose)

    with patch.object(AdminDatasetFactory, "create", local):
        if operation == "add":
            return _cli(
                [
                    "add",
                    "admin-divisions",
                    str(input_path),
                    str(output / "result.parquet"),
                    "--dataset",
                    "overture",
                    "--levels",
                    "country",
                    "--no-cache",
                ]
            )
        if operation == "partition":
            return _cli(
                [
                    "partition",
                    "admin",
                    str(input_path),
                    str(output / "parts"),
                    "--dataset",
                    "overture",
                    "--levels",
                    "country",
                    "--skip-analysis",
                ]
            )
        return aggregate_by_admin(
            str(input_path), str(output / "result.parquet"), breakdown="category"
        )
