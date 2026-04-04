"""Local HTTP server: Parquet asset, CSV legacy report, NDJSON log (temp files).

Uses **methods** on ``DataFrameModel``; stdlib ``urllib`` to assert the wire bytes.

Run::

    python docs/examples/io/http_local_fetch.py
"""

from __future__ import annotations

import os
import tempfile
import threading
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from pydantable import DataFrameModel


class ProductMetric(DataFrameModel):
    """Row materialized from a Parquet blob served over HTTP."""

    units_sold: int


class LegacyCsvRow(DataFrameModel):
    """Two-column report from an older system that only serves CSV."""

    region_id: int
    revenue_usd: int


class LogLine(DataFrameModel):
    """Single field from a newline-delimited log download."""

    trace_id: int


def _serve_blob(blob: bytes) -> tuple[HTTPServer, str]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Length", str(len(blob)))
            self.end_headers()
            self.wfile.write(blob)

        def log_message(self, *args: object) -> None:
            return

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_port}/blob"
    return server, url


def main() -> None:
    with tempfile.TemporaryDirectory() as td:
        pq = Path(td) / "metrics.parquet"
        ProductMetric({"units_sold": [10, 25, 3]}).write_parquet(str(pq))
        parquet_blob = pq.read_bytes()

    server, parquet_url = _serve_blob(parquet_blob)
    try:
        assert urllib.request.urlopen(parquet_url).read() == parquet_blob

        df = ProductMetric.read_parquet_url(parquet_url, experimental=True)
        try:
            assert [r.units_sold for r in df.collect()] == [10, 25, 3]
        finally:
            os.unlink(df._df._root_data.path)
    finally:
        server.shutdown()
        server.server_close()

    csv_blob = b"region_id,revenue_usd\n3,45000\n"
    server2, csv_url = _serve_blob(csv_blob)
    try:
        data = urllib.request.urlopen(csv_url).read()
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            f.write(data)
            csv_path = f.name
        try:
            d = LegacyCsvRow.read_csv(csv_path).to_dict()
            assert [int(x) for x in d["region_id"]] == [3]
            assert [int(x) for x in d["revenue_usd"]] == [45000]
        finally:
            os.unlink(csv_path)
    finally:
        server2.shutdown()
        server2.server_close()

    ndjson_blob = b'{"trace_id":9001}\n{"trace_id":9002}\n'
    server3, nd_url = _serve_blob(ndjson_blob)
    try:
        data = urllib.request.urlopen(nd_url).read()
        with tempfile.NamedTemporaryFile(suffix=".ndjson", delete=False) as f:
            f.write(data)
            nd_path = f.name
        try:
            d = LogLine.read_ndjson(nd_path).to_dict()
            assert [int(x) for x in d["trace_id"]] == [9001, 9002]
        finally:
            os.unlink(nd_path)
    finally:
        server3.shutdown()
        server3.server_close()

    print("http_local_fetch: ok")


if __name__ == "__main__":
    main()
