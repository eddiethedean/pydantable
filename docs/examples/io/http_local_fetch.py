"""Local HTTP server: Parquet bytes, lazy URL read, CSV/NDJSON temp paths.

Uses **methods** on ``DataFrameModel``; stdlib ``urllib`` for raw byte checks.

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


class ParqRow(DataFrameModel):
    c: int


class CsvRow(DataFrameModel):
    a: int
    b: int


class NdRow(DataFrameModel):
    p: int


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
        pq = Path(td) / "served.parquet"
        ParqRow({"c": [1, 2, 3]}).write_parquet(str(pq))
        parquet_blob = pq.read_bytes()

    server, parquet_url = _serve_blob(parquet_blob)
    try:
        assert urllib.request.urlopen(parquet_url).read() == parquet_blob

        eager = ParqRow.materialize_parquet(parquet_blob)
        assert eager.to_dict()["c"] == [1, 2, 3]

        df = ParqRow.read_parquet_url(parquet_url, experimental=True)
        try:
            assert [r.c for r in df.collect()] == [1, 2, 3]
        finally:
            os.unlink(df._df._root_data.path)
    finally:
        server.shutdown()
        server.server_close()

    csv_blob = b"a,b\n3,4\n"
    server2, csv_url = _serve_blob(csv_blob)
    try:
        data = urllib.request.urlopen(csv_url).read()
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            f.write(data)
            csv_path = f.name
        try:
            tbl = CsvRow.materialize_csv(csv_path)
            d = tbl.to_dict()
            assert [int(x) for x in d["a"]] == [3]
            assert [int(x) for x in d["b"]] == [4]
        finally:
            os.unlink(csv_path)
    finally:
        server2.shutdown()
        server2.server_close()

    ndjson_blob = b'{"p":1}\n{"p":2}\n'
    server3, nd_url = _serve_blob(ndjson_blob)
    try:
        data = urllib.request.urlopen(nd_url).read()
        with tempfile.NamedTemporaryFile(suffix=".ndjson", delete=False) as f:
            f.write(data)
            nd_path = f.name
        try:
            tbl = NdRow.materialize_ndjson(nd_path)
            assert [int(x) for x in tbl.to_dict()["p"]] == [1, 2]
        finally:
            os.unlink(nd_path)
    finally:
        server3.shutdown()
        server3.server_close()

    print("http_local_fetch: ok")


if __name__ == "__main__":
    main()
