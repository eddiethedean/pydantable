"""Local HTTP server: ``fetch_*`` and ``read_parquet_url`` (stdlib + pydantable).

Run::

    python docs/examples/io/http_local_fetch.py
"""

from __future__ import annotations

import os
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from pydantable import DataFrame
from pydantable.io import (
    export_parquet,
    fetch_bytes,
    fetch_csv_url,
    fetch_ndjson_url,
    fetch_parquet_url,
    read_parquet_url,
)
from pydantic import BaseModel


class ParqRow(BaseModel):
    c: int


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
        export_parquet(pq, {"c": [1, 2, 3]})
        parquet_blob = pq.read_bytes()

    server, parquet_url = _serve_blob(parquet_blob)
    try:
        assert fetch_bytes(parquet_url, experimental=True) == parquet_blob
        eager = fetch_parquet_url(parquet_url, experimental=True)
        assert eager["c"] == [1, 2, 3]

        root = read_parquet_url(parquet_url, experimental=True)
        try:
            df = DataFrame[ParqRow]._from_scan_root(root)
            assert [r.c for r in df.collect()] == [1, 2, 3]
        finally:
            os.unlink(root.path)
    finally:
        server.shutdown()
        server.server_close()

    csv_blob = b"a,b\n3,4\n"
    server2, csv_url = _serve_blob(csv_blob)
    try:
        csv_data = fetch_csv_url(csv_url, experimental=True)
        assert [int(x) for x in csv_data["a"]] == [3]
        assert [int(x) for x in csv_data["b"]] == [4]
    finally:
        server2.shutdown()
        server2.server_close()

    ndjson_blob = b'{"p":1}\n{"p":2}\n'
    server3, nd_url = _serve_blob(ndjson_blob)
    try:
        nd = fetch_ndjson_url(nd_url, experimental=True)
        assert [int(x) for x in nd["p"]] == [1, 2]
    finally:
        server3.shutdown()
        server3.server_close()

    print("http_local_fetch: ok")


if __name__ == "__main__":
    main()
