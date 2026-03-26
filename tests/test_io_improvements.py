"""Tests for I/O backlog: limits, JSON helpers, model shims, extension errors."""

from __future__ import annotations

import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest
from pydantable import DataFrame, DataFrameModel, MissingRustExtensionError
from pydantable.io import (
    export_json,
    export_parquet,
    fetch_bytes,
    materialize_json,
    read_from_object_store,
    read_json,
    read_ndjson,
    read_parquet_url_ctx,
)


class _Mini(DataFrameModel):
    k: int


def test_read_json_alias_matches_ndjson(tmp_path: Path) -> None:
    p = tmp_path / "x.ndjson"
    p.write_text('{"k": 1}\n{"k": 2}\n', encoding="utf-8")
    r1 = read_ndjson(p)
    r2 = read_json(p)
    assert type(r1) is type(r2)
    assert getattr(r1, "path", None) == getattr(r2, "path", None)


def test_materialize_json_array_and_lines(tmp_path: Path) -> None:
    arr = tmp_path / "a.json"
    arr.write_text('[{"x": 1}, {"x": 2}]', encoding="utf-8")
    assert materialize_json(arr) == {"x": [1, 2]}

    lines = tmp_path / "b.ndjson"
    lines.write_text('{"x": 3}\n', encoding="utf-8")
    assert materialize_json(lines) == {"x": [3]}


def test_export_json_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "out.json"
    export_json(path, {"a": [1, 2], "b": ["x", "y"]})
    import json

    got = json.loads(path.read_text(encoding="utf-8"))
    assert got == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]


def test_fetch_bytes_max_bytes() -> None:
    class H(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"12345")

        def log_message(self, *args: object) -> None:
            return

    srv = HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{srv.server_port}/b"
    try:
        assert fetch_bytes(url, experimental=True, max_bytes=10) == b"12345"
        with pytest.raises(ValueError, match="max_bytes=3"):
            fetch_bytes(url, experimental=True, max_bytes=3)
    finally:
        srv.shutdown()
        srv.server_close()


def test_read_from_object_store_max_bytes(tmp_path: Path) -> None:
    pytest.importorskip("fsspec")
    csv_path = tmp_path / "t.csv"
    csv_path.write_text("a\n1\n", encoding="utf-8")
    uri_csv = csv_path.resolve().as_uri()
    out = read_from_object_store(
        uri_csv, experimental=True, format="csv", max_bytes=100
    )
    assert "a" in out
    with pytest.raises(ValueError, match="max_bytes=3"):
        read_from_object_store(uri_csv, experimental=True, format="csv", max_bytes=3)


def test_missing_rust_extension_is_notimplemented_subclass() -> None:
    assert issubclass(MissingRustExtensionError, NotImplementedError)
    e = MissingRustExtensionError()
    assert isinstance(e, NotImplementedError)


def test_read_parquet_url_ctx_cleans_temp(tmp_path: Path) -> None:
    pytest.importorskip("pydantable._core")
    pq = tmp_path / "in.pq"
    export_parquet(pq, {"k": [1]})
    blob = pq.read_bytes()

    class H(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Length", str(len(blob)))
            self.end_headers()
            self.wfile.write(blob)

        def log_message(self, *args: object) -> None:
            return

    srv = HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{srv.server_port}/p"
    try:
        with read_parquet_url_ctx(
            DataFrame[_Mini._SchemaModel], url, experimental=True
        ) as df:
            assert df.collect()[0].k == 1
            pth = str(df._root_data.path)
            assert os.path.isfile(pth)
        assert not os.path.isfile(pth)
    finally:
        srv.shutdown()
        srv.server_close()


def test_dataframe_model_export_write_sql_sqlite(tmp_path: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_path / "t.db"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t2 (k INTEGER)"))
    out_pq = tmp_path / "e.pq"
    _Mini.export_parquet(out_pq, {"k": [2]})
    _Mini.write_sql({"k": [3]}, "t2", eng, if_exists="append")
    with eng.connect() as c:
        n = c.execute(text("SELECT COUNT(*) FROM t2")).scalar()
    assert n == 1


def test_from_sql_is_alias_docstring() -> None:
    assert "fetch_sql" in (_Mini.from_sql.__doc__ or "")
