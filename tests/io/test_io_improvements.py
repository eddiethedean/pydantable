"""Tests for I/O backlog: limits, JSON helpers, model shims, extension errors."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from pydantable import DataFrame, DataFrameModel, MissingRustExtensionError
from pydantable.io import (
    aexport_json,
    afetch_sql_raw,
    amaterialize_json,
    aread_parquet_url_ctx,
    awrite_sql_raw,
    export_json,
    export_parquet,
    fetch_bytes,
    materialize_csv,
    materialize_json,
    materialize_parquet,
    read_from_object_store,
    read_json,
    read_ndjson,
    read_parquet_url,
    read_parquet_url_ctx,
    write_sql_raw,
)

from tests._support.paths import repo_root


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


def test_materialize_json_empty_and_whitespace_only(tmp_path: Path) -> None:
    empty = tmp_path / "e.json"
    empty.write_text("", encoding="utf-8")
    assert materialize_json(empty) == {}

    blank = tmp_path / "w.json"
    blank.write_text("  \n\t  ", encoding="utf-8")
    assert materialize_json(blank) == {}


def test_materialize_json_array_validation_errors(tmp_path: Path) -> None:
    bad_scalar = tmp_path / "bad1.json"
    bad_scalar.write_text("[1, 2]", encoding="utf-8")
    with pytest.raises(ValueError, match="must be JSON objects"):
        materialize_json(bad_scalar)

    bad_mixed = tmp_path / "bad2.json"
    bad_mixed.write_text('[{"x": 1}, 2]', encoding="utf-8")
    with pytest.raises(ValueError, match="must be JSON objects"):
        materialize_json(bad_mixed)


def test_materialize_parquet_rust_disallows_column_projection() -> None:
    with pytest.raises(ValueError, match="Rust Parquet read"):
        materialize_parquet("/any/path.parquet", columns=["c"], engine="rust")


def test_export_json_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "out.json"
    export_json(path, {"a": [1, 2], "b": ["x", "y"]})
    got = json.loads(path.read_text(encoding="utf-8"))
    assert got == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]


@pytest.mark.network
def test_fetch_bytes_max_bytes(
    http_serve: Callable[[type[BaseHTTPRequestHandler]], str],
) -> None:
    class H(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"12345")

        def log_message(self, *args: object) -> None:
            return

    base = http_serve(H)
    url = f"{base}/b"
    assert fetch_bytes(url, experimental=True, max_bytes=10) == b"12345"
    with pytest.raises(ValueError, match="max_bytes=3"):
        fetch_bytes(url, experimental=True, max_bytes=3)


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


def test_subprocess_read_parquet_raises_missing_rust_when_core_incomplete() -> None:
    """Fresh interpreter with a stub native extension (no ``ScanFileRoot``)."""
    root = repo_root()
    code = """
import os, sys, tempfile, types
from pathlib import Path

root = Path(os.environ["PYDANTABLE_TEST_ROOT"])
sys.path.insert(0, str(root / "python"))
sys.modules["pydantable_native._core"] = types.ModuleType("pydantable_native._core")

from pydantable.io import read_parquet
from pydantable import MissingRustExtensionError

p = Path(tempfile.mkdtemp()) / "missing.parquet"
try:
    read_parquet(p)
except MissingRustExtensionError as e:
    text = str(e)
    if "ScanFileRoot" in text or "native extension" in text.lower():
        sys.exit(0)
    sys.exit(3)
sys.exit(2)
"""
    env = {**os.environ, "PYDANTABLE_TEST_ROOT": str(root)}
    proc = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


@pytest.mark.network
def test_read_parquet_url_ctx_cleans_temp(
    tmp_path: Path,
    http_serve: Callable[[type[BaseHTTPRequestHandler]], str],
) -> None:
    pytest.importorskip("pydantable_native._core")
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

    base = http_serve(H)
    url = f"{base}/p"
    with read_parquet_url_ctx(
        DataFrame[_Mini._SchemaModel], url, experimental=True
    ) as df:
        assert df.collect()[0].k == 1
        pth = str(df._root_data.path)
        assert os.path.isfile(pth)
    assert not os.path.isfile(pth)


@pytest.mark.asyncio
@pytest.mark.network
async def test_aread_parquet_url_ctx_cleans_temp(
    tmp_path: Path,
    http_serve: Callable[[type[BaseHTTPRequestHandler]], str],
) -> None:
    pytest.importorskip("pydantable_native._core")
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

    base = http_serve(H)
    url = f"{base}/p"
    async with aread_parquet_url_ctx(
        DataFrame[_Mini._SchemaModel], url, experimental=True
    ) as df:
        rows = await df.acollect()
        assert rows[0].k == 1
        pth = str(df._root_data.path)
        assert os.path.isfile(pth)
    assert not os.path.isfile(pth)


@pytest.mark.asyncio
async def test_amaterialize_json_matches_sync(tmp_path: Path) -> None:
    p = tmp_path / "z.json"
    p.write_text('[{"x": 1}]', encoding="utf-8")
    sync = materialize_json(p)
    async_got = await amaterialize_json(p)
    assert async_got == sync


@pytest.mark.asyncio
async def test_aexport_json_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "async.json"
    await aexport_json(path, {"a": [1], "b": ["z"]})
    got = json.loads(path.read_text(encoding="utf-8"))
    assert got == [{"a": 1, "b": "z"}]


def test_dataframe_model_export_write_sql_sqlite(tmp_path: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_path / "t.db"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t2 (k INTEGER)"))
    out_pq = tmp_path / "e.pq"
    _Mini.export_parquet(out_pq, {"k": [2]})
    write_sql_raw({"k": [3]}, "t2", eng, if_exists="append")
    with eng.connect() as c:
        n = c.execute(text("SELECT COUNT(*) FROM t2")).scalar()
    assert n == 1


@pytest.mark.asyncio
async def test_afetch_sql_awrite_sql_sqlite(tmp_path: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_path / "async_model.db"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t_async (k INTEGER)"))
        conn.execute(text("INSERT INTO t_async VALUES (5)"))
    cols = await afetch_sql_raw("SELECT k FROM t_async", eng)
    m = _Mini(cols)
    assert m.collect()[0].k == 5
    await awrite_sql_raw({"k": [6]}, "t_async", eng, if_exists="append")
    with eng.connect() as c:
        rows = c.execute(text("SELECT k FROM t_async ORDER BY k")).fetchall()
    assert [r[0] for r in rows] == [5, 6]


def test_read_parquet_url_fdopen_failure_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import pydantable.io as io_mod

    monkeypatch.setattr(io_mod, "fetch_bytes", lambda *a, **k: b"pq")

    def boom(*a: object, **k: object) -> object:
        raise OSError("fdopen failed")

    monkeypatch.setattr(io_mod.os, "fdopen", boom)
    with pytest.raises(OSError, match="fdopen failed"):
        read_parquet_url("http://example.com/f", experimental=True)


def test_read_parquet_url_scan_failure_unlinks_temp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pydantable_native._core")
    import pydantable.io as io_mod

    monkeypatch.setattr(io_mod, "fetch_bytes", lambda *a, **k: b"pq")

    def bad_scan(*a: object, **k: object) -> object:
        raise RuntimeError("no scan")

    monkeypatch.setattr(io_mod, "_scan_file_root", bad_scan)
    with pytest.raises(RuntimeError, match="no scan"):
        read_parquet_url("http://example.com/f", experimental=True)


def test_read_parquet_url_ctx_requires_nonempty_scan_path() -> None:
    with (
        patch(
            "pydantable.io.read_parquet_url",
            return_value=SimpleNamespace(path=""),
        ),
        pytest.raises(RuntimeError, match="path is empty"),
        read_parquet_url_ctx(
            DataFrame[_Mini._SchemaModel], "http://x", experimental=True
        ),
    ):
        pass


@pytest.mark.asyncio
async def test_aread_parquet_url_ctx_requires_nonempty_scan_path() -> None:
    import pydantable.io as io_mod

    async def fake_aread(*a: object, **k: object) -> object:
        return SimpleNamespace(path="")

    with (
        patch.object(io_mod, "aread_parquet_url", fake_aread),
        pytest.raises(RuntimeError, match="path is empty"),
    ):
        async with aread_parquet_url_ctx(
            DataFrame[_Mini._SchemaModel], "http://x", experimental=True
        ):
            pass


@pytest.mark.asyncio
async def test_materialize_csv_use_rap_rejects_when_event_loop_running(
    tmp_path: Path,
) -> None:
    pytest.importorskip("rapcsv")
    path = tmp_path / "rap.csv"
    path.write_text("a\n1\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="aread_csv_rap"):
        materialize_csv(path, use_rap=True, engine="auto")
