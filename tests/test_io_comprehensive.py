"""I/O round-trips and transport helpers (Rust core + Python façade)."""

from __future__ import annotations

import json
import tempfile as _stdlib_tempfile
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import pytest
from conftest import http_server_thread
from pydantable import DataFrame, DataFrameModel
from pydantable.io import (
    StreamingColumns,
    aexport_ipc,
    aexport_ndjson,
    aexport_parquet,
    afetch_sql,
    aiter_sql,
    amaterialize_csv,
    amaterialize_ipc,
    amaterialize_ndjson,
    amaterialize_parquet,
    arrow_table_to_column_dict,
    awrite_sql,
    awrite_sql_batches,
    export_csv,
    export_ipc,
    export_ndjson,
    export_parquet,
    fetch_bytes,
    fetch_csv_url,
    fetch_ndjson_url,
    fetch_parquet_url,
    fetch_sql,
    iter_sql,
    materialize_csv,
    materialize_ipc,
    materialize_ndjson,
    materialize_parquet,
    read_parquet_url,
    record_batch_to_column_dict,
    write_sql,
    write_sql_batches,
)
from pydantic import BaseModel


@pytest.fixture
def tmp_dir(tmp_path: Path) -> Path:
    return tmp_path


def test_csv_roundtrip_rust_or_fallback(tmp_dir: Path) -> None:
    path = tmp_dir / "a.csv"
    data = {"n": [1, 2], "s": ["a", "b"]}
    export_csv(path, data, engine="rust")
    got = materialize_csv(path, engine="auto")
    assert got["s"] == ["a", "b"]
    assert [int(x) for x in got["n"]] == [1, 2]


def test_export_csv_stdlib_fallback_when_rust_importerror(
    tmp_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``engine=auto`` uses stdlib writer if Rust path raises ``ImportError``."""
    import pydantable.io._core_io as core_io

    real = core_io.rust_write_csv_path

    def _raise(*args: object, **kwargs: object) -> None:
        raise ImportError("no rust csv")

    monkeypatch.setattr(core_io, "rust_write_csv_path", _raise)
    path = tmp_dir / "fb.csv"
    data = {"x": [9], "y": ["z"]}
    export_csv(path, data, engine="auto")
    monkeypatch.setattr(core_io, "rust_write_csv_path", real)
    got = materialize_csv(path, engine="auto")
    assert got == data


def test_export_ndjson_stdlib_fallback_when_rust_importerror(
    tmp_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import pydantable.io._core_io as core_io

    real = core_io.rust_write_ndjson_path

    def _raise(*args: object, **kwargs: object) -> None:
        raise ImportError("no rust ndjson")

    monkeypatch.setattr(core_io, "rust_write_ndjson_path", _raise)
    path = tmp_dir / "fb.ndjson"
    data = {"a": [1], "b": [True]}
    export_ndjson(path, data, engine="auto")
    monkeypatch.setattr(core_io, "rust_write_ndjson_path", real)
    got = materialize_ndjson(path, engine="auto")
    assert got == data


def test_export_parquet_pyarrow_fallback_when_rust_importerror(
    tmp_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import pydantable.io._core_io as core_io

    real = core_io.rust_write_parquet_path

    def _raise(*args: object, **kwargs: object) -> None:
        raise ImportError("no rust parquet")

    monkeypatch.setattr(core_io, "rust_write_parquet_path", _raise)
    path = tmp_dir / "fb.parquet"
    data = {"u": [1], "v": ["x"]}
    export_parquet(path, data, engine="auto")
    monkeypatch.setattr(core_io, "rust_write_parquet_path", real)
    got = materialize_parquet(path, engine="auto")
    assert got == data


def test_export_ipc_pyarrow_fallback_when_rust_importerror(
    tmp_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import pydantable.io._core_io as core_io

    real = core_io.rust_write_ipc_path

    def _raise(*args: object, **kwargs: object) -> None:
        raise ImportError("no rust ipc")

    monkeypatch.setattr(core_io, "rust_write_ipc_path", _raise)
    path = tmp_dir / "fb.arrow"
    data = {"a": [2]}
    export_ipc(path, data, engine="auto")
    monkeypatch.setattr(core_io, "rust_write_ipc_path", real)
    got = materialize_ipc(path, engine="auto")
    assert got == data


def test_parquet_roundtrip(tmp_dir: Path) -> None:
    path = tmp_dir / "f.parquet"
    data = {"x": [10, 20], "y": ["p", "q"]}
    export_parquet(path, data)
    got = materialize_parquet(path)
    assert got == data


def test_ndjson_roundtrip(tmp_dir: Path) -> None:
    path = tmp_dir / "f.ndjson"
    data = {"a": [True, False], "b": [1.5, 2.5]}
    export_ndjson(path, data)
    got = materialize_ndjson(path)
    assert got == data


def test_ipc_roundtrip(tmp_dir: Path) -> None:
    path = tmp_dir / "f.arrow"
    data = {"k": [1, 2, 3]}
    export_ipc(path, data)
    got = materialize_ipc(path)
    assert got == data


@pytest.mark.asyncio
async def test_amaterialize_parquet(tmp_dir: Path) -> None:
    path = tmp_dir / "p.parquet"
    data = {"z": [7, 8]}
    export_parquet(path, data)
    got = await amaterialize_parquet(path)
    assert got == data


@pytest.mark.asyncio
async def test_amaterialize_csv(tmp_dir: Path) -> None:
    path = tmp_dir / "c.csv"
    data = {"u": [1, 2], "v": [3, 4]}
    export_csv(path, data)
    got = await amaterialize_csv(path)
    assert [int(x) for x in got["u"]] == [1, 2]
    assert [int(x) for x in got["v"]] == [3, 4]


def test_streaming_columns_lazy_merge_matches_flat_dict() -> None:
    sc = StreamingColumns(
        [{"a": [1, 2], "b": [10, 20]}, {"a": [3], "b": [30]}, {"a": [], "b": []}]
    )
    assert list(sc) == ["a", "b"]
    assert len(sc) == 2
    assert sc["a"] == [1, 2, 3]
    assert sc["b"] == [10, 20, 30]
    # second access uses cache
    assert sc["a"] is sc["a"]
    assert sc.to_dict() == {"a": [1, 2, 3], "b": [10, 20, 30]}
    assert len(sc.batches()) == 3


def test_streaming_columns_empty_batches_is_empty_mapping() -> None:
    sc = StreamingColumns([])
    assert list(sc) == []
    assert len(sc) == 0
    assert sc.to_dict() == {}


def test_read_sql_write_sql_sqlite(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "t.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE m (id INTEGER, name TEXT)"))
    write_sql({"id": [1], "name": ["x"]}, "m", eng, if_exists="append")
    out = fetch_sql("SELECT * FROM m", eng)
    assert out == {"id": [1], "name": ["x"]}


def test_fetch_sql_auto_stream_threshold_returns_streaming_columns(
    tmp_dir: Path,
) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "auto_stream.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (n INTEGER NOT NULL)"))
        conn.execute(
            text(
                "WITH RECURSIVE seq(x) AS ("
                "SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x < 50"
                ") "
                "INSERT INTO t SELECT x FROM seq"
            )
        )

    out = fetch_sql(
        "SELECT n FROM t ORDER BY n",
        eng,
        auto_stream=True,
        auto_stream_threshold_rows=10,
        batch_size=7,
    )
    assert hasattr(out, "to_dict")
    got = out.to_dict()  # type: ignore[union-attr]
    assert got["n"][0] == 1
    assert got["n"][-1] == 50


def test_fetch_sql_auto_stream_disabled_returns_plain_dict(
    tmp_dir: Path,
) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "no_auto_stream.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (n INTEGER NOT NULL)"))
        conn.execute(
            text(
                "WITH RECURSIVE seq(x) AS ("
                "SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x < 50"
                ") "
                "INSERT INTO t SELECT x FROM seq"
            )
        )

    out = fetch_sql(
        "SELECT n FROM t ORDER BY n",
        eng,
        auto_stream=False,
        auto_stream_threshold_rows=10,
        batch_size=7,
    )
    assert isinstance(out, dict)
    assert not isinstance(out, StreamingColumns)
    assert out["n"][0] == 1
    assert out["n"][-1] == 50


def test_fetch_sql_streams_but_materializes_final_dict(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "many.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (n INTEGER NOT NULL)"))
        # enough rows to exercise internal fetchmany loop
        conn.execute(
            text(
                "WITH RECURSIVE seq(x) AS ("
                "SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x < 5000"
                ") "
                "INSERT INTO t SELECT x FROM seq"
            )
        )

    got = fetch_sql("SELECT n FROM t ORDER BY n", eng)
    assert got["n"][0] == 1
    assert got["n"][-1] == 5000


def test_fetch_sql_large_result_spans_multiple_internal_batches(tmp_dir: Path) -> None:
    """
    fetch_sql should not rely on result.mappings().all().

    We can't easily assert fetchmany() was called without deep mocking, but we *can*
    ensure correctness on a result set larger than the internal batch size.
    """
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "huge.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (n INTEGER NOT NULL)"))
        # > 65_536 rows to span multiple internal fetchmany batches.
        conn.execute(
            text(
                "WITH RECURSIVE seq(x) AS ("
                "SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x < 70000"
                ") "
                "INSERT INTO t SELECT x FROM seq"
            )
        )

    got = fetch_sql("SELECT n FROM t ORDER BY n", eng)
    assert got["n"][0] == 1
    assert got["n"][-1] == 70000


def test_write_sql_appends_in_multiple_executemany_roundtrips(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, event, text

    db = tmp_dir / "write_chunks.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (n INTEGER NOT NULL)"))

    insert_execs: list[str] = []

    def before_cursor_execute(
        _conn, _cursor, statement, _parameters, _context, _executemany
    ):
        stmt = str(statement or "")
        if stmt.lstrip().upper().startswith("INSERT"):
            insert_execs.append(stmt)

    event.listen(eng, "before_cursor_execute", before_cursor_execute)
    try:
        # Internal chunk size is 10_000; force 3 chunks.
        data = {"n": list(range(1, 25_051))}
        write_sql(data, "t", eng, if_exists="append")
    finally:
        event.remove(eng, "before_cursor_execute", before_cursor_execute)

    assert len(insert_execs) == 3
    out = fetch_sql("SELECT COUNT(*) AS c FROM t", eng)
    assert out["c"] == [25_050]


def test_iter_sql_batches_sqlite(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "batch.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (n INTEGER NOT NULL)"))
        # insert a size that forces multiple batches with small batch_size
        conn.execute(
            text(
                "WITH RECURSIVE seq(x) AS ("
                "SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x < 25"
                ") "
                "INSERT INTO t SELECT x FROM seq"
            )
        )

    batches = list(iter_sql("SELECT n FROM t ORDER BY n", eng, batch_size=7))
    assert len(batches) >= 3
    flat = [x for b in batches for x in b["n"]]
    assert flat == list(range(1, 26))


def test_write_sql_batches_appends_all_rows(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "sink.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (n INTEGER NOT NULL)"))

    batches = [{"n": [1, 2, 3]}, {"n": [4]}, {"n": [5, 6]}]
    write_sql_batches(batches, "t", eng, if_exists="append")
    out = fetch_sql("SELECT n FROM t ORDER BY n", eng)
    out2 = out.to_dict() if hasattr(out, "to_dict") else out
    assert out2["n"] == [1, 2, 3, 4, 5, 6]


@pytest.mark.asyncio
async def test_awrite_sql_batches_appends_all_rows(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "asink.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (n INTEGER NOT NULL)"))

    async def _gen():
        yield {"n": [1, 2]}
        yield {"n": [3]}

    await awrite_sql_batches(_gen(), "t", eng, if_exists="append")
    out = fetch_sql("SELECT n FROM t ORDER BY n", eng)
    out2 = out.to_dict() if hasattr(out, "to_dict") else out
    assert out2["n"] == [1, 2, 3]


def test_iter_sql_empty_result_yields_nothing(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "empty.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (n INTEGER NOT NULL)"))

    assert list(iter_sql("SELECT n FROM t WHERE 1=0", eng, batch_size=10)) == []


def test_iter_sql_parameters_and_connection_bind(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "params.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (n INTEGER NOT NULL)"))
        conn.execute(text("INSERT INTO t VALUES (1), (2), (3), (4), (5)"))
        batches = list(
            iter_sql(
                "SELECT n FROM t WHERE n >= :min_n ORDER BY n",
                conn,
                parameters={"min_n": 3},
                batch_size=2,
            )
        )

    flat = [x for b in batches for x in b["n"]]
    assert flat == [3, 4, 5]


def test_iter_sql_url_bind(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "url.sqlite"
    url = f"sqlite:///{db}"
    eng = create_engine(url)
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (n INTEGER NOT NULL)"))
        conn.execute(text("INSERT INTO t VALUES (9), (10)"))

    batches = list(iter_sql("SELECT n FROM t ORDER BY n", url, batch_size=1))
    assert [x for b in batches for x in b["n"]] == [9, 10]


def test_iter_sql_batch_size_validation(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine

    eng = create_engine(f"sqlite:///{tmp_dir / 'bs.sqlite'}")
    with pytest.raises(ValueError, match="batch_size"):
        _ = list(iter_sql("SELECT 1", eng, batch_size=0))


def test_iter_sql_respects_fetch_batch_size_env(
    tmp_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("sqlalchemy")
    monkeypatch.setenv("PYDANTABLE_SQL_FETCH_BATCH_SIZE", "5")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "env_bs.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (n INTEGER NOT NULL)"))
        conn.execute(
            text(
                "WITH RECURSIVE seq(x) AS ("
                "SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x < 12"
                ") "
                "INSERT INTO t SELECT x FROM seq"
            )
        )

    batches = list(iter_sql("SELECT n FROM t ORDER BY n", eng))
    assert len(batches) == 3
    flat = [x for b in batches for x in b["n"]]
    assert flat == list(range(1, 13))  # 12 rows → batches of 5, 5, 2


def test_iter_sql_rejects_bad_fetch_batch_size_env(
    tmp_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("sqlalchemy")
    monkeypatch.setenv("PYDANTABLE_SQL_FETCH_BATCH_SIZE", "not-an-int")
    from sqlalchemy import create_engine

    eng = create_engine(f"sqlite:///{tmp_dir / 'badenv.sqlite'}")
    with pytest.raises(ValueError, match="PYDANTABLE_SQL_FETCH_BATCH_SIZE"):
        _ = list(iter_sql("SELECT 1", eng))


def test_write_sql_chunk_size_zero_kwarg_rejected(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "chunk0.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (n INTEGER NOT NULL)"))
    with pytest.raises(ValueError, match="chunk_size"):
        write_sql({"n": [1]}, "t", eng, chunk_size=0)


def test_write_sql_rejects_bad_write_chunk_size_env(
    tmp_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("sqlalchemy")
    monkeypatch.setenv("PYDANTABLE_SQL_WRITE_CHUNK_SIZE", "0")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "bad_chunk_env.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (n INTEGER NOT NULL)"))
    with pytest.raises(ValueError, match="PYDANTABLE_SQL_WRITE_CHUNK_SIZE"):
        write_sql({"n": [1]}, "t", eng)


def test_write_sql_batches_empty_iterator_does_not_touch_table(
    tmp_dir: Path,
) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "empty_batches.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (n INTEGER NOT NULL)"))

    write_sql_batches(iter(()), "t", eng, if_exists="append")
    out = fetch_sql("SELECT COUNT(*) AS c FROM t", eng)
    plain = out.to_dict() if hasattr(out, "to_dict") else out
    assert plain["c"] == [0]


def test_write_sql_batches_accepts_dataframe_model_batches(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    class _BatchDF(DataFrameModel):
        n: int

    db = tmp_dir / "dfm_batches.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (n INTEGER NOT NULL)"))

    write_sql_batches(
        [_BatchDF({"n": [1, 2]}), _BatchDF({"n": [3]})],
        "t",
        eng,
        if_exists="append",
    )
    out = fetch_sql("SELECT n FROM t ORDER BY n", eng)
    plain = out.to_dict() if hasattr(out, "to_dict") else out
    assert plain["n"] == [1, 2, 3]


@pytest.mark.asyncio
async def test_awrite_sql_batches_empty_iterator_does_not_touch_table(
    tmp_dir: Path,
) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "aempty_batches.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (n INTEGER NOT NULL)"))

    async def _empty():
        for _ in range(0):
            yield {"n": [1]}

    await awrite_sql_batches(_empty(), "t", eng, if_exists="append")
    out = fetch_sql("SELECT COUNT(*) AS c FROM t", eng)
    plain = out.to_dict() if hasattr(out, "to_dict") else out
    assert plain["c"] == [0]


@pytest.mark.asyncio
async def test_aiter_sql_batch_size_zero_raises(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine

    eng = create_engine(f"sqlite:///{tmp_dir / 'az.sqlite'}")
    with pytest.raises(ValueError, match="batch_size"):
        async for _ in aiter_sql("SELECT 1", eng, batch_size=0):
            pass


@pytest.mark.asyncio
async def test_aiter_sql_batches_sqlite(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "abatch.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (n INTEGER NOT NULL)"))
        conn.execute(
            text(
                "WITH RECURSIVE seq(x) AS ("
                "SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x < 20"
                ") "
                "INSERT INTO t SELECT x FROM seq"
            )
        )

    out: list[int] = []
    async for b in aiter_sql("SELECT n FROM t ORDER BY n", eng, batch_size=6):
        out.extend(b["n"])
    assert out == list(range(1, 21))


@pytest.mark.asyncio
async def test_aiter_sql_propagates_sql_errors(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine
    from sqlalchemy.exc import SQLAlchemyError

    db = tmp_dir / "err.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with pytest.raises(SQLAlchemyError):
        async for _b in aiter_sql(
            "SELECT definitely_not_a_column FROM missing_table", eng
        ):
            pass


@pytest.mark.network
def test_fetch_bytes_and_fetch_parquet_url(tmp_dir: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pqw

    pq_path = tmp_dir / "local.parquet"
    table = pa.table({"c": [1, 2, 3]})
    pqw.write_table(table, pq_path)
    raw = pq_path.read_bytes()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *args: object) -> None:
            return

    server, _ = http_server_thread(Handler)
    try:
        url = f"http://127.0.0.1:{server.server_port}/f.parquet"
        assert fetch_bytes(url, experimental=True) == raw
        got = fetch_parquet_url(url, experimental=True)
        assert got["c"] == [1, 2, 3]
    finally:
        server.shutdown()
        server.server_close()


def test_ndjson_lines_file(tmp_dir: Path) -> None:
    path = tmp_dir / "lines.ndjson"
    lines = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    path.write_text("\n".join(json.dumps(x) for x in lines), encoding="utf-8")
    got = materialize_ndjson(path, engine="auto")
    assert got["a"] == [1, 3]
    assert got["b"] == [2, 4]


def test_ndjson_empty_and_blank_lines(tmp_dir: Path) -> None:
    path = tmp_dir / "empty.ndjson"
    path.write_text("\n\n  \n", encoding="utf-8")
    assert materialize_ndjson(path) == {}


def test_ndjson_sparse_object_keys(tmp_dir: Path) -> None:
    """Each line is a JSON object; missing keys become None in column lists."""
    path = tmp_dir / "sparse.ndjson"
    path.write_text(
        json.dumps({"a": 1, "b": 2}) + "\n" + json.dumps({"b": 3, "c": 4}) + "\n",
        encoding="utf-8",
    )
    got = materialize_ndjson(path)
    assert got["a"] == [1, None]
    assert got["b"] == [2, 3]
    assert got["c"] == [None, 4]


def test_fetch_bytes_requires_experimental(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PYDANTABLE_IO_EXPERIMENTAL", raising=False)
    with pytest.raises(ValueError, match="experimental"):
        fetch_bytes("http://127.0.0.1:9/x", experimental=False)


def test_fetch_bytes_rejects_non_http_scheme() -> None:
    with pytest.raises(ValueError, match="http"):
        fetch_bytes("file:///etc/passwd", experimental=True)


def test_read_parquet_column_subset(tmp_dir: Path) -> None:
    pytest.importorskip("pyarrow")
    path = tmp_dir / "wide.parquet"
    export_parquet(path, {"a": [1], "b": [2], "c": [3]})
    got = materialize_parquet(path, columns=["b", "c"])
    assert got == {"b": [2], "c": [3]}


def test_read_parquet_bytes_roundtrip() -> None:
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    buf = BytesIO()
    pq.write_table(pa.Table.from_pydict({"x": [9, 10]}), buf)
    raw = buf.getvalue()
    got = materialize_parquet(raw)
    assert got["x"] == [9, 10]


def test_read_parquet_engine_rust_invalid_source_raises() -> None:
    with pytest.raises(ValueError, match="Rust Parquet"):
        materialize_parquet(b"not a real parquet", engine="rust")


def test_read_ipc_stream_format(tmp_dir: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    table = pa.table({"s": ["a", "b"], "n": [1, 2]})
    buf = BytesIO()
    with pa.ipc.new_stream(buf, table.schema) as writer:
        writer.write_table(table)
    raw = buf.getvalue()
    got = materialize_ipc(BytesIO(raw), as_stream=True)
    assert got["s"] == ["a", "b"]
    assert got["n"] == [1, 2]

    path = tmp_dir / "stream.arrow"
    path.write_bytes(raw)
    with pytest.raises(ValueError, match="Rust IPC"):
        materialize_ipc(path, as_stream=True, engine="rust")


def test_read_ipc_file_engine_rust(tmp_dir: Path) -> None:
    path = tmp_dir / "file.arrow"
    export_ipc(path, {"z": [100]})
    got = materialize_ipc(path, engine="rust")
    assert got == {"z": [100]}


def test_write_sql_replace_sqlite(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "rep.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    write_sql({"u": [1, 2], "v": [3, 4]}, "t1", eng, if_exists="replace")
    with eng.connect() as conn:
        conn.execute(text("INSERT INTO t1 (u, v) VALUES (5, 6)"))
        conn.commit()
    out = fetch_sql("SELECT u, v FROM t1 ORDER BY u", eng)
    assert out["u"] == [1, 2, 5]
    write_sql({"u": [7], "v": [8]}, "t1", eng, if_exists="replace")
    out2 = fetch_sql("SELECT u, v FROM t1", eng)
    assert out2 == {"u": [7], "v": [8]}


def test_read_sql_with_parameters(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "p.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE p (id INTEGER, name TEXT)"))
        conn.execute(text("INSERT INTO p VALUES (1, 'a'), (2, 'b')"))
    out = fetch_sql(
        "SELECT id, name FROM p WHERE id > :lo ORDER BY id",
        eng,
        parameters={"lo": 0},
    )
    assert out == {"id": [1, 2], "name": ["a", "b"]}


def test_arrow_conversion_helpers() -> None:
    pa = pytest.importorskip("pyarrow")
    table = pa.table({"c": [1, 2], "d": ["x", "y"]})
    d1 = arrow_table_to_column_dict(table)
    assert d1 == {"c": [1, 2], "d": ["x", "y"]}
    batch = table.slice(0, 1).combine_chunks().to_batches()[0]
    d2 = record_batch_to_column_dict(batch)
    assert d2 == {"c": [1], "d": ["x"]}


def test_pydantable_io_engine_env_pyarrow(
    tmp_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("pyarrow")
    monkeypatch.setenv("PYDANTABLE_IO_ENGINE", "pyarrow")
    path = tmp_dir / "env.parquet"
    export_parquet(path, {"e": [1]})
    got = materialize_parquet(path)
    assert got == {"e": [1]}
    monkeypatch.delenv("PYDANTABLE_IO_ENGINE", raising=False)


@pytest.mark.asyncio
async def test_aread_parquet_with_thread_pool_executor(tmp_dir: Path) -> None:
    path = tmp_dir / "exec.parquet"
    export_parquet(path, {"q": [11, 22]})
    with ThreadPoolExecutor(max_workers=2) as ex:
        got = await amaterialize_parquet(path, executor=ex)
    assert got == {"q": [11, 22]}


@pytest.mark.asyncio
async def test_async_write_then_read_ipc_and_ndjson(tmp_dir: Path) -> None:
    p_ipc = tmp_dir / "a.arrow"
    p_nd = tmp_dir / "a.ndjson"
    await aexport_ipc(p_ipc, {"k": [1, 2]})
    await aexport_ndjson(p_nd, {"m": ["x", "y"]})
    assert await amaterialize_ipc(p_ipc) == {"k": [1, 2]}
    assert await amaterialize_ndjson(p_nd) == {"m": ["x", "y"]}


@pytest.mark.asyncio
async def test_async_parquet_roundtrip(tmp_dir: Path) -> None:
    path = tmp_dir / "async.pq"
    await aexport_parquet(path, {"a": [3, 4, 5]})
    got = await amaterialize_parquet(path)
    assert got == {"a": [3, 4, 5]}


@pytest.mark.asyncio
async def test_aread_sql_sqlite(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "async_sql.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE q (id INTEGER)"))
        conn.execute(text("INSERT INTO q VALUES (42)"))
    got = await afetch_sql("SELECT id FROM q", eng)
    assert got == {"id": [42]}


@pytest.mark.asyncio
async def test_awrite_sql_append(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "aw.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE w (n INTEGER)"))
    await awrite_sql({"n": [1]}, "w", eng, if_exists="append")
    await awrite_sql({"n": [2]}, "w", eng, if_exists="append")
    got = await afetch_sql("SELECT n FROM w ORDER BY n", eng)
    assert got == {"n": [1, 2]}


@pytest.mark.network
def test_fetch_csv_url_local(tmp_dir: Path) -> None:
    csv_path = tmp_dir / "served.csv"
    csv_path.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
    raw = csv_path.read_bytes()

    class H(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *args: object) -> None:
            return

    server, _ = http_server_thread(H)
    try:
        url = f"http://127.0.0.1:{server.server_port}/data.csv"
        got = fetch_csv_url(url, experimental=True)
        # Rust path may infer integer columns; stdlib fallback uses strings.
        assert got["a"] in ([1, 3], ["1", "3"])
        assert got["b"] in ([2, 4], ["2", "4"])
    finally:
        server.shutdown()
        server.server_close()


@pytest.mark.network
def test_fetch_ndjson_url_local(tmp_dir: Path) -> None:
    body = (json.dumps({"x": 1}) + "\n" + json.dumps({"x": 2}) + "\n").encode("utf-8")

    class H(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args: object) -> None:
            return

    server, _ = http_server_thread(H)
    try:
        url = f"http://127.0.0.1:{server.server_port}/l.jsonl"
        got = fetch_ndjson_url(url, experimental=True)
        assert got == {"x": [1, 2]}
    finally:
        server.shutdown()
        server.server_close()


def test_parquet_roundtrip_with_nulls(tmp_dir: Path) -> None:
    path = tmp_dir / "nulls.parquet"
    data = {"a": [1, None, 3], "b": ["x", None, "z"]}
    export_parquet(path, data)
    got = materialize_parquet(path)
    assert got["a"][0] == 1 and got["a"][2] == 3
    assert got["a"][1] is None
    assert got["b"][0] == "x" and got["b"][2] == "z"
    assert got["b"][1] is None


@pytest.mark.asyncio
async def test_aread_csv_matches_sync(tmp_dir: Path) -> None:
    path = tmp_dir / "sync_async.csv"
    export_csv(path, {"i": [5, 6], "j": [7, 8]})
    sync_got = materialize_csv(path)
    async_got = await amaterialize_csv(path)
    assert sync_got == async_got


def test_read_sql_empty_result(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "empty.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE e (id INTEGER)"))
    assert fetch_sql("SELECT * FROM e WHERE 1=0", eng) == {}


def test_read_sql_with_url_string(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "url.sqlite"
    url = f"sqlite:///{db}"
    eng = create_engine(url)
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE u (x INTEGER)"))
        conn.execute(text("INSERT INTO u VALUES (99)"))
    out = fetch_sql("SELECT x FROM u", url)
    assert out == {"x": [99]}


def test_read_sql_with_connection(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "conn.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE c (y TEXT)"))
        conn.execute(text("INSERT INTO c VALUES ('hi')"))
    with eng.connect() as conn:
        out = fetch_sql("SELECT y FROM c", conn)
    assert out == {"y": ["hi"]}


def test_write_sql_with_connection(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "wconn.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as c:
        c.execute(text("CREATE TABLE wc (k INTEGER)"))
    with eng.begin() as conn:
        write_sql({"k": [99]}, "wc", conn, if_exists="append")
    with eng.connect() as c2:
        n = c2.execute(text("SELECT COUNT(*) FROM wc")).scalar()
        row = c2.execute(text("SELECT k FROM wc")).scalar()
    assert n == 1
    assert row == 99


def test_write_sql_invalid_if_exists(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "bad.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE b (n INTEGER)"))
    with pytest.raises(ValueError, match="if_exists"):
        write_sql({"n": [1]}, "b", eng, if_exists="truncate")


def test_write_sql_column_length_mismatch(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "mismatch.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE m (a INTEGER, b INTEGER)"))
    with pytest.raises(ValueError, match="same length"):
        write_sql({"a": [1, 2], "b": [3]}, "m", eng, if_exists="append")


def test_write_sql_append_requires_table(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine

    db = tmp_dir / "missing.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with pytest.raises(ValueError, match="does not exist"):
        write_sql({"n": [1]}, "ghost", eng, if_exists="append")


def test_write_sql_empty_data_is_noop(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "noop.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE z (n INTEGER)"))
    write_sql({}, "z", eng, if_exists="append")


@pytest.mark.network
def test_fetch_bytes_experimental_via_env(
    tmp_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PYDANTABLE_IO_EXPERIMENTAL", "1")
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pqw

    pq_path = tmp_dir / "e.parquet"
    pqw.write_table(pa.table({"w": [1]}), pq_path)
    raw = pq_path.read_bytes()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *args: object) -> None:
            return

    server, _ = http_server_thread(Handler)
    try:
        url = f"http://127.0.0.1:{server.server_port}/x.parquet"
        assert fetch_bytes(url, experimental=False) == raw
    finally:
        server.shutdown()
        server.server_close()


@pytest.mark.network
def test_fetch_parquet_url_column_subset(tmp_dir: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pqw

    pq_path = tmp_dir / "sub.parquet"
    pqw.write_table(pa.table({"keep": [1], "drop": [9]}), pq_path)
    raw = pq_path.read_bytes()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *args: object) -> None:
            return

    server, _ = http_server_thread(Handler)
    try:
        url = f"http://127.0.0.1:{server.server_port}/p.parquet"
        got = fetch_parquet_url(url, experimental=True, columns=["keep"])
        assert got == {"keep": [1]}
    finally:
        server.shutdown()
        server.server_close()


def test_read_csv_stdlib_path_when_engine_not_rust(tmp_dir: Path) -> None:
    """Any engine other than auto/rust skips the Rust reader and uses stdlib csv."""
    path = tmp_dir / "fb.csv"
    path.write_text("a,b\n1,2\n", encoding="utf-8")
    got = materialize_csv(path, engine="stdlib")
    assert got["a"] == ["1"]
    assert got["b"] == ["2"]


def test_export_ipc_read_ipc_pyarrow_fallback(tmp_dir: Path) -> None:
    pytest.importorskip("pyarrow")
    path = tmp_dir / "ipc_fb.arrow"
    export_ipc(path, {"p": [1.0, 2.0]}, engine="pyarrow")
    got = materialize_ipc(path, engine="pyarrow")
    assert got == {"p": [1.0, 2.0]}


@pytest.mark.asyncio
async def test_aread_sql_with_executor(tmp_dir: Path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_dir / "ex.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (n INTEGER)"))
        conn.execute(text("INSERT INTO t VALUES (3)"))
    with ThreadPoolExecutor(max_workers=1) as ex:
        got = await afetch_sql("SELECT n FROM t", eng, executor=ex)
    assert got == {"n": [3]}


def test_read_parquet_filter_write_roundtrip(tmp_dir: Path) -> None:
    """Lazy scan → filter → sink (no full Python dict for the scanned table)."""

    class Row(BaseModel):
        x: int

    path_in = tmp_dir / "in.pq"
    path_out = tmp_dir / "out.pq"
    export_parquet(path_in, {"x": [1, 2, 3]})
    df = DataFrame[Row].read_parquet(str(path_in))
    df2 = df.filter(df.x > 1)
    df2.write_parquet(str(path_out))
    got = materialize_parquet(path_out)
    assert got["x"] == [2, 3]


def test_read_csv_scan_kwargs_and_write_parquet_write_kwargs(tmp_dir: Path) -> None:
    """``**scan_kwargs`` on lazy CSV read; ``write_kwargs`` on lazy Parquet write."""

    class Row(BaseModel):
        a: int
        b: int

    path_csv = tmp_dir / "semi.csv"
    path_out = tmp_dir / "out_kw.pq"
    path_csv.write_text("a;b\n1;2\n", encoding="utf-8")
    df = DataFrame[Row].read_csv(str(path_csv), separator=";")
    df.write_parquet(str(path_out), write_kwargs={"compression": "snappy"})
    got = materialize_parquet(path_out)
    assert got == {"a": [1], "b": [2]}


def test_read_parquet_join_inner_collect_lists(tmp_dir: Path) -> None:
    class L(BaseModel):
        k: int
        a: int

    class R(BaseModel):
        k: int
        b: int

    lp = tmp_dir / "lj.pq"
    rp = tmp_dir / "rj.pq"
    export_parquet(lp, {"k": [1, 2], "a": [10, 20]})
    export_parquet(rp, {"k": [2, 3], "b": [100, 200]})
    left = DataFrame[L].read_parquet(str(lp))
    right = DataFrame[R](materialize_parquet(rp))
    joined = left.join(right, on="k", how="inner")
    rows = joined.collect(as_lists=True)
    assert rows["k"] == [2]
    assert rows["a"] == [20]
    assert rows["b"] == [100]


def test_read_parquet_concat_vertical(tmp_dir: Path) -> None:
    class R(BaseModel):
        x: int

    p1 = tmp_dir / "c1.pq"
    p2 = tmp_dir / "c2.pq"
    export_parquet(p1, {"x": [1]})
    export_parquet(p2, {"x": [2]})
    d1 = DataFrame[R].read_parquet(str(p1))
    d2 = DataFrame[R].read_parquet(str(p2))
    out = DataFrame.concat([d1, d2], how="vertical")
    assert [r.x for r in out.collect()] == [1, 2]


def test_read_parquet_groupby_sum(tmp_dir: Path) -> None:
    class R(BaseModel):
        k: int
        v: int

    p = tmp_dir / "gb.pq"
    export_parquet(p, {"k": [1, 1, 2], "v": [10, 20, 5]})
    df = DataFrame[R].read_parquet(str(p))
    g = df.group_by("k").agg(s=("sum", "v"))
    rows = g.collect(as_lists=True)
    pairs = sorted(zip(rows["k"], rows["s"], strict=True))
    assert pairs == [(1, 30), (2, 5)]


def test_read_parquet_melt(tmp_dir: Path) -> None:
    class R(BaseModel):
        id: int
        a: int
        b: int

    p = tmp_dir / "mlt.pq"
    export_parquet(p, {"id": [1, 2], "a": [3, 4], "b": [5, 6]})
    df = DataFrame[R].read_parquet(str(p))
    long = df.melt(id_vars=["id"], value_vars=["a", "b"])
    rows = long.collect(as_lists=True)
    assert len(rows["id"]) == 4


def test_write_csv_and_collect_batches(tmp_dir: Path) -> None:
    pytest.importorskip("polars")

    class S(BaseModel):
        n: int

    p = tmp_dir / "b.pq"
    csv_out = tmp_dir / "out.csv"
    export_parquet(p, {"n": [1, 2, 3, 4]})
    sdf = DataFrame[S].read_parquet(str(p))
    sdf.filter(sdf.n > 1).write_csv(str(csv_out))
    got = materialize_csv(csv_out, engine="auto")
    assert [int(x) for x in got["n"]] == [2, 3, 4]

    batches = sdf.collect_batches(batch_size=2)
    assert sum(b.height for b in batches) == 4
    assert len(batches) == 2


@pytest.mark.network
def test_read_parquet_url_tmp_then_cleanup(tmp_dir: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pqw

    pq_path = tmp_dir / "http.pq"
    pqw.write_table(pa.table({"z": [7, 8]}), pq_path)
    raw = pq_path.read_bytes()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *args: object) -> None:
            return

    server, _ = http_server_thread(Handler)
    try:
        url = f"http://127.0.0.1:{server.server_port}/t.pq"
        root = read_parquet_url(url, experimental=True)
        path = root.path

        class Z(BaseModel):
            z: int

        df = DataFrame[Z]._from_scan_root(root)
        assert [r.z for r in df.collect()] == [7, 8]
        assert Path(path).is_file()
        Path(path).unlink(missing_ok=True)
    finally:
        server.shutdown()
        server.server_close()


def test_read_parquet_url_unlinks_temp_when_scan_root_fails() -> None:
    paths: list[str] = []
    real_mkstemp = _stdlib_tempfile.mkstemp

    def track_mkstemp(*args: object, **kwargs: object) -> tuple[int, str]:
        fd, name = real_mkstemp(*args, **kwargs)
        paths.append(name)
        return fd, name

    with (
        patch("pydantable.io.tempfile.mkstemp", side_effect=track_mkstemp),
        patch("pydantable.io.fetch_bytes", return_value=b"x"),
        patch(
            "pydantable.io._scan_file_root",
            side_effect=ValueError("scan root failed"),
        ),
        pytest.raises(ValueError, match="scan root failed"),
    ):
        read_parquet_url("http://example.com/fake.parquet", experimental=True)

    assert len(paths) == 1
    assert not Path(paths[0]).exists()
