"""Tests for optional I/O extras (BigQuery, Snowflake, Kafka, ORC, Avro)."""

from __future__ import annotations

import io
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar
from unittest.mock import patch

import pytest
from pydantable.io.extras import (
    iter_avro,
    iter_bigquery,
    iter_delta,
    iter_excel,
    iter_kafka_json,
    iter_orc,
    iter_snowflake,
    read_avro,
    read_csv_stdin,
    read_delta,
    read_excel,
    read_kafka_json_batch,
    write_csv_stdout,
)


def test_iter_excel_openpyxl_import_error(monkeypatch: pytest.MonkeyPatch) -> None:
    import builtins

    real_import = builtins.__import__

    def fake_import(name: str, *a: object, **kw: object):
        if name == "openpyxl":
            raise ImportError("no openpyxl")
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(ImportError, match="iter_excel requires openpyxl"):
        next(iter_excel(Path("nope.xlsx"), experimental=True))


def test_read_excel_openpyxl_import_error(monkeypatch: pytest.MonkeyPatch) -> None:
    import builtins

    real_import = builtins.__import__

    def fake_import(name: str, *a: object, **kw: object):
        if name == "openpyxl":
            raise ImportError("no openpyxl")
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(ImportError, match="read_excel requires openpyxl"):
        read_excel(Path("nope.xlsx"), experimental=True)


def test_read_excel_no_rows_returns_empty_dict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("openpyxl")

    class _Ws:
        def iter_rows(self, values_only: bool = True):
            return iter(())

    class _Wb:
        def __init__(self) -> None:
            self.worksheets = [_Ws()]

        def __getitem__(self, _k: object) -> _Ws:
            return self.worksheets[0]

        def close(self) -> None:
            return None

    monkeypatch.setattr("openpyxl.load_workbook", lambda *a, **k: _Wb())
    out = read_excel(tmp_path / "empty.xlsx", experimental=True)
    assert out == {}


def test_iter_excel_stop_iteration_no_header_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("openpyxl")

    class _Ws:
        def iter_rows(self, values_only: bool = True):
            return iter(())

    class _Wb:
        def __init__(self) -> None:
            self.worksheets = [_Ws()]

        def __getitem__(self, _k: object) -> _Ws:
            return self.worksheets[0]

        def close(self) -> None:
            return None

    monkeypatch.setattr("openpyxl.load_workbook", lambda *a, **k: _Wb())
    assert list(iter_excel(tmp_path / "empty2.xlsx", experimental=True)) == []


def test_iter_delta_batch_size_zero_raises(tmp_path: Path) -> None:
    pytest.importorskip("pyarrow")
    root = tmp_path / "d0"
    root.mkdir()
    with pytest.raises(ValueError, match="batch_size"):
        next(iter_delta(root, batch_size=0, experimental=True))


def test_read_delta_experimental_false_without_env_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("PYDANTABLE_IO_EXPERIMENTAL", raising=False)
    with pytest.raises(ValueError, match="experimental"):
        read_delta(tmp_path / "delta", experimental=False)


def test_read_avro_attribute_error_when_pyarrow_avro_missing(
    tmp_path: Path,
) -> None:
    pa = pytest.importorskip("pyarrow")
    path = tmp_path / "m.avro"
    path.write_bytes(b"")
    with (
        patch.object(pa, "avro", SimpleNamespace(), create=True),
        pytest.raises(ImportError, match=r"pyarrow\.avro"),
    ):
        read_avro(path, experimental=True)


def test_iter_orc_batches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pa = pytest.importorskip("pyarrow")
    pytest.importorskip("pyarrow.orc")
    import pyarrow.orc as orc_mod

    class _ORC:
        def __init__(self, _f: object) -> None:
            pass

        def iter_batches(self, batch_size: int = 65_536):
            yield pa.record_batch([pa.array([1, 2, 3])], names=["oc"])

    monkeypatch.setattr(orc_mod, "ORCFile", _ORC)
    path = tmp_path / "t.orc"
    path.write_bytes(b"orc")
    parts = list(iter_orc(path, batch_size=2, experimental=True))
    assert sum(len(p["oc"]) for p in parts) == 3


@pytest.mark.optional_cloud
def test_iter_bigquery_uses_arrow_iterable(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("pyarrow")
    pytest.importorskip("google.cloud.bigquery")

    class _RB:
        def to_pydict(self) -> dict[str, list[int]]:
            return {"it": [1, 2]}

    class _Rows:
        page_size = 10

        def to_arrow_iterable(self):
            yield _RB()

    class _Job:
        def result(self, **_kwargs: object):
            return _Rows()

    class _Client:
        def __init__(self, *a: object, **k: object) -> None:
            pass

        def query(self, _q: str):
            return _Job()

    monkeypatch.setattr("google.cloud.bigquery.Client", _Client)
    parts = list(iter_bigquery("SELECT 1", experimental=True, batch_size=100))
    assert parts == [{"it": [1, 2]}]


@pytest.mark.optional_cloud
def test_iter_bigquery_fallback_without_arrow_iterable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pa = pytest.importorskip("pyarrow")
    pytest.importorskip("google.cloud.bigquery")

    class _Result:
        def to_arrow(self):
            return pa.table({"fb": [7]})

    class _Job:
        def result(self, **_kwargs: object):
            return _Result()

    class _Client:
        def __init__(self, *a: object, **k: object) -> None:
            pass

        def query(self, _q: str):
            return _Job()

    monkeypatch.setattr("google.cloud.bigquery.Client", _Client)
    parts = list(iter_bigquery("SELECT 1", experimental=True, batch_size=50))
    assert parts == [{"fb": [7]}]


@pytest.mark.optional_cloud
def test_iter_bigquery_batch_size_invalid() -> None:
    pytest.importorskip("google.cloud.bigquery")
    with pytest.raises(ValueError, match="batch_size"):
        next(iter_bigquery("SELECT 1", experimental=True, batch_size=0))


@pytest.mark.optional_cloud
def test_iter_snowflake_yields_batches() -> None:
    pytest.importorskip("snowflake.connector")

    batches = [[(1, "a"), (2, "b")], []]

    class _Cur:
        description: ClassVar[tuple[tuple[str, ...], ...]] = (("n",), ("s",))
        _bi = 0

        def execute(self, _sql: str) -> None:
            return None

        def fetchmany(self, n: int):
            if self._bi >= len(batches):
                return []
            chunk = batches[self._bi]
            self._bi += 1
            return chunk

    class _Conn:
        def cursor(self):
            return _Cur()

        def close(self) -> None:
            return None

    with patch("snowflake.connector.connect", return_value=_Conn()):
        parts = list(
            iter_snowflake("SELECT 1", experimental=True, batch_size=10, account="a")
        )
    assert len(parts) == 1
    assert parts[0]["n"] == [1, 2]
    assert parts[0]["s"] == ["a", "b"]


@pytest.mark.optional_cloud
def test_iter_snowflake_empty_description_returns_nothing() -> None:
    pytest.importorskip("snowflake.connector")

    class _Cur:
        description = None

        def execute(self, _sql: str) -> None:
            return None

    class _Conn:
        def cursor(self):
            return _Cur()

        def close(self) -> None:
            return None

    with patch("snowflake.connector.connect", return_value=_Conn()):
        assert list(iter_snowflake("SELECT 1", experimental=True, account="a")) == []


def test_iter_snowflake_batch_size_zero() -> None:
    pytest.importorskip("snowflake.connector")
    with pytest.raises(ValueError, match="batch_size"):
        next(iter_snowflake("SELECT 1", experimental=True, batch_size=0, account="a"))


def test_iter_kafka_json_yields_and_flushes_tail() -> None:
    pytest.importorskip("kafka")

    class _Msg:
        def __init__(self, key: bytes | None, val: object, off: int) -> None:
            self.key = key
            self.value = val
            self.partition = 0
            self.offset = off

    msgs = [
        _Msg(b"k1", {"v": 1}, 0),
        _Msg(None, {"v": 2}, 1),
    ]

    class _Consumer:
        def __init__(self, *a: object, **k: object) -> None:
            self._i = 0

        def poll(self, timeout_ms: int = 0):
            if self._i >= len(msgs):
                return {}
            m = msgs[self._i]
            self._i += 1
            return {("t", 0): [m]}

        def close(self) -> None:
            return None

    with patch("kafka.KafkaConsumer", _Consumer):
        parts = list(
            iter_kafka_json(
                "topic",
                bootstrap_servers="localhost:9092",
                batch_size=1,
                max_messages=3,
                experimental=True,
            )
        )
    assert len(parts) >= 1
    assert "v" in parts[0]


def test_iter_kafka_json_batch_size_invalid() -> None:
    pytest.importorskip("kafka")
    with pytest.raises(ValueError, match="batch_size"):
        next(
            iter_kafka_json(
                "t",
                bootstrap_servers="localhost:9092",
                batch_size=0,
                experimental=True,
            )
        )


def test_read_kafka_json_batch_non_dict_value_branch() -> None:
    pytest.importorskip("kafka")

    class _Msg:
        key = None
        value = "not-a-dict"
        partition = 0
        offset = 0

    class _Consumer:
        def __init__(self, *a: object, **k: object) -> None:
            self._done = False

        def poll(self, timeout_ms: int = 0):
            if self._done:
                return {}
            self._done = True
            return {("t", 0): [_Msg()]}

        def close(self) -> None:
            return None

    with patch("kafka.KafkaConsumer", _Consumer):
        got = read_kafka_json_batch(
            "t",
            bootstrap_servers="localhost:9092",
            max_messages=5,
            experimental=True,
        )
    assert got.get("partition") == [0]


def test_iter_avro_fallback_to_read_avro(tmp_path: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    path = tmp_path / "x.avro"
    path.write_bytes(b"")

    def boom(_p: str):
        raise RuntimeError("no streaming")

    with (
        patch.object(pa, "avro", SimpleNamespace(open_file=boom), create=True),
        patch(
            "pydantable.io.extras.read_avro",
            return_value={"av": [1]},
        ) as ro,
    ):
        out = list(iter_avro(path, experimental=True))
    ro.assert_called_once()
    assert out == [{"av": [1]}]


def test_read_csv_stdin_binary_read() -> None:
    class _Bin:
        def read(self) -> bytes:
            return b"a,b\n1,2\n"

    got = read_csv_stdin(_Bin(), engine="auto")  # type: ignore[arg-type]
    assert "a" in got and "b" in got


def test_write_csv_stdout_textio_wrapper_uses_buffer(tmp_path: Path) -> None:
    bio = io.BytesIO()
    stream = io.TextIOWrapper(bio, encoding="utf-8", newline="")
    write_csv_stdout({"z": [9]}, stream=stream, engine="auto")
    stream.flush()
    raw = bio.getvalue()
    assert b"z" in raw
    assert b"9" in raw


def test_write_csv_stdout_none_writes_to_stdout_buffer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pydantable_native.io_core", reason="export needs rust or path")
    # Use a temp path and avoid touching real stdout: patch sys.stdout.buffer
    buf = io.BytesIO()

    class _Out:
        buffer = buf

    monkeypatch.setattr(sys, "stdout", _Out())
    write_csv_stdout({"q": [1]}, stream=None, engine="auto")
    data = buf.getvalue()
    assert b"q" in data
