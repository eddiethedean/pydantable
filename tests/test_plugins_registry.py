from __future__ import annotations

from pydantable.plugins import get_reader, list_readers


def test_builtin_readers_registered() -> None:
    names = {p.name for p in list_readers()}
    assert "read_parquet" in names
    assert "materialize_parquet" in names


def test_get_reader_returns_callable() -> None:
    fn = get_reader("read_parquet")
    assert callable(fn)

