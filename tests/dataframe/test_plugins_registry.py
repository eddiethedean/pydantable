from __future__ import annotations

import pytest
from pydantable.plugins import get_reader, get_writer, list_readers, list_writers


def test_builtin_readers_registered() -> None:
    names = {p.name for p in list_readers()}
    assert "read_parquet" in names
    assert "materialize_parquet" in names


def test_list_writers_sorted_and_non_empty() -> None:
    writers = list_writers()
    names = [p.name for p in writers]
    assert names == sorted(names)
    assert len(names) > 0


def test_get_reader_returns_callable() -> None:
    fn = get_reader("read_parquet")
    assert callable(fn)


def test_get_reader_unknown_lists_registered() -> None:
    with pytest.raises(ValueError, match="unknown reader"):
        get_reader("not_a_registered_reader")


def test_get_writer_unknown_lists_registered() -> None:
    with pytest.raises(ValueError, match="unknown writer"):
        get_writer("not_a_registered_writer")
