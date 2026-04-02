"""Phase 0-1 SQLModel read I/O: fetch_sqlmodel / iter_sqlmodel and async mirrors."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pydantable.errors import MissingOptionalDependency
from pydantable.io import (
    StreamingColumns,
    afetch_sqlmodel,
    aiter_sqlmodel,
    fetch_sqlmodel,
    iter_sqlmodel,
)

pytest.importorskip("sqlmodel")

from sqlalchemy import bindparam
from sqlmodel import Field, Session, SQLModel, create_engine

if TYPE_CHECKING:
    from pathlib import Path


class _TRow(SQLModel, table=True):
    n: int = Field(primary_key=True)


class _Person(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    score: int = 0


def _engine_with_rows(tmp_path: Path):
    db = tmp_path / "sqlmodel_phase01.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    SQLModel.metadata.create_all(eng)
    with Session(eng) as session:
        session.add_all(_TRow(n=i) for i in range(1, 51))
        session.commit()
    return eng


def _empty_t_table_engine(tmp_path: Path):
    db = tmp_path / "empty.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    SQLModel.metadata.create_all(eng)
    return eng


def test_iter_sqlmodel_empty_table_yields_nothing(tmp_path: Path) -> None:
    eng = _empty_t_table_engine(tmp_path)
    assert list(iter_sqlmodel(_TRow, eng)) == []


def test_fetch_sqlmodel_empty_table_returns_empty_dict(tmp_path: Path) -> None:
    eng = _empty_t_table_engine(tmp_path)
    assert fetch_sqlmodel(_TRow, eng) == {}


def test_iter_sqlmodel_where_and_parameters(tmp_path: Path) -> None:
    eng = _engine_with_rows(tmp_path)
    batches = list(
        iter_sqlmodel(
            _TRow,
            eng,
            where=_TRow.n >= bindparam("min_n"),
            parameters={"min_n": 47},
            order_by=[_TRow.n],
            batch_size=2,
        )
    )
    flat = [x for b in batches for x in b["n"]]
    assert flat == [47, 48, 49, 50]


def test_iter_sqlmodel_limit(tmp_path: Path) -> None:
    eng = _engine_with_rows(tmp_path)
    flat = [
        x
        for b in iter_sqlmodel(_TRow, eng, order_by=[_TRow.n], limit=5)
        for x in b["n"]
    ]
    assert flat == [1, 2, 3, 4, 5]


def test_iter_sqlmodel_columns_projection_multi_column(tmp_path: Path) -> None:
    db = tmp_path / "proj.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    SQLModel.metadata.create_all(eng)
    with Session(eng) as session:
        session.add(_Person(name="ada", score=10))
        session.add(_Person(name="bob", score=20))
        session.commit()

    batches = list(
        iter_sqlmodel(
            _Person,
            eng,
            columns=[_Person.name, _Person.score],
            order_by=[_Person.name],
            batch_size=1,
        )
    )
    assert {frozenset(b.keys()) for b in batches} == {frozenset({"name", "score"})}
    names = [x for b in batches for x in b["name"]]
    scores = [x for b in batches for x in b["score"]]
    assert names == ["ada", "bob"]
    assert scores == [10, 20]


def test_iter_sqlmodel_url_string_bind(tmp_path: Path) -> None:
    eng = _engine_with_rows(tmp_path)
    url = str(eng.url)
    flat = []
    for b in iter_sqlmodel(_TRow, url, order_by=[_TRow.n], limit=3):
        flat.extend(b["n"])
    assert flat == [1, 2, 3]


def test_iter_sqlmodel_connection_bind(tmp_path: Path) -> None:
    eng = _engine_with_rows(tmp_path)
    with eng.connect() as conn:
        batches = list(
            iter_sqlmodel(
                _TRow,
                conn,
                where=_TRow.n.in_(bindparam("vals", expanding=True)),
                parameters={"vals": [2, 4, 6]},
                order_by=[_TRow.n],
            )
        )
    flat = [x for b in batches for x in b["n"]]
    assert flat == [2, 4, 6]


def test_iter_sqlmodel_batch_size_zero_raises(tmp_path: Path) -> None:
    eng = _engine_with_rows(tmp_path)
    with pytest.raises(ValueError, match="batch_size"):
        next(iter(iter_sqlmodel(_TRow, eng, batch_size=0)))


def test_iter_sqlmodel_respects_fetch_batch_size_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PYDANTABLE_SQL_FETCH_BATCH_SIZE", "5")
    eng = _engine_with_rows(tmp_path)
    batches = list(iter_sqlmodel(_TRow, eng, order_by=[_TRow.n]))
    assert len(batches) == 10
    flat = [x for b in batches for x in b["n"]]
    assert flat == list(range(1, 51))


def test_fetch_sqlmodel_auto_stream_disabled_merges_all_batches(tmp_path: Path) -> None:
    eng = _engine_with_rows(tmp_path)
    out = fetch_sqlmodel(
        _TRow,
        eng,
        order_by=[_TRow.n],
        auto_stream=False,
        auto_stream_threshold_rows=5,
        batch_size=11,
    )
    assert isinstance(out, dict)
    assert not isinstance(out, StreamingColumns)
    assert out["n"] == list(range(1, 51))


def test_fetch_sqlmodel_large_result_internal_batching(tmp_path: Path) -> None:
    db = tmp_path / "huge.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    SQLModel.metadata.create_all(eng)
    with Session(eng) as session:
        session.add_all(_TRow(n=i) for i in range(1, 5001))
        session.commit()
    got = fetch_sqlmodel(_TRow, eng, order_by=[_TRow.n], batch_size=1000)
    assert got["n"][0] == 1
    assert got["n"][-1] == 5000
    assert len(got["n"]) == 5000


def test_iter_sqlmodel_batches_stable_keys(tmp_path: Path) -> None:
    eng = _engine_with_rows(tmp_path)
    batches = list(
        iter_sqlmodel(_TRow, eng, order_by=[_TRow.n], batch_size=7),
    )
    keys = {frozenset(b.keys()) for b in batches}
    assert keys == {frozenset({"n"})}
    assert sum(len(next(iter(b.values()))) for b in batches) == 50
    flat = []
    for b in batches:
        flat.extend(b["n"])
    assert flat == list(range(1, 51))


def test_fetch_sqlmodel_materialized_dict(tmp_path: Path) -> None:
    eng = _engine_with_rows(tmp_path)
    out = fetch_sqlmodel(_TRow, eng, order_by=[_TRow.n], batch_size=10)
    assert isinstance(out, dict)
    assert out == {"n": list(range(1, 51))}


def test_fetch_sqlmodel_auto_stream_returns_streaming_columns(tmp_path: Path) -> None:
    eng = _engine_with_rows(tmp_path)
    out = fetch_sqlmodel(
        _TRow,
        eng,
        order_by=[_TRow.n],
        auto_stream=True,
        auto_stream_threshold_rows=10,
        batch_size=7,
    )
    assert isinstance(out, StreamingColumns)
    got = out.to_dict()
    assert got["n"][0] == 1
    assert got["n"][-1] == 50


def test_non_table_sqlmodel_raises_type_error() -> None:
    class _NotTable(SQLModel):
        x: int

    eng = create_engine("sqlite:///:memory:")
    with pytest.raises(TypeError, match="table=True"):
        next(iter(iter_sqlmodel(_NotTable, eng)))


@pytest.mark.asyncio
async def test_afetch_sqlmodel_sqlite(tmp_path: Path) -> None:
    eng = _engine_with_rows(tmp_path)
    got = await afetch_sqlmodel(_TRow, eng, order_by=[_TRow.n])
    assert got == {"n": list(range(1, 51))}


@pytest.mark.asyncio
async def test_aiter_sqlmodel_batch_size_zero_raises(tmp_path: Path) -> None:
    eng = _engine_with_rows(tmp_path)
    with pytest.raises(ValueError, match="batch_size"):
        async for _ in aiter_sqlmodel(_TRow, eng, batch_size=0):
            pass


@pytest.mark.asyncio
async def test_aiter_sqlmodel_propagates_sql_errors(tmp_path: Path) -> None:
    from sqlalchemy.exc import SQLAlchemyError

    eng = create_engine(f"sqlite:///{tmp_path / 'no_schema.sqlite'}")
    with pytest.raises(SQLAlchemyError):
        async for _b in aiter_sqlmodel(_TRow, eng):
            pass


@pytest.mark.asyncio
async def test_aiter_sqlmodel_sqlite(tmp_path: Path) -> None:
    eng = _engine_with_rows(tmp_path)
    batches: list[dict] = []
    async for b in aiter_sqlmodel(_TRow, eng, order_by=[_TRow.n], batch_size=6):
        batches.append(b)
    flat = []
    for b in batches:
        flat.extend(b["n"])
    assert flat == list(range(1, 51))


def test_require_sqlmodel_import_error_message(monkeypatch: pytest.MonkeyPatch) -> None:
    import builtins

    import pydantable.io.sqlmodel_read as smr

    real_import = builtins.__import__

    def _deny_sqlmodel(
        name: str,
        globals=None,
        locals=None,
        fromlist=(),
        level: int = 0,
    ):
        if name == "sqlmodel" or name.startswith("sqlmodel."):
            raise ImportError("denied")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _deny_sqlmodel)
    with pytest.raises(MissingOptionalDependency, match=r"pydantable\[sql\]"):
        smr._require_sqlmodel()
