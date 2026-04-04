"""SqlDataFrame / SqlDataFrameModel (moltres-core) with SQLite and rapsqlite.

``MoltresPydantableEngine`` uses Moltres' synchronous :class:`sqlalchemy.engine.Engine`
(``EngineConfig(dsn=...)``). Use **sync** SQLite URLs such as ``sqlite:///:memory:``.

**rapsqlite** provides the ``sqlite+rapsqlite`` dialect for **async** SQLAlchemy
(``create_async_engine``). That path is exercised here for async SQL I/O smoke tests;
``SqlDataFrame`` / ``SqlDataFrameModel`` async *terminals* (``ato_dict``, ``acollect``)
still run against the sync engine (Moltres executes SQL in a thread pool).
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("moltres_core")

from moltres_core import ConnectionManager, EngineConfig
from pydantable import Schema
from pydantable.sql_moltres import (
    SqlDataFrame,
    SqlDataFrameModel,
    moltres_engine_from_sql_config,
)
from pydantable_protocol import UnsupportedEngineOperationError

# Sync SQLAlchemy + stdlib sqlite — what ``moltres_engine_from_sql_config`` uses.
SQLITE_SYNC_MEMORY = "sqlite:///:memory:"
# Async SQLAlchemy + rapsqlite (``pip install rapsqlite`` / ``pydantable[moltres]``).
SQLITE_RAPSQLITE_ASYNC = "sqlite+rapsqlite:///:memory:"


class _S(Schema):
    id: int


class _W(Schema):
    id: int
    label: str


class _M(SqlDataFrameModel):
    id: int


class _MW(SqlDataFrameModel):
    id: int
    label: str


def _sync_config() -> EngineConfig:
    return EngineConfig(dsn=SQLITE_SYNC_MEMORY)


def test_moltres_engine_from_sql_config() -> None:
    eng = moltres_engine_from_sql_config(_sync_config())
    assert eng.capabilities.backend == "custom"
    assert eng.capabilities.has_async_execute_plan is True


def test_sql_dataframe_sql_config() -> None:
    df = SqlDataFrame[_S]({"id": [1, 2]}, sql_config=_sync_config())
    assert df.to_dict() == {"id": [1, 2]}


def test_sql_dataframe_explicit_moltres_engine() -> None:
    eng = moltres_engine_from_sql_config(_sync_config())
    df = SqlDataFrame[_S]({"id": [3]}, moltres_engine=eng)
    assert df.to_dict() == {"id": [3]}


def test_sql_dataframe_engine_kwarg_wins_over_sql_config() -> None:
    """``engine=`` is resolved first; ``sql_config=`` may still be passed."""
    eng = moltres_engine_from_sql_config(_sync_config())
    other = EngineConfig(dsn="sqlite:///:memory:")
    df = SqlDataFrame[_S]({"id": [7]}, engine=eng, sql_config=other)
    assert df.to_dict() == {"id": [7]}


def test_sql_dataframe_requires_engine_args() -> None:
    with pytest.raises(TypeError, match="sql_config"):
        SqlDataFrame[_S]({"id": [1]})


def test_sql_dataframe_select_collect_head() -> None:
    df = SqlDataFrame[_W](
        {"id": [1, 2], "label": ["a", "b"]},
        sql_config=_sync_config(),
    )
    s = df.select("id")
    assert s.to_dict() == {"id": [1, 2]}
    assert [r.model_dump() for r in s.collect()] == [{"id": 1}, {"id": 2}]
    assert s.head(1).to_dict() == {"id": [1]}


def test_sql_dataframe_filter_expr_unsupported() -> None:
    df = SqlDataFrame[_S]({"id": [1, 2]}, sql_config=_sync_config())
    with pytest.raises(UnsupportedEngineOperationError, match="Moltres"):
        df.filter(df.id > 1)


def test_sql_dataframe_with_columns_unsupported() -> None:
    df = SqlDataFrame[_S]({"id": [1]}, sql_config=_sync_config())
    with pytest.raises(UnsupportedEngineOperationError, match="Moltres"):
        df.with_columns(x=df.id * 2)


def test_sql_dataframe_model_to_dict_collect() -> None:
    m = _M({"id": [1]}, sql_config=_sync_config())
    assert m.to_dict() == {"id": [1]}
    assert [r.model_dump() for r in m.collect()] == [{"id": 1}]


def test_pydantable_lazy_import_sql_classes() -> None:
    from pydantable import SqlDataFrame as SDF
    from pydantable import SqlDataFrameModel as SDM

    assert SDF is SqlDataFrame
    assert SDM is SqlDataFrameModel


@pytest.mark.asyncio
async def test_sql_dataframe_ato_dict() -> None:
    df = SqlDataFrame[_S]({"id": [1, 2]}, sql_config=_sync_config())
    assert await df.ato_dict() == {"id": [1, 2]}


@pytest.mark.asyncio
async def test_sql_dataframe_model_acollect() -> None:
    m = _MW({"id": [1], "label": ["z"]}, sql_config=_sync_config())
    rows = await m.acollect()
    assert len(rows) == 1
    assert rows[0].model_dump() == {"id": 1, "label": "z"}


@pytest.mark.asyncio
async def test_rapsqlite_async_engine_smoke() -> None:
    """Async SQLAlchemy + ``sqlite+rapsqlite`` (separate from Moltres sync pool)."""
    pytest.importorskip("rapsqlite")
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    eng = create_async_engine(SQLITE_RAPSQLITE_ASYNC)
    try:
        async with eng.connect() as conn:
            assert (await conn.execute(text("SELECT 1"))).scalar_one() == 1
    finally:
        await eng.dispose()


def test_shared_moltres_engine_two_frames() -> None:
    eng = moltres_engine_from_sql_config(_sync_config())
    a = SqlDataFrame[_S]({"id": [1]}, moltres_engine=eng)
    b = SqlDataFrame[_S]({"id": [2]}, moltres_engine=eng)
    assert a.to_dict() == {"id": [1]}
    assert b.to_dict() == {"id": [2]}


def test_from_sql_table_requires_schema_subscript() -> None:
    from sqlalchemy import Column, Integer, MetaData, Table

    md = MetaData()
    t = Table("x", md, Column("id", Integer, primary_key=True))
    with pytest.raises(TypeError, match="SqlDataFrame\\[YourSchema\\]"):
        SqlDataFrame.from_sql_table(
            t,
            sql_config=EngineConfig(dsn=SQLITE_SYNC_MEMORY),
        )


def test_sql_dataframe_from_sql_table_lazy_file(tmp_path: Path) -> None:
    from sqlalchemy import Column, Integer, MetaData, String, Table, insert

    db_path = tmp_path / "lazy.db"
    cfg = EngineConfig(dsn=f"sqlite:///{db_path}")
    eng = moltres_engine_from_sql_config(cfg)
    cm = ConnectionManager(cfg)
    md = MetaData()
    t = Table(
        "items",
        md,
        Column("id", Integer, primary_key=True),
        Column("label", String(20)),
    )
    md.create_all(cm.engine)
    with cm.engine.connect() as conn:
        conn.execute(insert(t).values(id=1, label="x"))
        conn.commit()

    df = SqlDataFrame[_W].from_sql_table(t, moltres_engine=eng)
    assert type(df._root_data).__name__ == "SqlRootData"
    assert df.to_dict() == {"id": [1], "label": ["x"]}
    slim = df.select("id")
    assert slim.to_dict() == {"id": [1]}


def test_sql_dataframe_model_read_sql_table_lazy(tmp_path: Path) -> None:
    from sqlalchemy import Column, Integer, MetaData, String, Table, insert

    db_path = tmp_path / "lazy2.db"
    cfg = EngineConfig(dsn=f"sqlite:///{db_path}")
    eng = moltres_engine_from_sql_config(cfg)
    cm = ConnectionManager(cfg)
    md = MetaData()
    t = Table(
        "items",
        md,
        Column("id", Integer, primary_key=True),
        Column("label", String(20)),
    )
    md.create_all(cm.engine)
    with cm.engine.connect() as conn:
        conn.execute(insert(t).values(id=2, label="y"))
        conn.commit()

    m = _MW.read_sql_table(t, moltres_engine=eng)
    assert m.to_dict() == {"id": [2], "label": ["y"]}
