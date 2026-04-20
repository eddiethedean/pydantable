"""SqlDataFrame / SqlDataFrameModel (lazy-SQL stack) with SQLite and rapsqlite.

The optional lazy-SQL engine uses a synchronous :class:`sqlalchemy.engine.Engine`
(``EngineConfig(dsn=...)``). Use **sync** SQLite URLs such as ``sqlite:///:memory:``.

**rapsqlite** provides the ``sqlite+rapsqlite`` dialect for **async** SQLAlchemy
(``create_async_engine``). That path is exercised here for async SQL I/O smoke tests;
``SqlDataFrame`` / ``SqlDataFrameModel`` async *terminals* (``ato_dict``, ``acollect``)
still run against the sync engine (SQL runs in a worker thread pool).
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("moltres_core")

from moltres_core import ConnectionManager, EngineConfig
from pydantable import Schema
from pydantable.sql_dataframe import (
    SqlDataFrame,
    SqlDataFrameModel,
    sql_engine_from_config,
)
from pydantable_protocol import UnsupportedEngineOperationError

# Sync SQLAlchemy + stdlib sqlite — what ``sql_engine_from_config`` uses.
SQLITE_SYNC_MEMORY = "sqlite:///:memory:"
# Async SQLAlchemy + rapsqlite (``pip install rapsqlite``; optional, not in ``[sql]``).
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


def test_sql_engine_from_config() -> None:
    eng = sql_engine_from_config(_sync_config())
    assert eng.capabilities.backend == "custom"
    assert eng.capabilities.has_async_execute_plan is True


def test_sql_dataframe_sql_config() -> None:
    df = SqlDataFrame[_S]({"id": [1, 2]}, sql_config=_sync_config())
    assert df.to_dict() == {"id": [1, 2]}


def test_sql_dataframe_explicit_sql_engine() -> None:
    eng = sql_engine_from_config(_sync_config())
    df = SqlDataFrame[_S]({"id": [3]}, sql_engine=eng)
    assert df.to_dict() == {"id": [3]}


def test_sql_dataframe_engine_kwarg_wins_over_sql_config() -> None:
    """``engine=`` is resolved first; ``sql_config=`` may still be passed."""
    eng = sql_engine_from_config(_sync_config())
    other = EngineConfig(dsn="sqlite:///:memory:")
    df = SqlDataFrame[_S]({"id": [7]}, engine=eng, sql_config=other)
    assert df.to_dict() == {"id": [7]}


def test_sql_dataframe_engine_mode_default_forces_default_engine() -> None:
    from pydantable.engine import get_default_engine

    df = SqlDataFrame[_S](
        {"id": [1]},
        sql_config=_sync_config(),
        engine_mode="default",
    )
    assert df._engine is get_default_engine()


def test_sql_dataframe_explicit_engine_wins_over_engine_mode_default() -> None:
    eng = sql_engine_from_config(_sync_config())
    df = SqlDataFrame[_S](
        {"id": [1]},
        sql_config=_sync_config(),
        engine=eng,
        engine_mode="default",
    )
    assert df._engine is eng


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
    """Async SQLAlchemy + ``sqlite+rapsqlite`` (separate from lazy-SQL sync pool)."""
    pytest.importorskip("rapsqlite")
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    eng = create_async_engine(SQLITE_RAPSQLITE_ASYNC)
    try:
        async with eng.connect() as conn:
            assert (await conn.execute(text("SELECT 1"))).scalar_one() == 1
    finally:
        await eng.dispose()


def test_shared_sql_engine_two_frames() -> None:
    eng = sql_engine_from_config(_sync_config())
    a = SqlDataFrame[_S]({"id": [1]}, sql_engine=eng)
    b = SqlDataFrame[_S]({"id": [2]}, sql_engine=eng)
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
    eng = sql_engine_from_config(cfg)
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

    df = SqlDataFrame[_W].from_sql_table(t, sql_engine=eng)
    assert type(df._root_data).__name__ == "SqlRootData"
    assert df.to_dict() == {"id": [1], "label": ["x"]}
    slim = df.select("id")
    assert slim.to_dict() == {"id": [1]}


def test_sql_dataframe_model_read_sql_table_lazy(tmp_path: Path) -> None:
    from sqlalchemy import Column, Integer, MetaData, String, Table, insert

    db_path = tmp_path / "lazy2.db"
    cfg = EngineConfig(dsn=f"sqlite:///{db_path}")
    eng = sql_engine_from_config(cfg)
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

    m = _MW.read_sql_table(t, sql_engine=eng)
    assert m.to_dict() == {"id": [2], "label": ["y"]}


def test_sql_dataframe_drop_slice_sort_in_memory() -> None:
    """Documented lazy-SQL-safe transforms: drop, head, slice, sort."""
    df = SqlDataFrame[_W](
        {"id": [3, 1, 2], "label": ["c", "a", "b"]},
        sql_config=_sync_config(),
    )
    assert df.drop("label").to_dict() == {"id": [3, 1, 2]}
    assert df.slice(0, 2).to_dict() == {"id": [3, 1], "label": ["c", "a"]}
    assert df.sort("id").to_dict() == {"id": [1, 2, 3], "label": ["a", "b", "c"]}


def test_sql_dataframe_empty_in_memory() -> None:
    df = SqlDataFrame[_S]({"id": []}, sql_config=_sync_config())
    assert df.to_dict() == {"id": []}
    assert df.head(3).to_dict() == {"id": []}


def test_sql_dataframe_from_sql_table_three_rows_sort_filter_not_used(
    tmp_path: Path,
) -> None:
    """Lazy table read + sort; multiple INSERT order."""
    from sqlalchemy import Column, Integer, MetaData, String, Table, insert

    db_path = tmp_path / "multi.db"
    cfg = EngineConfig(dsn=f"sqlite:///{db_path}")
    eng = sql_engine_from_config(cfg)
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
        for i, lab in [(3, "c"), (1, "a"), (2, "b")]:
            conn.execute(insert(t).values(id=i, label=lab))
        conn.commit()

    df = SqlDataFrame[_W].from_sql_table(t, sql_engine=eng)
    assert df.sort("id").to_dict() == {"id": [1, 2, 3], "label": ["a", "b", "c"]}
    # Unordered SELECT: do not assert raw ``head`` row order; chain ``sort`` first.
    assert df.sort("id").head(2).to_dict() == {"id": [1, 2], "label": ["a", "b"]}


def test_from_sql_table_accepts_sql_config_instead_of_engine(
    tmp_path: Path,
) -> None:
    """``from_sql_table(..., sql_config=)`` resolves a lazy-SQL engine internally."""
    from sqlalchemy import Column, Integer, MetaData, String, Table, insert

    db_path = tmp_path / "cfg_only.db"
    cfg = EngineConfig(dsn=f"sqlite:///{db_path}")
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
        conn.execute(insert(t).values(id=9, label="cfg"))
        conn.commit()

    df = SqlDataFrame[_W].from_sql_table(t, sql_config=cfg)
    assert df.to_dict() == {"id": [9], "label": ["cfg"]}


def test_from_sql_table_engine_mode_default_forces_default_engine(
    tmp_path: Path,
) -> None:
    """engine_mode='default' uses get_default_engine() with sql_config/sql_engine."""
    from pydantable.engine import get_default_engine
    from sqlalchemy import Column, Integer, MetaData, String, Table

    db_path = tmp_path / "engine_mode_default.db"
    cfg = EngineConfig(dsn=f"sqlite:///{db_path}")
    md = MetaData()
    t = Table(
        "items",
        md,
        Column("id", Integer, primary_key=True),
        Column("label", String(20)),
    )

    df = SqlDataFrame[_W].from_sql_table(t, sql_config=cfg, engine_mode="default")
    assert df._engine is get_default_engine()


def test_from_sql_table_engine_explicit_wins_over_engine_mode_default(
    tmp_path: Path,
) -> None:
    from sqlalchemy import Column, Integer, MetaData, String, Table

    db_path = tmp_path / "engine_mode_explicit.db"
    cfg = EngineConfig(dsn=f"sqlite:///{db_path}")
    explicit = sql_engine_from_config(cfg)
    md = MetaData()
    t = Table(
        "items",
        md,
        Column("id", Integer, primary_key=True),
        Column("label", String(20)),
    )

    df = SqlDataFrame[_W].from_sql_table(
        t, sql_config=cfg, engine=explicit, engine_mode="default"
    )
    assert df._engine is explicit


def test_sql_to_native_handoff_allows_native_transforms(tmp_path: Path) -> None:
    """SQL engine → materialize → native engine transforms."""
    from sqlalchemy import Column, Integer, MetaData, String, Table, insert

    db_path = tmp_path / "handoff.db"
    cfg = EngineConfig(dsn=f"sqlite:///{db_path}")
    eng = sql_engine_from_config(cfg)
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
        conn.execute(insert(t).values(id=2, label="b"))
        conn.commit()

    sql_df = SqlDataFrame[_W].from_sql_table(t, sql_engine=eng).select("id", "label")
    native_df = sql_df.to_native()
    out = native_df.with_columns(id2=native_df.id * 2).select("id2").to_dict()
    assert out == {"id2": [4]}


def test_read_sql_table_accepts_sql_config(tmp_path: Path) -> None:
    from sqlalchemy import Column, Integer, MetaData, String, Table, insert

    db_path = tmp_path / "read_cfg.db"
    cfg = EngineConfig(dsn=f"sqlite:///{db_path}")
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
        conn.execute(insert(t).values(id=5, label="m"))
        conn.commit()

    m = _MW.read_sql_table(t, sql_config=cfg)
    assert m.select("label").to_dict() == {"label": ["m"]}


def test_read_sql_table_engine_mode_default_forces_default_engine(
    tmp_path: Path,
) -> None:
    from pydantable.engine import get_default_engine
    from sqlalchemy import Column, Integer, MetaData, String, Table

    db_path = tmp_path / "read_engine_mode_default.db"
    cfg = EngineConfig(dsn=f"sqlite:///{db_path}")
    cm = ConnectionManager(cfg)
    md = MetaData()
    t = Table(
        "items",
        md,
        Column("id", Integer, primary_key=True),
        Column("label", String(20)),
    )
    md.create_all(cm.engine)

    m = _MW.read_sql_table(t, sql_config=cfg, engine_mode="default")
    assert m._df._engine is get_default_engine()


def test_sql_dataframe_model_to_native_handoff(tmp_path: Path) -> None:
    from sqlalchemy import Column, Integer, MetaData, String, Table, insert

    db_path = tmp_path / "dfm_to_native.db"
    cfg = EngineConfig(dsn=f"sqlite:///{db_path}")
    eng = sql_engine_from_config(cfg)
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
        conn.execute(insert(t).values(id=2, label="b"))
        conn.commit()

    m = _MW.read_sql_table(t, sql_engine=eng)
    native = m.to_native()
    out = native._df.with_columns(id2=native._df.id * 2).select("id2").to_dict()
    assert out == {"id2": [4]}


def test_to_sql_engine_dataframe_roundtrip(tmp_path: Path) -> None:
    """Native frame → to_sql_engine (in-memory root) → SQL engine execution."""
    from pydantable import DataFrame

    db_path = tmp_path / "to_sql_engine.db"
    cfg = EngineConfig(dsn=f"sqlite:///{db_path}")

    df = DataFrame[_W]({"id": [2, 1], "label": ["b", "a"]})
    sql_df = df.to_sql_engine(sql_config=cfg)
    assert sql_df.sort("id").to_dict() == {"id": [1, 2], "label": ["a", "b"]}


def test_to_sql_engine_engine_mode_default_forces_default_engine(
    tmp_path: Path,
) -> None:
    from pydantable import DataFrame
    from pydantable.engine import get_default_engine

    db_path = tmp_path / "to_sql_engine_default_engine.db"
    cfg = EngineConfig(dsn=f"sqlite:///{db_path}")

    df = DataFrame[_W]({"id": [1], "label": ["a"]})
    out = df.to_sql_engine(sql_config=cfg, engine_mode="default")
    assert out._engine is get_default_engine()


def test_to_sql_engine_dataframe_model_roundtrip(tmp_path: Path) -> None:
    from pydantable import DataFrameModel

    class M(DataFrameModel):
        id: int
        label: str

    db_path = tmp_path / "to_sql_engine_model.db"
    cfg = EngineConfig(dsn=f"sqlite:///{db_path}")

    m = M({"id": [2, 1], "label": ["b", "a"]})
    sql_m = m.to_sql_engine(sql_config=cfg)
    # Keep the sort key in the projection: the SQL engine sorts over row dicts
    # during execution, so dropping the key would be ambiguous.
    out = sql_m.sort("id").select("id", "label").to_dict()
    assert out == {"id": [1, 2], "label": ["a", "b"]}


def test_multi_engine_workflow_sql_write_then_native(tmp_path: Path) -> None:
    """Cookbook flow: SQL read → write → read → to_native → native transforms."""
    from pydantable.io.sql import write_sql_raw
    from sqlalchemy import Column, Integer, MetaData, String, Table, insert

    db_path = tmp_path / "multi_engine.db"
    cfg = EngineConfig(dsn=f"sqlite:///{db_path}")
    eng = sql_engine_from_config(cfg)
    cm = ConnectionManager(cfg)

    md = MetaData()
    src = Table(
        "src_items",
        md,
        Column("id", Integer, primary_key=True),
        Column("label", String(20)),
    )
    md.create_all(cm.engine)
    with cm.engine.connect() as conn:
        conn.execute(insert(src).values(id=2, label="b"))
        conn.execute(insert(src).values(id=1, label="a"))
        conn.commit()

    df = SqlDataFrame[_W].from_sql_table(src, sql_engine=eng).sort("id")
    cols = df.to_dict()
    # SQL write path (I/O helper), then re-read as a lazy SQL root.
    write_sql_raw(cols, "dst_items", cm.engine, if_exists="replace")

    md2 = MetaData()
    dst = Table("dst_items", md2, autoload_with=cm.engine)
    df2 = SqlDataFrame[_W].from_sql_table(dst, sql_engine=eng)
    native = df2.to_native()
    out = native.with_columns(id2=native.id * 2).select("id2").to_dict()
    assert out == {"id2": [2, 4]}


@pytest.mark.asyncio
async def test_sql_dataframe_model_async_chain_select(tmp_path: Path) -> None:
    from sqlalchemy import Column, Integer, MetaData, String, Table, insert

    db_path = tmp_path / "async_chain.db"
    cfg = EngineConfig(dsn=f"sqlite:///{db_path}")
    eng = sql_engine_from_config(cfg)
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
        conn.execute(insert(t).values(id=1, label="p"))
        conn.commit()

    m = _MW.read_sql_table(t, sql_engine=eng)
    slim = m.select("id")
    assert await slim.ato_dict() == {"id": [1]}


def test_sqlmodel_table_lazy_read_sql_dataframe(tmp_path: Path) -> None:
    """``SQLModel.__table__`` with ``from_sql_table`` (same idea as SQL_ENGINE.md)."""
    pytest.importorskip("sqlmodel")
    from sqlmodel import Field, Session, SQLModel

    class Item(SQLModel, table=True):
        id: int | None = Field(default=None, primary_key=True)
        label: str = Field(max_length=40)

    db_path = tmp_path / "sqlmodel.db"
    cfg = EngineConfig(dsn=f"sqlite:///{db_path}")
    eng = sql_engine_from_config(cfg)
    cm = ConnectionManager(cfg)

    SQLModel.metadata.create_all(cm.engine)
    with Session(cm.engine) as session:
        session.add(Item(id=7, label="sm"))
        session.commit()

    df = SqlDataFrame[_W].from_sql_table(Item.__table__, sql_engine=eng)
    assert df.to_dict() == {"id": [7], "label": ["sm"]}
    assert df.select("label").to_dict() == {"label": ["sm"]}


def test_sql_dataframe_expr_unsupported_message_consistent() -> None:
    """Multiple Expr entrypoints surface the backend name in the error."""
    df = SqlDataFrame[_W](
        {"id": [1], "label": ["x"]},
        sql_config=_sync_config(),
    )
    for op in (
        lambda: df.filter(df.id == 1),
        lambda: df.with_columns(x=df.id),
    ):
        with pytest.raises(UnsupportedEngineOperationError, match="Moltres"):
            op()


def test_sql_dataframe_from_sql_table_then_select_preserves_engine(
    tmp_path: Path,
) -> None:
    """Chained ops on lazy root still materialize."""
    from sqlalchemy import Column, Integer, MetaData, String, Table, insert

    db_path = tmp_path / "chain.db"
    cfg = EngineConfig(dsn=f"sqlite:///{db_path}")
    eng = sql_engine_from_config(cfg)
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
        conn.execute(insert(t).values(id=1, label="z"))
        conn.commit()

    base = SqlDataFrame[_W].from_sql_table(t, sql_engine=eng)
    slim = base.select("id")
    assert slim.to_dict() == {"id": [1]}
    assert slim.collect()[0].model_dump() == {"id": 1}


def test_pandas_module_lazy_sql_exports() -> None:
    import pydantable.pandas as pd
    from pydantable.pandas_sql_dataframe import SqlDataFrame as PandasSqlDF

    assert pd.SqlDataFrame is PandasSqlDF


def test_pyspark_module_lazy_sql_exports() -> None:
    from pydantable import pyspark as ps
    from pydantable.pyspark.sql_dataframe import SqlDataFrame as SparkSqlDF

    assert ps.SqlDataFrame is SparkSqlDF


def test_pandas_moltres_sql_dataframe_sort_values(tmp_path: Path) -> None:
    from pydantable.pandas_sql_dataframe import SqlDataFrame as PandasSqlDF

    eng = sql_engine_from_config(
        EngineConfig(dsn=f"sqlite:///{tmp_path}/t.db"),
    )
    df = PandasSqlDF[_S]({"id": [2, 1]}, sql_engine=eng)
    out = df.sort_values("id")
    assert isinstance(out, PandasSqlDF)
    assert out.to_dict() == {"id": [1, 2]}


def test_pyspark_moltres_sql_dataframe_order_by_preserves_engine(
    tmp_path: Path,
) -> None:
    """``_as_pyspark_df`` keeps the lazy-SQL engine on the frame."""
    from pydantable.pyspark.sql_dataframe import SqlDataFrame as SparkSqlDF

    eng = sql_engine_from_config(
        EngineConfig(dsn=f"sqlite:///{tmp_path}/w.db"),
    )
    df = SparkSqlDF[_S]({"id": [2, 1]}, sql_engine=eng)
    before = df._engine
    out = df.orderBy("id")
    assert isinstance(out, SparkSqlDF)
    assert out._engine is before
    assert out.to_dict() == {"id": [1, 2]}


def test_pyspark_moltres_sql_dataframe_to_df_preserves_engine(tmp_path: Path) -> None:
    """``toDF`` passes ``engine=`` through; materialize needs matching root columns."""
    from pydantable.pyspark.sql_dataframe import SqlDataFrame as SparkSqlDF

    eng = sql_engine_from_config(
        EngineConfig(dsn=f"sqlite:///{tmp_path}/todb.db"),
    )
    df = SparkSqlDF[_S]({"id": [1]}, sql_engine=eng)
    out = df.toDF("pk")
    assert isinstance(out, SparkSqlDF)
    assert out._engine is df._engine
