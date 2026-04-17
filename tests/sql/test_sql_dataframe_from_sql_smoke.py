from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

pytest.importorskip("moltres_core")

from moltres_core import ConnectionManager, EngineConfig
from pydantable.schema import Schema
from pydantable.sql_dataframe import SqlDataFrame, sql_engine_from_config

if TYPE_CHECKING:
    from pathlib import Path


class Row(Schema):
    id: int
    label: str


def test_sql_dataframe_from_sql_selectable(tmp_path: Path) -> None:
    from sqlalchemy import Column, Integer, MetaData, String, Table, insert, select

    db_path = tmp_path / "from_sql.db"
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
        conn.execute(insert(t).values(id=1, label="a"))
        conn.execute(insert(t).values(id=2, label="b"))
        conn.commit()

    stmt = select(t.c.id, t.c.label)
    df = SqlDataFrame[Row].from_sql(stmt, sql_engine=eng)
    assert df.to_dict() == {"id": [1, 2], "label": ["a", "b"]}


def test_sql_dataframe_from_sql_missing_schema_column(tmp_path: Path) -> None:
    from sqlalchemy import Column, Integer, MetaData, Table, insert, select

    db_path = tmp_path / "from_sql_missing.db"
    cfg = EngineConfig(dsn=f"sqlite:///{db_path}")
    eng = sql_engine_from_config(cfg)
    cm = ConnectionManager(cfg)

    md = MetaData()
    t = Table(
        "items",
        md,
        Column("id", Integer, primary_key=True),
    )
    md.create_all(cm.engine)
    with cm.engine.connect() as conn:
        conn.execute(insert(t).values(id=1))
        conn.commit()

    stmt = select(t.c.id)
    with pytest.raises(ValueError, match="missing schema columns"):
        SqlDataFrame[Row].from_sql(stmt, sql_engine=eng)
