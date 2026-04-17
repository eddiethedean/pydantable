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


def test_sql_dataframe_where_pushdown(tmp_path: Path) -> None:
    from sqlalchemy import Column, Integer, MetaData, String, Table, insert

    db_path = tmp_path / "where.db"
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
        conn.execute(insert(t).values(id=3, label="c"))
        conn.commit()

    df = SqlDataFrame[Row].from_sql_table(t, sql_engine=eng)
    out = df.where(t.c.id > 1).to_dict()
    assert out == {"id": [2, 3], "label": ["b", "c"]}
