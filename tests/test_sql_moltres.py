"""SqlDataFrame / SqlDataFrameModel with moltres-core (optional extra)."""

from __future__ import annotations

import pytest

pytest.importorskip("moltres_core")

from moltres_core import EngineConfig
from pydantable import Schema
from pydantable.sql_moltres import (
    SqlDataFrame,
    SqlDataFrameModel,
    moltres_engine_from_sql_config,
)


class _S(Schema):
    id: int


def test_moltres_engine_from_sql_config() -> None:
    cfg = EngineConfig(dsn="sqlite:///:memory:")
    eng = moltres_engine_from_sql_config(cfg)
    assert eng.capabilities.backend == "custom"


def test_sql_dataframe_sql_config() -> None:
    cfg = EngineConfig(dsn="sqlite:///:memory:")
    df = SqlDataFrame[_S]({"id": [1, 2]}, sql_config=cfg)
    assert df.to_dict() == {"id": [1, 2]}


def test_sql_dataframe_explicit_engine() -> None:
    cfg = EngineConfig(dsn="sqlite:///:memory:")
    eng = moltres_engine_from_sql_config(cfg)
    df = SqlDataFrame[_S]({"id": [3]}, moltres_engine=eng)
    assert df.to_dict() == {"id": [3]}


class _M(SqlDataFrameModel):
    id: int


def test_sql_dataframe_model() -> None:
    cfg = EngineConfig(dsn="sqlite:///:memory:")
    m = _M({"id": [1]}, sql_config=cfg)
    assert m.to_dict() == {"id": [1]}


def test_sql_dataframe_requires_engine_args() -> None:
    with pytest.raises(TypeError, match="sql_config"):
        SqlDataFrame[_S]({"id": [1]})
