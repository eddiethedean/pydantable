from __future__ import annotations

import pytest
from pydantable import DataFrame, Schema
from pydantable.engine import get_default_engine

pytest.importorskip(
    "raikou_core",
    reason='pip install "pydantable[spark]"',
)


class Row(Schema):
    x: int


def test_to_spark_engine_engine_mode_default_forces_default_engine() -> None:
    df = DataFrame[Row]({"x": [1]})
    out = df.to_spark_engine(engine_mode="default")
    assert out._engine is get_default_engine()


def test_to_spark_engine_explicit_engine_wins_over_engine_mode_default() -> None:
    from raikou_core.engine import SparkExecutionEngine

    explicit = SparkExecutionEngine()
    df = DataFrame[Row]({"x": [1]})
    out = df.to_spark_engine(engine=explicit, engine_mode="default")
    assert out._engine is explicit

