"""Registry and protocol checks for StubExecutionEngine."""

from __future__ import annotations

import pytest
from pydantable import DataFrameModel
from pydantable.engine import (
    ExecutionEngine,
    NativePolarsEngine,
    get_default_engine,
    set_default_engine,
)
from pydantable.engine.stub import StubExecutionEngine
from pydantable.errors import UnsupportedEngineOperationError


class _Row(DataFrameModel):
    x: int


def test_dataframe_model_passes_engine_to_inner_dataframe() -> None:
    eng = NativePolarsEngine()
    m = _Row({"x": [1, 2]}, engine=eng)
    assert m._df._engine is eng


def test_stub_engine_capabilities_and_protocol() -> None:
    stub = StubExecutionEngine()
    assert stub.capabilities.backend == "stub"
    assert isinstance(stub, ExecutionEngine)


def test_set_default_engine_round_trip() -> None:
    prev = get_default_engine()
    assert isinstance(prev, NativePolarsEngine)
    try:
        set_default_engine(StubExecutionEngine())
        assert isinstance(get_default_engine(), StubExecutionEngine)
    finally:
        set_default_engine(None)
    assert isinstance(get_default_engine(), NativePolarsEngine)


def test_expression_runtime_requires_native_or_override() -> None:
    from pydantable.engine import get_expression_runtime, set_expression_runtime

    set_default_engine(StubExecutionEngine())
    try:
        with pytest.raises(UnsupportedEngineOperationError):
            get_expression_runtime()
    finally:
        set_default_engine(None)
        set_expression_runtime(None)

    assert get_expression_runtime() is not None


def test_pandas_rolling_routes_plan_rolling_agg_through_frame_engine() -> None:
    from pydantable.pandas import DataFrame as PandasDF
    from pydantable.schema import Schema

    class V(Schema):
        x: int

    stub = StubExecutionEngine()
    df = PandasDF[V]({"x": [1, 2, 3]}, engine=stub)
    assert df._engine is stub
    with pytest.raises(UnsupportedEngineOperationError, match="plan_rolling_agg"):
        df.rolling(window=2, min_periods=1).sum("x")
