"""Registry and protocol checks for :class:`~pydantable.engine.stub.StubExecutionEngine`."""

from __future__ import annotations

import pytest

from pydantable.errors import UnsupportedEngineOperationError
from pydantable.engine import (
    ExecutionEngine,
    NativePolarsEngine,
    get_default_engine,
    set_default_engine,
)
from pydantable.engine.stub import StubExecutionEngine


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
