"""Contract tests for the execution engine abstraction."""

from __future__ import annotations

from pydantable.engine import (
    NativePolarsEngine,
    get_default_engine,
    native_engine_capabilities,
)
from pydantable.engine.protocols import ExecutionEngine, PlanExecutor, SinkWriter


def test_get_default_engine_is_native_singleton() -> None:
    e1 = get_default_engine()
    e2 = get_default_engine()
    assert isinstance(e1, NativePolarsEngine)
    assert e1 is e2


def test_native_engine_is_structural_plan_executor_and_sink_writer() -> None:
    eng = NativePolarsEngine()
    assert isinstance(eng, PlanExecutor)
    assert isinstance(eng, SinkWriter)
    assert isinstance(eng, ExecutionEngine)


def test_capabilities_match_engine_surface() -> None:
    caps = native_engine_capabilities()
    eng = get_default_engine()
    assert caps.backend == "native"
    assert eng.capabilities == caps
    if caps.extension_loaded:
        assert caps.has_execute_plan
