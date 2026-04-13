"""Regression: ``native_engine_capabilities`` returns a structured result."""

from __future__ import annotations

from pydantable.engine.protocols import EngineCapabilities, native_engine_capabilities


def test_native_engine_capabilities_is_structured_without_crashing() -> None:
    caps = native_engine_capabilities()
    assert isinstance(caps, EngineCapabilities)
    assert caps.backend == "native"
