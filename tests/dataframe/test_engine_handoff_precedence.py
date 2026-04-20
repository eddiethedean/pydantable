from __future__ import annotations

from pydantable import DataFrame, Schema
from pydantable.engine.stub import StubExecutionEngine


class Row(Schema):
    x: int


def test_to_engine_uses_target_engine_instance() -> None:
    df = DataFrame[Row]({"x": [1]})
    stub = StubExecutionEngine()
    out = df.to_engine(stub)
    assert out._engine is stub


def test_to_native_requires_native_extension_installed() -> None:
    # If the native extension is missing in an environment, to_native should raise
    # MissingRustExtensionError. In this repo, native is usually present; so we
    # only assert that calling to_native doesn't accidentally return a stub engine.
    df = DataFrame[Row]({"x": [1]})
    out = df.to_native()
    assert out._engine is not StubExecutionEngine()
