from __future__ import annotations

import pytest


def test_unsupported_engine_operation_message_includes_backend_and_capability() -> None:
    from pydantable.errors import unsupported_engine_operation

    exc = unsupported_engine_operation(
        backend="stub",
        operation="expression_runtime",
        required_capability="rust_expression_runtime",
        hint="Install native extension.",
    )
    msg = str(exc)
    assert "Backend 'stub':" in msg
    assert "unsupported operation 'expression_runtime'" in msg
    assert "Requires capability 'rust_expression_runtime'" in msg
    assert "Install native extension." in msg


def test_stub_engine_rust_core_message_uses_helper() -> None:
    from pydantable.engine.stub import StubExecutionEngine

    eng = StubExecutionEngine()
    with pytest.raises(Exception) as ei:
        _ = eng.rust_core
    assert "Backend 'stub':" in str(ei.value)
    assert "Requires capability 'rust_expression_runtime'" in str(ei.value)
