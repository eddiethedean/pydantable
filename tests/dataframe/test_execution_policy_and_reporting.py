from __future__ import annotations

import pytest


def test_engine_report_and_explain_execution_are_stable_dicts() -> None:
    from pydantable import DataFrame, Schema

    class Row(Schema):
        id: int

    df = DataFrame[Row]({"id": [1, 2, 3]})
    rep = df.engine_report()
    assert isinstance(rep, dict)
    assert rep["engine_type"]
    assert rep["root_kind"]

    exp = df.explain_execution()
    assert isinstance(exp, dict)
    assert "engine_report" in exp


def test_execution_policy_strict_modes_raise_actionable_error() -> None:
    from pydantable import DataFrame, Schema
    from pydantable.engine.stub import StubExecutionEngine

    class Row(Schema):
        id: int

    df = DataFrame[Row]({"id": [1]}, engine=StubExecutionEngine())

    with pytest.raises(Exception) as ei:
        _ = df.to_dict(execution_policy="pushdown")
    assert "execution_policy='fallback_to_native'" in str(
        ei.value
    ) or "To allow fallback" in str(ei.value)
