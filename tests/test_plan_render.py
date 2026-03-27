from __future__ import annotations

from typing import Any, cast

import pytest
from pydantable.plan import explain, render_plan_text


def test_render_plan_text_minimal() -> None:
    text = render_plan_text({"version": 2, "steps": []})
    assert "Plan(version=2)" in text
    assert "steps: 0" in text


def test_render_plan_text_optional_meta_and_invalid_step() -> None:
    plan = {
        "version": 1,
        "engine_streaming": True,
        "root_data_kind": "dict",
        "steps": [
            "not-a-dict",
            {"kind": "unknown_op", "extra": 1},
        ],
    }
    text = render_plan_text(plan)
    assert "engine_streaming: True" in text
    assert "root_data: dict" in text
    assert "steps: 2" in text
    assert "0: <invalid step>" in text
    assert "1: unknown_op" in text


@pytest.mark.parametrize(
    ("step", "needle"),
    [
        ({"kind": "select", "columns": ["a", "b"]}, "select(['a', 'b'])"),
        (
            {"kind": "with_columns", "columns": {"z": {}, "a": {}}},
            "with_columns(['a', 'z'])",
        ),
        ({"kind": "filter"}, "filter(...)"),
        (
            {"kind": "sort", "by": ["x"], "descending": [True]},
            "sort(by=['x'], descending=[True])",
        ),
        (
            {"kind": "unique", "subset": ["s"], "keep": "first"},
            "unique(subset=['s'], keep=first)",
        ),
        ({"kind": "rename", "columns": {"a": "b"}}, "rename({'a': 'b'})"),
        ({"kind": "slice", "offset": 1, "length": 10}, "slice(offset=1, length=10)"),
        (
            {"kind": "fill_null", "subset": ["c"], "value": 0, "strategy": None},
            "fill_null(subset=['c'], value=0, strategy=None)",
        ),
        ({"kind": "drop_nulls", "subset": ["c"]}, "drop_nulls(subset=['c'])"),
        (
            {"kind": "global_select", "columns": {"b": 1, "a": 2}},
            "global_select(['a', 'b'])",
        ),
    ],
)
def test_render_plan_text_step_kinds(step: dict[str, Any], needle: str) -> None:
    text = render_plan_text({"steps": [step]})
    assert needle in text


def test_explain_json_and_text() -> None:
    class _Plan:
        def to_serializable(self) -> dict[str, Any]:
            return {"version": 3, "steps": []}

    p = _Plan()
    d = explain(p, format="json")
    assert isinstance(d, dict)
    assert d["version"] == 3
    d2 = explain(p, format="json", engine_streaming=False, root_data_kind="lists")
    assert d2["engine_streaming"] is False
    assert d2["root_data_kind"] == "lists"

    t = explain(p, format="text")
    assert "Plan(version=3)" in t


def test_explain_bad_format() -> None:
    class _Plan:
        def to_serializable(self) -> dict[str, Any]:
            return {"version": 1, "steps": []}

    with pytest.raises(ValueError, match="Unsupported format"):
        explain(_Plan(), format=cast("Any", "xml"))
