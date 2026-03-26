from __future__ import annotations

from pydantable import DataFrame, Schema


class Row(Schema):
    x: int
    y: int | None


def test_explain_text_includes_steps() -> None:
    df = DataFrame[Row]({"x": [1, 2, 3], "y": [None, 2, 3]})
    out = df.with_columns(z=df.x + 1).filter(df.y.is_not_null()).explain()
    assert "with_columns" in out
    assert "filter" in out


def test_explain_json_is_serializable_shape() -> None:
    df = DataFrame[Row]({"x": [1, 2, 3], "y": [None, 2, 3]})
    j = df.select("x").explain(format="json")
    assert isinstance(j, dict)
    assert j["version"] == 1
    assert isinstance(j["steps"], list)
    assert j.get("engine_streaming") in (True, False)
    assert j.get("root_data_kind") in ("in_memory", "scan_file_root")
