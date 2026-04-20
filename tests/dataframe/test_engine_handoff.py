from __future__ import annotations

import pytest
from pydantable import DataFrame, Schema
from pydantable.engine import NativePolarsEngine, get_default_engine, set_default_engine


class Row(Schema):
    x: int


def test_to_engine_requires_target_engine() -> None:
    df = DataFrame[Row]({"x": [1]})
    with pytest.raises(TypeError, match="target_engine"):
        df.to_engine(None)  # type: ignore[arg-type]


def test_to_native_reuses_default_native_engine_instance() -> None:
    eng = NativePolarsEngine()
    set_default_engine(eng)
    try:
        df = DataFrame[Row]({"x": [1]})
        out = df.to_native()
        assert out._engine is eng
        assert out.to_dict() == {"x": [1]}
    finally:
        set_default_engine(None)


def test_to_engine_roundtrips_columns_into_target_engine() -> None:
    df = DataFrame[Row]({"x": [2]})
    eng = get_default_engine()
    df2 = df.to_engine(eng)
    assert df2._engine is eng
    assert df2.to_dict() == {"x": [2]}

