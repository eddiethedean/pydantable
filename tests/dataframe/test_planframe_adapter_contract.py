"""Verify :class:`PydantableAdapter` implements PlanFrame ``BaseAdapter`` fully.

When PlanFrame adds abstract methods to ``BaseAdapter``, extend
``test_pydantable_adapter_defines_all_base_adapter_abstract_methods`` and implement
them on ``PydantableAdapter`` before upgrading the pin.
"""

from __future__ import annotations

import pytest
from planframe.backend.adapter import BaseAdapter
from pydantable.planframe_adapter.adapter import PydantableAdapter


def test_pydantable_adapter_defines_all_base_adapter_abstract_methods() -> None:
    abstract = {
        name
        for name, member in BaseAdapter.__dict__.items()
        if getattr(member, "__isabstractmethod__", False)
    }
    missing = sorted(
        name
        for name in abstract
        if name not in PydantableAdapter.__dict__
        or getattr(PydantableAdapter.__dict__[name], "__isabstractmethod__", False)
    )
    assert not missing, f"PydantableAdapter must implement: {missing}"


def test_pydantable_adapter_hint_is_base_noop() -> None:
    """``hint`` defaults to identity (BaseAdapter); pydantable does not override."""

    from pydantable.dataframe import DataFrame
    from pydantic import BaseModel

    class M(BaseModel):
        a: int

    df = DataFrame[M]({"a": [1]})
    ad = PydantableAdapter(engine=df._engine)
    out = ad.hint(df, hints=("broadcast",), kv={})
    assert out is df


@pytest.fixture
def minimal_adapter() -> PydantableAdapter:
    from pydantable.dataframe import DataFrame
    from pydantic import BaseModel

    class S(BaseModel):
        x: int

    return PydantableAdapter(engine=DataFrame[S]({"x": [1]})._engine)


def test_compile_expr_smoke(minimal_adapter: PydantableAdapter) -> None:
    from types import SimpleNamespace

    from planframe.expr import api as pf

    schema_obj = SimpleNamespace(
        fields=(SimpleNamespace(name="x", dtype=int),),
    )
    out = minimal_adapter.compile_expr(pf.col("x"), schema=schema_obj)
    assert hasattr(out, "referenced_columns")
