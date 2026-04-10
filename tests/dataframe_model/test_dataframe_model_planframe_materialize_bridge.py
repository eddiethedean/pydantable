from __future__ import annotations

from planframe.expr import api as pf
from pydantable import DataFrameModel
from pydantable.planframe_adapter import (
    amaterialize_dataframe_model,
    materialize_dataframe_model,
)


class Before(DataFrameModel):
    id: int
    age: int


class After(DataFrameModel):
    id: int


def test_materialize_dataframe_model_from_planframe_chain() -> None:
    df = Before({"id": [1, 2, 3], "age": [10, 0, 20]})
    pf_out = df.planframe.filter(pf.col("age") > 0).select("id")
    out = materialize_dataframe_model(pf_out, After)
    assert isinstance(out, After)
    assert out.to_dict() == {"id": [1, 3]}


async def test_amaterialize_dataframe_model_from_planframe_chain() -> None:
    df = Before({"id": [1, 2, 3], "age": [10, 0, 20]})
    pf_out = df.planframe.filter(pf.col("age") > 0).select("id")
    out = await amaterialize_dataframe_model(pf_out, After)
    assert isinstance(out, After)
    assert out.to_dict() == {"id": [1, 3]}
