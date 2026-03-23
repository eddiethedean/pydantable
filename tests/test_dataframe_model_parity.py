"""Parity tests between DataFrame and DataFrameModel."""

from __future__ import annotations

import pytest
from pydantable import DataFrame, DataFrameModel, Schema


class P(Schema):
    id: int
    age: int | None


class PM(DataFrameModel):
    id: int
    age: int | None


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"validate_data": False},
        {"trusted_mode": "shape_only"},
        {"trusted_mode": "strict"},
    ],
)
def test_dataframe_and_model_collect_parity_across_ingest_modes(
    kwargs: dict[str, object],
) -> None:
    data = {"id": [1, 2, 3], "age": [20, None, 40]}
    core = DataFrame[P](data, **kwargs)
    model = PM(data, **kwargs)
    assert model.collect(as_lists=True) == core.collect(as_lists=True)


def test_dataframe_and_model_transform_parity() -> None:
    data = {"id": [1, 2, 3], "age": [20, None, 40]}
    core_df = DataFrame[P](data)
    core = (
        core_df.with_columns(age2=core_df.age + 1)
        .filter(core_df.id > 1)
        .select("id", "age2")
    )
    model_df = PM(data)
    model = (
        model_df.with_columns(age2=model_df.age + 1)
        .filter(model_df.id > 1)
        .select("id", "age2")
    )
    assert model.collect(as_lists=True) == core.collect(as_lists=True)


def test_dataframe_and_model_strict_error_parity() -> None:
    bad = {"id": ["x", "y"], "age": [10, 20]}
    with pytest.raises(ValueError, match="strict trusted mode"):
        DataFrame[P](bad, trusted_mode="strict")
    with pytest.raises(ValueError, match="strict trusted mode"):
        PM(bad, trusted_mode="strict")


class MapSchema(Schema):
    m: dict[str, int]


class MapModel(DataFrameModel):
    m: dict[str, int]


def test_dataframe_and_model_map_from_entries_roundtrip_parity() -> None:
    data = {"m": [{"a": 1, "b": 2}, {"c": 3}]}
    core_df = DataFrame[MapSchema](data)
    core = core_df.with_columns(
        r=core_df.m.map_entries().map_from_entries(),
    )
    model_df = MapModel(data)
    model = model_df.with_columns(
        r=model_df.m.map_entries().map_from_entries(),
    )
    assert model.collect(as_lists=True) == core.collect(as_lists=True)


def test_dataframe_and_model_element_at_parity() -> None:
    data = {"m": [{"x": 10}, {"x": 20, "y": 30}]}
    core_df = DataFrame[MapSchema](data)
    core = core_df.with_columns(v=core_df.m.element_at("x"))
    model_df = MapModel(data)
    model = model_df.with_columns(v=model_df.m.element_at("x"))
    assert model.collect(as_lists=True) == core.collect(as_lists=True)
