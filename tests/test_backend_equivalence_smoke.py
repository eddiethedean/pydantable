from __future__ import annotations

import importlib

import pytest
from conftest import assert_table_eq_sorted
from pydantable import DataFrameModel as PolarsDataFrameModel


@pytest.mark.parametrize("backend_mod", ["pydantable.pandas", "pydantable.pyspark"])
def test_backend_equivalence_select_with_columns_filter_collect(
    backend_mod: str,
) -> None:
    backend = importlib.import_module(backend_mod)
    BackendDataFrameModel = backend.DataFrameModel

    class UserDefault(PolarsDataFrameModel):
        id: int
        age: int | None

    class UserBackend(BackendDataFrameModel):
        id: int
        age: int | None

    payload = {"id": [1, 2], "age": [20, None]}

    default_df = UserDefault(payload)
    backend_df = UserBackend(payload)

    default_df2 = default_df.with_columns(age2=default_df.age * 2).select("id", "age2")
    default_out = default_df2.filter(default_df2.age2 > 10).collect()

    backend_df2 = backend_df.with_columns(age2=backend_df.age * 2).select("id", "age2")
    backend_out = backend_df2.filter(backend_df2.age2 > 10).collect()

    assert_table_eq_sorted(default_out, backend_out, keys=["id"])


@pytest.mark.parametrize("backend_mod", ["pydantable.pandas", "pydantable.pyspark"])
def test_backend_equivalence_join_and_groupby_all_null_semantics(
    backend_mod: str,
) -> None:
    backend = importlib.import_module(backend_mod)
    BackendDataFrameModel = backend.DataFrameModel

    class LeftDefault(PolarsDataFrameModel):
        id: int
        age: int | None
        score: int

    class RightDefault(PolarsDataFrameModel):
        id: int
        age: int | None
        country: str
        score: int

    class LeftBackend(BackendDataFrameModel):
        id: int
        age: int | None
        score: int

    class RightBackend(BackendDataFrameModel):
        id: int
        age: int | None
        country: str
        score: int

    left_payload = {"id": [1, 1], "age": [None, None], "score": [10, 20]}
    right_payload = {
        "id": [1, 2],
        "age": [None, None],
        "country": ["US", "CA"],
        "score": [100, 200],
    }

    default_left = LeftDefault(left_payload)
    default_right = RightDefault(right_payload)
    backend_left = LeftBackend(left_payload)
    backend_right = RightBackend(right_payload)

    default_joined = default_left.join(default_right, on="id", how="inner", suffix="_r")
    backend_joined = backend_left.join(backend_right, on="id", how="inner", suffix="_r")

    default_join_out = default_joined.collect()
    backend_join_out = backend_joined.collect()
    assert_table_eq_sorted(default_join_out, backend_join_out, keys=["id"])

    # all-null group semantics: sum/mean -> None, count -> 0
    default_grouped = default_joined.group_by("id").agg(
        age_sum=("sum", "age"),
        age_mean=("mean", "age"),
        age_count=("count", "age"),
    )
    backend_grouped = backend_joined.group_by("id").agg(
        age_sum=("sum", "age"),
        age_mean=("mean", "age"),
        age_count=("count", "age"),
    )

    default_group_out = default_grouped.collect()
    backend_group_out = backend_grouped.collect()
    assert_table_eq_sorted(default_group_out, backend_group_out, keys=["id"])


@pytest.mark.parametrize("backend_mod", ["pydantable.pandas"])
def test_backend_equivalence_assign_merge_collect(backend_mod: str) -> None:
    backend = importlib.import_module(backend_mod)
    BackendDataFrameModel = backend.DataFrameModel

    class LeftDefault(PolarsDataFrameModel):
        id: int
        score: int

    class RightDefault(PolarsDataFrameModel):
        id: int
        country: str
        score: int

    class LeftBackend(BackendDataFrameModel):
        id: int
        score: int

    class RightBackend(BackendDataFrameModel):
        id: int
        country: str
        score: int

    left_payload = {"id": [1, 2], "score": [10, 20]}
    right_payload = {"id": [1, 2], "country": ["US", "CA"], "score": [100, 200]}

    dl, dr = LeftDefault(left_payload), RightDefault(right_payload)
    bl, br = LeftBackend(left_payload), RightBackend(right_payload)

    default_m = dl.join(dr, on="id", how="inner", suffix="_r")
    backend_m = bl.merge(br, on="id", how="inner", suffixes=("_x", "_r"))
    assert_table_eq_sorted(default_m.collect(), backend_m.collect(), keys=["id"])

    default_a = dl.with_columns(twice=dl.score * 2).select("id", "twice")
    backend_a = bl.assign(twice=bl.score * 2).select("id", "twice")
    assert_table_eq_sorted(default_a.collect(), backend_a.collect(), keys=["id"])
