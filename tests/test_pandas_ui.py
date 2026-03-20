from __future__ import annotations

import pytest
from conftest import assert_table_eq_sorted
from pydantable import DataFrameModel as PolarsDataFrameModel
from pydantable.pandas import DataFrameModel as PandasDataFrameModel


def test_pandas_ui_assign_matches_with_columns() -> None:
    class User(PandasDataFrameModel):
        id: int
        age: int | None

    payload = {"id": [1, 2], "age": [20, None]}
    df = User(payload)
    a0 = df.assign(age2=df.age * 2).select("id", "age2")
    a = a0.filter(a0.age2 > 10)
    b0 = df.with_columns(age2=df.age * 2).select("id", "age2")
    b = b0.filter(b0.age2 > 10)
    assert_table_eq_sorted(a.collect(), b.collect(), keys=["id"])


def test_pandas_ui_merge_matches_join() -> None:
    class Left(PandasDataFrameModel):
        id: int
        score: int

    class Right(PandasDataFrameModel):
        id: int
        country: str
        score: int

    left = Left({"id": [1, 2], "score": [10, 20]})
    right = Right({"id": [1, 2], "country": ["US", "CA"], "score": [100, 200]})
    j = left.join(right, on="id", how="inner", suffix="_r")
    m = left.merge(right, on="id", how="inner", suffixes=("_x", "_r"))
    assert_table_eq_sorted(j.collect(), m.collect(), keys=["id"])


def test_pandas_ui_assign_rejects_callable() -> None:
    class User(PandasDataFrameModel):
        id: int

    df = User({"id": [1]})
    with pytest.raises(TypeError, match="callable"):
        df.assign(bad=lambda x: x)


def test_pandas_ui_query_not_implemented() -> None:
    class User(PandasDataFrameModel):
        id: int

    df = User({"id": [1]})
    with pytest.raises(NotImplementedError, match="filter\\(Expr\\)"):
        df.query("id > 0")


def test_pandas_ui_merge_rejects_left_on() -> None:
    class L(PandasDataFrameModel):
        a: int

    class R(PandasDataFrameModel):
        b: int

    with pytest.raises(NotImplementedError, match="left_on"):
        L({"a": [1]}).merge(R({"b": [1]}), on="a", left_on="a", right_on="b")


def test_pandas_backend_matches_polars_for_pipeline() -> None:
    class UserP(PolarsDataFrameModel):
        id: int
        age: int | None

    class UserPd(PandasDataFrameModel):
        id: int
        age: int | None

    payload = {"id": [1, 2], "age": [20, None]}
    p = UserP(payload)
    pd_df = UserPd(payload)
    p1 = p.with_columns(age2=p.age * 2).select("id", "age2")
    p2 = p1.filter(p1.age2 > 10)
    pd1 = pd_df.with_columns(age2=pd_df.age * 2).select("id", "age2")
    pd2 = pd1.filter(pd1.age2 > 10)
    assert_table_eq_sorted(p2.collect(), pd2.collect(), keys=["id"])


def test_pandas_ui_introspection_and_getitem() -> None:
    class User(PandasDataFrameModel):
        id: int
        age: int | None

    df = User({"id": [1, 2], "age": [3, 4]})
    assert df.columns == ["id", "age"]
    assert df.shape == (2, 2)
    assert df.empty is False
    assert "id" in df.dtypes
    sub = df[["id"]]
    assert sub.collect() == {"id": [1, 2]}
    assert sub.columns == ["id"]


def test_pandas_ui_head_tail() -> None:
    class User(PandasDataFrameModel):
        id: int

    df = User({"id": [1, 2, 3]})
    assert df.head(2).collect() == {"id": [1, 2]}
    assert df.tail(2).collect() == {"id": [2, 3]}


def test_pandas_ui_groupby_sum_alias() -> None:
    class Row(PandasDataFrameModel):
        k: int
        v: int

    df = Row({"k": [1, 1, 2], "v": [10, 20, 30]})
    a = df.group_by("k").sum("v")
    b = df.group_by("k").agg(v_sum=("sum", "v"))
    assert_table_eq_sorted(a.collect(), b.collect(), keys=["k"])
