from __future__ import annotations

import pytest
from conftest import assert_table_eq_sorted
from pydantable import DataFrameModel as PolarsDataFrameModel
from pydantable import Schema
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
    assert_table_eq_sorted(
        a.collect(as_lists=True), b.collect(as_lists=True), keys=["id"]
    )


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
    assert_table_eq_sorted(
        j.collect(as_lists=True), m.collect(as_lists=True), keys=["id"]
    )


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


def test_pandas_ui_matches_default_for_pipeline() -> None:
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
    assert_table_eq_sorted(
        p2.collect(as_lists=True), pd2.collect(as_lists=True), keys=["id"]
    )


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
    assert sub.collect(as_lists=True) == {"id": [1, 2]}
    assert sub.columns == ["id"]


def test_pandas_ui_head_tail() -> None:
    class User(PandasDataFrameModel):
        id: int

    df = User({"id": [1, 2, 3]})
    assert df.head(2).collect(as_lists=True) == {"id": [1, 2]}
    assert df.tail(2).collect(as_lists=True) == {"id": [2, 3]}


def test_pandas_core_head_tail_empty_columns() -> None:
    from pydantable.pandas import DataFrame as PDF

    class Row(Schema):
        x: int

    df = PDF[Row]({"x": []})
    assert df.head(3).collect(as_lists=True) == {"x": []}
    assert df.tail(3).collect(as_lists=True) == {"x": []}


def test_pandas_core_query_not_implemented() -> None:
    from pydantable.pandas import DataFrame as PDF

    class Row(Schema):
        x: int

    df = PDF[Row]({"x": [1]})
    with pytest.raises(NotImplementedError, match="filter\\(Expr\\)"):
        df.query("x > 0")


def test_pandas_ui_merge_default_suffix_when_single_tuple_element() -> None:
    class L(PandasDataFrameModel):
        id: int
        v: int

    class R(PandasDataFrameModel):
        id: int
        v: int

    left = L({"id": [1], "v": [10]})
    right = R({"id": [1], "v": [20]})
    out = left.merge(right, on="id", how="inner", suffixes=("_only",))
    cols = list(out.schema_fields().keys())
    assert any(c.endswith("_right") for c in cols)


def test_pandas_model_group_mean_and_count() -> None:
    class Row(PandasDataFrameModel):
        k: int
        v: int

    df = Row({"k": [1, 1], "v": [10, 20]})
    gm = df.group_by("k").mean("v").collect(as_lists=True)
    gc = df.group_by("k").count("v").collect(as_lists=True)
    assert gm["v_mean"] == [15]
    assert gc["v_count"] == [2]


def test_pandas_ui_merge_rejects_extra_kwargs() -> None:
    class L(PandasDataFrameModel):
        a: int

    class R(PandasDataFrameModel):
        a: int

    with pytest.raises(TypeError, match="unsupported keyword"):
        L({"a": [1]}).merge(R({"a": [1]}), on="a", copy=False)


def test_pandas_ui_merge_requires_on() -> None:
    class L(PandasDataFrameModel):
        a: int

    class R(PandasDataFrameModel):
        a: int

    with pytest.raises(TypeError, match="requires on"):
        L({"a": [1]}).merge(R({"a": [1]}))


def test_pandas_ui_merge_rejects_indicator_and_validate() -> None:
    class L(PandasDataFrameModel):
        a: int

    class R(PandasDataFrameModel):
        a: int

    base = L({"a": [1]}), R({"a": [1]})
    with pytest.raises(NotImplementedError, match="indicator"):
        base[0].merge(base[1], on="a", indicator=True)
    with pytest.raises(NotImplementedError, match="validate"):
        base[0].merge(base[1], on="a", validate="one_to_one")


def test_pandas_ui_assign_rejects_series_like() -> None:
    class User(PandasDataFrameModel):
        id: int

    class _FakeSeries:
        pass

    _FakeSeries.__name__ = "Series"
    _FakeSeries.__module__ = "pandas.core.series"
    df = User({"id": [1]})
    with pytest.raises(TypeError, match="Series"):
        df.assign(x=_FakeSeries())


def test_pandas_ui_getitem_errors() -> None:
    from pydantable.pandas import DataFrame, Schema

    class Row(Schema):
        a: int
        b: int

    df = DataFrame[Row]({"a": [1], "b": [2]})
    with pytest.raises(ValueError, match="non-empty"):
        df[[]]
    with pytest.raises(TypeError, match="supports"):
        df[object()]  # type: ignore[index]


def test_pandas_ui_groupby_requires_columns() -> None:
    class Row(PandasDataFrameModel):
        k: int
        v: int

    df = Row({"k": [1], "v": [2]})
    with pytest.raises(TypeError, match="at least one column"):
        df.group_by("k").sum()
    with pytest.raises(TypeError, match="at least one column"):
        df.group_by("k").mean()
    with pytest.raises(TypeError, match="at least one column"):
        df.group_by("k").count()


def test_pandas_model_merge_type_error() -> None:
    class User(PandasDataFrameModel):
        id: int

    df = User({"id": [1]})
    with pytest.raises(TypeError, match="DataFrameModel"):
        df.merge(object(), on="id")  # type: ignore[arg-type]


def test_pandas_ui_groupby_sum_alias() -> None:
    class Row(PandasDataFrameModel):
        k: int
        v: int

    df = Row({"k": [1, 1, 2], "v": [10, 20, 30]})
    a = df.group_by("k").sum("v")
    b = df.group_by("k").agg(v_sum=("sum", "v"))
    assert_table_eq_sorted(
        a.collect(as_lists=True), b.collect(as_lists=True), keys=["k"]
    )
