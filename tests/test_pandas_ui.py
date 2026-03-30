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
    out = df.assign(ok=lambda d: d.id + 1).collect(as_lists=True)
    assert out["ok"] == [2]


def test_pandas_ui_assign_callable_must_return_expr_or_literal() -> None:
    class User(PandasDataFrameModel):
        id: int

    df = User({"id": [1]})
    with pytest.raises(TypeError):
        df.assign(bad=lambda d: object())


def test_pandas_ui_query_string_support() -> None:
    class User(PandasDataFrameModel):
        id: int

    df = User({"id": [1]})
    assert df.query("id > 0").collect(as_lists=True) == {"id": [1]}


def test_pandas_ui_query_string_basic() -> None:
    class User(PandasDataFrameModel):
        id: int
        age: int | None

    df = User({"id": [1, 2, 3], "age": [20, None, 5]})
    out = df.query("id > 1 and age != None").collect(as_lists=True)
    assert_table_eq_sorted(out, {"id": [3], "age": [5]}, keys=["id"])


def test_pandas_ui_query_rejects_unsupported_syntax() -> None:
    class User(PandasDataFrameModel):
        id: int

    df = User({"id": [1]})
    with pytest.raises(NotImplementedError, match="Call"):
        df.query("max(id) > 0")


def test_pandas_ui_merge_left_on_requires_right_on() -> None:
    class L(PandasDataFrameModel):
        a: int

    class R(PandasDataFrameModel):
        b: int

    with pytest.raises(TypeError, match="left_on"):
        L({"a": [1]}).merge(R({"b": [1]}), left_on="a")  # type: ignore[arg-type]


def test_pandas_ui_merge_left_on_right_on_different_names_drops_right_keys() -> None:
    class L(PandasDataFrameModel):
        left_id: int
        v: int

    class R(PandasDataFrameModel):
        right_id: int
        w: int

    left = L({"left_id": [1, 2], "v": [10, 20]})
    right = R({"right_id": [1, 2], "w": [100, 200]})

    out = left.merge(right, left_on="left_id", right_on="right_id", how="inner")
    data = out.collect(as_lists=True)
    assert "left_id" in data
    assert "right_id" not in data
    assert data["left_id"] == [1, 2]
    assert data["v"] == [10, 20]
    assert data["w"] == [100, 200]


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
    assert df.query("x > 0").collect(as_lists=True) == {"x": [1]}


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


def test_pandas_ui_merge_indicator_outer() -> None:
    class L(PandasDataFrameModel):
        a: int
        x: int

    class R(PandasDataFrameModel):
        a: int
        y: int

    left = L({"a": [1, 2], "x": [10, 20]})
    right = R({"a": [2, 3], "y": [200, 300]})

    out = left.merge(right, on="a", how="outer", indicator=True).collect(as_lists=True)
    # Order is not guaranteed; sort by key.
    rows = sorted(zip(out["a"], out["_merge"], strict=True), key=lambda t: t[0])
    assert rows == [(1, "left_only"), (2, "both"), (3, "right_only")]


def test_pandas_ui_merge_indicator_left_on_right_on() -> None:
    class L(PandasDataFrameModel):
        left_id: int
        x: int

    class R(PandasDataFrameModel):
        right_id: int
        y: int

    left = L({"left_id": [1, 2], "x": [10, 20]})
    right = R({"right_id": [2, 3], "y": [200, 300]})

    out = left.merge(
        right,
        left_on="left_id",
        right_on="right_id",
        how="outer",
        indicator=True,
    ).collect(as_lists=True)
    assert "right_id" not in out  # pandas-like policy: drop right keys
    rows = sorted(
        zip(out["left_id"], out["_merge"], strict=True),
        key=lambda t: t[0],
    )
    assert rows == [(1, "left_only"), (2, "both"), (3, "right_only")]


def test_pandas_ui_merge_validate_one_to_one_and_one_to_many_and_many_to_one() -> None:
    class L(PandasDataFrameModel):
        a: int
        x: int

    class R(PandasDataFrameModel):
        b: int
        y: int

    left_ok = L({"a": [1, 2], "x": [10, 20]})
    right_ok = R({"b": [1, 2], "y": [100, 200]})

    _ = left_ok.merge(
        right_ok, left_on="a", right_on="b", validate="one_to_one", how="inner"
    )
    _ = left_ok.merge(
        right_ok, left_on="a", right_on="b", validate="one_to_many", how="inner"
    )
    _ = left_ok.merge(
        right_ok, left_on="a", right_on="b", validate="many_to_one", how="inner"
    )

    left_dupe = L({"a": [1, 1], "x": [10, 11]})
    right_dupe = R({"b": [2, 2], "y": [20, 21]})

    with pytest.raises(ValueError, match="one_to_one"):
        left_dupe.merge(right_ok, left_on="a", right_on="b", validate="one_to_one")
    with pytest.raises(ValueError, match="one_to_many"):
        left_dupe.merge(right_ok, left_on="a", right_on="b", validate="one_to_many")
    with pytest.raises(ValueError, match="many_to_one"):
        left_ok.merge(right_dupe, left_on="a", right_on="b", validate="many_to_one")


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
