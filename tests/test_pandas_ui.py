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


def test_pandas_ui_assign_callable_can_return_literal() -> None:
    class User(PandasDataFrameModel):
        id: int

    df = User({"id": [1, 2]})
    out = df.assign(one=lambda d: 1).collect(as_lists=True)
    assert out["one"] == [1, 1]


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


def test_pandas_ui_query_string_parentheses_and_precedence() -> None:
    class Row(PandasDataFrameModel):
        id: int
        age: int | None

    df = Row({"id": [1, 2, 3, 4], "age": [10, None, 5, None]})

    # `and` binds tighter than `or` in Python; ensure we follow that.
    out1 = df.query("id == 1 or id == 2 and age == None").collect(as_lists=True)
    # Should include id==1 regardless of age, plus id==2 only if age is None.
    assert_table_eq_sorted(out1, {"id": [1, 2], "age": [10, None]}, keys=["id"])

    # Parentheses should change the result.
    out2 = df.query("(id == 1 or id == 2) and age == None").collect(as_lists=True)
    assert_table_eq_sorted(out2, {"id": [2], "age": [None]}, keys=["id"])


def test_pandas_ui_query_string_not_operator() -> None:
    class Row(PandasDataFrameModel):
        id: int

    df = Row({"id": [1, 2, 3]})
    out = df.query("not (id == 2)").collect(as_lists=True)
    assert_table_eq_sorted(out, {"id": [1, 3]}, keys=["id"])


def test_pandas_ui_query_string_none_is_null_and_not_null() -> None:
    class Row(PandasDataFrameModel):
        id: int
        age: int | None

    df = Row({"id": [1, 2, 3], "age": [None, 10, None]})
    is_null = df.query("age == None").collect(as_lists=True)
    is_not_null = df.query("age != None").collect(as_lists=True)
    assert_table_eq_sorted(is_null, {"id": [1, 3], "age": [None, None]}, keys=["id"])
    assert_table_eq_sorted(is_not_null, {"id": [2], "age": [10]}, keys=["id"])


def test_pandas_ui_query_string_arithmetic_and_membership() -> None:
    class Row(PandasDataFrameModel):
        id: int

    df = Row({"id": [1, 2, 3]})
    out = df.query("id * 2 + 1 >= 5 and id in (2, 3)").collect(as_lists=True)
    assert_table_eq_sorted(out, {"id": [2, 3]}, keys=["id"])


def test_pandas_ui_query_string_not_in_and_in_list_literal() -> None:
    class Row(PandasDataFrameModel):
        id: int

    df = Row({"id": [1, 2, 3]})
    out = df.query("id not in [1, 3]").collect(as_lists=True)
    assert_table_eq_sorted(out, {"id": [2]}, keys=["id"])


def test_pandas_ui_query_rejects_non_literal_in_list() -> None:
    class Row(PandasDataFrameModel):
        id: int

    df = Row({"id": [1, 2, 3]})
    with pytest.raises(NotImplementedError, match="literal"):
        df.query("id in (id, 2)")


def test_pandas_ui_query_rejects_unsupported_syntax() -> None:
    class User(PandasDataFrameModel):
        id: int

    df = User({"id": [1]})
    with pytest.raises(NotImplementedError, match="Call"):
        df.query("max(id) > 0")


def test_pandas_ui_query_accepts_engine_and_dict_params_but_raises_when_used() -> None:
    class Row(PandasDataFrameModel):
        id: int

    df = Row({"id": [1]})
    _ = df.query("id > 0", engine="python", inplace=False)
    with pytest.raises(NotImplementedError, match="engine"):
        df.query("id > 0", engine="numexpr")  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="inplace"):
        df.query("id > 0", inplace=True)
    with pytest.raises(NotImplementedError, match="local_dict"):
        df.query("id > 0", local_dict={"x": 1})
    with pytest.raises(NotImplementedError, match="global_dict"):
        df.query("id > 0", global_dict={"x": 1})


def test_pandas_ui_merge_left_on_requires_right_on() -> None:
    class L(PandasDataFrameModel):
        a: int

    class R(PandasDataFrameModel):
        b: int

    with pytest.raises(TypeError, match="left_on"):
        L({"a": [1]}).merge(R({"b": [1]}), left_on="a")  # type: ignore[arg-type]


def test_pandas_ui_merge_rejects_on_with_left_on_right_on() -> None:
    class L(PandasDataFrameModel):
        a: int

    class R(PandasDataFrameModel):
        b: int

    with pytest.raises(TypeError, match="either on"):
        L({"a": [1]}).merge(R({"b": [1]}), on="a", left_on="a", right_on="b")


def test_pandas_ui_merge_validate_rejects_unknown_value() -> None:
    class L(PandasDataFrameModel):
        a: int

    class R(PandasDataFrameModel):
        a: int

    with pytest.raises(ValueError, match="validate"):
        L({"a": [1]}).merge(R({"a": [1]}), on="a", validate="wat")  # type: ignore[arg-type]


def test_pandas_ui_merge_cross_basic_and_indicator() -> None:
    class L(PandasDataFrameModel):
        a: int

    class R(PandasDataFrameModel):
        b: int

    left = L({"a": [1, 2]})
    right = R({"b": [10, 20, 30]})
    out = left.merge(right, how="cross").collect(as_lists=True)
    assert out["a"] == [1, 1, 1, 2, 2, 2]
    assert out["b"] == [10, 20, 30, 10, 20, 30]

    out2 = left.merge(right, how="cross", indicator=True).collect(as_lists=True)
    assert out2["_merge"] == ["both"] * 6


def test_pandas_ui_merge_cross_rejects_keys_and_validate() -> None:
    class L(PandasDataFrameModel):
        a: int

    class R(PandasDataFrameModel):
        b: int

    left = L({"a": [1]})
    right = R({"b": [2]})
    with pytest.raises(TypeError, match=r"cross.*on"):
        left.merge(right, how="cross", on="a")
    with pytest.raises(TypeError, match=r"cross.*validate"):
        left.merge(right, how="cross", validate="one_to_one")


def test_pandas_ui_merge_accepts_copy_and_rejects_sort_and_index_joins() -> None:
    class L(PandasDataFrameModel):
        a: int

    class R(PandasDataFrameModel):
        a: int

    left = L({"a": [1]})
    right = R({"a": [1]})
    _ = left.merge(right, on="a", copy=True)
    with pytest.raises(NotImplementedError, match="sort"):
        left.merge(right, on="a", sort=True)
    with pytest.raises(NotImplementedError, match="left_index"):
        left.merge(right, on="a", left_index=True)


def test_pandas_ui_merge_suffix_collision_raises() -> None:
    class L(PandasDataFrameModel):
        id: int
        v: int
        v_y: int

    class R(PandasDataFrameModel):
        id: int
        v: int

    left = L({"id": [1], "v": [10], "v_y": [99]})
    right = R({"id": [1], "v": [20]})
    with pytest.raises(ValueError, match="duplicate output column"):
        left.merge(right, on="id", suffixes=("_x", "_y"))


def test_pandas_ui_merge_suffix_collision_left_on_right_on_raises() -> None:
    class L(PandasDataFrameModel):
        lk: int
        v: int
        v_y: int

    class R(PandasDataFrameModel):
        rk: int
        v: int

    left = L({"lk": [1], "v": [10], "v_y": [99]})
    right = R({"rk": [1], "v": [20]})
    with pytest.raises(ValueError, match="duplicate output column"):
        left.merge(right, left_on="lk", right_on="rk", suffixes=("_x", "_y"))


def test_pandas_ui_merge_indicator_key_only_frames_not_supported() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        a: int

    left = DataFrame[Row]({"a": [1]})
    right = DataFrame[Row]({"a": [1]})
    with pytest.raises(NotImplementedError, match="non-key column"):
        left.merge(right, on="a", how="inner", indicator=True)


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


def test_pandas_ui_merge_outer_left_on_right_on_coalesces_left_keys() -> None:
    class L(PandasDataFrameModel):
        left_id: int
        x: int

    class R(PandasDataFrameModel):
        right_id: int
        y: int

    left = L({"left_id": [1, 2], "x": [10, 20]})
    right = R({"right_id": [2, 3], "y": [200, 300]})

    out = left.merge(right, left_on="left_id", right_on="right_id", how="outer")
    data = out.collect(as_lists=True)
    # Right keys dropped, and left key filled for right-only rows (id=3).
    assert "right_id" not in data
    rows = sorted(
        zip(data["left_id"], data.get("x"), data.get("y"), strict=True),
        key=lambda t: t[0],
    )
    assert rows == [(1, 10, None), (2, 20, 200), (3, None, 300)]


def test_pandas_ui_merge_right_join_coalesces_left_keys() -> None:
    class L(PandasDataFrameModel):
        left_id: int
        x: int

    class R(PandasDataFrameModel):
        right_id: int
        y: int

    left = L({"left_id": [1, 2], "x": [10, 20]})
    right = R({"right_id": [2, 3], "y": [200, 300]})

    out = left.merge(right, left_on="left_id", right_on="right_id", how="right")
    data = out.collect(as_lists=True)
    assert "right_id" not in data
    rows = sorted(
        zip(data["left_id"], data.get("x"), data.get("y"), strict=True),
        key=lambda t: t[0],
    )
    assert rows == [(2, 20, 200), (3, None, 300)]


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


def test_pandas_model_group_size_and_nunique() -> None:
    class Row(PandasDataFrameModel):
        k: int
        v: int | None

    df = Row({"k": [1, 1, 2], "v": [10, None, 10]})
    size = df.group_by("k").size().collect(as_lists=True)
    assert_table_eq_sorted(size, {"k": [1, 2], "size": [2, 1]}, keys=["k"])

    nu = df.group_by("k").nunique("v").collect(as_lists=True)
    # nunique drops nulls by design (matches current engine n_unique).
    assert_table_eq_sorted(nu, {"k": [1, 2], "v_nunique": [1, 1]}, keys=["k"])


def test_pandas_model_group_nunique_multiple_columns() -> None:
    class Row(PandasDataFrameModel):
        k: int
        a: int | None
        b: int | None

    df = Row({"k": [1, 1, 1, 2], "a": [1, 1, None, 2], "b": [5, None, 5, 6]})
    out = df.group_by("k").nunique("a", "b").collect(as_lists=True)
    assert_table_eq_sorted(
        out,
        {"k": [1, 2], "a_nunique": [1, 1], "b_nunique": [1, 1]},
        keys=["k"],
    )


def test_pandas_model_group_more_aggs_and_agg_multi() -> None:
    class Row(PandasDataFrameModel):
        k: int
        v: int

    df = Row({"k": [1, 1, 2], "v": [10, 20, 30]})
    first_last = (
        df.group_by("k")
        .first("v")
        .join(df.group_by("k").last("v"), on="k", how="inner", suffix="_r")
    )
    assert_table_eq_sorted(
        first_last.collect(as_lists=True),
        {"k": [1, 2], "v_first": [10, 30], "v_last": [20, 30]},
        keys=["k"],
    )

    out = df.group_by("k").agg_multi(v=["sum", "mean"]).collect(as_lists=True)
    assert_table_eq_sorted(
        out,
        {"k": [1, 2], "v_sum": [30, 30], "v_mean": [15, 30]},
        keys=["k"],
    )


def test_pandas_model_group_std_var_median_smoke() -> None:
    class Row(PandasDataFrameModel):
        k: int
        v: int

    df = Row({"k": [1, 1, 2], "v": [10, 20, 30]})
    out = df.group_by("k").agg_multi(v=["median", "std", "var"]).collect(as_lists=True)
    # Values depend on ddof=1 behavior; just assert columns and key presence.
    assert set(out.keys()) == {"k", "v_median", "v_std", "v_var"}
    assert_table_eq_sorted({"k": out["k"]}, {"k": [1, 2]}, keys=["k"])


def test_pandas_ui_group_agg_multi_rejects_bad_inputs() -> None:
    class Row(PandasDataFrameModel):
        k: int
        v: int

    df = Row({"k": [1], "v": [2]})
    with pytest.raises(TypeError, match="list\\[str\\]"):
        df.group_by("k").agg_multi(v="sum")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="list\\[str\\]"):
        df.group_by("k").agg_multi(v=[])  # type: ignore[arg-type]


def test_pandas_ui_sort_values_drop_rename_fillna_astype() -> None:
    class Row(PandasDataFrameModel):
        a: int | None
        b: int

    df = Row({"a": [None, 2, 1], "b": [3, 2, 1]})

    sorted_df = df.sort_values("b", ascending=False).collect(as_lists=True)
    assert sorted_df["b"] == [3, 2, 1]

    dropped = df.drop(columns=["b"]).collect(as_lists=True)
    assert dropped == {"a": [None, 2, 1]}

    renamed = df.rename(columns={"b": "c"}, errors="raise").collect(as_lists=True)
    assert "c" in renamed and "b" not in renamed

    filled = df.fillna(0, subset="a").collect(as_lists=True)
    assert filled["a"] == [0, 2, 1]

    casted = df.astype({"b": float}).collect(as_lists=True)
    assert casted["b"] == [3.0, 2.0, 1.0]


def test_pandas_ui_sort_values_validation_and_na_position() -> None:
    class Row(PandasDataFrameModel):
        a: int
        b: int

    df = Row({"a": [1, 2], "b": [2, 1]})
    with pytest.raises(ValueError, match="ascending"):
        df.sort_values(["a", "b"], ascending=[True])  # wrong length
    with pytest.raises(NotImplementedError, match="na_position"):
        df.sort_values("a", na_position="last")
    with pytest.raises(NotImplementedError, match="kind"):
        df.sort_values("a", kind="mergesort")  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="ignore_index"):
        df.sort_values("a", ignore_index=True)
    with pytest.raises(NotImplementedError, match="key"):
        df.sort_values("a", key=lambda s: s)  # type: ignore[arg-type]


def test_pandas_ui_drop_errors_ignore_and_raise() -> None:
    class Row(PandasDataFrameModel):
        a: int

    df = Row({"a": [1]})
    out = df.drop(columns=["missing"], errors="ignore").collect(as_lists=True)
    assert out == {"a": [1]}
    with pytest.raises(KeyError, match="not found"):
        df.drop(columns=["missing"], errors="raise")
    with pytest.raises(NotImplementedError, match="index"):
        df.drop(index=[0])
    with pytest.raises(NotImplementedError, match="inplace"):
        df.drop(columns=["a"], inplace=True)
    with pytest.raises(NotImplementedError, match="level"):
        df.drop(columns=["a"], level=0)


def test_pandas_ui_rename_errors_raise_and_ignore() -> None:
    class Row(PandasDataFrameModel):
        a: int

    df = Row({"a": [1]})
    out = df.rename(columns={"missing": "x"}, errors="ignore").collect(as_lists=True)
    assert out == {"a": [1]}
    with pytest.raises(KeyError, match="not found"):
        df.rename(columns={"missing": "x"}, errors="raise")
    with pytest.raises(NotImplementedError, match="index"):
        df.rename(index={"a": "b"})
    with pytest.raises(NotImplementedError, match="axis"):
        df.rename(columns={"a": "b"}, axis=0)
    with pytest.raises(NotImplementedError, match="inplace"):
        df.rename(columns={"a": "b"}, inplace=True)
    with pytest.raises(NotImplementedError, match="level"):
        df.rename(columns={"a": "b"}, level=0)


def test_pandas_ui_fillna_requires_value() -> None:
    class Row(PandasDataFrameModel):
        a: int | None

    df = Row({"a": [None]})
    with pytest.raises(TypeError, match="non-None"):
        df.fillna(None)
    with pytest.raises(NotImplementedError, match="method"):
        df.fillna(method="ffill")  # type: ignore[call-arg]
    with pytest.raises(NotImplementedError, match="limit"):
        df.fillna(0, limit=1)
    with pytest.raises(NotImplementedError, match="downcast"):
        df.fillna(0, downcast="infer")  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="inplace"):
        df.fillna(0, inplace=True)


def test_pandas_ui_astype_missing_column_raises() -> None:
    class Row(PandasDataFrameModel):
        a: int

    df = Row({"a": [1]})
    with pytest.raises(KeyError, match="not found"):
        df.astype({"missing": int})  # type: ignore[arg-type]


def test_pandas_ui_astype_copy_and_errors_params() -> None:
    class Row(PandasDataFrameModel):
        a: int

    df = Row({"a": [1]})
    _ = df.astype(int, copy=True)
    with pytest.raises(NotImplementedError, match="errors"):
        df.astype(int, errors="ignore")  # type: ignore[arg-type]


def test_pandas_core_dataframe_query_sort_drop_rename_fillna_astype_smoke() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        id: int
        x: int | None

    df = DataFrame[Row]({"id": [1, 2, 3], "x": [None, 10, 20]})

    out = (
        df.assign(x2=lambda d: d.x + 1)
        .query("id in (2, 3) and x2 != None and x2 >= 11")
        .sort_values("id", ascending=False)
        .drop(columns=["x"])
        .rename(columns={"x2": "y"}, errors="raise")
        .fillna(0, subset="y")
        .astype({"y": float}, copy=True)
        .collect(as_lists=True)
    )
    assert out == {"id": [3, 2], "y": [21.0, 11.0]}


def test_pandas_core_dataframe_merge_cross_rejects_keys() -> None:
    from pydantable.pandas import DataFrame

    class L(Schema):
        a: int

    class R(Schema):
        b: int

    left = DataFrame[L]({"a": [1]})
    right = DataFrame[R]({"b": [2]})
    with pytest.raises(TypeError, match="cross"):
        left.merge(right, how="cross", on="a")  # type: ignore[arg-type]


def test_pandas_ui_merge_rejects_extra_kwargs() -> None:
    class L(PandasDataFrameModel):
        a: int

    class R(PandasDataFrameModel):
        a: int

    with pytest.raises(TypeError, match="unsupported keyword"):
        L({"a": [1]}).merge(R({"a": [1]}), on="a", unexpected=True)  # type: ignore[arg-type]


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
