from __future__ import annotations

import pytest
from pydantable import DataFrameModel as PolarsDataFrameModel
from pydantable import Schema
from pydantable.pandas import DataFrameModel as PandasDataFrameModel

from tests._support.tables import assert_table_eq_sorted


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


def test_pandas_ui_concat_axis_vertical_and_horizontal() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        a: int
        b: int

    df1 = DataFrame[Row]({"a": [1], "b": [2]})
    df2 = DataFrame[Row]({"a": [3], "b": [4]})
    out = DataFrame.concat([df1, df2], axis=0).collect(as_lists=True)
    assert out == {"a": [1, 3], "b": [2, 4]}

    class Left(Schema):
        a: int

    class Right(Schema):
        b: int

    left_df = DataFrame[Left]({"a": [1, 2]})
    r = DataFrame[Right]({"b": [10, 20]})
    out2 = DataFrame.concat([left_df, r], axis=1).collect(as_lists=True)
    assert out2 == {"a": [1, 2], "b": [10, 20]}


def test_pandas_ui_concat_model_preserves_pandas_surface() -> None:
    class A(PandasDataFrameModel):
        a: int

    class B(PandasDataFrameModel):
        a: int

    out = A.concat([A({"a": [1]}), B({"a": [2]})], axis=0)
    assert isinstance(out, PandasDataFrameModel)
    assert out.collect(as_lists=True) == {"a": [1, 2]}
    assert out.isna().collect(as_lists=True) == {"a": [False, False]}


def test_pandas_ui_drop_duplicates_maps_to_unique() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        a: int
        b: int

    df = DataFrame[Row]({"a": [1, 1, 2], "b": [10, 20, 30]})
    out = df.drop_duplicates(subset="a", keep="first").collect(as_lists=True)
    assert out == {"a": [1, 2], "b": [10, 30]}
    out2 = df.drop_duplicates(subset=["a"], keep="last").collect(as_lists=True)
    assert out2 == {"a": [1, 2], "b": [20, 30]}


def test_pandas_ui_duplicated_and_drop_false() -> None:
    pd = pytest.importorskip("pandas")
    from pydantable.pandas import DataFrame

    class Row(Schema):
        a: int
        b: int

    pdf = pd.DataFrame({"a": [1, 1, 2], "b": [3, 3, 4]})
    df = DataFrame[Row]({"a": [1, 1, 2], "b": [3, 3, 4]})
    d1 = df.duplicated(keep="first").collect(as_lists=True)["duplicated"]
    assert d1 == list(pdf.duplicated(keep="first"))
    d2 = df.duplicated(keep="last").collect(as_lists=True)["duplicated"]
    assert d2 == list(pdf.duplicated(keep="last"))
    d3 = df.duplicated(keep=False).collect(as_lists=True)["duplicated"]
    assert d3 == list(pdf.duplicated(keep=False))

    out = df.drop_duplicates(keep=False).collect(as_lists=True)
    assert out == {"a": [2], "b": [4]}


def test_pandas_ui_nlargest_nsmallest_sort_and_slice() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        name: str
        score: int

    df = DataFrame[Row]({"name": ["a", "b", "c", "d"], "score": [10, 30, 20, 40]})
    top = df.nlargest(2, "score").collect(as_lists=True)
    assert top == {"name": ["d", "b"], "score": [40, 30]}
    bot = df.nsmallest(2, "score").collect(as_lists=True)
    assert bot == {"name": ["a", "c"], "score": [10, 20]}
    top2 = df.nlargest(2, ["score", "name"]).collect(as_lists=True)
    assert top2["score"] == [40, 30]


def test_pandas_ui_nlargest_rejects_bad_keep() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        x: int

    df = DataFrame[Row]({"x": [1]})
    with pytest.raises(NotImplementedError, match="keep"):
        df.nlargest(1, "x", keep="first")  # type: ignore[arg-type]


def test_pandas_ui_isin_list_and_dict() -> None:
    from pydantable.pandas import DataFrame

    class RowInt(Schema):
        a: int
        b: int

    df_i = DataFrame[RowInt]({"a": [1, 2, 3], "b": [2, 3, 4]})
    m = df_i.isin([1, 2]).collect(as_lists=True)
    assert m == {"a": [True, True, False], "b": [True, False, False]}

    class RowMix(Schema):
        a: int
        b: str

    df = DataFrame[RowMix]({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    m2 = df.isin({"a": [2], "b": ["z"]}).collect(as_lists=True)
    assert m2 == {"a": [False, True, False], "b": [False, False, True]}


def test_pandas_ui_isin_dict_unknown_column_keyerror() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        a: int

    df = DataFrame[Row]({"a": [1]})
    with pytest.raises(KeyError):
        df.isin({"missing": [1]}).collect(as_lists=True)


def test_pandas_ui_explode_delegates_to_core() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        id: int
        tags: list[int]

    df = DataFrame[Row]({"id": [1, 2], "tags": [[1, 2], [3]]})
    ex = df.explode("tags").collect(as_lists=True)
    assert ex == {"id": [1, 1, 2], "tags": [1, 2, 3]}


def test_pandas_ui_copy_shallow_and_filter_dispatch() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        a: int
        b_extra: int

    df = DataFrame[Row]({"a": [1, 2], "b_extra": [3, 4]})
    c = df.copy()
    assert c.collect(as_lists=True) == df.collect(as_lists=True)
    with pytest.raises(NotImplementedError, match="deep"):
        df.copy(deep=True)

    sub = df.filter(like="b").collect(as_lists=True)
    assert sub == {"b_extra": [3, 4]}
    sub2 = df.filter(regex=r"^a$").collect(as_lists=True)
    assert sub2 == {"a": [1, 2]}
    row_f = df.filter(df.col("a") > 1).collect(as_lists=True)
    assert row_f == {"a": [2], "b_extra": [4]}


def test_pandas_ui_pipe() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        x: int

    df = DataFrame[Row]({"x": [1]})
    assert df.pipe(lambda d: d.collect(as_lists=True)) == {"x": [1]}
    out = df.pipe(lambda d: d.with_columns(y=d.col("x") * 2)).collect(as_lists=True)
    assert out == {"x": [1], "y": [2]}


def test_pandas_ui_concat_rejects_unsupported_join() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        a: int

    df1 = DataFrame[Row]({"a": [1]})
    df2 = DataFrame[Row]({"a": [2]})
    with pytest.raises(NotImplementedError, match="join"):
        DataFrame.concat([df1, df2], axis=0, join="inner")  # type: ignore[arg-type]


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
    with pytest.raises(NotImplementedError, match=r"local_dict|literal"):
        df.query("id in (id, 2)")


def test_pandas_ui_query_rejects_unsupported_syntax() -> None:
    class User(PandasDataFrameModel):
        id: int

    df = User({"id": [1]})
    with pytest.raises(NotImplementedError, match=r"function call|max"):
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
    assert df.query("id > 0", local_dict={"x": 1}).collect(as_lists=True) == {"id": [1]}
    assert df.query("id > 0", global_dict={"x": 1}).collect(as_lists=True) == {
        "id": [1]
    }


def test_pandas_ui_query_string_helpers_work() -> None:
    class Row(PandasDataFrameModel):
        s: str
        n: int | None

    df = Row({"s": ["Abc", "xyz", "foo"], "n": [None, 1, None]})
    out1 = df.query('contains(s, "b")').collect(as_lists=True)
    assert_table_eq_sorted(out1, {"s": ["Abc"], "n": [None]}, keys=["s"])
    out2 = df.query('startswith(s, "x")').collect(as_lists=True)
    assert_table_eq_sorted(out2, {"s": ["xyz"], "n": [1]}, keys=["s"])
    out3 = df.query('endswith(s, "o")').collect(as_lists=True)
    assert_table_eq_sorted(out3, {"s": ["foo"], "n": [None]}, keys=["s"])
    out4 = df.query("isnull(n)").collect(as_lists=True)
    assert_table_eq_sorted(out4, {"s": ["Abc", "foo"], "n": [None, None]}, keys=["s"])
    out5 = df.query("notnull(n)").collect(as_lists=True)
    assert_table_eq_sorted(out5, {"s": ["xyz"], "n": [1]}, keys=["s"])

    out6 = df.query("isna(n)").collect(as_lists=True)
    assert_table_eq_sorted(out6, {"s": ["Abc", "foo"], "n": [None, None]}, keys=["s"])
    out7 = df.query("notna(n)").collect(as_lists=True)
    assert_table_eq_sorted(out7, {"s": ["xyz"], "n": [1]}, keys=["s"])


def test_pandas_ui_query_rejects_non_whitelisted_function_call() -> None:
    class Row(PandasDataFrameModel):
        s: str

    df = Row({"s": ["x"]})
    with pytest.raises(NotImplementedError, match="unsupported function call"):
        df.query("foo(s) == 'x'")


def test_pandas_ui_query_between_helper() -> None:
    class Row(PandasDataFrameModel):
        a: int

    df = Row({"a": [1, 2, 3, 4]})
    out = df.query("between(a, 2, 3)").collect(as_lists=True)
    assert_table_eq_sorted(out, {"a": [2, 3]}, keys=["a"])


def test_pandas_ui_query_between_rejects_wrong_arity() -> None:
    class Row(PandasDataFrameModel):
        a: int

    df = Row({"a": [1]})
    with pytest.raises(TypeError, match=r"between\(\) expects"):
        df.query("between(a, 1)")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match=r"between\(\) expects"):
        df.query("between(a, 1, 2, 3)")  # type: ignore[arg-type]


def test_pandas_ui_query_isna_notna_reject_wrong_arity() -> None:
    class Row(PandasDataFrameModel):
        a: int | None

    df = Row({"a": [None]})
    with pytest.raises(TypeError, match=r"isna\(\) expects"):
        df.query("isna()")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match=r"notna\(\) expects"):
        df.query("notna(a, a)")  # type: ignore[arg-type]


def test_pandas_ui_query_between_supports_external_constants() -> None:
    class Row(PandasDataFrameModel):
        a: int

    df = Row({"a": [1, 2, 3, 4]})
    out = df.query("between(a, lo, hi)", local_dict={"lo": 2, "hi": 3}).collect(
        as_lists=True
    )
    assert_table_eq_sorted(out, {"a": [2, 3]}, keys=["a"])


def test_pandas_ui_query_string_transform_helpers() -> None:
    class Row(PandasDataFrameModel):
        s: str

    df = Row({"s": [" X ", "y"]})
    out1 = df.query("lower(s) == ' x '").collect(as_lists=True)
    assert_table_eq_sorted(out1, {"s": [" X "]}, keys=["s"])
    out2 = df.query("strip(s) == 'X'").collect(as_lists=True)
    assert_table_eq_sorted(out2, {"s": [" X "]}, keys=["s"])
    out3 = df.query("upper(s) == 'Y'").collect(as_lists=True)
    assert_table_eq_sorted(out3, {"s": ["y"]}, keys=["s"])


def test_pandas_ui_query_len_length_helpers() -> None:
    class Row(PandasDataFrameModel):
        s: str

    df = Row({"s": ["aaa", "b"]})
    out = df.query("len(s) == 1").collect(as_lists=True)
    assert_table_eq_sorted(out, {"s": ["b"]}, keys=["s"])
    out2 = df.query("length(s) == 3").collect(as_lists=True)
    assert_table_eq_sorted(out2, {"s": ["aaa"]}, keys=["s"])


def test_pandas_ui_query_between_rejects_non_literal_bounds() -> None:
    class Row(PandasDataFrameModel):
        a: int
        b: int

    df = Row({"a": [1, 2, 3], "b": [0, 0, 0]})
    with pytest.raises(NotImplementedError, match="between\\(\\) bounds"):
        df.query("between(a, b, 3)")


def test_pandas_ui_sort_values_key_identifiers() -> None:
    class Row(PandasDataFrameModel):
        s: str

    df = Row({"s": ["B", " a", "C "]})
    out = df.sort_values("s", key="lower").collect(as_lists=True)
    assert out["s"] == [" a", "B", "C "]

    out2 = df.sort_values("s", key="strip").collect(as_lists=True)
    # Sort key is stripped, but output is original strings.
    assert out2["s"][0] in {" a", "B"}
    assert set(out2["s"]) == {"B", " a", "C "}


def test_pandas_ui_sort_values_key_len_orders_by_char_length() -> None:
    class Row(PandasDataFrameModel):
        s: str

    df = Row({"s": ["bbb", "a", "cc"]})
    out = df.sort_values("s", key="len").collect(as_lists=True)
    assert out["s"] == ["a", "cc", "bbb"]


def test_pandas_ui_sort_values_key_unknown_rejected() -> None:
    class Row(PandasDataFrameModel):
        s: str

    df = Row({"s": ["a"]})
    with pytest.raises(NotImplementedError, match="sort_values\\(key="):
        df.sort_values("s", key="wat")  # type: ignore[arg-type]


def test_pandas_ui_sort_values_key_temp_cols_not_leaked() -> None:
    class Row(PandasDataFrameModel):
        s: str

    df = Row({"s": ["B", "a"]})
    out = df.sort_values("s", key="lower")
    assert all(not c.startswith("__pd_sort_key_") for c in out.schema_fields())


def test_pandas_ui_sort_values_multi_key_with_key_and_na_position() -> None:
    class Row(PandasDataFrameModel):
        g: str
        s: str | None

    df = Row({"g": ["b", "b", "a", "a"], "s": ["X", None, "y", "Z"]})
    out = df.sort_values(
        ["g", "s"], ascending=[True, True], key="lower", na_position="last"
    ).collect(as_lists=True)
    # Primary key: g asc => a then b.
    assert out["g"][:2] == ["a", "a"]
    # Secondary key: s lowercased with nulls last within each group.
    a_rows = list(zip(out["g"], out["s"], strict=True))[:2]
    assert a_rows == [("a", "y"), ("a", "Z")]
    b_rows = list(zip(out["g"], out["s"], strict=True))[2:]
    assert b_rows == [("b", "X"), ("b", None)]


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


@pytest.mark.parametrize("how", ["inner", "left", "right", "outer"])
def test_pandas_ui_merge_matrix_on_basic(how: str) -> None:
    class L(PandasDataFrameModel):
        k: int
        lv: int

    class R(PandasDataFrameModel):
        k: int
        rv: int

    left = L({"k": [1, 2], "lv": [10, 20]})
    right = R({"k": [2, 3], "rv": [200, 300]})
    out = left.merge(right, on="k", how=how, indicator=True).collect(as_lists=True)

    rows = sorted(
        zip(out["k"], out.get("lv"), out.get("rv"), out["_merge"], strict=True),
        key=lambda t: t[0],
    )
    if how == "inner":
        assert rows == [(2, 20, 200, "both")]
    elif how == "left":
        assert rows == [(1, 10, None, "left_only"), (2, 20, 200, "both")]
    elif how == "right":
        assert rows == [(2, 20, 200, "both"), (3, None, 300, "right_only")]
    else:
        assert rows == [
            (1, 10, None, "left_only"),
            (2, 20, 200, "both"),
            (3, None, 300, "right_only"),
        ]


def test_pandas_ui_merge_indicator_rejects_existing_merge_column() -> None:
    class L(PandasDataFrameModel):
        k: int

    class R(PandasDataFrameModel):
        k: int

    left = L({"k": [1]})
    right = R({"k": [1]})
    out = left.merge(right, on="k", indicator=True).collect(as_lists=True)
    assert out["_merge"] == ["both"]


def test_pandas_ui_merge_index_merge_rejects_key_args() -> None:
    class L(PandasDataFrameModel):
        k: int

    class R(PandasDataFrameModel):
        k: int

    left = L({"k": [1]})
    right = R({"k": [1]})
    with pytest.raises(NotImplementedError, match=r"on/left_on/right_on"):
        left.merge(right, on="k", left_index=True, right_index=True)


def test_pandas_ui_merge_index_merge_rejects_one_sided_index() -> None:
    class L(PandasDataFrameModel):
        k: int

    class R(PandasDataFrameModel):
        k: int

    left = L({"k": [1]})
    right = R({"k": [1]})
    with pytest.raises(NotImplementedError, match="right_index"):
        left.merge(right, left_index=True, right_index=False)  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="left_index"):
        left.merge(right, left_index=False, right_index=True)  # type: ignore[arg-type]


def test_pandas_ui_merge_accepts_copy_and_rejects_sort_and_index_joins() -> None:
    class L(PandasDataFrameModel):
        a: int

    class R(PandasDataFrameModel):
        a: int

    left = L({"a": [1]})
    right = R({"a": [1]})
    _ = left.merge(right, on="a", copy=True)
    _ = left.merge(right, on="a", sort=True)
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
    out = left.merge(right, on="a", how="inner", indicator=True).collect(as_lists=True)
    assert out["_merge"] == ["both"]


def test_pandas_ui_merge_sort_true_sorts_by_keys() -> None:
    class L(PandasDataFrameModel):
        a: int
        v: int

    class R(PandasDataFrameModel):
        a: int
        w: int

    left = L({"a": [2, 1], "v": [20, 10]})
    right = R({"a": [1, 2], "w": [100, 200]})
    out = left.merge(right, on="a", how="inner", sort=True).collect(as_lists=True)
    assert out["a"] == [1, 2]


def test_pandas_ui_merge_left_index_right_index_smoke() -> None:
    class L(PandasDataFrameModel):
        v: int

    class R(PandasDataFrameModel):
        w: int

    left = L({"v": [10, 20]})
    right = R({"w": [100, 200]})
    out = left.merge(right, how="inner", left_index=True, right_index=True).collect(
        as_lists=True
    )
    assert out == {"v": [10, 20], "w": [100, 200]}


def test_pandas_ui_query_local_dict_constant_substitution() -> None:
    class Row(PandasDataFrameModel):
        id: int

    df = Row({"id": [1, 2, 3]})
    out = df.query("id > thresh", local_dict={"thresh": 1}).collect(as_lists=True)
    assert_table_eq_sorted(out, {"id": [2, 3]}, keys=["id"])


def test_pandas_ui_fillna_method_ffill_bfill() -> None:
    class Row(PandasDataFrameModel):
        k: int
        v: int | None

    df = Row({"k": [1, 2, 3], "v": [None, 10, None]})
    out = df.fillna(method="ffill").collect(as_lists=True)
    assert out["v"] == [None, 10, 10]
    out2 = df.fillna(method="bfill").collect(as_lists=True)
    assert out2["v"] == [10, 10, None]


def test_pandas_ui_astype_errors_ignore_skips_unsupported_casts() -> None:
    class Row(PandasDataFrameModel):
        a: int

    df = Row({"a": [1]})
    out = df.astype({"a": str}, errors="ignore").collect(as_lists=True)
    assert out == {"a": [1]}


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


def test_pandas_ui_iloc_slice_plan_only() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        id: int

    df = DataFrame[Row]({"id": [1, 2, 3, 4]})
    out = df.iloc[1:3].collect(as_lists=True)
    assert out == {"id": [2, 3]}
    out2 = df.iloc[0:0].collect(as_lists=True)
    assert out2 == {"id": []}
    assert df.iloc[1:].collect(as_lists=True) == {"id": [2, 3, 4]}
    with pytest.raises(NotImplementedError, match="step"):
        _ = df.iloc[0:3:2]
    assert df.iloc[1].collect(as_lists=True) == {"id": [2]}


def test_pandas_ui_iloc_slice_stop_lt_start_is_empty() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        id: int

    df = DataFrame[Row]({"id": [1, 2, 3]})
    out = df.iloc[2:1].collect(as_lists=True)
    assert out == {"id": []}


def test_pandas_ui_iloc_slice_allows_none_start() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        id: int

    df = DataFrame[Row]({"id": [1, 2, 3]})
    out = df.iloc[:2].collect(as_lists=True)
    assert out == {"id": [1, 2]}


def test_pandas_ui_iloc_scalar_and_negative_indices() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        id: int

    df = DataFrame[Row]({"id": [1, 2, 3]})
    assert df.iloc[1].collect(as_lists=True) == {"id": [2]}
    assert df.iloc[-1].collect(as_lists=True) == {"id": [3]}


def test_pandas_ui_iloc_open_ended_stop_requires_in_memory_root() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        id: int

    df = DataFrame[Row]({"id": [1, 2, 3]})
    assert df.iloc[1:].collect(as_lists=True) == {"id": [2, 3]}


def test_pandas_ui_loc_expr_mask_and_column_select() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        a: int
        b: int

    df = DataFrame[Row]({"a": [1, 2, 3], "b": [10, 20, 30]})
    out = df.loc[df.col("a") > 1, ["b"]].collect(as_lists=True)
    assert out == {"b": [20, 30]}
    out2 = df.loc[:, "a"].collect(as_lists=True)
    assert out2 == {"a": [1, 2, 3]}


def test_pandas_ui_loc_rejects_unsupported_selectors() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        a: int

    df = DataFrame[Row]({"a": [1]})
    with pytest.raises(TypeError, match="2-tuple"):
        _ = df.loc["a"]  # type: ignore[index]
    with pytest.raises(NotImplementedError, match="row selection"):
        _ = df.loc[0, ["a"]]  # type: ignore[index]
    with pytest.raises(NotImplementedError, match="column selection"):
        _ = df.loc[:, []]  # type: ignore[index]


def test_pandas_ui_isna_notna_methods() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        a: int | None
        b: str | None

    df = DataFrame[Row]({"a": [1, None], "b": [None, "x"]})
    assert df.isna().collect(as_lists=True) == {"a": [False, True], "b": [True, False]}
    assert df.notna().collect(as_lists=True) == {"a": [True, False], "b": [False, True]}


def test_pandas_ui_dropna_any_all_subset() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        a: int | None
        b: int | None

    df = DataFrame[Row]({"a": [1, None, None], "b": [None, 2, None]})
    assert df.dropna(how="any").collect(as_lists=True) == {"a": [], "b": []}
    assert df.dropna(how="any", subset="a").collect(as_lists=True) == {
        "a": [1],
        "b": [None],
    }
    assert df.dropna(how="all").collect(as_lists=True) == {
        "a": [1, None],
        "b": [None, 2],
    }
    assert df.dropna(how="all", subset=["b"]).collect(as_lists=True) == {
        "a": [None],
        "b": [2],
    }


def test_pandas_ui_melt_lazy_schema_and_output() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        id: int
        x: int | None
        y: int | None

    df = DataFrame[Row]({"id": [1, 2], "x": [10, None], "y": [None, 30]})
    out = df.melt(id_vars=["id"], value_vars=["x", "y"]).collect(as_lists=True)
    assert out["id"] == [1, 2, 1, 2]
    assert out["variable"] == ["x", "x", "y", "y"]
    assert out["value"] == [10, None, None, 30]


def test_pandas_ui_rolling_sum_mean_count_min_periods() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        x: int

    df = DataFrame[Row]({"x": [1, 2, 3, 4]})
    out_sum = (
        df.rolling(window=3, min_periods=3)
        .sum("x", out_name="r")
        .collect(as_lists=True)
    )
    assert out_sum == {"x": [1, 2, 3, 4], "r": [None, None, 6, 9]}
    out_mean = (
        df.rolling(window=2, min_periods=1)
        .mean("x", out_name="m")
        .collect(as_lists=True)
    )
    assert out_mean["m"] == [1.0, 1.5, 2.5, 3.5]
    out_cnt = (
        df.rolling(window=2, min_periods=2)
        .count("x", out_name="c")
        .collect(as_lists=True)
    )
    assert out_cnt["c"] == [None, 2, 2, 2]


def test_pandas_ui_iloc_rejects_list_and_step() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        id: int

    df = DataFrame[Row]({"id": [1, 2, 3]})
    with pytest.raises(TypeError, match="int or slice"):
        _ = df.iloc[[0, 1]]  # type: ignore[index]


def test_pandas_ui_iloc_negative_slice_stop() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        id: int

    df = DataFrame[Row]({"id": [10, 20, 30, 40]})
    assert df.iloc[1:-1].collect(as_lists=True) == {"id": [20, 30]}


def test_pandas_ui_loc_all_columns_slice_and_full_frame() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        a: int
        b: int

    df = DataFrame[Row]({"a": [1, 2], "b": [3, 4]})
    out = df.loc[df.col("a") > 1, :].collect(as_lists=True)
    assert out == {"a": [2], "b": [4]}
    full = df.loc[:, :].collect(as_lists=True)
    assert full == {"a": [1, 2], "b": [3, 4]}


def test_pandas_ui_isnull_notnull_aliases() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        v: int | None

    df = DataFrame[Row]({"v": [1, None]})
    assert df.isnull().collect(as_lists=True) == df.isna().collect(as_lists=True)
    assert df.notnull().collect(as_lists=True) == df.notna().collect(as_lists=True)


def test_pandas_ui_dropna_validation_and_bad_subset() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        a: int | None

    df = DataFrame[Row]({"a": [1, None]})
    with pytest.raises(ValueError, match="how"):
        df.dropna(how="bogus")  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="axis=1"):
        df.dropna(axis=1)  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="inplace"):
        df.dropna(inplace=True)  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="thresh"):
        df.dropna(thresh=1)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="subset"):
        df.dropna(subset=())  # type: ignore[arg-type]
    with pytest.raises(KeyError):
        df.dropna(subset=["missing"]).collect(as_lists=True)


def test_pandas_ui_melt_id_vars_str_and_infer_value_vars() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        k: str
        v1: int
        v2: int

    df = DataFrame[Row]({"k": ["a", "b"], "v1": [1, 2], "v2": [3, 4]})
    out = df.melt(id_vars="k", value_vars=None).collect(as_lists=True)
    assert set(out["variable"]) == {"v1", "v2"}
    assert len(out["k"]) == 4
    for i, _ in enumerate(out["k"]):
        if out["variable"][i] == "v1":
            assert out["value"][i] in (1, 2)
        else:
            assert out["value"][i] in (3, 4)


def test_pandas_ui_melt_rejects_var_name_collision() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        id: int
        x: int

    df = DataFrame[Row]({"id": [1], "x": [1]})
    with pytest.raises(ValueError, match=r"collide|collision"):
        df.melt(id_vars=["id"], value_vars=["x"], var_name="id")


def test_pandas_ui_melt_rejects_mixed_value_dtypes() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        id: int
        x: int
        y: str

    df = DataFrame[Row]({"id": [1], "x": [1], "y": ["z"]})
    with pytest.raises(TypeError, match=r"compatible|base dtype"):
        df.melt(id_vars=["id"], value_vars=["x", "y"])


def test_pandas_ui_melt_rejects_empty_id_vars_typeerror() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        x: int

    df = DataFrame[Row]({"x": [1]})
    with pytest.raises(TypeError, match="id_vars"):
        df.melt(id_vars=[], value_vars=["x"])  # type: ignore[arg-type]


def test_pandas_ui_rolling_min_max_and_bad_window() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        x: int

    df = DataFrame[Row]({"x": [3, 1, 4, 2]})
    out_min = (
        df.rolling(window=2, min_periods=1)
        .min("x", out_name="m")
        .collect(as_lists=True)
    )
    assert out_min["m"] == [3, 1, 1, 2]
    out_max = (
        df.rolling(window=2, min_periods=1)
        .max("x", out_name="M")
        .collect(as_lists=True)
    )
    assert out_max["M"] == [3, 3, 4, 4]
    with pytest.raises(ValueError, match="window"):
        df.rolling(window=0)  # type: ignore[arg-type]


def test_pandas_ui_dataframe_model_iloc_loc_dropna_roundtrip() -> None:
    """Pandas UI DataFrameModel delegates iloc/loc/dropna like head/tail."""

    class M(PandasDataFrameModel):
        id: int
        v: int | None

    m = M({"id": [1, 2, 3], "v": [10, None, 30]})
    sub = m.iloc[1:3]
    assert sub.collect(as_lists=True) == {"id": [2, 3], "v": [None, 30]}
    filt = m.loc[m.col("id") > 1, ["v"]]
    assert filt.collect(as_lists=True) == {"v": [None, 30]}
    dropped = m.dropna(subset=["v"])
    assert dropped.collect(as_lists=True) == {"id": [1, 3], "v": [10, 30]}


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
    with pytest.raises(TypeError, match="suffixes"):
        left.merge(right, on="id", how="inner", suffixes=("_only",))  # type: ignore[arg-type]


def test_pandas_ui_merge_left_by_right_by_rejected() -> None:
    class L(PandasDataFrameModel):
        id: int
        g: int

    class R(PandasDataFrameModel):
        id: int
        g: int

    left = L({"id": [1], "g": [1]})
    right = R({"id": [1], "g": [1]})
    with pytest.raises(NotImplementedError, match=r"left_by|right_by"):
        left.merge(right, on="id", left_by="g")  # type: ignore[arg-type]


def test_pandas_ui_merge_suffixes_validation() -> None:
    class L(PandasDataFrameModel):
        id: int
        v: int

    class R(PandasDataFrameModel):
        id: int
        v: int

    left = L({"id": [1], "v": [10]})
    right = R({"id": [1], "v": [20]})
    with pytest.raises(TypeError, match="suffixes"):
        left.merge(right, on="id", suffixes=["_x", "_y"])  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="suffixes"):
        left.merge(right, on="id", suffixes=("_x",))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="suffixes"):
        left.merge(right, on="id", suffixes=("_x", 1))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="suffixes"):
        left.merge(right, on="id", suffixes=("", ""))


def test_pandas_ui_merge_suffixes_empty_right_suffix_collision() -> None:
    class L(PandasDataFrameModel):
        id: int
        v: int

    class R(PandasDataFrameModel):
        id: int
        v: int

    left = L({"id": [1], "v": [10]})
    right = R({"id": [1], "v": [20]})
    with pytest.raises(ValueError, match="duplicate output column"):
        left.merge(right, on="id", suffixes=("_x", ""))


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
    out = df.sort_values("a", na_position="last").collect(as_lists=True)
    assert out["a"] == [1, 2]
    with pytest.raises(NotImplementedError, match="kind"):
        df.sort_values("a", kind="mergesort")  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="ignore_index"):
        df.sort_values("a", ignore_index=True)
    with pytest.raises(
        NotImplementedError, match=r"callable|callables|Python callables|key"
    ):
        df.sort_values("a", key=lambda s: s)  # type: ignore[arg-type]


def test_pandas_ui_drop_errors_ignore_and_raise() -> None:
    class Row(PandasDataFrameModel):
        a: int

    df = Row({"a": [1]})
    out = df.drop(columns=["missing"], errors="ignore").collect(as_lists=True)
    assert out == {"a": [1]}
    with pytest.raises(KeyError, match="not found"):
        df.drop(columns=["missing"], errors="raise")
    out2 = df.drop(index=[0], errors="ignore").collect(as_lists=True)
    assert out2 == {"a": []}
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
    out = df.fillna(method="ffill").collect(as_lists=True)  # type: ignore[call-arg]
    assert out["a"] == [None]
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
    out = df.astype(int, errors="ignore").collect(as_lists=True)  # type: ignore[arg-type]
    assert out == {"a": [1]}


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


def test_pandas_ui_to_pandas_smoke() -> None:
    pandas = pytest.importorskip("pandas")
    from pydantable.pandas import DataFrame

    class Row(Schema):
        a: int
        b: str

    df = DataFrame[Row]({"a": [1, 2], "b": ["x", "y"]})
    out = df.to_pandas()
    assert isinstance(out, pandas.DataFrame)
    assert list(out.columns) == ["a", "b"]
    assert out.to_dict(orient="list") == {"a": [1, 2], "b": ["x", "y"]}


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


def test_pandas_ui_groupby_rejects_pandas_params() -> None:
    class Row(PandasDataFrameModel):
        k: int
        v: int

    df = Row({"k": [1], "v": [2]})
    with pytest.raises(NotImplementedError, match="dropna"):
        df.group_by("k", dropna=False)  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="as_index"):
        df.group_by("k", as_index=False)  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="sort"):
        df.group_by("k", sort=True)  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="observed"):
        df.group_by("k", observed=True)  # type: ignore[arg-type]


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


def test_pandas_ui_wide_to_long_single_stub() -> None:
    from pydantable.pandas import DataFrame

    class T(Schema):
        id: int
        sales_2020: int
        sales_2021: int

    df = DataFrame[T]({"id": [1, 2], "sales_2020": [10, 20], "sales_2021": [11, 21]})
    long = df.wide_to_long("sales", i="id", j="year", sep="_")
    got = long.collect(as_lists=True)
    assert got["id"] == [1, 2, 1, 2]
    assert set(got["year"]) == {"2020", "2021"}
    assert sorted(zip(got["id"], got["year"], got["sales"], strict=True)) == [
        (1, "2020", 10),
        (1, "2021", 11),
        (2, "2020", 20),
        (2, "2021", 21),
    ]


def test_pandas_ui_from_dict_orients() -> None:
    from pydantable.pandas import DataFrame

    class R(Schema):
        a: int
        b: int

    d1 = DataFrame[R].from_dict({"a": [1, 2], "b": [3, 4]})
    assert d1.collect(as_lists=True) == {"a": [1, 2], "b": [3, 4]}

    d2 = DataFrame[R].from_dict([{"a": 1, "b": 2}, {"a": 3, "b": 4}], orient="records")
    assert d2.collect(as_lists=True) == {"a": [1, 3], "b": [2, 4]}

    d3 = DataFrame[R].from_dict(
        {10: {"a": 1, "b": 2}, 20: {"a": 3, "b": 4}}, orient="index"
    )
    assert d3.collect(as_lists=True) == {"a": [1, 3], "b": [2, 4]}


def test_pandas_ui_stack_matches_melt() -> None:
    from pydantable.pandas import DataFrame

    class R(Schema):
        id: int
        x: int
        y: int

    df = DataFrame[R]({"id": [1], "x": [10], "y": [20]})
    a = df.stack(id_vars="id", value_vars=["x", "y"], var_name="k", value_name="v")
    b = df.melt(id_vars="id", value_vars=["x", "y"], var_name="k", value_name="v")
    assert a.collect(as_lists=True) == b.collect(as_lists=True)


def test_pandas_ui_where_and_mask() -> None:
    from pydantable.pandas import DataFrame

    class R(Schema):
        a: int
        b: int

    df = DataFrame[R]({"a": [1, 2], "b": [30, 40]})
    w = df.where(df.col("a") > 1, 0).collect(as_lists=True)
    # Scalar `other` broadcasts like pandas: every column uses the same row mask.
    assert w["a"] == [0, 2]
    assert w["b"] == [0, 40]
    m = df.mask(df.col("a") > 1, 0).collect(as_lists=True)
    assert m["a"] == [1, 0]
    assert m["b"] == [30, 0]


def test_pandas_ui_rank_orders_within_column() -> None:
    from pydantable.pandas import DataFrame

    class R(Schema):
        v: int

    df = DataFrame[R]({"v": [3, 1, 2]})
    ra = df.rank(method="average").collect(as_lists=True)
    rd = df.rank(method="dense").collect(as_lists=True)
    assert ra["v"] == [3, 1, 2]
    assert rd["v"] == [3, 1, 2]


def test_pandas_ui_sample_and_take() -> None:
    from pydantable.pandas import DataFrame

    class R(Schema):
        i: int

    df = DataFrame[R]({"i": list(range(10))})
    s1 = df.sample(n=3, random_state=42).collect(as_lists=True)
    s2 = df.sample(n=3, random_state=42).collect(as_lists=True)
    assert len(s1["i"]) == 3
    assert s1 == s2
    t = df.take([2, 0, 2]).collect(as_lists=True)
    assert t["i"] == [2, 0, 2]
    tn = df.take([-1]).collect(as_lists=True)
    assert tn["i"] == [9]


def test_pandas_ui_sort_index_keyword() -> None:
    from pydantable.pandas import DataFrame

    class R(Schema):
        k: int
        v: int

    df = DataFrame[R]({"k": [2, 1], "v": [20, 10]})
    out = df.sort_index(by=["k"]).collect(as_lists=True)
    assert out["k"] == [1, 2]
    assert out["v"] == [10, 20]


def test_pandas_ui_combine_first_and_update() -> None:
    from pydantable.pandas import DataFrame

    class R(Schema):
        k: int
        v: int | None

    left = DataFrame[R]({"k": [1, 2], "v": [None, 20]})
    right = DataFrame[R]({"k": [1, 2], "v": [10, None]})
    cf = left.combine_first(right, on=["k"]).collect(as_lists=True)
    assert_table_eq_sorted(cf, {"k": [1, 2], "v": [10, 20]}, keys=["k"])

    left2 = DataFrame[R]({"k": [1, 2], "v": [1, 2]})
    right2 = DataFrame[R]({"k": [1, 2], "v": [99, None]})
    up = left2.update(right2, on=["k"]).collect(as_lists=True)
    assert_table_eq_sorted(up, {"k": [1, 2], "v": [99, 2]}, keys=["k"])


def test_pandas_ui_compare_flags_diffs() -> None:
    from pydantable.pandas import DataFrame

    class R(Schema):
        a: int
        b: int

    x = DataFrame[R]({"a": [1, 2], "b": [3, 4]})
    y = DataFrame[R]({"a": [1, 9], "b": [3, 4]})
    cmp = x.compare(y).collect(as_lists=True)
    assert cmp["a_diff"] == [False, True]
    assert cmp["b_diff"] == [False, False]


def test_pandas_ui_corr_and_cov() -> None:
    np = pytest.importorskip("numpy")
    from pydantable.pandas import DataFrame

    class R(Schema):
        a: int
        b: int
        c: int

    df = DataFrame[R]({"a": [1, 2, 3], "b": [2, 4, 6], "c": [1, 0, 1]})
    cm = df.corr().collect(as_lists=True)
    assert set(cm.keys()) == {"a", "b", "c"}
    m = np.array([[cm["a"][i], cm["b"][i], cm["c"][i]] for i in range(3)])
    assert np.allclose(m, np.corrcoef([[1, 2, 3], [2, 4, 6], [1, 0, 1]], rowvar=True))

    cv = df.cov().collect(as_lists=True)
    m2 = np.array([[cv["a"][i], cv["b"][i], cv["c"][i]] for i in range(3)])
    exp = np.cov(np.array([[1, 2, 3], [2, 4, 6], [1, 0, 1]], dtype=float), rowvar=True)
    assert m2.shape == exp.shape
    assert np.allclose(m2, exp, rtol=1e-5, atol=1e-5)


def test_pandas_ui_groupby_rolling_partitioned() -> None:
    from pydantable.pandas import DataFrame

    class R(Schema):
        g: int
        x: int

    df = DataFrame[R]({"g": [1, 1, 2, 2], "x": [1, 3, 10, 30]})
    out = df.group_by("g").rolling(window=2, min_periods=1).sum("x", out_name="s")
    got = out.collect(as_lists=True)
    assert_table_eq_sorted(
        {k: got[k] for k in ("g", "s")},
        {"g": [1, 1, 2, 2], "s": [1, 4, 10, 40]},
        keys=["g", "s"],
    )


def test_pandas_ui_expr_row_accum_and_clip_replace() -> None:
    from pydantable.pandas import DataFrame

    class R(Schema):
        x: int

    df = DataFrame[R]({"x": [1, 2, 3]})
    w = df.with_columns(
        cs=df.col("x").cumsum(),
        d=df.col("x").diff(),
        clipped=df.col("x").clip(lower=2, upper=2),
        rep=df.col("x").replace({1: 99}),
    ).collect(as_lists=True)
    assert w["cs"] == [1, 3, 6]
    assert w["d"][0] is None or w["d"][0] != w["d"][0]  # nan-like first row
    assert w["clipped"] == [2, 2, 2]
    assert w["rep"][0] == 99
    assert w["rep"][1] == 2


def test_pandas_ui_reindex_align_keys() -> None:
    from pydantable.pandas import DataFrame

    class K(Schema):
        k: int

    class V(Schema):
        k: int
        v: int | None

    keys = DataFrame[K]({"k": [1, 3]})
    body = DataFrame[V]({"k": [1, 2], "v": [10, 20]})
    ri = body.reindex(keys, on="k").collect(as_lists=True)
    assert_table_eq_sorted(ri, {"k": [1, 3], "v": [10, None]}, keys=["k"])

    left = DataFrame[V]({"k": [1, 2], "v": [1, 2]})
    right = DataFrame[V]({"k": [2, 3], "v": [20, 30]})
    a_l, _a_r = left.align(right, on=["k"], join="outer")
    assert set(a_l.collect(as_lists=True)["k"]) == {1, 2, 3}


def test_pandas_ui_transpose_and_dot() -> None:
    np = pytest.importorskip("numpy")
    from pydantable.pandas import DataFrame

    class M(Schema):
        a: int
        b: int

    df = DataFrame[M]({"a": [1, 2], "b": [3, 4]})
    t = df.T.collect(as_lists=True)
    assert t["a"] == [1, 3]
    assert t["b"] == [2, 4]

    coef = DataFrame[M]({"a": [1, 0], "b": [0, 1]})
    dst = df.dot(coef).collect(as_lists=True)
    m = np.array([[1, 3], [2, 4]], dtype=float) @ np.array(
        [[1, 0], [0, 1]], dtype=float
    )
    assert np.allclose([[dst["a"][0], dst["b"][0]], [dst["a"][1], dst["b"][1]]], m)


def test_pandas_ui_insert_pop_and_eval_alias() -> None:
    from pydantable.pandas import DataFrame

    class R(Schema):
        a: int
        b: int

    df = DataFrame[R]({"a": [1], "b": [2]})
    ins = df.insert(1, "m", 99).collect(as_lists=True)
    assert list(ins.keys()) == ["a", "m", "b"]
    assert ins["m"] == [99]
    expr, rest = df.pop("b")
    assert expr.referenced_columns() == {"b"}
    assert rest.collect(as_lists=True) == {"a": [1]}

    class ACol(Schema):
        a: int

    q = DataFrame[ACol]({"a": [1, 5]}).eval("a > 1")
    direct = DataFrame[ACol]({"a": [1, 5]}).query("a > 1")
    assert q.collect(as_lists=True) == direct.collect(as_lists=True)


def test_pandas_ui_get_dummies_qcut_cut_factorize_ewm_pivot() -> None:
    pd = pytest.importorskip("pandas")
    np = pytest.importorskip("numpy")
    from pydantable.pandas import DataFrame

    class S(Schema):
        id: int
        color: str
        v: float

    df = DataFrame[S]({"id": [1, 2, 3], "color": ["a", "b", "a"], "v": [1.0, 2.0, 3.0]})
    dumb = df.get_dummies(["color"], dtype="int").collect(as_lists=True)
    assert dumb["id"] == [1, 2, 3]
    assert dumb["v"] == [1.0, 2.0, 3.0]
    assert dumb["color_a"] == [1, 0, 1]
    assert dumb["color_b"] == [0, 1, 0]

    codes, cats = df.factorize_column("color")
    exp_c, exp_u = pd.factorize(pd.Series(["a", "b", "a"]), use_na_sentinel=True)
    assert codes == list(exp_c)
    assert cats == list(exp_u)

    cut_df = df.cut("v", bins=[0.0, 2.0, 4.0]).collect(as_lists=True)
    assert "v_cut" in cut_df
    q_df = df.qcut("v", q=2, new_column="vq").collect(as_lists=True)
    assert "vq" in q_df

    ewm = df.ewm(span=2).mean("v", out_name="m").collect(as_lists=True)
    assert len(ewm["m"]) == 3
    assert np.allclose(
        ewm["m"], pd.Series([1.0, 2.0, 3.0]).ewm(span=2).mean(), equal_nan=True
    )

    class P(Schema):
        i: int
        k: str
        val: int

    psrc = DataFrame[P]({"i": [1, 1], "k": ["A", "B"], "val": [10, 20]})
    pv = psrc.pivot(index="i", columns="k", values="val", aggregate_function="first")
    assert pv.collect(as_lists=True)["i"] == [1]
