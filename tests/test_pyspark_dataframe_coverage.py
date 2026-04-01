from __future__ import annotations

import pytest
from conftest import assert_table_eq_sorted
from pydantable.pyspark import DataFrame, DataFrameModel
from pydantable.pyspark.dataframe import _text_show_table
from pydantable.schema import Schema


class User(DataFrameModel):
    id: int
    name: str
    age: int | None


class Row(Schema):
    id: int
    name: str
    age: int | None


class RowNA(Schema):
    id: int
    name: str | None
    age: int | None


def test_pyspark_dataframe_transform_rejects_non_dataframe() -> None:
    df = DataFrame[Row]({"id": [1], "name": ["a"], "age": [10]})
    with pytest.raises(TypeError, match="return a DataFrame"):
        df.transform(lambda x: "not-a-df")  # type: ignore[arg-type, return-value]


def test_pyspark_model_transform_rejects_non_model() -> None:
    df = User({"id": [1], "name": ["a"], "age": [10]})
    with pytest.raises(TypeError, match="DataFrameModel"):
        df.transform(lambda m: None)  # type: ignore[arg-type, return-value]


def test_pyspark_select_typed_requires_columns() -> None:
    df = User({"id": [1], "name": ["a"], "age": [10]})
    with pytest.raises(ValueError, match="select_typed"):
        df.select_typed()


def test_pyspark_order_by_requires_columns() -> None:
    df = User({"id": [1], "name": ["a"], "age": [10]})
    with pytest.raises(ValueError, match="orderBy"):
        df.orderBy()


def test_pyspark_order_by_ascending_length_mismatch() -> None:
    df = User({"id": [1, 2], "name": ["a", "b"], "age": [10, 20]})
    with pytest.raises(ValueError, match="ascending length"):
        df.orderBy("id", "name", ascending=[True])


def test_pyspark_limit_rejects_negative() -> None:
    df = User({"id": [1], "name": ["a"], "age": [10]})
    with pytest.raises(ValueError, match="limit"):
        df.limit(-1)


def test_pyspark_union_all_alias() -> None:
    a = User({"id": [1], "name": ["x"], "age": [1]})
    b = User({"id": [2], "name": ["y"], "age": [2]})
    out = a.unionAll(b).collect(as_lists=True)
    assert sorted(out["id"]) == [1, 2]


def test_pyspark_drop_duplicates_with_and_without_subset() -> None:
    df = User(
        {
            "id": [1, 1, 2],
            "name": ["a", "a", "b"],
            "age": [10, 10, 20],
        }
    )
    u1 = df.dropDuplicates()
    assert len(u1.collect(as_lists=True)["id"]) == 2
    u2 = df.dropDuplicates(["name"])
    assert u2.collect(as_lists=True)["name"] == ["a", "b"]


def test_pyspark_order_by_rejects_maintain_order_kwarg() -> None:
    df = User(
        {
            "id": [1, 2, 3, 4],
            "name": ["a", "b", "c", "d"],
            "age": [10, 10, 10, 20],
        }
    )
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        _ = df.orderBy("age", maintain_order=True)  # type: ignore[call-arg]


def test_pyspark_dataframe_getitem_errors() -> None:
    df = DataFrame[Row]({"id": [1], "name": ["a"], "age": [10]})
    with pytest.raises(ValueError, match="non-empty"):
        df[[]]  # type: ignore[index]
    with pytest.raises(TypeError, match="supports"):
        df[object()]  # type: ignore[index]


def test_pyspark_show_truncates_long_cell(capsys: pytest.CaptureFixture[str]) -> None:
    long = "y" * 80
    df = User({"id": [1], "name": [long], "age": [1]})
    df.show(n=5, truncate=True)
    out = capsys.readouterr().out
    assert "…" in out or "..." in out or len(out) < len(long) * 2


def test_text_show_table_empty_and_truncation() -> None:
    assert _text_show_table({}, truncate=True) == "(empty)"
    wide = {"c": ["x" * 100]}
    txt = _text_show_table(wide, truncate=True)
    assert "…" in txt or "..." in txt


def test_pyspark_model_union_accepts_bare_dataframe() -> None:
    m = User({"id": [1], "name": ["a"], "age": [10]})
    raw = DataFrame[Row]({"id": [2], "name": ["b"], "age": [20]})
    out = m.union(raw).collect(as_lists=True)
    assert sorted(out["id"]) == [1, 2]


def test_pyspark_groupby_agg_returns_pyspark_dataframe() -> None:
    df = DataFrame[Row]({"id": [1, 1, 2], "name": ["a", "b", "c"], "age": [1, 2, 3]})
    out = df.groupBy("id").agg(c=("count", "name"))
    assert isinstance(out, DataFrame)
    assert out.__class__.__module__ == "pydantable.pyspark.dataframe"


def test_pyspark_groupby_count_no_args_is_per_group_len() -> None:
    df = DataFrame[Row]({"id": [1, 1, 2], "name": ["a", "b", "c"], "age": [1, 2, 3]})
    out = df.groupBy("id").count().to_dict()
    by_id = sorted(zip(out["id"], out["len"], strict=True))
    assert by_id == [(1, 2), (2, 1)]


def test_pyspark_groupby_pivot_agg_multi_ops_and_explicit_values() -> None:
    class S(Schema):
        g: str
        k: str
        v: int

    df = DataFrame[S](
        {
            "g": ["A", "A", "B"],
            "k": ["x", "y", "x"],
            "v": [1, 2, 3],
        }
    )
    out = (
        df.groupBy("g")
        .pivot("k", values=["x", "y"])
        .agg(s=("sum", "v"), m=("max", "v"))
        .to_dict()
    )
    order = sorted(range(len(out["g"])), key=lambda i: out["g"][i])
    got = {k: [out[k][i] for i in order] for k in out}
    assert got["g"] == ["A", "B"]
    assert got["x_s"] == [1, 3]
    assert got["y_s"] == [2, None]
    assert got["x_m"] == [1, 3]
    assert got["y_m"] == [2, None]


def test_pyspark_groupby_pivot_rejects_non_list_values() -> None:
    class S(Schema):
        g: str
        k: str
        v: int

    df = DataFrame[S]({"g": ["A"], "k": ["x"], "v": [1]})
    with pytest.raises(TypeError, match=r"pivot\(values="):
        _ = df.groupBy("g").pivot("k", values=("x", "y"))  # type: ignore[arg-type]


def test_pyspark_groupby_pivot_agg_requires_specs() -> None:
    class S(Schema):
        g: str
        k: str
        v: int

    df = DataFrame[S]({"g": ["A"], "k": ["x"], "v": [1]})
    with pytest.raises(TypeError, match=r"agg\(\) requires at least one"):
        df.groupBy("g").pivot("k").agg()  # type: ignore[call-arg]


def test_pyspark_groupby_pivot_agg_rejects_out_name_with_internal_sep() -> None:
    class S(Schema):
        g: str
        k: str
        v: int

    df = DataFrame[S]({"g": ["A"], "k": ["x"], "v": [1]})
    with pytest.raises(ValueError, match="cannot contain"):
        df.groupBy("g").pivot("k").agg(**{"bad__name": ("sum", "v")})


def test_pyspark_groupby_pivot_agg_preserves_explicit_pivot_order() -> None:
    class S(Schema):
        g: str
        k: str
        v: int

    df = DataFrame[S]({"g": ["A"], "k": ["x"], "v": [1]})
    out = (
        df.groupBy("g")
        .pivot("k", values=["y", "x"])
        .agg(s=("sum", "v"))
        .to_dict()
    )
    # Explicit pivot values must be present (even if missing in data).
    assert out["g"] == ["A"]
    assert out["x_s"] == [1]
    assert out["y_s"] == [None]


def test_pyspark_cross_join_and_count() -> None:
    class A(Schema):
        x: int

    class B(Schema):
        y: int

    a = DataFrame[A]({"x": [1, 2]})
    b = DataFrame[B]({"y": [10, 20]})
    c = a.crossJoin(b)
    assert c.count() == 4
    d = c.to_dict()
    assert len(d["x"]) == 4 and len(d["y"]) == 4


def test_pyspark_print_schema_smoke(capsys: pytest.CaptureFixture[str]) -> None:
    df = User({"id": [1], "name": ["a"], "age": [10]})
    df.printSchema()
    out = capsys.readouterr().out
    assert "root" in out
    assert "id" in out


def test_pyspark_fillna_dropna_and_na_facade() -> None:
    df = DataFrame[RowNA]({"id": [1, 2], "name": [None, "b"], "age": [10, None]})
    f = df.fillna(0, subset=["age"])
    assert f.to_dict()["age"] == [10, 0]
    d = df.dropna(subset=["name"])
    assert d.to_dict()["id"] == [2]
    n = df.na.fill(7, subset=["age"])
    assert n.to_dict()["age"] == [10, 7]


def test_pyspark_union_by_name_column_order() -> None:
    df1 = DataFrame[Row]({"id": [1], "name": ["a"], "age": [1]})
    df2 = DataFrame[Row]({"name": ["b"], "id": [2], "age": [2]})
    u = df1.unionByName(df2)
    assert u.to_dict() == {"id": [1, 2], "name": ["a", "b"], "age": [1, 2]}


def test_pyspark_union_by_name_allow_missing_columns() -> None:
    class W(Schema):
        id: int
        x: int | None

    class V(Schema):
        id: int

    df1 = DataFrame[W]({"id": [1], "x": [10]})
    df2 = DataFrame[V]({"id": [2]})
    with pytest.raises(ValueError, match="allowMissingColumns"):
        df1.unionByName(df2)
    u = df1.unionByName(df2, allowMissingColumns=True)
    assert u.count() == 2
    d = u.to_dict()
    assert d["id"] == [1, 2]
    assert d["x"][0] == 10
    assert d["x"][1] is None


def test_pyspark_intersect_and_subtract() -> None:
    df1 = DataFrame[Row]({"id": [1, 2], "name": ["a", "b"], "age": [1, 2]})
    df2 = DataFrame[Row]({"id": [2, 3], "name": ["b", "c"], "age": [2, 3]})
    assert df1.intersect(df2).to_dict() == {"id": [2], "name": ["b"], "age": [2]}
    sub = df1.subtract(df2).to_dict()
    assert sub["id"] == [1]


def test_pyspark_except_all_and_intersect_all_multiset_semantics() -> None:
    df1 = DataFrame[Row]({"id": [1, 1, 2], "name": ["a", "a", "b"], "age": [1, 1, 2]})
    df2 = DataFrame[Row]({"id": [1, 2], "name": ["a", "b"], "age": [1, 2]})

    # exceptAll keeps one of the duplicate (1,'a',1) rows.
    out_ex = df1.exceptAll(df2).to_dict()
    assert out_ex == {"id": [1], "name": ["a"], "age": [1]}

    # intersectAll keeps min multiplicities: (1,'a',1) once and (2,'b',2) once.
    out_in = df1.intersectAll(df2).to_dict()
    assert sorted(zip(out_in["id"], out_in["name"], out_in["age"], strict=True)) == [
        (1, "a", 1),
        (2, "b", 2),
    ]


def test_pyspark_setops_all_treat_nulls_as_equal() -> None:
    class S(Schema):
        id: int
        v: int | None

    a = DataFrame[S]({"id": [1, 1], "v": [None, None]})
    b = DataFrame[S]({"id": [1], "v": [None]})
    ex = a.exceptAll(b).to_dict()
    assert ex["id"] == [1]
    assert ex["v"] == [None]
    it = a.intersectAll(b).to_dict()
    assert it["id"] == [1]
    assert it["v"] == [None]

def test_pyspark_model_groupby_and_cross_join() -> None:
    class MX(DataFrameModel):
        x: int

    class MY(DataFrameModel):
        y: int

    mx = MX({"x": [1, 2]})
    my = MY({"y": [10]})
    g = mx.groupBy("x")
    assert type(g).__name__ == "PySparkGroupedDataFrameModel"
    agg = g.agg(s=("sum", "x"))
    assert_table_eq_sorted(
        agg.collect(as_lists=True),
        {"x": [1, 2], "s": [1, 2]},
        keys=["x"],
    )
    assert mx.crossJoin(my).count() == 2


def test_pyspark_group_by_snake_case_returns_pyspark_grouped() -> None:
    df = DataFrame[Row]({"id": [1, 1], "name": ["a", "b"], "age": [1, 2]})
    g = df.group_by("id")
    assert type(g).__name__ == "PySparkGroupedDataFrame"


def test_pyspark_join_returns_pyspark_dataframe() -> None:
    left = DataFrame[Row]({"id": [1, 2], "name": ["a", "b"], "age": [10, 20]})
    right = DataFrame[Row]({"id": [1, 2], "name": ["a", "b"], "age": [10, 20]})
    j = left.join(right, on="id", how="inner")
    assert j.__class__.__module__ == "pydantable.pyspark.dataframe"


def test_pyspark_except_all_matches_subtract() -> None:
    df1 = DataFrame[Row]({"id": [1, 2], "name": ["a", "b"], "age": [1, 2]})
    df2 = DataFrame[Row]({"id": [2, 3], "name": ["b", "c"], "age": [2, 3]})
    assert df1.exceptAll(df2).to_dict() == df1.subtract(df2).to_dict()


def test_pyspark_intersect_union_by_name_require_compatible_schemas() -> None:
    df1 = DataFrame[Row]({"id": [1], "name": ["a"], "age": [1]})

    class Other(Schema):
        id: int
        x: str

    df_other = DataFrame[Other]({"id": [1], "x": ["z"]})
    with pytest.raises(ValueError, match="identical schemas"):
        df1.intersect(df_other)

    class Extra(Schema):
        id: int
        name: str
        age: int
        z: int

    df_extra = DataFrame[Extra]({"id": [2], "name": ["b"], "age": [2], "z": [0]})
    with pytest.raises(ValueError, match="allowMissingColumns"):
        df1.unionByName(df_extra)


def test_pyspark_empty_dataframe_count_is_zero() -> None:
    empty = DataFrame[Row]({"id": [], "name": [], "age": []})
    assert empty.count() == 0


def test_pyspark_explain_prints_non_empty(capsys: pytest.CaptureFixture[str]) -> None:
    df = User({"id": [1], "name": ["a"], "age": [10]})
    df.explain()
    out = capsys.readouterr().out
    assert len(out.strip()) > 0


def test_pyspark_dataframe_model_count_print_schema_explain_na(
    capsys: pytest.CaptureFixture[str],
) -> None:
    m = User({"id": [1], "name": ["a"], "age": [10]})
    assert m.count() == 1
    m.printSchema()
    assert "id" in capsys.readouterr().out
    m.explain()
    assert len(capsys.readouterr().out.strip()) > 0
    dropped = m.na.drop(subset=["name"])
    assert dropped.count() == 1


def test_pyspark_dropna_with_thresh() -> None:
    df = DataFrame[RowNA](
        {"id": [1, 2, 3], "name": ["a", None, "c"], "age": [1, None, 3]}
    )
    out = df.dropna(thresh=2, subset=["name", "age"])
    assert out.to_dict()["id"] == [1, 3]
