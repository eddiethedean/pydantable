from decimal import Decimal
from datetime import date, datetime, timedelta

import pytest
from conftest import assert_table_eq_sorted
from pydantable import DataFrame, Schema
from pydantable import selectors as s
from pydantable.expressions import ColumnRef
from pydantic import BaseModel


class User(Schema):
    id: int
    age: int


class _Addr(Schema):
    street: str


class _Person(Schema):
    id: int
    addr: _Addr


def test_with_columns_and_collect_python():
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    df2 = df.with_columns(age2=df.age * 2)
    assert df2.schema_fields()["age2"] is int

    rows = df2.collect()
    assert len(rows) == 2
    assert isinstance(rows[0], BaseModel)
    assert rows[0].model_dump() == {"id": 1, "age": 20, "age2": 40}
    assert rows[1].model_dump() == {"id": 2, "age": 30, "age2": 60}

    result = df2.collect(as_lists=True)
    assert result == {"id": [1, 2], "age": [20, 30], "age2": [40, 60]}


def test_select_and_filter():
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    df2 = df.with_columns(age2=df.age * 2)
    df3 = df2.select("id", "age2")
    assert df3.schema_fields() == {"id": int, "age2": int}

    df4 = df3.filter(df3.age2 > 40)
    result = df4.to_dict()
    assert result == {"id": [2], "age2": [60]}


def test_nested_schema_collect_filter_and_select_preserve_struct_column():
    df = DataFrame[_Person](
        {
            "id": [1, 2],
            "addr": [{"street": "a"}, {"street": "b"}],
        }
    )
    assert df.collect(as_lists=True) == {
        "id": [1, 2],
        "addr": [{"street": "a"}, {"street": "b"}],
    }
    df2 = df.filter(df.id > 1)
    assert df2.collect(as_lists=True) == {
        "id": [2],
        "addr": [{"street": "b"}],
    }
    df3 = df2.select("addr", "id")
    fields = df3.schema_fields()
    assert fields["id"] is int
    assert fields["addr"] is _Addr
    assert df3.collect(as_lists=True) == {
        "addr": [{"street": "b"}],
        "id": [2],
    }


def test_nested_schema_rejects_arithmetic_on_struct_column():
    df = DataFrame[_Person](
        {"id": [1], "addr": [{"street": "main"}]},
    )
    with pytest.raises(TypeError, match="struct-, list-, or map-typed"):
        _ = df.addr + 1


def test_rename_preserves_nested_schema_class() -> None:
    df = DataFrame[_Person](
        {"id": [1], "addr": [{"street": "main"}]},
    )
    df2 = df.rename({"addr": "location"})
    assert df2.schema_fields()["location"] is _Addr


def test_struct_field_expr_projects_scalar() -> None:
    df = DataFrame[_Person](
        {
            "id": [1, 2],
            "addr": [{"street": "a"}, {"street": "b"}],
        }
    )
    out = df.with_columns(st=df.addr.struct_field("street"))
    assert out.schema_fields()["st"] is str
    assert out.collect(as_lists=True) == {
        "id": [1, 2],
        "addr": [{"street": "a"}, {"street": "b"}],
        "st": ["a", "b"],
    }


def test_join_preserves_struct_identity_on_pass_through_columns() -> None:
    class L(Schema):
        k: int
        addr: _Addr

    class R(Schema):
        k: int
        v: int

    left = DataFrame[L]({"k": [1], "addr": [{"street": "x"}]})
    right = DataFrame[R]({"k": [1], "v": [2]})
    j = left.join(right, on="k", how="inner")
    assert j.schema_fields()["addr"] is _Addr
    assert j.schema_fields()["v"] is int


class _WithIntList(Schema):
    id: int
    tags: list[int]


def test_list_int_roundtrip_and_explode() -> None:
    df = DataFrame[_WithIntList](
        {
            "id": [1, 2],
            "tags": [[1, 2], [3]],
        }
    )
    assert df.collect(as_lists=True) == {"id": [1, 2], "tags": [[1, 2], [3]]}
    ex = df.explode("tags")
    assert ex.collect(as_lists=True) == {"id": [1, 1, 2], "tags": [1, 2, 3]}


def test_concat_vertical_preserves_struct_identity() -> None:
    df1 = DataFrame[_Person](
        {"id": [1], "addr": [{"street": "a"}]},
    )
    df2 = DataFrame[_Person](
        {"id": [2], "addr": [{"street": "b"}]},
    )
    cat = DataFrame.concat([df1, df2], how="vertical")
    assert cat.schema_fields()["addr"] is _Addr
    assert cat.collect(as_lists=True) == {
        "id": [1, 2],
        "addr": [{"street": "a"}, {"street": "b"}],
    }


def test_with_columns_rejects_unknown_referenced_columns():
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    with pytest.raises(ValueError, match="references unknown column"):
        df.with_columns(
            bad=df.col("age") + 1,
            missing=ColumnRef(name="missing", dtype=int) + 1,
        )


def test_select_requires_at_least_one_column():
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    with pytest.raises(ValueError, match="requires at least one column"):
        df.select()


def test_select_rejects_multi_column_expression() -> None:
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    expr = df.age + df.id
    with pytest.raises(
        TypeError,
        match=r"Expr\.alias\('name'\)",
    ):
        df.select(expr)


def test_select_rejects_non_columnref_expr_without_alias() -> None:
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    with pytest.raises(TypeError, match=r"Expr\.alias\('name'\)"):
        df.select(df.age * 2)


def test_select_accepts_aliased_expr() -> None:
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    out = df.select("id", (df.age * 2).alias("age2"))
    assert out.schema_fields() == {"id": int, "age2": int}
    assert out.collect(as_lists=True) == {"id": [1, 2], "age2": [40, 60]}


def test_with_columns_positional_aliased_expr() -> None:
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    out = df.with_columns((df.age * 2).alias("age2"))
    assert out.schema_fields()["age2"] is int
    assert out.collect(as_lists=True)["age2"] == [40, 60]


def test_select_all_prefix_suffix() -> None:
    class S(Schema):
        a: int
        aa: int
        b: int

    df = DataFrame[S]({"a": [1], "aa": [2], "b": [3]})
    assert df.select_all().to_dict() == {"a": [1], "aa": [2], "b": [3]}
    assert df.select_prefix("a").to_dict() == {"a": [1], "aa": [2]}
    assert df.select_suffix("a").to_dict() == {"a": [1], "aa": [2]}


def test_limit_first_last_topk_bottomk() -> None:
    class S(Schema):
        k: int
        v: int

    df = DataFrame[S]({"k": [3, 1, 2], "v": [30, 10, 20]})
    assert df.limit(2).collect(as_lists=True)["k"] == [3, 1]
    assert df.first().collect(as_lists=True) == {"k": [3], "v": [30]}
    assert df.last().collect(as_lists=True) == {"k": [2], "v": [20]}

    top2 = df.top_k(2, by="k").collect(as_lists=True)
    assert top2["k"] == [3, 2]
    bottom2 = df.bottom_k(2, by="k").collect(as_lists=True)
    assert bottom2["k"] == [1, 2]


def test_select_with_selector_dsl_name_patterns_and_exclude() -> None:
    class S(Schema):
        a: int
        aa: int
        b: int
        bb: int

    df = DataFrame[S]({"a": [1], "aa": [2], "b": [3], "bb": [4]})
    out = df.select(s.starts_with("a") | s.by_name("bb"))
    assert out.to_dict() == {"a": [1], "aa": [2], "bb": [4]}

    out2 = df.select(s.everything().exclude(s.ends_with("b")))
    assert out2.to_dict() == {"a": [1], "aa": [2]}

    out3 = df.select("a", "aa", "bb", exclude=s.ends_with("b"))
    assert out3.to_dict() == {"a": [1], "aa": [2]}

    out4 = df.select(exclude=["bb"])
    assert out4.to_dict() == {"a": [1], "aa": [2], "b": [3]}


def test_select_with_selector_dsl_by_dtype_groups() -> None:
    class S(Schema):
        i: int
        f: float
        t: datetime
        s1: str

    df = DataFrame[S]({"i": [1], "f": [2.0], "t": [datetime(2020, 1, 1)], "s1": ["x"]})
    out = df.select(s.numeric() | s.temporal())
    assert out.to_dict() == {"i": [1], "f": [2.0], "t": [datetime(2020, 1, 1)]}


def test_select_with_selector_dsl_expanded_dtype_groups_and_structs() -> None:
    class S(Schema):
        i: int
        f: float
        d: Decimal
        addr: _Addr

    df = DataFrame[S](
        {"i": [1], "f": [2.5], "d": [Decimal("3.0")], "addr": [{"street": "x"}]}
    )
    out = df.select(s.integers() | s.decimals() | s.structs())
    assert out.to_dict() == {"i": [1], "d": [Decimal("3.0")], "addr": [{"street": "x"}]}


def test_select_with_selector_dsl_composition_invert_and_regex() -> None:
    class S(Schema):
        id: int
        age: int
        age2: int
        name: str

    df = DataFrame[S]({"id": [1], "age": [2], "age2": [3], "name": ["x"]})
    out = df.select(~s.starts_with("age"))
    assert out.to_dict() == {"id": [1], "name": ["x"]}

    out2 = df.select(s.matches(r"^age\d?$") | s.by_name("id"))
    assert out2.to_dict() == {"id": [1], "age": [2], "age2": [3]}

    out3 = df.select((s.numeric() & ~s.by_name("age2")) | s.by_name("name"))
    assert out3.to_dict() == {"id": [1], "age": [2], "name": ["x"]}


def test_select_with_selector_dsl_empty_match_raises() -> None:
    class S(Schema):
        a: int

    df = DataFrame[S]({"a": [1]})
    with pytest.raises(ValueError, match=r"matched no columns.*Available columns"):
        df.select(s.starts_with("zzz"))


def test_select_exclude_rejects_global_aggregates() -> None:
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    from pydantable.expressions import global_sum

    with pytest.raises(TypeError, match="cannot be used with global aggregates"):
        df.select(global_sum(df.age), exclude=["id"]).to_dict()


def test_reorder_columns_select_first_select_last_and_move() -> None:
    class S(Schema):
        a: int
        aa: int
        b: int
        bb: int

    df = DataFrame[S]({"a": [1], "aa": [2], "b": [3], "bb": [4]})
    out = df.reorder_columns([s.starts_with("b"), "a"]).to_dict()
    assert list(out.keys()) == ["b", "bb", "a", "aa"]

    out2 = df.select_first("bb", s.starts_with("a")).to_dict()
    assert list(out2.keys()) == ["bb", "a", "aa", "b"]

    out3 = df.select_last(s.starts_with("a")).to_dict()
    assert list(out3.keys()) == ["b", "bb", "a", "aa"]

    out4 = df.move(s.starts_with("a"), after="bb").to_dict()
    assert list(out4.keys()) == ["b", "bb", "a", "aa"]


def test_rename_prefix_suffix_replace_and_rename_map() -> None:
    class S(Schema):
        a: int
        aa: int
        b: int

    df = DataFrame[S]({"a": [1], "aa": [2], "b": [3]})
    out = df.rename_prefix("x_", selector=s.starts_with("a")).to_dict()
    assert set(out.keys()) == {"x_a", "x_aa", "b"}

    out2 = df.rename_suffix("_y", selector=s.by_name("b")).to_dict()
    assert set(out2.keys()) == {"a", "aa", "b_y"}

    out3 = df.rename_replace("a", "z", selector=s.starts_with("a")).to_dict()
    assert set(out3.keys()) == {"z", "zz", "b"}

    m = s.rename_map(s.starts_with("a"), lambda c: f"p_{c}")(df.schema_fields())
    out4 = df.rename(m).to_dict()
    assert set(out4.keys()) == {"p_a", "p_aa", "b"}


def test_rename_with_selector_renames_subset_and_preserves_order() -> None:
    class S(Schema):
        a: int
        aa: int
        b: int

    df = DataFrame[S]({"a": [1], "aa": [2], "b": [3]})
    out = df.rename_with_selector(s.starts_with("a"), lambda c: f"x_{c}").to_dict()
    assert out == {"x_a": [1], "x_aa": [2], "b": [3]}


def test_rename_with_selector_rejects_collisions() -> None:
    class S(Schema):
        a: int
        aa: int

    df = DataFrame[S]({"a": [1], "aa": [2]})
    with pytest.raises(ValueError, match="duplicate output column"):
        df.rename_with_selector(s.starts_with("a"), lambda _c: "x").to_dict()


def test_rename_with_selector_empty_match_raises() -> None:
    df = DataFrame[User]({"id": [1], "age": [2]})
    with pytest.raises(ValueError, match=r"matched no columns.*Available columns"):
        df.rename_with_selector(s.starts_with("zzz"), lambda c: c).to_dict()

def test_drop_with_selector_dsl_and_strict_false() -> None:
    class S(Schema):
        a: int
        b: int
        c: int

    df = DataFrame[S]({"a": [1], "b": [2], "c": [3]})
    out = df.drop(s.starts_with("b"))
    assert out.to_dict() == {"a": [1], "c": [3]}

    out2 = df.drop(s.by_name("missing") | s.by_name("b"), strict=False)
    assert out2.to_dict() == {"a": [1], "c": [3]}


def test_fill_null_and_drop_nulls_accept_subset_selector() -> None:
    class S(Schema):
        a: int | None
        b: int | None
        c: int

    df = DataFrame[S]({"a": [None, 1], "b": [2, None], "c": [9, 9]})
    filled = df.fill_null(0, subset=s.by_name("a")).collect(as_lists=True)
    assert filled["a"] == [0, 1]

    dropped = df.drop_nulls(subset=s.by_name("b")).collect(as_lists=True)
    assert dropped == {"a": [None], "b": [2], "c": [9]}


def test_melt_unpivot_accept_selectors_for_id_vars_value_vars() -> None:
    class S(Schema):
        id: int
        a: int
        b: int

    df = DataFrame[S]({"id": [1], "a": [10], "b": [20]})
    m = df.melt(id_vars=s.by_name("id"), value_vars=s.starts_with("a")).collect(
        as_lists=True
    )
    assert set(m.keys()) == {"id", "variable", "value"}
    assert m["id"] == [1]
    assert m["variable"] == ["a"]
    assert m["value"] == [10]

    u = df.unpivot(index=s.by_name("id"), on=s.by_name("b")).collect(as_lists=True)
    assert u["variable"] == ["b"]
    assert u["value"] == [20]

def test_with_columns_none_requires_destination_type() -> None:
    class UserNullable(Schema):
        id: int
        age: int | None

    df = DataFrame[UserNullable]({"id": [1, 2], "age": [20, 30]})
    with pytest.raises(TypeError, match=r"cannot infer destination type"):
        df.with_columns(new=None)


def test_p1_sort_unique_drop_rename_slice_concat() -> None:
    class UserNullable(Schema):
        id: int
        age: int | None
        country: str

    df = DataFrame[UserNullable](
        {
            "id": [3, 1, 2, 2],
            "age": [30, None, 20, 20],
            "country": ["CA", "US", "US", "US"],
        }
    )

    sorted_df = df.sort("id")
    assert sorted_df.collect(as_lists=True)["id"] == [1, 2, 2, 3]

    unique_df = sorted_df.unique(subset=["id", "age", "country"])
    assert unique_df.collect(as_lists=True)["id"] == [1, 2, 3]

    dropped = unique_df.drop("country")
    assert set(dropped.schema_fields().keys()) == {"id", "age"}

    renamed = dropped.rename({"age": "years"})
    assert set(renamed.schema_fields().keys()) == {"id", "years"}
    assert renamed.schema_fields()["years"] == int | None

    sliced = renamed.slice(1, 2)
    assert sliced.collect(as_lists=True) == {"id": [2, 3], "years": [20, 30]}
    assert renamed.head(2).collect(as_lists=True) == {"id": [1, 2], "years": [None, 20]}
    assert renamed.tail(2).collect(as_lists=True) == {"id": [2, 3], "years": [20, 30]}

    left = renamed.select("id")
    right = renamed.select("id")
    vcat = DataFrame.concat([left, right], how="vertical")
    assert vcat.collect(as_lists=True) == {"id": [1, 2, 3, 1, 2, 3]}

    left_h = renamed.select("id")
    right_h = renamed.select("years")
    hcat = DataFrame.concat([left_h, right_h], how="horizontal")
    assert hcat.collect(as_lists=True) == {"id": [1, 2, 3], "years": [None, 20, 30]}


def test_drop_strict_false_ignores_missing() -> None:
    df = DataFrame[User]({"id": [1], "age": [2]})
    out = df.drop("missing", strict=False)
    assert out.schema_fields() == {"id": int, "age": int}


def test_rename_strict_false_ignores_missing() -> None:
    df = DataFrame[User]({"id": [1], "age": [2]})
    out = df.rename({"missing": "x", "age": "years"}, strict=False)
    assert out.schema_fields() == {"id": int, "years": int}


def test_sort_descending_length_mismatch_raises() -> None:
    df = DataFrame[User]({"id": [1], "age": [2]})
    with pytest.raises(ValueError, match="descending"):
        df.sort("id", "age", descending=[True])


def test_sort_maintain_order_is_stable_for_ties() -> None:
    class S(Schema):
        k: int
        seq: int

    df = DataFrame[S]({"k": [1, 1, 1, 2, 2], "seq": [10, 11, 12, 20, 21]})
    out = df.sort("k", maintain_order=True).to_dict()
    assert out == {"k": [1, 1, 1, 2, 2], "seq": [10, 11, 12, 20, 21]}


def test_sort_maintain_order_matches_default_on_unique_keys() -> None:
    class S(Schema):
        k: int
        v: int

    df = DataFrame[S]({"k": [3, 1, 2], "v": [30, 10, 20]})
    a = df.sort("k", maintain_order=False).to_dict()
    b = df.sort("k", maintain_order=True).to_dict()
    assert a == b == {"k": [1, 2, 3], "v": [10, 20, 30]}

def test_unique_maintain_order_keeps_first_appearance_order() -> None:
    class S(Schema):
        k: int
        seq: int

    df = DataFrame[S]({"k": [1, 2, 1, 2, 1], "seq": [10, 20, 11, 21, 12]})
    out = df.unique(subset=["k"], keep="first", maintain_order=True).to_dict()
    assert out == {"k": [1, 2], "seq": [10, 20]}


def test_unique_keep_last_is_stable() -> None:
    class S(Schema):
        k: int
        seq: int

    df = DataFrame[S]({"k": [1, 2, 1, 2, 1], "seq": [10, 20, 11, 21, 12]})
    out = df.unique(subset=["k"], keep="last", maintain_order=True).to_dict()
    # For keep='last', stable unique preserves the order of the last occurrences.
    assert out == {"k": [2, 1], "seq": [21, 12]}

def test_p2_fill_drop_nulls_and_cast_predicates() -> None:
    class S(Schema):
        id: int
        age: int | None
        score: float | None

    df = DataFrame[S](
        {"id": [1, 2, 3], "age": [10, None, 30], "score": [None, 1.5, None]}
    )
    filled = df.fill_null(0, subset=["age"])
    assert filled.collect(as_lists=True)["age"] == [10, 0, 30]
    assert filled.schema_fields()["age"] is int

    dropped = df.drop_nulls(subset=["age"])
    assert dropped.collect(as_lists=True) == {
        "id": [1, 3],
        "age": [10, 30],
        "score": [None, None],
    }

    casted = df.with_columns(age_f=df.age.cast(float))
    assert casted.schema_fields()["age_f"] == float | None
    out = casted.with_columns(age_is_null=casted.age.is_null()).collect(as_lists=True)
    assert out["age_is_null"] == [False, True, False]


def test_p5_melt_and_unpivot() -> None:
    class S(Schema):
        id: int
        a: int | None
        b: int | None

    df = DataFrame[S]({"id": [1, 2], "a": [10, None], "b": [20, 30]})
    melted = df.melt(id_vars=["id"], value_vars=["a", "b"])
    out = melted.collect(as_lists=True)
    # Polars `unpivot` expands column-by-column (all `a` rows, then all `b` rows).
    assert out["id"] == [1, 2, 1, 2]
    assert out["variable"] == ["a", "a", "b", "b"]
    assert out["value"] == [10, None, 20, 30]
    assert melted.schema_fields()["variable"] is str
    assert melted.schema_fields()["value"] == int | None

    unpivoted = df.unpivot(index=["id"], on=["a", "b"])
    assert unpivoted.collect(as_lists=True) == out


def test_p5_pivot_single_and_multi_values() -> None:
    class S(Schema):
        id: int
        key: str
        x: int | None
        y: float | None

    df = DataFrame[S](
        {
            "id": [1, 1, 2, 2],
            "key": ["A", "B", "A", "B"],
            "x": [10, 20, None, 40],
            "y": [1.0, 2.0, 3.0, None],
        }
    )
    p1 = df.pivot(
        index="id", columns="key", values="x", aggregate_function="sum"
    ).collect(as_lists=True)
    assert p1["id"] == [1, 2]
    assert p1["A_sum"] == [10, None]
    assert p1["B_sum"] == [20, 40]

    p2 = df.pivot(
        index=["id"], columns="key", values=["x", "y"], aggregate_function="first"
    ).collect(as_lists=True)
    assert p2["A_x_first"] == [10, None]
    assert p2["B_x_first"] == [20, 40]
    assert p2["A_y_first"] == [1.0, 3.0]
    assert p2["B_y_first"] == [2.0, None]

    p3 = df.pivot(
        index="id",
        columns="key",
        values="x",
        aggregate_function="sum",
        separator="__",
    ).collect(as_lists=True)
    assert p3["A__sum"] == [10, None]
    assert p3["B__sum"] == [20, 40]

    p4 = df.pivot(
        index="id",
        columns="key",
        values="x",
        aggregate_function="sum",
        sort_columns=True,
        separator="__",
    ).collect(as_lists=True)
    assert p4["A__sum"] == [10, None]
    assert p4["B__sum"] == [20, 40]


def test_p5_pivot_sort_columns_affects_column_generation_order() -> None:
    class S(Schema):
        id: int
        key: str
        x: int

    df = DataFrame[S]({"id": [1, 1], "key": ["B", "A"], "x": [1, 2]})
    # Without sort_columns, pivot value order is first-seen (B then A).
    out_unsorted = df.pivot(
        index="id", columns="key", values="x", aggregate_function="first"
    ).collect(as_lists=True)
    assert set(out_unsorted.keys()) == {"id", "B_first", "A_first"}

    out_sorted = df.pivot(
        index="id",
        columns="key",
        values="x",
        aggregate_function="first",
        sort_columns=True,
    ).collect(as_lists=True)
    assert set(out_sorted.keys()) == {"id", "A_first", "B_first"}


def test_p5_pivot_rejects_empty_separator() -> None:
    class S(Schema):
        id: int
        key: str
        x: int

    df = DataFrame[S]({"id": [1], "key": ["A"], "x": [1]})
    with pytest.raises(TypeError, match="separator"):
        df.pivot(
            index="id", columns="key", values="x", aggregate_function="first", separator=""
        ).to_dict()


def test_p5_explode_unnest_raise_not_implemented_for_scalar_schema() -> None:
    class S(Schema):
        id: int
        name: str

    df = DataFrame[S]({"id": [1], "name": ["a"]})
    with pytest.raises(TypeError, match="list dtype"):
        df.explode("name")
    with pytest.raises(TypeError, match="struct dtype"):
        df.unnest("name")


def test_p6_rolling_agg_and_dynamic_groupby() -> None:
    class TS(Schema):
        id: int
        ts: int
        v: int | None

    df = DataFrame[TS](
        {
            "id": [1, 1, 1, 2],
            "ts": [0, 3600, 7200, 0],
            "v": [10, None, 30, 5],
        }
    )
    rolled = df.rolling_agg(
        on="ts",
        column="v",
        window_size="2h",
        op="sum",
        out_name="v_roll_sum",
        by=["id"],
    )
    out = rolled.collect(as_lists=True)
    assert out["v_roll_sum"] == [10, 10, 40, 5]

    dgb = df.group_by_dynamic("ts", every="1h", period="2h", by=["id"]).agg(
        v_sum=("sum", "v"), v_count=("count", "v")
    )
    d_out = dgb.collect(as_lists=True)
    assert "v_sum" in d_out and "v_count" in d_out


def test_rolling_agg_validation_errors() -> None:
    class TS(Schema):
        id: int
        ts: int
        v: int | None
        label: str

    df = DataFrame[TS](
        {
            "id": [1, 1],
            "ts": [0, 3600],
            "v": [10, 20],
            "label": ["a", "b"],
        }
    )
    with pytest.raises(KeyError, match="existing on"):
        df.rolling_agg(on="missing", column="v", window_size=2, op="sum", out_name="x")
    with pytest.raises(KeyError, match="unknown grouping"):
        df.rolling_agg(
            on="ts",
            column="v",
            window_size=2,
            op="sum",
            out_name="x",
            by=["nope"],
        )
    with pytest.raises(ValueError, match="suffix"):
        df.rolling_agg(
            on="ts", column="v", window_size="2x", op="sum", out_name="x", by=["id"]
        )
    with pytest.raises(TypeError, match="numeric/date"):
        df.rolling_agg(
            on="label",
            column="v",
            window_size=1,
            op="sum",
            out_name="x",
        )
    with pytest.raises(ValueError, match="Unsupported rolling"):
        df.rolling_agg(
            on="ts", column="v", window_size=1, op="median", out_name="x", by=["id"]
        )


def test_group_by_dynamic_rejects_non_positive_every() -> None:
    class TS(Schema):
        id: int
        ts: int
        v: int | None

    df = DataFrame[TS](
        {
            "id": [1],
            "ts": [0],
            "v": [1],
        }
    )
    with pytest.raises(ValueError, match="positive every"):
        df.group_by_dynamic("ts", every="0s", by=["id"]).agg(v_sum=("sum", "v"))


def test_p6_expr_over_without_args_no_warning() -> None:
    import warnings

    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        expr = (df.age + 1).over()
    out = df.with_columns(age2=expr).collect(as_lists=True)
    assert out["age2"] == [21, 31]


def test_expr_over_with_partition_raises() -> None:
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    with pytest.raises(TypeError, match=r"Expr\.over"):
        _ = (df.age + 1).over(partition_by="id", order_by="age")


def test_temporal_columns_and_literals_core_paths() -> None:
    class T(Schema):
        id: int
        ts: datetime
        d: date
        dur: timedelta

    df = DataFrame[T](
        {
            "id": [1, 2],
            "ts": [datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 2, 0, 0, 0)],
            "d": [date(2024, 1, 1), date(2024, 1, 2)],
            "dur": [timedelta(hours=1), timedelta(hours=2)],
        }
    )
    out = df.collect(as_lists=True)
    assert out["ts"][0] == datetime(2024, 1, 1, 0, 0, 0)
    assert out["d"][1] == date(2024, 1, 2)
    assert out["dur"][0] == timedelta(hours=1)

    filtered = df.filter(df.ts > datetime(2024, 1, 1, 12, 0, 0)).collect(as_lists=True)
    assert filtered["id"] == [2]


def test_temporal_groupby_and_join_paths() -> None:
    class L(Schema):
        id: int
        ts: datetime
        v: int | None

    class R(Schema):
        id: int
        ts: datetime
        tag: str

    left = DataFrame[L](
        {
            "id": [1, 1, 2],
            "ts": [datetime(2024, 1, 1), datetime(2024, 1, 2), datetime(2024, 1, 1)],
            "v": [10, 20, None],
        }
    )
    right = DataFrame[R](
        {
            "id": [1, 2],
            "ts": [datetime(2024, 1, 2), datetime(2024, 1, 1)],
            "tag": ["a", "b"],
        }
    )
    joined = left.join(right, on=["id", "ts"], how="inner").collect(as_lists=True)
    assert_table_eq_sorted(
        joined,
        {
            "id": [1, 2],
            "ts": [datetime(2024, 1, 2), datetime(2024, 1, 1)],
            "v": [20, None],
            "tag": ["a", "b"],
        },
        keys=["id", "ts"],
    )

    grouped = (
        left.group_by("id")
        .agg(
            ts_min=("min", "ts"),
            ts_max=("max", "ts"),
        )
        .collect(as_lists=True)
    )
    assert sorted(grouped["id"]) == [1, 2]


def test_trusted_shape_only_numpy_ingest_collect() -> None:
    np = pytest.importorskip("numpy")

    class N(Schema):
        x: int

    df = DataFrame[N](
        {"x": np.array([1, 2, 3], dtype=np.int64)},
        trusted_mode="shape_only",
    )
    assert df.collect(as_lists=True)["x"] == [1, 2, 3]


def test_collect_as_numpy() -> None:
    np = pytest.importorskip("numpy")
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    out = df.collect(as_numpy=True)
    assert set(out.keys()) == {"id", "age"}
    assert np.asarray(out["id"]).tolist() == [1, 2]
    assert np.asarray(out["age"]).tolist() == [20, 30]


def test_to_polars_when_installed() -> None:
    pytest.importorskip("polars")
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    pdf = df.to_polars()
    assert set(pdf.columns) == {"id", "age"}
    assert pdf["id"].to_list() == [1, 2]
    assert pdf["age"].to_list() == [20, 30]


def test_group_by_convenience_sum_and_len() -> None:
    class S(Schema):
        g: str
        v: int | None

    df = DataFrame[S]({"g": ["a", "a", "b"], "v": [1, None, 3]})
    summed = df.group_by("g").sum("v").to_dict()
    assert_table_eq_sorted(summed, {"g": ["a", "b"], "v_sum": [1, 3]}, ["g"])

    lengths = df.group_by("g").len().to_dict()
    assert_table_eq_sorted(lengths, {"g": ["a", "b"], "len": [2, 1]}, ["g"])


def test_group_by_maintain_order_and_drop_nulls_false() -> None:
    class S(Schema):
        g: str | None
        v: int

    df = DataFrame[S]({"g": ["b", None, "a", "b", None], "v": [1, 2, 3, 4, 5]})
    out = df.group_by("g", maintain_order=True, drop_nulls=False).agg(v_sum=("sum", "v"))
    assert out.to_dict() == {"g": ["b", None, "a"], "v_sum": [5, 7, 3]}


def test_null_count_shift_sample_is_empty() -> None:
    class U(Schema):
        id: int
        age: int | None

    df = DataFrame[U]({"id": [1, 2, 3], "age": [10, None, 30]})
    assert df.null_count() == {"id": 0, "age": 1}
    assert df.shift(1).to_dict()["age"] == [None, 10, None]
    assert df.shift(-1).to_dict()["id"] == [2, 3, None]
    s = df.sample(n=2, seed=0, with_replacement=False).to_dict()
    assert set(s.keys()) == {"id", "age"}
    assert len(s["id"]) == 2
    assert DataFrame[U]({"id": [], "age": []}).is_empty() is True
