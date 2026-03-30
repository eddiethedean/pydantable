"""Extra pandas UI tests: duplicates, dummies, binning, factorize, ewm, pivot."""

from __future__ import annotations

import pytest
from pydantable import Schema
from pydantable.pandas import DataFrameModel as PandasDataFrameModel


def test_pandas_ui_duplicated_subset_multicol_matches_pandas() -> None:
    pd = pytest.importorskip("pandas")
    from pydantable.pandas import DataFrame

    class Row(Schema):
        a: int
        b: int
        c: str

    payload = {"a": [1, 1, 2], "b": [10, 10, 20], "c": ["x", "y", "x"]}
    pdf = pd.DataFrame(payload)
    df = DataFrame[Row](payload)

    for keep in ("first", "last"):
        exp = list(pdf.duplicated(subset=["a", "b"], keep=keep))
        got = df.duplicated(subset=["a", "b"], keep=keep).collect(as_lists=True)[
            "duplicated"
        ]
        assert got == exp

    exp_false = list(pdf.duplicated(subset=["a", "b"], keep=False))
    assert (
        df.duplicated(subset=["a", "b"], keep=False).collect(as_lists=True)[
            "duplicated"
        ]
        == exp_false
    )


def test_pandas_ui_drop_duplicates_false_with_subset_matches_pandas() -> None:
    pd = pytest.importorskip("pandas")
    from pydantable.pandas import DataFrame

    class Row(Schema):
        k: int
        v: str

    payload = {"k": [1, 1, 2, 3], "v": ["a", "b", "c", "d"]}
    pdf = pd.DataFrame(payload)
    df = DataFrame[Row](payload)

    sub_pdf = (
        pdf.drop_duplicates(subset=["k"], keep=False)
        .sort_values("k")
        .reset_index(drop=True)
    )
    sub = (
        df.drop_duplicates(subset=["k"], keep=False)
        .sort_values("k")
        .collect(as_lists=True)
    )
    assert sub["k"] == list(sub_pdf["k"])
    assert sub["v"] == list(sub_pdf["v"])


def test_pandas_ui_duplicated_validation_errors() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        a: int

    df = DataFrame[Row]({"a": [1, 1]})
    with pytest.raises(ValueError, match="keep=True"):
        df.duplicated(keep=True)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="first"):
        df.duplicated(keep="maybe")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="non-empty"):
        df.duplicated(subset=[])  # type: ignore[arg-type]


def test_pandas_ui_drop_duplicates_validation() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        a: int

    df = DataFrame[Row]({"a": [1]})
    with pytest.raises(ValueError, match="keep"):
        df.drop_duplicates(keep="all")  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError):
        df.drop_duplicates(inplace=True)  # type: ignore[arg-type]


def test_pandas_ui_get_dummies_prefix_sep_drop_first_and_dummy_na() -> None:
    pd = pytest.importorskip("pandas")
    from pydantable.pandas import DataFrame

    class Row(Schema):
        id: int
        tag: str | None

    df = DataFrame[Row]({"id": [1, 2, 3, 4], "tag": ["y", None, "x", "y"]})

    out = df.get_dummies(
        ["tag"],
        prefix="t",
        prefix_sep=".",
        dummy_na=True,
        dtype="int",
    ).collect(as_lists=True)
    assert out["id"] == [1, 2, 3, 4]
    assert out["t.nan"] == [0, 1, 0, 0]
    assert out["t.x"] == [0, 0, 1, 0]
    assert out["t.y"] == [1, 0, 0, 1]

    d_first = df.get_dummies(["tag"], drop_first=True, dtype="int").collect(
        as_lists=True
    )
    # Sorted distinct None, x, y; with default dummy_na, None is not a dummy level.
    df_pd = pd.DataFrame({"id": [1, 2, 3, 4], "tag": ["y", None, "x", "y"]})
    exp = pd.get_dummies(df_pd, columns=["tag"], drop_first=True, dtype=int)
    assert set(d_first.keys()) == set(exp.columns)
    for c in exp.columns:
        assert d_first[c] == exp[c].tolist()

    d_bool = df.get_dummies(["tag"], dtype="bool").collect(as_lists=True)
    # Null tags yield null (not False) in dummy columns (three-valued logic).
    assert d_bool["tag_x"] == [False, None, True, False]


def test_pandas_ui_get_dummies_two_columns_and_mapping_prefix() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        u: str
        v: str

    df = DataFrame[Row]({"u": ["a", "b"], "v": ["c", "d"]})
    out = df.get_dummies(["u", "v"], prefix={"u": "U", "v": "V"}).collect(as_lists=True)
    assert out["U_a"] == [True, False]
    assert out["U_b"] == [False, True]
    assert out["V_c"] == [True, False]
    assert out["V_d"] == [False, True]


def test_pandas_ui_get_dummies_errors() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        a: int
        region: str
        region_extra: str

    df = DataFrame[Row](
        {
            "a": [1],
            "region": ["extra"],
            "region_extra": ["collision"],
        }
    )
    with pytest.raises(ValueError, match="collides"):
        df.get_dummies(["region"])

    df2 = DataFrame[Row](
        {"a": [1, 2], "region": ["x", "y"], "region_extra": ["p", "q"]}
    )
    with pytest.raises(ValueError, match="max_categories"):
        df2.get_dummies(["region"], max_categories=1)

    with pytest.raises(KeyError):
        df2.get_dummies(["nope"])  # type: ignore[list-item]

    with pytest.raises(TypeError, match="non-empty"):
        df2.get_dummies([])  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="dtype"):
        df2.get_dummies(["region"], dtype="float")  # type: ignore[arg-type]


def test_pandas_ui_factorize_nulls_and_keyerror() -> None:
    pd = pytest.importorskip("pandas")
    from pydantable.pandas import DataFrame

    class Row(Schema):
        s: str | None

    df = DataFrame[Row]({"s": ["a", None, "b", None]})
    codes, cats = df.factorize_column("s")
    exp_c, exp_u = pd.factorize(pd.Series(["a", None, "b", None]), use_na_sentinel=True)
    assert codes == list(exp_c)
    assert cats == list(exp_u)

    with pytest.raises(KeyError):
        df.factorize_column("missing")


def test_pandas_ui_cut_qcut_interval_strings_match_pandas() -> None:
    pd = pytest.importorskip("pandas")
    from pydantable.pandas import DataFrame

    class Row(Schema):
        v: float

    df = DataFrame[Row]({"v": [0.5, 1.5, 2.5, float("nan")]})
    ser = pd.Series([0.5, 1.5, 2.5, float("nan")])
    cats = pd.cut(ser, bins=[0.0, 1.0, 2.0, 3.0])
    cut = df.cut("v", bins=[0.0, 1.0, 2.0, 3.0], new_column="bins").collect(
        as_lists=True
    )
    assert cut["bins"] == [None if pd.isna(x) else str(x) for x in cats]

    df2 = DataFrame[Row]({"v": [1.0, 2.0, 3.0, 4.0]})
    ser2 = pd.Series([1.0, 2.0, 3.0, 4.0])
    qc = pd.qcut(ser2, q=2, duplicates="drop")
    qout = df2.qcut("v", q=2, new_column="q").collect(as_lists=True)["q"]
    assert qout == [None if pd.isna(x) else str(x) for x in qc]

    with pytest.raises(KeyError):
        df.cut("nope", bins=[0, 1])  # type: ignore[arg-type]


def test_pandas_ui_ewm_com_alpha_and_validation() -> None:
    pd = pytest.importorskip("pandas")
    np = pytest.importorskip("numpy")
    from pydantable.pandas import DataFrame

    class Row(Schema):
        v: float

    vals = [10.0, 2.0, 14.0, 7.0]
    df = DataFrame[Row]({"v": vals})
    s = pd.Series(vals)

    for com in (1.0, 3.5):
        out = df.ewm(com=com).mean("v", out_name="m").collect(as_lists=True)["m"]
        assert np.allclose(out, s.ewm(com=com).mean(), equal_nan=True)

    out = df.ewm(alpha=0.3, adjust=False).mean("v").collect(as_lists=True)["v_ewm_mean"]
    assert np.allclose(out, s.ewm(alpha=0.3, adjust=False).mean(), equal_nan=True)

    with pytest.raises(TypeError, match="exactly one"):
        df.ewm()  # type: ignore[call-arg]
    with pytest.raises(TypeError, match="exactly one"):
        df.ewm(com=1.0, span=2.0)  # type: ignore[call-arg]

    with pytest.raises(KeyError):
        df.ewm(span=2).mean("nope")  # type: ignore[arg-type]


def test_pandas_ui_pivot_wide_shape_and_values() -> None:
    from pydantable.pandas import DataFrame

    class P(Schema):
        i: int
        k: str
        val: int

    psrc = DataFrame[P]({"i": [1, 1, 2], "k": ["A", "B", "A"], "val": [10, 20, 30]})
    pv = psrc.pivot(index="i", columns="k", values="val", aggregate_function="first")
    wide = pv.collect(as_lists=True)
    assert set(wide.keys()) == {"i", "A_first", "B_first"}
    # Row i=1: A=10, B=20; row i=2: A=30, B missing
    by_i = {
        wide["i"][r]: (wide["A_first"][r], wide["B_first"][r])
        for r in range(len(wide["i"]))
    }
    assert by_i[1] == (10, 20)
    assert by_i[2][0] == 30
    assert by_i[2][1] is None


def test_pandas_ui_model_wraps_new_helpers() -> None:
    pd = pytest.importorskip("pandas")
    np = pytest.importorskip("numpy")

    class Row(PandasDataFrameModel):
        id: int
        tag: str

    m = Row({"id": [1, 2], "tag": ["a", "b"]})
    dup = m.duplicated().collect(as_lists=True)["duplicated"]
    assert dup == [False, False]

    dummies = m.get_dummies(["tag"], dtype="int").collect(as_lists=True)
    assert "id" in dummies and "tag_a" in dummies

    ewm = m.ewm(span=2).mean("id").collect(as_lists=True)
    assert np.allclose(
        ewm["id_ewm_mean"],
        pd.Series([1.0, 2.0]).ewm(span=2).mean(),
        equal_nan=True,
    )
