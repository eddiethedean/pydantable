from __future__ import annotations

import pytest
from pydantable import Schema


def test_pandas_ui_concat_validation_errors() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        a: int

    df = DataFrame[Row]({"a": [1]})

    with pytest.raises(NotImplementedError, match="join="):
        DataFrame.concat([df, df], join="inner")  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="ignore_index=True"):
        DataFrame.concat([df, df], ignore_index=True)  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="keys/levels/names"):
        DataFrame.concat([df, df], keys=[1, 2])  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="verify_integrity"):
        DataFrame.concat([df, df], verify_integrity=True)  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="sort="):
        DataFrame.concat([df, df], sort=True)  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="copy="):
        DataFrame.concat([df, df], copy=True)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="axis"):
        DataFrame.concat([df, df], axis=2)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="how="):
        DataFrame.concat([df, df], how="diagonal")  # type: ignore[arg-type]


def test_pandas_ui_assign_rejects_pandas_series_and_callable_value() -> None:
    pd = pytest.importorskip("pandas")
    from pydantable.pandas import DataFrame

    class Row(Schema):
        a: int

    df = DataFrame[Row]({"a": [1, 2]})

    with pytest.raises(TypeError, match="pandas Series"):
        df.assign(b=pd.Series([1, 2]))  # type: ignore[arg-type]

    out = df.assign(b=lambda d: d.col("a") * 2).collect(as_lists=True)
    assert out == {"a": [1, 2], "b": [2, 4]}


def test_pandas_ui_merge_suffixes_validation_and_index_key_limits() -> None:
    from pydantable.pandas import DataFrame

    class L(Schema):
        k: int
        v: int

    class R(Schema):
        k: int
        w: int

    left = DataFrame[L]({"k": [1, 2], "v": [10, 20]})
    right = DataFrame[R]({"k": [1, 2], "w": [100, 200]})

    with pytest.raises(TypeError, match="suffixes"):
        left.merge(right, on="k", suffixes="_x")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="length 2"):
        left.merge(right, on="k", suffixes=("_x",))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="tuple\\[str, str\\]"):
        left.merge(right, on="k", suffixes=("_x", 1))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="cannot be"):
        left.merge(right, on="k", suffixes=("", ""))

    with pytest.raises(NotImplementedError, match="left_index/right_index=True"):
        left.merge(right, left_index=True, right_index=True, on="k")  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="vice versa"):
        left.merge(right, left_index=True, right_index=False)  # type: ignore[arg-type]


def test_pandas_ui_sort_values_validations_and_key_behaviors() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        s: str
        x: int

    df = DataFrame[Row]({"s": [" b", "a ", "c"], "x": [2, 1, 3]})

    with pytest.raises(NotImplementedError, match="kind="):
        df.sort_values("x", kind="mergesort")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="na_position"):
        df.sort_values("x", na_position="middle")  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="ignore_index=True"):
        df.sort_values("x", ignore_index=True)  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="Python callables"):
        df.sort_values("x", key=lambda s: s)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="at least one"):
        df.sort_values([])  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="ascending must match"):
        df.sort_values(["x", "s"], ascending=[True])  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="not supported"):
        df.sort_values("x", key="unknown")  # type: ignore[arg-type]

    # Key transforms should execute via temporary columns then drop them.
    out = df.sort_values("s", key="strip").collect(as_lists=True)
    assert out["s"] == ["a ", " b", "c"]


def test_pandas_ui_drop_index_errors_and_ignore_mode() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        a: int

    df = DataFrame[Row]({"a": [10, 20, 30]})

    with pytest.raises(IndexError, match="out of range"):
        df.drop(index=[-1, 3])

    out = df.drop(index=[-1, 3], errors="ignore").collect(as_lists=True)
    assert out == {"a": [10, 20, 30]}

    out2 = df.drop(index=[0, 2], errors="ignore").collect(as_lists=True)
    assert out2 == {"a": [20]}


def test_pandas_ui_query_validation_and_error_messages() -> None:
    from pydantable.pandas import DataFrame

    class Row(Schema):
        a: int
        b: int

    df = DataFrame[Row]({"a": [1, 2], "b": [10, 20]})

    with pytest.raises(ValueError, match="invalid expression"):
        df.query("a >")  # type: ignore[arg-type]

    with pytest.raises(NotImplementedError, match="unknown name"):
        df.query("missing > 1")  # type: ignore[arg-type]

    # between() bounds that reference a column should be rejected.
    with pytest.raises(NotImplementedError, match="between\\(\\) bounds"):
        df.query("between(a, b, 999)")  # type: ignore[arg-type]


def test_pandas_ui_to_pandas_basic_roundtrip_shape() -> None:
    pd = pytest.importorskip("pandas")
    from pydantable.pandas import DataFrame

    class Row(Schema):
        a: int
        s: str | None

    df = DataFrame[Row]({"a": [1, 2], "s": ["x", None]})
    pdf = df.to_pandas()
    assert isinstance(pdf, pd.DataFrame)
    assert list(pdf.columns) == ["a", "s"]
    assert pdf["a"].tolist() == [1, 2]
