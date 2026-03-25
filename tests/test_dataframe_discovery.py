"""Tests for columns, shape, info, describe on core DataFrame."""

from __future__ import annotations

import io
from contextlib import redirect_stdout
from datetime import date

from pydantable import DataFrame, DataFrameModel
from pydantic import BaseModel


class _T(BaseModel):
    x: int
    y: float


class _StrOnly(BaseModel):
    name: str
    flag: bool


class _DateOnly(BaseModel):
    when: date


class _OptNums(BaseModel):
    a: int | None
    b: float | None


def test_columns_shape_dtypes() -> None:
    df = DataFrame[_T]({"x": [1, 2], "y": [1.0, 2.0]})
    assert df.columns == ["x", "y"]
    assert df.shape == (2, 2)
    assert not df.empty
    assert "x" in df.dtypes
    assert list(df.dtypes.keys()) == df.columns


def test_empty_frame_shape_and_empty() -> None:
    class E(BaseModel):
        x: int

    df = DataFrame[E]({"x": []})
    assert df.shape == (0, 1)
    assert df.empty
    assert df.columns == ["x"]


def test_shape_root_unchanged_after_filter() -> None:
    """Root buffer row count stays in ``shape``; materialized rows differ."""

    class S(BaseModel):
        x: int

    df = DataFrame[S]({"x": [1, 2, 3]})
    filtered = df.filter(df.x > 2)
    assert filtered.shape == (3, 1)
    assert filtered.to_dict() == {"x": [3]}


def test_describe_materializes_filtered_plan() -> None:
    """describe() uses to_dict(); stats match executed rows, not root length."""
    df = DataFrame[_T]({"x": [1, 2, 10], "y": [1.0, 2.0, 10.0]})
    f = df.filter(df.x > 2)
    d = f.describe()
    assert "count=1" in d
    assert "10" in d


def test_info_contains_shape_line() -> None:
    df = DataFrame[_T]({"x": [1], "y": [2.0]})
    s = df.info()
    assert "shape (root buffer)" in s
    assert "dtypes:" in s
    assert "lazy transforms" in s.lower() or "filter" in s
    assert "x" in s and "y" in s


def test_describe_numeric() -> None:
    df = DataFrame[_T]({"x": [1, 2, 3], "y": [1.0, 2.0, 3.0]})
    d = df.describe()
    assert "x:" in d and "mean=" in d
    assert "std=" in d  # >=2 rows per column


def test_describe_no_supported_columns() -> None:
    df = DataFrame[_DateOnly]({"when": [date(2020, 1, 1), date(2020, 1, 2)]})
    assert df.describe() == "describe(): no int/float/bool/str columns in schema."


def test_describe_bool_and_str_columns() -> None:
    df = DataFrame[_StrOnly]({"name": ["a", "bb"], "flag": [True, False]})
    d = df.describe()
    assert "true=" in d and "false=" in d
    assert "n_unique=" in d and "min_len=" in d


def test_describe_numeric_exact_values() -> None:
    df = DataFrame[_T]({"x": [10, 20], "y": [0.0, 0.0]})
    d = df.describe()
    assert "x:" in d
    assert "count=2" in d
    assert "min=10" in d and "max=20" in d
    assert "15" in d  # mean of 10 and 20


def test_describe_single_row_float_no_std() -> None:
    df = DataFrame[_T]({"x": [1], "y": [2.5]})
    d = df.describe()
    assert "std=" not in d
    assert "count=1" in d


def test_describe_optional_int_skips_nulls_in_stats() -> None:
    df = DataFrame[_OptNums]({"a": [None, 2, 3], "b": [1.0, None, 3.0]})
    d = df.describe()
    assert "a:" in d and "count=2" in d
    assert "b:" in d and "count=2" in d


def test_describe_all_null_numeric_column() -> None:
    df = DataFrame[_OptNums]({"a": [None, None], "b": [None, None]})
    d = df.describe()
    assert "count=0 (all null)" in d


def test_dataframe_model_delegates() -> None:
    class M(DataFrameModel):
        x: int

    m = M({"x": [1, 2]})
    assert m.columns == ["x"]
    assert m.shape[0] == 2
    assert not m.empty
    assert "x" in m.dtypes
    assert "schema" in m.info().lower() or "Schema" in m.info()
    assert "count=2" in m.describe()


def test_pandas_dataframe_inherits_discovery() -> None:
    from pydantable.pandas import DataFrame as PDF

    class P(BaseModel):
        n: int

    df = PDF[P]({"n": [1, 2, 3]})
    assert df.columns == ["n"]
    assert df.shape == (3, 1)
    assert "n:" in df.describe()


def test_pyspark_show_and_summary() -> None:
    from pydantable.pyspark import DataFrame as PSDataFrame

    class R(BaseModel):
        a: int

    df = PSDataFrame[R]({"a": [1, 2, 3]})
    s = df.summary()
    assert "a:" in s and "count=3" in s
    assert s == df.describe()


def test_pyspark_show_prints_table(capsys) -> None:
    from pydantable.pyspark import DataFrame as PSDataFrame

    class R(BaseModel):
        a: int

    df = PSDataFrame[R]({"a": [1, 2]})
    df.show(n=2, truncate=False)
    out = capsys.readouterr().out
    assert "a" in out
    assert "|" in out or "1" in out


def test_pyspark_show_vertical(capsys) -> None:
    from pydantable.pyspark import DataFrame as PSDataFrame

    class R(BaseModel):
        a: int

    df = PSDataFrame[R]({"a": [42]})
    df.show(n=1, vertical=True)
    out = capsys.readouterr().out
    assert "record 0" in out
    assert "a:" in out


def test_pyspark_show_empty() -> None:
    from pydantable.pyspark import DataFrame as PSDataFrame

    class R(BaseModel):
        a: int

    df = PSDataFrame[R]({"a": []})
    buf = io.StringIO()
    with redirect_stdout(buf):
        df.show()
    out = buf.getvalue()
    # Header is still printed; zero data rows.
    assert "a" in out and "-" in out
    assert out.count("\n") >= 2


def test_pyspark_dataframe_model_show_and_summary() -> None:
    from pydantable.pyspark import DataFrameModel as PSModel

    class M(PSModel):
        k: int

    m = M({"k": [5, 6]})
    assert "count=2" in m.summary()
    buf = io.StringIO()
    with redirect_stdout(buf):
        m.show(n=10)
    assert "k" in buf.getvalue()
