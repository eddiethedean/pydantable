from __future__ import annotations

import sys

import pytest
from pydantable import DataFrame, DataFrameModel
from pydantic import BaseModel


def test_dataframe_to_arrow_missing_pyarrow_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Row(BaseModel):
        x: int

    df = DataFrame[Row]({"x": [1]})

    monkeypatch.setitem(sys.modules, "pyarrow", None)
    with pytest.raises(ImportError, match="pyarrow is required for to_arrow\\(\\)"):
        df.to_arrow()


def test_dataframe_dunder_dataframe_missing_pyarrow_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Row(BaseModel):
        x: int

    df = DataFrame[Row]({"x": [1]})

    monkeypatch.setitem(sys.modules, "pyarrow", None)
    with pytest.raises(ImportError, match="pyarrow is required for to_arrow\\(\\)"):
        df.__dataframe__()


def test_dataframe_consortium_standard_missing_dataframe_api_compat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Row(BaseModel):
        x: int

    df = DataFrame[Row]({"x": [1]})

    monkeypatch.setitem(sys.modules, "dataframe_api_compat", None)
    with pytest.raises(ImportError, match="dataframe-api-compat is required"):
        df.__dataframe_consortium_standard__()


def test_dataframe_model_consortium_standard_delegates() -> None:
    pytest.importorskip("dataframe_api_compat")

    class SmallDF(DataFrameModel):
        x: int

    df = SmallDF({"x": [1]})
    std = df.__dataframe_consortium_standard__(api_version="2023.11-beta")
    assert hasattr(std, "dataframe")
