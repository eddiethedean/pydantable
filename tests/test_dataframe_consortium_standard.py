from __future__ import annotations

import pytest


def test_dataframe_consortium_standard_entrypoint() -> None:
    dataframe_api_compat = pytest.importorskip("dataframe_api_compat")
    pd = pytest.importorskip("pandas")

    from pydantable import DataFrameModel

    class SmallDF(DataFrameModel):
        id: int
        name: str

    df = SmallDF({"id": [1, 2], "name": ["a", "b"]})

    std = df.__dataframe_consortium_standard__(api_version="2023.11-beta")

    # Basic sanity: wrapper can round-trip to a concrete dataframe.
    pdf = std.dataframe
    assert isinstance(pdf, pd.DataFrame)
    assert set(pdf.columns) == {"id", "name"}
    assert pdf["id"].tolist() == [1, 2]
    assert pdf["name"].tolist() == ["a", "b"]

    # And the wrapper exposes the standard's namespace hook.
    ns = std.__dataframe_namespace__()
    assert getattr(ns, "__dataframe_api_version__", None) == "2023.11-beta"
