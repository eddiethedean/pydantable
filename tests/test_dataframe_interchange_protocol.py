from __future__ import annotations

import pytest

pytest.importorskip("pyarrow")


def test_interchange_protocol_consumed_by_pandas() -> None:
    """pandas can import objects implementing `__dataframe__`."""

    pd = pytest.importorskip("pandas")

    from pydantable import DataFrameModel

    class SmallDF(DataFrameModel):
        id: int
        name: str
        age: int | None

    df = SmallDF({"id": [1, 2], "name": ["a", "b"], "age": [10, None]})

    out = pd.api.interchange.from_dataframe(df)
    # Interchange importers are not required to preserve column order.
    assert set(out.columns) == {"id", "name", "age"}
    assert out["id"].tolist() == [1, 2]
    assert out["name"].tolist() == ["a", "b"]
    # pandas may upcast nullable integer columns to float + NaN.
    age = out["age"].tolist()
    assert age[0] == 10 or age[0] == 10.0
    assert pd.isna(age[1])


def test_interchange_protocol_nan_as_null_passthrough() -> None:
    """`nan_as_null` is forwarded (and may be rejected by PyArrow)."""

    pd = pytest.importorskip("pandas")

    from pydantable import DataFrameModel
    import pyarrow as pa

    class Floats(DataFrameModel):
        x: float | None

    df = Floats({"x": [1.0, float("nan"), None]})

    # PyArrow's interchange implementation currently rejects nan_as_null=True.
    with pytest.raises(RuntimeError, match="nan_as_null=True"):
        _ = df.__dataframe__(nan_as_null=True)

    # Default export should still be consumable.
    out = pd.api.interchange.from_dataframe(df)
    got = out["x"].tolist()
    assert got[0] == 1.0
    assert isinstance(got[1], float) and pa.compute.is_nan(pa.scalar(got[1])).as_py()
    assert pd.isna(got[2])


def test_interchange_protocol_consumed_by_polars_if_available() -> None:
    """Polars can import objects implementing `__dataframe__` (optional)."""

    pl = pytest.importorskip("polars")

    from pydantable import DataFrameModel

    class SmallDF(DataFrameModel):
        id: int
        name: str

    df = SmallDF({"id": [1, 2], "name": ["a", "b"]})

    out = pl.from_dataframe(df)
    assert set(out.columns) == {"id", "name"}
    assert out["id"].to_list() == [1, 2]
    assert out["name"].to_list() == ["a", "b"]
