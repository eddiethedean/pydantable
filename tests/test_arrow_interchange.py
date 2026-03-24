"""Arrow interchange: read/write helpers, to_arrow, Table and RecordBatch inputs."""

from __future__ import annotations

from io import BytesIO

import pytest

pytest.importorskip("pyarrow")

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
from pydantable import DataFrame, DataFrameModel, read_ipc, read_parquet
from pydantable.io import record_batch_to_column_dict
from pydantic import BaseModel


class SmallDF(DataFrameModel):
    id: int
    name: str


def test_read_parquet_roundtrip(tmp_path) -> None:
    col = {"id": [1, 2], "name": ["a", "b"]}
    table = pa.Table.from_pydict(col)
    path = tmp_path / "t.parquet"
    pq.write_table(table, path)
    out = read_parquet(path)
    assert out == col
    df = SmallDF(out)
    assert df.to_dict() == col


def test_read_ipc_file_format_roundtrip(tmp_path) -> None:
    col = {"id": [1], "name": ["x"]}
    table = pa.Table.from_pydict(col)
    path = tmp_path / "t.arrow"
    with ipc.new_file(path, table.schema) as writer:
        writer.write_table(table)
    out = read_ipc(path, as_stream=False)
    assert out == col


def test_read_ipc_stream_format_bytes() -> None:
    col = {"id": [3, 4], "name": ["p", "q"]}
    table = pa.Table.from_pydict(col)
    buf = BytesIO()
    with ipc.new_stream(buf, table.schema) as writer:
        writer.write_table(table)
    raw = buf.getvalue()
    out = read_ipc(raw, as_stream=True)
    assert out == col


def test_to_arrow_and_from_pydict_matches_to_dict() -> None:
    df = SmallDF({"id": [1, 2], "name": ["a", "b"]})
    col = df.to_dict()
    tbl = df.to_arrow()
    assert isinstance(tbl, pa.Table)
    # Column order follows :meth:`to_dict` (schema / engine ordering), not
    # constructor key order.
    assert tbl.column_names == list(col.keys())
    assert tbl.column("id").to_pylist() == [1, 2]
    assert tbl.column("name").to_pylist() == ["a", "b"]


def test_constructor_accepts_pa_table() -> None:
    table = pa.Table.from_pydict({"id": [9], "name": ["z"]})
    df = SmallDF(table)
    assert df.to_dict() == {"id": [9], "name": ["z"]}


def test_dataframe_generic_accepts_pa_table() -> None:
    """``DataFrame[Schema](pa.Table)`` goes through ``validate_columns_strict``."""

    class Row(BaseModel):
        id: int
        name: str

    table = pa.Table.from_pydict({"id": [9], "name": ["z"]})
    df = DataFrame[Row](table)
    assert df.to_dict() == {"id": [9], "name": ["z"]}


def test_constructor_accepts_record_batch() -> None:
    table = pa.Table.from_pydict({"id": [9], "name": ["z"]})
    batch = table.to_batches()[0]
    assert record_batch_to_column_dict(batch) == {"id": [9], "name": ["z"]}
    df = SmallDF(batch)
    assert df.to_dict() == {"id": [9], "name": ["z"]}


@pytest.mark.asyncio
async def test_ato_arrow() -> None:
    df = SmallDF({"id": [1], "name": ["a"]})
    tbl = await df.ato_arrow()
    assert isinstance(tbl, pa.Table)
    assert tbl.column("id").to_pylist() == [1]
