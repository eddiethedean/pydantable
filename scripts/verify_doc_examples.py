"""Run doc snippets as tests (offline; requires built ``pydantable._core``).

Run from the repository root::

    PYTHONPATH=python .venv/bin/python scripts/verify_doc_examples.py

Fails on assertion errors or import failures so README and documentation snippets stay
in sync with the API.

Runnable scripts under ``docs/examples/`` are also executed by
``tests/io/test_docs_example_scripts.py`` (subprocess per file).
"""

# ruff: noqa: E402

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

PROJECT_ROOT = str(Path(__file__).resolve().parents[1])
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pydantable import DataFrameModel
from pydantable.pandas import DataFrameModel as PandasDataFrameModel
from pydantable.pyspark import DataFrameModel as PySparkDataFrameModel
from pydantable.pyspark.sql import functions as F
from pydantic import BaseModel

from scripts.doc_examples.cookbook import (
    run_fastapi_async_materialization,
    run_fastapi_columnar_bodies,
    run_transforms_join_groupby,
    run_windows_framing_primer,
)


def _sort_rows_by_country(out: dict[str, list]) -> dict[str, list]:
    """Stable row order for tests (group_by output order is not guaranteed)."""
    n = len(out["country"])
    order = sorted(range(n), key=lambda i: out["country"][i])
    return {k: [out[k][i] for i in order] for k in out}


# README Quick Start


class User(DataFrameModel):
    id: int
    age: int | None


df = User({"id": [1, 2], "age": [20, None]})
df2 = df.with_columns(age2=df.age * 2)
df3 = df2.select("id", "age2")
df4 = df3.filter(df3.age2 > 10)
assert df4.to_dict() == {"id": [1], "age2": [40]}

# Same pattern as docs index / README quick start


class UserReadme(DataFrameModel):
    id: int
    age: int


df = UserReadme({"id": [1, 2], "age": [20, 30]})
df2 = df.with_columns(age2=df.age * 2)
df3 = df2.select("id", "age2")
df4 = df3.filter(df3.age2 > 40)
assert df4.to_dict() == {"id": [2], "age2": [60]}

# docs/DATAFRAMEMODEL.md


class DFUser(DataFrameModel):
    id: int
    age: int


df1 = DFUser({"id": [1, 2], "age": [20, 30]})
assert df1.to_dict() == {"id": [1, 2], "age": [20, 30]}

df_row = DFUser([{"id": 1, "age": 20}, {"id": 2, "age": 30}])
assert df_row.to_dict() == {"age": [20, 30], "id": [1, 2]}

_RM = DFUser.row_model()
df_rm = DFUser([_RM(id=1, age=20), _RM(id=2, age=30)])
assert df_rm.to_dict() == {"age": [20, 30], "id": [1, 2]}

df_c = DFUser({"id": [1, 2], "age": [20, 40]})
df_c2 = df_c.with_columns(age2=df_c.age * 2)
assert df_c2.to_dict() == {"id": [1, 2], "age": [20, 40], "age2": [40, 80]}

df_q = DFUser({"id": [1, 2, 3], "age": [10, 50, 60]})
df_q2 = df_q.with_columns(age2=df_q.age * 2)
df_q3 = df_q2.select("id", "age2")
df_q4 = df_q3.filter(df_q3.age2 > 40)
assert df_q4.to_dict() == {"id": [2, 3], "age2": [100, 120]}

# docs/PANDAS_UI


class Sales(PandasDataFrameModel):
    region: str
    amount: int


df = Sales({"region": ["US", "EU"], "amount": [10, 20]})
df2 = df.assign(doubled=df.amount * 2)
assert df2.to_dict() == {
    "doubled": [20, 40],
    "region": ["US", "EU"],
    "amount": [10, 20],
}

# docs/PYSPARK_UI


class UserSparkUi(PySparkDataFrameModel):
    id: int
    name: str


df = UserSparkUi({"id": [1], "name": ["Ada"]})
out = df.withColumn("greeting", F.concat(F.col("name", dtype=str), F.lit("!")))
assert out.to_dict() == {"id": [1], "name": ["Ada"], "greeting": ["Ada!"]}

# docs/POLARS_WORKFLOWS


class Orders(DataFrameModel):
    order_id: int
    user_id: int
    amount: float | None


class Users(DataFrameModel):
    user_id: int
    country: str


orders = Orders(
    {
        "order_id": [1, 2, 3],
        "user_id": [10, 10, 20],
        "amount": [50.0, None, 20.0],
    }
)
users = Users({"user_id": [10, 20], "country": ["US", "CA"]})
out = (
    orders.join(users, on="user_id", how="left")
    .group_by("country")
    .agg(total=("sum", "amount"), n_orders=("count", "order_id"))
    .to_dict()
)
assert _sort_rows_by_country(out) == {
    "country": ["CA", "US"],
    "total": [20.0, 50.0],
    "n_orders": [1, 2],
}


class Metrics(DataFrameModel):
    id: int
    metric: str
    value: int | None


df = Metrics(
    {
        "id": [1, 1, 2, 2],
        "metric": ["A", "B", "A", "B"],
        "value": [10, 20, None, 40],
    }
)
wide = df.pivot(
    index="id", columns="metric", values="value", aggregate_function="first"
)
assert wide.to_dict() == {
    "id": [1, 2],
    "A_first": [10, None],
    "B_first": [20, 40],
}


class TS(DataFrameModel):
    id: int
    ts: int
    v: int | None


df = TS({"id": [1, 1, 1], "ts": [0, 3600, 7200], "v": [10, None, 30]})
rolled = df.rolling_agg(
    on="ts",
    column="v",
    window_size="2h",
    op="sum",
    out_name="v_roll",
    by=["id"],
)
assert rolled.to_dict() == {
    "v": [10, None, 30],
    "id": [1, 1, 1],
    "v_roll": [10, 10, 40],
    "ts": [0, 3600, 7200],
}
dynamic = df.group_by_dynamic("ts", every="1h", by=["id"]).agg(
    v_sum=("sum", "v"),
    v_count=("count", "v"),
)
assert dynamic.to_dict() == {
    "ts": [0, 3600, 7200],
    "id": [1, 1, 1],
    "v_sum": [10, None, 30],
    "v_count": [1, 0, 1],
}

# docs/PYSPARK_INTERFACE


class OrdersPySpark(PySparkDataFrameModel):
    order_id: int
    user_id: int
    amount: float | None


class UsersPySpark(PySparkDataFrameModel):
    user_id: int
    country: str


orders = OrdersPySpark(
    {
        "order_id": [1, 2, 3],
        "user_id": [10, 10, 20],
        "amount": [50.0, None, 20.0],
    }
)
users = UsersPySpark({"user_id": [10, 20], "country": ["US", "CA"]})
result = (
    orders.join(users, on="user_id", how="left")
    .fill_null(0, subset=["amount"])
    .group_by("country")
    .agg(total=("sum", "amount"), n_orders=("count", "order_id"))
    .to_dict()
)
assert _sort_rows_by_country(result) == {
    "country": ["CA", "US"],
    "total": [20.0, 50.0],
    "n_orders": [1, 2],
}


class UserPySparkSelect(PySparkDataFrameModel):
    id: int
    name: str
    age: int | None


df = UserPySparkSelect({"id": [1], "name": ["a"], "age": [10]})
step = df.withColumn("age2", df.age * 2)
out = (
    step.withColumnRenamed("name", "name_new")
    .select_typed("id", "name_new", age_x4=step.age2 * 2)
    .rename({"id": "uid", "name_new": "uname", "age_x4": "uage_x4"})
    .collect()
)
assert [r.model_dump() for r in out] == [{"uage_x4": 40, "uid": 1, "uname": "a"}]

# docs/FASTAPI (transformation logic only)


class UserFastApi(DataFrameModel):
    id: int
    age: int | None


RM = UserFastApi.row_model()
df = UserFastApi([RM(id=1, age=20), RM(id=2, age=None)])
df2 = df.with_columns(age2=df.age + 1).select("id", "age2")
assert [m.model_dump() for m in df2.collect()] == [
    {"id": 1, "age2": 21},
    {"id": 2, "age2": None},
]

df = UserFastApi(
    [
        RM(id=1, age=22),
        RM(id=2, age=None),
        RM(id=3, age=15),
        RM(id=4, age=30),
    ]
)
ranked = df.filter(df.age >= 18).sort("age", descending=True).head(2)
assert [m.model_dump() for m in ranked.collect()] == [
    {"id": 4, "age": 30},
    {"id": 1, "age": 22},
]


class OrderLineDF(DataFrameModel):
    order_id: int
    user_id: int
    amount: float | None


class UserDimDF(DataFrameModel):
    user_id: int
    country: str


orders = OrderLineDF(
    {
        "order_id": [1, 2, 3],
        "user_id": [10, 10, 20],
        "amount": [50.0, None, 20.0],
    }
)
users = UserDimDF({"user_id": [10, 20], "country": ["US", "CA"]})
rolled = (
    orders.join(users, on="user_id", how="left")
    .fill_null(0.0, subset=["amount"])
    .group_by("country")
    .agg(total=("sum", "amount"), n_orders=("count", "order_id"))
    .sort("country")
)
assert [m.model_dump() for m in rolled.collect()] == [
    {"country": "CA", "n_orders": 1, "total": 20.0},
    {"country": "US", "n_orders": 2, "total": 50.0},
]


class LineItemDF(DataFrameModel):
    sku: str
    qty: int
    unit_price: float


LM = LineItemDF.row_model()
df = LineItemDF(
    [LM(sku="A", qty=2, unit_price=10.0), LM(sku="B", qty=1, unit_price=5.0)]
)
df2 = df.with_columns(line_total=df.qty * df.unit_price)
df3 = (
    df2.filter(df2.line_total >= 10.0)
    .sort("line_total", descending=True)
    .head(1)
    .select("sku", "qty", "line_total")
)
assert [m.model_dump() for m in df3.collect()] == [
    {"sku": "A", "qty": 2, "line_total": 20.0},
]

# docs/FASTAPI — trusted_mode + columnar body (mirrors new guide sections)

df_trusted = UserFastApi(
    [{"id": 1, "age": 20}, {"id": 2, "age": None}],
    trusted_mode="shape_only",
)
assert [m.model_dump() for m in df_trusted.collect()] == [
    {"id": 1, "age": 20},
    {"id": 2, "age": None},
]


class UsersColumnarBody(BaseModel):
    id: list[int]
    age: list[int | None]


body = UsersColumnarBody(id=[1, 2], age=[20, None])
df_col = UserFastApi({"id": body.id, "age": body.age})
assert df_col.to_dict() == {"id": [1, 2], "age": [20, None]}

# docs/FASTAPI — async materialization (acollect / ato_dict)


async def _fastapi_async_snippet() -> None:
    df_a = UserFastApi([RM(id=1, age=20), RM(id=2, age=None)])
    rows = await df_a.acollect()
    assert [m.model_dump() for m in rows] == [
        {"id": 1, "age": 20},
        {"id": 2, "age": None},
    ]
    col = await df_a.ato_dict()
    assert col == {"id": [1, 2], "age": [20, None]}


asyncio.run(_fastapi_async_snippet())

# docs/FASTAPI / interchange: materialize_parquet + to_arrow. PyArrow optional at
# runtime; CI installs it for pytest and this script.
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pass
else:
    from io import BytesIO

    from pydantable import materialize_parquet

    _buf = BytesIO()
    pq.write_table(pa.Table.from_pydict({"id": [1], "age": [20]}), _buf)
    _cols = materialize_parquet(_buf.getvalue())
    _df_pq = UserFastApi(_cols, trusted_mode="shape_only")
    assert _df_pq.to_dict() == {"id": [1], "age": [20]}
    _at = _df_pq.to_arrow()
    assert _at.column("id").to_pylist() == [1]

# Cookbook (new docs section)
run_fastapi_columnar_bodies()
run_fastapi_async_materialization()
run_transforms_join_groupby()
run_windows_framing_primer()

print("verify_doc_examples: ok", flush=True)

# Intermittent SIGABRT (exit 134) during interpreter teardown after successful
# runs (PyO3 / Polars native drops). Skip normal shutdown when run as ``__main__``
# so CI does not need to treat 134 as success.
if __name__ == "__main__":
    os._exit(0)
