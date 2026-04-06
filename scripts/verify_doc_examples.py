"""Run doc snippets as tests (offline; requires built ``pydantable._core``).

Run from the repository root::

    PYTHONPATH=python .venv/bin/python scripts/verify_doc_examples.py

Fails on assertion errors or import failures so README and Sphinx snippets stay
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
from pydantable.expressions import Literal, coalesce
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


class UserWithAge2(User):
    age2: int | None


class UserOut(DataFrameModel):
    id: int
    age2: int | None


df2 = df.with_columns_as(UserWithAge2, age2=df.col.age * 2)
df3 = df2.select_as(UserOut, df2.col.id, df2.col.age2)
df4 = df3.filter(df3.col.age2 > 10)
assert df4.to_dict() == {"id": [1], "age2": [40]}

# Same pattern as docs index / README quick start


class UserReadme(DataFrameModel):
    id: int
    age: int


df = UserReadme({"id": [1, 2], "age": [20, 30]})


class UserReadmeWithAge2(UserReadme):
    age2: int


class UserReadmeOut(DataFrameModel):
    id: int
    age2: int


df2 = df.with_columns_as(UserReadmeWithAge2, age2=df.col.age * 2)
df3 = df2.select_as(UserReadmeOut, df2.col.id, df2.col.age2)
df4 = df3.filter(df3.col.age2 > 40)
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


class DFUserWithAge2(DFUser):
    age2: int


df_c2 = df_c.with_columns_as(DFUserWithAge2, age2=df_c.col.age * 2)
assert df_c2.to_dict() == {"id": [1, 2], "age": [20, 40], "age2": [40, 80]}

df_q = DFUser({"id": [1, 2, 3], "age": [10, 50, 60]})
df_q2 = df_q.with_columns_as(DFUserWithAge2, age2=df_q.col.age * 2)
df_q3 = df_q2.select_as(DFUserWithAge2, df_q2.col.id, df_q2.col.age, df_q2.col.age2)
df_q4 = df_q3.filter(df_q3.col.age2 > 40)
assert df_q4.to_dict() == {"id": [2, 3], "age": [50, 60], "age2": [100, 120]}

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


class OrderUser(Orders):
    country: str | None


class CountryAgg(DataFrameModel):
    country: str | None
    total: float | None
    n_orders: int


joined = orders.join_as(users, OrderUser, on=[orders.col.user_id], how="left")
out = joined.group_by_agg_as(
    CountryAgg,
    keys=[joined.col.country],
    total=("sum", joined.col.amount),
    n_orders=("count", joined.col.order_id),
).to_dict()
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


class Wide(DataFrameModel):
    id: int
    A_first: int | None
    B_first: int | None


wide = df.pivot_as(
    Wide,
    index=[df.col.id],
    columns=df.col.metric,
    values=[df.col.value],
    aggregate_function="first",
    pivot_values=["A", "B"],
)
assert wide.to_dict() == {
    "id": [1, 2],
    "A_first": [10, None],
    "B_first": [20, 40],
}


#
# NOTE: strict 2.0 removed `rolling_agg`, `group_by_dynamic`, and the `pyspark` facade.
#

# docs/FASTAPI (transformation logic only)


class UserFastApi(DataFrameModel):
    id: int
    age: int | None


RM = UserFastApi.row_model()
df = UserFastApi([RM(id=1, age=20), RM(id=2, age=None)])


class UserFastApiWithAge2(UserFastApi):
    age2: int | None


class UserFastApiOut(DataFrameModel):
    id: int
    age2: int | None


df2_full = df.with_columns_as(UserFastApiWithAge2, age2=df.col.age + 1)
df2 = df2_full.select_as(UserFastApiOut, df2_full.col.id, df2_full.col.age2)
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
ranked = df.filter(df.col.age >= 18).sort("age", descending=True).head(2)
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


class OrderUser2(DataFrameModel):
    order_id: int
    user_id: int
    amount: float | None
    country: str | None


class CountryRevenueRowDF(DataFrameModel):
    country: str | None
    total: float | None
    n_orders: int


joined2 = orders.join_as(users, OrderUser2, on=[orders.col.user_id], how="left")


class OrderUser2Filled(OrderUser2):
    amount_filled: float | None


filled2 = joined2.with_columns_as(
    OrderUser2Filled, amount_filled=coalesce(joined2.col.amount, Literal(value=0.0))
)
rolled = filled2.group_by_agg_as(
    CountryRevenueRowDF,
    keys=[filled2.col.country],
    total=("sum", filled2.col.amount_filled),
    n_orders=("count", filled2.col.order_id),
).sort("country")
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


class LineItemWithTotalDF(LineItemDF):
    line_total: float


class LineTotalDF(DataFrameModel):
    sku: str
    qty: int
    line_total: float


df2 = df.with_columns_as(LineItemWithTotalDF, line_total=df.col.qty * df.col.unit_price)
df3 = (
    df2.filter(df2.col.line_total >= 10.0)
    .sort("line_total", descending=True)
    .head(1)
    .select_as(LineTotalDF, df2.col.sku, df2.col.qty, df2.col.line_total)
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
