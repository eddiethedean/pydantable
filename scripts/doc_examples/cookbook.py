from __future__ import annotations

import asyncio

from pydantable import DataFrameModel
from pydantable.expressions import row_number
from pydantable.window_spec import Window
from pydantic import BaseModel


def run_fastapi_columnar_bodies() -> None:
    class UsersBody(BaseModel):
        id: list[int]
        age: list[int | None]

    class User(DataFrameModel):
        id: int
        age: int | None

    body = UsersBody(id=[1, 2], age=[20, None])
    df = User({"id": body.id, "age": body.age})
    assert df.to_dict() == {"id": [1, 2], "age": [20, None]}


def run_fastapi_async_materialization() -> None:
    class User(DataFrameModel):
        id: int
        age: int | None

    async def run() -> None:
        df = User({"id": [1, 2], "age": [20, None]})
        rows = await df.acollect()
        assert [r.model_dump() for r in rows] == [
            {"id": 1, "age": 20},
            {"id": 2, "age": None},
        ]
        cols = await df.ato_dict()
        assert cols == {"id": [1, 2], "age": [20, None]}

    asyncio.run(run())


def run_transforms_join_groupby() -> None:
    class Orders(DataFrameModel):
        order_id: int
        user_id: int
        amount: float | None

    class Users(DataFrameModel):
        user_id: int
        country: str

    class OrderUser(DataFrameModel):
        order_id: int
        user_id: int
        amount: float | None
        country: str | None

    class CountryAgg(DataFrameModel):
        country: str | None
        total: float | None
        n_orders: int

    orders = Orders(
        {
            "order_id": [1, 2, 3],
            "user_id": [10, 10, 20],
            "amount": [50.0, None, 20.0],
        }
    )
    users = Users({"user_id": [10, 20], "country": ["US", "CA"]})
    joined = orders.join_as(users, OrderUser, on=[orders.col.user_id], how="left")
    out = joined.group_by_agg_as(
        CountryAgg,
        keys=[joined.col.country],
        total=("sum", joined.col.amount),
        n_orders=("count", joined.col.order_id),
    ).to_dict()
    assert set(out.keys()) == {"country", "total", "n_orders"}


def run_windows_framing_primer() -> None:
    class Row(DataFrameModel):
        group: str
        v: int

    df = Row({"group": ["a", "a", "b"], "v": [2, 1, 5]})
    w = Window.partitionBy("group").orderBy("v")

    class WithRank(DataFrameModel):
        group: str
        v: int
        rn: int | None

    out = df.with_columns_as(WithRank, rn=row_number().over(w)).to_dict()
    assert "rn" in out
