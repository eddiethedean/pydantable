from __future__ import annotations

from pydantable import DataFrameModel


class Orders(DataFrameModel):
    order_id: int
    user_id: int
    amount: float | None


class Users(DataFrameModel):
    user_id: int
    country: str


class Metrics(DataFrameModel):
    id: int
    metric: str
    value: int | None


class TS(DataFrameModel):
    id: int
    ts: int
    v: int | None


def test_workflow_join_groupby() -> None:
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
        .collect()
    )
    assert set(out["country"]) == {"US", "CA"}
    assert sorted(out["n_orders"]) == [1, 2]


def test_workflow_reshape() -> None:
    df = Metrics(
        {
            "id": [1, 1, 2, 2],
            "metric": ["A", "B", "A", "B"],
            "value": [10, 20, None, 40],
        }
    )
    out = df.pivot(
        index="id",
        columns="metric",
        values="value",
        aggregate_function="first",
    ).collect()
    assert out["id"] == [1, 2]
    assert "A_first" in out and "B_first" in out


def test_workflow_time_series() -> None:
    df = TS({"id": [1, 1, 1], "ts": [0, 3600, 7200], "v": [10, None, 30]})
    rolled = df.rolling_agg(
        on="ts", column="v", window_size="2h", op="sum", out_name="v_roll", by=["id"]
    ).collect()
    assert "v_roll" in rolled

    dynamic = df.group_by_dynamic("ts", every="1h", by=["id"]).agg(
        v_sum=("sum", "v"),
        v_count=("count", "v"),
    )
    out = dynamic.collect()
    assert "v_sum" in out and "v_count" in out
