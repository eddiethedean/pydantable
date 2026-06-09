"""Join + groupby + window — runnable quickstart extension."""

from __future__ import annotations

from pydantable import DataFrameModel
from pydantable.expressions import row_number
from pydantable.window_spec import Window


class Orders(DataFrameModel):
    order_id: int
    user_id: int
    amount: float | None


class Users(DataFrameModel):
    user_id: int
    country: str


def main() -> None:
    orders = Orders(
        {
            "order_id": [1, 2, 3, 4],
            "user_id": [10, 10, 20, 20],
            "amount": [50.0, 30.0, None, 20.0],
        }
    )
    users = Users({"user_id": [10, 20], "country": ["US", "CA"]})

    joined = orders.join(users, on="user_id", how="left").filter(
        orders.amount.is_not_null()
    )

    w = Window.partitionBy("country").orderBy("amount")
    ranked = joined.with_columns(rn=row_number().over(w))

    by_country = (
        ranked.group_by("country")
        .agg(total=("sum", "amount"), n_orders=("count", "order_id"))
        .sort("country")
    )

    cols = by_country.to_dict()
    assert set(cols.keys()) == {"country", "total", "n_orders"}
    countries = sorted(cols["country"])
    assert countries == ["CA", "US"]
    print("grouped:", cols)
    print("ok")


if __name__ == "__main__":
    main()
