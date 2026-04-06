# Transforms: join + group_by + agg

This recipe demonstrates a common analytics pattern with typed schemas.

## Recipe

```python
from pydantable import DataFrameModel


class Orders(DataFrameModel):
    order_id: int
    user_id: int
    amount: float | None


class Users(DataFrameModel):
    user_id: int
    country: str

class OrderUser(Orders):
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

joined = orders.join_as(
    other=users,
    model=OrderUser,
    on=[orders.col.user_id],
    how="left",
)
agg = joined.group_by_agg_as(
    CountryAgg,
    keys=[joined.col.country],
    total=("sum", joined.col.amount),
    n_orders=("count", joined.col.order_id),
)
out = agg.to_dict()
assert set(out.keys()) == {"country", "total", "n_orders"}
```

## Pitfalls

- **Row order is not guaranteed** for many operations; compare using sorted keys (see {doc}`/INTERFACE_CONTRACT`).
- **All-null groups**: `sum/mean/min/max/...` yield `None`; `count` yields `0` (see {doc}`/INTERFACE_CONTRACT`).

