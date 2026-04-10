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
assert set(out.keys()) == {"country", "total", "n_orders"}
```

## Pitfalls

- **Row order is not guaranteed** for many operations; compare using sorted keys (see {doc}`/INTERFACE_CONTRACT`).
- **All-null groups**: `sum/mean/min/max/...` yield `None`; `count` yields `0` (see {doc}`/INTERFACE_CONTRACT`).

## See also

- **`DataFrameModel` + PlanFrame:** how these transforms run and when escape hatches apply — {doc}`/PLANFRAME_FALLBACKS`; adapter details — {doc}`/PLANFRAME_ADAPTER_ROADMAP`.

