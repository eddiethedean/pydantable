# End-to-End Polars-Style Workflows

These examples mirror common Polars workflows using typed `pydantable` APIs.

## 1) Join + enrich + aggregate

```python
from pydantable import DataFrameModel

class Orders(DataFrameModel):
    order_id: int
    user_id: int
    amount: float | None

class Users(DataFrameModel):
    user_id: int
    country: str

orders = Orders({
    "order_id": [1, 2, 3],
    "user_id": [10, 10, 20],
    "amount": [50.0, None, 20.0],
})
users = Users({"user_id": [10, 20], "country": ["US", "CA"]})

out = (
    orders.join(users, on="user_id", how="left")
    .group_by("country")
    .agg(total=("sum", "amount"), n_orders=("count", "order_id"))
    .collect()
)
```

## 2) Reshape long-to-wide

```python
from pydantable import DataFrameModel

class Metrics(DataFrameModel):
    id: int
    metric: str
    value: int | None

df = Metrics({
    "id": [1, 1, 2, 2],
    "metric": ["A", "B", "A", "B"],
    "value": [10, 20, None, 40],
})

wide = df.pivot(index="id", columns="metric", values="value", aggregate_function="first")
out = wide.collect()
# Output columns follow contract naming (for example: "A_first", "B_first").
```

## 3) Time-series rolling + dynamic windows

```python
from pydantable import DataFrameModel

class TS(DataFrameModel):
    id: int
    ts: int
    v: int | None

df = TS({"id": [1, 1, 1], "ts": [0, 3600, 7200], "v": [10, None, 30]})

rolled = df.rolling_agg(
    on="ts", column="v", window_size="2h", op="sum", out_name="v_roll", by=["id"]
)

dynamic = df.group_by_dynamic("ts", every="1h", by=["id"]).agg(
    v_sum=("sum", "v"),
    v_count=("count", "v"),
)
```
