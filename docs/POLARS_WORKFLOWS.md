# End-to-End Polars-Style Workflows

These examples mirror common Polars workflows using typed `pydantable` APIs.

For the same patterns through the PySpark import surface, use
`from pydantable.pyspark import DataFrameModel` (see `docs/PYSPARK_INTERFACE.md`).

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
    .to_dict()
)
# Stable row order for printing (group_by order is not guaranteed; see INTERFACE_CONTRACT.md).
order = sorted(range(len(out["country"])), key=lambda i: out["country"][i])
print({k: [out[k][i] for i in order] for k in out})
```

Output:

```text
{'country': ['CA', 'US'], 'total': [20.0, 50.0], 'n_orders': [1, 2]}
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
print(wide.to_dict())
# Column names follow the contract (for example: "A_first", "B_first").
```

Output (one run):

```text
{'id': [1, 2], 'A_first': [10, None], 'B_first': [20, 40]}
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
print(rolled.to_dict())
print(dynamic.to_dict())
```

Output (one run):

```text
{'v': [10, None, 30], 'ts': [0, 3600, 7200], 'id': [1, 1, 1], 'v_roll': [10, 10, 40]}
{'ts': [0, 3600, 7200], 'id': [1, 1, 1], 'v_sum': [10, None, 30], 'v_count': [1, 0, 1]}
```

## 4) Single-row metrics (`select` globals, 0.8.0)

Whole-frame aggregates return one row — useful for dashboards or summaries after `filter`:

```python
from pydantable import DataFrameModel
from pydantable.expressions import global_count, global_row_count, global_sum

class Sales(DataFrameModel):
    region: str
    amount: int | None

df = Sales(
    {
        "region": ["US", "US", "EU"],
        "amount": [10, None, 5],
    }
)
hot = df.filter(df.region == "US")
out = hot.select(
    global_row_count(),
    global_count(hot.amount),
    global_sum(hot.amount),
).to_dict()
print(out)
```

Output (one run):

```text
{'row_count': [2], 'sum_amount': [10], 'count_amount': [1]}
```

PySpark UI: same idea with `from pydantable.pyspark.sql import functions as F` and
`df.select(F.count(), F.count(F.col("amount", dtype=int | None)), F.sum(F.col("amount", dtype=int | None)))`.

## 5) Computed expressions in `select` (alias) and schema-driven selectors

Polars commonly uses `select` for computed expressions. In pydantable, computed expressions
must be explicitly named with `Expr.alias(...)`:

```python
from pydantable import DataFrameModel

class User(DataFrameModel):
    id: int
    age: int

df = User({"id": [1, 2], "age": [20, 30]})

out = df.select(
    "id",
    (df.age * 2).alias("age2"),
)
print(out.to_dict())
```

Schema-driven “selector” helpers expand against the current schema (no wildcard DSL):

```python
df2 = df.with_columns(age2=df.age * 2, age3=df.age * 3)
print(df2.select_prefix("age").to_dict())  # age, age2, age3
print(df2.select_all().to_dict())          # full schema order
```
