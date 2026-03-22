"""
Run the runnable snippets from user-facing docs (no network; requires built _core).

Usage from repo root:

    PYTHONPATH=python .venv/bin/python scripts/verify_doc_examples.py
"""

from __future__ import annotations

from pydantable import DataFrameModel
from pydantable.pandas import DataFrameModel as PandasDataFrameModel
from pydantable.pyspark import DataFrameModel as PySparkDataFrameModel
from pydantable.pyspark.sql import functions as F


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

# docs/pydantable_readme


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
    [RM(id=1, age=22), RM(id=2, age=None), RM(id=3, age=15)]
)
assert [m.model_dump() for m in df.filter(df.age >= 18).collect()] == [
    {"id": 1, "age": 22},
]


class EventDF(DataFrameModel):
    user_id: int
    spend: float | None


ERM = EventDF.row_model()
df = EventDF([ERM(user_id=1, spend=150.0), ERM(user_id=2, spend=50.0)])
df2 = (
    df.with_columns(spend_usd=df.spend * 1.0)
    .filter(df.spend > 100.0)
    .select("user_id", "spend_usd")
)
assert [m.model_dump() for m in df2.collect()] == [
    {"user_id": 1, "spend_usd": 150.0},
]

print("verify_doc_examples: ok")
