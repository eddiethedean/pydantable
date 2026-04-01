# PySpark Interface

`pydantable.pyspark` provides a PySpark-branded import path for the same typed
DataFrame contract.

## Select the interface

```python
from pydantable.pyspark import DataFrameModel
```

This import succeeds when pydantable is installed; it prints nothing by itself.

## Execution model

The PySpark interface uses the same **Rust execution core** (Polars engine) as
the default export. The `pyspark` module is an import/naming variant for
PySpark-style method names and ergonomics, not a separate Spark runtime.

See:

- `docs/EXECUTION.md`
- `docs/INTERFACE_CONTRACT.md`

## PySpark select feature mapping

| PySpark API | Typed pydantable API | Status | Notes |
|---|---|---|---|
| `select` | `select` | Implemented | Column names/refs; **global** aggregates (`F.sum`, `F.count(col)`, **`F.count()`** with no arg for row count — **0.8.0**) yield a single-row frame per `INTERFACE_CONTRACT.md`. |
| `withColumn` | `withColumn` | Implemented | PySpark-style wrapper over `with_columns`. |
| `withColumns` | `withColumns` | Implemented | Mapping wrapper over `with_columns`. |
| `withColumnRenamed` | `withColumnRenamed` | Implemented | Wrapper over `rename({old: new})`. |
| `withColumnsRenamed` | `withColumnsRenamed` | Implemented | Mapping wrapper over `rename`. |
| `drop` | `drop` | Implemented | Same behavior via typed schema checks. |
| `toDF` | `toDF` | Implemented | Full-column rename with strict arity validation. |
| `transform` | `transform` | Implemented | Callable pipeline helper returning DataFrame/DataFrameModel. |
| `selectExpr` | `select_typed` | Out of scope | SQL-string expressions intentionally excluded; use typed expressions + aliases. |
| `groupBy` | `groupBy` / `group_by` | Implemented (**1.9.0+**) | Returns **`PySparkGroupedDataFrame`** (or model wrapper); `.agg(...)` uses tuple specs. |
| `groupBy(...).pivot(...).agg(...)` | `groupBy(...).pivot(...).agg(...)` | Implemented (**1.9.0+**) | Spark-shaped grouped pivot. `pivot(values=[...])` fixes the pivot value set (missing values become null columns). Output naming uses `<pivot_value>_<out_name>` for grouped pivot aggregations. |
| `groupBy(...).pivot(...).count/sum/avg/min/max` | `groupBy(...).pivot(...).count/sum/avg/min/max` | Implemented (**1.9.0+**) | Convenience wrappers over grouped pivot; `count()` counts rows per group+pivot cell and names outputs as `<pivot_value>_count`. |
| `count()` (action) | `count()` | Implemented (**1.9.0+**) | **`int`** row count via **`global_row_count()`**; distinct from grouped **`count(...)`**. |
| `sort` / `crossJoin` | same | Implemented (**1.9.0+**) | Global **`sort`** only; **`crossJoin`** → **`join(how="cross")`**. |
| `join(..., how=\"left_semi\"|\"left_anti\")` | same | Implemented (**1.9.0+**) | Spark-ish join-mode names mapped to core `semi`/`anti`. |
| `join(..., on=[...])` key handling | same | Implemented (**1.9.0+**) | Spark-ish USING behavior: duplicate right join key columns are dropped by default; opt out with `keepRightJoinKeys=True`. |
| `unionByName` | same | Implemented (**1.9.0+**) | Name order + optional **`allowMissingColumns`**. |
| `intersect` / `subtract` / `except` | same | Partial (**1.9.0+**) | Distinct-set semantics. `except` is exposed at runtime as `df.except(...)` but implemented as `except_` in Python/stubs. |
| `exceptAll` / `intersectAll` | same | Implemented (**1.9.0+**) | Multiset semantics via Rust/Polars core. |
| `fillna` / `dropna` / `na` | same | Implemented (**1.9.0+**) | **`fill_null`** / **`drop_nulls`** with Spark-shaped kwargs. |
| `printSchema` / `explain` | same | Implemented (**1.9.0+**) | Text schema tree; printed plan. |
| `toPandas` | same | Implemented (**1.9.0+**) | Eager; requires **pandas**. |

## End-to-end workflow example

```python
from pydantable.pyspark import DataFrameModel


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

result = (
    orders.join(users, on="user_id", how="left")
    .fill_null(0, subset=["amount"])
    .groupBy("country")
    .agg(total=("sum", "amount"), n_orders=("count", "order_id"))
    .to_dict()
)
order = sorted(range(len(result["country"])), key=lambda i: result["country"][i])
print({k: [result[k][i] for i in order] for k in result})
```

Output:

```text
{'country': ['CA', 'US'], 'total': [20.0, 50.0], 'n_orders': [1, 2]}
```

### Select-style wrappers example

```python
from pydantable.pyspark import DataFrameModel

class User(DataFrameModel):
    id: int
    name: str
    age: int | None

df = User({"id": [1], "name": ["a"], "age": [10]})
step = df.withColumn("age2", df.age * 2)
out = (
    step.withColumnRenamed("name", "name_new")
    .select_typed("id", "name_new", age_x4=step.age2 * 2)
    .rename({"id": "uid", "name_new": "uname", "age_x4": "uage_x4"})
    .collect()
)
print([row.model_dump() for row in out])
```

`toDF(...)` names columns in **schema field order**, which may not match the
order of arguments in `select_typed`; use `rename({...})` when you need explicit
names.

Output:

```text
[{'uage_x4': 40, 'uid': 1, 'uname': 'a'}]
```

Supported operation families mirror the default interface, including core table
ops, joins/group-by, reshape (`melt`/`pivot`), rolling/dynamic windows, temporal
columns/literals, **global `select` aggregates** (`F.sum`, `F.avg`, `F.count`, `F.min`,
`F.max`), and **window** functions (`row_number`, `lag`, `lead`, …). Details:
[`PYSPARK_PARITY.md`](PYSPARK_PARITY.md).

## Regression tests

Automated coverage for `pydantable.pyspark` lives in:

- `tests/test_pyspark_dataframe_coverage.py` — Spark-named `DataFrame` / `DataFrameModel` methods (including **1.9.0** additions such as `groupBy`, `count()`, `unionByName`, `fillna`/`dropna`, set-style helpers, `explain` / `printSchema`).
- `tests/test_pyspark_interface_surface.py` — larger pipelines (join → `groupBy` → agg → melt/pivot, rolling/dynamic windows, temporal filters).

Run selectively: `pytest tests/test_pyspark_dataframe_coverage.py tests/test_pyspark_interface_surface.py`.
