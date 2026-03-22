# PySpark UI (`pydantable.pyspark`)

The **PySpark UI** is an optional import surface that adds **Apache Spark–style names** (`withColumn`, `where`, `orderBy`, …) and a small **`pyspark.sql`-like** submodule on top of pydantable’s typed logical DataFrame. It is **not** the Apache Spark `DataFrame`; there is no JVM, no `SparkSession`, and no runtime dependency on the `pyspark` package.

Execution uses pydantable’s Rust/Polars core (see [Execution](EXECUTION.md)).

## When to use it

- You want **Spark-flavored method names** and **`pydantable.pyspark.sql.functions as F`** while keeping **schema-safe `Expr`** and **`DataFrameModel`**.
- You are migrating or comparing code mentally against PySpark and need familiar entry points.

```python
from pydantable.pyspark import DataFrame, DataFrameModel
from pydantable.pyspark.sql import functions as F

class User(DataFrameModel):
    id: int
    name: str

df = User({"id": [1], "name": ["Ada"]})
out = df.withColumn("greeting", F.concat(F.col("name", dtype=str), F.lit("!")))
print(out.to_dict())
```

Output (one run):

```text
{'greeting': ['Ada!'], 'name': ['Ada'], 'id': [1]}
```

## Imports

| Symbol | Role |
|--------|------|
| `DataFrame` | Core logical `DataFrame` + Spark-like methods (same Rust engine as default). |
| `DataFrameModel` | Pydantic model; inner frame is the PySpark UI `DataFrame`. |
| `Expr`, `Schema` | Re-exported from pydantable core. |
| `sql` | Package with `functions`, `types`, `Column`, etc. |

Implementation for the DataFrame wrappers is in `python/pydantable/pyspark/dataframe.py`.

## `DataFrame` (PySpark UI)

Core operations (`collect`, `join`, `group_by`, typed `filter`, …) behave like the default pydantable `DataFrame`. Spark-named aliases:

| Method | Maps to |
|--------|---------|
| `withColumn(name, col)` | `with_columns(**{name: col})` |
| `where(condition)` / `filter(condition)` | `filter(condition)` |
| `select(*cols)` | Core `select` |
| `orderBy(*columns, ascending=...)` / `sort(...)` | `order_by(...)` |
| `limit(num)` | `limit(num)` |
| `drop(*cols)` | `drop(*cols)` |
| `distinct()` | All-column distinct rows |
| `withColumnRenamed(existing, new)` | `with_column_renamed` |
| `dropDuplicates(subset=None)` | `distinct()` if `subset is None`; otherwise **raises** (`subset=` not implemented) |
| `union` / `unionAll` | Core `union` (may raise if vertical concat is not implemented in the planner) |

### Schema and columns

- **`columns`** — list of logical column names (same idea as Spark’s `df.columns`).
- **`schema`** — a lightweight **`StructType`** built from pydantable annotations via `annotation_to_data_type` (see `pyspark/sql/types.py`). These types are **facade objects**, not JVM Spark `DataType` instances.

### `__getitem__`

- **`df["col"]`** → **`Expr`** for that column.
- **`df[["a", "b"]]`** → **`select("a", "b")`**.

## `DataFrameModel` (PySpark UI)

Delegates Spark-like methods to the inner PySpark UI `DataFrame` and re-wraps as the same model class. **`schema`** and **`columns`** follow the inner frame.

## `pydantable.pyspark.sql`

Mirrors **common import paths** only—not binary or behavioral parity with Spark.

```python
from pydantable.pyspark.sql import functions as F, Column, IntegerType, StructType
```

This block only checks that imports resolve; it has no printed output.

- **`functions`** — `lit`, typed **`col(..., dtype=...)`**, `isnull` / `isnotnull`, `coalesce`, `when` / `otherwise`, `cast`, `between`, `isin`, `concat`, `substring`, `length`, and aggregate stubs that direct you to **`group_by().agg`**.
- **`Column`** — type alias for pydantable **`Expr`**.
- **`types`** — simple `IntegerType`, `StringType`, `StructField`, `StructType`, … for documentation and `schema` views.

Full coverage vs Spark is summarized in **[PySpark parity matrix](PYSPARK_PARITY.md)**.

## What is intentionally out of scope

- **`SparkSession`**, **`spark.sql("...")`**, streaming, catalogs.
- **`Window`** and window functions (roadmapped separately).
- Untyped **`F.col("x")`** without **`dtype=`** (pydantable requires static types at build time).
- Interop with a real **`pyspark.sql.DataFrame`** unless a dedicated integration is added later.

## Further reading

- [PySpark parity matrix](PYSPARK_PARITY.md)
- [Execution](EXECUTION.md)
- [Interface contract](INTERFACE_CONTRACT.md)
