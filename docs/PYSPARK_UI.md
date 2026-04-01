# PySpark UI (`pydantable.pyspark`)

The **PySpark UI** is an optional import surface that adds **Apache Spark–style names** (`withColumn`, `where`, `orderBy`, …) and a small **`pyspark.sql`-like** submodule on top of pydantable’s typed logical DataFrame. It is **not** the Apache Spark `DataFrame`; there is no JVM, no `SparkSession`, and no runtime dependency on the `pyspark` package.

Execution uses pydantable’s Rust/Polars core (see [Execution](EXECUTION.md)).

## Release context (1.8.0 vs 1.9.0)

- **1.8.0** focused on **core** ergonomics (selectors, joins, `drop_nulls`, reshape parity, etc.)—the same engine every import style uses; see the {doc}`changelog` **1.8.0** section.
- **1.9.0** adds the **Spark-shaped `DataFrame` / `DataFrameModel` surface** in this document: `groupBy`, frame `count()`, `crossJoin`, `unionByName`, set-style helpers, `fillna` / `dropna` / `.na`, `printSchema`, `explain`, `toPandas`, and related typing/stubs. Behavior and limitations are summarized in [PySpark parity](PYSPARK_PARITY.md) and [Interface contract](INTERFACE_CONTRACT.md).

## Tests

CI and local runs exercise the PySpark UI via:

- `tests/test_pyspark_dataframe_coverage.py` — method coverage, error contracts, `DataFrame` / `DataFrameModel`, grouped handles, `unionByName`, set ops, NA helpers, `explain` / `printSchema`.
- `tests/test_pyspark_interface_surface.py` — end-to-end pipelines (joins, `groupBy().agg`, melt/pivot, windows, temporal filters).

When adding Spark-named wrappers, extend those files (or add focused tests next to them) so regressions are caught on all platforms.

## Semantic differences vs Apache Spark

- **No cluster:** all methods lower to the **in-process** Rust/Polars plan; `count()` is a logical row count, not a distributed action across executors.
- **`exceptAll`:** implemented as **`subtract`** (anti join + distinct semantics as documented)—not Spark multiset **`EXCEPT ALL`**.
- **`sort`/`orderBy`:** global sort only; there is no **`sortWithinPartitions`**.
- **`summary()`:** still the same **string** as core `describe()` for numeric columns (MVP), not Spark’s full multi-column `summary` table unless/until a future release adds a table-shaped stats path.

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
| `groupBy(...)` / `group_by(...)` | Core `group_by`; returns **`PySparkGroupedDataFrame`** so `.agg()` stays Spark-flavored (**1.9.0+**). |
| `groupBy(...).pivot(pivot_col, values=[...]).agg(...)` | Group at `(keys + pivot_col)` then core `pivot` to wide columns (**1.9.0+**). |
| `orderBy(*columns, ascending=...)` / `sort(...)` | Core `sort` / `order_by` (global sort only; not Spark `sortWithinPartitions`) |
| `crossJoin(other)` | `join(other, how="cross")` (**1.9.0+**) |
| `count()` | Row count as **`int`** via `global_row_count()` in the plan (**1.9.0+**); distinct from grouped `GroupedDataFrame.count(*cols)` |
| `unionByName(other, allowMissingColumns=False)` | Reorder `other` by name, then vertical `concat`; optional null-padding for missing columns (**1.9.0+**) |
| `intersect` / `subtract` / `exceptAll` | Typed join + `distinct` / anti-join (**1.9.0+**); `exceptAll` is **`subtract`** here — not Spark multiset `EXCEPT ALL` |
| `fillna` / `dropna` / `na.drop` / `na.fill` | `fill_null` / `drop_nulls` with Spark-shaped kwargs (**1.9.0+**) |
| `printSchema()` | Text tree from `df.schema` (**1.9.0+**) |
| `explain(...)` | Prints core logical plan string (**1.9.0+**) |
| `toPandas()` | `to_dict()` → `pandas.DataFrame` (eager; requires **pandas**) (**1.9.0+**) |
| `limit(num)` | `limit(num)` |
| `drop(*cols)` | `drop(*cols)` |
| `distinct()` | All-column distinct rows |
| `withColumnRenamed(existing, new)` | `with_column_renamed` |
| `dropDuplicates(subset=None)` | Core `distinct(subset=...)` when `subset` is set; else all-column `distinct()` |
| `union` / `unionAll` | Core vertical `concat` (same schema required) |
| `show(n=20, truncate=True, vertical=False)` | **0.20.0+** — prints a bounded text table (`head`-like sample). |
| `summary()` | **0.20.0+** — returns the same **string** as core **`describe()`** (numeric columns MVP), not Apache Spark’s full **`summary`** statistics set. |

### Naming map (core ↔ pandas ↔ PySpark)

See {doc}`PANDAS_UI` **Naming map** for **`with_columns` / `assign` / `withColumn`**, **`filter`**, joins, and sorts—same Rust engine for all three import styles.

### Schema and columns

- **`columns`** — list of logical column names (same idea as Spark’s `df.columns`).
- **`schema`** — a lightweight **`StructType`** built from pydantable annotations via `annotation_to_data_type` (see `pyspark/sql/types.py`). These types are **facade objects**, not JVM Spark `DataType` instances.

### `__getitem__`

- **`df["col"]`** → **`Expr`** for that column.
- **`df[["a", "b"]]`** → **`select("a", "b")`**.

## `DataFrameModel` (PySpark UI)

Delegates Spark-like methods to the inner PySpark UI `DataFrame` and re-wraps as the same model class. **`schema`** and **`columns`** follow the inner frame. **1.9.0+** adds the same **`groupBy`**, **`sort`**, **`crossJoin`**, **`count()`**, **`unionByName`**, set-style helpers, **`fillna` / `dropna` / `.na`**, **`printSchema`**, **`explain`**, and **`toPandas`** surface as on `DataFrame`.

## `pydantable.pyspark.sql`

Mirrors **common import paths** only—not binary or behavioral parity with Spark.

```python
from pydantable.pyspark.sql import functions as F, Column, IntegerType, StructType
```

This block only checks that imports resolve; it has no printed output.

- **`functions`** — `lit`, typed **`col(..., dtype=...)`**, `isnull` / `isnotnull`, `coalesce`, `when` / `otherwise`, `cast`, `between`, `isin`, `concat`, `substring`, `length`, **`year` / `month` / `day` / `hour` / `minute` / `second` / `nanosecond` / `to_date` / `unix_timestamp`** (wrappers over core `Expr` temporal APIs), global **`sum`/`avg`/`mean`/`count`/`min`/`max`** for **`select`** (including **`count()`** with no args → row count), and window helpers — see the parity matrix.
- **`Column`** — type alias for pydantable **`Expr`**.
- **`types`** — simple `IntegerType`, `StringType`, `StructField`, `StructType`, … for documentation and `schema` views.

Full coverage vs Spark is summarized in **[PySpark parity matrix](PYSPARK_PARITY.md)**.

## What is intentionally out of scope

- **`SparkSession`**, **`spark.sql("...")`**, streaming, catalogs.
- **SQL window frames** (`rowsBetween` / `rangeBetween`): partition + order via **`Window`** / **`WindowSpec`** are supported (see `pydantable.pyspark.sql.window`); frames execute on the Polars-backed core per [`INTERFACE_CONTRACT.md`](INTERFACE_CONTRACT.md) (including `rangeBetween` multi-key rules in [`WINDOW_SQL_SEMANTICS.md`](WINDOW_SQL_SEMANTICS.md)).
- Untyped **`F.col("x")`** without **`dtype=`** (pydantable requires static types at build time).
- Interop with a real **`pyspark.sql.DataFrame`** unless a dedicated integration is added later.

## Further reading

- [PySpark parity matrix](PYSPARK_PARITY.md)
- [Execution](EXECUTION.md)
- [Interface contract](INTERFACE_CONTRACT.md)
