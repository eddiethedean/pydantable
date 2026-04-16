# PySpark UI (`pydantable.pyspark`)

The **PySpark UI** is an optional import surface that adds **Apache Spark–style names** (`withColumn`, `where`, `orderBy`, …) and a small **`pyspark.sql`-like** submodule on top of pydantable’s typed logical DataFrame. It is **not** the Apache Spark `DataFrame`; there is no JVM, no `SparkSession`, and no runtime dependency on the `pyspark` package.

Execution uses pydantable’s Rust/Polars core (see [Execution](EXECUTION.md)).

## Install: base package vs `pydantable[spark]`

- **`import pydantable.pyspark`** (this façade — `DataFrame`, `DataFrameModel`, `pydantable.pyspark.sql`, …) works with **only** the core `pip install pydantable` dependencies. It does **not** require PySpark, SparkDantic, or raikou-core at import time.
- **`pydantable.pyspark.sparkdantic`** (JVM schema helpers from [SparkDantic](https://github.com/mitchelllisle/sparkdantic)) is loaded **lazily** and needs `pip install "pydantable[spark]"` (pulls in `sparkdantic`, `pyspark`, `raikou-core`, …). Accessing `pydantable.pyspark.sparkdantic` without those packages installed raises `ModuleNotFoundError` for the missing optional dependency.
- **Real PySpark execution** (`SparkDataFrame`, `SparkDataFrameModel` over a `pyspark.sql.DataFrame`) is documented in [Spark engine](SPARK_ENGINE.md) and requires the same `[spark]` stack plus a JVM.

## Release context (1.8.0 vs 1.9.0)

- **1.8.0** focused on **core** ergonomics (selectors, joins, `drop_nulls`, reshape parity, etc.)—the same engine every import style uses; see the {doc}`CHANGELOG` **1.8.0** section.
- **1.9.0** adds the **Spark-shaped `DataFrame` / `DataFrameModel` surface** in this document: `groupBy`, frame `count()`, `crossJoin`, `unionByName`, set-style helpers, `fillna` / `dropna` / `.na`, `printSchema`, `explain`, `toPandas`, **`F.dayofyear`** / **`F.from_unixtime`**, and related typing/stubs. Core **`describe()`** (and PySpark **`summary()`**) include **`date`** / **`datetime`** summary lines when those columns exist. Behavior and limitations are summarized in [PySpark parity](PYSPARK_PARITY.md) and [Interface contract](INTERFACE_CONTRACT.md).

## Tests

CI and local runs exercise the PySpark UI via:

- `tests/test_pyspark_dataframe_coverage.py` — method coverage, error contracts, `DataFrame` / `DataFrameModel`, grouped handles, `unionByName`, set ops, NA helpers, `explain` / `printSchema`.
- `tests/test_pyspark_interface_surface.py` — end-to-end pipelines (joins, `groupBy().agg`, melt/pivot, windows, temporal filters).

When adding Spark-named wrappers, extend those files (or add focused tests next to them) so regressions are caught on all platforms.

## Semantic differences vs Apache Spark

- **No cluster:** all methods lower to the **in-process** Rust/Polars plan; `count()` is a logical row count, not a distributed action across executors.
- **`subtract`:** implemented as an anti join on all columns (distinct-set semantics), not Spark multiset **`EXCEPT ALL`**. Use `exceptAll` for multiset semantics.
- **`sort`/`orderBy`:** global sort only; there is no **`sortWithinPartitions`**.
- **`summary()`:** same **string** as core **`describe()`** (int/float/bool/str/**date**/**datetime** lines where those columns exist), not Spark’s full multi-column **`summary`** **`DataFrame`** unless/until a future release adds a table-shaped stats path.

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
| `groupBy(...).agg({\"v\": \"sum\", \"w\": \"max\"})` | Dict-form `agg` (Spark-ish) auto-names outputs as `v_sum`, `w_max`; multi-op per column supported via lists (**1.9.0+**). |
| `groupBy(...).pivot(...).agg({\"v\": [\"sum\", \"max\"]})` | Dict-form pivot `agg` auto-names per pivot value: e.g. `x_v_sum`, `x_v_max` (**1.9.0+**). |
| `groupBy(...).pivot(...).count()` / `.sum(...)` / `.avg(...)` / `.min(...)` / `.max(...)` | Convenience wrappers over grouped-pivot `.agg(...)` (Spark-shaped), including `count()` as rows per `(keys, pivot_value)` cell (**1.9.0+**). |
| `orderBy(*columns, ascending=...)` / `sort(...)` | Core `sort` / `order_by` (global sort only; not Spark `sortWithinPartitions`) |
| `crossJoin(other)` | `join(other, how="cross")` (**1.9.0+**) |
| `join(other, on=[...], how="left_semi"|"left_anti")` | Core `join(how="semi"|"anti")` with Spark-ish names; output is left-only columns (**1.9.0+**). |
| `join(other, on=[...], how="right_semi"|"right_anti")` | Spark-ish aliases implemented by swapping join sides; output is right-only columns (**1.9.0+**). |
| `join(..., on=[...])` key handling | Spark-ish USING behavior: duplicate right join keys are dropped by default; set `keepRightJoinKeys=True` to opt out (**1.9.0+**). |
| `join(left_on=[...], right_on=[...])` | Join on differently named keys (Spark-style). Keys accept `str` or `ColumnRef` and list/tuple forms (**1.9.0+**). |
| `join(validate="1:1"|"1:m"|"m:1"|"m:m")` | Join cardinality validation shorthands (forwarded to core join) (**1.9.0+**). |
| `count()` | Row count as **`int`** via `global_row_count()` in the plan (**1.9.0+**); distinct from grouped `GroupedDataFrame.count(*cols)` |
| `unionByName(other, allowMissingColumns=False)` | Reorder `other` by name, then vertical `concat`; optional null-padding for missing columns (**1.9.0+**) |
| `intersect` / `subtract` / `except` | Typed join + `distinct` / anti-join (**1.9.0+**). Note: `except` is exposed at runtime as `df.except(...)` but is implemented as `except_` in Python/stubs since `except` is a keyword. |
| `intersectAll` / `exceptAll` | Multiset set ops (**1.9.0+**); `exceptAll` keeps duplicates per `max(left-right,0)` counts, `intersectAll` keeps `min(left,right)` counts. |
| `fillna` / `dropna` / `na.drop` / `na.fill` | `fill_null` / `drop_nulls` with Spark-shaped kwargs (**1.9.0+**) |
| `printSchema()` | Text tree from `df.schema` (**1.9.0+**) |
| `explain(...)` | Prints core logical plan string (**1.9.0+**) |
| `toPandas()` | `to_dict()` → `pandas.DataFrame` (eager; requires **pandas**) (**1.9.0+**) |
| `limit(num)` | `limit(num)` |
| `sample(withReplacement=False, fraction=..., seed=None)` | Core `sample(fraction=..., seed=..., with_replacement=...)` (eager; materializes via `to_dict()`) |
| `explode(column(s), outer=False)` | Core list reshape (Polars `explode`); `outer=True` maps to Spark-ish `explode_outer` null/empty handling (see parity doc). |
| `explode_outer(...)` | Same as `explode(..., outer=True)`. |
| `explode_all()` | Explode every **list**-typed column (schema-driven). |
| `posexplode(column, pos='pos', value=None, outer=False)` | One list column → synchronized position (0-based) + element columns; `value` defaults to the list column name. |
| `posexplode_outer(...)` | `posexplode(..., outer=True)`. |
| `unnest(column(s))` / `unnest_all()` | Core **struct** flattening (Spark users often want this for nested structs, not `explode`). |
| `drop(*cols)` | `drop(*cols)` |
| `distinct()` | All-column distinct rows |
| `withColumnRenamed(existing, new)` | `with_column_renamed` |
| `dropDuplicates(subset=None)` | Core `distinct(keep="first")` (engine-dependent “first” unless ordered); `subset=` maps to `distinct(subset=..., keep="first")` |
| `union` / `unionAll` | Core vertical `concat` (same schema required) |
| `show(n=20, truncate=True, vertical=False)` | **0.20.0+** — prints a bounded text table (`head`-like sample). |
| `summary()` | **0.20.0+** — see [**`summary` vs Spark**](#summary-vs-apache-spark) below. |

(summary-vs-apache-spark)=
### `summary` vs Apache Spark

Spark’s **`DataFrame.summary()`** returns another **DataFrame** of statistics (and accepts optional stat names). In pydantable, PySpark **`summary()`** is deliberately a thin alias of core **`describe()`**: it returns a **single multi-line string**, not a table-shaped frame.

**What `describe()` / `summary()` include** (after one **`to_dict()`** materialization): **int** and **float** (count, mean, std, min, max; skew/kurtosis/sem when **numpy** is available and count ≥ 4), **bool** (true/false/null counts), **str** (count, `n_unique`, min/max length), **`date` / `datetime`** (non-null count, min, max, nulls). Other column types are omitted from the report.

For distributional stats Spark’s **`summary`** column set, use Polars or pandas on a materialized export, or build explicit **`select`** aggregates.

### Naming map (core ↔ pandas ↔ PySpark)

See {doc}`PANDAS_UI` **Naming map** for **`with_columns` / `assign` / `withColumn`**, **`filter`**, joins, and sorts—same Rust engine for all three import styles.

### Schema and columns

- **`columns`** — list of logical column names (same idea as Spark’s `df.columns`).
- **`schema`** — a lightweight **`StructType`** built from pydantable annotations via `annotation_to_data_type` (see `pyspark/sql/types.py`). These types are **facade objects**, not JVM Spark `DataType` instances.

### `__getitem__`

- **`df["col"]`** → **`Expr`** for that column.
- **`df[["a", "b"]]`** → **`select("a", "b")`**.

## `DataFrameModel` (PySpark UI)

Delegates Spark-like methods to the inner PySpark UI `DataFrame` and re-wraps as the same model class. **`schema`** and **`columns`** follow the inner frame. **1.9.0+** adds the same **`groupBy`**, **`sort`**, **`crossJoin`**, **`count()`**, **`unionByName`**, set-style helpers, **`fillna` / `dropna` / `.na`**, **`printSchema`**, **`explain`**, and **`toPandas`** surface as on `DataFrame`. **`explode`**, **`posexplode`**, **`unnest`**, and **`explode_all` / `unnest_all`** are documented on both `DataFrame` and `DataFrameModel`.

### List explosion vs `functions.explode`

Apache Spark allows `select(explode(col))` because `explode` is a generator expression. **pydantable does not:** there is no table-generating `Expr` for explosion in `select`. Importing **`pydantable.pyspark.sql.functions as F`** and calling **`F.explode(...)`** raises **`TypeError`** with directions to use **`DataFrame.explode(...)`** or **`DataFrame.posexplode(...)`** instead.

## `pydantable.pyspark.sql`

Mirrors **common import paths** only—not binary or behavioral parity with Spark.

```python
from pydantable.pyspark.sql import functions as F, Column, IntegerType, StructType
```

This block only checks that imports resolve; it has no printed output.

- **`functions`** — `lit`, typed **`col(..., dtype=...)`**, `isnull` / `isnotnull`, `coalesce`, `when` / `otherwise`, `cast`, `between`, `isin`, `concat`, `substring`, `length`, **`explode`** (raises **`TypeError`**; use **`DataFrame.explode`** / **`posexplode`**), **date/time:** `year`, `month`, `day`, `dayofmonth`, `dayofweek`, `quarter`, `weekofyear`, `dayofyear`, `hour`, `minute`, `second`, `nanosecond`, `to_date`, `strptime`, `unix_timestamp`, `from_unixtime` (see **[PySpark parity](PYSPARK_PARITY.md)** Phase B for semantics), global **`sum`/`avg`/`mean`/`count`/`min`/`max`** for **`select`**, and window helpers.
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
