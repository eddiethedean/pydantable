# PySpark API parity (pydantable.pyspark)

This matrix compares **Apache Spark** `pyspark.sql` concepts to **pydantable**’s
facade. It is not a guarantee of behavioral identity with Spark.

For how to import and use the PySpark-style `DataFrame` and `sql` package, see
[PYSPARK_UI](/integrations/alternate-surfaces/pyspark-ui.md).

| Spark API area | PydanTable status | Notes |
|----------------|-------------------|--------|
| `SparkSession`, `spark.sql(...)` | **Out of scope** | No distributed engine or SQL parser in pydantable. |
| `DataFrame.select`, `filter`, `where` | **Supported** | Typed `Expr`; `where` mirrors Spark. |
| `DataFrame.withColumn` | **Supported** | |
| `DataFrame.join` | **Supported** | Suffix/collision rules per `INTERFACE_CONTRACT.md`. |
| `DataFrame.join(..., how=\"left_semi\"|\"left_anti\")` | **Supported** (**1.9.0+**) | Spark-ish `left_semi`/`left_anti` map to core `semi`/`anti` joins; output is left-only columns. |
| `DataFrame.join(..., how=\"right_semi\"|\"right_anti\")` | **Supported** (**1.9.0+**) | Spark-ish `right_*` aliases implemented by swapping join sides; output is right-only columns. |
| `DataFrame.join(left_on=..., right_on=...)` | **Supported** (**1.9.0+**) | Join on differently named keys; list/tuple and `ColumnRef` keys supported in the PySpark facade. |
| `DataFrame.join(validate=\"1:1\"|\"1:m\"|\"m:1\"|\"m:m\")` | **Supported** (**1.9.0+**) | Shorthands forwarded to core join validation. |
| `DataFrame.groupBy` / `.agg` | **Supported** (**1.9.0+**) | CamelCase `groupBy` returns a PySpark grouped wrapper; tuple `agg` specs (not Spark `agg(expr)` only). |
| `GroupedDataFrame.agg({col: op(s)})` | **Supported** (**1.9.0+**) | Dict-form `agg`: `{\"v\":\"sum\"}` → `v_sum`; multi-op lists supported; common Spark op synonyms mapped (e.g. `avg` → `mean`). |
| `GroupedDataFrame.pivot(...).agg(...)` | **Supported** (**1.9.0+**) | Spark-style `groupBy(...).pivot(...).agg(...)` lowers to core `group_by(...).agg(...)` + `pivot(...)` (typed, in-process). |
| `GroupedDataFrame.pivot(...).agg({col: op(s)})` | **Supported** (**1.9.0+**) | Dict-form pivot `agg` with Spark-ish naming: `<pivot_value>_<col_op>` (e.g. `x_v_sum`). |
| `GroupedDataFrame.pivot(...).count/sum/avg/min/max` | **Supported** (**1.9.0+**) | Convenience wrappers over `pivot(...).agg(...)`; `count()` counts rows per group+pivot cell. |
| `GroupedData.count()` (no args) | **Supported** (**1.9.0+**) | Per-group row count (core `len` / synthetic sum). |
| `DataFrame.orderBy` / `sort` | **Supported** | Column names + ascending flags; global sort only (not `sortWithinPartitions`). |
| `DataFrame.crossJoin` | **Supported** (**1.9.0+**) | `join(how="cross")`. |
| `DataFrame.count()` (action) | **Supported** (**1.9.0+**) | Returns **`int`** via `global_row_count()` in the plan. |
| `DataFrame.unionByName` | **Supported** (**1.9.0+**) | Name-aligned concat; optional `allowMissingColumns` null-fill. |
| `DataFrame.intersect` / `subtract` / `except` | **Partial** (**1.9.0+**) | Distinct-set semantics: `intersect` ≈ inner join on all columns + `distinct`; `subtract`/`except` ≈ anti join on all columns + `distinct`. (`except` is a runtime alias of `except_` in Python.) |
| `DataFrame.exceptAll` / `intersectAll` | **Supported** (**1.9.0+**) | Multiset semantics: `exceptAll` yields `max(count_left-count_right,0)`; `intersectAll` yields `min(count_left,count_right)`. |
| `DataFrame.fillna` / `dropna` / `na` | **Supported** (**1.9.0+**) | Map to `fill_null` / `drop_nulls`; unsupported kw combinations raise clearly. |
| `DataFrame.printSchema` / `explain` | **Supported** (**1.9.0+**) | Readable schema tree; printed logical plan. |
| `DataFrame.toPandas` | **Supported** (**1.9.0+**) | Eager via `to_dict()`; requires **pandas**. |
| `DataFrame.limit` | **Supported** | |
| `DataFrame.show` | **Supported** (**0.20.0**) | Prints a bounded text preview (`head`-like); not distributed Spark. |
| `DataFrame.summary` | **Partial** (**0.20.0**) | Returns the same string as core **`describe()`** (int/float/bool/str/**date**/**datetime** summaries; one string, not a stats **`DataFrame`**)—not Spark’s full **`summary`** column set. |
| `DataFrame.drop` | **Supported** | Drop by column name(s). |
| `DataFrame.distinct` | **Supported** | All-column distinct rows; optional `subset=` matches core `distinct`. |
| `DataFrame.withColumnRenamed` | **Supported** | Single rename per call (or use `rename` with dict). |
| `DataFrame.union` / `unionAll` | **Supported** | Vertical concat via core `concat(..., how="vertical")`. |
| `DataFrame.explode` / `explode_outer` | **Supported** | **List columns only**; implemented as a **frame reshape** (Polars `explode`). `outer=True` / `explode_outer` uses Spark-ish **null/empty** handling via Polars `ExplodeOptions` (`empty_as_null: true`, `keep_nulls: true`). |
| `DataFrame.posexplode` / `posexplode_outer` | **Supported** | One list column at a time; adds a **0-based** position column plus the element column (name configurable). |
| `DataFrame.unnest` / `unnest_all` | **Supported** | Struct flattening to top-level fields (often what Spark users mean by “struct explode”). |
| `DataFrame.explode_all` | **Supported** | Schema-driven: explode every list-typed column. |
| `functions.explode` | **Raises `TypeError`** | pydantable has no `select(explode(col))` generator expressions; use **`DataFrame.explode`** / **`posexplode`**. |
| `functions.lit`, `functions.col` | **Supported** | `col` requires `dtype=` or use `df.col()`. |
| `functions.isnull`, `isnotnull`, `coalesce` | **Supported** | Via Rust `ExprNode`. |
| `functions.when` / `otherwise` | **Supported** | `CaseWhen` in Rust; chain `.when(...).otherwise(...)`. |
| `functions.cast`, `between`, `isin`, `concat`, `substring`, `length` | **Supported** | Base types only; `substring` is 1-based (Spark-style). |
| `functions.str_replace`, `regexp_replace`, `strip_prefix`, `strip_suffix`, `strip_chars`, `strptime`, `binary_len`, `list_len`, `list_get`, `list_contains`, `list_min`, `list_max`, `list_sum` | **Supported** (**0.17.0**) | Thin wrappers over core :class:`~pydantable.expressions.Expr` methods (same Rust lowering). `regexp_replace` is an alias for literal substring replace, not full regex. |
| `functions.rlike` / `regexp_like` / `regexp_substr` | **Supported** (**1.9.0+**) | Regex predicates and substring extract via Rust `regex` dialect; requires Polars-backed execution for regex. |
| `functions.year` … `unix_timestamp`, `dayofyear`, `from_unixtime`, … | **Supported** | See Phase B; epoch conversions are **UTC** naive; ISO week / weekday where noted. |
| `functions.sum`, `avg`, `count`, `min`, `max`, … as column exprs | **Supported** (global) | Global `sum`/`avg`/`mean`/`count`/`min`/`max` on a typed `Expr` in `DataFrame.select(...)` (single-row). **`count()`** with **no** argument → row count (**0.8.0**). Grouped paths use `group_by().agg`. |
| `Column.cast`, `isin`, `between`, `substr`/`char_length` | **Supported** | On `Expr` / `Column`; includes **`str` → `date` / `datetime`** via Polars parsing (use `strptime` for fixed formats). |
| `Window`, window functions | **Partial** | `Window.partitionBy().orderBy(..., nulls_last=...)` (**NULLS FIRST/LAST**); `row_number`, `rank`, `dense_rank`, `window_sum`, `window_avg`, `window_min`, `window_max`, `lag`, `lead` + core `Expr` lowering. Framing support includes `rowsBetween` for all named window ops and `rangeBetween` for numeric/temporal aggregates: **first** `orderBy` column must be numeric, `date`, `datetime`, or `duration`; additional `orderBy` columns are sort tie-breakers only ([`WINDOW_SQL_SEMANTICS.md`](/semantics/window-sql-semantics.md)). Unframed multi-key windows: only the first key’s `nulls_last` is passed to Polars `SortOptions`. |
| `functions.map_len`, `map_get`, `map_contains_key`, `map_keys`, `map_values`, `map_entries`, `map_from_entries`, `element_at` | **Supported** | Per-row map cardinality, lookup, membership, key/value lists, entry structs, and entry-to-map reconstruction on `dict[str, T]` columns; `element_at` is a map lookup alias. |
| `types` (Array, Map, nested Struct, Decimal, Timestamp) | **Partial** | Engine supports nested structs/lists, `Decimal`, `datetime`/`date`, homogeneous `dict[str, T]` maps, `bytes`, and `time`; PySpark `types` mirrors annotations for docs/schema views. |
| `Row`, encoders, streaming | **Out of scope** | |

For execution, the PySpark UI uses the same Rust/Polars path as the default export.

**0.18.0:** The parity matrix above is **unchanged**—no new `sql.functions` wrappers this release.

**0.19.0:** Matrix **unchanged**—documentation and **0.x** versioning policy only; see [`ROADMAP.md`](/project/roadmap.md) **Shipped in 0.19.0**.

**0.20.0:** **`DataFrame.show()`** / **`summary()`** rows above; core discovery helpers are shared with the default **`DataFrame`**. See [`ROADMAP.md`](/project/roadmap.md) **Shipped in 0.20.0**.

**1.9.0:** PySpark-shaped **`groupBy`**, row-count **`count()`**, **`crossJoin`**, **`unionByName`**, join-layer **set ops**, **`fillna` / `dropna` / `.na`**, **`printSchema`**, **`explain`**, **`toPandas`**, and matching **`DataFrameModel`** methods — see table rows marked **1.9.0+** above. Automated tests: `tests/test_pyspark_dataframe_coverage.py`, `tests/test_pyspark_interface_surface.py`

## Phase B status (expression surface)

Delivered in-tree: **`IsNull`**, **`IsNotNull`**, **`Coalesce`**, **`CaseWhen`** (`when` / `otherwise`), **`Cast`**, **`InList`**, **`Between`**, **`StringConcat`**, **`Substring`**, **`StringLength`** — Rust `ExprNode`, Polars lowering, and `pydantable.pyspark.sql.functions` / `Expr` methods.

**Also delivered — date/datetime (`functions` + `Expr`):** `year`, `month`, `day` / `dayofmonth`, `dayofweek`, `quarter`, `weekofyear`, `dayofyear`, `hour`, `minute`, `second`, `nanosecond`, `to_date` (optional `format=` for strings), `strptime`, `unix_timestamp` (and **`from_unixtime`** for numeric epoch → `datetime`, UTC naive). **Week semantics:** `weekofyear` / core **`dt_week`** use **ISO 8601** week number (Polars `dt.week()`); `dayofweek` is **ISO weekday** (Monday = 1 … Sunday = 7). Compare Spark’s `weekofyear` / `dayofweek` definitions if you need exact JVM parity.

**String / numeric:** `lower`, `upper`, `trim` (core `Expr.strip`); `abs`, `round`, `floor`, `ceil`.

**Global row count:** `pydantable.expressions.global_row_count()` or `functions.count()` with no argument.

**Deferred:** **`current_date`** / **`current_timestamp`** as lazy plan literals (no clock node today). Spark’s optional **`from_unixtime` format string** is not modeled — use parsing helpers on string columns instead.

## Phase D — Aggregates as `functions.sum(Column)`

**Global aggregates:** `functions.sum(F.col("x", dtype=int))` / `avg` / `mean` build
`ExprNode::GlobalAgg` nodes. Use them in **`DataFrame.select(...)`** (positional or
keyword) to get a **single-row** frame, matching Spark’s `select(F.sum(...))` without a
`groupBy`. **Grouped** aggregations remain **`group_by(...).agg(...)`**.

**Row count without a column:** `global_row_count()` or `functions.count()` with no argument in global `select`.

## Phase E — Windows

Delivered: **Rust `ExprNode::Window`** with Polars `.over(...)` lowering; Python
`row_number()`, `rank()`, `dense_rank()`, `window_sum()`, `window_mean()`, `lag()`, `lead()` finished with
`Window.partitionBy(...).orderBy(...)` / `.spec()` (see `pydantable.window_spec` and
`pydantable.pyspark.sql.window`). **`row_number` requires `order_by`** in the window spec
(Spark-style ordering). **`lag` / `lead` require `order_by`.**
Framed status:
- `rowsBetween`: supported for `row_number`, `rank`, `dense_rank`, `window_sum`, `window_avg`, `window_min`, `window_max`, `lag`, and `lead`.
- `rangeBetween`: supported for `window_sum`, `window_avg`, `window_min`, and `window_max` with **multi-column** `orderBy`: range offsets use the **first** key only; see [`WINDOW_SQL_SEMANTICS.md`](/semantics/window-sql-semantics.md).
- Unsupported framed combinations raise typed errors.

## Phases F–G — Nested types and real Spark

- **F:** `ArrayType` / `MapType` / nested structs imply a v2 columnar schema contract in Rust.
- **G:** A `PySparkBackend` that translates logical plans to `pyspark.sql.DataFrame` is a
  separate product track from façade completeness.
