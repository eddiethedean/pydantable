# PySpark API parity (pydantable.pyspark)

This matrix compares **Apache Spark** `pyspark.sql` concepts to **pydantable**’s
facade. It is not a guarantee of behavioral identity with Spark.

For how to import and use the PySpark-style `DataFrame` and `sql` package, see
{doc}`PYSPARK_UI`.

| Spark API area | PydanTable status | Notes |
|----------------|-------------------|--------|
| `SparkSession`, `spark.sql(...)` | **Out of scope** | No distributed engine or SQL parser in pydantable. |
| `DataFrame.select`, `filter`, `where` | **Supported** | Typed `Expr`; `where` mirrors Spark. |
| `DataFrame.withColumn` | **Supported** | |
| `DataFrame.join` | **Supported** | Suffix/collision rules per `INTERFACE_CONTRACT.md`. |
| `DataFrame.groupBy.agg` | **Supported** | Tuple specs, not Spark `agg(expr)` only. |
| `DataFrame.orderBy` / `sort` | **Supported** | Column names + ascending flags; see core `DataFrame`. |
| `DataFrame.limit` | **Supported** | |
| `DataFrame.drop` | **Supported** | Drop by column name(s). |
| `DataFrame.distinct` | **Supported** | All-column distinct rows; optional `subset=` matches core `distinct`. |
| `DataFrame.withColumnRenamed` | **Supported** | Single rename per call (or use `rename` with dict). |
| `DataFrame.union` / `unionAll` | **Supported** | Vertical concat via core `concat(..., how="vertical")`. |
| `functions.lit`, `functions.col` | **Supported** | `col` requires `dtype=` or use `df.col()`. |
| `functions.isnull`, `isnotnull`, `coalesce` | **Supported** | Via Rust `ExprNode`. |
| `functions.when` / `otherwise` | **Supported** | `CaseWhen` in Rust; chain `.when(...).otherwise(...)`. |
| `functions.cast`, `between`, `isin`, `concat`, `substring`, `length` | **Supported** | Base types only; `substring` is 1-based (Spark-style). |
| `functions.sum`, `avg`, … as column exprs | **Partial** | Global `sum`/`avg`/`mean` on a typed `Expr` work in `DataFrame.select(...)` (single-row); grouped paths use `group_by().agg`. |
| `Column.cast`, `isin`, `between`, `substr`/`char_length` | **Supported** | On `Expr` / `Column`; date/timestamp casts **not yet**. |
| `Window`, window functions | **Partial** | `Window.partitionBy().orderBy()`, `row_number`, `rank`, `dense_rank`, `window_sum`, `window_avg` + core `Expr` lowering; framing APIs not exposed. |
| `types` (Array, Map, nested Struct, Decimal, Timestamp) | **Partial** | Engine supports nested structs/lists, `Decimal`, `datetime`/`date`, homogeneous `dict[str, T]` maps, `bytes`, and `time`; PySpark `types` mirrors annotations for docs/schema views. |
| `Row`, encoders, streaming | **Out of scope** | |

For execution, the PySpark UI uses the same Rust/Polars path as the default export.

## Phase B status (expression surface)

Delivered in-tree: **`IsNull`**, **`IsNotNull`**, **`Coalesce`**, **`CaseWhen`** (`when` / `otherwise`), **`Cast`**, **`InList`**, **`Between`**, **`StringConcat`**, **`Substring`**, **`StringLength`** — Rust `ExprNode`, Polars lowering, and `pydantable.pyspark.sql.functions` / `Expr` methods.

**Also delivered:** Spark-named **date/datetime** helpers in `pydantable.pyspark.sql.functions` — `year`, `month`, `day`, `hour`, `minute`, `second`, `to_date` — thin wrappers over core `Expr.dt_*` / `dt_date()` (same Rust lowering as the core API).

**Deferred:** `unix_timestamp`, string-parsed `to_date`, and other Spark-specific temporal helpers without a dedicated `ExprNode` path.

## Phase D — Aggregates as `functions.sum(Column)`

**Global aggregates:** `functions.sum(F.col("x", dtype=int))` / `avg` / `mean` build
`ExprNode::GlobalAgg` nodes. Use them in **`DataFrame.select(...)`** (positional or
keyword) to get a **single-row** frame, matching Spark’s `select(F.sum(...))` without a
`groupBy`. **Grouped** aggregations remain **`group_by(...).agg(...)`**.

**Not yet:** `count(*)` / `count(1)` as a global select expression (use `group_by` or
add a dedicated plan step later).

## Phase E — Windows

Delivered: **Rust `ExprNode::Window`** with Polars `.over(...)` lowering; Python
`row_number()`, `rank()`, `dense_rank()`, `window_sum()`, `window_mean()` finished with
`Window.partitionBy(...).orderBy(...)` / `.spec()` (see `pydantable.window_spec` and
`pydantable.pyspark.sql.window`). **`row_number` requires `order_by`** in the window spec
(Spark-style ordering). Not yet: arbitrary SQL window frames (`rowsBetween`, etc.).

## Phases F–G — Nested types and real Spark

- **F:** `ArrayType` / `MapType` / nested structs imply a v2 columnar schema contract in Rust.
- **G:** A `PySparkBackend` that translates logical plans to `pyspark.sql.DataFrame` is a
  separate product track from façade completeness.
