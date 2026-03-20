# PySpark API parity (pydantable.pyspark)

This matrix compares **Apache Spark** `pyspark.sql` concepts to **pydantable**’s
facade. It is not a guarantee of behavioral identity with Spark.

| Spark API area | Pydantable status | Notes |
|----------------|-------------------|--------|
| `SparkSession`, `spark.sql(...)` | **Out of scope** | No distributed engine or SQL parser in pydantable. |
| `DataFrame.select`, `filter`, `where` | **Supported** | Typed `Expr`; `where` mirrors Spark. |
| `DataFrame.withColumn` | **Supported** | |
| `DataFrame.join` | **Supported** | Suffix/collision rules per `INTERFACE_CONTRACT.md`. |
| `DataFrame.groupBy.agg` | **Supported** | Tuple specs, not Spark `agg(expr)` only. |
| `DataFrame.orderBy` / `sort` | **Supported** | Column names + ascending flags; see core `DataFrame`. |
| `DataFrame.limit` | **Supported** | |
| `DataFrame.drop` | **Supported** | Drop by column name(s). |
| `DataFrame.distinct` | **Supported** | All-column distinct rows. |
| `DataFrame.withColumnRenamed` | **Supported** | Single rename per call (or use `rename` with dict). |
| `DataFrame.union` / `unionAll` | **Raises** | `NotImplementedError` until vertical concat exists in the planner. |
| `functions.lit`, `functions.col` | **Supported** | `col` requires `dtype=` or use `df.col()`. |
| `functions.isnull`, `isnotnull`, `coalesce` | **Supported** | Via Rust `ExprNode`. |
| `functions.when` / `otherwise` | **Supported** | `CaseWhen` in Rust; chain `.when(...).otherwise(...)`. |
| `functions.cast`, `between`, `isin`, `concat`, `substring`, `length` | **Supported** | Base types only; `substring` is 1-based (Spark-style). |
| `functions.sum`, `avg`, … as column exprs | **Use `group_by().agg`** | See Phase D below. |
| `Column.cast`, `isin`, `between`, `substr`/`char_length` | **Supported** | On `Expr` / `Column`; date/timestamp casts **not yet**. |
| `Window`, window functions | **Not yet** | Phase E: new plan + Polars windows. |
| `types` (Array, Map, nested Struct, Decimal, Timestamp) | **Partial** | Flat scalars only in engine; Phase F. |
| `Row`, encoders, streaming | **Out of scope** | |

For execution, the **pyspark** backend name still uses the Rust/Polars path unless
a real PySpark backend is added.

### Phase B status (expression surface)

Delivered in-tree: **`IsNull`**, **`IsNotNull`**, **`Coalesce`**, **`CaseWhen`** (`when` / `otherwise`), **`Cast`**, **`InList`**, **`Between`**, **`StringConcat`**, **`Substring`**, **`StringLength`** — Rust `ExprNode`, Polars lowering, pandas executor, and `pydantable.pyspark.sql.functions` / `Expr` methods.

**Deferred (still Phase B-adjacent):** rich **date/timestamp** expression helpers and other Spark string functions beyond concat / substring / length.

### Phase D — Aggregates as `functions.sum(Column)`

PySpark allows `select(F.sum("amount"))` without an explicit `groupBy`. Pydantable keeps
aggregations on **`group_by(...).agg(out_name=("sum"|"mean"|"count", column))`** for now.
`functions.sum` / `avg` / … raise with a message pointing at `group_by().agg`. Adding
lazy aggregate expressions would duplicate group-by semantics in the Rust planner (see
roadmap).

### Phase E — Windows

`Window`, `WindowSpec`, and functions like `row_number()` require a window plan/expression
model and Polars window lowering—not started.

### Phases F–G — Nested types and real Spark

- **F:** `ArrayType` / `MapType` / nested structs imply a v2 columnar schema contract in Rust.
- **G:** A `PySparkBackend` that translates logical plans to `pyspark.sql.DataFrame` is a
  separate product track from façade completeness.
