# PydanTable Interface Contract (Polars-style)

This document records the behavior contract that the typed DataFrame API guarantees.
It is intended to be independent of Python import style (default vs `pandas` / `pyspark` UI)
at the *type/semantics* level, while allowing implementation-specific physical ordering
(e.g. row order from `to_dict()` / `collect(as_lists=True)`).

**Row models:** `collect()` with default arguments returns a **list of Pydantic models**;
order of that list follows the same non-guarantee as columnar materialization unless
documented otherwise for a specific op.

## Ordering

Columnar materialization (`to_dict()`, `collect(as_lists=True)`) output order is **not a stable API guarantee**.

When tests or user assertions need deterministic comparisons, compare on the
subset of columns that define identity (for example, join keys) rather than
row position. The project test-suite uses sorted comparisons to enforce this.

## Join semantics (collision + keys)

### Join keys
- `join(on=...)` requires at least one join key.
- `on` must reference a column that exists on both sides.
- `join(left_on=..., right_on=...)` supports column names or single-column expressions.
- `cross` joins do not accept `on`/`left_on`/`right_on`.

### Collision handling
- Column name collisions introduced by the right-hand side are resolved by
  renaming right-side non-key columns with the provided `suffix` (default:
  `"_right"`).
- Left-hand columns keep their original names.

In other words: collisions on non-key columns become `"<name><suffix>"` for the
right side, while join key columns remain singletons.

### Supported join kinds
- `inner`, `left`, `full`, `right`, `semi`, `anti`, `cross`
- `semi`/`anti` return only left-side columns.

## Null semantics

Null handling is SQL-like (`propagate_nulls`):
- arithmetic: `NULL` + anything yields `NULL`
- comparisons: if either side is `NULL`, the comparison result is `NULL`
- `filter(condition)`: retains rows where the condition evaluates to exactly
  `True`, and drops rows where it is `False` or `NULL`

These rules are enforced in the Rust core so that derived schemas and runtime
values remain aligned.

### Boolean expressions (`&`, `|`, `~`)

`Expr` supports typed boolean combinations (`&`, `|`, `~`) on boolean columns or
comparisons. Null propagation follows **Polars** (Kleene / three-valued) rules for
`and` / `or` / `not`, not only SQL `WHERE` filtering rules. For example, `filter`
still keeps rows where the predicate is exactly `True`; combined expressions may
produce `NULL` boolean cells where operands are null.

## Global aggregates in `select` (whole-frame)

`DataFrame.select(...)` can return a **single-row** frame of **global** aggregates:

- **Positional:** either only global aggregate expressions (e.g. `global_sum(df.v)`,
  `global_row_count()`) **or** only plain column name projections — **not both** in the
  same call.
- **Keyword:** only named global aggregates (each value is an `Expr`, e.g.
  `total=global_sum(df.v)`); cannot mix keywords with positional column names or
  positional aggregates in the same call.
- **`global_row_count()`** counts **rows** in the current logical frame (SQL `COUNT(*)`).
  **`global_count(column)`** counts **non-null** values of that column (SQL `COUNT(col)`).
- **PySpark façade:** `functions.count()` with **no** column argument matches
  **`global_row_count()`**; `functions.count(F.col(...))` matches **`global_count`** on
  that column.
- Global aggregate expressions may also appear in **`with_columns`**, where they lower to
  a scalar **broadcast** to every row (same height as the frame).

## Group-by aggregation semantics (all-null groups)

Supported aggregation operators:
- `count`, `sum`, `mean`, `min`, `max`, `median`, `std`, `var`, `first`, `last`, `n_unique`

For groups where all input values for an aggregated column are `NULL`:
- `sum`, `mean`, `min`, `max`, `median`, `std`, `var`, `first`, and `last` yield `None`
- `count` yields `0`
- `n_unique` yields `0` (SQL-like distinct-count behavior over non-null values)

The output field nullability is preserved/derived accordingly:
- nullable aggregates above are typed as `Optional[...]`
- `count` and `n_unique` outputs remain non-optional integers

## Reshaping semantics

Supported reshape methods:
- `melt` / `unpivot`
- `pivot`
- `explode` and `unnest` API entrypoints

`melt` / `unpivot`:
- `id_vars` are preserved as-is.
- `variable_name` is always non-nullable `str`.
- `value_name` requires all source `value_vars` to share a compatible scalar base dtype.
- `value_vars` cannot overlap with `id_vars`.

`pivot`:
- Requires `index`, `columns`, and at least one `values` column.
- Supports aggregate functions: `count`, `sum`, `mean`, `min`, `max`, `median`, `std`, `var`, `first`, `last`, `n_unique`.
- Numeric aggregates (`sum`, `mean`, `median`, `std`, `var`) require numeric value dtypes.
- Generated output columns use deterministic names:
  - single value column: `<pivot_value>_<agg>`
  - multiple value columns: `<pivot_value>_<value_col>_<agg>`

`explode` / `unnest`:
- **Homogeneous list** columns (`list[T]` / `List[T]` with supported `T`) are modeled end-to-end; `explode(columns)` unwraps **one** list level and updates the schema to the inner dtype (**always nullable** after explode, matching Polars’ post-explode nullability for element cells). Execution uses Polars `explode` with `empty_as_null=false` and `keep_nulls=true` (same defaults as the Rust engine’s Polars call).
- **Multi-column explode:** all named columns must be list-typed; Polars requires **matching list lengths per row**. Mismatched lengths for the same row raise at execution (contract-tested).
- **Empty lists:** an empty list cell yields **no output rows** for that input row (Polars behavior); other columns are not replicated for that row.
- **`unnest`** for **struct** columns (nested model columns) uses Polars `unnest` with separator **`_`**: each struct field becomes a top-level column named **`{parent}_{field}`** (e.g. `addr_street`). The logical schema follows that naming; struct nullability is propagated to field columns per the Rust descriptor rules. See [`SUPPORTED_TYPES.md`](SUPPORTED_TYPES.md).

## Row-wise expression evaluation (`polars_engine` disabled)

When the extension is built **without** the Polars execution engine, a small subset of `ExprNode::eval` runs for tests and offline paths. **List** expressions (`list_len`, `list_get`, `list_contains`, `list_min` / `list_max` / `list_sum`) and **struct field** projection are **not** implemented there (no list cell representation in the row-wise literal context). **`TemporalPart`** extractors are implemented for **UTC** wall time from `datetime` microsecond literals and for `date` cells encoded as Polars/Python day offsets; use the normal Polars-backed `collect()` path for production semantics.

## Window and time-series semantics

Supported P6 API surface:
- Generic **`Expr.over(partition_by=..., order_by=...)`** (keyword form) is **not supported**: passing either argument raises **`TypeError`**. Use **named window functions** with **`Window.partitionBy(...).orderBy(...)`** (and optional `.rowsBetween` / `.rangeBetween`).
- **Named window functions** (`row_number`, `rank`, `dense_rank`, `window_sum`, `window_mean`, `window_min`, `window_max`, `lag`, `lead`) use Rust `ExprNode::Window` and lower to Polars **`.over_with_options(..., WindowMapping::default())`** when no frame is present. **`Window.orderBy(..., nulls_last=...)`** controls **NULLS FIRST / LAST** per sort key (default **`nulls_last=False`**). Unframed multi-key windows: only the **first** key’s flag is passed to Polars **`SortOptions`**; framed windows honor every key (see {doc}`WINDOW_SQL_SEMANTICS`). Framed windows use a dedicated executor fallback path:
  - `rowsBetween(start, end)`: supported for `row_number`, `rank`, `dense_rank`, `window_sum`, `window_mean`, `window_min`, `window_max`, `lag`, and `lead`.
- `rangeBetween(start, end)`: supported for numeric aggregates (`window_sum`, `window_mean`, `window_min`, `window_max`) with **at least one** `orderBy` column. Rows are sorted **lexicographically** by all `orderBy` keys; **range bounds apply only to the first** `orderBy` column, which must be numeric, `date`, `datetime`, or `duration` (PostgreSQL-style multi-column `RANGE`; see [`WINDOW_SQL_SEMANTICS.md`](WINDOW_SQL_SEMANTICS.md)). Bounds use that column’s native unit (for `datetime`/`duration`, microseconds). Identical results across all SQL engines are **not** guaranteed.
  - Unsupported framed combinations raise typed errors.
  - Unframed behavior remains unchanged; **`lag` / `lead`** are implemented as **`shift(±n)`** in that window context and require **`order_by`**.
- `rolling_agg(...)`
- `group_by_dynamic(...).agg(...)` requires **positive** `every` and `period` duration strings (e.g. `every="0s"` raises `ValueError` to avoid infinite loops in the reference dynamic implementation).

Temporal typing:
- Schema descriptors support `datetime`, `date`, `duration`, and **`time`** base types (including nullable variants).
- Temporal descriptors round-trip through Rust schema descriptors into derived Python schema types.

## Struct columns (nested Pydantic models)

- Nested **`Schema` / `BaseModel`** fields are supported as **struct** dtypes in Rust and Polars; see [`SUPPORTED_TYPES.md`](SUPPORTED_TYPES.md) for the descriptor format and expression limits.
- **Pass-through** operations (`select`, `filter`, `sort`, `slice`, `join` on scalar keys, `concat` when schemas align) keep struct columns when the logical schema still matches.
- **Python type identity** after transforms: when a new Rust descriptor matches the previous column’s annotation, pydantable keeps your original nested class; new or renamed columns, or changed dtypes, may use anonymous `create_model` types while preserving validation shape.
- **Struct field access**: `Expr.struct_field(name)` (e.g. `df.addr.struct_field("street")`) projects a scalar field; invalid names fail at expression build time.

Rolling/dynamic contracts:
- Rolling windows support grouped trailing windows with deterministic ordering by `on` (and optional `by` keys).
- Dynamic windows support `every` / `period` with `s/m/h/d` suffixes and explicit aggregation contracts.
- Nulls are ignored for numeric aggregations; all-null windows yield `None` for nullable aggregates and `0` for `count`.

## Related documentation

- **Multi-key `RANGE` window frames:** {doc}`WINDOW_SQL_SEMANTICS` (sort keys vs range axis on the first `orderBy` column).
- **Trusted ingest (`trusted_mode`, legacy `validate_data`):** {doc}`DATAFRAMEMODEL`, {doc}`SUPPORTED_TYPES`.
- **Materialization (sync + async):** {doc}`EXECUTION`, {doc}`ROADMAP`, {doc}`FASTAPI`.

## Migration Notes (Polars -> PydanTable)

- Keep schema-first modeling: declare columns on `DataFrameModel` before transforms.
- Prefer `out_name=(op, column)` aggregation specs instead of ad-hoc expression maps.
- Treat row order as non-contractual unless explicitly sorted before assertions.
- Use typed nullability (`T | None`) consistently; output schema nullability is contract-driven.
- For reshape workflows, follow deterministic output naming contracts documented in this file.

