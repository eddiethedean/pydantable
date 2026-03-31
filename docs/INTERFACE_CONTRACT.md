# PydanTable Interface Contract (Polars-style)

This document records the behavior contract that the typed DataFrame API guarantees.
It is intended to be independent of Python import style (default vs `pandas` / `pyspark` UI)
at the *type/semantics* level, while allowing implementation-specific physical ordering
(e.g. row order from `to_dict()` / `collect(as_lists=True)`).

## Semver and stability (0.x / 1.x)

Versioning expectations (0.x and 1.x policy, extension alignment) are summarized in {doc}`VERSIONING`. **This document** records what the library **does** for joins, nulls, windows, grouped aggregation, trusted ingest, async materialization, and Arrow interchange—not which release added each surface. For **1.x**, this file is the behavioral source of truth referenced by the semver policy.

**Row models:** `collect()` with default arguments returns a **list of Pydantic models**;
order of that list follows the same non-guarantee as columnar materialization unless
documented otherwise for a specific op.

(ordering)=
## Ordering

Columnar materialization (`to_dict()`, `collect(as_lists=True)`) output order is **not a stable API guarantee**.

When tests or user assertions need deterministic comparisons, compare on the
subset of columns that define identity (for example, join keys) rather than
row position. The project test-suite uses sorted comparisons to enforce this.

### Schema-driven selection ergonomics

Some schema-first helpers exist to reduce verbosity when working with explicit column lists:

- **`select(exclude=...)`**: remove columns from a projection using names or `Selector` objects.\n  If `select()` is called with **no** positional columns, `exclude=...` means “everything except …”.\n  `exclude` is not supported for global-aggregate `select(...)` calls.\n- **Column reordering**: `reorder_columns(...)`, `select_first(...)`, `select_last(...)`, and `move(..., before=.../after=...)` reorder columns without computing new ones.\n- **Rename convenience**: `rename_prefix(...)`, `rename_suffix(...)`, `rename_replace(...)`, and `rename_with_selector(...)` build deterministic rename maps; collisions raise `ValueError`.\n+
### `maintain_order`

Some operations accept `maintain_order=...` for Polars parity:

- **`sort(..., maintain_order=True)`**: requests stable ordering for ties (rows with equal sort keys keep their first-appearance order).
- **`unique(..., maintain_order=True)`**: requests stable “first appearance” semantics for which duplicate is retained (subject to `keep=`).
- **`group_by(..., maintain_order=True)`**: requests stable group key output ordering (groups emitted in first-appearance order).

These flags are supported by the Polars engine. When you need deterministic comparisons in tests, prefer key-sorted comparisons even when `maintain_order=True` (see above), because downstream operations may still affect physical ordering unless explicitly documented.

## Introspection (`shape`, `columns`, `dtypes`, `info`, `describe`)

- **`columns`** and **`dtypes`** reflect the **current logical schema** (projected column names and Pydantic field annotations).
- **`shape`** and **`empty`** are derived from the **root** ingested column buffers when present (same idea as the pandas UI table in {doc}`PANDAS_UI`). When the logical plan applies filters or other transforms **without** replacing that root buffer, **`shape[0]`** may **not** equal the number of rows returned by **`to_dict()`**, **`collect()`**, or **`head()`** after execution. Use materialized output for an accurate row count.
- **`info()`** returns a multi-line **string** summarizing column names, dtypes, and row count consistent with the **`shape`** policy above (not a full **`collect()`** unless documented elsewhere).
- **`describe()`** (**0.20.0+**) returns a multi-line **string** after one **`to_dict()`** pass: **int** / **float** (mean, min, max, std where applicable), **bool** (true/false/null counts), **str** (counts, **`n_unique`**, min/max string length, nulls). Other dtypes are omitted.
- **`value_counts(column)`** (**0.20.0+**) returns per-value counts via group-by aggregation (engine path) as a Python **`dict`** (value → count), not a pandas **`Series`**; optional **`normalize=True`** returns fractions. The pandas UI does not change this core return type—see {doc}`PANDAS_UI` (**Core `value_counts`**).

## Join semantics (collision + keys)

### Join keys
- `join(on=...)` requires at least one join key.
- `on` must reference a column that exists on both sides.
- `join(left_on=..., right_on=...)` supports column names or single-column expressions.
- `cross` joins do not accept `on`/`left_on`/`right_on`.

### Join validation (`validate=...`)

`join(validate=...)` performs an explicit join cardinality check:

- Allowed values: `one_to_one`, `one_to_many`, `many_to_one`, `many_to_many` (also accepts `1:1`, `1:m`, `m:1`, `m:m`).
- Supported on **in-memory roots and scan roots**.
  - **Cost note:** validation is implemented via engine operations on the join keys; it may require an additional collect-like pass and can be expensive on large scans.
  - Keep it explicit (do not enable by default) and document it in service code when used.

### Join key coalescing (`coalesce=...`)

`join(coalesce=...)` controls whether side-specific join keys (`left_on` / `right_on`) are merged into a single observable key column.

- **`coalesce=None` (default)**: preserve current behavior.
- **`coalesce=True` (typed-safe)**:
  - **Supported** for `left_on` / `right_on` **column-name keys** (including multi-key) with join kinds: `inner`, `left`, `right`.
  - Produces exactly **one key column per key pair**:
    - `inner` / `left`: keeps the **left** key name(s) and drops the right key column(s).
    - `right`: keeps the **right** key name(s) and drops the left key column(s) (so right-only rows still have a non-null key).
  - **Not supported**:
    - `cross` joins
    - `full`/`outer` joins with side-specific keys (requires explicit nullability widening rules)
    - expression keys (`left_on=df.col_expr`, `right_on=...`) because output key naming is not guaranteed to be stable.
- **`coalesce=False`**: accepted for Polars parity; currently a no-op under the schema-first contract.

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

### Filter-oriented Expr helper conveniences

The core `Expr` surface includes a few small helpers commonly used for `filter(...)` predicates:

- **Aliases**: `Expr.is_in(...)` is an alias of `Expr.isin(...)`.
- **String predicates**: `is_empty_str`, `is_blank_str`, `matches` (Rust regex dialect), and null-friendly combinations like `is_null_or_empty_str`.
- **List/map predicates**: `contains_any` / `contains_all` on list columns (composed from `list_contains`), plus `list_is_empty`, `map_is_empty`, and `map_has_any_key`.

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

**Polars runtime errors (0.18.0+):** On **`collect()`** failure during **`group_by().agg()`**, the raised **`ValueError`** may include **`(group_by().agg())`** in the message (Rust **`polars_err_ctx`**) so the error is attributable to grouped aggregation. This does not change the aggregation rules above; see {doc}`EXECUTION`.

### `drop_nulls`

`group_by(..., drop_nulls=...)` controls whether null key rows participate in grouping:

- `drop_nulls=True` (default): rows where **any** grouping key is null are excluded from grouping.
- `drop_nulls=False`: null-key groups are retained (Polars parity).

## Duplicate row detection

- **`unique(subset=..., keep="first"|"last")`**: deduplicates to one row per distinct key (all columns participate when **`subset`** is omitted); pandas UI **`drop_duplicates(..., keep="first"|"last")`** maps here.
- **`duplicated(subset=..., keep="first"|"last"|False)`**: returns a single-column boolean frame **`duplicated`** with pandas-aligned semantics (**`keep=False`** marks every row that belongs to a non-unique group).
- **`drop_duplicate_groups(subset=...)`**: removes **all** rows whose key appears in a duplicate group (equivalent to pandas **`drop_duplicates(keep=False)`** on the pandas UI).

These compile to Rust plan steps when the Polars engine is enabled; the row-wise executor implements the same semantics when Polars is disabled. See {doc}`PANDAS_UI` (**`drop_duplicates` / `duplicated`**) and tests **`tests/test_pandas_ui.py`**, **`tests/test_pandas_ui_popular_features.py`**.

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
- `id_vars` and `value_vars` accept schema-driven `Selector` objects (resolved against the current schema).

`pivot`:
- Requires `index`, `columns`, and at least one `values` column.
- Supports aggregate functions: `count`, `sum`, `mean`, `min`, `max`, `median`, `std`, `var`, `first`, `last`, `n_unique`.
- Numeric aggregates (`sum`, `mean`, `median`, `std`, `var`) require numeric value dtypes.
- Generated output columns use deterministic names:
  - single value column: `<pivot_value><separator><agg>`
  - multiple value columns: `<pivot_value><separator><value_col><separator><agg>`
 - `sort_columns=True` sorts pivot values before generating output columns.
 - `separator` controls output naming (default `"_"`).

`explode` / `unnest`:
- **Homogeneous list** columns (`list[T]` / `List[T]` with supported `T`) are modeled end-to-end; `explode(columns)` unwraps **one** list level and updates the schema to the inner dtype (**always nullable** after explode, matching Polars’ post-explode nullability for element cells). Execution uses Polars `explode` with `empty_as_null=false` and `keep_nulls=true` (same defaults as the Rust engine’s Polars call).
- **Multi-column explode:** all named columns must be list-typed; Polars requires **matching list lengths per row**. Mismatched lengths for the same row raise at execution (contract-tested).
- **Empty lists:** an empty list cell yields **no output rows** for that input row (Polars behavior); other columns are not replicated for that row.
- **`unnest`** for **struct** columns (nested model columns) uses Polars `unnest` with separator **`_`**: each struct field becomes a top-level column named **`{parent}_{field}`** (e.g. `addr_street`). The logical schema follows that naming; struct nullability is propagated to field columns per the Rust descriptor rules. See [`SUPPORTED_TYPES.md`](SUPPORTED_TYPES.md).

## Row-wise expression evaluation (`polars_engine` disabled)

When the extension is built **without** the Polars execution engine, a small subset of `ExprNode::eval` runs for tests and offline paths. **List** expressions (`list_len`, `list_get`, `list_contains`, `list_min` / `list_max` / `list_sum`) and **struct field** projection are **not** implemented there (no list cell representation in the row-wise literal context). **`ListMean`**, **`ListJoin`**, **`ListSort`**, **`ListUnique`**, **`StringSplit`**, **`StringExtract`** (regex), and **`StringJsonPathMatch`** are Polars-only in practice (same bucket as other list-returning / non-scalar-literal paths). **`StringReplace`** with **`literal=False`** (regex) and **`StringPredicate`** with regex mode are **not** evaluated in this stub; use Polars-backed **`collect()`** / **`to_dict()`**. Literal **`StringPredicate`** (starts/ends/substring contains) is implemented for string-like literals. **`TemporalPart`** extractors are implemented for **UTC** wall time from `datetime` microsecond literals and for `date` cells encoded as Polars/Python day offsets (**including `weekday` / `quarter` / ISO `week`** for `date` / `datetime` literals). Row-wise eval also implements simple UTF-8 **`str_reverse`**, **`str_pad_start` / `str_pad_end`**, and **`str_zfill`** on string literals; use Polars-backed execution to match engine edge cases exactly.

## Window and time-series semantics

Supported P6 API surface:
- Generic **`Expr.over(partition_by=..., order_by=...)`** (keyword form) is **not supported**: passing either argument raises **`TypeError`**. Use **named window functions** with **`Window.partitionBy(...).orderBy(...)`** (and optional `.rowsBetween` / `.rangeBetween`).
- **Named window functions** (`row_number`, `rank`, `dense_rank`, `window_sum`, `window_mean`, `window_min`, `window_max`, `lag`, `lead`) use Rust `ExprNode::Window` and lower to Polars **`.over_with_options(..., WindowMapping::default())`** when no frame is present. **`Window.orderBy(..., nulls_last=...)`** controls **NULLS FIRST / LAST** per sort key (default **`nulls_last=False`**). Unframed multi-key windows: Polars allows only **one** **`SortOptions`** for the combined sort—if sort keys disagree on **`ascending`** or **`nulls_last`**, pydantable raises **`ValueError`** (use matching flags on all keys or a framed window, which honors per-key placement; see {doc}`WINDOW_SQL_SEMANTICS`). Framed windows use a dedicated executor fallback path:
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

## Arrow interchange (0.16.0)

- **`pydantable.io.materialize_parquet`** / **`materialize_ipc`** feed **`dict[str, list]`** into **`DataFrameModel(...)`** / **`DataFrame(...)`** constructors. **`materialize_ipc(..., as_stream=True)`** selects the **streaming** IPC format; default is **file** IPC. For lazy local files use **`read_*`** + **`DataFrame.write_parquet`** ({doc}`EXECUTION`).
- **`DataFrame.to_arrow`** / **`DataFrame.ato_arrow`:** same logical materialization as **`to_dict`**, then build a PyArrow **`Table`** in Python (**not** a zero-copy view of internal Polars buffers). **`DataFrameModel`** exposes the same methods by delegation.
- **Constructors:** **`pyarrow.Table`** and **`RecordBatch`** are accepted when **`pyarrow`** is installed (converted to Python lists before validation); see {doc}`SUPPORTED_TYPES`.

## Four terminal materialization modes

Lazy **`DataFrame`** / **`DataFrameModel`** plans are materialized through one of **four** scheduling patterns: **blocking** (**`collect`**, **`to_dict`**, …), **async** (**`acollect`**, **`ato_dict`**, …), **deferred** (**`submit`** → **`ExecutionHandle`**), or **chunked** (**`stream`** / **`astream`**). They share engine semantics; see {doc}`MATERIALIZATION` for the full table and **`PlanMaterialization`** labels.

## Async materialization, `submit`, `stream`, and `astream` (1.6.0+)

- **`acollect`** / **`ato_dict`** / **`ato_polars`** / **`ato_arrow`:** same logical result as the synchronous methods. Ordering of rows in columnar output follows the same **non-guarantee** as **`to_dict()`** ({ref}`ordering`).
- **`submit` → `ExecutionHandle`:** **`await handle.result()`** is equivalent to **`collect()`** with the same keyword arguments. **`handle.cancel()`** only affects the wait on a **`concurrent.futures.Future`** when cancellation wins the race before work starts; it does **not** cooperatively abort an in-flight Polars **`collect`**.
- **`stream`** / **`astream`:** perform **one** terminal engine materialization, then yield **`dict[str, list]`** batches of adjacent rows (same slicing contract as **`collect_batches`**). **`stream`** is synchronous (for **`def`** routes and **`StreamingResponse`**); **`astream`** is async. This is **chunked replay**, not a guarantee of bounded memory for unbounded scans. Batch order follows the same row-order policy as **`to_dict()`** unless the plan includes an explicit **`sort`**. Requires the **`polars`** Python package for chunk conversion.

## Polars `LazyFrame` escape hatch (deferred)

The compiled extension executes with a Rust Polars **`LazyFrame`**, which is **not** interchangeable with **`polars.LazyFrame`** in Python. A **`to_polars_lazy()`**-style API would need **`pyo3-polars`** (or similar) or a portable serialized plan—not shipped in core today.

**Interoperability patterns:** keep pipelines in pydantable with **`read_*`** / **`read_parquet_url`** + transforms + **`write_parquet`**, **`write_csv`**, **`write_ipc`**, **`write_ndjson`**, or **`collect()`** / **`to_dict()`** / **`to_polars()`**; use **`materialize_*`** when you want a Python column dict first; use **`polars.scan_parquet`** (etc.) when the entire workload should stay in native Polars.

## Related documentation

- **Multi-key `RANGE` window frames:** {doc}`WINDOW_SQL_SEMANTICS` (sort keys vs range axis on the first `orderBy` column).
- **Trusted ingest (`trusted_mode`):** {doc}`DATAFRAMEMODEL`, {doc}`SUPPORTED_TYPES`.
- **Materialization (sync + async + four modes):** {doc}`MATERIALIZATION`, {doc}`EXECUTION`, {doc}`ROADMAP`, {doc}`FASTAPI`.
- **Versioning (0.x):** {doc}`VERSIONING`.

## Migration Notes (Polars -> PydanTable)

- Keep schema-first modeling: declare columns on `DataFrameModel` before transforms.
- Prefer `out_name=(op, column)` aggregation specs instead of ad-hoc expression maps.
- Treat row order as non-contractual unless explicitly sorted before assertions.
- Use typed nullability (`T | None`) consistently; output schema nullability is contract-driven.
- For reshape workflows, follow deterministic output naming contracts documented in this file.

