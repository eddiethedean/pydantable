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
- **`describe()`** (**0.20.0+**) returns a multi-line **string** after one **`to_dict()`** pass: **int** / **float** (mean, min, max, std where applicable), **bool** (true/false/null counts), **str** (counts, **`n_unique`**, min/max string length, nulls), **`date`** / **`datetime`** (non-null count, min, max, null count). Other dtypes are omitted.
- **`value_counts(column)`** (**0.20.0+**) returns per-value counts via group-by aggregation (engine path) as a Python **`dict`** (value → count), not a pandas **`Series`**; optional **`normalize=True`** returns fractions. The pandas UI does not change this core return type—see {doc}`PANDAS_UI` (**Core `value_counts`**).

## Join semantics (collision + keys)

### Join keys
- `join(on=...)` requires at least one join key.
- `on` must reference a column that exists on both sides.
- `join(left_on=..., right_on=...)` supports column names or single-column expressions.
- `Selector` objects are supported for join keys (schema-first, deterministic):
  - `join(on=Selector)`: selector resolves against the **left** schema; it must match at least one column, and every resolved name must also exist in the **right** schema.
  - `join(left_on=Selector, right_on=Selector)`: each selector resolves against its own side; both must be non-empty and resolve to the **same number** of columns.
  - If a selector matches no columns, `ValueError` is raised including available columns.
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
  - **Supported** for `left_on` / `right_on` **column-name keys** (including multi-key) with join kinds: `inner`, `left`, `right`, `semi`, `anti`, `full`.\n    - For `semi`/`anti`, coalesce is accepted but has no observable effect (left-only output).\n    - For `full`, coalescing is supported only when the key base dtypes match exactly (no casts); the coalesced output key is nullable.
  - Produces exactly **one key column per key pair**:
    - `inner` / `left`: keeps the **left** key name(s) and drops the right key column(s).
    - `right`: keeps the **right** key name(s) and drops the left key column(s) (so right-only rows still have a non-null key).
  - **Not supported**:
    - `cross` joins
    - expression keys beyond simple `ColumnRef` (computed expressions) because output key naming is not guaranteed to be stable.
- **`coalesce=False`**:\n  - For side-specific name keys, attempts to preserve **both** key columns when it is schema-safe (no output name collisions).\n  - Some combinations may raise `NotImplementedError` with guidance.

### Join null-key matching (`join_nulls=...`)

`join(join_nulls=...)` controls whether **null join keys match** each other (Polars `nulls_equal`):

- **Default (`None`)**: uses the engine default (**null keys do not match**).
- **`join_nulls=True`**: null keys are considered equal for join matching.
- **`join_nulls=False`**: null keys do not match.

Supported on **in-memory roots and scan roots**.

### Join output ordering (`maintain_order=...`)

`join(maintain_order=...)` controls whether join output preserves a deterministic order:

- Allowed values: `None`, `True`/`False`, or one of `"none"`, `"left"`, `"right"`.
  - `True` maps to `"left"`, `False` maps to `"none"`.
- Supported on **in-memory roots and scan roots**.

### Join parallelism flags (`allow_parallel=...`, `force_parallel=...`)

These arguments are accepted for parity planning but are currently **not implemented** in this build. Passing either raises `NotImplementedError`.

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
- **PySpark UI `DataFrame.count()`** (frame action, not grouped): returns **`int`**
  using **`global_row_count()`** in the logical plan (same semantics as
  `select(global_row_count())`).
- **PySpark UI `unionByName` / `intersect` / `subtract` / `exceptAll`:** implemented
  over core **`concat`**, **`join`**, **`distinct`**, and typed column alignment — not
  separate distributed physical operators. **`exceptAll`** is an alias of **`subtract`**
  on the façade and does **not** implement Apache Spark multiset **`EXCEPT ALL`**
  semantics.
- **Typed NULL literal cast:** **`Literal(None).cast(T)`** (equivalently **`cast_expr`** on a
  **`None`** literal) lowers to a **nullable** scalar of the target base type, so missing
  columns in **`unionByName(..., allowMissingColumns=True)`** can be filled with SQL NULL
  while keeping engine typing consistent.
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
- `pivot_longer` / `pivot_wider` (aliases)
- `pivot`
- `explode` and `unnest` API entrypoints
- `explode_all` / `unnest_all` (schema-driven helpers)

## Core convenience helpers (schema-first)

### `with_row_count`

`with_row_count(name="row_nr", offset=0)` adds a deterministic row number column:

- Output schema adds a non-nullable `int` column named `name`.
- `offset` controls the starting value; `offset >= 0` is required.
- This is implemented in the Rust plan so it works for in-memory and scan roots.

### `clip`

`clip(lower=..., upper=..., subset=...)` clamps numeric values:

- By default (`subset=None`), clamps all schema-numeric columns.
- `subset` accepts a column name, a sequence of names, or a schema-driven `Selector`.
- Non-numeric subset columns raise `TypeError`.

### `drop_nulls` (row filter)

`drop_nulls(subset=..., how=..., threshold=...)` filters rows based on nulls:

- `subset` accepts a column name, a sequence of names, or a schema-driven `Selector`.
- `how="any"` (default): drop rows with **any** null in the subset (require all non-null).
- `how="all"`: drop rows only if **all** subset values are null (require at least one non-null).
- `threshold=n`: keep rows with at least `n` non-null values in the subset.

`melt` / `unpivot`:
- `id_vars` are preserved as-is.
- `variable_name` is always non-nullable `str`.
- `value_name` requires all source `value_vars` to share a compatible scalar base dtype.
- `value_vars` cannot overlap with `id_vars`.
- `id_vars` and `value_vars` accept schema-driven `Selector` objects (resolved against the current schema). If a selector matches no columns, `ValueError` is raised with a list of available columns.
- `id_vars` / `value_vars` also accept a single column name (`str`) as a convenience.

`pivot`:
- Requires `index`, `columns`, and at least one `values` column.
- Supports aggregate functions: `count`, `sum`, `mean`, `min`, `max`, `median`, `std`, `var`, `first`, `last`, `n_unique`.
- Numeric aggregates (`sum`, `mean`, `median`, `std`, `var`) require numeric value dtypes.
- Generated output columns use deterministic names:
  - single value column: `<pivot_value><separator><agg>`
  - multiple value columns: `<pivot_value><separator><value_col><separator><agg>`
 - `sort_columns=True` sorts pivot values before generating output columns.
 - `pivot_values=[...]` (when provided) fixes the pivot-value set and output column order; `sort_columns` is ignored.
 - `separator` controls output naming (default `"_"`).
 - `index` and `values` accept schema-driven `Selector` objects (resolved against the current schema).
 - `columns` accepts a schema-driven `Selector` only when it matches **exactly one** column; otherwise `ValueError` is raised.

`explode` / `unnest`:
- **Homogeneous list** columns (`list[T]` / `List[T]` with supported `T`) are modeled end-to-end; `explode(columns)` unwraps **one** list level and updates the schema to the inner dtype (**always nullable** after explode, matching Polars’ post-explode nullability for element cells). Execution uses Polars `explode` with `empty_as_null=false` and `keep_nulls=true` (same defaults as the Rust engine’s Polars call).
- **Multi-column explode:** all named columns must be list-typed; Polars requires **matching list lengths per row**. Mismatched lengths for the same row raise at execution (contract-tested).
- **Empty lists:** an empty list cell yields **no output rows** for that input row (Polars behavior); other columns are not replicated for that row.
- **`unnest`** for **struct** columns (nested model columns) uses Polars `unnest` with separator **`_`**: each struct field becomes a top-level column named **`{parent}_{field}`** (e.g. `addr_street`). The logical schema follows that naming; struct nullability is propagated to field columns per the Rust descriptor rules. See [`SUPPORTED_TYPES.md`](SUPPORTED_TYPES.md).
- `explode(columns=...)` and `unnest(columns=...)` accept schema-driven `Selector` objects; an empty match raises `ValueError` with available columns.

## Row-wise expression evaluation (`polars_engine` disabled)

When the extension is built **without** the Polars execution engine, a small subset of `ExprNode::eval` runs for tests and offline paths. **List** expressions (`list_len`, `list_get`, `list_contains`, `list_min` / `list_max` / `list_sum`) and **struct** projection (`struct_field`, **`struct_json_encode`**, **`struct_json_path_match`**, **`struct_rename_fields`**, **`struct_with_fields`**) are **not** implemented there (no list/struct cell representation in the row-wise literal context). **`ListMean`**, **`ListJoin`**, **`ListSort`**, **`ListUnique`**, **`StringSplit`**, **`StringExtract`** (regex), **`StringJsonPathMatch`**, and **`StringJsonDecode`** are Polars-only in practice (same bucket as other list-returning / non-scalar-literal paths). **`StringReplace`** with **`literal=False`** (regex) and **`StringPredicate`** with regex mode are **not** evaluated in this stub; use Polars-backed **`collect()`** / **`to_dict()`**. Literal **`StringPredicate`** (starts/ends/substring contains) is implemented for string-like literals. **`TemporalPart`** extractors are implemented for **UTC** wall time from `datetime` microsecond literals and for `date` cells encoded as Polars/Python day offsets (**including `weekday` / `quarter` / ISO `week`** for `date` / `datetime` literals). Row-wise eval also implements simple UTF-8 **`str_reverse`**, **`str_pad_start` / `str_pad_end`**, and **`str_zfill`** on string literals; use Polars-backed execution to match engine edge cases exactly.

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
- **Struct JSON and reshaping:** `Expr.struct_json_encode()`, `Expr.struct_json_path_match(path)`, `Expr.struct_rename_fields(names)`, and `Expr.struct_with_fields(...)` are **Polars-only** (same materialization requirement as other struct ops). **`struct_rename_fields`** requires one new name per subfield (unique names); **`struct_with_fields`** requires at least one keyword **`field=Expr`**.
- **String → typed JSON:** `Expr.str_json_decode(dtype)` (Polars **`str.json_decode`**) parses **JSON text** per row into a **struct** (nested model) or **map** dtype given at plan build time. Null string cells decode to null. With Polars **0.53**, **invalid JSON in any row typically fails `collect()`** for that plan (unlike **`str_json_path_match`**, which usually nulls bad cells). **Map** targets use list-of-`{key,value}` JSON (e.g. `[{"key":"a","value":1}]`), not a bare JSON object string.

Rolling/dynamic contracts:
- Rolling windows support grouped trailing windows with deterministic ordering by `on` (and optional `by` keys).
- Dynamic windows support `every` / `period` with `s/m/h/d` suffixes and explicit aggregation contracts.
- Nulls are ignored for numeric aggregations; all-null windows yield `None` for nullable aggregates and `0` for `count`.

## Local lazy file scans (multi-file and `glob`)

- Lazy **`read_*` / `aread_*`** roots delegate **file discovery, glob expansion, and hive-style path handling** to **Polars** according to the Rust **`scan_kwargs`** pydantable forwards (see {ref}`Polars 0.53 vs pydantable scan audit <local-io-audit>`). **Parquet** scans can request **`include_file_paths`**, **`hive_partitioning`**, and **`row_index_*`** via **`scan_kwargs`**. **CSV** scans can request **`include_file_paths`**, **`row_index_*`**, and other documented **`LazyCsvReader`** options via **`scan_kwargs`**. **NDJSON** scans can request **`glob`** ( **`glob=False`** raises **`ValueError`** ), **`include_file_paths`**, and **`row_index_*`** via **`scan_kwargs`**. **Schema union across files** and **partition column dtypes** follow Polars for the pinned version; **`HiveOptions.schema`** overrides are **not** exposed yet.
- **Typed validation** (**`trusted_mode`**, strict cell checks, optional-column filling, …) applies at **materialization** (**`collect()`**, **`to_dict()`**, **`to_arrow()`**, …), not when the **`ScanFileRoot`** is constructed.

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

