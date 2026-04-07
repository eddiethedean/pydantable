# Pandas UI (`pydantable.pandas`)

```{warning}
`pydantable.pandas` is **deprecated** and will be **removed in pydantable 2.0**.
Prefer the primary `DataFrameModel` interface from `pydantable`.
```

The **pandas UI** is an optional import surface that layers **pandas-like method and property names** on top of pydantable’s typed logical DataFrame. It does **not** wrap a `pandas.DataFrame` for the schema-driven `DataFrame` / `DataFrameModel` types; execution uses the same **Rust engine** as the default export (see [Execution](EXECUTION.md)).

## Index-light model and lazy vs eager

pydantable is **schema-first** and does not implement a pandas **`Index`** object on the logical frame. For the pandas UI:

- **“Index” operations** (`sort_index`, `set_index`, `reset_index`, `reindex`, `align`, …) use **named key column(s)** that you pass explicitly—there is no hidden row index and no **`MultiIndex`** unless you model it with normal columns (e.g. string keys) or future structured types.
- Some helpers match **pandas names** but are **narrow**: reshape (`stack` / `unstack` / `wide_to_long`), `combine_first` / `update` (join keys required), and `compare` (subset of pandas). See each method’s docstring.
- **Lazy-safe** methods stay on the logical plan (filters, joins, many `with_columns` / **`assign`** paths, windowed **`rank`** when built from expressions, rolling steps executed in Polars).
- **Eager** methods materialize like **`head`** / **`tail`** / **`describe`** / core **`rolling_agg(..., on=..., by=...)`**: they **`collect`/`to_dict`** (or similar) before continuing. Examples: **`sample`**, **`take`**, **`corr`/`cov`** (numeric; NumPy), **`dot`** (numeric; NumPy), **`compare`**, **`transpose`/`T`**, **`get_dummies`** / **`cut`** / **`qcut`** / **`factorize_column`** (scan + **pandas**), **`ewm().mean()`** (via pandas), and half-eager paths noted in the tables below.
- **Typed `pivot` on this façade**: **`pivot(...)`** delegates to the core API (explicit index / columns / values / aggregate)—not pandas’ unconstrained dynamic pivot.

## When to use it

- You prefer **`assign`**, **`merge`**, **`head`/`tail`**, and **`group_by().sum(...)`**-style ergonomics while keeping **Pydantic `DataFrameModel`** typing.
- You import **`pydantable.pandas`** explicitly for those names (or use the default `pydantable` export for Polars-style names).

```python
from pydantable.pandas import DataFrameModel, Expr, Schema

class Sales(DataFrameModel):
    region: str
    amount: int

df = Sales({"region": ["US", "EU"], "amount": [10, 20]})
df2 = (
    df.assign(doubled=df.amount * 2)
    .query("doubled >= 20 and region in ('US', 'EU')")
    .sort_values("doubled", ascending=False)
)
print(df2.to_dict())
```

Output (one run):

```text
{'region': ['US', 'EU'], 'amount': [10, 20], 'doubled': [20, 40]}
```

## Imports

| Symbol | Role |
|--------|------|
| `DataFrame` | Subclass of core `DataFrame` with pandas-style helpers. |
| `DataFrameModel` | Pydantic model whose `.df` / operations use `PandasDataFrame`. |
| `Expr`, `Schema` | Same as top-level pydantable (re-exported for convenience). |

Implementation lives in `python/pydantable/pandas.py` (pandas-flavored helpers and public `DataFrame` / `DataFrameModel`).

## `DataFrame` (pandas UI)

Inherits the full typed API from the core `DataFrame` (`with_columns`, `filter`, `join`, `group_by().agg`, `collect`, …) and adds:

### `concat([...], axis=0|1, join="outer")` / core `how=`

`concat` is available as a classmethod on the pandas UI `DataFrame`:

- `axis=0` stacks rows (**vertical**); both inputs must have identical columns.
- `axis=1` stacks columns (**horizontal**); duplicate column names are rejected.

For parity with the core API, you may pass **`how="vertical"`** or **`how="horizontal"`** instead of `axis` (when set, `how` selects the stacking direction).

Only `join="outer"` is accepted today (typed-first; no column union/reindex semantics).

### `assign(**kwargs)`

Same behavior as **`with_columns`**: each value must be an **`Expr`** or a literal compatible with expression building.

**Not supported:**

- Raw **`pandas.Series`** as a column value

Use column expressions (e.g. `df.x + 1`) instead.

**Supported:** pandas-style callables that return an `Expr` or literal, e.g. `assign(x2=lambda df: df.x * 2)`.

### `merge(other, *, how="inner", on=..., suffixes=("_x", "_y"), ...)`

Maps to **`join`**: `on` is required; the **right suffix** is taken from `suffixes[1]` (default second element `"_y"` → passed as `suffix` to `join`). For **`on=...`** merges with **`how="outer"`**, **`"right"`**, or **`"full"`**, a suffixed duplicate of a join key from the engine (often **`k_right`**) is **coalesced** into the primary key column and dropped.

**Supports:**

- `how="cross"` (Cartesian product). Keys (`on/left_on/right_on`) are rejected for cross joins.
- `left_on` / `right_on` (including different key names). Output uses a pandas-like policy: right key columns are dropped, and for `how="outer"` / `how="right"` the left key columns are filled from the right keys for right-only rows.
- `validate=...` (`one_to_one`, `one_to_many`, `many_to_one`, `many_to_many` and `1:1` / `1:m` / `m:1` / `m:m` aliases).
- `indicator=True` (adds `_merge` with values `left_only` / `right_only` / `both`).
- `copy=...` is accepted for parity (no effect; logical plans are copy-free).
- `left_by` / `right_by` are accepted for parity but currently raise `NotImplementedError`.

**Still raises `NotImplementedError` for:**

- Unsupported `query()` constructs beyond the limited grammar described below.
- `sort=True`, `left_index=True`, `right_index=True`.
  - `sort=True` is now supported for key-based merges (sorts by the join keys). For `how=\"cross\"` and index-merges, `sort=True` still raises `NotImplementedError`.
  - `left_index=True, right_index=True` is supported in a limited way: it materializes both frames to lists, synthesizes a positional index, joins on that, then drops the synthetic index columns.

Unknown keyword arguments raise **`TypeError`**.

**Collision safety:** if a merge would produce duplicate output column names (e.g. because `suffixes[1]` makes a right column collide with an existing left column), `merge()` raises `ValueError` instead of silently overwriting.

**Accepted parameters (summary):**

| Parameter | Status |
|----------|--------|
| `how` | supported (including `"cross"`) |
| `on` / `left_on` / `right_on` | supported (but not with `how="cross"`) |
| `suffixes` | supported (right suffix is used; collisions raise `ValueError`) |
| `validate` | supported |
| `indicator` | supported |
| `copy` | accepted (no-op) |
| `sort` | supported for key-based merges; rejected for cross and index merges |
| `left_index` / `right_index` | supported for `left_index=True, right_index=True` (limited, eager) |
| `left_by` / `right_by` | accepted, raises `NotImplementedError` |

### Introspection

The **default** **`pydantable.DataFrame`** (and **`DataFrameModel`**) now exposes the same **`columns`**, **`shape`**, **`empty`**, **`dtypes`**, **`info()`**, and **`describe()`** helpers (**0.20.0+**). The pandas façade inherits them unchanged; use either import path—see {doc}`INTERFACE_CONTRACT` **Introspection** for **`shape`** vs materialized row count.

| Member | Behavior |
|--------|----------|
| `columns` | List of current logical column names. |
| `shape` | `(n_rows, n_cols)` from root column data. |
| `empty` | `True` when row count is 0. |
| `dtypes` | `dict[str, annotation]` from the current schema (not pandas `dtype` objects). |

### `head(n=5)` / `tail(n=5)`

**Eager**: they **`collect()`** the current plan, slice rows in Python, and return a new `DataFrame` with a fresh identity plan over the sliced data. Not a lazy, zero-copy view.

### `__getitem__`

- **`df["col"]`** → column as **`Expr`** (via `col`).
- **`df[["a", "b"]]`** → **`select("a", "b")`** (non-empty list required).

### `query(str)`

Supports a **limited** boolean expression grammar and compiles to `filter(Expr)`:

- Operators: `== != < <= > >=`, `and` / `or` / `not`, parentheses
- Arithmetic: `+ - * /` (on expressions and literals)
- Membership: `in` / `not in` against **literal** tuples/lists (compiled via `Expr.isin(...)`)
- Column refs: bare identifiers (e.g. `age > 10`)
- Literals: ints, floats, quoted strings, `None`, `True`/`False`

Additionally supports a small whitelist of helper functions (function-call form only; no attribute access):

- `contains(col, "sub")`
- `startswith(col, "pre")`
- `endswith(col, "suf")`
- `isnull(col)` / `notnull(col)`
- `isna(col)` / `notna(col)` (aliases)
- `between(col, low, high)` (inclusive)
- `lower(col)` / `upper(col)` / `strip(col)`
- `len(col)` / `length(col)` (string length)

Unsupported syntax (other function calls, attribute access, subscripts, etc.) raises `NotImplementedError`.

**Accepted parameters (typed-first):**

- `engine`: only `"python"` is supported; other values raise `NotImplementedError`.
- `inplace`: only `False` is supported; `True` raises `NotImplementedError`.
- `local_dict` / `global_dict`: accepted but currently must be `None`/empty; non-empty dicts raise `NotImplementedError`.
  - `local_dict`/`global_dict` now support **literal constant substitution** for names that are not schema columns (ints/floats/strings/bools/None and list/tuple of those).

**Tip:** prefer explicit column names and literals. Example: `query("id in (1,2,3) and amount * 2 >= 10")`.

### `group_by(...)`

Returns **`PandasGroupedDataFrame`**, which adds:

| Method | Equivalent |
|--------|------------|
| `sum("c1", "c2", ...)` | `agg(c1_sum=("sum", "c1"), ...)` |
| `mean(...)` | `agg(c_mean=("mean", c), ...)` |
| `count(...)` | `agg(c_count=("count", c), ...)` |
| `size()` | per-group row count (includes nulls) |
| `nunique("c")` | `agg(c_nunique=("n_unique", "c"))` (drops nulls) |
| `first("c")` / `last("c")` | `agg(c_first=("first","c"))` / `agg(c_last=("last","c"))` |
| `median("c")` / `std("c")` / `var("c")` | engine-backed numeric aggregations |
| `agg_multi(v=["sum","mean"], ...)` | expands into multiple `agg()` specs (`v_sum`, `v_mean`, ...) |

At least one column name is required for each shortcut.

### `sort_values(by, ascending=True, na_position=None)`

Alias for `sort(...)` with pandas-shaped arguments.

**Accepted parameters (typed-first):**

- `ascending`: `bool` or `list[bool]` (must match `by` length).
- `kind`: accepted but raise `NotImplementedError`.
- `na_position`: supports `"first"` or `"last"` (maps to engine-level null ordering). Other values raise `ValueError`.
- `key`: supported only as one of the string identifiers `"lower"`, `"upper"`, `"abs"`, `"strip"`, `"length"`, `"len"` (case-insensitive). Python callables raise `NotImplementedError`.
- `ignore_index`: `False` is accepted; `True` raises `NotImplementedError` (no Index semantics).

### `.iloc[...]` (limited, plan-only)

`iloc` supports a minimal, **plan-only** subset backed by the core `slice()` operator:

- **Slices**: `df.iloc[start:stop]`
  - `start` / `stop` may be omitted (e.g. `df.iloc[:5]`, `df.iloc[5:]`).
  - Negative indices are supported **only when the frame has in-memory root data** (not scan roots).
  - `step` is not supported.
- **Scalar row**: `df.iloc[i]` returns a **single-row** `DataFrame` (still typed-first; no pandas `Series`).

Not supported (raises `NotImplementedError` / `TypeError`): list/array of indices, or slices with `step`.

**Scan / lazy roots:** Open-ended slices (`df.iloc[3:]`) and negative indices need a known row count. If the root is a file or lazy scan and length is unknown, use `collect` / materialize first, or avoid `iloc` and use `filter` / `slice` on expressions.

### `.loc[...]` (very limited)

`loc` supports a strict subset of pandas-shaped selection:

- **Row selector**:
  - `:` (all rows)
  - a boolean **`Expr`** mask (compiled to `filter(mask)` with SQL-like null-to-false semantics)
- **Column selector**:
  - `:` (all columns after the row filter)
  - `str` or non-empty `list[str]` (compiled to `select(...)`)

Examples:

```python
df.loc[df.col("a") > 1, ["b", "c"]]
df.loc[df.col("flag"), :]           # all columns, filtered rows
df.loc[:, :]                        # identity (same as the frame)
```

**Order of operations:** The row mask is applied **before** column projection, so expressions in the mask may reference any column in the current schema, even if those columns are not selected in the second argument.

Not supported (raises `NotImplementedError`): label-based row indexing, alignment semantics, list-of-bools selectors, or callables.

### Missing-data methods: `isna/isnull/notna/notnull` and `dropna`

- `df.isna()` / `df.isnull()` returns a boolean `DataFrame` where each column is replaced with `col(c).is_null()`.
- `df.notna()` / `df.notnull()` returns a boolean `DataFrame` where each column is replaced with `col(c).is_not_null()`.

`dropna(...)` supports a limited signature:

- `axis=0` only (`axis=1` raises `NotImplementedError`)
- `subset=str|list[str]` (defaults to all columns)
- `how="any"|"all"`
  - `how="any"` maps to core `drop_nulls(subset=...)`
  - `how="all"` keeps rows where **any** subset column is not null (compiled to a filter expression)

### `melt(...)` (lazy)

`melt` is implemented as a **plan step** (lazy; no collect fallback).

Supported shape:

- `id_vars`: `str` or non-empty `list[str]`
- `value_vars`: `None`, `str`, or non-empty `list[str]` (`None` means: every column not in `id_vars`, in deterministic sorted name order)
- `var_name` / `value_name` (default `"variable"` / `"value"`)

**Validation:** `var_name` and `value_name` must differ and must not collide with existing column names. All `value_vars` must share the same **scalar** base type (e.g. melting an `int` column with a `str` column raises `TypeError`).

### `pivot(...)` (lazy; core contract)

**`pivot`** on this façade is a thin wrapper around the **typed core** `pivot` (explicit **`index`**, **`columns`**, **`values`**, **`aggregate_function`**). Output column names follow the deterministic rules in {doc}`INTERFACE_CONTRACT` (e.g. `<pivot_value>_<agg>` such as **`A_first`**, **`B_first`** when **`aggregate_function="first"`**). This is **not** pandas’ unconstrained dynamic pivot; use **`to_pandas()`** if you need full pandas reshape semantics.

### `rolling(window=..., min_periods=...)` (lazy, row-based)

**On a plain `DataFrame`:** rolling is **global** (scan order): each window runs over the whole table in row order.

**On `group_by(...).rolling(...)`:** the window is **partitioned by the group key columns** (Polars `.over(...)`). The planner inserts a **stable multi-key sort** (partition columns first, then remaining columns) before the rolling step so behavior is deterministic—if you need a custom order within groups, **sort before `group_by`** when your workflow allows it.

`rolling(...)` returns a small helper with:

- `sum(column, out_name=...)`
- `mean(...)`
- `min(...)`
- `max(...)`
- `count(...)`

**Not on this façade yet:** time/index-based rolling matching pandas `rolling(on=...)` for datetime columns; use core **`rolling_agg`** (see {doc}`EXECUTION`) where the typed API exposes it.

**Engine:** Implemented in the Polars-backed executor; builds without the Polars engine will not be able to execute rolling plans. `min_periods` must not exceed `window` (Polars validates this at execution time).

### Reshape helpers (narrow vs pandas)

These names match pandas where possible but are **typed** and sometimes **stricter**:

| Method | Notes |
|--------|--------|
| `wide_to_long(stubnames, i, j, sep="_", suffix=r"\d+", value_name=...)` | **Single stub** only (`str` or length-1 list); same as `melt` plus **`j` = captured suffix** (regex group 1), so `sales_2020` → stub `sales`, `j` value `2020`, not the full column name. |
| `stack(id_vars=..., value_vars=..., var_name=..., value_name=...)` | Alias for `melt` (no MultiIndex `stack`). |
| `unstack(index=..., columns=..., values=..., aggregate_function=...)` | Thin wrapper over core **`pivot`** (explicit index/columns/values). |
| `from_dict(data, orient="columns"\|"list"\|"index"\|"records", columns=...)` | Classmethod on **`DataFrame[Schema]`** only; **`records`** and **`index`** are converted to **`dict[str, list]`** in **schema field order** (empty **`records`** yields empty columns per schema). |

### Row and index-light helpers

| Method | Notes |
|--------|--------|
| `sort_index(by=...)` or `sort_index(level=...)` | **`by`/`level` required**: list of key column names. Delegates to `sort_values`. No pandas `Index` object. |
| `set_index(keys, drop=True, append=False)` | Reorders columns so `keys` are **first**; `drop`/`append` mostly parity flags (see implementation). |
| `reset_index(...)` | No-op aside from rejecting unsupported pandas-only parameters (no stored Index). |
| `reindex(other, on=..., how=..., suffix=...)` | Join **`other`’s key projection** to `self` on **`on`** (index-light). |
| `reindex_like(other, ...)` | Uses **all columns of `other`** as join keys; tends to be strict—prefer **`reindex(..., on=[...])`** for clarity. |
| `align(other, on=[...], join="outer"\|...)` | Returns **two** frames on the union/intersection of keys (unique key rows), then left-joins each side. |
| `sample(n=..., frac=..., random_state=..., replace=False)` | **Eager**; `replace=True` not supported. |
| `take(indices)` | **Eager** positional row pick; negative indices allowed like Python. |

### Two-frame and compare/update

| Method | Notes |
|--------|--------|
| `combine_first(other, on=[...])` | **Outer** merge on keys; overlapping value columns **`coalesce(left, right)`**; drops `*_other` join columns. |
| `update(other, on=[...])` | **Left** merge; for overlaps, **`coalesce(right, left)`** (other wins when non-null). |
| `compare(other)` | **Eager** cell-wise inequality; returns a **new** boolean frame (`*_diff` columns). Same columns and row count required. |

### Whole-frame numeric summaries

| Method | Notes |
|--------|--------|
| `corr(method="pearson")` | **Eager**; needs **NumPy**; at least two numeric columns; returns a correlation **matrix** as a typed `DataFrame` (dynamic schema). |
| `cov()` | Same pattern for sample covariance matrix. |

**`describe()`** on the core frame may append **skew / kurtosis / sem** (when NumPy is available and \(n \ge 4\)) for numeric columns; **`date`** / **`datetime`** columns get non-null **count**, **min**, **max**, and **null** counts—see {doc}`INTERFACE_CONTRACT` **Introspection** and `dataframe/_impl.py`.

### Transforms (facade and `Expr`)

**DataFrame / lazy where Polars lowers the expression:**

- **`where(cond, other=None)` / `mask(cond, other=None)`** — row-wise `CASE` over **every** column: the same boolean `cond` is applied to each column, and a **scalar** `other` **broadcasts** to all columns (pandas parity). Use per-column expressions if you need column-specific replacement.
- **`rank(method="average"\|"dense", ...)`** — per-column window rank over that column as the sole sort key; **`min`/`max`/`first`** methods not implemented on the façade.
- **`interpolate(method="ffill"\|"bfill")`** — maps to `fill_null(strategy=...)`. **`method="linear"`** raises `NotImplementedError` until a dedicated engine path exists.
- **`expanding()`** — **`sum`/`count`** via `cumsum`; **`mean`** not implemented.
- **`ewm(com=... | span=... | alpha=...)`** — narrow helper; **`.mean(column, out_name=...)`** only, **eager** (pandas `Series.ewm`); requires **pandas** at runtime.
- **`eval(expr, ...)`** — alias for **`query`** (same grammar and dict rules).

**On `Expr` (also usable in `assign` / `with_columns`):**

- **`cumsum`**, **`cumprod`**, **`cummin`**, **`cummax`**, **`diff`**, **`pct_change`**
- **`clip(lower=..., upper=...)`**, **`replace({old: new, ...})`** (bounded mapping size)

### Utilities

| Method | Notes |
|--------|--------|
| `dot(other)` | **Eager** matmul; **`other.shape[0]`** must equal **`self.shape[1]`**; numeric columns only; NumPy. **`@`** is not overloaded; use **`dot`**. |
| `transpose()` / `T` | **Square** table only; **single shared dtype** across columns; materializes then permutes columns by row index. |
| `insert(loc, column, value, ...)` | Returns **new** frame with column order **`[... before loc, new col, ... after]`** (not pandas in-place). |
| `pop(column)` | Returns **`(Expr, DataFrame)`** — immutable style, unlike pandas. |

### `to_pandas()` (eager)

`to_pandas()` materializes the current logical plan and returns a `pandas.DataFrame` (requires the optional `pandas` dependency).

**Realistic pattern:** sort then materialize:

```python
df.sort_values(["region", "amount"], ascending=[True, False]).collect(as_lists=True)
```

### `drop(...)` / `rename(...)` keyword forms

- `drop(columns=..., errors="raise"|"ignore")` → core `drop(*cols)`
- `rename(columns={"old":"new"}, errors="raise"|"ignore")` → core `rename(mapping)`

Additional pandas parameters are accepted but may raise `NotImplementedError` (e.g. `drop(index=...)`, `drop(inplace=True)`, `rename(index=...)`, `rename(inplace=True)`, etc.).

### `drop_duplicates(...)` / `duplicated(...)`

- `drop_duplicates(subset=..., keep="first"|"last")` maps to core `unique(subset=..., keep=...)`.
- `drop_duplicates(..., keep=False)` drops **every** row whose key appears in a duplicate group (engine: **Polars** `is_duplicated` filter when enabled; row-wise fallback otherwise).
- `duplicated(subset=..., keep="first"|"last"|False)` returns a **single-column** frame **`duplicated: bool`** (pandas-aligned semantics). Core **`DataFrame`** also exposes **`drop_duplicate_groups(...)`** for the same filter without building the mask.

### `get_dummies` / `cut` / `qcut` / `factorize_column`

| Method | Notes |
|--------|-------|
| `get_dummies(columns=[...], prefix=..., prefix_sep=..., drop_first=..., dtype="bool"\|"int", max_categories=512, dummy_na=False)` | **Eager** category scan; **requires explicit `columns`**; keeps other columns; raises if dummy names collide or cardinality exceeds **`max_categories`**. With **`dtype="bool"`**, rows where the source cell is null produce **`None`** in dummy columns (three-valued logic from **`Expr`**, not pandas’ all-**`False`**). Use **`dummy_na=True`** to materialize a dedicated null level. |
| `cut(column, bins, new_column=..., labels=..., ...)` | **Eager**; uses **pandas `cut`**; adds a **nullable string** interval column. |
| `qcut(column, q, new_column=..., duplicates=...)` | **Eager**; **pandas `qcut`**. |
| `factorize_column(column)` | **Eager** `(codes, uniques)` tuple; **pandas `factorize`** on a `Series`. |

### Core `value_counts` (dict return)

The typed **`DataFrame.value_counts(column, ...)`** returns a **`dict`** (count per key), not a pandas **`Series`**—see [Interface contract](INTERFACE_CONTRACT.md) and core docstrings.

### `nlargest(n, columns, keep="all")` / `nsmallest(n, columns, keep="all")`

Implemented as **`sort_values`** on `columns` (descending for `nlargest`, ascending for `nsmallest`) then **`slice(0, n)`**. Only **`keep="all"`** is accepted; other `keep` values raise **`NotImplementedError`**. **`n`** must be **`>= 0`**. Unknown column names raise **`KeyError`**.

**Tie semantics:** pandas can expand ties at the rank boundary when `keep="all"`; pydantable takes the first **`n`** rows after sort only. If you need exact pandas tie behavior, materialize and use pandas, or express the selection with explicit ranking in the engine when that becomes available.

### `isin(values)`

Elementwise membership per column, compiled to **`with_columns`** / **`Expr.isin`**:

- **`list` / `tuple` / `set`:** the same value set is applied to **every** column. Values must be compatible with each column’s dtype; mixed values that the engine cannot compare to a column type may raise **`TypeError`**.
- **`dict`:** keys are column names (must exist in the schema); each value is an iterable of candidates for that column. Columns omitted from the dict become an all-**false** boolean column (`Literal(False)`).
- **`pandas.Series`** or **`pandas.DataFrame`** → **`NotImplementedError`**.

### `explode(column, ignore_index=False)` / `explode([...])`

Delegates to the core **`explode`** (same rules as the default `DataFrame`): single column name or a list of columns, without mixing the two call styles.

**Materialization:** like other list-expanding ops, executing the plan may require the engine to explode nested list data; very large or deeply nested values can be costly. Prefer narrowing rows/columns first when possible.

### `copy(deep=False)` / `pipe(func, *args, **kwargs)` / `filter(...)`

- **`copy(deep=False)`:** returns a new pandas UI frame that **shares** the same logical roots/plan as the original (shallow identity). **`copy(deep=True)`** raises **`NotImplementedError`**; use **`collect(as_lists=True)`** and rebuild if you need a detached data copy.
- **`pipe`:** calls **`func(self, *args, **kwargs)`** (pandas-style).
- **`filter`:** overloaded:
  - **One positional `Expr`:** row filter (**`super().filter(expr)`**), same as the core API.
  - **Exactly one of** `items=`, `like=`, or `regex=`: **column** selection by name (implemented via **`select`**). `items` must be **`list[str]`**. **`axis=1`** is not supported (**`NotImplementedError`**).

### `fillna(value, subset=None)` / `astype(dtype|mapping)`

- `fillna(value=..., subset=...)` maps to core `fill_null(value=..., subset=...)`.
  - `fillna(method="ffill"|"bfill")` is supported (maps to `fill_null(strategy="forward"|"backward")`).
  - `limit/inplace/downcast/axis` are accepted for parity but currently raise `NotImplementedError`.
- `astype(dtype)` casts all columns; `astype({"col": dtype})` casts selected columns via `Expr.cast(...)`.
  - `copy` is accepted (no-op); `errors="ignore"` is supported as best-effort numeric widening; unsupported casts are skipped.

## `DataFrameModel` (pandas UI)

Wraps the pandas UI `DataFrame` and delegates:

- **`assign`**, **`merge`**, **`head`/`tail`**, **`__getitem__`**, **`group_by`** → same semantics as above on the inner frame.
- **`query`**, **`sort_values`**, **`drop`**, **`rename`**, **`fillna`**, **`astype`** → same semantics as above on the inner frame.
- **`concat`**, **`nlargest`**, **`nsmallest`**, **`isin`**, **`explode`**, **`copy`**, **`pipe`**, **`filter`** (row `Expr` vs `items`/`like`/`regex`) → delegated to the inner pandas UI frame and re-wrapped.
- **`iloc`**, **`loc`**, **`isna`/`isnull`/`notna`/`notnull`**, **`dropna`**, **`melt`**, **`pivot`**, **`rolling`**, **`group_by` → `.rolling(...)`**, and other pandas UI helpers documented above (**`get_dummies`**, **`cut`/`qcut`**, **`factorize_column`**, **`ewm`**, **`duplicated`**, **`wide_to_long`**, **`from_dict`**, **`where`/`mask`**, **`rank`**, **`sample`/`take`**, **`corr`/`cov`**, **`combine_first`/`update`/`compare`**, **`reindex`/`align`**, **`dot`/`transpose`/`insert`/`pop`**, …) → delegated to the inner **`PandasDataFrame`** via **`__getattr__`** (or explicit façade methods where listed earlier). Methods that return a plain **`DataFrame`** (e.g. **`pop`**) are **not** auto-wrapped into a **`DataFrameModel`** unless you add explicit façade methods; call **`type(self)._from_dataframe(...)`** when you need a model instance.

Properties **`columns`**, **`shape`**, **`empty`**, **`dtypes`** read from the inner frame.

**`PandasGroupedDataFrameModel`** mirrors the groupby convenience methods (`sum/mean/count/size/nunique/first/last/median/std/var/agg_multi`).

## Naming map (core ↔ pandas ↔ PySpark)

Same engine; different method names. PySpark column is in {doc}`PYSPARK_UI`.

| Operation | Core / default | Pandas UI | PySpark UI |
|-----------|----------------|-----------|------------|
| Add column | `with_columns` | `assign` | `withColumn` |
| Filter | `filter` | `filter` | `where` / `filter` |
| Join | `join` | `merge` | `join` |
| Sort | `sort` / `order_by` | `sort` | `orderBy` |
| Rename | `rename` | `rename` | `withColumnRenamed` |
| Pivot (typed) | `pivot` | `pivot` | — |

## Relationship to the default export

- **`from pydantable.pandas import DataFrameModel`** uses pandas-style method names on the same Rust execution path as **`from pydantable import DataFrameModel`**.

## Further reading

- [Interface contract](INTERFACE_CONTRACT.md) — null semantics, join rules, duplicate detection, pivot naming.
- [Execution](EXECUTION.md) — Rust engine overview.
- Tests: **`tests/test_pandas_ui.py`**, **`tests/test_pandas_ui_popular_features.py`** (duplicates, dummies, binning, factorize, ewm, pivot).
