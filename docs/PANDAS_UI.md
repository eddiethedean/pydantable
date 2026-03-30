# Pandas UI (`pydantable.pandas`)

The **pandas UI** is an optional import surface that layers **pandas-like method and property names** on top of pydantable’s typed logical DataFrame. It does **not** wrap a `pandas.DataFrame` for the schema-driven `DataFrame` / `DataFrameModel` types; execution uses the same **Rust engine** as the default export (see [Execution](EXECUTION.md)).

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

### `assign(**kwargs)`

Same behavior as **`with_columns`**: each value must be an **`Expr`** or a literal compatible with expression building.

**Not supported:**

- Raw **`pandas.Series`** as a column value

Use column expressions (e.g. `df.x + 1`) instead.

**Supported:** pandas-style callables that return an `Expr` or literal, e.g. `assign(x2=lambda df: df.x * 2)`.

### `merge(other, *, how="inner", on=..., suffixes=("_x", "_y"), ...)`

Maps to **`join`**: `on` is required; the **right suffix** is taken from `suffixes[1]` (default second element `"_y"` → passed as `suffix` to `join`).

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
- `na_position`, `kind`: accepted but raise `NotImplementedError`.
- `key`: supported only as one of the string identifiers `"lower"`, `"upper"`, `"abs"`, `"strip"`, `"length"`, `"len"` (case-insensitive). Python callables raise `NotImplementedError`.
- `ignore_index`: `False` is accepted; `True` raises `NotImplementedError` (no Index semantics).

### `.iloc[...]` (limited)

`iloc` supports a minimal, **plan-only** slice form backed by the core `slice()` operator:

- `df.iloc[start:stop]` (no `step`)

Not supported (raises `NotImplementedError` / `TypeError`): scalar row access, list-of-indices, or slices with `step`.

### `to_pandas()` (eager)

`to_pandas()` materializes the current logical plan and returns a `pandas.DataFrame` (requires the optional `pandas` dependency).

**Realistic pattern:** sort then materialize:

```python
df.sort_values(["region", "amount"], ascending=[True, False]).collect(as_lists=True)
```

### `drop(...)` / `rename(...)` keyword forms

- `drop(columns=..., errors="raise"|"ignore")` → core `drop(*cols)`\n- `rename(columns={"old":"new"}, errors="raise"|"ignore")` → core `rename(mapping)`

Additional pandas parameters are accepted but may raise `NotImplementedError` (e.g. `drop(index=...)`, `drop(inplace=True)`, `rename(index=...)`, `rename(inplace=True)`, etc.).

### `fillna(value, subset=None)` / `astype(dtype|mapping)`

- `fillna(value=..., subset=...)` maps to core `fill_null(value=..., subset=...)`.\n  - `fillna(method=\"ffill\"|\"bfill\")` is supported (maps to `fill_null(strategy=\"forward\"|\"backward\")`).\n  - `limit/inplace/downcast/axis` are accepted for parity but currently raise `NotImplementedError`.\n- `astype(dtype)` casts all columns; `astype({\"col\": dtype})` casts selected columns via `Expr.cast(...)`.\n  - `copy` is accepted (no-op); `errors=\"ignore\"` is supported as best-effort numeric widening; unsupported casts are skipped.

**Caveat:** `fillna(method=...)`/`limit=...` are currently not implemented; prefer explicit expressions (`Expr.fill_null(...)`) or engine-native operations.

## `DataFrameModel` (pandas UI)

Wraps the pandas UI `DataFrame` and delegates:

- **`assign`**, **`merge`**, **`head`/`tail`**, **`__getitem__`**, **`group_by`** → same semantics as above on the inner frame.
- **`query`**, **`sort_values`**, **`drop`**, **`rename`**, **`fillna`**, **`astype`** → same semantics as above on the inner frame.

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

## Relationship to the default export

- **`from pydantable.pandas import DataFrameModel`** uses pandas-style method names on the same Rust execution path as **`from pydantable import DataFrameModel`**.

## Further reading

- [Interface contract](INTERFACE_CONTRACT.md) — null semantics and join rules.
- [Execution](EXECUTION.md) — Rust engine overview.
