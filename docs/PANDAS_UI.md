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
df2 = df.assign(doubled=df.amount * 2)
print(df2.to_dict())
```

Output:

```text
{'doubled': [20, 40], 'region': ['US', 'EU'], 'amount': [10, 20]}
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

- Callable columns (e.g. `lambda df: ...`)
- Raw **`pandas.Series`** as a column value

Use column expressions (e.g. `df.x + 1`) instead.

### `merge(other, *, how="inner", on=..., suffixes=("_x", "_y"), ...)`

Maps to **`join`**: `on` is required; the **right suffix** is taken from `suffixes[1]` (default second element `"_y"` → passed as `suffix` to `join`).

**Raises `NotImplementedError` for:**

- `left_on` / `right_on` (use symmetric `on=` keys only)
- `indicator=True`
- `validate=...`

Unknown keyword arguments raise **`TypeError`**.

### Introspection

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

Always raises **`NotImplementedError`**. Use **`filter(Expr)`** with typed comparisons.

### `group_by(...)`

Returns **`PandasGroupedDataFrame`**, which adds:

| Method | Equivalent |
|--------|------------|
| `sum("c1", "c2", ...)` | `agg(c1_sum=("sum", "c1"), ...)` |
| `mean(...)` | `agg(c_mean=("mean", c), ...)` |
| `count(...)` | `agg(c_count=("count", c), ...)` |

At least one column name is required for each shortcut.

## `DataFrameModel` (pandas UI)

Wraps the pandas UI `DataFrame` and delegates:

- **`assign`**, **`merge`**, **`head`/`tail`**, **`__getitem__`**, **`group_by`** → same semantics as above on the inner frame.
- **`query`** → same **not implemented** behavior.

Properties **`columns`**, **`shape`**, **`empty`**, **`dtypes`** read from the inner frame.

**`PandasGroupedDataFrameModel`** mirrors **`sum` / `mean` / `count`** on grouped models.

## Relationship to the default export

- **`from pydantable.pandas import DataFrameModel`** uses pandas-style method names on the same Rust execution path as **`from pydantable import DataFrameModel`**.

## Further reading

- [Interface contract](INTERFACE_CONTRACT.md) — null semantics and join rules.
- [Execution](EXECUTION.md) — Rust engine overview.
