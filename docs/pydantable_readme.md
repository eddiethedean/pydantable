# PydanTable

**The dataframe layer built for FastAPI + Pydantic services.**

PydanTable enforces schemas and tracks types through transformations, with a
SQLModel-like developer experience that integrates cleanly with FastAPI.

**0.5.x materialization:** `collect()` returns a **list of Pydantic row models** for the
current schema; use **`to_dict()`** for columnar **`dict[str, list]`**. A Polars
**`DataFrame`** in Python is optional via **`to_polars()`** after
`pip install 'pydantable[polars]'`. See `docs/EXECUTION.md`.

## Public API Direction

The current primary FastAPI-facing API is `DataFrameModel`, a class that:

- represents the *whole DataFrame*
- generates a per-row Pydantic `RowModel` for request/response typing
- accepts column dicts, row dicts, or sequences of Pydantic row models:
  - column dict: `{"id": [1,2], "age": [20,30]}`
  - row dict list: `[{"id": 1, "age": 20}, ...]`
  - row model list: `[User.RowModel(id=1, age=20), ...]` (e.g. from `list[User.RowModel]` in FastAPI)
- returns a new model type for every transformation (schema migration)
- uses replacement semantics for `with_columns` name collisions

See:
- `docs/SUPPORTED_TYPES.md` for **scalar column dtypes**, unsupported cases, and **when** unsupported `DataFrameModel` field types fail (at class definition)
- `docs/DATAFRAMEMODEL.md` for the DataFrameModel contract
- `docs/EXECUTION.md` for the Rust execution model and optional UI modules
- `docs/FASTAPI.md` for end-to-end FastAPI examples
- `docs/WHY_NOT_POLARS.md` for positioning vs Polars

## Example (current primary FastAPI-facing API)

```python
from pydantable import DataFrameModel

class User(DataFrameModel):
    id: int
    age: int

df = User({"id": [1, 2], "age": [20, 30]})
df2 = df.with_columns(age2=df.age * 2)
df3 = df2.select("id", "age2")
df4 = df3.filter(df3.age2 > 40)

result = df4.to_dict()
print(result)
```

Output from `print(result)` (one run):

```text
{'age2': [60], 'id': [2]}
```

## Current Repository Status

In the `0.5.0` release, `DataFrameModel` is available as the primary
FastAPI-facing API, backed by the same typed expression and Rust execution core
as the lower-level `DataFrame[Schema]` API.

Phase 2 is complete for the expression system:
- expression behavior parity is validated between `DataFrameModel` and `DataFrame[Schema]`
- reflected arithmetic is supported (e.g. `2 + df.age`)
- AST-build-time failures are enforced for invalid operator combinations

Phase 3 is complete for basic transformations:
- `select()`, `with_columns()`, and `filter()` behavior is locked
- `with_columns()` replacement semantics are verified for collisions
- schema migration and row-input/column-input parity are validated through tests

Phase 4 is complete for logical-plan boundary hardening:
- Rust now owns the remaining transformation-time validation contract
- Python schema migration consumes Rust schema descriptors (`base`, `nullable`)
- Rust-side plan tests and Python metadata-flow integration tests are in place

### Supported expression dtypes + null semantics
Rust enforces expression typing (at AST-build time) for scalar dtypes:
`int`, `float`, `bool`, `str`, `datetime`, `date`, `timedelta`. See
`docs/SUPPORTED_TYPES.md` for the full list, nullability, and unsupported cases.

Null semantics are SQL-like (`propagate_nulls`):
- arithmetic: `NULL` + anything yields `NULL`
- comparisons: if either side is `NULL`, the result is `NULL` (typed as `Optional[bool]`)
- `filter(condition)`: keeps rows where the condition evaluates to exactly `True`; drops rows where the condition is `False` or `NULL`

These rules are implemented in the Rust core so derived schemas and runtime
values stay aligned.

`Optional[T]` handling:
- schema fields annotated as `Optional[T]` accept `None` values at DataFrame
  construction time
- derived schemas produced by `select()` / `with_columns()` / `filter()`
  propagate nullability through expression result types

## Roadmap

-   MVP schema + expressions
-   Rust expression typing and logical planning (`pydantable-core`)
-   Polars-backed execution via PyO3 (`pydantable._core`, called from `rust_engine.py`)
-   Advanced query operations

## License

MIT
