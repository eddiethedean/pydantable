# Pydantable

**Strongly-typed DataFrames for Python, powered by Rust.**

Pydantable enforces schemas and tracks types through transformations, with a
SQLModel-like developer experience that integrates cleanly with FastAPI.

## Public API Direction

The intended user-facing interface is `DataFrameModel`, a class that:

- represents the *whole DataFrame*
- generates a per-row Pydantic `RowModel` for request/response typing
- accepts both input formats:
  - column dict: `{"id": [1,2], "age": [20,30]}`
  - row list: `[{"id": 1, "age": 20}, ...]`
- returns a new model type for every transformation (schema migration)
- uses replacement semantics for `with_columns` name collisions

Full spec: `docs/DATAFRAMEMODEL.md`.

## Example (current API)

```python
from pydantable import DataFrame, Schema

class User(Schema):
    id: int
    age: int

df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
df2 = df.with_columns(age2=df.age * 2)
df3 = df2.select("id", "age2")
df4 = df3.filter(df3.age2 > 40)

result = df4.collect()
print(result)  # {"id": [2], "age2": [60]}
```

## Current Repository Status

In the `0.4.0` skeleton, the implementation you can run today is the lower-level
API: `DataFrame[Schema]` + typed expressions. `DataFrameModel` is the next DX layer
on top of that.

### Supported Expression Dtypes + Null Semantics (skeleton)
Rust enforces expression typing (at AST-build time) and executes expressions
with supported dtypes: `int`, `float`, `bool`, `str`.

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
-   Rust backend and logical planning
-   Rust Polars execution engine
-   Advanced query operations

## License

MIT
