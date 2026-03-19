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

## Example (target interface)

```python
from pydantable import DataFrameModel

class UserDF(DataFrameModel):
    id: int
    age: int

df = UserDF({"id": [1, 2], "age": [20, 30]})
df2 = df.with_columns(age2=df.age * 2)
df3 = df2.select("id", "age2")
df4 = df3.filter(df3.age2 > 40)

# later: row-wise materialization for FastAPI
# rows: list[UserDF.RowModel] = df4.rows()
```

## Current Repository Status

In the `0.4.0` skeleton, the implementation you can run today is the lower-level
API: `DataFrame[Schema]` + typed expressions. `DataFrameModel` is the next DX layer
on top of that.

## Roadmap

-   MVP schema + expressions
-   Rust backend and logical planning
-   Rust Polars execution engine
-   Advanced query operations

## License

MIT
