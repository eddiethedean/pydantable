# Pydantable

**Strongly-typed DataFrames for Python, powered by Rust.**

Pydantable combines Pydantic schemas with a query engine so DataFrame operations
can be validated and typed end-to-end.

## Status (v0.4.0 skeleton)

This release provides:
- Typed DataFrames core (`DataFrame[Schema]`) with runtime schema enforcement (low-level API)
- A typed expression AST with operator overloads (`df.age * 2`, `df.age > 10`, ...)
- `select()`, `with_columns()`, `filter()`, and a pure-Python `collect()`

Public API direction (next step): `DataFrameModel` as a SQLModel-like wrapper that:
- represents the whole DataFrame
- generates a per-row Pydantic model for FastAPI
- supports both input formats (columns and rows)
- returns new model types for every transformation (schema migration)

Rust planner/execution and Rust Polars integration are stubbed for now; attempting
to use `collect(engine="rust")` will raise `NotImplementedError`.

## Installation

From this repo:

```bash
pip install .
```

`pip install .` will build the Rust extension via `maturin` when toolchains are
available. Pure-Python execution still works even if the extension is not
present (as long as you use the default `collect()`).

## Quick start

```python
from pydantable import DataFrameModel

class UserDF(DataFrameModel):
    id: int
    age: int

df = UserDF({"id": [1, 2], "age": [20, 30]})

df2 = df.with_columns(age2=df.age * 2)
df3 = df2.select("id", "age2")
df4 = df3.filter(df3.age2 > 40)
result = df4.collect()
print(result)  # {"id": [2], "age2": [60]}
```

See the `DataFrameModel` design spec: `docs/DATAFRAMEMODEL.md`.

## Project Roadmap

See `docs/ROADMAP.md`.

## License

MIT

