# FastAPI Integration Guide

This guide shows full FastAPI-oriented examples using `DataFrameModel` as the
primary FastAPI-facing API, with typed expressions and Rust-backed execution.

## Why this matters

For FastAPI services, `pydantable` gives you:

- Pydantic schema validation at API boundaries
- typed dataframe transformations in service logic
- `DataFrameModel` construction from column dicts, row dicts, or **sequences of Pydantic models** (including `YourDF.RowModel` from a typed request body)
- Rust execution for `to_dict()` (columnar) and **`collect()`** (row list: **`list`** of Pydantic models for the **current** projected schema)

## Install

From this repository:

```bash
pip install .
```

`pydantable` requires the Rust extension in the current skeleton.

## Example 1: Request payload -> transformed response (DataFrameModel)

This endpoint accepts a **JSON array of row objects** typed as `list[UserDF.RowModel]`,
builds the `DataFrameModel` from those Pydantic instances, applies typed dataframe
operations, and returns a **JSON array of row objects** using **`collect()`** (each
element is a Pydantic model for the projected schema). The handler maps those models
onto stable **`UserAge2Row`** DTOs for OpenAPI; you can **`return df2.collect()`**
directly when you do not need a separate response class.

```python
from typing import Optional

from fastapi import FastAPI
from pydantic import BaseModel

from pydantable import DataFrameModel


class UserDF(DataFrameModel):
    id: int
    age: Optional[int]


class UserAge2Row(BaseModel):
    """API response row; fields match the transformed projection."""

    id: int
    age2: Optional[int]


app = FastAPI()


@app.post("/users/age2", response_model=list[UserAge2Row])
def users_age2(rows: list[UserDF.RowModel]) -> list[UserAge2Row]:
    # Pydantic validates each row before DataFrameModel construction.
    df = UserDF(rows)

    # Typed expression + schema migration.
    df2 = df.with_columns(age2=df.age + 1).select("id", "age2")

    # Rust executes the plan; collect() -> list of Pydantic models (df2.schema_type).
    return [UserAge2Row.model_validate(m.model_dump()) for m in df2.collect()]
```

If the request body is `[{"id": 1, "age": 20}, {"id": 2, "age": null}]`, the response body is:

```json
[{"id": 1, "age2": 21}, {"id": 2, "age2": null}]
```

Behavior notes:

- `age2` is typed as `Optional[int]` because nulls propagate.
- if `age` is `None`, `age2` is `None`.

## Example 2: Filtering with nullable conditions (DataFrameModel)

`filter(condition)` keeps rows where condition is exactly `True` and drops
rows where it is `False` or `NULL`.

```python
from typing import Optional

from fastapi import FastAPI
from pydantic import BaseModel

from pydantable import DataFrameModel


class UserDF(DataFrameModel):
    id: int
    age: Optional[int]


class AdultRow(BaseModel):
    id: int
    age: Optional[int]


app = FastAPI()


@app.post("/users/adults", response_model=list[AdultRow])
def adults(rows: list[UserDF.RowModel]) -> list[AdultRow]:
    df = UserDF(rows)

    # condition dtype: Optional[bool]
    df2 = df.filter(df.age >= 18)
    return [AdultRow.model_validate(m.model_dump()) for m in df2.collect()]
```

With input:

```json
[{"id": 1, "age": 22}, {"id": 2, "age": null}, {"id": 3, "age": 15}]
```

Response body:

```json
[{"id": 1, "age": 22}]
```

## Example 3: Chained transformation endpoint (DataFrameModel)

This example shows a realistic service flow: enrich, filter, project.

```python
from typing import Optional

from fastapi import FastAPI
from pydantic import BaseModel

from pydantable import DataFrameModel


class EventDF(DataFrameModel):
    user_id: int
    spend: Optional[float]


class HighValueRow(BaseModel):
    user_id: int
    spend_usd: Optional[float]


app = FastAPI()


@app.post("/events/high-value", response_model=list[HighValueRow])
def high_value(rows: list[EventDF.RowModel]) -> list[HighValueRow]:
    df = EventDF(rows)

    df2 = (
        df.with_columns(spend_usd=df.spend * 1.0)
        .filter(df.spend > 100.0)
        .select("user_id", "spend_usd")
    )
    return [HighValueRow.model_validate(m.model_dump()) for m in df2.collect()]
```

For a request body `[{"user_id": 1, "spend": 150.0}, {"user_id": 2, "spend": 50.0}]`, the response body is:

```json
[{"user_id": 1, "spend_usd": 150.0}]
```

## Columnar vs row-shaped responses

- **`to_dict()`** — `dict[str, list]`; use when your API returns **columns** (e.g. bulk arrays in one JSON object).
- **`collect()`** — `list` of Pydantic models for the **current** projected schema (`df.schema_type`); use for **row arrays** in JSON. For OpenAPI, map with `YourRowDto.model_validate(m.model_dump())` when you want a stable named type, or return **`df.collect()`** directly if the generated row type is enough.

## Error timing and API safety

In the current Rust-first design:

- invalid expression type combinations fail when building the expression AST
  (during operator overloads)
- invalid `filter()` condition types fail before execution
- invalid `select()` projections (for example, empty projections) fail from Rust
  logical-plan validation before execution

This keeps FastAPI handlers predictable: many category errors are raised before
query execution.

Phase 3 (basic transformations) is complete:

- `select()`, `with_columns()`, and `filter()` behavior is locked
- `with_columns()` replacement semantics are deterministic for collisions
- row-input (dict rows, Pydantic row models) and column-input transformation parity is validated

Phase 4 (logical-plan boundary hardening) is complete:

- Rust is authoritative for plan validation and migration metadata
- Python derives output model annotations from Rust schema descriptors
- behavior compatibility is preserved for existing FastAPI transformation flows

## Practical pattern for services

For larger apps, a clean split is:

- **Route layer**: Pydantic request/response models; materialize rows with **`collect()`** when responses are row lists
- **Service layer**: `DataFrameModel` transforms
- **Persistence layer**: source/sink adapters (db, queue, storage)

That keeps your data contract (`DataFrameModel` schema annotations) and transformation contract in one
typed place.

