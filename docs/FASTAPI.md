# FastAPI Integration Guide

This guide shows full FastAPI-oriented examples using `DataFrameModel` as the
primary FastAPI-facing API, with typed expressions and Rust-backed execution.

## Why this matters

For FastAPI services, `pydantable` gives you:

- Pydantic schema validation at API boundaries
- typed dataframe transformations in service logic
- Rust execution for `collect()` on supported skeleton operations

## Install

From this repository:

```bash
pip install .
```

`pydantable` requires the Rust extension in the current skeleton.

## Example 1: Request payload -> transformed response (DataFrameModel)

This endpoint accepts a typed payload, applies typed dataframe operations, and
returns transformed columns.

```python
from typing import List, Optional

from fastapi import FastAPI
from pydantic import BaseModel

from pydantable import DataFrameModel


class UserDF(DataFrameModel):
    id: int
    age: Optional[int]


class UsersPayload(BaseModel):
    id: List[int]
    age: List[Optional[int]]


class UsersOut(BaseModel):
    id: List[int]
    age2: List[Optional[int]]


app = FastAPI()


@app.post("/users/age2", response_model=UsersOut)
def users_age2(payload: UsersPayload) -> UsersOut:
    # Pydantic validates request shape before DataFrameModel construction.
    df = UserDF(payload.model_dump())

    # Typed expression + schema migration.
    df2 = df.with_columns(age2=df.age + 1).select("id", "age2")

    # Rust executes the plan; returns Dict[str, list[Any]].
    result = df2.collect()
    return UsersOut(**result)
```

Behavior notes:

- `age2` is typed as `Optional[int]` because nulls propagate.
- if `age` is `None`, `age2` is `None`.

## Example 2: Filtering with nullable conditions (DataFrameModel)

`filter(condition)` keeps rows where condition is exactly `True` and drops
rows where it is `False` or `NULL`.

```python
from typing import List, Optional

from fastapi import FastAPI
from pydantic import BaseModel

from pydantable import DataFrameModel


class UserDF(DataFrameModel):
    id: int
    age: Optional[int]


class UsersPayload(BaseModel):
    id: List[int]
    age: List[Optional[int]]


class AdultOut(BaseModel):
    id: List[int]
    age: List[Optional[int]]


app = FastAPI()


@app.post("/users/adults", response_model=AdultOut)
def adults(payload: UsersPayload) -> AdultOut:
    df = UserDF(payload.model_dump())

    # condition dtype: Optional[bool]
    df2 = df.filter(df.age >= 18)
    result = df2.collect()
    return AdultOut(**result)
```

With input:

```python
{"id": [1, 2, 3], "age": [22, None, 15]}
```

Output is:

```python
{"id": [1], "age": [22]}
```

## Example 3: Chained transformation endpoint (DataFrameModel)

This example shows a realistic service flow: enrich, filter, project.

```python
from typing import List, Optional

from fastapi import FastAPI
from pydantic import BaseModel

from pydantable import DataFrameModel


class EventDF(DataFrameModel):
    user_id: int
    spend: Optional[float]


class EventsPayload(BaseModel):
    user_id: List[int]
    spend: List[Optional[float]]


class HighValueOut(BaseModel):
    user_id: List[int]
    spend_usd: List[Optional[float]]


app = FastAPI()


@app.post("/events/high-value", response_model=HighValueOut)
def high_value(payload: EventsPayload) -> HighValueOut:
    df = EventDF(payload.model_dump())

    df2 = (
        df.with_columns(spend_usd=df.spend * 1.0)
        .filter(df.spend > 100.0)
        .select("user_id", "spend_usd")
    )
    return HighValueOut(**df2.collect())
```

## Error timing and API safety

In the current Rust-first design:

- invalid expression type combinations fail when building the expression AST
  (during operator overloads)
- invalid `filter()` condition types fail before execution

This keeps FastAPI handlers predictable: many category errors are raised before
query execution.

Phase 3 (basic transformations) is complete:

- `select()`, `with_columns()`, and `filter()` behavior is locked
- `with_columns()` replacement semantics are deterministic for collisions
- row-input and column-input transformation parity is validated

## Practical pattern for services

For larger apps, a clean split is:

- **Route layer**: Pydantic request/response models
- **Service layer**: `DataFrameModel` transforms
- **Persistence layer**: source/sink adapters (db, queue, storage)

That keeps your data contract (`DataFrameModel` schema annotations) and transformation contract in one
typed place.

