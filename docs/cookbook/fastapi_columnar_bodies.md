# FastAPI: columnar request bodies

This recipe shows how to accept **column-shaped** JSON bodies (good for large batches)
and convert them into a typed `DataFrameModel`, with **OpenAPI** support from
`pydantable.fastapi`.

## Why columnar bodies?

- Better for large payloads (no per-row dict overhead in the request body)
- Matches the `dict[str, list]` shape used by `to_dict()`

## Recipe (generated model)

Use **`columnar_body_model_from_dataframe_model`** so you do not hand-write a
parallel body model:

```python
from pydantable import DataFrameModel
from pydantable.fastapi import columnar_body_model_from_dataframe_model


class User(DataFrameModel):
    user_id: int
    email: str
    signup_year: int | None = None


UsersColumnar = columnar_body_model_from_dataframe_model(
    User,
    example={
        "user_id": [1001, 1002],
        "email": ["ada@example.com", "bob@example.org"],
        "signup_year": [2024, None],
    },
)
body = UsersColumnar(
    user_id=[1001, 1002],
    email=["ada@example.com", "bob@example.org"],
    signup_year=[2024, None],
)
df = User(body.model_dump())
assert df.to_dict() == {
    "user_id": [1001, 1002],
    "email": ["ada@example.com", "bob@example.org"],
    "signup_year": [2024, None],
}
```

## Recipe (`Depends` on the frame)

For FastAPI routes, use **`columnar_dependency`** so the handler receives a
**`DataFrameModel`** directly. Call **`register_exception_handlers`** once so
**`ColumnLengthMismatchError`** becomes **400** instead of **500**:

```python
from typing import Annotated

from fastapi import Depends, FastAPI

from pydantable.fastapi import columnar_dependency, register_exception_handlers

app = FastAPI()
register_exception_handlers(app)


@app.post("/users/batch")
def ingest_users_batch(
    df: Annotated[User, Depends(columnar_dependency(User, trusted_mode="strict"))],
) -> dict[str, list]:
    # e.g. df.select(...).filter(...) then return await … in an async route
    return df.to_dict()
```

Row-array JSON bodies: **`rows_dependency(User)`** (same **`trusted_mode`** kwargs).

See {doc}`/FASTAPI` **Columnar OpenAPI and Depends** and **`tests/test_pydantable_fastapi_columnar.py`**.

## Pitfalls

- **Length mismatches** across columns: Pydantic may accept the JSON, then **`DataFrameModel`** raises **`ColumnLengthMismatchError`**. With **`register_exception_handlers`**, FastAPI returns **400**; otherwise you often see **500**. Validate lengths explicitly if you need a custom **4xx** body.
- **Validation cost** depends on `trusted_mode` (see {doc}`/DATAFRAMEMODEL`).
- **Nested** row fields become **`list[NestedModel]`** in columnar JSON (see {doc}`/FASTAPI` **Columnar OpenAPI and Depends**).
