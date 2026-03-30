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
    id: int
    age: int | None


UsersColumnar = columnar_body_model_from_dataframe_model(
    User,
    example={"id": [1, 2], "age": [20, None]},
)
body = UsersColumnar(id=[1, 2], age=[20, None])
df = User(body.model_dump())
assert df.to_dict() == {"id": [1, 2], "age": [20, None]}
```

## Recipe (`Depends` on the frame)

For FastAPI routes, use **`columnar_dependency`** so the handler receives a
**`DataFrameModel`** directly:

```python
from typing import Annotated

from fastapi import Depends, FastAPI

from pydantable.fastapi import columnar_dependency

app = FastAPI()

@app.post("/users")
def create_users(
    df: Annotated[User, Depends(columnar_dependency(User, trusted_mode="strict"))],
) -> dict[str, list]:
    return df.to_dict()
```

Row-array JSON bodies: **`rows_dependency(User)`** (same **`trusted_mode`** kwargs).

See {doc}`/FASTAPI` **Columnar OpenAPI and Depends** and **`tests/test_pydantable_fastapi_columnar.py`**.

## Pitfalls

- **Length mismatches** across columns: Pydantic may accept the JSON, then **`DataFrameModel`** raises **`ValueError`** when lengths differ—typically **500** in FastAPI unless you handle it. Validate lengths explicitly if you need **4xx** with a custom message.
- **Validation cost** depends on `trusted_mode` (see {doc}`/DATAFRAMEMODEL`).
- **Nested** row fields become **`list[NestedModel]`** in columnar JSON (see {doc}`/FASTAPI` **Columnar OpenAPI and Depends**).
