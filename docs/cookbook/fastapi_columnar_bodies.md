# FastAPI: columnar request bodies

This recipe shows how to accept **column-shaped** JSON bodies (good for large batches)
and convert them into a typed `DataFrameModel`.

## Why columnar bodies?

- Better for large payloads (no per-row dict overhead in the request body)
- Matches the `dict[str, list]` shape used by `to_dict()`

## Recipe

```python
from pydantable import DataFrameModel
from pydantic import BaseModel


class UsersBody(BaseModel):
    id: list[int]
    age: list[int | None]


class User(DataFrameModel):
    id: int
    age: int | None


body = UsersBody(id=[1, 2], age=[20, None])
df = User({"id": body.id, "age": body.age})
assert df.to_dict() == {"id": [1, 2], "age": [20, None]}
```

## Pitfalls

- **Length mismatches** across columns raise at ingest/validation time (shape check).
- **Validation cost** depends on `trusted_mode` (see {doc}`/DATAFRAMEMODEL`).

