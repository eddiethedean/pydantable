# Service ergonomics (OpenAPI, aliases, redaction)

This page collects Phase 5 patterns for using **pydantable** in services.

## Columnar request bodies and OpenAPI

Use `pydantable.fastapi.columnar_dependency(...)` to accept column-shaped JSON (same
shape as `df.to_dict()`), while still getting an OpenAPI schema.

Phase 5 adds:

- **Field metadata propagation**: `Field(description=...)` and `Field(examples=...)`
  on your `DataFrameModel` schema fields can be propagated into the generated
  columnar OpenAPI schema.
- **Optional example generation**: set `generate_examples=True` to populate
  field-level `examples` (best-effort).

## Alias-aware ingestion (columnar)

For columnar bodies, you can choose what keys to accept:

- `input_key_mode="python"`: accept python field names only (default).
- `input_key_mode="aliases"`: accept field aliases only.
- `input_key_mode="both"`: accept either; if both the python key and alias key are
  present for the same field, validation fails.

## Redaction defaults on output

You can flag columns for redaction using field policy metadata:

```python
from pydantic import Field
from pydantable import DataFrameModel


class Users(DataFrameModel):
    email: str = Field(json_schema_extra={"pydantable": {"redact": True}})
```

Then use:

- `df.to_dicts(redact=True)` / `await df.ato_dicts(redact=True)` to apply redaction.
- Or set a model default via `__pydantable__ = {"redact": True}`.

