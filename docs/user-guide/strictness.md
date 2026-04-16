# Strictness (per-column and nested)

Phase 4 adds **opt-in** controls for how pydantable validates/coerces input values at
ingest time under `trusted_mode="off"` (full Pydantic validation).

Strictness is configured per column using Pydantic field metadata and can be
defaulted via `validation_profile` (or `__pydantable__` model policy).

## Column policy keys

Declare strictness using `Field(json_schema_extra={"pydantable": {...}})`:

```python
from pydantic import Field
from pydantable import DataFrameModel


class DF(DataFrameModel):
    id: int = Field(json_schema_extra={"pydantable": {"strictness": "strict"}})
```

Supported values:

- `coerce` (default): Pydantic coercion (current behavior).
- `strict`: strict Pydantic validation for this column when element validation runs.
- `off`: skip per-element validation for this column (shape/nullability still enforced).
- `inherit`: use defaults from `validation_profile` / `__pydantable__`.

### Nested strictness

For nested types (`BaseModel` struct columns, `list[T]`, `dict[str, T]`), you can set
`nested_strictness`:

```python
from pydantic import BaseModel, Field
from pydantable import DataFrameModel


class Address(BaseModel):
    zip: int


class DF(DataFrameModel):
    addr: Address = Field(
        json_schema_extra={"pydantable": {"nested_strictness": "strict"}}
    )
```

`nested_strictness` uses the same value set as `strictness`.

## Defaults via validation profiles / model policy

Validation profiles can set defaults:

- `column_strictness_default`
- `nested_strictness_default`

And model policy can set defaults via `__pydantable__`:

```python
class DF(DataFrameModel):
    __pydantable__ = {
        "column_strictness_default": "coerce",
        "nested_strictness_default": "strict",
    }
```

Column policies override these defaults.

## Trusted modes

- `trusted_mode="off"`: strictness settings control whether values are validated
  strictly, coerced, or skipped (per column).
- `trusted_mode="shape_only"` / `"strict"`: pydantable primarily performs shape/dtype
  compatibility checks; strictness settings do not force additional per-element
  validation.

