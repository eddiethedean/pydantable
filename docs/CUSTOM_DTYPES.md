# Custom dtypes (semantic scalar types)

PydanTable supports **semantic scalar types**: user-defined Python types that behave
like scalars (typically `str`, `int`, or `bytes`) but carry domain meaning and can be
validated/coerced by **Pydantic v2**.

Examples:

- `ULID` (string-like identifier)
- `SnowflakeID` (integer-like identifier)
- `CountryCode` (string-like)

## Define a semantic scalar type (Pydantic v2)

Follow the same pattern used by `pydantable.types.WKB`: implement
`__get_pydantic_core_schema__` to coerce/validate input.

Example (string-like):

```python
from __future__ import annotations

from typing import Any

from pydantic_core import CoreSchema, core_schema


class ULID(str):
    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: Any) -> CoreSchema:
        def coerce(v: object) -> ULID:
            if isinstance(v, ULID):
                return v
            if isinstance(v, str):
                s = v.strip()
                # add your validation here
                return ULID(s)
            raise TypeError(f\"ULID expects str, got {type(v).__name__}\")

        return core_schema.no_info_after_validator_function(
            coerce,
            core_schema.str_schema(),
        )
```

## Register the dtype with pydantable

Register your semantic type as a **base scalar** so pydantable can treat it as a
supported column dtype in schemas and strict-mode compatibility checks.

```python
from pydantable.dtypes import register_scalar

register_scalar(ULID, base=\"str\")
```

## How it behaves in pydantable

- **Schema support**: registered semantic scalars are accepted in `DataFrameModel`
  field annotations (like their base type).
- **Trusted modes**:
  - `trusted_mode=\"off\"`: values are validated/coerced via Pydantic, so your
    CoreSchema logic runs.
  - `trusted_mode=\"shape_only\"` / `\"strict\"`: pydantable checks compatibility
    against the **base scalar**.\n
- **Derived schemas**: pydantable keeps custom scalar identity when the underlying
  Rust dtype remains compatible (semantic alias approach).

## Caveat: global registry

The dtype registry is global process state. Register dtypes once at startup (e.g.
module import time) and avoid re-registering different bases for the same type.

