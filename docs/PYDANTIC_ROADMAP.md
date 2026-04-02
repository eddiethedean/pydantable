# PydanTable × Pydantic roadmap (ambitious)

This document is a **forward-looking** roadmap for deepening PydanTable’s use of **Pydantic v2** and adding **pydantable-native model features** that build on Pydantic rather than competing with it.

It is intentionally **broader than a single release**. Items are grouped by theme and then by suggested phases. Each theme includes:

- **Why** (what it unlocks)
- **Proposed API** (rough sketch)
- **Implementation notes** (where it likely lives in this repo)
- **Risks / trade-offs**
- **Test strategy** (how we’d lock in behavior)

## Guiding principles

- **Pydantic stays the schema authority** for row-level validation/serialization semantics.
- **Typed DataFrames remain columnar-first** for performance; “row model” features must not force row-by-row work unless explicitly requested.
- **Rust stays the plan validator/executor**; Python/Pydantic features should enrich validation/serialization and surface metadata into planning when useful.
- **Additive-by-default**: new functionality should be opt-in or backward compatible unless it closes a correctness gap.

## Current baseline (what exists today)

- **Generated row models for `DataFrameModel`** are created via `pydantic.create_model` using `Schema` as the base.
  - File: `python/pydantable/dataframe_model.py`
- **Schema base** is `Schema(BaseModel)` with `extra="forbid"`.
  - File: `python/pydantable/schema/_impl.py`
- **Per-element validation** uses `TypeAdapter.validate_python` for each column element when `trusted_mode="off"`.
  - File: `python/pydantable/schema/_impl.py` (`validate_columns_strict`)
- **Nested Pydantic models are supported** as struct columns; type hints are resolved with `get_type_hints(include_extras=True)`.
  - File: `python/pydantable/schema/_impl.py`
- **Custom scalar dtype example**: `WKB` implements `__get_pydantic_core_schema__`.
  - File: `python/pydantable/types.py`

## Theme A — RowModel customization (validators, config, computed fields)

### A1. User-defined base for generated RowModel (`row_base`)

- **Why**: today, `DataFrameModel` users can’t easily attach Pydantic config/validators to the *generated* row type. This limits normalization rules (e.g., strip whitespace), cross-field checks, and custom serialization.
- **Proposed API**:

```python
from pydantic import ConfigDict, field_validator, computed_field
from pydantable import DataFrameModel, Schema


class UserRowBase(Schema):
    model_config = ConfigDict(str_strip_whitespace=True, populate_by_name=True)

    @field_validator("email")
    @classmethod
    def normalize_email(cls, v: str) -> str:
        return v.lower()

    @computed_field
    @property
    def email_domain(self) -> str:
        return self.email.split("@", 1)[-1]


class Users(DataFrameModel):
    __row_base__ = UserRowBase   # new hook

    id: int
    email: str
```

- **Implementation notes**:
  - `DataFrameModel.__init_subclass__` currently calls `create_model(..., __base__=Schema, **field_defs)`.
  - Change to pick `__row_base__` if present, else `Schema`.
  - Ensure `__row_base__` is a `BaseModel` subclass (ideally `Schema`) to preserve `extra="forbid"` defaults unless intentionally overridden.
- **Risks / trade-offs**:
  - Validators and computed fields can be expensive if applied row-by-row on large datasets; we need clear documentation about where they execute:
    - **row-sequence inputs** already validate per-row.
    - **columnar inputs** validate per-element; computed fields shouldn’t run unless materializing row models.
- **Test strategy**:
  - `RowModel` includes base config (`model_config`) behavior.
  - `to_dicts()` includes computed fields iff requested (see A3).

### A2. Allow defining a nested `class Row(Schema): ...` inside `DataFrameModel`

- **Why**: reduces boilerplate and keeps schema + row policies co-located.
- **Proposed API**:

```python
class Users(DataFrameModel):
    class Row(Schema):
        model_config = ConfigDict(str_strip_whitespace=True)

    id: int
    name: str
```

- **Implementation notes**:
  - Detect `Row` attribute on subclass during `__init_subclass__`.
  - Merge/compose the nested Row with generated fields:
    - Option 1: `__base__=Users.Row`
    - Option 2: multiple inheritance (`(Users.Row, Schema)`), but Pydantic model inheritance needs careful rules.
- **Risks**:
  - Python class namespace ordering; avoid creating confusing MROs.
- **Test strategy**:
  - Ensure nested `Row` validators apply for row inputs and for `collect()` results.

### A3. First-class serialization options on `to_dicts()` / `ato_dicts()`

- **Why**: Pydantic v2 serialization options are powerful (aliases, exclude_none, json mode). PydanTable already routes through `RowModel.model_dump()`, but doesn’t expose options.
- **Proposed API**:

```python
df.to_dicts(by_alias=True, exclude_none=True, mode="json")
df.to_dicts(include_computed=True)          # optional
df.to_dicts(context={"request_id": "..."} ) # optional; passed to serializers
```

- **Implementation notes**:
  - `DataFrameModel.to_dicts` currently does `[row.model_dump() for row in self.rows()]`.
  - Thread through kwargs to `model_dump`.
  - Decide defaults and whether computed fields are included (Pydantic supports computed fields, but toggling inclusion is a pydantable UX decision).
- **Risks**:
  - API surface area; keep names aligned with Pydantic (`by_alias`, `exclude`, `include`, `exclude_none`, `mode`).
- **Test strategy**:
  - Alias serialization on nested models.
  - `mode="json"` for datetimes/decimals/UUIDs.

## Theme B — Column metadata as policy (not just typing)

### B1. Column-level policies via `Annotated[..., Field(json_schema_extra=...)]`

- **Why**: users already type columns; adding policy metadata enables consistent behavior for:
  - redaction in API responses
  - strictness per-column
  - documentation/OpenAPI enhancement
- **Proposed API**:

```python
from typing import Annotated
from pydantic import Field

Email = Annotated[str, Field(json_schema_extra={"pydantable": {"pii": True}})]

class Users(DataFrameModel):
    id: int
    email: Email
```

- **Implementation notes**:
  - Use `Schema.model_fields[name]` to read `FieldInfo.json_schema_extra`.
  - Build small utilities:
    - `get_column_policy(model, name) -> dict`
    - `redact_dicts(rows, policy=...)`
- **Risks**:
  - Avoid overfitting to one policy format; keep namespaced (`"pydantable": {...}`).
- **Test strategy**:
  - Redaction behavior is stable and opt-in.

### B2. Schema-level policy sets (`__pydantable__` config)

- **Why**: common policies should be set once per model.
- **Proposed API**:

```python
class Users(DataFrameModel):
    __pydantable__ = {
        "redaction": {"default": "keep", "columns": {"email": "hash"}},
        "validation_profile": "strict_api",
    }
```

- **Implementation notes**:
  - Keep this separate from `model_config` (Pydantic) to avoid collisions.
- **Test strategy**:
  - Verify policy is inherited by derived models where it makes sense (or explicitly not).

## Theme C — Custom dtypes and “semantic types” powered by Pydantic CoreSchema

### C1. Generalize custom scalar dtype pattern (beyond `WKB`)

- **Why**: enable rich domain types (ULID, SnowflakeID, CountryCode, Currency, EmailStr-like semantics, strongly typed IDs) while keeping runtime storage compatible (usually `str`/`int`/`bytes`).
- **Proposed API**:
  - Provide docs + helper base class patterns:

```python
class ULID(str):
    @classmethod
    def __get_pydantic_core_schema__(cls, source_type, handler):
        # validate/coerce; return a string schema wrapped with validator
        ...
```

  - Optional: `pydantable.types` ships a few common ones (careful: scope creep).
- **Implementation notes**:
  - `schema/_impl.py`’s strict/trusted dtype matching may treat custom types as “scalar”: it currently checks `issubclass(inner, BaseModel)` and `inner in SUPPORTED_SCALARS`.
  - We may need a hook: “custom scalar types that behave like str/int/bytes” for strict mode.
- **Risks**:
  - Rust dtype mapping: do we treat as base scalar (e.g. string) or as semantic subtype? If semantic, we need stable descriptor encoding.
- **Test strategy**:
  - Round-trip through `to_dict`/`collect` and (optionally) `to_arrow` without losing meaning.

### C2. Custom dtype registry (opt-in)

- **Why**: users shouldn’t need to patch pydantable internals to teach strict mode how to treat custom scalars.
- **Proposed API**:

```python
from pydantable.dtypes import register_scalar

register_scalar(ULID, base="str")
register_scalar(MyIntId, base="int")
```

- **Implementation notes**:
  - Schema validation (`is_supported_*`) can accept registered types.
  - Strict matching uses registry base to validate trusted buffers.
- **Risks**:
  - Global mutable registry; offer local/override options where possible.
- **Test strategy**:
  - Registry affects `validate_columns_strict` strict-mode checks.

## Theme D — Better “structured columns” (nested models, maps, lists) via Pydantic features

### D1. Typed map columns: `dict[str, V]` with Pydantic value adapters

- **Why**: you already support `dict[str, V]` and do some Arrow map normalization; expand to leverage Pydantic for value parsing and consistent serialization.
- **Proposed features**:
  - value coercion via `TypeAdapter(V)` when in `trusted_mode="off"`.
  - optional per-column constraints via `Annotated[dict[str, V], Field(...)]`.
- **Implementation notes**:
  - `schema/_impl.py` already has `_normalize_pyarrow_map_column` and `_trusted_pyarrow_map_value_matches`.
- **Risks**:
  - Performance: validating each map value may be heavy; keep it opt-in or limited to `trusted_mode="off"`.

### D2. Nested model columns: allow per-field constraints/aliases to propagate

- **Why**: nested `BaseModel` columns are supported; ensure `get_type_hints(include_extras=True)` preserves `Annotated` constraints.
- **Implementation notes**:
  - Already uses `include_extras=True`; add docs and tests to confirm constraints are enforced.

## Theme E — Validation profiles and “quality modes”

### E1. Named validation profiles (“service strict”, “batch lenient”, “trusted upstream”)

- **Why**: current `trusted_mode` is powerful but coarse. Services often want:
  - strict input validation at boundaries,
  - but more lenient internal transforms / IO scans.
- **Proposed API**:

```python
class Users(DataFrameModel):
    __pydantable__ = {"validation_profile": "service_strict"}

df = Users(data, validation_profile="batch_lenient")
```

- **Suggested semantics**:
  - profile expands into:
    - default `trusted_mode`
    - default `fill_missing_optional`
    - default `ignore_errors`
    - serialization defaults (`exclude_none`, `mode="json"`)
- **Implementation notes**:
  - Keep as a thin preset layer over existing knobs.
- **Test strategy**:
  - Profiles map deterministically to existing behavior.

### E2. Column-specific strictness

- **Why**: some columns (IDs, timestamps) must be strict; others (freeform tags) can be lenient.
- **Proposed API**:

```python
StrictInt = Annotated[int, Field(json_schema_extra={"pydantable": {"strict": True}})]
LenientStr = Annotated[str, Field(json_schema_extra={"pydantable": {"coerce": True}})]
```

- **Implementation notes**:
  - This likely integrates with `TypeAdapter` configuration and/or pre-validators.
- **Risks**:
  - Hard to do without full row-level validation; consider limiting to row inputs / boundary serialization.

## Theme F — FastAPI/OpenAPI “model-first” ergonomics

### F1. OpenAPI-friendly columnar models with aliases + examples

- **Why**: pydantable already supports columnar bodies; enrich OpenAPI with better schemas/examples derived from Pydantic metadata.
- **Proposed features**:
  - include example payloads for columnar and row-list shapes.
  - propagate `Field(description=..., examples=...)` into OpenAPI.
- **Implementation notes**:
  - Likely in `python/pydantable/fastapi/` modules.

### F2. Error modeling: structured `ValidationError` surfaces for batch ingest

- **Why**: you already allow `ignore_errors` + `on_validation_errors`. Standardize error schemas for API responses.
- **Proposed API**:
  - `pydantable.fastapi` exports a Pydantic model for error payloads (row index, field errors, raw row).

## Theme G — Developer experience: introspection, debugging, and docs generation

### G1. `model_schema()` / `column_policies()` helpers

- **Why**: make it easy to inspect how Pydantic sees the table.
- **Proposed API**:

```python
Users.row_model().model_json_schema()
Users.schema_model().model_json_schema()
Users.column_policies()
```

### G2. “Contract tests” generator for schema changes

- **Why**: schema-evolving transforms are core; provide a helper to assert schema transitions.

## Suggested phased plan

### Phase 1 — “Pydantic-first schema hooks”

Implemented (target: **1.12.0**):

- **A1/A2 RowModel base hooks**: `DataFrameModel` supports both:
  - `__row_base__ = SomePydanticModel`
  - nested `class Row(Schema): ...`
  with precedence **nested `Row` wins**, else `__row_base__`, else default `Schema`.
- **A3 Serialization passthrough**:
  - `df.to_dicts(**model_dump_kwargs)` forwards to `row.model_dump(**...)`
  - `await df.ato_dicts(**model_dump_kwargs)` forwards similarly
- **B1 Column metadata reading**:
  - `MyDF.column_policies()` / `MyDF.column_policy(name)` read from
    `Field(json_schema_extra={"pydantable": {...}})` on schema fields.
- **G1 Introspection helpers**:
  - `MyDF.row_json_schema(**kwargs)` / `MyDF.schema_json_schema(**kwargs)`
    wrap Pydantic `model_json_schema`.

### Phase 2 — “Policies become behavior”

- B2 schema-level policy sets
- E1 validation profiles (preset layer)
- F2 structured ingest error schema + FastAPI mapping helpers

### Phase 3 — “Custom dtypes as a first-class extension point”

- C2 dtype registry
- C1 docs + a few blessed semantic types (very conservative)

### Phase 4 — “Per-column and nested strictness”

- E2 column-specific strictness (careful: performance + semantics)
- D1 deeper typed map/list validation options (opt-in)

### Phase 5 — “Model-driven service ergonomics”

- F1 richer OpenAPI generation + examples
- Optional: schema-driven request/response transforms (aliases, redaction defaults)

## Non-goals / cautions

- Avoid turning PydanTable into a second validation framework; defer to Pydantic whenever possible.
- Avoid making default execution row-wise; keep columnar plan execution as the happy path.
- Be explicit about where validators run (ingest vs materialization vs serialization).

