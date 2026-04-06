# PydanTable 2.0 Roadmap — Strict Typed DataFrame Spec Compliance

This document is the implementation-facing roadmap for a **breaking** `2.0.0` release
whose goal is **full compliance** with the rules in `docs/typed_dataframe_spec.md`.

## Non‑negotiable goals (2.0 contract)

- **Schema evolution must be knowable at type-check time**
- **No stringly-typed column access**
- **No runtime-dependent schema changes**
- **No in-place mutation**
- **No arbitrary Python execution inside transformations**
- **No untyped escape hatches**

PydanTable 2.0 treats typed DataFrames as a **typed relational algebra engine** with a
pluggable execution backend — not as a “flexible dataframe wrapper”.

## Status: what 2.0 has implemented (in this branch)

### Column access

- **Removed**: `df.<field>` column access (schema fields are no longer exposed via
  `__getattr__`).
- **Required**: `df.col.<field>` for typed column access.

### Schema-changing transforms

All schema-changing transforms require an explicit output schema and enforce it at
runtime by comparing the engine’s derived schema to the provided `AfterSchema`.

- **Removed (raise `TypeError`)**:
  - `select(...)`, `select_schema(...)`
  - `with_columns(...)`
  - `drop(...)`
  - `rename(...)`
  - `join(...)`
  - `group_by(...)`
  - dynamic helpers: `select_prefix/suffix`, `reorder_columns`, `move`,
    `rename_prefix/suffix`, `rename_with_selector`, `join_as_schema` (+ related helpers)

- **Added**:
  - `DataFrame.select_as(AfterSchema, ...)`
  - `DataFrame.with_columns_as(AfterSchema, ...)`
  - `DataFrame.drop_as(AfterSchema, ...)`
  - `DataFrame.rename_as(AfterSchema, mapping)`
  - `DataFrame.join_as(AfterSchema, other, on=[...]/left_on/right_on=[...])` — prefer keywords: `after_schema_type=` / `schema=` and `other=`
  - `DataFrame.group_by_agg_as(AfterSchema, keys=[...], **aggs)`

`DataFrameModel` mirrors these constraints:

- `DataFrameModel.select_as(AfterModel, ...)`
- `DataFrameModel.with_columns_as(AfterModel, ...)`
- `DataFrameModel.drop_as(AfterModel, ...)`
- `DataFrameModel.rename_as(AfterModel, ...)`
- `DataFrameModel.join_as(other, AfterModel, ...)` — prefer keywords: `other=` / `model=` / `after_model=`
- `DataFrameModel.group_by_agg_as(AfterModel, keys=[...], ...)`

## Migration guide (1.x → 2.0 patterns)

### Column access

**1.x**

```python
df.age * 2
```

**2.0**

```python
df.col.age * 2
```

### Select / projection

**1.x**

```python
out = df.select("id", "age")
```

**2.0**

```python
class After(DataFrameModel):
    id: int
    age: int

out = df.select_as(After.schema_model(), df.col.id, df.col.age)
```

### with_columns (add/replace)

**1.x**

```python
out = df.with_columns(age2=df.age * 2)
```

**2.0**

```python
class After(DataFrameModel):
    id: int
    age2: int

out = df.with_columns_as(After.schema_model(), age2=df.col.age * 2)
```

### Rename

**1.x**

```python
out = df.rename({"old": "new"})
```

**2.0**

```python
class After(DataFrameModel):
    new: int

out = df.rename_as(After.schema_model(), {df.col.old: "new"})
```

### Join

**1.x**

```python
out = left.join(right, on="id")
```

**2.0**

```python
class After(DataFrameModel):
    id: int
    # ... joined columns ...

# DataFrameModel: right-hand frame first if positionals; prefer keywords.
out = left.join_as(other=right, model=After, on=[left.col.id])
```

If your inputs are **`DataFrame[Schema]`** (not `DataFrameModel`), use **`DataFrame.join_as`**: `left.join_as(schema=After, other=right, on=[left.col.id])` (see {doc}`TRANSFORMS_QUICK_REF`).

### Group-by + aggregate

**1.x**

```python
out = df.group_by("g").agg(total=("sum", "v"))
```

**2.0**

```python
class After(DataFrameModel):
    g: int
    total: int

out = df.group_by_agg_as(After.schema_model(), keys=[df.col.g], total=("sum", df.col.v))
```

## Removed features (intentional)

These are removed because they violate the spec:

- Any schema-changing API accepting `str` column names.
- Any schema-changing API accepting `Selector` (dynamic column sets).
- Any schema-changing API that accepts Python callables to produce schema changes.
- “Convenience” helpers that compute the output schema from runtime column patterns
  (prefix/suffix, regex matches, dtype selectors, etc.).

## Typing strategy (2.0)

2.0 does not rely on mypy-plugin inference for schema evolution. Instead:

- **Every schema-changing transform** requires an explicit `AfterSchema`/`AfterModel`.
- Stubs mark removed methods as `Never` so type checkers flag them.

## Engineering checklist for a merge-ready 2.0

- Update remaining schema-changing APIs not yet migrated to `*_as` (e.g. casts,
  reshape/pivot APIs, rolling/window helpers) to either:
  - become `*_as` with explicit schema, or
  - be removed if inherently runtime-dependent.
- Remove or redesign optional façades (`pydantable.pandas`, `pydantable.pyspark`) that
  are fundamentally string-based.
- Rewrite docs (`README.md`, `docs/TYPING.md`, selector docs, cookbook) to the new
  API and remove legacy examples.
- Update typing generator (`scripts/generate_typing_artifacts.py`) and regenerate
  committed artifacts.
- Replace/refresh typing contract tests under `tests/typing/` for the new API.
- Add runtime tests that confirm **strings/selectors/callables are rejected** for
  schema-changing transforms.

## Timeline (suggested)

- **Milestone A**: core API strictness (column namespace + select/with/drop/rename) ✅
- **Milestone B**: join + group-by strictness ✅
- **Milestone C**: remove/replace remaining dynamic features (selectors, rolling,
  reshape, facades) ☐
- **Milestone D**: docs + typing contracts + CI stabilization ☐
- **2.0.0-beta** then **2.0.0** ☐

