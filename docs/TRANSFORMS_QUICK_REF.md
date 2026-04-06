# Transforms quick reference

Compact map of **schema-changing** APIs (strict 2.x). Full context: {doc}`DATAFRAMEMODEL`, {doc}`TYPING`, {doc}`INTERFACE_CONTRACT`.

## Core `*_as` methods

| Goal | API |
|------|-----|
| Add or replace columns (expressions) | `with_columns_as(After, **name=expr)` |
| Keep a subset of columns | `select_as(After, col1, col2, ...)` |
| Drop columns | `drop_as(After, col_to_drop, ...)` |
| Rename to match `After` field names | `rename_as(After, mapping)` |
| Join two frames | See **Join** below |
| Group + aggregate | `group_by_agg_as(After, keys=[...], name=(op, col), ...)` |
| Melts / pivots / explode / unnest | `melt_as`, `pivot_as`, `explode_as`, `unnest_as` (each takes `After`) |

Use **`try_as_model(After)`** / **`assert_model(After)`** when you need softer or richer schema checks around a frame.

## Join: prefer keywords

Positional argument **order differs** between `DataFrame` and `DataFrameModel`, so **prefer keywords** and always pass **`other=`** for the right-hand side.

**`DataFrame[S]`** — output schema first if positional, or use keywords:

```python
left.join_as(after_schema_type=After, other=right, on=[left.col.id], how="left")
# Alias: schema= is the same as after_schema_type=
left.join_as(schema=After, other=right, on=[left.col.id])
```

**`DataFrameModel`** — right-hand frame first if positional:

```python
left.join_as(other=right, model=After, on=[left.col.id], how="left")
# Alias: after_model= is the same as model=
left.join_as(other=right, after_model=After, on=[left.col.id])
```

Do not pass **`after_schema_type`** and **`schema=`** (or **`model`** and **`after_model=`**) with **different** types; pydantable raises `TypeError`.

## Subclassing vs sibling `After` models

When the result **extends** the input (same columns **plus** new ones, e.g. a join adds nullable columns), define **`class After(Base): new_col: ...`** and use that type in **`*_as`** so you do not re-declare inherited fields. See **Subclassing (merged schema)** in {doc}`DATAFRAMEMODEL`.

When you **drop** or **reshape** so the result is **not** a pure extension of the left schema, use a **separate** sibling `After` model (or chain `select_as` / `drop_as`).

## Organizing `DataFrameModel` classes in apps

- Keep **stable table / entity** models in one module (e.g. `schemas/tables.py`) and **pipeline stages** (join outputs, aggregates) in another (e.g. `schemas/pipeline.py`) so import cycles stay predictable.
- Prefer **`TYPE_CHECKING`** and **lazy imports** if a pipeline module needs API types from routers.
- Colocate a join/agg stage’s **`After`** type next to the function that introduces it when it is **only** used there.
- Example layout for a service: see `docs/examples/fastapi/service_layout/` (and {doc}`GOLDEN_PATH_FASTAPI`).

## Editor snippets

VS Code–style snippets for these patterns live in the repo at `.vscode/pydantable-transforms.code-snippets` (copy into your project if you like).
