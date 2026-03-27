## Typing overview

PydanTable targets two complementary typing experiences:

- **mypy**: a plugin infers schema-evolving return types for many `DataFrameModel` transform chains.
- **pyright/Pylance**: uses shipped stubs; for schema evolution, you opt into explicit `as_model(...)`/`try_as_model(...)`/`assert_model(...)`.

This page consolidates the typing story and links to the relevant contracts.

## The typing contract (nominal model, derived row type, structural helpers)

- **Nominal table type**: users name subclasses of `DataFrameModel` (for example `class Users(DataFrameModel): ...`).
- **Row type is derived**: each `DataFrameModel` subclass generates a per-row Pydantic model exposed as `Users.RowModel`.
- **Generics are for relationships / helpers**: for cross-model helpers, prefer structural typing rather than pretending `DataFrameModel[Row]` “is” a particular subclass.

### Structural helper types (`pydantable.typing`)

For reusable helpers that accept *any* model with a given row type, use the Protocol:

```python
from pydantable.typing import DataFrameModelWithRow

def materialize_rows(m: DataFrameModelWithRow[RowT]) -> list[RowT]:
    return m.rows()
```

## pyright/Pylance workflow (explicit after-model)

Pyright cannot express dependent “schema evolution” from transform chains, so the ergonomic pattern is:

```python
from pydantable import DataFrameModel

class Before(DataFrameModel):
    id: int
    age: int

class After(DataFrameModel):
    id: int
    age2: int

def pipeline(df: Before) -> After:
    out = df.with_columns(age2=df.age * 2).select("id", "age2")
    return out.as_model(After)
```

Safer variants:

- `try_as_model(After)` returns `After | None` on mismatch (no exception).
- `assert_model(After)` raises with a richer schema diff (missing/extra/type mismatches).

## mypy workflow (plugin-based inference)

### Enabling the plugin

Add the plugin to your mypy config:

```toml
[tool.mypy]
plugins = ["pydantable.mypy_plugin"]
```

### What the plugin can infer

Inference is intentionally conservative: it refines return types when arguments are **literal enough**.

- **Schema-evolving transforms** (when literal column names / literal config are provided):
  - `with_columns(...)` (best-effort type inference from mypy’s expression types + literals)
  - `select(...)`, `drop(...)` (string/list/tuple literals)
  - `rename({...})` (dict literal)
  - `join(..., on=..., suffix=...)`
  - `group_by(...).agg(out=("op","col"), ...)` (tuple literals; some ops map to `int`/`float`)
  - `melt(...)`, `unpivot(...)` (literal `id_vars`/`index`, plus literal `variable_name`/`value_name`)
  - `rolling_agg(..., op=..., out_name=...)`

- **Schema-preserving transforms** (kept as the same model type):
  - `fill_null`, `drop_nulls`, `explode`, `unnest`

- **Not inferred / intentionally skipped**:
  - dynamic/computed column name lists (variables, comprehensions, f-strings, unpacking)
  - `pivot(...)` (output columns depend on data values)

When the plugin can’t infer safely, it falls back to the original model type (and you can still use `as_model(...)`).

## Stubs and drift prevention

PydanTable ships `py.typed` and `.pyi` stubs for the public surface. In the repo:

- `scripts/generate_typing_artifacts.py` regenerates committed typing artifacts.
- `scripts/generate_typing_artifacts.py --check` fails if stubs are out of date.
- `make check-typing` runs: generator drift check → mypy → typing snippet tests.

## Related docs

- `DATAFRAMEMODEL.md`: end-user guide with typing examples.
- `SUPPORTED_TYPES.md`: dtype/nullable contract used by expression typing.
- `TROUBLESHOOTING.md`: common typing pitfalls.

