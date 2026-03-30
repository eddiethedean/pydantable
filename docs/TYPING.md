# Typing overview

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

### `SupportsLazyAsyncMaterialize` (async `acollect`)

- **Use `DataFrameModelWithRow[RowT]`** when the helper needs **sync** row APIs (`rows`, `collect`, …) tied to a known `RowModel`.
- **Use `SupportsLazyAsyncMaterialize[Any]`** (or parameterize `RowT` if you do) when the helper only **awaits** **`acollect`** and must accept **both** a concrete `DataFrameModel` **and** a lazy `AwaitableDataFrameModel` (for example after **`aread_*`** or chained **`select`** / **`filter`** / …).

`SupportsLazyAsyncMaterialize` describes the **`acollect`** contract. It does **not** include sync **`collect`**: synchronous APIs should take `DataFrameModel` (or a subclass) instead.

The core **`DataFrame`** type also implements a compatible **`acollect`**; static typing treats the protocol as structural, so anything with a matching **`acollect`** is a candidate.

**Example — shared async materialization**

```python
from typing import Any

from pydantable.typing import SupportsLazyAsyncMaterialize


async def materialize_async(m: SupportsLazyAsyncMaterialize[Any]) -> Any:
    return await m.acollect()
```

**Example — endpoint or callback** (caller passes either `UserDF(...)` or `UserDF.aread_parquet(...)` then transforms)

```python
from typing import Any

from pydantable.typing import SupportsLazyAsyncMaterialize


async def handle(m: SupportsLazyAsyncMaterialize[Any]) -> Any:
    return await m.acollect()
```

At **runtime**, `SupportsLazyAsyncMaterialize` is `@runtime_checkable`, so `isinstance(x, SupportsLazyAsyncMaterialize)` succeeds when `x` has a callable **`acollect`** (duck typing). That check does **not** validate coroutine return types or argument kinds; use mypy/pyright for that.

**Static checkers:** Stubs may not list every lazy **`aread_*`** classmethod on each `DataFrameModel` subclass. If mypy/pyright complains on **`MyModel.aread_parquet(...)`**, assign via **`typing.cast(SupportsLazyAsyncMaterialize[Any], MyModel.aread_parquet(...))`**, bind **`_aread = MyModel.aread_parquet  # type: ignore[attr-defined]`**, or enable the pydantable **mypy plugin** where applicable.

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

### 1.2.0 column types (Literal, IP, WKB, `Annotated[str, ...]`)

These scalars are ordinary fields on your `DataFrameModel` subclass: the plugin still
matches transform outputs by **field name** and **static field type** from the class
body (`Literal[...]`, `ipaddress` classes, `WKB`, and plain or `Annotated` strings show
up in mypy’s analysis like `int` / `str`).

Pyright users keep the same workflow as other scalars: chained methods are typed as
`DataFrameModel[Any]` in stubs, so use **`as_model(After)`** / **`try_as_model`** /
**`assert_model`** when you need an explicit **`After`** type after `select` /
`with_columns` / `rename`.

Contract coverage lives in:

- `tests/test_extended_scalar_dtypes_v12.py` (runtime + schema helpers)
- `tests/test_typing_engine_parity.py` (Rust plan descriptors vs runtime `schema_fields`)
- `tests/test_mypy_dataframe_model_return_types.py` (`test_mypy_accepts_literal_ip_wkb_...`)
- `tests/test_pyright_dataframe_model_return_types.py` (`test_pyright_accepts_literal_ip_wkb_...`)

## Stubs and drift prevention

PydanTable ships `py.typed` and `.pyi` stubs for the public surface. In the repo:

- `scripts/generate_typing_artifacts.py` regenerates committed typing artifacts.
- `scripts/generate_typing_artifacts.py --check` fails if stubs are out of date.
- `make check-typing` runs: generator drift check → mypy → typing snippet tests.

## Related docs

- `DATAFRAMEMODEL.md`: end-user guide with typing examples.
- `SUPPORTED_TYPES.md`: dtype/nullable contract and **per-method** `Expr` rules
  (what dtypes each method accepts, null behavior, Polars vs stub execution).
- `INTERFACE_CONTRACT.md`: engine capabilities (Polars-backed vs row-wise stub).
- `TROUBLESHOOTING.md`: common typing pitfalls.

