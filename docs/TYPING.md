# Typing overview

PydanTable supports **one end-user strategy** for `DataFrameModel` static typing, plus a **checker** used to validate the library itself:

| Strategy | Checkers | Schema-evolving chains |
|----------|----------|------------------------|
| **Explicit after-model** | **Pyright**, **Pylance**, **Astral `ty`**, **mypy**, and any checker | Shipped `.pyi` stubs; schema-evolving transforms require `*_as(AfterModel, ...)` so the output schema is explicit. |

For application code type-checked with **Astral `ty`**, follow the same explicit pattern as
Pyright/Pylance: use `*_as(AfterModel, ...)` for schema evolution.

The **pydantable** repo runs **`ty check`** on first-party trees in CI (`make check-python`). That validates annotations and APIs.

This page consolidates the typing story and links to the relevant contracts.

## The typing contract (nominal model, derived row type, structural helpers)

- **Nominal table type**: users name subclasses of `DataFrameModel` (for example `class Users(DataFrameModel): ...`). Evolved schemas can **subclass** a base model to inherit its columns and only declare deltas.
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

**Deprecation (2.0):** avoid **`acollect(..., as_polars=...)`** and **`collect(..., as_polars=...)`**; they emit **`DeprecationWarning`** and will be removed in pydantable 2.0. Prefer **`ato_polars()`** / **`to_polars()`**, or columnar **`to_dict()`** / **`collect(as_lists=True)`**. See {doc}`VERSIONING` (planned removals).

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

At **runtime**, `SupportsLazyAsyncMaterialize` is `@runtime_checkable`, so `isinstance(x, SupportsLazyAsyncMaterialize)` succeeds when `x` has a callable **`acollect`** (duck typing). That check does **not** validate coroutine return types or argument kinds; use mypy, Pyright, or `ty` for that.

**Static checkers:** Stubs may not list every lazy **`aread_*`** classmethod on each `DataFrameModel` subclass. If **mypy**, **Pyright/Pylance**, or **`ty`** complains on **`MyModel.aread_parquet(...)`**, assign via **`typing.cast(SupportsLazyAsyncMaterialize[Any], MyModel.aread_parquet(...))`** or bind **`_aread = MyModel.aread_parquet  # type: ignore[attr-defined]`**.

## Strict 2.0 typing (explicit output model)

**Pyright**, **Pylance**, and **Astral `ty`** follow the same stub-based pattern. In strict 2.0, schema-changing transforms require `*_as(AfterModel, ...)`, and schema-preserving transforms keep the same model type.

In strict 2.0 mode, schema-changing transforms **require an explicit output model** (and enforce it at runtime).

```python
from pydantable import DataFrameModel

class Before(DataFrameModel):
    id: int
    age: int

class After(DataFrameModel):
    id: int
    age2: int

def pipeline(df: Before) -> After:
    class AfterFull(Before):
        age2: int

    out_full = df.with_columns_as(AfterFull, age2=df.col.age * 2)
    return out_full.drop_as(After, out_full.col.age)
```

Safer variants:

- `try_as_model(After)` returns `After | None` on mismatch (no exception).
- `assert_model(After)` raises with a richer schema diff (missing/extra/type mismatches).

### Aggregations (explicit output model)

Aggregations are inherently **schema-changing**. In strict 2.0, use the explicit `*_as`
APIs so the output schema is declared and runtime-validated.

```python
from pydantable import DataFrameModel

class Events(DataFrameModel):
    g: int
    v: int
    ts: str

class ByGroup(DataFrameModel):
    g: int
    total: int

class WithRolling(DataFrameModel):
    ts: str
    roll: int

def grouped(df: Events) -> ByGroup:
    return df.group_by_agg_as(ByGroup, keys=[df.col.g], total=("sum", df.col.v))
```

### Deterministic schema evolution (`*_as`)

In strict 2.0, schema evolution is always explicit: use the `*_as` APIs so the output
schema is statically declared and runtime-validated. **`AfterModel`** can **subclass**
the input model when the result adds columns (or overrides types/defaults) so you do not
re-list every inherited field; see {doc}`DATAFRAMEMODEL` **Subclassing (merged schema)**.

### Column types (Literal, IP, WKB, `Annotated[str, ...]`)

These scalars are ordinary fields on your `DataFrameModel` subclass. In strict 2.0,
schema evolution is explicit via `*_as(AfterModel, ...)` and validated at runtime.

Contract coverage lives in:

- `tests/test_extended_scalar_dtypes_v12.py` (runtime + schema helpers)
- `tests/test_typing_engine_parity.py` (Rust plan descriptors vs runtime `schema_fields`)
- `tests/test_pyright_dataframe_model_return_types.py` (`test_pyright_accepts_literal_ip_wkb_...`)

## Stubs and drift prevention

PydanTable ships `py.typed` and `.pyi` stubs for the public surface. In the repo:

- `scripts/generate_typing_artifacts.py` regenerates committed typing artifacts.
- `scripts/generate_typing_artifacts.py --check` fails if stubs are out of date.
- `make check-typing` runs: generator drift check → ty → typing snippet tests.

## Contributor workflow (static typing)

### Which checker does what

| Tool | Role |
|------|------|
| **Astral `ty`** | Primary checker for `python/pydantable`, `pydantable-protocol`, and `pydantable-native` (see `[tool.ty]` in `pyproject.toml`). Used in `make check-python` / CI. |
| **mypy** | Optional checker for contributors. The strict 2.0 API does not require a mypy plugin; schema evolution is explicit via `*_as(AfterModel, ...)`. |
| **Pyright** | Narrow config (`pyrightconfig.json`) targets typing **contract** tests under `tests/` plus `typings/`. Same explicit-`as_model` contract as **`ty`** for app code. Optional `pyrightconfig-strict.json` type-checks the full `python/pydantable` tree for maintainers (`make pyright-check-strict`); expect noise and optional deps. |

### Public vs internal API (pragmatic `Any`)

- **Public surface** — imports from `pydantable`, `pydantable.dataframe`, and documented I/O helpers: prefer concrete types, `Protocol`s, `PathLike`, `Mapping`/`Sequence`, and `TYPE_CHECKING` imports for types that would create cycles.
- **Internal modules** — engine plans, Rust handles, and dynamic adapters may keep `Any` where the runtime type is opaque or checker-specific; narrow with `NewType` / small `Protocol`s only when it reduces real bugs without lying.

### Policy: `typing.Any` must be justified

`Any` disables static checking for that value. **Do not use it because a precise type is merely inconvenient.** Prefer, in order:

1. **Concrete types** (`str`, `Path`, `bytes`, models, `DataFrame[...]`).
2. **`TypeVar` / `Generic`** when the same function is polymorphic in a known way.
3. **`Protocol`** for structural APIs (duck typing with a name).
4. **`object`** when the code only needs identity, `repr`, or a few `isinstance` branches (unknown cell values, opaque scan objects).
5. **`Mapping[str, ...]` / `Sequence[...]`** instead of untyped `dict` / `list`.

**When `Any` *is* justified** (document the category in PRs for new hotspots; legacy code is covered by the table below):

| Category | Where it shows up | Why `Any` instead of lying |
|----------|-------------------|----------------------------|
| **Opaque Rust / PyO3** | `rust_engine.py`, `pydantable_native/*`, plan handles, `execute_plan(plan, data, …)` | Runtime objects are defined in Rust; Python sees untyped or generated bindings. Mirrors `ExecutionEngine` in `pydantable_protocol` (which uses `Any` for plan/data until a portable IR exists). |
| **Optional / heavy deps** | `io/extras.py`, SQL, Kafka, cloud clients | Third-party libraries may be absent or thinly stubbed; signatures stay permissive at boundaries. |
| **Dynamic adapters** | `pandas.py`, `pyspark/*`, plugin surfaces | APIs mimic other ecosystems; parameters are intentionally wide. |
| **Schema / Pydantic internals** | `schema/_impl.py`, `dataframe_model.py` | `TypeAdapter`, `create_model`, and validation hooks use dynamic types from Pydantic. |
| **Public “column dict”** | `dict[str, list[Any]]` for materialized columns | Column element types vary by dtype; a precise `Union` would be enormous and still incomplete. Prefer documenting invariants in `SUPPORTED_TYPES.md`. |
| **Explicit escape hatch** | Rare | Only with a **short comment** at the definition site: why a `Protocol` or `TypeVar` is not yet possible (e.g. circular import, pending refactor). |

**Review rule:** If you add new `Any` on a **public** symbol, add a sentence in the docstring or link to this section. If you can use `object` or a `Protocol` without changing behavior, use that instead.

### Phased strictness (`ty`)

`[tool.ty.rules]` in `pyproject.toml` enables some rule families gradually. Currently enforced at **error** (where clean): **`unknown-argument`**, **`invalid-argument-type`**, **`invalid-return-type`**, **`unsupported-operator`**. Others (for example **`not-iterable`**) stay **ignore** until Astral `ty` handles async generators and `async for` over imported helpers without false positives. When tightening a rule, fix callsites or add a **narrow** suppression with a short comment; prefer fixing types over broad `[tool.ty.analysis]` overrides.

Hotspots for future annotation work (rough counts in `python/pydantable/**/*.py`, subject to churn): `Any` appears most often in `dataframe/_impl.py`, `dataframe_model.py`, `pandas.py`, `rust_engine.py`, and `io/__init__.py`; `# type: ignore` is concentrated in `io/extras.py`, `pandas.py`, and `schema/_impl.py`; `cast(` is common in `pyspark/dataframe.py` and `dataframe_model.py`.

### Local commands

```bash
make ty-check              # Astral ty (matches main CI config)
make ty-check-minimal      # ty in a minimal venv (optional imports stay sound)
make check-typing          # stub drift check + ty + mypy/pyright contract tests
make pyright-check-strict  # optional full-package Pyright (see pyrightconfig-strict.json)
```

### Environment notes

Use a **single-Python** virtualenv for local runs (for example only `lib/python3.10` under `.venv`). If `ty` reports unresolved imports for core deps like `pydantic`, recreate the venv or align the interpreter `ty` resolves with the one that has your dependencies installed (`make ty-check-minimal` uses a dedicated minimal venv).

## Related docs

- `DATAFRAMEMODEL.md`: end-user guide with typing examples.
- `SUPPORTED_TYPES.md`: dtype/nullable contract and **per-method** `Expr` rules
  (what dtypes each method accepts, null behavior, Polars vs stub execution).
- `INTERFACE_CONTRACT.md`: engine capabilities (Polars-backed vs row-wise stub).
- `TROUBLESHOOTING.md`: common typing pitfalls.

