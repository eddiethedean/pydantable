# Typing overview

PydanTable supports **two end-user strategies** for `DataFrameModel` static typing, plus a **third checker** used to validate the library itself:

| Strategy | Checkers | Schema-evolving chains |
|----------|----------|------------------------|
| **Inferred chains** | **mypy** with `pydantable.mypy_plugin` | Return types refine from literals / conservative plugin rules. |
| **Explicit after-model** | **Pyright**, **Pylance**, **Astral `ty`**, and any checker **without** the plugin | Shipped `.pyi` stubs; after a transform, use `as_model(...)` / `try_as_model(...)` / `assert_model(...)`. |

**Astral `ty`** does not load mypy plugins. For application code type-checked with `ty`, treat it like **Pyright/Pylance**: use the explicit after-model pattern, not plugin inference.

The **pydantable** repo runs **`ty check`** on first-party trees in CI (`make check-python`). That validates annotations and APIs; it is **not** a substitute for running mypy with the plugin in your project if you rely on inferred chains.

**PlanFrame-first static typing (roadmap):** being a PlanFrame adapter means leaning on **PlanFrame’s** `Frame` / `Expr` typing, generated stubs, and `materialize_model` boundaries rather than duplicating Resolve logic in the [**mypy plugin**](https://github.com/eddiethedean/pydantable/blob/main/python/pydantable/mypy_plugin.py) forever. Long-term direction, phases, and plugin scope: {doc}`PLANFRAME_TYPING_ROADMAP`.

This page consolidates the typing story and links to the relevant contracts.

## PlanFrame boundary types (what stubs expose)

Even if you don’t use the `df.planframe` typing-first chain directly, some `DataFrameModel` APIs accept **PlanFrame types** at the boundary:

- **Sort/join keys**: `str` or `planframe.expr.api.Expr` (e.g. `pf.col("id")`)
- **Group keys**: `str` or `planframe.expr.api.Col` (not general expressions; add a key column first)
- **Selector inputs**: pydantable `Selector` (has `resolve(...)`) or PlanFrame `ColumnSelector` (has `select(schema)`)

These are typed in the shipped stubs so Pyright and Astral `ty` match the runtime contract.

## Phase T3 boundary recipe (PlanFrame chain → exact `DataFrameModel`)

When you build a typing-first chain with PlanFrame (`df.planframe...`), the supported way to get back to an **exact** pydantable model type is to materialize **columnar** data at the PlanFrame boundary and then construct the target `DataFrameModel`.

Use `materialize_dataframe_model`:

```python
from __future__ import annotations

from planframe.expr import api as pf

from pydantable import DataFrameModel
from pydantable.planframe_adapter import materialize_dataframe_model


class Before(DataFrameModel):
    id: int
    age: int


class After(DataFrameModel):
    id: int


def pipeline(df: Before) -> After:
    pf_out = df.planframe.filter(pf.col("age") > 0).select("id")
    return materialize_dataframe_model(pf_out, After, trusted_mode="shape_only")
```

## Phase T0 checker matrix (recommended path per checker)

This table records the **expected** and **recommended** typing story per checker, including the planned PlanFrame-first path (Phase T1) and boundary model (Phase T3). Until Phase T1 lands, treat the “Frame exposure” column as a target state.

| Checker | `DataFrameModel` chains only (today) | PlanFrame `Frame` exposure (Phase T1) | `materialize_model` bridge (Phase T3) |
|---|---|---|---|
| **mypy + plugin** | **Best** schema-evolving inference when args are literal enough. Fallback: `as_model(...)`. | Prefer PlanFrame chain typing when you need Pyright-like Resolve semantics; plugin remains for `DataFrameModel`-only ergonomics. | Use boundary recipe to get exact output types; still compatible with plugin on the pydantable side. |
| **mypy (no plugin)** | Treat like Pyright: **explicit** after-model (`as_model` / `assert_model`) for schema changes; chains remain loose. | PlanFrame chain becomes the primary way to get “inferred” schema changes without the plugin (via stubs/Resolve tiers). | Boundary recipe yields exact output model at explicit points (no plugin required). |
| **Pyright / Pylance** | **Explicit after-model** workflow (`as_model` / `try_as_model` / `assert_model`, plus `*_as_model` helpers). | PlanFrame chain is the intended “fully typed transforms” path (literals + upstream stubs; future: optional plugin). | Use boundary recipe for exact output types where needed; complements PlanFrame chain typing. |
| **Astral `ty`** | Same as Pyright: **no mypy plugins**; rely on shipped stubs + explicit after-model helpers. | PlanFrame chain should provide the strongest static typing story without relying on mypy internals. | Boundary recipe gives explicit exact output types; keep `DataFrameModel` validation semantics centralized. |

**Common constraint for typed paths:** prefer **literal column names** and avoid dynamic/computed column name lists when you want the checker to follow schema evolution rules (matches PlanFrame’s typing constraints).

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

**Static checkers:** Stubs may not list every lazy **`aread_*`** classmethod on each `DataFrameModel` subclass. If **mypy** (no plugin), **Pyright/Pylance**, or **`ty`** complains on **`MyModel.aread_parquet(...)`**, assign via **`typing.cast(SupportsLazyAsyncMaterialize[Any], MyModel.aread_parquet(...))`**, bind **`_aread = MyModel.aread_parquet  # type: ignore[attr-defined]`**, or enable the pydantable **mypy plugin** (mypy only) where applicable.

## Pyright, Pylance, and Astral `ty` (explicit after-model)

**Pyright**, **Pylance**, and **Astral `ty`** cannot apply the mypy plugin, so they follow the same stub-based pattern: chained transforms are loosely typed until you assert an after-model. The examples below say “Pyright”; use the identical **`as_model` / `try_as_model` / `assert_model`** workflow with **`ty check`** on your project.

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

### Typed escape hatches for aggregations (Pyright / `ty`)

Some operations are inherently **schema-changing** (for example grouped aggregations and
rolling aggregations). For Pyright/`ty` users, prefer the explicit `*_as_model` helpers
so the return type is your declared after-model.

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
    return df.group_by("g").agg_as_model(ByGroup, total=("sum", "v"))

def rolling(df: Events) -> WithRolling:
    return df.rolling_agg_as_model(
        WithRolling,
        on="ts",
        column="v",
        window_size=3,
        op="sum",
        out_name="roll",
    )
```

### Typed escape hatches for schema-changing transforms (Pyright / `ty`)

For other deterministic schema-changing transforms, use the dedicated helpers:

- `melt_as_model(...)` / `melt_try_as_model(...)` / `melt_assert_model(...)`
- `unpivot_as_model(...)` / `unpivot_try_as_model(...)` / `unpivot_assert_model(...)`
- `join_as_model(...)` / `join_try_as_model(...)` / `join_assert_model(...)`

## mypy workflow (plugin-based inference)

If you use **Astral `ty`** or **Pyright** on your project instead of mypy, use the **explicit after-model** section above — the plugin applies **only** to mypy.

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

Users without the mypy plugin (Pyright, Pylance, **`ty`**, and so on) keep the same workflow as other scalars: chained methods are typed as
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
- `make check-typing` runs: generator drift check → ty → typing snippet tests.

## Contributor workflow (static typing)

### Which checker does what

| Tool | Role |
|------|------|
| **Astral `ty`** | Primary checker for `python/pydantable`, `pydantable-protocol`, and `pydantable-native` (see `[tool.ty]` in `pyproject.toml`). Used in `make check-python` / CI. **No mypy plugins** — for `DataFrameModel`, it matches the **stub + `as_model`** story (same as Pyright), not plugin inference. |
| **mypy** + `pydantable.mypy_plugin` | Optional schema-evolving `DataFrameModel` chains for **mypy** users; run via `tests/test_mypy_*.py` or `mypy` with the repo config. `tests.*` is ignored by mypy by design. |
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
| **Mypy plugin** | `mypy_plugin.py` | Operates on mypy’s internal IR (`Any` is required by the plugin API). |
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

