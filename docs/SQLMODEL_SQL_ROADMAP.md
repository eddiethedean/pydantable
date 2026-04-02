# SQLModel-first SQL I/O roadmap (v1.13.0 focus)

This document is the **plan and phased roadmap** for migrating pydantable’s SQL I/O to a **SQLModel-first** design, targeting **v1.13.0** as the first release where the new APIs are the recommended default.

## Goals (what “SQLModel-first” means)

- **SQLModel is required for SQL I/O**: if you want to read/write from a database using `pydantable.io`, you install **`pydantable[sql]`**, which includes **SQLModel + SQLAlchemy** (drivers remain user-installed).
- **User defines table schema explicitly** via a `SQLModel` class with `table=True`. pydantable does **not** infer SQL DDL from column dict samples.
- **Reads return column dict batches** (`dict[str, list]`) and can be wrapped into a typed `DataFrameModel` (`MyDF(cols, ...)`) just like today.
- **Writes are schema-driven**: `if_exists="replace"` is supported, but it requires a fully defined SQLModel table class for the new schema.

## Non-goals (v1.13.0)

- A SQL execution engine for pydantable logical plans (lowering transforms to SQL) — that’s “future engine” scope.
- Full ORM relationship loading / joined eager loads — SQLModel support is for **table schemas + queries**, not ORM graphs.
- Automatic generation of SQLModel from `DataFrameModel` (or vice versa) as the primary UX. We may add helpers later, but v1.13.0 prioritizes **explicit** user-owned models.

## Design principles

- **Schema authority**: SQL table schema comes from **SQLModel**, dataframe schema comes from **`DataFrameModel`** annotations + Pydantic.
- **Two-phase typing**: operations that can’t be statically typed remain `DataFrameModel[Any]`, and users can “re-type” with `as_model(...)`.
- **Safety by default**: dangerous actions (dropping tables) require explicit user intent and trusted identifiers.
- **Streaming-friendly**: preserve the current `iter_*` / `fetch_*` split and batch semantics.

---

## Phase 0 — Dependency + policy decision (v1.13.0)

**Deliverables**

- `pydantable[sql]` includes **`sqlmodel`** (and still `sqlalchemy`).
- Update docs to state: **SQL I/O requires SQLModel** (DBAPI drivers still required).
- Define import / availability checks and error messages (e.g. a single `MissingOptionalDependencyError`-style message for `sqlmodel`).

**Acceptance criteria**

- Installing `pydantable[sql]` yields `import sqlmodel` success.
- Importing `pydantable.io` does **not** require SQLModel unless SQL functions are used (keep optional deps optional).

**Status (v1.13.0 dev): Implemented** — `sqlmodel` is listed under **`pydantable[sql]`** (and dev/docs extras where applicable); lazy import via **`pydantable.io.sqlmodel_read._require_sqlmodel()`** raises **`pydantable.errors.MissingOptionalDependency`** with an install hint.

---

## Phase 1 — New SQLModel-first read APIs (v1.13.0)

**Problem:** current read path is raw `sql: str` + `text(sql)`. It’s flexible, but it makes the schema contract implicit.

**Deliverables**

- Add `pydantable.io.fetch_sqlmodel(...)` and `pydantable.io.iter_sqlmodel(...)`.
- Add async variants mirroring the existing pattern:
  - `afetch_sqlmodel(..., executor=None)`
  - `aiter_sqlmodel(..., executor=None)`
- Ensure parity with existing knobs where it matters:
  - `batch_size`
  - `auto_stream` + threshold behavior (return a `StreamingColumns`-like object or keep the same type)

**Proposed API shape**

```python
from sqlmodel import SQLModel
from sqlalchemy.engine import Engine, Connection

def fetch_sqlmodel(
    model: type[SQLModel],
    bind: str | Engine | Connection,
    *,
    where: object | None = None,
    parameters: dict[str, object] | None = None,
    columns: list[object] | None = None,
    order_by: list[object] | None = None,
    limit: int | None = None,
    batch_size: int | None = None,
    auto_stream: bool = True,
    auto_stream_threshold_rows: int | None = None,
) -> dict[str, list[object]] | object: ...

def iter_sqlmodel(
    model: type[SQLModel],
    bind: str | Engine | Connection,
    *,
    where: object | None = None,
    parameters: dict[str, object] | None = None,
    columns: list[object] | None = None,
    order_by: list[object] | None = None,
    limit: int | None = None,
    batch_size: int | None = None,
) -> "Iterator[dict[str, list[object]]]": ...
```

**Notes**

- `where` / `order_by` / `columns` can be SQLAlchemy expressions; keep typing permissive in v1.13.0 and tighten later.
- `parameters` exists for cases where the `where` clause includes bound params.

**Acceptance criteria**

- A round-trip example can do:
  - `cols = fetch_sqlmodel(UserTable, engine)`
  - `df = UsersDF(cols, trusted_mode=...)`
- `iter_sqlmodel` yields rectangular batches with stable column keys.

**Status (v1.13.0 dev): Implemented** — synchronous **`fetch_sqlmodel`** / **`iter_sqlmodel`** in **`python/pydantable/io/sqlmodel_read.py`**, async **`afetch_sqlmodel`** / **`aiter_sqlmodel`** in **`python/pydantable/io/__init__.py`**, re-exported from **`pydantable`**. Batching and **`StreamingColumns`** / **`auto_stream_threshold_rows`** match **`fetch_sql`**. Tests: **`tests/test_sqlmodel_io_phase01.py`**.

---

## Phase 2 — New SQLModel-first write APIs (v1.13.0)

**Problem:** current `if_exists="replace"` uses inferred SQL column types (`bool|int|float|text`), which is not robust.

**Deliverables**

- Add `pydantable.io.write_sqlmodel(data, model, bind, ..., if_exists="append|replace")`.
- Add batch helpers mirroring existing `write_sql_batches`:
  - `write_sqlmodel_batches(batches, model, bind, ...)`
  - async variants using `asyncio.to_thread`.
- Implement `if_exists="replace"` as:
  - drop existing table (if present)
  - create new table from `model.__table__`
  - insert rows

**Proposed API shape**

```python
def write_sqlmodel(
    data: dict[str, list[object]],
    model: type[SQLModel],
    bind: str | Engine | Connection,
    *,
    if_exists: str = "append",
    chunk_size: int | None = None,
    validate_rows: bool = False,
    replace_ok: bool = False,
) -> None: ...
```

**Safety**

- Require `replace_ok=True` (or similar) when `if_exists="replace"` to make destructive intent explicit.

**Acceptance criteria**

- `replace` uses the SQLModel schema (no inference).
- Writes are chunked, transactional, and error messages include useful context (table name, offending column, row index if validation is enabled).

---

## Phase 3 — `DataFrameModel` conveniences (v1.13.0)

**Deliverables**

- Add `MyDF.write_sqlmodel(UserTable, bind=..., if_exists=..., ...)` instance method.
- Add `MyDF.iter_sqlmodel(UserTable, bind=..., ...)` classmethod that yields `Iterator[MyDF]` batches (mirroring `iter_*` file readers).
- Optionally add `MyDF.fetch_sqlmodel(...) -> MyDF` as a convenience wrapper over constructor + `fetch_sqlmodel`.

**Acceptance criteria**

- Services can be written in a “model-first” style on both ends:
  - SQLModel defines the DB table
  - `DataFrameModel` defines the dataframe schema and Pydantic behavior

---

## Phase 4 — Deprecate/reshape the legacy SQLAlchemy-string APIs (v1.13.0 → v1.14.0)

This project already has users of:

- `fetch_sql(sql: str, ...)`
- `iter_sql(sql: str, ...)`
- `write_sql(data, table_name, ...)`

**Deliverables (v1.13.0)**

- Update docs to recommend **SQLModel-first**.
- Keep legacy APIs for compatibility, but:
  - either require SQLModel installed anyway (since `pydantable[sql]` now includes it), or
  - move legacy APIs behind a separate extra (not recommended if you want one SQL story).

**Deliverables (v1.14.0 target)**

- Deprecation warnings on legacy APIs pointing to SQLModel equivalents.
- Optional: introduce `fetch_sql_raw(...)` naming to make “raw SQL” an explicit escape hatch.

**Acceptance criteria**

- Users can migrate without losing streaming/batching behavior.
- Docs clearly state the “two-tier” story: SQLModel-first APIs are the default; raw SQL is advanced.

---

## Phase 5 — Typed schema bridging helpers (stretch for v1.13.0, otherwise v1.14.0+)

**Goal:** reduce friction between SQLModel and `DataFrameModel` while preserving explicit ownership.

Candidate helpers:

- `pydantable.io.sqlmodel_columns(model)` → list of SQL column names/keys
- `MyDF.assert_sqlmodel_compatible(UserTable, *, direction="read|write")`
- Optional name-mapping support (SQL column names vs python attributes) as an explicit mapping dict.

---

## Phase 6 — Documentation + examples + testing gates (v1.13.0)

**Docs**

- Update {doc}`IO_SQL` to reflect SQLModel-first APIs and the `replace_ok` safety knob.
- Add a runnable example:
  - SQLite round-trip using SQLModel table + pydantable `DataFrameModel`
  - streaming example mirroring the existing `iter_sql` example

**Tests**

- Unit tests for:
  - batch shapes for `iter_sqlmodel`
  - `replace` DDL correctness (table columns and basic types)
  - `append` into existing table
  - `replace_ok` guardrail
- Integration tests using SQLite (like current SQL tests) that run under CI without external services.

**Release gate**

- `make check-full` green on the release commit
- docs examples validated (if you keep example-verification tooling)

---

## Proposed milestone mapping (v1.13.0)

- **M1 (core)**: Phase 0–2 (extras + SQLModel read/write APIs)
- **M2 (DX)**: Phase 3 (DataFrameModel convenience wrappers)
- **M3 (ship)**: Phase 6 (docs + examples + tests)
- **Post-ship**: Phase 4/5 hardening (deprecation strategy + compatibility helpers)

