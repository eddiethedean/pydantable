# SQLModel-first SQL I/O roadmap

This document records the **plan and phased roadmap** for pydantable’s **SQLModel-first** SQL I/O. **v1.13.0** is the release where the new APIs are the recommended default and **Phases 0–6** are implemented (see status lines under each phase below).

## Goals (what “SQLModel-first” means)

- **SQLModel is required for SQL I/O**: if you want to read/write from a database using **`from pydantable import …`** (SQL helpers) or **`DataFrameModel`**, you install **`pydantable[sql]`**, which includes **SQLModel + SQLAlchemy** (drivers remain user-installed).
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
from sqlalchemy.engine import Connection, Engine  # bind types (SQLModel uses SQLAlchemy engines under the hood)

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

**Status (v1.13.0 dev): Implemented** — **`write_sqlmodel`** in **`python/pydantable/io/sqlmodel_write.py`** (chunked inserts, **`schema=`** must match **`model.__table__.schema`**, strict column-key alignment with the table, **`replace_ok`** required for **`if_exists="replace"`**, optional **`validate_rows`** with row index on **`ValidationError`**). **`write_sqlmodel_batches`**, **`awrite_sqlmodel`**, and **`awrite_sqlmodel_batches`** in **`python/pydantable/io/__init__.py`**; **`write_sqlmodel`** registered as a built-in writer. Package re-exports match reads. Tests: **`tests/test_sqlmodel_io_phase02.py`**.

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

**Status (v1.13.0 dev): Implemented** — **`DataFrameModel`** exposes SQLModel I/O alongside the **`pydantable.io`** functions: instance **`write_sqlmodel` / `awrite_sqlmodel`** (they call **`to_dict()`** then the dict writers); classmethods **`fetch_sqlmodel`**, **`afetch_sqlmodel`** (**`AwaitableDataFrameModel`**), **`iter_sqlmodel`**, **`aiter_sqlmodel`**, and dict-path **`write_sqlmodel_data` / `awrite_sqlmodel_data`** (the **`Async`** namespace **`write_sqlmodel`** delegates to **`awrite_sqlmodel_data`**). Implementation: **`python/pydantable/dataframe_model.py`**. Tests: **`tests/test_sqlmodel_dataframe_model.py`**.

---

## Phase 4 — Deprecate/reshape the legacy SQLAlchemy-string APIs (v1.13.0)

This project already has users of:

- `fetch_sql(sql: str, ...)`
- `iter_sql(sql: str, ...)`
- `write_sql(data, table_name, ...)`

**Deliverables (documentation / policy)**

- Update docs to recommend **SQLModel-first** and describe **raw SQL** as the advanced / migration path — see {doc}`IO_SQL`, {doc}`DATAFRAMEMODEL`, and this page.
- Keep legacy string-SQL APIs (**`fetch_sql`**, **`iter_sql`**, **`write_sql`**, and async mirrors) for compatibility. **`pydantable[sql]`** remains the single extra for SQL I/O (it bundles **SQLModel**); there is no separate extra split for “raw SQL only.”

**Deliverables (runtime migration path, v1.13.0)**

- Deprecation warnings on legacy string-SQL APIs, with replacements **`fetch_sql_raw`**, **`iter_sql_raw`**, **`write_sql_raw`**, **`afetch_sql_raw`**, **`aiter_sql_raw`**, **`awrite_sql_raw`** (and deprecation of **`write_sql_batches`** / **`awrite_sql_batches`**).
- Explicit **`*_raw`** naming for string-SQL I/O (see {doc}`IO_SQL`).

**Status (v1.13.0): Implemented** — docs and **`DataFrameModel`** pointers in place; warnings in **`python/pydantable/io/sql.py`** and **`python/pydantable/io/__init__.py`**; tests **`tests/test_sql_string_deprecation.py`**; suite-wide **`DeprecationWarning`** filter in **`pyproject.toml`** for existing tests; plugin registry marks legacy readers/writers **`stable=False`** and **`*_raw`** as **`stable=True`**.

**Removal policy**

- Legacy names remain until a **major** release (no earlier than **`2.0.0`**); see {doc}`VERSIONING`.

**Acceptance criteria**

- Users can migrate without losing streaming/batching behavior.
- Docs clearly state the “two-tier” story: SQLModel-first APIs are the default; explicit **`*_raw`** string-SQL is advanced; deprecated unprefixed names warn.

---

## Phase 5 — Typed schema bridging helpers (v1.13.0)

**Goal:** reduce friction between SQLModel and `DataFrameModel` while preserving explicit ownership.

**Deliverables**

- `pydantable.io.sqlmodel_columns(model)` → ordered list of SQLAlchemy column keys for `model.__table__` (same keys as full-table `fetch_sqlmodel` / `write_sqlmodel`).
- `MyDF.assert_sqlmodel_compatible(UserTable, *, direction="read"|"write", column_map=None, read_keys=None)` — dev-time check; `direction="write"` enforces exact key parity with the table (after `column_map`); `direction="read"` requires every mapped dataframe field to appear in the expected result keys (default: full table, or pass `read_keys` for `fetch_sqlmodel(..., columns=...)`).
- Optional `column_map: dict[str, str]` mapping **dataframe field name → SQL column key** when names differ.

**Status (v1.13.0): Implemented** — `python/pydantable/io/sqlmodel_schema.py`, `DataFrameModel.assert_sqlmodel_compatible` in `python/pydantable/dataframe_model.py`; tests `tests/test_sqlmodel_bridge_phase05.py`; docs {doc}`IO_SQL`, {doc}`DATAFRAMEMODEL`.

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

**Status (v1.13.0): Implemented** — {doc}`IO_SQL` documents **raw string SQL** vs **SQLModel-first** APIs; the runnable doc example is **`docs/examples/io/sql_sqlite_sqlmodel_roundtrip.py`** (**`tests/test_doc_io_examples.py`**). SQLModel I/O behavior is covered by **`tests/test_sqlmodel_io_phase01.py`**, **`tests/test_sqlmodel_io_phase02.py`**, **`tests/test_sqlmodel_dataframe_model.py`**.

---

## Proposed milestone mapping

- **M1 (core)**: Phase 0–2 (extras + SQLModel read/write APIs)
- **M2 (DX)**: Phase 3 (DataFrameModel convenience wrappers)
- **M3 (ship)**: Phase 6 (docs + examples + tests) — **v1.13.0**
- **Also in v1.13.0**: Phase 4 (legacy string-SQL deprecation + **`*_raw`** APIs); Phase 5 (schema bridging)

