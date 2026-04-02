# SQL I/O (SQLAlchemy)

**Recommended for new code (SQLModel):** **`pydantable.io.fetch_sqlmodel`**, **`iter_sqlmodel`**, **`afetch_sqlmodel`**, **`aiter_sqlmodel`** — same column-dict / **`StreamingColumns`** behavior as the raw-SQL helpers, but you pass a **`SQLModel`** class with **`table=True`** instead of a SQL string. Install **`pydantable[sql]`** (includes **SQLModel** + SQLAlchemy); that one extra covers both SQLModel-first APIs and legacy string-SQL entrypoints.

**Legacy / escape hatch (raw SQL):** **`pydantable.io.fetch_sql`**, **`afetch_sql`**, **`iter_sql`**, **`aiter_sql`**, **`write_sql`**, **`awrite_sql`** (and batch variants). Use these when the query or destination is **not** a mapped **`SQLModel`** table — for example **dynamic SQL**, ad hoc reporting, migrations, or integrating with existing string-based SQL. They return **`dict[str, list]`** batches (or streaming containers) for a literal **`sql`** string (reads) or take a **`table_name`** string (writes). For **app / service** code with a stable table schema, prefer **`fetch_sqlmodel` / `iter_sqlmodel` / `write_sqlmodel`** and the **`DataFrameModel`** mirrors below.

**Write path:** **`pydantable.io.write_sqlmodel`** / **`awrite_sqlmodel`** for schema-driven tables, **`MyModel.write_sqlmodel_data`** / **`await MyModel.awrite_sqlmodel_data`** for dict payloads, **`my_frame.write_sqlmodel(...)`** / **`await my_frame.awrite_sqlmodel(...)`** using the frame’s **`to_dict()`**, or legacy **`MyModel.write_sql`** / **`await MyModel.awrite_sql`** when you only have a table name string.

Install **`pydantable[sql]`** plus the **DB-API driver** your URL needs. SQLAlchemy supports many dialects; pydantable does not bundle drivers.

## SQLModel-first reads

Use a mapped table model and optional **`where`**, **`order_by`**, **`limit`**, **`columns`**, and bound **`parameters`** (for parameterized **`where`** clauses):

```python
from pydantable.io import fetch_sqlmodel
from sqlmodel import Field, SQLModel

class User(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str

cols = fetch_sqlmodel(User, engine, order_by=[User.id])
# MyUserModel(cols, trusted_mode=...)
```

**`iter_sqlmodel`** / **`aiter_sqlmodel`** stream **`dict[str, list]`** batches; **`fetch_sqlmodel`** / **`afetch_sqlmodel`** mirror **`fetch_sql`** for **`batch_size`**, **`auto_stream`**, and **`auto_stream_threshold_rows`**. Without SQLModel installed, these APIs raise **`MissingOptionalDependency`** — install **`pydantable[sql]`**.

## SQLModel-first writes

Use a **`SQLModel`** class with **`table=True`** so DDL comes from **`model.__table__`** (not from inferred types like legacy **`write_sql`** **`if_exists="replace"`**).

- **`write_sqlmodel(data, model, bind, *, schema=None, if_exists="append", chunk_size=None, validate_rows=False, replace_ok=False)`** — insert a column dict. **`if_exists="append"`** requires the table to exist. **`if_exists="replace"`** drops and recreates the table from the model, then inserts; you must pass **`replace_ok=True`** (destructive).
- **`write_sqlmodel_batches(batches, model, bind, …)`** — same pattern as **`write_sql_batches`**: first batch uses **`if_exists`**, later batches append.
- **`await awrite_sqlmodel(..., executor=None)`** / **`awrite_sqlmodel_batches`** — **`asyncio.to_thread`** wrappers (same as **`awrite_sql`**).

**`data`** keys must match the model’s table columns exactly (including nullable / autoincrement columns; **`None`** primary keys are omitted on insert where appropriate). With **`validate_rows=True`**, each row is checked with **`model.model_validate`**; failures include the row index.

## `DataFrameModel`

**Read (typed), SQLModel-first**

- **`MyModel.fetch_sqlmodel(UserTable, bind, *, trusted_mode=..., ...)`** — same validation kwargs as lazy file readers (**`trusted_mode`**, **`fill_missing_optional`**, **`ignore_errors`**, **`on_validation_errors`**).
- **`await MyModel.afetch_sqlmodel(UserTable, bind, ...)`** → **`AwaitableDataFrameModel`**: **`await …`** for a concrete frame (see {doc}`DATAFRAMEMODEL`).
- **`MyModel.iter_sqlmodel(...)`** / **`async for batch in MyModel.aiter_sqlmodel(...)`** — typed batches.

**Read (raw SQL)**

- Call **`fetch_sql`** / **`await afetch_sql`** / **`iter_sql`** / **`aiter_sql`** from **`pydantable.io`**, then **`MyModel(cols, trusted_mode=...)`** (or **`MyModel(batch, ...)`** per batch from **`iter_sql`** / **`aiter_sql`**).

**`bind`** may be a SQLAlchemy **URL string**, **`Engine`**, or **`Connection`**. Use **bound parameters** only; never interpolate untrusted input into **`sql`**.

**Write to a database**

- **SQLModel:** **`my_frame.write_sqlmodel(UserTable, bind, *, if_exists=..., ...)`** / **`await my_frame.awrite_sqlmodel(...)`**; or **`MyModel.write_sqlmodel_data(data, UserTable, bind, ...)`** / **`await MyModel.awrite_sqlmodel_data(...)`** for a column dict. **`MyModel.Async.write_sqlmodel`** matches **`awrite_sqlmodel_data`**.
- **Legacy string table name:** **`MyModel.write_sql(data, table_name, bind, *, schema=None, if_exists="append")`** / **`await MyModel.awrite_sql(..., executor=None)`**.

**`data`** is **`dict[str, list]`** — typically **`model.to_dict()`** or the column dict from **`pydantable.io.fetch_sql`**. Raw **`pydantable.io.write_sql`** is the same operation without a **`DataFrameModel`** class.

## `pydantable.io`

**Read**

- **`fetch_sql(sql, bind, *, parameters=None, batch_size=None, auto_stream=True, auto_stream_threshold_rows=None)`** → **`dict[str, list]`** *or* a streaming container with **`.to_dict()`**
- **`iter_sql(sql, bind, *, parameters=None, batch_size=None)`** → iterator of **`dict[str, list]`** batches (**streaming**)
- **`afetch_sql(..., *, executor=None)`** — **`asyncio.to_thread`** (optional **`Executor`**)
- **`aiter_sql(..., batch_size=65_536, executor=None)`** — async generator yielding batches (threaded sync SQLAlchemy)

### When to use `iter_sql` / `aiter_sql`

Use streaming when the result set might be too large to fit comfortably in memory:

- **`fetch_sql`**: simplest API; often returns a plain `dict[str, list]` (one batch internally). Above **`auto_stream_threshold_rows`**, it may return **`StreamingColumns`** (see below)—call **`.to_dict()`** to build one materialized dict when you need it.
- **`iter_sql`**: yields **`dict[str, list]`** batches; process or persist each batch and drop it so peak memory stays bounded. Omit **`batch_size`** to use **`PYDANTABLE_SQL_FETCH_BATCH_SIZE`** (or the library default).
- **`aiter_sql`**: same streaming pattern in **`async def`** handlers (work runs off the event loop). Its default **`batch_size`** is a fixed **`65_536`** unless you pass another positive int (it does not read the env var when you rely on that default).

### `StreamingColumns` (large `fetch_sql` results)

When **`fetch_sql`** switches to the streaming path, the return value is a **`collections.abc.Mapping`** of column name → list. Columns are built **lazily** the first time you index them (**`result["col"]`**), then cached. Use:

- **`.to_dict()`** — materialize every column into a single `dict[str, list]` (same shape as a normal **`fetch_sql`** dict).
- **`.batches()`** — inspect the underlying list of batch dicts (advanced / debugging).

For small results, **`fetch_sql`** returns a plain **`dict`** (including multi-batch merges when auto-streaming is off or the row count stays under the threshold).

### Configuration (env vars)

Set these before importing callers if you want process-wide defaults (invalid values raise **`ValueError`** at read time):

- **`PYDANTABLE_SQL_FETCH_BATCH_SIZE`**: default **`batch_size`** for **`iter_sql`** / **`fetch_sql`** when **`batch_size`** is omitted.
- **`PYDANTABLE_SQL_WRITE_CHUNK_SIZE`**: default **`chunk_size`** for **`write_sql`** / **`write_sql_batches`** when **`chunk_size`** is omitted.
- **`PYDANTABLE_SQL_AUTO_STREAM_THRESHOLD_ROWS`**: row count above which **`fetch_sql`** returns **`StreamingColumns`** when **`auto_stream=True`** (default).

**Write**

- **`write_sql(data, table_name, bind, *, schema=None, if_exists="append", chunk_size=None)`**
- **`awrite_sql(..., chunk_size=None, executor=None)`**
- **`write_sql_batches(batches, table_name, bind, *, if_exists="append", chunk_size=None)`**
- **`awrite_sql_batches(..., chunk_size=None, executor=None)`**

**`data`** is **`dict[str, list]`**. **`if_exists="append"`** requires the table to exist already. **`if_exists="replace"`** drops the table if present, recreates it with inferred column types, then inserts (**`table_name`** / **`schema`** must be trusted identifiers, not user-controlled strings).

## Runnable example

Requires **`sqlalchemy`**. Run conventions: {doc}`IO_OVERVIEW` (**Runnable example**).

```bash
python docs/examples/io/sql_sqlite_roundtrip.py
```

```{literalinclude} examples/io/sql_sqlite_roundtrip.py
:language: python
```

## Runnable streaming example

```bash
python docs/examples/io/sql_sqlite_streaming.py
```

```{literalinclude} examples/io/sql_sqlite_streaming.py
:language: python
```

## See also

{doc}`IO_OVERVIEW` · {doc}`DATA_IO_SOURCES` (async stacks) · {doc}`FASTAPI`
