# SQL I/O (SQLAlchemy)

**Recommended for new code (SQLModel):** **`fetch_sqlmodel`**, **`iter_sqlmodel`**, **`afetch_sqlmodel`**, **`aiter_sqlmodel`** (**``from pydantable import …``**) — same column-dict / **`StreamingColumns`** behavior as the raw-SQL helpers, but you pass a **`SQLModel`** class with **`table=True`** instead of a SQL string. Install **`pydantable[sql]`** (includes **SQLModel** + SQLAlchemy); that one extra covers SQLModel-first APIs, explicit string-SQL helpers, and the deprecated legacy names.

**Explicit raw string SQL:** **`fetch_sql_raw`**, **`iter_sql_raw`**, **`write_sql_raw`**, **`afetch_sql_raw`**, **`aiter_sql_raw`**, **`awrite_sql_raw`** — use these when the query or destination is **not** a mapped **`SQLModel`** table (dynamic SQL, ad hoc reporting, migrations, string **`table_name`** writes). Same semantics as the deprecated unprefixed names, without **`DeprecationWarning`**.

**Deprecated (compatibility):** **`fetch_sql`**, **`afetch_sql`**, **`iter_sql`**, **`aiter_sql`**, **`write_sql`**, **`awrite_sql`**, **`write_sql_batches`**, **`awrite_sql_batches`** emit **`DeprecationWarning`** (see [VERSIONING](/semantics/versioning/)); migrate to **`*_raw`** or SQLModel APIs. For **app / service** code with a stable table schema, prefer **`fetch_sqlmodel` / `iter_sqlmodel` / `write_sqlmodel`** and the **`DataFrameModel`** mirrors below.

**Write path:** **`write_sqlmodel`** / **`awrite_sqlmodel`** for schema-driven tables, **`MyModel.write_sqlmodel_data`** / **`await MyModel.awrite_sqlmodel_data`** for dict payloads, **`my_frame.write_sqlmodel(...)`** / **`await my_frame.awrite_sqlmodel(...)`** using the frame’s **`to_dict()`**, **`write_sql_raw`** / **`await awrite_sql_raw`** for string table names, or deprecated **`MyModel.write_sql`** / **`await MyModel.awrite_sql`** (same warning as the legacy **`pydantable.io`** entrypoints).

Install **`pydantable[sql]`** plus the **DB-API driver** your URL needs. SQLAlchemy supports many dialects; pydantable does not bundle drivers.

## SQLModel-first reads

Use a mapped table model and optional **`where`**, **`order_by`**, **`limit`**, **`columns`**, and bound **`parameters`** (for parameterized **`where`** clauses):

```python
from pydantable import fetch_sqlmodel
from sqlmodel import Field, SQLModel, create_engine

class User(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str

engine = create_engine("sqlite:///./app.db")
cols = fetch_sqlmodel(User, engine, order_by=[User.id])
# MyUserModel(cols, trusted_mode=...)
```

**`iter_sqlmodel`** / **`aiter_sqlmodel`** stream **`dict[str, list]`** batches; **`fetch_sqlmodel`** / **`afetch_sqlmodel`** mirror **`fetch_sql_raw`** for **`batch_size`**, **`auto_stream`**, and **`auto_stream_threshold_rows`**. Without SQLModel installed, these APIs raise **`MissingOptionalDependency`** — install **`pydantable[sql]`**.

## SQLModel-first writes

Use a **`SQLModel`** class with **`table=True`** so DDL comes from **`model.__table__`** (not from inferred types like legacy **`write_sql_raw`** **`if_exists="replace"`**).

- **`write_sqlmodel(data, model, bind, *, schema=None, if_exists="append", chunk_size=None, validate_rows=False, replace_ok=False)`** — insert a column dict. **`if_exists="append"`** requires the table to exist. **`if_exists="replace"`** drops and recreates the table from the model, then inserts; you must pass **`replace_ok=True`** (destructive).
- **`write_sqlmodel_batches(batches, model, bind, …)`** — same pattern as repeated **`write_sql_raw`**: first batch uses **`if_exists`**, later batches append.
- **`await awrite_sqlmodel(..., executor=None)`** / **`awrite_sqlmodel_batches`** — **`asyncio.to_thread`** wrappers (same pattern as **`awrite_sql_raw`**).

**`data`** keys must match the model’s table columns exactly (including nullable / autoincrement columns; **`None`** primary keys are omitted on insert where appropriate). With **`validate_rows=True`**, each row is checked with **`model.model_validate`**; failures include the row index.

### Schema bridging (Phase 5)

Use **`sqlmodel_columns`** (**``from pydantable import sqlmodel_columns``**) to list ordered SQLAlchemy column keys for a **`table=True`** model—the same key set a full **`fetch_sqlmodel`** result uses and **`write_sqlmodel`** expects.

**`MyModel.assert_sqlmodel_compatible(UserTable, *, direction='read'|'write', column_map=None, read_keys=None)`** checks that your **`DataFrameModel`** field names align with that table before I/O:

- **`direction='write'`** — after applying **`column_map`** (dataframe field → SQL key), the mapped names must equal the table’s keys exactly (matches **`write_sqlmodel`** / **`to_dict()`**).
- **`direction='read'`** — every mapped field must appear in the keys you expect from SQL (default: full table; pass **`read_keys`** when using **`fetch_sqlmodel(..., columns=(...))`**).

## `DataFrameModel`

**Read (typed), SQLModel-first**

- **`MyModel.fetch_sqlmodel(UserTable, bind, *, trusted_mode=..., ...)`** — same validation kwargs as lazy file readers (**`trusted_mode`**, **`fill_missing_optional`**, **`ignore_errors`**, **`on_validation_errors`**).
- **`await MyModel.afetch_sqlmodel(UserTable, bind, ...)`** → **`AwaitableDataFrameModel`**: **`await …`** for a concrete frame (see [DATAFRAMEMODEL](/user-guide/dataframemodel/)).
- **`MyModel.iter_sqlmodel(...)`** / **`async for batch in MyModel.aiter_sqlmodel(...)`** — typed batches.

**Read (raw SQL)**

- Call **`fetch_sql_raw`** / **`await afetch_sql_raw`** / **`iter_sql_raw`** / **`aiter_sql_raw`** (**``from pydantable import …``**), then **`MyModel(cols, trusted_mode=...)`** (or **`MyModel(batch, ...)`** per batch). Deprecated unprefixed names still work but warn.

**`bind`** may be a SQLAlchemy **URL string**, **`Engine`**, or **`Connection`**. Use **bound parameters** only; never interpolate untrusted input into **`sql`**.

**Write to a database**

- **SQLModel:** **`my_frame.write_sqlmodel(UserTable, bind, *, if_exists=..., ...)`** / **`await my_frame.awrite_sqlmodel(...)`**; or **`MyModel.write_sqlmodel_data(data, UserTable, bind, ...)`** / **`await MyModel.awrite_sqlmodel_data(...)`** for a column dict. **`MyModel.Async.write_sqlmodel`** matches **`awrite_sqlmodel_data`**.
- **String table name:** **`MyModel.write_sql(...)`** / **`await MyModel.awrite_sql(...)`** (deprecated) or prefer **`write_sql_raw`** / **`awrite_sql_raw`** (**``from pydantable import …``**) in new code.

**`data`** is **`dict[str, list]`** — typically **`model.to_dict()`** or the column dict from **`fetch_sql_raw`**. **`write_sql_raw`** is the same write path without a **`DataFrameModel`** class.

## Reference (implementation module `pydantable.io`)

The following signatures are defined on **`pydantable.io`** for documentation and extension authors. **Application code** imports the same names **from `pydantable`** (see the root **`__init__.py`**).

**Read**

- **`fetch_sql_raw(sql, bind, *, parameters=None, batch_size=None, auto_stream=True, auto_stream_threshold_rows=None)`** → **`dict[str, list]`** *or* a streaming container with **`.to_dict()`**
- **`iter_sql_raw(sql, bind, *, parameters=None, batch_size=None)`** → iterator of **`dict[str, list]`** batches (**streaming**)
- **`afetch_sql_raw(..., *, executor=None)`** — **`asyncio.to_thread`** (optional **`Executor`**)
- **`aiter_sql_raw(..., batch_size=65_536, executor=None)`** — async generator yielding batches (threaded sync SQLAlchemy)
- Deprecated aliases **`fetch_sql`**, **`iter_sql`**, **`afetch_sql`**, **`aiter_sql`** — same signatures; emit **`DeprecationWarning`**.

### When to use `iter_sql_raw` / `aiter_sql_raw`

Use streaming when the result set might be too large to fit comfortably in memory:

- **`fetch_sql_raw`**: simplest API; often returns a plain `dict[str, list]` (one batch internally). Above **`auto_stream_threshold_rows`**, it may return **`StreamingColumns`** (see below)—call **`.to_dict()`** to build one materialized dict when you need it.
- **`iter_sql_raw`**: yields **`dict[str, list]`** batches; process or persist each batch and drop it so peak memory stays bounded. Omit **`batch_size`** to use **`PYDANTABLE_SQL_FETCH_BATCH_SIZE`** (or the library default).
- **`aiter_sql_raw`**: same streaming pattern in **`async def`** handlers (work runs off the event loop). Its default **`batch_size`** is a fixed **`65_536`** unless you pass another positive int (it does not read the env var when you rely on that default).

### `StreamingColumns` (large `fetch_sql_raw` results)

When **`fetch_sql_raw`** switches to the streaming path, the return value is a **`collections.abc.Mapping`** of column name → list. Columns are built **lazily** the first time you index them (**`result["col"]`**), then cached. Use:

- **`.to_dict()`** — materialize every column into a single `dict[str, list]` (same shape as a normal **`fetch_sql_raw`** dict).
- **`.batches()`** — inspect the underlying list of batch dicts (advanced / debugging).

For small results, **`fetch_sql_raw`** returns a plain **`dict`** (including multi-batch merges when auto-streaming is off or the row count stays under the threshold).

### Configuration (env vars)

Set these before importing callers if you want process-wide defaults (invalid values raise **`ValueError`** at read time):

- **`PYDANTABLE_SQL_FETCH_BATCH_SIZE`**: default **`batch_size`** for **`iter_sql_raw`** / **`fetch_sql_raw`** when **`batch_size`** is omitted.
- **`PYDANTABLE_SQL_WRITE_CHUNK_SIZE`**: default **`chunk_size`** for **`write_sql_raw`** / batched write helpers when **`chunk_size`** is omitted.
- **`PYDANTABLE_SQL_AUTO_STREAM_THRESHOLD_ROWS`**: row count above which **`fetch_sql_raw`** returns **`StreamingColumns`** when **`auto_stream=True`** (default).

**Write**

- **`write_sql_raw(data, table_name, bind, *, schema=None, if_exists="append", chunk_size=None)`**
- **`awrite_sql_raw(..., chunk_size=None, executor=None)`**
- **`write_sql_batches`** / **`awrite_sql_batches`** — deprecated; call **`write_sql_raw`** / **`awrite_sql_raw`** per batch or use **`write_sqlmodel_batches`**
- Deprecated **`write_sql`**, **`awrite_sql`** — same behavior; warn once per call.

**`data`** is **`dict[str, list]`**. **`if_exists="append"`** requires the table to exist already. **`if_exists="replace"`** drops the table if present, recreates it with inferred column types, then inserts (**`table_name`** / **`schema`** must be trusted identifiers, not user-controlled strings).

## Runnable example (SQLModel-first)

Doc examples focus on **lazy file execution** ([IO_OVERVIEW](/io/overview/)); SQL I/O does not use a Polars **`LazyFrame`**, but the runnable script below follows the same **SQLModel-first** style as application code.

Requires **`pydantable[sql]`** (SQLModel + SQLAlchemy). Uses a **`SQLModel`** class with **`table=True`** and **`DataFrameModel`** helpers.

### Round-trip (`fetch_sqlmodel` / `write_sqlmodel`)

```bash
python docs/examples/io/sql_sqlite_sqlmodel_roundtrip.py
```

??? note "Setup (optional: create a temp SQLite DB)"

    The example script is fully runnable as-is. The setup is just a temp directory + engine:

    ```python
    import tempfile
    from pathlib import Path

    from sqlmodel import create_engine

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "app.db"
        eng = create_engine(f"sqlite:///{db}")
    ```


--8<-- "examples/io/sql_sqlite_sqlmodel_roundtrip.py"

### Output

```text
--8<-- "examples/io/sql_sqlite_sqlmodel_roundtrip.py.out.txt"
```

**Raw string SQL** (**`fetch_sql_raw`**, **`iter_sql_raw`**, **`write_sql_raw`**) and **streaming batches** are documented in the reference sections above; they are not duplicated as separate runnable snippets here.

## See also

[IO_OVERVIEW](/io/overview/) · [DATA_IO_SOURCES](/io/data-io-sources/) (async stacks) · [FASTAPI](/integrations/fastapi/fastapi/)
