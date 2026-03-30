# SQL I/O (SQLAlchemy)

**Primary:** **`pydantable.io.fetch_sql`**, **`afetch_sql`**, **`iter_sql`**, **`aiter_sql`** — they return **`dict[str, list]`** batches (or streaming containers). Wrap results in **`MyModel(cols, ...)`** for a typed **`DataFrameModel`**. **`DataFrameModel`** no longer exposes eager SQL loaders; use **`pydantable.io`** and the constructor (or **`collect`** / **`to_dict`** only after you have a frame).

**Write path:** **`MyModel.write_sql`** / **`await MyModel.awrite_sql`** (same as **`pydantable.io.write_sql`** with a concrete model class for ergonomics).

Install **`pydantable[sql]`** plus the **DB-API driver** your URL needs. SQLAlchemy supports many dialects; pydantable does not bundle drivers.

## `DataFrameModel`

**Read (typed)**

- Call **`fetch_sql`** / **`await afetch_sql`** / **`iter_sql`** / **`aiter_sql`** from **`pydantable.io`**, then **`MyModel(cols, trusted_mode=...)`** (or **`MyModel(batch, ...)`** per batch from **`iter_sql`** / **`aiter_sql`**).

**`bind`** may be a SQLAlchemy **URL string**, **`Engine`**, or **`Connection`**. Use **bound parameters** only; never interpolate untrusted input into **`sql`**.

**Write to a database**

- **`MyModel.write_sql(data, table_name, bind, *, schema=None, if_exists="append")`**
- **`await MyModel.awrite_sql(..., executor=None)`**

**`data`** is **`dict[str, list]`** — typically **`model.to_dict()`** or the column dict from **`pydantable.io.fetch_sql`**. Raw **`pydantable.io.write_sql`** is the same operation without a model class.

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
