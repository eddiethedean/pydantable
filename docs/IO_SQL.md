# SQL I/O (SQLAlchemy)

**Primary:** **`DataFrameModel.fetch_sql`**, **`afetch_sql`** on a concrete subclass (typed rows from **`SELECT`**). **Secondary:** **`pydantable.io.fetch_sql`**, **`write_sql`** (and async mirrors) — raw **`dict[str, list]`** without a model.

Install **`pydantable[sql]`** plus the **DB-API driver** your URL needs. SQLAlchemy supports many dialects; pydantable does not bundle drivers.

## `DataFrameModel`

**Read**

- **`MyModel.fetch_sql(sql, bind, *, parameters=None, batch_size=None, auto_stream=True, auto_stream_threshold_rows=None)`**
- **`await MyModel.afetch_sql(..., batch_size=None, auto_stream=True, auto_stream_threshold_rows=None, executor=None)`**
- **`MyModel.iter_sql(..., batch_size=None)`** → iterator of **typed** `DataFrameModel` batches
- **`async for b in MyModel.aiter_sql(..., batch_size=None, executor=None)`** → async iterator of **typed** batches

**`bind`** may be a SQLAlchemy **URL string**, **`Engine`**, or **`Connection`**. Use **bound parameters** only; never interpolate untrusted input into **`sql`**.

**Write to a database**

There is **no** **`DataFrameModel.write_sql`**. Call **`write_sql`** / **`awrite_sql`** from **`pydantable.io`** with a column dict (e.g. **`model.to_dict()`** or **`materialize_*`** output).

## `pydantable.io`

**Read**

- **`fetch_sql(sql, bind, *, parameters=None, batch_size=None, auto_stream=True, auto_stream_threshold_rows=None)`** → **`dict[str, list]`** *or* a streaming container with **`.to_dict()`**
- **`iter_sql(sql, bind, *, parameters=None, batch_size=None)`** → iterator of **`dict[str, list]`** batches (**streaming**)
- **`afetch_sql(..., *, executor=None)`** — **`asyncio.to_thread`** (optional **`Executor`**)
- **`aiter_sql(..., batch_size=None, executor=None)`** — async generator yielding batches (threaded sync SQLAlchemy)

### When to use `iter_sql` / `aiter_sql`

Use streaming when the result set might be too large to fit comfortably in memory:

- **`fetch_sql`**: simplest; returns `dict[str, list]` for small queries. For large queries it may return a streaming container; call `.to_dict()` to force full materialization.\n - **`iter_sql`**: yields **batches**; you can process/write each batch and discard it.\n - **`aiter_sql`**: same idea for `async def` contexts (FastAPI), without blocking the event loop.
- **`iter_sql`**: yields **batches**; you can process/write each batch and discard it.
- **`aiter_sql`**: same idea for `async def` contexts (FastAPI), without blocking the event loop.

### Configuration (env vars)\n+\n- **`PYDANTABLE_SQL_FETCH_BATCH_SIZE`**: default batch size for cursor `fetchmany`.\n- **`PYDANTABLE_SQL_WRITE_CHUNK_SIZE`**: default chunk size for inserts.\n- **`PYDANTABLE_SQL_AUTO_STREAM_THRESHOLD_ROWS`**: row count threshold above which `fetch_sql` returns the streaming container by default.\n+
**Write**

- **`write_sql(data, table_name, bind, *, schema=None, if_exists=\"append\", chunk_size=None)`**
- **`awrite_sql(..., chunk_size=None, executor=None)`**
- **`write_sql_batches(batches, table_name, bind, *, if_exists=\"append\", chunk_size=None)`**
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
