# SQL I/O (SQLAlchemy)

**Primary:** **`DataFrameModel.fetch_sql`**, **`afetch_sql`** on a concrete subclass (typed rows from **`SELECT`**). **Secondary:** **`pydantable.io.fetch_sql`**, **`write_sql`** (and async mirrors) — raw **`dict[str, list]`** without a model.

Install **`pydantable[sql]`** plus the **DB-API driver** your URL needs. SQLAlchemy supports many dialects; pydantable does not bundle drivers.

## `DataFrameModel`

**Read**

- **`MyModel.fetch_sql(sql, bind, *, parameters=None)`**
- **`await MyModel.afetch_sql(..., executor=None)`**

**`bind`** may be a SQLAlchemy **URL string**, **`Engine`**, or **`Connection`**. Use **bound parameters** only; never interpolate untrusted input into **`sql`**.

**Write to a database**

There is **no** **`DataFrameModel.write_sql`**. Call **`write_sql`** / **`awrite_sql`** from **`pydantable.io`** with a column dict (e.g. **`model.to_dict()`** or **`materialize_*`** output).

## `pydantable.io`

**Read**

- **`fetch_sql(sql, bind, *, parameters=None)`** → **`dict[str, list]`**
- **`afetch_sql(..., *, executor=None)`** — **`asyncio.to_thread`** (optional **`Executor`**)

**Write**

- **`write_sql(data, table_name, bind, *, schema=None, if_exists="append")`**
- **`awrite_sql(..., *, executor=None)`**

**`data`** is **`dict[str, list]`**. **`if_exists="append"`** requires the table to exist already. **`if_exists="replace"`** drops the table if present, recreates it with inferred column types, then inserts (**`table_name`** / **`schema`** must be trusted identifiers, not user-controlled strings).

## Runnable example

Requires **`sqlalchemy`**. Run conventions: {doc}`IO_OVERVIEW` (**Runnable example**).

```bash
python docs/examples/io/sql_sqlite_roundtrip.py
```

```{literalinclude} examples/io/sql_sqlite_roundtrip.py
:language: python
```

## See also

{doc}`IO_OVERVIEW` · {doc}`DATA_IO_SOURCES` (async stacks) · {doc}`FASTAPI`
