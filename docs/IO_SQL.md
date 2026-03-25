# SQL I/O (SQLAlchemy)

**Module:** `pydantable.io.sql` · **Re-exports:** `pydantable.io`, package root **`pydantable`** (`fetch_sql`, `afetch_sql`, `write_sql`, `awrite_sql`). **`DataFrameModel.fetch_sql` / `afetch_sql`** wrap the same functions.

## Install

Install **`pydantable[sql]`** plus the **DB-API driver** your URL needs (PostgreSQL, MySQL, SQLite, etc.). SQLAlchemy supports many dialects; pydantable does not bundle drivers.

## Read (source) → `dict[str, list]`

- **`fetch_sql(sql, bind, *, parameters=None)`**
- **`afetch_sql(..., *, executor=None)`** — runs the sync implementation in **`asyncio.to_thread`** (optional **`concurrent.futures.Executor`**).

**`bind`** may be a SQLAlchemy **URL string**, **`Engine`**, or **`Connection`**. Use **bound parameters** only; never interpolate untrusted input into **`sql`**.

Result columns become dict keys; each value is a **list** of row values for that column (columnar layout for **`DataFrameModel`** / **`DataFrame`** constructors).

## Write (target)

- **`write_sql(data, table_name, bind, *, schema=None, if_exists="append")`**
- **`awrite_sql(..., *, executor=None)`**

**`data`** is **`dict[str, list]`**. **`if_exists="append"`** requires the table to exist already; rows are inserted. **`if_exists="replace"`** drops the table if present, recreates it with inferred column types, then inserts (**`table_name`** / **`schema`** must be trusted identifiers, not user-controlled strings).

## Runnable example

Requires **`sqlalchemy`** (install **`pydantable[sql]`**). Run conventions: {doc}`IO_OVERVIEW` (**Runnable example**).

```bash
python docs/examples/io/sql_sqlite_roundtrip.py
```

```{literalinclude} examples/io/sql_sqlite_roundtrip.py
:language: python
```

## See also

{doc}`IO_OVERVIEW` · {doc}`DATA_IO_SOURCES` (async stacks) · {doc}`FASTAPI`
