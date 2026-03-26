# Choosing an I/O API

Use this page to pick the right entry point. Execution semantics (lazy collect vs streaming) are in {doc}`EXECUTION`.

## Quick reference

| Goal | Use | Returns / effect |
|------|-----|-------------------|
| **Scan a local file without loading full columns into Python** | **`DataFrame[Schema].read_*`** or **`MyModel.read_*`**, or **`pydantable.io.read_*`** | **`ScanFileRoot`** → lazy plan |
| **Run transforms on a big file, then write without a giant dict** | **`read_*`** → … → **`DataFrame.write_parquet`** (or **`write_csv`** / **`write_ipc`** / **`write_ndjson`**) | File on disk |
| **Load everything into `dict[str, list]`** (eager) | **`materialize_*`** / **`amaterialize_*`**, **`fetch_sql`**, **`fetch_*_url`**, **`read_from_object_store`** | Column dict |
| **Persist an in-memory column dict to a file** (eager) | **`export_*`** / **`aexport_*`** | File on disk |
| **Query or load from a database** | **`fetch_sql`** / **`afetch_sql`**; **`write_sql`** / **`awrite_sql`** for append/replace | Column dict in / none out |
| **HTTP(S) download** | **`fetch_bytes`**, **`fetch_parquet_url`**, **`fetch_csv_url`**, **`fetch_ndjson_url`** (eager dicts); **`read_parquet_url`** / **`aread_parquet_url`** for **lazy** Parquet (temp file — see {doc}`IO_HTTP`) | Varies |
| **Object-store URIs (`s3://`, …)** | **`read_from_object_store`** (**`[cloud]`**) | Column dict |
| **Tier-2 readers (Excel, Delta, …)** | **`pydantable.io.extras`** | Column dict or helpers |

**Full surface:** import from **`pydantable.io`** — the top-level **`pydantable`** package re-exports only a small subset for common Parquet paths; see {doc}`IO_OVERVIEW`.

## Engine selection (`materialize_parquet` and friends)

Set **`PYDANTABLE_IO_ENGINE`** to **`auto`** (default), **`rust`**, or **`pyarrow`** where supported.

### Parquet (`materialize_parquet`)

- **`auto` / `rust`:** local **file path** and **`columns is None`** → Rust fast path when the extension is built.
- **PyArrow path:** **`columns`** is set, or **`source`** is **`bytes`** / **`BinaryIO`**, or Rust fails and **`auto`** falls back.

### CSV / NDJSON (`materialize_csv`, `materialize_ndjson`)

- **`auto` / `rust`:** try Rust readers for a **local path**; on failure **`auto`** falls back to stdlib **`csv`** or JSON lines parsing in Python.

### IPC (`materialize_ipc`)

- **`auto` / `rust`:** local on-disk IPC **file** with **`as_stream=False`**; streams and buffers use PyArrow.

See implementation notes in **`pydantable.io`** module docstrings for edge cases.

## Typed frame first vs `pydantable.io` only

- Prefer **`DataFrameModel`** / **`DataFrame[Schema]`** when you want **validation** and **typed** **`Expr`** pipelines.
- Use bare **`pydantable.io`** functions in scripts, tests, or when building a **`dict[str, list]`** before wrapping **`MyModel(cols)`**.

For **`DataFrameModel`** convenience wrappers around **`pydantable.io`** — **`from_sql`** / **`afrom_sql`**, **`export_*`**, **`write_sql`** / **`awrite_sql`** — see {doc}`DATAFRAMEMODEL` and {doc}`IO_OVERVIEW`.
